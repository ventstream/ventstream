//! In-memory state for the join engine.
//!
//! Three indexes, all keyed by [`PkValue`] (composite-aware):
//!
//! - **`foreign_rows`**: raw foreign row by `(table, pk)`. Used to
//!   re-materialize a primary's embedded data on every emit.
//! - **`primary_rows`**: per-primary-row state — the raw payload plus
//!   the FK values it currently references (so deletes can clean up
//!   the reverse index cleanly).
//! - **`primary_reverse`**: for each `related.id`, FK value → set of
//!   primary `(table, pk)` references. Drives re-emission when a
//!   foreign row changes.
//! - **`foreign_by_fk`**: for `cardinality: many` related entries,
//!   FK value → set of foreign PKs. Drives composition lookup
//!   ("give me every line_item with order_id = 5").
//!
//! Concurrency: a single mutator (the JoinEngine drain loop) is the
//! only writer. Readers can be added later behind a `parking_lot::RwLock`
//! but v1 keeps everything owned by one task — no locks, no contention.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};

use bytes::Bytes;
use serde_json::Value;
use tracing::warn;

use crate::key::PkValue;
use crate::persistence::{PersistError, PersistentBackend};

/// Compact in-memory representation of a row.
///
/// We previously stored `serde_json::Value`, which is convenient but
/// burns ~3-5x the bytes the JSON itself takes (per-string heap allocs,
/// Map/Vec overhead, enum discriminants). For the join engine the only
/// access pattern is *deserialize on compose*, so caching the raw JSON
/// bytes and decoding on demand cuts the resident set by 50–70% on
/// realistic schemas while paying only a small CPU tax per access.
///
/// The bytes are reference-counted via [`bytes::Bytes`], so the same
/// foreign row referenced by 20 primaries doesn't get cloned 20 times
/// — they all share the same backing buffer.
#[derive(Clone, Debug)]
pub struct CompactRow(Bytes);

impl CompactRow {
    /// Construct from a borrowed `Value`. Allocates the JSON bytes once.
    pub fn from_value(v: &Value) -> Self {
        // Failure is unreachable for any Value the rest of the engine
        // would ever pass in (no NaN/Inf top-level), but we don't want
        // to panic in production code. Fall back to an explicit null
        // bytestring — a downstream decode produces `Value::Null`,
        // which the compose path already handles.
        let bytes = serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec());
        Self(Bytes::from(bytes))
    }

    /// Wrap pre-serialized JSON bytes. Used by the persistence-load
    /// path to skip a round-trip through `Value`.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(Bytes::from(bytes))
    }

    /// Decode back to a `Value`. Allocates fresh on every call —
    /// callers cache the result if they read it multiple times.
    pub fn as_value(&self) -> Value {
        match serde_json::from_slice(&self.0) {
            Ok(v) => v,
            Err(err) => {
                warn!(error = %err, "CompactRow decode failed; returning Value::Null");
                Value::Null
            }
        }
    }

    /// Borrow the raw JSON bytes. Used by persistence to dump without
    /// re-serializing.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Reverse-index key — the `id` field of a `RelatedDefinition`.
pub type RelatedId = String;

/// Fully-qualified table name in `{namespace}.{relation}` form.
pub type TableName = String;

/// Identity of a single row in the system: `(table, pk)`.
pub type RowKey = (TableName, PkValue);

/// Keyset cursor for walking every primary in one related reverse index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelatedPrimaryCursor {
    primary: RowKey,
}

/// What we remember per primary row.
#[derive(Debug, Clone)]
pub struct PrimaryRowState {
    /// Raw payload last seen for the primary row, stored as the
    /// serialized JSON bytes the source originally emitted. We keep
    /// it so we can re-compose without persisting the composed doc.
    /// Compact representation: see [`CompactRow`] — decoded
    /// lazily during compose, never held as a `Value` in steady state.
    pub raw: CompactRow,
    /// For each related id, the FK value the primary currently
    /// references. Used to clean up the reverse index on delete /
    /// PK-change update.
    pub fk_values: HashMap<RelatedId, PkValue>,
}

/// In-memory join state. Default-constructed empty.
///
/// When a [`PersistentBackend`] is attached via
/// [`Self::with_backend`], every mutation is mirrored to disk and
/// the saved state can be replayed into a fresh instance via
/// [`Self::load_from_backend`]. The in-memory maps remain the
/// authoritative source during steady-state operation; the backend
/// is write-through.
#[derive(Debug)]
pub struct JoinState {
    foreign_rows: HashMap<RowKey, CompactRow>,
    primary_rows: HashMap<RowKey, PrimaryRowState>,
    primary_reverse: HashMap<RelatedId, BTreeMap<PkValue, BTreeSet<RowKey>>>,
    foreign_by_fk: HashMap<RelatedId, HashMap<PkValue, HashSet<PkValue>>>,
    /// Optional persistent backing store. When `Some`, every
    /// mutation is mirrored to disk.
    backend: Option<PersistentBackend>,
    /// Stable source/checkpoint identity written with a completed snapshot.
    persistence_identity: Option<String>,
    /// First write-through failure observed since startup. Mutators still
    /// update memory so one logical event remains internally consistent, but
    /// the next durability boundary fails and prevents source checkpoint
    /// advancement.
    persist_failure: Option<String>,
    /// Runtime toggle that suppresses backend writes without
    /// detaching the backend. Used by the bootstrap path to skip
    /// per-row persistence during the snapshot window — the
    /// orchestrator dumps the final state with one call afterwards.
    /// Defaults to `true`; only false when explicitly disabled.
    persist_enabled: bool,
}

impl Default for JoinState {
    fn default() -> Self {
        Self {
            foreign_rows: HashMap::default(),
            primary_rows: HashMap::default(),
            primary_reverse: HashMap::default(),
            foreign_by_fk: HashMap::default(),
            backend: None,
            persistence_identity: None,
            persist_failure: None,
            persist_enabled: true,
        }
    }
}

impl JoinState {
    /// Construct an empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a persistent backend. Mutations from this point on
    /// will be mirrored to disk.
    pub fn with_backend(mut self, backend: PersistentBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Associate this state store with one source checkpoint namespace.
    #[must_use]
    pub fn with_persistence_identity(mut self, identity: impl Into<String>) -> Self {
        self.persistence_identity = Some(identity.into());
        self
    }

    /// Turn per-row persistence on or off without detaching the
    /// backend. Used by the bootstrap path: disable for the snapshot
    /// window, run the snapshot in-memory only, then call
    /// [`Self::dump_to_persistent`] and re-enable.
    pub fn set_persist_enabled(&mut self, enabled: bool) {
        self.persist_enabled = enabled;
    }

    /// Whether per-mutation backend writes are currently routed.
    /// `true` does NOT imply a backend is attached — the actual write
    /// also requires `Self::with_backend`.
    pub fn persist_enabled(&self) -> bool {
        self.persist_enabled
    }

    /// Borrow the attached backend, returning `None` when there is
    /// none OR when persistence is currently disabled. Used by every
    /// mutator to gate the per-row write.
    fn persist_backend_opt(&self) -> Option<&PersistentBackend> {
        if self.persist_enabled {
            self.backend.as_ref()
        } else {
            None
        }
    }

    fn record_persist_error(&mut self, op: &str, err: &PersistError) {
        warn!(
            op,
            error = %err,
            "persistent state write failed; the next durability boundary will fail"
        );
        if self.persist_failure.is_none() {
            self.persist_failure = Some(format!("{op}: {err}"));
        }
    }

    fn ensure_persistence_healthy(&self) -> Result<(), PersistError> {
        match self.persist_failure.as_deref() {
            Some(detail) => Err(PersistError::Transaction(format!(
                "an earlier join-state mutation was not persisted ({detail})"
            ))),
            None => Ok(()),
        }
    }

    /// Force-flush the backend's pending writes. No-op when no
    /// backend is attached. Called from the engine drain loop on
    /// shutdown or after the snapshot dump.
    pub fn flush_persistent(&mut self) -> Result<(), PersistError> {
        self.ensure_persistence_healthy()?;
        match self.backend.as_ref() {
            Some(b) => b.flush(),
            None => Ok(()),
        }
    }

    /// Signal the end of one fully-applied logical event so the backend can
    /// commit its batched transaction on a clean boundary (M1). No-op without
    /// a backend. The engine calls this after each `handle()`.
    pub fn commit_boundary(&mut self) -> Result<(), PersistError> {
        self.ensure_persistence_healthy()?;
        match self.backend.as_ref() {
            Some(b) => b.commit_boundary(),
            None => Ok(()),
        }
    }

    /// Push the full in-memory state to the attached backend as a
    /// single redb transaction. Truncates the existing on-disk state
    /// first — this is a replacement, not a merge.
    ///
    /// No-op when no backend is attached. Returns the underlying
    /// persistence error if the dump cannot complete; the runtime
    /// treats that as fatal and does not advance source progress.
    pub fn dump_to_persistent(&mut self) -> Result<(), PersistError> {
        self.ensure_persistence_healthy()?;
        let Some(b) = self.backend.as_ref() else {
            return Ok(());
        };
        let identity = self.persistence_identity.as_deref().ok_or_else(|| {
            PersistError::Transaction(
                "persistent join state is missing its source identity".to_owned(),
            )
        })?;
        let foreign_iter = self
            .foreign_rows
            .iter()
            .map(|((table, pk), row)| (table.as_str(), pk, row.as_bytes()));
        let primary_iter = self
            .primary_rows
            .iter()
            .map(|((table, pk), state)| (table.as_str(), pk, state));
        let reverse_items = self.primary_reverse.iter().flat_map(|(related_id, by_fk)| {
            by_fk.iter().flat_map(move |(fk_value, primaries)| {
                primaries.iter().map(move |(primary_table, primary_pk)| {
                    (
                        related_id.as_str(),
                        fk_value,
                        primary_table.as_str(),
                        primary_pk,
                    )
                })
            })
        });
        let by_fk_items = self.foreign_by_fk.iter().flat_map(|(related_id, by_fk)| {
            by_fk.iter().flat_map(move |(fk_value, foreigns)| {
                foreigns
                    .iter()
                    .map(move |foreign_pk| (related_id.as_str(), fk_value, foreign_pk))
            })
        });
        b.dump_state_with_identity(
            foreign_iter,
            primary_iter,
            reverse_items,
            by_fk_items,
            identity,
        )
    }

    /// Load every persisted record from the backend into the
    /// in-memory maps. Call this BEFORE attaching the backend with
    /// [`Self::with_backend`] so the replay itself doesn't double-
    /// write to disk.
    ///
    /// Returns the counts of replayed entries (for log lines /
    /// metrics).
    pub fn load_from_backend(
        &mut self,
        backend: &PersistentBackend,
    ) -> Result<crate::persistence::LoadStats, crate::persistence::PersistError> {
        backend.load(
            |table, pk, row_bytes| {
                self.foreign_rows
                    .insert((table.to_owned(), pk), CompactRow::from_bytes(row_bytes));
            },
            |table, pk, raw_bytes, fk_values| {
                self.primary_rows.insert(
                    (table.to_owned(), pk),
                    PrimaryRowState {
                        raw: CompactRow::from_bytes(raw_bytes),
                        fk_values,
                    },
                );
            },
            |related_id, fk, primary_table, primary_pk| {
                self.primary_reverse
                    .entry(related_id.to_owned())
                    .or_default()
                    .entry(fk)
                    .or_default()
                    .insert((primary_table.to_owned(), primary_pk));
            },
            |related_id, fk, foreign_pk| {
                self.foreign_by_fk
                    .entry(related_id.to_owned())
                    .or_default()
                    .entry(fk)
                    .or_default()
                    .insert(foreign_pk);
            },
        )
    }

    // ---- foreign row CRUD ------------------------------------------------

    /// Store or replace a foreign row. The caller passes a [`Value`];
    /// we serialize once into a [`CompactRow`] for compact in-memory
    /// retention and reuse those same bytes for the persistence write.
    pub fn set_foreign(&mut self, table: &str, pk: &PkValue, row: &Value) {
        let compact = CompactRow::from_value(row);
        let persist_error = self
            .persist_backend_opt()
            .and_then(|backend| backend.set_foreign(table, pk, compact.as_bytes()).err());
        if let Some(err) = persist_error {
            self.record_persist_error("set_foreign", &err);
        }
        self.foreign_rows
            .insert((table.to_owned(), pk.clone()), compact);
    }

    /// Remove a foreign row. Returns the decoded prior row if any.
    /// Returning `Value` keeps the call site ergonomic; the decode
    /// only fires when the caller actually needed the row.
    pub fn delete_foreign(&mut self, table: &str, pk: &PkValue) -> Option<Value> {
        let persist_error = self
            .persist_backend_opt()
            .and_then(|backend| backend.delete_foreign(table, pk).err());
        if let Some(err) = persist_error {
            self.record_persist_error("delete_foreign", &err);
        }
        self.foreign_rows
            .remove(&(table.to_owned(), pk.clone()))
            .map(|c| c.as_value())
    }

    /// Retrieve a foreign row as a `Value`. Allocates fresh on each
    /// call — compose paths cache the decoded value within a single
    /// re-emit cycle. Returns `None` when the row isn't in state
    /// (caller can fall back to sync-on-miss).
    pub fn get_foreign(&self, table: &str, pk: &PkValue) -> Option<Value> {
        self.foreign_rows
            .get(&(table.to_owned(), pk.clone()))
            .map(|c| c.as_value())
    }

    // ---- primary row CRUD ------------------------------------------------

    /// Store or replace a primary row's state.
    pub fn set_primary(&mut self, table: &str, pk: &PkValue, state: PrimaryRowState) {
        let persist_error = self
            .persist_backend_opt()
            .and_then(|backend| backend.set_primary(table, pk, &state).err());
        if let Some(err) = persist_error {
            self.record_persist_error("set_primary", &err);
        }
        self.primary_rows
            .insert((table.to_owned(), pk.clone()), state);
    }

    /// Remove a primary row. Returns its prior state, useful for
    /// reverse-index cleanup.
    pub fn take_primary(&mut self, table: &str, pk: &PkValue) -> Option<PrimaryRowState> {
        let persist_error = self
            .persist_backend_opt()
            .and_then(|backend| backend.take_primary(table, pk).err());
        if let Some(err) = persist_error {
            self.record_persist_error("take_primary", &err);
        }
        self.primary_rows.remove(&(table.to_owned(), pk.clone()))
    }

    /// Borrow a primary row's state.
    pub fn get_primary(&self, table: &str, pk: &PkValue) -> Option<&PrimaryRowState> {
        self.primary_rows.get(&(table.to_owned(), pk.clone()))
    }

    // ---- primary reverse index -------------------------------------------

    /// Add `(primary_table, primary_pk)` to the set of primaries that
    /// reference foreign-key value `fk_value` for the given related id.
    pub fn add_primary_reverse(
        &mut self,
        related_id: &str,
        fk_value: &PkValue,
        primary_table: &str,
        primary_pk: &PkValue,
    ) {
        let persist_error = self.persist_backend_opt().and_then(|backend| {
            backend
                .add_primary_reverse(related_id, fk_value, primary_table, primary_pk)
                .err()
        });
        if let Some(err) = persist_error {
            self.record_persist_error("add_primary_reverse", &err);
        }
        self.primary_reverse
            .entry(related_id.to_owned())
            .or_default()
            .entry(fk_value.clone())
            .or_default()
            .insert((primary_table.to_owned(), primary_pk.clone()));
    }

    /// Remove a single primary from the reverse index. Cleans up empty
    /// buckets so a long-running pipeline doesn't accumulate residue.
    pub fn remove_primary_reverse(
        &mut self,
        related_id: &str,
        fk_value: &PkValue,
        primary_table: &str,
        primary_pk: &PkValue,
    ) {
        let persist_error = self.persist_backend_opt().and_then(|backend| {
            backend
                .remove_primary_reverse(related_id, fk_value, primary_table, primary_pk)
                .err()
        });
        if let Some(err) = persist_error {
            self.record_persist_error("remove_primary_reverse", &err);
        }
        let Some(by_value) = self.primary_reverse.get_mut(related_id) else {
            return;
        };
        let key = (primary_table.to_owned(), primary_pk.clone());
        if let Some(set) = by_value.get_mut(fk_value) {
            set.remove(&key);
            if set.is_empty() {
                by_value.remove(fk_value);
            }
        }
        if by_value.is_empty() {
            self.primary_reverse.remove(related_id);
        }
    }

    /// Look up every primary row that references the given FK value
    /// for a related id. Returns an empty list if none.
    #[cfg(test)]
    pub fn primaries_for(&self, related_id: &str, fk_value: &PkValue) -> Vec<RowKey> {
        self.primary_reverse
            .get(related_id)
            .and_then(|m| m.get(fk_value))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Return at most `limit` primaries from the ordered union of one or two
    /// FK buckets, strictly after `after`. The merge deduplicates a primary
    /// present in both buckets without building a fan-out-sized temporary set.
    pub(crate) fn primaries_for_keys_chunk(
        &self,
        related_id: &str,
        fk_value: &PkValue,
        secondary_fk_value: Option<&PkValue>,
        after: Option<&RowKey>,
        limit: usize,
    ) -> Vec<RowKey> {
        if limit == 0 {
            return Vec::new();
        }
        let Some(by_fk) = self.primary_reverse.get(related_id) else {
            return Vec::new();
        };

        let mut primary = rows_after(by_fk.get(fk_value), after).peekable();
        let mut secondary = rows_after(
            secondary_fk_value
                .filter(|secondary| *secondary != fk_value)
                .and_then(|secondary| by_fk.get(secondary)),
            after,
        )
        .peekable();
        let mut rows = Vec::with_capacity(limit);

        while rows.len() < limit {
            match (primary.peek(), secondary.peek()) {
                (Some(left), Some(right)) => match left.cmp(right) {
                    std::cmp::Ordering::Less => {
                        rows.push((*left).clone());
                        primary.next();
                    }
                    std::cmp::Ordering::Greater => {
                        rows.push((*right).clone());
                        secondary.next();
                    }
                    std::cmp::Ordering::Equal => {
                        rows.push((*left).clone());
                        primary.next();
                        secondary.next();
                    }
                },
                (Some(row), None) => {
                    rows.push((*row).clone());
                    primary.next();
                }
                (None, Some(row)) => {
                    rows.push((*row).clone());
                    secondary.next();
                }
                (None, None) => break,
            }
        }
        rows
    }

    /// Return at most `limit` unique primaries from every FK bucket for a
    /// relation. The page is ordered by primary identity, so the last row is a
    /// stable keyset cursor even when one primary appears in multiple buckets.
    /// The temporary set never grows beyond `limit + 1`.
    pub(crate) fn primaries_for_relation_chunk(
        &self,
        related_id: &str,
        after: Option<&RelatedPrimaryCursor>,
        limit: usize,
    ) -> (Vec<RowKey>, Option<RelatedPrimaryCursor>) {
        if limit == 0 {
            return (Vec::new(), after.cloned());
        }
        let Some(by_fk) = self.primary_reverse.get(related_id) else {
            return (Vec::new(), None);
        };

        let mut rows = BTreeSet::new();
        for bucket in by_fk.values() {
            for primary in
                rows_after(bucket.into(), after.map(|cursor| &cursor.primary)).take(limit)
            {
                rows.insert(primary.clone());
                if rows.len() > limit {
                    rows.pop_last();
                }
            }
        }
        let rows = rows.into_iter().collect::<Vec<_>>();
        let next = rows
            .last()
            .cloned()
            .map(|primary| RelatedPrimaryCursor { primary });
        (rows, next)
    }

    // ---- foreign-by-FK secondary index (for cardinality:many) ------------

    /// Record that `foreign_pk` in the related table has the FK value
    /// `fk_value`. Drives 1-to-many compose lookups.
    pub fn add_foreign_by_fk(
        &mut self,
        related_id: &str,
        fk_value: &PkValue,
        foreign_pk: &PkValue,
    ) {
        let persist_error = self.persist_backend_opt().and_then(|backend| {
            backend
                .add_foreign_by_fk(related_id, fk_value, foreign_pk)
                .err()
        });
        if let Some(err) = persist_error {
            self.record_persist_error("add_foreign_by_fk", &err);
        }
        self.foreign_by_fk
            .entry(related_id.to_owned())
            .or_default()
            .entry(fk_value.clone())
            .or_default()
            .insert(foreign_pk.clone());
    }

    /// Drop a single (`related_id`, `fk_value`, `foreign_pk`) entry.
    pub fn remove_foreign_by_fk(
        &mut self,
        related_id: &str,
        fk_value: &PkValue,
        foreign_pk: &PkValue,
    ) {
        let persist_error = self.persist_backend_opt().and_then(|backend| {
            backend
                .remove_foreign_by_fk(related_id, fk_value, foreign_pk)
                .err()
        });
        if let Some(err) = persist_error {
            self.record_persist_error("remove_foreign_by_fk", &err);
        }
        let Some(by_value) = self.foreign_by_fk.get_mut(related_id) else {
            return;
        };
        if let Some(set) = by_value.get_mut(fk_value) {
            set.remove(foreign_pk);
            if set.is_empty() {
                by_value.remove(fk_value);
            }
        }
        if by_value.is_empty() {
            self.foreign_by_fk.remove(related_id);
        }
    }

    /// Enumerate every foreign PK in the related table that has the
    /// given FK value. Used by primary compose for `cardinality: many`.
    pub fn foreign_pks_for(&self, related_id: &str, fk_value: &PkValue) -> Vec<PkValue> {
        self.foreign_by_fk
            .get(related_id)
            .and_then(|m| m.get(fk_value))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    // ---- truncate (M8) ---------------------------------------------------

    /// Remove and return at most `limit` primary IDs for a truncated table.
    /// Every row and reverse link is mirrored through the normal persistence
    /// mutators, avoiding both a table-sized ID list and a full-state re-dump.
    pub(crate) fn take_primary_table_page(&mut self, table: &str, limit: usize) -> Vec<PkValue> {
        if limit == 0 {
            return Vec::new();
        }
        let keys: Vec<RowKey> = self
            .primary_rows
            .keys()
            .filter(|(t, _)| t == table)
            .take(limit)
            .cloned()
            .collect();
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            if self.take_primary(&key.0, &key.1).is_some() {
                removed.push(key.1);
            }
        }

        let removed_set = removed.iter().cloned().collect::<HashSet<_>>();
        let backend = self.persist_backend_opt().cloned();
        let mut persist_errors = Vec::new();
        self.primary_reverse.retain(|related_id, by_fk| {
            by_fk.retain(|fk_value, primaries| {
                primaries.retain(|(primary_table, primary_pk)| {
                    let remove = primary_table == table && removed_set.contains(primary_pk);
                    if remove {
                        if let Some(backend) = backend.as_ref() {
                            if let Err(err) = backend.remove_primary_reverse(
                                related_id,
                                fk_value,
                                primary_table,
                                primary_pk,
                            ) {
                                persist_errors.push(err);
                            }
                        }
                    }
                    !remove
                });
                !primaries.is_empty()
            });
            !by_fk.is_empty()
        });
        for err in persist_errors {
            self.record_persist_error("take_primary_table_page.remove_primary_reverse", &err);
        }
        removed
    }

    /// Purge all in-memory state for a TRUNCATED related table, scoped to one
    /// `related_id`: drop the foreign rows keyed `(table, *)` and the now-stale
    /// `foreign_by_fk` bucket for this relation. `primary_reverse` is KEPT so
    /// the engine can page through affected primaries without materializing
    /// every ID, and so a later child re-insert re-emits the primary. Mirror to
    /// disk through the normal per-entry persistence mutators.
    pub fn purge_related_table(&mut self, related_id: &str, table: &str) {
        let backend = self.persist_backend_opt().cloned();
        let mut persist_errors = Vec::new();
        self.foreign_rows.retain(|(row_table, pk), _| {
            if row_table != table {
                return true;
            }
            if let Some(backend) = backend.as_ref() {
                if let Err(err) = backend.delete_foreign(row_table, pk) {
                    persist_errors.push(("purge_related_table.delete_foreign", err));
                }
            }
            false
        });

        if let Some(by_fk) = self.foreign_by_fk.remove(related_id) {
            let backend = self.persist_backend_opt().cloned();
            for (fk_value, foreign_pks) in by_fk {
                for foreign_pk in foreign_pks {
                    if let Some(backend) = backend.as_ref() {
                        if let Err(err) =
                            backend.remove_foreign_by_fk(related_id, &fk_value, &foreign_pk)
                        {
                            persist_errors.push(("purge_related_table.remove_foreign_by_fk", err));
                        }
                    }
                }
            }
        }
        for (op, err) in persist_errors {
            self.record_persist_error(op, &err);
        }
    }

    // ---- diagnostics -----------------------------------------------------

    /// Total primary rows tracked.
    pub fn primary_count(&self) -> usize {
        self.primary_rows.len()
    }

    /// Total foreign rows tracked.
    pub fn foreign_count(&self) -> usize {
        self.foreign_rows.len()
    }
}

fn rows_after<'a>(
    rows: Option<&'a BTreeSet<RowKey>>,
    after: Option<&RowKey>,
) -> impl Iterator<Item = &'a RowKey> {
    let lower = after.cloned().map_or(Unbounded, Excluded);
    rows.into_iter()
        .flat_map(move |rows| rows.range((lower.clone(), Unbounded)))
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pk(value: i64) -> PkValue {
        PkValue::from_single(&json!(value))
    }

    #[test]
    fn foreign_round_trip() {
        let mut s = JoinState::new();
        s.set_foreign(
            "public.customers",
            &pk(1),
            &json!({"id": 1, "name": "Alice"}),
        );
        assert_eq!(s.foreign_count(), 1);
        assert_eq!(
            s.get_foreign("public.customers", &pk(1)),
            Some(json!({"id": 1, "name": "Alice"}))
        );
        assert!(s.delete_foreign("public.customers", &pk(1)).is_some());
        assert_eq!(s.foreign_count(), 0);
    }

    #[test]
    fn persistence_mutation_failure_is_sticky_at_durability_boundaries() {
        let mut state = JoinState::new();
        state.record_persist_error(
            "set_primary",
            &PersistError::Transaction("injected failure".to_owned()),
        );

        let flush_error = state.flush_persistent().expect_err("flush must fail");
        assert!(flush_error
            .to_string()
            .contains("an earlier join-state mutation was not persisted"));
        assert!(state.commit_boundary().is_err());
        assert!(state.dump_to_persistent().is_err());
    }

    #[test]
    fn primary_reverse_index_collects_multiple_primaries_per_fk() {
        let mut s = JoinState::new();
        s.add_primary_reverse("customer", &pk(5), "public.orders", &pk(1));
        s.add_primary_reverse("customer", &pk(5), "public.orders", &pk(2));
        s.add_primary_reverse("customer", &pk(7), "public.orders", &pk(3));

        let mut for_5 = s.primaries_for("customer", &pk(5));
        for_5.sort();
        assert_eq!(
            for_5,
            vec![
                ("public.orders".into(), pk(1)),
                ("public.orders".into(), pk(2)),
            ]
        );
        assert_eq!(
            s.primaries_for("customer", &pk(7)),
            vec![("public.orders".into(), pk(3))]
        );
    }

    #[test]
    fn primary_reverse_index_cleans_empty_buckets() {
        let mut s = JoinState::new();
        s.add_primary_reverse("customer", &pk(5), "public.orders", &pk(1));
        s.remove_primary_reverse("customer", &pk(5), "public.orders", &pk(1));
        // Bucket should be gone — operationally, this prevents
        // unbounded growth in long-running pipelines.
        assert!(s.primaries_for("customer", &pk(5)).is_empty());
    }

    #[test]
    fn primary_key_union_pages_are_bounded_and_deduplicated() {
        let mut s = JoinState::new();
        let row_count = 600;
        for id in 0..row_count {
            s.add_primary_reverse("coupon", &pk(5), "public.orders", &pk(id));
            if id % 2 == 0 {
                // Simulate a stale overlap while a foreign row is reparented.
                s.add_primary_reverse("coupon", &pk(6), "public.orders", &pk(id));
            }
        }

        let mut after = None;
        let mut all = Vec::new();
        let mut page_sizes = Vec::new();
        loop {
            let page =
                s.primaries_for_keys_chunk("coupon", &pk(6), Some(&pk(5)), after.as_ref(), 256);
            let Some(last) = page.last().cloned() else {
                break;
            };
            page_sizes.push(page.len());
            all.extend(page);
            after = Some(last);
        }

        assert_eq!(page_sizes, vec![256, 256, 88]);
        assert_eq!(all.len(), row_count as usize);
        let unique = all.iter().collect::<HashSet<_>>();
        assert_eq!(
            unique.len(),
            all.len(),
            "overlapping FK buckets deduplicate"
        );
    }

    #[test]
    fn foreign_by_fk_groups_for_one_to_many() {
        let mut s = JoinState::new();
        // Three line items belong to order 1, one to order 2.
        s.add_foreign_by_fk("items", &pk(1), &pk(100));
        s.add_foreign_by_fk("items", &pk(1), &pk(101));
        s.add_foreign_by_fk("items", &pk(1), &pk(102));
        s.add_foreign_by_fk("items", &pk(2), &pk(200));

        let mut for_1 = s.foreign_pks_for("items", &pk(1));
        for_1.sort();
        assert_eq!(for_1, vec![pk(100), pk(101), pk(102)]);
        assert_eq!(s.foreign_pks_for("items", &pk(2)), vec![pk(200)]);
    }

    #[test]
    fn unrelated_lookups_return_empty() {
        let s = JoinState::new();
        assert!(s.primaries_for("nope", &pk(1)).is_empty());
        assert!(s.foreign_pks_for("nope", &pk(1)).is_empty());
    }

    fn primary_state(id: i64, related: &str, fk: i64) -> PrimaryRowState {
        let mut fk_values = std::collections::HashMap::new();
        fk_values.insert(related.to_owned(), pk(fk));
        PrimaryRowState {
            raw: CompactRow::from_value(&json!({ "id": id })),
            fk_values,
        }
    }

    #[test]
    fn primary_truncate_page_removes_rows_and_reverse_links_only_for_that_table() {
        let mut s = JoinState::new();
        // Two orders (each referencing a customer FK) + reverse links.
        s.set_primary("public.orders", &pk(1), primary_state(1, "customer", 5));
        s.add_primary_reverse("customer", &pk(5), "public.orders", &pk(1));
        s.set_primary("public.orders", &pk(2), primary_state(2, "customer", 6));
        s.add_primary_reverse("customer", &pk(6), "public.orders", &pk(2));
        // A second definition sharing this primary can leave links that are
        // not represented in the row state's last-written `fk_values`.
        s.add_primary_reverse("coupon", &pk(7), "public.orders", &pk(1));
        // An unrelated primary in another table must survive.
        s.set_primary("public.invoices", &pk(9), primary_state(9, "customer", 5));
        s.add_primary_reverse("customer", &pk(5), "public.invoices", &pk(9));

        let mut removed = s.take_primary_table_page("public.orders", 256);
        removed.sort();
        assert_eq!(
            removed,
            vec![pk(1), pk(2)],
            "returns the purged primary PKs"
        );
        assert!(
            s.get_primary("public.invoices", &pk(9)).is_some(),
            "other table survives"
        );
        assert!(s.get_primary("public.orders", &pk(1)).is_none());
        // Orders' reverse links gone; the invoice's reverse link under the same
        // FK (5) is preserved.
        assert_eq!(
            s.primaries_for("customer", &pk(5)),
            vec![("public.invoices".into(), pk(9))]
        );
        assert!(s.primaries_for("customer", &pk(6)).is_empty());
        assert!(s.primaries_for("coupon", &pk(7)).is_empty());
    }

    #[test]
    fn large_primary_truncate_is_removed_in_bounded_pages() {
        let mut s = JoinState::new();
        let row_count = 600;
        for id in 0..row_count {
            s.set_primary(
                "public.orders",
                &pk(id),
                primary_state(id, "customer", id % 11),
            );
            s.add_primary_reverse("customer", &pk(id % 11), "public.orders", &pk(id));
        }

        let mut page_sizes = Vec::new();
        let mut removed = 0usize;
        loop {
            let page = s.take_primary_table_page("public.orders", 256);
            if page.is_empty() {
                break;
            }
            page_sizes.push(page.len());
            removed += page.len();
        }

        assert_eq!(page_sizes, vec![256, 256, 88]);
        assert_eq!(removed, row_count as usize);
        assert_eq!(s.primary_count(), 0);
        for fk in 0..11 {
            assert!(s.primaries_for("customer", &pk(fk)).is_empty());
        }
    }

    #[test]
    fn purge_related_table_clears_children_and_keeps_reverse_index() {
        let mut s = JoinState::new();
        // Two line items (FK = order 1) cached for the "items" relation.
        s.set_foreign("public.line_items", &pk(100), &json!({"id":100}));
        s.set_foreign("public.line_items", &pk(101), &json!({"id":101}));
        s.add_foreign_by_fk("items", &pk(1), &pk(100));
        s.add_foreign_by_fk("items", &pk(1), &pk(101));
        // Order 1 embeds them (reverse index).
        s.add_primary_reverse("items", &pk(1), "public.orders", &pk(1));
        // Unrelated foreign row in another table must survive.
        s.set_foreign("public.customers", &pk(5), &json!({"id":5}));

        s.purge_related_table("items", "public.line_items");
        // Truncated table's foreign rows gone; the other table's row stays.
        assert!(s.get_foreign("public.line_items", &pk(100)).is_none());
        assert!(s.get_foreign("public.customers", &pk(5)).is_some());
        // Stale child cache cleared...
        assert!(s.foreign_pks_for("items", &pk(1)).is_empty());
        // ...but the reverse index is KEPT so a re-inserted child re-emits the
        // primary.
        assert_eq!(
            s.primaries_for("items", &pk(1)),
            vec![("public.orders".into(), pk(1))]
        );
    }

    #[test]
    fn related_truncate_primary_pages_are_bounded_across_fk_buckets() {
        let mut s = JoinState::new();
        let row_count = 600;
        for id in 0..row_count {
            let fk = id % 11;
            s.add_primary_reverse("items", &pk(fk), "public.orders", &pk(id));
            s.add_primary_reverse("items", &pk(fk + 11), "public.orders", &pk(id));
        }
        s.purge_related_table("items", "public.line_items");

        let mut cursor = None;
        let mut all = Vec::new();
        let mut page_sizes = Vec::new();
        loop {
            let (page, next) = s.primaries_for_relation_chunk("items", cursor.as_ref(), 256);
            if page.is_empty() {
                break;
            }
            page_sizes.push(page.len());
            all.extend(page);
            cursor = next;
        }

        assert_eq!(page_sizes, vec![256, 256, 88]);
        assert_eq!(all.len(), row_count as usize);
        let unique = all.iter().collect::<HashSet<_>>();
        assert_eq!(
            unique.len(),
            all.len(),
            "a primary in multiple FK buckets must be emitted once"
        );
    }
}
