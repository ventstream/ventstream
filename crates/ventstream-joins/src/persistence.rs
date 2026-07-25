//! Persistent backing store for the join engine's in-memory state.
//!
//! When the engine is configured with a state directory, every
//! mutation to [`JoinState`](crate::state::JoinState) is mirrored to
//! a redb database on disk. On engine startup the saved state is
//! reloaded before WAL streaming resumes — restarts no longer mean
//! a cold-start cache miss for every primary row.
//!
//! ## Why redb
//!
//! - Pure-Rust embedded key-value store, no native deps.
//! - Copy-on-write B+ tree, MVCC reads, ACID writes.
//! - Single-process; matches the engine's single-mutator design.
//! - On-disk format is stable across redb 2.x releases.
//!
//! ## Concurrency
//!
//! redb allows multiple concurrent read transactions and one writer.
//! The join engine drives all mutations from a single task, so we
//! hold a single pending [`redb::WriteTransaction`] behind a mutex
//! and append to it from every mutator. The transaction commits
//! either when its op count reaches a configurable batch threshold
//! or when callers explicitly invoke [`PersistentBackend::flush`]
//! (e.g. at end of snapshot).
//!
//! ## Why batching matters
//!
//! Each redb `commit()` triggers an `fsync`, which on local SSDs caps
//! out at ~250 IOPS. With one fsync per state mutation, a single
//! primary row with five related entries paid six fsyncs — about
//! 25 ms per row. Batching at 5000 ops collapses that to one fsync
//! per ~800 primary rows, lifting bootstrap throughput from ~40
//! rows/sec to "wire speed" territory.
//!
//! Source progress advances only after the join engine confirms the sink
//! barrier and flushes this state. A write-through failure is retained as a
//! fatal state error, so a later successful transaction cannot hide a partial
//! logical mutation and advance the source checkpoint.

use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;

use crate::key::PkValue;
use crate::state::PrimaryRowState;

/// Default ops per redb commit. ~800 primary rows for a 6-related
/// join — small enough that a crash drops <1 second of work, large
/// enough that fsync stops being the bottleneck.
const DEFAULT_BATCH_OPS: usize = 5_000;

// === Table definitions ====================================================

/// `foreign_rows`: full row JSON keyed by `(table_name, pk_bytes)`.
const FOREIGN_ROWS: TableDefinition<'_, (&str, &[u8]), &[u8]> =
    TableDefinition::new("foreign_rows");

/// `primary_rows`: per-primary state (raw payload + fk_values) keyed
/// by `(table_name, pk_bytes)`. Value is JSON-encoded
/// [`PersistedPrimaryRow`].
const PRIMARY_ROWS: TableDefinition<'_, (&str, &[u8]), &[u8]> =
    TableDefinition::new("primary_rows");

/// `primary_reverse`: set membership keyed by `(related_id,
/// fk_pk_bytes, primary_table, primary_pk_bytes)`. Value is empty —
/// presence indicates membership.
type PrimaryReverseKey<'a> = (&'a str, &'a [u8], &'a str, &'a [u8]);
const PRIMARY_REVERSE: TableDefinition<'_, PrimaryReverseKey<'static>, ()> =
    TableDefinition::new("primary_reverse");

/// `foreign_by_fk`: set membership keyed by `(related_id,
/// fk_pk_bytes, foreign_pk_bytes)`. Same semantics as
/// `primary_reverse`.
type ForeignByFkKey<'a> = (&'a str, &'a [u8], &'a [u8]);
const FOREIGN_BY_FK: TableDefinition<'_, ForeignByFkKey<'static>, ()> =
    TableDefinition::new("foreign_by_fk");

/// Metadata committed atomically with a completed state snapshot.
const METADATA: TableDefinition<'_, &str, &str> = TableDefinition::new("metadata");
const STATE_IDENTITY_KEY: &str = "source_identity";

// === Errors ===============================================================

/// Errors from the persistence layer.
#[derive(Debug, Error)]
pub enum PersistError {
    /// Could not open the redb database file (directory missing,
    /// permissions, corrupt file).
    #[error("opening redb database at {path}: {detail}")]
    Open {
        /// Path of the database file.
        path: String,
        /// Stringified underlying redb error.
        detail: String,
    },

    /// A redb transaction failed.
    #[error("redb transaction failed: {0}")]
    Transaction(String),

    /// A serde encode/decode failure.
    #[error("encoding state: {0}")]
    Encoding(String),
}

// === Persisted shapes =====================================================

/// On-disk shape of a primary row's state.
///
/// `raw_json` holds the row payload as a JSON string — what the CDC
/// source originally emitted. We use `String` rather than `Value` so
/// the in-memory representation can be a compact `bytes::Bytes` and
/// round-trip to disk without re-parsing. `fk_values` uses raw byte
/// arrays so [`PkValue`] composite keys round-trip exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPrimaryRow {
    raw_json: String,
    fk_values: std::collections::HashMap<String, Vec<u8>>,
}

// === Public backend =======================================================

/// Persistent backing for the join engine's state.
///
/// Cheap to clone — the database handle is internally `Arc`-shared.
/// The clone semantics let us hand a writer reference to
/// [`JoinState`](crate::state::JoinState) without surrendering the
/// owning reference, useful for tests that want to inspect on-disk
/// state directly.
#[derive(Clone)]
pub struct PersistentBackend {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for PersistentBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // redb's Database doesn't derive Debug; nothing here is
        // useful to print anyway. Show the type name + a sentinel
        // so log lines that include a JoinState don't look broken.
        f.debug_struct("PersistentBackend").finish_non_exhaustive()
    }
}

struct Inner {
    db: Database,
    /// A single pending write transaction shared across mutators.
    /// Mutations append into it; the transaction commits when its op
    /// count reaches `max_batch_ops` or when [`PersistentBackend::flush`]
    /// is called.
    pending: Mutex<Pending>,
    /// Op-count threshold for an automatic commit.
    max_batch_ops: usize,
}

/// Open write transaction + the count of mutations applied to it
/// since the last commit.
#[derive(Default)]
struct Pending {
    tx: Option<WriteTransaction>,
    ops: usize,
}

impl PersistentBackend {
    /// Open (or create) the database at `path`. The parent directory
    /// must exist; redb does not create one for us.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistError> {
        Self::open_with_batch_size(path, DEFAULT_BATCH_OPS)
    }

    /// Like [`Self::open`] but with an explicit batch threshold. Used
    /// by tests that want deterministic commit points, and by future
    /// operators tuning durability/throughput trade-offs.
    pub fn open_with_batch_size(
        path: impl AsRef<Path>,
        max_batch_ops: usize,
    ) -> Result<Self, PersistError> {
        let path_str = path.as_ref().display().to_string();
        let db = Database::create(&path).map_err(|err| PersistError::Open {
            path: path_str.clone(),
            detail: err.to_string(),
        })?;

        // One-time setup: open every table so subsequent reads don't
        // race against table creation. `open_table` is idempotent —
        // a table that already exists is reused.
        let tx = db
            .begin_write()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        {
            let _ = tx
                .open_table(FOREIGN_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            let _ = tx
                .open_table(PRIMARY_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            let _ = tx
                .open_table(PRIMARY_REVERSE)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            let _ = tx
                .open_table(FOREIGN_BY_FK)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            let _ = tx
                .open_table(METADATA)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        tx.commit()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;

        info!(
            path = %path_str,
            max_batch_ops,
            "persistent join state opened"
        );
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                pending: Mutex::new(Pending::default()),
                max_batch_ops,
            }),
        })
    }

    /// Borrow the live pending transaction, opening a fresh one if
    /// none exists. Returns `Err` if the mutex is poisoned or if redb
    /// can't begin a transaction.
    fn ensure_pending_tx<'a>(
        pending: &'a mut Pending,
        db: &Database,
    ) -> Result<&'a mut WriteTransaction, PersistError> {
        if pending.tx.is_none() {
            let tx = db
                .begin_write()
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            pending.tx = Some(tx);
            pending.ops = 0;
        }
        match pending.tx.as_mut() {
            Some(tx) => Ok(tx),
            // Unreachable — we just set tx to Some above and hold the
            // mutex guard. Avoid `expect` to keep clippy quiet.
            None => Err(PersistError::Transaction(
                "internal: pending transaction missing after init".into(),
            )),
        }
    }

    /// Commit the pending transaction if its op count reached the
    /// threshold. Called only from [`Self::commit_boundary`] — never
    /// mid-event — so the batch only ever spans whole logical operations.
    fn maybe_commit(pending: &mut Pending, max_batch_ops: usize) -> Result<(), PersistError> {
        if pending.ops >= max_batch_ops {
            if let Some(tx) = pending.tx.take() {
                tx.commit()
                    .map_err(|err| PersistError::Transaction(err.to_string()))?;
            }
            pending.ops = 0;
        }
        Ok(())
    }

    /// Mark a logical-operation boundary: the end of one fully-applied CDC
    /// event. Commits the batched transaction iff it has reached the op
    /// threshold (M1). Mutators no longer commit themselves, so the on-disk
    /// state can only ever reflect a whole number of logical events — a crash
    /// can never persist a half-applied event (primary updated but its reverse
    /// index not yet). Cheap when nothing is pending or the threshold isn't
    /// met.
    pub fn commit_boundary(&self) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        Self::maybe_commit(&mut pending, self.inner.max_batch_ops)
    }

    /// Commit whatever is pending right now. Idempotent — a no-op
    /// when no transaction is open. Used at end-of-snapshot and on
    /// graceful shutdown to make sure no batched writes are lost.
    pub fn flush(&self) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        if let Some(tx) = pending.tx.take() {
            tx.commit()
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops = 0;
        Ok(())
    }

    /// Source identity of the last fully dumped snapshot. `None` means this
    /// database has never reached a recoverable snapshot boundary.
    pub fn state_identity(&self) -> Result<Option<String>, PersistError> {
        let tx = self
            .inner
            .db
            .begin_read()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        let table = tx
            .open_table(METADATA)
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        table
            .get(STATE_IDENTITY_KEY)
            .map(|value| value.map(|value| value.value().to_owned()))
            .map_err(|err| PersistError::Transaction(err.to_string()))
    }

    // --- foreign rows --------------------------------------------------

    pub(crate) fn set_foreign(
        &self,
        table: &str,
        pk: &PkValue,
        row_bytes: &[u8],
    ) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(FOREIGN_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.insert((table, pk.as_bytes()), row_bytes)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    pub(crate) fn delete_foreign(&self, table: &str, pk: &PkValue) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(FOREIGN_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.remove((table, pk.as_bytes()))
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    // --- primary rows --------------------------------------------------

    pub(crate) fn set_primary(
        &self,
        table: &str,
        pk: &PkValue,
        state: &PrimaryRowState,
    ) -> Result<(), PersistError> {
        // raw is already a JSON byte buffer — interpret as UTF-8 to
        // store as `raw_json`. CDC sources always produce valid UTF-8
        // for row payloads; the `from_utf8_lossy` fallback only kicks
        // in for synthetic / corrupted inputs.
        let raw_json = String::from_utf8_lossy(state.raw.as_bytes()).into_owned();
        let persisted = PersistedPrimaryRow {
            raw_json,
            fk_values: state
                .fk_values
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                .collect(),
        };
        let bytes = serde_json::to_vec(&persisted)
            .map_err(|err| PersistError::Encoding(err.to_string()))?;

        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(PRIMARY_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.insert((table, pk.as_bytes()), bytes.as_slice())
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    pub(crate) fn take_primary(&self, table: &str, pk: &PkValue) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(PRIMARY_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.remove((table, pk.as_bytes()))
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    // --- reverse indexes ---------------------------------------------

    pub(crate) fn add_primary_reverse(
        &self,
        related_id: &str,
        fk_value: &PkValue,
        primary_table: &str,
        primary_pk: &PkValue,
    ) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(PRIMARY_REVERSE)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.insert(
                (
                    related_id,
                    fk_value.as_bytes(),
                    primary_table,
                    primary_pk.as_bytes(),
                ),
                (),
            )
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    pub(crate) fn remove_primary_reverse(
        &self,
        related_id: &str,
        fk_value: &PkValue,
        primary_table: &str,
        primary_pk: &PkValue,
    ) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(PRIMARY_REVERSE)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.remove((
                related_id,
                fk_value.as_bytes(),
                primary_table,
                primary_pk.as_bytes(),
            ))
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    pub(crate) fn add_foreign_by_fk(
        &self,
        related_id: &str,
        fk_value: &PkValue,
        foreign_pk: &PkValue,
    ) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(FOREIGN_BY_FK)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.insert((related_id, fk_value.as_bytes(), foreign_pk.as_bytes()), ())
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    pub(crate) fn remove_foreign_by_fk(
        &self,
        related_id: &str,
        fk_value: &PkValue,
        foreign_pk: &PkValue,
    ) -> Result<(), PersistError> {
        let mut pending = self.inner.pending.lock();
        {
            let tx = Self::ensure_pending_tx(&mut pending, &self.inner.db)?;
            let mut t = tx
                .open_table(FOREIGN_BY_FK)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.remove((related_id, fk_value.as_bytes(), foreign_pk.as_bytes()))
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        pending.ops += 1;
        // Commit is deferred to `commit_boundary()` at the logical-event
        // boundary (M1) — committing here, mid-event, could persist a torn
        // state that never existed in memory (e.g. primary row updated but
        // reverse index not yet).
        Ok(())
    }

    // --- bulk dump ---------------------------------------------------

    /// Write every entry from the supplied iterators into one redb
    /// transaction. Used by the bootstrap path to push the entire
    /// in-memory state to disk in a single fsync after snapshot
    /// completes (the per-op write-throughs were skipped during the
    /// snapshot window for throughput).
    ///
    /// Truncates the existing tables first — a dump replaces the
    /// on-disk state, it doesn't merge with it. The caller is
    /// responsible for ensuring the in-memory state is authoritative
    /// at the time of the dump.
    pub fn dump_state<'a, FI, PI, RI, BI>(
        &self,
        foreign_rows: FI,
        primary_rows: PI,
        primary_reverse: RI,
        foreign_by_fk: BI,
    ) -> Result<(), PersistError>
    where
        FI: IntoIterator<Item = (&'a str, &'a PkValue, &'a [u8])>,
        PI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PrimaryRowState)>,
        RI: IntoIterator<Item = (&'a str, &'a PkValue, &'a str, &'a PkValue)>,
        BI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PkValue)>,
    {
        self.dump_state_inner(
            foreign_rows,
            primary_rows,
            primary_reverse,
            foreign_by_fk,
            None,
        )
    }

    /// Replace all persisted state and atomically bind it to a source
    /// checkpoint identity.
    pub fn dump_state_with_identity<'a, FI, PI, RI, BI>(
        &self,
        foreign_rows: FI,
        primary_rows: PI,
        primary_reverse: RI,
        foreign_by_fk: BI,
        source_identity: &str,
    ) -> Result<(), PersistError>
    where
        FI: IntoIterator<Item = (&'a str, &'a PkValue, &'a [u8])>,
        PI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PrimaryRowState)>,
        RI: IntoIterator<Item = (&'a str, &'a PkValue, &'a str, &'a PkValue)>,
        BI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PkValue)>,
    {
        self.dump_state_inner(
            foreign_rows,
            primary_rows,
            primary_reverse,
            foreign_by_fk,
            Some(source_identity),
        )
    }

    fn dump_state_inner<'a, FI, PI, RI, BI>(
        &self,
        foreign_rows: FI,
        primary_rows: PI,
        primary_reverse: RI,
        foreign_by_fk: BI,
        source_identity: Option<&str>,
    ) -> Result<(), PersistError>
    where
        FI: IntoIterator<Item = (&'a str, &'a PkValue, &'a [u8])>,
        PI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PrimaryRowState)>,
        RI: IntoIterator<Item = (&'a str, &'a PkValue, &'a str, &'a PkValue)>,
        BI: IntoIterator<Item = (&'a str, &'a PkValue, &'a PkValue)>,
    {
        // Flush any pending appends first — otherwise our fresh write
        // txn below would conflict with one already open. After flush,
        // we still hold the pending lock so steady-state mutators
        // block until the dump commits.
        self.flush()?;
        let pending = self.inner.pending.lock();

        let tx = self
            .inner
            .db
            .begin_write()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        let mut ops = 0usize;

        // Truncate. redb has no "drop all" — we open each table and
        // call retain(|_,_| false) to clear it.
        {
            let mut t = tx
                .open_table(FOREIGN_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.retain(|_, _| false)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        {
            let mut t = tx
                .open_table(PRIMARY_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.retain(|_, _| false)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        {
            let mut t = tx
                .open_table(PRIMARY_REVERSE)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.retain(|_, _| false)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }
        {
            let mut t = tx
                .open_table(FOREIGN_BY_FK)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            t.retain(|_, _| false)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
        }

        // foreign_rows
        {
            let mut t = tx
                .open_table(FOREIGN_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            for (table, pk, row_bytes) in foreign_rows {
                t.insert((table, pk.as_bytes()), row_bytes)
                    .map_err(|err| PersistError::Transaction(err.to_string()))?;
                ops += 1;
            }
        }
        // primary_rows
        {
            let mut t = tx
                .open_table(PRIMARY_ROWS)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            for (table, pk, state) in primary_rows {
                let raw_json = String::from_utf8_lossy(state.raw.as_bytes()).into_owned();
                let persisted = PersistedPrimaryRow {
                    raw_json,
                    fk_values: state
                        .fk_values
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                        .collect(),
                };
                let bytes = serde_json::to_vec(&persisted)
                    .map_err(|err| PersistError::Encoding(err.to_string()))?;
                t.insert((table, pk.as_bytes()), bytes.as_slice())
                    .map_err(|err| PersistError::Transaction(err.to_string()))?;
                ops += 1;
            }
        }
        // primary_reverse
        {
            let mut t = tx
                .open_table(PRIMARY_REVERSE)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            for (related_id, fk_value, primary_table, primary_pk) in primary_reverse {
                t.insert(
                    (
                        related_id,
                        fk_value.as_bytes(),
                        primary_table,
                        primary_pk.as_bytes(),
                    ),
                    (),
                )
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
                ops += 1;
            }
        }
        // foreign_by_fk
        {
            let mut t = tx
                .open_table(FOREIGN_BY_FK)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            for (related_id, fk_value, foreign_pk) in foreign_by_fk {
                t.insert((related_id, fk_value.as_bytes(), foreign_pk.as_bytes()), ())
                    .map_err(|err| PersistError::Transaction(err.to_string()))?;
                ops += 1;
            }
        }
        {
            let mut metadata = tx
                .open_table(METADATA)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            metadata
                .retain(|_, _| false)
                .map_err(|err| PersistError::Transaction(err.to_string()))?;
            if let Some(source_identity) = source_identity {
                metadata
                    .insert(STATE_IDENTITY_KEY, source_identity)
                    .map_err(|err| PersistError::Transaction(err.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        drop(pending);
        info!(ops_written = ops, "persistent state dumped");
        Ok(())
    }

    // --- load on startup --------------------------------------------------

    /// Read every persisted record and pass it to the supplied
    /// callbacks for replay into the in-memory state. We avoid
    /// taking a direct dependency on [`JoinState`](crate::state::JoinState)
    /// here so the persistence layer stays loosely coupled.
    pub fn load<F1, F2, F3, F4>(
        &self,
        mut on_foreign: F1,
        mut on_primary: F2,
        mut on_primary_reverse: F3,
        mut on_foreign_by_fk: F4,
    ) -> Result<LoadStats, PersistError>
    where
        F1: FnMut(&str, PkValue, Vec<u8>),
        F2: FnMut(&str, PkValue, Vec<u8>, std::collections::HashMap<String, PkValue>),
        F3: FnMut(&str, PkValue, &str, PkValue),
        F4: FnMut(&str, PkValue, PkValue),
    {
        let mut stats = LoadStats::default();
        let tx = self
            .inner
            .db
            .begin_read()
            .map_err(|err| PersistError::Transaction(err.to_string()))?;

        // foreign_rows — load raw JSON bytes; in-memory wraps in
        // CompactRow so the engine never decodes to Value during load.
        let t = tx
            .open_table(FOREIGN_ROWS)
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        for entry in t
            .iter()
            .map_err(|err| PersistError::Transaction(err.to_string()))?
        {
            let (k, v) = entry.map_err(|err| PersistError::Transaction(err.to_string()))?;
            let (table, pk_bytes) = k.value();
            on_foreign(
                table,
                PkValue::from_bytes(pk_bytes.to_vec()),
                v.value().to_vec(),
            );
            stats.foreign_rows += 1;
        }
        drop(t);

        // primary_rows — decode the small envelope (fk_values + raw_json),
        // hand raw_json bytes to the callback.
        let t = tx
            .open_table(PRIMARY_ROWS)
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        for entry in t
            .iter()
            .map_err(|err| PersistError::Transaction(err.to_string()))?
        {
            let (k, v) = entry.map_err(|err| PersistError::Transaction(err.to_string()))?;
            let (table, pk_bytes) = k.value();
            let persisted: PersistedPrimaryRow = serde_json::from_slice(v.value())
                .map_err(|err| PersistError::Encoding(err.to_string()))?;
            let fk_values = persisted
                .fk_values
                .into_iter()
                .map(|(k, v)| (k, PkValue::from_bytes(v)))
                .collect();
            on_primary(
                table,
                PkValue::from_bytes(pk_bytes.to_vec()),
                persisted.raw_json.into_bytes(),
                fk_values,
            );
            stats.primary_rows += 1;
        }
        drop(t);

        // primary_reverse
        let t = tx
            .open_table(PRIMARY_REVERSE)
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        for entry in t
            .iter()
            .map_err(|err| PersistError::Transaction(err.to_string()))?
        {
            let (k, _) = entry.map_err(|err| PersistError::Transaction(err.to_string()))?;
            let (related_id, fk_bytes, primary_table, primary_pk_bytes) = k.value();
            on_primary_reverse(
                related_id,
                PkValue::from_bytes(fk_bytes.to_vec()),
                primary_table,
                PkValue::from_bytes(primary_pk_bytes.to_vec()),
            );
            stats.primary_reverse += 1;
        }
        drop(t);

        // foreign_by_fk
        let t = tx
            .open_table(FOREIGN_BY_FK)
            .map_err(|err| PersistError::Transaction(err.to_string()))?;
        for entry in t
            .iter()
            .map_err(|err| PersistError::Transaction(err.to_string()))?
        {
            let (k, _) = entry.map_err(|err| PersistError::Transaction(err.to_string()))?;
            let (related_id, fk_bytes, foreign_pk_bytes) = k.value();
            on_foreign_by_fk(
                related_id,
                PkValue::from_bytes(fk_bytes.to_vec()),
                PkValue::from_bytes(foreign_pk_bytes.to_vec()),
            );
            stats.foreign_by_fk += 1;
        }
        drop(t);

        info!(
            foreign_rows = stats.foreign_rows,
            primary_rows = stats.primary_rows,
            primary_reverse = stats.primary_reverse,
            foreign_by_fk = stats.foreign_by_fk,
            "persistent state replayed"
        );
        Ok(stats)
    }
}

/// Counts returned by [`PersistentBackend::load`] for observability.
#[derive(Debug, Default, Clone, Copy)]
pub struct LoadStats {
    /// Number of foreign rows replayed.
    pub foreign_rows: usize,
    /// Number of primary rows replayed.
    pub primary_rows: usize,
    /// Number of (related_id, fk, primary) triples replayed.
    pub primary_reverse: usize,
    /// Number of (related_id, fk, foreign_pk) triples replayed.
    pub foreign_by_fk: usize,
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
    use crate::key::PkValue;
    use serde_json::json;

    fn temp_path() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "vs-persist-test-{}-{}-{}.redb",
            std::process::id(),
            nanos,
            N.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Count committed foreign rows via a read transaction (`load` reads the
    /// committed snapshot, NOT the open write tx — so this sees only what's
    /// durably committed).
    fn committed_foreign_rows(b: &PersistentBackend) -> usize {
        b.load(
            |_t, _pk, _row| {},
            |_t, _pk, _row, _fk| {},
            |_id, _fk, _t, _pk| {},
            |_id, _fk, _pk| {},
        )
        .expect("load")
        .foreign_rows
    }

    #[test]
    fn mutators_defer_commit_until_boundary() {
        // M1: a mutator must NOT commit on its own — even with the batch
        // threshold at 1 — so a crash mid-event can't persist a half-applied
        // logical operation. The commit happens only at `commit_boundary`.
        let path = temp_path();
        let backend = PersistentBackend::open_with_batch_size(&path, 1).expect("open");

        backend
            .set_foreign("t", &PkValue::from_single(&json!("a")), b"{}")
            .expect("set_foreign");
        assert_eq!(
            committed_foreign_rows(&backend),
            0,
            "mutator committed mid-event — M1 regression"
        );

        backend.commit_boundary().expect("commit_boundary");
        assert_eq!(
            committed_foreign_rows(&backend),
            1,
            "commit_boundary should commit the batched op"
        );

        drop(backend);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn commit_boundary_is_noop_below_threshold() {
        // With a higher threshold, a single op stays pending across a boundary
        // — the batch keeps accumulating until the threshold is met (or a
        // flush). Confirms boundaries don't force a commit every event.
        let path = temp_path();
        let backend = PersistentBackend::open_with_batch_size(&path, 5).expect("open");
        backend
            .set_foreign("t", &PkValue::from_single(&json!("a")), b"{}")
            .expect("set_foreign");
        backend.commit_boundary().expect("boundary");
        assert_eq!(
            committed_foreign_rows(&backend),
            0,
            "below threshold the boundary must not commit yet"
        );
        // flush() commits unconditionally (shutdown/idle path).
        backend.flush().expect("flush");
        assert_eq!(
            committed_foreign_rows(&backend),
            1,
            "flush commits the batch"
        );
        drop(backend);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn snapshot_dump_commits_source_identity_atomically() {
        let path = temp_path();
        let backend = PersistentBackend::open(&path).expect("open");
        assert_eq!(backend.state_identity().expect("read identity"), None);

        backend
            .dump_state_with_identity(
                std::iter::empty::<(&str, &PkValue, &[u8])>(),
                std::iter::empty::<(&str, &PkValue, &PrimaryRowState)>(),
                std::iter::empty::<(&str, &PkValue, &str, &PkValue)>(),
                std::iter::empty::<(&str, &PkValue, &PkValue)>(),
                "postgres:db:slot",
            )
            .expect("dump");
        assert_eq!(
            backend.state_identity().expect("read identity"),
            Some("postgres:db:slot".to_owned())
        );

        drop(backend);
        std::fs::remove_file(&path).ok();
    }
}
