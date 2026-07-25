//! [`JoinEngine`] — the state machine that turns per-table CDC events
//! into composed, denormalized documents.
//!
//! ### Routing
//!
//! Each incoming event is matched against every configured
//! [`JoinDefinition`]:
//!
//! 1. If it targets a `primary` table, the engine composes its
//!    enriched form and emits **one** event downstream.
//! 2. If it targets a `related` table, the engine updates state and
//!    re-emits **every affected primary** with the new related data.
//! 3. If it targets neither, it passes through unchanged.
//!
//! ### Event-payload layout assumed
//!
//! The engine expects payloads produced by `ventstream-sources` /
//! `event_mapper`:
//!
//! - `INSERT` → payload is the row object directly (e.g.
//!   `{"id": 1, "email": "..."}`)
//! - `UPDATE` → `{"new": {...}, "old": {...} | null}`
//! - `DELETE` → `{"old": {...}}`
//! - `TRUNCATE` → ignored by the engine (passes through)
//!
//! It uses the `ventstream.cdc.namespace` and
//! `ventstream.cdc.relation` headers (set by every CDC source) to
//! identify the source table. Falling back to subject parsing if
//! headers are absent.
//!
//! ### Composed-event shape
//!
//! The original subject is preserved (downstream routing keeps
//! working). The payload is replaced with the composed object. The
//! header `ventstream.join.name` is added so observers can see which
//! join produced the event.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{Map, Value};
use tracing::{debug, info, warn};
use ventstream_core::{
    ContentType, Event, EventReceiver, EventSender, Headers, Payload, ShutdownToken, SourceUri,
    Subject,
};

use crate::config::{BackfillMode, Cardinality, JoinDefinition, OnMissing, RelatedDefinition};
use crate::error::JoinError;
use crate::fetcher::{FetchError, FetchOutcome, RelatedFetcher};
use crate::key::{extract_pk, PkValue};
use crate::state::{JoinState, PrimaryRowState, RelatedPrimaryCursor, RowKey};

/// Default cadence for the engine drain loop's idle-flush tick.
/// Bounded durability gap during quiet periods; configurable per
/// instance via [`JoinEngine::with_idle_flush_interval`].
const DEFAULT_IDLE_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Maximum number of primary documents materialized during a related-side
/// fan-out. Each chunk still uses set-based related fetches, then is emitted
/// before the next chunk is loaded from state.
const RE_EMIT_PRIMARY_CHUNK_SIZE: usize = 256;

const ACK_BARRIER_HEADER: &str = "ventstream.internal.ack_barrier";
const ACK_SEQUENCE_HEADER: &str = "ventstream.cdc.ack_seq";
const LSN_HEADER: &str = "ventstream.cdc.lsn";
const TX_ID_HEADER: &str = "ventstream.cdc.tx_id";
const JOIN_BARRIER_HEADER: &str = "ventstream.internal.join_barrier";
const JOIN_SEQUENCE_HEADER: &str = "ventstream.internal.join_seq";

/// Two-stage durability handoff for stateful joins.
///
/// The dispatcher advances `sink_progress` only after the ordered sink prefix
/// is durable. The join runtime then commits its matching state transaction
/// and advances `source_progress`, which is the only watermark visible to the
/// CDC source. This prevents source checkpoints from outrunning join state.
#[derive(Clone)]
pub struct JoinDurability {
    sink_progress: Arc<AtomicU64>,
    source_progress: Arc<AtomicU64>,
}

impl JoinDurability {
    /// Build a durability handoff from dispatcher-owned and source-visible
    /// progress atomics.
    #[must_use]
    pub fn new(sink_progress: Arc<AtomicU64>, source_progress: Arc<AtomicU64>) -> Self {
        Self {
            sink_progress,
            source_progress,
        }
    }
}

/// Owns the state and processes events.
pub struct JoinEngine {
    joins: Vec<JoinDefinition>,
    state: Mutex<JoinState>,
    fetcher: Arc<dyn RelatedFetcher>,
    idle_flush_interval: std::time::Duration,
}

impl JoinEngine {
    /// Construct an engine with an empty in-memory state.
    pub fn new(joins: Vec<JoinDefinition>, fetcher: Arc<dyn RelatedFetcher>) -> Self {
        Self {
            joins,
            state: Mutex::new(JoinState::new()),
            fetcher,
            idle_flush_interval: DEFAULT_IDLE_FLUSH_INTERVAL,
        }
    }

    /// Construct an engine with a pre-built state — typically one
    /// that's been loaded from a [`PersistentBackend`](crate::PersistentBackend).
    /// Callers wire this up in the binary so the engine code stays
    /// agnostic to persistence config.
    pub fn with_state(
        joins: Vec<JoinDefinition>,
        fetcher: Arc<dyn RelatedFetcher>,
        state: JoinState,
    ) -> Self {
        Self {
            joins,
            state: Mutex::new(state),
            fetcher,
            idle_flush_interval: DEFAULT_IDLE_FLUSH_INTERVAL,
        }
    }

    /// Override the idle-flush cadence. Default 1s. Set higher for
    /// burstier workloads (less fsync pressure); set lower to bound
    /// the unflushed-state window during quiet periods.
    #[must_use]
    pub fn with_idle_flush_interval(mut self, interval: std::time::Duration) -> Self {
        self.idle_flush_interval = interval;
        self
    }

    /// Whether any joins are configured. If not, callers should bypass
    /// the engine entirely rather than insert a no-op in the pipeline.
    pub fn has_joins(&self) -> bool {
        !self.joins.is_empty()
    }

    /// Drain `input` and emit composed events until shutdown or input closure.
    ///
    /// This compatibility entry point runs without the source/sink durability
    /// handoff. Runtime integrations that checkpoint a CDC source should call
    /// [`Self::run_with_durability`] instead.
    pub async fn run(
        self: Arc<Self>,
        input: EventReceiver,
        output: EventSender,
        shutdown: ShutdownToken,
    ) {
        let run_shutdown = shutdown.clone();
        if let Err(err) = self
            .run_with_durability(input, output, run_shutdown, None)
            .await
        {
            warn!(error = %err, "join engine stopped after a processing failure");
            shutdown.cancel();
        }
    }

    /// Drain `input`, optionally coordinate durable CDC progress, and return
    /// any processing or persistence failure to the runtime.
    pub async fn run_with_durability(
        self: Arc<Self>,
        mut input: EventReceiver,
        output: EventSender,
        shutdown: ShutdownToken,
        durability: Option<JoinDurability>,
    ) -> Result<(), JoinError> {
        info!(join_count = self.joins.len(), "join engine starting");
        // Idle-flush tick: commit any pending persistence batch even
        // if the threshold hasn't been hit. Bounds the durability gap
        // during quiet periods (otherwise a partially-filled batch
        // would sit in memory until the next event or shutdown).
        let mut idle_flush = tokio::time::interval(self.idle_flush_interval);
        idle_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately — discard so we don't spuriously
        // flush an empty batch right at engine entry.
        let _ = idle_flush.tick().await;
        let mut pending_work = false;
        let mut pending_source_watermark = 0u64;
        let mut next_join_sequence = 1u64;
        let mut snapshot_dump_pending = false;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    info!("join engine shutdown requested");
                    // Never flush unconfirmed state during shutdown. The source
                    // watermark remains behind and the work replays.
                    return Ok(());
                }
                _ = idle_flush.tick(), if pending_work => {
                    if let Some(gate) = durability.as_ref() {
                        if !self
                            .commit_durable_boundary(
                                &output,
                                &shutdown,
                                gate,
                                &mut next_join_sequence,
                                pending_source_watermark,
                                &mut snapshot_dump_pending,
                            )
                            .await?
                        {
                            return Ok(());
                        }
                    } else {
                        self.flush_state(snapshot_dump_pending)?;
                        snapshot_dump_pending = false;
                    }
                    pending_work = false;
                    pending_source_watermark = 0;
                }
                event = input.recv() => match event {
                    Some(event) => {
                        let source_watermark = source_watermark(&event);
                        let source_barrier =
                            event.headers.get(ACK_BARRIER_HEADER) == Some("true");
                        if event.headers.get("ventstream.cdc.bootstrap")
                            == Some("snapshot-complete")
                        {
                            snapshot_dump_pending = true;
                        }
                        match self.handle_inner(&event, &output, &shutdown).await {
                            Ok(true) => {
                            }
                            Ok(false) => {
                                let event_id = event.id;
                                if let Err(err) = send(&output, event, &shutdown).await {
                                    warn!(
                                        event_id = %event_id,
                                        error = %err,
                                        "join engine failed to emit pass-through event; cancelling runtime"
                                    );
                                    shutdown.cancel();
                                    return Err(err);
                                }
                            }
                            Err(err) => {
                                warn!(
                                    event_id = %event.id,
                                    subject = %event.subject,
                                    error = %err,
                                    "join engine failed to process event; cancelling runtime"
                                );
                                shutdown.cancel();
                                return Err(err);
                            }
                        }
                        pending_work = true;
                        pending_source_watermark =
                            pending_source_watermark.max(source_watermark);

                        if source_barrier {
                            if let Some(gate) = durability.as_ref() {
                                if !self
                                    .commit_durable_boundary(
                                        &output,
                                        &shutdown,
                                        gate,
                                        &mut next_join_sequence,
                                        pending_source_watermark,
                                        &mut snapshot_dump_pending,
                                    )
                                    .await?
                                {
                                    return Ok(());
                                }
                            } else {
                                self.flush_state(snapshot_dump_pending)?;
                                snapshot_dump_pending = false;
                            }
                            pending_work = false;
                            pending_source_watermark = 0;
                        } else if durability.is_none() {
                            self.state
                                .lock()
                                .commit_boundary()
                                .map_err(|err| JoinError::Internal(format!(
                                    "persistence commit boundary: {err}"
                                )))?;
                        }
                    }
                    None => {
                        info!("join engine input closed");
                        if pending_work {
                            if let Some(gate) = durability.as_ref() {
                                if !self
                                    .commit_durable_boundary(
                                        &output,
                                        &shutdown,
                                        gate,
                                        &mut next_join_sequence,
                                        pending_source_watermark,
                                        &mut snapshot_dump_pending,
                                    )
                                    .await?
                                {
                                    return Ok(());
                                }
                            } else {
                                self.flush_state(snapshot_dump_pending)?;
                            }
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_durable_boundary(
        &self,
        output: &EventSender,
        shutdown: &ShutdownToken,
        durability: &JoinDurability,
        next_sequence: &mut u64,
        source_watermark: u64,
        snapshot_dump_pending: &mut bool,
    ) -> Result<bool, JoinError> {
        let sequence = *next_sequence;
        let Some(following_sequence) = next_sequence.checked_add(1) else {
            shutdown.cancel();
            return Err(JoinError::Internal(
                "join durability sequence exhausted".to_owned(),
            ));
        };
        *next_sequence = following_sequence;
        let barrier = join_barrier(sequence)?;
        if let Err(err) = send(output, barrier, shutdown).await {
            if shutdown.is_cancelled() {
                return Ok(false);
            }
            shutdown.cancel();
            return Err(err);
        }

        let mut poll = tokio::time::interval(std::time::Duration::from_millis(5));
        loop {
            if durability.sink_progress.load(Ordering::Acquire) >= sequence {
                break;
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(false),
                _ = poll.tick() => {}
            }
        }

        if let Err(err) = self.flush_state(*snapshot_dump_pending) {
            shutdown.cancel();
            return Err(err);
        }
        *snapshot_dump_pending = false;
        if source_watermark > 0 {
            durability
                .source_progress
                .fetch_max(source_watermark, Ordering::Release);
        }
        Ok(true)
    }

    fn flush_state(&self, snapshot_dump_pending: bool) -> Result<(), JoinError> {
        let mut state = self.state.lock();
        if snapshot_dump_pending {
            state
                .dump_to_persistent()
                .map_err(|err| JoinError::Internal(format!("snapshot state dump: {err}")))?;
            state.set_persist_enabled(true);
        } else {
            state
                .flush_persistent()
                .map_err(|err| JoinError::Internal(format!("persistence flush: {err}")))?;
        }
        Ok(())
    }

    /// Single-event handler. Public for test harnesses that want to
    /// drive the engine synchronously.
    pub async fn handle(
        &self,
        event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<(), JoinError> {
        if self.handle_inner(event, output, shutdown).await? {
            Ok(())
        } else {
            // The public test/embedding API borrows its input, so it retains
            // clone semantics. The owned runtime path above moves unmatched
            // events directly to the output bus.
            send(output, event.clone(), shutdown).await
        }
    }

    /// Handle a configured join event or bootstrap sentinel. Returns `false`
    /// only when the event is unrelated and should pass through unchanged.
    async fn handle_inner(
        &self,
        event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<bool, JoinError> {
        // Internal sink-ack barriers are control-plane events. They may not
        // carry a CDC-shaped subject or payload and must reach the dispatcher
        // byte-for-byte unchanged.
        if event.headers.get(ACK_BARRIER_HEADER) == Some("true") {
            return Ok(false);
        }

        // Bootstrap-phase transitions, driven by the
        // `ventstream.cdc.bootstrap` header set by the snapshot
        // source. `snapshot` events suppress per-row persistence;
        // the `snapshot-complete` sentinel triggers a single dump
        // and re-enables per-row writes. Non-bootstrap events
        // bypass this block entirely.
        match event.headers.get("ventstream.cdc.bootstrap") {
            Some("snapshot") => {
                let mut state = self.state.lock();
                if state.persist_enabled() {
                    state.set_persist_enabled(false);
                    info!(
                        "snapshot mode begun — per-row persistence \
                         suspended; will dump at end of bootstrap"
                    );
                }
            }
            Some("snapshot-complete") => {
                // The runtime dumps and re-enables persistence only after its
                // sink-durability barrier is confirmed. The sentinel is
                // consumed here and never reaches the customer sink.
                info!("snapshot mode ended — awaiting durable state boundary");
                return Ok(true);
            }
            _ => {}
        }

        let table = source_table(event)?;
        let op = subject_op(event)?;
        let primary_truncate =
            matches!(op, Op::Truncate) && self.joins.iter().any(|def| def.primary.table == table);
        if primary_truncate {
            self.handle_primary_truncate(&table, event, output, shutdown)
                .await?;
        }

        // Route by table membership across ALL join definitions. A table can
        // be a join's primary AND/OR a related table in one or more joins —
        // e.g. the same `customers` table embedded as both "billing" and
        // "shipping" customer (two `related` entries). Handle EVERY match, not
        // just the first: short-circuiting let the other relations silently go
        // stale (H1). (If one foreign row is *both* relations for the same
        // primary — e.g. billing_id == shipping_id — that primary re-emits
        // twice; harmless, the sink upsert is idempotent.)
        let mut matched = false;
        for def in &self.joins {
            if def.primary.table == table {
                matched = true;
                if !primary_truncate {
                    self.handle_primary(def, &table, op, event, output, shutdown)
                        .await?;
                }
            }
            for rel in def.related.iter().filter(|r| r.table == table) {
                matched = true;
                self.handle_related(def, rel, op, event, output, shutdown)
                    .await?;
            }
        }
        Ok(matched)
    }

    // ---- primary path ----------------------------------------------------

    async fn handle_primary_truncate(
        &self,
        table: &str,
        event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<(), JoinError> {
        let mut removed = 0usize;
        loop {
            let primary_ids = self
                .state
                .lock()
                .take_primary_table_page(table, RE_EMIT_PRIMARY_CHUNK_SIZE);
            if primary_ids.is_empty() {
                break;
            }
            removed += primary_ids.len();
            for def in self.joins.iter().filter(|def| def.primary.table == table) {
                for primary_pk in &primary_ids {
                    let emit = build_tombstone(event, def, table, primary_pk);
                    send(output, emit, shutdown).await?;
                }
            }
        }
        debug!(
            table,
            removed, "primary table truncated — state purged, tombstones emitted"
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_primary(
        &self,
        def: &JoinDefinition,
        table: &str,
        op: Op,
        event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<(), JoinError> {
        // Primary truncates are handled once in `handle_inner`, before routing
        // across definitions that may share this table.
        if matches!(op, Op::Truncate) {
            return Ok(());
        }

        let payload = parse_payload(event)?;
        let row_opt = current_row(&payload, op);
        // PK from the row when present (insert/update, or a PG delete carrying
        // the old tuple), else from the doc-id header (full-image `{}` delete).
        let pk = match row_opt.and_then(|row| extract_pk(row, def.primary.pk.columns())) {
            Some(pk) => pk,
            None => pk_from_doc_id(event, def.primary.pk.columns().len()).ok_or_else(|| {
                JoinError::InvalidPayload {
                    table: table.to_owned(),
                    detail: format!(
                        "event subject '{}' has no row and no usable doc id",
                        event.subject
                    ),
                }
            })?,
        };

        match op {
            Op::Insert | Op::Update => {
                let row = row_opt.ok_or_else(|| JoinError::InvalidPayload {
                    table: table.to_owned(),
                    detail: format!("event subject '{}' missing op row", event.subject),
                })?;
                // pgoutput omits unchanged TOAST columns from an UPDATE's
                // `new` row (the mapper drops their keys — see
                // event_mapper::column_to_json). The engine replaces the
                // stored primary row wholesale and the sink full-replaces the
                // doc, so a blindly-stored partial row would *drop* those
                // columns from state and from every future recomposition — a
                // worse silent loss than the original null-clobber. Merge
                // `new` over the prior stored row so unchanged columns survive
                // (H4). On INSERT, or the first time we see this pk, there is
                // no prior row and `new` is complete, so this is a no-op.
                let merged_holder: Option<Value> = if matches!(op, Op::Update) {
                    let prior = self
                        .state
                        .lock()
                        .get_primary(table, &pk)
                        .map(|p| p.raw.as_value());
                    match prior {
                        Some(mut base) => match (base.as_object_mut(), row.as_object()) {
                            (Some(base_obj), Some(new_obj)) => {
                                for (k, v) in new_obj {
                                    base_obj.insert(k.clone(), v.clone());
                                }
                                Some(base)
                            }
                            // Prior or new wasn't an object — nothing to merge.
                            _ => None,
                        },
                        // No prior state for this pk — `new` stands alone.
                        None => None,
                    }
                } else {
                    None
                };
                let row: &Value = merged_holder.as_ref().unwrap_or(row);

                // Update reverse index BEFORE compose so newly seen FK
                // values are tracked even if the related row isn't in
                // state yet.
                let mut fk_values: HashMap<String, PkValue> = HashMap::new();
                for rel in &def.related {
                    let fk_value =
                        extract_pk(row, rel.join_on.from.columns()).unwrap_or_else(|| {
                            // The FK columns weren't present on the primary
                            // row. Treat as the null key — composing will
                            // emit `null`/`[]` per `on_missing`.
                            PkValue::from_values(&[])
                        });
                    fk_values.insert(rel.id.clone(), fk_value);
                }
                self.update_primary_indexes(def, table, &pk, &fk_values);

                // Stash primary state. We compact-encode the raw row
                // here so the in-memory rep is bytes — saves ~3-5x
                // the Value's overhead for the same row.
                self.state.lock().set_primary(
                    table,
                    &pk,
                    PrimaryRowState {
                        raw: crate::state::CompactRow::from_value(row),
                        fk_values: fk_values.clone(),
                    },
                );

                // Compose and emit.
                let composed = self.compose_primary(def, row, &fk_values).await?;
                let emit = build_composed(event, def, table, &pk, &composed);
                send(output, emit, shutdown).await?;
            }
            Op::Delete => {
                // Cleanup reverse indexes using stored fk_values.
                let prior = self.state.lock().take_primary(table, &pk);
                if let Some(prior_state) = prior {
                    let mut state = self.state.lock();
                    for (rel_id, fk_val) in &prior_state.fk_values {
                        state.remove_primary_reverse(rel_id, fk_val, table, &pk);
                    }
                }
                // Pass the delete event through. The sink uses
                // `ventstream.doc.id` to know which doc to tombstone,
                // so we stamp it on the way out — without it, the
                // sink would fall back to event.id (a fresh ULID per
                // emit) and end up deleting nothing.
                let emit = build_tombstone(event, def, table, &pk);
                send(output, emit, shutdown).await?;
            }
            // Handled above (early return) — TRUNCATE has no row to reach here.
            Op::Truncate => {}
        }
        Ok(())
    }

    /// Add reverse-index entries for a primary row. If the primary
    /// already existed, callers are expected to have cleared its old
    /// FK references first (see [`Self::clear_primary_reverse`]).
    fn update_primary_indexes(
        &self,
        def: &JoinDefinition,
        primary_table: &str,
        primary_pk: &PkValue,
        fk_values: &HashMap<String, PkValue>,
    ) {
        // If the primary already existed with different FK values, we
        // need to undo the old reverse links. Simpler than diffing: do
        // the cleanup unconditionally based on the previously stored
        // state.
        self.clear_primary_reverse(def, primary_table, primary_pk);
        let mut state = self.state.lock();
        for rel in &def.related {
            if let Some(fk) = fk_values.get(&rel.id) {
                if !fk.is_null() {
                    state.add_primary_reverse(&rel.id, fk, primary_table, primary_pk);
                }
            }
        }
    }

    fn clear_primary_reverse(
        &self,
        def: &JoinDefinition,
        primary_table: &str,
        primary_pk: &PkValue,
    ) {
        let prior = {
            let state = self.state.lock();
            state
                .get_primary(primary_table, primary_pk)
                .map(|s| s.fk_values.clone())
        };
        let Some(prior_fk) = prior else { return };
        let mut state = self.state.lock();
        for (rel_id, fk_val) in prior_fk {
            let _ = def; // def used only for clarity in the call site
            state.remove_primary_reverse(&rel_id, &fk_val, primary_table, primary_pk);
        }
    }

    // ---- related path ----------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn handle_related(
        &self,
        def: &JoinDefinition,
        rel: &RelatedDefinition,
        op: Op,
        event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<(), JoinError> {
        // TRUNCATE carries no row, so handle it BEFORE row extraction (M8) —
        // `current_row` returns `None` for it, which would otherwise error
        // ("missing op row") and the handling below would never run. Purge the
        // truncated related table's foreign state for THIS relation and
        // recompose every primary that embedded a child from it, so the
        // embedded array drops the now-gone children. handle() routes a related
        // truncate here once per matching (def, rel).
        if matches!(op, Op::Truncate) {
            self.state.lock().purge_related_table(&rel.id, &rel.table);
            let count = self
                .re_emit_primaries_for_relation(def, &rel.id, event, output, shutdown)
                .await?;
            debug!(
                table = %rel.table,
                related = %rel.id,
                affected = count,
                "related table truncated — state purged, primaries recomposed"
            );
            return Ok(());
        }

        let payload = parse_payload(event)?;
        let row_opt = current_row(&payload, op);

        // Foreign PK from the row when present, else from the doc-id header
        // (full-image sources emit `{}` on delete and carry the PK only there).
        let foreign_pk = match row_opt.and_then(|row| extract_pk(row, rel.pk.columns())) {
            Some(pk) => pk,
            None => pk_from_doc_id(event, rel.pk.columns().len()).ok_or_else(|| {
                JoinError::InvalidPayload {
                    table: rel.table.clone(),
                    detail: format!(
                        "event subject '{}' has no row and no usable doc id",
                        event.subject
                    ),
                }
            })?,
        };
        // The value of the FK column(s) on the foreign side — drives both
        // `foreign_by_fk` (compose lookup) and `primary_reverse` (re-emission
        // target). Absent on a `{}` delete; the Delete arm recovers it from the
        // stored prior row, so a null key here is fine (overridden by `effective`).
        let lookup_value = row_opt
            .and_then(|row| extract_pk(row, rel.join_on.to.columns()))
            .unwrap_or_else(|| PkValue::from_values(&[]));

        // Resolve the FK we use to find affected primaries below.
        // Insert/update read it straight from the event row. A DELETE's
        // old tuple, however, only carries the columns in the table's
        // REPLICA IDENTITY — under Postgres' default that's just the PK,
        // so `lookup_value` arrived as a null key (extract_pk substitutes
        // null for absent columns). Recover the real FK from the row we
        // stored when the insert/update was first seen (it has every
        // column), falling back to the event value — which is correct
        // under REPLICA IDENTITY FULL, or when the FK is part of the PK.
        // If an UPDATE re-keys the foreign row (its `join_on.to` value
        // changes), this holds the OLD value so we also re-emit the primaries
        // that used to embed it (they must drop the now-unmatched child). H2.
        let mut reparented_from: Option<PkValue> = None;
        let lookup_value = match op {
            Op::Insert | Op::Update => {
                let row = row_opt.ok_or_else(|| JoinError::InvalidPayload {
                    table: rel.table.clone(),
                    detail: format!("event subject '{}' missing op row", event.subject),
                })?;
                let mut state = self.state.lock();
                // Prior stored foreign row (if any) — drives both the H4
                // value-merge and the H2 re-key cleanup below.
                let prior = if matches!(op, Op::Update) {
                    state.get_foreign(&rel.table, &foreign_pk)
                } else {
                    None
                };
                // H4 (foreign side): an UPDATE may omit unchanged TOAST
                // columns from `new`. set_foreign replaces the stored foreign
                // row wholesale, so every primary that embeds this row would
                // recompose without those columns. Merge `new` over the prior
                // row so unchanged columns survive. INSERT / first-sight = no
                // prior row = no-op.
                let merged: Option<Value> = match prior.as_ref() {
                    Some(p) => match (p.as_object(), row.as_object()) {
                        (Some(prior_obj), Some(new_obj)) => {
                            let mut m = prior_obj.clone();
                            for (k, v) in new_obj {
                                m.insert(k.clone(), v.clone());
                            }
                            Some(Value::Object(m))
                        }
                        _ => None,
                    },
                    None => None,
                };
                state.set_foreign(&rel.table, &foreign_pk, merged.as_ref().unwrap_or(row));
                // Derive the join key from the MERGED row, not the raw event:
                // if the join column ever arrived as an unchanged-TOAST
                // omission it would otherwise degrade to a null key (and the
                // re-key path below would misfire). Join columns are small and
                // never TOASTed in practice, but this keeps it consistent with
                // the H4 merge. Falls back to the event-derived value.
                let lookup_value =
                    extract_pk(merged.as_ref().unwrap_or(row), rel.join_on.to.columns())
                        .unwrap_or(lookup_value);
                // Maintain the FK secondary index for ALL cardinalities.
                // For `cardinality: one` where the FK targets a non-PK
                // unique column (e.g. show_buyer.show_id), the secondary
                // index is the only way compose can find the row.
                state.add_foreign_by_fk(&rel.id, &lookup_value, &foreign_pk);

                // H2: if this UPDATE changed the join column (`join_on.to`),
                // the foreign row was re-keyed. Drop the stale OLD-value index
                // entry — otherwise `foreign_by_fk` leaks the row forever and
                // every primary that joined on the old value keeps embedding
                // this now-unmatched child. Remember the old value so its
                // primaries get re-emitted below.
                if matches!(op, Op::Update) {
                    let old_lookup = prior
                        .as_ref()
                        .and_then(|p| extract_pk(p, rel.join_on.to.columns()))
                        .filter(|old| *old != lookup_value);
                    if let Some(old) = old_lookup {
                        state.remove_foreign_by_fk(&rel.id, &old, &foreign_pk);
                        reparented_from = Some(old);
                    }
                }
                drop(state);
                lookup_value
            }
            Op::Delete => {
                let mut state = self.state.lock();
                let prior = state.delete_foreign(&rel.table, &foreign_pk);
                let effective = prior
                    .as_ref()
                    .and_then(|r| extract_pk(r, rel.join_on.to.columns()))
                    .unwrap_or(lookup_value);
                // Use the recovered FK for the secondary-index removal too,
                // otherwise the (null-keyed) bucket entry would be leaked.
                state.remove_foreign_by_fk(&rel.id, &effective, &foreign_pk);
                drop(state);
                effective
            }
            // Handled above (early return) — TRUNCATE has no row to reach here.
            Op::Truncate => return Ok(()),
        };

        // Find every primary that references this foreign row and re-emit.
        // Primaries embedding this foreign row under the (new/effective)
        // lookup value, PLUS — if an UPDATE re-keyed it — those that embedded
        // it under the OLD value, so they recompose and drop the child (H2).
        // Merge and deduplicate the two reverse-index buckets page by page,
        // since a stale/overlapping link can otherwise emit a primary twice.
        let affected = self
            .re_emit_primaries_for_keys(
                def,
                &rel.id,
                &lookup_value,
                reparented_from.as_ref(),
                event,
                output,
                shutdown,
            )
            .await?;
        debug!(
            related = %rel.id,
            foreign_pk = %foreign_pk,
            affected,
            "related update — re-emitting affected primaries"
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn re_emit_primaries_for_keys(
        &self,
        def: &JoinDefinition,
        related_id: &str,
        fk_value: &PkValue,
        secondary_fk_value: Option<&PkValue>,
        triggering_event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<usize, JoinError> {
        let mut after: Option<RowKey> = None;
        let mut affected = 0;
        loop {
            let chunk = self.state.lock().primaries_for_keys_chunk(
                related_id,
                fk_value,
                secondary_fk_value,
                after.as_ref(),
                RE_EMIT_PRIMARY_CHUNK_SIZE,
            );
            let Some(last) = chunk.last().cloned() else {
                break;
            };
            affected += chunk.len();
            self.re_emit_primary_chunk(def, &chunk, triggering_event, output, shutdown)
                .await?;
            after = Some(last);
        }
        Ok(affected)
    }

    #[allow(clippy::too_many_arguments)]
    async fn re_emit_primaries_for_relation(
        &self,
        def: &JoinDefinition,
        related_id: &str,
        triggering_event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<usize, JoinError> {
        let mut after: Option<RelatedPrimaryCursor> = None;
        let mut affected = 0;
        loop {
            let (chunk, next) = self.state.lock().primaries_for_relation_chunk(
                related_id,
                after.as_ref(),
                RE_EMIT_PRIMARY_CHUNK_SIZE,
            );
            if chunk.is_empty() {
                break;
            }
            affected += chunk.len();
            self.re_emit_primary_chunk(def, &chunk, triggering_event, output, shutdown)
                .await?;
            after = next;
        }
        Ok(affected)
    }

    /// Recompose one bounded primary-ID chunk. Related cache misses remain
    /// set-based within the chunk, and all materialized documents are released
    /// before the engine asks state for another page.
    #[allow(clippy::too_many_arguments)]
    async fn re_emit_primary_chunk(
        &self,
        def: &JoinDefinition,
        affected: &[RowKey],
        triggering_event: &Event,
        output: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<(), JoinError> {
        debug_assert!(affected.len() <= RE_EMIT_PRIMARY_CHUNK_SIZE);
        let mut primaries = {
            let state = self.state.lock();
            affected
                .iter()
                .filter_map(|(table, pk)| {
                    state.get_primary(table, pk).map(|stored| {
                        (
                            table.clone(),
                            pk.clone(),
                            stored.raw.as_value(),
                            stored.fk_values.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };
        if primaries.is_empty() {
            return Ok(());
        }

        for rel in &def.related {
            let keys = primaries
                .iter()
                .filter_map(|(_, _, _, fk_values)| fk_values.get(&rel.id).cloned())
                .collect::<Vec<_>>();
            let outcomes = self.lookup_related_batch(def, rel, &keys).await?;
            for (table, _, document, fk_values) in &mut primaries {
                let Some(key) = fk_values.get(&rel.id) else {
                    continue;
                };
                let outcome = outcomes
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| FetchOutcome::empty_for(rel.cardinality));
                let embedded = apply_select_and_shape(rel, outcome);
                let object = document
                    .as_object_mut()
                    .ok_or_else(|| JoinError::InvalidPayload {
                        table: table.clone(),
                        detail: "primary row must be a JSON object to embed related data".into(),
                    })?;
                object.insert(rel.embed_as.clone(), embedded);
            }
        }

        for (table, pk, document, _) in primaries {
            let emit = build_re_emit(triggering_event, def, &table, &pk, &document);
            send(output, emit, shutdown).await?;
        }
        Ok(())
    }

    // ---- composition -----------------------------------------------------

    async fn compose_primary(
        &self,
        def: &JoinDefinition,
        primary_row: &Value,
        fk_values: &HashMap<String, PkValue>,
    ) -> Result<Value, JoinError> {
        let mut composed = primary_row.clone();
        let composed_obj = composed
            .as_object_mut()
            .ok_or_else(|| JoinError::InvalidPayload {
                table: def.primary.table.clone(),
                detail: "primary row must be a JSON object to embed related data".into(),
            })?;

        for rel in &def.related {
            let Some(fk_val) = fk_values.get(&rel.id) else {
                continue;
            };
            let outcome = self.lookup_related(def, rel, fk_val).await?;
            let embedded = apply_select_and_shape(rel, outcome);
            composed_obj.insert(rel.embed_as.clone(), embedded);
        }

        Ok(Value::Object(std::mem::take(composed_obj)))
    }

    async fn lookup_related(
        &self,
        def: &JoinDefinition,
        rel: &RelatedDefinition,
        fk_value: &PkValue,
    ) -> Result<FetchOutcome, JoinError> {
        if fk_value.is_null() {
            return Ok(FetchOutcome::empty_for(rel.cardinality));
        }

        // For a `Many` relation with backfill enabled the cache may be
        // INCOMPLETE — children can arrive via CDC before their parent, so a
        // non-empty `foreign_by_fk` bucket isn't proof we have them all. The
        // source is the authoritative full set, so always reconcile against it
        // rather than trusting a partial cache (M7). `One` relations (a single
        // row, authoritative once cached) and backfill-disabled `Many` (no
        // source to reconcile against) still trust the cache.
        let always_reconcile = should_reconcile_from_source(rel.cardinality, def.backfill.mode);

        // Try state first. Both cardinalities route through `foreign_by_fk` —
        // that index is keyed by the *FK value* (the `join_on.to` column on the
        // foreign side), so it works identically for "FK is the related PK" and
        // "FK is a non-PK unique column" alike.
        if !always_reconcile {
            let from_state = {
                let state = self.state.lock();
                let pks = state.foreign_pks_for(&rel.id, fk_value);
                if pks.is_empty() {
                    None
                } else {
                    // `get_foreign` now decodes from CompactRow bytes on
                    // each call. We collect to Vec so the lock can drop
                    // immediately after.
                    let rows: Vec<Value> = pks
                        .iter()
                        .filter_map(|pk| state.get_foreign(&rel.table, pk))
                        .collect();
                    Some(match rel.cardinality {
                        Cardinality::One => FetchOutcome::One(rows.into_iter().next()),
                        Cardinality::Many => FetchOutcome::Many(rows),
                    })
                }
            };
            if let Some(outcome) = from_state {
                return Ok(outcome);
            }
        }

        // State miss (or a forced Many reconcile) — consult the fetcher per
        // backfill mode.
        if def.backfill.mode == BackfillMode::None {
            return Ok(FetchOutcome::empty_for(rel.cardinality));
        }

        // Both cardinalities query the fetcher by `join_on.to`. Many is
        // a `fetch_many` (multiple rows); One uses `fetch_many` too and
        // takes the first (or none).
        //
        // Fetch the FK column(s) (`join_on.to`) ALONGSIDE the user's
        // `select`, even when `select` omits them. The rows returned here
        // are persisted as the foreign-row state (below). A later child
        // DELETE under Postgres' default replica identity carries only the
        // related PK in its old tuple, so the engine recovers the parent FK
        // from that stored foreign row — which must therefore contain it.
        // Without the FK column the recovery yields a null key and the
        // delete silently fails to recompose the parent (H17). The extra
        // column never reaches the embedded doc: `apply_select_and_shape`
        // projects strictly to `rel.select`.
        let fetch_select = select_with_fk(&rel.select, rel.join_on.to.columns());
        let rows = self
            .fetcher
            .fetch_many(
                &rel.table,
                rel.join_on.to.columns(),
                fk_value,
                &fetch_select,
            )
            .await
            .map_err(|source| JoinError::Fetcher {
                related_id: rel.id.clone(),
                source,
            })?;
        {
            let mut state = self.state.lock();
            for row in &rows {
                if let Some(pk) = extract_pk(row, rel.pk.columns()) {
                    state.set_foreign(&rel.table, &pk, row);
                    state.add_foreign_by_fk(&rel.id, fk_value, &pk);
                }
            }
        }
        let outcome = match rel.cardinality {
            Cardinality::One => FetchOutcome::One(rows.into_iter().next()),
            Cardinality::Many => FetchOutcome::Many(rows),
        };

        // Surface fetcher use to debug log for visibility.
        debug!(
            join = %def.effective_name(),
            related = %rel.id,
            fk_value = %fk_value,
            "backfill via fetcher"
        );

        // Confirm we don't shadow the `_` binding above unused.
        Ok(outcome)
    }

    async fn lookup_related_batch(
        &self,
        def: &JoinDefinition,
        rel: &RelatedDefinition,
        keys: &[PkValue],
    ) -> Result<HashMap<PkValue, FetchOutcome>, JoinError> {
        let mut unique = HashSet::with_capacity(keys.len());
        let unique = keys
            .iter()
            .filter(|key| unique.insert((*key).clone()))
            .cloned()
            .collect::<Vec<_>>();
        let always_reconcile = should_reconcile_from_source(rel.cardinality, def.backfill.mode);
        let mut outcomes = HashMap::with_capacity(unique.len());
        let mut unresolved = Vec::new();

        for key in unique {
            if key.is_null() {
                outcomes.insert(key, FetchOutcome::empty_for(rel.cardinality));
                continue;
            }
            if !always_reconcile {
                let from_state = {
                    let state = self.state.lock();
                    let pks = state.foreign_pks_for(&rel.id, &key);
                    if pks.is_empty() {
                        None
                    } else {
                        let rows = pks
                            .iter()
                            .filter_map(|pk| state.get_foreign(&rel.table, pk))
                            .collect::<Vec<_>>();
                        Some(match rel.cardinality {
                            Cardinality::One => FetchOutcome::One(rows.into_iter().next()),
                            Cardinality::Many => FetchOutcome::Many(rows),
                        })
                    }
                };
                if let Some(outcome) = from_state {
                    outcomes.insert(key, outcome);
                    continue;
                }
            }
            if def.backfill.mode == BackfillMode::None {
                outcomes.insert(key, FetchOutcome::empty_for(rel.cardinality));
            } else {
                unresolved.push(key);
            }
        }

        if unresolved.is_empty() {
            return Ok(outcomes);
        }
        let fetch_select = select_with_fk(&rel.select, rel.join_on.to.columns());
        let fetched = self
            .fetcher
            .fetch_many_batch(
                &rel.table,
                rel.join_on.to.columns(),
                &unresolved,
                &fetch_select,
            )
            .await
            .map_err(|source| JoinError::Fetcher {
                related_id: rel.id.clone(),
                source,
            })?;
        let mut fetched = fetched.into_iter().collect::<HashMap<_, _>>();
        for key in unresolved {
            let rows = fetched.remove(&key).unwrap_or_default();
            {
                let mut state = self.state.lock();
                for row in &rows {
                    if let Some(pk) = extract_pk(row, rel.pk.columns()) {
                        state.set_foreign(&rel.table, &pk, row);
                        state.add_foreign_by_fk(&rel.id, &key, &pk);
                    }
                }
            }
            let outcome = match rel.cardinality {
                Cardinality::One => FetchOutcome::One(rows.into_iter().next()),
                Cardinality::Many => FetchOutcome::Many(rows),
            };
            outcomes.insert(key, outcome);
        }
        debug!(
            join = %def.effective_name(),
            related = %rel.id,
            keys = outcomes.len(),
            metric = "joins.fetcher.batch",
            "batch backfill via fetcher"
        );
        Ok(outcomes)
    }
}

// ---- helpers ---------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Resolve the source table from event headers, falling back to the
/// subject's `{namespace}.{relation}` segments if headers are absent.
fn source_table(event: &Event) -> Result<String, JoinError> {
    let ns = event.headers.get("ventstream.cdc.namespace");
    let rel = event.headers.get("ventstream.cdc.relation");
    if let (Some(n), Some(r)) = (ns, rel) {
        return Ok(format!("{n}.{r}"));
    }
    let subject = event.subject.as_str();
    let segments: Vec<&str> = subject.split('.').collect();
    if segments.len() >= 4 {
        // postgres.public.users.insert → segments[1].segments[2]
        if let (Some(ns), Some(rel)) = (segments.get(1), segments.get(2)) {
            return Ok(format!("{ns}.{rel}"));
        }
    }
    Err(JoinError::MalformedSubject {
        subject: subject.to_owned(),
    })
}

fn subject_op(event: &Event) -> Result<Op, JoinError> {
    let subject = event.subject.as_str();
    match subject.rsplit('.').next() {
        Some("insert") => Ok(Op::Insert),
        Some("update") => Ok(Op::Update),
        Some("delete") => Ok(Op::Delete),
        Some("truncate") => Ok(Op::Truncate),
        _ => Err(JoinError::MalformedSubject {
            subject: subject.to_owned(),
        }),
    }
}

fn parse_payload(event: &Event) -> Result<Value, JoinError> {
    serde_json::from_slice(event.payload.as_slice()).map_err(|err| JoinError::InvalidPayload {
        table: event.subject.as_str().to_owned(),
        detail: err.to_string(),
    })
}

/// Extract the "current" row from a CDC payload based on the operation.
///
/// Two payload conventions are accepted. Postgres wraps updates/deletes as
/// `{"new": …}` / `{"old": …}`; the full-image sources (MySQL, Mongo, Kafka)
/// emit the whole post-image directly on insert/update and `{}` on delete (the
/// PK lives in the `ventstream.doc.id` header — see [`pk_from_doc_id`]). For an
/// update we use `new` when present, else treat the bare object as the row.
fn current_row(payload: &Value, op: Op) -> Option<&Value> {
    match op {
        Op::Insert => Some(payload),
        Op::Update => match payload.get("new") {
            Some(v) => Some(v),
            None => payload.is_object().then_some(payload),
        },
        Op::Delete => payload.get("old"),
        Op::Truncate => None,
    }
}

/// Recover a row's primary key from the `ventstream.doc.id` header
/// (`{table}:["c1","c2"]`). Used when a full-image source emits a `{}` delete
/// that carries no row tuple — the PK is only in the doc id. Returns `None`
/// unless the component count matches `expected_columns`.
fn pk_from_doc_id(event: &Event, expected_columns: usize) -> Option<PkValue> {
    let doc_id = event.headers.get("ventstream.doc.id")?;
    // Split on the first ':' — the table prefix has no colon; everything after
    // is the JSON component array.
    let (_table, suffix) = doc_id.split_once(':')?;
    let components: Vec<Value> = serde_json::from_str(suffix).ok()?;
    if components.len() != expected_columns {
        return None;
    }
    Some(PkValue::from_values(&components))
}

/// Apply `select` projection and shape the [`FetchOutcome`] into a
/// JSON value suitable for embedding under `embed_as`.
fn apply_select_and_shape(rel: &RelatedDefinition, outcome: FetchOutcome) -> Value {
    match (rel.cardinality, outcome) {
        (Cardinality::One, FetchOutcome::One(None)) => match rel.on_missing {
            OnMissing::EmptyObject => Value::Object(Map::new()),
            OnMissing::Null | OnMissing::DropPrimary => Value::Null,
        },
        (Cardinality::One, FetchOutcome::One(Some(row))) => project_row(&row, &rel.select),
        (Cardinality::Many, FetchOutcome::Many(rows)) => {
            let mut rows: Vec<Value> = rows
                .into_iter()
                .map(|r| project_row(&r, &rel.select))
                .collect();
            if let Some(sort_col) = rel.sort_by.as_ref() {
                rows.sort_by(|a, b| {
                    json_compare(
                        a.get(sort_col).unwrap_or(&Value::Null),
                        b.get(sort_col).unwrap_or(&Value::Null),
                    )
                });
            }
            Value::Array(rows)
        }
        // Cardinality / outcome mismatch should not happen — guard
        // defensively with an empty embedding.
        _ => Value::Null,
    }
}

/// The columns to fetch for a related row: the user's `select` plus any
/// `join_on.to` (FK) column it omits, so the persisted foreign row always
/// carries the FK needed to recover the parent on a PK-only DELETE (H17).
///
/// An empty `select` means "all columns" (the fetcher emits `SELECT *`),
/// which already includes the FK — so it's returned unchanged.
fn select_with_fk(select: &[String], fk_columns: &[String]) -> Vec<String> {
    if select.is_empty() {
        return Vec::new();
    }
    let mut cols = select.to_vec();
    for fk in fk_columns {
        if !cols.iter().any(|c| c == fk) {
            cols.push(fk.clone());
        }
    }
    cols
}

fn project_row(row: &Value, select: &[String]) -> Value {
    if select.is_empty() {
        return row.clone();
    }
    let Some(obj) = row.as_object() else {
        return Value::Null;
    };
    let mut out = Map::with_capacity(select.len());
    for col in select {
        if let Some(v) = obj.get(col) {
            out.insert(col.clone(), v.clone());
        }
    }
    Value::Object(out)
}

/// Deterministic JSON value compare for `sort_by`. Numbers compare by
/// value; strings lexicographically; nulls sort first; mismatched
/// types fall back to string representation.
fn json_compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Number(x), Value::Number(y)) => {
            let xf = x.as_f64().unwrap_or(0.0);
            let yf = y.as_f64().unwrap_or(0.0);
            xf.partial_cmp(&yf).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// Rewrite a subject's trailing op segment to `delete`.
///
/// A tombstone is by definition a delete, so its subject MUST end in `.delete`
/// regardless of the triggering event's op — the OpenSearch sink classifies
/// deletes purely by a `.delete` subject suffix (`is_delete_event`), so a
/// tombstone carrying any other op (notably a TRUNCATE-triggered one ending in
/// `.truncate`) would be re-indexed instead of removing the doc (M8). No-op for
/// a subject that already ends in `.delete` (the `Op::Delete` caller).
fn delete_subject(source: &Subject) -> Subject {
    let s = source.as_str();
    let base = s.rsplit_once('.').map_or(s, |(head, _)| head);
    Subject::new(format!("{base}.delete")).unwrap_or_else(|_| source.clone())
}

/// Build a delete event tagged with the stable `ventstream.doc.id`
/// header. The sink uses that header to address the right doc when
/// emitting a `delete` bulk action — without it we'd send a random
/// per-emit ULID and tombstone nothing. The subject is normalized to
/// `.delete` (see [`delete_subject`]) so the sink classifies it as a delete
/// even when triggered by a TRUNCATE.
fn build_tombstone(
    source: &Event,
    def: &JoinDefinition,
    primary_table: &str,
    primary_pk: &PkValue,
) -> Event {
    let mut headers_map = std::collections::HashMap::new();
    for (k, v) in source.headers.iter() {
        headers_map.insert(k.clone(), v.clone());
    }
    headers_map.insert(
        "ventstream.join.name".into(),
        def.effective_name().to_owned(),
    );
    headers_map.insert("ventstream.join.primary_pk".into(), primary_pk.to_string());
    headers_map.insert(
        "ventstream.doc.id".into(),
        doc_id_value(primary_table, primary_pk),
    );
    stamp_target_index(&mut headers_map, def);
    Event::builder(source.source.clone(), delete_subject(&source.subject))
        .id(source.id)
        .payload(source.payload.clone())
        .content_type(source.content_type.clone())
        .occurred_at(source.occurred_at)
        .headers(Headers::from_map(headers_map))
        .build()
}

fn build_composed(
    source: &Event,
    def: &JoinDefinition,
    primary_table: &str,
    primary_pk: &PkValue,
    composed: &Value,
) -> Event {
    let mut headers_map = std::collections::HashMap::new();
    for (k, v) in source.headers.iter() {
        headers_map.insert(k.clone(), v.clone());
    }
    headers_map.insert(
        "ventstream.join.name".into(),
        def.effective_name().to_owned(),
    );
    headers_map.insert("ventstream.join.primary_pk".into(), primary_pk.to_string());
    // Stable per-logical-row id. Sinks (e.g. OpenSearch) prefer this
    // over the per-emit `event.id` so re-emits update the same doc
    // instead of creating a new one each time.
    headers_map.insert(
        "ventstream.doc.id".into(),
        doc_id_value(primary_table, primary_pk),
    );
    stamp_target_index(&mut headers_map, def);
    let bytes = serde_json::to_vec(composed).unwrap_or_else(|_| source.payload.as_slice().to_vec());
    Event::builder(source.source.clone(), source.subject.clone())
        .id(source.id)
        .payload(Payload::from_vec(bytes))
        .content_type(source.content_type.clone())
        .occurred_at(source.occurred_at)
        .headers(Headers::from_map(headers_map))
        .build()
}

/// Whether a relation must reconcile against the source on every compose
/// rather than trusting the cache (M7). True only for a `Many` relation with
/// backfill enabled: its cache can be incomplete (children arriving via CDC
/// before the parent), and the source is authoritative for the full set. `One`
/// relations (single authoritative row once cached) and backfill-disabled
/// `Many` (no source to reconcile against) trust the cache.
fn should_reconcile_from_source(cardinality: Cardinality, backfill: BackfillMode) -> bool {
    cardinality == Cardinality::Many && backfill != BackfillMode::None
}

fn doc_id_value(primary_table: &str, primary_pk: &PkValue) -> String {
    // Use the shared canonical encoder so this matches the SQL-denormalize
    // path and the reconcile parser byte-for-byte. Components are
    // text-normalized: an integer PK now renders `["5"]`, not the `[5]` this
    // used to emit via `PkValue`'s Display — otherwise the same row got two
    // different doc-ids across modes and switching modes orphaned it (M5).
    let components = ventstream_core::doc_id::components_from_json(&primary_pk.to_json());
    ventstream_core::doc_id::doc_id(primary_table, &components)
}

/// Build a re-emit triggered by a foreign-side change. Reuses the
/// primary's original subject and source, mints a fresh event id so
/// downstream consumers see this as a new doc-update (not a duplicate
/// of the original insert).
fn build_re_emit(
    triggering_event: &Event,
    def: &JoinDefinition,
    primary_table: &str,
    primary_pk: &PkValue,
    composed: &Value,
) -> Event {
    let mut headers_map = std::collections::HashMap::new();
    for (k, v) in triggering_event.headers.iter() {
        headers_map.insert(k.clone(), v.clone());
    }
    headers_map.insert(
        "ventstream.join.name".into(),
        def.effective_name().to_owned(),
    );
    headers_map.insert(
        "ventstream.join.reason".into(),
        format!("foreign-side change on '{}'", triggering_event.subject),
    );
    headers_map.insert("ventstream.cdc.relation".into(), {
        // Split "namespace.relation" → "relation"
        primary_table
            .rsplit('.')
            .next()
            .unwrap_or(primary_table)
            .to_owned()
    });
    headers_map.insert(
        "ventstream.cdc.namespace".into(),
        primary_table.split('.').next().unwrap_or("").to_owned(),
    );
    headers_map.insert("ventstream.join.primary_pk".into(), primary_pk.to_string());
    headers_map.insert(
        "ventstream.doc.id".into(),
        doc_id_value(primary_table, primary_pk),
    );
    stamp_target_index(&mut headers_map, def);

    // Preserve the source's 4-segment subject convention
    // (`{scheme}.{namespace}.{relation}.{op}`). The triggering event
    // has the scheme as its first segment; we keep that, splice in
    // the primary's `{namespace}.{relation}`, and use `update` for
    // the op (the row's effective content changed). Without this,
    // sinks whose index templates pick a positional segment land
    // re-emits in a different index from primary INSERTs.
    let scheme = triggering_event
        .subject
        .as_str()
        .split('.')
        .next()
        .unwrap_or("source");
    let subject = Subject::new(format!("{scheme}.{primary_table}.update"))
        .unwrap_or_else(|_| triggering_event.subject.clone());
    let bytes = serde_json::to_vec(composed).unwrap_or_default();
    Event::builder(triggering_event.source.clone(), subject)
        .payload(Payload::from_vec(bytes))
        .content_type(triggering_event.content_type.clone())
        .headers(Headers::from_map(headers_map))
        .build()
}

fn stamp_target_index(
    headers: &mut std::collections::HashMap<String, String>,
    def: &JoinDefinition,
) {
    if let Some(index) = def.target_index() {
        headers.insert("ventstream.target.index".to_owned(), index.to_owned());
    }
}

fn source_watermark(event: &Event) -> u64 {
    [ACK_SEQUENCE_HEADER, LSN_HEADER, TX_ID_HEADER]
        .into_iter()
        .filter_map(|header| event.headers.get(header))
        .filter_map(|value| value.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
}

fn join_barrier(sequence: u64) -> Result<Event, JoinError> {
    let source = SourceUri::new("internal://join-durability")
        .map_err(|err| JoinError::Internal(format!("join barrier source: {err}")))?;
    let subject = Subject::new("ventstream.internal.join.barrier")
        .map_err(|err| JoinError::Internal(format!("join barrier subject: {err}")))?;
    let mut headers = HashMap::new();
    headers.insert(JOIN_BARRIER_HEADER.to_owned(), "true".to_owned());
    headers.insert(JOIN_SEQUENCE_HEADER.to_owned(), sequence.to_string());
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .headers(Headers::from_map(headers))
        .build())
}

async fn send(
    output: &EventSender,
    event: Event,
    shutdown: &ShutdownToken,
) -> Result<(), JoinError> {
    output
        .send(event, shutdown)
        .await
        .map_err(|err| JoinError::Internal(format!("publish: {err}")))
}

// `FetchError` is referenced via `JoinError::Fetcher`; keep import live.
#[allow(dead_code)]
fn _ensure_fetch_error_is_referenced(_e: &FetchError) {}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use crate::config::{
        BackfillConfig, BackfillMode, JoinOn, OnMissing, PkSpec, PrimaryRef, StateBackend,
        StateConfig, TargetConfig,
    };
    use crate::fetcher::NoopFetcher;
    use async_trait::async_trait;

    #[test]
    fn many_with_backfill_reconciles_one_and_none_trust_cache() {
        // M7: only a Many relation with backfill enabled forces a source
        // reconcile (its cache can be partial). One relations and
        // backfill-disabled Many trust the cache.
        assert!(should_reconcile_from_source(
            Cardinality::Many,
            BackfillMode::SyncOnMiss
        ));
        assert!(!should_reconcile_from_source(
            Cardinality::Many,
            BackfillMode::None
        ));
        assert!(!should_reconcile_from_source(
            Cardinality::One,
            BackfillMode::SyncOnMiss
        ));
        assert!(!should_reconcile_from_source(
            Cardinality::One,
            BackfillMode::None
        ));
    }
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn doc_id_text_normalizes_integer_pk_to_match_sql_and_core() {
        // M5: an integer PK used to render `public.orders:[5]` (JSON number)
        // via PkValue's Display, while the SQL path emitted `["5"]` — the same
        // row got two doc-ids across modes. Now it text-normalizes to match.
        let pk = PkValue::from_single(&json!(5));
        let id = doc_id_value("public.orders", &pk);
        assert_eq!(
            id, r#"public.orders:["5"]"#,
            "integer PK must text-normalize"
        );
        assert_eq!(
            id,
            ventstream_core::doc_id::doc_id("public.orders", &["5".to_owned()]),
            "must equal the shared canonical encoder"
        );
        // Text/uuid keys are unchanged by the fix.
        let txt = PkValue::from_single(&json!("abc-123"));
        assert_eq!(
            doc_id_value("public.orders", &txt),
            r#"public.orders:["abc-123"]"#
        );
    }
    use ventstream_core::{bus, ContentType, ShutdownToken, SourceUri};

    fn join_orders_with_customer() -> JoinDefinition {
        JoinDefinition {
            name: Some("orders_denormalized".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "customer".into(),
                table: "public.customers".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["customer_id".into()]),
                    to: PkSpec(vec!["id".into()]),
                },
                select: vec!["id".into(), "name".into()],
                embed_as: "customer".into(),
                cardinality: Cardinality::One,
                on_missing: OnMissing::Null,
                sort_by: None,
            }],
            target: TargetConfig::default(),
            state: StateConfig {
                backend: StateBackend::Memory,
            },
            backfill: BackfillConfig {
                mode: BackfillMode::None,
            },
        }
    }

    /// Orders with the SAME `customers` table embedded twice — as `billing`
    /// (join on `billing_id`) and `shipping` (join on `shipping_id`). Used to
    /// exercise H1 (a table in two `related` entries).
    fn join_orders_two_customer_rels() -> JoinDefinition {
        let cust_rel = |id: &str, from: &str, embed: &str| RelatedDefinition {
            id: id.into(),
            table: "public.customers".into(),
            pk: PkSpec(vec!["id".into()]),
            join_on: JoinOn {
                from: PkSpec(vec![from.into()]),
                to: PkSpec(vec!["id".into()]),
            },
            select: vec!["id".into(), "name".into()],
            embed_as: embed.into(),
            cardinality: Cardinality::One,
            on_missing: OnMissing::Null,
            sort_by: None,
        };
        JoinDefinition {
            name: Some("orders_two".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![
                cust_rel("billing", "billing_id", "billing"),
                cust_rel("shipping", "shipping_id", "shipping"),
            ],
            target: TargetConfig::default(),
            state: StateConfig {
                backend: StateBackend::Memory,
            },
            backfill: BackfillConfig {
                mode: BackfillMode::None,
            },
        }
    }

    /// Orders embedding a `coupon` joined on a NON-PK, mutable column
    /// (`coupons.code`), so a foreign UPDATE can change the join key. Used to
    /// exercise H2 (foreign-side re-key cleanup).
    fn join_orders_with_coupon_by_code() -> JoinDefinition {
        JoinDefinition {
            name: Some("orders_coupon".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "coupon".into(),
                table: "public.coupons".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["coupon_code".into()]),
                    to: PkSpec(vec!["code".into()]),
                },
                select: vec!["code".into(), "pct".into()],
                embed_as: "coupon".into(),
                cardinality: Cardinality::One,
                on_missing: OnMissing::Null,
                sort_by: None,
            }],
            target: TargetConfig::default(),
            state: StateConfig {
                backend: StateBackend::Memory,
            },
            backfill: BackfillConfig {
                mode: BackfillMode::None,
            },
        }
    }

    // `payload: Value` is taken by value purely for ergonomic call
    // sites (every test passes a fresh `json!(...)` literal). Clippy's
    // needless_pass_by_value fires on this — we silence it locally
    // rather than thread `&json!(...)` through every assertion.
    #[allow(clippy::needless_pass_by_value)]
    fn make_event(table: &str, op: &str, payload: Value) -> Event {
        let (ns, rel) = table.split_once('.').unwrap_or(("public", table));
        let subject = Subject::new(format!("postgres.{ns}.{rel}.{op}")).expect("subject");
        let source = SourceUri::new(format!("postgres://pub/{table}")).expect("uri");
        let mut headers = std::collections::HashMap::new();
        headers.insert("ventstream.cdc.namespace".into(), ns.to_owned());
        headers.insert("ventstream.cdc.relation".into(), rel.to_owned());
        Event::builder(source, subject)
            .payload(Payload::from_vec(serde_json::to_vec(&payload).unwrap()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build()
    }

    async fn drain(receiver: &mut EventReceiver) -> Vec<Event> {
        let mut out = Vec::new();
        while let Ok(Some(e)) =
            tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv()).await
        {
            out.push(e);
        }
        out
    }

    /// Programmable fetcher for tests.
    #[derive(Default)]
    struct CannedFetcher {
        one_responses: parking_lot::Mutex<Vec<Value>>,
        many_responses: parking_lot::Mutex<Vec<Vec<Value>>>,
        calls: AtomicUsize,
    }
    impl CannedFetcher {
        fn one(value: Value) -> Self {
            // The engine now uses `fetch_many` for both cardinalities
            // and takes the first row for `cardinality: one`. So a
            // canned "one row" response is encoded as a single-element
            // many-response.
            Self {
                one_responses: parking_lot::Mutex::new(Vec::new()),
                many_responses: parking_lot::Mutex::new(vec![vec![value]]),
                calls: AtomicUsize::new(0),
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(AtomicOrdering::SeqCst)
        }
    }
    #[async_trait]
    impl RelatedFetcher for CannedFetcher {
        async fn fetch_one(
            &self,
            _table: &str,
            _pk: &[String],
            _v: &PkValue,
            _select: &[String],
        ) -> Result<Option<Value>, FetchError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(self.one_responses.lock().pop())
        }
        async fn fetch_many(
            &self,
            _table: &str,
            _fk: &[String],
            _v: &PkValue,
            _select: &[String],
        ) -> Result<Vec<Value>, FetchError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(self.many_responses.lock().pop().unwrap_or_default())
        }
    }

    #[tokio::test]
    async fn primary_event_with_known_foreign_composes_embedded_object() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // Pre-populate the customer in state via a foreign-side event.
        let customer_event = make_event(
            "public.customers",
            "insert",
            json!({"id": 5, "name": "Alice", "email": "alice@x"}),
        );
        engine
            .handle(&customer_event, &sender, &shutdown)
            .await
            .expect("ok");

        let order_event = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5, "total": 99.99}),
        );
        engine
            .handle(&order_event, &sender, &shutdown)
            .await
            .expect("ok");

        let events = drain(&mut receiver).await;
        // Expect: one customer pass-through? No — customers IS related,
        // so the engine consumes it. Only the composed order should
        // come out.
        assert_eq!(events.len(), 1);
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert_eq!(composed["id"], 1);
        assert_eq!(composed["customer"]["name"], "Alice");
        // `select` projection: email was NOT requested, so it must be absent.
        assert!(composed["customer"].get("email").is_none());
    }

    /// Full-image source event (MySQL/Mongo/Kafka shape): bare row on
    /// insert/update, `{}` on delete, PK carried in the `ventstream.doc.id`
    /// header.
    #[allow(clippy::needless_pass_by_value)]
    fn make_full_image_event(table: &str, op: &str, payload: Value, pk: &PkValue) -> Event {
        let (ns, rel) = table.split_once('.').unwrap_or(("public", table));
        let subject = Subject::new(format!("mysql.{ns}.{rel}.{op}")).expect("subject");
        let source = SourceUri::new(format!("mysql://{table}")).expect("uri");
        let mut headers = std::collections::HashMap::new();
        headers.insert("ventstream.cdc.namespace".into(), ns.to_owned());
        headers.insert("ventstream.cdc.relation".into(), rel.to_owned());
        headers.insert("ventstream.doc.id".into(), doc_id_value(table, pk));
        Event::builder(source, subject)
            .payload(Payload::from_vec(serde_json::to_vec(&payload).unwrap()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build()
    }

    #[tokio::test]
    async fn full_image_update_recomposes_primary() {
        // MySQL/Mongo/Kafka emit the whole post-image on update with NO
        // `{"new"}` wrapper. current_row must treat the bare object as the row.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "insert",
                    json!({"id": 5, "name": "Alice"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5, "total": 100}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        // Full-image update: bare row, `.update` subject, no `{"new"}`.
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "update",
                    json!({"id": 1, "customer_id": 5, "total": 175}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        let events = drain(&mut receiver).await;
        let last: Value =
            serde_json::from_slice(events.last().expect("an event").payload.as_slice()).unwrap();
        assert_eq!(last["total"], json!(175));
        assert_eq!(last["customer"]["name"], "Alice");
    }

    #[tokio::test]
    async fn empty_delete_tombstones_primary_via_doc_id() {
        // Full-image `{}` delete: the PK is only in the doc-id header.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5, "total": 100}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        let _ = drain(&mut receiver).await;

        let pk = PkValue::from_single(&json!(1));
        let del = make_full_image_event("public.orders", "delete", json!({}), &pk);
        engine.handle(&del, &sender, &shutdown).await.expect("ok");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        assert!(is_delete(&events[0]));
        assert_eq!(
            events[0].headers.get("ventstream.doc.id"),
            Some(r#"public.orders:["1"]"#)
        );
    }

    #[tokio::test]
    async fn empty_delete_on_related_recomposes_primary_via_doc_id() {
        // Deleting the customer with a `{}` payload (PK only in doc id) must
        // still find the order that embedded it and recompose with customer null.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "insert",
                    json!({"id": 5, "name": "Alice"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5, "total": 100}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        let _ = drain(&mut receiver).await;

        let pk = PkValue::from_single(&json!(5));
        let del = make_full_image_event("public.customers", "delete", json!({}), &pk);
        engine.handle(&del, &sender, &shutdown).await.expect("ok");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert_eq!(composed["id"], json!(1));
        assert!(composed["customer"].is_null());
    }

    #[test]
    fn delete_subject_rewrites_trailing_op_to_delete() {
        let trunc = Subject::new("postgres.public.orders.truncate".to_owned()).expect("subj");
        assert_eq!(
            delete_subject(&trunc).as_str(),
            "postgres.public.orders.delete"
        );
        // No-op when the subject already ends in `.delete` (the Op::Delete caller).
        let del = Subject::new("postgres.public.orders.delete".to_owned()).expect("subj");
        assert_eq!(
            delete_subject(&del).as_str(),
            "postgres.public.orders.delete"
        );
    }

    /// Mirror of the sink's `is_delete_event`: trailing subject segment.
    fn is_delete(event: &Event) -> bool {
        event.subject.as_str().rsplit('.').next() == Some("delete")
    }

    #[tokio::test]
    async fn primary_truncate_emits_delete_classified_tombstones() {
        // M8 regression: a primary-table TRUNCATE must emit per-doc tombstones
        // the sink classifies as DELETES. build_tombstone reused the source's
        // `.truncate` subject, so the sink would re-index the doc instead of
        // deleting it — defeating the whole point of the truncate handling.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        let order = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5}),
        );
        engine.handle(&order, &sender, &shutdown).await.expect("ok");
        let _ = drain(&mut receiver).await; // discard the insert's composed emit

        let trunc = make_event("public.orders", "truncate", json!({}));
        engine.handle(&trunc, &sender, &shutdown).await.expect("ok");
        let events = drain(&mut receiver).await;

        assert_eq!(events.len(), 1, "one tombstone for the one purged primary");
        assert!(
            is_delete(&events[0]),
            "primary-truncate tombstone must be delete-classified, got subject {}",
            events[0].subject.as_str()
        );
    }

    #[tokio::test]
    async fn primary_truncate_emits_each_shared_primary_target_once() {
        let mut live = join_orders_with_customer();
        live.name = Some("orders_live".into());
        live.target.index = Some("orders-live".into());
        let mut archive = live.clone();
        archive.name = Some("orders_archive".into());
        archive.target.index = Some("orders-archive".into());
        let engine = Arc::new(JoinEngine::new(vec![live, archive], Arc::new(NoopFetcher)));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("primary insert");
        let _ = drain(&mut receiver).await;

        engine
            .handle(
                &make_event("public.orders", "truncate", json!({})),
                &sender,
                &shutdown,
            )
            .await
            .expect("primary truncate");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].headers.get("ventstream.join.name"),
            Some("orders_live")
        );
        assert_eq!(
            events[0].headers.get("ventstream.target.index"),
            Some("orders-live")
        );
        assert_eq!(
            events[1].headers.get("ventstream.join.name"),
            Some("orders_archive")
        );
        assert_eq!(
            events[1].headers.get("ventstream.target.index"),
            Some("orders-archive")
        );
        assert!(events.iter().all(is_delete));
        assert_eq!(engine.state.lock().primary_count(), 0);
    }

    #[tokio::test]
    async fn related_truncate_recompose_is_upsert_not_delete() {
        // The related path recomposes affected primaries (an upsert minus the
        // gone children), so its emit must NOT be delete-classified.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        let customer = make_event(
            "public.customers",
            "insert",
            json!({"id": 5, "name": "Alice"}),
        );
        engine
            .handle(&customer, &sender, &shutdown)
            .await
            .expect("ok");
        let order = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5}),
        );
        engine.handle(&order, &sender, &shutdown).await.expect("ok");
        let _ = drain(&mut receiver).await; // discard the insert emits

        let trunc = make_event("public.customers", "truncate", json!({}));
        engine.handle(&trunc, &sender, &shutdown).await.expect("ok");
        let events = drain(&mut receiver).await;

        assert_eq!(events.len(), 1, "the affected order recomposes once");
        assert!(
            !is_delete(&events[0]),
            "related-truncate recompose must be an upsert, got subject {}",
            events[0].subject.as_str()
        );
    }

    #[tokio::test]
    async fn primary_update_omitting_toast_column_retains_it_via_merge() {
        // H4 end-to-end (join path): an UPDATE whose `new` omits an unchanged
        // TOAST column (the mapper drops the key) must NOT drop that column
        // from the composed doc or engine state — it's merged from the prior
        // stored row. Without the merge, `note` would vanish here (the engine
        // replaces the stored row wholesale and the sink full-replaces).
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // Insert with a large, TOAST-able `note` column.
        let insert = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5, "total": 99.99, "note": "BIG"}),
        );
        engine
            .handle(&insert, &sender, &shutdown)
            .await
            .expect("ok");
        let _ = drain(&mut receiver).await; // discard the insert's composed doc

        // UPDATE changes `total`; `note` is unchanged so pgoutput/the mapper
        // omit it from `new`.
        let update = make_event(
            "public.orders",
            "update",
            json!({"new": {"id": 1, "customer_id": 5, "total": 150.0}}),
        );
        engine
            .handle(&update, &sender, &shutdown)
            .await
            .expect("ok");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1, "the update re-emits the composed primary");
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert_eq!(composed["id"], 1);
        assert_eq!(composed["total"], 150.0, "changed column is updated");
        assert_eq!(
            composed["note"], "BIG",
            "unchanged-TOAST column must be retained from the prior row, not dropped"
        );
    }

    #[tokio::test]
    async fn related_update_omitting_toast_column_retains_it_in_embed() {
        // H4 (foreign side): a related-table UPDATE that omits an unchanged
        // TOAST column must NOT drop it from the stored foreign row — else
        // every primary embedding the row recomposes without it. The related
        // `select` here is [id, name], so `name` stands in for the TOAST col.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        let cust = make_event(
            "public.customers",
            "insert",
            json!({"id": 5, "name": "Alice", "email": "a@x"}),
        );
        engine.handle(&cust, &sender, &shutdown).await.expect("ok");
        let order = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5, "total": 99}),
        );
        engine.handle(&order, &sender, &shutdown).await.expect("ok");
        let _ = drain(&mut receiver).await; // discard the insert composes

        // Update the customer; `name` is unchanged so it's omitted from `new`.
        let cust_upd = make_event(
            "public.customers",
            "update",
            json!({"new": {"id": 5, "email": "new@x"}}),
        );
        engine
            .handle(&cust_upd, &sender, &shutdown)
            .await
            .expect("ok");

        let events = drain(&mut receiver).await;
        assert!(
            !events.is_empty(),
            "related update must re-emit the embedding primary"
        );
        let composed: Value =
            serde_json::from_slice(events.last().unwrap().payload.as_slice()).unwrap();
        assert_eq!(composed["id"], 1);
        assert_eq!(
            composed["customer"]["name"], "Alice",
            "unchanged-TOAST column on the related row must be retained in the embed"
        );
    }

    #[tokio::test]
    async fn related_table_in_two_relations_updates_both() {
        // H1: `customers` is embedded twice (billing + shipping). An order
        // bills customer 5 and ships to customer 7. Updating the SHIPPING
        // customer (7) must re-emit the order — under the old routing only the
        // FIRST relation (billing) was handled, whose reverse index has no
        // order with billing_id=7, so the order was never re-emitted and its
        // shipping embed went stale.
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_two_customer_rels()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        for (id, name) in [(5, "Alice"), (7, "Carol")] {
            engine
                .handle(
                    &make_event(
                        "public.customers",
                        "insert",
                        json!({"id": id, "name": name}),
                    ),
                    &sender,
                    &shutdown,
                )
                .await
                .expect("ok");
        }
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "billing_id": 5, "shipping_id": 7}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        let _ = drain(&mut receiver).await;

        // Update only the shipping customer.
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "update",
                    json!({"new": {"id": 7, "name": "Dave"}}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");

        let events = drain(&mut receiver).await;
        assert!(
            !events.is_empty(),
            "updating the shipping customer must re-emit the order (H1)"
        );
        let composed: Value =
            serde_json::from_slice(events.last().unwrap().payload.as_slice()).unwrap();
        assert_eq!(composed["billing"]["name"], "Alice", "billing unchanged");
        assert_eq!(
            composed["shipping"]["name"], "Dave",
            "shipping embed must update — the second relation is no longer skipped"
        );
    }

    #[tokio::test]
    async fn foreign_join_key_change_corrects_old_parent_no_leak() {
        // H2: coupon joined on its non-PK `code`. Re-keying the coupon
        // (code SAVE10 -> SAVE20) must (a) re-emit the order that referenced
        // the OLD code and (b) drop its now-unmatched embed. Under the bug the
        // old foreign_by_fk["SAVE10"] entry leaked, the old parent was never
        // re-emitted, and a recompose would embed the re-keyed coupon (stale).
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_coupon_by_code()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event(
                    "public.coupons",
                    "insert",
                    json!({"id": 9, "code": "SAVE10", "pct": 10}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "coupon_code": "SAVE10"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");
        let after_insert = drain(&mut receiver).await;
        let doc: Value =
            serde_json::from_slice(after_insert.last().unwrap().payload.as_slice()).unwrap();
        assert_eq!(
            doc["coupon"]["pct"], 10,
            "order embeds the coupon at insert"
        );

        // Re-key the coupon: SAVE10 -> SAVE20.
        engine
            .handle(
                &make_event(
                    "public.coupons",
                    "update",
                    json!({"new": {"id": 9, "code": "SAVE20", "pct": 10}}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("ok");

        let events = drain(&mut receiver).await;
        assert!(
            !events.is_empty(),
            "re-keying the coupon must re-emit the old parent (H2)"
        );
        let composed: Value =
            serde_json::from_slice(events.last().unwrap().payload.as_slice()).unwrap();
        assert_eq!(composed["id"], 1);
        assert!(
            composed["coupon"].is_null(),
            "order's coupon_code (SAVE10) matches no coupon now → embed dropped, not stale"
        );
    }

    #[tokio::test]
    async fn foreign_join_key_change_deduplicates_overlapping_parent_buckets() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_coupon_by_code()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event(
                    "public.coupons",
                    "insert",
                    json!({"id": 9, "code": "SAVE10", "pct": 10}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("coupon insert");
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "coupon_code": "SAVE10"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("order insert");
        let _ = drain(&mut receiver).await;

        // Model an overlapping/stale link while the child moves from SAVE10
        // to SAVE20. The paged old/new bucket merge must emit this order once.
        engine.state.lock().add_primary_reverse(
            "coupon",
            &PkValue::from_single(&json!("SAVE20")),
            "public.orders",
            &PkValue::from_single(&json!(1)),
        );
        engine
            .handle(
                &make_event(
                    "public.coupons",
                    "update",
                    json!({"new": {"id": 9, "code": "SAVE20", "pct": 10}}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("coupon reparent");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1, "overlapping parent must not be duplicated");
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert_eq!(composed["id"], 1);
    }

    #[tokio::test]
    async fn foreign_update_reemits_affected_primary() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // Seed customer + order.
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "insert",
                    json!({"id": 5, "name": "Alice"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        drain(&mut receiver).await; // discard initial emit

        // Now update customer.name.
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "update",
                    json!({"new": {"id": 5, "name": "Alicia"}, "old": null}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1, "exactly one re-emit");
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert_eq!(composed["customer"]["name"], "Alicia");
        assert_eq!(
            events[0].headers.get("ventstream.join.name"),
            Some("orders_denormalized")
        );
    }

    #[tokio::test]
    async fn primary_with_missing_foreign_uses_on_missing_null() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        let order_event = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 999}), // no customer with id 999
        );
        engine
            .handle(&order_event, &sender, &shutdown)
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        assert!(composed["customer"].is_null());
    }

    #[tokio::test]
    async fn primary_update_changing_fk_rewires_reverse_index() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // Two customers.
        engine
            .handle(
                &make_event("public.customers", "insert", json!({"id": 5, "name": "A"})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        engine
            .handle(
                &make_event("public.customers", "insert", json!({"id": 6, "name": "B"})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        // Order references customer 5.
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        drain(&mut receiver).await;

        // Update order to reference customer 6.
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "update",
                    json!({"new": {"id": 1, "customer_id": 6}, "old": {"id": 1, "customer_id": 5}}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let after_update = drain(&mut receiver).await;
        assert_eq!(after_update.len(), 1);
        let v: Value = serde_json::from_slice(after_update[0].payload.as_slice()).unwrap();
        assert_eq!(v["customer"]["name"], "B");

        // Now change customer 5 → should NOT re-emit (order no longer points there)
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "update",
                    json!({"new": {"id": 5, "name": "A-renamed"}, "old": null}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let after_customer_5 = drain(&mut receiver).await;
        assert!(
            after_customer_5.is_empty(),
            "stale fk should not trigger re-emit: got {after_customer_5:?}"
        );

        // But changing customer 6 SHOULD re-emit.
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "update",
                    json!({"new": {"id": 6, "name": "Beatrice"}, "old": null}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let after_customer_6 = drain(&mut receiver).await;
        assert_eq!(after_customer_6.len(), 1);
        let v: Value = serde_json::from_slice(after_customer_6[0].payload.as_slice()).unwrap();
        assert_eq!(v["customer"]["name"], "Beatrice");
    }

    #[tokio::test]
    async fn one_to_many_embeds_array_with_sort_by() {
        let def = JoinDefinition {
            name: Some("orders_with_items".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "items".into(),
                table: "public.line_items".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["id".into()]),
                    to: PkSpec(vec!["order_id".into()]),
                },
                select: vec!["id".into(), "product".into()],
                embed_as: "items".into(),
                cardinality: Cardinality::Many,
                on_missing: OnMissing::Null,
                sort_by: Some("id".into()),
            }],
            target: TargetConfig::default(),
            state: StateConfig::default(),
            backfill: BackfillConfig {
                mode: BackfillMode::None,
            },
        };
        let engine = Arc::new(JoinEngine::new(vec![def], Arc::new(NoopFetcher)));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        // Two line items belonging to order 1 (arriving out of order).
        engine
            .handle(
                &make_event(
                    "public.line_items",
                    "insert",
                    json!({"id": 102, "order_id": 1, "product": "gadget"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        engine
            .handle(
                &make_event(
                    "public.line_items",
                    "insert",
                    json!({"id": 100, "order_id": 1, "product": "widget"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        engine
            .handle(
                &make_event("public.orders", "insert", json!({"id": 1, "total": 50})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        let items = composed["items"].as_array().expect("array");
        assert_eq!(items.len(), 2);
        // Sorted by id ascending — 100 first, 102 second.
        assert_eq!(items[0]["id"], 100);
        assert_eq!(items[0]["product"], "widget");
        assert_eq!(items[1]["id"], 102);
        assert_eq!(items[1]["product"], "gadget");
    }

    #[tokio::test]
    async fn target_index_header_is_stamped_on_composed_events() {
        let def: JoinDefinition = serde_yaml::from_str(
            r"
name: orders
primary:
  table: public.orders
  pk: id
target:
  index: tenant_a_orders
",
        )
        .unwrap();
        let engine = Arc::new(JoinEngine::new(vec![def], Arc::new(NoopFetcher)));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event("public.orders", "insert", json!({"id": 1, "total": 50})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].headers.get("ventstream.target.index"),
            Some("tenant_a_orders")
        );
    }

    /// A child-row DELETE under Postgres' default REPLICA IDENTITY carries
    /// only the child PK in its old tuple — the FK linking it to the parent
    /// is absent. The engine must recover that FK from the row it stored on
    /// insert and still recompose the parent. Regression guard for the
    /// silent "deleted child lingers in the embedded array" bug.
    #[tokio::test]
    async fn related_delete_with_pk_only_old_tuple_recovers_fk_and_reemits() {
        let def = JoinDefinition {
            name: Some("orders_with_items".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "items".into(),
                table: "public.line_items".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["id".into()]),
                    to: PkSpec(vec!["order_id".into()]),
                },
                select: vec!["id".into(), "product".into()],
                embed_as: "items".into(),
                cardinality: Cardinality::Many,
                on_missing: OnMissing::Null,
                sort_by: Some("id".into()),
            }],
            target: TargetConfig::default(),
            state: StateConfig::default(),
            backfill: BackfillConfig {
                mode: BackfillMode::None,
            },
        };
        let engine = Arc::new(JoinEngine::new(vec![def], Arc::new(NoopFetcher)));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        // Two items on order 1, then the order. Insert tuples always carry
        // every column, regardless of the table's replica identity.
        for item in [
            json!({"id": 100, "order_id": 1, "product": "widget"}),
            json!({"id": 102, "order_id": 1, "product": "gadget"}),
        ] {
            engine
                .handle(
                    &make_event("public.line_items", "insert", item),
                    &sender,
                    &shutdown,
                )
                .await
                .unwrap();
        }
        engine
            .handle(
                &make_event("public.orders", "insert", json!({"id": 1, "total": 50})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        drain(&mut receiver).await; // discard insert emits

        // Delete item 100 with a PK-ONLY old tuple — no `order_id`, exactly
        // what Postgres emits under the default REPLICA IDENTITY.
        engine
            .handle(
                &make_event("public.line_items", "delete", json!({"old": {"id": 100}})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(
            events.len(),
            1,
            "parent must be re-emitted even though the delete event lacked the FK"
        );
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        let items = composed["items"].as_array().expect("array");
        assert_eq!(
            items.len(),
            1,
            "the deleted child must leave the embedded array"
        );
        assert_eq!(
            items[0]["id"], 102,
            "the surviving item is the one not deleted"
        );
    }

    #[test]
    fn select_with_fk_appends_missing_fk_columns() {
        // FK omitted from select → appended.
        assert_eq!(
            select_with_fk(&["id".into(), "product".into()], &["order_id".into()]),
            vec!["id".to_owned(), "product".to_owned(), "order_id".to_owned()]
        );
        // FK already present → no duplicate.
        assert_eq!(
            select_with_fk(&["id".into(), "order_id".into()], &["order_id".into()]),
            vec!["id".to_owned(), "order_id".to_owned()]
        );
        // Empty select means "all columns" (SELECT *) → left empty.
        assert!(select_with_fk(&[], &["order_id".into()]).is_empty());
    }

    /// A fetcher that returns stored child rows **projected to exactly the
    /// requested `select`** — mimicking Postgres `SELECT <cols>`. This makes
    /// the test sensitive to whether the engine asks for the FK column: if it
    /// doesn't, the returned (and cached) row lacks the FK and the PK-only
    /// delete can't recover the parent.
    struct ProjectingFetcher {
        // FK value (text) → current child rows in the "database".
        children: parking_lot::Mutex<std::collections::HashMap<String, Vec<Value>>>,
    }
    impl ProjectingFetcher {
        fn new() -> Self {
            Self {
                children: parking_lot::Mutex::new(std::collections::HashMap::new()),
            }
        }
        fn set(&self, fk: &str, rows: Vec<Value>) {
            self.children.lock().insert(fk.to_owned(), rows);
        }
    }
    fn pk_single_text(v: &PkValue) -> String {
        match v.to_json() {
            Value::Array(arr) => arr
                .into_iter()
                .next()
                .map(|x| match x {
                    Value::String(s) => s,
                    other => other.to_string(),
                })
                .unwrap_or_default(),
            Value::String(s) => s,
            other => other.to_string(),
        }
    }
    #[async_trait]
    impl RelatedFetcher for ProjectingFetcher {
        async fn fetch_one(
            &self,
            _t: &str,
            _pk: &[String],
            _v: &PkValue,
            _s: &[String],
        ) -> Result<Option<Value>, FetchError> {
            Ok(None)
        }
        async fn fetch_many(
            &self,
            _t: &str,
            _fk: &[String],
            v: &PkValue,
            select: &[String],
        ) -> Result<Vec<Value>, FetchError> {
            let key = pk_single_text(v);
            let rows = self.children.lock().get(&key).cloned().unwrap_or_default();
            let projected = rows
                .into_iter()
                .map(|r| {
                    if select.is_empty() {
                        return r;
                    }
                    let obj = r.as_object().cloned().unwrap_or_default();
                    let mut out = Map::new();
                    for c in select {
                        if let Some(x) = obj.get(c) {
                            out.insert(c.clone(), x.clone());
                        }
                    }
                    Value::Object(out)
                })
                .collect();
            Ok(projected)
        }
    }

    struct CountingBatchFetcher {
        single_calls: AtomicUsize,
        batch_calls: AtomicUsize,
    }

    #[async_trait]
    impl RelatedFetcher for CountingBatchFetcher {
        async fn fetch_one(
            &self,
            _table: &str,
            _columns: &[String],
            _key: &PkValue,
            _select: &[String],
        ) -> Result<Option<Value>, FetchError> {
            Ok(None)
        }

        async fn fetch_many(
            &self,
            _table: &str,
            _columns: &[String],
            _key: &PkValue,
            _select: &[String],
        ) -> Result<Vec<Value>, FetchError> {
            self.single_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(vec![json!({"id": 1, "category_id": 9, "name": "featured"})])
        }

        async fn fetch_many_batch(
            &self,
            _table: &str,
            _columns: &[String],
            keys: &[PkValue],
            _select: &[String],
        ) -> Result<Vec<(PkValue, Vec<Value>)>, FetchError> {
            self.batch_calls.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(keys
                .iter()
                .cloned()
                .map(|key| {
                    (
                        key,
                        vec![json!({"id": 1, "category_id": 9, "name": "featured"})],
                    )
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn related_update_batches_backfill_for_all_affected_primaries() {
        let def = JoinDefinition {
            name: Some("products_with_tags".into()),
            primary: PrimaryRef {
                table: "public.products".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "tags".into(),
                table: "public.tags".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["category_id".into()]),
                    to: PkSpec(vec!["category_id".into()]),
                },
                select: vec!["id".into(), "name".into()],
                embed_as: "tags".into(),
                cardinality: Cardinality::Many,
                on_missing: OnMissing::Null,
                sort_by: None,
            }],
            target: TargetConfig::default(),
            state: StateConfig::default(),
            backfill: BackfillConfig {
                mode: BackfillMode::SyncOnMiss,
            },
        };
        let fetcher = Arc::new(CountingBatchFetcher {
            single_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
        });
        let engine = JoinEngine::new(vec![def], Arc::clone(&fetcher) as Arc<dyn RelatedFetcher>);
        let (sender, mut receiver) = bus::channel(256);
        let shutdown = ShutdownToken::new();
        for id in 1..=100 {
            engine
                .handle(
                    &make_event(
                        "public.products",
                        "insert",
                        json!({"id": id, "category_id": 9}),
                    ),
                    &sender,
                    &shutdown,
                )
                .await
                .expect("primary insert");
        }
        let _ = drain(&mut receiver).await;
        fetcher.single_calls.store(0, AtomicOrdering::Relaxed);
        fetcher.batch_calls.store(0, AtomicOrdering::Relaxed);

        engine
            .handle(
                &make_event(
                    "public.tags",
                    "insert",
                    json!({"id": 1, "category_id": 9, "name": "featured"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("related insert");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 100);
        assert_eq!(fetcher.single_calls.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(fetcher.batch_calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[tokio::test]
    async fn related_update_reemits_affected_primaries_in_bounded_chunks() {
        let def = JoinDefinition {
            name: Some("products_with_tags".into()),
            primary: PrimaryRef {
                table: "public.products".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "tags".into(),
                table: "public.tags".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["category_id".into()]),
                    to: PkSpec(vec!["category_id".into()]),
                },
                select: vec!["id".into(), "name".into()],
                embed_as: "tags".into(),
                cardinality: Cardinality::Many,
                on_missing: OnMissing::Null,
                sort_by: None,
            }],
            target: TargetConfig::default(),
            state: StateConfig::default(),
            backfill: BackfillConfig {
                mode: BackfillMode::SyncOnMiss,
            },
        };
        let fetcher = Arc::new(CountingBatchFetcher {
            single_calls: AtomicUsize::new(0),
            batch_calls: AtomicUsize::new(0),
        });
        let engine = JoinEngine::new(vec![def], Arc::clone(&fetcher) as Arc<dyn RelatedFetcher>);
        let primary_count = RE_EMIT_PRIMARY_CHUNK_SIZE * 2 + 17;
        let (sender, mut receiver) = bus::channel(primary_count * 2);
        let shutdown = ShutdownToken::new();

        for id in 1..=primary_count {
            engine
                .handle(
                    &make_event(
                        "public.products",
                        "insert",
                        json!({"id": id, "category_id": 9}),
                    ),
                    &sender,
                    &shutdown,
                )
                .await
                .expect("primary insert");
        }
        let _ = drain(&mut receiver).await;
        fetcher.single_calls.store(0, AtomicOrdering::Relaxed);
        fetcher.batch_calls.store(0, AtomicOrdering::Relaxed);

        engine
            .handle(
                &make_event(
                    "public.tags",
                    "insert",
                    json!({"id": 1, "category_id": 9, "name": "featured"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("related insert");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), primary_count);
        assert_eq!(fetcher.single_calls.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(
            fetcher.batch_calls.load(AtomicOrdering::Relaxed),
            primary_count.div_ceil(RE_EMIT_PRIMARY_CHUNK_SIZE)
        );
    }

    #[tokio::test]
    async fn related_truncate_reemits_large_fanout_across_bounded_pages() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let primary_count = RE_EMIT_PRIMARY_CHUNK_SIZE * 2 + 17;
        let (sender, mut receiver) = bus::channel(primary_count * 2);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event(
                    "public.customers",
                    "insert",
                    json!({"id": 5, "name": "Alice"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .expect("customer insert");
        for id in 0..primary_count {
            engine
                .handle(
                    &make_event(
                        "public.orders",
                        "insert",
                        json!({"id": id, "customer_id": 5}),
                    ),
                    &sender,
                    &shutdown,
                )
                .await
                .expect("order insert");
        }
        let _ = drain(&mut receiver).await;

        engine
            .handle(
                &make_event("public.customers", "truncate", json!({})),
                &sender,
                &shutdown,
            )
            .await
            .expect("related truncate");

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), primary_count);
        for event in events {
            let composed: Value = serde_json::from_slice(event.payload.as_slice()).unwrap();
            assert!(composed["customer"].is_null());
        }
    }

    /// H17 regression: a `cardinality: many` + `sync_on_miss` relation whose
    /// `select` OMITS the FK column. Children are sourced via the fetcher
    /// (never inserted over CDC). A PK-only DELETE — Postgres' default replica
    /// identity — must still recompose the parent and drop the child. Before
    /// the fix the cached foreign row lacked the FK, so the delete silently
    /// no-op'd and the child lingered. The existing `BackfillMode::None` test
    /// can't catch this — only a fetcher-backed config exercises the overwrite.
    #[tokio::test]
    async fn child_delete_recomposes_when_select_omits_fk_and_backfill_on() {
        let def = JoinDefinition {
            name: Some("orders_with_items".into()),
            primary: PrimaryRef {
                table: "public.orders".into(),
                pk: PkSpec(vec!["id".into()]),
            },
            related: vec![RelatedDefinition {
                id: "items".into(),
                table: "public.line_items".into(),
                pk: PkSpec(vec!["id".into()]),
                join_on: JoinOn {
                    from: PkSpec(vec!["id".into()]),
                    to: PkSpec(vec!["order_id".into()]),
                },
                // FK column `order_id` deliberately NOT selected.
                select: vec!["id".into(), "product".into()],
                embed_as: "items".into(),
                cardinality: Cardinality::Many,
                on_missing: OnMissing::Null,
                sort_by: Some("id".into()),
            }],
            target: TargetConfig::default(),
            state: StateConfig::default(),
            backfill: BackfillConfig {
                mode: BackfillMode::SyncOnMiss,
            },
        };
        let fetcher = Arc::new(ProjectingFetcher::new());
        // DB starts with two items on order 1 (full rows incl. the FK).
        fetcher.set(
            "1",
            vec![
                json!({"id": 100, "order_id": 1, "product": "widget"}),
                json!({"id": 102, "order_id": 1, "product": "gadget"}),
            ],
        );
        let engine = Arc::new(JoinEngine::new(
            vec![def],
            Arc::clone(&fetcher) as Arc<dyn RelatedFetcher>,
        ));
        let (sender, mut receiver) = bus::channel(16);
        let shutdown = ShutdownToken::new();

        // Parent insert pulls both children via the fetcher and caches them.
        engine
            .handle(
                &make_event("public.orders", "insert", json!({"id": 1, "total": 50})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let first = drain(&mut receiver).await;
        let composed: Value = serde_json::from_slice(first[0].payload.as_slice()).unwrap();
        assert_eq!(composed["items"].as_array().map(Vec::len), Some(2));
        // FK must NOT leak into the embedded doc (project_row strips it).
        assert!(composed["items"][0].get("order_id").is_none());

        // DB now reflects the delete; the re-fetch returns only item 102.
        fetcher.set(
            "1",
            vec![json!({"id": 102, "order_id": 1, "product": "gadget"})],
        );
        // PK-only old tuple — no FK present, exactly like default replica id.
        engine
            .handle(
                &make_event("public.line_items", "delete", json!({"old": {"id": 100}})),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(
            events.len(),
            1,
            "parent must recompose on the child delete (FK recovered from cached row)"
        );
        let composed: Value = serde_json::from_slice(events[0].payload.as_slice()).unwrap();
        let items = composed["items"].as_array().expect("array");
        assert_eq!(
            items.len(),
            1,
            "deleted child must leave the embedded array"
        );
        assert_eq!(items[0]["id"], 102, "surviving item is the one not deleted");
    }

    #[tokio::test]
    async fn sync_on_miss_invokes_fetcher_then_caches() {
        let mut def = join_orders_with_customer();
        def.backfill.mode = BackfillMode::SyncOnMiss;

        let fetcher = Arc::new(CannedFetcher::one(json!({"id": 5, "name": "FromDB"})));
        let engine = Arc::new(JoinEngine::new(
            vec![def],
            Arc::clone(&fetcher) as Arc<dyn RelatedFetcher>,
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // No customer in state yet — order comes in, fetcher should fire once.
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let first = drain(&mut receiver).await;
        let v: Value = serde_json::from_slice(first[0].payload.as_slice()).unwrap();
        assert_eq!(v["customer"]["name"], "FromDB");
        assert_eq!(fetcher.calls(), 1);

        // Second order with same customer — should hit cache, fetcher NOT called again.
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 2, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let _second = drain(&mut receiver).await;
        assert_eq!(fetcher.calls(), 1, "second lookup should hit state cache");
    }

    #[tokio::test]
    async fn unrelated_table_event_passes_through() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        // Event for `public.audit_log` — not in the join.
        let ev = make_event("public.audit_log", "insert", json!({"id": 1, "msg": "hi"}));
        engine.handle(&ev, &sender, &shutdown).await.unwrap();

        let events = drain(&mut receiver).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].subject.as_str(),
            "postgres.public.audit_log.insert"
        );
    }

    #[tokio::test]
    async fn ack_barrier_passes_through_unchanged_before_cdc_parsing() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        let mut headers = std::collections::HashMap::new();
        headers.insert(ACK_BARRIER_HEADER.to_owned(), "true".to_owned());
        headers.insert("cursor".to_owned(), "mysql-bin.000042:9001".to_owned());
        let barrier = Event::builder(
            SourceUri::new("internal://mysql-cursor").unwrap(),
            Subject::new("internal.ack").unwrap(),
        )
        .payload(Payload::from_vec(b"not-cdc-json".to_vec()))
        .headers(Headers::from_map(headers))
        .build();
        let expected = barrier.clone();

        engine
            .handle(&barrier, &sender, &shutdown)
            .await
            .expect("ack barrier pass-through");

        let events = drain(&mut receiver).await;
        assert_eq!(events, vec![expected]);
    }

    #[tokio::test]
    async fn runtime_moves_unrelated_event_through_owned_path() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (input_sender, input_receiver) = bus::channel(8);
        let (output_sender, mut output_receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        let run_task = tokio::spawn(Arc::clone(&engine).run_with_durability(
            input_receiver,
            output_sender,
            shutdown.clone(),
            None,
        ));

        let event = make_event("public.audit_log", "insert", json!({"id": 1, "msg": "hi"}));
        let expected = event.clone();
        input_sender.send(event, &shutdown).await.unwrap();
        drop(input_sender);

        let received =
            tokio::time::timeout(std::time::Duration::from_secs(1), output_receiver.recv())
                .await
                .expect("runtime should emit the pass-through event")
                .expect("output bus should contain the event");
        assert_eq!(received, expected);
        run_task
            .await
            .expect("join engine task should not panic")
            .expect("join engine should stop cleanly");
    }

    #[tokio::test]
    async fn runtime_exposes_source_progress_only_after_sink_and_state_boundary() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (input_sender, input_receiver) = bus::channel(8);
        let (output_sender, mut output_receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();
        let sink_progress = Arc::new(AtomicU64::new(0));
        let source_progress = Arc::new(AtomicU64::new(0));
        let durability =
            JoinDurability::new(Arc::clone(&sink_progress), Arc::clone(&source_progress));
        let run_task = tokio::spawn(Arc::clone(&engine).run_with_durability(
            input_receiver,
            output_sender,
            shutdown.clone(),
            Some(durability),
        ));

        let mut event = make_event(
            "public.orders",
            "insert",
            json!({"id": 1, "customer_id": 5}),
        );
        event.headers = event
            .headers
            .with_header(LSN_HEADER.to_owned(), "99".to_owned());
        input_sender.send(event, &shutdown).await.unwrap();
        drop(input_sender);

        let mut boundary = None;
        for _ in 0..2 {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(1), output_receiver.recv())
                    .await
                    .expect("join output should arrive")
                    .expect("join output bus should remain open");
            if event.headers.get(JOIN_BARRIER_HEADER) == Some("true") {
                boundary = event
                    .headers
                    .get(JOIN_SEQUENCE_HEADER)
                    .and_then(|value| value.parse::<u64>().ok());
            }
        }
        assert_eq!(source_progress.load(Ordering::Acquire), 0);

        sink_progress.store(boundary.expect("join boundary sequence"), Ordering::Release);
        run_task
            .await
            .expect("join runtime task should not panic")
            .expect("join runtime should commit after sink durability");
        assert_eq!(source_progress.load(Ordering::Acquire), 99);
    }

    #[tokio::test]
    async fn runtime_cancels_and_does_not_forward_ack_barrier_after_join_error() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (input_sender, input_receiver) = bus::channel(8);
        let (output_sender, mut output_receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        let mut cdc_headers = std::collections::HashMap::new();
        cdc_headers.insert("ventstream.cdc.namespace".to_owned(), "public".to_owned());
        cdc_headers.insert("ventstream.cdc.relation".to_owned(), "orders".to_owned());
        let invalid = Event::builder(
            SourceUri::new("postgres://pub/public.orders").unwrap(),
            Subject::new("postgres.public.orders.insert").unwrap(),
        )
        .payload(Payload::from_vec(b"{invalid-json".to_vec()))
        .content_type(ContentType::Json)
        .headers(Headers::from_map(cdc_headers))
        .build();

        let mut barrier_headers = std::collections::HashMap::new();
        barrier_headers.insert(ACK_BARRIER_HEADER.to_owned(), "true".to_owned());
        let barrier = Event::builder(
            SourceUri::new("internal://mysql-cursor").unwrap(),
            Subject::new("internal.ack").unwrap(),
        )
        .payload(Payload::from_vec(b"barrier".to_vec()))
        .headers(Headers::from_map(barrier_headers))
        .build();

        input_sender.send(invalid, &shutdown).await.unwrap();
        input_sender.send(barrier, &shutdown).await.unwrap();
        drop(input_sender);

        let run_task = tokio::spawn(Arc::clone(&engine).run_with_durability(
            input_receiver,
            output_sender,
            shutdown.clone(),
            None,
        ));
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), run_task)
            .await
            .expect("join runtime should stop after the processing error")
            .expect("join runtime task should not panic");
        assert!(result.is_err(), "processing failure must reach the caller");

        assert!(shutdown.is_cancelled());
        assert_eq!(
            output_receiver.recv().await,
            None,
            "the ack barrier after failed work must not reach the dispatcher"
        );
    }

    #[tokio::test]
    async fn primary_delete_cleans_state_and_passes_through() {
        let engine = Arc::new(JoinEngine::new(
            vec![join_orders_with_customer()],
            Arc::new(NoopFetcher),
        ));
        let (sender, mut receiver) = bus::channel(8);
        let shutdown = ShutdownToken::new();

        engine
            .handle(
                &make_event(
                    "public.customers",
                    "insert",
                    json!({"id": 5, "name": "Alice"}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "insert",
                    json!({"id": 1, "customer_id": 5}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        drain(&mut receiver).await;

        // Delete order
        engine
            .handle(
                &make_event(
                    "public.orders",
                    "delete",
                    json!({"old": {"id": 1, "customer_id": 5}}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let after_delete = drain(&mut receiver).await;
        assert_eq!(after_delete.len(), 1, "delete passes through");

        // Now updating customer 5 should NOT re-emit (order is gone).
        engine
            .handle(
                &make_event(
                    "public.customers",
                    "update",
                    json!({"new": {"id": 5, "name": "Renamed"}, "old": null}),
                ),
                &sender,
                &shutdown,
            )
            .await
            .unwrap();
        let after_customer_update = drain(&mut receiver).await;
        assert!(
            after_customer_update.is_empty(),
            "deleted primary should not be re-emitted: {after_customer_update:?}"
        );
    }
}
