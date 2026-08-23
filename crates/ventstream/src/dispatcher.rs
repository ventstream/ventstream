//! Dispatcher — drains the event bus, batches events, and hands batches
//! to a [`Sink`].
//!
//! ### Batching policy
//!
//! A batch is flushed when **any** of these fires:
//! - It reaches [`DispatcherConfig::max_events`].
//! - [`DispatcherConfig::flush_interval`] elapses since the last flush
//!   *and* the batch is non-empty.
//! - The upstream source closes (bus receiver returns `None`) — drain
//!   the partial batch before exiting.
//! - The shutdown token fires — cancel in-flight sink work and leave its source
//!   progress pinned so the batch is replayed after restart.
//!
//! ### Failure handling
//!
//! - Sink returns `Ok(())` → continue.
//! - Sink returns `Connection` / transport error → fail closed without
//!   advancing source progress. Transient retries normally remain inside the
//!   sink for the lifetime of the process; a surfaced whole-batch error is
//!   therefore a pipeline blocker, never a DLQ candidate.
//! - Sink returns `Rejected` with `failed_items: Some(_)` → exactly
//!   those events go to the DLQ, each with its own per-item error
//!   string. The rest are considered written.
//! - Sink returns `Rejected` with `failed_items: None`, `Blocked`, `Internal`,
//!   or `Timeout` → fail closed. Only an explicit event offset proves that a
//!   failure is permanent and safe to quarantine.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};
use ventstream_core::{
    Event, EventReceiver, MemoryBudget, ShutdownToken, Sink, SinkBatch, SinkError,
};

use crate::dlq::{record_best_effort, DlqWriter};

/// Header set by the Postgres CDC source carrying the wal_end LSN of
/// the WAL record that produced an event. Re-emits from the join
/// engine carry the trigger event's LSN through unchanged. The
/// dispatcher inspects this header to drive sink-ack-gated LSN
/// advancement back at the source.
const LSN_HEADER: &str = "ventstream.cdc.lsn";

/// Header set by the Neo4j CDC source carrying the monotonic
/// transaction id of the change event. Plays the same role as
/// [`LSN_HEADER`] for Postgres — it's the per-source watermark the
/// dispatcher feeds back to the source so cursor advancement is
/// gated on the sink having durably written the event.
///
/// A process only runs one source kind at a time, so the dispatcher
/// reads both headers and either-or; they can't collide on the same
/// event because no source stamps both.
const TX_ID_HEADER: &str = "ventstream.cdc.tx_id";

/// Process-local sequence used by Kafka only to confirm sink durability before
/// committing broker offsets. It is deliberately separate from the source
/// version used by OpenSearch because this value resets on engine restart.
const ACK_SEQUENCE_HEADER: &str = "ventstream.cdc.ack_seq";

/// Internal source-to-dispatcher ordering marker. Barrier events are never
/// written to the customer sink; their acknowledgement sequence becomes
/// durable only after every earlier dispatcher batch has committed.
const ACK_BARRIER_HEADER: &str = "ventstream.internal.ack_barrier";

/// Internal join-to-dispatcher persistence boundary. The dispatcher consumes
/// these markers after every preceding sink write in the ordered batch prefix
/// is durable, then exposes [`JOIN_SEQUENCE_HEADER`] to the join runtime.
const JOIN_BARRIER_HEADER: &str = "ventstream.internal.join_barrier";
const JOIN_SEQUENCE_HEADER: &str = "ventstream.internal.join_seq";

/// Configuration for the dispatcher's batching and flushing policy.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Maximum events per batch handed to the sink.
    pub max_events: usize,
    /// Maximum estimated encoded bytes per batch. Composed docs can be
    /// large (5-way join with many holds embedded), so an event-count
    /// threshold alone may produce oversized downstream requests. Each sink
    /// supplies a protocol-aware estimate and still enforces its exact limit.
    pub max_batch_bytes: usize,
    /// Maximum wait before flushing a non-empty partial batch.
    pub flush_interval: Duration,
    /// How many bulk writes the dispatcher may have in flight to the
    /// sink at the same time. The drain task itself stays single, so
    /// events are still ordered into batches in the order they
    /// arrived; this knob just unblocks the drain when one bulk is
    /// waiting on the sink's network round-trip. 1 = old serial
    /// behavior; 4 is a good default for OpenSearch at ~7-8k docs/sec
    /// per bulk.
    ///
    /// Cross-batch ordering safety: with parallel bulks, two batches
    /// touching the same doc id can complete out of spawn order. The sink
    /// stamps each op with the source LSN/tx_id as the OpenSearch external
    /// document version (`version_type: external_gte`), so a stale,
    /// lower-watermark write that lands after a newer one is rejected by
    /// OpenSearch rather than clobbering or resurrecting the newer state.
    /// Sources without a durable external version must clamp this to 1; the
    /// MySQL runtime does so before constructing the dispatcher.
    pub max_parallel_bulks: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            max_events: 2_000,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_millis(500),
            max_parallel_bulks: 4,
        }
    }
}

/// Dispatcher failure modes.
///
/// Sink and DLQ failures are converted into ordered batch outcomes so source
/// progress remains pinned. `Internal` is reserved for a dispatcher invariant
/// that cannot be represented by one of those outcomes.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DispatcherError {
    /// Internal invariant violated. Currently unused; kept so the
    /// `Result<_, DispatcherError>` return type remains forward-compatible.
    #[error("dispatcher internal error: {0}")]
    #[allow(dead_code)]
    Internal(String),
    /// The ordered committer stopped because a batch could not be made
    /// durable — a sink failure whose rejects could not be written to the
    /// DLQ, or a DLQ write/fsync that failed. The source watermark was
    /// deliberately not advanced, so the affected events will replay on
    /// restart; this variant exists so that decision reaches the caller
    /// as an error instead of a clean shutdown (the supervisor treats a
    /// clean shutdown as a pause and restarts immediately, forever).
    #[error("dispatcher failed closed: {0}")]
    FailedClosed(String),
}

/// Drains an [`EventReceiver`] into a [`Sink`], failing closed for
/// whole-request errors and quarantining only exact permanent item failures.
pub struct Dispatcher {
    sink: Arc<dyn Sink>,
    dlq: DlqWriter,
    config: DispatcherConfig,
    shutdown: ShutdownToken,
    /// Sink-progress watermark — the highest WAL LSN whose owning
    /// event has been durably written by the sink (or routed to the
    /// DLQ; either outcome means it doesn't need WAL re-streaming).
    /// Updated via `fetch_max` after each successful batch. The
    /// Postgres CDC source reads this to gate WAL slot advancement.
    /// `None` falls back to no gating, leaving the slot to advance
    /// on bus publish (legacy behavior).
    sink_progress: Option<Arc<AtomicU64>>,
    /// Ordered sink-durability sequence used by the join layer. This is
    /// separate from source LSN/transaction progress so snapshot-only batches
    /// can participate without corrupting source checkpoint semantics.
    transform_progress: Option<Arc<AtomicU64>>,
    /// Process-level memory pressure composed with sink-local adaptation.
    memory_budget: Option<Arc<MemoryBudget>>,
}

impl Dispatcher {
    /// Construct a dispatcher. The sink is wrapped in `Arc` so the
    /// engine can hand a single instance to one dispatcher today and
    /// fan out to multiple later without re-allocating.
    pub fn new(
        sink: Arc<dyn Sink>,
        dlq: DlqWriter,
        config: DispatcherConfig,
        shutdown: ShutdownToken,
    ) -> Self {
        Self {
            sink,
            dlq,
            config,
            shutdown,
            sink_progress: None,
            transform_progress: None,
            memory_budget: None,
        }
    }

    /// Attach a sink-progress watermark. After each batch is durably
    /// handled (sink-acked OR DLQ-routed), the dispatcher updates the
    /// atomic with the max LSN it saw in that batch via `fetch_max`,
    /// so a source reading the atomic gets a monotonically-non-
    /// decreasing high-water mark.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    /// Attach the ordered sink-durability sequence consumed by the join layer.
    #[must_use]
    pub fn with_transform_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.transform_progress = Some(progress);
        self
    }

    /// Attach process-level memory pressure controls.
    #[must_use]
    pub fn with_memory_budget(mut self, budget: Arc<MemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Run the drain loop until shutdown or the receiver closes.
    ///
    /// Pattern: one task drains the bus and builds batches; up to
    /// `max_parallel_bulks` sink.write() calls run in parallel through
    /// a JoinSet. When at capacity, the drain awaits one in-flight
    /// bulk to complete before spawning another. Order is preserved
    /// per *batch* (events stay in the order received within a batch);
    /// across batches there's no strict ordering guarantee, but
    /// deterministic doc IDs plus source-LSN external versioning at the
    /// sink layer make writes last-*watermark*-wins by doc — a stale
    /// lower-LSN op that completes after a newer one is rejected by
    /// OpenSearch (409) rather than clobbering/resurrecting it — so
    /// steady-state CDC stays correct even for upsert-vs-delete races
    /// (H18).
    pub async fn run(self, mut receiver: EventReceiver) -> Result<(), DispatcherError> {
        let max_events = self.config.max_events.max(1);
        let sink_request_limit = self.sink.max_request_bytes().unwrap_or(usize::MAX);
        let base_max_bytes = self.config.max_batch_bytes.min(sink_request_limit).max(1);
        // Keep the receive quantum bounded even when operators configure very
        // large sink batches. This amortizes channel wakeups without starving
        // flush timers or completed sink writes for tens of thousands of events.
        let receive_limit = max_events.min(4_096);
        let mut incoming: Vec<Event> = Vec::with_capacity(receive_limit);
        let mut batch: Vec<Event> = Vec::with_capacity(max_events);
        let mut batch_bytes: usize = 0;
        let mut in_flight: JoinSet<BatchResult> = JoinSet::new();
        let max_parallel = self.config.max_parallel_bulks.max(1);

        // Single watermark authority. Every reaped batch result — from the
        // select branch, the spawn_flush capacity-wait, and both drain loops —
        // is committed here, in spawn order, so out-of-order completion can't
        // advance the source slot past a non-durable lower batch (H16).
        let mut committer = OrderedCommitter::new(
            self.sink_progress.clone(),
            self.transform_progress.clone(),
            self.shutdown.clone(),
        );
        // Monotonic spawn-order sequence handed to each batch.
        let mut next_seq: u64 = 0;

        let mut flush_timer = interval(self.config.flush_interval);
        flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        flush_timer.tick().await;

        info!(
            sink = %self.sink.id(),
            max_parallel,
            max_events,
            configured_max_batch_bytes = self.config.max_batch_bytes,
            sink_request_limit,
            effective_max_bytes = base_max_bytes,
            memory_controlled = self.memory_budget.is_some(),
            "dispatcher starting"
        );

        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    info!(
                        sink = %self.sink.id(),
                        pending = batch.len(),
                        in_flight = in_flight.len(),
                        "dispatcher shutdown requested; draining"
                    );
                    if !batch.is_empty() {
                        // Terminal flush before exit — seq is not reused after this.
                        self.spawn_flush(&mut in_flight, &mut committer, next_seq, std::mem::take(&mut batch)).await;
                    }
                    // Drain remaining bulks, committing each so the slot
                    // advances as far as durably possible before exit.
                    while let Some(joined) = in_flight.join_next().await {
                        committer.on_joined(joined);
                    }
                    if let Err(err) = self.dlq.shutdown().await {
                        warn!(error = %err, "DLQ shutdown reported error");
                    }
                    // A fail-closed committer means events exist in neither
                    // the sink nor the DLQ. Report that as an error so the
                    // supervisor backs off and alerts, instead of treating
                    // it as a clean pause and rebuilding immediately forever.
                    return match committer.fatal_reason() {
                        Some(reason) => Err(DispatcherError::FailedClosed(reason.to_owned())),
                        None => Ok(()),
                    };
                }
                // Reap a completed bulk, commit in order, and flush what
                // accumulated meanwhile — batches grow to full size under
                // load instead of being carved up by the timer.
                Some(joined) = in_flight.join_next(), if !in_flight.is_empty() => {
                    committer.on_joined(joined);
                    if !batch.is_empty() {
                        let seq = next_seq;
                        next_seq += 1;
                        self.spawn_flush(&mut in_flight, &mut committer, seq, std::mem::take(&mut batch)).await;
                        batch_bytes = 0;
                    }
                }
                // Timer flush covers the idle tail only; under load the
                // reap branch above delivers batches.
                _ = flush_timer.tick(), if !batch.is_empty() && in_flight.is_empty() => {
                    let seq = next_seq;
                    next_seq += 1;
                    self.spawn_flush(&mut in_flight, &mut committer, seq, std::mem::take(&mut batch)).await;
                    batch_bytes = 0;
                }
                received = receiver.recv_many(&mut incoming, receive_limit) => {
                    if received == 0 {
                        info!(
                            sink = %self.sink.id(),
                            pending = batch.len(),
                            in_flight = in_flight.len(),
                            "dispatcher source closed; draining"
                        );
                        if !batch.is_empty() {
                            // Terminal flush before exit — seq is not reused after this.
                            self.spawn_flush(&mut in_flight, &mut committer, next_seq, std::mem::take(&mut batch)).await;
                        }
                        // Drain remaining bulks, committing each so the slot
                        // advances as far as durably possible before exit.
                        while let Some(joined) = in_flight.join_next().await {
                            committer.on_joined(joined);
                        }
                        if let Err(err) = self.dlq.shutdown().await {
                            warn!(error = %err, "DLQ shutdown reported error");
                        }
                        // See the shutdown arm above: fail-closed must not
                        // look like a clean exit.
                        return match committer.fatal_reason() {
                            Some(reason) => {
                                Err(DispatcherError::FailedClosed(reason.to_owned()))
                            }
                            None => Ok(()),
                        };
                    }

                    for event in incoming.drain(..) {
                        let effective_max_bytes = self
                            .memory_budget
                            .as_ref()
                            .map_or(base_max_bytes, |budget| {
                                budget.recommended_batch_bytes(base_max_bytes)
                            });
                        let event_bytes = self.sink.estimate_event_bytes(&event).max(1);
                        let would_cross_bytes = batch_bytes
                            .checked_add(event_bytes)
                            .map(|next| next > effective_max_bytes)
                            .unwrap_or(true);

                        // Flush the existing batch before adding an event that
                        // would cross the estimated request limit. A single
                        // oversized event is still passed to the sink so its
                        // exact validator can isolate it without dropping its
                        // healthy neighbors.
                        if !batch.is_empty()
                            && (batch.len() >= max_events || would_cross_bytes)
                        {
                            let seq = next_seq;
                            next_seq += 1;
                            self.spawn_flush(&mut in_flight, &mut committer, seq, std::mem::take(&mut batch)).await;
                            batch_bytes = 0;
                        }

                        batch.push(event);
                        batch_bytes = batch_bytes.saturating_add(event_bytes);
                        if batch.len() >= max_events || batch_bytes >= effective_max_bytes {
                            let seq = next_seq;
                            next_seq += 1;
                            self.spawn_flush(&mut in_flight, &mut committer, seq, std::mem::take(&mut batch)).await;
                            batch_bytes = 0;
                        }
                    }
                }
            }
        }
    }

    /// Spawn a bulk write into the in-flight set, tagged with `seq`. Awaits an
    /// existing in-flight bulk to complete first if we're already at capacity —
    /// and routes that reaped result through the committer (this capacity-wait
    /// is a reap site too; dropping its result would stall the watermark).
    async fn spawn_flush(
        &self,
        in_flight: &mut JoinSet<BatchResult>,
        committer: &mut OrderedCommitter,
        seq: u64,
        events: Vec<Event>,
    ) {
        if events.is_empty() {
            return;
        }
        let max_parallel = self.config.max_parallel_bulks.max(1);
        loop {
            let sink_desired = self
                .sink
                .recommended_concurrency(max_parallel)
                .clamp(1, max_parallel);
            let desired = self.memory_budget.as_ref().map_or(sink_desired, |budget| {
                budget
                    .recommended_concurrency(max_parallel)
                    .min(sink_desired)
            });
            if in_flight.len() < desired {
                break;
            }
            // Wait for at least one slot to free up. join_next returns
            // None only when the set is empty, which can't happen here.
            if let Some(joined) = in_flight.join_next().await {
                committer.on_joined(joined);
            }
        }
        let sink = Arc::clone(&self.sink);
        let dlq = self.dlq.clone();
        let shutdown = self.shutdown.clone();
        in_flight.spawn(async move { write_one_batch(sink, dlq, events, seq, shutdown).await });
    }
}

/// Durability verdict for one batch, carried back to the committer. Keeps the
/// failure detail (write vs fsync) so the fail-hard log names the cause.
#[derive(Debug, Clone, Copy)]
enum BatchOutcome {
    /// Sink ack'd, or DLQ-routed AND confirmed durable (writes ok + fsync ok),
    /// or nothing was routed to the DLQ.
    Durable,
    /// A batch routed poison events to the DLQ but could not be made durable.
    NonDurable { writes_ok: bool, synced: bool },
    /// A whole-batch failure cannot be proven to be event poison. The source
    /// checkpoint must remain pinned and the pipeline must stop.
    FailedClosed,
    /// Shutdown cancelled an in-flight write. The batch remains replayable.
    Cancelled,
}

/// Result of writing one batch — pure data the [`OrderedCommitter`] commits in
/// spawn order. The watermark authority lives in the committer, not here, so
/// out-of-order parallel completion can't leapfrog a non-durable lower batch.
#[derive(Debug)]
struct BatchResult {
    /// Spawn-order sequence number. The committer advances the watermark only
    /// along the contiguous run of committed seqs.
    seq: u64,
    /// Highest source watermark (LSN / tx_id) in the batch; `0` for no-LSN
    /// batches (bootstrap / snapshot), which still advance the prefix.
    max_watermark: u64,
    /// Highest join persistence boundary in the batch.
    max_transform_watermark: u64,
    outcome: BatchOutcome,
}

/// Commits [`BatchResult`]s to the sink-progress watermark **in sequence**,
/// even though batches complete out of order. Holds a reorder buffer keyed by
/// spawn `seq` and advances the watermark only along the contiguous *durable*
/// prefix. A non-durable batch at the head of that prefix fails the pipeline
/// hard rather than letting a later sibling's higher watermark leapfrog it —
/// the H16 hazard. Owned single-threaded by the run task (no lock), so this is
/// pure logic over `BatchResult`s and unit-testable by interleaving seqs.
struct OrderedCommitter {
    /// Next seq expected to commit; the watermark reflects every seq below it.
    next_seq: u64,
    /// Completed-but-not-yet-committed results awaiting the prefix to reach them.
    pending: BTreeMap<u64, BatchResult>,
    /// Sink-progress atomic the source reads to gate slot advancement.
    sink_progress: Option<Arc<AtomicU64>>,
    /// Sink-durability progress read by the join runtime.
    transform_progress: Option<Arc<AtomicU64>>,
    /// Fired on a non-durable batch (or a panicked task) to stop the pipeline.
    shutdown: ShutdownToken,
    /// Once failed, stop advancing — the prefix has a permanent hole.
    failed: bool,
    /// Why the committer stopped, when it stopped for a reason the caller
    /// must hear about. `None` for a clean cancellation during shutdown,
    /// which is not an error.
    fatal: Option<String>,
}

impl OrderedCommitter {
    fn new(
        sink_progress: Option<Arc<AtomicU64>>,
        transform_progress: Option<Arc<AtomicU64>>,
        shutdown: ShutdownToken,
    ) -> Self {
        Self {
            next_seq: 0,
            pending: BTreeMap::new(),
            sink_progress,
            transform_progress,
            shutdown,
            failed: false,
            fatal: None,
        }
    }

    /// Route a reaped JoinSet result. A panicked task can't report its seq, so
    /// its slot in the prefix could never be filled — that would silently stall
    /// the watermark forever; fail hard instead (a panic is a bug, not a steady
    /// state).
    fn on_joined(&mut self, joined: Result<BatchResult, JoinError>) {
        match joined {
            Ok(result) => self.commit(result),
            Err(err) => {
                error!(
                    error = %err,
                    "batch task panicked; stopping pipeline (watermark cannot advance safely)"
                );
                self.fail(format!("batch task panicked: {err}"));
            }
        }
    }

    /// Insert a completed batch, then advance the durable contiguous prefix.
    fn commit(&mut self, result: BatchResult) {
        if self.failed {
            return;
        }
        self.pending.insert(result.seq, result);
        while let Some(entry) = self.pending.remove(&self.next_seq) {
            match entry.outcome {
                BatchOutcome::Durable => {
                    // wm 0 (bootstrap/snapshot) still advances the prefix; it's
                    // just a no-op on the watermark itself (fetch_max skips it).
                    if let Some(progress) = &self.sink_progress {
                        if entry.max_watermark > 0 {
                            progress.fetch_max(entry.max_watermark, Ordering::Relaxed);
                        }
                    }
                    if let Some(progress) = &self.transform_progress {
                        if entry.max_transform_watermark > 0 {
                            progress.fetch_max(entry.max_transform_watermark, Ordering::Release);
                        }
                    }
                    self.next_seq += 1;
                }
                BatchOutcome::NonDurable { writes_ok, synced } => {
                    error!(
                        seq = entry.seq,
                        writes_ok,
                        synced,
                        "DLQ not durable for a batch that routed poison events; stopping the \
                         pipeline so the source slot is not confirmed past data that lives in \
                         neither the DLQ nor the WAL (will replay on restart)"
                    );
                    self.fail(format!(
                        "DLQ not durable for batch {} (writes_ok={writes_ok}, synced={synced})",
                        entry.seq
                    ));
                    return;
                }
                BatchOutcome::FailedClosed => {
                    error!(
                        seq = entry.seq,
                        "sink batch failed closed; stopping pipeline without advancing source progress"
                    );
                    self.fail(format!("sink batch {} failed closed", entry.seq));
                    return;
                }
                BatchOutcome::Cancelled => {
                    debug!(
                        seq = entry.seq,
                        "sink batch cancelled during shutdown; source progress remains unchanged"
                    );
                    self.failed = true;
                    self.pending.clear();
                    return;
                }
            }
        }
    }

    fn fail(&mut self, reason: impl Into<String>) {
        self.failed = true;
        if self.fatal.is_none() {
            self.fatal = Some(reason.into());
        }
        self.shutdown.cancel();
    }

    /// The failure the caller must propagate, if any.
    fn fatal_reason(&self) -> Option<&str> {
        self.fatal.as_deref()
    }
}

/// Write one batch to the sink and route only explicit per-item failures to the
/// DLQ, returning a [`BatchResult`] for the committer to apply in order. Does NOT touch the
/// sink-progress watermark or the shutdown token itself — the run task's
/// single [`OrderedCommitter`] owns that decision so out-of-order completion
/// can't confirm the source slot past a non-durable lower batch (C3 / H16).
async fn write_one_batch(
    sink: Arc<dyn Sink>,
    dlq: DlqWriter,
    events: Vec<Event>,
    seq: u64,
    shutdown: ShutdownToken,
) -> BatchResult {
    let max_watermark = max_watermark_in(&events);
    let max_transform_watermark = max_transform_watermark_in(&events);
    let events: Vec<Event> = events
        .into_iter()
        .filter(|event| !is_internal_barrier(event))
        .collect();
    let batch_size = events.len();
    let batch_size_u64 = u64::try_from(batch_size).unwrap_or(u64::MAX);
    ventstream_telemetry::bump_events_received(batch_size_u64);
    if events.is_empty() {
        return BatchResult {
            seq,
            max_watermark,
            max_transform_watermark,
            outcome: BatchOutcome::Durable,
        };
    }
    let total_bytes: usize = events.iter().map(|e| e.payload.len()).sum();
    debug!(
        sink = %sink.id(),
        batch_size,
        total_bytes,
        max_watermark,
        metric = "dispatcher.bulk.start",
        "flushing batch"
    );
    let write_started = std::time::Instant::now();

    // Clone events for the sink call. Cheap — every field of Event
    // is refcounted (Arc/Bytes). The original Vec is kept so the DLQ
    // path can address individual entries by offset.
    let sink_batch = SinkBatch::new(events.clone());
    // Track whether anything was routed to the DLQ this batch, and whether
    // every such write succeeded (a failed write is just as much a
    // durability hole as a failed fsync — the event is in neither the DLQ
    // nor the WAL once the slot advances).
    let mut dlq_written = false;
    let mut dlq_writes_ok = true;
    let sink_result = tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            return BatchResult {
                seq,
                max_watermark,
                max_transform_watermark,
                outcome: BatchOutcome::Cancelled,
            };
        }
        result = sink.write(sink_batch) => result,
    };
    match sink_result {
        Ok(()) => {
            let elapsed_ms = write_started.elapsed().as_millis() as u64;
            debug!(
                sink = %sink.id(),
                batch_size,
                total_bytes,
                elapsed_ms,
                metric = "dispatcher.bulk.ack",
                "batch acknowledged"
            );
            ventstream_telemetry::bump_events_delivered(batch_size_u64);
            ventstream_telemetry::record_bulk_latency_sample(elapsed_ms);
        }
        Err(SinkError::Rejected {
            failed_items: Some(items),
            rejected_count,
            ..
        }) => {
            let mut offsets_seen = vec![false; batch_size];
            let precise = rejected_count == items.len()
                && rejected_count > 0
                && items.iter().all(|item| {
                    let Some(seen) = offsets_seen.get_mut(item.offset) else {
                        return false;
                    };
                    if *seen {
                        return false;
                    }
                    *seen = true;
                    true
                });
            if !precise {
                error!(
                    sink = %sink.id(),
                    batch_size,
                    rejected_count,
                    item_count = items.len(),
                    "sink returned invalid per-item rejection metadata; failing closed"
                );
                return BatchResult {
                    seq,
                    max_watermark,
                    max_transform_watermark,
                    outcome: BatchOutcome::FailedClosed,
                };
            }
            let elapsed_ms = write_started.elapsed().as_millis() as u64;
            let rejected_u64 = u64::try_from(rejected_count).unwrap_or(u64::MAX);
            ventstream_telemetry::bump_events_delivered(
                u64::try_from(batch_size.saturating_sub(rejected_count)).unwrap_or(u64::MAX),
            );
            ventstream_telemetry::bump_events_failed(rejected_u64);
            ventstream_telemetry::record_bulk_latency_sample(elapsed_ms);
            warn!(
                sink = %sink.id(),
                batch_size,
                rejected_count,
                "sink rejected some events; routing offsets to DLQ"
            );
            for item in items {
                if let Some(event) = events.get(item.offset) {
                    dlq_written = true;
                    // record_best_effort now bumps the success/failure counter
                    // per event (M16) — no per-batch attempt bump here, which
                    // wrongly counted failed writes as successes.
                    dlq_writes_ok &= record_best_effort(&dlq, event, &item.error).await;
                }
            }
        }
        Err(err) => {
            error!(
                sink = %sink.id(),
                batch_size,
                error = %err,
                "sink failed for whole batch; failing closed without advancing source progress"
            );
            return BatchResult {
                seq,
                max_watermark,
                max_transform_watermark,
                outcome: BatchOutcome::FailedClosed,
            };
        }
    }

    // C3: a DLQ-routed event must be durably on disk BEFORE the source slot
    // is confirmed past its LSN — otherwise a power loss in that window
    // loses it (only in the OS page cache, and the slot already moved past
    // it in the WAL, so no replay either). fsync the DLQ now and report the
    // verdict; the committer decides whether to advance or fail hard, in
    // sequence, so a sibling batch's higher LSN can't leapfrog a hole here.
    let outcome = if dlq_written {
        let synced = match dlq.sync().await {
            Ok(()) => true,
            Err(err) => {
                error!(error = %err, "DLQ fsync failed");
                false
            }
        };
        if !dlq_writes_ok || !synced {
            BatchOutcome::NonDurable {
                writes_ok: dlq_writes_ok,
                synced,
            }
        } else {
            BatchOutcome::Durable
        }
    } else {
        BatchOutcome::Durable
    };

    BatchResult {
        seq,
        max_watermark,
        max_transform_watermark,
        outcome,
    }
}

fn is_internal_barrier(event: &Event) -> bool {
    event.headers.get(ACK_BARRIER_HEADER) == Some("true")
        || event.headers.get(JOIN_BARRIER_HEADER) == Some("true")
        // The snapshot-complete sentinel is a signal for the join engine;
        // without joins it would otherwise reach sinks and be materialized
        // as a document.
        || event.headers.get("ventstream.cdc.bootstrap") == Some("snapshot-complete")
}

fn max_transform_watermark_in(events: &[Event]) -> u64 {
    events
        .iter()
        .filter(|event| event.headers.get(JOIN_BARRIER_HEADER) == Some("true"))
        .filter_map(|event| event.headers.get(JOIN_SEQUENCE_HEADER))
        .filter_map(|value| value.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
}

/// Largest source watermark across the batch. Inspects
/// `ventstream.cdc.ack_seq` (Kafka/MySQL barriers), `ventstream.cdc.lsn`
/// (PG), or `ventstream.cdc.tx_id` (Neo4j). Durable source-version headers are
/// intentionally ignored here: they order sink documents, but are not the
/// process-local acknowledgement sequence a source waits on. Events without a
/// checkpoint header (e.g., snapshot bootstrap
/// events, the snapshot-complete sentinel) are ignored — they have no
/// associated source position and must not influence slot
/// advancement.
fn max_watermark_in(events: &[Event]) -> u64 {
    let mut max = 0u64;
    for event in events {
        let raw = event
            .headers
            .get(ACK_SEQUENCE_HEADER)
            .or_else(|| event.headers.get(LSN_HEADER))
            .or_else(|| event.headers.get(TX_ID_HEADER));
        if let Some(raw) = raw {
            if let Ok(v) = raw.parse::<u64>() {
                if v > max {
                    max = v;
                }
            }
        }
    }
    max
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
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;
    use ventstream_core::{bus, ContentType, Headers, MemoryPressure, Payload, SourceUri, Subject};

    // NOTE on C3 coverage: the DLQ-route happy path (writes succeed, fsync
    // succeeds, watermark advances) is exercised by the poison/partial
    // tests below. The fail-hard branch (DLQ write/fsync failure →
    // `shutdown.cancel()` so the slot is never confirmed past a lost event)
    // is correctness-critical but needs fault injection (an unwritable /
    // failing DLQ) to exercise — validated by a kill-9-mid-batch drill like
    // the PG/Neo4j integration suites, not a unit test. A pure-function gate
    // test was intentionally removed: it could not catch the cross-batch
    // `fetch_max` interaction that motivated fail-hard, so it gave false
    // confidence in the durability guarantee.

    fn make_event(label: &str) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(format!("test.{label}")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::empty())
            .build()
    }

    fn make_event_with_header(key: &str, value: &str) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new("test.h").expect("subject");
        let mut h = std::collections::HashMap::new();
        h.insert(key.to_owned(), value.to_owned());
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(h))
            .build()
    }

    // ---- OrderedCommitter: in-order watermark advancement over out-of-order
    // completion. This is the unit proof H16 was missing — the committer is
    // pure logic over BatchResults, so we can interleave seqs deterministically.

    fn durable(seq: u64, wm: u64) -> BatchResult {
        BatchResult {
            seq,
            max_watermark: wm,
            max_transform_watermark: 0,
            outcome: BatchOutcome::Durable,
        }
    }

    fn non_durable(seq: u64, wm: u64) -> BatchResult {
        BatchResult {
            seq,
            max_watermark: wm,
            max_transform_watermark: 0,
            outcome: BatchOutcome::NonDurable {
                writes_ok: false,
                synced: false,
            },
        }
    }

    fn failed_closed(seq: u64, wm: u64) -> BatchResult {
        BatchResult {
            seq,
            max_watermark: wm,
            max_transform_watermark: 0,
            outcome: BatchOutcome::FailedClosed,
        }
    }

    #[test]
    fn committer_advances_watermark_in_seq_order_despite_out_of_order_completion() {
        // Batches complete 0, 2, 1. The watermark must never reflect seq 2's
        // wm until seq 1 has committed — otherwise a gap is confirmed.
        let progress = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(Some(Arc::clone(&progress)), None, shutdown.clone());

        c.commit(durable(0, 100));
        assert_eq!(
            progress.load(Ordering::Relaxed),
            100,
            "seq 0 commits immediately"
        );

        c.commit(durable(2, 300));
        assert_eq!(
            progress.load(Ordering::Relaxed),
            100,
            "seq 2 must NOT advance the watermark while seq 1 is missing"
        );

        c.commit(durable(1, 200));
        assert_eq!(
            progress.load(Ordering::Relaxed),
            300,
            "seq 1 then 2 flush the buffered prefix → watermark reaches 300"
        );
        assert!(!shutdown.is_cancelled());
    }

    #[test]
    fn committer_fail_hard_on_nondurable_does_not_leapfrog_to_higher_sibling() {
        // H16 (a): seq 0 durable (wm 100), seq 2 durable (wm 300) buffered,
        // seq 1 NON-durable. The committer must stop at seq 1 and NEVER let
        // seq 2's wm 300 advance the slot past the lost lower batch.
        let progress = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(Some(Arc::clone(&progress)), None, shutdown.clone());

        c.commit(durable(0, 100));
        c.commit(durable(2, 300));
        assert_eq!(progress.load(Ordering::Relaxed), 100);

        c.commit(non_durable(1, 200));
        assert_eq!(
            progress.load(Ordering::Relaxed),
            100,
            "watermark stays at the last contiguous durable point (100), never 200 or 300"
        );
        assert!(
            shutdown.is_cancelled(),
            "non-durable head of prefix fails the pipeline hard"
        );

        // A late-arriving result after failure must not advance anything.
        c.commit(durable(3, 400));
        assert_eq!(progress.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn fail_closed_is_reported_as_fatal_but_cancellation_is_not() {
        // The whole point of failing closed is that events exist in
        // neither the sink nor the DLQ. If that reaches the caller as a
        // clean shutdown, the supervisor treats it as a pause and
        // rebuilds the engine immediately — forever, at exit code 0.
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(None, None, shutdown.clone());
        c.commit(failed_closed(0, 100));
        assert!(
            c.fatal_reason().is_some(),
            "a failed-closed batch must surface a reason to propagate"
        );

        // A cancellation during shutdown also stops the committer, but it
        // is NOT an error — the caller must still see a clean exit.
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(None, None, shutdown.clone());
        c.commit(BatchResult {
            seq: 0,
            max_watermark: 100,
            max_transform_watermark: 0,
            outcome: BatchOutcome::Cancelled,
        });
        assert!(
            c.fatal_reason().is_none(),
            "shutdown cancellation is a clean exit, not a failure"
        );

        // Non-durable DLQ is fatal for the same reason as failed-closed.
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(None, None, shutdown.clone());
        c.commit(non_durable(0, 100));
        assert!(c.fatal_reason().is_some());
    }

    #[test]
    fn committer_failed_closed_does_not_leapfrog_to_higher_sibling() {
        let progress = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(Some(Arc::clone(&progress)), None, shutdown.clone());

        c.commit(durable(0, 100));
        c.commit(durable(2, 300));
        c.commit(failed_closed(1, 200));

        assert_eq!(
            progress.load(Ordering::Relaxed),
            100,
            "the source cursor must remain at the last durable contiguous batch"
        );
        assert!(shutdown.is_cancelled());

        c.commit(durable(3, 400));
        assert_eq!(progress.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn committer_no_lsn_batch_advances_prefix_without_moving_watermark() {
        // A wm-0 batch (bootstrap/snapshot) in the middle must not stall the
        // contiguous scan — it advances next_seq but is a no-op on the wm.
        let progress = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(Some(Arc::clone(&progress)), None, shutdown.clone());

        c.commit(durable(0, 100));
        c.commit(durable(1, 0)); // no-LSN batch in the middle
        c.commit(durable(2, 200));
        assert_eq!(
            progress.load(Ordering::Relaxed),
            200,
            "wm-0 batch advances the prefix so seq 2's 200 still lands"
        );
        assert!(!shutdown.is_cancelled());
    }

    #[test]
    fn committer_panicked_task_fails_hard() {
        let progress = Arc::new(AtomicU64::new(0));
        let shutdown = ShutdownToken::new();
        let mut c = OrderedCommitter::new(Some(Arc::clone(&progress)), None, shutdown.clone());

        // Manufacture a JoinError by aborting a spawned task and joining it.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let join_err = rt.block_on(async {
            let handle = tokio::spawn(async {
                futures_for_abort().await;
                durable(0, 100)
            });
            handle.abort();
            handle.await.expect_err("aborted task yields JoinError")
        });
        c.on_joined(Err(join_err));
        assert!(
            shutdown.is_cancelled(),
            "a panicked/aborted batch task fails hard"
        );
        assert_eq!(progress.load(Ordering::Relaxed), 0);
    }

    async fn futures_for_abort() {
        // Never resolves on its own; the test aborts it to produce a JoinError.
        std::future::pending::<()>().await;
    }

    #[test]
    fn max_watermark_reads_lsn_header() {
        let events = vec![
            make_event_with_header(LSN_HEADER, "1000"),
            make_event_with_header(LSN_HEADER, "1500"),
            make_event_with_header(LSN_HEADER, "1200"),
        ];
        assert_eq!(max_watermark_in(&events), 1500);
    }

    #[test]
    fn max_watermark_falls_back_to_tx_id_header_when_lsn_absent() {
        // Neo4j source stamps tx_id, not lsn — the dispatcher must still
        // surface a watermark so the source can gate cursor advancement.
        let events = vec![
            make_event_with_header(TX_ID_HEADER, "42"),
            make_event_with_header(TX_ID_HEADER, "99"),
        ];
        assert_eq!(max_watermark_in(&events), 99);
    }

    #[test]
    fn max_watermark_reads_kafka_ack_sequence() {
        let events = vec![
            make_event_with_header(ACK_SEQUENCE_HEADER, "1"),
            make_event_with_header(ACK_SEQUENCE_HEADER, "2"),
        ];
        assert_eq!(max_watermark_in(&events), 2);
    }

    #[tokio::test]
    async fn acknowledgement_barrier_advances_watermark_without_reaching_sink() {
        let sink = Arc::new(ScriptedSink::always_ok("barrier-test"));
        let dlq_path = temp_dlq();
        let dlq = DlqWriter::open(dlq_path.clone()).await.expect("dlq open");
        let mut barrier = make_event_with_header(ACK_SEQUENCE_HEADER, "17");
        barrier.headers = barrier
            .headers
            .with_header(ACK_BARRIER_HEADER.to_owned(), "true".to_owned());

        let result = write_one_batch(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq,
            vec![barrier],
            0,
            ShutdownToken::new(),
        )
        .await;

        assert_eq!(result.max_watermark, 17);
        assert!(matches!(result.outcome, BatchOutcome::Durable));
        assert!(sink.batches().is_empty(), "barrier must remain internal");
        let _ = std::fs::remove_file(dlq_path);
    }

    #[tokio::test]
    async fn join_barrier_advances_only_transform_progress_without_reaching_sink() {
        let sink = Arc::new(ScriptedSink::always_ok("join-barrier-test"));
        let dlq_path = temp_dlq();
        let dlq = DlqWriter::open(dlq_path.clone()).await.expect("dlq open");
        let mut barrier = make_event_with_header(JOIN_SEQUENCE_HEADER, "23");
        barrier.headers = barrier
            .headers
            .with_header(JOIN_BARRIER_HEADER.to_owned(), "true".to_owned());

        let result = write_one_batch(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq,
            vec![barrier],
            0,
            ShutdownToken::new(),
        )
        .await;

        assert_eq!(result.max_watermark, 0);
        assert_eq!(result.max_transform_watermark, 23);
        assert!(matches!(result.outcome, BatchOutcome::Durable));
        assert!(sink.batches().is_empty(), "barrier must remain internal");
        let _ = std::fs::remove_file(dlq_path);
    }

    #[test]
    fn max_watermark_handles_mixed_events_without_header() {
        // Bootstrap / snapshot-sentinel events have no header — they
        // must not influence the watermark.
        let events = vec![
            make_event_with_header(TX_ID_HEADER, "50"),
            make_event("no_header"),
            make_event_with_header(LSN_HEADER, "70"),
        ];
        assert_eq!(max_watermark_in(&events), 70);
    }

    #[test]
    fn source_version_orders_sink_writes_but_does_not_confirm_source_progress() {
        let event = make_event_with_header("ventstream.cdc.source_version", "4294967416");
        assert_eq!(max_watermark_in(&[event]), 0);
    }

    #[test]
    fn max_watermark_of_empty_batch_is_zero() {
        assert_eq!(max_watermark_in(&[]), 0);
    }

    fn temp_dlq() -> PathBuf {
        // Process-unique counter, not the wall clock: macOS's coarse clock
        // lets two parallel tests collide on `as_nanos()`, so the
        // file-read-back tests race under the multithreaded test runner.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "ventstream-dispatcher-test-{}-{}.jsonl",
            std::process::id(),
            n,
        ));
        path
    }

    /// Boxed closure that yields the sink result for the Nth `write` call.
    type ScriptedResultFn = Box<dyn FnMut(usize) -> Result<(), SinkError> + Send>;

    /// Records every batch it receives, returning a configurable result.
    struct ScriptedSink {
        id: String,
        received: Arc<Mutex<Vec<Vec<Event>>>>,
        flush_count: Arc<AtomicUsize>,
        result: Mutex<ScriptedResultFn>,
        estimated_overhead: usize,
        request_limit: Option<usize>,
    }

    impl ScriptedSink {
        fn always_ok(id: &str) -> Self {
            Self {
                id: id.to_owned(),
                received: Arc::new(Mutex::new(Vec::new())),
                flush_count: Arc::new(AtomicUsize::new(0)),
                result: Mutex::new(Box::new(|_| Ok(()))),
                estimated_overhead: 0,
                request_limit: None,
            }
        }

        fn with_result<F>(id: &str, f: F) -> Self
        where
            F: FnMut(usize) -> Result<(), SinkError> + Send + 'static,
        {
            Self {
                id: id.to_owned(),
                received: Arc::new(Mutex::new(Vec::new())),
                flush_count: Arc::new(AtomicUsize::new(0)),
                result: Mutex::new(Box::new(f)),
                estimated_overhead: 0,
                request_limit: None,
            }
        }

        fn with_size_limits(id: &str, estimated_overhead: usize, request_limit: usize) -> Self {
            Self {
                id: id.to_owned(),
                received: Arc::new(Mutex::new(Vec::new())),
                flush_count: Arc::new(AtomicUsize::new(0)),
                result: Mutex::new(Box::new(|_| Ok(()))),
                estimated_overhead,
                request_limit: Some(request_limit),
            }
        }

        fn batches(&self) -> Vec<Vec<Event>> {
            self.received.lock().clone()
        }
    }

    struct AdaptiveScriptedSink {
        desired: AtomicUsize,
        starts: AtomicUsize,
        active: AtomicUsize,
        tail_max_active: AtomicUsize,
    }

    struct ConcurrentScriptedSink {
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    struct PendingSink;

    #[async_trait]
    impl Sink for PendingSink {
        fn id(&self) -> &str {
            "pending"
        }

        fn kind(&self) -> &'static str {
            "scripted"
        }

        async fn write(&self, _batch: SinkBatch) -> Result<(), SinkError> {
            std::future::pending().await
        }
    }

    impl ConcurrentScriptedSink {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Sink for ConcurrentScriptedSink {
        fn id(&self) -> &str {
            "concurrent-scripted"
        }

        fn kind(&self) -> &'static str {
            "scripted"
        }

        async fn write(&self, _batch: SinkBatch) -> Result<(), SinkError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            sleep(Duration::from_millis(5)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl AdaptiveScriptedSink {
        fn new() -> Self {
            Self {
                desired: AtomicUsize::new(4),
                starts: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                tail_max_active: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl Sink for AdaptiveScriptedSink {
        fn id(&self) -> &str {
            "adaptive-scripted"
        }

        fn kind(&self) -> &'static str {
            "scripted"
        }

        fn recommended_concurrency(&self, configured_ceiling: usize) -> usize {
            self.desired.load(Ordering::SeqCst).min(configured_ceiling)
        }

        async fn write(&self, _batch: SinkBatch) -> Result<(), SinkError> {
            let write_number = self.starts.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            if write_number == 0 {
                // Simulate the first request observing downstream pressure.
                self.desired.store(1, Ordering::SeqCst);
            }
            // At most four writes can belong to the initial dispatcher wave.
            // Every later start must obey the reduced recommendation.
            if write_number >= 4 {
                self.tail_max_active.fetch_max(active, Ordering::SeqCst);
            }
            sleep(Duration::from_millis(5)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl Sink for ScriptedSink {
        fn id(&self) -> &str {
            &self.id
        }
        fn kind(&self) -> &'static str {
            "scripted"
        }
        fn estimate_event_bytes(&self, event: &Event) -> usize {
            event
                .payload
                .as_slice()
                .len()
                .saturating_add(self.estimated_overhead)
        }
        fn max_request_bytes(&self) -> Option<usize> {
            self.request_limit
        }
        async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
            let events = batch.into_events();
            let n = self.flush_count.fetch_add(1, Ordering::SeqCst);
            self.received.lock().push(events);
            (self.result.lock())(n)
        }
    }

    async fn run_dispatcher(
        sink: Arc<dyn Sink>,
        dlq_path: PathBuf,
        config: DispatcherConfig,
        events: Vec<Event>,
        explicit_shutdown_after: Option<Duration>,
    ) {
        run_dispatcher_with_memory(
            sink,
            dlq_path,
            config,
            events,
            explicit_shutdown_after,
            None,
        )
        .await;
    }

    async fn run_dispatcher_with_memory(
        sink: Arc<dyn Sink>,
        dlq_path: PathBuf,
        config: DispatcherConfig,
        events: Vec<Event>,
        explicit_shutdown_after: Option<Duration>,
        memory_budget: Option<Arc<MemoryBudget>>,
    ) {
        let dlq = DlqWriter::open(dlq_path).await.expect("dlq open");
        let shutdown = ShutdownToken::new();
        let mut dispatcher = Dispatcher::new(sink, dlq, config, shutdown.clone());
        if let Some(budget) = memory_budget {
            dispatcher = dispatcher.with_memory_budget(budget);
        }

        let (sender, receiver) = bus::channel(64);
        let producer_shutdown = shutdown.clone();
        let producer = tokio::spawn(async move {
            for e in events {
                let _ = sender.send(e, &producer_shutdown).await;
            }
            // Drop sender → receiver sees None unless shutdown fires first.
        });

        let dispatcher_handle = tokio::spawn(dispatcher.run(receiver));
        if let Some(d) = explicit_shutdown_after {
            sleep(d).await;
            shutdown.cancel();
        }
        let _ = producer.await;
        let _ = dispatcher_handle.await;
    }

    #[tokio::test]
    async fn batches_flush_at_max_events() {
        let sink = Arc::new(ScriptedSink::always_ok("test"));
        let cfg = DispatcherConfig {
            max_events: 3,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60), // effectively disabled,
            max_parallel_bulks: 4,
        };
        let events: Vec<Event> = (0..7).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
        )
        .await;

        let batches = sink.batches();
        // 7 events / max 3 = batches of [3, 3, 1] → 3 flushes total.
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 3);
        assert_eq!(batches[2].len(), 1);
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn dispatcher_obeys_a_runtime_concurrency_reduction() {
        let sink = Arc::new(AdaptiveScriptedSink::new());
        let cfg = DispatcherConfig {
            max_events: 1,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 8,
        };
        let events = (0..24)
            .map(|index| make_event(&format!("adaptive-{index}")))
            .collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
        )
        .await;

        assert_eq!(sink.starts.load(Ordering::SeqCst), 24);
        assert_eq!(
            sink.tail_max_active.load(Ordering::SeqCst),
            1,
            "batches started after the initial wave must honor the reduced limit"
        );
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn memory_pressure_caps_dispatcher_concurrency() {
        let sink = Arc::new(ConcurrentScriptedSink::new());
        let budget = MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024);
        budget.set_pressure(MemoryPressure::Critical);
        let cfg = DispatcherConfig {
            max_events: 1,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 8,
        };
        let events = (0..24)
            .map(|index| make_event(&format!("memory-{index}")))
            .collect();
        let dlq_path = temp_dlq();
        run_dispatcher_with_memory(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
            Some(budget),
        )
        .await;

        assert_eq!(
            sink.max_active.load(Ordering::SeqCst),
            1,
            "critical memory pressure must serialize sink writes"
        );
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn memory_pressure_reduces_dispatcher_batch_bytes() {
        let sink = Arc::new(ScriptedSink::always_ok("test"));
        let budget = MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024);
        budget.set_pressure(MemoryPressure::High);
        let cfg = DispatcherConfig {
            max_events: 100,
            max_batch_bytes: 4,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 1,
        };
        let events = (0..4).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher_with_memory(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
            Some(budget),
        )
        .await;

        let batch_sizes: Vec<usize> = sink.batches().iter().map(Vec::len).collect();
        assert_eq!(batch_sizes, vec![1, 1, 1, 1]);
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn sink_request_limit_flushes_before_crossing_estimated_bytes() {
        let sink = Arc::new(ScriptedSink::with_size_limits("test", 100, 250));
        let cfg = DispatcherConfig {
            max_events: 100,
            max_batch_bytes: 10_000,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 1,
        };
        // Each payload is two bytes plus 100 bytes of sink framing. Two fit
        // under 250, while a third must start the next request.
        let events: Vec<Event> = (0..5).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
        )
        .await;

        let batch_sizes: Vec<usize> = sink.batches().iter().map(Vec::len).collect();
        assert_eq!(batch_sizes, vec![2, 2, 1]);
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn flush_timer_flushes_partial_batch() {
        let sink = Arc::new(ScriptedSink::always_ok("test"));
        let cfg = DispatcherConfig {
            max_events: 1000,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_millis(80),
            max_parallel_bulks: 4,
        };
        // Only 3 events — never hits max — so flush must come from timer.
        let events: Vec<Event> = (0..3).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            Some(Duration::from_millis(250)),
        )
        .await;

        let batches = sink.batches();
        assert!(
            !batches.is_empty(),
            "expected at least one timer-driven flush"
        );
        let total: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(total, 3);
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn whole_batch_rejection_fails_closed_without_dlq() {
        let sink = Arc::new(ScriptedSink::with_result("test", |_| {
            Err(SinkError::Rejected {
                batch_size: 2,
                rejected_count: 2,
                message: "bad mapping".into(),
                failed_items: None,
            })
        }));
        let cfg = DispatcherConfig {
            max_events: 2,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 4,
        };
        let events: Vec<Event> = (0..2).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
        )
        .await;

        let dlq_contents = tokio::fs::read_to_string(&dlq_path)
            .await
            .expect("read dlq");
        assert!(
            dlq_contents.is_empty(),
            "a whole-batch failure has no proven poison event and must not enter the DLQ"
        );
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn invalid_per_item_rejection_metadata_fails_closed_without_dlq() {
        use ventstream_core::FailedItem;

        let sink = Arc::new(ScriptedSink::with_result("test", |_| {
            Err(SinkError::Rejected {
                batch_size: 2,
                rejected_count: 2,
                message: "invalid offsets".into(),
                failed_items: Some(vec![
                    FailedItem {
                        offset: 0,
                        error: "first".into(),
                    },
                    FailedItem {
                        offset: 0,
                        error: "duplicate".into(),
                    },
                ]),
            })
        }));
        let dlq_path = temp_dlq();
        let dlq = DlqWriter::open(dlq_path.clone()).await.expect("dlq open");
        let result = write_one_batch(
            sink as Arc<dyn Sink>,
            dlq,
            vec![make_event("e0"), make_event("e1")],
            0,
            ShutdownToken::new(),
        )
        .await;

        assert!(matches!(result.outcome, BatchOutcome::FailedClosed));
        assert!(tokio::fs::read_to_string(&dlq_path)
            .await
            .expect("read dlq")
            .is_empty());
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn shutdown_cancels_stalled_sink_without_dlq_or_progress() {
        let sink = Arc::new(PendingSink);
        let dlq_path = temp_dlq();
        let dlq = DlqWriter::open(dlq_path.clone()).await.expect("dlq open");
        let shutdown = ShutdownToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(write_one_batch(
            sink as Arc<dyn Sink>,
            dlq,
            vec![make_event_with_header(LSN_HEADER, "900")],
            0,
            task_shutdown,
        ));

        tokio::task::yield_now().await;
        shutdown.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("stalled sink must cancel promptly")
            .expect("batch task");

        assert!(matches!(result.outcome, BatchOutcome::Cancelled));
        assert_eq!(result.max_watermark, 900);
        assert!(tokio::fs::read_to_string(&dlq_path)
            .await
            .expect("read dlq")
            .is_empty());
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn partial_failure_routes_only_failed_offsets() {
        use ventstream_core::FailedItem;
        let sink = Arc::new(ScriptedSink::with_result("test", |_| {
            Err(SinkError::Rejected {
                batch_size: 3,
                rejected_count: 1,
                message: "mapper_parsing_exception".into(),
                failed_items: Some(vec![FailedItem {
                    offset: 1, // only the middle event failed
                    error: "mapper_parsing_exception for offset 1".into(),
                }]),
            })
        }));
        let cfg = DispatcherConfig {
            max_events: 3,
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60),
            max_parallel_bulks: 4,
        };
        let events: Vec<Event> = (0..3).map(|i| make_event(&format!("e{i}"))).collect();
        let dlq_path = temp_dlq();
        run_dispatcher(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq_path.clone(),
            cfg,
            events,
            None,
        )
        .await;

        let dlq_contents = tokio::fs::read_to_string(&dlq_path)
            .await
            .expect("read dlq");
        let lines: Vec<&str> = dlq_contents.lines().collect();
        assert_eq!(lines.len(), 1, "only the failed offset should be DLQ-ed");
        // Verify the DLQ record uses the PER-ITEM error string, not the
        // shared `message`. This is the regression test for the
        // sample-error-for-all-rejected-items bug.
        let record: serde_json::Value = serde_json::from_str(lines[0]).expect("dlq json");
        assert_eq!(record["event"]["subject"], "test.e1");
        assert_eq!(
            record["reason"], "mapper_parsing_exception for offset 1",
            "DLQ reason must be the per-item error, not the shared sample"
        );
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    #[tokio::test]
    async fn shutdown_leaves_pending_batch_replayable() {
        let sink = Arc::new(ScriptedSink::always_ok("test"));
        let cfg = DispatcherConfig {
            max_events: 1000, // never reached
            max_batch_bytes: 4 * 1024 * 1024,
            flush_interval: Duration::from_secs(60), // never fires,
            max_parallel_bulks: 4,
        };
        let dlq_path = temp_dlq();
        let dlq = DlqWriter::open(dlq_path.clone()).await.expect("dlq");
        let shutdown = ShutdownToken::new();
        let dispatcher = Dispatcher::new(
            Arc::clone(&sink) as Arc<dyn Sink>,
            dlq,
            cfg,
            shutdown.clone(),
        );

        let (sender, receiver) = bus::channel(64);
        let producer_shutdown = shutdown.clone();
        let producer = tokio::spawn(async move {
            for i in 0..3 {
                let _ = sender
                    .send(make_event(&format!("e{i}")), &producer_shutdown)
                    .await;
            }
            // Hold the sender so the receiver does NOT see None on its own.
            sleep(Duration::from_secs(60)).await;
            drop(sender);
        });

        let dispatcher_handle = tokio::spawn(dispatcher.run(receiver));
        sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let _ = dispatcher_handle.await;
        producer.abort();

        // Explicit shutdown cancels sink work so an unavailable sink cannot
        // block process termination indefinitely. CDC sources replay these
        // events because the source watermark remains pinned.
        let batches = sink.batches();
        let total: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(total, 0, "shutdown must not start a new sink write");
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }
}
