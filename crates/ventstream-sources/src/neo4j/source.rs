//! The [`ventstream_core::Source`] implementation for Neo4j CDC.
//!
//! ### Architecture
//!
//! - Transport: `neo4rs` over Bolt. We open one long-lived `Graph` handle
//!   and reuse it for cursor probes, bootstrap scans, and the live poll.
//! - Cursor: opaque string from `db.cdc.query` / `db.cdc.current`. We
//!   persist it via [`super::cursor::CursorFile`].
//! - Bootstrap: runs at most once per state dir — gated by the presence
//!   of the cursor file. See [`super::bootstrap`].
//! - Tail: a polling loop on the configured interval. Backpressure
//!   flows naturally — when the bus is full, `ctx.sender.send` awaits.
//! - Idle cursor advance: after N consecutive empty polls we refresh
//!   to `db.cdc.current()` so the underlying tx log can rotate without
//!   aging out our cursor.
//!
//! ### Shutdown
//!
//! `tokio::select!` against `ctx.shutdown.cancelled()` at every await
//! point that could otherwise block. Mirrors the PG source's pattern.
//!
//! ### Crash-safety (validated 2026-05-25 via the drill in this file's
//! sibling tests)
//!
//! **Tail crash** (kill -9 during normal operation): on restart the
//! source reads the persisted cursor file and resumes from there.
//! Events that landed AFTER the last cursor write are re-tailed from
//! Neo4j's CDC log. The downstream OS sink upserts by deterministic
//! `ventstream.doc.id` so any re-emission overwrites in place — no
//! duplicates. Validated with rename / create / delete / 2-hop fan-out
//! mutations all replayed cleanly.
//!
//! **Bootstrap crash** (kill -9 before the cursor file exists): on
//! restart the source detects the missing cursor and re-runs bootstrap
//! from scratch. Same deterministic doc_id story → idempotent. The
//! partial OS docs from the killed run get overwritten or supplemented
//! by the fresh bootstrap.
//!
//! **Sink-confirmed cursor advancement.** Cursor file writes are
//! gated on the dispatcher having durably written the corresponding
//! events. Each emitted batch's `(last_tx_id, last_cursor)` pair is
//! held in a small in-memory queue; on each tick we drain pairs whose
//! `tx_id <= sink_progress.load()` and persist the latest drained
//! cursor. A kill -9 between bus-send and sink-confirm leaves the
//! cursor file pointing BEFORE the un-confirmed events — on restart
//! they're re-tailed and re-applied to the (idempotent) OS sink.
//!
//! Falls back to legacy "write cursor immediately after batch" when
//! no sink_progress is attached (single-process tests, etc.).
//!
//! Idle cursor advance fires only when the pending queue is empty —
//! we never advance past un-confirmed events.
//!
//! ### Heartbeat task
//!
//! The 30s "neo4j tail heartbeat" log line is emitted by a separate
//! `tokio::spawn`ed task, not from inside the poll loop's `select!`.
//! This way a heavy event (a Cypher call that takes minutes, a cascade
//! event that processes thousands of recomposes) cannot block the
//! heartbeat — operators always see the binary is alive and what its
//! recent rate has been. Counters are shared via `Arc<HeartbeatCounters>`
//! with atomic fields.

use async_trait::async_trait;
use neo4rs::{query, BoltType, ConfigBuilder, Graph};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};
use ventstream_core::{ShutdownToken, Source, SourceContext, SourceError};

use super::bolt::bolt_to_json;
use super::bootstrap;
use super::config::Neo4jCdcConfig;
use super::cursor::CursorFile;
use super::denormalize;
use super::event_mapper::{self, LiveEventOutcome};
use super::retry;
use crate::error::Neo4jCdcError;
use crate::tls::ensure_crypto_provider;

/// Shared counters between the tail loop and the heartbeat task.
///
/// The tail loop updates these on every poll cycle; the heartbeat
/// task `swap`s them every 30s, emits the summary log line, and
/// resets to zero. Atomics with `Relaxed` ordering are sufficient:
/// we only need eventual-consistent counters for an operator-facing
/// log, not transactional guarantees.
#[derive(Default)]
struct HeartbeatCounters {
    /// Number of poll cycles since the last heartbeat emit.
    poll_cycles: AtomicU64,
    /// Total CDC events observed across those polls.
    events_seen: AtomicU64,
    /// Sum of `poll_once` wall-clock durations (ms) — used to compute
    /// average per-poll latency on emit.
    poll_ms_total: AtomicU64,
    /// Max single-poll latency (ms) observed in the window.
    poll_ms_max: AtomicU64,
    /// Current consecutive empty-poll count. Not reset on emit — it's
    /// a running counter for operator visibility into how long the
    /// source has been idle.
    idle_polls: AtomicU32,
}

/// Long-lived task that emits the heartbeat log line every 30s.
///
/// Spawned by [`Neo4jCdcSource::run_inner`] and lives until its
/// child shutdown token fires (which happens when the engine signals
/// shutdown — the child inherits from the source's shutdown).
async fn run_heartbeat_loop(counters: Arc<HeartbeatCounters>, shutdown: ShutdownToken) {
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(30));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Discard the immediate first tick so the first emit happens 30s
    // after start, not at start.
    let _ = timer.tick().await;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = timer.tick() => {
                let polls = counters.poll_cycles.swap(0, Ordering::Relaxed);
                let events = counters.events_seen.swap(0, Ordering::Relaxed);
                let poll_ms_total = counters.poll_ms_total.swap(0, Ordering::Relaxed);
                let max_poll_ms = counters.poll_ms_max.swap(0, Ordering::Relaxed);
                let idle_polls = counters.idle_polls.load(Ordering::Relaxed);
                let avg_poll_ms = if polls > 0 { poll_ms_total / polls } else { 0 };
                info!(
                    polls,
                    events,
                    avg_poll_ms,
                    max_poll_ms,
                    idle_polls,
                    metric = "neo4j.tail.summary",
                    "neo4j tail heartbeat (30s window)"
                );
            }
        }
    }
}

/// Source adapter for a Neo4j CDC stream.
pub struct Neo4jCdcSource {
    config: Neo4jCdcConfig,
    /// Sink-confirmed tx_id watermark. When `Some`, the source defers
    /// cursor-file writes — only advancing past events whose tx_id has
    /// been durably written by the sink. When `None`, the source falls
    /// back to "write cursor after bus publish" (legacy behavior, not
    /// crash-safe; retained for single-process tests).
    sink_progress: Option<Arc<AtomicU64>>,
}

impl Neo4jCdcSource {
    /// Construct a new source. Bolt connection is established lazily
    /// inside `run`.
    pub fn new(config: Neo4jCdcConfig) -> Self {
        Self {
            config,
            sink_progress: None,
        }
    }

    /// Attach a sink-progress watermark. Once set, the source uses
    /// this Arc to gate cursor file writes — pairs `(tx_id, cursor)`
    /// stay buffered in memory until the sink reports the
    /// corresponding tx_id has been durably written. Used by the
    /// engine to wire crash-safety in.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    async fn connect(&self) -> Result<Graph, Neo4jCdcError> {
        ensure_crypto_provider();
        let mut builder = ConfigBuilder::default()
            .uri(self.config.uri.as_str())
            .user(self.config.user.as_str())
            .password(self.config.password.as_str())
            .db(self.config.database.as_str());
        // Despite the name, `with_client_certificate` in neo4rs 0.8
        // ADDS the supplied PEM to the TLS root trust store — it's not
        // about mutual-TLS client auth. So this is exactly the hook we
        // want for trusting private-CA / self-signed server certs.
        if let Some(p) = &self.config.trust_cert_file {
            builder = builder.with_client_certificate(p);
        }
        let cfg = builder
            .build()
            .map_err(|err| Neo4jCdcError::Connection(format!("neo4rs config: {err}")))?;
        Graph::connect(cfg)
            .await
            .map_err(|err| Neo4jCdcError::Connection(err.to_string()))
    }

    async fn run_inner(&self, ctx: SourceContext) -> Result<(), Neo4jCdcError> {
        let cursor_file = CursorFile::new(&self.config.state_dir)?;
        info!(
            cursor_path = %cursor_file.path().display(),
            "neo4j cdc source starting"
        );

        let graph = self.connect().await?;
        info!(uri = %self.config.uri, database = %self.config.database, "neo4j connected");

        // Validate denormalize specs against the live graph BEFORE we
        // start serving CDC events. Catches syntax errors, missing
        // procedures, and write-keywords here rather than at the
        // first event (~3 hours later, when on-call is asleep).
        //
        // Validation now ALSO probes each anchor path for low-
        // cardinality endpoints — the per-spec eid sets returned here
        // feed the tail loop's hot-node filter. See `hot_endpoints`.
        let hot_endpoints: Vec<super::hot_endpoints::SpecHotEndpoints> =
            if let Some(specs) = self.config.denormalize.as_ref().filter(|s| !s.is_empty()) {
                let h = denormalize::validate_specs(
                    &graph,
                    specs,
                    self.config.projection_fan_out,
                    self.config.hot_node_threshold,
                )
                .await?;
                info!(specs = specs.len(), "neo4j denormalize specs validated");
                h
            } else {
                Vec::new()
            };
        // Precompute each spec's affected-primary selector once. The selector
        // returns only element IDs; full document projections run later in
        // bounded chunks so a large cascade cannot make one Neo4j transaction
        // construct every nested document at once.
        let fan_out_cyphers: Vec<String> = self
            .config
            .denormalize
            .as_ref()
            .map(|specs| {
                specs
                    .denormalize
                    .iter()
                    .map(|spec| {
                        denormalize::build_fan_out_selector_cypher_with_config(
                            spec,
                            self.config.projection_fan_out,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Counters shared with the heartbeat task spawned below. The
        // heartbeat task ticks every 30s independently of the tail
        // loop, so a long-running poll/cascade event can't starve the
        // operator-facing log line.
        let counters = Arc::new(HeartbeatCounters::default());
        tokio::spawn(run_heartbeat_loop(
            Arc::clone(&counters),
            ctx.shutdown.child(),
        ));

        // Auto-heal loop. A persisted cursor can be rejected by Neo4j
        // (`ChangeDataCapture.InvalidIdentifier`) when the source was
        // restored/recreated, CDC was re-enabled, or the pod was down
        // longer than the transaction-log retention. Rather than failing
        // fatally (→ k8s CrashLoopBackOff that never recovers), wipe the
        // cursor and re-bootstrap. Capped so a persistent failure still
        // surfaces instead of looping forever.
        const MAX_CURSOR_RECOVERIES: u32 = 5;
        let mut recoveries: u32 = 0;
        loop {
            let initial_cursor = self.resolve_cursor(&graph, &cursor_file, &ctx).await?;

            // Bootstrap (if any) has finished by here and we're about to
            // enter the tail loop. Mirror the PG source's transition so
            // the control plane sees `tailing` as the steady state.
            ventstream_telemetry::clear_error();
            ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Tailing);

            match self
                .tail_loop(
                    &graph,
                    &cursor_file,
                    initial_cursor,
                    &ctx,
                    Arc::clone(&counters),
                    &hot_endpoints,
                    &fan_out_cyphers,
                )
                .await
            {
                Ok(()) => return Ok(()), // graceful shutdown
                Err(err) if is_invalid_cursor(&err) => {
                    recoveries += 1;
                    if recoveries > MAX_CURSOR_RECOVERIES {
                        return Err(Neo4jCdcError::Internal(format!(
                            "cursor recovery exhausted after {MAX_CURSOR_RECOVERIES} attempts: {err}"
                        )));
                    }
                    warn!(
                        metric = "cursor.recover",
                        source = "neo4j",
                        attempt = recoveries,
                        error = %err,
                        "neo4j CDC cursor invalid/expired; wiping cursor and re-bootstrapping (auto-heal)"
                    );
                    metrics::counter!("vs_cursor_recover_total", "source" => "neo4j").increment(1);
                    cursor_file.delete()?;
                    // Loop: resolve_cursor now finds no cursor → re-bootstrap.
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Resolve the cursor to start tailing from: warm start reuses the
    /// persisted cursor; cold start (no cursor file) bootstraps and
    /// persists a fresh one. Called once on startup and again on each
    /// auto-heal after the stale cursor has been deleted.
    async fn resolve_cursor(
        &self,
        graph: &Graph,
        cursor_file: &CursorFile,
        ctx: &SourceContext,
    ) -> Result<String, Neo4jCdcError> {
        // A persisted cursor is only trustworthy if the prior bootstrap's
        // snapshot was confirmed sink-durable. The incomplete sentinel means
        // it was NOT (crash between bootstrap and sink confirmation) — resuming
        // would skip bootstrap and permanently lose any unmutated nodes whose
        // snapshot events never reached the sink. Re-bootstrap instead; it's
        // idempotent (stable doc-id upserts).
        if cursor_file.is_incomplete() {
            warn!(
                "neo4j bootstrap-incomplete sentinel present — prior snapshot was not \
                 sink-confirmed; discarding cursor and re-bootstrapping"
            );
            cursor_file.delete()?;
        } else if let Some(c) = cursor_file.read()? {
            info!(
                cursor_prefix = %&c[..c.len().min(30)],
                "neo4j warm start — resuming"
            );
            return Ok(c);
        }

        // Cold start (no cursor, or the sentinel forced a re-bootstrap).
        Ok(match (&self.config.denormalize, &self.config.bootstrap) {
            (Some(specs), _) if !specs.is_empty() => {
                // Denormalize mode skips the standard per-label scan and
                // runs each spec's user Cypher over every matching primary
                // instead. It emits no snapshot-COMPLETE *event*: in
                // denormalize mode the join engine isn't wired so nothing
                // consumes it, and routing one to OS via the standard index
                // template would render to an OS-reserved `_`-prefixed name
                // and DLQ the whole bulk batch. (Unrelated to the
                // bootstrap-incomplete *sentinel file* marked just below —
                // that's durability gating, not join state-dump signalling.)
                info!(
                    specs = specs.len(),
                    "neo4j cold start (denormalize mode — multi-spec)"
                );
                ventstream_telemetry::set_phase(
                    ventstream_telemetry::LifecyclePhase::Bootstrapping,
                );
                // Mark BEFORE the scan so a crash mid-snapshot forces a
                // re-bootstrap. Cleared once the tail loop sees the sink
                // confirm past the snapshot (see `tail_loop`).
                cursor_file.mark_incomplete()?;
                let c = denormalize::run_bootstrap_all(graph, &self.config, specs, ctx).await?;
                cursor_file.write(&c)?;
                self.clear_incomplete_if_ungated(cursor_file)?;
                c
            }
            // Either denormalize is unset OR it's set-but-empty
            // (degenerate config). In both cases fall through to
            // the standard scan.
            (_, Some(_)) => {
                ventstream_telemetry::set_phase(
                    ventstream_telemetry::LifecyclePhase::Bootstrapping,
                );
                cursor_file.mark_incomplete()?;
                let c = bootstrap::run_snapshot(graph, &self.config, ctx).await?;
                cursor_file.write(&c)?;
                self.clear_incomplete_if_ungated(cursor_file)?;
                c
            }
            (_, None) => {
                // Bootstrap disabled — no snapshot events are emitted, only
                // the resume point is seeded. Nothing durable to lose, so no
                // sentinel.
                let c = bootstrap::fetch_current_cursor(graph).await?;
                cursor_file.write(&c)?;
                info!(
                    cursor_prefix = %&c[..c.len().min(30)],
                    "neo4j cold start (bootstrap disabled) — seeded from db.cdc.current"
                );
                c
            }
        })
    }

    /// When no `sink_progress` watermark is wired, nothing in the tail loop
    /// will ever confirm the snapshot, so the sentinel would force an infinite
    /// re-bootstrap. In that (legacy / no-gating) mode there is no durability
    /// guarantee anyway, so clear it immediately — matching pre-fix behavior.
    /// With gating wired, leave it for `tail_loop` to clear on first confirm.
    fn clear_incomplete_if_ungated(&self, cursor_file: &CursorFile) -> Result<(), Neo4jCdcError> {
        if self.sink_progress.is_none() {
            cursor_file.clear_incomplete()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // cohesive tail context; see runner refactor
    async fn tail_loop(
        &self,
        graph: &Graph,
        cursor_file: &CursorFile,
        mut cursor: String,
        ctx: &SourceContext,
        counters: Arc<HeartbeatCounters>,
        hot_endpoints: &[super::hot_endpoints::SpecHotEndpoints],
        fan_out_cyphers: &[String],
    ) -> Result<(), Neo4jCdcError> {
        let mut poll_timer = tokio::time::interval(self.config.poll_interval);
        poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let _ = poll_timer.tick().await; // discard immediate first tick

        // Pending (tx_id, cursor) pairs awaiting sink confirmation. One
        // entry per emitted batch. Drained on each tick where
        // `sink_progress.load() >= tx_id`. When `self.sink_progress` is
        // None the queue stays empty — the legacy in-line cursor write
        // below handles those cases.
        let mut pending_cursors: VecDeque<(u64, String)> = VecDeque::new();

        // Monotonic cycle counter, useful for log correlation. Every
        // log line emitted from within a poll cycle inherits the
        // `cycle` field via the surrounding span, so an operator can
        // grep "cycle=42" to see every breadcrumb from that one poll.
        let mut cycle: u64 = 0;

        // The bootstrap-incomplete sentinel is cleared the first time the
        // sink confirms a tail cursor. Tail events are published AFTER all
        // bootstrap events on the same ordered bus, so a confirmed tail tx_id
        // implies every earlier bootstrap event is sink-durable — the precise
        // moment the snapshot is safe to resume past. (See `resolve_cursor`.)
        let mut bootstrap_confirmed = false;

        // Highest tx_id for which we've actually emitted ≥1 sink event. A
        // batch that emits nothing (all events filtered / recompose matched no
        // primary) gates its cursor on THIS value rather than on its own
        // last_tx_id — which the sink could never confirm — so the cursor
        // never stalls (M13).
        let mut last_emitted_tx: u64 = 0;

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    info!("neo4j cdc shutdown requested");
                    return Ok(());
                }
                _ = poll_timer.tick() => {
                    cycle += 1;
                    let poll_started = std::time::Instant::now();
                    debug!(
                        cycle,
                        cursor_prefix = %&cursor[..cursor.len().min(24)],
                        "poll cycle start"
                    );
                    // M12: if the NEXT empty poll would trigger an idle cursor
                    // advance, snapshot `current()` NOW — BEFORE the poll — and
                    // advance to THAT on an empty poll, never to a value read
                    // after the poll. A txn committing between the poll read and
                    // a post-poll `current()` would be skipped (the poll didn't
                    // see it, yet we'd advance the cursor past it → data loss).
                    // The pre-poll snapshot is ≤ what the poll covered, so
                    // advancing to it is gap-free, and the change lands next
                    // poll. Only fetched when an advance is imminent, so a busy
                    // (non-idle) DB pays nothing.
                    let pre_poll_cursor: Option<String> = {
                        let would_idle_advance = idle_advance_due(
                            counters.idle_polls.load(Ordering::Relaxed).saturating_add(1),
                            self.config.idle_advance_after_polls,
                            pending_cursors.is_empty(),
                        );
                        if would_idle_advance {
                            Some(
                                retry::with_backoff("fetch_current_cursor", || {
                                    bootstrap::fetch_current_cursor(graph)
                                })
                                .await?,
                            )
                        } else {
                            None
                        }
                    };
                    // Wrap every Neo4j call in bounded exponential backoff.
                    // A brief restart / cluster failover should not cascade
                    // into engine shutdown — restart-as-recovery costs
                    // pod-startup time + bootstrap re-validation.
                    let events = retry::with_backoff("poll_cdc_query", || poll_once(graph, &cursor)).await?;
                    let poll_elapsed_ms = poll_started.elapsed().as_millis() as u64;
                    counters.poll_cycles.fetch_add(1, Ordering::Relaxed);
                    counters.poll_ms_total.fetch_add(poll_elapsed_ms, Ordering::Relaxed);
                    counters.poll_ms_max.fetch_max(poll_elapsed_ms, Ordering::Relaxed);
                    counters.events_seen.fetch_add(events.len() as u64, Ordering::Relaxed);
                    // Drain pending cursors that the sink has confirmed.
                    // Even on an empty poll we want to flush the gate so
                    // a quiet period doesn't leave the cursor file behind.
                    if self.drain_confirmed_cursors(
                        &mut pending_cursors,
                        cursor_file,
                        &mut cursor,
                    )? && !bootstrap_confirmed {
                        cursor_file.clear_incomplete()?;
                        bootstrap_confirmed = true;
                    }

                    if events.is_empty() {
                        // fetch_add returns the OLD value — add 1 to
                        // get the post-increment for the modulo check.
                        let new_idle = counters
                            .idle_polls
                            .fetch_add(1, Ordering::Relaxed)
                            .saturating_add(1);
                        if idle_advance_due(
                            new_idle,
                            self.config.idle_advance_after_polls,
                            pending_cursors.is_empty(),
                        ) {
                            // Advance to the cursor snapshotted BEFORE this
                            // (empty) poll (M12). The poll from `cursor`
                            // returned nothing, so there are no changes between
                            // `cursor` and `pre_poll_cursor` — advancing there
                            // is gap-free and lets the tx log rotate without
                            // aging out our resume point. Only when no in-flight
                            // events are pending (else we'd advance past
                            // un-confirmed work). `pre_poll_cursor` is Some
                            // exactly when this branch can fire.
                            if let Some(now_cursor) = pre_poll_cursor {
                                if now_cursor != cursor {
                                    debug!(
                                        cursor_prefix = %&now_cursor[..now_cursor.len().min(30)],
                                        "neo4j idle cursor advance"
                                    );
                                    cursor_file.write(&now_cursor)?;
                                    cursor = now_cursor;
                                }
                            }
                        }
                        continue;
                    }
                    counters.idle_polls.store(0, Ordering::Relaxed);

                    // Publish each event in order. `last_id` becomes the new
                    // cursor; the gate tx is the highest tx we actually EMITTED
                    // this batch (`batch_emit_tx`) — NOT `last_tx_id`, which a
                    // no-emit batch could never get confirmed (M13).
                    let last_id = events.last().map(|e| e.id.clone());
                    let last_tx_id = events.last().map_or(0, |e| e.tx_id) as u64;
                    let batch_emit_tx: Option<u64> = if let Some(specs) =
                        self.config.denormalize.as_ref().filter(|s| !s.is_empty())
                    {
                        // Denormalize path: batch the whole poll's events into
                        // ONE recompose query per spec (CDC is post-commit, so
                        // every recompose reads the same current graph — N
                        // events touching one primary would otherwise run N
                        // identical queries). The helper extracts elementId /
                        // labels / endpoints, unions the anchors, and emits OS
                        // deletes for primary node removals. Wrapped in retry so
                        // a Neo4j blip during the batch doesn't kill the engine
                        // (re-emit is idempotent).
                        let batch: Vec<(&Value, i64)> =
                            events.iter().map(|e| (&e.event_json, e.tx_id)).collect();
                        let emitted = retry::with_backoff("denormalize_handle_batch", || {
                            denormalize::handle_tail_events_all(
                                graph,
                                &self.config,
                                specs,
                                hot_endpoints,
                                fan_out_cyphers,
                                &batch,
                                ctx,
                            )
                        })
                        .await?;
                        // Recompose/delete events are stamped with the batch's
                        // last tx_id, so if anything was emitted that's the tx
                        // sink_progress will reach.
                        (emitted > 0).then_some(last_tx_id)
                    } else {
                        let mut max_emit_tx: Option<u64> = None;
                        for ev in &events {
                            match event_mapper::live_event(&self.config, ev.tx_id, &ev.event_json) {
                                Ok(LiveEventOutcome::Emit(event)) => {
                                    ctx.sender.send(event, &ctx.shutdown).await.map_err(|err| {
                                        Neo4jCdcError::Internal(format!("publish failed: {err}"))
                                    })?;
                                    ventstream_telemetry::bump_events_emitted(1);
                                    let tx = ev.tx_id as u64;
                                    max_emit_tx = Some(max_emit_tx.map_or(tx, |m: u64| m.max(tx)));
                                }
                                Ok(LiveEventOutcome::Filtered) => {
                                    debug!(tx_id = ev.tx_id, "neo4j event filtered by label/reltype filter");
                                }
                                // A malformed event must NOT be fatal: propagating
                                // it leaves the cursor un-advanced, so the same bad
                                // event re-fails on every restart → permanent crash
                                // loop. Skip-and-log (the cursor advances past it),
                                // matching the denormalize path's defensiveness.
                                Err(Neo4jCdcError::MalformedEvent(msg)) => {
                                    warn!(
                                        tx_id = ev.tx_id,
                                        error = %msg,
                                        "neo4j malformed tail event — skipping (cursor advances past it)"
                                    );
                                    ventstream_telemetry::record_error(format!(
                                        "neo4j malformed tail event skipped: {msg}"
                                    ));
                                }
                                Err(other) => return Err(other),
                            }
                        }
                        max_emit_tx
                    };
                    if let Some(id) = last_id {
                        if self.sink_progress.is_some() {
                            // Gate path: queue the cursor until the sink confirms
                            // the gate tx. A batch that emitted something raises
                            // and gates on its emitted tx; a batch that emitted
                            // NOTHING rides `last_emitted_tx` (the prior confirmed
                            // watermark) so it drains with already-sent work
                            // instead of waiting forever on an unreachable tx_id.
                            if let Some(t) = batch_emit_tx {
                                last_emitted_tx = last_emitted_tx.max(t);
                            }
                            let gate = gate_tx_for_batch(batch_emit_tx, last_emitted_tx);
                            pending_cursors.push_back((gate, id));
                            if self.drain_confirmed_cursors(
                                &mut pending_cursors,
                                cursor_file,
                                &mut cursor,
                            )? && !bootstrap_confirmed {
                                cursor_file.clear_incomplete()?;
                                bootstrap_confirmed = true;
                            }
                        } else {
                            // Legacy path: no gating wired. Write immediately on
                            // bus-publish success.
                            cursor_file.write(&id)?;
                            cursor = id;
                        }
                    }
                }
            }
        }
    }

    /// Method-shaped wrapper so the tail loop can call without
    /// passing `sink_progress` explicitly. Forwards to the free
    /// function below — that's the directly testable form.
    fn drain_confirmed_cursors(
        &self,
        pending: &mut VecDeque<(u64, String)>,
        cursor_file: &CursorFile,
        cursor: &mut String,
    ) -> Result<bool, Neo4jCdcError> {
        drain_confirmed_cursors(self.sink_progress.as_ref(), pending, cursor_file, cursor)
    }
}

/// Whether the idle cursor advance should fire this cycle (M12). `projected`
/// is the idle-poll count *including* the current poll. Advancing requires the
/// count to land on the configured cadence AND no in-flight events pending
/// (else we'd advance past un-confirmed work). `advance_after == 0` disables
/// the advance. Pure so the gate is unit-testable.
fn idle_advance_due(projected: u32, advance_after: u32, pending_empty: bool) -> bool {
    advance_after != 0 && projected.is_multiple_of(advance_after) && pending_empty
}

/// The cursor's gate tx for a tail batch (M13). A batch that emitted ≥1 sink
/// event gates on that emitted tx; a batch that emitted NOTHING rides
/// `last_emitted_tx` (the prior confirmed watermark) so its cursor drains with
/// already-sent work instead of waiting forever on a tx the sink can't confirm.
fn gate_tx_for_batch(batch_emit_tx: Option<u64>, last_emitted_tx: u64) -> u64 {
    batch_emit_tx.unwrap_or(last_emitted_tx)
}

/// Drain `pending` from the front while the leading tx_id is
/// confirmed by `sink_progress`. Persists the latest drained cursor
/// once per call. No-op when `sink_progress` is None or nothing is
/// confirmed.
///
/// Free-function form so unit tests can call this without standing
/// up a full [`Neo4jCdcSource`].
fn drain_confirmed_cursors(
    sink_progress: Option<&Arc<AtomicU64>>,
    pending: &mut VecDeque<(u64, String)>,
    cursor_file: &CursorFile,
    cursor: &mut String,
) -> Result<bool, Neo4jCdcError> {
    let Some(progress) = sink_progress else {
        return Ok(false);
    };
    let confirmed = progress.load(Ordering::Relaxed);
    let pending_front_tx = pending.front().map(|(t, _)| *t);
    let mut latest_safe: Option<String> = None;
    while let Some((tid, _)) = pending.front() {
        if *tid <= confirmed {
            // front() returned Some above, so pop_front() can't be None;
            // the else arm is unreachable but avoids an expect().
            let Some((_, c)) = pending.pop_front() else {
                break;
            };
            latest_safe = Some(c);
        } else {
            break;
        }
    }
    if let Some(c) = &latest_safe {
        cursor_file.write(c)?;
        *cursor = c.clone();
    }
    debug!(
        sink_progress = confirmed,
        pending_front_tx,
        pending_after = pending.len(),
        cursor_advanced = latest_safe.is_some(),
        "neo4j cursor drain"
    );
    Ok(latest_safe.is_some())
}

#[async_trait]
impl Source for Neo4jCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "neo4j_cdc"
    }

    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
        self.run_inner(ctx).await.map_err(|err| match err {
            Neo4jCdcError::Connection(msg) => SourceError::Connection(msg),
            other => SourceError::Internal(other.to_string()),
        })
    }
}

/// One row from `db.cdc.query`. Lightweight — just what the event
/// mapper needs.
#[derive(Debug)]
struct CdcRow {
    /// Opaque cursor id of THIS event — used to resume past it.
    id: String,
    /// Monotonic transaction id within the database.
    tx_id: i64,
    /// `event` field of the row, already JSON-converted.
    event_json: Value,
}

/// Does this error mean the persisted CDC cursor is no longer valid for
/// the current database? Neo4j returns
/// `Neo.ClientError.ChangeDataCapture.InvalidIdentifier` (surfaced through
/// `db.cdc.query`) when the cursor was issued by a different DB instance,
/// CDC was re-enabled, or the change-id aged out of the transaction logs.
/// The auto-heal path wipes the cursor and re-bootstraps on this.
fn is_invalid_cursor(err: &Neo4jCdcError) -> bool {
    matches!(err, Neo4jCdcError::Query(msg) if msg.contains("InvalidIdentifier"))
}

async fn poll_once(graph: &Graph, cursor: &str) -> Result<Vec<CdcRow>, Neo4jCdcError> {
    let started = std::time::Instant::now();
    let mut rows = graph
        .execute(
            query(
                "CALL db.cdc.query($cursor) YIELD id, txId, seq, event, metadata \
                 RETURN id, txId, seq, event",
            )
            .param("cursor", cursor),
        )
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.cdc.query: {err}")))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.cdc.query iter: {err}")))?
    {
        let id = row
            .get::<String>("id")
            .map_err(|err| Neo4jCdcError::Query(format!("row.id: {err}")))?;
        let tx_id = row
            .get::<i64>("txId")
            .map_err(|err| Neo4jCdcError::Query(format!("row.txId: {err}")))?;
        let event_bolt: BoltType = row
            .get("event")
            .map_err(|err| Neo4jCdcError::Query(format!("row.event: {err}")))?;
        let event_json = bolt_to_json(&event_bolt);
        out.push(CdcRow {
            id,
            tx_id,
            event_json,
        });
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    // Logged at DEBUG so the volume isn't a problem in production but
    // is invaluable when chasing "why are events slow to land?" — the
    // first thing you check is whether poll_once itself is taking long.
    debug!(
        cursor_prefix = %&cursor[..cursor.len().min(24)],
        rows = out.len(),
        elapsed_ms,
        metric = "neo4j.poll.cypher",
        "db.cdc.query returned"
    );
    Ok(out)
}

// Real integration testing for this source happens against a live
// Neo4j Enterprise container — the binary is exercised end-to-end with
// crash and resume drills (see `examples/neo4j-cdc-spike` for the
// underlying mechanics and `docs/` for the run script). A unit-level
// "fails on bad connect" test was tried; `neo4rs` has long internal
// retry backoff making it impractical without bespoke connect-timeout
// plumbing that doesn't exist in the v0.8 crate.

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn live_neo4j_source() -> Neo4jCdcSource {
        let uri = std::env::var("VENTSTREAM_TEST_NEO4J_URI").expect("VENTSTREAM_TEST_NEO4J_URI");
        let user = std::env::var("VENTSTREAM_TEST_NEO4J_USER").expect("VENTSTREAM_TEST_NEO4J_USER");
        let password = std::env::var("VENTSTREAM_TEST_NEO4J_PASSWORD")
            .expect("VENTSTREAM_TEST_NEO4J_PASSWORD");
        let database =
            std::env::var("VENTSTREAM_TEST_NEO4J_DATABASE").unwrap_or_else(|_| "neo4j".to_owned());
        let mut config = Neo4jCdcConfig::new(
            "neo4j-tls-test",
            uri,
            user,
            password,
            database,
            tmp_state_dir(),
        );
        if let Ok(path) = std::env::var("VENTSTREAM_TEST_NEO4J_CA_FILE") {
            config.trust_cert_file = Some(path.into());
        }
        Neo4jCdcSource::new(config)
    }

    async fn probe_neo4j(source: &Neo4jCdcSource) -> Result<(), Neo4jCdcError> {
        let graph = tokio::time::timeout(std::time::Duration::from_secs(30), source.connect())
            .await
            .map_err(|_| Neo4jCdcError::Connection("Neo4j TLS connection timed out".into()))??;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            graph.run(query("RETURN 1 AS ok")),
        )
        .await
        .map_err(|_| Neo4jCdcError::Connection("Neo4j TLS probe timed out".into()))?;
        result.map_err(|err| Neo4jCdcError::Connection(err.to_string()))
    }

    #[tokio::test]
    #[ignore = "requires VENTSTREAM_TEST_NEO4J_URI, user, password, and optional database/CA"]
    async fn strict_tls_connects_to_a_trusted_neo4j_server() {
        let source = live_neo4j_source();
        assert!(
            source.config.uri.starts_with("neo4j+s://")
                || source.config.uri.starts_with("bolt+s://"),
            "live TLS test requires a strict +s URI"
        );
        probe_neo4j(&source)
            .await
            .expect("strict Neo4j TLS connection");
    }

    #[tokio::test]
    #[ignore = "requires a wrong VENTSTREAM_TEST_NEO4J_CA_FILE"]
    async fn strict_tls_rejects_an_untrusted_neo4j_ca() {
        let source = live_neo4j_source();
        assert!(
            probe_neo4j(&source).await.is_err(),
            "an unrelated CA must be rejected"
        );
    }

    #[tokio::test]
    #[ignore = "requires a hostname-mismatched VENTSTREAM_TEST_NEO4J_URI"]
    async fn strict_tls_rejects_a_neo4j_hostname_mismatch() {
        let source = live_neo4j_source();
        assert!(
            probe_neo4j(&source).await.is_err(),
            "a hostname mismatch must be rejected"
        );
    }

    #[test]
    fn invalid_cursor_classification() {
        // The Neo4j change-id rejection → auto-heal.
        assert!(is_invalid_cursor(&Neo4jCdcError::Query(
            "db.cdc.query iter: ... Neo.ClientError.ChangeDataCapture.InvalidIdentifier".into()
        )));
        // Other query errors must NOT trigger a wipe + re-bootstrap.
        assert!(!is_invalid_cursor(&Neo4jCdcError::Query(
            "db.cdc.query iter: connection reset".into()
        )));
        assert!(!is_invalid_cursor(&Neo4jCdcError::Connection(
            "InvalidIdentifier".into()
        )));
    }

    fn tmp_state_dir() -> std::path::PathBuf {
        // Each test gets its own dir via the per-test counter so
        // tests can run in parallel without stomping on each other.
        use std::sync::atomic::AtomicU64;
        static N: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "vs-neo4j-cursor-{}-{}-{}",
            std::process::id(),
            nanos,
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("mkdir tmp state");
        dir
    }

    #[test]
    fn drain_with_no_sink_progress_is_noop() {
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        cf.write("initial").expect("initial write");
        let mut pending: VecDeque<(u64, String)> =
            VecDeque::from([(10, "c10".to_owned()), (20, "c20".to_owned())]);
        let mut cursor = "initial".to_owned();
        drain_confirmed_cursors(None, &mut pending, &cf, &mut cursor).expect("ok");
        // Nothing drained; cursor file unchanged.
        assert_eq!(pending.len(), 2);
        assert_eq!(cursor, "initial");
        assert_eq!(cf.read().unwrap().as_deref(), Some("initial"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn drain_advances_to_highest_confirmed_cursor() {
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        cf.write("c0").expect("initial write");
        let progress = Arc::new(AtomicU64::new(20)); // sink confirmed up to 20
        let mut pending: VecDeque<(u64, String)> = VecDeque::from([
            (10, "c10".to_owned()),
            (20, "c20".to_owned()),
            (30, "c30".to_owned()),
        ]);
        let mut cursor = "c0".to_owned();
        drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok");
        // c10 and c20 drained; c30 stays pending.
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.front().unwrap().0, 30);
        // Cursor advanced to the LATEST confirmed, not the first.
        assert_eq!(cursor, "c20");
        assert_eq!(cf.read().unwrap().as_deref(), Some("c20"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn drain_when_nothing_confirmed_keeps_cursor_unchanged() {
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        cf.write("c0").expect("initial write");
        let progress = Arc::new(AtomicU64::new(5)); // confirmed below pending front
        let mut pending: VecDeque<(u64, String)> =
            VecDeque::from([(10, "c10".to_owned()), (20, "c20".to_owned())]);
        let mut cursor = "c0".to_owned();
        drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok");
        assert_eq!(pending.len(), 2);
        assert_eq!(cursor, "c0");
        assert_eq!(cf.read().unwrap().as_deref(), Some("c0"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn drain_writes_file_only_once_per_call() {
        // Important property — we want to avoid one fsync per drained
        // entry. Persist only the latest after the loop terminates.
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        let progress = Arc::new(AtomicU64::new(100));
        let mut pending: VecDeque<(u64, String)> = VecDeque::from(
            (1..=50)
                .map(|i| (i as u64, format!("c{}", i)))
                .collect::<Vec<_>>(),
        );
        let mut cursor = "c0".to_owned();
        drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok");
        assert!(pending.is_empty());
        assert_eq!(cursor, "c50");
        assert_eq!(cf.read().unwrap().as_deref(), Some("c50"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn drain_reports_whether_it_advanced() {
        // The bool return is what clears the bootstrap-incomplete sentinel on
        // first confirmation, so its semantics matter: true only when a cursor
        // was actually persisted this call.
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        cf.write("c0").expect("initial write");
        let progress = Arc::new(AtomicU64::new(5));
        let mut pending: VecDeque<(u64, String)> = VecDeque::from([(10, "c10".to_owned())]);
        let mut cursor = "c0".to_owned();
        // Front (10) not yet confirmed (5) -> no advance.
        assert!(
            !drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok"),
            "nothing confirmed -> false"
        );
        // Confirm it -> advances.
        progress.store(10, Ordering::Relaxed);
        assert!(
            drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok"),
            "newly confirmed -> true"
        );
        // Queue now empty -> no further advance.
        assert!(
            !drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok"),
            "already drained -> false"
        );
        // No sink_progress wired -> never advances.
        assert!(
            !drain_confirmed_cursors(None, &mut pending, &cf, &mut cursor).expect("ok"),
            "ungated -> false"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn idle_advance_due_gate() {
        // M12: fires only when the projected idle count hits the cadence AND
        // no events are pending.
        assert!(idle_advance_due(20, 20, true), "on cadence + empty -> due");
        assert!(idle_advance_due(40, 20, true), "multiple of cadence -> due");
        assert!(!idle_advance_due(19, 20, true), "off cadence -> not due");
        assert!(
            !idle_advance_due(20, 20, false),
            "pending in flight -> not due (don't advance past un-confirmed work)"
        );
        assert!(
            !idle_advance_due(20, 0, true),
            "advance disabled -> never due"
        );
    }

    #[test]
    fn gate_tx_picks_emitted_or_prior_watermark() {
        // M13: a batch that emitted something gates on that tx; a no-emit batch
        // rides the prior confirmed watermark.
        assert_eq!(gate_tx_for_batch(Some(42), 10), 42, "emitted -> its tx");
        assert_eq!(
            gate_tx_for_batch(None, 10),
            10,
            "no-emit -> prior watermark, not the batch's last_tx_id"
        );
        assert_eq!(
            gate_tx_for_batch(None, 0),
            0,
            "first-ever no-emit -> 0, which drains immediately (nothing un-durable)"
        );
    }

    #[test]
    fn no_emit_batch_cursor_drains_on_prior_watermark_confirm() {
        // M13 end-to-end at the gate+drain seam: a no-emit batch pushes
        // (last_emitted_tx, id); it must NOT drain before prior emitted work is
        // confirmed, and MUST drain once sink_progress reaches that watermark —
        // never stalling on the batch's own (unreachable) last_tx_id.
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        cf.write("c0").expect("initial write");
        let progress = Arc::new(AtomicU64::new(0));
        let last_emitted_tx = 50u64; // prior batch emitted up to tx 50, not yet confirmed
        let mut pending: VecDeque<(u64, String)> = VecDeque::new();
        // No-emit batch (batch_emit_tx = None) at a much higher last_tx_id (say
        // 90) — but it gates on last_emitted_tx (50), not 90.
        let gate = gate_tx_for_batch(None, last_emitted_tx);
        assert_eq!(gate, 50);
        pending.push_back((gate, "c-noemit".to_owned()));
        let mut cursor = "c0".to_owned();
        // Prior work not yet confirmed (progress 0) -> cursor stays put.
        assert!(
            !drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok")
        );
        assert_eq!(cursor, "c0");
        // Prior emitted work confirms (sink_progress reaches 50) -> drains.
        progress.store(50, Ordering::Relaxed);
        assert!(
            drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok")
        );
        assert_eq!(
            cursor, "c-noemit",
            "no-emit cursor advances once prior work durable"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn heartbeat_counters_swap_resets_to_zero() {
        // Verifies the heartbeat task's swap-on-emit semantics: after
        // a swap, the counter is zero, and subsequent fetch_add
        // accumulates the next window's samples cleanly.
        let c = Arc::new(HeartbeatCounters::default());
        c.poll_cycles.fetch_add(10, Ordering::Relaxed);
        c.events_seen.fetch_add(123, Ordering::Relaxed);
        c.poll_ms_total.fetch_add(500, Ordering::Relaxed);
        c.poll_ms_max.fetch_max(75, Ordering::Relaxed);

        // Simulate the heartbeat task emitting + resetting.
        let polls = c.poll_cycles.swap(0, Ordering::Relaxed);
        let events = c.events_seen.swap(0, Ordering::Relaxed);
        let poll_ms = c.poll_ms_total.swap(0, Ordering::Relaxed);
        let max = c.poll_ms_max.swap(0, Ordering::Relaxed);
        assert_eq!(polls, 10);
        assert_eq!(events, 123);
        assert_eq!(poll_ms, 500);
        assert_eq!(max, 75);

        // After swap, the counters are back to zero.
        assert_eq!(c.poll_cycles.load(Ordering::Relaxed), 0);
        assert_eq!(c.events_seen.load(Ordering::Relaxed), 0);
        assert_eq!(c.poll_ms_total.load(Ordering::Relaxed), 0);
        assert_eq!(c.poll_ms_max.load(Ordering::Relaxed), 0);

        // Next window accumulates from zero.
        c.poll_cycles.fetch_add(3, Ordering::Relaxed);
        c.poll_ms_max.fetch_max(40, Ordering::Relaxed);
        assert_eq!(c.poll_cycles.load(Ordering::Relaxed), 3);
        assert_eq!(c.poll_ms_max.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn heartbeat_idle_polls_is_running_count_not_reset_on_emit() {
        // Per the heartbeat task: idle_polls is `load`ed not `swap`ped.
        // We want operators to see "this source has been idle for N
        // consecutive polls," which is only useful if it's running.
        let c = Arc::new(HeartbeatCounters::default());
        c.idle_polls.fetch_add(1, Ordering::Relaxed);
        c.idle_polls.fetch_add(1, Ordering::Relaxed);
        c.idle_polls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(c.idle_polls.load(Ordering::Relaxed), 3);
        // No swap on emit — idle stays at 3 until the tail loop resets it.
        c.idle_polls.store(0, Ordering::Relaxed);
        assert_eq!(c.idle_polls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn heartbeat_loop_exits_on_shutdown() {
        // Spawn the heartbeat task, signal shutdown, confirm it exits
        // promptly. start_paused lets us drive the 30s timer without
        // wall-clock waits.
        let counters = Arc::new(HeartbeatCounters::default());
        let shutdown = ShutdownToken::new();
        let task = tokio::spawn(run_heartbeat_loop(Arc::clone(&counters), shutdown.child()));
        // Advance time so the heartbeat fires once.
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        // Signal shutdown.
        shutdown.cancel();
        // Task should exit within a small window.
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
        assert!(result.is_ok(), "heartbeat task did not exit on shutdown");
        let join_result = result.expect("timeout");
        assert!(
            join_result.is_ok(),
            "heartbeat task panicked: {:?}",
            join_result
        );
    }

    #[test]
    fn drain_handles_exact_match_at_confirmed_boundary() {
        // Confirmed == pending.front().tx_id should drain that entry.
        let dir = tmp_state_dir();
        let cf = CursorFile::new(&dir).expect("CursorFile::new");
        let progress = Arc::new(AtomicU64::new(10));
        let mut pending: VecDeque<(u64, String)> =
            VecDeque::from([(10, "c10".to_owned()), (11, "c11".to_owned())]);
        let mut cursor = "c0".to_owned();
        drain_confirmed_cursors(Some(&progress), &mut pending, &cf, &mut cursor).expect("ok");
        assert_eq!(pending.len(), 1);
        assert_eq!(cursor, "c10");
        std::fs::remove_dir_all(&dir).ok();
    }
}
