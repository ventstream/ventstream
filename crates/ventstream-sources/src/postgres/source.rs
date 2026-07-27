//! The [`ventstream_core::Source`] implementation for Postgres CDC.
//!
//! ### Architecture
//!
//! Transport is delegated to [`pgwire_replication::ReplicationClient`]:
//! it handles the TCP connection, SCRAM auth, optional TLS, the outer
//! streaming-replication framing, and periodic standby-status updates.
//!
//! We own everything above the wire:
//!
//! - The `pgoutput` decoder for the inner row-level
//!   messages carried inside `XLogData`.
//! - The [`RelationCache`] tracking schemas across the stream.
//! - The [`event_mapper`] turning decoded messages into
//!   [`ventstream_core::Event`] values.
//! - The orchestration in `PostgresCdcSource::run_replication` —
//!   `select!`-ing between inbound replication events and the engine
//!   shutdown signal, propagating events into the bus, and advancing
//!   the applied LSN.
//!
//! ### LSN advance (at-least-once)
//!
//! After every successful publish into the bus we call
//! [`ReplicationClient::update_applied_lsn`] with the `wal_end` of the
//! batch we just emitted. The next standby status update — which
//! pgwire-replication sends on its own schedule (every
//! [`PostgresCdcConfig::status_interval`]) — reports that LSN as
//! write / flush / apply, advancing the slot's `confirmed_flush_lsn`
//! and letting Postgres recycle WAL segments.
//!
//! **Shutdown edge case (known, mild):** events processed between the
//! last status-interval tick and shutdown have their LSN advances
//! buffered in [`SharedProgress`](https://docs.rs/pgwire-replication)
//! but may not reach the server before the connection closes. Postgres
//! redelivers them on the next reconnect, so at-least-once semantics
//! are preserved — the cost is duplicate work, not lost data. A
//! `flush_status_on_stop()` upstream would close this. Until then,
//! operators on hot pipelines may want to sleep for one
//! `status_interval` between draining and shutting down.
//!
//! Sink-ACK gating (waiting for downstream sinks to confirm writes
//! before advancing) is a later phase and replaces the
//! immediate-advance call here.
//!
//! ### Operator prerequisites
//!
//! - A publication that lists the tables of interest.
//! - A logical replication slot using `pgoutput`.
//! - A user with the `REPLICATION` attribute (or superuser).
//!
//! These are not auto-created — they live outside the engine.
//!
//! ### Choosing `REPLICA IDENTITY` per table
//!
//! Postgres' replica identity setting controls what data appears in the
//! `old` tuple of UPDATE and DELETE events. Verified behavior:
//!
//! | Setting | UPDATE.old | DELETE.old |
//! |---------|-----------|-----------|
//! | `DEFAULT` (the default) | `null` unless a PK column changed, in which case the prior PK values | PK columns only; non-PK columns are JSON `null` |
//! | `FULL` (`ALTER TABLE t REPLICA IDENTITY FULL`) | The complete prior row | The complete prior row |
//! | `USING INDEX <unique_idx>` | The indexed columns | The indexed columns |
//! | `NOTHING` | UPDATE / DELETE silently dropped from the publication | UPDATE / DELETE silently dropped |
//!
//! Pick `DEFAULT` when downstream only needs to identify the row (and
//! is willing to look up extras itself). Pick `FULL` when downstream
//! needs before/after diffs (audit logs, CDC-to-search with tombstones,
//! conflict resolution). `FULL` writes more WAL — measure on large tables.

use async_trait::async_trait;
use pgwire_replication::{
    Lsn as PgwLsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Event header carrying the wal_end LSN (u64) of the WAL record
/// that produced this event. Snapshot events do not set it.
pub const LSN_HEADER: &str = "ventstream.cdc.lsn";

/// Default cadence for `standby_status_update` pushes. Operators can
/// override via [`PostgresCdcSource::with_lsn_flush_interval`]. Smaller
/// values trade more PG-side traffic for faster slot reclamation;
/// larger values trade slot lag for less wire chatter.
const DEFAULT_LSN_FLUSH_INTERVAL: Duration = Duration::from_millis(200);
use tracing::{debug, info, warn};
use ventstream_core::{Event, Source, SourceContext, SourceError};

use super::config::PostgresCdcConfig;
use super::event_mapper;
use super::pgoutput::{self, LogicalMessage};
use super::schema::RelationCache;
use super::snapshot;
use crate::error::PostgresCdcError;
use crate::tls::DatabaseTlsMode;

/// Source adapter for a Postgres logical replication slot.
pub struct PostgresCdcSource {
    config: PostgresCdcConfig,
    relations: RelationCache,
    /// Sink-confirmed LSN watermark. When `Some`, the source defers
    /// `update_applied_lsn` calls — only advancing to the value held
    /// here, which represents the highest LSN whose corresponding
    /// event has been durably written by the sink. When `None`, the
    /// source falls back to the Phase-0 behavior of advancing on
    /// successful bus publish (not crash-safe; kept for backward
    /// compatibility with callers that haven't wired the gating yet).
    sink_progress: Option<Arc<AtomicU64>>,
    /// How often to push the latest acked LSN back to Postgres.
    lsn_flush_interval: Duration,
    /// Rebuild local join state from a snapshot while retaining an existing
    /// replication slot for subsequent WAL replay.
    bootstrap_existing_slot: bool,
}

impl PostgresCdcSource {
    /// Construct a new source from configuration. Connection setup
    /// happens lazily inside [`Source::run`].
    pub fn new(config: PostgresCdcConfig) -> Self {
        Self {
            config,
            relations: RelationCache::new(),
            sink_progress: None,
            lsn_flush_interval: DEFAULT_LSN_FLUSH_INTERVAL,
            bootstrap_existing_slot: false,
        }
    }

    /// Attach a sink-progress watermark. Once set, the source uses
    /// this Arc to gate WAL slot advancement — events the sink has
    /// confirmed durably are released to Postgres for WAL recycling;
    /// in-flight or DLQ-routed events are retained on the slot until
    /// they catch up. Used by the engine to wire crash-safety in.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    /// Override the LSN-flush cadence (default 200 ms).
    #[must_use]
    pub fn with_lsn_flush_interval(mut self, interval: Duration) -> Self {
        self.lsn_flush_interval = interval;
        self
    }

    /// Force the configured snapshot while retaining an existing slot.
    #[must_use]
    pub fn with_existing_slot_bootstrap(mut self, enabled: bool) -> Self {
        self.bootstrap_existing_slot = enabled;
        self
    }

    /// Borrow the source's relation cache. Mainly useful for tests and
    /// telemetry.
    pub fn relations(&self) -> &RelationCache {
        &self.relations
    }

    /// Decode one `pgoutput` payload and convert it (if applicable) into
    /// zero or more events. Pure function over `(self.relations, bytes)`
    /// — drives the schema cache as a side effect when `RELATION`
    /// messages arrive.
    pub fn process_payload(&self, bytes: &[u8]) -> Result<Vec<Event>, PostgresCdcError> {
        let message = pgoutput::decode(bytes)?;
        match message {
            LogicalMessage::Begin(begin) => {
                debug!(
                    xid = begin.xid,
                    final_lsn = %begin.final_lsn,
                    "pgoutput BEGIN (forwarded by transport layer; usually filtered)"
                );
                Ok(Vec::new())
            }
            LogicalMessage::Commit(commit) => {
                debug!(
                    commit_lsn = %commit.commit_lsn,
                    end_lsn = %commit.end_lsn,
                    "pgoutput COMMIT (forwarded by transport layer; usually filtered)"
                );
                Ok(Vec::new())
            }
            LogicalMessage::Relation(relation) => {
                info!(
                    relation_id = relation.id,
                    namespace = %relation.namespace,
                    name = %relation.name,
                    column_count = relation.columns.len(),
                    "pgoutput RELATION cached"
                );
                // Surface source schema drift (added/dropped/retyped
                // columns) before replacing the cached schema. Warn-only.
                if let Some(prior) = self.relations.get(relation.id) {
                    super::schema::detect_drift(&prior, &relation);
                }
                self.relations.insert(relation);
                Ok(Vec::new())
            }
            LogicalMessage::Insert(insert) => {
                let relation = self
                    .relations
                    .get(insert.relation_id)
                    .ok_or(PostgresCdcError::UnknownRelation(insert.relation_id))?;
                let event =
                    event_mapper::insert_to_event(&self.config.publication, &relation, &insert)?;
                Ok(vec![event])
            }
            LogicalMessage::Update(update) => {
                let relation = self
                    .relations
                    .get(update.relation_id)
                    .ok_or(PostgresCdcError::UnknownRelation(update.relation_id))?;
                let event =
                    event_mapper::update_to_event(&self.config.publication, &relation, &update)?;
                Ok(vec![event])
            }
            LogicalMessage::Delete(delete) => {
                let relation = self
                    .relations
                    .get(delete.relation_id)
                    .ok_or(PostgresCdcError::UnknownRelation(delete.relation_id))?;
                let event =
                    event_mapper::delete_to_event(&self.config.publication, &relation, &delete)?;
                Ok(vec![event])
            }
            LogicalMessage::Truncate(truncate) => {
                let mut events = Vec::with_capacity(truncate.relation_ids.len());
                for oid in &truncate.relation_ids {
                    let relation = self
                        .relations
                        .get(*oid)
                        .ok_or(PostgresCdcError::UnknownRelation(*oid))?;
                    let event = event_mapper::truncate_to_event(
                        &self.config.publication,
                        &relation,
                        truncate.cascade,
                        truncate.restart_identity,
                    )?;
                    events.push(event);
                }
                Ok(events)
            }
            // Type messages describe a user-defined column type and
            // precede a Relation message. We don't act on them — the
            // Relation message that follows carries the column OIDs
            // and lengths we actually need. Surface at DEBUG so an
            // operator can see what enums their publication carries.
            LogicalMessage::Type(t) => {
                debug!(
                    oid = t.oid,
                    namespace = %t.namespace,
                    name = %t.name,
                    metric = "pg.replication.type_message",
                    "pgoutput: user-defined type seen (ignored — Relation has the metadata we need)"
                );
                Ok(Vec::new())
            }
            // Tags we recognise but don't process (Origin, logical
            // decoding messages, stream-mode markers). We surface
            // their existence so they don't disappear silently.
            LogicalMessage::Ignored { tag } => {
                debug!(
                    tag = format!("'{}'", char::from(tag)),
                    metric = "pg.replication.ignored_message",
                    "pgoutput: ignored protocol message"
                );
                Ok(Vec::new())
            }
        }
    }

    /// Build the `pgwire-replication` config from our own.
    fn build_replication_config(&self) -> ReplicationConfig {
        let tls = match self.config.tls.as_ref() {
            None
            | Some(crate::tls::DatabaseTlsConfig {
                mode: DatabaseTlsMode::Disabled,
                ..
            }) => TlsConfig::disabled(),
            Some(config) => TlsConfig {
                mode: pgwire_replication::config::SslMode::VerifyFull,
                ca_pem_path: config.ca_file.clone(),
                ..TlsConfig::default()
            },
        };
        ReplicationConfig {
            host: self.config.host.clone(),
            port: self.config.port,
            user: self.config.user.clone(),
            password: self.config.password.clone(),
            database: self.config.database.clone(),
            tls,
            slot: self.config.slot_name.clone(),
            publication: self.config.publication.clone(),
            start_lsn: PgwLsn::ZERO,
            status_interval: self.config.status_interval,
            ..Default::default()
        }
    }

    /// Connect via `pgwire-replication`, then run the event loop until
    /// shutdown or fatal error.
    async fn run_replication(&self, ctx: SourceContext) -> Result<(), PostgresCdcError> {
        // Snapshot bootstrap runs BEFORE the replication slot is
        // opened. It's a no-op when (a) no bootstrap config is set or
        // (b) the slot already exists. See `snapshot.rs` for the
        // non-transactional v1 caveat.
        if let Err(err) =
            snapshot::maybe_bootstrap(&self.config, self.bootstrap_existing_slot, &ctx).await
        {
            ventstream_telemetry::record_error(format!("snapshot bootstrap failed: {err}"));
            return Err(err);
        }

        // Bootstrap done (or skipped). Drive the replication stream,
        // reconnecting in-process on transient connection drops.
        self.drive_with_reconnect(&ctx).await
    }

    /// Connect and drive the replication stream, reconnecting in-process
    /// on transient connection drops (a mid-stream reset, or a load
    /// balancer killing an idle connection) with bounded exponential
    /// backoff.
    ///
    /// Re-connecting with the same slot resumes from the server's
    /// confirmed-flush LSN, so no committed change is missed: events
    /// since the last ack are re-delivered (at-least-once) and the sink
    /// upserts idempotently by doc id.
    ///
    /// A connection that streams for a sustained period resets the
    /// backoff budget, so brief blips / idle-kills recover instantly,
    /// while a genuinely-dead upstream (rapid flapping) exhausts the
    /// budget and surfaces to Kubernetes as a pod restart rather than
    /// spinning forever — keeping a dead DB visible to liveness.
    async fn drive_with_reconnect(&self, ctx: &SourceContext) -> Result<(), PostgresCdcError> {
        const MAX_CONSECUTIVE_FAILURES: u32 = 10;
        const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);
        // A connection healthy at least this long resets the budget.
        const HEALTHY_THRESHOLD: Duration = Duration::from_secs(30);

        let mut consecutive_failures: u32 = 0;
        let mut backoff = INITIAL_BACKOFF;

        loop {
            if ctx.shutdown.is_cancelled() {
                return Ok(());
            }

            let cfg = self.build_replication_config();
            info!(
                host = %cfg.host,
                port = cfg.port,
                database = %cfg.database,
                slot = %cfg.slot,
                publication = %cfg.publication,
                "connecting to postgres for logical replication"
            );

            let connected_at = std::time::Instant::now();
            let mut client = match ReplicationClient::connect(cfg).await {
                Ok(client) => client,
                Err(err) => {
                    consecutive_failures += 1;
                    ventstream_telemetry::record_error(format!(
                        "postgres replication connect failed: {err}"
                    ));
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        return Err(PostgresCdcError::Connection(format!(
                            "reconnect budget exhausted after {consecutive_failures} consecutive failures: {err}"
                        )));
                    }
                    warn!(
                        attempt = consecutive_failures,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "postgres connect failed; retrying after backoff"
                    );
                    tokio::select! {
                        () = ctx.shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
            };

            // Stream is open. Transition to tail mode and clear any stale
            // error captured by a prior connect attempt.
            ventstream_telemetry::clear_error();
            ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Tailing);

            let outcome = self.drive_replication(&mut client, ctx).await;
            if let Err(err) = client.shutdown().await {
                warn!(error = %err, "postgres replication shutdown reported error");
            }

            match outcome {
                // drive_replication returns Ok only on shutdown.
                Ok(()) => return Ok(()),
                // A connection-level drop is transient — reconnect.
                Err(err @ PostgresCdcError::Connection(_)) => {
                    // Healthy for a while → treat as a fresh transient
                    // event and reset the budget.
                    if connected_at.elapsed() >= HEALTHY_THRESHOLD {
                        consecutive_failures = 0;
                        backoff = INITIAL_BACKOFF;
                    }
                    consecutive_failures += 1;
                    ventstream_telemetry::record_error(format!(
                        "postgres replication connection dropped: {err}"
                    ));
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        return Err(err);
                    }
                    warn!(
                        attempt = consecutive_failures,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "postgres replication dropped; reconnecting after backoff"
                    );
                    tokio::select! {
                        () = ctx.shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                // Decode / Internal (e.g. the consumer bus closed) → fatal.
                Err(err) => return Err(err),
            }
        }
    }

    async fn drive_replication(
        &self,
        client: &mut ReplicationClient,
        ctx: &SourceContext,
    ) -> Result<(), PostgresCdcError> {
        // High-water mark of WAL LSNs we've seen — bounds how far we
        // could ever advance the slot regardless of sink confirmations.
        let mut wal_high_water: u64 = 0;
        // Last value we pushed to the driver via update_applied_lsn,
        // so we skip redundant calls.
        let mut last_acked: u64 = 0;
        let mut lsn_flush_timer = tokio::time::interval(self.lsn_flush_interval);
        lsn_flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately — discard so we don't double-advance.
        let _ = lsn_flush_timer.tick().await;

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    info!("postgres cdc shutdown requested");
                    client.stop();
                    return Ok(());
                }
                _ = lsn_flush_timer.tick() => {
                    // Periodic re-evaluation of the safe ack point.
                    // Cheap and idempotent — update_applied_lsn does
                    // nothing if the value hasn't moved.
                    let advance_to = self.compute_safe_ack(wal_high_water);
                    if advance_to > last_acked {
                        debug!(
                            advance_to,
                            last_acked,
                            wal_high_water,
                            metric = "pg.replication.lsn_advance",
                            "advancing replication slot (periodic flush)"
                        );
                        client.update_applied_lsn(PgwLsn(advance_to));
                        last_acked = advance_to;
                    }
                }
                event = client.recv() => {
                    match event.map_err(|err| PostgresCdcError::Connection(err.to_string()))? {
                        Some(ReplicationEvent::XLogData { wal_start, wal_end, data, .. }) => {
                            let events = match self.process_payload(&data) {
                                Ok(events) => events,
                                // A row message for a relation we have no
                                // RELATION metadata for (not in the publication,
                                // or its RELATION message was never seen).
                                // Propagating this is FATAL and the slot resumes
                                // at the same LSN on restart → the same record
                                // re-fails forever (crash loop, zero progress).
                                // Skip-and-log instead: emit nothing and let the
                                // cursor advance past it (M11).
                                Err(PostgresCdcError::UnknownRelation(oid)) => {
                                    warn!(
                                        relation_oid = oid,
                                        metric = "pg.replication.unknown_relation_skipped",
                                        "skipping WAL message for an uncached relation; advancing past it"
                                    );
                                    ventstream_telemetry::record_error(format!(
                                        "pg unknown relation oid {oid} skipped"
                                    ));
                                    Vec::new()
                                }
                                Err(other) => return Err(other),
                            };
                            let wal_start_u64 = wal_start.as_u64();
                            let wal_end_u64 = wal_end.as_u64();
                            // Per-payload breadcrumb — operators chasing
                            // "did this row get emitted?" want to see
                            // each WAL message's event count + LSN.
                            // Row CONTENTS are NOT logged; emitted as
                            // structured Events instead, which the
                            // dispatcher logs by size, not value.
                            debug!(
                                wal_start = %wal_start,
                                wal_end = %wal_end,
                                events_in_payload = events.len(),
                                payload_bytes = data.len(),
                                metric = "pg.replication.xlog_data",
                                "wal payload yielded events"
                            );
                            for event in events {
                                // Stamp the record's OWN start LSN, not wal_end.
                                // wal_end is the server's current end-of-WAL (>=
                                // this record) and is documented as possibly 0
                                // for mid-transaction messages; stamping it would
                                // let sink_progress reflect a position ahead of
                                // the actual durable event — or 0 — and the
                                // contiguous-watermark ack gating would lose
                                // precision (or break on the 0). wal_start is the
                                // precise, always-valid, monotonic position.
                                let stamped = stamp_lsn(event, wal_start_u64);
                                ctx.sender.send(stamped, &ctx.shutdown).await.map_err(|err| {
                                    PostgresCdcError::Internal(format!("publish failed: {err}"))
                                })?;
                                ventstream_telemetry::bump_events_emitted(1);
                            }
                            // Raise the ack ceiling from whichever pointer is
                            // higher; wal_end can be 0 mid-transaction, so fall
                            // back to wal_start so the ceiling still advances.
                            // Acking stays gated by sink_progress, so a ceiling
                            // at an uncommitted record's start is never acked
                            // until that record is sink-confirmed.
                            let bound = wal_end_u64.max(wal_start_u64);
                            if bound > wal_high_water {
                                wal_high_water = bound;
                            }
                            // No immediate update_applied_lsn — defer
                            // to the periodic flush, gated by sink
                            // progress. Falls back to wal_high_water
                            // when no sink-progress watermark is
                            // attached (legacy behavior).
                            let advance_to = self.compute_safe_ack(wal_high_water);
                            if advance_to > last_acked {
                                debug!(
                                    advance_to,
                                    last_acked,
                                    wal_high_water,
                                    metric = "pg.replication.lsn_advance",
                                    "advancing replication slot"
                                );
                                client.update_applied_lsn(PgwLsn(advance_to));
                                last_acked = advance_to;
                            }
                        }
                        Some(ReplicationEvent::Begin { final_lsn, xid, .. }) => {
                            debug!(
                                xid,
                                final_lsn = %final_lsn,
                                metric = "pg.replication.txn_begin",
                                "txn begin"
                            );
                        }
                        Some(ReplicationEvent::Commit { lsn, end_lsn, .. }) => {
                            debug!(
                                commit_lsn = %lsn,
                                end_lsn = %end_lsn,
                                metric = "pg.replication.txn_commit",
                                "txn commit"
                            );
                            let end_u64 = end_lsn.as_u64();
                            if end_u64 > wal_high_water {
                                wal_high_water = end_u64;
                            }
                            // Same deferral logic as XLogData — the
                            // commit LSN bounds how far we *could*
                            // advance, but the sink decides when.
                            let advance_to = self.compute_safe_ack(wal_high_water);
                            if advance_to > last_acked {
                                debug!(
                                    advance_to,
                                    last_acked,
                                    wal_high_water,
                                    metric = "pg.replication.lsn_advance",
                                    "advancing replication slot (post-commit)"
                                );
                                client.update_applied_lsn(PgwLsn(advance_to));
                                last_acked = advance_to;
                            }
                        }
                        Some(ReplicationEvent::KeepAlive { wal_end, .. }) => {
                            debug!(
                                wal_end = %wal_end,
                                metric = "pg.replication.keepalive",
                                "server keepalive"
                            );
                        }
                        Some(ReplicationEvent::Message { prefix, content, .. }) => {
                            // Logs the prefix (schema-like, safe) and
                            // byte count only — `content` is the actual
                            // message payload (sensitive) and is
                            // deliberately NOT logged.
                            debug!(
                                %prefix,
                                bytes = content.len(),
                                metric = "pg.replication.logical_message_ignored",
                                "logical replication message (ignored)"
                            );
                        }
                        Some(ReplicationEvent::StoppedAt { reached }) => {
                            info!(lsn = %reached, "replication stopped at configured stop_at_lsn");
                            return Ok(());
                        }
                        None => {
                            warn!("postgres replication stream ended");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Compute the safe ack point for the WAL slot. With a sink
    /// progress tracker attached, returns `min(wal_high_water,
    /// sink_progress)` — never advancing past durable events.
    /// Without one, returns `wal_high_water` (legacy behavior).
    fn compute_safe_ack(&self, wal_high_water: u64) -> u64 {
        match &self.sink_progress {
            Some(progress) => wal_high_water.min(progress.load(Ordering::Relaxed)),
            None => wal_high_water,
        }
    }
}

/// Attach `ventstream.cdc.lsn` to a WAL-derived event. The event was
/// just built by the mapper and isn't shared yet, so `with_header`
/// inserts in place via `Arc::make_mut` — no per-event rebuild of the
/// header map.
fn stamp_lsn(event: Event, lsn: u64) -> Event {
    Event {
        headers: event
            .headers
            .with_header(LSN_HEADER.to_owned(), lsn.to_string()),
        ..event
    }
}

#[async_trait]
impl Source for PostgresCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "postgres_cdc"
    }

    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
        self.run_replication(ctx).await.map_err(|err| match err {
            PostgresCdcError::Connection(msg) => SourceError::Connection(msg),
            PostgresCdcError::Setup { statement, message } => {
                SourceError::Connection(format!("setup failed: {statement}: {message}"))
            }
            PostgresCdcError::Decode(decode_err) => SourceError::Decode(decode_err.to_string()),
            PostgresCdcError::UnknownRelation(oid) => {
                SourceError::Decode(format!("unknown relation oid {oid}"))
            }
            PostgresCdcError::Internal(msg) => SourceError::Internal(msg),
        })
    }
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

    /// Mirror of the test builder in the decoder tests — kept inline so
    /// the integration-style cases here don't reach into other modules.
    struct Builder {
        bytes: Vec<u8>,
    }
    impl Builder {
        fn new(tag: u8) -> Self {
            Self { bytes: vec![tag] }
        }
        fn u8(mut self, v: u8) -> Self {
            self.bytes.push(v);
            self
        }
        fn i16(mut self, v: i16) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u32(mut self, v: u32) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn i64(mut self, v: i64) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn u64(mut self, v: u64) -> Self {
            self.bytes.extend_from_slice(&v.to_be_bytes());
            self
        }
        fn cstr(mut self, s: &str) -> Self {
            self.bytes.extend_from_slice(s.as_bytes());
            self.bytes.push(0);
            self
        }
        fn raw(mut self, bs: &[u8]) -> Self {
            self.bytes.extend_from_slice(bs);
            self
        }
        fn build(self) -> Vec<u8> {
            self.bytes
        }
    }

    fn make_source() -> PostgresCdcSource {
        PostgresCdcSource::new(PostgresCdcConfig::new(
            "test-source",
            "localhost",
            "postgres",
            "secret",
            "testdb",
            "test_pub",
            "test_slot",
        ))
    }

    #[test]
    fn replication_uses_the_configured_tls_policy() {
        let mut source = make_source();
        source.config.tls = Some(crate::tls::DatabaseTlsConfig {
            mode: crate::tls::DatabaseTlsMode::VerifyFull,
            ca_file: Some(std::path::PathBuf::from("/run/secrets/postgres-ca.pem")),
        });
        let config = source.build_replication_config();
        assert_eq!(
            config.tls.mode,
            pgwire_replication::config::SslMode::VerifyFull
        );
        assert_eq!(
            config.tls.ca_pem_path.as_deref(),
            Some(std::path::Path::new("/run/secrets/postgres-ca.pem"))
        );
    }

    fn cache_users_relation(source: &PostgresCdcSource) {
        let relation_bytes = Builder::new(b'R')
            .u32(16_384)
            .cstr("public")
            .cstr("users")
            .u8(b'd')
            .i16(2)
            .u8(0x01)
            .cstr("id")
            .u32(23)
            .i32(-1)
            .u8(0x00)
            .cstr("email")
            .u32(25)
            .i32(-1)
            .build();
        let events = source.process_payload(&relation_bytes).expect("relation");
        assert!(events.is_empty());
    }

    #[test]
    fn insert_produces_one_event() {
        let source = make_source();
        cache_users_relation(&source);
        let insert_bytes = Builder::new(b'I')
            .u32(16_384)
            .u8(b'N')
            .i16(2)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .u8(b't')
            .i32(17)
            .raw(b"alice@example.com")
            .build();
        let events = source.process_payload(&insert_bytes).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subject.as_str(), "postgres.public.users.insert");
    }

    #[test]
    fn stamp_lsn_writes_record_start_lsn_into_cdc_header() {
        let source = make_source();
        cache_users_relation(&source);
        let insert_bytes = Builder::new(b'I')
            .u32(16_384)
            .u8(b'N')
            .i16(2)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .u8(b't')
            .i32(17)
            .raw(b"alice@example.com")
            .build();
        let event = source
            .process_payload(&insert_bytes)
            .expect("events")
            .into_iter()
            .next()
            .expect("one event");
        // The recv loop stamps each event with the record's wal_start. A real
        // start LSN is non-zero and monotonic; the contiguous-watermark ack
        // gating reads this header back as sink_progress, so it must reflect
        // the record's own position (not wal_end, which can be 0 mid-txn).
        let wal_start: u64 = 0x0123_4567_89AB_CDEF;
        let stamped = stamp_lsn(event, wal_start);
        assert_eq!(
            stamped.headers.get(LSN_HEADER),
            Some(wal_start.to_string().as_str()),
            "cdc.lsn header must carry the exact stamped start LSN"
        );
    }

    #[test]
    fn update_with_key_old_tuple_produces_update_event() {
        let source = make_source();
        cache_users_relation(&source);
        let update_bytes = Builder::new(b'U')
            .u32(16_384)
            .u8(b'K')
            .i16(2)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .u8(b'n')
            .u8(b'N')
            .i16(2)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .u8(b't')
            .i32(3)
            .raw(b"new")
            .build();
        let events = source.process_payload(&update_bytes).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subject.as_str(), "postgres.public.users.update");
    }

    #[test]
    fn delete_produces_delete_event() {
        let source = make_source();
        cache_users_relation(&source);
        let delete_bytes = Builder::new(b'D')
            .u32(16_384)
            .u8(b'K')
            .i16(2)
            .u8(b't')
            .i32(2)
            .raw(b"42")
            .u8(b'n')
            .build();
        let events = source.process_payload(&delete_bytes).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subject.as_str(), "postgres.public.users.delete");
    }

    #[test]
    fn truncate_produces_one_event_per_relation() {
        let source = make_source();
        cache_users_relation(&source);
        let orders = Builder::new(b'R')
            .u32(16_385)
            .cstr("public")
            .cstr("orders")
            .u8(b'd')
            .i16(1)
            .u8(0x01)
            .cstr("id")
            .u32(23)
            .i32(-1)
            .build();
        source.process_payload(&orders).expect("relation");
        let truncate_bytes = Builder::new(b'T')
            .u32(2)
            .u8(0x03)
            .u32(16_384)
            .u32(16_385)
            .build();
        let events = source.process_payload(&truncate_bytes).expect("events");
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn begin_and_commit_produce_no_events() {
        let source = make_source();
        let begin = Builder::new(b'B').u64(0x100).i64(1).u32(1).build();
        assert!(source.process_payload(&begin).expect("begin").is_empty());
        let commit = Builder::new(b'C')
            .u8(0)
            .u64(0x100)
            .u64(0x108)
            .i64(1)
            .build();
        assert!(source.process_payload(&commit).expect("commit").is_empty());
    }

    /// Custom Postgres schemas (anything other than `public`) flow through
    /// the same pgoutput `RELATION` namespace field. The decoder, schema
    /// cache, event mapper, source URI, and subject formatting must all
    /// honour the actual namespace rather than assuming `public`.
    #[test]
    fn custom_schema_is_honoured_in_subject_and_source_uri() {
        let source = make_source();

        // RELATION for `analytics.events(event_id bigint key, payload text)`
        let relation_bytes = Builder::new(b'R')
            .u32(40_000)
            .cstr("analytics")
            .cstr("events")
            .u8(b'd')
            .i16(2)
            .u8(0x01)
            .cstr("event_id")
            .u32(20)
            .i32(-1)
            .u8(0x00)
            .cstr("payload")
            .u32(25)
            .i32(-1)
            .build();
        source.process_payload(&relation_bytes).expect("relation");

        let insert_bytes = Builder::new(b'I')
            .u32(40_000)
            .u8(b'N')
            .i16(2)
            .u8(b't')
            .i32(1)
            .raw(b"7")
            .u8(b't')
            .i32(4)
            .raw(b"data")
            .build();
        let events = source.process_payload(&insert_bytes).expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].subject.as_str(),
            "postgres.analytics.events.insert",
            "subject must reflect the relation's actual namespace"
        );
        assert_eq!(
            events[0].source.as_str(),
            "postgres://test_pub/analytics/events",
            "source URI must reflect the relation's actual namespace"
        );
        assert_eq!(
            events[0].headers.get("ventstream.cdc.namespace"),
            Some("analytics"),
            "CDC namespace header must carry the actual schema"
        );
    }

    /// Multi-tenant schema names like `tenant_42` are valid Subject
    /// segments — the validator accepts ASCII alphanumerics, `_`, and `-`.
    /// This locks in that we won't reject perfectly reasonable schemas.
    #[test]
    fn underscore_and_digit_schema_names_are_accepted() {
        let source = make_source();
        let relation_bytes = Builder::new(b'R')
            .u32(40_001)
            .cstr("tenant_42")
            .cstr("audit_log")
            .u8(b'd')
            .i16(1)
            .u8(0x01)
            .cstr("id")
            .u32(20)
            .i32(-1)
            .build();
        source.process_payload(&relation_bytes).expect("relation");

        let insert_bytes = Builder::new(b'I')
            .u32(40_001)
            .u8(b'N')
            .i16(1)
            .u8(b't')
            .i32(1)
            .raw(b"1")
            .build();
        let events = source.process_payload(&insert_bytes).expect("events");
        assert_eq!(
            events[0].subject.as_str(),
            "postgres.tenant_42.audit_log.insert"
        );
    }
}
