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
use bytes::Bytes;
use pgwire_replication::{
    Lsn as PgwLsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig,
};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
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
const TRANSACTION_MEMORY_LIMIT_BYTES: usize = 8 * 1024 * 1024;
static TRANSACTION_SPOOL_SEQUENCE: AtomicU64 = AtomicU64::new(1);
use tracing::{debug, error, info, warn};
use ventstream_core::event::Payload;
use ventstream_core::{Event, Source, SourceContext, SourceError};

use crate::credential::{exhausted_message, CredentialFailureBudget};

use super::config::PostgresCdcConfig;
use super::connection::connect_client;
use super::event_mapper;
use super::pgoutput::{self, LogicalMessage};
use super::preflight;
use super::schema::RelationCache;
use super::snapshot;
use crate::error::PostgresCdcError;
use crate::tls::DatabaseTlsMode;

struct WalRecord {
    wal_start: u64,
    data: Bytes,
}

enum TransactionStorage {
    Memory {
        records: VecDeque<WalRecord>,
        bytes: usize,
    },
    Disk {
        file: File,
        path: PathBuf,
        remaining: usize,
        reading: bool,
    },
}

struct TransactionBuffer {
    storage: TransactionStorage,
    spool_dir: PathBuf,
}

impl TransactionBuffer {
    fn new(spool_dir: PathBuf) -> Self {
        Self {
            storage: TransactionStorage::Memory {
                records: VecDeque::new(),
                bytes: 0,
            },
            spool_dir,
        }
    }

    fn push(&mut self, record: WalRecord) -> io::Result<()> {
        let record_bytes = record.data.len().saturating_add(16);
        match &mut self.storage {
            TransactionStorage::Memory { records, bytes }
                if bytes.saturating_add(record_bytes) <= TRANSACTION_MEMORY_LIMIT_BYTES =>
            {
                records.push_back(record);
                *bytes = bytes.saturating_add(record_bytes);
                Ok(())
            }
            TransactionStorage::Memory { .. } => {
                self.spill_to_disk(&record)?;
                Ok(())
            }
            TransactionStorage::Disk {
                file, remaining, ..
            } => {
                write_wal_record(file, &record)?;
                *remaining = remaining.saturating_add(1);
                Ok(())
            }
        }
    }

    fn next_record(&mut self) -> io::Result<Option<WalRecord>> {
        match &mut self.storage {
            TransactionStorage::Memory { records, .. } => Ok(records.pop_front()),
            TransactionStorage::Disk {
                file,
                remaining,
                reading,
                ..
            } => {
                if !*reading {
                    file.flush()?;
                    file.seek(SeekFrom::Start(0))?;
                    *reading = true;
                }
                if *remaining == 0 {
                    return Ok(None);
                }
                let record = read_wal_record(file)?;
                *remaining = remaining.saturating_sub(1);
                Ok(Some(record))
            }
        }
    }

    fn spill_to_disk(&mut self, next: &WalRecord) -> io::Result<()> {
        let (mut records, count) = match std::mem::replace(
            &mut self.storage,
            TransactionStorage::Memory {
                records: VecDeque::new(),
                bytes: 0,
            },
        ) {
            TransactionStorage::Memory { records, .. } => {
                let count = records.len().saturating_add(1);
                (records, count)
            }
            TransactionStorage::Disk { .. } => unreachable!("spill called for disk storage"),
        };
        let (mut file, path) = create_spool_file(&self.spool_dir)?;
        while let Some(record) = records.pop_front() {
            write_wal_record(&mut file, &record)?;
        }
        write_wal_record(&mut file, next)?;
        self.storage = TransactionStorage::Disk {
            file,
            path,
            remaining: count,
            reading: false,
        };
        metrics::counter!("vs_postgres_transaction_spills_total").increment(1);
        Ok(())
    }

    #[cfg(test)]
    fn is_spilled(&self) -> bool {
        matches!(self.storage, TransactionStorage::Disk { .. })
    }

    #[cfg(test)]
    fn spool_path(&self) -> Option<&Path> {
        match &self.storage {
            TransactionStorage::Disk { path, .. } => Some(path),
            TransactionStorage::Memory { .. } => None,
        }
    }
}

impl Drop for TransactionBuffer {
    fn drop(&mut self) {
        let storage = std::mem::replace(
            &mut self.storage,
            TransactionStorage::Memory {
                records: VecDeque::new(),
                bytes: 0,
            },
        );
        if let TransactionStorage::Disk { file, path, .. } = storage {
            drop(file);
            let _ = std::fs::remove_file(path);
        }
    }
}

fn create_spool_file(directory: &Path) -> io::Result<(File, PathBuf)> {
    std::fs::create_dir_all(directory)?;
    for _ in 0..32 {
        let sequence = TRANSACTION_SPOOL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("transaction-{}-{sequence}.wal", std::process::id()));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a PostgreSQL transaction spool file",
    ))
}

fn write_wal_record(file: &mut File, record: &WalRecord) -> io::Result<()> {
    let length = u64::try_from(record.data.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "WAL record is too large"))?;
    file.write_all(&record.wal_start.to_le_bytes())?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&record.data)
}

fn read_wal_record(file: &mut File) -> io::Result<WalRecord> {
    let mut wal_start = [0u8; 8];
    let mut length = [0u8; 8];
    file.read_exact(&mut wal_start)?;
    file.read_exact(&mut length)?;
    let length = usize::try_from(u64::from_le_bytes(length)).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "spooled WAL record length exceeds this platform",
        )
    })?;
    let mut data = vec![0u8; length];
    file.read_exact(&mut data)?;
    Ok(WalRecord {
        wal_start: u64::from_le_bytes(wal_start),
        data: Bytes::from(data),
    })
}

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
    transaction_spool_dir: PathBuf,
    /// Consecutive WAL messages skipped for uncached relations; reset
    /// by any successfully decoded payload. Bounded by
    /// [`MAX_UNKNOWN_RELATION_STREAK`].
    unknown_relation_streak: AtomicU64,
    /// Lazy auxiliary SQL connection + per-table metadata caches for
    /// TOAST refetch. Never the replication connection.
    toast_refetch: tokio::sync::Mutex<ToastRefetchState>,
}

/// Cached state for re-reading full rows when an UPDATE's new image
/// omitted unchanged TOAST columns (see `TOAST_INCOMPLETE_HEADER`).
#[derive(Default)]
struct ToastRefetchState {
    client: Option<tokio_postgres::Client>,
    /// Per-qualified-table refetch metadata, resolved once.
    tables: std::collections::HashMap<String, TableRefetchMeta>,
}

/// Cached per-table metadata for TOAST refetch queries.
struct TableRefetchMeta {
    /// Primary-key column names, index order.
    pk: Vec<String>,
    /// The PK columns' Postgres types, for `CAST($n::text AS type)`.
    pk_types: Vec<String>,
    /// Canonical-text column select list (see `build_cast_select`).
    cast_select: String,
}

/// Tolerated run of consecutive unknown-relation skips before the
/// source halts. Generous for the reconnect race (a handful of stale
/// messages), far below anything a broken relation cache produces.
const MAX_UNKNOWN_RELATION_STREAK: u64 = 64;

impl PostgresCdcSource {
    /// Construct a new source from configuration. Connection setup
    /// happens lazily inside [`Source::run`].
    pub fn new(config: PostgresCdcConfig) -> Self {
        let transaction_spool_dir = config
            .transaction_spool_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("ventstream-postgres-transactions"));
        Self {
            config,
            relations: RelationCache::new(),
            sink_progress: None,
            lsn_flush_interval: DEFAULT_LSN_FLUSH_INTERVAL,
            bootstrap_existing_slot: false,
            transaction_spool_dir,
            unknown_relation_streak: AtomicU64::new(0),
            toast_refetch: tokio::sync::Mutex::new(ToastRefetchState::default()),
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

    /// Set the directory used when a PostgreSQL transaction exceeds the
    /// in-memory transaction buffer.
    #[must_use]
    pub fn with_transaction_spool_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.transaction_spool_dir = directory.into();
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
                ca_pem_path: config.ca_file.clone().or_else(|| {
                    // No operator CA — fall back to the packaged root for
                    // known private-CA providers (Supabase). Failure to
                    // materialize surfaces later as a clear TLS error.
                    crate::tls::implicit_trust_provider(&self.config.host).and_then(|provider| {
                        crate::tls::materialize_provider_ca_bundle(provider)
                            .map_err(|err| {
                                warn!(error = %err, "packaged provider CA unavailable");
                                err
                            })
                            .ok()
                    })
                }),
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
        // Preflight: definitive misconfigurations (pooler endpoint,
        // wal_level, missing publication) fail fast with the fix in
        // the message. Transient connect trouble is NOT fatal here —
        // the replication path owns retries.
        preflight::check_endpoint(&self.config)?;
        if let Err(err) = preflight::run(&self.config).await {
            ventstream_telemetry::record_error(format!("postgres preflight failed: {err}"));
            return Err(err);
        }

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

        // Slot-lag observability: a low-frequency poll on its own plain
        // connection (never the replication link). Retained-WAL growth
        // behind the slot is the operator's first sign of a stuck or
        // slow consumer; without this gauge it is invisible until the
        // disk fills. `max_slot_wal_keep_size` remains the server-side
        // backstop — see the postgres connector docs.
        let lag_task = {
            let config = self.config.clone();
            let shutdown = ctx.shutdown.clone();
            tokio::spawn(async move {
                let mut poll = tokio::time::interval(Duration::from_secs(30));
                poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        _ = poll.tick() => {}
                    }
                    let Ok(client) = connect_client(&config, "slot lag poll").await else {
                        continue;
                    };
                    let lag = client
                        .query_opt(
                            "SELECT COALESCE(\
                                 pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), \
                                 0)::bigint \
                             FROM pg_replication_slots WHERE slot_name = $1",
                            &[&config.slot_name],
                        )
                        .await;
                    if let Ok(Some(row)) = lag {
                        let bytes: i64 = row.get(0);
                        metrics::gauge!("vs_pg_slot_lag_bytes").set(bytes as f64);
                    }
                }
            })
        };

        // Bootstrap done (or skipped). Drive the replication stream,
        // reconnecting in-process on transient connection drops.
        let outcome = self.drive_with_reconnect(&ctx).await;
        lag_task.abort();
        outcome
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
        let mut credential_budget = CredentialFailureBudget::new();

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
                    let rendered = err.to_string();
                    // A missing slot can't heal by retrying — fail now
                    // so a supervised restart re-bootstraps instead of
                    // burning the whole backoff budget first.
                    if is_missing_slot_message(&rendered) {
                        let message = missing_slot_message(&self.config, &rendered);
                        error!(source = %self.config.id, "{message}");
                        ventstream_telemetry::record_error(message.clone());
                        return Err(PostgresCdcError::Connection(message));
                    }
                    consecutive_failures += 1;
                    if is_credential_message(&rendered)
                        && credential_budget.record_credential_failure()
                    {
                        let message = exhausted_message(&err);
                        error!(source = %self.config.id, error = %err, "{message}");
                        return Err(PostgresCdcError::Connection(message));
                    }
                    let detail = match preflight::unreachable_hint(
                        &self.config.host,
                        self.config.port,
                        &rendered,
                    )
                    .await
                    {
                        Some(hint) => format!("{rendered} ({hint})"),
                        None => rendered,
                    };
                    ventstream_telemetry::record_error(format!(
                        "postgres replication connect failed: {detail}"
                    ));
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        return Err(PostgresCdcError::Connection(format!(
                            "reconnect budget exhausted after {consecutive_failures} consecutive failures: {detail}"
                        )));
                    }
                    warn!(
                        attempt = consecutive_failures,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %detail,
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

            // Stream is open: this connect authenticated successfully.
            credential_budget.record_success();
            // Transition to tail mode and clear any stale error captured
            // by a prior connect attempt.
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
                // Exception: the replication worker reporting a missing
                // slot (it connects first, then START_REPLICATION fails)
                // — retrying can't recreate it, so fail now and let a
                // supervised restart re-bootstrap.
                Err(err @ PostgresCdcError::Connection(_))
                    if is_missing_slot_message(&err.to_string()) =>
                {
                    let message = missing_slot_message(&self.config, &err.to_string());
                    error!(source = %self.config.id, "{message}");
                    ventstream_telemetry::record_error(message.clone());
                    return Err(PostgresCdcError::Connection(message));
                }
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
        let mut transaction: Option<TransactionBuffer> = None;

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
                            let events = self.decode_payload_or_skip_unknown(&data)?;
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
                            if events.is_empty() {
                                continue;
                            }
                            if let Some(buffer) = transaction.as_mut() {
                                buffer
                                    .push(WalRecord {
                                        wal_start: wal_start_u64,
                                        data,
                                    })
                                    .map_err(|error| {
                                        PostgresCdcError::Internal(format!(
                                            "buffering PostgreSQL transaction: {error}"
                                        ))
                                    })?;
                            } else {
                                self.publish_events(events, wal_start_u64, ctx).await?;
                                let bound = wal_end_u64.max(wal_start_u64);
                                if bound > wal_high_water {
                                    wal_high_water = bound;
                                }
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
                        }
                        Some(ReplicationEvent::Begin { final_lsn, xid, .. }) => {
                            if transaction.is_some() {
                                return Err(PostgresCdcError::Internal(
                                    "received PostgreSQL BEGIN before the prior transaction committed"
                                        .to_owned(),
                                ));
                            }
                            transaction = Some(TransactionBuffer::new(
                                self.transaction_spool_dir.clone(),
                            ));
                            debug!(
                                xid,
                                final_lsn = %final_lsn,
                                metric = "pg.replication.txn_begin",
                                "txn begin"
                            );
                        }
                        Some(ReplicationEvent::Commit { lsn, end_lsn, .. }) => {
                            if let Some(mut buffered) = transaction.take() {
                                while let Some(record) = buffered.next_record().map_err(|error| {
                                    PostgresCdcError::Internal(format!(
                                        "reading buffered PostgreSQL transaction: {error}"
                                    ))
                                })? {
                                    let events = self.decode_payload_or_skip_unknown(&record.data)?;
                                    self.publish_events(events, record.wal_start, ctx).await?;
                                }
                            }
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
                            // Idle-advance: when every emitted event has been
                            // durably handled by the sink (or nothing was
                            // emitted at all), the keepalive's wal_end is a
                            // safe ack point. Without this, an idle
                            // publication — or a restart on a quiet database
                            // — never advances confirmed_flush and WAL
                            // retention grows behind the slot indefinitely.
                            //
                            // Safe because streaming of in-progress
                            // transactions is off (proto_version '1'):
                            // transactions arrive whole after commit, and
                            // pgoutput delivers all decodable content up to
                            // wal_end before the keepalive carrying it, so
                            // any transaction not yet received commits past
                            // wal_end and will still be replayed on restart.
                            let wal_end_u64 = u64::from(wal_end);
                            let drained = wal_high_water == 0
                                || self.compute_safe_ack(wal_high_water) == wal_high_water;
                            if drained && wal_end_u64 > last_acked {
                                debug!(
                                    wal_end = %wal_end,
                                    last_acked,
                                    metric = "pg.replication.idle_advance",
                                    "advancing replication slot (idle keepalive)"
                                );
                                client.update_applied_lsn(PgwLsn(wal_end_u64));
                                last_acked = wal_end_u64;
                            } else {
                                debug!(
                                    wal_end = %wal_end,
                                    metric = "pg.replication.keepalive",
                                    "server keepalive"
                                );
                            }
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

    fn decode_payload_or_skip_unknown(&self, data: &[u8]) -> Result<Vec<Event>, PostgresCdcError> {
        match self.process_payload(data) {
            Ok(events) => {
                if !events.is_empty() {
                    self.unknown_relation_streak.store(0, Ordering::Relaxed);
                }
                Ok(events)
            }
            Err(PostgresCdcError::UnknownRelation(oid)) => {
                // Tolerating a bounded run of unknown relations covers the
                // restart race this path exists for (RELATION messages the
                // server already sent before we reconnected). An unbounded
                // streak means the relation cache itself is broken and every
                // skip is silent data loss — halt so the supervisor restarts
                // with a fresh cache instead.
                let streak = self
                    .unknown_relation_streak
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if streak > MAX_UNKNOWN_RELATION_STREAK {
                    return Err(PostgresCdcError::Internal(format!(
                        "{streak} consecutive WAL messages referenced uncached relations                          (last oid {oid}); halting instead of silently skipping —                          restart rebuilds the relation cache"
                    )));
                }
                warn!(
                    relation_oid = oid,
                    streak,
                    metric = "pg.replication.unknown_relation_skipped",
                    "skipping WAL message for an uncached relation; advancing past it"
                );
                metrics::counter!(
                    "vs_pg_unknown_relation_skipped_total",
                    "relation_oid" => oid.to_string()
                )
                .increment(1);
                ventstream_telemetry::record_error(format!(
                    "pg unknown relation oid {oid} skipped"
                ));
                Ok(Vec::new())
            }
            Err(other) => Err(other),
        }
    }

    async fn publish_events(
        &self,
        events: Vec<Event>,
        wal_start: u64,
        ctx: &SourceContext,
    ) -> Result<(), PostgresCdcError> {
        for event in events {
            let event = if event
                .headers
                .get(event_mapper::TOAST_INCOMPLETE_HEADER)
                .is_some()
            {
                self.refetch_toasted_row(event).await
            } else {
                event
            };
            // Truncate visibility (A4): counted here, not at decode, because
            // a buffered transaction decodes each payload twice.
            if let Some(qualified) = event.subject.as_str().strip_suffix(".truncate") {
                // Subject is `postgres.<schema>.<table>` — drop the kind
                // segment so the label matches the drift counter's form.
                let table = qualified
                    .split_once('.')
                    .map_or(qualified, |(_, rest)| rest);
                metrics::counter!(
                    "vs_truncate_events_total",
                    "table" => table.to_owned()
                )
                .increment(1);
            }
            let stamped = stamp_lsn(event, wal_start);
            ctx.sender
                .send(stamped, &ctx.shutdown)
                .await
                .map_err(|error| PostgresCdcError::Internal(format!("publish failed: {error}")))?;
            ventstream_telemetry::bump_events_emitted(1);
        }
        Ok(())
    }

    /// Compute the safe ack point for the WAL slot. With a sink
    /// progress tracker attached, returns `min(wal_high_water,
    /// sink_progress)` — never advancing past durable events.
    /// Without one, returns `wal_high_water` (legacy behavior).
    /// Re-read the complete row for an UPDATE whose new image omitted
    /// unchanged TOAST columns, and splice it into the event's `new`
    /// payload. Flat sinks full-replace documents, so shipping the
    /// partial image would silently drop the omitted fields. Best
    /// effort: on any failure (row deleted meanwhile, connection
    /// trouble) the original event ships unchanged — a later WAL event
    /// converges the document — and the header is dropped either way.
    async fn refetch_toasted_row(&self, event: Event) -> Event {
        let (Some(namespace), Some(relation), Some(doc_id)) = (
            event.headers.get("ventstream.cdc.namespace"),
            event.headers.get("ventstream.cdc.relation"),
            event.headers.get("ventstream.doc.id"),
        ) else {
            return strip_toast_header(event);
        };
        let components: Vec<String> = match doc_id
            .split_once(':')
            .and_then(|(_, suffix)| serde_json::from_str(suffix).ok())
        {
            Some(components) => components,
            None => return strip_toast_header(event),
        };
        let qualified = format!(
            "{}.{}",
            snapshot::quote_ident(namespace),
            snapshot::quote_ident(relation)
        );

        let mut state = self.toast_refetch.lock().await;
        if state.client.as_ref().is_none_or(|c| c.is_closed()) {
            match connect_client(&self.config, "toast refetch").await {
                Ok(client) => state.client = Some(client),
                Err(err) => {
                    warn!(error = %err, metric = "pg.toast_refetch.connect_failed",
                          "toast refetch connect failed; shipping partial row");
                    return strip_toast_header(event);
                }
            }
        }
        let fetched = fetch_full_row(&mut state, &qualified, &components).await;
        drop(state);
        match fetched {
            Ok(Some(row)) => {
                metrics::counter!("vs_pg_toast_refetch_total").increment(1);
                splice_new_row(strip_toast_header(event), row)
            }
            Ok(None) => {
                // Row already gone — the delete is behind us in the WAL.
                strip_toast_header(event)
            }
            Err(err) => {
                warn!(error = %err, metric = "pg.toast_refetch.failed",
                      "toast refetch failed; shipping partial row");
                strip_toast_header(event)
            }
        }
    }

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

/// Drop the toast-incomplete marker so it never leaks downstream.
fn strip_toast_header(event: Event) -> Event {
    Event {
        headers: event
            .headers
            .without_header(event_mapper::TOAST_INCOMPLETE_HEADER),
        ..event
    }
}

/// Replace the `new` object of an update envelope with the freshly
/// fetched full row (already canonical-text-normalized).
fn splice_new_row(event: Event, row: serde_json::Value) -> Event {
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(event.payload.as_slice())
    else {
        return event;
    };
    let Some(object) = payload.as_object_mut() else {
        return event;
    };
    object.insert("new".to_owned(), row);
    let Ok(bytes) = serde_json::to_vec(&payload) else {
        return event;
    };
    Event {
        payload: Payload::from_vec(bytes),
        ..event
    }
}

/// Fetch one full row by primary key with canonical text rendering,
/// resolving and caching the table's PK metadata + cast list on first
/// use. Returns `Ok(None)` when the row no longer exists.
async fn fetch_full_row(
    state: &mut ToastRefetchState,
    qualified: &str,
    components: &[String],
) -> Result<Option<serde_json::Value>, PostgresCdcError> {
    let client = state
        .client
        .as_ref()
        .ok_or_else(|| PostgresCdcError::Internal("toast refetch client missing".into()))?;
    if !state.tables.contains_key(qualified) {
        let pk_rows = client
            .query(
                "SELECT a.attname                  FROM pg_index i                  JOIN pg_attribute a                    ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)                  WHERE i.indrelid = to_regclass($1) AND i.indisprimary                  ORDER BY array_position(i.indkey, a.attnum)",
                &[&qualified],
            )
            .await
            .map_err(|err| PostgresCdcError::Connection(format!("pk discovery: {err}")))?;
        let pk: Vec<String> = pk_rows.into_iter().map(|row| row.get(0)).collect();
        if pk.is_empty() {
            return Err(PostgresCdcError::Internal(format!(
                "{qualified} has no primary key for toast refetch"
            )));
        }
        let pk_types = snapshot::fetch_pk_types(client, qualified, &pk).await?;
        let cast_select = snapshot::build_cast_select(client, qualified, &pk).await?;
        state.tables.insert(
            qualified.to_owned(),
            TableRefetchMeta {
                pk,
                pk_types,
                cast_select,
            },
        );
    }
    let meta = state
        .tables
        .get(qualified)
        .ok_or_else(|| PostgresCdcError::Internal("pk cache miss".into()))?;
    let (pk, types, cast_select) = (&meta.pk, &meta.pk_types, &meta.cast_select);
    if pk.len() != components.len() {
        return Err(PostgresCdcError::Internal(format!(
            "doc id arity {} != pk arity {} for {qualified}",
            components.len(),
            pk.len()
        )));
    }
    let filter = pk
        .iter()
        .zip(types.iter())
        .enumerate()
        .map(|(i, (col, ty))| {
            format!(
                "{} = CAST(${}::text AS {ty})",
                snapshot::quote_ident(col),
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT to_jsonb(t.*) FROM (SELECT {cast_select} FROM {qualified}) t          WHERE {filter} LIMIT 1"
    );
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = components
        .iter()
        .map(|c| c as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let row = client
        .query_opt(&sql, &params)
        .await
        .map_err(|err| PostgresCdcError::Connection(format!("toast refetch: {err}")))?;
    Ok(row.map(|row| snapshot::stringify_top_level(row.get(0))))
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

/// Postgres auth failures that cannot heal in-process, matched on the
/// rendered error. Covers the replication client's server errors
/// ("SQLSTATE 28000"/"SQLSTATE 28P01"), tokio-postgres db errors after
/// [`super::connection::describe_db_error`] expansion (same SQLSTATE
/// text), and pgwire's client-side credential rejection, which renders
/// as "authentication error: ..." with no SQLSTATE.
pub fn is_credential_message(message: &str) -> bool {
    crate::credential::is_postgres_credential_text(message)
}

/// Missing replication slot at connect (server ERRCODE 42704, rendered
/// as text by the replication client). Retrying cannot fix it.
pub fn is_missing_slot_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("replication slot") && lowered.contains("does not exist")
}

/// Operator-facing message for a vanished slot. Managed providers
/// (Supabase, RDS blue/green) drop logical slots on engine upgrades
/// and project pauses, so name that cause and the way out.
fn missing_slot_message(config: &PostgresCdcConfig, cause: &str) -> String {
    let heal = if config.bootstrap.is_some() {
        "restarting will re-bootstrap and recreate it".to_owned()
    } else {
        format!(
            "recreate it (SELECT pg_create_logical_replication_slot('{}', 'pgoutput')) \
             or configure bootstrap",
            config.slot_name
        )
    };
    format!(
        "replication slot \"{}\" does not exist on the server: {cause}. If it existed \
         before, the provider likely dropped it (managed Postgres drops logical slots \
         on upgrades and pauses); {heal}",
        config.slot_name
    )
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

    #[test]
    fn transaction_buffer_preserves_in_memory_wal_order() {
        let directory = std::env::temp_dir().join(format!(
            "ventstream-pg-tx-memory-{}",
            TRANSACTION_SPOOL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut buffer = TransactionBuffer::new(directory);
        for wal_start in [11, 12, 13] {
            buffer
                .push(WalRecord {
                    wal_start,
                    data: Bytes::from(vec![u8::try_from(wal_start).expect("test byte")]),
                })
                .expect("buffer record");
        }
        assert!(!buffer.is_spilled());
        let mut observed = Vec::new();
        while let Some(record) = buffer.next_record().expect("read record") {
            observed.push((record.wal_start, record.data[0]));
        }
        assert_eq!(observed, vec![(11, 11), (12, 12), (13, 13)]);
    }

    #[test]
    fn large_transaction_spills_and_removes_its_private_file() {
        let directory = std::env::temp_dir().join(format!(
            "ventstream-pg-tx-spill-{}",
            TRANSACTION_SPOOL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut buffer = TransactionBuffer::new(directory.clone());
        for wal_start in 1..=5 {
            buffer
                .push(WalRecord {
                    wal_start,
                    data: Bytes::from(vec![
                        u8::try_from(wal_start).expect("test byte");
                        2 * 1024 * 1024
                    ]),
                })
                .expect("buffer record");
        }
        assert!(buffer.is_spilled());
        let spool_path = buffer.spool_path().expect("spool path").to_path_buf();
        assert!(spool_path.exists());

        let mut observed = Vec::new();
        while let Some(record) = buffer.next_record().expect("read record") {
            observed.push((record.wal_start, record.data[0]));
        }
        assert_eq!(observed, vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]);
        drop(buffer);
        assert!(!spool_path.exists());
        let _ = std::fs::remove_dir(directory);
    }

    #[test]
    fn sqlstate_28_class_is_credential_and_network_failures_are_transient() {
        // Shape produced by pgwire-replication's error-response parser.
        assert!(is_credential_message(
            "server error: password authentication failed for user \"ventstream\" (SQLSTATE 28P01)"
        ));
        assert!(is_credential_message(
            "server error: role \"ventstream\" is not permitted to log in (SQLSTATE 28000)"
        ));
        // pgwire's client-side rejection carries no SQLSTATE.
        assert!(is_credential_message(
            "authentication error: SCRAM authentication failed"
        ));
        assert!(!is_credential_message(
            "io error: Connection refused (os error 61)"
        ));
        assert!(!is_credential_message(
            "server error: canceling statement due to statement timeout (SQLSTATE 57014)"
        ));
    }

    #[test]
    fn missing_slot_is_terminal_and_other_absences_are_not() {
        assert!(is_missing_slot_message(
            "server error: replication slot \"vs_orders\" does not exist (SQLSTATE 42704)"
        ));
        assert!(!is_missing_slot_message(
            "server error: database \"orders\" does not exist (SQLSTATE 3D000)"
        ));
        assert!(!is_missing_slot_message(
            "server error: replication slot \"vs_orders\" is active for PID 123"
        ));
    }

    #[test]
    fn missing_slot_message_names_the_heal_path() {
        let mut config = PostgresCdcConfig::new("pg", "h", "u", "p", "db", "pub", "vs_orders");
        let plain = missing_slot_message(&config, "cause");
        assert!(plain.contains("pg_create_logical_replication_slot('vs_orders'"));
        config.bootstrap = Some(crate::postgres::SnapshotBootstrap {
            tables: Vec::new(),
            chunk_size: 10_000,
        });
        let with_bootstrap = missing_slot_message(&config, "cause");
        assert!(with_bootstrap.contains("re-bootstrap"));
    }

    #[test]
    fn slot_creation_arm_classifies_once_the_db_error_is_expanded() {
        use super::super::connection::is_credential_sqlstate;

        // The incident shape: tokio-postgres collapsed the auth failure to
        // "db error", which can never classify...
        assert!(!is_credential_message(
            "connect to create replication slot: postgres connection failed: \
             create replication slot: db error"
        ));
        // ...while the describe_db_error expansion of the same failure does.
        assert!(is_credential_message(
            "connect to create replication slot: postgres connection failed: \
             create replication slot: db error (SQLSTATE 28P01): \
             password authentication failed for user \"ventstream\""
        ));
        // Structured extraction: credential SQLSTATEs only.
        assert!(is_credential_sqlstate("28P01"));
        assert!(is_credential_sqlstate("28000"));
        assert!(!is_credential_sqlstate("57014"));
        assert!(!is_credential_sqlstate("28P02"));
    }
}
