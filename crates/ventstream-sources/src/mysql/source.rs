//! Source impl: binlog tail (trigger) + re-`SELECT` (body) + snapshot bootstrap.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Opts, Pool, Row, Value};
use rand::Rng;
use serde_json::{Map, Value as Json};
use tracing::{error, info, warn};
use ventstream_core::{
    doc_id, ContentType, Event, Headers, Payload, Source, SourceContext, SourceError, SourceUri,
    Subject,
};

use super::config::MySqlCdcConfig;
use super::cursor::{BinlogPos, CursorFile, IncompleteKind};
use super::ddl::{parse_ddl, DdlKind};
use super::event_mapper::{self, Op};
use super::schema::{detect_drift, SchemaCache, TableSchema};
use super::value::value_to_json;
use crate::credential::{exhausted_message, CredentialFailureBudget};
use crate::error::MySqlCdcError;

const ACK_SEQUENCE_HEADER: &str = "ventstream.cdc.ack_seq";
const ACK_BARRIER_HEADER: &str = "ventstream.internal.ack_barrier";

/// MySQL/MariaDB CDC source.
pub struct MySqlCdcSource {
    config: MySqlCdcConfig,
    sink_progress: Option<Arc<AtomicU64>>,
    emit_snapshot_completion_marker: bool,
    force_snapshot_from_cursor: bool,
    emit_transition_images: bool,
}

/// One decoded row change, retaining both images and omitted-column markers.
struct RowChange {
    before: Option<Vec<Option<Value>>>,
    after: Option<Vec<Option<Value>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessOutcome {
    completed: bool,
    emitted: bool,
}

/// Reconnect the binlog stream after this long with zero bytes read.
/// Long enough that real idle databases reconnect rarely; short enough
/// that a NAT-killed connection is detected the same operational day
/// it happens, not when someone notices missing data.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

impl MySqlCdcSource {
    /// Construct from configuration.
    pub fn new(config: MySqlCdcConfig) -> Self {
        Self {
            config,
            sink_progress: None,
            emit_snapshot_completion_marker: false,
            force_snapshot_from_cursor: false,
            emit_transition_images: false,
        }
    }

    /// Share the dispatcher's durable sink watermark with the source.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    /// Emit the internal snapshot-complete marker consumed by the in-memory
    /// join engine. Raw and SQL-denormalized pipelines leave this disabled.
    #[must_use]
    pub fn with_snapshot_completion_marker(mut self, enabled: bool) -> Self {
        self.emit_snapshot_completion_marker = enabled;
        self
    }

    /// Rebuild local join state from current rows while retaining the
    /// persisted binlog position for subsequent replay.
    #[must_use]
    pub fn with_forced_snapshot_from_cursor(mut self, enabled: bool) -> Self {
        self.force_snapshot_from_cursor = enabled;
        self
    }

    /// Include update pre-images for SQL recomposition.
    #[must_use]
    pub fn with_transition_images(mut self, enabled: bool) -> Self {
        self.emit_transition_images = enabled;
        self
    }

    fn opts(&self) -> Opts {
        self.config.opts()
    }

    async fn run_inner(&self, ctx: SourceContext) -> Result<(), MySqlCdcError> {
        let pool = Pool::new(self.opts());
        // Fail fast on the one server setting that turns this source
        // into a silent no-op (binlog_format != ROW).
        {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| MySqlCdcError::Connection(format!("preflight connect: {e}")))?;
            super::preflight::check(&mut conn).await?;
            super::preflight::warn_divergent_pk_types(&mut conn, &self.config.database).await?;
        }
        let cursor_file = CursorFile::new(&self.config.state_dir)?;
        let mut next_ack_sequence = 1u64;

        let incomplete = cursor_file.incomplete_kind()?;
        let persisted = match incomplete {
            Some(IncompleteKind::Bootstrap) => {
                warn!(source = %self.config.id, "prior bootstrap not sink-confirmed; re-bootstrapping");
                cursor_file.delete()?;
                None
            }
            Some(IncompleteKind::StateRebuild) => {
                warn!(
                    source = %self.config.id,
                    "prior local-state rebuild was interrupted; retaining cursor and rebuilding again"
                );
                Some(cursor_file.read()?.ok_or_else(|| {
                    MySqlCdcError::CursorIo(
                        "state-rebuild marker exists without a retained binlog cursor".to_owned(),
                    )
                })?)
            }
            None => cursor_file.read()?,
        };
        let rebuilding_state =
            self.force_snapshot_from_cursor || incomplete == Some(IncompleteKind::StateRebuild);

        // Start position: resume from the persisted one, else snapshot the
        // current master position before the bootstrap scan so the tail
        // picks up exactly where the snapshot was consistent.
        let start = match persisted {
            Some(pos) if !rebuilding_state => pos,
            persisted => {
                let has_retained_cursor = persisted.is_some();
                let pos = match persisted {
                    Some(pos) => pos,
                    None => master_pos(&pool).await?,
                };
                if self.config.bootstrap || rebuilding_state {
                    let marker = if has_retained_cursor {
                        IncompleteKind::StateRebuild
                    } else {
                        IncompleteKind::Bootstrap
                    };
                    cursor_file.mark_incomplete(marker)?;
                    info!(source = %self.config.id, database = %self.config.database, "bootstrap: scanning tables");
                    if !self.bootstrap(&pool, &ctx).await? {
                        return Ok(()); // cancelled mid-scan; sentinel stays
                    }
                    if self.emit_snapshot_completion_marker
                        && !self
                            .publish(&ctx, event_mapper::snapshot_complete(&self.config)?)
                            .await?
                    {
                        return Ok(());
                    }
                    if let Some(progress) = &self.sink_progress {
                        let ack_sequence = allocate_ack_sequence(&mut next_ack_sequence)?;
                        if !self.publish_ack_barrier(&ctx, ack_sequence).await? {
                            return Ok(());
                        }
                        if !wait_for_sink_progress(progress, ack_sequence, &ctx).await {
                            return Ok(());
                        }
                    }
                    cursor_file.write(&pos)?;
                    cursor_file.clear_incomplete()?;
                    info!(source = %self.config.id, "bootstrap complete");
                }
                pos
            }
        };

        // Losing the retained position cannot be repaired by taking another
        // snapshot: rows deleted before that snapshot would remain in the
        // sink. Fail closed until the operator performs a full sink
        // reconciliation or a controlled drain/reset.
        self.tail_with_reconnect(&pool, &cursor_file, start, &ctx, &mut next_ack_sequence)
            .await
    }

    async fn tail_with_reconnect(
        &self,
        pool: &Pool,
        cursor_file: &CursorFile,
        start: BinlogPos,
        ctx: &SourceContext,
        next_ack_sequence: &mut u64,
    ) -> Result<(), MySqlCdcError> {
        const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);
        const HEALTHY_THRESHOLD: Duration = Duration::from_secs(30);

        let mut resume = start;
        let mut consecutive_failures = 0u32;
        let mut backoff = INITIAL_BACKOFF;
        let mut credential_budget = CredentialFailureBudget::new();

        loop {
            if ctx.shutdown.is_cancelled() {
                return Ok(());
            }
            let connected_at = Instant::now();
            match self
                .tail(pool, cursor_file, resume.clone(), ctx, next_ack_sequence)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error @ MySqlCdcError::Connection(_)) => {
                    if is_preflight_misconfig(&error) {
                        // A definitive server misconfiguration (e.g. a live
                        // flip to binlog_format=STATEMENT) can't be retried
                        // away — halting mirrors the credential-exhaustion
                        // path so the operator sees it instead of an
                        // infinite backoff loop.
                        error!(source = %self.config.id, error = %error,
                               "server misconfiguration detected on reconnect; halting");
                        return Err(error);
                    }
                    if connected_at.elapsed() >= HEALTHY_THRESHOLD {
                        consecutive_failures = 0;
                        backoff = INITIAL_BACKOFF;
                        // A sustained healthy stream proves the current
                        // credentials worked — reset the credential streak.
                        credential_budget.record_success();
                    }
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if is_credential_error(&error) && credential_budget.record_credential_failure()
                    {
                        let message = exhausted_message(&error);
                        error!(source = %self.config.id, error = %error, "{message}");
                        return Err(MySqlCdcError::Connection(message));
                    }
                    ventstream_telemetry::record_error(format!(
                        "mysql binlog connection dropped: {error}"
                    ));

                    // The cursor contains only sink-confirmed progress. Any
                    // events between it and the dropped connection are replayed
                    // with stable document IDs and external ordering.
                    if let Some(persisted) = cursor_file.read()? {
                        resume = persisted;
                    }
                    let delay = jittered_delay(backoff);
                    warn!(
                        source = %self.config.id,
                        attempt = consecutive_failures,
                        backoff_ms = delay.as_millis() as u64,
                        file = %resume.file,
                        pos = resume.pos,
                        error = %error,
                        "mysql binlog connection dropped; reconnecting from sink-confirmed cursor"
                    );
                    tokio::select! {
                        biased;
                        () = ctx.shutdown.cancelled() => return Ok(()),
                        () = tokio::time::sleep(delay) => {}
                    }
                    backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Keyset-paginated snapshot of every in-scope table. `false` on shutdown.
    async fn bootstrap(&self, pool: &Pool, ctx: &SourceContext) -> Result<bool, MySqlCdcError> {
        let schema_cache = SchemaCache::new();
        let mut conn = pool.get_conn().await.map_err(op_err)?;
        let tables: Vec<String> = conn
            .exec(
                "SELECT TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
                (&self.config.database,),
            )
            .await
            .map_err(op_err)?;
        drop(conn);

        for table in tables {
            if !self.config.table_allowed(&table) {
                continue;
            }
            let schema = schema_cache
                .get(pool, &self.config.database, &table)
                .await?;
            if schema.pk_names.is_empty() {
                warn!(table = %table, "no primary key; skipping (raw 1:1 needs a PK)");
                continue;
            }
            let order = schema
                .pk_names
                .iter()
                .map(|n| format!("`{}`", esc(n)))
                .collect::<Vec<_>>()
                .join(",");
            let qualified = format!("`{}`.`{}`", esc(&self.config.database), esc(&table));
            let limit = self.config.bootstrap_chunk_size.max(1);

            // Prefetch: the next chunk's SELECT runs on its own pooled
            // connection while the current chunk emits.
            let mut rows = fetch_bootstrap_chunk(pool, &qualified, &order, limit, None).await?;
            loop {
                if rows.is_empty() {
                    break;
                }
                let full_page = rows.len() >= limit;
                let page_last = rows
                    .last()
                    .map(|row| pk_values_from_row(row, &schema.pk_names));
                let fetch_next = async {
                    match (&page_last, full_page) {
                        (Some(last), true) => Some(
                            fetch_bootstrap_chunk(pool, &qualified, &order, limit, Some(last))
                                .await,
                        ),
                        _ => None,
                    }
                };
                let emit_chunk = async {
                    for row in &rows {
                        if ctx.shutdown.is_cancelled() {
                            return Ok::<bool, MySqlCdcError>(false);
                        }
                        let pk_vals = pk_values_from_row(row, &schema.pk_names);
                        let pk = pk_components(&pk_vals);
                        let doc = row_to_json(row, &schema.json_columns);
                        let ev = event_mapper::snapshot_insert(&self.config, &table, &pk, doc)?;
                        if !self.publish(ctx, ev).await? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                };
                let (emit_result, next) = tokio::join!(emit_chunk, fetch_next);
                if !emit_result? {
                    return Ok(false);
                }
                match next {
                    Some(fetched) => rows = fetched?,
                    None => break,
                }
            }
        }
        Ok(true)
    }

    /// React to statement-level DDL in the binlog. Recognized DDL against a
    /// tracked table evicts the cached schema and stale TABLE_MAP metadata so
    /// the next rows event decodes against a fresh `INFORMATION_SCHEMA`
    /// fetch, then reports column-level drift. Warn-only: the pipeline keeps
    /// flowing; unrecognized statements are ignored.
    async fn handle_query_event(
        &self,
        pool: &Pool,
        schema_cache: &SchemaCache,
        table_map: &mut HashMap<u64, mysql_async::binlog::events::TableMapEvent<'static>>,
        default_db: &str,
        sql: &str,
    ) {
        let Some(statement) = parse_ddl(sql, default_db) else {
            return;
        };
        for (db, table) in &statement.tables {
            if db != &self.config.database || !self.config.table_allowed(table) {
                continue;
            }
            let table_label = format!("{db}.{table}");
            if statement.kind == DdlKind::Truncate {
                tracing::warn!(table = %table_label, "TRUNCATE observed in binlog");
                metrics::counter!("vs_truncate_events_total", "table" => table_label.clone())
                    .increment(1);
                continue;
            }
            // Stale TABLE_MAP metadata must never pair with the new schema.
            table_map.retain(|_, tme| !(tme.database_name() == *db && tme.table_name() == *table));
            let old = schema_cache.invalidate(db, table);
            match statement.kind {
                DdlKind::Drop => {
                    tracing::info!(table = %table_label, "tracked table dropped; schema cache evicted");
                }
                _ => match schema_cache.get(pool, db, table).await {
                    Ok(new) => {
                        if let Some(old) = old {
                            detect_drift(db, table, &old, &new);
                        }
                    }
                    Err(error) => {
                        // The refetch will be retried lazily by the next rows
                        // event; the eviction alone already prevents decoding
                        // against the stale layout.
                        tracing::warn!(table = %table_label, %error, "schema refetch after DDL failed");
                    }
                },
            }
        }
    }

    async fn tail(
        &self,
        pool: &Pool,
        cursor_file: &CursorFile,
        start: BinlogPos,
        ctx: &SourceContext,
        next_ack_sequence: &mut u64,
    ) -> Result<(), MySqlCdcError> {
        // Re-checked per (re)connect: a live `SET GLOBAL binlog_format`
        // flip must not silently starve the stream.
        {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| MySqlCdcError::Connection(format!("preflight connect: {e}")))?;
            super::preflight::check(&mut conn).await?;
        }
        info!(
            source = %self.config.id,
            file = %start.file, pos = start.pos,
            flush_ms = self.config.pos_flush_interval.as_millis() as u64,
            "tailing binlog"
        );
        let schema_cache = SchemaCache::new();
        let conn = Conn::new(self.opts()).await.map_err(conn_err)?;
        let req = BinlogStreamRequest::new(self.config.server_id)
            .with_filename(start.file.as_bytes())
            .with_pos(start.pos);
        let mut stream = conn.get_binlog_stream(req).await.map_err(stream_err)?;
        ventstream_telemetry::clear_error();
        ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Tailing);

        let mut table_map: HashMap<u64, TableMapEvent<'static>> = HashMap::new();
        let mut pos = start;
        let mut pending_gated: VecDeque<(u64, BinlogPos)> = VecDeque::new();
        let mut pending_write: Option<BinlogPos> = None;
        let mut flush = tokio::time::interval(self.config.pos_flush_interval);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    flush_confirmed_positions(
                        self.sink_progress.as_ref(),
                        cursor_file,
                        &mut pending_gated,
                        &mut pending_write,
                    )?;
                    return Ok(());
                }
                _ = flush.tick() => {
                    flush_confirmed_positions(
                        self.sink_progress.as_ref(),
                        cursor_file,
                        &mut pending_gated,
                        &mut pending_write,
                    )?;
                }
                next = tokio::time::timeout(READ_IDLE_TIMEOUT, stream.next()) => {
                    let Ok(next) = next else {
                        // No bytes for the whole window: either the DB is
                        // truly idle (we do not request MASTER_HEARTBEAT,
                        // so silence is expected there — the reconnect is
                        // cheap and the backoff resets each healthy cycle)
                        // or a NAT/LB silently killed the TCP stream —
                        // which otherwise hangs this loop forever with the
                        // source looking healthy. Reconnect through the
                        // normal path either way.
                        return Err(MySqlCdcError::Connection(format!(
                            "no binlog traffic for {}s; reconnecting (idle stream or dead \
                             connection)",
                            READ_IDLE_TIMEOUT.as_secs()
                        )));
                    };
                    let Some(event) = next else {
                        flush_confirmed_positions(
                            self.sink_progress.as_ref(),
                            cursor_file,
                            &mut pending_gated,
                            &mut pending_write,
                        )?;
                        return Err(MySqlCdcError::Connection(
                            "binlog stream ended before shutdown".to_owned(),
                        ));
                    };
                    let event = match event {
                        Ok(event) => event,
                        Err(error) => {
                            flush_confirmed_positions(
                                self.sink_progress.as_ref(),
                                cursor_file,
                                &mut pending_gated,
                                &mut pending_write,
                            )?;
                            return Err(stream_err(error));
                        }
                    };
                    let log_pos = u64::from(event.header().log_pos());

                    // Decode synchronously (borrows the event), then do the
                    // async re-SELECT/publish on owned data.
                    let decoded = match event.read_data().map_err(|e| {
                        MySqlCdcError::MalformedEvent(format!("binlog read_data: {e}"))
                    })? {
                        Some(EventData::RotateEvent(r)) => {
                            pos.file = sanitize_binlog_filename(&r.name());
                            pos.pos = r.position();
                            None
                        }
                        Some(EventData::TableMapEvent(tme)) => {
                            table_map.insert(tme.table_id(), tme.into_owned());
                            None
                        }
                        Some(EventData::RowsEvent(rows)) => decode_rows(&rows, &table_map)?,
                        Some(EventData::QueryEvent(query)) => {
                            self.handle_query_event(
                                pool,
                                &schema_cache,
                                &mut table_map,
                                &query.schema(),
                                &query.query(),
                            )
                            .await;
                            None
                        }
                        _ => None,
                    };

                    let mut emitted = false;
                    if let Some(batch) = decoded {
                        if batch.db == self.config.database && self.config.table_allowed(&batch.table) {
                            let outcome = self
                                .process(pool, &schema_cache, batch, ctx)
                                .await?;
                            if !outcome.completed {
                                flush_confirmed_positions(
                                    self.sink_progress.as_ref(),
                                    cursor_file,
                                    &mut pending_gated,
                                    &mut pending_write,
                                )?;
                                return Ok(());
                            }
                            emitted = outcome.emitted;
                        }
                    }
                    if log_pos != 0 {
                        pos.pos = log_pos;
                        if self.sink_progress.is_some() {
                            collect_confirmed_positions(
                                self.sink_progress.as_ref(),
                                &mut pending_gated,
                                &mut pending_write,
                            );
                            if emitted {
                                let ack_sequence =
                                    allocate_ack_sequence(next_ack_sequence)?;
                                if !self.publish_ack_barrier(ctx, ack_sequence).await? {
                                    flush_confirmed_positions(
                                        self.sink_progress.as_ref(),
                                        cursor_file,
                                        &mut pending_gated,
                                        &mut pending_write,
                                    )?;
                                    return Ok(());
                                }
                                pending_gated.push_back((ack_sequence, pos.clone()));
                            } else if let Some((_, pending_position)) = pending_gated.back_mut() {
                                // No sink work for this record. Its position can
                                // ride on the latest outstanding barrier because
                                // that barrier follows every earlier output.
                                *pending_position = pos.clone();
                            } else {
                                pending_write = Some(pos.clone());
                            }
                        } else {
                            pending_write = Some(pos.clone());
                        }
                    }
                }
            }
        }
    }

    /// re-SELECT each upsert's row and publish; tombstone deletes.
    async fn process(
        &self,
        pool: &Pool,
        schema_cache: &SchemaCache,
        batch: RowBatch,
        ctx: &SourceContext,
    ) -> Result<ProcessOutcome, MySqlCdcError> {
        let RowBatch {
            db,
            table,
            op,
            rows,
        } = batch;
        let schema = schema_cache.get(pool, &db, &table).await?;
        if schema.pk_names.is_empty() {
            return Ok(ProcessOutcome {
                completed: true,
                emitted: false,
            }); // unkeyed table — skip
        }
        let mut emitted = false;
        for change in rows {
            let before_doc = row_image_to_json(change.before.as_deref(), &schema);
            let pk_values: Vec<Value> = schema
                .pk_ordinals
                .iter()
                .map(|&ordinal| {
                    row_change_value(&change, op, ordinal)
                        .cloned()
                        .unwrap_or(Value::NULL)
                })
                .collect();
            let pk = pk_components_binlog(&pk_values, &schema.pk_ordinals, &schema);
            let ev = if op.is_delete() {
                event_mapper::change_event(&self.config, &table, op, &pk, Some(before_doc))?
            } else {
                let qualified = format!("`{}`.`{}`", esc(&db), esc(&table));
                let where_clause = schema
                    .pk_names
                    .iter()
                    .map(|n| format!("`{}` <=> ?", esc(n)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let sql = format!("SELECT * FROM {qualified} WHERE {where_clause}");
                let mut conn = pool.get_conn().await.map_err(op_err)?;
                let row: Option<Row> = conn.exec_first(sql, pk_values).await.map_err(op_err)?;
                drop(conn);
                match row {
                    Some(r) if self.emit_transition_images && matches!(op, Op::Update) => {
                        event_mapper::change_event_with_transition(
                            &self.config,
                            &table,
                            op,
                            &pk,
                            Some(row_to_json(&r, &schema.json_columns)),
                            Some(before_doc),
                        )?
                    }
                    Some(r) => event_mapper::change_event(
                        &self.config,
                        &table,
                        op,
                        &pk,
                        Some(row_to_json(&r, &schema.json_columns)),
                    )?,
                    None => continue, // row gone before re-read; a delete will follow
                }
            };
            if !self.publish(ctx, ev).await? {
                return Ok(ProcessOutcome {
                    completed: false,
                    emitted,
                });
            }
            emitted = true;
        }
        Ok(ProcessOutcome {
            completed: true,
            emitted,
        })
    }

    async fn publish(&self, ctx: &SourceContext, event: Event) -> Result<bool, MySqlCdcError> {
        match ctx.sender.send(event, &ctx.shutdown).await {
            Ok(()) => Ok(true),
            Err(ventstream_core::BackpressureError::Cancelled) => Ok(false),
            Err(e) => Err(MySqlCdcError::Internal(format!("bus publish failed: {e}"))),
        }
    }

    async fn publish_ack_barrier(
        &self,
        ctx: &SourceContext,
        ack_sequence: u64,
    ) -> Result<bool, MySqlCdcError> {
        let source = SourceUri::new(format!("mysql-cdc://{}", self.config.id))
            .map_err(|e| MySqlCdcError::Internal(format!("ack barrier source: {e}")))?;
        let subject = Subject::new("ventstream.internal.ack_barrier")
            .map_err(|e| MySqlCdcError::Internal(format!("ack barrier subject: {e}")))?;
        let mut headers = HashMap::new();
        headers.insert(ACK_BARRIER_HEADER.to_owned(), "true".to_owned());
        headers.insert(ACK_SEQUENCE_HEADER.to_owned(), ack_sequence.to_string());
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();
        self.publish(ctx, event).await
    }
}

#[async_trait]
impl Source for MySqlCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn kind(&self) -> &'static str {
        "mysql_cdc"
    }
    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
        self.run_inner(ctx).await.map_err(|error| match error {
            MySqlCdcError::Connection(message) => SourceError::Connection(message),
            MySqlCdcError::MalformedEvent(message) => SourceError::Decode(message),
            other => SourceError::Internal(other.to_string()),
        })
    }
}

/// A decoded rows event: target table + op + the per-row column images.
struct RowBatch {
    db: String,
    table: String,
    op: Op,
    rows: Vec<RowChange>,
}

/// Decode a rows event. Sync — borrows the event; returns owned data.
fn decode_rows(
    rows: &RowsEventData<'_>,
    table_map: &HashMap<u64, TableMapEvent<'static>>,
) -> Result<Option<RowBatch>, MySqlCdcError> {
    let Some(tme) = table_map.get(&rows.table_id()) else {
        warn!(
            table_id = rows.table_id(),
            "rows event before its table-map; skipping"
        );
        return Ok(None);
    };
    let db = tme.database_name().into_owned();
    let table = tme.table_name().into_owned();
    let op = match rows {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => Op::Insert,
        RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => Op::Update,
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => Op::Delete,
        _ => return Ok(None),
    };
    let mut out = Vec::new();
    for row in rows.rows(tme) {
        let (before, after) =
            row.map_err(|e| MySqlCdcError::MalformedEvent(format!("binlog row: {e}")))?;
        let before = before.map(|image| {
            (0..image.len())
                .map(|index| match image.as_ref(index) {
                    Some(BinlogValue::Value(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect()
        });
        let after = after.map(|image| {
            (0..image.len())
                .map(|index| match image.as_ref(index) {
                    Some(BinlogValue::Value(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect()
        });
        if before.is_some() || after.is_some() {
            out.push(RowChange { before, after });
        }
    }
    Ok(Some(RowBatch {
        db,
        table,
        op,
        rows: out,
    }))
}

/// One keyset-paginated bootstrap chunk on its own pooled connection.
/// Always the binary protocol so column types (and thus JSON shapes) match
/// the re-SELECT path; the text protocol returns every value as a string.
async fn fetch_bootstrap_chunk(
    pool: &Pool,
    qualified: &str,
    order: &str,
    limit: usize,
    last: Option<&Vec<Value>>,
) -> Result<Vec<Row>, MySqlCdcError> {
    let sql = match last {
        None => format!("SELECT * FROM {qualified} ORDER BY {order} LIMIT {limit}"),
        Some(vals) => {
            let ph = vec!["?"; vals.len()].join(",");
            format!(
                "SELECT * FROM {qualified} WHERE ({order}) > ({ph}) ORDER BY {order} LIMIT {limit}"
            )
        }
    };
    let mut conn = pool.get_conn().await.map_err(op_err)?;
    let rows: Vec<Row> = match last {
        None => conn.exec(sql, ()).await.map_err(op_err)?,
        Some(vals) => conn.exec(sql, vals.clone()).await.map_err(op_err)?,
    };
    drop(conn);
    Ok(rows)
}

fn pk_values_from_row(row: &Row, pk_names: &[String]) -> Vec<Value> {
    pk_names
        .iter()
        .map(|n| {
            row.get_opt::<Value, _>(n.as_str())
                .and_then(Result::ok)
                .unwrap_or(Value::NULL)
        })
        .collect()
}

fn pk_components(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .map(|v| doc_id::component_text(&value_to_json(v, false)))
        .collect()
}

/// PK components from binlog values, with ENUM/SET indexes normalized
/// to their labels so binlog-path doc ids match the SELECT-path ids
/// byte for byte (a diverging PK text means deletes orphan documents).
fn pk_components_binlog(values: &[Value], ordinals: &[usize], schema: &TableSchema) -> Vec<String> {
    values
        .iter()
        .zip(ordinals.iter())
        .map(|(value, &ordinal)| {
            let normalized = normalize_binlog_value(value, ordinal, schema);
            doc_id::component_text(&value_to_json(&normalized, false))
        })
        .collect()
}

/// Rewrite a binlog ENUM index (1-based; 0 = the empty invalid value)
/// or SET bitmask into the label text the SELECT paths return. Other
/// values pass through untouched. Out-of-range indexes (drifted enum
/// definition) fall back to the raw number rather than guessing.
fn normalize_binlog_value(value: &Value, ordinal: usize, schema: &TableSchema) -> Value {
    let index = match value {
        Value::Int(i) if *i >= 0 => *i as u64,
        Value::UInt(u) => *u,
        _ => return value.clone(),
    };
    if let Some(labels) = schema.enum_labels.get(&ordinal) {
        if index == 0 {
            return Value::Bytes(Vec::new());
        }
        if let Some(label) = labels.get((index - 1) as usize) {
            return Value::Bytes(label.clone().into_bytes());
        }
        return value.clone();
    }
    if let Some(labels) = schema.set_labels.get(&ordinal) {
        let mut parts = Vec::new();
        for (bit, label) in labels.iter().enumerate() {
            if bit < 64 && index & (1u64 << bit) != 0 {
                parts.push(label.as_str());
            }
        }
        return Value::Bytes(parts.join(",").into_bytes());
    }
    value.clone()
}

fn row_change_value(change: &RowChange, op: Op, ordinal: usize) -> Option<&Value> {
    let before = || {
        change
            .before
            .as_ref()
            .and_then(|image| image.get(ordinal))
            .and_then(Option::as_ref)
    };
    let after = || {
        change
            .after
            .as_ref()
            .and_then(|image| image.get(ordinal))
            .and_then(Option::as_ref)
    };
    match op {
        Op::Insert => after(),
        Op::Update => after().or_else(before),
        Op::Delete => before(),
    }
}

fn row_image_to_json(image: Option<&[Option<Value>]>, schema: &TableSchema) -> Json {
    let mut map = Map::new();
    let Some(image) = image else {
        return Json::Object(map);
    };
    for (ordinal, value) in image.iter().enumerate() {
        let (Some(value), Some(name)) = (value.as_ref(), schema.column_names.get(ordinal)) else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        let normalized = normalize_binlog_value(value, ordinal, schema);
        map.insert(
            name.clone(),
            value_to_json(&normalized, schema.json_columns.contains(name)),
        );
    }
    Json::Object(map)
}

fn row_to_json(row: &Row, json_columns: &std::collections::HashSet<String>) -> Json {
    let mut map = Map::new();
    for (i, col) in row.columns_ref().iter().enumerate() {
        let name = col.name_str().into_owned();
        let v = row.as_ref(i).cloned().unwrap_or(Value::NULL);
        let is_json = json_columns.contains(&name);
        map.insert(name, value_to_json(&v, is_json));
    }
    Json::Object(map)
}

async fn master_pos(pool: &Pool) -> Result<BinlogPos, MySqlCdcError> {
    let mut conn = pool.get_conn().await.map_err(conn_err)?;
    // `SHOW MASTER STATUS` is deprecated in MySQL 8.4+ (renamed
    // `SHOW BINARY LOG STATUS`); try the new name first, fall back for older
    // MySQL / MariaDB. Both expose the same `File`/`Position` columns.
    let row: Option<Row> = match conn.query_first("SHOW BINARY LOG STATUS").await {
        Ok(r) => r,
        Err(_) => conn
            .query_first("SHOW MASTER STATUS")
            .await
            .map_err(op_err)?,
    };
    let row = row.ok_or_else(|| {
        MySqlCdcError::Operation("binary log status empty — is binlog enabled?".to_owned())
    })?;
    let file: String = row
        .get("File")
        .ok_or_else(|| MySqlCdcError::Operation("binary log status missing File".to_owned()))?;
    let pos: u64 = row
        .get("Position")
        .ok_or_else(|| MySqlCdcError::Operation("binary log status missing Position".to_owned()))?;
    Ok(BinlogPos { file, pos })
}

/// True when a Connection error is really a definitive server
/// misconfiguration surfaced by preflight (same string-classification
/// idiom as `is_credential_error`).
fn is_preflight_misconfig(error: &MySqlCdcError) -> bool {
    error
        .to_string()
        .contains("row-based replication requires 'ROW'")
}

fn allocate_ack_sequence(next: &mut u64) -> Result<u64, MySqlCdcError> {
    let sequence = *next;
    *next = next.checked_add(1).ok_or_else(|| {
        MySqlCdcError::Internal("MySQL acknowledgement sequence exhausted".into())
    })?;
    Ok(sequence)
}

async fn wait_for_sink_progress(progress: &AtomicU64, expected: u64, ctx: &SourceContext) -> bool {
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if progress.load(Ordering::Acquire) >= expected {
            return true;
        }
        tokio::select! {
            biased;
            () = ctx.shutdown.cancelled() => return false,
            _ = poll.tick() => {}
        }
    }
}

fn flush_confirmed_positions(
    sink_progress: Option<&Arc<AtomicU64>>,
    cursor_file: &CursorFile,
    pending_gated: &mut VecDeque<(u64, BinlogPos)>,
    pending_write: &mut Option<BinlogPos>,
) -> Result<(), MySqlCdcError> {
    collect_confirmed_positions(sink_progress, pending_gated, pending_write);
    if let Some(p) = pending_write.take() {
        cursor_file.write(&p)?;
    }
    Ok(())
}

fn collect_confirmed_positions(
    sink_progress: Option<&Arc<AtomicU64>>,
    pending_gated: &mut VecDeque<(u64, BinlogPos)>,
    confirmed: &mut Option<BinlogPos>,
) {
    let Some(progress) = sink_progress else {
        return;
    };
    let durable = progress.load(Ordering::Acquire);
    while pending_gated
        .front()
        .is_some_and(|(ack_sequence, _)| *ack_sequence <= durable)
    {
        *confirmed = pending_gated.pop_front().map(|(_, position)| position);
    }
}

fn esc(ident: &str) -> String {
    ident.replace('`', "``")
}

// MariaDB rotate events carry a 4-byte CRC32 checksum that mysql_async strips
// for MySQL but not MariaDB, so the name arrives with trailing garbage bytes.
// Keep the leading run of legal binlog-filename chars (A-Za-z0-9 . _ -); on a
// clean MySQL name this is a no-op.
fn sanitize_binlog_filename(raw: &str) -> String {
    raw.chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect()
}

// Owned `Error` to satisfy `map_err`; only `Display` is used.
#[allow(clippy::needless_pass_by_value)]
fn op_err(e: mysql_async::Error) -> MySqlCdcError {
    if e.is_fatal() {
        MySqlCdcError::Connection(e.to_string())
    } else {
        MySqlCdcError::Operation(e.to_string())
    }
}
#[allow(clippy::needless_pass_by_value)]
fn conn_err(e: mysql_async::Error) -> MySqlCdcError {
    MySqlCdcError::Connection(e.to_string())
}

// Classify a binlog-stream error: ER_MASTER_FATAL_ERROR_READING_BINLOG (1236)
// means the requested position is gone (binlog purged, or a different primary
// after failover). This is classified distinctly so callers can fail closed
// and require reconciliation instead of silently abandoning deletion replay.
#[allow(clippy::needless_pass_by_value)]
fn stream_err(e: mysql_async::Error) -> MySqlCdcError {
    let purged = match &e {
        mysql_async::Error::Server(se) => se.code == 1236,
        _ => {
            let low = e.to_string().to_ascii_lowercase();
            (low.contains("could not find") && low.contains("log")) || low.contains("purged")
        }
    };
    if purged {
        MySqlCdcError::PurgedBinlog(e.to_string())
    } else {
        MySqlCdcError::Connection(format!("binlog stream: {e}"))
    }
}

fn jittered_delay(delay: Duration) -> Duration {
    delay.mul_f64(rand::thread_rng().gen_range(0.5..=1.0))
}

/// MySQL auth failures that cannot heal in-process, on the connection
/// errors the reconnect loop sees. Shares the code list with the
/// supervisory text classifier.
fn is_credential_error(error: &MySqlCdcError) -> bool {
    let MySqlCdcError::Connection(message) = error else {
        return false;
    };
    crate::credential::is_mysql_credential_text(message)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{
        allocate_ack_sequence, flush_confirmed_positions, jittered_delay, pk_components_binlog,
        row_change_value, row_image_to_json, sanitize_binlog_filename, BinlogPos, CursorFile, Op,
        RowChange, TableSchema,
    };
    use mysql_async::Value;
    use std::collections::{HashSet, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn clean_mysql_name_is_unchanged() {
        assert_eq!(
            sanitize_binlog_filename("mysql-bin.000005"),
            "mysql-bin.000005"
        );
        assert_eq!(
            sanitize_binlog_filename("maria-bin.000002"),
            "maria-bin.000002"
        );
    }

    #[test]
    fn mariadb_checksum_bytes_are_stripped() {
        // MariaDB rotate name with a trailing 4-byte CRC32 (07 5b 2c 09).
        let raw = "maria-bin.000002\u{07}[,\u{09}";
        assert_eq!(sanitize_binlog_filename(raw), "maria-bin.000002");
    }

    #[test]
    fn gated_cursor_only_persists_sink_confirmed_prefix() {
        let dir = std::env::temp_dir().join(format!(
            "vs-mysql-gated-cursor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let cursor = CursorFile::new(&dir).expect("cursor");
        let progress = Arc::new(AtomicU64::new(1));
        let mut pending = VecDeque::from([
            (
                1,
                BinlogPos {
                    file: "mysql-bin.000001".into(),
                    pos: 100,
                },
            ),
            (
                2,
                BinlogPos {
                    file: "mysql-bin.000001".into(),
                    pos: 200,
                },
            ),
        ]);
        let mut ungated = None;

        flush_confirmed_positions(Some(&progress), &cursor, &mut pending, &mut ungated)
            .expect("flush confirmed");
        assert_eq!(cursor.read().expect("read").expect("position").pos, 100);
        assert_eq!(pending.len(), 1);

        progress.store(2, Ordering::Release);
        flush_confirmed_positions(Some(&progress), &cursor, &mut pending, &mut ungated)
            .expect("flush confirmed");
        assert_eq!(cursor.read().expect("read").expect("position").pos, 200);
        assert!(pending.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn ack_sequence_allocation_is_monotonic_and_checked() {
        let mut next = 7;
        assert_eq!(allocate_ack_sequence(&mut next).expect("first"), 7);
        assert_eq!(allocate_ack_sequence(&mut next).expect("second"), 8);
        assert_eq!(next, 9);

        let mut exhausted = u64::MAX;
        assert!(allocate_ack_sequence(&mut exhausted).is_err());
    }

    #[test]
    fn reconnect_jitter_stays_bounded() {
        let base = std::time::Duration::from_secs(10);
        for _ in 0..1_000 {
            let delay = jittered_delay(base);
            assert!(delay >= std::time::Duration::from_secs(5));
            assert!(delay <= base);
        }
    }

    #[test]
    fn update_primary_key_uses_after_image_then_before_fallback() {
        let change = RowChange {
            before: Some(vec![
                Some(Value::Bytes(b"old-id".to_vec())),
                Some(Value::Bytes(b"stable".to_vec())),
            ]),
            after: Some(vec![Some(Value::Bytes(b"new-id".to_vec())), None]),
        };
        assert_eq!(
            row_change_value(&change, Op::Update, 0),
            Some(&Value::Bytes(b"new-id".to_vec()))
        );
        assert_eq!(
            row_change_value(&change, Op::Update, 1),
            Some(&Value::Bytes(b"stable".to_vec()))
        );
    }

    fn test_schema(columns: &[&str]) -> TableSchema {
        TableSchema {
            column_names: columns.iter().map(|c| (*c).to_owned()).collect(),
            column_types: vec![String::new(); columns.len()],
            pk_names: Vec::new(),
            pk_ordinals: Vec::new(),
            json_columns: HashSet::new(),
            enum_labels: std::collections::HashMap::new(),
            set_labels: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn enum_and_set_binlog_indexes_render_as_labels() {
        let mut schema = test_schema(&["id", "color", "tags"]);
        schema
            .enum_labels
            .insert(1, vec!["red".into(), "blue".into()]);
        schema
            .set_labels
            .insert(2, vec!["a".into(), "b".into(), "c".into()]);
        let image = vec![
            Some(Value::Bytes(b"7".to_vec())),
            Some(Value::Int(2)),  // enum index 2 -> "blue"
            Some(Value::UInt(5)), // set bits 0+2 -> "a,c"
        ];
        let document = row_image_to_json(Some(&image), &schema);
        assert_eq!(
            document,
            serde_json::json!({"id": "7", "color": "blue", "tags": "a,c"})
        );
        // PK components match the SELECT path's label text.
        let pk = pk_components_binlog(&[Value::Int(2)], &[1], &schema);
        assert_eq!(pk, vec!["blue".to_owned()]);
        // Out-of-range index (drifted definition): raw number, no guess.
        let pk = pk_components_binlog(&[Value::Int(9)], &[1], &schema);
        assert_eq!(pk, vec!["9".to_owned()]);
        // Enum index 0 is MySQL's empty invalid value.
        let pk = pk_components_binlog(&[Value::Int(0)], &[1], &schema);
        assert_eq!(pk, vec![String::new()]);
    }

    #[test]
    fn delete_before_image_preserves_present_columns_and_omits_missing_ones() {
        let image = vec![
            Some(Value::Bytes(b"item-1".to_vec())),
            Some(Value::Bytes(b"order-7".to_vec())),
            None,
        ];
        let document = row_image_to_json(
            Some(&image),
            &test_schema(&["id", "order_id", "description"]),
        );
        assert_eq!(
            document,
            serde_json::json!({"id": "item-1", "order_id": "order-7"})
        );
    }

    #[test]
    fn access_denied_is_credential_and_network_failures_are_transient() {
        use super::is_credential_error;
        use crate::error::MySqlCdcError;

        // Exact shape observed in production from mysql_async.
        let denied = MySqlCdcError::Connection(
            "mysql connection failed: Server error: `ERROR 28000 (1045): Access denied for \
             user 'ventstream'@'10.2.14.7' (using password: YES)`"
                .to_owned(),
        );
        assert!(is_credential_error(&denied));
        let db_denied = MySqlCdcError::Connection(
            "Server error: `ERROR 42000 (1044): Access denied for user 'vs'@'%' to database `app``"
                .to_owned(),
        );
        assert!(is_credential_error(&db_denied));
        let plugin = MySqlCdcError::Connection(
            "Server error: `ERROR 28000 (1698): Access denied for user 'root'@'localhost'`"
                .to_owned(),
        );
        assert!(is_credential_error(&plugin));

        let refused = MySqlCdcError::Connection(
            "mysql connection failed: Connection refused (os error 61)".to_owned(),
        );
        assert!(!is_credential_error(&refused));
        let timeout =
            MySqlCdcError::Connection("mysql connection failed: connection timed out".to_owned());
        assert!(!is_credential_error(&timeout));
        // Non-connection variants never classify.
        assert!(!is_credential_error(&MySqlCdcError::Operation(
            "ERROR 28000 (1045)".to_owned()
        )));
    }
}
