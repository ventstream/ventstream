//! One-time snapshot bootstrap.
//!
//! When the engine starts and snapshot bootstrap is enabled, this module
//! creates the replication slot before SELECTing every row of every configured
//! table and emits each as a synthetic INSERT event through the source's
//! outbound bus. The join engine treats those events identically to WAL-derived
//! inserts, so existing data populates state and reaches the sink the same way
//! new rows do. During a local join-state rebuild, an existing slot is retained
//! and the snapshot is followed by replay from that slot.
//!
//! ## What v1 trades for simplicity
//!
//! v1 is **non-transactional**: the slot is created before the SELECT scan,
//! but the scan itself is not a single transactionally consistent view. A row
//! may therefore be emitted once from the snapshot and again from WAL; the
//! deterministic document ID makes that duplicate harmless, and the WAL copy
//! carries the later value. For the search-index use case:
//!
//! - Pipeline A emits the same `_id` for any later UPDATE of that
//!   row, so the sink eventually overwrites the stale snapshot value.
//! - Subsequent foreign-side touches of the row also re-emit it.
//!
//! For workloads that need *transactional* initial sync (audit, CDC
//! to a system of record, anything requiring no-gap guarantees), v2
//! will use `CREATE_REPLICATION_SLOT ... USE_SNAPSHOT` to read at the
//! consistent point the slot was created at. Tracked as a follow-up.
//!
//! ## When does it run?
//!
//! - `PostgresCdcConfig::bootstrap` is `Some(...)`, AND
//! - the slot named in `slot_name` does not yet exist, unless the runtime is
//!   explicitly rebuilding local join state while retaining that slot.
//!
//! Existing slots normally short-circuit the bootstrap — operators get an
//! explicit log and the engine starts replication normally. The runtime uses
//! the retained-slot exception only when persisted join state and the source
//! checkpoint no longer form a recoverable pair.

use std::time::Instant;

use chrono::Utc;
use serde_json::Value;
use tokio_postgres::Client;
use tracing::{info, warn};
use ventstream_core::{ContentType, Event, Headers, Payload, SourceContext, SourceUri, Subject};

use super::config::{PostgresCdcConfig, SnapshotBootstrap, SnapshotTable};
use super::connection::connect_client;
use crate::error::PostgresCdcError;

/// Re-snapshot a specific set of tables on demand. Skips the
/// slot-existence check entirely — invoked by the admin
/// `/admin/resync` endpoint while WAL streaming is also running.
/// Concurrent emission with the slot's stream is safe: deterministic
/// doc IDs in the sink dedupe by primary-row identity, so a row that
/// arrives via both paths overwrites the same destination doc.
pub async fn resync_tables(
    config: &PostgresCdcConfig,
    tables: &[SnapshotTable],
    chunk_size: usize,
    ctx: &SourceContext,
) -> Result<ResyncStats, PostgresCdcError> {
    let client = connect_client(config, "resync connect").await?;

    info!(tables = tables.len(), chunk_size, "resync starting");
    let start = Instant::now();
    let bootstrap = SnapshotBootstrap {
        tables: tables.to_vec(),
        chunk_size,
    };
    // Version floor: the WAL position at scan start. Every row read by
    // this scan reflects at least this LSN, and any concurrent WAL
    // write commits past it — stamping it as the row's external_gte
    // version means a racing newer WAL write can never be clobbered by
    // a stale resync read landing late (the sink drops the stale write
    // on version conflict instead).
    let version_floor = capture_version_floor(&client).await?;
    let mut total = 0usize;
    for table in tables {
        total += snapshot_table(&client, config, &bootstrap, table, version_floor, ctx).await?;
    }
    let elapsed_ms = start.elapsed().as_millis() as u64;
    info!(total, elapsed_ms, "resync complete");
    Ok(ResyncStats { total, elapsed_ms })
}

/// Aggregate stats returned from a successful resync.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct ResyncStats {
    /// Total rows emitted across all tables.
    pub total: usize,
    /// Wall-clock elapsed in milliseconds.
    pub elapsed_ms: u64,
}

/// Run the snapshot bootstrap if the operator opted in and the slot
/// doesn't already exist. Returns silently when there's nothing to do.
pub(crate) async fn maybe_bootstrap(
    config: &PostgresCdcConfig,
    bootstrap_existing_slot: bool,
    ctx: &SourceContext,
) -> Result<(), PostgresCdcError> {
    let Some(bootstrap) = &config.bootstrap else {
        return Ok(());
    };

    let client = connect_client(config, "snapshot bootstrap connect").await?;

    let existing_slot = slot_exists(&client, &config.slot_name).await?;
    if existing_slot && !bootstrap_existing_slot {
        info!(
            slot = %config.slot_name,
            "snapshot bootstrap skipped — replication slot already exists"
        );
        return Ok(());
    }

    let bootstrap = if bootstrap.tables.is_empty() {
        let tables = fetch_publication_snapshot_tables(&client, &config.publication).await?;
        SnapshotBootstrap {
            tables,
            chunk_size: bootstrap.chunk_size,
        }
    } else {
        bootstrap.clone()
    };
    info!(
        slot = %config.slot_name,
        tables = bootstrap.tables.len(),
        chunk_size = bootstrap.chunk_size,
        "snapshot bootstrap starting"
    );
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Bootstrapping);

    // Create the slot BEFORE the snapshot reads. This way any change
    // that happens during the snapshot window is also captured in the
    // WAL stream — the engine may see the row twice (once from
    // snapshot, once from WAL replay) but the deterministic doc_id in
    // the sink dedupes that case. The alternative — snapshot first,
    // then create slot — drops in-flight changes entirely.
    if existing_slot {
        info!(
            slot = %config.slot_name,
            "rebuilding join state from snapshot while retaining existing replication slot"
        );
    } else {
        create_slot(&client, &config.slot_name).await?;
    }

    let version_floor = capture_version_floor(&client).await?;
    for table in &bootstrap.tables {
        let _ = snapshot_table(&client, config, &bootstrap, table, version_floor, ctx).await?;
    }

    // Sentinel event marking the end of the snapshot phase. The join
    // engine consumes this to flush its in-memory state to disk in
    // one shot and switch back to per-row write-throughs. Sources
    // without a join engine attached treat it as a no-op pass-through.
    let sentinel = build_snapshot_complete_sentinel(config)?;
    if ctx.sender.send(sentinel, &ctx.shutdown).await.is_err() {
        // Shutdown raced the send — fine, nothing to dump.
        return Ok(());
    }

    info!("snapshot bootstrap complete");
    Ok(())
}

async fn fetch_publication_snapshot_tables(
    client: &Client,
    publication: &str,
) -> Result<Vec<SnapshotTable>, PostgresCdcError> {
    let rows = client
        .query(
            "SELECT schemaname, tablename \
             FROM pg_publication_tables \
             WHERE pubname = $1 \
             ORDER BY schemaname, tablename",
            &[&publication],
        )
        .await
        .map_err(|err| {
            PostgresCdcError::Connection(format!(
                "listing publication tables for {publication}: {err}"
            ))
        })?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.get(0);
        let name: String = row.get(1);
        let pk_rows = client
            .query(
                "SELECT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON kcu.constraint_schema = tc.constraint_schema \
                  AND kcu.constraint_name = tc.constraint_name \
                  AND kcu.table_schema = tc.table_schema \
                  AND kcu.table_name = tc.table_name \
                 WHERE tc.constraint_type = 'PRIMARY KEY' \
                   AND tc.table_schema = $1 \
                   AND tc.table_name = $2 \
                 ORDER BY kcu.ordinal_position",
                &[&namespace, &name],
            )
            .await
            .map_err(|err| {
                PostgresCdcError::Connection(format!(
                    "listing primary key for publication table {namespace}.{name}: {err}"
                ))
            })?;
        let primary_key = pk_rows
            .into_iter()
            .map(|pk_row| pk_row.get(0))
            .collect::<Vec<String>>();
        if primary_key.is_empty() {
            warn!(
                table = %format!("{namespace}.{name}"),
                "snapshot bootstrap skipping publication table without primary key"
            );
            continue;
        }
        tables.push(SnapshotTable {
            namespace,
            name,
            primary_key,
        });
    }
    info!(
        publication,
        tables = tables.len(),
        "snapshot bootstrap discovered publication tables"
    );
    Ok(tables)
}

/// Build a synthetic event whose only purpose is to signal the join
/// engine that no more snapshot rows are coming. Carries the header
/// `ventstream.cdc.bootstrap=snapshot-complete`.
fn build_snapshot_complete_sentinel(config: &PostgresCdcConfig) -> Result<Event, PostgresCdcError> {
    let source = SourceUri::new(format!(
        "postgres://{publication}/_/_snapshot-complete",
        publication = percent(&config.publication),
    ))
    .map_err(|err| PostgresCdcError::Internal(err.to_string()))?;
    let subject = Subject::new("postgres._.snapshot.complete".to_owned())
        .map_err(|err| PostgresCdcError::Internal(err.to_string()))?;
    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "ventstream.cdc.publication".to_owned(),
        config.publication.clone(),
    );
    headers.insert(
        "ventstream.cdc.bootstrap".to_owned(),
        "snapshot-complete".to_owned(),
    );
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(headers))
        .build())
}

async fn create_slot(client: &Client, slot_name: &str) -> Result<(), PostgresCdcError> {
    // pgoutput plugin matches what pgwire-replication expects on
    // connect. Plain CREATE_REPLICATION_SLOT (no USE_SNAPSHOT) — v1
    // doesn't need the transactional snapshot ID.
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
            slot_name.replace('\'', "''")
        ))
        .await
        .map_err(|err| {
            // Shared with the SQL-denormalize path so both render the
            // SQLSTATE, message and HINT that tokio-postgres's Display
            // discards — see `describe_slot_creation_error`.
            PostgresCdcError::Connection(crate::postgres::connection::describe_slot_creation_error(
                slot_name, &err,
            ))
        })?;
    info!(slot = %slot_name, "replication slot created");
    Ok(())
}

async fn slot_exists(client: &Client, slot_name: &str) -> Result<bool, PostgresCdcError> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .map_err(|err| {
            let detail = crate::postgres::connection::describe_db_error(&err);
            PostgresCdcError::Connection(format!("checking slot {slot_name}: {detail}"))
        })?;
    Ok(row.get(0))
}

async fn snapshot_table(
    client: &Client,
    config: &PostgresCdcConfig,
    bootstrap: &SnapshotBootstrap,
    table: &SnapshotTable,
    version_floor: u64,
    ctx: &SourceContext,
) -> Result<usize, PostgresCdcError> {
    if table.primary_key.is_empty() {
        return Err(PostgresCdcError::Internal(format!(
            "snapshot table {}.{} has no primary key configured",
            table.namespace, table.name
        )));
    }

    let qualified = format!(
        "{}.{}",
        quote_ident(&table.namespace),
        quote_ident(&table.name),
    );
    let pk_select = table
        .primary_key
        .iter()
        .map(|c| format!("t.{}::text", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_order = table
        .primary_key
        .iter()
        .map(|c| format!("t.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    // Real Postgres types of the PK columns. The keyset filter binds the
    // captured cursor values as text and casts them back to these types,
    // so the `WHERE (pk) > ($n::type)` comparison runs in the column's
    // native type — matching the native `ORDER BY pk`. Without this the
    // filter compared `pk::text` lexicographically while ORDER BY ran
    // numerically; for integer keys the two disagree and the scan skips
    // rows / ends early (e.g. id 10000 sorts before '5000' as text).
    let pk_types = fetch_pk_types(client, &qualified, &table.primary_key).await?;

    // Column-level source list: numeric / array / timestamp-family
    // columns are cast ::text inside a derived table so Postgres
    // renders them with its own output functions — exactly the text
    // forms pgoutput ships on the WAL path. Left as to_jsonb defaults
    // they diverge: numerics round-trip through JSON as f64 (precision
    // loss), arrays render `[\"a\",\"b\"]` instead of `{a,b}`, and
    // timestamptz gains a `:00` offset suffix. PK columns are never
    // cast — the keyset ORDER BY/filter must run in native types.
    let source_select = build_cast_select(client, &qualified, &table.primary_key).await?;
    let from_clause = format!("(SELECT {source_select} FROM {qualified}) t");

    let mut total: u64 = 0;
    let mut chunks: u64 = 0;
    let mut last_pk: Option<Vec<String>> = None;
    let start = Instant::now();

    loop {
        let sql = match &last_pk {
            None => format!(
                "SELECT to_jsonb(t.*), {pk_select} \
                 FROM {from_clause} \
                 ORDER BY {pk_order} \
                 LIMIT {chunk}",
                chunk = bootstrap.chunk_size,
            ),
            Some(_) => {
                let where_clause = build_pk_gt_clause(&table.primary_key, &pk_types);
                format!(
                    "SELECT to_jsonb(t.*), {pk_select} \
                     FROM {from_clause} \
                     WHERE {where_clause} \
                     ORDER BY {pk_order} \
                     LIMIT {chunk}",
                    chunk = bootstrap.chunk_size,
                )
            }
        };

        // Bind the cursor PK values (if any) as text params, borrowed from
        // `last_pk` for the duration of the query — no leak. `last_pk` lives
        // across the whole loop iteration and isn't reassigned until after
        // the query completes, so these references stay valid (cf.
        // `fetcher.rs::run_query_opt`). `resync_tables` is called repeatedly
        // at runtime via /admin/resync, so a per-chunk leak would accumulate
        // for the process lifetime (H5).
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = match &last_pk {
            Some(values) => values
                .iter()
                .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect(),
            None => Vec::new(),
        };

        let rows = client
            .query(&sql, &params)
            .await
            .map_err(|err| PostgresCdcError::Connection(format!("snapshot select: {err}")))?;
        if rows.is_empty() {
            break;
        }

        for row in &rows {
            let json: Value = row.get(0);
            let event = build_synthetic_insert(config, table, &json, version_floor)?;
            ctx.sender
                .send(event, &ctx.shutdown)
                .await
                .map_err(|err| PostgresCdcError::Internal(format!("publish failed: {err}")))?;
            total += 1;
        }

        // Track the last PK for the next chunk.
        let last_row = rows
            .last()
            .ok_or_else(|| PostgresCdcError::Internal("empty rows after non-empty check".into()))?;
        let mut next_pk = Vec::with_capacity(table.primary_key.len());
        for i in 0..table.primary_key.len() {
            // Column at index `1 + i` (index 0 is to_jsonb).
            let s: String = last_row.get(1 + i);
            next_pk.push(s);
        }
        last_pk = Some(next_pk);
        chunks += 1;

        if rows.len() < bootstrap.chunk_size {
            break;
        }
    }

    info!(
        table = %format!("{}.{}", table.namespace, table.name),
        rows = total,
        chunks,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "snapshot table complete"
    );
    Ok(total as usize)
}

/// Builds a `WHERE (pk1, pk2, …) > ($1::type1, $2::type2, …)` row-tuple
/// comparison. The LHS is the bare PK columns (so the comparison runs in
/// each column's native type and can use the PK index), and each bound
/// cursor value — captured and bound as text — is cast back to its real
/// column type on the RHS. This keeps the keyset filter consistent with
/// the native `ORDER BY pk`.
///
/// The earlier form compared `pk::text > $n` (text) against a numeric
/// `ORDER BY pk`: for integer keys those orders disagree, so the keyset
/// skipped rows and the snapshot terminated early. `types[i]` comes from
/// `pg_typeof` on our own table, so it's trusted metadata, not user input.
/// Postgres row-tuple `(a,b,…) > (x,y,…)` handles single and composite
/// keys with the same syntax.
fn build_pk_gt_clause(pk: &[String], types: &[String]) -> String {
    let lhs = pk
        .iter()
        .map(|c| format!("t.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    // `$n::text::{type}`, not `$n::{type}`: the cursor values are bound as
    // Rust strings, so the param must be inferred as `text`. Casting the
    // param straight to e.g. `integer` makes Postgres infer the param type
    // as integer and reject the string bind ("error serializing parameter").
    // The leading `::text` pins it to text, then we cast text → the real
    // column type so the comparison is native.
    let rhs = types
        .iter()
        .enumerate()
        .map(|(i, ty)| format!("${}::text::{}", i + 1, ty))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({lhs}) > ({rhs})")
}

/// Fetch the native Postgres type of each PK column (via `pg_typeof` on a
/// sample row) so the keyset filter can cast its text-bound cursor values
/// back to the right type. Returns a placeholder `text` per column when
/// the table is empty — in that case the first chunk returns no rows and
/// the keyset clause is never built, so the value is unused.
pub(crate) async fn fetch_pk_types(
    client: &Client,
    qualified: &str,
    pk: &[String],
) -> Result<Vec<String>, PostgresCdcError> {
    let selects = pk
        .iter()
        .map(|c| format!("pg_typeof(t.{})::text", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {selects} FROM {qualified} t LIMIT 1");
    let rows = client
        .query(&sql, &[])
        .await
        .map_err(|err| PostgresCdcError::Connection(format!("fetch pk types: {err}")))?;
    match rows.first() {
        Some(row) => Ok((0..pk.len()).map(|i| row.get::<_, String>(i)).collect()),
        None => Ok(vec!["text".to_owned(); pk.len()]),
    }
}

/// Build the SQL identifier-quoted form: `"my_table"` for a table
/// named `my_table`. Doubles any embedded `"` per SQL spec.
pub(crate) fn quote_ident(s: &str) -> String {
    let escaped = s.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Emit a synthetic INSERT event for one snapshot row. The event
/// shape mirrors what the WAL-driven `event_mapper::insert_to_event`
/// produces so downstream handling is identical.
fn build_synthetic_insert(
    config: &PostgresCdcConfig,
    table: &SnapshotTable,
    row_json: &Value,
    version_floor: u64,
) -> Result<Event, PostgresCdcError> {
    let source = SourceUri::new(format!(
        "postgres://{publication}/{namespace}/{relation}",
        publication = percent(&config.publication),
        namespace = percent(&table.namespace),
        relation = percent(&table.name),
    ))
    .map_err(|err| PostgresCdcError::Internal(err.to_string()))?;

    let subject = Subject::new(format!(
        "postgres.{namespace}.{relation}.insert",
        namespace = sanitize_segment(&table.namespace),
        relation = sanitize_segment(&table.name),
    ))
    .map_err(|err| PostgresCdcError::Internal(err.to_string()))?;

    // pgoutput delivers column values as text; the join engine
    // (specifically the consumer-side fetcher) normalizes WAL inserts
    // to text-only top-level values. We do the same here so a row
    // arriving via snapshot has the same shape as a row arriving via
    // WAL — no mapping conflicts in the sink.
    let payload_value = stringify_top_level(row_json.clone());
    let payload_bytes = serde_json::to_vec(&payload_value)
        .map_err(|err| PostgresCdcError::Internal(format!("encoding row: {err}")))?;

    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "ventstream.cdc.namespace".to_owned(),
        table.namespace.clone(),
    );
    headers.insert("ventstream.cdc.relation".to_owned(), table.name.clone());
    headers.insert(
        "ventstream.cdc.publication".to_owned(),
        config.publication.clone(),
    );
    headers.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());
    // The scan-start WAL position doubles as the row's external_gte
    // version: newer concurrent WAL writes (higher LSN) always win,
    // and a stale snapshot read landing after one is dropped by the
    // sink's version conflict instead of clobbering it. Deliberately
    // NOT `ventstream.cdc.lsn` — that header feeds the dispatcher's
    // slot-ack watermark, and a committed snapshot batch must never
    // push the watermark past live events still in flight.
    if version_floor > 0 {
        headers.insert(
            "ventstream.cdc.source_version".to_owned(),
            version_floor.to_string(),
        );
    }
    // Stable doc id from the discovered primary key, matching the WAL path
    // and the SQL-denormalize path byte for byte. Values were text-normalized
    // above, so snapshot and WAL ids for the same row agree.
    if !table.primary_key.is_empty() {
        let mut components = Vec::with_capacity(table.primary_key.len());
        let mut complete = true;
        for column in &table.primary_key {
            match payload_value.get(column) {
                Some(value) if !value.is_null() => {
                    components.push(ventstream_core::doc_id::component_text(value));
                }
                _ => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            let table_name = format!("{}.{}", table.namespace, table.name);
            headers.insert(
                "ventstream.doc.id".to_owned(),
                ventstream_core::doc_id::doc_id(&table_name, &components),
            );
        }
    }

    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(payload_bytes))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(headers))
        .build())
}

/// Build the derived-table column list: every column of `qualified`,
/// with numeric, array, and timestamp-family columns cast `::text` so
/// their values arrive in Postgres's canonical output-function forms
/// (the same renderings pgoutput uses on the WAL path). PK columns are
/// excluded from casting — keyset pagination orders and filters on
/// them in their native types.
pub(crate) async fn build_cast_select(
    client: &Client,
    qualified: &str,
    primary_key: &[String],
) -> Result<String, PostgresCdcError> {
    let rows = client
        .query(
            "SELECT a.attname, t.typname, t.typcategory::text \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&qualified],
        )
        .await
        .map_err(|err| PostgresCdcError::Connection(format!("column type discovery: {err}")))?;
    if rows.is_empty() {
        return Err(PostgresCdcError::Internal(format!(
            "no columns discovered for {qualified}"
        )));
    }
    let mut parts = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let typname: String = row.get(1);
        let category: String = row.get(2);
        let is_pk = primary_key.iter().any(|pk| pk == &name);
        let needs_cast = !is_pk
            && (category == "A"
                || matches!(
                    typname.as_str(),
                    "numeric" | "timestamptz" | "timestamp" | "timetz" | "time" | "interval"
                ));
        let ident = quote_ident(&name);
        if needs_cast {
            parts.push(format!("{ident}::text AS {ident}"));
        } else {
            parts.push(ident);
        }
    }
    Ok(parts.join(", "))
}

/// The current WAL insert position, captured on the snapshot's own
/// connection at scan start. `0` (never produced by a live server)
/// would mean "unversioned"; the query always returns a real LSN.
async fn capture_version_floor(client: &Client) -> Result<u64, PostgresCdcError> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|err| PostgresCdcError::Connection(format!("capture wal position: {err}")))?;
    let text: String = row.get(0);
    parse_lsn(&text).ok_or_else(|| {
        PostgresCdcError::Internal(format!("unparseable pg_current_wal_lsn `{text}`"))
    })
}

/// Parse Postgres's `XXXXXXXX/XXXXXXXX` LSN text form into the u64 the
/// WAL stream's `ventstream.cdc.lsn` headers use.
fn parse_lsn(text: &str) -> Option<u64> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

/// Stringify the top-level fields of a JSON object so values match
/// the text-form convention pgoutput uses. Nested structures stay
/// untouched — this only normalizes scalars at depth 1.
///
/// Also normalizes timestamp string format: `to_jsonb(now()::timestamp)`
/// renders `"2026-05-23T22:40:35.600372"` (ISO 8601 with T separator)
/// while pgoutput renders the same value as
/// `"2026-05-23 22:40:35.600372"` (space separator). OpenSearch's
/// automatic date-field mapping picks the format of the first row it
/// sees and rejects all subsequent rows that don't match. We rewrite
/// the snapshot output to pgoutput's space form so dynamic mapping
/// stays consistent across snapshot and WAL inputs.
pub(crate) fn stringify_top_level(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                let stringified = match v {
                    Value::Null => Value::Null,
                    // Match pgoutput's bool text form (`t`/`f`), NOT serde's
                    // `true`/`false` — otherwise the same column lands as a
                    // different string via the snapshot path vs the WAL/fetcher
                    // path, causing dynamic-mapping conflicts / inconsistent
                    // index values (M10). fetcher.rs already emits `t`/`f`.
                    Value::Bool(b) => Value::String(if b { "t" } else { "f" }.to_owned()),
                    Value::Number(n) => Value::String(n.to_string()),
                    Value::String(s) => Value::String(normalize_iso_timestamp(&s)),
                    other @ (Value::Array(_) | Value::Object(_)) => {
                        Value::String(other.to_string())
                    }
                };
                out.insert(k, stringified);
            }
            Value::Object(out)
        }
        other => other,
    }
}

/// Rewrite an ISO-8601-style timestamp (`YYYY-MM-DDTHH:MM:SS…`) into
/// pgoutput's space-separator form (`YYYY-MM-DD HH:MM:SS…`) so the
/// downstream sink doesn't see two different formats for the same
/// column across snapshot and WAL paths. Strings that aren't a
/// timestamp at the start pass through unchanged.
fn normalize_iso_timestamp(s: &str) -> String {
    // Layout we're matching at the start: `YYYY-MM-DDTHH:MM:SS`.
    // 19 positions; 4 fixed separators (`-`, `-`, `T`, `:`, `:` — five
    // total when including the inner `:`s). We check each position
    // explicitly so indexing_slicing stays happy.
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return s.to_owned();
    }
    let digit = |i: usize| bytes.get(i).is_some_and(u8::is_ascii_digit);
    let sep = |i: usize, want: u8| bytes.get(i) == Some(&want);
    let looks_iso = digit(0)
        && digit(1)
        && digit(2)
        && digit(3)
        && sep(4, b'-')
        && digit(5)
        && digit(6)
        && sep(7, b'-')
        && digit(8)
        && digit(9)
        && sep(10, b'T')
        && digit(11)
        && digit(12)
        && sep(13, b':')
        && digit(14)
        && digit(15)
        && sep(16, b':')
        && digit(17)
        && digit(18);
    if !looks_iso {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    if let Some(before_t) = s.get(..10) {
        out.push_str(before_t);
    }
    out.push(' ');
    if let Some(after_t) = s.get(11..) {
        out.push_str(after_t);
    }
    out
}

fn percent(input: &str) -> String {
    // Same minimal percent-encoder as event_mapper, kept local so the
    // snapshot module doesn't reach into a private helper there.
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

fn sanitize_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    #[test]
    fn parse_lsn_matches_wal_header_encoding() {
        // 16/B374D848 = (0x16 << 32) | 0xB374D848 — the same u64 the
        // WAL path stamps in `ventstream.cdc.lsn`.
        assert_eq!(
            super::parse_lsn("16/B374D848"),
            Some((0x16u64 << 32) | 0xB374_D848)
        );
        assert_eq!(super::parse_lsn("0/0"), Some(0));
        assert_eq!(super::parse_lsn("junk"), None);
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn stringify_bools_match_pgoutput_t_f_form() {
        // M10: snapshot must emit `t`/`f` (pgoutput's form), not `true`/`false`,
        // so a bool column lands as the SAME string via snapshot and WAL paths.
        let out = stringify_top_level(json!({ "active": true, "deleted": false }));
        let obj = out.as_object().expect("object");
        assert_eq!(obj.get("active"), Some(&Value::String("t".to_owned())));
        assert_eq!(obj.get("deleted"), Some(&Value::String("f".to_owned())));
    }

    #[test]
    fn stringify_other_scalars_still_text() {
        let out = stringify_top_level(json!({ "n": 5, "s": "x", "nil": null }));
        let obj = out.as_object().expect("object");
        assert_eq!(obj.get("n"), Some(&Value::String("5".to_owned())));
        assert_eq!(obj.get("s"), Some(&Value::String("x".to_owned())));
        assert_eq!(obj.get("nil"), Some(&Value::Null));
    }
}
