//! Postgres implementation of [`RelatedFetcher`].
//!
//! Issues `SELECT to_jsonb(t.*) FROM …` against a **separate**
//! Postgres connection (not the replication slot's) so foreign-table
//! backfill SELECTs never block WAL streaming.
//!
//! ### SQL shape
//!
//! ```sql
//! -- fetch_one
//! SELECT to_jsonb(t.*) FROM "ns"."table" t
//! WHERE "pk_col1" = CAST($1::text AS bigint)
//!   AND "pk_col2" = CAST($2::text AS uuid)
//! LIMIT 1
//!
//! -- fetch_many
//! SELECT to_jsonb(t.*) FROM "ns"."table" t
//! WHERE "fk_col1" = CAST($1::text AS bigint)
//! ```
//!
//! Column types are resolved once from `pg_catalog` and cached. Casting the
//! bind value, rather than the indexed column, lets PostgreSQL use ordinary
//! PK/FK B-tree indexes while still accepting pgoutput's canonical text form.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio_postgres::Client;
use tracing::{debug, info, warn};
use ventstream_joins::{FetchError, PkValue, RelatedFetcher};

use super::config::PostgresCdcConfig;
use super::connection::{connect_client, describe_db_error};

/// A fetcher SELECT the server refused. Rendered through
/// [`describe_db_error`] so the SQLSTATE, message, DETAIL and HINT survive —
/// tokio-postgres's `Display` collapses all of them to a bare "db error",
/// which for a permission or missing-relation refusal is the whole answer.
fn query_failed(table: &str, message: String) -> FetchError {
    FetchError::Query {
        table: table.to_owned(),
        message,
    }
}

/// A row the fetcher could not decode. These are client-side errors with no
/// SQLSTATE, but rendering them through the same helper keeps every
/// tokio-postgres error in this module on one path.
fn decode_failed(table: &str, context: &str, err: &tokio_postgres::Error) -> FetchError {
    FetchError::Decode {
        table: table.to_owned(),
        message: format!("{context}: {}", describe_db_error(err)),
    }
}

/// Postgres-backed [`RelatedFetcher`].
///
/// Owns a small reconnecting client pool. Concurrent relation batches are
/// distributed round-robin and only the failed connection is invalidated.
pub struct PostgresFetcher {
    inner: Arc<Inner>,
}

/// Cached catalog types for one table plus the type-change epoch they
/// were loaded under.
type EpochedTypes = (u64, HashMap<String, String>);

const DEFAULT_POOL_SIZE: usize = 4;
const BATCH_KEY_CHUNK: usize = 256;

struct Inner {
    connection: FetcherConnection,
    clients: Vec<RwLock<Option<Arc<Client>>>>,
    next_client: AtomicUsize,
    /// Server-rendered SQL types keyed by configured table and column. Values
    /// come from `pg_catalog.format_type`, which quotes user-defined types
    /// safely for use in generated casts.
    /// Value is `(type_change_epoch at load, name -> type)`; an epoch bump
    /// from a source-observed column retype makes the entry stale.
    column_types: AsyncMutex<HashMap<String, EpochedTypes>>,
    /// Last time we logged the connection state. Used only to throttle
    /// the noisy reconnect-attempt log lines.
    last_log: Mutex<std::time::Instant>,
}

#[derive(Clone)]
struct FetcherConnection(Box<PostgresCdcConfig>);

impl PostgresFetcher {
    /// Construct a fetcher and open the initial connection. Inherits
    /// the source's TLS policy; opens fresh non-replication
    /// connections.
    pub async fn connect_config(source: PostgresCdcConfig) -> Result<Self, FetchError> {
        Self::connect_config_with_pool_size(source, DEFAULT_POOL_SIZE).await
    }

    /// Construct a fetcher with a bounded, lazily grown query pool.
    pub async fn connect_config_with_pool_size(
        source: PostgresCdcConfig,
        pool_size: usize,
    ) -> Result<Self, FetchError> {
        Self::connect_inner(FetcherConnection(Box::new(source)), pool_size).await
    }

    async fn connect_inner(
        connection: FetcherConnection,
        pool_size: usize,
    ) -> Result<Self, FetchError> {
        let pool_size = pool_size.max(1);
        let mut clients = Vec::with_capacity(pool_size);
        clients.push(RwLock::new(Some(Arc::new(open_client(&connection).await?))));
        for _ in 1..pool_size {
            clients.push(RwLock::new(None));
        }
        info!(
            pool_size,
            initial_connections = 1,
            "postgres fetcher pool initialized for sync-on-miss backfill"
        );
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                clients,
                next_client: AtomicUsize::new(0),
                column_types: AsyncMutex::new(HashMap::new()),
                last_log: Mutex::new(std::time::Instant::now()),
            }),
        })
    }

    /// Get the live client, reconnecting if the prior one was torn
    /// down by a previous error.
    async fn client(&self) -> Result<(usize, Arc<Client>), FetchError> {
        let index =
            self.inner.next_client.fetch_add(1, Ordering::Relaxed) % self.inner.clients.len();
        let slot = self.inner.clients.get(index).ok_or_else(|| {
            FetchError::Unreachable("fetcher selected an invalid pool slot".into())
        })?;
        if let Some(client) = slot.read().await.as_ref().cloned() {
            return Ok((index, client));
        }

        let mut slot_guard = slot.write().await;
        if slot_guard.is_none() {
            let now = std::time::Instant::now();
            {
                let mut last = self.inner.last_log.lock();
                if now.duration_since(*last) > std::time::Duration::from_secs(5) {
                    warn!("postgres fetcher reconnecting after prior failure");
                    *last = now;
                }
            }
            let fresh = open_client(&self.inner.connection).await?;
            *slot_guard = Some(Arc::new(fresh));
        }
        slot_guard
            .as_ref()
            .cloned()
            .map(|client| (index, client))
            .ok_or_else(|| FetchError::Unreachable("fetcher pool slot is unavailable".into()))
    }

    /// Mark the client as dead after a fatal error so the next call
    /// reconnects.
    async fn invalidate(&self, index: usize, failed: &Arc<Client>) {
        let Some(slot) = self.inner.clients.get(index) else {
            return;
        };
        let mut guard = slot.write().await;
        if guard
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, failed))
        {
            *guard = None;
        }
    }
}

async fn open_client(connection: &FetcherConnection) -> Result<Client, FetchError> {
    connect_client(&connection.0, "related-row fetcher")
        .await
        .map_err(|err| FetchError::Unreachable(err.to_string()))
}

#[async_trait]
impl RelatedFetcher for PostgresFetcher {
    async fn fetch_one(
        &self,
        table: &str,
        pk_columns: &[String],
        pk_value: &PkValue,
        select: &[String],
    ) -> Result<Option<Value>, FetchError> {
        let pk_components = decode_pk(pk_value);
        if pk_components.len() != pk_columns.len() {
            return Err(FetchError::Query {
                table: table.to_owned(),
                message: format!(
                    "expected {} PK component(s), got {}",
                    pk_columns.len(),
                    pk_components.len()
                ),
            });
        }

        let table_sql = qualify_table(table);
        let projection = self.cast_projection(table, select).await?;
        let column_types = self.resolve_column_types(table, pk_columns).await?;
        let (where_sql, params) = build_typed_where(pk_columns, &pk_components, &column_types);
        let sql =
            format!("SELECT to_jsonb(t.*) FROM (SELECT {projection} FROM {table_sql} {where_sql} LIMIT 1) t");

        // Logs the SQL template (parameterised, contains only column
        // names + `$N` placeholders, no values) and the param count.
        // Param VALUES are deliberately NOT logged — they can be PK
        // values, customer identifiers, anything sensitive.
        debug!(
            sql = %sql,
            param_count = params.len(),
            metric = "pg.fetcher.query",
            "postgres fetcher: fetch_one"
        );
        let row = self.run_query_opt(table, &sql, &params).await?;
        Ok(row)
    }

    async fn fetch_many(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_value: &PkValue,
        select: &[String],
    ) -> Result<Vec<Value>, FetchError> {
        let fk_components = decode_pk(fk_value);
        if fk_components.len() != fk_columns.len() {
            return Err(FetchError::Query {
                table: table.to_owned(),
                message: format!(
                    "expected {} FK component(s), got {}",
                    fk_columns.len(),
                    fk_components.len()
                ),
            });
        }

        let table_sql = qualify_table(table);
        let projection = self.cast_projection(table, select).await?;
        let resolved_types = self.resolve_column_types(table, fk_columns).await?;
        let column_types = resolved_types.as_slice();
        let (where_sql, params) = build_typed_where(fk_columns, &fk_components, column_types);
        let sql = format!(
            "SELECT to_jsonb(t.*) FROM (SELECT {projection} FROM {table_sql} {where_sql}) t"
        );

        // Param values redacted — see fetch_one for rationale.
        debug!(
            sql = %sql,
            param_count = params.len(),
            metric = "pg.fetcher.query",
            "postgres fetcher: fetch_many"
        );
        self.run_query_many(table, &sql, &params).await
    }

    async fn fetch_many_batch(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_values: &[PkValue],
        select: &[String],
    ) -> Result<Vec<(PkValue, Vec<Value>)>, FetchError> {
        let mut seen = HashSet::with_capacity(fk_values.len());
        let unique: Vec<PkValue> = fk_values
            .iter()
            .filter(|key| !key.is_null() && seen.insert((*key).clone()))
            .cloned()
            .collect();
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        let resolved_types = self.resolve_column_types(table, fk_columns).await?;
        let column_types = resolved_types.as_slice();
        let queries = unique
            .chunks(BATCH_KEY_CHUNK)
            .map(|keys| {
                let keys = keys.to_vec();
                async move {
                    self.run_batch_query(table, fk_columns, &keys, select, column_types)
                        .await
                }
            })
            .collect::<Vec<_>>();
        let chunks: Vec<Vec<(PkValue, Vec<Value>)>> = stream::iter(queries)
            .buffer_unordered(self.inner.clients.len())
            .try_collect()
            .await?;
        let mut grouped = HashMap::with_capacity(unique.len());
        for chunk in chunks {
            grouped.extend(chunk);
        }
        Ok(unique
            .into_iter()
            .map(|key| {
                let rows = grouped.remove(&key).unwrap_or_default();
                (key, rows)
            })
            .collect())
    }
}

impl PostgresFetcher {
    /// Cast-aware projection: numeric, array, and timestamp-family
    /// columns render `::text` so Postgres's own output functions
    /// produce the same text forms pgoutput ships on the WAL path
    /// (exact numerics, `{a,b}` arrays, `+00` offsets) — keeping
    /// fetcher-composed values byte-identical to WAL- and
    /// snapshot-composed ones. Derived from the same cached type map
    /// `resolve_column_types` fills; no extra catalog queries.
    async fn cast_projection(&self, table: &str, select: &[String]) -> Result<String, FetchError> {
        // Ensure the cache is populated for this table (any column
        // works; the resolver caches the whole table's types).
        let _ = self.resolve_column_types(table, &[]).await?;
        let cache = self.inner.column_types.lock().await;
        let Some((_, table_types)) = cache.get(table) else {
            return Ok(projection_sql(select));
        };
        let needs_cast = |ty: &str| {
            ty.ends_with("[]")
                || ty.starts_with("numeric")
                || ty.starts_with("timestamp")
                || ty.starts_with("time")
                || ty.starts_with("interval")
        };
        let render = |name: &str| {
            let ident = quote_ident(name);
            match table_types.get(name) {
                Some(ty) if needs_cast(ty) => format!("{ident}::text AS {ident}"),
                _ => ident,
            }
        };
        if select.is_empty() {
            let mut names: Vec<&String> = table_types.keys().collect();
            names.sort();
            return Ok(names
                .iter()
                .map(|name| render(name))
                .collect::<Vec<_>>()
                .join(", "));
        }
        Ok(select
            .iter()
            .map(|name| render(name))
            .collect::<Vec<_>>()
            .join(", "))
    }

    async fn resolve_column_types(
        &self,
        table: &str,
        columns: &[String],
    ) -> Result<Vec<String>, FetchError> {
        let epoch = super::schema::type_change_epoch(table);
        {
            let cache = self.inner.column_types.lock().await;
            if let Some((cached_epoch, table_types)) = cache.get(table) {
                if *cached_epoch == epoch {
                    if let Some(types) = ordered_column_types(table_types, columns) {
                        return Ok(types);
                    }
                } else {
                    debug!(
                        table,
                        "postgres fetcher: cached column types stale after type change, re-resolving"
                    );
                }
            }
        }

        const TYPE_QUERY: &str = "\
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) \
            FROM pg_catalog.pg_attribute a \
            WHERE a.attrelid = pg_catalog.to_regclass($1::text) \
              AND a.attnum > 0 \
              AND NOT a.attisdropped";
        let regclass = qualify_table(table);
        let (client_index, client) = self.client().await?;
        let result = client.query(TYPE_QUERY, &[&regclass]).await;
        let rows = match result {
            Ok(rows) => rows,
            Err(err) => {
                self.invalidate(client_index, &client).await;
                return Err(query_failed(
                    table,
                    format!(
                        "resolving indexed column types: {}",
                        describe_db_error(&err)
                    ),
                ));
            }
        };

        let mut table_types = HashMap::with_capacity(rows.len());
        for row in rows {
            let name: String = row
                .try_get(0)
                .map_err(|err| decode_failed(table, "decoding column name", &err))?;
            let sql_type: String = row.try_get(1).map_err(|err| {
                decode_failed(table, &format!("decoding column type for '{name}'"), &err)
            })?;
            table_types.insert(name, sql_type);
        }
        let types = ordered_column_types(&table_types, columns).ok_or_else(|| {
            let missing = columns
                .iter()
                .filter(|column| !table_types.contains_key(column.as_str()))
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            FetchError::Query {
                table: table.to_owned(),
                message: format!("column type metadata missing for: {missing}"),
            }
        })?;
        self.inner
            .column_types
            .lock()
            .await
            .insert(table.to_owned(), (epoch, table_types));
        Ok(types)
    }

    async fn run_query_opt(
        &self,
        table: &str,
        sql: &str,
        params: &[String],
    ) -> Result<Option<Value>, FetchError> {
        let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let (client_index, client) = self.client().await?;
        let result = client.query_opt(sql, &bind).await;
        match result {
            Ok(Some(row)) => Ok(row_first_column_as_value(table, &row)?),
            Ok(None) => Ok(None),
            Err(err) => {
                self.invalidate(client_index, &client).await;
                Err(query_failed(table, describe_db_error(&err)))
            }
        }
    }

    async fn run_batch_query(
        &self,
        table: &str,
        columns: &[String],
        keys: &[PkValue],
        select: &[String],
        column_types: &[String],
    ) -> Result<Vec<(PkValue, Vec<Value>)>, FetchError> {
        let (sql, params, active_keys) =
            build_typed_batch_query(table, columns, keys, select, column_types)?;
        if active_keys.is_empty() {
            return Ok(Vec::new());
        }
        let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|value| value as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let started = std::time::Instant::now();
        let (client_index, client) = self.client().await?;
        let result = client.query(&sql, &bind).await;
        let rows = match result {
            Ok(rows) => rows,
            Err(err) => {
                self.invalidate(client_index, &client).await;
                return Err(query_failed(table, describe_db_error(&err)));
            }
        };

        let mut grouped: Vec<Vec<Value>> = vec![Vec::new(); active_keys.len()];
        for row in rows {
            let ordinal: i32 = row
                .try_get(0)
                .map_err(|err| decode_failed(table, "decoding batch key ordinal", &err))?;
            let value: Option<Value> = row
                .try_get(1)
                .map_err(|err| decode_failed(table, "decoding batch row", &err))?;
            let Some(bucket) = usize::try_from(ordinal)
                .ok()
                .and_then(|index| grouped.get_mut(index))
            else {
                return Err(FetchError::Decode {
                    table: table.to_owned(),
                    message: format!("batch query returned invalid key ordinal {ordinal}"),
                });
            };
            if let Some(value) = value {
                bucket.push(stringify_top_level_values(value));
            }
        }
        debug!(
            table,
            keys = active_keys.len(),
            rows = grouped.iter().map(Vec::len).sum::<usize>(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            metric = "pg.fetcher.batch_query",
            "postgres fetcher batch query complete"
        );
        Ok(active_keys.into_iter().zip(grouped).collect())
    }

    async fn run_query_many(
        &self,
        table: &str,
        sql: &str,
        params: &[String],
    ) -> Result<Vec<Value>, FetchError> {
        let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let (client_index, client) = self.client().await?;
        let result = client.query(sql, &bind).await;
        match result {
            Ok(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for row in &rows {
                    if let Some(v) = row_first_column_as_value(table, row)? {
                        out.push(v);
                    }
                }
                Ok(out)
            }
            Err(err) => {
                self.invalidate(client_index, &client).await;
                Err(query_failed(table, describe_db_error(&err)))
            }
        }
    }
}

// ---- helpers --------------------------------------------------------------

/// Quote a Postgres identifier (table or column) by wrapping in double
/// quotes and doubling embedded double-quotes. Idempotent against
/// fully-qualified names by splitting on the first `.`.
fn qualify_table(table: &str) -> String {
    if let Some((ns, rel)) = table.split_once('.') {
        format!("{}.{}", quote_ident(ns), quote_ident(rel))
    } else {
        quote_ident(table)
    }
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// `select: []` → `*`; otherwise comma-separated identifiers.
fn projection_sql(select: &[String]) -> String {
    if select.is_empty() {
        return "*".to_string();
    }
    select
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ordered_column_types(
    table_types: &HashMap<String, String>,
    columns: &[String],
) -> Option<Vec<String>> {
    columns
        .iter()
        .map(|column| table_types.get(column).cloned())
        .collect()
}

/// Render indexed predicates with text parameters cast to the actual server
/// column types. Null join keys deliberately produce `FALSE`, matching the old
/// text predicate's no-match behavior without attempting to cast an empty
/// string to a numeric/UUID type.
fn build_typed_where(
    columns: &[String],
    components: &[Value],
    column_types: &[String],
) -> (String, Vec<String>) {
    let mut clauses = Vec::with_capacity(columns.len());
    let mut params = Vec::with_capacity(columns.len());
    for ((column, value), sql_type) in columns
        .iter()
        .zip(components.iter())
        .zip(column_types.iter())
    {
        if value.is_null() {
            clauses.push("FALSE".to_owned());
            continue;
        }
        let placeholder = format!("${}", params.len() + 1);
        clauses.push(format!(
            "{} = CAST({placeholder}::text AS {sql_type})",
            quote_ident(column),
        ));
        params.push(value_as_text(value));
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    (where_sql, params)
}

type BatchQuery = (String, Vec<String>, Vec<PkValue>);

/// Build one bounded set-based lookup. The ordinal carried through the CTE
/// lets rows be grouped back to their canonical [`PkValue`] without relying on
/// selected columns or PostgreSQL JSON type rendering.
fn build_typed_batch_query(
    table: &str,
    columns: &[String],
    keys: &[PkValue],
    select: &[String],
    column_types: &[String],
) -> Result<BatchQuery, FetchError> {
    if columns.is_empty() || columns.len() != column_types.len() {
        return Err(FetchError::Query {
            table: table.to_owned(),
            message: "batch lookup columns and type metadata do not align".into(),
        });
    }

    let mut params = Vec::new();
    let mut value_rows = Vec::new();
    let mut active_keys = Vec::new();
    for key in keys {
        let components = decode_pk(key);
        if components.len() != columns.len() {
            return Err(FetchError::Query {
                table: table.to_owned(),
                message: format!(
                    "expected {} key component(s), got {}",
                    columns.len(),
                    components.len()
                ),
            });
        }
        if components.iter().any(Value::is_null) {
            continue;
        }
        let ordinal = active_keys.len();
        let mut fields = vec![ordinal.to_string()];
        for (component, sql_type) in components.iter().zip(column_types) {
            params.push(value_as_text(component));
            fields.push(format!("CAST(${}::text AS {sql_type})", params.len()));
        }
        value_rows.push(format!("({})", fields.join(", ")));
        active_keys.push(key.clone());
    }
    if active_keys.is_empty() {
        return Ok((String::new(), params, active_keys));
    }

    let key_aliases = (0..columns.len())
        .map(|index| format!("__vs_key_{index}"))
        .collect::<Vec<_>>();
    let cte_columns = std::iter::once("__vs_ordinal".to_owned())
        .chain(key_aliases.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = columns
        .iter()
        .zip(&key_aliases)
        .map(|(column, alias)| format!("source.{} = requested.{alias}", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "WITH requested({cte_columns}) AS (VALUES {values}) \
         SELECT requested.__vs_ordinal, to_jsonb(projected.*) \
         FROM requested \
         JOIN LATERAL (\
           SELECT {projection} FROM {table_sql} source WHERE {predicates}\
         ) projected ON TRUE \
         ORDER BY requested.__vs_ordinal",
        values = value_rows.join(", "),
        projection = projection_sql(select),
        table_sql = qualify_table(table),
    );
    Ok((sql, params, active_keys))
}

/// Render a JSON value in pgoutput's canonical text form before the generated
/// predicate casts it back to the indexed column type:
/// - strings → the inner string (no quotes)
/// - numbers → their canonical numeric form
/// - bools → `t` / `f`
/// - null → empty string (and the WHERE will then not match — caller
///   typically guards against null FKs upstream)
/// - object/array → JSON encoding
fn value_as_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(true) => "t".into(),
        Value::Bool(false) => "f".into(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Decode a [`PkValue`] back into its JSON-array components.
fn decode_pk(pk: &PkValue) -> Vec<Value> {
    match pk.to_json() {
        Value::Array(arr) => arr,
        other => vec![other],
    }
}

/// Pull the first column off a row as `serde_json::Value`. With our
/// `to_jsonb(t.*)` projection that single column IS the row as JSON.
///
/// Postgres's `to_jsonb` preserves SQL types as native JSON types
/// (integers → numbers, booleans → JSON booleans, etc.). The CDC
/// source's `event_mapper`, by contrast, emits every column as a JSON
/// string (because pgoutput only gives us each value's canonical text
/// representation). To keep state values consistent across the two
/// paths — so OpenSearch's dynamic mapping doesn't see a column as
/// `long` from a fetcher backfill and then `text` from a stream
/// re-emit — we **normalize fetcher output to strings** at the seam.
fn row_first_column_as_value(
    table: &str,
    row: &tokio_postgres::Row,
) -> Result<Option<Value>, FetchError> {
    if row.is_empty() {
        return Ok(None);
    }
    let value: Option<Value> = row
        .try_get(0)
        .map_err(|err| decode_failed(table, "decoding first column", &err))?;
    Ok(value.map(stringify_top_level_values))
}

/// Replace every primitive value at the top level of a JSON object
/// with its canonical text form, matching what the CDC source's
/// `event_mapper` produces for streamed rows.
///
/// We only stringify the top level — nested objects and arrays
/// (e.g. JSON / JSONB columns) keep their structure but their values
/// are not recursively coerced. That matches what pgoutput would
/// deliver for a JSONB column (a single JSON value embedded in the
/// row), so consistency holds without losing structure.
fn stringify_top_level_values(value: Value) -> Value {
    let Value::Object(mut map) = value else {
        return value;
    };
    for v in map.values_mut() {
        let replacement = match v {
            Value::Null => None,
            Value::String(_) => None,
            Value::Bool(b) => Some(Value::String(if *b { "t".into() } else { "f".into() })),
            Value::Number(n) => Some(Value::String(n.to_string())),
            Value::Array(_) | Value::Object(_) => Some(Value::String(v.to_string())),
        };
        if let Some(new_val) = replacement {
            *v = new_val;
        }
    }
    Value::Object(map)
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

    /// Detail as `describe_db_error` renders a refused SELECT (reproduced
    /// in `tests/it_fetcher_error.rs`). Pins, without a live server, that
    /// the described text reaches the `Query` variant intact and that a
    /// table permission error is never crash-fast — the retryable case
    /// #177 deliberately kept out of the global matcher.
    #[test]
    fn query_failed_keeps_the_described_refusal_and_stays_retryable() {
        let described = "db error (SQLSTATE 42501): permission denied for table orders";
        let err = query_failed("direct.orders", described.to_owned());
        let FetchError::Query { table, message } = &err else {
            panic!("expected the Query variant, got: {err}");
        };
        assert_eq!(table, "direct.orders");
        assert_eq!(message, described);
        assert!(
            !crate::credential::is_crash_fast_text(&err.to_string()),
            "a table permission error must stay retryable: {err}"
        );
    }

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        assert_eq!(quote_ident("plain"), r#""plain""#);
        assert_eq!(quote_ident(r#"weird"name"#), r#""weird""name""#);
    }

    #[test]
    fn qualify_table_splits_on_first_dot_only() {
        assert_eq!(qualify_table("public.users"), r#""public"."users""#);
        assert_eq!(
            qualify_table(r#"weird.schema.has.dots"#),
            r#""weird"."schema.has.dots""#
        );
    }

    #[test]
    fn projection_empty_means_star() {
        assert_eq!(projection_sql(&[]), "*");
    }

    #[test]
    fn projection_quotes_each_column() {
        let cols = ["id".to_string(), "full_name".to_string()];
        assert_eq!(projection_sql(&cols), r#""id", "full_name""#);
    }

    #[test]
    fn build_typed_where_casts_parameters_and_preserves_indexable_columns() {
        let cols = ["region".to_string(), "id".to_string()];
        let vals = [json!("us-east"), json!(42)];
        let types = ["text".to_string(), "bigint".to_string()];
        let (where_sql, params) = build_typed_where(&cols, &vals, &types);
        assert_eq!(
            where_sql,
            r#"WHERE "region" = CAST($1::text AS text) AND "id" = CAST($2::text AS bigint)"#
        );
        assert_eq!(params, vec!["us-east".to_string(), "42".to_string()]);
    }

    #[test]
    fn build_typed_where_does_not_cast_null_join_keys() {
        let cols = ["tenant_id".to_string(), "parent_id".to_string()];
        let vals = [json!(null), json!(42)];
        let types = ["uuid".to_string(), "bigint".to_string()];
        let (where_sql, params) = build_typed_where(&cols, &vals, &types);
        assert_eq!(
            where_sql,
            r#"WHERE FALSE AND "parent_id" = CAST($1::text AS bigint)"#
        );
        assert_eq!(params, vec!["42".to_string()]);
    }

    #[test]
    fn batch_query_uses_typed_values_and_preserves_key_ordinals() {
        let keys = vec![
            PkValue::from_values(&[json!("tenant-a"), json!(1)]),
            PkValue::from_values(&[json!("tenant-b"), json!(2)]),
        ];
        let (sql, params, active) = build_typed_batch_query(
            "public.items",
            &["tenant_id".into(), "id".into()],
            &keys,
            &["tenant_id".into(), "id".into(), "name".into()],
            &["text".into(), "bigint".into()],
        )
        .expect("batch query");
        assert!(sql.contains("WITH requested(__vs_ordinal, __vs_key_0, __vs_key_1) AS (VALUES"));
        assert!(sql.contains("CAST($1::text AS text)"));
        assert!(sql.contains("CAST($2::text AS bigint)"));
        assert!(sql.contains("source.\"tenant_id\" = requested.__vs_key_0"));
        assert_eq!(params, vec!["tenant-a", "1", "tenant-b", "2"]);
        assert_eq!(active, keys);
    }

    #[test]
    fn batch_query_skips_keys_with_null_components() {
        let keys = vec![PkValue::from_values(&[json!("tenant-a"), Value::Null])];
        let (sql, params, active) = build_typed_batch_query(
            "public.items",
            &["tenant_id".into(), "id".into()],
            &keys,
            &[],
            &["text".into(), "bigint".into()],
        )
        .expect("batch query");
        assert!(sql.is_empty());
        assert!(params.is_empty());
        assert!(active.is_empty());
    }

    #[test]
    fn value_as_text_renders_canonical_forms() {
        assert_eq!(value_as_text(&json!(42)), "42");
        assert_eq!(value_as_text(&json!("alice")), "alice");
        assert_eq!(value_as_text(&json!(true)), "t");
        assert_eq!(value_as_text(&json!(null)), "");
        assert_eq!(value_as_text(&json!({"a":1})), r#"{"a":1}"#);
    }

    #[test]
    fn stringify_keeps_strings_and_nulls_coerces_numbers_and_bools() {
        let raw = json!({
            "id": 42,
            "name": "Alice",
            "active": true,
            "tier": null,
            "meta": { "nested": 1 },
            "tags": ["a", 2]
        });
        let out = stringify_top_level_values(raw);
        assert_eq!(out["id"], "42");
        assert_eq!(out["name"], "Alice");
        assert_eq!(out["active"], "t");
        assert!(out["tier"].is_null());
        // Nested object/array is preserved as its JSON-encoded string,
        // matching what pgoutput would deliver for a JSONB column.
        assert_eq!(out["meta"], r#"{"nested":1}"#);
        assert_eq!(out["tags"], r#"["a",2]"#);
    }

    #[test]
    fn decode_pk_handles_single_and_composite() {
        // PkValue text-normalizes numeric components (M2), so decode_pk yields
        // the text form. This is functionally harmless to the fetcher: the
        // typed WHERE clause casts the text bind back to the column type, and
        // `value_as_text` renders both number `5` and string `"5"` to "5".
        let single = PkValue::from_single(&json!(5));
        assert_eq!(decode_pk(&single), vec![json!("5")]);
        assert_eq!(value_as_text(&decode_pk(&single)[0]), "5");

        let composite = PkValue::from_values(&[json!("us-east"), json!(7)]);
        assert_eq!(decode_pk(&composite), vec![json!("us-east"), json!("7")]);
    }
}
