//! MySQL implementation of [`RelatedFetcher`] — sync-on-miss backfill.
//!
//! Issues `SELECT … WHERE col <=> ?` on a pooled connection (separate from the
//! binlog stream) and converts rows with the same `value_to_json` the source
//! uses, so backfilled and streamed rows are type-consistent at the sink.
//! Key parameters bind as text, with integer columns wrapped in
//! `CAST(? AS SIGNED/UNSIGNED)` — without the cast MySQL compares
//! varchar-vs-integer as `double`, whose 53-bit mantissa makes distinct
//! 64-bit keys compare equal on unindexed columns.

use std::collections::HashSet;

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Pool, Row, Value as MyValue};
use serde_json::{Map, Value};
use tracing::debug;
use ventstream_joins::{FetchError, PkValue, RelatedFetcher};

use super::config::MySqlCdcConfig;
use super::schema::{SchemaCache, TableSchema};
use super::value::value_to_json;

/// Max keys per batched related-row query. Mirrors the Postgres
/// fetcher's chunk and the engine's recompose batch cap, and bounds the
/// `UNION ALL` key list a single statement carries.
const BATCH_KEY_CHUNK: usize = 256;

/// Chunk queries in flight at once when a batch exceeds one chunk. The
/// pool hands out one connection per chunk; four keeps a 1,000-key batch
/// near one chunk's latency without hogging the pool.
const BATCH_CONCURRENCY: usize = 4;

/// How a key component placeholder is typed in SQL.
///
/// Text params against an integer column with no index are compared as
/// `double` by MySQL, and above 2^53 distinct keys collide — a parent
/// silently receives another parent's children. Casting restores exact
/// integer comparison; everything else keeps the plain text coercion
/// the streaming path relies on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeyCast {
    /// Bind the text as-is (`?`).
    Text,
    /// `CAST(? AS SIGNED)` — signed integer column.
    Signed,
    /// `CAST(? AS UNSIGNED)` — unsigned integer column; `SIGNED` would
    /// overflow a `bigint unsigned` key above `i64::MAX`.
    Unsigned,
}

impl KeyCast {
    fn placeholder(self) -> &'static str {
        match self {
            Self::Text => "?",
            Self::Signed => "CAST(? AS SIGNED)",
            Self::Unsigned => "CAST(? AS UNSIGNED)",
        }
    }

    /// Wrap a derived-table column reference the way [`Self::placeholder`]
    /// wraps a direct parameter.
    fn wrap(self, expr: &str) -> String {
        match self {
            Self::Text => expr.to_owned(),
            Self::Signed => format!("CAST({expr} AS SIGNED)"),
            Self::Unsigned => format!("CAST({expr} AS UNSIGNED)"),
        }
    }
}

/// The cast each key column needs, from the cached schema. Unknown
/// columns (or no schema at all) fall back to plain text binding — the
/// pre-cast behavior.
fn key_casts(schema: Option<&TableSchema>, columns: &[String]) -> Vec<KeyCast> {
    columns
        .iter()
        .map(|column| {
            let Some(schema) = schema else {
                return KeyCast::Text;
            };
            let Some(index) = schema.column_names.iter().position(|name| name == column) else {
                return KeyCast::Text;
            };
            let data_type = schema
                .column_types
                .get(index)
                .map(String::as_str)
                .unwrap_or_default();
            if matches!(
                data_type,
                "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
            ) {
                if schema.unsigned_columns.contains(column) {
                    KeyCast::Unsigned
                } else {
                    KeyCast::Signed
                }
            } else {
                KeyCast::Text
            }
        })
        .collect()
}

/// The per-table inputs every chunk of one batched lookup shares.
#[derive(Clone, Copy)]
struct BatchContext<'a> {
    table: &'a str,
    fk_columns: &'a [String],
    casts: &'a [KeyCast],
    select: &'a [String],
    json_columns: &'a HashSet<String>,
}

/// MySQL-backed [`RelatedFetcher`]. The physical database is fixed (`database`);
/// a join `table` of `"namespace.relation"` resolves to `` `database`.`relation` ``.
pub struct MySqlFetcher {
    pool: Pool,
    database: String,
    schema: SchemaCache,
}

impl MySqlFetcher {
    /// Construct from a pool and the physical database name.
    pub fn new(pool: Pool, database: impl Into<String>) -> Self {
        Self {
            pool,
            database: database.into(),
            schema: SchemaCache::new(),
        }
    }

    /// Build a fetcher (its own pool) from the source config. Used by the
    /// engine wiring so `main` needn't touch `mysql_async` types directly.
    pub fn connect(config: &MySqlCdcConfig) -> Self {
        // The batch lookup prepares one distinct statement per (table,
        // key count, projection) — up to BATCH_KEY_CHUNK texts per table.
        // mysql_async's default statement cache holds 10, which would
        // evict them constantly and pay a prepare round trip per batch.
        let opts = mysql_async::OptsBuilder::from_opts(config.opts()).stmt_cache_size(512);
        Self::new(Pool::new(opts), config.database.clone())
    }

    /// `"ns.relation"` (or bare `"relation"`) -> physical relation name.
    fn relation_of<'a>(&self, table: &'a str) -> &'a str {
        table.rsplit('.').next().unwrap_or(table)
    }

    /// Cached schema for a join table; `None` when the lookup fails —
    /// callers then skip JSON parsing and typed casts rather than
    /// failing the fetch (the pre-schema behavior).
    async fn table_schema(&self, table: &str) -> Option<std::sync::Arc<TableSchema>> {
        let relation = self.relation_of(table);
        self.schema
            .get(&self.pool, &self.database, relation)
            .await
            .ok()
    }

    async fn query(
        &self,
        table: &str,
        columns: &[String],
        value: &PkValue,
        select: &[String],
        limit_one: bool,
    ) -> Result<Vec<Value>, FetchError> {
        let relation = self.relation_of(table);
        let components = decode_pk(value);
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

        let schema = self.table_schema(table).await;
        let json_columns = schema
            .as_deref()
            .map(|s| s.json_columns.clone())
            .unwrap_or_default();
        let casts = key_casts(schema.as_deref(), columns);

        let where_sql = columns
            .iter()
            .zip(&casts)
            .map(|(c, cast)| format!("`{}` <=> {}", esc(c), cast.placeholder()))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT {proj} FROM `{db}`.`{rel}` WHERE {where_sql}{limit}",
            proj = projection_sql(select),
            db = esc(&self.database),
            rel = esc(relation),
            limit = if limit_one { " LIMIT 1" } else { "" },
        );
        let params = Params::Positional(components);

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| FetchError::Unreachable(e.to_string()))?;
        let rows: Vec<Row> = conn
            .exec(&sql, params)
            .await
            .map_err(|e| FetchError::Query {
                table: table.to_owned(),
                message: e.to_string(),
            })?;
        drop(conn);

        Ok(rows.iter().map(|r| row_to_json(r, &json_columns)).collect())
    }

    /// One set-based query for a chunk of `(global ordinal, components)`
    /// entries: a derived table of `(ordinal, key components)` rows joined
    /// against the target with the same NULL-safe `<=>` comparison and
    /// typed casts [`Self::query`] uses, so batched and per-key lookups
    /// coerce values identically. Returns `(global ordinal, row)` pairs;
    /// the caller groups them back onto keys.
    async fn run_batch_query(
        &self,
        ctx: &BatchContext<'_>,
        entries: &[(usize, Vec<MyValue>)],
    ) -> Result<Vec<(usize, Value)>, FetchError> {
        let BatchContext {
            table,
            fk_columns,
            casts,
            select,
            json_columns,
        } = *ctx;
        let relation = self.relation_of(table);
        let mut params: Vec<MyValue> = Vec::with_capacity(entries.len() * (fk_columns.len() + 1));
        for (ordinal, components) in entries {
            params.push(MyValue::from(*ordinal as u64));
            params.extend(components.iter().cloned());
        }

        let sql = build_batch_sql(
            &self.database,
            relation,
            fk_columns,
            casts,
            entries.len(),
            select,
        );
        let started = std::time::Instant::now();
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| FetchError::Unreachable(e.to_string()))?;
        let rows: Vec<Row> = conn
            .exec(&sql, Params::Positional(params))
            .await
            .map_err(|e| FetchError::Query {
                table: table.to_owned(),
                message: e.to_string(),
            })?;
        drop(conn);
        debug!(
            table,
            keys = entries.len(),
            rows = rows.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            metric = "mysql.fetcher.batch_query",
            "mysql fetcher batch query complete"
        );

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            // Column 0 is the ordinal; everything after it is the row.
            // `Row::get` panics on a conversion failure (`None` only means
            // out-of-range), so decode through `get_opt`.
            let ordinal: u64 = match row.get_opt(0) {
                Some(Ok(ordinal)) => ordinal,
                Some(Err(err)) => {
                    return Err(FetchError::Query {
                        table: table.to_owned(),
                        message: format!("decoding batch key ordinal: {err}"),
                    })
                }
                None => {
                    return Err(FetchError::Query {
                        table: table.to_owned(),
                        message: "batch query row is missing the key ordinal".to_owned(),
                    })
                }
            };
            let ordinal = usize::try_from(ordinal).map_err(|_| FetchError::Query {
                table: table.to_owned(),
                message: format!("batch query returned invalid key ordinal {ordinal}"),
            })?;
            out.push((ordinal, row_to_json_skipping(row, json_columns, 1)));
        }
        Ok(out)
    }
}

#[async_trait]
impl RelatedFetcher for MySqlFetcher {
    async fn fetch_one(
        &self,
        table: &str,
        pk_columns: &[String],
        pk_value: &PkValue,
        select: &[String],
    ) -> Result<Option<Value>, FetchError> {
        Ok(self
            .query(table, pk_columns, pk_value, select, true)
            .await?
            .into_iter()
            .next())
    }

    async fn fetch_many(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_value: &PkValue,
        select: &[String],
    ) -> Result<Vec<Value>, FetchError> {
        self.query(table, fk_columns, fk_value, select, false).await
    }

    /// Set-based override of the trait's per-key loop (#136): one
    /// related-row CDC event can recompose up to the engine's full
    /// recompose batch of primaries, and the default implementation
    /// turned that into one round-trip per key — the exact N+1 this
    /// method exists to avoid. One query per `BATCH_KEY_CHUNK` keys
    /// instead, keyed by ordinal like the Postgres fetcher; chunks run
    /// concurrently. Keys with a NULL component are returned with empty
    /// groups without querying, matching the Postgres batch builder —
    /// a SQL join never pairs rows on NULL.
    async fn fetch_many_batch(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_values: &[PkValue],
        select: &[String],
    ) -> Result<Vec<(PkValue, Vec<Value>)>, FetchError> {
        if fk_columns.is_empty() {
            return Err(FetchError::Query {
                table: table.to_owned(),
                message: "batch lookup requires at least one key column".to_owned(),
            });
        }
        let mut seen = HashSet::with_capacity(fk_values.len());
        let unique: Vec<PkValue> = fk_values
            .iter()
            .filter(|key| !key.is_null() && seen.insert((*key).clone()))
            .cloned()
            .collect();
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        let schema = self.table_schema(table).await;
        let json_columns = schema
            .as_deref()
            .map(|s| s.json_columns.clone())
            .unwrap_or_default();
        let casts = key_casts(schema.as_deref(), fk_columns);

        // Decode and validate every key up front; keys with a NULL
        // component keep their (empty) slot but are never queried.
        let mut queryable: Vec<(usize, Vec<MyValue>)> = Vec::with_capacity(unique.len());
        for (index, key) in unique.iter().enumerate() {
            let components = decode_pk(key);
            if components.len() != fk_columns.len() {
                return Err(FetchError::Query {
                    table: table.to_owned(),
                    message: format!(
                        "expected {} key component(s), got {}",
                        fk_columns.len(),
                        components.len()
                    ),
                });
            }
            if components.contains(&MyValue::NULL) {
                continue;
            }
            queryable.push((index, components));
        }

        let mut grouped: Vec<Vec<Value>> = vec![Vec::new(); unique.len()];
        let ctx = BatchContext {
            table,
            fk_columns,
            casts: &casts,
            select,
            json_columns: &json_columns,
        };
        let queries = queryable
            .chunks(BATCH_KEY_CHUNK)
            .map(|chunk| self.run_batch_query(&ctx, chunk))
            .collect::<Vec<_>>();
        let chunk_results: Vec<Vec<(usize, Value)>> = stream::iter(queries)
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await?;
        for (ordinal, row) in chunk_results.into_iter().flatten() {
            let Some(bucket) = grouped.get_mut(ordinal) else {
                return Err(FetchError::Query {
                    table: table.to_owned(),
                    message: format!("batch query returned out-of-range key ordinal {ordinal}"),
                });
            };
            bucket.push(row);
        }
        Ok(unique.into_iter().zip(grouped).collect())
    }
}

/// The batched lookup statement: a derived table of `(ordinal, key
/// components)` placeholder rows joined against the target relation.
///
/// The join compares with `<=>` against the (possibly cast) key column,
/// coercing exactly like the per-key path, and the ordinal travels
/// through the join so rows group back onto keys without requiring the
/// key columns in the projection (the caller's `select` stays
/// untouched). The ordinal column's alias never collides because rows
/// are decoded by position, not name.
fn build_batch_sql(
    database: &str,
    relation: &str,
    fk_columns: &[String],
    casts: &[KeyCast],
    key_count: usize,
    select: &[String],
) -> String {
    let key_aliases: Vec<String> = (0..fk_columns.len())
        .map(|i| format!("`_vs_k{i}`"))
        .collect();
    let first_row = std::iter::once("? AS `_vs_ord`".to_owned())
        .chain(key_aliases.iter().map(|alias| format!("? AS {alias}")))
        .collect::<Vec<_>>()
        .join(", ");
    let later_row = std::iter::repeat_n("?", fk_columns.len() + 1)
        .collect::<Vec<_>>()
        .join(", ");
    let keys_sql = (0..key_count)
        .map(|i| {
            if i == 0 {
                format!("SELECT {first_row}")
            } else {
                format!("UNION ALL SELECT {later_row}")
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let on_sql = fk_columns
        .iter()
        .zip(&key_aliases)
        .zip(casts.iter().chain(std::iter::repeat(&KeyCast::Text)))
        .map(|((column, alias), cast)| {
            format!(
                "t.`{}` <=> {}",
                esc(column),
                cast.wrap(&format!("k.{alias}"))
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let projection = if select.is_empty() {
        "t.*".to_owned()
    } else {
        select
            .iter()
            .map(|c| format!("t.`{}`", esc(c)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "SELECT k.`_vs_ord`, {projection} FROM ({keys_sql}) k \
         JOIN `{db}`.`{rel}` t ON {on_sql}",
        db = esc(database),
        rel = esc(relation),
    )
}

/// `select: []` -> `*`; else backtick-quoted columns.
fn projection_sql(select: &[String]) -> String {
    if select.is_empty() {
        return "*".to_owned();
    }
    select
        .iter()
        .map(|c| format!("`{}`", esc(c)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Row -> JSON, matching the source's `row_to_json` so fetched and streamed
/// rows agree on types (incl. JSON columns parsed to nested JSON).
fn row_to_json(row: &Row, json_columns: &HashSet<String>) -> Value {
    row_to_json_skipping(row, json_columns, 0)
}

/// [`row_to_json`], ignoring the first `skip` columns — the batch query
/// prefixes each row with its key ordinal, which must not leak into the
/// composed document.
fn row_to_json_skipping(row: &Row, json_columns: &HashSet<String>, skip: usize) -> Value {
    let mut map = Map::new();
    for (i, col) in row.columns_ref().iter().enumerate().skip(skip) {
        let name = col.name_str().into_owned();
        let v = row.as_ref(i).cloned().unwrap_or(MyValue::NULL);
        let is_json = json_columns.contains(&name);
        map.insert(name, value_to_json(&v, is_json));
    }
    Value::Object(map)
}

/// PkValue -> the components bound as MySQL params: text for everything
/// present (`col <=> ?`, optionally cast, coerces it to the column
/// type), and a real SQL `NULL` for a null component — binding `''`
/// would match rows where the column is `0` or `''` and never the NULL
/// rows.
fn decode_pk(pk: &PkValue) -> Vec<MyValue> {
    match pk.to_json() {
        Value::Array(arr) => arr.iter().map(component_value).collect(),
        other => vec![component_value(&other)],
    }
}

fn component_value(v: &Value) -> MyValue {
    match v {
        Value::Null => MyValue::NULL,
        Value::String(s) => MyValue::from(s.as_str()),
        other => MyValue::from(other.to_string()),
    }
}

fn esc(ident: &str) -> String {
    ident.replace('`', "``")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projection_empty_is_star() {
        assert_eq!(projection_sql(&[]), "*");
        assert_eq!(
            projection_sql(&["id".to_owned(), "name".to_owned()]),
            "`id`, `name`"
        );
    }

    #[test]
    fn decode_pk_single_composite_and_null() {
        assert_eq!(
            decode_pk(&PkValue::from_single(&json!(5))),
            vec![MyValue::from("5")]
        );
        assert_eq!(
            decode_pk(&PkValue::from_values(&[json!("ord-1"), json!(2)])),
            vec![MyValue::from("ord-1"), MyValue::from("2")]
        );
        // A null component binds as SQL NULL, never as ''.
        assert_eq!(
            decode_pk(&PkValue::from_values(&[json!(1), json!(null)])),
            vec![MyValue::from("1"), MyValue::NULL]
        );
    }

    #[test]
    fn esc_doubles_backticks() {
        assert_eq!(esc("a`b"), "a``b");
    }

    /// Integer key columns get exact casts; everything else binds text.
    #[test]
    fn key_casts_follow_the_schema() {
        let schema = TableSchema {
            column_names: vec!["id".into(), "ref_id".into(), "code".into()],
            column_types: vec!["int".into(), "bigint".into(), "varchar".into()],
            pk_names: vec!["id".into()],
            pk_ordinals: vec![0],
            json_columns: HashSet::new(),
            unsigned_columns: ["ref_id".to_owned()].into_iter().collect(),
            enum_labels: Default::default(),
            set_labels: Default::default(),
        };
        assert_eq!(
            key_casts(
                Some(&schema),
                &["id".to_owned(), "ref_id".to_owned(), "code".to_owned()]
            ),
            vec![KeyCast::Signed, KeyCast::Unsigned, KeyCast::Text]
        );
        // Unknown column or missing schema: plain text binding.
        assert_eq!(
            key_casts(Some(&schema), &["ghost".to_owned()]),
            vec![KeyCast::Text]
        );
        assert_eq!(key_casts(None, &["id".to_owned()]), vec![KeyCast::Text]);
    }

    /// The batch statement: ordinal-first derived table, one placeholder
    /// row per key, NULL-safe join per key column, projection untouched.
    #[test]
    fn batch_sql_shape() {
        let sql = build_batch_sql(
            "shop",
            "line_items",
            &["order_id".to_owned()],
            &[KeyCast::Text],
            3,
            &["id".to_owned(), "qty".to_owned()],
        );
        assert_eq!(
            sql,
            "SELECT k.`_vs_ord`, t.`id`, t.`qty` FROM \
             (SELECT ? AS `_vs_ord`, ? AS `_vs_k0` \
             UNION ALL SELECT ?, ? UNION ALL SELECT ?, ?) k \
             JOIN `shop`.`line_items` t ON t.`order_id` <=> k.`_vs_k0`"
        );
    }

    /// Composite keys join on every component; `select: []` stays `t.*`.
    #[test]
    fn batch_sql_composite_key_and_star_projection() {
        let sql = build_batch_sql(
            "shop",
            "items",
            &["tenant_id".to_owned(), "order_id".to_owned()],
            &[KeyCast::Text, KeyCast::Text],
            2,
            &[],
        );
        assert_eq!(
            sql,
            "SELECT k.`_vs_ord`, t.* FROM \
             (SELECT ? AS `_vs_ord`, ? AS `_vs_k0`, ? AS `_vs_k1` \
             UNION ALL SELECT ?, ?, ?) k \
             JOIN `shop`.`items` t ON \
             t.`tenant_id` <=> k.`_vs_k0` AND t.`order_id` <=> k.`_vs_k1`"
        );
    }

    /// Integer columns compare through CAST on the derived-table side so
    /// 64-bit keys never degrade to double comparison on unindexed
    /// columns.
    #[test]
    fn batch_sql_casts_integer_keys() {
        let sql = build_batch_sql(
            "shop",
            "children",
            &["parent_id".to_owned(), "ref_id".to_owned()],
            &[KeyCast::Signed, KeyCast::Unsigned],
            1,
            &[],
        );
        assert!(
            sql.contains("t.`parent_id` <=> CAST(k.`_vs_k0` AS SIGNED)"),
            "{sql}"
        );
        assert!(
            sql.contains("t.`ref_id` <=> CAST(k.`_vs_k1` AS UNSIGNED)"),
            "{sql}"
        );
    }

    /// Identifier escaping flows through table, database, and columns.
    #[test]
    fn batch_sql_escapes_identifiers() {
        let sql = build_batch_sql(
            "d`b",
            "re`l",
            &["c`ol".to_owned()],
            &[KeyCast::Text],
            1,
            &[],
        );
        assert!(sql.contains("JOIN `d``b`.`re``l` t"), "{sql}");
        assert!(sql.contains("t.`c``ol` <=> k.`_vs_k0`"), "{sql}");
    }
}
