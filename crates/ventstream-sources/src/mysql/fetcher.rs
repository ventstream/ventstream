//! MySQL implementation of [`RelatedFetcher`] — sync-on-miss backfill.
//!
//! Issues `SELECT … WHERE col <=> ?` on a pooled connection (separate from the
//! binlog stream) and converts rows with the same `value_to_json` the source
//! uses, so backfilled and streamed rows are type-consistent at the sink.

use std::collections::HashSet;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Pool, Row, Value as MyValue};
use serde_json::{Map, Value};
use ventstream_joins::{FetchError, PkValue, RelatedFetcher};

use super::config::MySqlCdcConfig;
use super::schema::SchemaCache;
use super::value::value_to_json;

/// Max keys per batched related-row query. Mirrors the Postgres
/// fetcher's chunk and the engine's recompose batch cap, and bounds the
/// `UNION ALL` key list a single statement carries.
const BATCH_KEY_CHUNK: usize = 256;

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
        Self::new(Pool::new(config.opts()), config.database.clone())
    }

    /// `"ns.relation"` (or bare `"relation"`) -> physical relation name.
    fn relation_of<'a>(&self, table: &'a str) -> &'a str {
        table.rsplit('.').next().unwrap_or(table)
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

        let json_columns = self
            .schema
            .get(&self.pool, &self.database, relation)
            .await
            .map(|s| s.json_columns.clone())
            .unwrap_or_default();

        let where_sql = columns
            .iter()
            .map(|c| format!("`{}` <=> ?", esc(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT {proj} FROM `{db}`.`{rel}` WHERE {where_sql}{limit}",
            proj = projection_sql(select),
            db = esc(&self.database),
            rel = esc(relation),
            limit = if limit_one { " LIMIT 1" } else { "" },
        );
        let params = Params::Positional(components.into_iter().map(MyValue::from).collect());

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

    /// One set-based query for a chunk of keys: a derived table of
    /// `(ordinal, key components)` rows joined against the target with the
    /// same NULL-safe `<=>` comparison [`Self::query`] uses, so batched
    /// and per-key lookups coerce values identically. Returns
    /// `(ordinal, row)` pairs; the caller groups them back onto keys.
    async fn run_batch_query(
        &self,
        table: &str,
        fk_columns: &[String],
        keys: &[PkValue],
        select: &[String],
        json_columns: &HashSet<String>,
    ) -> Result<Vec<(usize, Value)>, FetchError> {
        let relation = self.relation_of(table);
        let mut params: Vec<MyValue> = Vec::with_capacity(keys.len() * (fk_columns.len() + 1));
        for (ordinal, key) in keys.iter().enumerate() {
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
            params.push(MyValue::from(ordinal as u64));
            params.extend(components.into_iter().map(MyValue::from));
        }

        let sql = build_batch_sql(&self.database, relation, fk_columns, keys.len(), select);
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

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            // Column 0 is the ordinal; everything after it is the row.
            let ordinal: u64 = row.get(0).ok_or_else(|| FetchError::Query {
                table: table.to_owned(),
                message: "batch query row is missing the key ordinal".to_owned(),
            })?;
            let ordinal = usize::try_from(ordinal).map_err(|_| FetchError::Query {
                table: table.to_owned(),
                message: format!("batch query returned invalid key ordinal {ordinal}"),
            })?;
            if ordinal >= keys.len() {
                return Err(FetchError::Query {
                    table: table.to_owned(),
                    message: format!("batch query returned out-of-range key ordinal {ordinal}"),
                });
            }
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
    /// instead, keyed by ordinal like the Postgres fetcher.
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

        let relation = self.relation_of(table);
        let json_columns = self
            .schema
            .get(&self.pool, &self.database, relation)
            .await
            .map(|s| s.json_columns.clone())
            .unwrap_or_default();

        let mut grouped: Vec<Vec<Value>> = vec![Vec::new(); unique.len()];
        for (chunk_index, chunk) in unique.chunks(BATCH_KEY_CHUNK).enumerate() {
            let base = chunk_index * BATCH_KEY_CHUNK;
            for (ordinal, row) in self
                .run_batch_query(table, fk_columns, chunk, select, &json_columns)
                .await?
            {
                if let Some(bucket) = grouped.get_mut(base + ordinal) {
                    bucket.push(row);
                }
            }
        }
        Ok(unique.into_iter().zip(grouped).collect())
    }
}

/// The batched lookup statement: a derived table of `(ordinal, key
/// components)` placeholder rows joined against the target relation.
///
/// `<=>` against a text placeholder coerces exactly like the per-key
/// path's `col <=> ?`, and the ordinal travels through the join so rows
/// group back onto keys without requiring the key columns in the
/// projection (the caller's `select` stays untouched). The ordinal
/// column's alias never collides because rows are decoded by position,
/// not name.
fn build_batch_sql(
    database: &str,
    relation: &str,
    fk_columns: &[String],
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
        .map(|(column, alias)| format!("t.`{}` <=> k.{alias}", esc(column)))
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

/// PkValue -> the text components bound as MySQL params. `col <=> ?` coerces
/// the string to the column type, so binding text matches any column type.
fn decode_pk(pk: &PkValue) -> Vec<String> {
    match pk.to_json() {
        Value::Array(arr) => arr.iter().map(value_as_text).collect(),
        other => vec![value_as_text(&other)],
    }
}

fn value_as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
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
    fn decode_pk_single_and_composite() {
        assert_eq!(
            decode_pk(&PkValue::from_single(&json!(5))),
            vec!["5".to_owned()]
        );
        assert_eq!(
            decode_pk(&PkValue::from_values(&[json!("ord-1"), json!(2)])),
            vec!["ord-1".to_owned(), "2".to_owned()]
        );
    }

    #[test]
    fn esc_doubles_backticks() {
        assert_eq!(esc("a`b"), "a``b");
    }

    /// The batch statement: ordinal-first derived table, one placeholder
    /// row per key, NULL-safe join per key column, projection untouched.
    #[test]
    fn batch_sql_shape() {
        let sql = build_batch_sql(
            "shop",
            "line_items",
            &["order_id".to_owned()],
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

    /// Identifier escaping flows through table, database, and columns.
    #[test]
    fn batch_sql_escapes_identifiers() {
        let sql = build_batch_sql("d`b", "re`l", &["c`ol".to_owned()], 1, &[]);
        assert!(sql.contains("JOIN `d``b`.`re``l` t"), "{sql}");
        assert!(sql.contains("t.`c``ol` <=> k.`_vs_k0`"), "{sql}");
    }
}
