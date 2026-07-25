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
    let mut map = Map::new();
    for (i, col) in row.columns_ref().iter().enumerate() {
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
}
