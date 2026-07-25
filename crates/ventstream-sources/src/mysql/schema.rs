//! Per-table schema (PK columns/ordinals + JSON columns), cached.
//!
//! Binlog rows are positional and carry no column names, so we resolve the PK
//! (for doc-ids + the re-`SELECT` key) and JSON columns from `INFORMATION_SCHEMA`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::Pool;
use parking_lot::Mutex;

use crate::error::MySqlCdcError;

pub(crate) struct TableSchema {
    /// PK column names, in key order.
    pub(crate) pk_names: Vec<String>,
    /// PK positions in a binlog row (0-based), aligned with `pk_names`.
    pub(crate) pk_ordinals: Vec<usize>,
    /// Columns whose `DATA_TYPE` is `json` (parsed as nested JSON).
    pub(crate) json_columns: HashSet<String>,
}

type SchemaMap = HashMap<(String, String), Arc<TableSchema>>;

#[derive(Default)]
pub(crate) struct SchemaCache {
    inner: Mutex<SchemaMap>,
}

impl SchemaCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn get(
        &self,
        pool: &Pool,
        db: &str,
        table: &str,
    ) -> Result<Arc<TableSchema>, MySqlCdcError> {
        let key = (db.to_owned(), table.to_owned());
        if let Some(s) = self.inner.lock().get(&key).cloned() {
            return Ok(s);
        }
        let schema = Arc::new(load(pool, db, table).await?);
        self.inner.lock().insert(key, Arc::clone(&schema));
        Ok(schema)
    }
}

async fn load(pool: &Pool, db: &str, table: &str) -> Result<TableSchema, MySqlCdcError> {
    let op = |e: mysql_async::Error| MySqlCdcError::Operation(format!("schema lookup: {e}"));
    let mut conn = pool.get_conn().await.map_err(op)?;

    // (name, 1-based ordinal, data_type)
    let cols: Vec<(String, u32, String)> = conn
        .exec(
            "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (db, table),
        )
        .await
        .map_err(op)?;

    // PK columns in key order.
    let pk_names: Vec<String> = conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION",
            (db, table),
        )
        .await
        .map_err(op)?;

    let mut ordinal_of: HashMap<String, usize> = HashMap::new();
    let mut json_columns = HashSet::new();
    for (name, ord, dtype) in cols {
        ordinal_of.insert(name.clone(), (ord as usize).saturating_sub(1));
        if dtype.eq_ignore_ascii_case("json") {
            json_columns.insert(name);
        }
    }

    // MariaDB stores JSON as LONGTEXT guarded by a `json_valid()` CHECK
    // constraint, so DATA_TYPE='json' misses it. Pick up the guarded columns.
    // (information_schema.CHECK_CONSTRAINTS has no TABLE_NAME on MySQL 8 — that
    // query just errors there and we skip it; MySQL already reports 'json'.)
    if let Ok(clauses) = conn
        .exec::<String, _, _>(
            "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?",
            (db, table),
        )
        .await
    {
        for clause in clauses {
            if let Some(col) = json_valid_column(&clause) {
                json_columns.insert(col);
            }
        }
    }

    let pk_ordinals = pk_names
        .iter()
        .filter_map(|n| ordinal_of.get(n).copied())
        .collect();

    Ok(TableSchema {
        pk_names,
        pk_ordinals,
        json_columns,
    })
}

// Extract the column from a MariaDB `json_valid(`col`)` CHECK clause. Handles
// the backtick-quoted form MariaDB generates and a bare-identifier fallback.
fn json_valid_column(clause: &str) -> Option<String> {
    let lower = clause.to_ascii_lowercase();
    let start = lower.find("json_valid(")? + "json_valid(".len();
    let rest = clause[start..].trim_start();
    if let Some(rest) = rest.strip_prefix('`') {
        let end = rest.find('`')?;
        Some(rest[..end].to_owned())
    } else {
        let end = rest
            .find(|c: char| c == ')' || c.is_whitespace())
            .unwrap_or(rest.len());
        let col = rest[..end].trim();
        (!col.is_empty()).then(|| col.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::json_valid_column;

    #[test]
    fn parses_backtick_quoted_column() {
        assert_eq!(
            json_valid_column("json_valid(`meta`)").as_deref(),
            Some("meta")
        );
    }

    #[test]
    fn parses_bare_column_and_ignores_unrelated() {
        assert_eq!(
            json_valid_column("json_valid(payload)").as_deref(),
            Some("payload")
        );
        assert_eq!(json_valid_column("length(name) > 0"), None);
    }
}
