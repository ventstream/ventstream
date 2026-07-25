//! Pluggable backfill — fetches a foreign row when the state doesn't
//! have it.
//!
//! Production implementations issue a SELECT against the source
//! database (a separate connection from the replication slot, so the
//! WAL stream isn't blocked). Tests use [`NoopFetcher`] or a hand-rolled
//! mock that returns whatever the test scenario needs.

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

use crate::config::Cardinality;
use crate::key::PkValue;

/// Lookup outcome — either a single object (cardinality:one) or a
/// list (cardinality:many).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOutcome {
    /// 1-to-1 lookup: zero or one row.
    One(Option<Value>),
    /// 1-to-many lookup: zero or more rows.
    Many(Vec<Value>),
}

impl FetchOutcome {
    /// Convenience: build a `One(None)` for a missing 1-to-1 row.
    pub const fn one_missing() -> Self {
        Self::One(None)
    }

    /// Convenience: build an empty `Many` for a missing 1-to-many.
    pub const fn many_empty() -> Self {
        Self::Many(Vec::new())
    }

    /// Match the [`Cardinality`] of an empty result.
    pub const fn empty_for(cardinality: Cardinality) -> Self {
        match cardinality {
            Cardinality::One => Self::one_missing(),
            Cardinality::Many => Self::many_empty(),
        }
    }
}

/// Failures from a fetcher.
#[derive(Debug, Error)]
pub enum FetchError {
    /// The data source could not be reached.
    #[error("data source unreachable: {0}")]
    Unreachable(String),

    /// The SELECT against the data source failed.
    #[error("query failed for table '{table}': {message}")]
    Query {
        /// Target table.
        table: String,
        /// Diagnostic message.
        message: String,
    },

    /// A row could not be decoded.
    #[error("decoding row from '{table}' failed: {message}")]
    Decode {
        /// Target table.
        table: String,
        /// Diagnostic message.
        message: String,
    },
}

/// Pluggable interface for resolving missing related rows.
///
/// Implementations:
/// - Must be `Send + Sync + 'static`.
/// - Should treat repeated calls for the same `(table, columns,
///   filter)` as idempotent so the engine can retry safely on
///   transient errors.
/// - SHOULD return `Ok(empty)` rather than `Err` for "row doesn't
///   exist" — `Err` is reserved for transport / decode failures.
#[async_trait]
pub trait RelatedFetcher: Send + Sync + 'static {
    /// Fetch one row by primary key.
    ///
    /// - `table` is the fully-qualified table name as declared in the
    ///   join config (e.g. `"public.customers"`).
    /// - `pk_columns` are the column names of the primary key.
    /// - `pk_value` is the value of the PK to look up.
    /// - `select` is the projection. Empty means "select all".
    async fn fetch_one(
        &self,
        table: &str,
        pk_columns: &[String],
        pk_value: &PkValue,
        select: &[String],
    ) -> Result<Option<Value>, FetchError>;

    /// Fetch every row where the named columns equal the given value.
    ///
    /// Used for `cardinality: many` lookups (e.g. all `line_items`
    /// with `order_id = 5`).
    async fn fetch_many(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_value: &PkValue,
        select: &[String],
    ) -> Result<Vec<Value>, FetchError>;

    /// Fetch rows for several distinct foreign-key values.
    ///
    /// The default implementation preserves compatibility by issuing one
    /// [`Self::fetch_many`] call per key. Database adapters should override
    /// this with a set-based query so one related-row CDC event can recompose
    /// many affected primaries without an N+1 query pattern.
    async fn fetch_many_batch(
        &self,
        table: &str,
        fk_columns: &[String],
        fk_values: &[PkValue],
        select: &[String],
    ) -> Result<Vec<(PkValue, Vec<Value>)>, FetchError> {
        let mut results = Vec::with_capacity(fk_values.len());
        for key in fk_values {
            let rows = self.fetch_many(table, fk_columns, key, select).await?;
            results.push((key.clone(), rows));
        }
        Ok(results)
    }
}

/// A fetcher that always returns "row not present". Useful when
/// `backfill.mode = none` so the engine can have a non-optional
/// dependency.
#[derive(Debug, Default)]
pub struct NoopFetcher;

#[async_trait]
impl RelatedFetcher for NoopFetcher {
    async fn fetch_one(
        &self,
        _table: &str,
        _pk_columns: &[String],
        _pk_value: &PkValue,
        _select: &[String],
    ) -> Result<Option<Value>, FetchError> {
        Ok(None)
    }

    async fn fetch_many(
        &self,
        _table: &str,
        _fk_columns: &[String],
        _fk_value: &PkValue,
        _select: &[String],
    ) -> Result<Vec<Value>, FetchError> {
        Ok(Vec::new())
    }
}
