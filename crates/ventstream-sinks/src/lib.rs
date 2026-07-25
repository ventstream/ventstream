//! Sink adapters for VentStream.
//!
//! Phase 0 target: [`opensearch`] — bulk indexing into OpenSearch (and
//! Elasticsearch, which shares the same wire protocol and bulk API).
//! Future adapters (S3 / Parquet, generic HTTP webhook, Snowflake) will
//! live in sibling modules under this crate.

pub mod error;
pub mod opensearch;

pub use error::OpenSearchSinkError;
pub use opensearch::{AuthMode, BulkConfig, OpenSearchConfig, OpenSearchSink, RetryConfig};
