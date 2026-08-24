//! Bulk-indexing sink for OpenSearch / Elasticsearch.
//!
//! The two systems share the same HTTP+JSON wire protocol and the same
//! Bulk API shape (`POST /_bulk` with NDJSON body), so this adapter
//! works against both. AWS SigV4 signing (for AWS-managed OpenSearch
//! domains) is the only meaningful divergence — and that lands as an
//! optional `auth: aws_sigv4` later.
//!
//! ### Submodules
//!
//! - [`config`]: typed configuration ([`OpenSearchConfig`], [`AuthMode`],
//!   [`BulkConfig`], [`RetryConfig`]).
//! - [`index_template`]: render the per-event target index name from a
//!   template (e.g. `events-${subject:1}-%Y-%m-%d`).
//! - [`bulk`]: NDJSON request builder and typed response parser.
//! - [`retry`]: exponential-backoff retry policy for transient failures.
//! - [`sink`]: the [`ventstream_core::Sink`] implementation.

pub mod bulk;
pub mod config;
pub mod index_template;
pub mod reconcile;
pub mod retry;
pub(crate) mod scroll;
pub mod sink;

pub use config::{AuthMode, BulkConfig, OpenSearchConfig, RetryConfig};
pub use reconcile::{
    encode_pk_key, reconcile_orphans, DocIdFormat, OsReverseLookup, ReconcileError,
};
pub use sink::OpenSearchSink;
