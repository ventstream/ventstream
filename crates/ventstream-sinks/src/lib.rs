//! Sink adapters for VentStream.
//!
//! Materialization adapters for VentStream CDC pipelines.

pub mod error;
pub mod meilisearch;
pub mod opensearch;
pub mod read;
pub mod redis;
pub mod reverse_lookup;
mod util;

pub use error::OpenSearchSinkError;
pub use meilisearch::{
    MeilisearchBatchConfig, MeilisearchConfig, MeilisearchIndexRouting, MeilisearchSettings,
    MeilisearchSink, MeilisearchSinkError, MeilisearchTaskConfig,
};
pub use opensearch::{AuthMode, BulkConfig, OpenSearchConfig, OpenSearchSink, RetryConfig};
pub use redis::{
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDiagnosticReport, RedisDocumentFormat,
    RedisDriftReport, RedisKeyRouting, RedisKeyspaceOwnership, RedisSentinelTopology, RedisSink,
    RedisTargetDrift, RedisTlsConfig, RedisTopology, RedisView, RedisViewCondition,
    RedisViewConditionOperator, RedisViewFilter, RedisViewFilterMode, RedisViewKey,
    RedisViewMissingBehavior, RedisViewSchemaStatus, RedisViewSource, RedisViewValue,
};
pub use reverse_lookup::ReverseLookup;
