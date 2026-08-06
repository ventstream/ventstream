//! Sink adapters for VentStream.
//!
//! Materialization adapters for VentStream CDC pipelines.

pub mod error;
pub mod opensearch;
pub mod redis;

pub use error::OpenSearchSinkError;
pub use opensearch::{AuthMode, BulkConfig, OpenSearchConfig, OpenSearchSink, RetryConfig};
pub use redis::{
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDiagnosticReport, RedisDocumentFormat,
    RedisDriftReport, RedisKeyRouting, RedisKeyspaceOwnership, RedisSentinelTopology, RedisSink,
    RedisTargetDrift, RedisTlsConfig, RedisTopology, RedisView, RedisViewCondition,
    RedisViewConditionOperator, RedisViewFilter, RedisViewFilterMode, RedisViewKey,
    RedisViewMissingBehavior, RedisViewSchemaStatus, RedisViewSource, RedisViewValue,
};
