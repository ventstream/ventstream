//! Redis materialization sink.

mod adaptive;
mod capability;
mod config;
mod contract;
mod diagnostic;
mod drift;
mod error;
pub(crate) mod keyspace;
mod ownership;
mod script;
mod sink;
pub(crate) mod topology;
mod view;

pub use capability::RedisViewSchemaStatus;
pub use config::{
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDocumentFormat, RedisKeyRouting,
    RedisKeyspaceOwnership, RedisSentinelTopology, RedisTlsConfig, RedisTopology, RedisView,
    RedisViewCondition, RedisViewConditionOperator, RedisViewFilter, RedisViewFilterMode,
    RedisViewKey, RedisViewMissingBehavior, RedisViewSource, RedisViewValue,
};
pub use diagnostic::RedisDiagnosticReport;
pub use drift::{RedisDriftReport, RedisTargetDrift};
pub use sink::RedisSink;
