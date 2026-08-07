use std::time::Duration;

use super::config::{RedisConfig, RedisContract, RedisDocumentFormat};

pub(super) fn view_redis_operation(config: &RedisConfig) -> &'static str {
    match (config.document_format, config.contract) {
        (RedisDocumentFormat::String, RedisContract::MaterializedView) => "S",
        (RedisDocumentFormat::String, RedisContract::Cache { .. }) => "SP",
        (RedisDocumentFormat::Json, RedisContract::MaterializedView) => "J",
        (RedisDocumentFormat::Json, RedisContract::Cache { .. }) => "JP",
    }
}

pub(super) fn redis_operation_ttl(config: &RedisConfig) -> u64 {
    match config.contract {
        RedisContract::MaterializedView => 0,
        RedisContract::Cache { ttl } => duration_millis(ttl),
    }
}

pub(super) fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
