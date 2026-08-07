use serde::Serialize;
use ventstream_core::SinkError;

use super::capability::{
    inspect_view_schema, validate_document_capability, validate_operation_capabilities,
    validate_view_capabilities, RedisViewSchemaStatus,
};
use super::config::{
    RedisAcknowledgement, RedisConfig, RedisDocumentFormat, RedisKeyRouting, RedisTopology,
};
use super::error::map_connect_error;
use super::topology::{build_connector, connect_raw};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
/// Non-secret facts collected by an online Redis sink preflight.
pub struct RedisDiagnosticReport {
    /// Configured discovery mode.
    pub topology: String,
    /// Whether data-node transport uses TLS.
    pub tls: bool,
    /// Whether Redis authentication is configured.
    pub authenticated: bool,
    /// Redis server version when `INFO` is permitted.
    pub server_version: Option<String>,
    /// Redis server mode when `INFO` is permitted.
    pub server_mode: Option<String>,
    /// Role of the node reached by the diagnostic connection.
    pub server_role: Option<String>,
    /// Connected replicas reported by the reached primary.
    pub connected_replicas: Option<u64>,
    /// Replica acknowledgements required by the sink contract.
    pub required_replica_acks: u64,
    /// Replica acknowledgements observed by the capability write.
    pub observed_replica_acks: Option<u64>,
    /// Whether the sink contract requires a local primary AOF fsync.
    pub required_local_aof: bool,
    /// Local primary AOF fsync acknowledgements observed by the capability write.
    pub observed_local_aof_acks: Option<u64>,
    /// Whether the configured document format requires RedisJSON.
    pub redis_json: bool,
    /// Number of configured lookup views.
    pub view_count: usize,
    /// Compatibility of configured views with stored schema metadata.
    pub view_schema: RedisViewSchemaStatus,
}

pub(super) async fn diagnose(config: &RedisConfig) -> Result<RedisDiagnosticReport, SinkError> {
    config.validate().map_err(SinkError::Blocked)?;
    let connector = build_connector(config).await?;
    let mut connection = connect_raw(&connector, config).await?;
    validate_document_capability(config, &mut connection).await?;
    validate_view_capabilities(config, &mut connection).await?;
    let acknowledgement = validate_operation_capabilities(config, &mut connection).await?;
    let view_schema = inspect_view_schema(config, &mut connection).await?;

    let info = redis::cmd("INFO")
        .query_async::<String>(&mut connection)
        .await;
    let (server_version, server_mode, server_role, connected_replicas) = match info {
        Ok(info) => (
            info_field(&info, "redis_version").map(str::to_owned),
            info_field(&info, "redis_mode").map(str::to_owned),
            info_field(&info, "role").map(str::to_owned),
            info_field(&info, "connected_slaves").and_then(|value| value.parse().ok()),
        ),
        Err(error) if error.code() == Some("NOPERM") => (None, None, None, None),
        Err(error) => return Err(map_connect_error(&error, config)),
    };

    let (topology, tls) = match &config.topology {
        RedisTopology::Standalone { endpoint } => {
            ("standalone".to_owned(), endpoint.starts_with("rediss://"))
        }
        RedisTopology::Sentinel(sentinel) => ("sentinel".to_owned(), sentinel.data_node_tls),
        RedisTopology::Cluster { endpoints } => (
            "cluster".to_owned(),
            endpoints
                .first()
                .is_some_and(|endpoint| endpoint.starts_with("rediss://")),
        ),
    };
    let (required_local_aof, required_replica_acks) = match config.acknowledgement {
        RedisAcknowledgement::Primary => (false, 0),
        RedisAcknowledgement::Replicated { replicas, .. } => {
            (false, u64::try_from(replicas).unwrap_or(u64::MAX))
        }
        RedisAcknowledgement::Aof {
            local, replicas, ..
        } => (local, u64::try_from(replicas).unwrap_or(u64::MAX)),
    };

    Ok(RedisDiagnosticReport {
        topology,
        tls,
        authenticated: config.password.is_some(),
        server_version,
        server_mode,
        server_role,
        connected_replicas,
        required_replica_acks,
        observed_replica_acks: acknowledgement.replica_acks,
        required_local_aof,
        observed_local_aof_acks: acknowledgement.local_aof_acks,
        redis_json: config.document_format == RedisDocumentFormat::Json,
        view_count: match &config.key_routing {
            RedisKeyRouting::Views(views) => views.len(),
            _ => 0,
        },
        view_schema,
    })
}

fn info_field<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    info.lines()
        .filter_map(|line| line.trim_end_matches('\r').split_once(':'))
        .find_map(|(name, value)| (name == field).then_some(value))
}

#[cfg(test)]
mod tests {
    use super::info_field;

    #[test]
    fn extracts_exact_info_fields() {
        let info = "# Server\r\nredis_version:7.4.2\r\nredis_mode:standalone\r\nrole:master\r\n";
        assert_eq!(info_field(info, "redis_version"), Some("7.4.2"));
        assert_eq!(info_field(info, "redis_mode"), Some("standalone"));
        assert_eq!(info_field(info, "version"), None);
    }
}
