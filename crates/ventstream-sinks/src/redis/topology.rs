use redis::aio::{
    ConnectionLike, ConnectionManager, ConnectionManagerConfig, MultiplexedConnection,
};
use redis::{
    AsyncConnectionConfig, ClientTlsConfig, Cmd, ConnectionAddr, ErrorKind, FromRedisValue,
    IntoConnectionInfo, ServerErrorKind, TlsCertificates, TlsMode,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use ventstream_core::SinkError;

use super::config::{RedisConfig, RedisTlsConfig, RedisTopology};
use super::error::map_connect_error;

pub(crate) enum RedisConnector {
    Standalone(redis::Client),
    Sentinel(Mutex<redis::sentinel::SentinelClient>),
    Cluster(redis::cluster::ClusterClient),
}

pub(crate) enum RedisConnection {
    Standalone(ConnectionManager),
    Sentinel(MultiplexedConnection),
    Cluster(redis::cluster_async::ClusterConnection),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct RedisCredentialRevision([u8; 32]);

struct ResolvedCredentials {
    username: Option<String>,
    password: Option<String>,
    sentinel_username: Option<String>,
    sentinel_password: Option<String>,
    revision: RedisCredentialRevision,
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        command: &'a Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        match self {
            Self::Standalone(connection) => connection.req_packed_command(command),
            Self::Sentinel(connection) => connection.req_packed_command(command),
            Self::Cluster(connection) => connection.req_packed_command(command),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        match self {
            Self::Standalone(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Sentinel(connection) => connection.req_packed_commands(pipeline, offset, count),
            Self::Cluster(connection) => connection.req_packed_commands(pipeline, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Standalone(connection) => connection.get_db(),
            Self::Sentinel(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

impl RedisConnection {
    pub(crate) async fn query_on_primary<T: FromRedisValue>(
        &mut self,
        command: Cmd,
        key: &str,
    ) -> redis::RedisResult<T> {
        match self {
            Self::Cluster(connection) => {
                let routing = redis::cluster_routing::RoutingInfo::SingleNode(
                    redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(
                        redis::cluster_routing::Route::with_key(
                            key,
                            redis::cluster_routing::SlotAddr::Master,
                        ),
                    ),
                );
                let value = connection.route_command(command, routing).await?;
                redis::from_redis_value(value).map_err(Into::into)
            }
            _ => command.query_async(self).await,
        }
    }

    pub(super) async fn stable_cluster_slot_owner(
        &mut self,
        key: &str,
    ) -> redis::RedisResult<Option<String>> {
        if !matches!(self, Self::Cluster(_)) {
            return Ok(None);
        }
        let mut slot_command = redis::cmd("CLUSTER");
        slot_command.arg("KEYSLOT").arg(key);
        let slot = self.query_on_primary::<u16>(slot_command, key).await?;

        let mut id_command = redis::cmd("CLUSTER");
        id_command.arg("MYID");
        let node_id = self.query_on_primary::<String>(id_command, key).await?;

        let mut nodes_command = redis::cmd("CLUSTER");
        nodes_command.arg("NODES");
        let nodes = self.query_on_primary::<String>(nodes_command, key).await?;
        validate_cluster_slot_owner(&nodes, &node_id, slot)?;
        Ok(Some(node_id))
    }
}

pub(super) fn validate_cluster_slot_owner(
    nodes: &str,
    routed_node_id: &str,
    slot: u16,
) -> redis::RedisResult<()> {
    let fields = nodes
        .lines()
        .map(|line| line.split_ascii_whitespace().collect::<Vec<_>>())
        .find(|fields| {
            fields
                .get(2)
                .is_some_and(|flags| flags.split(',').any(|flag| flag == "myself"))
        })
        .ok_or_else(|| {
            redis::RedisError::from((
                ErrorKind::UnexpectedReturnType,
                "CLUSTER NODES did not identify the routed node",
            ))
        })?;
    if fields.first().copied() != Some(routed_node_id) {
        return Err(redis::RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "CLUSTER MYID and CLUSTER NODES returned different routed nodes",
        )));
    }
    let slot_fields = fields.get(8..).unwrap_or_default();
    if slot_fields
        .iter()
        .any(|field| cluster_migration_token_slot(field) == Some(slot))
    {
        return Err(cluster_slot_retry_error(
            "Redis Cluster slot migration is active during target cleanup",
        ));
    }
    if !slot_fields
        .iter()
        .any(|field| cluster_owned_slot_field_contains(field, slot))
    {
        return Err(cluster_slot_retry_error(
            "Redis Cluster slot ownership changed during target cleanup",
        ));
    }
    Ok(())
}

fn cluster_migration_token_slot(field: &str) -> Option<u16> {
    let token = field.strip_prefix('[')?.strip_suffix(']')?;
    token
        .split_once("->-")
        .or_else(|| token.split_once("-<-"))?
        .0
        .parse()
        .ok()
}

fn cluster_owned_slot_field_contains(field: &str, slot: u16) -> bool {
    if field.starts_with('[') {
        return false;
    }
    match field.split_once('-') {
        Some((start, end)) => start
            .parse::<u16>()
            .ok()
            .zip(end.parse::<u16>().ok())
            .is_some_and(|(start, end)| start <= slot && slot <= end),
        None => field.parse::<u16>() == Ok(slot),
    }
}

pub(super) fn cluster_slot_retry_error(reason: &'static str) -> redis::RedisError {
    redis::RedisError::from((ErrorKind::Server(ServerErrorKind::TryAgain), reason))
}

pub(crate) async fn build_connector(config: &RedisConfig) -> Result<RedisConnector, SinkError> {
    build_connector_with_revision(config)
        .await
        .map(|(connector, _)| connector)
}

pub(super) async fn build_connector_with_revision(
    config: &RedisConfig,
) -> Result<(RedisConnector, RedisCredentialRevision), SinkError> {
    let credentials = resolve_credentials(config).await?;
    let revision = credentials.revision;
    let connector = build_connector_with_credentials(config, &credentials).await?;
    Ok((connector, revision))
}

pub(super) async fn credential_revision(
    config: &RedisConfig,
) -> Result<RedisCredentialRevision, SinkError> {
    resolve_credentials(config)
        .await
        .map(|credentials| credentials.revision)
}

async fn build_connector_with_credentials(
    config: &RedisConfig,
    credentials: &ResolvedCredentials,
) -> Result<RedisConnector, SinkError> {
    match &config.topology {
        RedisTopology::Standalone { endpoint } => {
            let info = data_connection_info(endpoint, credentials)?;
            Ok(RedisConnector::Standalone(
                build_client(info, &config.tls).await?,
            ))
        }
        RedisTopology::Sentinel(sentinel) => {
            let mut addresses = Vec::with_capacity(sentinel.endpoints.len());
            for endpoint in &sentinel.endpoints {
                let info = endpoint_connection_info(endpoint, None, None)?;
                addresses.push(info.addr().clone());
            }
            let mut builder = redis::sentinel::SentinelClientBuilder::new(
                addresses,
                &sentinel.service_name,
                redis::sentinel::SentinelServerType::Master,
            )
            .map_err(|error| {
                SinkError::Blocked(format!("invalid Redis Sentinel configuration: {error}"))
            })?;
            if sentinel.data_node_tls {
                builder = builder.set_client_to_redis_tls_mode(TlsMode::Secure);
            }
            if let Some(username) = &credentials.username {
                builder = builder.set_client_to_redis_username(username);
            }
            if let Some(password) = &credentials.password {
                builder = builder.set_client_to_redis_password(password);
            }
            if let Some(certificates) = load_tls_certificates(&config.tls).await? {
                builder = builder.set_client_to_redis_certificates(certificates);
            }
            if sentinel
                .endpoints
                .first()
                .is_some_and(|endpoint| endpoint.starts_with("rediss://"))
            {
                builder = builder.set_client_to_sentinel_tls_mode(TlsMode::Secure);
            }
            if let Some(username) = &credentials.sentinel_username {
                builder = builder.set_client_to_sentinel_username(username);
            }
            if let Some(password) = &credentials.sentinel_password {
                builder = builder.set_client_to_sentinel_password(password);
            }
            if let Some(certificates) = load_tls_certificates(&sentinel.tls).await? {
                builder = builder.set_client_to_sentinel_certificates(certificates);
            }
            let client = builder.build().map_err(|error| {
                SinkError::Blocked(format!("invalid Redis Sentinel configuration: {error}"))
            })?;
            Ok(RedisConnector::Sentinel(Mutex::new(client)))
        }
        RedisTopology::Cluster { endpoints } => {
            let nodes = endpoints
                .iter()
                .map(|endpoint| data_connection_info(endpoint, credentials))
                .collect::<Result<Vec<_>, _>>()?;
            let mut builder = redis::cluster::ClusterClientBuilder::new(nodes)
                .retries(16)
                .connection_timeout(config.connect_timeout)
                .response_timeout(config.response_timeout)
                .overall_response_timeout(Some(config.response_timeout));
            if let Some(certificates) = load_tls_certificates(&config.tls).await? {
                builder = builder.certs(certificates);
            }
            let client = builder.build().map_err(|error| {
                SinkError::Blocked(format!("invalid Redis Cluster configuration: {error}"))
            })?;
            Ok(RedisConnector::Cluster(client))
        }
    }
}

async fn resolve_credentials(config: &RedisConfig) -> Result<ResolvedCredentials, SinkError> {
    let username = resolve_credential(
        config.username.as_deref(),
        config.username_file.as_deref(),
        "ACL username",
    )
    .await?;
    let password = resolve_credential(
        config.password.as_deref(),
        config.password_file.as_deref(),
        "password",
    )
    .await?;
    let (sentinel_username, sentinel_password) = match &config.topology {
        RedisTopology::Sentinel(sentinel) => (
            resolve_credential(
                sentinel.username.as_deref(),
                sentinel.username_file.as_deref(),
                "Sentinel ACL username",
            )
            .await?,
            resolve_credential(
                sentinel.password.as_deref(),
                sentinel.password_file.as_deref(),
                "Sentinel password",
            )
            .await?,
        ),
        _ => (None, None),
    };
    let mut hash = Sha256::new();
    for value in [
        username.as_deref(),
        password.as_deref(),
        sentinel_username.as_deref(),
        sentinel_password.as_deref(),
    ] {
        match value {
            Some(value) => {
                hash.update([1]);
                hash.update(value.len().to_le_bytes());
                hash.update(value.as_bytes());
            }
            None => hash.update([0]),
        }
    }
    Ok(ResolvedCredentials {
        username,
        password,
        sentinel_username,
        sentinel_password,
        revision: RedisCredentialRevision(hash.finalize().into()),
    })
}

async fn resolve_credential(
    value: Option<&str>,
    file: Option<&std::path::Path>,
    label: &str,
) -> Result<Option<String>, SinkError> {
    const MAX_CREDENTIAL_BYTES: u64 = 1024 * 1024;

    let Some(path) = file else {
        return Ok(value.map(str::to_owned));
    };
    let metadata = tokio::fs::metadata(path).await.map_err(|error| {
        SinkError::Connection(format!(
            "unable to inspect Redis {label} file {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_CREDENTIAL_BYTES {
        return Err(SinkError::Blocked(format!(
            "Redis {label} file {} must be a regular file of 1 to {MAX_CREDENTIAL_BYTES} bytes",
            path.display()
        )));
    }
    let value = tokio::fs::read_to_string(path).await.map_err(|error| {
        SinkError::Connection(format!(
            "unable to read Redis {label} file {}: {error}",
            path.display()
        ))
    })?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_owned();
    if value.is_empty() {
        return Err(SinkError::Blocked(format!(
            "Redis {label} file {} must not be empty",
            path.display()
        )));
    }
    Ok(Some(value))
}

pub(crate) async fn connect_raw(
    connector: &RedisConnector,
    config: &RedisConfig,
) -> Result<RedisConnection, SinkError> {
    let topology = topology_label(&config.topology);
    ventstream_telemetry::bump_redis_connection_attempt(topology);
    let result = connect_raw_inner(connector, config).await;
    ventstream_telemetry::record_redis_connection_result(topology, result.is_ok());
    result
}

async fn connect_raw_inner(
    connector: &RedisConnector,
    config: &RedisConfig,
) -> Result<RedisConnection, SinkError> {
    let mut connection = match connector {
        RedisConnector::Standalone(client) => {
            let manager_config = ConnectionManagerConfig::new()
                .set_connection_timeout(Some(config.connect_timeout))
                .set_response_timeout(Some(config.response_timeout))
                .set_number_of_retries(0);
            RedisConnection::Standalone(
                client
                    .get_connection_manager_with_config(manager_config)
                    .await
                    .map_err(|error| map_connect_error(&error, config))?,
            )
        }
        RedisConnector::Sentinel(client) => {
            let connection_config = AsyncConnectionConfig::new()
                .set_connection_timeout(Some(config.connect_timeout))
                .set_response_timeout(Some(config.response_timeout));
            RedisConnection::Sentinel(
                client
                    .lock()
                    .await
                    .get_async_connection_with_config(&connection_config)
                    .await
                    .map_err(|error| map_connect_error(&error, config))?,
            )
        }
        RedisConnector::Cluster(client) => RedisConnection::Cluster(
            client
                .get_async_connection()
                .await
                .map_err(|error| map_connect_error(&error, config))?,
        ),
    };
    redis::cmd("PING")
        .query_async::<String>(&mut connection)
        .await
        .map_err(|error| map_connect_error(&error, config))?;
    Ok(connection)
}

pub(super) const fn topology_label(topology: &RedisTopology) -> &'static str {
    match topology {
        RedisTopology::Standalone { .. } => "standalone",
        RedisTopology::Sentinel(_) => "sentinel",
        RedisTopology::Cluster { .. } => "cluster",
    }
}

async fn build_client(
    connection_info: redis::ConnectionInfo,
    tls: &RedisTlsConfig,
) -> Result<redis::Client, SinkError> {
    let Some(certificates) = load_tls_certificates(tls).await? else {
        return redis::Client::open(connection_info)
            .map_err(|err| SinkError::Blocked(format!("invalid Redis connection: {err}")));
    };
    redis::Client::build_with_tls(connection_info, certificates)
        .map_err(|err| SinkError::Blocked(format!("invalid Redis TLS configuration: {err}")))
}

async fn load_tls_certificates(tls: &RedisTlsConfig) -> Result<Option<TlsCertificates>, SinkError> {
    if tls.ca_file.is_none() && tls.client_cert_file.is_none() {
        return Ok(None);
    }
    let root_cert = match &tls.ca_file {
        Some(path) => Some(read_tls_file(path, "CA bundle").await?),
        None => None,
    };
    let client_tls = match (&tls.client_cert_file, &tls.client_key_file) {
        (Some(cert_path), Some(key_path)) => Some(ClientTlsConfig {
            client_cert: read_tls_file(cert_path, "client certificate").await?,
            client_key: read_tls_file(key_path, "client key").await?,
        }),
        (None, None) => None,
        _ => {
            return Err(SinkError::Blocked(
                "Redis mutual TLS requires both client_cert_file and client_key_file".to_owned(),
            ))
        }
    };
    Ok(Some(TlsCertificates {
        client_tls,
        root_cert,
    }))
}

async fn read_tls_file(path: &std::path::Path, label: &str) -> Result<Vec<u8>, SinkError> {
    const MAX_TLS_FILE_BYTES: u64 = 4 * 1024 * 1024;

    let metadata = tokio::fs::metadata(path).await.map_err(|err| {
        SinkError::Blocked(format!(
            "unable to read Redis TLS {label} {}: {err}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(SinkError::Blocked(format!(
            "Redis TLS {label} {} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() == 0 || metadata.len() > MAX_TLS_FILE_BYTES {
        return Err(SinkError::Blocked(format!(
            "Redis TLS {label} {} must be 1 to {MAX_TLS_FILE_BYTES} bytes",
            path.display()
        )));
    }
    tokio::fs::read(path).await.map_err(|err| {
        SinkError::Blocked(format!(
            "unable to read Redis TLS {label} {}: {err}",
            path.display()
        ))
    })
}

fn data_connection_info(
    endpoint: &str,
    credentials: &ResolvedCredentials,
) -> Result<redis::ConnectionInfo, SinkError> {
    endpoint_connection_info(
        endpoint,
        credentials.username.as_deref(),
        credentials.password.as_deref(),
    )
}

fn endpoint_connection_info(
    endpoint: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<redis::ConnectionInfo, SinkError> {
    let mut info = endpoint
        .into_connection_info()
        .map_err(|error| SinkError::Blocked(format!("invalid Redis endpoint: {error}")))?;
    if matches!(info.addr(), ConnectionAddr::TcpTls { insecure: true, .. }) {
        return Err(SinkError::Blocked(
            "Redis endpoints must verify the TLS certificate and hostname; #insecure is not accepted"
                .to_owned(),
        ));
    }
    let mut settings = info.redis_settings().clone();
    if let Some(username) = username {
        settings = settings.set_username(username);
    }
    if let Some(password) = password {
        settings = settings.set_password(password);
    }
    settings = settings.set_lib_name("ventstream", env!("CARGO_PKG_VERSION"));
    info = info.set_redis_settings(settings);
    Ok(info)
}
