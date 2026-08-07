use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use redis::IntoConnectionInfo;
use serde::Serialize;
use serde_json::Value;
use ventstream_core::SinkHealth;

use crate::RetryConfig;

pub(super) const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_KEY_BYTES: usize = 1024 * 1024;
pub(super) const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
pub(super) const MAX_RESPONSE_TIMEOUT: Duration = Duration::from_secs(600);
pub(super) const MIN_WRITER_LEASE: Duration = Duration::from_secs(3);
pub(super) const MAX_WRITER_LEASE: Duration = Duration::from_secs(600);

/// Redis value representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisDocumentFormat {
    /// Store the compact event payload with `SET`.
    String,
    /// Store the event payload with RedisJSON `JSON.SET`.
    Json,
}

/// Redis key target selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisKeyRouting {
    /// Use `ventstream.cdc.relation`.
    ByOutputRelation,
    /// Use `ventstream.target.index`.
    ByProjectionTarget,
    /// Use one configured target.
    Fixed(String),
    /// Materialize one or more declarative lookup views.
    Views(Vec<RedisView>),
}

/// One declarative Redis lookup view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedisView {
    /// Stable target name used in the Redis key namespace.
    pub name: String,
    /// CDC output selected by this view.
    pub source: RedisViewSource,
    /// Deterministic key expression.
    pub key: RedisViewKey,
    /// Optional document filter.
    pub filter: Option<RedisViewFilter>,
    /// Stored value selection.
    pub value: RedisViewValue,
}

/// Exact source metadata selected by a Redis view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedisViewSource {
    /// Optional CDC namespace, such as a database or schema.
    pub namespace: Option<String>,
    /// Raw source relation or collection.
    pub relation: Option<String>,
    /// Denormalized projection target.
    pub projection_target: Option<String>,
}

/// View key template and missing-field behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedisViewKey {
    /// Template using `${doc_id}`, `${header:NAME}`, or `${json:/pointer}`.
    pub template: String,
    /// Behavior when a key placeholder cannot be resolved.
    pub on_missing: RedisViewMissingBehavior,
}

/// Behavior when a view key cannot be rendered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewMissingBehavior {
    /// Stop the pipeline because the configured data contract was not met.
    #[default]
    Block,
    /// Remove any previous entry for this view and omit the new entry.
    Skip,
}

/// Bounded filter over the JSON document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedisViewFilter {
    /// Whether every or any condition must match.
    pub mode: RedisViewFilterMode,
    /// Scalar JSON-pointer conditions.
    pub conditions: Vec<RedisViewCondition>,
}

/// Filter condition composition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewFilterMode {
    /// Every condition must match.
    #[default]
    All,
    /// At least one condition must match.
    Any,
}

/// One JSON-pointer filter condition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RedisViewCondition {
    /// RFC 6901 JSON pointer.
    pub path: String,
    /// Comparison applied to the selected value.
    pub operator: RedisViewConditionOperator,
}

/// Supported view filter comparisons.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewConditionOperator {
    /// Exact JSON scalar equality.
    Equals(Value),
    /// Exact JSON scalar inequality.
    NotEquals(Value),
    /// Membership in a bounded scalar list.
    In(Vec<Value>),
    /// Non-membership in a bounded scalar list.
    NotIn(Vec<Value>),
    /// The pointer resolves.
    Exists,
    /// The pointer does not resolve.
    NotExists,
}

/// Value emitted by a Redis view.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewValue {
    /// Store the complete source or projection document.
    #[default]
    Document,
    /// Store one JSON subtree.
    Pointer(String),
    /// Build a JSON object from named JSON pointers.
    Fields(BTreeMap<String, String>),
}

/// Whether this pipeline may perform target-wide keyspace operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RedisKeyspaceOwnership {
    /// Other writers may use the same prefix and targets.
    #[default]
    Shared,
    /// This pipeline exclusively owns every routed target under the prefix.
    Exclusive,
}

/// Operational contract for retained keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisContract {
    /// Retained materialized state. Keys do not expire.
    MaterializedView,
    /// Expiring cache state. Every upsert receives the configured TTL.
    Cache {
        /// Expiration applied atomically with every upsert.
        ttl: Duration,
    },
}

/// Downstream acknowledgement required before source progress advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisAcknowledgement {
    /// The Redis primary applied the pipeline.
    Primary,
    /// At least `replicas` replicas acknowledged the writes.
    Replicated {
        /// Required replica acknowledgements.
        replicas: usize,
        /// Maximum wait per pipeline.
        timeout: Duration,
    },
    /// Required AOF fsync acknowledgements via Redis 7.2+ `WAITAOF`.
    Aof {
        /// Require the writable primary to fsync its local AOF.
        local: bool,
        /// Required replica AOF fsync acknowledgements.
        replicas: usize,
        /// Maximum wait per pipeline.
        timeout: Duration,
    },
}

/// Optional trust and client identity files for `rediss://` endpoints.
#[derive(Debug, Clone, Default)]
pub struct RedisTlsConfig {
    /// PEM CA bundle used instead of system roots.
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate chain for mutual TLS.
    pub client_cert_file: Option<PathBuf>,
    /// PEM private key for mutual TLS.
    pub client_key_file: Option<PathBuf>,
}

/// Redis deployment topology.
#[derive(Clone)]
pub enum RedisTopology {
    /// One Redis endpoint, including managed services that expose one stable endpoint.
    Standalone {
        /// Stable Redis endpoint.
        endpoint: String,
    },
    /// Sentinel-discovered writable primary.
    Sentinel(RedisSentinelTopology),
    /// Redis Cluster with client-side slot routing.
    Cluster {
        /// Initial Cluster nodes used to discover the complete slot map.
        endpoints: Vec<String>,
    },
}

impl fmt::Debug for RedisTopology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standalone { endpoint } => formatter
                .debug_struct("Standalone")
                .field("endpoint", &redact_endpoint_userinfo(endpoint))
                .finish(),
            Self::Sentinel(sentinel) => sentinel.fmt(formatter),
            Self::Cluster { endpoints } => formatter
                .debug_struct("Cluster")
                .field(
                    "endpoints",
                    &endpoints
                        .iter()
                        .map(|endpoint| redact_endpoint_userinfo(endpoint))
                        .collect::<Vec<_>>(),
                )
                .finish(),
        }
    }
}

/// Sentinel discovery and authentication settings.
#[derive(Clone)]
pub struct RedisSentinelTopology {
    /// Sentinel endpoints tried in order.
    pub endpoints: Vec<String>,
    /// Sentinel master service name.
    pub service_name: String,
    /// Whether discovered Redis data nodes require TLS.
    pub data_node_tls: bool,
    /// Optional Sentinel ACL username.
    pub username: Option<String>,
    /// Optional Sentinel password.
    pub password: Option<String>,
    /// Mounted Sentinel ACL username file, reloaded before reconnecting.
    pub username_file: Option<PathBuf>,
    /// Mounted Sentinel password file, reloaded before reconnecting.
    pub password_file: Option<PathBuf>,
    /// Sentinel TLS trust and client identity files.
    pub tls: RedisTlsConfig,
}

impl fmt::Debug for RedisSentinelTopology {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sentinel")
            .field(
                "endpoints",
                &self
                    .endpoints
                    .iter()
                    .map(|endpoint| redact_endpoint_userinfo(endpoint))
                    .collect::<Vec<_>>(),
            )
            .field("service_name", &self.service_name)
            .field("data_node_tls", &self.data_node_tls)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("username_file", &self.username_file)
            .field("password_file", &self.password_file)
            .field("tls", &self.tls)
            .finish()
    }
}

/// Redis sink configuration.
#[derive(Clone)]
pub struct RedisConfig {
    /// Sink identifier.
    pub id: String,
    /// Redis endpoint discovery and routing mode.
    pub topology: RedisTopology,
    /// Optional Redis ACL username.
    pub username: Option<String>,
    /// Optional Redis password.
    pub password: Option<String>,
    /// Mounted Redis ACL username file, reloaded before reconnecting.
    pub username_file: Option<PathBuf>,
    /// Mounted Redis password file, reloaded before reconnecting.
    pub password_file: Option<PathBuf>,
    /// TLS trust and client identity files.
    pub tls: RedisTlsConfig,
    /// Namespace prepended to every key.
    pub key_prefix: String,
    /// Target routing policy.
    pub key_routing: RedisKeyRouting,
    /// Permission for target-wide keyspace operations.
    pub keyspace_ownership: RedisKeyspaceOwnership,
    /// Stored value representation.
    pub document_format: RedisDocumentFormat,
    /// Retention contract.
    pub contract: RedisContract,
    /// Required downstream acknowledgement.
    pub acknowledgement: RedisAcknowledgement,
    /// Stable deployment identity recorded in target writer leases.
    pub writer_id: String,
    /// Duration for which a healthy writer owns an idle target between heartbeats.
    pub writer_lease: Duration,
    /// Previous writer identity permitted to be replaced during a controlled handoff.
    pub writer_takeover_from: Option<String>,
    /// Maximum encoded command bytes in one dispatcher batch.
    pub max_batch_bytes: usize,
    /// Maximum encoded Redis key bytes accepted from one event.
    pub max_key_bytes: usize,
    /// Maximum value size accepted from one event.
    pub max_value_bytes: usize,
    /// Connection establishment timeout.
    pub connect_timeout: Duration,
    /// Command response timeout.
    pub response_timeout: Duration,
    /// Transient failure retry policy.
    pub retry: RetryConfig,
    /// Shared readiness state.
    #[doc(hidden)]
    pub delivery_health: Option<SinkHealth>,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisConfig")
            .field("id", &self.id)
            .field("topology", &self.topology)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("username_file", &self.username_file)
            .field("password_file", &self.password_file)
            .field("tls", &self.tls)
            .field("key_prefix", &self.key_prefix)
            .field("key_routing", &self.key_routing)
            .field("keyspace_ownership", &self.keyspace_ownership)
            .field("document_format", &self.document_format)
            .field("contract", &self.contract)
            .field("acknowledgement", &self.acknowledgement)
            .field("writer_id", &self.writer_id)
            .field("writer_lease", &self.writer_lease)
            .field("writer_takeover_from", &self.writer_takeover_from)
            .field("max_batch_bytes", &self.max_batch_bytes)
            .field("max_key_bytes", &self.max_key_bytes)
            .field("max_value_bytes", &self.max_value_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("retry", &self.retry)
            .field("delivery_health", &self.delivery_health)
            .finish()
    }
}

impl RedisConfig {
    /// Construct a materialized-view sink with production defaults.
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        key_prefix: impl Into<String>,
        key_routing: RedisKeyRouting,
    ) -> Self {
        let id = id.into();
        Self {
            writer_id: id.clone(),
            id,
            topology: RedisTopology::Standalone {
                endpoint: endpoint.into(),
            },
            username: None,
            password: None,
            username_file: None,
            password_file: None,
            tls: RedisTlsConfig::default(),
            key_prefix: key_prefix.into(),
            key_routing,
            keyspace_ownership: RedisKeyspaceOwnership::Shared,
            document_format: RedisDocumentFormat::String,
            contract: RedisContract::MaterializedView,
            acknowledgement: RedisAcknowledgement::Primary,
            writer_lease: Duration::from_secs(30),
            writer_takeover_from: None,
            max_batch_bytes: 16 * 1024 * 1024,
            max_key_bytes: 16 * 1024,
            max_value_bytes: 8 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
            delivery_health: None,
        }
    }

    /// Select Sentinel or Cluster discovery instead of the standalone endpoint.
    #[must_use]
    pub fn with_topology(mut self, topology: RedisTopology) -> Self {
        self.topology = topology;
        self
    }

    /// Return the standalone endpoint or first discovery seed for diagnostics.
    pub fn discovery_endpoint(&self) -> Option<&str> {
        match &self.topology {
            RedisTopology::Standalone { endpoint } => Some(endpoint),
            RedisTopology::Sentinel(sentinel) => sentinel.endpoints.first().map(String::as_str),
            RedisTopology::Cluster { endpoints } => endpoints.first().map(String::as_str),
        }
    }

    pub(super) fn has_rotating_credentials(&self) -> bool {
        self.username_file.is_some()
            || self.password_file.is_some()
            || matches!(
                &self.topology,
                RedisTopology::Sentinel(sentinel)
                    if sentinel.username_file.is_some() || sentinel.password_file.is_some()
            )
    }

    /// Set ACL credentials.
    #[must_use]
    pub fn with_auth(mut self, username: Option<String>, password: Option<String>) -> Self {
        self.username = username;
        self.password = password;
        self.username_file = None;
        self.password_file = None;
        self
    }

    /// Set mounted credential files that are reloaded without restarting the engine.
    #[must_use]
    pub fn with_auth_files(
        mut self,
        username_file: Option<PathBuf>,
        password_file: Option<PathBuf>,
    ) -> Self {
        self.username = None;
        self.password = None;
        self.username_file = username_file;
        self.password_file = password_file;
        self
    }

    /// Set static and mounted credential components.
    #[must_use]
    pub fn with_auth_sources(
        mut self,
        username: Option<String>,
        password: Option<String>,
        username_file: Option<PathBuf>,
        password_file: Option<PathBuf>,
    ) -> Self {
        self.username = username;
        self.password = password;
        self.username_file = username_file;
        self.password_file = password_file;
        self
    }

    /// Set custom TLS trust or mutual-TLS identity files.
    #[must_use]
    pub fn with_tls(mut self, tls: RedisTlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set the stored value representation.
    #[must_use]
    pub const fn with_document_format(mut self, format: RedisDocumentFormat) -> Self {
        self.document_format = format;
        self
    }

    /// Set whether the pipeline exclusively owns its routed targets.
    #[must_use]
    pub const fn with_keyspace_ownership(mut self, ownership: RedisKeyspaceOwnership) -> Self {
        self.keyspace_ownership = ownership;
        self
    }

    /// Set the retention contract.
    #[must_use]
    pub const fn with_contract(mut self, contract: RedisContract) -> Self {
        self.contract = contract;
        self
    }

    /// Set the downstream acknowledgement.
    #[must_use]
    pub const fn with_acknowledgement(mut self, acknowledgement: RedisAcknowledgement) -> Self {
        self.acknowledgement = acknowledgement;
        self
    }

    /// Set the deployment identity used for Redis writer ownership.
    #[must_use]
    pub fn with_writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = writer_id.into();
        self
    }

    /// Set the renewable writer lease duration.
    #[must_use]
    pub const fn with_writer_lease(mut self, lease: Duration) -> Self {
        self.writer_lease = lease;
        self
    }

    /// Allow a controlled handoff from the named previous writer.
    #[must_use]
    pub fn with_writer_takeover_from(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_takeover_from = Some(writer_id.into());
        self
    }

    /// Attach the process readiness state.
    #[must_use]
    pub fn with_delivery_health(mut self, health: SinkHealth) -> Self {
        self.delivery_health = Some(health);
        self
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty()
            || self.id.trim() != self.id
            || self.id.len() > 256
            || self.id.chars().any(char::is_control)
        {
            return Err(
                "Redis sink id must be trimmed, 1 to 256 bytes, and contain no control characters"
                    .to_owned(),
            );
        }
        validate_topology(&self.topology, &self.tls, &self.key_routing)?;
        if self.username.is_some() && self.username_file.is_some()
            || self.password.is_some() && self.password_file.is_some()
        {
            return Err(
                "Redis credentials must use either inline values or mounted files, not both"
                    .to_owned(),
            );
        }
        if matches!(
            self.username.as_deref(),
            Some(username)
                if username.is_empty()
                    || username.len() > 1024
                    || username.chars().any(char::is_control)
        ) {
            return Err(
                "Redis ACL username must be 1 to 1024 bytes and contain no control characters"
                    .to_owned(),
            );
        }
        if (self.username.is_some() || self.username_file.is_some())
            && self.password.is_none()
            && self.password_file.is_none()
        {
            return Err("Redis ACL username requires a password".to_owned());
        }
        if matches!(self.password.as_deref(), Some(password) if password.is_empty()) {
            return Err("Redis password must not be empty".to_owned());
        }
        if matches!(self.password.as_deref(), Some(password) if password.len() > 1024 * 1024) {
            return Err("Redis password must not exceed 1048576 bytes".to_owned());
        }
        validate_credential_path("Redis ACL username", self.username_file.as_ref())?;
        validate_credential_path("Redis password", self.password_file.as_ref())?;
        let uses_tls = topology_uses_data_node_tls(&self.topology);
        let has_tls_files = self.tls.ca_file.is_some()
            || self.tls.client_cert_file.is_some()
            || self.tls.client_key_file.is_some();
        if has_tls_files && !uses_tls {
            return Err("Redis TLS files require a rediss:// endpoint".to_owned());
        }
        if self.tls.client_cert_file.is_some() != self.tls.client_key_file.is_some() {
            return Err(
                "Redis mutual TLS requires both client_cert_file and client_key_file".to_owned(),
            );
        }
        if self.key_prefix.is_empty()
            || self.key_prefix.len() > 256
            || !self.key_prefix.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-' | b'.')
            })
        {
            return Err(
                "Redis key prefix must be 1 to 256 ASCII letters, digits, ':', '_', '-', or '.'"
                    .to_owned(),
            );
        }
        validate_writer_identity("Redis writer id", &self.writer_id)?;
        if let Some(previous) = &self.writer_takeover_from {
            validate_writer_identity("Redis previous writer id", previous)?;
            if previous == &self.writer_id {
                return Err(
                    "Redis writer takeover identity must differ from the current writer id"
                        .to_owned(),
                );
            }
        }
        if self.writer_lease < MIN_WRITER_LEASE || self.writer_lease > MAX_WRITER_LEASE {
            return Err(format!(
                "Redis writer lease must be {} to {} milliseconds",
                MIN_WRITER_LEASE.as_millis(),
                MAX_WRITER_LEASE.as_millis()
            ));
        }
        match &self.key_routing {
            RedisKeyRouting::Fixed(target)
                if target.trim().is_empty()
                    || target.trim() != target
                    || target.len() > 1024
                    || target.chars().any(char::is_control) =>
            {
                return Err(
                    "Redis fixed key target must be trimmed, 1 to 1024 bytes, and contain no control characters"
                        .to_owned(),
                );
            }
            RedisKeyRouting::Views(views) => super::view::validate_views(views)?,
            _ => {}
        }
        if matches!(self.key_routing, RedisKeyRouting::Views(_))
            && self.keyspace_ownership != RedisKeyspaceOwnership::Exclusive
        {
            return Err("Redis views routing requires keyspace ownership=exclusive".to_owned());
        }
        if self.max_batch_bytes == 0 || self.max_key_bytes == 0 || self.max_value_bytes == 0 {
            return Err("Redis byte limits must be positive".to_owned());
        }
        if self.max_batch_bytes > MAX_BATCH_BYTES {
            return Err(format!(
                "Redis max_batch_bytes must not exceed {MAX_BATCH_BYTES}"
            ));
        }
        if self.max_key_bytes > MAX_KEY_BYTES {
            return Err(format!(
                "Redis max_key_bytes must not exceed {MAX_KEY_BYTES}"
            ));
        }
        if self.max_value_bytes > MAX_VALUE_BYTES {
            return Err(format!(
                "Redis max_value_bytes must not exceed {MAX_VALUE_BYTES}"
            ));
        }
        if self.connect_timeout.is_zero() || self.response_timeout.is_zero() {
            return Err("Redis timeouts must be positive".to_owned());
        }
        if self.connect_timeout > MAX_CONNECT_TIMEOUT {
            return Err(format!(
                "Redis connect_timeout must not exceed {} milliseconds",
                MAX_CONNECT_TIMEOUT.as_millis()
            ));
        }
        if self.response_timeout > MAX_RESPONSE_TIMEOUT {
            return Err(format!(
                "Redis response_timeout must not exceed {} milliseconds",
                MAX_RESPONSE_TIMEOUT.as_millis()
            ));
        }
        match self.acknowledgement {
            RedisAcknowledgement::Primary => {}
            RedisAcknowledgement::Replicated { replicas: 0, .. } => {
                return Err("Redis replica acknowledgement count must be positive".to_owned());
            }
            RedisAcknowledgement::Aof {
                local: false,
                replicas: 0,
                ..
            } => {
                return Err(
                    "Redis AOF acknowledgement must require a local or replica fsync".to_owned(),
                );
            }
            RedisAcknowledgement::Replicated { .. } | RedisAcknowledgement::Aof { .. } => {}
        }
        if matches!(
            self.acknowledgement,
            RedisAcknowledgement::Replicated { replicas, .. }
                | RedisAcknowledgement::Aof { replicas, .. }
                if replicas > usize::try_from(i64::MAX).unwrap_or(usize::MAX)
        ) {
            return Err(
                "Redis replica acknowledgement count exceeds the Redis integer range".to_owned(),
            );
        }
        if matches!(
            self.acknowledgement,
            RedisAcknowledgement::Replicated { timeout, .. }
                | RedisAcknowledgement::Aof { timeout, .. }
                if timeout < Duration::from_millis(1)
                    || timeout.as_millis() > i64::MAX as u128
        ) {
            return Err(
                "Redis replica acknowledgement timeout must be 1 to 9223372036854775807 milliseconds"
                    .to_owned(),
            );
        }
        if matches!(
            self.acknowledgement,
            RedisAcknowledgement::Replicated { timeout, .. }
                | RedisAcknowledgement::Aof { timeout, .. }
                if timeout > self.response_timeout
        ) {
            return Err(
                "Redis acknowledgement timeout must not exceed response_timeout".to_owned(),
            );
        }
        if matches!(
            self.contract,
            RedisContract::Cache { ttl }
                if ttl < Duration::from_millis(1) || ttl.as_millis() > i64::MAX as u128
        ) {
            return Err("Redis cache TTL must be 1 to 9223372036854775807 milliseconds".to_owned());
        }
        Ok(())
    }
}

fn validate_writer_identity(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > 256
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-' | b'.' | b'/')
        })
    {
        return Err(format!(
            "{label} must be 1 to 256 ASCII letters, digits, ':', '_', '-', '.', or '/'"
        ));
    }
    Ok(())
}

fn validate_credential_path(label: &str, path: Option<&PathBuf>) -> Result<(), String> {
    if path.is_some_and(|path| path.as_os_str().is_empty()) {
        return Err(format!("{label} file path must not be empty"));
    }
    Ok(())
}

fn validate_topology(
    topology: &RedisTopology,
    data_tls: &RedisTlsConfig,
    key_routing: &RedisKeyRouting,
) -> Result<(), String> {
    match topology {
        RedisTopology::Standalone { endpoint } => {
            validate_endpoints(std::slice::from_ref(endpoint), "Redis endpoint")?;
        }
        RedisTopology::Sentinel(sentinel) => {
            validate_endpoints(&sentinel.endpoints, "Redis Sentinel endpoint")?;
            validate_database_zero(&sentinel.endpoints, "Redis Sentinel endpoint")?;
            if sentinel.username.is_some() && sentinel.username_file.is_some()
                || sentinel.password.is_some() && sentinel.password_file.is_some()
            {
                return Err(
                    "Redis Sentinel credentials must use either inline values or mounted files, not both"
                        .to_owned(),
                );
            }
            if sentinel.username_file.is_none() && sentinel.password_file.is_none() {
                validate_identity(
                    sentinel.username.as_deref(),
                    sentinel.password.as_deref(),
                    "Redis Sentinel",
                )?;
            } else if (sentinel.username.is_some() || sentinel.username_file.is_some())
                && sentinel.password.is_none()
                && sentinel.password_file.is_none()
            {
                return Err("Redis Sentinel ACL username requires a password".to_owned());
            }
            validate_credential_path(
                "Redis Sentinel ACL username",
                sentinel.username_file.as_ref(),
            )?;
            validate_credential_path("Redis Sentinel password", sentinel.password_file.as_ref())?;
            validate_tls_files(&sentinel.tls, "Redis Sentinel")?;
            let sentinel_uses_tls = endpoints_use_tls(&sentinel.endpoints)?;
            if has_tls_files(&sentinel.tls) && !sentinel_uses_tls {
                return Err(
                    "Redis Sentinel TLS files require rediss:// Sentinel endpoints".to_owned(),
                );
            }
            if sentinel.service_name.is_empty()
                || sentinel.service_name.trim() != sentinel.service_name
                || sentinel.service_name.len() > 256
                || sentinel.service_name.chars().any(char::is_control)
            {
                return Err(
                    "Redis Sentinel service name must be trimmed, 1 to 256 bytes, and contain no control characters"
                        .to_owned(),
                );
            }
            if has_tls_files(data_tls) && !sentinel.data_node_tls {
                return Err(
                    "Redis data-node TLS files require Sentinel data_node_tls=true".to_owned(),
                );
            }
        }
        RedisTopology::Cluster { endpoints } => {
            validate_endpoints(endpoints, "Redis Cluster endpoint")?;
            validate_database_zero(endpoints, "Redis Cluster endpoint")?;
            if matches!(key_routing, RedisKeyRouting::Views(views) if views.len() > 1) {
                return Err(
                    "Redis Cluster supports at most one declarative view because cross-slot view fanout cannot be atomic"
                        .to_owned(),
                );
            }
            if has_tls_files(data_tls) && !endpoints_use_tls(endpoints)? {
                return Err("Redis TLS files require rediss:// Redis Cluster endpoints".to_owned());
            }
        }
    }
    validate_tls_files(data_tls, "Redis")
}

fn validate_endpoints(endpoints: &[String], label: &str) -> Result<(), String> {
    const MAX_TOPOLOGY_ENDPOINTS: usize = 32;
    if endpoints.is_empty() || endpoints.len() > MAX_TOPOLOGY_ENDPOINTS {
        return Err(format!(
            "{label} list must contain 1 to {MAX_TOPOLOGY_ENDPOINTS} entries"
        ));
    }
    let mut unique = std::collections::BTreeSet::new();
    for endpoint in endpoints {
        if endpoint.len() > 8 * 1024 || endpoint.chars().any(char::is_control) {
            return Err(format!(
                "{label} must be at most 8192 bytes and contain no control characters"
            ));
        }
        if !matches!(
            endpoint.split_once("://").map(|(scheme, _)| scheme),
            Some("redis" | "rediss")
        ) {
            return Err(format!("{label} must use redis:// or rediss://"));
        }
        if endpoint
            .split_once("://")
            .and_then(|(_, rest)| rest.split('/').next())
            .is_some_and(|authority| authority.contains('@'))
        {
            return Err(format!(
                "{label} must not contain credentials; configure Redis auth separately"
            ));
        }
        if !unique.insert(endpoint) {
            return Err(format!("{label} list must not contain duplicate entries"));
        }
        endpoint
            .as_str()
            .into_connection_info()
            .map_err(|error| format!("{label} is invalid: {error}"))?;
    }
    endpoints_use_tls(endpoints)?;
    Ok(())
}

fn validate_database_zero(endpoints: &[String], label: &str) -> Result<(), String> {
    for endpoint in endpoints {
        let info = endpoint
            .as_str()
            .into_connection_info()
            .map_err(|error| format!("{label} is invalid: {error}"))?;
        if info.redis_settings().db() != 0 {
            return Err(format!("{label} must use Redis database 0"));
        }
    }
    Ok(())
}

fn endpoints_use_tls(endpoints: &[String]) -> Result<bool, String> {
    let first = endpoints
        .first()
        .is_some_and(|endpoint| endpoint.starts_with("rediss://"));
    if endpoints
        .iter()
        .any(|endpoint| endpoint.starts_with("rediss://") != first)
    {
        return Err("Redis topology endpoints must use one consistent transport scheme".to_owned());
    }
    Ok(first)
}

fn topology_uses_data_node_tls(topology: &RedisTopology) -> bool {
    match topology {
        RedisTopology::Standalone { endpoint } => endpoint.starts_with("rediss://"),
        RedisTopology::Sentinel(sentinel) => sentinel.data_node_tls,
        RedisTopology::Cluster { endpoints } => endpoints
            .first()
            .is_some_and(|endpoint| endpoint.starts_with("rediss://")),
    }
}

fn has_tls_files(tls: &RedisTlsConfig) -> bool {
    tls.ca_file.is_some() || tls.client_cert_file.is_some() || tls.client_key_file.is_some()
}

fn validate_tls_files(tls: &RedisTlsConfig, label: &str) -> Result<(), String> {
    if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
        return Err(format!(
            "{label} mutual TLS requires both client_cert_file and client_key_file"
        ));
    }
    Ok(())
}

fn validate_identity(
    username: Option<&str>,
    password: Option<&str>,
    label: &str,
) -> Result<(), String> {
    if matches!(username, Some(value) if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control))
    {
        return Err(format!(
            "{label} ACL username must be 1 to 1024 bytes and contain no control characters"
        ));
    }
    if username.is_some() && password.is_none() {
        return Err(format!("{label} ACL username requires a password"));
    }
    if matches!(password, Some(value) if value.is_empty()) {
        return Err(format!("{label} password must not be empty"));
    }
    if matches!(password, Some(value) if value.len() > 1024 * 1024) {
        return Err(format!("{label} password must not exceed 1048576 bytes"));
    }
    Ok(())
}

fn redact_endpoint_userinfo(endpoint: &str) -> String {
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return endpoint.to_owned();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, suffix) = rest.split_at(authority_end);
    let Some((_, host)) = authority.rsplit_once('@') else {
        return endpoint.to_owned();
    };
    format!("{scheme}://[REDACTED]@{host}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RedisConfig {
        RedisConfig::new(
            "redis-sink",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        )
    }

    #[test]
    fn authentication_requires_complete_nonempty_credentials() {
        assert!(config()
            .with_auth(Some("writer".to_owned()), None)
            .validate()
            .is_err());
        assert!(config()
            .with_auth(Some(String::new()), Some("secret".to_owned()))
            .validate()
            .is_err());
        assert!(config()
            .with_auth(None, Some(String::new()))
            .validate()
            .is_err());

        assert!(config()
            .with_auth(None, Some("password only".to_owned()))
            .validate()
            .is_ok());
        assert!(config()
            .with_auth(
                Some("writer".to_owned()),
                Some("arbitrary password !@#$%^&*()".to_owned()),
            )
            .validate()
            .is_ok());
    }

    #[test]
    fn endpoint_and_identity_fields_are_bounded() {
        let mut invalid_id = config();
        invalid_id.id = " redis ".to_owned();
        assert!(invalid_id.validate().is_err());

        let mut invalid_endpoint = config();
        invalid_endpoint.topology = RedisTopology::Standalone {
            endpoint: format!("redis://{}", "a".repeat(8 * 1024)),
        };
        assert!(invalid_endpoint.validate().is_err());
    }

    #[test]
    fn operator_limits_and_timeouts_are_bounded_and_consistent() {
        let mut oversized_batch = config();
        oversized_batch.max_batch_bytes = MAX_BATCH_BYTES + 1;
        assert!(oversized_batch.validate().is_err());

        let mut slow_connect = config();
        slow_connect.connect_timeout = MAX_CONNECT_TIMEOUT + Duration::from_millis(1);
        assert!(slow_connect.validate().is_err());

        let mut short_response = config().with_acknowledgement(RedisAcknowledgement::Replicated {
            replicas: 1,
            timeout: Duration::from_secs(2),
        });
        short_response.response_timeout = Duration::from_secs(1);
        assert!(short_response.validate().is_err());
    }

    #[test]
    fn writer_identity_lease_and_handoff_are_validated() {
        assert!(config().with_writer_id("revision|one").validate().is_err());
        assert!(config()
            .with_writer_lease(MIN_WRITER_LEASE - Duration::from_millis(1))
            .validate()
            .is_err());
        assert!(config()
            .with_writer_id("revision-a")
            .with_writer_takeover_from("revision-a")
            .validate()
            .is_err());
        assert!(config()
            .with_writer_id("revision-b")
            .with_writer_takeover_from("revision-a")
            .with_writer_lease(Duration::from_secs(15))
            .validate()
            .is_ok());
    }

    #[test]
    fn cluster_rejects_cross_slot_view_fanout() {
        let view = RedisView {
            name: "lookup".to_owned(),
            source: RedisViewSource {
                namespace: None,
                relation: Some("orders".to_owned()),
                projection_target: None,
            },
            key: RedisViewKey {
                template: "${doc_id}".to_owned(),
                on_missing: RedisViewMissingBehavior::Block,
            },
            filter: None,
            value: RedisViewValue::Document,
        };
        let mut clustered = config().with_topology(RedisTopology::Cluster {
            endpoints: vec!["redis://127.0.0.1:7000".to_owned()],
        });
        clustered.key_routing = RedisKeyRouting::Views(vec![
            view.clone(),
            RedisView {
                name: "lookup-two".to_owned(),
                ..view
            },
        ]);
        assert!(clustered.validate().is_err());
    }

    #[test]
    fn sentinel_and_cluster_require_database_zero() {
        let cluster = config().with_topology(RedisTopology::Cluster {
            endpoints: vec!["redis://127.0.0.1:7000/1".to_owned()],
        });
        assert!(cluster.validate().is_err());

        let sentinel = config().with_topology(RedisTopology::Sentinel(RedisSentinelTopology {
            endpoints: vec!["redis://127.0.0.1:26379/1".to_owned()],
            service_name: "mymaster".to_owned(),
            data_node_tls: false,
            username: None,
            password: None,
            username_file: None,
            password_file: None,
            tls: RedisTlsConfig::default(),
        }));
        assert!(sentinel.validate().is_err());
    }

    #[test]
    fn sentinel_requires_explicit_data_node_tls_for_data_certificates() {
        let mut sentinel = config().with_topology(RedisTopology::Sentinel(RedisSentinelTopology {
            endpoints: vec!["redis://127.0.0.1:26379".to_owned()],
            service_name: "mymaster".to_owned(),
            data_node_tls: false,
            username: None,
            password: None,
            username_file: None,
            password_file: None,
            tls: RedisTlsConfig::default(),
        }));
        sentinel.tls.ca_file = Some(PathBuf::from("ca.pem"));
        assert!(sentinel.validate().is_err());
    }
}
