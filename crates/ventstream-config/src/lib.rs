//! Typed `ventstream.yaml` schema and validation.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer};
use thiserror::Error;

/// Engine configuration schema version supported by this crate.
pub const SUPPORTED_SCHEMA_VERSION: u64 = 1;

/// TLS policy shared by database sources and HTTPS sinks.
///
/// The block is optional for backward compatibility. When present, strict
/// certificate and hostname verification is the default.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Transport policy. Omit to use strict verification.
    #[serde(default)]
    pub mode: TlsMode,
    /// Optional PEM CA bundle for private certificate authorities.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Optional maintained trust bundle for a known database provider.
    #[serde(default)]
    pub trust: Option<TlsTrustConfig>,
}

impl TlsConfig {
    fn validate(&self, field: &'static str) -> Result<(), ConfigError> {
        if self.mode == TlsMode::Disabled && (self.ca_file.is_some() || self.trust.is_some()) {
            return Err(ConfigError::InvalidField(match field {
                "source.postgres.tls" => {
                    "source.postgres.tls trust settings require mode=verify_full"
                }
                "source.mysql.tls" => "source.mysql.tls trust settings require mode=verify_full",
                "source.mongodb.tls" => {
                    "source.mongodb.tls trust settings require mode=verify_full"
                }
                "source.neo4j.tls" => "source.neo4j.tls trust settings require mode=verify_full",
                "sink.opensearch.tls" => {
                    "sink.opensearch.tls trust settings require mode=verify_full"
                }
                _ => "TLS trust settings require mode=verify_full",
            }));
        }
        if self.ca_file.is_some() && self.trust.is_some() {
            return Err(ConfigError::InvalidField(match field {
                "source.postgres.tls" => {
                    "source.postgres.tls.ca_file and source.postgres.tls.trust are mutually exclusive"
                }
                "source.mysql.tls" => {
                    "source.mysql.tls.ca_file and source.mysql.tls.trust are mutually exclusive"
                }
                "source.mongodb.tls" => {
                    "source.mongodb.tls.ca_file and source.mongodb.tls.trust are mutually exclusive"
                }
                "source.neo4j.tls" => {
                    "source.neo4j.tls.ca_file and source.neo4j.tls.trust are mutually exclusive"
                }
                "sink.opensearch.tls" => {
                    "sink.opensearch.tls.ca_file and sink.opensearch.tls.trust are mutually exclusive"
                }
                _ => "tls.ca_file and tls.trust are mutually exclusive",
            }));
        }
        if self.trust.is_some() && !matches!(field, "source.postgres.tls" | "source.mysql.tls") {
            return Err(ConfigError::InvalidField(match field {
                "source.mongodb.tls" => {
                    "source.mongodb.tls.trust.provider=aws_rds is not supported"
                }
                "source.neo4j.tls" => "source.neo4j.tls.trust.provider=aws_rds is not supported",
                "sink.opensearch.tls" => {
                    "sink.opensearch.tls.trust.provider=aws_rds is not supported"
                }
                _ => "tls.trust.provider=aws_rds is not supported by this connector",
            }));
        }
        Ok(())
    }
}

/// Maintained trust material for a known database provider.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsTrustConfig {
    /// Provider whose CA bundle VentStream should supply.
    pub provider: TlsTrustProvider,
}

/// Provider trust bundles packaged with the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsTrustProvider {
    /// Amazon RDS global CA bundle for PostgreSQL and MySQL.
    AwsRds,
    /// Supabase root CA.
    Supabase,
}

/// Supported database transport-security policies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Require TLS and verify both the certificate chain and server hostname.
    #[default]
    VerifyFull,
    /// Disable TLS. Intended only for isolated local development.
    Disabled,
}

/// Managed-mode attachment. An agent key switches the engine into managed
/// mode: pipeline configuration comes from the control plane, keyed by the
/// deployment the key identifies.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedConfig {
    /// Agent key reference, e.g. `env:VS_AGENT_KEY`. Reference form only —
    /// the file never carries the raw key.
    pub agent_key_ref: ValueRef,
    /// Control gateway URL override.
    #[serde(default)]
    pub gateway_url: Option<String>,
    /// Enrollment gateway URL override.
    #[serde(default)]
    pub enrollment_url: Option<String>,
    /// Directory holding the bound agent identity.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
}

/// Parsed canonical engine configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Schema version. V1 is the only supported version.
    pub schema_version: u64,
    /// Managed-mode attachment. Mutually exclusive with local pipeline
    /// sections: a managed engine's pipeline config comes from the
    /// control plane.
    #[serde(default)]
    pub managed: Option<ManagedConfig>,
    /// Roles enabled in this engine process.
    #[serde(default = "default_roles")]
    pub roles: Vec<Role>,
    /// Optional CDC source configuration.
    #[serde(default)]
    pub source: Option<SourceConfig>,
    /// Optional sink configuration.
    #[serde(default)]
    pub sink: Option<SinkConfig>,
    /// Optional paths to larger spec files.
    #[serde(default)]
    pub specs: SpecFiles,
    /// Optional runtime tuning.
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl EngineConfig {
    /// Parse and validate an engine configuration from YAML text.
    pub fn from_yaml_str(input: &str) -> Result<Self, ConfigError> {
        let config: Self = serde_yaml::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate semantic constraints not expressible through serde.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != SUPPORTED_SCHEMA_VERSION {
            return Err(ConfigError::UnsupportedSchemaVersion(self.schema_version));
        }
        if let Some(managed) = &self.managed {
            // A managed engine's pipeline config comes from the control
            // plane; local pipeline sections make ownership ambiguous.
            if self.source.is_some() {
                return Err(ConfigError::InvalidField(
                    "managed engines receive pipeline configuration from the \
                     control plane; remove the local source section",
                ));
            }
            if self.sink.is_some() {
                return Err(ConfigError::InvalidField(
                    "managed engines receive pipeline configuration from the \
                     control plane; remove the local sink section",
                ));
            }
            if self.specs.any_set() {
                return Err(ConfigError::InvalidField(
                    "managed engines receive pipeline configuration from the \
                     control plane; remove the local specs section",
                ));
            }
            managed.validate()?;
            // Roles and pipeline coupling come from the applied control-plane
            // configuration, not this file.
            return Ok(());
        }
        if self.roles.is_empty() {
            return Err(ConfigError::InvalidField("roles must not be empty"));
        }
        for (index, role) in self.roles.iter().enumerate() {
            if self
                .roles
                .iter()
                .take(index)
                .any(|candidate| candidate == role)
            {
                return Err(ConfigError::InvalidField(
                    "roles must not contain duplicates",
                ));
            }
        }
        if self.roles.contains(&Role::Cdc) && self.source.is_none() {
            return Err(ConfigError::InvalidField(
                "source is required when the cdc role is enabled",
            ));
        }
        if self.sink.is_none() && self.roles.contains(&Role::Cdc) {
            return Err(ConfigError::InvalidField(
                "sink is required when the cdc role is enabled",
            ));
        }
        if self.source.is_some() && !self.roles.contains(&Role::Cdc) {
            return Err(ConfigError::InvalidField("source requires the cdc role"));
        }
        if self.sink.is_some()
            && !self.roles.contains(&Role::Cdc)
            && !self.roles.contains(&Role::Mcp)
        {
            return Err(ConfigError::InvalidField(
                "sink requires the cdc or mcp role",
            ));
        }
        // The mcp role reads the sink another deployment writes; it must
        // never share a process with the singleton cdc writer, and the
        // gateways have an unrelated lifecycle — keep it a solo role.
        if self.roles.contains(&Role::Mcp) {
            if self.roles.len() > 1 {
                return Err(ConfigError::InvalidField("the mcp role must run alone"));
            }
            if self.sink.is_none() {
                return Err(ConfigError::InvalidField(
                    "sink is required when the mcp role is enabled",
                ));
            }
            if self.runtime.mcp.is_none() {
                return Err(ConfigError::InvalidField(
                    "runtime.mcp is required when the mcp role is enabled",
                ));
            }
        } else if self.runtime.mcp.is_some() {
            return Err(ConfigError::InvalidField(
                "runtime.mcp requires the mcp role",
            ));
        }
        if let Some(source) = &self.source {
            source.validate()?;
        }
        if let Some(sink) = &self.sink {
            sink.validate()?;
            let by_projection_target = sink.opensearch.as_ref().is_some_and(|opensearch| {
                matches!(
                    opensearch.index_routing,
                    OpenSearchIndexRouting::ByProjectionTarget
                )
            }) || sink.meilisearch.as_ref().is_some_and(|meilisearch| {
                matches!(
                    meilisearch.index_routing,
                    MeilisearchIndexRoutingConfig::ByProjectionTarget
                )
            }) || sink.redis.as_ref().is_some_and(|redis| {
                matches!(
                    redis.keyspace.routing,
                    RedisKeyRoutingConfig::ByProjectionTarget
                ) || matches!(
                    &redis.keyspace.routing,
                    RedisKeyRoutingConfig::Views { views }
                        if views
                            .iter()
                            .any(|view| view.source.projection_target.is_some())
                )
            });
            if by_projection_target {
                if self.specs.joins.is_none() {
                    return Err(ConfigError::InvalidField(
                        "by_projection_target routing requires specs.joins",
                    ));
                }
                // The read-only mcp role has no source; the writer-side
                // source constraint applies to the cdc role only.
                if self.roles.contains(&Role::Cdc)
                    && !self.source.as_ref().is_some_and(|source| {
                        matches!(source.kind, SourceKind::Postgres | SourceKind::Mysql)
                    })
                {
                    return Err(ConfigError::InvalidField(
                        "by_projection_target routing is supported only for postgres and mysql sources",
                    ));
                }
            }
        }
        self.specs.validate()?;
        self.runtime.validate()?;
        if self.roles.contains(&Role::Graphql)
            && self.runtime.realtime.provider == Some(RealtimeBrokerProvider::NatsCore)
        {
            return Err(ConfigError::InvalidField(
                "runtime.realtime.provider=nats_core cannot serve the graphql role",
            ));
        }
        Ok(())
    }
}

/// Engine process role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// CDC source-to-sink pipeline.
    Cdc,
    /// Native WebSocket fan-out gateway.
    Ws,
    /// GraphQL subscription gateway.
    Graphql,
    /// Read-only MCP server over sink-materialized state.
    Mcp,
}

/// CDC source selector plus source-specific settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Source connector kind.
    pub kind: SourceKind,
    /// PostgreSQL logical replication source settings.
    #[serde(default)]
    pub postgres: Option<PostgresSourceConfig>,
    /// Neo4j CDC source settings.
    #[serde(default)]
    pub neo4j: Option<Neo4jSourceConfig>,
    /// MongoDB CDC source settings.
    #[serde(default)]
    pub mongodb: Option<MongodbSourceConfig>,
    /// MySQL/MariaDB CDC source settings.
    #[serde(default)]
    pub mysql: Option<MysqlSourceConfig>,
    /// Kafka/Redpanda CDC source settings.
    #[serde(default)]
    pub kafka: Option<KafkaSourceConfig>,
}

impl SourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let selected = match self.kind {
            SourceKind::Postgres => self.postgres.is_some(),
            SourceKind::Neo4j => self.neo4j.is_some(),
            SourceKind::Mongo | SourceKind::Mongodb => self.mongodb.is_some(),
            SourceKind::Mysql => self.mysql.is_some(),
            SourceKind::Kafka | SourceKind::Redpanda => self.kafka.is_some(),
        };
        let configured_blocks = [
            self.postgres.is_some(),
            self.neo4j.is_some(),
            self.mongodb.is_some(),
            self.mysql.is_some(),
            self.kafka.is_some(),
        ]
        .into_iter()
        .filter(|configured| *configured)
        .count();
        if configured_blocks > usize::from(selected) {
            return Err(ConfigError::InvalidField(
                "source connector settings must match source.kind",
            ));
        }
        match self.kind {
            SourceKind::Postgres => {
                if let Some(postgres) = &self.postgres {
                    postgres.validate()?;
                }
            }
            SourceKind::Neo4j => {
                if let Some(neo4j) = &self.neo4j {
                    neo4j.validate()?;
                }
            }
            SourceKind::Mongo | SourceKind::Mongodb => {
                if let Some(mongodb) = &self.mongodb {
                    mongodb.validate()?;
                }
            }
            SourceKind::Mysql => {
                if let Some(mysql) = &self.mysql {
                    mysql.validate()?;
                }
            }
            SourceKind::Kafka | SourceKind::Redpanda => {
                if let Some(kafka) = &self.kafka {
                    kafka.validate()?;
                }
            }
        }
        Ok(())
    }
}

/// PostgreSQL logical replication source settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    /// Postgres server hostname or IP.
    #[serde(default)]
    pub host: Option<String>,
    /// Postgres server hostname or IP from an external value reference.
    #[serde(default)]
    pub host_ref: Option<ValueRef>,
    /// Postgres server port.
    #[serde(default)]
    pub port: Option<u16>,
    /// Login user.
    #[serde(default)]
    pub user: Option<String>,
    /// Login user from an external value reference.
    #[serde(default)]
    pub user_ref: Option<ValueRef>,
    /// Login password from an external value reference.
    #[serde(default)]
    pub password_ref: Option<ValueRef>,
    /// Database name.
    #[serde(default)]
    pub database: Option<String>,
    /// Database name from an external value reference.
    #[serde(default)]
    pub database_ref: Option<ValueRef>,
    /// Existing logical replication publication.
    #[serde(default)]
    pub publication: Option<String>,
    /// Existing logical replication publication from an external value reference.
    #[serde(default)]
    pub publication_ref: Option<ValueRef>,
    /// Existing logical replication slot.
    #[serde(default)]
    pub slot: Option<String>,
    /// Existing logical replication slot from an external value reference.
    #[serde(default)]
    pub slot_ref: Option<ValueRef>,
    /// Optional snapshot bootstrap settings.
    #[serde(default)]
    pub bootstrap: SourceBootstrapConfig,
    /// Projection execution mode.
    #[serde(default)]
    pub denormalize_mode: Option<SqlDenormalizeMode>,
    /// Allow sink reverse lookups for child deletes in SQL denormalize mode.
    #[serde(default)]
    pub sink_reverse_lookup: Option<bool>,
    /// Maximum pooled connections for in-memory related-row fetches.
    #[serde(default)]
    pub related_fetch_pool_size: Option<usize>,
    /// Writable directory for transaction records that exceed the in-memory buffer.
    #[serde(default)]
    pub transaction_spool_dir: Option<PathBuf>,
    /// TLS transport policy.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl PostgresSourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_value_or_ref(
            "source.postgres.host",
            self.host.as_ref(),
            self.host_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.postgres.user",
            self.user.as_ref(),
            self.user_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.postgres.database",
            self.database.as_ref(),
            self.database_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.postgres.publication",
            self.publication.as_ref(),
            self.publication_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.postgres.slot",
            self.slot.as_ref(),
            self.slot_ref.as_ref(),
        )?;
        if self.bootstrap.chunk_size == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.postgres.bootstrap.chunk_size must be positive",
            ));
        }
        if self.related_fetch_pool_size == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.postgres.related_fetch_pool_size must be positive",
            ));
        }
        if self
            .transaction_spool_dir
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::InvalidField(
                "source.postgres.transaction_spool_dir must not be empty",
            ));
        }
        if let Some(tls) = &self.tls {
            tls.validate("source.postgres.tls")?;
        }
        Ok(())
    }
}

/// Neo4j CDC source settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Neo4jSourceConfig {
    /// Bolt URI, e.g. `bolt://127.0.0.1:7687`.
    #[serde(default)]
    pub uri: Option<String>,
    /// Bolt URI from an external value reference.
    #[serde(default)]
    pub uri_ref: Option<ValueRef>,
    /// Login user.
    #[serde(default)]
    pub user: Option<String>,
    /// Login user from an external value reference.
    #[serde(default)]
    pub user_ref: Option<ValueRef>,
    /// Login password from an external value reference.
    #[serde(default)]
    pub password_ref: Option<ValueRef>,
    /// Neo4j database name.
    #[serde(default)]
    pub database: Option<String>,
    /// Neo4j database name from an external value reference.
    #[serde(default)]
    pub database_ref: Option<ValueRef>,
    /// Logical namespace stamped on CDC events.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Cursor state directory.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// Poll interval in milliseconds.
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
    /// Idle poll count before advancing the persisted cursor.
    #[serde(default)]
    pub idle_advance_after_polls: Option<u32>,
    /// Neo4j label to logical table mapping.
    #[serde(default)]
    pub label_tables: BTreeMap<String, String>,
    /// Neo4j relationship type to logical table mapping.
    #[serde(default)]
    pub reltype_tables: BTreeMap<String, String>,
    /// Label allowlist.
    #[serde(default)]
    pub label_filter: Vec<String>,
    /// Relationship type allowlist.
    #[serde(default)]
    pub reltype_filter: Vec<String>,
    /// Canonical label priority.
    #[serde(default)]
    pub label_priority: Vec<String>,
    /// Snapshot bootstrap settings.
    #[serde(default)]
    pub bootstrap: SourceBootstrapConfig,
    /// Recompose chunk size.
    #[serde(default)]
    pub recompose_chunk: Option<usize>,
    /// Recompose query concurrency.
    #[serde(default)]
    pub recompose_concurrency: Option<usize>,
    /// Enable projection-aware fan-out for denormalization specs.
    #[serde(default)]
    pub projection_fan_out: Option<bool>,
    /// Hot-endpoint detection threshold. `0` disables detection.
    #[serde(default)]
    pub hot_node_threshold: Option<usize>,
    /// Optional PEM trust bundle path.
    #[serde(default)]
    pub trust_cert_file: Option<PathBuf>,
    /// TLS transport policy. Prefer this over `trust_cert_file`.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl Neo4jSourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_value_or_ref("source.neo4j.uri", self.uri.as_ref(), self.uri_ref.as_ref())?;
        validate_value_or_ref(
            "source.neo4j.user",
            self.user.as_ref(),
            self.user_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.neo4j.database",
            self.database.as_ref(),
            self.database_ref.as_ref(),
        )?;
        if self.poll_interval_ms == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.neo4j.poll_interval_ms must be positive",
            ));
        }
        if self.idle_advance_after_polls == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.neo4j.idle_advance_after_polls must be positive",
            ));
        }
        if self.bootstrap.chunk_size == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.neo4j.bootstrap.chunk_size must be positive",
            ));
        }
        if self.recompose_chunk == Some(0) || self.recompose_concurrency == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.neo4j recompose values must be positive",
            ));
        }
        if let Some(tls) = &self.tls {
            tls.validate("source.neo4j.tls")?;
            if tls.ca_file.is_some() && self.trust_cert_file.is_some() {
                return Err(ConfigError::InvalidField(
                    "source.neo4j.tls.ca_file and source.neo4j.trust_cert_file are mutually exclusive",
                ));
            }
        }
        Ok(())
    }
}

/// MongoDB change-stream source settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongodbSourceConfig {
    /// MongoDB connection string from an external value reference.
    #[serde(default)]
    pub uri_ref: Option<ValueRef>,
    /// Database name.
    #[serde(default)]
    pub database: Option<String>,
    /// Database name from an external value reference.
    #[serde(default)]
    pub database_ref: Option<ValueRef>,
    /// Logical namespace stamped on CDC events.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Cursor state directory.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// Collection allowlist. Empty means every collection in the database.
    #[serde(default)]
    pub collections: Vec<String>,
    /// MongoDB full-document mode for update events.
    #[serde(default)]
    pub full_document: Option<MongodbFullDocumentMode>,
    /// Snapshot bootstrap settings.
    #[serde(default)]
    pub bootstrap: SourceBootstrapConfig,
    /// Resume-token flush cadence in milliseconds.
    #[serde(default)]
    pub token_flush_ms: Option<u64>,
    /// TLS transport policy.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl MongodbSourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_value_or_ref(
            "source.mongodb.database",
            self.database.as_ref(),
            self.database_ref.as_ref(),
        )?;
        if let Some(uri_ref) = &self.uri_ref {
            uri_ref.validate()?;
        }
        if self.bootstrap.chunk_size == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.mongodb.bootstrap.chunk_size must be positive",
            ));
        }
        if self.token_flush_ms == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.mongodb.token_flush_ms must be positive",
            ));
        }
        validate_nonempty_list("source.mongodb.collections", &self.collections)?;
        if let Some(tls) = &self.tls {
            tls.validate("source.mongodb.tls")?;
        }
        Ok(())
    }
}

/// MongoDB change stream `fullDocument` behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MongodbFullDocumentMode {
    /// Re-read and emit the post-image for update events.
    UpdateLookup,
    /// Emit only the change stream default delta.
    Default,
}

/// MySQL/MariaDB binlog source settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlSourceConfig {
    /// MySQL server hostname or IP.
    #[serde(default)]
    pub host: Option<String>,
    /// MySQL server hostname or IP from an external value reference.
    #[serde(default)]
    pub host_ref: Option<ValueRef>,
    /// MySQL server port.
    #[serde(default)]
    pub port: Option<u16>,
    /// Login user.
    #[serde(default)]
    pub user: Option<String>,
    /// Login user from an external value reference.
    #[serde(default)]
    pub user_ref: Option<ValueRef>,
    /// Login password from an external value reference.
    #[serde(default)]
    pub password_ref: Option<ValueRef>,
    /// Database name.
    #[serde(default)]
    pub database: Option<String>,
    /// Database name from an external value reference.
    #[serde(default)]
    pub database_ref: Option<ValueRef>,
    /// Logical namespace stamped on CDC events.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Binlog client id.
    #[serde(default)]
    pub server_id: Option<u32>,
    /// Cursor state directory.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// Table allowlist. Empty means every table in the database.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Snapshot bootstrap settings.
    #[serde(default)]
    pub bootstrap: SourceBootstrapConfig,
    /// Binlog position flush cadence in milliseconds.
    #[serde(default)]
    pub pos_flush_ms: Option<u64>,
    /// Projection execution mode.
    #[serde(default)]
    pub denormalize_mode: Option<SqlDenormalizeMode>,
    /// Allow sink reverse lookups for child deletes in SQL denormalize mode.
    #[serde(default)]
    pub sink_reverse_lookup: Option<bool>,
    /// Maximum primary keys in one SQL recomposition query.
    #[serde(default)]
    pub recompose_chunk: Option<usize>,
    /// Maximum concurrent SQL recomposition queries.
    #[serde(default)]
    pub recompose_concurrency: Option<usize>,
    /// TLS transport policy.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl MysqlSourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_value_or_ref(
            "source.mysql.host",
            self.host.as_ref(),
            self.host_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.mysql.user",
            self.user.as_ref(),
            self.user_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.mysql.database",
            self.database.as_ref(),
            self.database_ref.as_ref(),
        )?;
        if self.bootstrap.chunk_size == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.mysql.bootstrap.chunk_size must be positive",
            ));
        }
        if self.pos_flush_ms == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.mysql.pos_flush_ms must be positive",
            ));
        }
        if self.recompose_chunk == Some(0) || self.recompose_concurrency == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.mysql recompose values must be positive",
            ));
        }
        validate_nonempty_list("source.mysql.tables", &self.tables)?;
        if let Some(tls) = &self.tls {
            tls.validate("source.mysql.tls")?;
        }
        Ok(())
    }
}

/// SQL projection execution mode for relational sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlDenormalizeMode {
    /// Use the in-memory join engine.
    Memory,
    /// Use bounded SQL recomposition.
    Sql,
}

/// Kafka/Redpanda source settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceConfig {
    /// Bootstrap broker list.
    #[serde(default)]
    pub brokers: Option<String>,
    /// Bootstrap broker list from an external value reference.
    #[serde(default)]
    pub brokers_ref: Option<ValueRef>,
    /// Topic list or one regex topic entry prefixed with `^`.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Consumer group id.
    #[serde(default)]
    pub group_id: Option<String>,
    /// Consumer group id from an external value reference.
    #[serde(default)]
    pub group_id_ref: Option<ValueRef>,
    /// Logical namespace override.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Message value unwrap mode.
    #[serde(default)]
    pub unwrap: Option<KafkaUnwrapMode>,
    /// Kafka auto.offset.reset value.
    #[serde(default)]
    pub auto_offset_reset: Option<String>,
    /// Kafka security.protocol value.
    #[serde(default)]
    pub security_protocol: Option<String>,
    /// Kafka sasl.mechanism value.
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// SASL username from an external value reference.
    #[serde(default)]
    pub sasl_username_ref: Option<ValueRef>,
    /// SASL password from an external value reference.
    #[serde(default)]
    pub sasl_password_ref: Option<ValueRef>,
    /// TLS CA bundle path.
    #[serde(default)]
    pub ssl_ca_location: Option<PathBuf>,
    /// Raw mode key field fallback.
    #[serde(default)]
    pub raw_key_field: Option<String>,
    /// Offset commit cadence in milliseconds.
    #[serde(default)]
    pub commit_ms: Option<u64>,
}

impl KafkaSourceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_value_or_ref(
            "source.kafka.brokers",
            self.brokers.as_ref(),
            self.brokers_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.kafka.group_id",
            self.group_id.as_ref(),
            self.group_id_ref.as_ref(),
        )?;
        validate_value_or_ref(
            "source.kafka.sasl_username",
            self.sasl_username.as_ref(),
            self.sasl_username_ref.as_ref(),
        )?;
        if let Some(password_ref) = &self.sasl_password_ref {
            password_ref.validate()?;
        }
        if self.commit_ms == Some(0) {
            return Err(ConfigError::InvalidField(
                "source.kafka.commit_ms must be positive",
            ));
        }
        validate_nonempty_list("source.kafka.topics", &self.topics)?;
        Ok(())
    }
}

/// Kafka message unwrap mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaUnwrapMode {
    /// Debezium change envelope.
    Debezium,
    /// Raw document payload.
    Raw,
}

/// Source snapshot bootstrap mode and tuning.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBootstrapConfig {
    /// Bootstrap mode.
    #[serde(default)]
    pub mode: Option<BootstrapMode>,
    /// Source-specific chunk/page size.
    #[serde(default)]
    pub chunk_size: Option<usize>,
}

/// Source snapshot bootstrap mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapMode {
    /// Run a one-shot snapshot before tailing.
    Snapshot,
    /// Do not run a snapshot.
    None,
}

/// Supported CDC source connector kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// PostgreSQL logical replication source.
    Postgres,
    /// Neo4j CDC polling source.
    Neo4j,
    /// MongoDB change-stream source.
    Mongo,
    /// MongoDB change-stream source.
    Mongodb,
    /// MySQL binlog source.
    Mysql,
    /// Kafka or Redpanda topic source.
    Kafka,
    /// Kafka or Redpanda topic source.
    Redpanda,
}

/// Sink selector plus sink-specific settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// Sink connector kind.
    pub kind: SinkKind,
    /// OpenSearch/Elasticsearch sink settings.
    #[serde(default)]
    pub opensearch: Option<OpenSearchSinkConfig>,
    /// Redis sink settings.
    #[serde(default)]
    pub redis: Option<RedisSinkConfig>,
    /// Meilisearch sink settings.
    #[serde(default)]
    pub meilisearch: Option<MeilisearchSinkConfig>,
    /// SurrealDB sink settings.
    #[serde(default)]
    pub surrealdb: Option<SurrealDbSinkConfig>,
}

impl SinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self.kind {
            SinkKind::Opensearch | SinkKind::Elasticsearch => {
                if self.redis.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.redis cannot be set for opensearch/elasticsearch sinks",
                    ));
                }
                if self.meilisearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.meilisearch cannot be set for opensearch/elasticsearch sinks",
                    ));
                }
                if self.surrealdb.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.surrealdb cannot be set for opensearch/elasticsearch sinks",
                    ));
                }
                let Some(opensearch) = &self.opensearch else {
                    return Err(ConfigError::InvalidField(
                        "sink.opensearch is required for opensearch/elasticsearch sinks",
                    ));
                };
                opensearch.validate()
            }
            SinkKind::Redis => {
                if self.opensearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.opensearch cannot be set for redis sinks",
                    ));
                }
                if self.meilisearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.meilisearch cannot be set for redis sinks",
                    ));
                }
                if self.surrealdb.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.surrealdb cannot be set for redis sinks",
                    ));
                }
                let Some(redis) = &self.redis else {
                    return Err(ConfigError::InvalidField(
                        "sink.redis is required for redis sinks",
                    ));
                };
                redis.validate()
            }
            SinkKind::Meilisearch => {
                if self.opensearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.opensearch cannot be set for meilisearch sinks",
                    ));
                }
                if self.redis.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.redis cannot be set for meilisearch sinks",
                    ));
                }
                if self.surrealdb.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.surrealdb cannot be set for meilisearch sinks",
                    ));
                }
                let Some(meilisearch) = &self.meilisearch else {
                    return Err(ConfigError::InvalidField(
                        "sink.meilisearch is required for meilisearch sinks",
                    ));
                };
                meilisearch.validate()
            }
            SinkKind::Surrealdb => {
                if self.opensearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.opensearch cannot be set for surrealdb sinks",
                    ));
                }
                if self.redis.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.redis cannot be set for surrealdb sinks",
                    ));
                }
                if self.meilisearch.is_some() {
                    return Err(ConfigError::InvalidField(
                        "sink.meilisearch cannot be set for surrealdb sinks",
                    ));
                }
                let Some(surrealdb) = &self.surrealdb else {
                    return Err(ConfigError::InvalidField(
                        "sink.surrealdb is required for surrealdb sinks",
                    ));
                };
                surrealdb.validate()
            }
        }
    }
}

/// Supported sink connector kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkKind {
    /// OpenSearch sink.
    Opensearch,
    /// Elasticsearch-compatible sink.
    Elasticsearch,
    /// Redis materialization sink.
    Redis,
    /// Meilisearch search sink.
    Meilisearch,
    /// SurrealDB multi-model sink.
    Surrealdb,
}

/// SurrealDB sink settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurrealDbSinkConfig {
    /// Deployment-provided endpoint reference, usually `env:VS_SURREAL_ENDPOINT`.
    pub endpoint_ref: ValueRef,
    /// Namespace to write into.
    pub namespace: String,
    /// Database to write into.
    pub database: String,
    /// Basic-auth username reference.
    pub username_ref: ValueRef,
    /// Basic-auth password reference.
    pub password_ref: ValueRef,
    /// Opt-in: create the namespace/database at startup (needs root or
    /// namespace credentials). Default false.
    #[serde(default)]
    pub auto_create_database: Option<bool>,
    /// Per-event table routing policy.
    #[serde(default)]
    pub table_routing: SurrealTableRoutingConfig,
    /// Prefix prepended to routed table names.
    #[serde(default)]
    pub table_prefix: Option<String>,
    /// Maximum documents per statement run. Default 1000.
    #[serde(default)]
    pub max_batch_docs: Option<usize>,
    /// Maximum request body bytes. Default 8MiB.
    #[serde(default)]
    pub max_batch_bytes: Option<usize>,
    /// Per-request HTTP timeout in milliseconds. Default 30000.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    /// HNSW vector indexes ensured at startup.
    #[serde(default)]
    pub vector_indexes: Vec<SurrealVectorIndexConfig>,
    /// Join paths materialized per document for cheap reverse lookups.
    #[serde(default)]
    pub lookup_fields: Vec<SurrealLookupFieldConfig>,
    /// FK-derived graph edges materialized as RELATE relations.
    #[serde(default)]
    pub graph_edges: Vec<SurrealEdgeConfig>,
    /// Disable TLS certificate verification. Development only.
    #[serde(default)]
    pub insecure_tls: Option<bool>,
    /// TLS transport policy. Prefer this over `insecure_tls`.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl SurrealDbSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        const MAX_BATCH_DOCS: usize = 250_000;
        const MAX_BATCH_BYTES: usize = 96 * 1024 * 1024;
        const MAX_REQUEST_TIMEOUT_MS: u64 = 600_000;

        if self.namespace.is_empty() {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.namespace must not be empty",
            ));
        }
        if self.database.is_empty() {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.database must not be empty",
            ));
        }
        if let SurrealTableRoutingConfig::Fixed { table } = &self.table_routing {
            if table.is_empty() {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.table_routing.table must not be empty",
                ));
            }
        }
        if self.max_batch_docs == Some(0) || self.max_batch_docs > Some(MAX_BATCH_DOCS) {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.max_batch_docs must be between 1 and 250000",
            ));
        }
        if self.max_batch_bytes == Some(0) || self.max_batch_bytes > Some(MAX_BATCH_BYTES) {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.max_batch_bytes must be between 1 and 96MiB",
            ));
        }
        if self.request_timeout_ms == Some(0)
            || self.request_timeout_ms > Some(MAX_REQUEST_TIMEOUT_MS)
        {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.request_timeout_ms must be between 1 and 600000",
            ));
        }
        let mut edge_names = std::collections::HashSet::new();
        for edge in &self.graph_edges {
            let safe = |value: &str| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
            };
            if !safe(&edge.name) || !safe(&edge.from_table) || !safe(&edge.to_table) {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.graph_edges name/from_table/to_table must match [A-Za-z0-9_.-]",
                ));
            }
            if edge.fk_columns.is_empty() || edge.fk_columns.iter().any(|c| c.is_empty()) {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.graph_edges entries need at least one non-empty fk_column",
                ));
            }
            if !edge_names.insert(edge.name.as_str()) {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.graph_edges names must be unique",
                ));
            }
        }
        if !self.graph_edges.is_empty()
            && matches!(self.table_routing, SurrealTableRoutingConfig::Fixed { .. })
        {
            return Err(ConfigError::InvalidField(
                "sink.surrealdb.graph_edges requires by_output_relation or by_projection_target table_routing",
            ));
        }
        for entry in &self.lookup_fields {
            let safe_path = |value: &str| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
                    && value.split('.').all(|segment| !segment.is_empty())
            };
            if entry.table.is_empty() || !safe_path(&entry.field) {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.lookup_fields entries need a table and a dotted field in [A-Za-z0-9_.-]",
                ));
            }
        }
        for index in &self.vector_indexes {
            let safe = |value: &str| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
            };
            if !safe(&index.table) || !safe(&index.field) {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.vector_indexes table/field must match [A-Za-z0-9_.-]",
                ));
            }
            if index.dimension == 0 || index.dimension > 65_535 {
                return Err(ConfigError::InvalidField(
                    "sink.surrealdb.vector_indexes dimension must be between 1 and 65535",
                ));
            }
        }
        if let Some(tls) = &self.tls {
            tls.validate("sink.surrealdb.tls")?;
        }
        Ok(())
    }
}

/// How events map to SurrealDB tables.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum SurrealTableRoutingConfig {
    /// Route by the event output relation. Default.
    #[default]
    ByOutputRelation,
    /// Route by the projection target header.
    ByProjectionTarget,
    /// All events target one table.
    Fixed {
        /// Target table name, before prefixing.
        table: String,
    },
}

/// One join path materialized for reverse lookups.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurrealLookupFieldConfig {
    /// Routed table name (post-prefix, as written).
    pub table: String,
    /// Dotted document path of the embedded join key.
    pub field: String,
}

/// One FK-derived graph edge kept in sync as SurrealDB RELATE records.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurrealEdgeConfig {
    /// Edge table name (pre-prefix), e.g. `wrote`.
    pub name: String,
    /// Source relation owning the FK (`ventstream.cdc.relation` value).
    pub from_table: String,
    /// FK column(s) on `from_table`, in the referenced key's order.
    pub fk_columns: Vec<String>,
    /// Referenced relation.
    pub to_table: String,
    /// Point the edge referenced -> FK-owner instead of the FK-forward
    /// default (`person->wrote->article` rather than
    /// `article->authored_by->person`).
    #[serde(default)]
    pub reversed: Option<bool>,
}

/// One HNSW vector index ensured at startup.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurrealVectorIndexConfig {
    /// Routed table name the index lives on (post-prefix, as written).
    pub table: String,
    /// Document field holding the embedding array.
    pub field: String,
    /// Embedding dimension.
    pub dimension: u32,
    /// Distance function. Default cosine.
    #[serde(default)]
    pub distance: SurrealVectorDistanceConfig,
}

/// Distance functions for SurrealDB HNSW indexes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurrealVectorDistanceConfig {
    /// Cosine distance. Default.
    #[default]
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
}

/// Meilisearch sink settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeilisearchSinkConfig {
    /// Deployment-provided endpoint reference, usually `env:VS_MEILI_ENDPOINT`.
    pub endpoint_ref: ValueRef,
    /// API key reference. Use a scoped key, never the master key.
    #[serde(default)]
    pub api_key_ref: Option<ValueRef>,
    /// Per-event index routing policy.
    #[serde(default)]
    pub index_routing: MeilisearchIndexRoutingConfig,
    /// Prefix prepended to routed index UIDs. `[A-Za-z0-9_-]` only.
    #[serde(default)]
    pub index_prefix: Option<String>,
    /// Create missing indexes implicitly on first write. Default true.
    #[serde(default)]
    pub auto_create_indexes: Option<bool>,
    /// Document attribute holding the encoded primary key. Default `_vs_pk`.
    #[serde(default)]
    pub primary_key_field: Option<String>,
    /// Maximum documents per request. Default 2000.
    #[serde(default)]
    pub max_batch_docs: Option<usize>,
    /// Maximum request body bytes. Default 16MiB.
    #[serde(default)]
    pub max_batch_bytes: Option<usize>,
    /// Task-poll deadline in milliseconds. Default 120000.
    #[serde(default)]
    pub task_deadline_ms: Option<u64>,
    /// Per-request HTTP timeout in milliseconds. Default 30000.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    /// Declared managed index settings (fixed routing only).
    #[serde(default)]
    pub settings: Option<MeilisearchSettingsConfig>,
    /// Disable TLS certificate verification. Development only.
    #[serde(default)]
    pub insecure_tls: Option<bool>,
    /// TLS transport policy. Prefer this over `insecure_tls`.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl MeilisearchSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        const MAX_BATCH_DOCS: usize = 250_000;
        const MAX_BATCH_BYTES: usize = 96 * 1024 * 1024;
        const MAX_TASK_DEADLINE_MS: u64 = 3_600_000;
        const MAX_REQUEST_TIMEOUT_MS: u64 = 600_000;

        if let Some(prefix) = &self.index_prefix {
            if !prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return Err(ConfigError::InvalidField(
                    "sink.meilisearch.index_prefix must match [A-Za-z0-9_-]",
                ));
            }
        }
        if let Some(field) = &self.primary_key_field {
            if field.is_empty()
                || !field
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            {
                return Err(ConfigError::InvalidField(
                    "sink.meilisearch.primary_key_field must be non-empty [A-Za-z0-9_-]",
                ));
            }
        }
        if matches!(self.max_batch_docs, Some(v) if v == 0 || v > MAX_BATCH_DOCS) {
            return Err(ConfigError::InvalidField(
                "sink.meilisearch.max_batch_docs must be between 1 and 250000",
            ));
        }
        if matches!(self.max_batch_bytes, Some(v) if v == 0 || v > MAX_BATCH_BYTES) {
            return Err(ConfigError::InvalidField(
                "sink.meilisearch.max_batch_bytes must be between 1 and 96MiB",
            ));
        }
        if matches!(self.task_deadline_ms, Some(v) if v == 0 || v > MAX_TASK_DEADLINE_MS) {
            return Err(ConfigError::InvalidField(
                "sink.meilisearch.task_deadline_ms must be between 1 and 3600000",
            ));
        }
        if matches!(self.request_timeout_ms, Some(v) if v == 0 || v > MAX_REQUEST_TIMEOUT_MS) {
            return Err(ConfigError::InvalidField(
                "sink.meilisearch.request_timeout_ms must be between 1 and 600000",
            ));
        }
        if let Some(tls) = &self.tls {
            tls.validate("sink.meilisearch.tls")?;
            if self.insecure_tls == Some(true) {
                return Err(ConfigError::InvalidField(
                    "sink.meilisearch.tls and sink.meilisearch.insecure_tls=true are mutually exclusive",
                ));
            }
        }
        if let MeilisearchIndexRoutingConfig::Fixed { index } = &self.index_routing {
            if index.is_empty() {
                return Err(ConfigError::InvalidField(
                    "sink.meilisearch.index_routing.index must not be empty",
                ));
            }
        } else if self.settings.is_some() {
            return Err(ConfigError::InvalidField(
                "sink.meilisearch.settings requires fixed index routing",
            ));
        }
        Ok(())
    }
}

/// Meilisearch index routing policy.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum MeilisearchIndexRoutingConfig {
    /// Route by the event output relation. Default.
    #[default]
    ByOutputRelation,
    /// Route by the projection target header.
    ByProjectionTarget,
    /// All events target one index.
    Fixed {
        /// Target index name, before prefixing and encoding.
        index: String,
    },
}

/// Declared Meilisearch index settings. Only declared attributes are
/// asserted; everything else is left untouched.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeilisearchSettingsConfig {
    /// Attributes usable in filter expressions.
    #[serde(default)]
    pub filterable_attributes: Vec<String>,
    /// Attributes usable in sort expressions.
    #[serde(default)]
    pub sortable_attributes: Vec<String>,
}

/// Redis materialization sink settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisSinkConfig {
    /// Deployment-provided `redis://` or `rediss://` endpoint reference.
    #[serde(default)]
    pub endpoint_ref: Option<ValueRef>,
    /// Optional Sentinel or Cluster topology.
    #[serde(default)]
    pub topology: Option<RedisTopologyConfig>,
    /// Optional Redis ACL credentials.
    #[serde(default)]
    pub auth: Option<RedisAuthConfig>,
    /// Optional custom trust or mutual-TLS identity files.
    #[serde(default)]
    pub tls: Option<RedisTlsConfig>,
    /// Deterministic key construction.
    pub keyspace: RedisKeyspaceConfig,
    /// Stored value representation.
    #[serde(default)]
    pub document: RedisDocumentConfig,
    /// Retention contract.
    #[serde(default)]
    pub contract: RedisContractConfig,
    /// Required downstream acknowledgement.
    #[serde(default)]
    pub acknowledgement: RedisAcknowledgementConfig,
    /// Renewable writer ownership and controlled handoff settings.
    #[serde(default)]
    pub writer: RedisWriterConfig,
    /// Maximum approximate pipeline bytes.
    #[serde(default)]
    pub max_batch_bytes: Option<usize>,
    /// Maximum encoded key bytes accepted from one event.
    #[serde(default)]
    pub max_key_bytes: Option<usize>,
    /// Maximum value bytes accepted from one event.
    #[serde(default)]
    pub max_value_bytes: Option<usize>,
    /// Connection timeout.
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
    /// Command response timeout.
    #[serde(default)]
    pub response_timeout_ms: Option<u64>,
}

impl RedisSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
        const DEFAULT_MAX_KEY_BYTES: usize = 16 * 1024;
        const DEFAULT_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
        const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
        const MAX_KEY_BYTES: usize = 1024 * 1024;
        const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
        const DEFAULT_RESPONSE_TIMEOUT_MS: u64 = 30_000;
        const MAX_CONNECT_TIMEOUT_MS: u64 = 120_000;
        const MAX_RESPONSE_TIMEOUT_MS: u64 = 600_000;

        match (&self.endpoint_ref, &self.topology) {
            (Some(endpoint), None) => endpoint.validate()?,
            (None, Some(topology)) => topology.validate()?,
            (Some(_), Some(_)) => {
                return Err(ConfigError::InvalidField(
                    "sink.redis must configure endpoint_ref or topology, not both",
                ));
            }
            (None, None) => {
                return Err(ConfigError::InvalidField(
                    "sink.redis requires endpoint_ref or topology",
                ));
            }
        }
        if let Some(auth) = &self.auth {
            auth.validate()?;
        }
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        self.keyspace.validate()?;
        if matches!(self.topology, Some(RedisTopologyConfig::Cluster { .. }))
            && matches!(&self.keyspace.routing, RedisKeyRoutingConfig::Views { views } if views.len() > 1)
        {
            return Err(ConfigError::InvalidField(
                "sink.redis Cluster topology supports at most one declarative view",
            ));
        }
        self.contract.validate()?;
        self.acknowledgement.validate()?;
        self.writer.validate()?;
        if self.max_batch_bytes == Some(0)
            || self.max_key_bytes == Some(0)
            || self.max_value_bytes == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "sink.redis byte limits must be positive",
            ));
        }
        let max_batch_bytes = self.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES);
        let max_key_bytes = self.max_key_bytes.unwrap_or(DEFAULT_MAX_KEY_BYTES);
        let max_value_bytes = self.max_value_bytes.unwrap_or(DEFAULT_MAX_VALUE_BYTES);
        if max_batch_bytes > MAX_BATCH_BYTES
            || max_key_bytes > MAX_KEY_BYTES
            || max_value_bytes > MAX_VALUE_BYTES
        {
            return Err(ConfigError::InvalidField(
                "sink.redis byte limits exceed the supported safety ceilings",
            ));
        }
        if self.connect_timeout_ms == Some(0) || self.response_timeout_ms == Some(0) {
            return Err(ConfigError::InvalidField(
                "sink.redis timeouts must be positive",
            ));
        }
        if self
            .connect_timeout_ms
            .is_some_and(|timeout| timeout > MAX_CONNECT_TIMEOUT_MS)
            || self
                .response_timeout_ms
                .is_some_and(|timeout| timeout > MAX_RESPONSE_TIMEOUT_MS)
        {
            return Err(ConfigError::InvalidField(
                "sink.redis timeouts exceed the supported safety ceilings",
            ));
        }
        if matches!(
            self.acknowledgement,
            RedisAcknowledgementConfig::Replicated { timeout_ms, .. }
                | RedisAcknowledgementConfig::Aof { timeout_ms, .. }
                if timeout_ms > self.response_timeout_ms.unwrap_or(DEFAULT_RESPONSE_TIMEOUT_MS)
        ) {
            return Err(ConfigError::InvalidField(
                "sink.redis acknowledgement timeout must not exceed response_timeout_ms",
            ));
        }
        Ok(())
    }
}

/// Renewable Redis target writer ownership.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisWriterConfig {
    /// Stable deployment identity. Fleet deployments normally reference their deployment ID.
    #[serde(default)]
    pub id_ref: Option<ValueRef>,
    /// Writer lease duration. The active engine renews it while idle.
    #[serde(default)]
    pub lease_ms: Option<u64>,
    /// Previous writer identity accepted for one controlled compare-and-swap handoff.
    #[serde(default)]
    pub takeover_from_ref: Option<ValueRef>,
}

impl RedisWriterConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        const MIN_LEASE_MS: u64 = 3_000;
        const MAX_LEASE_MS: u64 = 600_000;
        if let Some(reference) = &self.id_ref {
            reference.validate()?;
        }
        if let Some(reference) = &self.takeover_from_ref {
            reference.validate()?;
        }
        if self
            .lease_ms
            .is_some_and(|lease| !(MIN_LEASE_MS..=MAX_LEASE_MS).contains(&lease))
        {
            return Err(ConfigError::InvalidField(
                "sink.redis.writer.lease_ms must be between 3000 and 600000",
            ));
        }
        Ok(())
    }
}

/// Redis discovery and client-side routing mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum RedisTopologyConfig {
    /// Discover the current writable primary through Redis Sentinel.
    Sentinel {
        /// Sentinel master service name.
        service_name: String,
        /// Sentinel endpoint references tried in order.
        endpoints: Vec<ValueRef>,
        /// Whether Sentinel-discovered Redis data nodes require TLS.
        data_node_tls: bool,
        /// Optional Sentinel-specific credentials.
        #[serde(default)]
        sentinel_auth: Option<RedisAuthConfig>,
        /// Optional Sentinel-specific trust or mutual-TLS identity.
        #[serde(default)]
        sentinel_tls: Option<RedisTlsConfig>,
    },
    /// Discover and route Redis Cluster hash slots from initial nodes.
    Cluster {
        /// Initial Cluster endpoint references.
        endpoints: Vec<ValueRef>,
    },
}

impl RedisTopologyConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        const MAX_ENDPOINTS: usize = 32;
        match self {
            Self::Sentinel {
                service_name,
                endpoints,
                sentinel_auth,
                sentinel_tls,
                ..
            } => {
                if service_name.is_empty()
                    || service_name.trim() != service_name
                    || service_name.len() > 256
                    || service_name.chars().any(char::is_control)
                {
                    return Err(ConfigError::InvalidField(
                        "sink.redis.topology.service_name must be trimmed safe text of at most 256 bytes",
                    ));
                }
                validate_redis_topology_endpoints(endpoints, MAX_ENDPOINTS)?;
                if let Some(auth) = sentinel_auth {
                    auth.validate()?;
                }
                if let Some(tls) = sentinel_tls {
                    tls.validate()?;
                }
            }
            Self::Cluster { endpoints } => {
                validate_redis_topology_endpoints(endpoints, MAX_ENDPOINTS)?;
            }
        }
        Ok(())
    }
}

fn validate_redis_topology_endpoints(
    endpoints: &[ValueRef],
    maximum: usize,
) -> Result<(), ConfigError> {
    if endpoints.is_empty() || endpoints.len() > maximum {
        return Err(ConfigError::InvalidField(
            "sink.redis.topology.endpoints must contain 1 to 32 references",
        ));
    }
    for endpoint in endpoints {
        endpoint.validate()?;
    }
    Ok(())
}

/// Custom TLS material for a Redis sink.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisTlsConfig {
    /// PEM CA bundle used instead of system roots.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate chain for mutual TLS.
    #[serde(default)]
    pub client_cert_file: Option<PathBuf>,
    /// PEM private key for mutual TLS.
    #[serde(default)]
    pub client_key_file: Option<PathBuf>,
}

impl RedisTlsConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        for path in [
            self.ca_file.as_ref(),
            self.client_cert_file.as_ref(),
            self.client_key_file.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if path.as_os_str().is_empty() {
                return Err(ConfigError::InvalidField(
                    "sink.redis.tls file paths must not be empty",
                ));
            }
        }
        if self.client_cert_file.is_some() != self.client_key_file.is_some() {
            return Err(ConfigError::InvalidField(
                "sink.redis.tls requires both client_cert_file and client_key_file for mutual TLS",
            ));
        }
        Ok(())
    }
}

/// Redis authentication mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum RedisAuthConfig {
    /// No Redis authentication.
    None,
    /// Redis ACL username and password.
    Acl {
        /// Username reference.
        username_ref: ValueRef,
        /// Password reference.
        password_ref: ValueRef,
    },
    /// Password-only Redis authentication.
    Password {
        /// Password reference.
        password_ref: ValueRef,
    },
}

impl RedisAuthConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::None => Ok(()),
            Self::Acl {
                username_ref,
                password_ref,
            } => {
                username_ref.validate()?;
                password_ref.validate()
            }
            Self::Password { password_ref } => password_ref.validate(),
        }
    }
}

/// Redis key construction.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisKeyspaceConfig {
    /// Key prefix owned by this pipeline.
    pub prefix: String,
    /// Target routing strategy.
    #[serde(default)]
    pub routing: RedisKeyRoutingConfig,
    /// Permission for target-wide keyspace operations.
    #[serde(default)]
    pub ownership: RedisKeyspaceOwnershipConfig,
}

impl RedisKeyspaceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.prefix.is_empty()
            || self.prefix.len() > 256
            || !self.prefix.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-' | b'.')
            })
        {
            return Err(ConfigError::InvalidField(
                "sink.redis.keyspace.prefix must be 1 to 256 ASCII letters, digits, ':', '_', '-', or '.'",
            ));
        }
        if matches!(self.routing, RedisKeyRoutingConfig::Views { .. })
            && self.ownership != RedisKeyspaceOwnershipConfig::Exclusive
        {
            return Err(ConfigError::InvalidField(
                "sink.redis views routing requires keyspace.ownership=exclusive",
            ));
        }
        self.routing.validate()
    }
}

/// Whether a Redis pipeline may perform target-wide keyspace operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisKeyspaceOwnershipConfig {
    /// Other writers may use the same prefix and targets.
    #[default]
    Shared,
    /// This pipeline exclusively owns every routed target under the prefix.
    Exclusive,
}

/// Redis key target routing.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "strategy", rename_all = "snake_case")]
pub enum RedisKeyRoutingConfig {
    /// Route by `ventstream.cdc.relation`.
    #[default]
    ByOutputRelation,
    /// Route by `ventstream.target.index`.
    ByProjectionTarget,
    /// Route all documents to one target.
    Fixed {
        /// Fixed target segment.
        name: String,
    },
    /// Materialize declarative lookup views.
    Views {
        /// Bounded view definitions.
        views: Vec<RedisViewConfig>,
    },
}

impl RedisKeyRoutingConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if matches!(
            self,
            Self::Fixed { name }
                if name.trim().is_empty()
                    || name.trim() != name
                    || name.len() > 1024
                    || name.chars().any(char::is_control)
        ) {
            return Err(ConfigError::InvalidField(
                "sink.redis.keyspace.routing.name must be trimmed, 1 to 1024 bytes, and contain no control characters",
            ));
        }
        if let Self::Views { views } = self {
            validate_redis_views(views)?;
        }
        Ok(())
    }
}

/// Declarative Redis lookup view.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisViewConfig {
    /// Stable target name.
    pub name: String,
    /// Exact source selection.
    pub source: RedisViewSourceConfig,
    /// Deterministic key template.
    pub key: RedisViewKeyConfig,
    /// Optional document filter.
    #[serde(default)]
    pub filter: Option<RedisViewFilterConfig>,
    /// Stored value selection.
    #[serde(default)]
    pub value: RedisViewValueConfig,
}

/// Raw relation or projection selected by a view.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisViewSourceConfig {
    /// Optional source namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Raw relation or collection.
    #[serde(default)]
    pub relation: Option<String>,
    /// Denormalized projection target.
    #[serde(default)]
    pub projection_target: Option<String>,
}

/// View key template.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisViewKeyConfig {
    /// `${doc_id}`, `${header:NAME}`, and `${json:/pointer}` template.
    pub template: String,
    /// Missing placeholder behavior.
    #[serde(default)]
    pub on_missing: RedisViewMissingBehaviorConfig,
}

/// Missing key placeholder behavior.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewMissingBehaviorConfig {
    /// Block the pipeline.
    #[default]
    Block,
    /// Remove the prior view entry and omit the new one.
    Skip,
}

/// Bounded JSON filter.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisViewFilterConfig {
    /// Condition composition.
    #[serde(default)]
    pub mode: RedisViewFilterModeConfig,
    /// Scalar JSON-pointer conditions.
    pub conditions: Vec<RedisViewConditionConfig>,
}

/// View filter composition.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisViewFilterModeConfig {
    /// Every condition must match.
    #[default]
    All,
    /// At least one condition must match.
    Any,
}

/// Supported view filter comparisons.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "operator", rename_all = "snake_case")]
pub enum RedisViewConditionConfig {
    /// Exact equality.
    Equals {
        /// RFC 6901 JSON pointer.
        path: String,
        /// Expected scalar.
        value: serde_json::Value,
    },
    /// Exact inequality.
    NotEquals {
        /// RFC 6901 JSON pointer.
        path: String,
        /// Excluded scalar.
        value: serde_json::Value,
    },
    /// Membership.
    In {
        /// RFC 6901 JSON pointer.
        path: String,
        /// Accepted scalars.
        values: Vec<serde_json::Value>,
    },
    /// Non-membership.
    NotIn {
        /// RFC 6901 JSON pointer.
        path: String,
        /// Excluded scalars.
        values: Vec<serde_json::Value>,
    },
    /// Pointer existence.
    Exists {
        /// RFC 6901 JSON pointer.
        path: String,
    },
    /// Pointer absence.
    NotExists {
        /// RFC 6901 JSON pointer.
        path: String,
    },
}

/// Value emitted by a Redis view.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum RedisViewValueConfig {
    /// Complete document.
    #[default]
    Document,
    /// One JSON subtree.
    Pointer {
        /// RFC 6901 JSON pointer.
        path: String,
    },
    /// Named JSON object fields.
    Fields {
        /// Output field name to JSON pointer.
        fields: BTreeMap<String, String>,
    },
}

fn validate_redis_views(views: &[RedisViewConfig]) -> Result<(), ConfigError> {
    if views.is_empty() || views.len() > 32 {
        return Err(ConfigError::InvalidField(
            "sink.redis views routing requires 1 to 32 views",
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for view in views {
        if invalid_bounded_redis_view_text(&view.name, 256) || view.name.starts_with("__ventstream")
        {
            return Err(ConfigError::InvalidField(
                "sink.redis view names must be trimmed, unique, 1 to 256 bytes, and not use the reserved __ventstream prefix",
            ));
        }
        if !names.insert(view.name.as_str()) {
            return Err(ConfigError::InvalidField(
                "sink.redis view names must be trimmed, unique, 1 to 256 bytes, and not use the reserved __ventstream prefix",
            ));
        }
        if usize::from(view.source.relation.is_some())
            + usize::from(view.source.projection_target.is_some())
            != 1
        {
            return Err(ConfigError::InvalidField(
                "sink.redis view source must define exactly one of relation or projection_target",
            ));
        }
        for value in [
            view.source.namespace.as_deref(),
            view.source.relation.as_deref(),
            view.source.projection_target.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if invalid_bounded_redis_view_text(value, 1024) {
                return Err(ConfigError::InvalidField(
                    "sink.redis view source selectors must be trimmed, 1 to 1024 bytes, and contain no control characters",
                ));
            }
        }
        validate_redis_view_template(&view.key.template)?;
        if let Some(filter) = &view.filter {
            if filter.conditions.is_empty() || filter.conditions.len() > 32 {
                return Err(ConfigError::InvalidField(
                    "sink.redis view filters require 1 to 32 conditions",
                ));
            }
            for condition in &filter.conditions {
                match condition {
                    RedisViewConditionConfig::Equals { path, value }
                    | RedisViewConditionConfig::NotEquals { path, value } => {
                        validate_redis_view_pointer(path)?;
                        validate_redis_view_scalar(value)?;
                    }
                    RedisViewConditionConfig::In { path, values }
                    | RedisViewConditionConfig::NotIn { path, values } => {
                        validate_redis_view_pointer(path)?;
                        if values.is_empty() || values.len() > 64 {
                            return Err(ConfigError::InvalidField(
                                "sink.redis view membership conditions require 1 to 64 scalar values",
                            ));
                        }
                        for value in values {
                            validate_redis_view_scalar(value)?;
                        }
                    }
                    RedisViewConditionConfig::Exists { path }
                    | RedisViewConditionConfig::NotExists { path } => {
                        validate_redis_view_pointer(path)?;
                    }
                }
            }
        }
        match &view.value {
            RedisViewValueConfig::Document => {}
            RedisViewValueConfig::Pointer { path } => validate_redis_view_pointer(path)?,
            RedisViewValueConfig::Fields { fields } => {
                if fields.is_empty() || fields.len() > 128 {
                    return Err(ConfigError::InvalidField(
                        "sink.redis view value fields require 1 to 128 entries",
                    ));
                }
                for (name, path) in fields {
                    if invalid_bounded_redis_view_text(name, 256) {
                        return Err(ConfigError::InvalidField(
                            "sink.redis view output field names must be trimmed, 1 to 256 bytes, and contain no control characters",
                        ));
                    }
                    validate_redis_view_pointer(path)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_redis_view_template(template: &str) -> Result<(), ConfigError> {
    if template.is_empty() || template.len() > 4096 || template.chars().any(char::is_control) {
        return Err(ConfigError::InvalidField(
            "sink.redis view key templates must be 1 to 4096 bytes with no control characters",
        ));
    }
    let mut remaining = template;
    let mut parts = 0usize;
    let mut has_identity = false;
    while let Some(start) = remaining.find("${") {
        parts = parts.saturating_add(usize::from(start > 0));
        let expression = &remaining[start + 2..];
        let Some(end) = expression.find('}') else {
            return Err(ConfigError::InvalidField(
                "sink.redis view key template has an unterminated placeholder",
            ));
        };
        let token = &expression[..end];
        if token == "doc_id" {
            has_identity = true;
        } else if let Some(header) = token.strip_prefix("header:") {
            if header.is_empty() || header.len() > 2048 || header.chars().any(char::is_control) {
                return Err(ConfigError::InvalidField(
                    "sink.redis view key header reference is invalid",
                ));
            }
        } else if let Some(pointer) = token.strip_prefix("json:") {
            validate_redis_view_pointer(pointer)?;
            has_identity = true;
        } else {
            return Err(ConfigError::InvalidField(
                "sink.redis view key template contains an unsupported placeholder",
            ));
        }
        parts = parts.saturating_add(1);
        remaining = &expression[end + 1..];
    }
    parts = parts.saturating_add(usize::from(!remaining.is_empty()));
    if parts > 64 {
        return Err(ConfigError::InvalidField(
            "sink.redis view key template exceeds 64 parts",
        ));
    }
    if !has_identity {
        return Err(ConfigError::InvalidField(
            "sink.redis view key template must include ${doc_id} or ${json:/pointer}",
        ));
    }
    Ok(())
}

fn validate_redis_view_pointer(pointer: &str) -> Result<(), ConfigError> {
    if pointer.len() > 2048
        || (!pointer.is_empty() && !pointer.starts_with('/'))
        || pointer.chars().any(char::is_control)
    {
        return Err(ConfigError::InvalidField(
            "sink.redis view paths must be RFC 6901 JSON pointers up to 2048 bytes",
        ));
    }
    let mut bytes = pointer.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
            return Err(ConfigError::InvalidField(
                "sink.redis view path contains an invalid JSON pointer escape",
            ));
        }
    }
    Ok(())
}

fn validate_redis_view_scalar(value: &serde_json::Value) -> Result<(), ConfigError> {
    if matches!(
        value,
        serde_json::Value::Array(_) | serde_json::Value::Object(_)
    ) {
        return Err(ConfigError::InvalidField(
            "sink.redis view filter values must be JSON scalars",
        ));
    }
    Ok(())
}

fn invalid_bounded_redis_view_text(value: &str, max_bytes: usize) -> bool {
    value.trim().is_empty()
        || value.trim() != value
        || value.len() > max_bytes
        || value.chars().any(char::is_control)
}

/// Redis value settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisDocumentConfig {
    /// Stored representation.
    #[serde(default)]
    pub format: RedisDocumentFormatConfig,
}

/// Redis value representation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisDocumentFormatConfig {
    /// Store compact bytes with `SET`.
    #[default]
    String,
    /// Store JSON with RedisJSON `JSON.SET`.
    Json,
}

/// Redis retention contract.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum RedisContractConfig {
    /// Retain materialized state without expiry.
    #[default]
    MaterializedView,
    /// Expire every upsert after the configured TTL.
    Cache {
        /// Key TTL.
        ttl_ms: u64,
    },
}

impl RedisContractConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Self::Cache { ttl_ms } = self {
            if *ttl_ms == 0 || *ttl_ms > i64::MAX as u64 {
                return Err(ConfigError::InvalidField(
                    "sink.redis.contract.ttl_ms must be within the Redis integer range",
                ));
            }
        }
        Ok(())
    }
}

/// Redis write acknowledgement policy.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum RedisAcknowledgementConfig {
    /// Advance after the primary applies the pipeline.
    #[default]
    Primary,
    /// Require replica acknowledgement via `WAIT`.
    Replicated {
        /// Minimum replica acknowledgements.
        replicas: usize,
        /// Maximum acknowledgement wait.
        timeout_ms: u64,
    },
    /// Require local and/or replica AOF fsync via Redis 7.2+ `WAITAOF`.
    Aof {
        /// Require the writable primary to fsync its local AOF.
        #[serde(default = "default_redis_aof_local")]
        local: bool,
        /// Minimum replica AOF fsync acknowledgements.
        #[serde(default)]
        replicas: usize,
        /// Maximum acknowledgement wait.
        timeout_ms: u64,
    },
}

impl RedisAcknowledgementConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let (local, replicas, timeout_ms) = match self {
            Self::Primary => return Ok(()),
            Self::Replicated {
                replicas,
                timeout_ms,
            } => (false, *replicas, *timeout_ms),
            Self::Aof {
                local,
                replicas,
                timeout_ms,
            } => (*local, *replicas, *timeout_ms),
        };
        if timeout_ms == 0
            || replicas > usize::try_from(i64::MAX).unwrap_or(usize::MAX)
            || timeout_ms > i64::MAX as u64
            || (matches!(self, Self::Replicated { .. }) && replicas == 0)
            || (matches!(self, Self::Aof { .. }) && !local && replicas == 0)
        {
            return Err(ConfigError::InvalidField(
                "sink.redis.acknowledgement requires a positive timeout and at least one requested acknowledgement",
            ));
        }
        Ok(())
    }
}

const fn default_redis_aof_local() -> bool {
    true
}

/// OpenSearch/Elasticsearch sink settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenSearchSinkConfig {
    /// Deployment-provided endpoint reference, usually `env:VS_OS_ENDPOINT`.
    pub endpoint_ref: ValueRef,
    /// Optional authentication reference.
    #[serde(default)]
    pub auth: Option<OpenSearchAuthConfig>,
    /// Per-output index routing policy.
    #[serde(default)]
    pub index_routing: OpenSearchIndexRouting,
    /// Allow destructive full-purge reconciliation.
    #[serde(default)]
    pub reconcile_allow_full_purge: Option<bool>,
    /// Disable TLS certificate verification. Development only.
    #[serde(default)]
    pub insecure_tls: Option<bool>,
    /// TLS transport policy. Prefer this over `insecure_tls`.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl OpenSearchSinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(auth) = &self.auth {
            auth.validate()?;
        }
        if let Some(tls) = &self.tls {
            tls.validate("sink.opensearch.tls")?;
            if self.insecure_tls == Some(true) {
                return Err(ConfigError::InvalidField(
                    "sink.opensearch.tls and sink.opensearch.insecure_tls=true are mutually exclusive",
                ));
            }
        }
        self.index_routing.validate()
    }
}

/// OpenSearch authentication mode.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "mode", rename_all = "snake_case")]
pub enum OpenSearchAuthConfig {
    /// No sink authentication.
    None,
    /// Basic authentication from external references.
    Basic {
        /// Username reference.
        username_ref: ValueRef,
        /// Password reference.
        password_ref: ValueRef,
    },
    /// API-key authentication from an external reference.
    ApiKey {
        /// API key reference.
        api_key_ref: ValueRef,
    },
}

impl OpenSearchAuthConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::None => Ok(()),
            Self::Basic {
                username_ref,
                password_ref,
            } => {
                username_ref.validate()?;
                password_ref.validate()
            }
            Self::ApiKey { api_key_ref } => api_key_ref.validate(),
        }
    }
}

/// Per-output OpenSearch index routing policy.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, tag = "strategy", rename_all = "snake_case")]
pub enum OpenSearchIndexRouting {
    /// Route to the event's output relation: `${header:ventstream.cdc.relation}`.
    #[default]
    ByOutputRelation,
    /// Route to the projection-owned target: `${header:ventstream.target.index}`.
    ByProjectionTarget,
    /// Route every output event to one fixed index.
    Fixed {
        /// Concrete index name.
        name: String,
    },
    /// Route with the existing OpenSearch template syntax.
    Template {
        /// Existing per-event template string.
        template: String,
    },
}

impl OpenSearchIndexRouting {
    /// Render this routing policy to the sink's current template model.
    pub fn as_legacy_template(&self) -> &str {
        match self {
            Self::ByOutputRelation => "${header:ventstream.cdc.relation}",
            Self::ByProjectionTarget => "${header:ventstream.target.index}",
            Self::Fixed { name } => name,
            Self::Template { template } => template,
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::ByOutputRelation | Self::ByProjectionTarget => Ok(()),
            Self::Fixed { name } => validate_nonempty("sink.opensearch.index_routing.name", name),
            Self::Template { template } => {
                validate_nonempty("sink.opensearch.index_routing.template", template)
            }
        }
    }
}

/// Paths to larger domain-specific specs.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecFiles {
    /// Postgres/MySQL relational join spec.
    #[serde(default)]
    pub joins: Option<PathBuf>,
    /// Neo4j denormalization spec.
    #[serde(default)]
    pub neo4j_denormalize: Option<PathBuf>,
    /// GraphQL SDL schema path.
    #[serde(default)]
    pub graphql_schema: Option<PathBuf>,
    /// GraphQL subscriptions YAML path.
    #[serde(default)]
    pub graphql_subscriptions: Option<PathBuf>,
    /// GraphQL discoverability manifest path.
    #[serde(default)]
    pub graphql_manifest: Option<PathBuf>,
}

impl ManagedConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("gateway_url", self.gateway_url.as_deref()),
            ("enrollment_url", self.enrollment_url.as_deref()),
        ] {
            if let Some(url) = value {
                if !url.starts_with("https://") && !url.starts_with("wss://") {
                    let _ = field;
                    return Err(ConfigError::InvalidField(
                        "managed gateway URLs must use https:// or wss://",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl SpecFiles {
    /// Whether any spec path is configured.
    pub fn any_set(&self) -> bool {
        self.joins.is_some()
            || self.neo4j_denormalize.is_some()
            || self.graphql_schema.is_some()
            || self.graphql_subscriptions.is_some()
            || self.graphql_manifest.is_some()
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if [
            self.joins.as_ref(),
            self.neo4j_denormalize.as_ref(),
            self.graphql_schema.as_ref(),
            self.graphql_subscriptions.as_ref(),
            self.graphql_manifest.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::InvalidField(
                "spec file paths must not be empty",
            ));
        }
        Ok(())
    }
}

/// Runtime settings shared by roles.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Health server listen address.
    #[serde(default)]
    pub health_listen: Option<String>,
    /// Capacity of the in-process source-to-dispatcher bus.
    #[serde(default)]
    pub bus_capacity: Option<usize>,
    /// Dead-letter queue file path.
    #[serde(default)]
    pub dlq_path: Option<PathBuf>,
    /// Dispatcher settings.
    #[serde(default)]
    pub dispatch: DispatchConfig,
    /// Adaptive process/cgroup memory protection.
    #[serde(default)]
    pub memory: MemoryRuntimeConfig,
    /// Join engine persistence and flushing settings.
    #[serde(default)]
    pub joins: JoinRuntimeConfig,
    /// Shared realtime broker settings used by both gateway roles.
    #[serde(default)]
    pub realtime: RealtimeRuntimeConfig,
    /// Native WebSocket gateway settings.
    #[serde(default)]
    pub ws: WebSocketRuntimeConfig,
    /// GraphQL gateway settings.
    #[serde(default)]
    pub graphql: GraphqlRuntimeConfig,
    /// Optional admin HTTP server settings.
    #[serde(default)]
    pub admin: AdminRuntimeConfig,
    /// Read-only MCP server settings for the `mcp` role.
    #[serde(default)]
    pub mcp: Option<McpRuntimeConfig>,
    /// Single-tenant deployment guard used by realtime gateways.
    #[serde(default)]
    pub tenant: Option<String>,
    /// Logging output format.
    #[serde(default)]
    pub log_format: Option<LogFormat>,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.bus_capacity == Some(0) {
            return Err(ConfigError::InvalidField(
                "runtime.bus_capacity must be positive",
            ));
        }
        if let Some(tenant) = &self.tenant {
            validate_nonempty("runtime.tenant", tenant)?;
        }
        self.dispatch.validate()?;
        self.memory.validate()?;
        self.joins.validate()?;
        self.realtime.validate()?;
        self.ws.validate()?;
        self.graphql.validate()?;
        if let Some(mcp) = &self.mcp {
            mcp.validate()?;
        }
        if let (Some(shared), Some(local)) = (self.realtime.provider, self.ws.provider) {
            if shared != local {
                return Err(ConfigError::InvalidField(
                    "runtime.realtime.provider conflicts with runtime.ws.provider",
                ));
            }
        }
        if let (Some(shared), Some(local)) = (self.realtime.provider, self.graphql.provider) {
            if shared != local {
                return Err(ConfigError::InvalidField(
                    "runtime.realtime.provider conflicts with runtime.graphql.provider",
                ));
            }
        }
        if self.realtime.redis_streams.is_some()
            && (self.ws.redis_streams.is_some() || self.graphql.redis_streams.is_some())
        {
            return Err(ConfigError::InvalidField(
                "define Redis Streams settings once under runtime.realtime or a role-specific block",
            ));
        }
        if self.realtime.redis_streams.is_some() && self.ws.jetstream.is_some() {
            return Err(ConfigError::InvalidField(
                "runtime.ws.jetstream conflicts with shared Redis Streams settings",
            ));
        }
        if self.realtime.redis_streams.is_some()
            && (matches!(
                self.ws.provider,
                Some(RealtimeBrokerProvider::NatsCore | RealtimeBrokerProvider::NatsJetstream)
            ) || matches!(
                self.graphql.provider,
                Some(RealtimeBrokerProvider::NatsCore | RealtimeBrokerProvider::NatsJetstream)
            ))
        {
            return Err(ConfigError::InvalidField(
                "role-specific NATS provider conflicts with shared Redis Streams settings",
            ));
        }
        if matches!(
            self.realtime.provider,
            Some(RealtimeBrokerProvider::NatsCore | RealtimeBrokerProvider::NatsJetstream)
        ) && (self.ws.redis_streams.is_some() || self.graphql.redis_streams.is_some())
        {
            return Err(ConfigError::InvalidField(
                "role-specific Redis Streams settings conflict with the shared NATS provider",
            ));
        }
        if self.realtime.provider == Some(RealtimeBrokerProvider::NatsCore)
            && self.ws.jetstream.is_some()
        {
            return Err(ConfigError::InvalidField(
                "runtime.ws.jetstream conflicts with the shared NATS Core provider",
            ));
        }
        self.admin.validate()
    }
}

/// Shared durable broker settings for realtime gateway roles.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealtimeRuntimeConfig {
    /// Broker provider selected for all enabled realtime roles.
    #[serde(default)]
    pub provider: Option<RealtimeBrokerProvider>,
    /// Redis Streams connection and fan-out settings.
    #[serde(default)]
    pub redis_streams: Option<RedisStreamsRuntimeConfig>,
}

impl RealtimeRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(redis) = &self.redis_streams {
            redis.validate()?;
        }
        if self.redis_streams.is_some()
            && matches!(
                self.provider,
                Some(RealtimeBrokerProvider::NatsCore | RealtimeBrokerProvider::NatsJetstream)
            )
        {
            return Err(ConfigError::InvalidField(
                "runtime.realtime.redis_streams requires provider=redis_streams",
            ));
        }
        Ok(())
    }
}

/// Dispatcher batching settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchConfig {
    /// Maximum events per sink batch.
    #[serde(default)]
    pub max_events: Option<usize>,
    /// Maximum bytes per sink batch.
    #[serde(default)]
    pub max_batch_bytes: Option<usize>,
    /// Maximum time to hold a non-empty batch before flushing.
    #[serde(default)]
    pub flush_ms: Option<u64>,
    /// Maximum sink writes in flight.
    #[serde(default)]
    pub parallel_bulks: Option<usize>,
}

/// Adaptive memory admission and pressure thresholds.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryRuntimeConfig {
    /// Enable automatic cgroup detection and byte-weighted admission.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Explicit event-memory budget. When omitted, a finite cgroup limit is used.
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    /// Maximum conservative memory estimate accepted for one event.
    #[serde(default)]
    pub max_event_bytes: Option<usize>,
    /// Process/cgroup sampling interval.
    #[serde(default)]
    pub sample_ms: Option<u64>,
    /// Continuous time below a recovery threshold before relaxing controls.
    #[serde(default)]
    pub recovery_ms: Option<u64>,
    /// Percent at which admission and request sizes begin shrinking.
    #[serde(default)]
    pub target_percent: Option<u8>,
    /// Percent at which high-pressure controls engage.
    #[serde(default)]
    pub high_percent: Option<u8>,
    /// Percent at which critical OOM protection engages.
    #[serde(default)]
    pub critical_percent: Option<u8>,
    /// Percentage points required below a threshold before recovery.
    #[serde(default)]
    pub hysteresis_percent: Option<u8>,
}

impl MemoryRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.budget_bytes == Some(0)
            || self.max_event_bytes == Some(0)
            || self.sample_ms == Some(0)
            || self.recovery_ms == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.memory byte and interval values must be positive",
            ));
        }

        let target = self.target_percent.unwrap_or(65);
        let high = self.high_percent.unwrap_or(75);
        let critical = self.critical_percent.unwrap_or(85);
        let hysteresis = self.hysteresis_percent.unwrap_or(5);
        if target == 0
            || target >= high
            || high >= critical
            || critical >= 100
            || hysteresis == 0
            || hysteresis >= target
        {
            return Err(ConfigError::InvalidField(
                "runtime.memory requires 0 < target_percent < high_percent < critical_percent < 100 and 0 < hysteresis_percent < target_percent",
            ));
        }

        if let (Some(budget), Some(max_event)) = (self.budget_bytes, self.max_event_bytes) {
            if max_event > budget / 4 {
                return Err(ConfigError::InvalidField(
                    "runtime.memory.max_event_bytes must not exceed one quarter of budget_bytes",
                ));
            }
        }
        Ok(())
    }
}

/// Join engine persistence/runtime settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRuntimeConfig {
    /// Persistent join-state directory.
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// redb commit threshold.
    #[serde(default)]
    pub persist_batch_ops: Option<usize>,
    /// Idle persistence flush cadence in milliseconds.
    #[serde(default)]
    pub idle_flush_ms: Option<u64>,
    /// Postgres LSN flush cadence in milliseconds.
    #[serde(default)]
    pub lsn_flush_ms: Option<u64>,
    /// Auto-resync when the joins YAML fingerprint changes.
    #[serde(default)]
    pub auto_resync_on_yaml_change: Option<bool>,
    /// Force a Postgres resync on this boot.
    #[serde(default)]
    pub force_resync: Option<bool>,
}

impl JoinRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.persist_batch_ops == Some(0)
            || self.idle_flush_ms == Some(0)
            || self.lsn_flush_ms == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.joins values must be positive",
            ));
        }
        Ok(())
    }
}

/// Native WebSocket gateway runtime settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketRuntimeConfig {
    /// HTTP/WebSocket listen address.
    #[serde(default)]
    pub listen: Option<String>,
    /// Realtime broker provider. Omit to retain legacy inference.
    #[serde(default)]
    pub provider: Option<RealtimeBrokerProvider>,
    /// NATS server URL.
    #[serde(default)]
    pub nats_url: Option<String>,
    /// Accepted NATS subject filters.
    #[serde(default)]
    pub subjects: Vec<String>,
    /// Per-connection mailbox depth.
    #[serde(default)]
    pub mailbox: Option<usize>,
    /// Ping interval in milliseconds.
    #[serde(default)]
    pub ping_interval_ms: Option<u64>,
    /// Pong timeout in milliseconds.
    #[serde(default)]
    pub pong_timeout_ms: Option<u64>,
    /// Max established connections per pod.
    #[serde(default)]
    pub max_connections: Option<usize>,
    /// JetStream settings. `None` preserves NATS Core mode.
    #[serde(default)]
    pub jetstream: Option<JetStreamRuntimeConfig>,
    /// Redis Streams settings.
    #[serde(default)]
    pub redis_streams: Option<RedisStreamsRuntimeConfig>,
}

impl WebSocketRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.mailbox == Some(0)
            || self.ping_interval_ms == Some(0)
            || self.pong_timeout_ms == Some(0)
            || self.max_connections == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.ws values must be positive",
            ));
        }
        validate_nonempty_list("runtime.ws.subjects", &self.subjects)?;
        if let Some(jetstream) = &self.jetstream {
            jetstream.validate()?;
        }
        if let Some(redis) = &self.redis_streams {
            redis.validate()?;
        }
        match self.provider {
            Some(RealtimeBrokerProvider::NatsCore)
                if self.jetstream.is_some() || self.redis_streams.is_some() =>
            {
                return Err(ConfigError::InvalidField(
                    "runtime.ws.provider=nats_core does not accept jetstream or redis_streams settings",
                ));
            }
            Some(RealtimeBrokerProvider::NatsJetstream) if self.redis_streams.is_some() => {
                return Err(ConfigError::InvalidField(
                    "runtime.ws.provider=nats_jetstream does not accept redis_streams settings",
                ));
            }
            Some(RealtimeBrokerProvider::RedisStreams) if self.jetstream.is_some() => {
                return Err(ConfigError::InvalidField(
                    "runtime.ws.provider=redis_streams does not accept jetstream settings",
                ));
            }
            None if self.jetstream.is_some() && self.redis_streams.is_some() => {
                return Err(ConfigError::InvalidField(
                    "runtime.ws.jetstream and runtime.ws.redis_streams are mutually exclusive",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Realtime broker selected by the native WebSocket gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealtimeBrokerProvider {
    /// Ephemeral NATS Core fan-out.
    NatsCore,
    /// Durable NATS JetStream sessions.
    NatsJetstream,
    /// Durable Redis Streams sessions.
    RedisStreams,
}

/// Redis Streams settings for either realtime gateway role.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisStreamsRuntimeConfig {
    /// Redis URL reference. Supports `redis://` and `rediss://` values.
    #[serde(default)]
    pub url_ref: Option<ValueRef>,
    /// Prefix used for per-tenant stream keys.
    #[serde(default)]
    pub key_prefix: Option<String>,
    /// Maximum records read from Redis per command.
    #[serde(default)]
    pub read_batch: Option<usize>,
    /// Blocking XREAD timeout in milliseconds.
    #[serde(default)]
    pub block_timeout_ms: Option<u64>,
    /// Local per-tenant fan-out capacity.
    #[serde(default)]
    pub broadcast_capacity: Option<usize>,
    /// Maximum per-process tenant tailers.
    #[serde(default)]
    pub max_tenant_hubs: Option<usize>,
    /// Target maximum retained entries per tenant stream.
    #[serde(default)]
    pub max_length: Option<usize>,
    /// Redis connection timeout in milliseconds.
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
    /// Redis command timeout and blocking-read response margin in milliseconds.
    #[serde(default)]
    pub response_timeout_ms: Option<u64>,
}

impl RedisStreamsRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(reference) = &self.url_ref {
            reference.validate()?;
        }
        if self.key_prefix.as_ref().is_some_and(|value| {
            value.is_empty()
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'_' | b'-'))
        }) {
            return Err(ConfigError::InvalidField(
                "runtime Redis Streams key_prefix is invalid",
            ));
        }
        if self.read_batch == Some(0)
            || self.block_timeout_ms == Some(0)
            || self.broadcast_capacity == Some(0)
            || self.max_tenant_hubs == Some(0)
            || self.max_length == Some(0)
            || self.connect_timeout_ms == Some(0)
            || self.response_timeout_ms == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.ws.redis_streams values must be positive",
            ));
        }
        if self.block_timeout_ms.is_some_and(|value| value > 30_000) {
            return Err(ConfigError::InvalidField(
                "runtime Redis Streams block_timeout_ms must not exceed 30000",
            ));
        }
        Ok(())
    }
}

/// Native WebSocket JetStream settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JetStreamRuntimeConfig {
    /// JetStream stream name.
    #[serde(default)]
    pub stream: Option<String>,
    /// Pod id used in generated consumer names.
    #[serde(default)]
    pub pod_id: Option<String>,
    /// Consumer inactive threshold in milliseconds.
    #[serde(default)]
    pub inactive_threshold_ms: Option<u64>,
    /// Orphan reaper cadence in milliseconds.
    #[serde(default)]
    pub reaper_interval_ms: Option<u64>,
    /// Stream replica count.
    #[serde(default)]
    pub replicas: Option<usize>,
    /// Stream storage backend.
    #[serde(default)]
    pub storage: Option<JetStreamStorage>,
    /// Stream max age in seconds.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    /// Stream max bytes.
    #[serde(default)]
    pub max_bytes: Option<i64>,
    /// Stream max messages.
    #[serde(default)]
    pub max_msgs: Option<i64>,
}

impl JetStreamRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.inactive_threshold_ms == Some(0)
            || self.reaper_interval_ms == Some(0)
            || self.replicas == Some(0)
            || self.max_age_secs == Some(0)
            || self.max_bytes == Some(0)
            || self.max_msgs == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.ws.jetstream values must be non-zero",
            ));
        }
        Ok(())
    }
}

/// JetStream storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JetStreamStorage {
    /// File-backed stream storage.
    File,
    /// Memory-backed stream storage.
    Memory,
}

/// GraphQL gateway runtime settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphqlRuntimeConfig {
    /// HTTP/WebSocket listen address.
    #[serde(default)]
    pub listen: Option<String>,
    /// Durable realtime provider. GraphQL does not support NATS Core mode.
    #[serde(default)]
    pub provider: Option<RealtimeBrokerProvider>,
    /// NATS server URL.
    #[serde(default)]
    pub nats_url: Option<String>,
    /// JetStream stream to consume.
    #[serde(default)]
    pub stream: Option<String>,
    /// Pod id used in generated consumer names.
    #[serde(default)]
    pub pod_id: Option<String>,
    /// Consumer inactive threshold in milliseconds.
    #[serde(default)]
    pub inactive_threshold_ms: Option<u64>,
    /// Orphan reaper cadence in milliseconds.
    #[serde(default)]
    pub reaper_interval_ms: Option<u64>,
    /// Per-connection broadcast capacity.
    #[serde(default)]
    pub broadcast_capacity: Option<usize>,
    /// Enable GraphiQL playground.
    #[serde(default)]
    pub playground: Option<bool>,
    /// Redis Streams settings.
    #[serde(default)]
    pub redis_streams: Option<RedisStreamsRuntimeConfig>,
}

impl GraphqlRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.inactive_threshold_ms == Some(0)
            || self.reaper_interval_ms == Some(0)
            || self.broadcast_capacity == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.graphql values must be positive",
            ));
        }
        if self.provider == Some(RealtimeBrokerProvider::NatsCore) {
            return Err(ConfigError::InvalidField(
                "runtime.graphql.provider does not support nats_core",
            ));
        }
        if let Some(redis) = &self.redis_streams {
            redis.validate()?;
        }
        if self.provider == Some(RealtimeBrokerProvider::NatsJetstream)
            && self.redis_streams.is_some()
        {
            return Err(ConfigError::InvalidField(
                "runtime.graphql.provider=nats_jetstream does not accept redis_streams settings",
            ));
        }
        Ok(())
    }
}

/// Optional admin HTTP server settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRuntimeConfig {
    /// Admin HTTP listen address. Unset disables the admin server.
    #[serde(default)]
    pub listen: Option<String>,
    /// Bearer token reference.
    #[serde(default)]
    pub token_ref: Option<ValueRef>,
}

impl AdminRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(listen) = &self.listen {
            validate_nonempty("runtime.admin.listen", listen)?;
        }
        if let Some(token_ref) = &self.token_ref {
            token_ref.validate()?;
        }
        Ok(())
    }
}

/// Read-only MCP server settings for the `mcp` role.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRuntimeConfig {
    /// MCP HTTP listen address (`host:port`).
    pub listen: String,
    /// Value reference to one full-access bearer token.
    #[serde(default)]
    pub auth_token_ref: Option<String>,
    /// Value reference to a per-token keys YAML document.
    #[serde(default)]
    pub keys_ref: Option<String>,
    /// Server-level target allowlist. Empty = every target.
    #[serde(default)]
    pub allow_targets: Vec<String>,
    /// Explicit target names for by_output_relation-routed sinks.
    #[serde(default)]
    pub targets: Vec<String>,
}

impl McpRuntimeConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        validate_nonempty("runtime.mcp.listen", &self.listen)?;
        let listen: std::net::SocketAddr = self
            .listen
            .parse()
            .map_err(|_| ConfigError::InvalidField("runtime.mcp.listen must be host:port"))?;
        if self.auth_token_ref.is_some() && self.keys_ref.is_some() {
            return Err(ConfigError::InvalidField(
                "runtime.mcp.auth_token_ref and runtime.mcp.keys_ref are mutually exclusive",
            ));
        }
        for reference in [&self.auth_token_ref, &self.keys_ref].into_iter().flatten() {
            ValueRef::parse(reference)?;
        }
        // Mirror of the HTTP exposure rule: no auth is dev-only, loopback.
        if self.auth_token_ref.is_none() && self.keys_ref.is_none() && !listen.ip().is_loopback() {
            return Err(ConfigError::InvalidField(
                "runtime.mcp requires auth_token_ref or keys_ref on a non-loopback listen",
            ));
        }
        for target in self.allow_targets.iter().chain(&self.targets) {
            if target.trim().is_empty() {
                return Err(ConfigError::InvalidField(
                    "runtime.mcp target names must be non-empty",
                ));
            }
        }
        Ok(())
    }
}

/// Logging output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable tracing fmt output.
    Pretty,
    /// JSON tracing fmt output.
    Json,
}

impl DispatchConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_events == Some(0)
            || self.max_batch_bytes == Some(0)
            || self.flush_ms == Some(0)
            || self.parallel_bulks == Some(0)
        {
            return Err(ConfigError::InvalidField(
                "runtime.dispatch values must be positive",
            ));
        }
        Ok(())
    }
}

/// Reference to a value supplied by the deployment environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef {
    /// Read the value from an environment variable.
    Env(String),
    /// Read the value from a deployment-mounted file.
    File(PathBuf),
}

impl ValueRef {
    /// Parse an environment or mounted-file value reference.
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        if let Some(name) = value.strip_prefix("env:") {
            validate_env_name(name)?;
            return Ok(Self::Env(name.to_owned()));
        }
        if let Some(path) = value.strip_prefix("file:") {
            if path.is_empty() {
                return Err(ConfigError::InvalidReference(value.to_owned()));
            }
            return Ok(Self::File(PathBuf::from(path)));
        }
        Err(ConfigError::InvalidReference(value.to_owned()))
    }

    /// Return the reference as a displayable string.
    pub fn as_str(&self) -> String {
        match self {
            Self::Env(name) => format!("env:{name}"),
            Self::File(path) => format!("file:{}", path.display()),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Env(name) => validate_env_name(name),
            Self::File(path) if path.as_os_str().is_empty() => {
                Err(ConfigError::InvalidReference("file:".to_owned()))
            }
            Self::File(_) => Ok(()),
        }
    }
}

impl<'de> Deserialize<'de> for ValueRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// Errors returned by config parsing and validation.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// YAML could not be decoded into the config schema.
    #[error("invalid engine config YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// Unsupported schema version.
    #[error("unsupported engine config schema_version {0}")]
    UnsupportedSchemaVersion(u64),
    /// A semantic field rule failed.
    #[error("invalid engine config: {0}")]
    InvalidField(&'static str),
    /// A value reference is not supported.
    #[error("invalid value reference {0:?}; expected env:NAME or file:/path")]
    InvalidReference(String),
}

fn default_roles() -> Vec<Role> {
    vec![Role::Cdc]
}

fn validate_nonempty(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidField(field));
    }
    Ok(())
}

fn validate_value_or_ref(
    field: &'static str,
    value: Option<&String>,
    reference: Option<&ValueRef>,
) -> Result<(), ConfigError> {
    if value.is_some() && reference.is_some() {
        return Err(ConfigError::InvalidField(
            "direct values and *_ref values are mutually exclusive",
        ));
    }
    if let Some(value) = value {
        validate_nonempty(field, value)?;
    }
    if let Some(reference) = reference {
        reference.validate()?;
    }
    Ok(())
}

fn validate_nonempty_list(field: &'static str, values: &[String]) -> Result<(), ConfigError> {
    for value in values {
        validate_nonempty(field, value)?;
    }
    Ok(())
}

fn validate_env_name(name: &str) -> Result<(), ConfigError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ConfigError::InvalidField(
            "value references must use non-empty uppercase env names",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    #[test]
    fn managed_block_alone_is_valid() {
        let config = EngineConfig::from_yaml_str(
            "schema_version: 1\nmanaged:\n  agent_key_ref: env:VS_AGENT_KEY\n",
        )
        .expect("managed-only config is valid");
        assert!(config.managed.is_some());
    }

    #[test]
    fn managed_with_local_pipeline_sections_is_rejected() {
        let with_source = "schema_version: 1\nmanaged:\n  agent_key_ref: env:VS_AGENT_KEY\nroles: [cdc]\nsource:\n  kind: postgres\n  postgres:\n    host: localhost\nsink:\n  kind: opensearch\n  opensearch:\n    endpoint_ref: env:VS_OS_ENDPOINT\n";
        let err = EngineConfig::from_yaml_str(with_source).expect_err("must reject");
        assert!(err.to_string().contains("control plane"), "got {err}");

        let with_specs = "schema_version: 1\nmanaged:\n  agent_key_ref: env:VS_AGENT_KEY\nspecs:\n  joins: joins.yaml\n";
        let err = EngineConfig::from_yaml_str(with_specs).expect_err("must reject");
        assert!(err.to_string().contains("specs"), "got {err}");
    }

    #[test]
    fn managed_gateway_urls_must_be_tls() {
        let err = EngineConfig::from_yaml_str(
            "schema_version: 1\nmanaged:\n  agent_key_ref: env:VS_AGENT_KEY\n  gateway_url: http://gateway.example.com\n",
        )
        .expect_err("must reject plain http");
        assert!(err.to_string().contains("https"), "got {err}");
    }

    use super::*;

    #[test]
    fn parses_opensearch_output_relation_routing() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    publication: ventstream_pub
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: by_output_relation
specs:
  joins: ./joins.yaml
"#,
        )?;

        let sink = config
            .sink
            .ok_or(ConfigError::InvalidField("sink should exist"))?;
        let opensearch = sink
            .opensearch
            .ok_or(ConfigError::InvalidField("opensearch should exist"))?;
        let postgres = config
            .source
            .and_then(|source| source.postgres)
            .ok_or(ConfigError::InvalidField("postgres settings should exist"))?;
        assert_eq!(postgres.publication.as_deref(), Some("ventstream_pub"));
        assert_eq!(
            opensearch.index_routing.as_legacy_template(),
            "${header:ventstream.cdc.relation}"
        );
        Ok(())
    }

    #[test]
    fn parses_projection_target_routing_and_basic_auth_refs() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    auth:
      mode: basic
      username_ref: env:VS_OS_USERNAME
      password_ref: env:VS_OS_PASSWORD
    index_routing:
      strategy: by_projection_target
specs:
  joins: joins.yaml
"#,
        )?;

        let opensearch = config
            .sink
            .and_then(|sink| sink.opensearch)
            .ok_or(ConfigError::InvalidField("opensearch should exist"))?;
        assert_eq!(
            opensearch.index_routing.as_legacy_template(),
            "${header:ventstream.target.index}"
        );
        Ok(())
    }

    #[test]
    fn rejects_projection_target_routing_for_sources_without_projection_headers() {
        let result = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: shop
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: by_projection_target
specs:
  joins: joins.yaml
"#,
        );
        assert!(matches!(
            result,
            Err(ConfigError::InvalidField(
                "by_projection_target routing is supported only for postgres and mysql sources"
            ))
        ));
    }

    #[test]
    fn rejects_literal_refs_and_missing_cdc_source() {
        assert!(
            EngineConfig::from_yaml_str("schema_version: 1\nsink:\n  kind: opensearch\n").is_err()
        );
        assert!(ValueRef::parse("https://search.example.com").is_err());
        assert!(ValueRef::parse("env:lowercase").is_err());
        assert_eq!(
            ValueRef::parse("file:/run/secrets/redis-password")
                .expect("mounted file reference should parse"),
            ValueRef::File(PathBuf::from("/run/secrets/redis-password"))
        );
        assert!(ValueRef::parse("file:").is_err());
    }

    #[test]
    fn parses_source_runtime_settings() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: neo4j
  neo4j:
    uri_ref: env:VS_NEO4J_URI
    user: neo4j
    password_ref: env:VS_NEO4J_PASSWORD
    database: graph
    namespace: catalog
    state_dir: /var/lib/ventstream/neo4j
    poll_interval_ms: 250
    idle_advance_after_polls: 10
    label_tables:
      Product: products
    reltype_tables:
      SUPPLIED_BY: supplied_by
    label_filter: [Product]
    reltype_filter: [SUPPLIED_BY]
    label_priority: [Product, CatalogItem]
    bootstrap:
      mode: snapshot
      chunk_size: 500
    recompose_chunk: 64
    recompose_concurrency: 4
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
runtime:
  health_listen: 0.0.0.0:4043
  bus_capacity: 2048
  dlq_path: /var/lib/ventstream/dlq.jsonl
  dispatch:
    max_events: 1000
    max_batch_bytes: 1048576
    flush_ms: 250
    parallel_bulks: 2
"#,
        )?;

        let neo4j = config
            .source
            .and_then(|source| source.neo4j)
            .ok_or(ConfigError::InvalidField("neo4j settings should exist"))?;
        assert_eq!(neo4j.user.as_deref(), Some("neo4j"));
        assert_eq!(neo4j.namespace.as_deref(), Some("catalog"));
        assert_eq!(neo4j.bootstrap.mode, Some(BootstrapMode::Snapshot));
        assert_eq!(config.runtime.bus_capacity, Some(2048));
        assert_eq!(config.runtime.dispatch.parallel_bulks, Some(2));
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_source_values_and_zero_runtime() {
        let ambiguous = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
  postgres:
    host: localhost
    host_ref: env:VS_PG_HOST
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(ambiguous.is_err());

        let zero_runtime = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
runtime:
  bus_capacity: 0
"#,
        );
        assert!(zero_runtime.is_err());

        let zero_fetch_pool = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
  postgres:
    related_fetch_pool_size: 0
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(zero_fetch_pool.is_err());
    }

    #[test]
    fn parses_all_source_and_realtime_runtime_blocks() -> Result<(), ConfigError> {
        let mongo = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: shop
    namespace: commerce
    collections: [orders, customers]
    full_document: default
    bootstrap:
      mode: snapshot
      chunk_size: 250
    token_flush_ms: 500
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        )?;
        assert_eq!(
            mongo
                .source
                .and_then(|source| source.mongodb)
                .and_then(|mongodb| mongodb.token_flush_ms),
            Some(500)
        );

        let mysql = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mysql
  mysql:
    host_ref: env:VS_MYSQL_HOST
    user: repl
    password_ref: env:VS_MYSQL_PASSWORD
    database: shop
    tables: [orders]
    denormalize_mode: sql
    sink_reverse_lookup: false
    recompose_chunk: 128
    recompose_concurrency: 6
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        )?;
        let mysql = mysql
            .source
            .and_then(|source| source.mysql)
            .ok_or(ConfigError::InvalidField("mysql settings should exist"))?;
        assert_eq!(mysql.denormalize_mode, Some(SqlDenormalizeMode::Sql));
        assert_eq!(mysql.recompose_chunk, Some(128));
        assert_eq!(mysql.recompose_concurrency, Some(6));

        let realtime = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws, graphql]
runtime:
  tenant: tenant_a
  log_format: json
  ws:
    listen: 0.0.0.0:4040
    nats_url: nats://nats:4222
    subjects: [vs.t.tenant_a.>]
    mailbox: 512
    jetstream:
      stream: ventstream
      storage: memory
      max_age_secs: 60
      max_bytes: 1048576
      max_msgs: -1
  graphql:
    listen: 0.0.0.0:4041
    nats_url: nats://nats:4222
    stream: ventstream
    broadcast_capacity: 2048
    playground: true
"#,
        )?;
        assert_eq!(realtime.runtime.log_format, Some(LogFormat::Json));
        assert_eq!(
            realtime
                .runtime
                .ws
                .jetstream
                .and_then(|jetstream| jetstream.storage),
            Some(JetStreamStorage::Memory)
        );
        Ok(())
    }

    #[test]
    fn rejects_secret_like_inline_and_bad_runtime_values() {
        let inline_secret = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: kafka
  kafka:
    brokers: localhost:9092
    topics: [orders]
  sasl_password: nope
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(inline_secret.is_err());

        let bad_runtime = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  ws:
    mailbox: 0
"#,
        );
        assert!(bad_runtime.is_err());
    }

    #[test]
    fn parses_redis_streams_websocket_provider() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  tenant: tenant_a
  ws:
    provider: redis_streams
    listen: 0.0.0.0:4040
    mailbox: 512
    redis_streams:
      url_ref: env:VS_REDIS_URL
      key_prefix: ventstream
      read_batch: 128
      block_timeout_ms: 1000
      broadcast_capacity: 4096
      connect_timeout_ms: 2000
      response_timeout_ms: 3000
"#,
        )?;
        let ws = config.runtime.ws;
        assert_eq!(ws.provider, Some(RealtimeBrokerProvider::RedisStreams));
        let redis = ws.redis_streams.ok_or(ConfigError::InvalidField(
            "Redis configuration should exist",
        ))?;
        assert_eq!(
            redis.url_ref,
            Some(ValueRef::Env("VS_REDIS_URL".to_owned()))
        );
        assert_eq!(redis.read_batch, Some(128));
        assert_eq!(redis.broadcast_capacity, Some(4096));
        Ok(())
    }

    #[test]
    fn parses_shared_redis_provider_for_both_gateways() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws, graphql]
runtime:
  tenant: tenant_a
  realtime:
    provider: redis_streams
    redis_streams:
      url_ref: env:VS_REDIS_URL
      key_prefix: ventstream
      broadcast_capacity: 4096
  ws:
    listen: 0.0.0.0:4040
  graphql:
    listen: 0.0.0.0:4041
"#,
        )?;
        assert_eq!(
            config.runtime.realtime.provider,
            Some(RealtimeBrokerProvider::RedisStreams)
        );
        assert!(config.runtime.realtime.redis_streams.is_some());
        Ok(())
    }

    #[test]
    fn rejects_ambiguous_realtime_broker_configuration() {
        let mixed = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  ws:
    jetstream: {}
    redis_streams:
      url_ref: env:VS_REDIS_URL
"#,
        );
        assert!(mixed.is_err());

        let provider_mismatch = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  ws:
    provider: nats_core
    redis_streams:
      url_ref: env:VS_REDIS_URL
"#,
        );
        assert!(provider_mismatch.is_err());

        let inline_url = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  ws:
    provider: redis_streams
    redis_streams:
      url_ref: redis://user:secret@redis:6379
"#,
        );
        assert!(inline_url.is_err());

        let shared_conflict = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws, graphql]
runtime:
  realtime:
    provider: redis_streams
    redis_streams:
      url_ref: env:VS_REDIS_URL
  ws:
    provider: nats_jetstream
"#,
        );
        assert!(shared_conflict.is_err());

        let shared_nats_with_role_redis = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  realtime:
    provider: nats_core
  ws:
    redis_streams:
      url_ref: env:VS_REDIS_URL
"#,
        );
        assert!(shared_nats_with_role_redis.is_err());

        let inferred_shared_redis_with_jetstream = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  realtime:
    redis_streams:
      url_ref: env:VS_REDIS_URL
  ws:
    jetstream: {}
"#,
        );
        assert!(inferred_shared_redis_with_jetstream.is_err());

        let unsafe_redis_runtime = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  ws:
    provider: redis_streams
    redis_streams:
      url_ref: env:VS_REDIS_URL
      key_prefix: "bad{prefix}"
      block_timeout_ms: 30001
"#,
        );
        assert!(unsafe_redis_runtime.is_err());
    }

    #[test]
    fn rejects_unknown_mismatched_and_unsupported_configuration() {
        let unknown_source_field = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
  postgres:
    publcation: ventstream_pub
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(unknown_source_field.is_err());

        let mismatched_connector = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
  mysql:
    database: shop
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(mismatched_connector.is_err());

        let unsupported_routes = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: routes
      routes:
        - output: orders
          index: orders_v1
"#,
        );
        assert!(unsupported_routes.is_err());

        let projection_routing_without_specs = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
source:
  kind: postgres
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: by_projection_target
"#,
        );
        assert!(projection_routing_without_specs.is_err());

        let duplicate_role = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws, ws]
"#,
        );
        assert!(duplicate_role.is_err());

        let inactive_cdc_settings = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
source:
  kind: postgres
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
"#,
        );
        assert!(inactive_cdc_settings.is_err());
    }

    #[test]
    fn validates_adaptive_memory_settings() -> Result<(), ConfigError> {
        let valid = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  memory:
    enabled: true
    budget_bytes: 268435456
    max_event_bytes: 33554432
    sample_ms: 250
    recovery_ms: 1000
    target_percent: 65
    high_percent: 75
    critical_percent: 85
    hysteresis_percent: 5
"#,
        )?;
        assert_eq!(valid.runtime.memory.budget_bytes, Some(268_435_456));
        assert_eq!(valid.runtime.memory.recovery_ms, Some(1000));

        let invalid_thresholds = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  memory:
    target_percent: 90
    high_percent: 80
"#,
        );
        assert!(invalid_thresholds.is_err());

        let invalid_recovery = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  memory:
    recovery_ms: 0
"#,
        );
        assert!(invalid_recovery.is_err());

        let invalid_headroom = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [ws]
runtime:
  memory:
    budget_bytes: 67108864
    max_event_bytes: 33554432
"#,
        );
        assert!(invalid_headroom.is_err());
        Ok(())
    }

    #[test]
    fn tls_blocks_default_to_strict_verification() {
        let tls: TlsConfig = serde_yaml::from_str("{}").expect("parse TLS defaults");
        assert_eq!(tls.mode, TlsMode::VerifyFull);
        assert!(tls.trust.is_none());

        let tls: TlsConfig =
            serde_yaml::from_str("mode: verify_full\nca_file: /run/secrets/database-ca.pem\n")
                .expect("parse strict TLS");
        assert_eq!(tls.mode, TlsMode::VerifyFull);
        assert_eq!(
            tls.ca_file.as_deref(),
            Some(std::path::Path::new("/run/secrets/database-ca.pem"))
        );

        let tls: TlsConfig =
            serde_yaml::from_str("mode: verify_full\ntrust:\n  provider: aws_rds\n")
                .expect("parse provider trust");
        assert_eq!(
            tls.trust.as_ref().map(|trust| trust.provider),
            Some(TlsTrustProvider::AwsRds)
        );
    }

    #[test]
    fn rejects_weak_or_contradictory_tls_settings() {
        assert!(serde_yaml::from_str::<TlsConfig>("mode: require\n").is_err());

        let disabled_with_ca: TlsConfig =
            serde_yaml::from_str("mode: disabled\nca_file: /tmp/ca.pem\n")
                .expect("deserialize before semantic validation");
        assert!(disabled_with_ca.validate("source.postgres.tls").is_err());

        let conflicting_trust: TlsConfig = serde_yaml::from_str(
            "mode: verify_full\nca_file: /tmp/ca.pem\ntrust:\n  provider: aws_rds\n",
        )
        .expect("deserialize before semantic validation");
        assert!(conflicting_trust.validate("source.postgres.tls").is_err());

        let unsupported_provider: TlsConfig =
            serde_yaml::from_str("mode: verify_full\ntrust:\n  provider: aws_rds\n")
                .expect("deserialize before semantic validation");
        assert!(unsupported_provider.validate("source.mongodb.tls").is_err());

        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
    tls: {}
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    insecure_tls: true
    tls: {}
"#,
        );
        assert!(config.is_err());
    }

    #[test]
    fn aws_rds_trust_is_accepted_for_postgres_and_mysql() {
        for yaml in [
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    database_ref: env:VS_PG_DATABASE
    publication: app_publication
    slot: app_slot
    tls:
      mode: verify_full
      trust:
        provider: aws_rds
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: fixed
      name: app
"#,
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mysql
  mysql:
    host_ref: env:VS_MYSQL_HOST
    database_ref: env:VS_MYSQL_DATABASE
    tls:
      mode: verify_full
      trust:
        provider: aws_rds
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: fixed
      name: app
"#,
        ] {
            EngineConfig::from_yaml_str(yaml).expect("accept AWS RDS provider trust");
        }
    }

    #[test]
    fn parses_surrealdb_graph_edges_and_rejects_bad_specs() {
        let base = |edges: &str, routing: &str| {
            format!(
                r#"
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    host_ref: env:H
    user_ref: env:U
    password_ref: env:P
    database_ref: env:D
    publication_ref: env:PB
    slot_ref: env:SL
sink:
  kind: surrealdb
  surrealdb:
    endpoint_ref: env:SE
    namespace: vs
    database: app
    username_ref: env:SU
    password_ref: env:SP
{routing}
    graph_edges:
{edges}
"#
            )
        };
        let good = base(
            "      - name: wrote\n        from_table: articles\n        fk_columns: [author_id]\n        to_table: people\n        reversed: true",
            "",
        );
        let config = EngineConfig::from_yaml_str(&good).expect("valid graph edges");
        let surreal = config
            .sink
            .as_ref()
            .and_then(|sink| sink.surrealdb.as_ref())
            .expect("surreal sink");
        let edge = surreal.graph_edges.first().expect("one edge spec");
        assert_eq!(edge.name, "wrote");
        assert_eq!(edge.reversed, Some(true));

        // Edge names embed in RELATE statements: bad charset rejected.
        let bad_name = base(
            "      - name: \"wr ote\"\n        from_table: articles\n        fk_columns: [author_id]\n        to_table: people",
            "",
        );
        EngineConfig::from_yaml_str(&bad_name).expect_err("space in edge name must reject");

        // Duplicate edge names collide on one edge table.
        let dup = base(
            "      - name: wrote\n        from_table: articles\n        fk_columns: [author_id]\n        to_table: people\n      - name: wrote\n        from_table: comments\n        fk_columns: [author_id]\n        to_table: people",
            "",
        );
        EngineConfig::from_yaml_str(&dup).expect_err("duplicate edge names must reject");

        // Fixed routing cannot host resolvable RELATE endpoints.
        let fixed = base(
            "      - name: wrote\n        from_table: articles\n        fk_columns: [author_id]\n        to_table: people",
            "    table_routing:\n      mode: fixed\n      table: everything",
        );
        EngineConfig::from_yaml_str(&fixed).expect_err("fixed routing + edges must reject");

        // Empty fk_columns declares no relationship.
        let empty = base(
            "      - name: wrote\n        from_table: articles\n        fk_columns: []\n        to_table: people",
            "",
        );
        EngineConfig::from_yaml_str(&empty).expect_err("empty fk_columns must reject");
    }

    #[test]
    fn parses_redis_materialized_view_sink() {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
    collections: [orders]
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    auth:
      mode: acl
      username_ref: env:VS_REDIS_USERNAME
      password_ref: env:VS_REDIS_PASSWORD
    tls:
      ca_file: /run/secrets/redis-ca.pem
      client_cert_file: /run/secrets/redis-client.crt
      client_key_file: /run/secrets/redis-client.key
    keyspace:
      prefix: ventstream:example:production
      ownership: exclusive
      routing:
        strategy: by_output_relation
    document:
      format: json
    contract:
      mode: materialized_view
    acknowledgement:
      mode: replicated
      replicas: 1
      timeout_ms: 1000
"#,
        )
        .expect("parse Redis sink");
        let sink = config.sink.expect("sink");
        assert_eq!(sink.kind, SinkKind::Redis);
        let redis = sink.redis.expect("Redis settings");
        assert_eq!(redis.document.format, RedisDocumentFormatConfig::Json);
        assert!(matches!(
            redis.keyspace.routing,
            RedisKeyRoutingConfig::ByOutputRelation
        ));
        assert_eq!(
            redis.keyspace.ownership,
            RedisKeyspaceOwnershipConfig::Exclusive
        );
        assert!(matches!(
            redis.acknowledgement,
            RedisAcknowledgementConfig::Replicated {
                replicas: 1,
                timeout_ms: 1000
            }
        ));
        let tls = redis.tls.expect("Redis TLS settings");
        assert_eq!(
            tls.ca_file.as_deref(),
            Some(std::path::Path::new("/run/secrets/redis-ca.pem"))
        );
    }

    #[test]
    fn parses_redis_aof_acknowledgement_with_local_default() {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source: { kind: mongodb }
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    acknowledgement:
      mode: aof
      replicas: 1
      timeout_ms: 2000
"#,
        )
        .expect("parse Redis AOF acknowledgement");
        let acknowledgement = config
            .sink
            .and_then(|sink| sink.redis)
            .map(|redis| redis.acknowledgement)
            .expect("Redis acknowledgement");
        assert!(matches!(
            acknowledgement,
            RedisAcknowledgementConfig::Aof {
                local: true,
                replicas: 1,
                timeout_ms: 2000
            }
        ));
    }

    #[test]
    fn parses_redis_sentinel_and_cluster_topologies() {
        let sentinel = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source: { kind: mongodb }
sink:
  kind: redis
  redis:
    topology:
      mode: sentinel
      service_name: cache-primary
      endpoints:
        - env:VS_REDIS_SENTINEL_1
        - env:VS_REDIS_SENTINEL_2
      data_node_tls: true
      sentinel_auth:
        mode: password
        password_ref: env:VS_REDIS_SENTINEL_PASSWORD
      sentinel_tls:
        ca_file: /run/secrets/sentinel-ca.pem
    auth:
      mode: acl
      username_ref: env:VS_REDIS_USERNAME
      password_ref: env:VS_REDIS_PASSWORD
    keyspace:
      prefix: ventstream:sentinel
      routing: { strategy: fixed, name: orders }
"#,
        )
        .expect("parse Redis Sentinel topology");
        let sentinel_redis = sentinel
            .sink
            .and_then(|sink| sink.redis)
            .expect("Sentinel Redis settings");
        assert!(matches!(
            sentinel_redis.topology,
            Some(RedisTopologyConfig::Sentinel {
                data_node_tls: true,
                ..
            })
        ));

        let cluster = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source: { kind: mongodb }
sink:
  kind: redis
  redis:
    topology:
      mode: cluster
      endpoints: [env:VS_REDIS_CLUSTER_1, env:VS_REDIS_CLUSTER_2]
    keyspace:
      prefix: ventstream:cluster
      routing: { strategy: by_output_relation }
"#,
        )
        .expect("parse Redis Cluster topology");
        let cluster_redis = cluster
            .sink
            .and_then(|sink| sink.redis)
            .expect("Cluster Redis settings");
        assert!(matches!(
            cluster_redis.topology,
            Some(RedisTopologyConfig::Cluster { .. })
        ));
    }

    #[test]
    fn rejects_ambiguous_redis_topology_and_cross_slot_views() {
        for yaml in [
            r#"
schema_version: 1
roles: [cdc]
source: { kind: mongodb }
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    topology:
      mode: cluster
      endpoints: [env:VS_REDIS_CLUSTER_1]
    keyspace:
      prefix: ventstream:cluster
      routing: { strategy: by_output_relation }
"#,
            r#"
schema_version: 1
roles: [cdc]
source: { kind: mongodb }
sink:
  kind: redis
  redis:
    topology:
      mode: cluster
      endpoints: [env:VS_REDIS_CLUSTER_1]
    keyspace:
      prefix: ventstream:cluster
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: first
            source: { relation: orders }
            key: { template: "first:${doc_id}" }
          - name: second
            source: { relation: orders }
            key: { template: "second:${doc_id}" }
"#,
        ] {
            assert!(EngineConfig::from_yaml_str(yaml).is_err());
        }
    }

    #[test]
    fn parses_bounded_redis_lookup_views() {
        let config = EngineConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
    collections: [orders]
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: ventstream:app
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: open_order_by_id
            source:
              namespace: app
              relation: orders
            key:
              template: "order:${json:/id}"
              on_missing: block
            filter:
              mode: all
              conditions:
                - path: /status
                  operator: in
                  values: [pending, processing]
                - path: /deleted_at
                  operator: not_exists
            value:
              mode: fields
              fields:
                id: /id
                status: /status
"#,
        )
        .expect("parse Redis views");

        let redis = config
            .sink
            .and_then(|sink| sink.redis)
            .expect("Redis settings");
        let views = match redis.keyspace.routing {
            RedisKeyRoutingConfig::Views { views } => views,
            _ => Vec::new(),
        };
        assert_eq!(views.len(), 1);
        let Some(view) = views.first() else {
            return;
        };
        assert_eq!(view.name, "open_order_by_id");
        assert!(matches!(view.value, RedisViewValueConfig::Fields { .. }));
    }

    #[test]
    fn rejects_unsafe_redis_lookup_views() {
        for yaml in [
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: ventstream:app
      routing:
        strategy: views
        views:
          - name: orders
            source: { relation: orders }
            key: { template: "order:${doc_id}" }
"#,
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: ventstream:app
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: orders
            source:
              relation: orders
              projection_target: order_projection
            key: { template: "order:${doc_id}" }
"#,
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: ventstream:app
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: orders
            source: { relation: orders }
            key: { template: "constant" }
"#,
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: app
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: ventstream:app
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: orders
            source: { relation: orders }
            key: { template: "order:${doc_id}" }
            filter:
              conditions:
                - path: /status~
                  operator: equals
                  value: { unsafe: object }
"#,
        ] {
            assert!(
                EngineConfig::from_yaml_str(yaml).is_err(),
                "unsafe Redis view was accepted:\n{yaml}"
            );
        }
    }

    #[test]
    fn rejects_ambiguous_or_unbounded_redis_sink_settings() {
        for yaml in [
            r#"
schema_version: 1
sink:
  kind: redis
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    contract:
      mode: cache
      ttl_ms: 0
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    acknowledgement:
      mode: replicated
      replicas: 0
      timeout_ms: 1000
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    tls:
      client_cert_file: /run/secrets/redis-client.crt
    keyspace:
      prefix: app
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: " app "
      routing:
        strategy: fixed
        name: " orders "
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    max_batch_bytes: 67108865
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    acknowledgement:
      mode: replicated
      replicas: 1
      timeout_ms: 2000
    response_timeout_ms: 1000
"#,
            r#"
schema_version: 1
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_URL
    keyspace:
      prefix: app
    acknowledgement:
      mode: aof
      local: false
      replicas: 0
      timeout_ms: 1000
"#,
        ] {
            assert!(EngineConfig::from_yaml_str(yaml).is_err());
        }
    }

    const MCP_SINK_YAML: &str = "sink:\n  kind: opensearch\n  opensearch:\n    endpoint_ref: env:VS_OS_ENDPOINT\n    index_routing:\n      strategy: fixed\n      name: orders\n";

    #[test]
    fn parses_mcp_role_with_runtime_block() -> Result<(), ConfigError> {
        let config = EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n    keys_ref: env:VS_MCP_KEYS\n    allow_targets: [orders]\n    targets: []\n"
        ))?;
        assert_eq!(config.roles, vec![Role::Mcp]);
        let mcp = config.runtime.mcp.expect("mcp runtime");
        assert_eq!(mcp.listen, "127.0.0.1:8790");
        assert_eq!(mcp.keys_ref.as_deref(), Some("env:VS_MCP_KEYS"));
        assert_eq!(mcp.allow_targets, ["orders"]);
        // Loopback listen with no auth is dev-allowed.
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n"
        ))
        .is_ok());
        Ok(())
    }

    #[test]
    fn mcp_role_coupling_is_validated() {
        // mcp must run alone (never with the singleton cdc writer).
        for roles in ["[mcp, cdc]", "[mcp, ws]", "[mcp, graphql]"] {
            assert!(EngineConfig::from_yaml_str(&format!(
                "schema_version: 1\nroles: {roles}\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n"
            ))
            .is_err());
        }
        // runtime.mcp without the role.
        assert!(EngineConfig::from_yaml_str(
            "schema_version: 1\nroles: [ws]\nruntime:\n  mcp:\n    listen: 127.0.0.1:8790\n"
        )
        .is_err());
        // Role without runtime.mcp, and role without sink.
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}"
        ))
        .is_err());
        assert!(EngineConfig::from_yaml_str(
            "schema_version: 1\nroles: [mcp]\nruntime:\n  mcp:\n    listen: 127.0.0.1:8790\n"
        )
        .is_err());
        // Both auth refs are mutually exclusive.
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n    auth_token_ref: env:VS_A\n    keys_ref: env:VS_B\n"
        ))
        .is_err());
        // Non-loopback listen requires auth; with auth it passes.
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 0.0.0.0:8790\n"
        ))
        .is_err());
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 0.0.0.0:8790\n    auth_token_ref: env:VS_MCP_TOKEN\n"
        ))
        .is_ok());
        // Bad ref syntax and empty target names are rejected.
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n    keys_ref: notaref\n"
        ))
        .is_err());
        assert!(EngineConfig::from_yaml_str(&format!(
            "schema_version: 1\nroles: [mcp]\n{MCP_SINK_YAML}runtime:\n  mcp:\n    listen: 127.0.0.1:8790\n    allow_targets: [\" \"]\n"
        ))
        .is_err());
    }
}
