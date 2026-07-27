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
}

impl TlsConfig {
    fn validate(&self, field: &'static str) -> Result<(), ConfigError> {
        if self.mode == TlsMode::Disabled && self.ca_file.is_some() {
            return Err(ConfigError::InvalidField(match field {
                "source.postgres.tls" => "source.postgres.tls.ca_file requires mode=verify_full",
                "source.mysql.tls" => "source.mysql.tls.ca_file requires mode=verify_full",
                "source.mongodb.tls" => "source.mongodb.tls.ca_file requires mode=verify_full",
                "source.neo4j.tls" => "source.neo4j.tls.ca_file requires mode=verify_full",
                "sink.opensearch.tls" => "sink.opensearch.tls.ca_file requires mode=verify_full",
                _ => "tls.ca_file requires mode=verify_full",
            }));
        }
        Ok(())
    }
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

/// Parsed canonical engine configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Schema version. V1 is the only supported version.
    pub schema_version: u64,
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
        if !self.roles.contains(&Role::Cdc) && (self.source.is_some() || self.sink.is_some()) {
            return Err(ConfigError::InvalidField(
                "source and sink require the cdc role",
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
            });
            if by_projection_target {
                if self.specs.joins.is_none() {
                    return Err(ConfigError::InvalidField(
                        "by_projection_target routing requires specs.joins",
                    ));
                }
                if !self.source.as_ref().is_some_and(|source| {
                    matches!(source.kind, SourceKind::Postgres | SourceKind::Mysql)
                }) {
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
}

impl SinkConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self.kind {
            SinkKind::Opensearch | SinkKind::Elasticsearch => {
                let Some(opensearch) = &self.opensearch else {
                    return Err(ConfigError::InvalidField(
                        "sink.opensearch is required for opensearch/elasticsearch sinks",
                    ));
                };
                opensearch.validate()
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

impl SpecFiles {
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
}

impl ValueRef {
    /// Parse a value reference such as `env:VS_OS_ENDPOINT`.
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        let Some(name) = value.strip_prefix("env:") else {
            return Err(ConfigError::InvalidReference(value.to_owned()));
        };
        validate_env_name(name)?;
        Ok(Self::Env(name.to_owned()))
    }

    /// Return the reference as a displayable string.
    pub fn as_str(&self) -> String {
        match self {
            Self::Env(name) => format!("env:{name}"),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Env(name) => validate_env_name(name),
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
    #[error("invalid value reference {0:?}; expected env:NAME")]
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

        let tls: TlsConfig =
            serde_yaml::from_str("mode: verify_full\nca_file: /run/secrets/database-ca.pem\n")
                .expect("parse strict TLS");
        assert_eq!(tls.mode, TlsMode::VerifyFull);
        assert_eq!(
            tls.ca_file.as_deref(),
            Some(std::path::Path::new("/run/secrets/database-ca.pem"))
        );
    }

    #[test]
    fn rejects_weak_or_contradictory_tls_settings() {
        assert!(serde_yaml::from_str::<TlsConfig>("mode: require\n").is_err());

        let disabled_with_ca: TlsConfig =
            serde_yaml::from_str("mode: disabled\nca_file: /tmp/ca.pem\n")
                .expect("deserialize before semantic validation");
        assert!(disabled_with_ca.validate("source.postgres.tls").is_err());

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
}
