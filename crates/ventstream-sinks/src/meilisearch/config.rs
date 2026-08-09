//! Configuration for the Meilisearch sink.

use std::path::PathBuf;
use std::time::Duration;

use ventstream_core::SinkHealth;

use crate::opensearch::RetryConfig;

/// Top-level sink configuration.
#[derive(Debug, Clone)]
pub struct MeilisearchConfig {
    /// Sink identifier matching the pipeline's sink `id`.
    pub id: String,

    /// Instance base URL — e.g. `https://search.example.com:7700`.
    pub endpoint: String,

    /// API key sent as `Authorization: Bearer <key>`. Use a scoped key,
    /// never the master key.
    pub api_key: Option<String>,

    /// Per-event index routing.
    pub index_routing: MeilisearchIndexRouting,

    /// Prefix prepended to routed index UIDs. Must match Meilisearch's
    /// `[A-Za-z0-9_-]` charset.
    pub index_prefix: String,

    /// Create missing indexes implicitly on first write. When false, the
    /// startup probe requires every fixed index to already exist.
    pub auto_create_indexes: bool,

    /// Document attribute holding the encoded primary key.
    pub primary_key_field: String,

    /// Batching limits shared with the dispatcher.
    pub batching: MeilisearchBatchConfig,

    /// Retry policy for transient failures.
    pub retry: RetryConfig,

    /// Task-completion polling policy.
    pub task: MeilisearchTaskConfig,

    /// Per-request HTTP timeout.
    pub request_timeout: Duration,

    /// When false, accept self-signed TLS certificates. Dev-only.
    pub verify_tls: bool,

    /// Optional PEM CA bundle for a private certificate authority.
    pub ca_file: Option<PathBuf>,

    /// Declared index settings, applied and verified at startup for fixed
    /// routing. Settings not declared here are never touched.
    pub settings: Option<MeilisearchSettings>,

    /// Process-local availability state shared with the health server.
    #[doc(hidden)]
    pub delivery_health: Option<SinkHealth>,
}

impl MeilisearchConfig {
    /// Construct a config with defaults for the optional fields.
    pub fn new(id: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            api_key: None,
            index_routing: MeilisearchIndexRouting::ByOutputRelation,
            index_prefix: "vs_".to_owned(),
            auto_create_indexes: true,
            primary_key_field: "_vs_pk".to_owned(),
            batching: MeilisearchBatchConfig::default(),
            retry: RetryConfig::default(),
            task: MeilisearchTaskConfig::default(),
            request_timeout: Duration::from_secs(30),
            verify_tls: true,
            ca_file: None,
            settings: None,
            delivery_health: None,
        }
    }
}

/// How events map to Meilisearch index UIDs.
#[derive(Debug, Clone)]
pub enum MeilisearchIndexRouting {
    /// Route by the `ventstream.cdc.relation` header.
    ByOutputRelation,
    /// Route by the `ventstream.target.index` header.
    ByProjectionTarget,
    /// All events target one index.
    Fixed(String),
}

/// Batching limits applied by the engine's dispatcher.
#[derive(Debug, Clone, Copy)]
pub struct MeilisearchBatchConfig {
    /// Maximum documents per request. Default 2000.
    pub max_docs: usize,
    /// Maximum request body bytes. Default 16MiB — well under
    /// Meilisearch's default 100MB payload cap.
    pub max_bytes: usize,
}

impl Default for MeilisearchBatchConfig {
    fn default() -> Self {
        Self {
            max_docs: 2000,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Task-completion polling policy. Meilisearch indexing is asynchronous;
/// delivery is only durable once the task reaches a terminal status.
#[derive(Debug, Clone, Copy)]
pub struct MeilisearchTaskConfig {
    /// First poll delay after enqueue. Default 50ms.
    pub poll_initial: Duration,
    /// Ceiling on the poll interval. Default 2s.
    pub poll_max: Duration,
    /// Give up waiting for a terminal status after this long. Default 120s.
    pub deadline: Duration,
}

impl Default for MeilisearchTaskConfig {
    fn default() -> Self {
        Self {
            poll_initial: Duration::from_millis(50),
            poll_max: Duration::from_secs(2),
            deadline: Duration::from_secs(120),
        }
    }
}

/// Managed index settings. Only declared attributes are asserted.
#[derive(Debug, Clone, Default)]
pub struct MeilisearchSettings {
    /// Attributes usable in filter expressions.
    pub filterable_attributes: Vec<String>,
    /// Attributes usable in sort expressions.
    pub sortable_attributes: Vec<String>,
}
