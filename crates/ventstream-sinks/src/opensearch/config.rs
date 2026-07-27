//! Configuration for the OpenSearch / Elasticsearch sink.

use std::path::PathBuf;
use std::time::Duration;

/// Top-level sink configuration.
#[derive(Debug, Clone)]
pub struct OpenSearchConfig {
    /// Sink identifier matching the pipeline's sink `id`.
    pub id: String,

    /// Cluster base URL — e.g. `https://search.example.com:9200`. Must
    /// include scheme. The Bulk API path is appended automatically.
    pub endpoint: String,

    /// Authentication mode.
    pub auth: AuthMode,

    /// Per-event index name template. See
    /// [`super::index_template`] for the substitution syntax.
    pub index_template: String,

    /// Batching limits applied by the engine before calling
    /// [`Sink::write`](ventstream_core::Sink::write). The sink itself
    /// just sends what it's given; these values are surfaced here so
    /// the engine and operator share one source of truth.
    pub bulk: BulkConfig,

    /// Retry policy for transient failures.
    pub retry: RetryConfig,

    /// Per-request HTTP timeout. Bulk indexing latency on real
    /// clusters is usually well under 1s; default of 30s is generous
    /// to tolerate occasional slow shards.
    pub request_timeout: Duration,

    /// When false, accept self-signed TLS certificates. Dev-only — never
    /// disable in production.
    pub verify_tls: bool,

    /// Optional PEM CA bundle for a private certificate authority.
    pub ca_file: Option<PathBuf>,

    /// Override the reconcile mass-deletion guardrails. When false (the
    /// default), [`super::reconcile_orphans`] refuses to run if the source
    /// returned **zero** valid PKs, or if more than
    /// [`super::reconcile::MAX_DELETE_FRACTION`] of the examined docs would
    /// be deleted — these are almost always a transient/truncated source
    /// query, and deleting on them wipes a live index. Set true only when a
    /// genuine full purge is intended (e.g. a table truly emptied).
    pub reconcile_allow_full_purge: bool,
}

impl OpenSearchConfig {
    /// Construct a config with sane defaults for the optional fields.
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        index_template: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            auth: AuthMode::None,
            index_template: index_template.into(),
            bulk: BulkConfig::default(),
            retry: RetryConfig::default(),
            request_timeout: Duration::from_secs(30),
            verify_tls: true,
            ca_file: None,
            reconcile_allow_full_purge: false,
        }
    }

    /// Set the authentication mode.
    #[must_use]
    pub fn with_auth(mut self, auth: AuthMode) -> Self {
        self.auth = auth;
        self
    }

    /// Override the bulk batching limits.
    #[must_use]
    pub const fn with_bulk(mut self, bulk: BulkConfig) -> Self {
        self.bulk = bulk;
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub const fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Override the per-request timeout.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Disable TLS verification (development only).
    #[must_use]
    pub const fn with_insecure_tls(mut self) -> Self {
        self.verify_tls = false;
        self
    }

    /// Add a private CA bundle while retaining full certificate verification.
    #[must_use]
    pub fn with_ca_file(mut self, path: PathBuf) -> Self {
        self.ca_file = Some(path);
        self
    }

    /// Allow reconcile to delete on an empty/over-threshold valid-PK set
    /// (bypass the mass-deletion guardrails). Use only for an intended
    /// full purge.
    #[must_use]
    pub const fn with_reconcile_allow_full_purge(mut self, allow: bool) -> Self {
        self.reconcile_allow_full_purge = allow;
        self
    }
}

/// Authentication mode for the sink.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AuthMode {
    /// No authentication header. Plain-HTTP development clusters only.
    None,
    /// HTTP Basic auth with `username:password`.
    Basic {
        /// Login name.
        username: String,
        /// Password.
        password: String,
    },
    /// API key sent as `Authorization: ApiKey <key>`. The provided
    /// string MUST already be in OpenSearch's expected encoding (for
    /// most setups: base64 of `id:api_key`).
    ApiKey(String),
}

/// Batching limits applied by the engine's dispatcher.
#[derive(Debug, Clone, Copy)]
pub struct BulkConfig {
    /// Maximum events per bulk request. Default 2000.
    pub max_events: usize,
    /// Maximum total NDJSON body bytes per bulk request. Default 5MB.
    /// Acts as a safety net for unusually large event payloads.
    pub max_bytes: usize,
    /// Maximum wait before flushing a partial batch. Default 500ms.
    pub flush_interval: Duration,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            max_events: 2000,
            max_bytes: 5 * 1024 * 1024,
            flush_interval: Duration::from_millis(500),
        }
    }
}

/// Retry policy for transient sink failures.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Maximum total attempts (initial + retries). Default 5.
    pub max_attempts: u32,
    /// Backoff before the first retry. Default 100ms.
    pub initial_backoff: Duration,
    /// Ceiling on per-attempt backoff. Default 30s.
    pub max_backoff: Duration,
    /// Multiplier applied to the previous backoff. Default 2.0.
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            backoff_factor: 2.0,
        }
    }
}
