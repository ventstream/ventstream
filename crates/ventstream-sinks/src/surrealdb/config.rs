//! Configuration for the SurrealDB sink.

use std::path::PathBuf;
use std::time::Duration;

use ventstream_core::SinkHealth;

use crate::opensearch::RetryConfig;

/// Top-level sink configuration.
#[derive(Debug, Clone)]
pub struct SurrealDbConfig {
    /// Sink identifier matching the pipeline's sink `id`.
    pub id: String,

    /// Instance base URL — e.g. `http://127.0.0.1:8000` or the
    /// `https://` endpoint of a Surreal Cloud instance.
    pub endpoint: String,

    /// Namespace to write into.
    pub namespace: String,

    /// Database to write into.
    pub database: String,

    /// Basic-auth username (root, namespace, or database user).
    pub username: String,

    /// Basic-auth password.
    pub password: String,

    /// Opt-in: ensure the namespace and database exist at startup with
    /// `DEFINE … IF NOT EXISTS`. Requires root- or namespace-level
    /// credentials, so it is OFF by default — production deployments
    /// provision the scopes once and run the sink with a
    /// database-scoped user.
    pub auto_create_database: bool,

    /// Per-event table routing.
    pub table_routing: SurrealTableRouting,

    /// Prefix prepended to routed table names. Unlike search sinks no
    /// charset encoding is applied — SurrealDB accepts arbitrary table
    /// names and every statement addresses them via `type::table()`.
    pub table_prefix: String,

    /// Batching limits shared with the dispatcher.
    pub batching: SurrealBatchConfig,

    /// Retry policy for transient failures.
    pub retry: RetryConfig,

    /// Per-request HTTP timeout.
    pub request_timeout: Duration,

    /// When false, accept self-signed TLS certificates. Dev-only.
    pub verify_tls: bool,

    /// Optional PEM CA bundle for a private certificate authority.
    pub ca_file: Option<PathBuf>,

    /// HNSW vector indexes ensured at startup. Documents carry embedding
    /// arrays as ordinary JSON fields; declaring them here makes them
    /// searchable with `<|K|>` KNN queries.
    pub vector_indexes: Vec<SurrealVectorIndex>,

    /// Process-local availability state shared with the health server.
    #[doc(hidden)]
    pub delivery_health: Option<SinkHealth>,
}

impl SurrealDbConfig {
    /// Construct a config with defaults for the optional fields.
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            namespace: namespace.into(),
            database: database.into(),
            username: username.into(),
            password: password.into(),
            auto_create_database: false,
            table_routing: SurrealTableRouting::ByOutputRelation,
            table_prefix: String::new(),
            batching: SurrealBatchConfig::default(),
            retry: RetryConfig::default(),
            request_timeout: Duration::from_secs(30),
            verify_tls: true,
            ca_file: None,
            vector_indexes: Vec::new(),
            delivery_health: None,
        }
    }
}

/// How events map to SurrealDB table names.
#[derive(Debug, Clone)]
pub enum SurrealTableRouting {
    /// Route by the `ventstream.cdc.relation` header.
    ByOutputRelation,
    /// Route by the `ventstream.target.index` header.
    ByProjectionTarget,
    /// All events target one table.
    Fixed(String),
}

/// Batching limits applied by the engine's dispatcher.
#[derive(Debug, Clone, Copy)]
pub struct SurrealBatchConfig {
    /// Maximum documents per statement run. Default 1000 — SurrealDB
    /// executes runs as one transaction; very large transactions raise
    /// conflict probability under concurrent writers.
    pub max_docs: usize,
    /// Maximum request body bytes. Default 8MiB.
    pub max_bytes: usize,
}

impl Default for SurrealBatchConfig {
    fn default() -> Self {
        Self {
            max_docs: 1000,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// One HNSW vector index ensured at startup with
/// `DEFINE INDEX IF NOT EXISTS`.
#[derive(Debug, Clone)]
pub struct SurrealVectorIndex {
    /// Routed table name the index lives on (post-prefix, as written).
    pub table: String,
    /// Document field holding the embedding array.
    pub field: String,
    /// Embedding dimension.
    pub dimension: u32,
    /// Distance function.
    pub distance: SurrealVectorDistance,
}

/// Distance functions supported by SurrealDB's HNSW index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurrealVectorDistance {
    /// Cosine distance (typical for text embeddings).
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
}

impl SurrealVectorDistance {
    /// SurrealQL keyword for the distance function.
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Manhattan => "MANHATTAN",
        }
    }
}

/// Charset accepted for identifiers embedded verbatim into DDL and
/// reverse-lookup statements (table names, field names). Everything on
/// the data path rides bind variables instead; this gate only exists for
/// the few positions SurrealQL cannot parameterize.
pub(crate) fn is_safe_identifier(input: &str) -> bool {
    !input.is_empty()
        && input
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
}
