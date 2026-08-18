//! Configuration for the SurrealDB sink.

use std::path::PathBuf;
use std::time::Duration;

use ventstream_core::SinkHealth;

use crate::opensearch::RetryConfig;

/// Top-level sink configuration.
#[derive(Clone)]
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

    /// Join paths whose string-canonical values are materialized onto
    /// each document (as `_vs_lx_*` fields) so reverse lookups filter a
    /// flat field instead of evaluating a flatten/map closure per row —
    /// measured ~5x cheaper at 30k docs, and index-ready if SurrealDB's
    /// planner gains CONTAINSANY support. Declare the 1:many embed
    /// paths of joined pipelines, e.g. `{table: "orders", field:
    /// "items.item_id"}`.
    pub lookup_fields: Vec<SurrealLookupField>,

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
            lookup_fields: Vec::new(),
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
    /// executes runs as one transaction, and transaction cost grows
    /// superlinearly: benchmarked locally, 1000-doc runs sustain
    /// ~1.45M docs/min vs ~1.14M at 2000 and ~0.86M at 5000. Raising
    /// this only pays on high-RTT links where round-trips dominate.
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

/// One join path materialized for cheap reverse lookups.
#[derive(Debug, Clone)]
pub struct SurrealLookupField {
    /// Routed table name (post-prefix, as written).
    pub table: String,
    /// Dotted document path of the embedded join key.
    pub field: String,
}

/// Field name carrying the materialized string-canonical values of a
/// lookup path: `items.item_id` → `_vs_lx_items_item_id`.
pub(crate) fn lookup_field_name(path: &str) -> String {
    let mut name = String::with_capacity(path.len() + 7);
    name.push_str("_vs_lx_");
    for byte in path.bytes() {
        name.push(if byte.is_ascii_alphanumeric() {
            byte as char
        } else {
            '_'
        });
    }
    name
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

impl std::fmt::Debug for SurrealDbConfig {
    /// Manual impl so the password can never ride a `{:?}` into logs
    /// or error strings (same convention as `RedisConfig`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurrealDbConfig")
            .field("id", &self.id)
            .field("endpoint", &self.endpoint)
            .field("namespace", &self.namespace)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("auto_create_database", &self.auto_create_database)
            .field("table_routing", &self.table_routing)
            .field("table_prefix", &self.table_prefix)
            .field("batching", &self.batching)
            .field("vector_indexes", &self.vector_indexes)
            .field("lookup_fields", &self.lookup_fields)
            .finish_non_exhaustive()
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
        // Dotted paths render per segment; an empty segment would emit
        // a bare ⟨⟩ and fail the statement.
        && input.split('.').all(|segment| !segment.is_empty())
}
