//! Configuration for a Neo4j CDC source.
//!
//! Loaded by the binary from `VS_NEO4J_*` env vars and passed to
//! [`super::source::Neo4jCdcSource::new`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::tls::DatabaseTlsConfig;

/// Configuration for a [`super::Neo4jCdcSource`].
#[derive(Debug, Clone)]
pub struct Neo4jCdcConfig {
    /// Stable identifier matching the pipeline's source `id` (used in
    /// log lines and the `SourceUri` of every emitted event).
    pub id: String,

    /// Bolt URI, e.g. `bolt://127.0.0.1:7687` or `neo4j+s://aura-host`.
    pub uri: String,

    /// Username (default driver setup is `neo4j`).
    pub user: String,

    /// Password — supply via environment variable, never commit.
    pub password: String,

    /// Database name. Neo4j 4+ supports multi-DB; pick one.
    pub database: String,

    /// Logical "namespace" stamped into the
    /// `ventstream.cdc.namespace` header on every emitted event. The
    /// join engine concatenates this with the relation name to match
    /// `JoinDefinition.primary.table`. Defaults to `"neo4j"`.
    pub namespace: String,

    /// Map a Neo4j label → the logical "table" name used in the event
    /// subject and the `ventstream.cdc.relation` header. When a label
    /// is not in the map, the label itself is used unchanged.
    pub label_table_map: HashMap<String, String>,

    /// Map a Neo4j relationship type → the logical "table" name. Same
    /// fallback as labels.
    pub reltype_table_map: HashMap<String, String>,

    /// If non-empty, only emit events for these labels. Empty = emit all.
    pub label_filter: Vec<String>,

    /// If non-empty, only emit events for these reltypes. Empty = emit all.
    pub reltype_filter: Vec<String>,

    /// Priority order used to pick the **canonical label** for nodes
    /// that carry more than one (e.g. `Author:Person`, `Author:Company`).
    /// The first label in this list that the node also has wins; the
    /// node emits exactly one event under that canonical label.
    ///
    /// Without a priority list the source falls back to "first label
    /// from `labels(n)`" — which is the order Neo4j returns and is
    /// undocumented / unstable across server versions. Set this list
    /// whenever you have composite-label nodes you care about.
    pub label_priority: Vec<String>,

    /// State directory for the CDC cursor file. The source writes
    /// `{state_dir}/neo4j_cursor` on every batch.
    pub state_dir: PathBuf,

    /// Optional path to a PEM file containing an additional certificate
    /// to add to the TLS trust chain. Use this for:
    /// - private CAs not in the system trust store
    /// - self-signed certs in dev / staging environments
    ///
    /// **Not needed for Aura** — Aura uses Let's Encrypt, which is
    /// already in macOS Keychain / Linux ca-certificates / Windows
    /// trust store. Leave `None` and `bolt+s://` / `neo4j+s://` URIs
    /// will validate automatically.
    pub trust_cert_file: Option<PathBuf>,

    /// Optional TLS policy. `None` leaves encryption selection to the URI.
    pub tls: Option<DatabaseTlsConfig>,

    /// How long to wait between polls of `db.cdc.query`. Smaller =
    /// lower live-tail latency at the cost of more idle Bolt traffic.
    pub poll_interval: Duration,

    /// After this many consecutive empty polls, refresh the persisted
    /// cursor to `db.cdc.current()`. Mitigates transaction-log rotation
    /// expiring the cursor on idle databases.
    pub idle_advance_after_polls: u32,

    /// Maximum affected primaries per recompose query on the live tail.
    /// A poll's events are coalesced and recomposed in chunks of this
    /// size: too large degrades the fan-out query plan (a big element-id
    /// `IN`-list can flip the projection branches off their indexed seek
    /// onto a scan); too small loses per-query round-trip amortization.
    /// Tunable via `VS_NEO4J_RECOMPOSE_CHUNK`.
    ///
    /// Default 128: a sweep on a 100k-node 3-hop graph peaked at 128–256
    /// (~4.3k recomposes/s) then fell off a cliff at 512 (the large
    /// `IN`-list degrades the plan). 128 sits at the peak with margin to
    /// the cliff — denser graphs hit it sooner, so prefer the smaller knee.
    pub recompose_chunk: usize,

    /// How many recompose chunks to run concurrently on the live tail.
    /// A large fan-out's chunk queries are independent (disjoint primary
    /// sets), so they can run in parallel up to the bolt connection pool.
    /// Tunable via `VS_NEO4J_RECOMPOSE_CONCURRENCY`. Default 8 — half the
    /// default 16-connection pool, leaving headroom for polling / cursor
    /// work.
    pub recompose_concurrency: usize,

    /// Enable projection-aware fan-out for denormalization specs.
    pub projection_fan_out: bool,

    /// Hot-endpoint detection threshold. `0` disables detection.
    pub hot_node_threshold: usize,

    /// Optional snapshot bootstrap configuration. When `Some` and the
    /// cursor file does not yet exist, the source paginates every
    /// label and reltype before starting the tail.
    pub bootstrap: Option<Neo4jBootstrap>,

    /// Optional denormalisation specs. When non-empty, the source
    /// switches from "emit one event per node / relationship" to "emit
    /// one denormalised document per primary on every change within
    /// N hops of the primary" — the real-time equivalent of a periodic
    /// generic Cypher sync, generalised to any graph shape.
    ///
    /// Multiple specs run in parallel; each writes to its own
    /// `output_table` in OpenSearch.
    pub denormalize: Option<super::denormalize::DenormalizeSpecs>,
}

/// Snapshot bootstrap settings.
#[derive(Debug, Clone)]
pub struct Neo4jBootstrap {
    /// Rows per `SKIP/LIMIT` page when scanning nodes / relationships.
    /// 2_000 is a sane default for spike-scale graphs; large graphs
    /// will want larger (10_000+) at the cost of per-page latency.
    pub batch_size: i64,
}

impl Default for Neo4jBootstrap {
    fn default() -> Self {
        Self { batch_size: 2_000 }
    }
}

impl Neo4jCdcConfig {
    /// Sane defaults except for the connection-shaped fields.
    pub fn new(
        id: impl Into<String>,
        uri: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        state_dir: PathBuf,
    ) -> Self {
        Self {
            id: id.into(),
            uri: uri.into(),
            user: user.into(),
            password: password.into(),
            database: database.into(),
            namespace: "neo4j".to_owned(),
            label_table_map: HashMap::new(),
            reltype_table_map: HashMap::new(),
            label_filter: Vec::new(),
            reltype_filter: Vec::new(),
            label_priority: Vec::new(),
            state_dir,
            trust_cert_file: None,
            tls: None,
            poll_interval: Duration::from_millis(500),
            idle_advance_after_polls: 20,
            recompose_chunk: 128,
            recompose_concurrency: 8,
            projection_fan_out: true,
            hot_node_threshold: super::hot_endpoints::DEFAULT_HOT_NODE_THRESHOLD,
            bootstrap: Some(Neo4jBootstrap::default()),
            denormalize: None,
        }
    }

    /// Resolve a label to the logical table name used in the event
    /// subject and the `ventstream.cdc.relation` header.
    ///
    /// Default fallback **lowercases** the label, because the most
    /// common downstream consumer (OpenSearch index names via
    /// `events-${header:ventstream.cdc.relation}`) requires lowercase.
    /// An explicit `VS_NEO4J_LABEL_TABLES=Person:Person` mapping wins,
    /// at the operator's risk.
    pub fn resolve_label_table(&self, label: &str) -> String {
        self.label_table_map
            .get(label)
            .cloned()
            .unwrap_or_else(|| label.to_lowercase())
    }

    /// Resolve a reltype to the logical table name. Same lowercase
    /// fallback as labels — see [`Self::resolve_label_table`].
    pub fn resolve_reltype_table(&self, reltype: &str) -> String {
        self.reltype_table_map
            .get(reltype)
            .cloned()
            .unwrap_or_else(|| reltype.to_lowercase())
    }

    /// True iff this label should produce events. Empty filter = pass.
    pub fn label_allowed(&self, label: &str) -> bool {
        self.label_filter.is_empty() || self.label_filter.iter().any(|l| l == label)
    }

    /// True iff this reltype should produce events. Empty filter = pass.
    pub fn reltype_allowed(&self, reltype: &str) -> bool {
        self.reltype_filter.is_empty() || self.reltype_filter.iter().any(|r| r == reltype)
    }

    /// Pick the canonical label for a node carrying one or more labels.
    ///
    /// - If [`Self::label_priority`] is set, returns the first label
    ///   from the priority list that the node also has.
    /// - Otherwise falls back to the first element of `labels`.
    /// - Returns `None` if the node has no labels at all.
    ///
    /// Used by both bootstrap and the live tail so a composite-label
    /// node (e.g. `Author:Person`) emits exactly one event under a
    /// consistent table name regardless of which source is in use.
    pub fn canonical_label<'a>(&'a self, labels: &'a [String]) -> Option<&'a str> {
        if !self.label_priority.is_empty() {
            for p in &self.label_priority {
                if labels.iter().any(|l| l == p) {
                    return Some(p.as_str());
                }
            }
            // Node has labels but none match the priority list — fall
            // through to the first-label fallback rather than dropping
            // the event. The label_filter is the right place to drop
            // unwanted labels; canonicalisation should only choose,
            // not exclude.
        }
        labels.first().map(String::as_str)
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn empty_config() -> Neo4jCdcConfig {
        Neo4jCdcConfig::new(
            "test",
            "bolt://x",
            "u",
            "p",
            "db",
            PathBuf::from("/tmp/test"),
        )
    }

    #[test]
    fn no_priority_returns_first_label() {
        let cfg = empty_config();
        let labels = vec!["Person".to_owned(), "Author".to_owned()];
        assert_eq!(cfg.canonical_label(&labels), Some("Person"));
    }

    #[test]
    fn priority_wins_over_native_order() {
        let mut cfg = empty_config();
        cfg.label_priority = vec!["Author".to_owned()];
        let labels = vec!["Person".to_owned(), "Author".to_owned()];
        assert_eq!(cfg.canonical_label(&labels), Some("Author"));
    }

    #[test]
    fn priority_falls_through_when_none_match() {
        let mut cfg = empty_config();
        cfg.label_priority = vec!["DoesNotExist".to_owned()];
        let labels = vec!["Person".to_owned(), "Author".to_owned()];
        assert_eq!(cfg.canonical_label(&labels), Some("Person"));
    }

    #[test]
    fn no_labels_returns_none() {
        let cfg = empty_config();
        let labels: Vec<String> = Vec::new();
        assert_eq!(cfg.canonical_label(&labels), None);
    }

    #[test]
    fn priority_picks_earliest_match() {
        let mut cfg = empty_config();
        cfg.label_priority = vec![
            "Author".to_owned(),
            "Person".to_owned(),
            "Company".to_owned(),
        ];
        let labels = vec!["Person".to_owned(), "Company".to_owned()];
        assert_eq!(cfg.canonical_label(&labels), Some("Person"));
    }
}
