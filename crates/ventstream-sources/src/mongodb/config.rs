//! Configuration for the MongoDB CDC source.
//!
//! Populated from `VS_MONGO_*` env vars in the engine binary; the struct
//! itself is plain data so it stays unit-testable without any environment.

use std::path::PathBuf;
use std::time::Duration;

use crate::tls::DatabaseTlsConfig;

/// How much of the document the source asks MongoDB for on **update**
/// events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullDocument {
    /// `fullDocument: "updateLookup"` — re-read the post-image so every
    /// event carries the current whole document. Costs one extra read per
    /// update; required for raw 1:1 mode (the sink writes the whole doc).
    /// This is the default.
    UpdateLookup,
    /// Deltas only — the change event carries just `updateDescription`.
    /// Cheaper, but raw mode then has nothing to write on an update.
    Default,
}

impl FullDocument {
    /// Parse the `VS_MONGO_FULL_DOCUMENT` value; unknown values fall back to
    /// the default (`updateLookup`).
    pub fn from_str_lenient(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "default" | "delta" | "none" => Self::Default,
            _ => Self::UpdateLookup,
        }
    }
}

/// Configuration for one MongoDB CDC source.
#[derive(Debug, Clone)]
pub struct MongoCdcConfig {
    /// Stable source id (matches the engine's source registry entry).
    pub id: String,
    /// Standard connection string — `mongodb://…` or `mongodb+srv://…` —
    /// including the replica-set / mongos host(s), auth, and TLS options.
    /// Change streams require a replica set or sharded cluster.
    pub uri: String,
    /// Database to watch and namespace documents under.
    pub database: String,
    /// Logical namespace label stamped on subjects/headers. Defaults to the
    /// database name; lets operators decouple the routing namespace from the
    /// physical database if they want.
    pub namespace: String,
    /// Collections to watch. **Empty = every collection in `database`.**
    pub collections: Vec<String>,
    /// Directory holding the resume-token cursor file.
    pub state_dir: PathBuf,
    /// Document mode for update events.
    pub full_document: FullDocument,
    /// Run a snapshot bootstrap on cold start (no cursor file present).
    pub bootstrap: bool,
    /// Snapshot scan page size (documents fetched per `find` batch).
    pub bootstrap_chunk_size: usize,
    /// How often the tail flushes the latest resume token to disk. Batched
    /// rather than per-event so a slow durable filesystem (a k8s PVC, a
    /// macOS bind-mount) doesn't cap tail throughput on the fsync. Safe to
    /// batch: delivery is at-least-once and doc-ids are idempotent, so a
    /// crash only re-tails at most this window's worth of already-applied
    /// (and thus harmlessly re-upserted) events.
    pub token_flush_interval: Duration,
    /// Optional TLS policy. `None` leaves TLS behavior to the MongoDB URI.
    pub tls: Option<DatabaseTlsConfig>,
}

impl MongoCdcConfig {
    /// Construct a config with sensible defaults; callers override fields.
    pub fn new(
        id: impl Into<String>,
        uri: impl Into<String>,
        database: impl Into<String>,
        state_dir: impl Into<PathBuf>,
    ) -> Self {
        let database = database.into();
        Self {
            id: id.into(),
            uri: uri.into(),
            namespace: database.clone(),
            database,
            collections: Vec::new(),
            state_dir: state_dir.into(),
            full_document: FullDocument::UpdateLookup,
            bootstrap: true,
            bootstrap_chunk_size: 1000,
            token_flush_interval: Duration::from_millis(1000),
            tls: None,
        }
    }

    /// Whether `collection` is in scope. An empty `collections` list means
    /// "watch every collection in the database".
    pub fn collection_allowed(&self, collection: &str) -> bool {
        self.collections.is_empty() || self.collections.iter().any(|c| c == collection)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_collections_allows_all() {
        let c = MongoCdcConfig::new("m", "mongodb://localhost", "shop", "/tmp/x");
        assert!(c.collection_allowed("orders"));
        assert!(c.collection_allowed("anything"));
        assert_eq!(c.namespace, "shop");
        assert_eq!(c.full_document, FullDocument::UpdateLookup);
    }

    #[test]
    fn explicit_collections_filter() {
        let mut c = MongoCdcConfig::new("m", "mongodb://localhost", "shop", "/tmp/x");
        c.collections = vec!["orders".into(), "customers".into()];
        assert!(c.collection_allowed("orders"));
        assert!(!c.collection_allowed("invoices"));
    }

    #[test]
    fn full_document_parse() {
        assert_eq!(
            FullDocument::from_str_lenient("delta"),
            FullDocument::Default
        );
        assert_eq!(
            FullDocument::from_str_lenient("updateLookup"),
            FullDocument::UpdateLookup
        );
        assert_eq!(
            FullDocument::from_str_lenient("garbage"),
            FullDocument::UpdateLookup
        );
    }
}
