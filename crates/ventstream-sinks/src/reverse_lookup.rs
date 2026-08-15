//! Sink-agnostic reverse lookup for 1:many child deletes.
//!
//! When a source's delete image lacks the parent key (Postgres
//! `REPLICA IDENTITY DEFAULT`, MySQL deletes reduced to the child PK), the
//! denormalizers ask the *sink* which parent documents embed the deleted
//! child and recompose those. This wrapper gives both SQL denormalizers one
//! interface over the per-sink implementations.

use ventstream_core::Event;

use crate::meilisearch::MeiliReverseLookup;
use crate::opensearch::{index_template, OsReverseLookup};

/// Reverse-lookup client for whichever sink the pipeline writes to.
pub enum ReverseLookup {
    /// OpenSearch/Elasticsearch: `terms` query on keyword fields.
    OpenSearch(Box<OsReverseLookup>),
    /// Meilisearch: filter query on (auto-ensured) filterable fields.
    Meilisearch(Box<MeiliReverseLookup>),
}

impl ReverseLookup {
    /// Resolve the concrete index a probe event's documents land in, using
    /// the same routing the sink writer applies.
    pub fn resolve_index_for(&self, probe: &Event) -> Option<String> {
        match self {
            Self::OpenSearch(lookup) => {
                index_template::render(lookup.index_template(), probe, chrono::Utc::now()).ok()
            }
            Self::Meilisearch(lookup) => lookup.resolve_index_for(probe),
        }
    }

    /// Canonical parent doc ids embedding any of `values` at `field`.
    pub async fn doc_ids_by_terms(
        &self,
        index: &str,
        field: &str,
        values: &[String],
    ) -> Result<Vec<String>, String> {
        match self {
            Self::OpenSearch(lookup) => lookup
                .doc_ids_by_terms(index, field, values)
                .await
                .map_err(|err| err.to_string()),
            Self::Meilisearch(lookup) => lookup.doc_ids_by_terms(index, field, values).await,
        }
    }

    /// Composite-child-PK variant.
    pub async fn doc_ids_by_term_tuples(
        &self,
        index: &str,
        fields: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<String>, String> {
        match self {
            Self::OpenSearch(lookup) => lookup
                .doc_ids_by_term_tuples(index, fields, tuples)
                .await
                .map_err(|err| err.to_string()),
            Self::Meilisearch(lookup) => {
                lookup.doc_ids_by_term_tuples(index, fields, tuples).await
            }
        }
    }
}
