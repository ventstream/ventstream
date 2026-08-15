//! Meilisearch materialization sink.
//!
//! Translates CDC events into add-or-replace documents, batched deletes,
//! and index clears, confirmed through Meilisearch's asynchronous task
//! API. Design notes: `docs/design/meilisearch-sink.md`.

pub mod config;
pub(crate) mod documents;
pub mod error;
pub mod reverse_lookup;
pub(crate) mod sink;

pub use config::{
    MeilisearchBatchConfig, MeilisearchConfig, MeilisearchIndexRouting, MeilisearchSettings,
    MeilisearchTaskConfig,
};
pub use error::MeilisearchSinkError;
pub use reverse_lookup::MeiliReverseLookup;
pub use sink::MeilisearchSink;
