//! Meilisearch materialization sink.
//!
//! Translates CDC events into add-or-replace documents, batched deletes,
//! and index clears, confirmed through Meilisearch's asynchronous task
//! API. Design notes: `docs/design/meilisearch-sink.md`.

pub mod config;
mod documents;
pub mod error;
mod sink;

pub use config::{
    MeilisearchBatchConfig, MeilisearchConfig, MeilisearchIndexRouting, MeilisearchSettings,
    MeilisearchTaskConfig,
};
pub use error::MeilisearchSinkError;
pub use sink::MeilisearchSink;
