//! SurrealDB materialization sink.
//!
//! Translates CDC events into full-replace upserts on native (array)
//! record ids, batched deletes, and table clears over the HTTP RPC
//! protocol. SurrealDB commits synchronously — no task polling. Declared
//! HNSW vector indexes are ensured at startup so embedding fields become
//! KNN-searchable. Design notes: `docs/design/surrealdb-sink.md`.

pub(crate) mod cbor;
pub mod config;
pub mod error;
pub mod reverse_lookup;
pub(crate) mod sink;
pub(crate) mod statements;

pub use config::{
    SurrealBatchConfig, SurrealDbConfig, SurrealEdgeSpec, SurrealLookupField,
    SurrealTableRouting, SurrealVectorDistance, SurrealVectorIndex,
};
pub use error::SurrealSinkError;
pub use reverse_lookup::SurrealReverseLookup;
pub use sink::SurrealDbSink;
