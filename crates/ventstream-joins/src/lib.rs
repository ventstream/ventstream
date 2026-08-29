//! Configurable, stateful joins between CDC streams.
//!
//! The [`JoinEngine`] sits between the source bus and the dispatcher.
//! It consumes raw per-table events, maintains an in-memory state
//! (forward index of primary rows, reverse indexes from foreign-key
//! values to affected primary rows, secondary index for 1-to-many
//! lookups), and emits **composed** events where each primary row is
//! enriched with embedded data from its related rows.
//!
//! Foreign-side updates trigger **re-emission** of every affected
//! composed document — the property that makes denormalized search
//! indexes stay correct without external stream-processing.
//!
//! ### Pipeline placement
//!
//! ```text
//! [Source] → [Bus] → [JoinEngine] → [Bus'] → [Dispatcher] → [Sink]
//! ```
//!
//! When no joins are configured the engine is bypassed entirely and
//! events flow directly from the source bus to the dispatcher.
//!
//! ### What we cover today
//!
//! - 1-to-1 (orders → customer)
//! - 1-to-many (orders → line_items)
//! - Multiple foreign keys to the same table (billing + shipping)
//! - Self-joins (employees → manager)
//! - Composite primary / foreign keys
//! - Sync-on-miss backfill via a pluggable [`RelatedFetcher`]
//!
//! ### Deferred to a later phase
//!
//! - Nested join chains (`order → item → product`)
//! - Junction-table many-to-many as native config
//! - Persistent state (currently in-memory only)
//! - Eager bootstrap via slot snapshot

pub mod config;
pub mod engine;
pub mod error;
pub mod fetcher;
pub mod key;
pub mod persistence;
pub mod state;

pub use config::{
    BackfillMode, Cardinality, JoinDefinition, JoinOn, OnMissing, PkSpec, PrimaryRef,
    RelatedDefinition, StateBackend,
};
pub use engine::{JoinDurability, JoinEngine, PoisonSink};
pub use error::JoinError;
pub use fetcher::{FetchError, RelatedFetcher};
pub use key::PkValue;
pub use persistence::{LoadStats, PersistError, PersistentBackend};
pub use state::JoinState;
