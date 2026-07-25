//! Neo4j CDC source.
//!
//! Pulls graph mutations from a Neo4j 5.17+ Enterprise instance via the
//! built-in `db.cdc.*` Cypher procedures and emits them as
//! [`ventstream_core::Event`] values into the engine's bus.
//!
//! Neo4j CDC is a **pull-based polling model** rather than push: we
//! call `db.cdc.query($cursor)` on a configurable interval, persist the
//! resulting cursor, and resume on restart. The Bolt protocol is handled
//! by the `neo4rs` crate.
//!
//! ### Submodules
//!
//! - [`config`]: configuration struct + label/reltype mapping.
//! - [`bolt`]: `BoltType` → `serde_json::Value` conversion, including
//!   correct handling of temporal types (`DateTime`, `Date`, `Time`,
//!   `Duration`) as ISO-8601 strings.
//! - [`cursor`]: file-backed persistence of the CDC cursor.
//! - [`event_mapper`]: convert a CDC payload (live tail) or a Cypher row
//!   (bootstrap) into the unified `Event` shape with the same header
//!   conventions the join engine already expects from the PG source.
//! - [`bootstrap`]: cold-start snapshot scan over every label and
//!   relationship type, emits synthetic insert events, then a
//!   `snapshot-complete` sentinel.
//! - [`source`]: the [`ventstream_core::Source`] trait implementation.
//!
//! ### Edition requirement
//!
//! Neo4j CDC is **Enterprise only**, GA in 5.17. Not available on
//! Community Edition or Aura Free / Aura Professional. CDC must be
//! explicitly enabled per database via `txLogEnrichment`:
//!
//! - `'DIFF'` — **default / recommended** for denormalize mode. The
//!   engine re-queries the graph to recompose each document, so the CDC
//!   event is only a trigger; the lighter `DIFF` payload loses nothing
//!   (deletes still carry labels for tombstones) at ~half the tx-log
//!   write of `FULL`.
//! - `'FULL'` — only needed for a raw (non-denormalize) tail whose
//!   consumers want the complete entity state on every event.
//!
//! ```cypher
//! ALTER DATABASE neo4j SET OPTION txLogEnrichment 'DIFF';
//! ```

pub mod bolt;
pub mod bootstrap;
pub mod config;
pub mod cursor;
pub mod denormalize;
pub mod event_mapper;
pub mod hot_endpoints;
pub(crate) mod projection;
pub mod reconcile;
pub mod retry;
pub mod source;

pub use config::{Neo4jBootstrap, Neo4jCdcConfig};
pub use denormalize::{
    analyze_specs, estimate_max_hops_in_cypher, AnalyzeRow, DenormalizeSpec, DenormalizeSpecs,
};
pub use hot_endpoints::SpecHotEndpoints;
pub use reconcile::list_node_element_ids;
pub use source::Neo4jCdcSource;
