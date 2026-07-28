//! Source adapters for VentStream.
//!
//! Phase 0 target: [`postgres`] — logical replication consumer that
//! decodes the `pgoutput` plugin format into [`ventstream_core::Event`]
//! values. [`neo4j`] adds graph-CDC ingest from Neo4j 5.17+ Enterprise.
//! Future adapters (Mongo change streams, Kafka, Redis Streams,
//! RabbitMQ) will live in sibling modules under this crate.
//!
//! ### Crate organization
//!
//! - [`postgres::pgoutput`]: pure parser for the logical replication wire
//!   format. No I/O, fully unit-tested.
//! - [`postgres::config`]: configuration struct for a Postgres CDC source.
//! - [`postgres::source`]: the [`ventstream_core::Source`] implementation
//!   that ties the parser to a live replication connection.
//! - [`neo4j::config`]: configuration struct for a Neo4j CDC source.
//! - [`neo4j::source`]: polling-loop implementation against the
//!   `db.cdc.query` Cypher procedure.

pub mod error;
pub mod kafka;
pub mod mongodb;
pub mod mysql;
pub mod neo4j;
pub mod postgres;
pub mod tls;

pub use error::{KafkaCdcError, MongoCdcError, MySqlCdcError, Neo4jCdcError, PostgresCdcError};
pub use kafka::{KafkaCdcConfig, KafkaCdcSource, UnwrapMode};
pub use mongodb::{FullDocument, MongoCdcConfig, MongoCdcSource};
pub use mysql::{MySqlCdcConfig, MySqlCdcSource};
pub use neo4j::{Neo4jBootstrap, Neo4jCdcConfig, Neo4jCdcSource};
pub use postgres::{PostgresCdcConfig, PostgresCdcSource};
pub use tls::{
    materialize_provider_ca_bundle, DatabaseTlsConfig, DatabaseTlsMode, DatabaseTlsTrustProvider,
};
