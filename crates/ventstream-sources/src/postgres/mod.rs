//! Postgres logical replication source.
//!
//! Reads write-ahead-log changes from a Postgres replication slot using
//! the `pgoutput` plugin, decodes them into typed messages, and emits
//! [`ventstream_core::Event`] values into the engine's bus.
//!
//! ### Submodules
//!
//! - [`config`]: configuration struct (Phase 0 — hardcoded; later loaded
//!   from `pipeline.yaml`).
//! - [`pgoutput`]: pure parser for the logical replication wire format.
//!   Fully unit-tested against known byte sequences.
//! - [`schema`]: cache of `RELATION` messages keyed by relation oid, used
//!   to decode subsequent `INSERT`/`UPDATE`/`DELETE` tuples.
//! - [`event_mapper`]: converts decoded `pgoutput` messages into
//!   [`ventstream_core::Event`] values.
//! - [`source`]: the [`ventstream_core::Source`] trait implementation.
//!
//! ### Phase 0a scope
//!
//! - Decoded message types: `BEGIN`, `COMMIT`, `RELATION`, `INSERT`.
//! - `UPDATE`, `DELETE`, `TRUNCATE`, `TYPE`, `ORIGIN`, `MESSAGE` land in
//!   Phase 0b alongside ACK-gated LSN advance.

pub mod config;
pub mod connection;
pub mod event_mapper;
pub mod fetcher;
pub mod pgoutput;
pub mod schema;
pub mod snapshot;
pub mod source;

pub use config::{PostgresCdcConfig, SnapshotBootstrap, SnapshotTable};
pub use connection::{
    connect_client, describe_db_error, is_credential_db_error, is_credential_sqlstate, sqlstate,
};
pub use fetcher::PostgresFetcher;
pub use schema::type_change_epoch;
pub use snapshot::{resync_tables, ResyncStats};
pub use source::is_credential_message;
pub use source::PostgresCdcSource;
