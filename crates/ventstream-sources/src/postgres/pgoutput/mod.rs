//! Pure parser for the Postgres `pgoutput` logical replication format.
//!
//! No I/O happens in this module — it operates on `&[u8]` slices. That
//! makes it deterministic, fast to test, and trivially fuzzable later.
//!
//! Wire format reference:
//! <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>
//!
//! ### Phase 0a coverage
//!
//! - `B` BEGIN
//! - `C` COMMIT
//! - `R` RELATION
//! - `I` INSERT
//!
//! ### Future
//!
//! `U` UPDATE, `D` DELETE, `T` TRUNCATE, `Y` TYPE, `O` ORIGIN,
//! `M` MESSAGE — all use the same primitive readers and the same
//! [`LogicalMessage`] enum will grow new variants.

pub mod decoder;
pub mod messages;

pub use decoder::{decode, DecodeError};
pub use messages::{
    Begin, Column, ColumnFlags, Commit, Delete, Insert, LogicalMessage, Lsn, OldTuple,
    OldTupleKind, Relation, ReplicaIdentity, Truncate, Tuple, TupleColumn, Update,
};
