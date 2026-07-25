//! MongoDB CDC source.
//!
//! Streams a MongoDB replica set / sharded cluster into
//! [`ventstream_core::Event`] values via **change streams** (`watch`),
//! with a **snapshot bootstrap** (collection scan) for cold start.
//!
//! Like the Neo4j source, change capture is **pull/resume-based**: each
//! change event carries an opaque **resume token** which we persist to a
//! file and resume from on restart. And like every VentStream source, the
//! change is treated as a *trigger* — for raw 1:1 mode the source carries
//! the (re-read) full document; for join mode (future) the change drives a
//! re-query of the affected primaries.
//!
//! ### Requirements
//!
//! Change streams require a **replica set or sharded cluster** (not a
//! standalone `mongod`) — connect via the replica-set URI or a `mongos`.
//! Works on self-hosted MongoDB 4.0+ and Atlas (`mongodb+srv://`).
//!
//! ### Submodules
//!
//! - [`config`]: configuration struct + `FullDocument` mode.
//! - [`bson`]: `bson::Bson` → `serde_json::Value` conversion (ObjectId →
//!   hex, dates → ISO strings) — the BSON analog of the Neo4j `bolt` module.
//! - [`cursor`]: file-backed resume-token persistence.
//! - [`event_mapper`]: change event / snapshot document → unified `Event`,
//!   stamping the deterministic `ventstream.doc.id`.

pub mod bson;
pub mod config;
pub mod cursor;
pub mod event_mapper;
pub mod source;

pub use config::{FullDocument, MongoCdcConfig};
pub use source::MongoCdcSource;
