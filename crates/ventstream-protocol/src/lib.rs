//! VentStream wire protocol — the schema that flows over the WebSocket
//! fan-out path.
//!
//! Two concerns live in this crate, independent of the engine's
//! transport plumbing:
//!
//! 1. **[`Event`]** — the envelope every application event is wrapped
//!    in. Deserializing rejects unknown fields. There is no "loose mode."
//!
//! 2. **[`Subject`] and [`SubjectPattern`]** — the strict subject
//!    grammar (`vs.t.{tenant}.{event}.{id}`, where event may be dotted)
//!    and its NATS-compatible pattern matcher (`*` for one segment,
//!    `>` for trailing segments).
//!
//! The CDC pipeline in this engine does **not** use this protocol —
//! pgoutput events flow through a separate, freer shape. This crate
//! is the contract between developer SDKs and the WebSocket sink.

#![deny(missing_docs)]

mod actor;
mod entity;
mod error;
mod event;
mod identifier;
mod metadata;
mod publish;
mod subject;

pub use actor::Actor;
pub use entity::Entity;
pub use error::ProtocolError;
pub use event::{Event, CURRENT_SCHEMA_VERSION};
pub use metadata::Metadata;
pub use publish::PublishInput;
pub use subject::{Segment, Subject, SubjectBuilder, SubjectPattern};
