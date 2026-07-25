//! Core types and traits for the VentStream engine.
//!
//! This crate defines the data model and contracts that every other crate
//! depends on:
//!
//! - [`Event`]: the immutable, zero-copy-cloneable record that flows through
//!   the engine. Newtype wrappers ([`EventId`], [`SourceUri`], [`Subject`])
//!   prevent accidental field-mixing at the type system.
//! - [`Source`] and [`Sink`]: the two trait contracts every input/output
//!   adapter implements. Sources push events into the bus; sinks drain them.
//! - [`EventBus`]: bounded MPSC channel that decouples source ingest rate
//!   from sink drain rate and exposes backpressure semantics.
//! - [`ShutdownToken`]: graceful cancellation propagated to every long-lived
//!   task, so the engine never leaks tasks on shutdown.
//! - [`ReadinessSignal`]: shared role-to-health-server readiness state.
//!
//! Everything in this crate is `Send + Sync` and `'static`-bounded where it
//! needs to be stored in trait objects. No `unsafe` is permitted in the
//! workspace (`unsafe_code = "forbid"` at the workspace root).

pub mod bus;
pub mod doc_id;
pub mod error;
pub mod event;
pub mod memory;
pub mod readiness;
pub mod shutdown;
pub mod sink;
pub mod source;

pub use bus::{EventBus, EventReceiver, EventSender};
pub use error::{BackpressureError, CoreError, FailedItem, SinkError, SourceError};
pub use event::{ContentType, Event, EventId, Headers, Payload, SourceUri, Subject};
pub use memory::{MemoryAdmission, MemoryBudget, MemoryPressure};
pub use readiness::ReadinessSignal;
pub use shutdown::ShutdownToken;
pub use sink::{Sink, SinkBatch};
pub use source::{Source, SourceContext};
