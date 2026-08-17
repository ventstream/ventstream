//! Kafka/Redpanda CDC source.
//!
//! Consumes Debezium change topics (or raw JSON topics) and unwraps each
//! message into a [`ventstream_core::Event`]. Unlike the DB sources there is
//! no capture and no re-query — the message carries the row image; resume is
//! the consumer group's committed offsets, gated on sink durability. v1 is
//! raw 1:1 (one message -> one sink doc), JSON values only.

mod config;
// The message-unwrap internals are only consumed by the real source,
// which is Unix-only; gating them avoids dead-code denials on Windows.
#[cfg(not(windows))]
mod envelope;
#[cfg(not(windows))]
mod event_mapper;
#[cfg(not(windows))]
mod offset;
#[cfg(not(windows))]
mod source;
#[cfg(windows)]
mod source_unsupported;

pub use config::{KafkaCdcConfig, UnwrapMode};
#[cfg(not(windows))]
pub use source::KafkaCdcSource;
#[cfg(windows)]
pub use source_unsupported::KafkaCdcSource;
