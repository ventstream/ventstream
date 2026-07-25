//! Kafka/Redpanda CDC source.
//!
//! Consumes Debezium change topics (or raw JSON topics) and unwraps each
//! message into a [`ventstream_core::Event`]. Unlike the DB sources there is
//! no capture and no re-query — the message carries the row image; resume is
//! the consumer group's committed offsets, gated on sink durability. v1 is
//! raw 1:1 (one message -> one sink doc), JSON values only.

mod config;
mod envelope;
mod event_mapper;
mod offset;
mod source;

pub use config::{KafkaCdcConfig, UnwrapMode};
pub use source::KafkaCdcSource;
