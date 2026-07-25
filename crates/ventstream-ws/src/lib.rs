//! WebSocket fan-out gateway.
//!
//! The crate exposes [`run`] and [`run_with_readiness`] entry points that bind the
//! HTTP/WS server, connects to the configured realtime broker, and forwards
//! matching events to subscribed WebSocket clients until shutdown.
//!
//! High-level shape:
//!
//! ```text
//! broker stream ─► gateway fan-out ─► per-connection task ─► WebSocket
//! ```
//!
//! Each connection runs as its own task with a bounded outbound
//! mailbox; a slow client cannot stall a fast publisher (we disconnect
//! the slow client instead). NATS Core provides ephemeral delivery; NATS
//! JetStream and Redis Streams provide replayable cursors.

#![deny(missing_docs)]

mod bus;
mod config;
mod connection;
mod error;
mod jetstream;
mod protocol;
mod registry;
mod server;

pub use config::{JetStreamConfig, StreamStorage, WsConfig};
pub use error::WsError;
pub use server::{run, run_with_readiness};
pub use ventstream_redis::RedisStreamsConfig;
