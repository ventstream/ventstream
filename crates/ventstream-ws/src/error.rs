//! Errors that can stop the WS gateway.

use thiserror::Error;

/// Top-level error type for the WebSocket gateway. Returned only from
/// [`run`](crate::run); per-connection errors are logged and the
/// connection closed but never propagated up.
#[derive(Debug, Error)]
pub enum WsError {
    /// Could not bind the listening socket.
    #[error("binding listener on {addr}: {source}")]
    Bind {
        /// The address we tried to bind.
        addr: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// Could not connect to the NATS bus.
    #[error("connecting to NATS at {url}: {detail}")]
    Nats {
        /// The configured NATS URL.
        url: String,
        /// Stringified underlying client error (the exact type
        /// varies across `async-nats` releases; we capture the
        /// message rather than chase the type).
        detail: String,
    },

    /// Subscribing to a configured bus subject failed.
    #[error("subscribing to '{subject}' on NATS: {detail}")]
    NatsSubscribe {
        /// Subject the gateway tried to subscribe to.
        subject: String,
        /// Stringified underlying client error.
        detail: String,
    },

    /// The selected durable realtime broker could not initialize.
    #[error("initializing realtime broker: {0}")]
    Broker(String),

    /// An I/O error during HTTP/WS serving.
    #[error("HTTP serving: {0}")]
    Serve(#[from] std::io::Error),
}
