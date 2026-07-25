//! Errors that can stop the GraphQL gateway.

use thiserror::Error;

/// Top-level error type for the GraphQL gateway.
#[derive(Debug, Error)]
pub enum GraphQlError {
    /// Could not bind the listening socket.
    #[error("binding listener on {addr}: {source}")]
    Bind {
        /// Address we tried to bind.
        addr: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// NATS connection or JetStream bootstrap failed.
    #[error("NATS / JetStream: {0}")]
    Nats(String),

    /// The configured realtime broker could not initialize.
    #[error("realtime broker: {0}")]
    Broker(String),

    /// Configuration was missing or malformed (e.g. invalid YAML).
    #[error("config: {0}")]
    Config(String),

    /// I/O error during HTTP/WS serving.
    #[error("serve: {0}")]
    Serve(#[from] std::io::Error),
}
