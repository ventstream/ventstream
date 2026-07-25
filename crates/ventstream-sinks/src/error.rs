//! Error types for sink adapters.

use thiserror::Error;

/// Errors emitted by the OpenSearch sink.
#[derive(Debug, Error)]
pub enum OpenSearchSinkError {
    /// Building or rendering the index template failed (e.g. unknown
    /// placeholder, malformed pattern).
    #[error("index template error: {0}")]
    IndexTemplate(String),

    /// HTTP transport failure — connection refused, DNS, TLS handshake,
    /// timeout. Always treated as retryable.
    #[error("transport error: {0}")]
    Transport(String),

    /// Server returned a 5xx response. Retryable.
    #[error("server error (HTTP {status}): {message}")]
    Server {
        /// HTTP status code.
        status: u16,
        /// Body excerpt for diagnostics.
        message: String,
    },

    /// Server returned a 429 rate-limit response. Retryable with longer
    /// backoff than ordinary server errors.
    #[error("rate limited (HTTP 429): {0}")]
    RateLimited(String),

    /// Server returned a 4xx response other than 429. Not retryable;
    /// the batch is poison and should be routed to the DLQ.
    #[error("client error (HTTP {status}): {message}")]
    Client {
        /// HTTP status code.
        status: u16,
        /// Body excerpt for diagnostics.
        message: String,
    },

    /// Authentication credentials were rejected. Not retryable.
    #[error("authentication failed (HTTP {status}): {message}")]
    Auth {
        /// HTTP status code (401 or 403).
        status: u16,
        /// Body excerpt for diagnostics.
        message: String,
    },

    /// Bulk response body could not be parsed as JSON.
    #[error("malformed bulk response: {0}")]
    MalformedResponse(String),

    /// The bulk API accepted the request but reported per-item failures.
    /// Carries each failed item's offset *and* its specific error
    /// message so the engine can DLQ with accurate per-event reasons.
    #[error(
        "bulk response had {failed_count} of {batch_size} items rejected (first: {sample_error})"
    )]
    PartialFailure {
        /// Total events in the batch.
        batch_size: usize,
        /// Number of items the server rejected.
        failed_count: usize,
        /// First per-item error, included for log lines.
        sample_error: String,
        /// One entry per failed item, with its specific error.
        failed_items: Vec<ventstream_core::FailedItem>,
    },

    /// The fully encoded NDJSON request exceeded the configured body limit.
    /// The sink uses this internally to split multi-event batches. It reaches
    /// the dispatcher only when one event cannot fit by itself.
    #[error("bulk request of {actual_bytes} bytes exceeds max_bytes={max_bytes}")]
    RequestTooLarge {
        /// Encoded request body size.
        actual_bytes: usize,
        /// Configured request body limit.
        max_bytes: usize,
    },

    /// Internal failure (serialization, configuration, etc.).
    #[error("internal sink error: {0}")]
    Internal(String),
}

impl OpenSearchSinkError {
    /// Whether the engine should retry the same batch after backoff.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::Server { .. } | Self::RateLimited(_)
        )
    }

    /// Whether the error indicates poison data that should go to the DLQ
    /// rather than be retried.
    pub fn is_poison(&self) -> bool {
        matches!(self, Self::Client { .. } | Self::Auth { .. })
    }
}
