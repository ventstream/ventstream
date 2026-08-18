//! Error taxonomy for the SurrealDB sink.
//!
//! Two failure layers: the HTTP exchange and per-statement query results.
//! SurrealDB commits synchronously, so a successful response IS delivery
//! confirmation — there is no asynchronous task layer. Optimistic
//! transaction conflicts are the load-bearing transient case: concurrent
//! writers genuinely collide and the whole run must retry (idempotent by
//! record id). Unknown statement failures block delivery rather than
//! poison the DLQ.

use thiserror::Error;

/// Failures surfaced by the SurrealDB sink.
#[derive(Debug, Error)]
pub enum SurrealSinkError {
    /// Transport-level failure (connect, DNS, TLS, timeout).
    #[error("surrealdb transport error: {0}")]
    Transport(String),

    /// Retryable server response (5xx / 408 / 429).
    #[error("surrealdb server error HTTP {status}: {message}")]
    Server {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// Authentication or authorization rejection (401/403).
    #[error("surrealdb auth error HTTP {status}: {message}")]
    Auth {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// Non-retryable client error (other 4xx).
    #[error("surrealdb client error HTTP {status}: {message}")]
    Client {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// A statement failed with a retryable engine condition
    /// (transaction conflict, resource pressure).
    #[error("surrealdb statement failed transiently: {0}")]
    QueryTransient(String),

    /// A statement failed with a document/configuration error. Blocks
    /// delivery; the run's events stay pending for the operator.
    #[error("surrealdb statement rejected: {0}")]
    QueryBlocked(String),

    /// Client-side per-document failures with exact batch offsets.
    #[error("{failed_count} of {batch_size} events rejected: {sample_error}")]
    PartialFailure {
        /// Total events in the batch.
        batch_size: usize,
        /// Rejected event count.
        failed_count: usize,
        /// First rejection reason, for log lines.
        sample_error: String,
        /// Exact per-item failures.
        failed_items: Vec<ventstream_core::FailedItem>,
    },

    /// Unexpected response shape from the server.
    #[error("surrealdb malformed response: {0}")]
    MalformedResponse(String),

    /// A non-recoverable internal error.
    #[error("surrealdb sink internal error: {0}")]
    Internal(String),
}

impl SurrealSinkError {
    /// Whether the whole request should be retried with backoff.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_) | Self::Server { .. } | Self::QueryTransient(_)
        )
    }
}

/// Statement error texts that indicate engine-level contention rather
/// than a problem with the documents. SurrealDB's optimistic concurrency
/// surfaces conflicts as failed transactions that succeed on replay.
/// Conservative: everything not matched here blocks delivery.
pub(super) fn is_transient_query_error(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("transaction conflict")
        || lowered.contains("failed to commit")
        || lowered.contains("conflict occurred")
        || lowered.contains("transaction was retried")
        || lowered.contains("resource temporarily unavailable")
        || lowered.contains("timed out")
        || lowered.contains("too many requests")
}

/// Truncate an HTTP or statement error body for diagnostics.
pub(super) fn truncate_body(body: &str) -> String {
    const MAX: usize = 1024;
    if body.len() <= MAX {
        body.to_owned()
    } else {
        let mut end = MAX;
        while end > 0 && !body.is_char_boundary(end) {
            end -= 1;
        }
        format!(
            "{}… ({} bytes)",
            body.get(..end).unwrap_or_default(),
            body.len()
        )
    }
}
