//! Error taxonomy for the Meilisearch sink.
//!
//! Two failure layers: the HTTP exchange (enqueue and task polls) and the
//! asynchronous task outcome. Unknown failures block delivery rather than
//! poison the DLQ.

use std::time::Duration;

use thiserror::Error;

/// Failures surfaced by the Meilisearch sink.
#[derive(Debug, Error)]
pub enum MeilisearchSinkError {
    /// Transport-level failure (connect, DNS, TLS, timeout).
    #[error("meilisearch transport error: {0}")]
    Transport(String),

    /// Retryable server response (5xx / 408).
    #[error("meilisearch server error HTTP {status}: {message}")]
    Server {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// HTTP 429 with an optional Retry-After hint.
    #[error("meilisearch rate limited: {message}")]
    RateLimited {
        /// Truncated response body.
        message: String,
        /// Server-provided retry delay.
        retry_after: Option<Duration>,
    },

    /// Authentication or authorization rejection (401/403).
    #[error("meilisearch auth error HTTP {status}: {message}")]
    Auth {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// Non-retryable client error (other 4xx).
    #[error("meilisearch client error HTTP {status}: {message}")]
    Client {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        message: String,
    },

    /// A task reached `failed` with a retryable (capacity) error code.
    #[error("meilisearch task failed transiently ({code}): {message}")]
    TaskTransient {
        /// Meilisearch error code.
        code: String,
        /// Task error message.
        message: String,
    },

    /// A task reached `failed` with a document/configuration error code.
    /// v1 blocks delivery on these; bisection-based DLQ isolation is a
    /// planned follow-up.
    #[error("meilisearch task failed ({code}): {message}")]
    TaskBlocked {
        /// Meilisearch error code.
        code: String,
        /// Task error message.
        message: String,
    },

    /// A task was canceled server-side; the batch will be retried.
    #[error("meilisearch task {task_uid} was canceled")]
    TaskCanceled {
        /// Canceled task uid.
        task_uid: u64,
    },

    /// No terminal task status within the configured deadline.
    #[error("meilisearch task {task_uid} not terminal after {millis}ms")]
    TaskTimeout {
        /// Polled task uid.
        task_uid: u64,
        /// Deadline in milliseconds.
        millis: u64,
    },

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
    #[error("meilisearch malformed response: {0}")]
    MalformedResponse(String),

    /// A non-recoverable internal error.
    #[error("meilisearch sink internal error: {0}")]
    Internal(String),
}

impl MeilisearchSinkError {
    /// Whether the whole request should be retried with backoff.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Transport(_)
                | Self::Server { .. }
                | Self::RateLimited { .. }
                | Self::TaskTransient { .. }
                | Self::TaskCanceled { .. }
        )
    }

    /// Server-provided retry delay, when present.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Task error codes that indicate server capacity pressure rather than a
/// problem with the documents. Conservative: everything not listed here or
/// in the transient set blocks delivery.
pub(super) fn is_transient_task_code(code: &str) -> bool {
    matches!(
        code,
        "no_space_left_on_device" | "task_queue_full" | "internal" | "index_creation_failed"
    )
}

/// Truncate an HTTP error body for diagnostics.
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
