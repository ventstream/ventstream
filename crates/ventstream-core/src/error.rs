//! Error types for the core crate and the trait contracts it exposes.
//!
//! Per-concern enums (rather than one mega-error) so each adapter can
//! enumerate only the failure modes it actually produces.

use thiserror::Error;

/// Errors produced while constructing or validating core types.
#[derive(Debug, Error)]
pub enum CoreError {
    /// An [`crate::EventId`] string failed to parse.
    #[error("invalid event id: {0}")]
    InvalidEventId(String),

    /// A [`crate::SourceUri`] failed validation.
    #[error("invalid source URI: {0}")]
    InvalidSourceUri(String),

    /// A [`crate::Subject`] failed validation.
    #[error("invalid subject: {0}")]
    InvalidSubject(String),
}

/// Errors that arise during event publication into the [`crate::EventBus`].
#[derive(Debug, Error)]
pub enum BackpressureError {
    /// The bus is at capacity and the caller asked for a non-blocking publish.
    #[error("event bus is at capacity (capacity={capacity})")]
    Full {
        /// Configured capacity of the bus.
        capacity: usize,
    },

    /// The byte-weighted memory budget has no immediate capacity.
    #[error(
        "event memory budget is full (requested={requested_bytes}, used={used_bytes}, limit={limit_bytes})"
    )]
    MemoryFull {
        /// Estimated bytes required by this event.
        requested_bytes: u64,
        /// Bytes currently reserved by retained events.
        used_bytes: u64,
        /// Effective admission limit for the current pressure state.
        limit_bytes: u64,
    },

    /// One event exceeds the configured safety ceiling.
    #[error("event is too large (estimated={estimated_bytes}, max={max_bytes})")]
    EventTooLarge {
        /// Conservative event memory estimate.
        estimated_bytes: u64,
        /// Maximum accepted event estimate.
        max_bytes: u64,
    },

    /// All receivers have dropped; the bus is closed.
    #[error("event bus is closed: no active receivers")]
    Closed,

    /// The shutdown token was cancelled before the publish could complete.
    #[error("publish cancelled by shutdown")]
    Cancelled,
}

/// Errors returned by [`crate::Source::run`].
#[derive(Debug, Error)]
pub enum SourceError {
    /// The source could not establish or maintain its upstream connection.
    #[error("source connection failed: {0}")]
    Connection(String),

    /// The upstream produced data the source could not decode.
    #[error("source decode failed: {0}")]
    Decode(String),

    /// Publishing into the bus failed.
    #[error("publish failed: {0}")]
    Publish(#[from] BackpressureError),

    /// A non-recoverable internal error.
    #[error("source internal error: {0}")]
    Internal(String),
}

/// One event's failure within a batch of [`SinkError::Rejected`].
///
/// Carrying the per-item error (rather than a single shared
/// `sample_error`) means the DLQ records the exact reason each event
/// was rejected — critical for diagnosing schema/mapping issues that
/// affect specific rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedItem {
    /// Zero-based offset of the failed event within the batch the sink
    /// received.
    pub offset: usize,
    /// Per-event diagnostic from the downstream.
    pub error: String,
}

/// Errors returned by [`crate::Sink::write`].
#[derive(Debug, Error)]
pub enum SinkError {
    /// The sink could not reach its target.
    #[error("sink connection failed: {0}")]
    Connection(String),

    /// The downstream rejected one or more events in the batch.
    #[error("downstream rejected {rejected_count} of {batch_size} events: {message}")]
    Rejected {
        /// Total events in the failed batch.
        batch_size: usize,
        /// Number of events the downstream rejected.
        rejected_count: usize,
        /// Summary diagnostic — typically the first per-item error or
        /// the whole-batch HTTP message. Used for log lines.
        message: String,
        /// Per-item failure details with their own error strings.
        /// `None` means the sink could not enumerate per-item failures
        /// (treat as whole-batch failure). `Some(items)` lets the engine
        /// route exactly those events to the DLQ with each event's
        /// specific rejection reason.
        failed_items: Option<Vec<FailedItem>>,
    },

    /// Request timed out.
    #[error("sink request timed out after {millis}ms")]
    Timeout {
        /// Duration in milliseconds.
        millis: u64,
    },

    /// A non-recoverable internal error.
    #[error("sink internal error: {0}")]
    Internal(String),
}
