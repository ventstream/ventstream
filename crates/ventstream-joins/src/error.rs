//! Error types for the join engine.

use thiserror::Error;

/// Errors produced while applying a join.
///
/// Join errors are fatal to the current pipeline iteration. Continuing would
/// allow source checkpoints to move past state that was never composed or
/// persisted, so the supervisor must surface the failure and restart only
/// under its explicit retry policy.
#[derive(Debug, Error)]
pub enum JoinError {
    /// A required column was missing from an event payload.
    #[error("missing required column '{column}' on event from '{table}'")]
    MissingColumn {
        /// Table the event came from.
        table: String,
        /// Column the join definition expected to find on the event.
        column: String,
    },

    /// Event payload could not be decoded as a JSON object.
    #[error("event from '{table}' does not deserialize as a JSON object: {detail}")]
    InvalidPayload {
        /// Table the event came from.
        table: String,
        /// Diagnostic detail.
        detail: String,
    },

    /// The synchronous backfill fetcher returned an error.
    #[error("fetcher error while resolving '{related_id}': {source}")]
    Fetcher {
        /// Id of the related entry being resolved.
        related_id: String,
        /// Underlying error.
        #[source]
        source: super::fetcher::FetchError,
    },

    /// A subject didn't look like a CDC event subject
    /// (`{kind}.{namespace}.{relation}.{op}`).
    #[error("malformed CDC subject: '{subject}'")]
    MalformedSubject {
        /// The unparseable subject.
        subject: String,
    },

    /// Internal invariant violated.
    #[error("join engine internal error: {0}")]
    Internal(String),
}
