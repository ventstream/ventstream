//! Error types for the join engine.

use thiserror::Error;

/// Errors produced while applying a join.
///
/// Two kinds, distinguished by [`JoinError::is_permanent`]:
///
/// - **Permanent** — the event itself cannot be joined, and replaying it can
///   only fail the same way (malformed subject, undecodable payload, a row
///   missing what the join definition requires). When the runtime provides
///   a [`PoisonSink`](crate::PoisonSink) the engine quarantines the event
///   there and advances past it; without one it is fatal, as below.
/// - **Transient** — the environment failed, not the event (a fetcher that
///   could not reach the source, a persistence failure). These are fatal to
///   the current pipeline iteration: continuing would let source checkpoints
///   move past state that was never composed or persisted, so the supervisor
///   surfaces the failure and restarts under its explicit retry policy.
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

impl JoinError {
    /// True when the failure is a property of the event itself, so replaying
    /// the same event can only produce the same error.
    ///
    /// These are the per-row defects a poison sink exists for: without one,
    /// a single such row halts the iteration, the checkpoint never advances,
    /// and every restart replays it — an unrecoverable crash loop from one
    /// bad record.
    ///
    /// Everything else stays transient on purpose. A fetcher failure is about
    /// the source's availability or grants, not the event — the same event
    /// composes fine once the source is back — and an internal error is an
    /// invariant violation the engine cannot reason about per row. Dead-
    /// lettering either would drop a row that would have succeeded, which
    /// is the worse failure: data silently missing from the sink.
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::MissingColumn { .. }
            | Self::InvalidPayload { .. }
            | Self::MalformedSubject { .. } => true,
            Self::Fetcher { .. } | Self::Internal(_) => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::fetcher::FetchError;

    #[test]
    fn event_defects_are_permanent() {
        assert!(JoinError::InvalidPayload {
            table: "public.orders".into(),
            detail: "expected value at line 1".into(),
        }
        .is_permanent());
        assert!(JoinError::MalformedSubject {
            subject: "not.a.cdc.subject.at.all".into(),
        }
        .is_permanent());
        assert!(JoinError::MissingColumn {
            table: "public.orders".into(),
            column: "customer_id".into(),
        }
        .is_permanent());
    }

    /// The direction that matters more: a fetcher outage must never be
    /// dead-lettered, or the row it would have composed goes missing.
    #[test]
    fn environment_failures_stay_transient() {
        assert!(!JoinError::Fetcher {
            related_id: "customer".into(),
            source: FetchError::Unreachable("connection refused".into()),
        }
        .is_permanent());
        assert!(!JoinError::Fetcher {
            related_id: "customer".into(),
            source: FetchError::Query {
                table: "public.customers".into(),
                message: "db error (SQLSTATE 42501): permission denied for table customers".into(),
            },
        }
        .is_permanent());
        assert!(!JoinError::Internal("persistence flush: disk full".into()).is_permanent());
    }
}
