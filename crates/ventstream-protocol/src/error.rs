//! Protocol-level errors. These fire at the boundary — when an SDK
//! tries to build an invalid event, or when the engine receives a
//! malformed envelope from the bus. Internal engine code does not
//! produce these.

use thiserror::Error;

/// Errors produced when validating or parsing protocol values.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// A subject string did not match the 7-segment grammar.
    #[error("invalid subject '{subject}': {reason}")]
    InvalidSubject {
        /// The offending input.
        subject: String,
        /// Specific reason the grammar rejected it.
        reason: String,
    },

    /// A subject pattern was structurally malformed (e.g. `>` not at
    /// the end, empty segment, illegal character in a literal).
    #[error("invalid subject pattern '{pattern}': {reason}")]
    InvalidSubjectPattern {
        /// The offending pattern.
        pattern: String,
        /// Specific reason the parser rejected it.
        reason: String,
    },

    /// An identifier segment (tenant, domain, entity kind/id, action)
    /// did not match its character class.
    #[error("invalid identifier in '{field}': {value} ({reason})")]
    InvalidIdentifier {
        /// Which named slot the bad value was found in.
        field: &'static str,
        /// The offending value.
        value: String,
        /// What the character-class rule expected.
        reason: &'static str,
    },

    /// An event-type string did not match its expected shape.
    /// (Legacy variant retained for compatibility; the current grammar
    /// validates event names via the identifier rules.)
    #[error("invalid event type '{event_type}': {reason}")]
    InvalidEventType {
        /// The offending input.
        event_type: String,
        /// Specific reason the parser rejected it.
        reason: String,
    },

    /// An [`Event`](crate::Event) failed envelope-level validation
    /// (e.g. tenant in subject doesn't match tenant in envelope).
    #[error("invalid event: {reason}")]
    InvalidEvent {
        /// What was wrong with the event.
        reason: String,
    },
}
