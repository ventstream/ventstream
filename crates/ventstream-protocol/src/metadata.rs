//! Tracing/correlation hints attached to every event.

use serde::{Deserialize, Serialize};

/// Tracing metadata. All fields optional — present when the publisher
/// has them, absent otherwise. Consumers should treat missing fields as
/// "unknown," not as an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// OpenTelemetry trace ID (lowercase hex, 32 chars) — ties this
    /// event to a distributed trace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,

    /// Application-defined correlation ID — ties events that belong to
    /// the same logical operation (a user request, a workflow run).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// The event ID that caused this one. Useful for reconstructing
    /// causal chains in audit and replay tooling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn empty_metadata_serializes_to_empty_object() {
        let m = Metadata::default();
        assert_eq!(serde_json::to_string(&m).unwrap(), "{}");
    }

    #[test]
    fn partial_serializes_without_nulls() {
        let m = Metadata {
            trace_id: Some("abc".into()),
            correlation_id: None,
            causation_id: None,
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(!j.contains("null"));
        assert!(j.contains("trace_id"));
    }

    #[test]
    fn deserialize_rejects_unknown() {
        let r: Result<Metadata, _> = serde_json::from_str(r#"{"x":"y"}"#);
        assert!(r.is_err());
    }
}
