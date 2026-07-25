//! The wire protocol between WebSocket clients and the gateway.
//!
//! Every frame is a single JSON object discriminated by `type`. The
//! client sends `hello` → `subscribe`/`unsubscribe`/`ping`; the server
//! responds with `ready` → `subscribed`/`unsubscribed`/`event`/`error`/`pong`.
//!
//! The protocol is intentionally tiny — anything richer (binary
//! payloads, batched subscriptions, presence) lives in v2.

use std::sync::Arc;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use ventstream_realtime::Cursor;

#[derive(Deserialize)]
#[serde(untagged)]
enum SequenceInput {
    Number(u64),
    String(String),
}

fn deserialize_resume_cursor<'de, D>(deserializer: D) -> Result<Option<Cursor>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<SequenceInput>::deserialize(deserializer)? {
        None => Ok(None),
        Some(SequenceInput::Number(value)) => Ok(Some(Cursor::jetstream(value))),
        Some(SequenceInput::String(value)) => Cursor::from_wire_compatible(&value)
            .map(Some)
            .map_err(D::Error::custom),
    }
}

/// Messages sent from a WebSocket client to the gateway.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ClientMessage {
    /// First message after connection. Establishes the tenant scope
    /// for this connection. The token is validated by the gateway
    /// (JWT validation lands as a follow-up; v1 trusts the tenant
    /// field if the token is non-empty for demo purposes).
    Hello {
        /// Tenant identifier the connection wants to scope to.
        tenant: String,
        /// Auth token. v1: any non-empty string. v2: JWT validation.
        token: String,
        /// Legacy JetStream resume sequence. New clients should use
        /// `resume_from_cursor`. Absent/`None` means live-only.
        #[serde(default, deserialize_with = "deserialize_resume_cursor")]
        resume_from_seq: Option<Cursor>,
        /// Provider-neutral resume cursor. New clients should use this field;
        /// `resume_from_seq` remains accepted for compatibility.
        #[serde(default, deserialize_with = "deserialize_resume_cursor")]
        resume_from_cursor: Option<Cursor>,
    },

    /// Subscribe to a subject pattern in *unanchored* form (the
    /// gateway prepends `vs.t.{tenant}.` from the connection's
    /// established scope).
    Subscribe {
        /// Client-chosen subscription ID — echoed back in
        /// `Subscribed`/`Unsubscribed` events and event deliveries
        /// so the client can route incoming events to handlers.
        id: String,
        /// Subject pattern, unanchored — e.g.
        /// `"orders.order.status_changed.*"`.
        pattern: String,
    },

    /// Remove a previously-installed subscription.
    Unsubscribe {
        /// The subscription ID from the original `Subscribe`.
        id: String,
    },

    /// Application-level ping. The gateway also sends WebSocket-level
    /// pings; this is for clients that prefer a JSON heartbeat.
    Ping,
}

/// Messages sent from the gateway to a WebSocket client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    /// First message after `Hello` succeeds. Acks the tenant scope
    /// and hands the client its assigned connection ID (useful for
    /// log correlation).
    Ready {
        /// ULID identifying this connection.
        connection_id: String,
        /// Legacy JetStream sequence used to resume this session. Omitted for
        /// live-only sessions and non-JetStream providers.
        #[serde(skip_serializing_if = "Option::is_none")]
        resumed_from: Option<u64>,
        /// Provider-neutral cursor used to open this resumed session.
        #[serde(skip_serializing_if = "Option::is_none")]
        resumed_from_cursor: Option<String>,
    },

    /// Confirms a `Subscribe`. The `anchored_pattern` is the
    /// internal form (with tenant prefix) — clients usually ignore
    /// it; we surface it so debugging tools can match log lines to
    /// NATS subjects.
    Subscribed {
        /// Client-supplied subscription ID.
        id: String,
        /// Internal anchored form, for diagnostics.
        anchored_pattern: String,
    },

    /// Confirms an `Unsubscribe`.
    Unsubscribed {
        /// Client-supplied subscription ID.
        id: String,
    },

    /// Delivery of a matched event.
    ///
    /// Serialized via [`ServerMessage::to_text`], not the derived
    /// `Serialize` — see that method for why `event_json` is spliced
    /// raw rather than re-encoded.
    Event {
        /// The NATS subject the event arrived on (anchored form).
        subject: String,
        /// The subscription IDs on this connection that matched.
        /// Multiple subscriptions may match the same event; we
        /// deliver once and list which IDs matched so the client can
        /// fan out to its own handlers.
        matched: Vec<String>,
        /// The event envelope **already serialized to JSON**, shared
        /// across the whole fan-out. This is the raw bytes that
        /// arrived off the bus (the publisher's JSON is reused
        /// verbatim), so a hot subject with N subscribers serializes
        /// the event **zero** extra times — `to_text` splices these
        /// bytes straight into the frame. `Arc<str>` so the fan-out
        /// shares one allocation.
        #[serde(skip)]
        event_json: Arc<str>,
        /// Provider-neutral resume cursor. `None` in ephemeral Core mode.
        #[serde(skip)]
        cursor: Option<Cursor>,
        /// Legacy JetStream numeric sequence. Omitted for other providers.
        seq: Option<u64>,
    },

    /// Application-level error frame.
    Error {
        /// Stable machine-readable code.
        code: ErrorCode,
        /// Human-readable detail.
        message: String,
    },

    /// Reply to a `Ping`.
    Pong,
}

impl ServerMessage {
    /// Serialize this message to the JSON text frame sent on the wire.
    ///
    /// The `Event` variant is special-cased: its `event_json` is
    /// already-serialized JSON (the bytes that arrived off the bus),
    /// so we splice it directly rather than decode-and-re-encode the
    /// envelope. Only the tiny per-connection parts (`subject`,
    /// `matched`) are encoded here — under a hot fan-out this turns N
    /// full event serializations into N cheap string assemblies. All
    /// other variants are small control frames and use the derived
    /// `Serialize`.
    pub(crate) fn to_text(&self) -> String {
        match self {
            ServerMessage::Event {
                subject,
                matched,
                event_json,
                cursor,
                seq,
            } => {
                // `subject`/`matched` are short; encoding them is cheap
                // and handles JSON escaping correctly. `event_json` is
                // trusted valid JSON (it round-tripped through
                // `Event::validate` on the way in).
                let subject = serde_json::to_string(subject).unwrap_or_else(|_| "\"\"".to_owned());
                let matched = serde_json::to_string(matched).unwrap_or_else(|_| "[]".to_owned());
                let cursor = cursor
                    .as_ref()
                    .map(Cursor::to_compatible_wire)
                    .or_else(|| seq.map(|value| value.to_string()));
                let cursor = cursor
                    .as_ref()
                    .and_then(|value| serde_json::to_string(value).ok())
                    .map(|value| format!(r#","cursor":{value}"#))
                    .unwrap_or_default();
                let sequence = seq
                    .map(|value| format!(r#","seq":{value}"#))
                    .unwrap_or_default();
                format!(
                    r#"{{"type":"event","subject":{subject},"matched":{matched}{cursor}{sequence},"event":{event_json}}}"#
                )
            }
            other => serde_json::to_string(other).unwrap_or_else(|err| {
                format!(r#"{{"type":"error","message":"serialize: {err}"}}"#)
            }),
        }
    }
}

/// Stable error codes the gateway emits in `Error` frames.
///
/// Some variants are not yet emitted on any code path but live in the
/// protocol surface so SDKs can pre-implement handling — adding new
/// codes later is a breaking change for clients.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ErrorCode {
    /// The client sent a frame that the gateway could not parse.
    BadFrame,
    /// The client tried to act before sending `Hello`.
    NotReady,
    /// The auth handshake (`Hello`) failed.
    AuthFailed,
    /// A subscribe pattern was malformed.
    BadPattern,
    /// The connection's outbound mailbox overflowed — the client is
    /// being disconnected as a slow consumer.
    SlowConsumer,
    /// The requested cursor has fallen outside stream retention.
    ResumeExpired,
    /// The requested cursor is invalid, belongs to another provider, or is
    /// ahead of this stream.
    InvalidCursor,
    /// Generic catch-all for internal errors. Detail in `message`.
    Internal,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::{ClientMessage, ServerMessage};
    use std::sync::Arc;
    use ventstream_realtime::Cursor;

    #[test]
    fn resume_sequence_accepts_decimal_string_and_legacy_number() {
        for (input, expected) in [(r#""42""#, 42), ("42", 42)] {
            let message: ClientMessage = serde_json::from_str(&format!(
                r#"{{"type":"hello","tenant":"acme","token":"demo","resume_from_seq":{input}}}"#
            ))
            .expect("valid hello");
            match message {
                ClientMessage::Hello {
                    resume_from_seq, ..
                } => assert_eq!(resume_from_seq, Some(Cursor::jetstream(expected))),
                _ => panic!("expected hello"),
            }
        }
    }

    #[test]
    fn malformed_resume_sequence_is_rejected() {
        let result = serde_json::from_str::<ClientMessage>(
            r#"{"type":"hello","tenant":"acme","token":"demo","resume_from_seq":"nope"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn event_cursor_preserves_full_u64_precision() {
        let message = ServerMessage::Event {
            subject: "vs.t.acme.orders.updated".to_owned(),
            matched: vec!["orders".to_owned()],
            event_json: Arc::from(r#"{"id":"event-1"}"#),
            cursor: Some(Cursor::jetstream(u64::MAX)),
            seq: Some(u64::MAX),
        };

        let frame: serde_json::Value =
            serde_json::from_str(&message.to_text()).expect("valid event frame");
        assert_eq!(
            frame.get("cursor").and_then(serde_json::Value::as_str),
            Some(u64::MAX.to_string().as_str())
        );
        assert_eq!(
            frame.get("seq").and_then(serde_json::Value::as_u64),
            Some(u64::MAX)
        );
    }

    #[test]
    fn redis_event_cursor_is_prefixed_and_has_no_numeric_sequence() {
        let message = ServerMessage::Event {
            subject: "vs.t.acme.orders.updated".to_owned(),
            matched: vec!["orders".to_owned()],
            event_json: Arc::from(r#"{"id":"event-1"}"#),
            cursor: Some(Cursor::redis_streams(1712345678901, 7)),
            seq: None,
        };

        let frame: serde_json::Value =
            serde_json::from_str(&message.to_text()).expect("valid event frame");
        assert_eq!(
            frame.get("cursor").and_then(serde_json::Value::as_str),
            Some("rs:1712345678901-7")
        );
        assert!(frame.get("seq").is_none());
    }
}
