//! Tiny template engine used by the manifest.
//!
//! Two flavors of substitution:
//!
//! - **Subject templates** like `orders.order.status_changed.{args.orderId}`
//!   only support `{args.NAME}` substitutions; bad refs are rejected.
//! - **Source expressions** on inline fields support:
//!     * `{args.NAME}` — value from the subscription's args
//!     * `$data.PATH` — extract from the event's `data` JSON (dot
//!       path inside `data`; e.g. `$data.from`, `$data.nested.field`)
//!     * `$event.FIELD` — pull from the envelope: `id`, `tenant`,
//!       `event`, `entityId`, `occurredAt`, `receivedAt`, `subject`
//!     * A literal expression with no special tokens is passed
//!       through as a string constant.
//!
//! These are intentionally minimal — anything richer (default
//! values, type coercion expressions, conditionals) is out of scope
//! for v1.

use std::collections::HashMap;

use serde_json::Value;
use thiserror::Error;

/// Errors from template parsing / expansion.
#[derive(Debug, Error)]
pub(crate) enum TemplateError {
    /// A `{args.NAME}` placeholder referenced an arg the
    /// subscription doesn't declare.
    #[error("template references unknown arg '{arg}'")]
    UnknownArg {
        /// Name that was referenced.
        arg: String,
    },

    /// Source expression referenced an envelope field that doesn't
    /// exist (`$event.FOO` for an unknown `FOO`).
    #[error("template references unknown envelope field '{field}'")]
    UnknownEvent {
        /// Field that was referenced.
        field: String,
    },
}

/// Expand a subject template against the supplied arg map. Returns
/// the resolved subject string.
pub(crate) fn expand_subject(
    template: &str,
    args: &HashMap<String, String>,
) -> Result<String, TemplateError> {
    expand_placeholders(template, |name| match args.get(name) {
        Some(v) => Ok(v.clone()),
        None => Err(TemplateError::UnknownArg {
            arg: name.to_owned(),
        }),
    })
}

/// Resolve a source expression to a JSON value, given the args map
/// and the published event.
pub(crate) fn resolve_source(
    expr: &str,
    args: &HashMap<String, String>,
    event: &ventstream_protocol::Event,
    nats_subject: &str,
) -> Result<Value, TemplateError> {
    let trimmed = expr.trim();
    if let Some(rest) = trimmed.strip_prefix("$data.") {
        Ok(extract_json_path(&event.data, rest))
    } else if let Some(rest) = trimmed.strip_prefix("$event.") {
        envelope_field(event, nats_subject, rest)
    } else if trimmed.contains("{args.") {
        // Single placeholder or literal-with-placeholders → string.
        let s = expand_placeholders(trimmed, |name| match args.get(name) {
            Some(v) => Ok(v.clone()),
            None => Err(TemplateError::UnknownArg {
                arg: name.to_owned(),
            }),
        })?;
        Ok(Value::String(s))
    } else {
        // Literal string constant.
        Ok(Value::String(trimmed.to_owned()))
    }
}

fn expand_placeholders<F>(template: &str, mut resolver: F) -> Result<String, TemplateError>
where
    F: FnMut(&str) -> Result<String, TemplateError>,
{
    const SENTINEL: &str = "{args.";
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find(SENTINEL) {
        // Copy text up to the placeholder verbatim.
        if let Some(prefix) = rest.get(..start) {
            out.push_str(prefix);
        }
        // Skip the sentinel.
        let after_sentinel = rest.get(start + SENTINEL.len()..).unwrap_or("");
        // Find the closing brace inside the placeholder.
        match after_sentinel.find('}') {
            Some(end) => {
                let arg_name = after_sentinel.get(..end).unwrap_or("");
                let value = resolver(arg_name)?;
                out.push_str(&value);
                rest = after_sentinel.get(end + 1..).unwrap_or("");
            }
            None => {
                // Unterminated placeholder — emit the rest verbatim
                // (lenient — operator-typo recovery; the bus
                // subscriber will refuse the bogus subject anyway).
                if let Some(tail) = rest.get(start..) {
                    out.push_str(tail);
                }
                rest = "";
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// Drill into a serde_json::Value with a dot-separated path.
fn extract_json_path(v: &Value, path: &str) -> Value {
    let mut cur = v;
    for segment in path.split('.') {
        match cur {
            Value::Object(map) => match map.get(segment) {
                Some(next) => cur = next,
                None => return Value::Null,
            },
            _ => return Value::Null,
        }
    }
    cur.clone()
}

/// Look up a field on the envelope. The set of supported names is
/// fixed (and intentionally small) so typos surface as errors.
fn envelope_field(
    event: &ventstream_protocol::Event,
    nats_subject: &str,
    field: &str,
) -> Result<Value, TemplateError> {
    let v = match field {
        "id" => Value::String(event.id.to_string()),
        "tenant" => Value::String(event.tenant.clone()),
        "occurredAt" | "occurred_at" => Value::String(event.occurred_at.to_rfc3339()),
        "receivedAt" | "received_at" => Value::String(event.received_at.to_rfc3339()),
        // `type` kept as an alias for the event name for older templates.
        "event" | "type" => Value::String(event.event.clone()),
        "entityId" | "entity_id" => Value::String(event.entity_id.clone()),
        "subject" => Value::String(nats_subject.to_owned()),
        "schemaVersion" | "schema_version" => {
            Value::Number(serde_json::Number::from(event.schema_version))
        }
        other => {
            return Err(TemplateError::UnknownEvent {
                field: other.to_owned(),
            })
        }
    };
    Ok(v)
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
    use chrono::Utc;
    use ventstream_protocol::{Event, Metadata};

    fn sample_event() -> Event {
        Event::publish(
            "acme",
            "orders.order.statusChanged",
            "order_123",
            None,
            Utc::now(),
            serde_json::json!({"from": "pending", "to": "confirmed"}),
            Metadata::default(),
        )
        .unwrap()
    }

    #[test]
    fn expands_subject_with_args() {
        let mut args = HashMap::new();
        args.insert("orderId".into(), "o1".into());
        let s = expand_subject("orders.order.status_changed.{args.orderId}", &args).unwrap();
        assert_eq!(s, "orders.order.status_changed.o1");
    }

    #[test]
    fn rejects_missing_arg() {
        let args = HashMap::new();
        let err = expand_subject("orders.order.status_changed.{args.orderId}", &args).unwrap_err();
        assert!(matches!(err, TemplateError::UnknownArg { .. }));
    }

    #[test]
    fn resolves_data_path() {
        let ev = sample_event();
        let v = resolve_source(
            "$data.from",
            &HashMap::new(),
            &ev,
            "vs.t.acme.orders.order.status_changed.o1",
        )
        .unwrap();
        assert_eq!(v, Value::String("pending".into()));
    }

    #[test]
    fn resolves_event_field() {
        let ev = sample_event();
        let mut args = HashMap::new();
        args.insert("orderId".into(), "o1".into());
        let v = resolve_source("$event.tenant", &args, &ev, "vs.t.acme.x").unwrap();
        assert_eq!(v, Value::String("acme".into()));
    }

    #[test]
    fn resolves_arg_placeholder() {
        let ev = sample_event();
        let mut args = HashMap::new();
        args.insert("orderId".into(), "o1".into());
        let v = resolve_source("{args.orderId}", &args, &ev, "x").unwrap();
        assert_eq!(v, Value::String("o1".into()));
    }

    #[test]
    fn unknown_envelope_field_errors() {
        let ev = sample_event();
        let err = resolve_source("$event.nope", &HashMap::new(), &ev, "x").unwrap_err();
        assert!(matches!(err, TemplateError::UnknownEvent { .. }));
    }
}
