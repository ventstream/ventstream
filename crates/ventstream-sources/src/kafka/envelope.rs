//! Parse a Kafka message (key + value bytes) into a mapped change.
//!
//! Debezium mode unwraps the change envelope (`before`/`after`/`source`/`op`);
//! raw mode treats the value as the document. Both handle the JSON-Converter
//! schema wrapper (`{schema, payload}`) and null-value tombstones. Pure — no
//! rdkafka or I/O types, fully unit-tested.

use serde_json::Value;
use ventstream_core::doc_id::component_text;

use super::config::{KafkaCdcConfig, UnwrapMode};
use crate::error::KafkaCdcError;

/// The operation a message represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    pub(crate) fn suffix(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
    pub(crate) fn is_delete(self) -> bool {
        matches!(self, Self::Delete)
    }
}

/// A message mapped to a change. `body` is `None` for deletes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mapped {
    pub(crate) namespace: String,
    pub(crate) relation: String,
    pub(crate) op: Op,
    pub(crate) pk: Vec<String>,
    pub(crate) body: Option<Value>,
    pub(crate) occurred_at_ms: Option<i64>,
}

/// Outcome of mapping a message.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Mapping {
    /// A change to publish.
    Event(Box<Mapped>),
    /// Nothing to publish (null-value tombstone, truncate, heartbeat).
    Skip,
}

/// Map a message according to the configured unwrap mode.
pub(crate) fn map(
    config: &KafkaCdcConfig,
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<Mapping, KafkaCdcError> {
    match config.unwrap {
        UnwrapMode::Debezium => map_debezium(config, topic, key, value),
        UnwrapMode::Raw => map_raw(config, topic, key, value),
    }
}

fn map_debezium(
    config: &KafkaCdcConfig,
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<Mapping, KafkaCdcError> {
    // Null value = log-compaction tombstone trailing a delete we already
    // handled via the op=d event. Skip it.
    let Some(value) = value else {
        return Ok(Mapping::Skip);
    };
    let value: Value = parse_json(value)?;
    let payload = unwrap_payload(&value);

    let op = payload.get("op").and_then(Value::as_str).unwrap_or("");
    let op = match op {
        "c" | "r" | "u" => {
            if op == "u" {
                Op::Update
            } else {
                Op::Insert
            }
        }
        "d" => Op::Delete,
        // t (truncate), m (message/heartbeat), or unknown — nothing to index.
        _ => return Ok(Mapping::Skip),
    };

    let source = payload.get("source");
    let relation = source
        .and_then(|s| field_str(s, "table").or_else(|| field_str(s, "collection")))
        .or_else(|| relation_from_topic(topic))
        .ok_or_else(|| {
            KafkaCdcError::MalformedEvent(format!("no table/collection in source (topic {topic})"))
        })?;
    let namespace = config
        .namespace_override
        .clone()
        .or_else(|| source.and_then(|s| field_str(s, "schema").or_else(|| field_str(s, "db"))))
        .unwrap_or_else(|| relation.clone());

    let pk = key_components(key)?;
    if pk.is_empty() {
        return Err(KafkaCdcError::MalformedEvent(format!(
            "message on {topic} has no key; cannot form a doc id"
        )));
    }

    let body = if op.is_delete() {
        None
    } else {
        Some(extract_body(payload, "after").ok_or_else(|| {
            KafkaCdcError::MalformedEvent(format!(
                "op {} on {topic} has no `after` image",
                op.suffix()
            ))
        })?)
    };

    let occurred_at_ms = source
        .and_then(|s| s.get("ts_ms"))
        .or_else(|| payload.get("ts_ms"))
        .and_then(Value::as_i64);

    Ok(Mapping::Event(Box::new(Mapped {
        namespace,
        relation,
        op,
        pk,
        body,
        occurred_at_ms,
    })))
}

fn map_raw(
    config: &KafkaCdcConfig,
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<Mapping, KafkaCdcError> {
    let relation = relation_from_topic(topic).unwrap_or_else(|| topic.to_owned());
    let namespace = config
        .namespace_override
        .clone()
        .unwrap_or_else(|| relation.clone());

    // Null value = delete tombstone; needs a key to know which doc.
    let Some(value) = value else {
        let pk = key_components(key)?;
        if pk.is_empty() {
            return Ok(Mapping::Skip);
        }
        return Ok(Mapping::Event(Box::new(Mapped {
            namespace,
            relation,
            op: Op::Delete,
            pk,
            body: None,
            occurred_at_ms: None,
        })));
    };

    let body: Value = parse_json(value)?;
    // Doc id: prefer the message key; else a configured field in the value.
    let mut pk = key_components(key)?;
    if pk.is_empty() {
        if let Some(field) = &config.raw_key_field {
            if let Some(v) = body.get(field) {
                pk = vec![component_text(v)];
            }
        }
    }
    if pk.is_empty() {
        return Err(KafkaCdcError::MalformedEvent(format!(
            "raw message on {topic} has no key and no raw_key_field match"
        )));
    }

    Ok(Mapping::Event(Box::new(Mapped {
        namespace,
        relation,
        op: Op::Insert,
        pk,
        body: Some(body),
        occurred_at_ms: None,
    })))
}

/// Unwrap the JSON-Converter `{schema, payload}` wrapper if present.
fn unwrap_payload(value: &Value) -> &Value {
    match value {
        Value::Object(m) if m.contains_key("schema") && m.contains_key("payload") => {
            m.get("payload").unwrap_or(value)
        }
        _ => value,
    }
}

/// Extract a row image. Debezium relational connectors put an object under
/// `after`; the Mongo connector puts a JSON *string* — parse that too.
fn extract_body(payload: &Value, field: &str) -> Option<Value> {
    match payload.get(field) {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => serde_json::from_str(s).ok(),
        Some(other) => Some(other.clone()),
    }
}

/// Doc-id components from the message key. Honors schema field order when the
/// key is `{schema, payload}`; otherwise sorts object keys for determinism.
/// A scalar key (raw topics) is a single component.
fn key_components(key: Option<&[u8]>) -> Result<Vec<String>, KafkaCdcError> {
    let Some(bytes) = key else {
        return Ok(Vec::new());
    };
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let key: Value = parse_json(bytes)?;

    let order = field_order(&key);
    let payload = unwrap_payload(&key);

    match payload {
        Value::Object(m) => {
            let names: Vec<String> = if let Some(order) = order {
                order
            } else {
                let mut n: Vec<String> = m.keys().cloned().collect();
                n.sort();
                n
            };
            Ok(names
                .iter()
                .filter_map(|n| m.get(n))
                .map(component_text)
                .collect())
        }
        Value::Null => Ok(Vec::new()),
        scalar => Ok(vec![component_text(scalar)]),
    }
}

/// Schema field order for a `{schema:{fields:[{field}...]}}` key.
fn field_order(key: &Value) -> Option<Vec<String>> {
    key.as_object()
        .filter(|m| m.contains_key("payload"))
        .and_then(|m| m.get("schema"))
        .and_then(|s| s.get("fields"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|f| f.get("field").and_then(Value::as_str).map(str::to_owned))
                .collect()
        })
}

fn field_str(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Debezium topics are `{server}.{db}.{table}` — take the last segment as a
/// relation fallback when the `source` block lacks a table.
fn relation_from_topic(topic: &str) -> Option<String> {
    topic
        .rsplit('.')
        .next()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn parse_json(bytes: &[u8]) -> Result<Value, KafkaCdcError> {
    serde_json::from_slice(bytes)
        .map_err(|e| KafkaCdcError::MalformedEvent(format!("invalid JSON: {e}")))
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
    use serde_json::json;

    fn cfg() -> KafkaCdcConfig {
        KafkaCdcConfig::new("k", "b", "g")
    }

    fn ev(m: Mapping) -> Mapped {
        match m {
            Mapping::Event(b) => *b,
            Mapping::Skip => panic!("expected event, got skip"),
        }
    }

    #[test]
    fn debezium_schemaless_insert() {
        let key = json!({"id": 1}).to_string();
        let val = json!({
            "op": "c", "ts_ms": 1000,
            "source": {"db": "shop", "table": "orders", "ts_ms": 900},
            "after": {"id": 1, "total": 100}
        })
        .to_string();
        let m = ev(map(
            &cfg(),
            "srv.shop.orders",
            Some(key.as_bytes()),
            Some(val.as_bytes()),
        )
        .unwrap());
        assert_eq!(m.op, Op::Insert);
        assert_eq!(m.namespace, "shop");
        assert_eq!(m.relation, "orders");
        assert_eq!(m.pk, vec!["1".to_owned()]);
        assert_eq!(m.body.unwrap()["total"], json!(100));
        assert_eq!(m.occurred_at_ms, Some(900));
    }

    #[test]
    fn debezium_schemad_envelope_and_key() {
        let key = json!({
            "schema": {"fields": [{"field": "id"}]},
            "payload": {"id": 7}
        })
        .to_string();
        let val = json!({
            "schema": {"type": "struct"},
            "payload": {
                "op": "u",
                "source": {"db": "shop", "table": "orders"},
                "after": {"id": 7, "total": 222}
            }
        })
        .to_string();
        let m = ev(map(
            &cfg(),
            "srv.shop.orders",
            Some(key.as_bytes()),
            Some(val.as_bytes()),
        )
        .unwrap());
        assert_eq!(m.op, Op::Update);
        assert_eq!(m.pk, vec!["7".to_owned()]);
        assert_eq!(m.body.unwrap()["total"], json!(222));
    }

    #[test]
    fn debezium_delete_then_tombstone() {
        let key = json!({"id": 3}).to_string();
        let del = json!({
            "op": "d",
            "source": {"db": "shop", "table": "orders"},
            "before": {"id": 3}, "after": null
        })
        .to_string();
        let m = ev(map(
            &cfg(),
            "srv.shop.orders",
            Some(key.as_bytes()),
            Some(del.as_bytes()),
        )
        .unwrap());
        assert_eq!(m.op, Op::Delete);
        assert_eq!(m.pk, vec!["3".to_owned()]);
        assert!(m.body.is_none());
        // trailing null-value tombstone is skipped
        assert_eq!(
            map(&cfg(), "srv.shop.orders", Some(key.as_bytes()), None).unwrap(),
            Mapping::Skip
        );
    }

    #[test]
    fn composite_key_uses_schema_order() {
        let key = json!({
            "schema": {"fields": [{"field": "order_id"}, {"field": "line"}]},
            "payload": {"line": 2, "order_id": "ord-1"}
        })
        .to_string();
        let val = json!({
            "op": "c",
            "source": {"db": "shop", "table": "items"},
            "after": {"order_id": "ord-1", "line": 2}
        })
        .to_string();
        let m = ev(map(
            &cfg(),
            "srv.shop.items",
            Some(key.as_bytes()),
            Some(val.as_bytes()),
        )
        .unwrap());
        // schema order order_id,line — not the payload's object order
        assert_eq!(m.pk, vec!["ord-1".to_owned(), "2".to_owned()]);
    }

    #[test]
    fn truncate_is_skipped() {
        let val = json!({"op": "t", "source": {"db": "shop", "table": "orders"}}).to_string();
        assert_eq!(
            map(
                &cfg(),
                "srv.shop.orders",
                Some(b"{}".as_ref()),
                Some(val.as_bytes())
            )
            .unwrap(),
            Mapping::Skip
        );
    }

    #[test]
    fn raw_mode_value_is_body() {
        let mut c = cfg();
        c.unwrap = UnwrapMode::Raw;
        let key = json!("abc").to_string();
        let val = json!({"name": "x"}).to_string();
        let m = ev(map(&c, "events", Some(key.as_bytes()), Some(val.as_bytes())).unwrap());
        assert_eq!(m.op, Op::Insert);
        assert_eq!(m.relation, "events");
        assert_eq!(m.pk, vec!["abc".to_owned()]);
        assert_eq!(m.body.unwrap()["name"], json!("x"));
    }

    #[test]
    fn raw_mode_null_value_is_delete() {
        let mut c = cfg();
        c.unwrap = UnwrapMode::Raw;
        let key = json!("gone").to_string();
        let m = ev(map(&c, "events", Some(key.as_bytes()), None).unwrap());
        assert_eq!(m.op, Op::Delete);
        assert_eq!(m.pk, vec!["gone".to_owned()]);
    }
}
