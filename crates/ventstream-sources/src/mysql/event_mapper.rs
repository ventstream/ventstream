//! Row change -> `Event`. Subject `mysql.{ns}.{table}.{op}`,
//! doc.id `{database}.{table}:["pk", ...]` (PG-compatible shape).

use std::collections::HashMap;

use chrono::Utc;
use serde_json::Value;
use ventstream_core::{doc_id, ContentType, Event, Headers, Payload, SourceUri, Subject};

use super::config::MySqlCdcConfig;
use crate::error::MySqlCdcError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    fn suffix(self) -> &'static str {
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

fn build_doc_id(config: &MySqlCdcConfig, table: &str, pk: &[String]) -> String {
    doc_id::doc_id(&format!("{}.{}", config.database, table), pk)
}

fn headers(config: &MySqlCdcConfig, table: &str, pk: &[String]) -> HashMap<String, String> {
    let mut h = HashMap::with_capacity(5);
    h.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    h.insert("ventstream.cdc.relation".to_owned(), table.to_owned());
    h.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    h.insert("ventstream.cdc.event_type".to_owned(), "row".to_owned());
    h.insert(
        "ventstream.doc.id".to_owned(),
        build_doc_id(config, table, pk),
    );
    h
}

fn subject(config: &MySqlCdcConfig, table: &str, op: Op) -> Result<Subject, MySqlCdcError> {
    Subject::new(format!(
        "mysql.{}.{}.{}",
        sanitize(&config.namespace),
        sanitize(table),
        op.suffix()
    ))
    .map_err(|e| MySqlCdcError::Internal(e.to_string()))
}

fn source_uri(config: &MySqlCdcConfig, table: &str) -> Result<SourceUri, MySqlCdcError> {
    SourceUri::new(format!(
        "mysql://{}/{}",
        percent(&config.database),
        percent(table)
    ))
    .map_err(|e| MySqlCdcError::Internal(e.to_string()))
}

fn event(
    config: &MySqlCdcConfig,
    table: &str,
    op: Op,
    pk: &[String],
    body: Vec<u8>,
    bootstrap: bool,
) -> Result<Event, MySqlCdcError> {
    let mut h = headers(config, table, pk);
    if bootstrap {
        h.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());
    }
    Ok(
        Event::builder(source_uri(config, table)?, subject(config, table, op)?)
            .payload(Payload::from_vec(body))
            .content_type(ContentType::Json)
            .occurred_at(Utc::now())
            .headers(Headers::from_map(h))
            .build(),
    )
}

/// Live change. Upserts require the re-read row. Deletes include the available
/// binlog before-image so downstream joins can recover foreign keys.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn change_event(
    config: &MySqlCdcConfig,
    table: &str,
    op: Op,
    pk: &[String],
    full_doc: Option<Value>,
) -> Result<Event, MySqlCdcError> {
    change_event_with_transition(config, table, op, pk, full_doc, None)
}

pub(crate) fn change_event_with_transition(
    config: &MySqlCdcConfig,
    table: &str,
    op: Op,
    pk: &[String],
    full_doc: Option<Value>,
    before_doc: Option<Value>,
) -> Result<Event, MySqlCdcError> {
    let body = match (op, full_doc, before_doc) {
        (Op::Delete, Some(doc), _) => serde_json::to_vec(&serde_json::json!({"old": doc}))
            .map_err(|e| MySqlCdcError::Internal(e.to_string()))?,
        (Op::Update, Some(doc), Some(before)) => {
            serde_json::to_vec(&serde_json::json!({"new": doc, "old": before}))
                .map_err(|e| MySqlCdcError::Internal(e.to_string()))?
        }
        (Op::Insert | Op::Update, Some(doc), _) => {
            serde_json::to_vec(&doc).map_err(|e| MySqlCdcError::Internal(e.to_string()))?
        }
        (Op::Insert | Op::Update, None, _) => {
            return Err(MySqlCdcError::MalformedEvent(format!(
                "upsert for {table} has no row body"
            )));
        }
        (Op::Delete, None, _) => b"{}".to_vec(),
    };
    event(config, table, op, pk, body, false)
}

/// Bootstrap-scanned row.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn snapshot_insert(
    config: &MySqlCdcConfig,
    table: &str,
    pk: &[String],
    doc: Value,
) -> Result<Event, MySqlCdcError> {
    let body = serde_json::to_vec(&doc).map_err(|e| MySqlCdcError::Internal(e.to_string()))?;
    event(config, table, Op::Insert, pk, body, true)
}

/// Internal marker consumed by the join engine after the last snapshot row.
pub(crate) fn snapshot_complete(config: &MySqlCdcConfig) -> Result<Event, MySqlCdcError> {
    let source = SourceUri::new(format!(
        "mysql://{}/_snapshot-complete",
        percent(&config.database)
    ))
    .map_err(|err| MySqlCdcError::Internal(err.to_string()))?;
    let subject = Subject::new(format!(
        "mysql.{}._snapshot_complete",
        sanitize(&config.namespace)
    ))
    .map_err(|err| MySqlCdcError::Internal(err.to_string()))?;
    let mut headers = HashMap::new();
    headers.insert(
        "ventstream.cdc.bootstrap".to_owned(),
        "snapshot-complete".to_owned(),
    );
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(headers))
        .build())
}

fn percent(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

fn sanitize(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() {
        "_".to_owned()
    } else {
        out
    }
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

    fn cfg() -> MySqlCdcConfig {
        MySqlCdcConfig::new("m", "h", "u", "p", "shop", std::env::temp_dir())
    }

    #[test]
    fn upsert_doc_id_and_body() {
        let ev = change_event(
            &cfg(),
            "orders",
            Op::Insert,
            &["ord-1".to_owned()],
            Some(json!({"order_id": "ord-1", "status": "paid"})),
        )
        .expect("event");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["ord-1"]"#)
        );
        assert!(ev.subject.as_str().ends_with(".insert"));
        let body: Value = serde_json::from_slice(ev.payload.as_bytes().as_ref()).expect("json");
        assert_eq!(body["status"], json!("paid"));
    }

    #[test]
    fn snapshot_completion_marker_is_internal_join_boundary() {
        let event = snapshot_complete(&cfg()).expect("snapshot complete");
        assert_eq!(
            event.headers.get("ventstream.cdc.bootstrap"),
            Some("snapshot-complete")
        );
    }

    #[test]
    fn delete_is_tombstone() {
        let ev =
            change_event(&cfg(), "orders", Op::Delete, &["ord-1".to_owned()], None).expect("event");
        assert!(ev.subject.as_str().ends_with(".delete"));
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["ord-1"]"#)
        );
        assert_eq!(ev.payload.as_slice(), b"{}");
    }

    #[test]
    fn delete_carries_available_before_image() {
        let ev = change_event(
            &cfg(),
            "line_items",
            Op::Delete,
            &["item-1".to_owned()],
            Some(json!({"id": "item-1", "order_id": "ord-1"})),
        )
        .expect("event");
        assert_eq!(
            serde_json::from_slice::<Value>(ev.payload.as_slice()).expect("payload"),
            json!({"old": {"id": "item-1", "order_id": "ord-1"}})
        );
    }

    #[test]
    fn update_transition_carries_both_images() {
        let ev = change_event_with_transition(
            &cfg(),
            "line_items",
            Op::Update,
            &["item-1".to_owned()],
            Some(json!({"id": "item-1", "order_id": "ord-2"})),
            Some(json!({"id": "item-1", "order_id": "ord-1"})),
        )
        .expect("event");
        assert_eq!(
            serde_json::from_slice::<Value>(ev.payload.as_slice()).expect("payload"),
            json!({
                "new": {"id": "item-1", "order_id": "ord-2"},
                "old": {"id": "item-1", "order_id": "ord-1"}
            })
        );
    }

    #[test]
    fn composite_pk_doc_id() {
        let ev = change_event(
            &cfg(),
            "line_items",
            Op::Insert,
            &["ord-1".to_owned(), "3".to_owned()],
            Some(json!({"order_id": "ord-1", "line": 3})),
        )
        .expect("event");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.line_items:["ord-1","3"]"#)
        );
    }
}
