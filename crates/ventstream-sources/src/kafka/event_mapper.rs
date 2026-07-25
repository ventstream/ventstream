//! Mapped change -> `Event`. Subject `kafka.{ns}.{relation}.{op}`,
//! doc.id `{namespace}.{relation}:["pk", ...]` (PG/MySQL-compatible shape).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use ventstream_core::{doc_id, ContentType, Event, Headers, Payload, SourceUri, Subject};

use super::envelope::Mapped;
use crate::error::KafkaCdcError;

fn build_doc_id(m: &Mapped) -> String {
    doc_id::doc_id(&format!("{}.{}", m.namespace, m.relation), &m.pk)
}

fn headers(m: &Mapped) -> HashMap<String, String> {
    let mut h = HashMap::with_capacity(5);
    h.insert("ventstream.cdc.namespace".to_owned(), m.namespace.clone());
    h.insert("ventstream.cdc.relation".to_owned(), m.relation.clone());
    h.insert("ventstream.cdc.database".to_owned(), m.namespace.clone());
    h.insert("ventstream.cdc.event_type".to_owned(), "row".to_owned());
    h.insert("ventstream.doc.id".to_owned(), build_doc_id(m));
    h
}

fn subject(m: &Mapped) -> Result<Subject, KafkaCdcError> {
    Subject::new(format!(
        "kafka.{}.{}.{}",
        sanitize(&m.namespace),
        sanitize(&m.relation),
        m.op.suffix()
    ))
    .map_err(|e| KafkaCdcError::Internal(e.to_string()))
}

fn source_uri(m: &Mapped) -> Result<SourceUri, KafkaCdcError> {
    SourceUri::new(format!(
        "kafka://{}/{}",
        percent(&m.namespace),
        percent(&m.relation)
    ))
    .map_err(|e| KafkaCdcError::Internal(e.to_string()))
}

/// Build an `Event` from a mapped change. Deletes become `{}` tombstones
/// keyed by the doc id; upserts carry the row image as the body.
pub(crate) fn to_event(m: &Mapped) -> Result<Event, KafkaCdcError> {
    let body = match &m.body {
        _ if m.op.is_delete() => b"{}".to_vec(),
        Some(doc) => serde_json::to_vec(doc).map_err(|e| KafkaCdcError::Internal(e.to_string()))?,
        None => {
            return Err(KafkaCdcError::MalformedEvent(format!(
                "upsert for {}.{} has no body",
                m.namespace, m.relation
            )))
        }
    };
    let occurred_at = m
        .occurred_at_ms
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .unwrap_or_else(Utc::now);

    Ok(Event::builder(source_uri(m)?, subject(m)?)
        .payload(Payload::from_vec(body))
        .content_type(ContentType::Json)
        .occurred_at(occurred_at)
        .headers(Headers::from_map(headers(m)))
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
    use super::super::envelope::Op;
    use super::*;
    use serde_json::{json, Value};

    fn mapped(op: Op, body: Option<Value>) -> Mapped {
        Mapped {
            namespace: "shop".to_owned(),
            relation: "orders".to_owned(),
            op,
            pk: vec!["1".to_owned()],
            body,
            occurred_at_ms: Some(1_000),
        }
    }

    #[test]
    fn upsert_doc_id_subject_body() {
        let ev = to_event(&mapped(Op::Insert, Some(json!({"id": 1, "total": 5})))).unwrap();
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["1"]"#)
        );
        assert!(ev.subject.as_str().ends_with(".insert"));
        let body: Value = serde_json::from_slice(ev.payload.as_bytes().as_ref()).unwrap();
        assert_eq!(body["total"], json!(5));
    }

    #[test]
    fn delete_is_tombstone() {
        let ev = to_event(&mapped(Op::Delete, None)).unwrap();
        assert!(ev.subject.as_str().ends_with(".delete"));
        assert_eq!(ev.payload.as_bytes().as_ref(), b"{}");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["1"]"#)
        );
    }

    #[test]
    fn composite_pk_doc_id() {
        let mut m = mapped(Op::Insert, Some(json!({"a": 1})));
        m.relation = "items".to_owned();
        m.pk = vec!["ord-1".to_owned(), "2".to_owned()];
        let ev = to_event(&m).unwrap();
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.items:["ord-1","2"]"#)
        );
    }
}
