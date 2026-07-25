//! Convert MongoDB change events (and snapshot documents) into `Event`s.
//!
//! The emitted shape matches the Postgres / Neo4j sources so the
//! dispatcher and sinks treat all sources interchangeably:
//!
//! - Subject: `mongo.{namespace}.{collection}.{op}` — the same 4-segment
//!   `{source}.{ns}.{relation}.{op}` grammar; the sink reads the final
//!   segment to detect deletes (`… .delete`).
//! - Headers: `ventstream.cdc.namespace` / `ventstream.cdc.relation` /
//!   `ventstream.cdc.database`, identical keys to the other sources.
//! - `ventstream.doc.id`: the **deterministic** sink id. For raw 1:1 mode
//!   there is no join/denormalize layer to stamp it, so this module sets it
//!   directly — `{database}.{collection}:["<_id>"]` (same canonical form as
//!   the PG source's `shop.orders:["…"]`). Deletes carry the same id so the
//!   sink can tombstone the exact document.
//!
//! Mongo-specific header: `ventstream.cdc.event_type = "document"`.

use std::collections::HashMap;

use chrono::Utc;
use serde_json::Value;
use ventstream_core::{doc_id, ContentType, Event, Headers, Payload, SourceUri, Subject};

use super::config::MongoCdcConfig;
use crate::error::MongoCdcError;

/// Change-stream operation, mapped to the subject's final segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `insert`.
    Insert,
    /// `update` or `replace` — both write the whole (re-read) document.
    Update,
    /// `delete` — a tombstone.
    Delete,
}

impl Op {
    /// Map a MongoDB change-event `operationType` to our op. Lifecycle ops
    /// the raw source doesn't act on (`drop`, `rename`, `invalidate`, …)
    /// return `None` so the caller can skip them while still advancing the
    /// resume token.
    pub fn from_operation_type(op: &str) -> Option<Self> {
        match op {
            "insert" => Some(Self::Insert),
            "update" | "replace" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    /// Whether this op is a tombstone.
    pub fn is_delete(self) -> bool {
        matches!(self, Self::Delete)
    }
}

/// The `{database}.{collection}` doc-id table prefix — the namespaced id
/// space, mirroring the PG source's `{schema}.{table}`.
fn doc_id_table(config: &MongoCdcConfig, collection: &str) -> String {
    format!("{}.{}", config.database, collection)
}

/// Build the deterministic sink doc id for a document `_id`.
/// `{database}.{collection}:["<id-text>"]`.
fn build_doc_id(config: &MongoCdcConfig, collection: &str, id: &Value) -> String {
    let component = doc_id::component_text(id);
    doc_id::doc_id(&doc_id_table(config, collection), &[component])
}

/// OpenSearch (and Elasticsearch) reserve `_id` as a document-metadata field
/// and reject it inside the body. MongoDB documents always carry `_id`, so
/// relocate it to a regular `id` field — the deterministic doc-id already
/// encodes the value as the sink's `_id`, and this keeps it queryable in
/// `_source` too. If the document already has an `id`, drop `_id` rather than
/// clobber the author's field.
fn relocate_mongo_id(doc: Value) -> Value {
    match doc {
        Value::Object(mut map) => {
            if let Some(id) = map.remove("_id") {
                map.entry("id".to_owned()).or_insert(id);
            }
            Value::Object(map)
        }
        other => other,
    }
}

fn base_headers(config: &MongoCdcConfig, collection: &str, id: &Value) -> HashMap<String, String> {
    let mut h = HashMap::with_capacity(6);
    h.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    h.insert("ventstream.cdc.relation".to_owned(), collection.to_owned());
    h.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    h.insert(
        "ventstream.cdc.event_type".to_owned(),
        "document".to_owned(),
    );
    h.insert(
        "ventstream.doc.id".to_owned(),
        build_doc_id(config, collection, id),
    );
    h
}

fn build_subject(
    config: &MongoCdcConfig,
    collection: &str,
    op: Op,
) -> Result<Subject, MongoCdcError> {
    Subject::new(format!(
        "mongo.{ns}.{coll}.{op}",
        ns = sanitize_segment(&config.namespace),
        coll = sanitize_segment(collection),
        op = op.suffix(),
    ))
    .map_err(|err| MongoCdcError::Internal(err.to_string()))
}

fn build_source_uri(config: &MongoCdcConfig, collection: &str) -> Result<SourceUri, MongoCdcError> {
    SourceUri::new(format!(
        "mongodb://{db}/{coll}",
        db = percent(&config.database),
        coll = percent(collection),
    ))
    .map_err(|err| MongoCdcError::Internal(err.to_string()))
}

/// Build an event for a live change. `full_doc` must be `Some` for an
/// upsert (the whole post-image, from `fullDocument: updateLookup`); it is
/// ignored for a delete. The `_id` is always required.
#[allow(clippy::needless_pass_by_value)] // full_doc is serialized into the event
pub fn change_event(
    config: &MongoCdcConfig,
    collection: &str,
    op: Op,
    id: &Value,
    full_doc: Option<Value>,
) -> Result<Event, MongoCdcError> {
    let headers = base_headers(config, collection, id);
    let bytes = if op.is_delete() {
        // Tombstone: action-only at the sink; doc.id (in headers) targets
        // the exact document. Body is unused but kept valid JSON.
        b"{}".to_vec()
    } else {
        let doc = full_doc.ok_or_else(|| {
            MongoCdcError::MalformedEvent(format!(
                "upsert for {collection} has no full document; \
                 enable fullDocument=updateLookup"
            ))
        })?;
        serde_json::to_vec(&relocate_mongo_id(doc))
            .map_err(|err| MongoCdcError::Internal(format!("encoding document: {err}")))?
    };

    Ok(Event::builder(
        build_source_uri(config, collection)?,
        build_subject(config, collection, op)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build())
}

/// Build a synthetic insert event for a bootstrap-scanned document.
#[allow(clippy::needless_pass_by_value)] // doc is serialized into the event
pub fn snapshot_insert(
    config: &MongoCdcConfig,
    collection: &str,
    id: &Value,
    doc: Value,
) -> Result<Event, MongoCdcError> {
    let bytes = serde_json::to_vec(&relocate_mongo_id(doc))
        .map_err(|err| MongoCdcError::Internal(format!("encoding snapshot doc: {err}")))?;
    let mut headers = base_headers(config, collection, id);
    headers.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());

    Ok(Event::builder(
        build_source_uri(config, collection)?,
        build_subject(config, collection, Op::Insert)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build())
}

/// Sentinel emitted after the snapshot completes — mirrors the PG/Neo4j
/// `snapshot-complete` event so the join engine's state-dump fires
/// identically regardless of source.
pub fn snapshot_complete(config: &MongoCdcConfig) -> Result<Event, MongoCdcError> {
    let source = SourceUri::new(format!(
        "mongodb://{db}/_snapshot-complete",
        db = percent(&config.database),
    ))
    .map_err(|err| MongoCdcError::Internal(err.to_string()))?;
    let subject = Subject::new("mongo._.snapshot.complete".to_owned())
        .map_err(|err| MongoCdcError::Internal(err.to_string()))?;
    let mut headers = HashMap::new();
    headers.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    headers.insert("ventstream.cdc.relation".to_owned(), "_sentinel".to_owned());
    headers.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
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

/// Minimal percent-encoder — same shape the other sources use.
fn percent(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
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

/// Strip any character that would break the Subject's `.`-separated grammar.
fn sanitize_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
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

    fn cfg() -> MongoCdcConfig {
        MongoCdcConfig::new("m", "mongodb://localhost", "shop", std::env::temp_dir())
    }

    #[test]
    fn op_mapping() {
        assert_eq!(Op::from_operation_type("insert"), Some(Op::Insert));
        assert_eq!(Op::from_operation_type("replace"), Some(Op::Update));
        assert_eq!(Op::from_operation_type("update"), Some(Op::Update));
        assert_eq!(Op::from_operation_type("delete"), Some(Op::Delete));
        assert_eq!(Op::from_operation_type("drop"), None);
        assert!(Op::Delete.is_delete());
    }

    #[test]
    fn upsert_sets_namespaced_doc_id_and_payload() {
        let ev = change_event(
            &cfg(),
            "orders",
            Op::Insert,
            &json!("507f1f77bcf86cd799439011"),
            Some(json!({"_id": "507f1f77bcf86cd799439011", "status": "paid"})),
        )
        .expect("event");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["507f1f77bcf86cd799439011"]"#)
        );
        assert_eq!(ev.headers.get("ventstream.cdc.relation"), Some("orders"));
        assert!(ev.subject.as_str().ends_with(".insert"));
        let body: Value = serde_json::from_slice(ev.payload.as_bytes().as_ref()).expect("json");
        assert_eq!(body["status"], json!("paid"));
    }

    #[test]
    fn mongo_id_relocated_out_of_body_to_id() {
        // `_id` is reserved metadata in OpenSearch; it must not appear in the
        // body. The Mongo `_id` is relocated to `id` (and stays in the doc-id).
        let ev = change_event(
            &cfg(),
            "orders",
            Op::Insert,
            &json!("ord-9"),
            Some(json!({"_id": "ord-9", "status": "paid"})),
        )
        .expect("event");
        let body: Value = serde_json::from_slice(ev.payload.as_bytes().as_ref()).expect("json");
        assert!(
            body.get("_id").is_none(),
            "_id must be removed from the body (OS reserves it), got {body}"
        );
        assert_eq!(body["id"], json!("ord-9"));
        assert_eq!(body["status"], json!("paid"));
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["ord-9"]"#)
        );
    }

    #[test]
    fn delete_is_tombstone_subject_with_doc_id() {
        let ev = change_event(&cfg(), "orders", Op::Delete, &json!("ord-1"), None).expect("event");
        assert!(
            ev.subject.as_str().ends_with(".delete"),
            "delete must end in .delete so the sink tombstones, got {}",
            ev.subject.as_str()
        );
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["ord-1"]"#)
        );
    }

    #[test]
    fn upsert_without_full_doc_is_malformed() {
        let r = change_event(&cfg(), "orders", Op::Update, &json!("ord-1"), None);
        assert!(matches!(r, Err(MongoCdcError::MalformedEvent(_))));
    }

    #[test]
    fn integer_id_stringifies_in_doc_id() {
        let ev = change_event(
            &cfg(),
            "orders",
            Op::Insert,
            &json!(7),
            Some(json!({"_id": 7})),
        )
        .expect("event");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some(r#"shop.orders:["7"]"#)
        );
    }
}
