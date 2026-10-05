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

pub(crate) use ventstream_core::SOURCE_VERSION_HEADER;

/// Pack a binlog coordinate into the `u64` the sinks compare.
///
/// The binlog file's numeric suffix (`binlog.000042` → 42) goes in the high
/// half and the event's end position in the low 32 bits, so versions order
/// exactly as the binlog does: by file, then by position within the file —
/// durable across restarts, unlike the per-session ack sequence. The result
/// stays within a signed 64-bit sink version for any file sequence below
/// 2^31 (two billion rotations); beyond that — or for a filename with no
/// numeric suffix — `None` is returned and the event ships unversioned
/// rather than mis-ordered.
///
/// Positions are masked to 32 bits, matching the binlog protocol's own u32
/// `log_pos`. A file grown past 4 GiB by one giant transaction wraps that
/// counter at the protocol level too; a wrapped position would pack
/// *lower* than earlier rows in the same file and a versioning sink
/// would reject those writes as stale — the tail loop detects the wrap
/// and ships that file's remaining rows unversioned instead.
pub(crate) fn binlog_version(file: &str, pos: u64) -> Option<u64> {
    let digits_start = file
        .rfind(|c: char| !c.is_ascii_digit())
        .map_or(0, |i| i + 1);
    let suffix = file.get(digits_start..).unwrap_or("");
    if suffix.is_empty() {
        return None;
    }
    let sequence: u64 = suffix.parse().ok()?;
    if sequence > (i64::MAX as u64) >> 32 {
        return None;
    }
    Some((sequence << 32) | (pos & 0xFFFF_FFFF))
}

/// Stamp an event with its source version, when one is known. `None`
/// leaves the event unversioned (last-arrival-wins at the sink); callers
/// log that case once.
#[must_use]
pub(crate) fn with_source_version(event: Event, version: Option<u64>) -> Event {
    match version {
        Some(version) => Event {
            headers: event
                .headers
                .with_header(SOURCE_VERSION_HEADER.to_owned(), version.to_string()),
            ..event
        },
        None => event,
    }
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

/// A TRUNCATE observed in the binlog, as an event (subject suffix
/// `truncate`, no doc id — there is no row). Only published when the
/// source was built `with_truncate_events`; the SQL-denormalize engine
/// consumes it to clear and rebuild the projection (#154).
pub(crate) fn truncate_event(config: &MySqlCdcConfig, table: &str) -> Result<Event, MySqlCdcError> {
    let subject = Subject::new(format!(
        "mysql.{}.{}.truncate",
        sanitize(&config.namespace),
        sanitize(table)
    ))
    .map_err(|e| MySqlCdcError::Internal(e.to_string()))?;
    let mut h = HashMap::with_capacity(4);
    h.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    h.insert("ventstream.cdc.relation".to_owned(), table.to_owned());
    h.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    h.insert(
        "ventstream.cdc.event_type".to_owned(),
        "truncate".to_owned(),
    );
    Ok(Event::builder(source_uri(config, table)?, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(h))
        .build())
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

    /// #118: the packed binlog coordinate must order exactly as the binlog
    /// does — by file sequence, then by position — and never exceed the
    /// signed 64-bit sink version (the #190 lesson, applied here from the
    /// start).
    #[test]
    fn binlog_version_orders_like_the_binlog_and_fits_a_signed_64() {
        let early_file = binlog_version("binlog.000007", 4_000_000).expect("version");
        let later_file = binlog_version("binlog.000008", 4).expect("version");
        assert!(early_file < later_file, "file sequence dominates position");
        assert!(
            binlog_version("binlog.000007", 100) < binlog_version("binlog.000007", 200),
            "position orders within a file"
        );
        assert_eq!(
            binlog_version("mysql-bin.000042", 1_457),
            Some((42u64 << 32) | 1_457),
            "decodable packing"
        );
        let max_seq = (i64::MAX as u64) >> 32;
        let ceiling = binlog_version(&format!("binlog.{max_seq}"), u64::from(u32::MAX))
            .expect("largest representable coordinate");
        assert!(ceiling <= i64::MAX as u64);
        assert_eq!(
            binlog_version(&format!("binlog.{}", max_seq + 1), 0),
            None,
            "a sequence past the signed-64 ceiling ships unversioned, never mis-ordered"
        );
    }

    #[test]
    fn binlog_version_requires_a_numeric_suffix_and_masks_the_position() {
        assert_eq!(binlog_version("binlog", 10), None);
        assert_eq!(binlog_version("", 10), None);
        // Positions wrap at the protocol's own 32-bit boundary.
        assert_eq!(
            binlog_version("binlog.000001", (1u64 << 32) | 5),
            Some((1u64 << 32) | 5)
        );
    }

    #[test]
    fn with_source_version_stamps_only_when_known() {
        let event = change_event(
            &cfg(),
            "orders",
            Op::Insert,
            &["1".to_owned()],
            Some(serde_json::json!({"id": 1})),
        )
        .expect("event");
        assert_eq!(event.headers.get(SOURCE_VERSION_HEADER), None);
        let stamped = with_source_version(event.clone(), Some(99));
        assert_eq!(stamped.headers.get(SOURCE_VERSION_HEADER), Some("99"));
        let untouched = with_source_version(event, None);
        assert_eq!(untouched.headers.get(SOURCE_VERSION_HEADER), None);
    }
}
