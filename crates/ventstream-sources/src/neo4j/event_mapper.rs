//! Convert Neo4j CDC payloads (and bootstrap rows) into `Event` values.
//!
//! The shape we emit is intentionally aligned with what the Postgres
//! source produces, so the join engine, dispatcher, and sinks can treat
//! both sources interchangeably:
//!
//! - Subject: `neo4j.{namespace}.{table}.{op}` — same 4-segment pattern
//!   the join engine parses to recover `(namespace, relation, op)`.
//! - Headers: `ventstream.cdc.namespace` + `ventstream.cdc.relation`
//!   are the same keys the PG source emits; the join engine's
//!   `source_table()` helper picks them up identically.
//! - Bootstrap rows carry `ventstream.cdc.bootstrap=snapshot`; the
//!   completion sentinel carries `ventstream.cdc.bootstrap=snapshot-complete`.
//!
//! Neo4j-specific extensions:
//!
//! - `ventstream.cdc.tx_id`: the monotonic transaction id from
//!   Neo4j (string-form because we treat all source cursors as opaque
//!   strings in the engine's ack abstraction).
//! - `ventstream.cdc.event_type`: `n` for node events, `r` for
//!   relationship events. Lets the join engine make graph-vs-row
//!   decisions if it cares to.
//! - For relationships: `ventstream.cdc.start_eid` and
//!   `ventstream.cdc.end_eid` so downstream consumers can resolve
//!   endpoints without re-parsing the payload.

use std::collections::HashMap;

use chrono::Utc;
use serde_json::Value;
use ventstream_core::{ContentType, Event, Headers, Payload, SourceUri, Subject};

use super::config::Neo4jCdcConfig;
use crate::error::Neo4jCdcError;

/// Operation discriminator in the subject's final segment.
#[derive(Debug, Clone, Copy)]
pub enum Op {
    /// New node or relationship.
    Insert,
    /// Property or label change on an existing node/relationship.
    Update,
    /// Node or relationship removed.
    Delete,
}

impl Op {
    /// Map Neo4j CDC operation chars (`c`/`u`/`d`) to our op enum.
    pub fn from_cdc_char(c: &str) -> Result<Self, Neo4jCdcError> {
        match c {
            "c" => Ok(Self::Insert),
            "u" => Ok(Self::Update),
            "d" => Ok(Self::Delete),
            other => Err(Neo4jCdcError::MalformedEvent(format!(
                "unknown CDC operation '{other}'"
            ))),
        }
    }

    /// Subject-suffix form, matching the PG source's vocabulary.
    pub fn as_subject_suffix(&self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Build the headers map shared by live and bootstrap events. The
/// caller adds the `bootstrap` key for snapshot rows and the
/// `event_type` / `tx_id` / endpoint keys for live ones.
fn base_headers(config: &Neo4jCdcConfig, table: &str) -> HashMap<String, String> {
    let mut h = HashMap::with_capacity(8);
    h.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    h.insert("ventstream.cdc.relation".to_owned(), table.to_owned());
    h.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    h
}

fn build_subject(config: &Neo4jCdcConfig, table: &str, op: Op) -> Result<Subject, Neo4jCdcError> {
    Subject::new(format!(
        "neo4j.{namespace}.{table}.{op}",
        namespace = sanitize_segment(&config.namespace),
        table = sanitize_segment(table),
        op = op.as_subject_suffix(),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))
}

fn build_source_uri(config: &Neo4jCdcConfig, table: &str) -> Result<SourceUri, Neo4jCdcError> {
    SourceUri::new(format!(
        "neo4j://{database}/{namespace}/{table}",
        database = percent(&config.database),
        namespace = percent(&config.namespace),
        table = percent(table),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))
}

/// Build a synthetic insert event for a bootstrap-scanned node.
#[allow(clippy::needless_pass_by_value)] // `payload` is serialized into the event
pub fn synth_node_insert(
    config: &Neo4jCdcConfig,
    label_table: &str,
    payload: Value,
) -> Result<Event, Neo4jCdcError> {
    let bytes = serde_json::to_vec(&payload)
        .map_err(|err| Neo4jCdcError::Internal(format!("encoding node payload: {err}")))?;
    let mut headers = base_headers(config, label_table);
    headers.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());
    headers.insert("ventstream.cdc.event_type".to_owned(), "n".to_owned());

    let event = Event::builder(
        build_source_uri(config, label_table)?,
        build_subject(config, label_table, Op::Insert)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build();
    Ok(event)
}

/// Build a synthetic insert event for a bootstrap-scanned relationship.
#[allow(clippy::needless_pass_by_value)] // `payload` is serialized into the event
pub fn synth_rel_insert(
    config: &Neo4jCdcConfig,
    reltype_table: &str,
    payload: Value,
    start_eid: &str,
    end_eid: &str,
) -> Result<Event, Neo4jCdcError> {
    let bytes = serde_json::to_vec(&payload)
        .map_err(|err| Neo4jCdcError::Internal(format!("encoding rel payload: {err}")))?;
    let mut headers = base_headers(config, reltype_table);
    headers.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());
    headers.insert("ventstream.cdc.event_type".to_owned(), "r".to_owned());
    headers.insert("ventstream.cdc.start_eid".to_owned(), start_eid.to_owned());
    headers.insert("ventstream.cdc.end_eid".to_owned(), end_eid.to_owned());

    let event = Event::builder(
        build_source_uri(config, reltype_table)?,
        build_subject(config, reltype_table, Op::Insert)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build();
    Ok(event)
}

/// Sentinel emitted after bootstrap completes. Mirrors the PG source's
/// `snapshot-complete` event so the join engine's state-dump logic
/// fires identically for either source.
///
/// We deliberately stamp `ventstream.cdc.relation = "_sentinel"` so
/// the OS index-template substitution (`events-${header:…relation}`)
/// renders to a discrete `events-_sentinel` index when no join engine
/// is wired to consume the event. The join engine itself ignores
/// payload events with `ventstream.cdc.bootstrap=snapshot-complete`
/// and never forwards them, so this only routes when joins are absent.
pub fn snapshot_complete(config: &Neo4jCdcConfig) -> Result<Event, Neo4jCdcError> {
    let source = SourceUri::new(format!(
        "neo4j://{database}/_/_snapshot-complete",
        database = percent(&config.database),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;
    let subject = Subject::new("neo4j._.snapshot.complete".to_owned())
        .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;
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

/// Build a live tail event from one row of `db.cdc.query(...)` output.
/// The CDC payload (already converted to JSON) tells us node vs
/// relationship + the operation.
pub fn live_event(
    config: &Neo4jCdcConfig,
    tx_id: i64,
    cdc_event: &Value,
) -> Result<LiveEventOutcome, Neo4jCdcError> {
    let obj = cdc_event
        .as_object()
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("event is not an object".to_owned()))?;

    let event_type = obj
        .get("eventType")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("missing eventType".to_owned()))?;
    let op_char = obj
        .get("operation")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("missing operation".to_owned()))?;
    let op = Op::from_cdc_char(op_char)?;

    match event_type {
        "n" => build_node_live(config, tx_id, obj, op),
        "r" => build_rel_live(config, tx_id, obj, op),
        other => Err(Neo4jCdcError::MalformedEvent(format!(
            "unknown eventType '{other}'"
        ))),
    }
}

/// Returned from [`live_event`] — either an event for the bus or a
/// filtered-out marker so the caller can advance the cursor without
/// publishing anything.
#[derive(Debug)]
pub enum LiveEventOutcome {
    /// Pass to the bus.
    Emit(Event),
    /// Event was deliberately suppressed by the source's label /
    /// reltype filter. Cursor still advances.
    Filtered,
}

fn build_node_live(
    config: &Neo4jCdcConfig,
    tx_id: i64,
    obj: &serde_json::Map<String, Value>,
    op: Op,
) -> Result<LiveEventOutcome, Neo4jCdcError> {
    // Labels live at the top level for create / update (the node's CURRENT
    // labels). A node-DELETE payload has no top-level labels — they're under
    // `state.before.labels` instead. So use top-level when present, and ONLY
    // fall back to state.before.labels when top-level is absent/empty.
    //
    // We deliberately do NOT merge the two: on an update that REMOVED a label,
    // top-level carries the current set while state.before carries the old set
    // — merging would resurrect the removed label and could route the doc to
    // the stale table under a label-priority canonicalisation. Top-level is
    // authoritative whenever it exists; state.before is the delete-only
    // fallback (which previously caused deletes to be mis-flagged as malformed
    // and crash-loop the tail).
    let mut labels: Vec<String> = Vec::new();
    if let Some(arr) = obj.get("labels").and_then(|v| v.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                labels.push(s.to_owned());
            }
        }
    }
    if labels.is_empty() {
        if let Some(arr) = obj
            .get("state")
            .and_then(|s| s.pointer("/before/labels"))
            .and_then(|v| v.as_array())
        {
            for v in arr {
                if let Some(s) = v.as_str() {
                    labels.push(s.to_owned());
                }
            }
        }
    }
    let canonical = config
        .canonical_label(&labels)
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("node has empty labels".to_owned()))?;

    // Filter is checked against the canonical label (and only the
    // canonical), so a Author:Person node passes if Author is allowed
    // even when Person is not. Mirrors what users actually mean when
    // they list Author in the filter.
    if !config.label_allowed(canonical) {
        return Ok(LiveEventOutcome::Filtered);
    }
    let table = config.resolve_label_table(canonical);
    let element_id = obj
        .get("elementId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("node missing elementId".to_owned()))?;

    let payload = Value::Object(obj.clone());
    let bytes = serde_json::to_vec(&payload)
        .map_err(|err| Neo4jCdcError::Internal(format!("encoding live node: {err}")))?;
    let mut headers = base_headers(config, &table);
    headers.insert("ventstream.cdc.event_type".to_owned(), "n".to_owned());
    headers.insert("ventstream.cdc.tx_id".to_owned(), tx_id.to_string());
    headers.insert(
        "ventstream.cdc.element_id".to_owned(),
        element_id.to_owned(),
    );

    let event = Event::builder(
        build_source_uri(config, &table)?,
        build_subject(config, &table, op)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build();
    Ok(LiveEventOutcome::Emit(event))
}

fn build_rel_live(
    config: &Neo4jCdcConfig,
    tx_id: i64,
    obj: &serde_json::Map<String, Value>,
    op: Op,
) -> Result<LiveEventOutcome, Neo4jCdcError> {
    let rtype = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Neo4jCdcError::MalformedEvent("relationship missing type".to_owned()))?;
    if !config.reltype_allowed(rtype) {
        return Ok(LiveEventOutcome::Filtered);
    }
    let table = config.resolve_reltype_table(rtype);
    let element_id = obj
        .get("elementId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            Neo4jCdcError::MalformedEvent("relationship missing elementId".to_owned())
        })?;
    let start_eid = obj
        .get("start")
        .and_then(|v: &Value| v.get("elementId"))
        .and_then(|v: &Value| v.as_str())
        .unwrap_or("");
    let end_eid = obj
        .get("end")
        .and_then(|v: &Value| v.get("elementId"))
        .and_then(|v: &Value| v.as_str())
        .unwrap_or("");

    let payload = Value::Object(obj.clone());
    let bytes = serde_json::to_vec(&payload)
        .map_err(|err| Neo4jCdcError::Internal(format!("encoding live rel: {err}")))?;
    let mut headers = base_headers(config, &table);
    headers.insert("ventstream.cdc.event_type".to_owned(), "r".to_owned());
    headers.insert("ventstream.cdc.tx_id".to_owned(), tx_id.to_string());
    headers.insert(
        "ventstream.cdc.element_id".to_owned(),
        element_id.to_owned(),
    );
    headers.insert("ventstream.cdc.start_eid".to_owned(), start_eid.to_owned());
    headers.insert("ventstream.cdc.end_eid".to_owned(), end_eid.to_owned());

    let event = Event::builder(
        build_source_uri(config, &table)?,
        build_subject(config, &table, op)?,
    )
    .payload(Payload::from_vec(bytes))
    .content_type(ContentType::Json)
    .occurred_at(Utc::now())
    .headers(Headers::from_map(headers))
    .build();
    Ok(LiveEventOutcome::Emit(event))
}

/// Minimal percent-encoder — same shape the PG source uses.
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

/// Strip any character that would break the Subject's `.`-separated
/// segment grammar.
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
    use crate::neo4j::config::Neo4jCdcConfig;
    use serde_json::json;

    fn test_config() -> Neo4jCdcConfig {
        Neo4jCdcConfig::new(
            "test",
            "neo4j://localhost",
            "neo4j",
            "secret",
            "neo4j",
            std::env::temp_dir(),
        )
    }

    #[test]
    fn node_delete_reads_labels_from_state_before() {
        // Real CDC node-delete shape: no top-level `labels`; they live under
        // state.before.labels. This used to be MalformedEvent → crash loop.
        let cfg = test_config();
        let event_json = json!({
            "elementId": "4:p:1",
            "eventType": "n",
            "operation": "d",
            "state": {
                "before": { "labels": ["Person", "Author"], "properties": {} },
                "after": null
            }
        });
        match live_event(&cfg, 42, &event_json).expect("delete must not be malformed") {
            LiveEventOutcome::Emit(event) => {
                assert_eq!(event.headers.get("ventstream.cdc.event_type"), Some("n"));
                assert_eq!(
                    event.headers.get("ventstream.cdc.element_id"),
                    Some("4:p:1")
                );
                assert!(
                    event.subject.as_str().ends_with(".delete"),
                    "expected a delete subject, got {}",
                    event.subject.as_str()
                );
            }
            LiveEventOutcome::Filtered => panic!("Person/Author should pass the empty filter"),
        }
    }

    #[test]
    fn node_with_top_level_labels_still_works() {
        let cfg = test_config();
        let event_json = json!({
            "elementId": "4:p:2",
            "eventType": "n",
            "operation": "c",
            "labels": ["Person"],
            "state": { "before": null, "after": { "properties": { "name": "x" } } }
        });
        assert!(matches!(
            live_event(&cfg, 1, &event_json).expect("create"),
            LiveEventOutcome::Emit(_)
        ));
    }

    #[test]
    fn label_removing_update_uses_current_top_level_labels_not_stale_before() {
        // An UPDATE that removed the "Author" label: top-level carries the
        // CURRENT labels (just Person); state.before carries the OLD set
        // (Person, Author). With a label-priority that prefers Author, merging
        // would resurrect Author and route the doc to the stale table. Top-level
        // must win, so the canonical label is Person → table "person".
        let mut cfg = test_config();
        cfg.label_priority = vec!["Author".to_owned(), "Person".to_owned()];
        let event_json = json!({
            "elementId": "4:p:9",
            "eventType": "n",
            "operation": "u",
            "labels": ["Person"],
            "state": {
                "before": { "labels": ["Person", "Author"], "properties": {} },
                "after": { "labels": ["Person"], "properties": {} }
            }
        });
        match live_event(&cfg, 7, &event_json).expect("update") {
            LiveEventOutcome::Emit(event) => {
                // resolve_label_table lowercases by default → "person", not
                // the resurrected "author".
                assert_eq!(event.headers.get("ventstream.cdc.relation"), Some("person"));
                assert!(
                    event.subject.as_str().contains(".person."),
                    "must route by the current label, got {}",
                    event.subject.as_str()
                );
            }
            LiveEventOutcome::Filtered => panic!("Person passes the empty filter"),
        }
    }

    #[test]
    fn node_with_no_labels_anywhere_is_malformed_not_panic() {
        let cfg = test_config();
        let event_json = json!({
            "elementId": "4:p:3",
            "eventType": "n",
            "operation": "d",
            "state": { "before": { "properties": {} }, "after": null }
        });
        // Still an error (so the tail loop logs + skips), but a typed
        // MalformedEvent — never a panic / unwrap.
        match live_event(&cfg, 1, &event_json) {
            Err(Neo4jCdcError::MalformedEvent(_)) => {}
            other => panic!("expected MalformedEvent, got {other:?}"),
        }
    }
}
