//! Bulk API request building and response parsing.
//!
//! The OpenSearch / Elasticsearch Bulk API expects an `application/x-ndjson`
//! body consisting of action/document pairs:
//!
//! ```text
//! {"index":{"_index":"users-2026-05-23","_id":"01JE9X..."}}
//! {"payload":"..."}
//! {"index":{"_index":"users-2026-05-23","_id":"01JE9Y..."}}
//! {"payload":"..."}
//! ```
//!
//! We use the `index` action exclusively (rather than `create`) so a
//! retry that sees the same `_id` is a safe no-op rather than an
//! `id_already_exists` error. Event ids are ULIDs (sortable, unique),
//! so they make ideal idempotency keys.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;
use ventstream_core::Event;

use super::index_template;
use crate::error::OpenSearchSinkError;

/// Header carrying the Postgres WAL-end LSN (stamped by the PG CDC
/// source; preserved unchanged through join re-emits). Used as the
/// OpenSearch external document version so a stale, lower-LSN write that
/// arrives **after** a newer one — possible once `max_parallel_bulks > 1`
/// lets sibling batches complete out of order — is rejected by
/// OpenSearch instead of clobbering or *resurrecting* the newer state.
/// This is the fix for H18 (a child-delete-triggered recompose upsert
/// overtaking the parent's delete tombstone across parallel bulks,
/// leaving an empty-`items` orphan).
const LSN_HEADER: &str = "ventstream.cdc.lsn";

/// Header carrying the Neo4j transaction id — that source's monotonic
/// watermark, used identically to [`LSN_HEADER`] for external
/// versioning. A process runs a single source kind at a time, so only
/// one of the two is ever present on an event.
const TX_ID_HEADER: &str = "ventstream.cdc.tx_id";

/// Durable ordering value for a source whose checkpoint is neither a
/// PostgreSQL LSN nor a Neo4j transaction id. Kafka stamps partition offset +
/// 1 so versions remain positive and survive engine restarts; MongoDB stamps
/// the change's `clusterTime` packed as `time << 31 | increment` (the
/// oplog's ordering key, kept within a Java `long` for this sink), and its
/// snapshot rows the scan-start `operationTime` as a floor.
const SOURCE_VERSION_HEADER: &str = "ventstream.cdc.source_version";

/// The source watermark to stamp as the OpenSearch external doc version,
/// if the event carries one. Postgres snapshot/resync rows carry their
/// scan-start WAL position in the source-version header so a stale
/// snapshot read can never clobber a newer live write; events with
/// neither header (other sources' bootstraps) fall back to unversioned
/// internal-versioning writes.
fn external_version(event: &Event) -> Option<u64> {
    event
        .headers
        .get(LSN_HEADER)
        .or_else(|| event.headers.get(TX_ID_HEADER))
        .or_else(|| event.headers.get(SOURCE_VERSION_HEADER))
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|&v| v > 0)
}

/// Build the `{_index, _id[, version, version_type]}` metadata object
/// shared by the `index` and `delete` bulk actions. When `version` is
/// `Some`, we stamp `version_type: external_gte` so OpenSearch enforces
/// last-writer-wins by source LSN/tx_id even when parallel bulks land out
/// of order: a write whose version is **<** the stored version is
/// rejected (409 version conflict), while an **equal** version (a retry
/// of the same event) is re-applied idempotently. `external_gte` (not
/// `external`) is deliberate — plain `external` would 409 a benign retry
/// of the same event against itself.
#[derive(Serialize)]
struct ActionMeta<'a> {
    #[serde(rename = "_index")]
    index: &'a str,
    #[serde(rename = "_id")]
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_type: Option<&'static str>,
}

impl<'a> ActionMeta<'a> {
    fn new(index: &'a str, id: &'a str, version: Option<u64>) -> Self {
        Self {
            index,
            id,
            version,
            version_type: version.map(|_| "external_gte"),
        }
    }
}

#[derive(Serialize)]
struct IndexAction<'a> {
    index: ActionMeta<'a>,
}

#[derive(Serialize)]
struct DeleteAction<'a> {
    delete: ActionMeta<'a>,
}

/// Build the NDJSON body for one bulk request.
///
/// Returns the raw bytes plus the per-event index assignment so that
/// per-item retries on partial failure can target the same index.
pub struct BulkRequest {
    /// NDJSON bytes ready for `POST /_bulk`.
    pub body: Vec<u8>,
    /// Index name resolved for each event, in batch order. Used by the
    /// partial-failure handler to construct a smaller retry batch.
    pub indices: Vec<String>,
    /// Event offset behind each bulk action, in action order. A key-changing
    /// update contributes two actions (delete old id + index) that map back
    /// to the same event.
    pub item_event_offsets: Vec<usize>,
}

/// Build a bulk request from a slice of events and a template.
pub fn build_bulk_request(
    events: &[Event],
    template: &str,
    now: DateTime<Utc>,
) -> Result<BulkRequest, OpenSearchSinkError> {
    build_bulk_request_inner(events, template, now, true)
}

/// Build only the NDJSON body used by the HTTP hot path.
///
/// The public request builder retains per-event index assignments for callers
/// that need them. The sink retry path tracks event offsets directly, so it can
/// avoid allocating and cloning an otherwise-unused `Vec<String>`.
pub fn build_bulk_body(
    events: &[Event],
    template: &str,
    now: DateTime<Utc>,
) -> Result<(Vec<u8>, Vec<usize>), OpenSearchSinkError> {
    build_bulk_request_inner(events, template, now, false)
        .map(|request| (request.body, request.item_event_offsets))
}

fn build_bulk_request_inner(
    events: &[Event],
    template: &str,
    now: DateTime<Utc>,
    collect_indices: bool,
) -> Result<BulkRequest, OpenSearchSinkError> {
    let mut body = Vec::with_capacity(events.len() * 256);
    let mut item_event_offsets = Vec::with_capacity(events.len());
    let mut indices = if collect_indices {
        Vec::with_capacity(events.len())
    } else {
        Vec::new()
    };

    // Literal targets are overwhelmingly common for projection pipelines.
    // Render and validate them once rather than allocating one String per
    // event. Dynamic templates retain the existing per-event behavior.
    let static_index = if !template.contains('%') && !template.contains("${") {
        events
            .first()
            .map(|event| index_template::render(template, event, now))
            .transpose()?
    } else {
        None
    };

    for (event_offset, event) in events.iter().enumerate() {
        // Target clears are applied by the sink before the bulk request
        // (delete-by-query — there's no bulk action for "empty the
        // index"). Skipping them here is what stops them being written
        // as junk `{}` documents under generated ids.
        if is_target_clear(event) {
            continue;
        }
        let index = match static_index.as_deref() {
            Some(index) => Cow::Borrowed(index),
            None => Cow::Owned(index_template::render(template, event, now)?),
        };
        // The stable per-logical-row id, when the upstream produced one
        // (e.g. the join engine / denormalize modes set `ventstream.doc.id`
        // = `{primary_table}:{primary_pk}` so re-emits overwrite the same
        // doc). All denormalize modes stamp it.
        let stable_id = event.headers.get("ventstream.doc.id");
        // Source watermark → external doc version (None for bootstrap).
        let version = external_version(event);

        if is_delete_event(event) {
            // A delete MUST target the stable logical doc id. Falling back to
            // the per-event ULID would address a doc that never existed → OS
            // returns 404 (swallowed by the bulk response) and the doc that
            // should be removed lives forever. Block delivery instead of
            // silently emitting an unmatchable delete (H9); a delete without
            // this header means a misconfigured / raw passthrough path.
            let Some(doc_id) = stable_id else {
                return Err(OpenSearchSinkError::Internal(format!(
                    "delete event {} (subject '{}') has no `ventstream.doc.id`; \
                     cannot identify the doc to remove. Delivery is blocked until \
                     the source or denormalization mode stamps a stable doc id.",
                    event.id, event.subject
                )));
            };
            // Bulk delete action: action line only, no payload body.
            // Versioned (external_gte) so a stale recompose-upsert can't
            // overtake this tombstone across parallel bulks (H18).
            let action = DeleteAction {
                delete: ActionMeta::new(index.as_ref(), doc_id, version),
            };
            serde_json::to_writer(&mut body, &action).map_err(|err| {
                OpenSearchSinkError::Internal(format!("serializing bulk action: {err}"))
            })?;
            body.push(b'\n');
            item_event_offsets.push(event_offset);
        } else {
            // A key-changing update relocated the doc: remove the previous
            // doc first so the old id does not linger as a stale copy.
            if let Some(old_id) = event.headers.get("ventstream.doc.old_id") {
                let action = DeleteAction {
                    delete: ActionMeta::new(index.as_ref(), old_id, version),
                };
                serde_json::to_writer(&mut body, &action).map_err(|err| {
                    OpenSearchSinkError::Internal(format!("serializing bulk action: {err}"))
                })?;
                body.push(b'\n');
                item_event_offsets.push(event_offset);
            }
            // Upsert. Use the stable id when present; otherwise fall back to
            // the per-event id — which makes each event its own doc (no upsert
            // dedup, so re-emits duplicate). Surface the fallback so a
            // doc-id-less path is observable rather than silently duplicating.
            let doc_id = match stable_id {
                Some(id) => Cow::Borrowed(id),
                None => {
                    warn!(
                        event_id = %event.id,
                        subject = %event.subject,
                        "index event has no `ventstream.doc.id`; using the per-event \
                         id as `_id` — re-emits create new docs instead of upserting"
                    );
                    Cow::Owned(event.id.to_string())
                }
            };
            let action = IndexAction {
                index: ActionMeta::new(index.as_ref(), doc_id.as_ref(), version),
            };
            serde_json::to_writer(&mut body, &action).map_err(|err| {
                OpenSearchSinkError::Internal(format!("serializing bulk action: {err}"))
            })?;
            body.push(b'\n');
            item_event_offsets.push(event_offset);

            // Must be a single line of JSON. Source produces compact
            // JSON; we still fold any stray '\n' to a space so a
            // misbehaving source can't corrupt NDJSON framing for the
            // rest of the batch.
            match update_row_body(event) {
                Some(row) => append_payload_line(&mut body, &row),
                None => append_payload_line(&mut body, event.payload.as_slice()),
            }
            body.push(b'\n');
        }

        if collect_indices {
            indices.push(index.into_owned());
        }
    }

    Ok(BulkRequest {
        body,
        indices,
        item_event_offsets,
    })
}

/// For flat CDC updates the payload is the `{"new":…, "old":…}` transition
/// envelope; the document to materialize is the `new` row. Denormalized
/// paths re-emit plain documents and pass through untouched.
fn update_row_body(event: &Event) -> Option<Vec<u8>> {
    if !event.subject.as_str().ends_with(".update") {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_slice(event.payload.as_slice()).ok()?;
    let row = parsed.get("new")?;
    if !row.is_object() {
        return None;
    }
    serde_json::to_vec(row).ok()
}

/// True for the TRUNCATE-derived event that asks the sink to clear the
/// truncated relation's documents, rather than to write or delete one.
pub(crate) fn is_target_clear(event: &ventstream_core::Event) -> bool {
    // Header ONLY. Matching on the `.truncate` subject would fire for raw
    // source truncates in plain (non-denormalize) mode, which bypass the
    // `with_target_clears` gate entirely — the sink would then issue a
    // clear for a relation whose documents it was never asked to manage.
    event.headers.get("ventstream.target.clear") == Some("true")
}

/// Detect whether an event is a deletion based on the CDC source's
/// subject convention. Subjects look like `postgres.<ns>.<rel>.<op>`
/// where `<op>` is `insert | update | delete | truncate`. A delete
/// event in the join engine's primary path was passed through with the
/// same subject (carrying the row's `old` payload), so we can recognize
/// it here and emit an OS bulk `delete` action instead of `index`.
fn is_delete_event(event: &Event) -> bool {
    event
        .subject
        .as_str()
        .rsplit('.')
        .next()
        .map(|op| op == "delete")
        .unwrap_or(false)
}

/// Append `payload` to `out`, replacing any embedded `\n` byte with a
/// space so the NDJSON framing stays intact.
fn append_payload_line(out: &mut Vec<u8>, payload: &[u8]) {
    if payload.contains(&b'\n') {
        out.reserve(payload.len());
        for &b in payload {
            out.push(if b == b'\n' { b' ' } else { b });
        }
    } else {
        out.extend_from_slice(payload);
    }
}

/// Typed view of the JSON response body returned by `POST /_bulk`.
#[derive(Debug, Deserialize)]
pub struct BulkResponse {
    /// Server-side processing time in milliseconds.
    pub took: u64,
    /// True iff at least one item failed.
    pub errors: bool,
    /// One entry per request item, in order.
    pub items: Vec<BulkResponseItem>,
}

/// One item in the bulk response. Both OpenSearch and Elasticsearch
/// wrap the per-item result under the action name (`index`, `create`,
/// `update`, `delete`) — we use `#[serde(flatten)]` to peel that off.
#[derive(Debug, Deserialize)]
pub struct BulkResponseItem {
    /// The action-keyed result.
    #[serde(flatten)]
    pub action: BulkResponseAction,
}

/// The action-keyed wrapper carrying the per-item result.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkResponseAction {
    /// `index` action result.
    Index(BulkResponseEntry),
    /// `create` action result.
    Create(BulkResponseEntry),
    /// `update` action result.
    Update(BulkResponseEntry),
    /// `delete` action result.
    Delete(BulkResponseEntry),
}

impl BulkResponseAction {
    /// Borrow the underlying entry regardless of which action variant.
    pub fn entry(&self) -> &BulkResponseEntry {
        match self {
            Self::Index(e) | Self::Create(e) | Self::Update(e) | Self::Delete(e) => e,
        }
    }

    /// Whether a delete reached the desired state because the document was
    /// already absent. Replayed deletes must remain idempotent across source
    /// and sink restarts. An `index_not_found_exception` is a deployment
    /// failure, not an idempotent delete result, and must block delivery.
    pub fn is_delete_not_found(&self) -> bool {
        matches!(
            self,
            Self::Delete(entry)
                if entry.status == 404
                    && (entry.error.is_none()
                        || entry.error_type() == Some("document_missing_exception"))
        )
    }
}

/// One item's outcome — the inner object under `index` / `create` /
/// `update` / `delete` in the response.
#[derive(Debug, Deserialize)]
pub struct BulkResponseEntry {
    /// Echo of the `_id` we sent.
    #[serde(rename = "_id")]
    pub id: Option<String>,
    /// HTTP-style status code. 200-299 = success.
    pub status: u16,
    /// Error object on failure. Shape varies; treated as opaque.
    pub error: Option<serde_json::Value>,
}

impl BulkResponseEntry {
    /// Whether the item was accepted by the server.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Whether the server's failure was a rate-limit (retryable with
    /// longer backoff).
    pub fn is_rate_limited(&self) -> bool {
        self.status == 429
    }

    /// Whether the item carries an HTTP client-error status other than the
    /// separately classified rate limit. This does not imply the failure is a
    /// permanent document error.
    pub fn is_client_error(&self) -> bool {
        (400..500).contains(&self.status) && self.status != 429
    }

    /// Human-readable failure reason, when the response supplied one.
    pub fn error_reason(&self) -> Option<&str> {
        self.error
            .as_ref()
            .and_then(|error| error.get("reason"))
            .and_then(serde_json::Value::as_str)
    }

    /// OpenSearch/Elasticsearch error type, when the response supplied one.
    pub fn error_type(&self) -> Option<&str> {
        self.error
            .as_ref()
            .and_then(|error| error.get("type"))
            .and_then(serde_json::Value::as_str)
    }

    /// Whether the item failed with an OpenSearch external-version
    /// conflict (HTTP 409). With `version_type: external_gte` (see
    /// `action_meta`) a 409 means the stored doc already carries a
    /// version **>=** the one we sent — i.e. a newer write already won
    /// and this one is stale. That is the *desired* end state, not a
    /// failure: the caller treats it as a success so the stale write is
    /// silently dropped instead of being routed to the DLQ (H18). In our
    /// bulk requests a 409 can only be a version conflict, so status
    /// alone is sufficient; we still cross-check the error `type` when
    /// present to stay precise.
    pub fn is_version_conflict(&self) -> bool {
        if self.status != 409 {
            return false;
        }
        match self.error_type() {
            Some(t) => t.contains("version_conflict"),
            // No typed error body — a bare 409 from bulk is a version
            // conflict by construction (we only ever send versioned ops).
            None => true,
        }
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
    use chrono::TimeZone;
    use ventstream_core::{ContentType, Event, Headers, Payload, SourceUri, Subject};

    fn make_event(subject: &str, payload: &[u8]) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(payload.to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::empty())
            .build()
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap()
    }

    #[test]
    fn bulk_request_emits_two_lines_per_event() {
        let events = vec![
            make_event("a.b", br#"{"x":1}"#),
            make_event("a.b", br#"{"x":2}"#),
        ];
        let req = build_bulk_request(&events, "static-index", now()).unwrap();
        let text = String::from_utf8(req.body).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4);
        // Action lines: any key order is fine (OpenSearch doesn't care).
        // We check structure with `contains` rather than locking in serde_json's
        // alphabetical key emission.
        assert!(lines[0].starts_with(r#"{"index":{"#));
        assert!(lines[0].contains(r#""_index":"static-index""#));
        assert!(lines[0].contains(r#""_id":""#));
        assert_eq!(lines[1], r#"{"x":1}"#);
        assert!(lines[2].starts_with(r#"{"index":{"#));
        assert!(lines[2].contains(r#""_index":"static-index""#));
        assert_eq!(lines[3], r#"{"x":2}"#);
    }

    #[test]
    fn bulk_request_terminates_body_with_newline() {
        let events = vec![make_event("a.b", b"{}")];
        let req = build_bulk_request(&events, "x", now()).unwrap();
        assert_eq!(*req.body.last().unwrap(), b'\n');
    }

    #[test]
    fn bulk_request_uses_event_id_as_underscore_id() {
        let event = make_event("a.b", b"{}");
        let id = event.id.to_string();
        let req = build_bulk_request(&[event], "static", now()).unwrap();
        let text = String::from_utf8(req.body).unwrap();
        assert!(text.contains(&id), "expected event id {id} in bulk body");
    }

    #[test]
    fn bulk_request_resolves_index_per_event_via_template() {
        let events = vec![
            make_event("postgres.app.products.insert", b"{}"),
            make_event("postgres.app.products.update", b"{}"),
        ];
        let req = build_bulk_request(&events, "events-${subject:1}-%Y-%m-%d", now()).unwrap();
        assert_eq!(
            req.indices,
            vec!["events-app-2026-05-23", "events-app-2026-05-23"]
        );
    }

    #[test]
    fn bulk_request_replaces_embedded_newlines_in_payload() {
        // A misbehaving source that produced a payload with an embedded
        // newline would otherwise break the NDJSON framing.
        let event = make_event("a.b", b"{\"a\":1,\n\"b\":2}");
        let req = build_bulk_request(&[event], "static", now()).unwrap();
        let text = String::from_utf8(req.body).unwrap();
        // Should be 3 lines total: action + doc + (trailing empty).
        assert_eq!(text.matches('\n').count(), 2);
        assert!(text.contains("{\"a\":1, \"b\":2}"));
    }

    #[test]
    fn bulk_response_parses_all_success() {
        let json = serde_json::json!({
            "took": 5,
            "errors": false,
            "items": [
                { "index": { "_id": "x", "status": 201 } },
                { "index": { "_id": "y", "status": 200 } }
            ]
        });
        let resp: BulkResponse = serde_json::from_value(json).unwrap();
        assert!(!resp.errors);
        assert_eq!(resp.items.len(), 2);
        assert!(resp.items[0].action.entry().is_success());
        assert!(resp.items[1].action.entry().is_success());
    }

    #[test]
    fn bulk_response_parses_mixed_outcome() {
        let json = serde_json::json!({
            "took": 5,
            "errors": true,
            "items": [
                { "index": { "_id": "x", "status": 201 } },
                { "index": { "_id": "y", "status": 400, "error": { "type": "mapper_parsing_exception" } } },
                { "index": { "_id": "z", "status": 429, "error": { "type": "es_rejected_execution_exception" } } }
            ]
        });
        let resp: BulkResponse = serde_json::from_value(json).unwrap();
        assert!(resp.errors);
        assert!(resp.items[0].action.entry().is_success());
        assert!(resp.items[1].action.entry().is_client_error());
        assert!(!resp.items[1].action.entry().is_rate_limited());
        assert!(resp.items[2].action.entry().is_rate_limited());
        assert!(!resp.items[2].action.entry().is_client_error());
    }

    fn make_event_with_doc_id(subject: &str, doc_id: Option<&str>) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        let mut map = std::collections::HashMap::new();
        if let Some(id) = doc_id {
            map.insert("ventstream.doc.id".to_string(), id.to_string());
        }
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(map))
            .build()
    }

    #[test]
    fn target_clears_are_excluded_from_the_bulk_body() {
        // A batch of only clears must produce an EMPTY body — the clear is
        // applied out-of-band by the sink. Encoding it as an index action
        // wrote a junk `{}` document under a generated id; posting a
        // zero-byte body instead is a hard 400, so the sink guards on
        // emptiness. This is the common shape: the denormalizer follows a
        // TRUNCATE with a rebuild that reads the emptied table and emits
        // nothing.
        let clear = make_event_with_headers(
            "postgres.public.orders.truncate",
            &[
                ("ventstream.target.clear", "true"),
                ("ventstream.cdc.namespace", "public"),
                ("ventstream.cdc.relation", "orders"),
            ],
        );
        let (body, offsets) = build_bulk_body(
            std::slice::from_ref(&clear),
            "orders",
            chrono::Utc.timestamp_opt(0, 0).single().expect("ts"),
        )
        .expect("build");
        assert!(body.is_empty(), "a lone clear must not produce a bulk body");
        assert!(offsets.is_empty());

        // A raw source TRUNCATE without the header is NOT a clear: the
        // subject alone would fire for plain (non-denormalize) pipelines
        // that never opted into target clears.
        let raw =
            make_event_with_doc_id("postgres.public.orders.truncate", Some("public.orders:1"));
        assert!(!is_target_clear(&raw));
        assert!(is_target_clear(&clear));
    }

    fn make_event_with_headers(subject: &str, headers: &[(&str, &str)]) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        let mut map = std::collections::HashMap::new();
        for (k, v) in headers {
            map.insert((*k).to_string(), (*v).to_string());
        }
        Event::builder(source, subject)
            .payload(Payload::from_vec(br#"{"x":1}"#.to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(map))
            .build()
    }

    #[test]
    fn delete_without_doc_id_blocks_delivery() {
        // H9: a delete with no stable doc id can't target the doc to remove;
        // block delivery rather than emit an unmatchable delete that 404s and
        // leaves the doc indexed forever.
        let event = make_event_with_doc_id("a.b.delete", None);
        let err = match build_bulk_request(&[event], "idx", now()) {
            Err(e) => e,
            Ok(_) => panic!("delete without doc.id must block, not be emitted"),
        };
        assert!(
            err.to_string().contains("ventstream.doc.id"),
            "error must name the missing header: {err}"
        );
    }

    #[test]
    fn delete_with_doc_id_targets_that_id() {
        // A delete WITH the stable id targets it (not the per-event ULID).
        let event = make_event_with_doc_id("a.b.delete", Some("orders:5"));
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1, "delete is action-line only, no payload");
        assert!(lines[0].starts_with(r#"{"delete":{"#));
        assert!(
            lines[0].contains(r#""_id":"orders:5""#),
            "delete must target the stable doc id: {}",
            lines[0]
        );
    }

    #[test]
    fn index_action_stamps_external_version_from_lsn_header() {
        // H18: a tail upsert carrying an LSN must be versioned so a
        // stale, lower-LSN sibling can't clobber it across parallel bulks.
        let event = make_event_with_headers(
            "postgres.shop.orders.update",
            &[
                ("ventstream.doc.id", "shop.orders:[\"ord-1\"]"),
                ("ventstream.cdc.lsn", "24000000"),
            ],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(action.starts_with(r#"{"index":{"#), "got: {action}");
        assert!(action.contains(r#""version":24000000"#), "got: {action}");
        assert!(
            action.contains(r#""version_type":"external_gte""#),
            "got: {action}"
        );
    }

    #[test]
    fn delete_action_stamps_external_version_from_lsn_header() {
        // The delete tombstone must carry the (higher) LSN so a later-
        // arriving but lower-LSN recompose-upsert is rejected (the H18
        // resurrection path).
        let event = make_event_with_headers(
            "postgres.shop.orders.delete",
            &[
                ("ventstream.doc.id", "shop.orders:[\"ord-1\"]"),
                ("ventstream.cdc.lsn", "24000500"),
            ],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(action.starts_with(r#"{"delete":{"#), "got: {action}");
        assert!(action.contains(r#""version":24000500"#), "got: {action}");
        assert!(
            action.contains(r#""version_type":"external_gte""#),
            "got: {action}"
        );
    }

    #[test]
    fn neo4j_tx_id_header_is_used_as_external_version() {
        let event = make_event_with_headers(
            "neo4j.Product.update",
            &[
                ("ventstream.doc.id", "Product:42"),
                ("ventstream.cdc.tx_id", "9001"),
            ],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(action.contains(r#""version":9001"#), "got: {action}");
        assert!(
            action.contains(r#""version_type":"external_gte""#),
            "got: {action}"
        );
    }

    #[test]
    fn kafka_source_version_survives_process_ack_sequence_reset() {
        let event = make_event_with_headers(
            "kafka.shop.orders.insert",
            &[
                ("ventstream.doc.id", "shop.orders:[\"ord-1\"]"),
                ("ventstream.cdc.ack_seq", "1"),
                ("ventstream.cdc.source_version", "3"),
            ],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(action.contains(r#""version":3"#), "got: {action}");
        assert!(!action.contains(r#""version":1"#), "got: {action}");
    }

    #[test]
    fn bootstrap_event_without_watermark_is_unversioned() {
        // Snapshot/bootstrap rows have no LSN/tx_id — they must stay on
        // internal versioning (a snapshot row precedes any tail event for
        // the same doc, so it never needs cross-batch ordering). Stamping
        // version 0 would also be rejected by OpenSearch.
        let event = make_event_with_headers(
            "postgres.shop.orders.insert",
            &[("ventstream.doc.id", "shop.orders:[\"ord-1\"]")],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(!action.contains("version"), "must be unversioned: {action}");
    }

    #[test]
    fn zero_watermark_is_treated_as_no_version() {
        // A literal "0" watermark must not be stamped (OpenSearch rejects
        // external version 0).
        let event = make_event_with_headers(
            "postgres.shop.orders.insert",
            &[
                ("ventstream.doc.id", "shop.orders:[\"ord-1\"]"),
                ("ventstream.cdc.lsn", "0"),
            ],
        );
        let req = build_bulk_request(&[event], "idx", now()).expect("ok");
        let text = String::from_utf8(req.body).unwrap();
        let action = text.lines().next().expect("action line");
        assert!(!action.contains("version"), "must be unversioned: {action}");
    }

    #[test]
    fn is_version_conflict_recognizes_409() {
        let conflict = BulkResponseEntry {
            id: Some("x".into()),
            status: 409,
            error: Some(serde_json::json!({
                "type": "version_conflict_engine_exception",
                "reason": "version conflict, current version [5] is higher or equal to [3]"
            })),
        };
        assert!(conflict.is_version_conflict());
        assert!(!conflict.is_success());

        // Bare 409 with no typed error body still reads as a conflict.
        let bare = BulkResponseEntry {
            id: Some("x".into()),
            status: 409,
            error: None,
        };
        assert!(bare.is_version_conflict());

        // A 400 mapper error is NOT a version conflict.
        let mapper = BulkResponseEntry {
            id: Some("y".into()),
            status: 400,
            error: Some(serde_json::json!({ "type": "mapper_parsing_exception" })),
        };
        assert!(!mapper.is_version_conflict());
    }

    #[test]
    fn delete_not_found_requires_delete_action_without_error() {
        let already_absent = BulkResponseAction::Delete(BulkResponseEntry {
            id: Some("gone".into()),
            status: 404,
            error: None,
        });
        assert!(already_absent.is_delete_not_found());

        let missing_index = BulkResponseAction::Delete(BulkResponseEntry {
            id: Some("gone".into()),
            status: 404,
            error: Some(serde_json::json!({ "type": "index_not_found_exception" })),
        });
        assert!(!missing_index.is_delete_not_found());

        let missing_indexed_document = BulkResponseAction::Index(BulkResponseEntry {
            id: Some("gone".into()),
            status: 404,
            error: None,
        });
        assert!(!missing_indexed_document.is_delete_not_found());
    }
}
