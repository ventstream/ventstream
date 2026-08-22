//! Convert decoded `pgoutput` messages into [`ventstream_core::Event`].
//!
//! The mapping rules for Phase 0a:
//!
//! - **Source URI**: `postgres://{publication}/{namespace}.{relation_name}`
//! - **Subject**: `postgres.{namespace}.{relation_name}.{op}` where `op`
//!   is `insert` for Phase 0a (`update`, `delete`, `truncate` arrive
//!   alongside the corresponding decoder variants in 0b).
//! - **Content type**: `application/json`.
//! - **Payload**: a JSON object `{ "column_name": <value>, ... }` where
//!   `<value>` is:
//!   - `null` for SQL NULL.
//!   - the raw text representation for `Text` columns (Postgres returns
//!     these as the canonical `_send`/`_recv` text encoding — we forward
//!     them verbatim; downstream consumers parse as their target type).
//!   - a hex-encoded string for `Binary` columns (uncommon in Phase 0a
//!     since we don't request binary protocol).
//!   - **omitted entirely** for `UnchangedToast` — the column's value was
//!     not sent (an unchanged TOAST value on an UPDATE), so the key is left
//!     out so the upsert keeps the existing value. (Emitting `null` here
//!     would clobber the real value.)
//!
//! Why not type-aware decoding? The pgoutput stream only carries the
//! column's type oid, not the type itself. Parsing `oid → SQL type →
//! native value` requires either a hardcoded oid table (fragile) or
//! a round-trip to `pg_type` (slow). The downstream search index can
//! re-parse the text representation per its own schema.

use ventstream_core::{ContentType, Event, Headers, Payload, SourceUri, Subject};

use super::pgoutput::{Delete, Insert, OldTuple, Relation, Tuple, TupleColumn, Update};
use crate::error::PostgresCdcError;

/// Build a [`SourceUri`] for events originating from a relation in a
/// given publication.
///
/// Format: `postgres://{publication}/{namespace}/{relation_name}` with
/// each component percent-encoded. The `/` separator (rather than `.`)
/// keeps schema and table names visually distinct and lets dots in
/// identifier names round-trip as `%2E` without ambiguity.
pub fn source_uri(
    publication: &str,
    namespace: &str,
    relation_name: &str,
) -> Result<SourceUri, PostgresCdcError> {
    SourceUri::new(format!(
        "postgres://{}/{}/{}",
        percent_encode(publication),
        percent_encode(namespace),
        percent_encode(relation_name),
    ))
    .map_err(|err| PostgresCdcError::Internal(err.to_string()))
}

/// Build a [`Subject`] for a CDC event.
///
/// Format: `postgres.{namespace}.{relation_name}.{op}` after sanitizing
/// `namespace` and `relation_name` via `sanitize_subject_segment`.
/// Sanitization replaces any character outside `[A-Za-z0-9_-]` with `_`,
/// so a schema named `"my.weird"` (literal dot) becomes `my_weird` and
/// the dot can no longer fracture the subject's hierarchy. Unicode and
/// other multi-byte characters collapse to one `_` per code point.
///
/// The raw (unsanitized) namespace and relation name are always
/// preserved in event headers under `ventstream.cdc.namespace` and
/// `ventstream.cdc.relation`, plus a unique `ventstream.cdc.relation_oid`
/// header — so downstream consumers can recover original names and
/// disambiguate the rare cases where two real identifiers sanitize to
/// the same segment.
pub fn subject_for_op(
    namespace: &str,
    relation_name: &str,
    op: &str,
) -> Result<Subject, PostgresCdcError> {
    Subject::new(format!(
        "postgres.{}.{}.{}",
        sanitize_subject_segment(namespace),
        sanitize_subject_segment(relation_name),
        op,
    ))
    .map_err(|err| PostgresCdcError::Internal(err.to_string()))
}

/// Sanitize a single identifier so it is safe as one segment of a NATS-
/// style subject (`[A-Za-z0-9_-]+`).
///
/// Any character outside that set is replaced with `_`. If the input is
/// empty, or sanitization removes every character, the result is `_`.
/// This guarantees the function always returns a valid segment.
fn sanitize_subject_segment(input: &str) -> String {
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

/// Percent-encode bytes that are not in the URI unreserved set
/// (`A–Z`, `a–z`, `0–9`, `-`, `.`, `_`, `~`).
///
/// Used for source URI components so dots, spaces, slashes, and
/// non-ASCII bytes round-trip losslessly.
fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            // `from_digit` is total for 0..16; both halves of a byte
            // are by construction in range. `unwrap_or('0')` removes
            // the panic path while remaining unreachable in practice.
            let high = char::from_digit(u32::from(byte >> 4), 16)
                .unwrap_or('0')
                .to_ascii_uppercase();
            let low = char::from_digit(u32::from(byte & 0x0F), 16)
                .unwrap_or('0')
                .to_ascii_uppercase();
            out.push('%');
            out.push(high);
            out.push(low);
        }
    }
    out
}

/// Map an `INSERT` pgoutput message plus its cached relation schema
/// into a [`ventstream_core::Event`].
pub fn insert_to_event(
    publication: &str,
    relation: &Relation,
    insert: &Insert,
) -> Result<Event, PostgresCdcError> {
    check_tuple_arity(relation, &insert.tuple, "INSERT")?;
    let source = source_uri(publication, &relation.namespace, &relation.name)?;
    let subject = subject_for_op(&relation.namespace, &relation.name, "insert")?;
    let payload_json =
        serde_json::to_vec(&tuple_as_json_value(relation, &insert.tuple.columns)?)
            .map_err(|err| PostgresCdcError::Internal(format!("JSON encode failed: {err}")))?;
    let mut headers = default_headers(relation);
    if let Some(doc_id) = tuple_doc_id(relation, &insert.tuple.columns)? {
        headers = headers.with_header(DOC_ID_HEADER.to_owned(), doc_id);
    }
    Ok(build_event(source, subject, payload_json, headers))
}

/// Header marking an UPDATE whose new-row image omitted one or more
/// unchanged TOAST columns. Flat sinks full-replace documents, so the
/// source must re-read the complete row before such an event reaches
/// them — otherwise the omitted fields silently vanish from the
/// destination document.
pub const TOAST_INCOMPLETE_HEADER: &str = "ventstream.pg.toast_incomplete";

/// Map an `UPDATE` pgoutput message into an [`Event`].
pub fn update_to_event(
    publication: &str,
    relation: &Relation,
    update: &Update,
) -> Result<Event, PostgresCdcError> {
    check_tuple_arity(relation, &update.new, "UPDATE.new")?;
    if let Some(old) = update.old.as_ref() {
        check_tuple_arity(relation, &old.tuple, "UPDATE.old")?;
    }
    let source = source_uri(publication, &relation.namespace, &relation.name)?;
    let subject = subject_for_op(&relation.namespace, &relation.name, "update")?;

    let mut envelope = serde_json::Map::with_capacity(2);
    envelope.insert(
        "new".into(),
        tuple_as_json_value(relation, &update.new.columns)?,
    );
    envelope.insert(
        "old".into(),
        match update.old.as_ref() {
            Some(old) => tuple_as_json_value(relation, &old.tuple.columns)?,
            None => serde_json::Value::Null,
        },
    );

    let payload_json = serde_json::to_vec(&serde_json::Value::Object(envelope))
        .map_err(|err| PostgresCdcError::Internal(format!("JSON encode failed: {err}")))?;
    let mut headers = default_headers(relation);
    if update
        .new
        .columns
        .iter()
        .any(|column| matches!(column, TupleColumn::UnchangedToast))
    {
        headers = headers.with_header(TOAST_INCOMPLETE_HEADER.to_owned(), "1".to_owned());
    }
    if let Some(doc_id) = tuple_doc_id(relation, &update.new.columns)? {
        // pgoutput only ships an old tuple when the key changed (or under
        // REPLICA IDENTITY FULL); a differing old key means the doc moved
        // and the previous doc must be removed by the sink.
        if let Some(old) = update.old.as_ref() {
            if let Some(old_id) = tuple_doc_id(relation, &old.tuple.columns)? {
                if old_id != doc_id {
                    headers = headers.with_header(OLD_DOC_ID_HEADER.to_owned(), old_id);
                }
            }
        }
        headers = headers.with_header(DOC_ID_HEADER.to_owned(), doc_id);
    }
    Ok(build_event(source, subject, payload_json, headers))
}

/// Map a `DELETE` pgoutput message into an [`Event`].
pub fn delete_to_event(
    publication: &str,
    relation: &Relation,
    delete: &Delete,
) -> Result<Event, PostgresCdcError> {
    check_old_tuple_arity(relation, &delete.old, "DELETE.old")?;
    let source = source_uri(publication, &relation.namespace, &relation.name)?;
    let subject = subject_for_op(&relation.namespace, &relation.name, "delete")?;

    let mut envelope = serde_json::Map::with_capacity(1);
    envelope.insert(
        "old".into(),
        tuple_as_json_value(relation, &delete.old.tuple.columns)?,
    );

    let payload_json = serde_json::to_vec(&serde_json::Value::Object(envelope))
        .map_err(|err| PostgresCdcError::Internal(format!("JSON encode failed: {err}")))?;
    let mut headers = default_headers(relation);
    if let Some(doc_id) = tuple_doc_id(relation, &delete.old.tuple.columns)? {
        headers = headers.with_header(DOC_ID_HEADER.to_owned(), doc_id);
    }
    Ok(build_event(source, subject, payload_json, headers))
}

/// Map a `TRUNCATE` of one specific relation into an [`Event`]. The
/// caller is responsible for iterating each relation oid in the
/// pgoutput `Truncate` message and resolving names via the schema
/// cache.
pub fn truncate_to_event(
    publication: &str,
    relation: &Relation,
    cascade: bool,
    restart_identity: bool,
) -> Result<Event, PostgresCdcError> {
    let source = source_uri(publication, &relation.namespace, &relation.name)?;
    let subject = subject_for_op(&relation.namespace, &relation.name, "truncate")?;

    let envelope = serde_json::json!({
        "cascade": cascade,
        "restart_identity": restart_identity,
    });
    let payload_json = serde_json::to_vec(&envelope)
        .map_err(|err| PostgresCdcError::Internal(format!("JSON encode failed: {err}")))?;
    Ok(build_event(
        source,
        subject,
        payload_json,
        default_headers(relation),
    ))
}

fn build_event(
    source: SourceUri,
    subject: Subject,
    payload_json: Vec<u8>,
    headers: Headers,
) -> Event {
    Event::builder(source, subject)
        .payload(Payload::from_vec(payload_json))
        .content_type(ContentType::Json)
        .headers(headers)
        .build()
}

fn check_tuple_arity(relation: &Relation, tuple: &Tuple, op: &str) -> Result<(), PostgresCdcError> {
    if tuple.columns.len() != relation.columns.len() {
        return Err(PostgresCdcError::Internal(format!(
            "{op} tuple has {} columns but relation '{}.{}' (oid {}) has {}",
            tuple.columns.len(),
            relation.namespace,
            relation.name,
            relation.id,
            relation.columns.len(),
        )));
    }
    Ok(())
}

fn check_old_tuple_arity(
    relation: &Relation,
    old: &OldTuple,
    op: &str,
) -> Result<(), PostgresCdcError> {
    check_tuple_arity(relation, &old.tuple, op)
}

fn tuple_as_json_value(
    relation: &Relation,
    columns: &[TupleColumn],
) -> Result<serde_json::Value, PostgresCdcError> {
    let mut object = serde_json::Map::with_capacity(columns.len());
    for (col_schema, col_value) in relation.columns.iter().zip(columns.iter()) {
        // `None` ⇒ omit the key entirely (an unchanged TOAST value); see
        // `column_to_json`.
        if let Some(value) = column_to_json(col_schema.name.as_str(), col_value)? {
            object.insert(col_schema.name.clone(), value);
        }
    }
    Ok(serde_json::Value::Object(object))
}

/// Map one column value to JSON, or `None` to **omit the key**.
///
/// `UnchangedToast` (pgoutput's `u`) means a TOASTed column was *not* sent
/// because it didn't change on an UPDATE. We must omit it — emitting JSON
/// `null` would overwrite the existing (large) value with null in the
/// upsert sink, silently corrupting the row (H4). A real SQL `Null` is
/// distinct and *is* emitted as JSON `null`.
fn column_to_json(
    name: &str,
    value: &TupleColumn,
) -> Result<Option<serde_json::Value>, PostgresCdcError> {
    Ok(match value {
        TupleColumn::Null => Some(serde_json::Value::Null),
        TupleColumn::UnchangedToast => None,
        TupleColumn::Text(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|err| {
                PostgresCdcError::Internal(format!(
                    "non-UTF8 text payload for column '{name}': {err}"
                ))
            })?;
            Some(serde_json::Value::String(text.to_owned()))
        }
        TupleColumn::Binary(bytes) => Some(serde_json::Value::String(hex_encode(bytes))),
    })
}

const DOC_ID_HEADER: &str = "ventstream.doc.id";
const OLD_DOC_ID_HEADER: &str = "ventstream.doc.old_id";

/// Key components from a tuple using the replica-identity column flags.
///
/// `None` when the relation advertises no key columns (PK-less table) or a
/// key value is unavailable — the event then ships without a stable doc id
/// and sinks apply their own policy.
fn key_components(
    relation: &Relation,
    columns: &[TupleColumn],
) -> Result<Option<Vec<String>>, PostgresCdcError> {
    let mut components = Vec::new();
    let mut any_key = false;
    for (schema, value) in relation.columns.iter().zip(columns.iter()) {
        if schema.flags.is_key_part() {
            any_key = true;
            match column_to_json(schema.name.as_str(), value)? {
                Some(value) if !value.is_null() => {
                    components.push(ventstream_core::doc_id::component_text(&value));
                }
                _ => return Ok(None),
            }
        }
    }
    Ok(any_key.then_some(components))
}

/// Stable doc id for a tuple: `{namespace}.{name}:["pk",...]`, matching the
/// SQL-denormalize path byte for byte.
fn tuple_doc_id(
    relation: &Relation,
    columns: &[TupleColumn],
) -> Result<Option<String>, PostgresCdcError> {
    let table = format!("{}.{}", relation.namespace, relation.name);
    Ok(key_components(relation, columns)?
        .map(|components| ventstream_core::doc_id::doc_id(&table, &components)))
}

/// Headers attached to every CDC event.
///
/// We keep the **raw** identifiers here — never sanitized — so downstream
/// consumers can always recover the original Postgres schema and table
/// names. The relation oid is also surfaced; it is the only truly
/// unique handle on a relation across the lifetime of a database and
/// disambiguates the rare cases where sanitization collapses two
/// distinct names to the same subject segment.
fn default_headers(relation: &Relation) -> Headers {
    use std::collections::HashMap;
    let mut map = HashMap::with_capacity(3);
    map.insert(
        "ventstream.cdc.namespace".into(),
        relation.namespace.clone(),
    );
    map.insert("ventstream.cdc.relation".into(), relation.name.clone());
    map.insert(
        "ventstream.cdc.relation_oid".into(),
        relation.id.to_string(),
    );
    Headers::from_map(map)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        // `from_digit` always succeeds for 0..16 — both halves of a byte
        // are by construction in range. The default fallback to '0' is
        // unreachable but keeps us off the panic path.
        let high = char::from_digit(u32::from(b >> 4), 16).unwrap_or('0');
        let low = char::from_digit(u32::from(b & 0x0F), 16).unwrap_or('0');
        out.push(high);
        out.push(low);
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
    use crate::postgres::pgoutput::{
        Column, ColumnFlags, OldTuple, OldTupleKind, ReplicaIdentity, Tuple,
    };

    fn users_relation() -> Relation {
        Relation {
            id: 16_384,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                Column {
                    flags: ColumnFlags(0x01),
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    flags: ColumnFlags(0x00),
                    name: "email".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    #[test]
    fn unchanged_toast_updates_are_flagged_for_refetch() {
        let rel = users_relation();
        let update = Update {
            relation_id: rel.id,
            old: None,
            new: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::UnchangedToast,
                ],
            },
        };
        let event = update_to_event("p", &rel, &update).expect("update");
        assert_eq!(event.headers.get(TOAST_INCOMPLETE_HEADER), Some("1"));
        // The omitted column must not appear as null in the payload.
        let payload: serde_json::Value =
            serde_json::from_slice(event.payload.as_slice()).expect("json");
        assert!(payload["new"].get("email").is_none());

        // A complete image carries no flag.
        let complete = Update {
            relation_id: rel.id,
            old: None,
            new: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::Text(b"a@b.c".to_vec()),
                ],
            },
        };
        let event = update_to_event("p", &rel, &complete).expect("update");
        assert_eq!(event.headers.get(TOAST_INCOMPLETE_HEADER), None);
    }

    #[test]
    fn flat_events_carry_the_canonical_doc_id() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::Text(b"alice@example.com".to_vec()),
                ],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("insert");
        assert_eq!(
            event.headers.get("ventstream.doc.id"),
            Some("public.users:[\"42\"]")
        );

        let update = Update {
            relation_id: rel.id,
            old: None,
            new: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::Text(b"new@example.com".to_vec()),
                ],
            },
        };
        let event = update_to_event("p", &rel, &update).expect("update");
        assert_eq!(
            event.headers.get("ventstream.doc.id"),
            Some("public.users:[\"42\"]")
        );
        assert_eq!(event.headers.get("ventstream.doc.old_id"), None);

        let delete = Delete {
            relation_id: rel.id,
            old: OldTuple {
                kind: OldTupleKind::Key,
                tuple: Tuple {
                    columns: vec![TupleColumn::Text(b"42".to_vec()), TupleColumn::Null],
                },
            },
        };
        let event = delete_to_event("p", &rel, &delete).expect("delete");
        assert_eq!(
            event.headers.get("ventstream.doc.id"),
            Some("public.users:[\"42\"]")
        );
    }

    #[test]
    fn key_changing_update_carries_the_previous_doc_id() {
        let rel = users_relation();
        let update = Update {
            relation_id: rel.id,
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                tuple: Tuple {
                    columns: vec![TupleColumn::Text(b"42".to_vec()), TupleColumn::Null],
                },
            }),
            new: Tuple {
                columns: vec![
                    TupleColumn::Text(b"43".to_vec()),
                    TupleColumn::Text(b"alice@example.com".to_vec()),
                ],
            },
        };
        let event = update_to_event("p", &rel, &update).expect("update");
        assert_eq!(
            event.headers.get("ventstream.doc.id"),
            Some("public.users:[\"43\"]")
        );
        assert_eq!(
            event.headers.get("ventstream.doc.old_id"),
            Some("public.users:[\"42\"]")
        );
    }

    #[test]
    fn keyless_relations_ship_without_a_doc_id() {
        let mut rel = users_relation();
        for column in &mut rel.columns {
            column.flags = ColumnFlags(0x00);
        }
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::Text(b"alice@example.com".to_vec()),
                ],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("insert");
        assert_eq!(event.headers.get("ventstream.doc.id"), None);
    }

    #[test]
    fn insert_with_text_columns_renders_json() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::Text(b"alice@example.com".to_vec()),
                ],
            },
        };

        let event = insert_to_event("ventstream_pub", &rel, &insert).expect("event");
        assert_eq!(
            event.source.as_str(),
            "postgres://ventstream_pub/public/users"
        );
        assert_eq!(event.subject.as_str(), "postgres.public.users.insert");
        assert_eq!(event.content_type, ContentType::Json);
        let json: serde_json::Value =
            serde_json::from_slice(event.payload.as_slice()).expect("json");
        assert_eq!(json["id"], "42");
        assert_eq!(json["email"], "alice@example.com");
    }

    #[test]
    fn insert_with_null_column_renders_json_null() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![TupleColumn::Text(b"42".to_vec()), TupleColumn::Null],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("event");
        let json: serde_json::Value =
            serde_json::from_slice(event.payload.as_slice()).expect("json");
        assert!(json["email"].is_null());
        // Real NULL keeps the key present (contrast with UnchangedToast below).
        assert!(json.as_object().expect("obj").contains_key("email"));
    }

    #[test]
    fn unchanged_toast_column_is_omitted_not_nulled() {
        // H4: on an UPDATE where the TOASTed `email` column didn't change,
        // pgoutput sends `UnchangedToast`. The key must be OMITTED so the
        // upsert keeps the existing value — emitting `null` would clobber it.
        // (tuple_as_json_value is shared with the UPDATE path, so an INSERT
        // fixture exercises the same omit logic.)
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"42".to_vec()),
                    TupleColumn::UnchangedToast,
                ],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("event");
        let json: serde_json::Value =
            serde_json::from_slice(event.payload.as_slice()).expect("json");
        assert_eq!(json["id"], "42");
        assert!(
            json.as_object().expect("obj").get("email").is_none(),
            "unchanged-TOAST column must be omitted, not null; got {json}"
        );
    }

    #[test]
    fn insert_with_binary_column_renders_hex_string() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"1".to_vec()),
                    TupleColumn::Binary(vec![0xAB, 0xCD]),
                ],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("event");
        let json: serde_json::Value =
            serde_json::from_slice(event.payload.as_slice()).expect("json");
        assert_eq!(json["email"], "abcd");
    }

    #[test]
    fn mismatched_column_count_returns_error() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![TupleColumn::Text(b"only one".to_vec())],
            },
        };
        let err = insert_to_event("p", &rel, &insert).expect_err("err");
        assert!(matches!(err, PostgresCdcError::Internal(_)));
    }

    #[test]
    fn invalid_utf8_in_text_column_returns_error() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"1".to_vec()),
                    TupleColumn::Text(vec![0xFF, 0xFE]),
                ],
            },
        };
        let err = insert_to_event("p", &rel, &insert).expect_err("err");
        assert!(matches!(err, PostgresCdcError::Internal(_)));
    }

    #[test]
    fn event_includes_cdc_headers() {
        let rel = users_relation();
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![
                    TupleColumn::Text(b"1".to_vec()),
                    TupleColumn::Text(b"x".to_vec()),
                ],
            },
        };
        let event = insert_to_event("p", &rel, &insert).expect("event");
        assert_eq!(
            event.headers.get("ventstream.cdc.namespace"),
            Some("public")
        );
        assert_eq!(event.headers.get("ventstream.cdc.relation"), Some("users"));
        assert_eq!(
            event.headers.get("ventstream.cdc.relation_oid"),
            Some("16384"),
            "relation oid must be present so consumers can disambiguate"
        );
    }

    // ---- sanitization & encoding edge cases -----------------------------

    #[test]
    fn sanitize_replaces_dots_with_underscore() {
        assert_eq!(sanitize_subject_segment("my.weird"), "my_weird");
        assert_eq!(sanitize_subject_segment("a.b.c"), "a_b_c");
    }

    #[test]
    fn sanitize_keeps_alphanumerics_underscore_hyphen() {
        assert_eq!(sanitize_subject_segment("tenant_42"), "tenant_42");
        assert_eq!(sanitize_subject_segment("Tenant-A"), "Tenant-A");
        assert_eq!(sanitize_subject_segment("ABC123"), "ABC123");
    }

    #[test]
    fn sanitize_replaces_unicode_with_underscores_per_char() {
        // Each char (regardless of byte width) becomes one underscore.
        assert_eq!(sanitize_subject_segment("événement"), "_v_nement");
    }

    #[test]
    fn sanitize_replaces_spaces_and_punctuation() {
        assert_eq!(sanitize_subject_segment("hello world!"), "hello_world_");
    }

    #[test]
    fn sanitize_returns_single_underscore_for_empty_or_all_invalid() {
        assert_eq!(sanitize_subject_segment(""), "_");
        // String of dots is all-invalid — collapses to `___` (one `_` per char).
        assert_eq!(sanitize_subject_segment("..."), "___");
    }

    #[test]
    fn percent_encode_keeps_unreserved_chars() {
        assert_eq!(percent_encode("abc-123_def.foo~bar"), "abc-123_def.foo~bar");
    }

    #[test]
    fn percent_encode_escapes_space_and_special_chars() {
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("a/b"), "a%2Fb");
    }

    #[test]
    fn percent_encode_escapes_utf8_byte_by_byte() {
        // 'é' is UTF-8 0xC3 0xA9 → "%C3%A9"
        assert_eq!(percent_encode("café"), "caf%C3%A9");
    }

    #[test]
    fn subject_for_op_sanitizes_dotted_schema() {
        // Without sanitization the dot would split the schema across
        // two subject segments and corrupt routing.
        let subject = subject_for_op("my.weird", "users", "insert").expect("subject");
        assert_eq!(subject.as_str(), "postgres.my_weird.users.insert");
    }

    #[test]
    fn subject_for_op_handles_unicode_schema() {
        let subject = subject_for_op("événement", "logs", "delete").expect("subject");
        assert_eq!(subject.as_str(), "postgres._v_nement.logs.delete");
    }

    #[test]
    fn source_uri_keeps_dots_as_unreserved_and_encodes_unicode() {
        // `.` is in the RFC 3986 unreserved set, so it stays literal in
        // path segments. With `/` as the segment separator, a dot inside
        // a name is unambiguous. Non-ASCII gets percent-encoded byte-by-byte.
        let uri = source_uri("vspub", "my.weird", "café").expect("uri");
        assert_eq!(uri.as_str(), "postgres://vspub/my.weird/caf%C3%A9");
    }

    #[test]
    fn source_uri_encodes_slashes_and_spaces_inside_segments() {
        // A literal `/` inside an identifier WOULD collide with the
        // separator, so it must be percent-encoded.
        let uri = source_uri("vspub", "weird/name", "with space").expect("uri");
        assert_eq!(uri.as_str(), "postgres://vspub/weird%2Fname/with%20space");
    }

    #[test]
    fn raw_identifier_preserved_in_headers_even_when_sanitized() {
        // Build a fake relation with a dotted, unicode-ish namespace and
        // verify the raw value flows through to headers untouched while
        // the subject and URI are safely encoded.
        let rel = Relation {
            id: 99_999,
            namespace: "my.weird".into(),
            name: "café".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![Column {
                flags: ColumnFlags(0x01),
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
            }],
        };
        let insert = Insert {
            relation_id: rel.id,
            tuple: Tuple {
                columns: vec![TupleColumn::Text(b"1".to_vec())],
            },
        };
        let event = insert_to_event("vspub", &rel, &insert).expect("event");

        assert_eq!(event.subject.as_str(), "postgres.my_weird.caf_.insert");
        assert_eq!(event.source.as_str(), "postgres://vspub/my.weird/caf%C3%A9");
        assert_eq!(
            event.headers.get("ventstream.cdc.namespace"),
            Some("my.weird"),
            "raw namespace must be preserved verbatim in headers"
        );
        assert_eq!(
            event.headers.get("ventstream.cdc.relation"),
            Some("café"),
            "raw relation name must be preserved verbatim in headers"
        );
        assert_eq!(
            event.headers.get("ventstream.cdc.relation_oid"),
            Some("99999"),
        );
    }
}
