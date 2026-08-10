//! Event → Meilisearch document translation: primary-key encoding, index
//! UID encoding, and grouping a dispatcher batch into ordered API runs.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest, Sha256};
use ventstream_core::{Event, FailedItem};

use super::config::{MeilisearchConfig, MeilisearchIndexRouting};

pub(super) const DOC_ID_HEADER: &str = "ventstream.doc.id";
pub(super) const RELATION_HEADER: &str = "ventstream.cdc.relation";
pub(super) const PROJECTION_TARGET_HEADER: &str = "ventstream.target.index";
/// Canonical doc id retained on the document for debugging and filtering.
pub(crate) const CANONICAL_ID_FIELD: &str = "_vs_id";

/// Meilisearch primary keys allow `[A-Za-z0-9_-]`, max 511 bytes. The
/// canonical doc id (`table:["pk",...]`) is not representable, so keys are
/// base64url (no padding) of the canonical id — injective and reversible.
/// Ids whose encoding would exceed the cap fall back to a SHA-256 digest.
pub(crate) fn encode_primary_key(doc_id: &str) -> String {
    const MAX_KEY_BYTES: usize = 511;
    let encoded = URL_SAFE_NO_PAD.encode(doc_id.as_bytes());
    if encoded.len() <= MAX_KEY_BYTES {
        encoded
    } else {
        format!("h-{:x}", Sha256::digest(doc_id.as_bytes()))
    }
}

/// Encode one index-UID segment into `[A-Za-z0-9_-]`, injectively:
/// `_` doubles to `__`; any other disallowed byte becomes `_HH` (hex).
pub(crate) fn encode_index_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' => out.push(byte as char),
            b'_' => out.push_str("__"),
            other => {
                out.push('_');
                out.push_str(&format!("{other:02X}"));
            }
        }
    }
    out
}

/// One ordered API call: consecutive same-index, same-operation events.
#[derive(Debug)]
pub(super) struct Run {
    /// Fully encoded index UID including the prefix.
    pub index_uid: String,
    /// Operation and its payload.
    pub kind: RunKind,
    /// Offsets into the original dispatcher batch, in order.
    pub offsets: Vec<usize>,
    /// Approximate encoded body bytes.
    pub bytes: usize,
}

#[derive(Debug)]
pub(super) enum RunKind {
    /// `POST /indexes/{uid}/documents` — full add-or-replace.
    Upsert(Vec<Value>),
    /// `POST /indexes/{uid}/documents/delete-batch` — by primary key.
    Delete(Vec<String>),
    /// `DELETE /indexes/{uid}/documents` — clear the index.
    Truncate,
}

/// Output of batch translation: ordered runs plus client-side rejects
/// (unparseable payloads, missing headers) with exact batch offsets.
pub(super) struct Translated {
    pub runs: Vec<Run>,
    pub rejects: Vec<FailedItem>,
}

/// Translate a dispatcher batch. Order is preserved: runs execute
/// sequentially, and a new run starts whenever the index, the operation
/// class, or a batching limit changes.
pub(super) fn translate_batch(config: &MeilisearchConfig, events: &[Event]) -> Translated {
    let mut runs: Vec<Run> = Vec::new();
    let mut rejects: Vec<FailedItem> = Vec::new();

    for (offset, event) in events.iter().enumerate() {
        let subject = event.subject.as_str();
        let is_delete = subject.ends_with(".delete");
        let is_truncate = subject.ends_with(".truncate");

        let index_uid = match resolve_index(config, event) {
            Ok(uid) => uid,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };

        if is_truncate {
            runs.push(Run {
                index_uid,
                kind: RunKind::Truncate,
                offsets: vec![offset],
                bytes: 0,
            });
            continue;
        }

        let doc_id = match required_doc_id(event) {
            Ok(id) => id,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };
        let primary_key = encode_primary_key(doc_id);

        if is_delete {
            let bytes = primary_key.len() + 4;
            match runs.last_mut() {
                Some(run)
                    if run.index_uid == index_uid
                        && matches!(run.kind, RunKind::Delete(_))
                        && run.offsets.len() < config.batching.max_docs
                        && run.bytes + bytes <= config.batching.max_bytes =>
                {
                    if let RunKind::Delete(keys) = &mut run.kind {
                        keys.push(primary_key);
                    }
                    run.offsets.push(offset);
                    run.bytes += bytes;
                }
                _ => runs.push(Run {
                    index_uid,
                    kind: RunKind::Delete(vec![primary_key]),
                    offsets: vec![offset],
                    bytes,
                }),
            }
            continue;
        }

        let document = match build_document(config, event, &primary_key, doc_id) {
            Ok(doc) => doc,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };
        let bytes = document.to_string().len() + 2;
        if bytes > config.batching.max_bytes {
            rejects.push(FailedItem {
                offset,
                error: format!(
                    "document encodes to {bytes} bytes, exceeding max_bytes={}",
                    config.batching.max_bytes
                ),
            });
            continue;
        }
        match runs.last_mut() {
            Some(run)
                if run.index_uid == index_uid
                    && matches!(run.kind, RunKind::Upsert(_))
                    && run.offsets.len() < config.batching.max_docs
                    && run.bytes + bytes <= config.batching.max_bytes =>
            {
                if let RunKind::Upsert(docs) = &mut run.kind {
                    docs.push(document);
                }
                run.offsets.push(offset);
                run.bytes += bytes;
            }
            _ => runs.push(Run {
                index_uid,
                kind: RunKind::Upsert(vec![document]),
                offsets: vec![offset],
                bytes,
            }),
        }
    }

    Translated { runs, rejects }
}

fn resolve_index(config: &MeilisearchConfig, event: &Event) -> Result<String, String> {
    let target = match &config.index_routing {
        MeilisearchIndexRouting::ByOutputRelation => {
            event.headers.get(RELATION_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{RELATION_HEADER}` for index routing",
                    event.id
                )
            })?
        }
        MeilisearchIndexRouting::ByProjectionTarget => {
            event.headers.get(PROJECTION_TARGET_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{PROJECTION_TARGET_HEADER}` for index routing",
                    event.id
                )
            })?
        }
        MeilisearchIndexRouting::Fixed(index) => index,
    };
    index_uid_for_target(config, target)
        .map_err(|_| format!("event {} routed to an empty index name", event.id))
}

/// Encode one routed target into its full index UID. Fixed routing pins the
/// UID to the configured index regardless of the target; the read path must
/// resolve through here so read UIDs match write UIDs byte-for-byte.
pub(crate) fn index_uid_for_target(
    config: &MeilisearchConfig,
    target: &str,
) -> Result<String, String> {
    let target = match &config.index_routing {
        MeilisearchIndexRouting::Fixed(index) => index,
        MeilisearchIndexRouting::ByOutputRelation | MeilisearchIndexRouting::ByProjectionTarget => {
            target
        }
    };
    if target.is_empty() {
        return Err("empty index name".to_owned());
    }
    Ok(format!(
        "{}{}",
        config.index_prefix,
        encode_index_segment(target)
    ))
}

fn required_doc_id(event: &Event) -> Result<&str, String> {
    let doc_id = event.headers.get(DOC_ID_HEADER).ok_or_else(|| {
        format!(
            "event {} has no `{DOC_ID_HEADER}`; Meilisearch materialization requires a stable document id",
            event.id
        )
    })?;
    if doc_id.is_empty() || doc_id.chars().any(char::is_control) {
        return Err(format!(
            "event {} has an empty or control-character `{DOC_ID_HEADER}`",
            event.id
        ));
    }
    Ok(doc_id)
}

fn build_document(
    config: &MeilisearchConfig,
    event: &Event,
    primary_key: &str,
    doc_id: &str,
) -> Result<Value, String> {
    let parsed: Value = serde_json::from_slice(event.payload.as_slice())
        .map_err(|err| format!("event {} payload is not valid JSON: {err}", event.id))?;
    let Value::Object(mut map) = parsed else {
        return Err(format!(
            "event {} payload is not a JSON object; Meilisearch documents must be objects",
            event.id
        ));
    };
    map.insert(
        config.primary_key_field.clone(),
        Value::String(primary_key.to_owned()),
    );
    map.insert(
        CANONICAL_ID_FIELD.to_owned(),
        Value::String(doc_id.to_owned()),
    );
    Ok(Value::Object(map))
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

    #[test]
    fn primary_key_is_base64url_of_canonical_id() {
        let key = encode_primary_key(r#"orders:["123"]"#);
        assert!(key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        let decoded = URL_SAFE_NO_PAD.decode(key.as_bytes()).expect("decode");
        assert_eq!(decoded, br#"orders:["123"]"#);
    }

    #[test]
    fn oversized_primary_key_falls_back_to_digest() {
        let long = format!(r#"t:["{}"]"#, "x".repeat(600));
        let key = encode_primary_key(&long);
        assert!(key.starts_with("h-"));
        assert!(key.len() <= 511);
    }

    #[test]
    fn index_segment_encoding_is_injective_for_underscores() {
        assert_eq!(encode_index_segment("public.orders"), "public_2Eorders");
        assert_eq!(encode_index_segment("a_b"), "a__b");
        assert_ne!(encode_index_segment("a_2Eb"), encode_index_segment("a.b"));
    }
}
