//! CBOR transport codec for the SurrealDB RPC endpoint.
//!
//! The JSON RPC protocol is lossy by design: SurrealDB revives incoming
//! strings that parse as SurrealQL literals, so `"a:b"` becomes record
//! `a:b`, `"mailto:x@y"` becomes a record, ISO text becomes a datetime —
//! and record parsing stops at the first unparsable segment, silently
//! truncating opaque keys like Neo4j elementIds (`products:4:uuid:6` →
//! `products:4`). CBOR is the type-faithful protocol: text stays text.
//! Requests encode `serde_json::Value` params directly (plain CBOR, no
//! tags); responses arrive with SurrealDB's semantic tags, which decode
//! back to the JSON shapes the rest of the sink already consumes.

use ciborium::value::{Integer, Value as Cbor};
use serde_json::{json, Value as Json};

/// SurrealDB CBOR tags (surrealdb/core/src/rpc/format/cbor).
const TAG_STRING_DATETIME: u64 = 0;
const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORD_ID: u64 = 8;
const TAG_STRING_UUID: u64 = 9;
const TAG_STRING_DECIMAL: u64 = 10;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_STRING_DURATION: u64 = 13;
const TAG_CUSTOM_DURATION: u64 = 14;

/// Serialize an RPC body to CBOR bytes.
pub(crate) fn encode<T: serde::Serialize>(body: &T) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(body, &mut bytes).map_err(|err| err.to_string())?;
    Ok(bytes)
}

/// Decode a CBOR RPC response into the JSON value shape the response
/// handlers consume. Tagged values render to their canonical strings.
pub(crate) fn decode_to_json(bytes: &[u8]) -> Result<Json, String> {
    let value: Cbor = ciborium::de::from_reader(bytes).map_err(|err| err.to_string())?;
    Ok(to_json(value))
}

fn to_json(value: Cbor) -> Json {
    match value {
        Cbor::Null => Json::Null,
        Cbor::Bool(b) => Json::Bool(b),
        Cbor::Integer(i) => integer_to_json(i),
        Cbor::Float(f) => serde_json::Number::from_f64(f).map_or(Json::Null, Json::Number),
        Cbor::Text(t) => Json::String(t),
        Cbor::Bytes(b) => Json::String(String::from_utf8_lossy(&b).into_owned()),
        Cbor::Array(items) => Json::Array(items.into_iter().map(to_json).collect()),
        Cbor::Map(entries) => Json::Object(
            entries
                .into_iter()
                .map(|(key, val)| (key_to_string(key), to_json(val)))
                .collect(),
        ),
        Cbor::Tag(tag, inner) => tagged_to_json(tag, *inner),
        _ => Json::Null,
    }
}

fn tagged_to_json(tag: u64, inner: Cbor) -> Json {
    match tag {
        TAG_NONE => Json::Null,
        TAG_RECORD_ID => record_id_to_json(inner),
        TAG_CUSTOM_DATETIME => custom_datetime_to_json(inner),
        TAG_CUSTOM_DURATION => custom_duration_to_json(inner),
        // String-canonical tags (datetime, table, uuid, decimal,
        // duration) carry their text form as the inner value.
        TAG_STRING_DATETIME | TAG_TABLE | TAG_STRING_UUID | TAG_STRING_DECIMAL
        | TAG_STRING_DURATION => to_json(inner),
        _ => to_json(inner),
    }
}

/// Record ids arrive as `(table, key)`; render the canonical
/// `table:key` string so log lines and assertions stay readable.
fn record_id_to_json(inner: Cbor) -> Json {
    let rendered = match inner {
        Cbor::Array(mut parts) if parts.len() == 2 => {
            let key = to_json(parts.pop().unwrap_or(Cbor::Null));
            let table = to_json(parts.pop().unwrap_or(Cbor::Null));
            let table = table.as_str().map(ToOwned::to_owned).unwrap_or_default();
            let key = match key {
                Json::String(s) => s,
                other => other.to_string(),
            };
            format!("{table}:{key}")
        }
        other => match to_json(other) {
            Json::String(s) => s,
            other => other.to_string(),
        },
    };
    Json::String(rendered)
}

fn custom_datetime_to_json(inner: Cbor) -> Json {
    if let Cbor::Array(parts) = &inner {
        if let (Some(Cbor::Integer(secs)), Some(Cbor::Integer(nanos))) =
            (parts.first(), parts.get(1))
        {
            let secs = i128::from(*secs) as i64;
            let nanos = i128::from(*nanos) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                return Json::String(dt.to_rfc3339());
            }
        }
    }
    to_json(inner)
}

fn custom_duration_to_json(inner: Cbor) -> Json {
    if let Cbor::Array(parts) = &inner {
        let secs = parts
            .first()
            .and_then(|v| match v {
                Cbor::Integer(i) => Some(i128::from(*i)),
                _ => None,
            })
            .unwrap_or(0);
        let nanos = parts
            .get(1)
            .and_then(|v| match v {
                Cbor::Integer(i) => Some(i128::from(*i)),
                _ => None,
            })
            .unwrap_or(0);
        return json!(format!("{secs}s{nanos}ns"));
    }
    to_json(inner)
}

fn integer_to_json(value: Integer) -> Json {
    let wide = i128::from(value);
    if let Ok(v) = i64::try_from(wide) {
        return Json::Number(v.into());
    }
    if let Ok(v) = u64::try_from(wide) {
        return Json::Number(v.into());
    }
    Json::String(wide.to_string())
}

fn key_to_string(key: Cbor) -> String {
    match key {
        Cbor::Text(t) => t,
        other => match to_json(other) {
            Json::String(s) => s,
            other => other.to_string(),
        },
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn strings_round_trip_without_literal_revival() {
        // The exact shapes the JSON protocol mangles: colon-bearing
        // opaque keys, URIs, ISO datetimes, uuids.
        let body = json!({
            "method": "query",
            "params": ["RETURN $v", {"v": [
                "products:4:a1ff6fb6-9b57-4547-b3da-4f01274bbb72:6",
                "mailto:user@example.com",
                "2024-01-15T10:30:00Z",
            ]}],
        });
        let bytes = encode(&body).expect("encode");
        let back = decode_to_json(&bytes).expect("decode");
        assert_eq!(back, body, "plain CBOR must round-trip JSON exactly");
    }

    #[test]
    fn surreal_response_tags_decode_to_canonical_strings() {
        let response = Cbor::Map(vec![(
            Cbor::Text("result".into()),
            Cbor::Array(vec![Cbor::Map(vec![
                (Cbor::Text("status".into()), Cbor::Text("OK".into())),
                (
                    Cbor::Text("result".into()),
                    Cbor::Array(vec![Cbor::Map(vec![
                        (
                            Cbor::Text("id".into()),
                            Cbor::Tag(
                                TAG_RECORD_ID,
                                Box::new(Cbor::Array(vec![
                                    Cbor::Text("products".into()),
                                    Cbor::Array(vec![Cbor::Text("4:abc:6".into())]),
                                ])),
                            ),
                        ),
                        (
                            Cbor::Text("none".into()),
                            Cbor::Tag(TAG_NONE, Box::new(Cbor::Null)),
                        ),
                    ])]),
                ),
            ])]),
        )]);
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&response, &mut bytes).expect("serialize");
        let back = decode_to_json(&bytes).expect("decode");
        let row = &back["result"][0]["result"][0];
        assert_eq!(row["id"], json!(r#"products:["4:abc:6"]"#));
        assert_eq!(row["none"], Json::Null);
    }
}
