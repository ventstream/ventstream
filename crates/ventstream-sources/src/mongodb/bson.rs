//! `bson::Bson` → `serde_json::Value` conversion.
//!
//! MongoDB change events and snapshot documents arrive as BSON. The
//! OpenSearch sink (and every other downstream consumer) speaks
//! `serde_json::Value`, so this module bridges them — the BSON analog of
//! the Neo4j source's `bolt.rs`.
//!
//! Conversion choices favour *clean, indexable* JSON over MongoDB
//! Extended JSON: an `ObjectId` becomes its plain hex string (not
//! `{"$oid": …}`), a `DateTime` an ISO-8601 string, a `Decimal128` its
//! decimal string. That way OpenSearch dynamic mapping infers sensible
//! types and the documents read naturally, rather than carrying `$`-tagged
//! wrappers. Exotic/rarely-used variants fall back to their `Display`
//! form so the payload stays structurally valid rather than dropping data.

use mongodb::bson::{Bson, Document};
use serde_json::{json, Value};

/// Convert a BSON value to a JSON value.
pub fn bson_to_json(b: &Bson) -> Value {
    match b {
        Bson::Double(v) => json!(v),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Array(a) => Value::Array(a.iter().map(bson_to_json).collect()),
        Bson::Document(d) => document_to_json(d),
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Int32(v) => json!(v),
        Bson::Int64(v) => json!(v),
        // Plain hex, not `{"$oid": …}` — this is also what the doc-id is
        // built from, so it must be a stable bare string.
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        // RFC 3339 (e.g. `2024-01-15T10:30:00Z`) so OpenSearch dynamic
        // mapping infers `date` — range-queryable and sortable. `Display`
        // would emit `2024-01-15 10:30:00.0 +00:00:00`, which maps as `text`.
        // Fall back to Display only if the (infallible-in-practice) RFC 3339
        // render ever errors.
        Bson::DateTime(dt) => Value::String(
            dt.try_to_rfc3339_string()
                .unwrap_or_else(|_| dt.to_string()),
        ),
        Bson::Decimal128(d) => Value::String(d.to_string()),
        Bson::Binary(bin) => Value::String(hex_encode(&bin.bytes)),
        Bson::Timestamp(ts) => json!({ "t": ts.time, "i": ts.increment }),
        // Symbol, JavaScript code, regex, min/max key, db pointer — keep the
        // value as its Display form rather than discarding it.
        other => Value::String(other.to_string()),
    }
}

/// Convert a BSON document to a JSON object, recursively.
pub fn document_to_json(d: &Document) -> Value {
    let mut m = serde_json::Map::with_capacity(d.len());
    for (k, v) in d {
        m.insert(k.clone(), bson_to_json(v));
    }
    Value::Object(m)
}

/// Lowercase hex, dependency-free (used for `Binary` payloads). Nibbles are
/// always < 16, so `from_digit` never fails; `unwrap_or` keeps it panic-free.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit(u32::from(b >> 4), 16).unwrap_or('0'));
        s.push(char::from_digit(u32::from(b & 0x0f), 16).unwrap_or('0'));
    }
    s
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
    use mongodb::bson::{doc, oid::ObjectId, Bson, DateTime as BsonDateTime};

    #[test]
    fn datetime_is_rfc3339_not_display() {
        // Epoch → `1970-01-01T00:00:00Z` (RFC 3339), so OpenSearch infers a
        // `date` field. The Display form `1970-01-01 0:00:00.0 +00:00:00`
        // would map as `text`.
        let dt = BsonDateTime::from_millis(0);
        assert_eq!(
            bson_to_json(&Bson::DateTime(dt)),
            json!("1970-01-01T00:00:00Z")
        );
    }

    #[test]
    fn primitives_round_trip() {
        assert_eq!(bson_to_json(&Bson::String("hi".into())), json!("hi"));
        assert_eq!(bson_to_json(&Bson::Int32(42)), json!(42));
        assert_eq!(bson_to_json(&Bson::Int64(42)), json!(42));
        assert_eq!(bson_to_json(&Bson::Boolean(true)), json!(true));
        assert_eq!(bson_to_json(&Bson::Null), Value::Null);
    }

    #[test]
    fn object_id_becomes_bare_hex() {
        let oid = ObjectId::parse_str("507f1f77bcf86cd799439011").expect("valid oid");
        assert_eq!(
            bson_to_json(&Bson::ObjectId(oid)),
            json!("507f1f77bcf86cd799439011")
        );
    }

    #[test]
    fn nested_document_and_array() {
        let d = doc! {
            "id": "a1",
            "name": "Ada",
            "tags": ["x", "y"],
            "meta": { "active": true, "n": 3_i32 },
        };
        let v = document_to_json(&d);
        assert_eq!(v["id"], json!("a1"));
        assert_eq!(v["name"], json!("Ada"));
        assert_eq!(v["tags"], json!(["x", "y"]));
        assert_eq!(v["meta"]["active"], json!(true));
        assert_eq!(v["meta"]["n"], json!(3));
    }
}
