//! Composite-aware key type used by every state index.
//!
//! [`PkValue`] holds one or more JSON values that together identify a
//! row (a single-column PK becomes a 1-element list; a composite
//! `(region, id)` becomes a 2-element list).
//!
//! The internal representation is the canonical JSON bytes of the
//! values, which gives us:
//! - O(1) hash + equality via the byte buffer
//! - Stable ordering across runs (same input → same bytes)
//! - Trivial Debug / Display rendering for log lines

use std::fmt;

use serde_json::Value;

/// A primary or foreign key value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PkValue {
    bytes: Vec<u8>,
}

impl PkValue {
    /// Build a key from a tuple of JSON values (one per column).
    ///
    /// Empty input is treated as the "null key" — used as a sentinel
    /// when a primary's FK columns are all NULL. Such rows still get
    /// indexed but they all collide on the same bucket. That matches
    /// SQL semantics (NULL ≠ NULL) only loosely — operators should
    /// avoid building joins on nullable FKs.
    pub fn from_values(values: &[Value]) -> Self {
        // Normalize each component before encoding so the two join sides agree
        // on equality even when they emit a PK in different JSON types (M2):
        // a number `5` and the string `"5"` both become `"5"`, so they don't
        // silently miss the join. NULL is left as JSON null (the null-key
        // sentinel — see the type docs).
        let normalized: Vec<Value> = values.iter().map(normalize_component).collect();
        let bytes = serde_json::to_vec(&normalized).unwrap_or_default();
        Self { bytes }
    }

    /// Convenience for the common single-column case.
    pub fn from_single(value: &Value) -> Self {
        Self::from_values(std::slice::from_ref(value))
    }

    /// Decode back to a JSON array. Returns an empty array if the
    /// buffer is empty (the null-key sentinel).
    pub fn to_json(&self) -> Value {
        if self.bytes.is_empty() {
            return Value::Array(Vec::new());
        }
        serde_json::from_slice(&self.bytes).unwrap_or(Value::Null)
    }

    /// Whether this key is the null sentinel.
    pub fn is_null(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Borrow the canonical JSON-bytes encoding. Used by the
    /// persistence layer to key redb tables; safe to round-trip
    /// through [`Self::from_bytes`].
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Reconstruct a [`PkValue`] from its canonical byte encoding.
    /// Inverse of [`Self::as_bytes`].
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl fmt::Display for PkValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.bytes.is_empty() {
            return f.write_str("∅");
        }
        match std::str::from_utf8(&self.bytes) {
            Ok(s) => f.write_str(s),
            Err(_) => write!(f, "{:?}", self.bytes),
        }
    }
}

/// Canonicalize one key component so the two join sides compare equal
/// regardless of which JSON scalar type they emitted it as (M2).
///
/// Scalars collapse to their text form (`5` → `"5"`, `true` → `"true"`), so a
/// number-typed PK on one side matches the same value rendered as a string on
/// the other. `null` is preserved as JSON null — that's the null-key sentinel
/// (NULL FKs deliberately collide on one bucket). Arrays/objects (not valid PK
/// components in practice) pass through unchanged.
fn normalize_component(v: &Value) -> Value {
    match v {
        Value::Null | Value::String(_) | Value::Array(_) | Value::Object(_) => v.clone(),
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Number(n) => Value::String(n.to_string()),
    }
}

/// Extract a [`PkValue`] from a JSON object using the named columns.
/// Missing columns are treated as `null` (still part of the key).
///
/// Returns `None` if `payload` is not a JSON object.
pub fn extract_pk(payload: &Value, columns: &[String]) -> Option<PkValue> {
    let obj = payload.as_object()?;
    let mut values = Vec::with_capacity(columns.len());
    for col in columns {
        values.push(obj.get(col).cloned().unwrap_or(Value::Null));
    }
    Some(PkValue::from_values(&values))
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

    #[test]
    fn single_column_round_trip() {
        // M2: numeric components are text-normalized, so a single-column key
        // round-trips as the text form.
        let pk = PkValue::from_single(&json!(42));
        let back = pk.to_json();
        assert_eq!(back, json!(["42"]));
    }

    #[test]
    fn composite_round_trip() {
        let pk = PkValue::from_values(&[json!("us-east"), json!(7)]);
        let back = pk.to_json();
        assert_eq!(back, json!(["us-east", "7"]));
    }

    #[test]
    fn number_and_string_components_compare_equal() {
        // The M2 fix: a PK emitted as a JSON number by one join side and as a
        // string by the other must produce the SAME key, or the join silently
        // misses.
        assert_eq!(
            PkValue::from_single(&json!(5)),
            PkValue::from_single(&json!("5"))
        );
        assert_eq!(
            PkValue::from_values(&[json!("r"), json!(7)]),
            PkValue::from_values(&[json!("r"), json!("7")]),
        );
        assert_eq!(
            PkValue::from_single(&json!(true)),
            PkValue::from_single(&json!("true")),
        );
        // Distinct values still differ.
        assert_ne!(
            PkValue::from_single(&json!(5)),
            PkValue::from_single(&json!(6))
        );
    }

    #[test]
    fn null_components_form_part_of_key() {
        let a = PkValue::from_values(&[json!(1), Value::Null]);
        let b = PkValue::from_values(&[json!(1), json!(0)]);
        assert_ne!(a, b);
        // NULL stays JSON null (the sentinel), NOT the string "null" — so a
        // real "null" string can't collide with a SQL NULL.
        assert_ne!(
            PkValue::from_single(&Value::Null),
            PkValue::from_single(&json!("null")),
        );
    }

    #[test]
    fn extract_pk_with_missing_column_substitutes_null() {
        let payload = json!({ "id": 1 });
        let pk = extract_pk(&payload, &["id".into(), "missing".into()]).expect("object");
        assert_eq!(pk.to_json(), json!(["1", null]));
    }

    #[test]
    fn extract_pk_non_object_returns_none() {
        let payload = json!([1, 2, 3]);
        assert!(extract_pk(&payload, &["id".into()]).is_none());
    }

    #[test]
    fn display_renders_canonical_json_for_humans() {
        // Numeric components are text-normalized (M2), so they render quoted.
        let pk = PkValue::from_values(&[json!("ab"), json!(3)]);
        assert_eq!(pk.to_string(), r#"["ab","3"]"#);
    }
}
