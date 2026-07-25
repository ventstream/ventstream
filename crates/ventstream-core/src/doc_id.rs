//! Canonical OpenSearch doc-id encoding — the single source of truth shared
//! by every denormalized-doc producer and the reconcile parser.
//!
//! Three code paths must agree byte-for-byte on the id of a denormalized
//! primary row, or docs silently orphan / get wrongly deleted:
//!
//! 1. the in-memory join engine (`ventstream-joins`) emits doc updates,
//! 2. the SQL-denormalize path (the binary) emits the same docs, and
//! 3. the reconcile pass (`ventstream_sinks::opensearch`) parses doc-ids back
//!    to a key to diff against the source's live PK set.
//!
//! Before this module each had its own encoder. They drifted on numeric keys:
//! the in-memory engine rendered an integer PK as the JSON number array `[5]`
//! while the SQL path rendered the text array `["5"]`, so the *same* row got
//! two different doc-ids depending on mode — switching modes orphaned every
//! integer-keyed doc (M5), and there was no contract test that a producer's
//! id round-trips through the parser (M3).
//!
//! The canonical form is **`{table}:["c1","c2",...]`** with every component
//! **text-normalized** (a number `5`, a string `"5"`, and a bool `true` all
//! collapse to the text the source side produces). This matches the SQL path
//! and the source's `valid_pks`, so all three agree regardless of column type.

use serde_json::Value;

/// Render one PK component to its canonical text form.
///
/// Strings pass through; everything else uses its JSON scalar text (`5`,
/// `true`, `null`) — so an integer PK and the source's text rendering of it
/// produce the same component.
pub fn component_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The doc-id suffix after `{table}:` — a JSON array of the text-normalized
/// components, e.g. `["5"]` or `["a","b"]`. Identical for every producer.
pub fn doc_id_suffix(components: &[String]) -> String {
    serde_json::to_string(components).unwrap_or_else(|_| components.join("|"))
}

/// The full canonical doc-id: `{table}:["c1",...]`.
pub fn doc_id(table: &str, components: &[String]) -> String {
    format!("{table}:{}", doc_id_suffix(components))
}

/// Convert a decoded PK array (one [`Value`] per column) into text components.
/// A non-array (or the null-key sentinel) yields no components.
pub fn components_from_json(pk: &Value) -> Vec<String> {
    match pk {
        Value::Array(arr) => arr.iter().map(component_text).collect(),
        _ => Vec::new(),
    }
}

/// Canonical comparison key for a primary key — shared by the source-side
/// live PK set and the doc-id parser, so a row is recognised as still-present.
///
/// Single-column keys use the bare component (preserving single-column doc-id
/// matching); composite keys use the JSON array of components. Returns `None`
/// for an empty component list (no key to compare).
pub fn pk_key(components: &[String]) -> Option<String> {
    match components {
        [] => None,
        [single] => Some(single.clone()),
        many => serde_json::to_string(many).ok(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integer_and_text_pk_produce_identical_doc_ids() {
        // The M5 regression: an int PK and its text rendering must produce the
        // SAME doc-id, so in-memory mode (typed values) and SQL mode (text)
        // agree on one index.
        let from_typed = components_from_json(&json!([5]));
        let from_text = vec!["5".to_owned()];
        assert_eq!(from_typed, from_text);
        assert_eq!(
            doc_id("public.orders", &from_typed),
            doc_id("public.orders", &from_text),
        );
        assert_eq!(
            doc_id("public.orders", &from_text),
            r#"public.orders:["5"]"#
        );
    }

    #[test]
    fn composite_text_normalized() {
        let comps = components_from_json(&json!([5, "abc", true]));
        assert_eq!(comps, vec!["5", "abc", "true"]);
        assert_eq!(doc_id("t", &comps), r#"t:["5","abc","true"]"#);
    }

    #[test]
    fn pk_key_single_is_bare_composite_is_array() {
        assert_eq!(pk_key(&["5".into()]), Some("5".to_owned()));
        assert_eq!(
            pk_key(&["a".into(), "b".into()]),
            Some(r#"["a","b"]"#.to_owned())
        );
        assert_eq!(pk_key(&[]), None);
    }
}
