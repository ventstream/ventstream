//! Pipeline-level validation for SurrealDB graph edge specs.
//!
//! The invariant here spans both halves of a pipeline: an edge table name
//! must not collide with any relation the pipeline routes *documents* for.
//! The sink cannot answer that on its own — it never learns what the source
//! publishes — and the source cannot either, since it has no view of the
//! sink's edge specs. So it is checked where both are in scope, alongside
//! `validate_related_ids_unique` and `validate_projection_target_indexes`.
//!
//! The sink keeps its own, narrower check (a name against other specs'
//! endpoints). That is a different question answered with less information,
//! not a duplicate of this one.

use anyhow::{anyhow, Result};
use ventstream_sinks::surrealdb::config::SurrealEdgeSpec;

/// Reject any edge whose name collides with a routed document relation.
///
/// A collision is silent and destructive: edge rows land in the document
/// table under RELATE ids, overwriting documents that share an id and
/// leaving the rest as edges masquerading as documents. Nothing surfaces in
/// logs, metrics or the DLQ, and recovery needs a full re-bootstrap.
///
/// `relations` is the bare relation names the pipeline routes documents
/// for — matching `ventstream.cdc.relation`, which carries the table name
/// without its schema.
pub fn validate_edge_names_against_relations(
    specs: &[SurrealEdgeSpec],
    relations: &[String],
) -> Result<()> {
    for spec in specs {
        if relations.iter().any(|relation| relation == &spec.name) {
            return Err(anyhow!(
                "graph edge `{}` collides with published relation `{}`. Edge rows would be \
                 written into that relation's document table, overwriting documents whose ids \
                 match and leaving the rest as edges posing as documents — silently. Rename the \
                 edge",
                spec.name,
                spec.name
            ));
        }
    }
    Ok(())
}

/// Warn about edge specs whose `from_table` matches no published relation.
///
/// Such a spec produces no edges at all while the pipeline reports healthy,
/// so a typo is indistinguishable from a working config without querying
/// the target. This is a warning rather than an error because the relation
/// set is only known for some sources, and a spec may legitimately name a
/// table added to the publication later.
///
/// Note the naming contract: `from_table` and `to_table` are bare relation
/// names. `ventstream.cdc.relation` carries the table without its schema,
/// so `biz.pets` can never match — it must be `pets`.
pub fn unmatched_edge_sources(specs: &[SurrealEdgeSpec], relations: &[String]) -> Vec<String> {
    specs
        .iter()
        .filter(|spec| {
            !relations
                .iter()
                .any(|relation| relation == &spec.from_table)
        })
        .map(|spec| {
            let hint = if spec.from_table.contains('.') {
                format!(
                    " (`{}` is schema-qualified; use the bare relation name `{}`)",
                    spec.from_table,
                    spec.from_table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&spec.from_table)
                )
            } else {
                String::new()
            };
            format!(
                "graph edge `{}` has from_table `{}`, which matches no published relation{hint}",
                spec.name, spec.from_table
            )
        })
        .collect()
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

    fn spec(name: &str, from: &str, to: &str) -> SurrealEdgeSpec {
        SurrealEdgeSpec {
            name: name.to_owned(),
            from_table: from.to_owned(),
            to_table: to.to_owned(),
            fk_columns: vec!["author_id".to_owned()],
            reversed: false,
        }
    }

    fn relations() -> Vec<String> {
        ["people", "articles", "teams", "members"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect()
    }

    #[test]
    fn an_edge_named_after_a_published_relation_is_rejected() {
        // The reported case: `members` is a real table, and is not any
        // spec's endpoint, so the sink-level check cannot see it.
        let specs = vec![spec("members", "articles", "people")];
        let err = validate_edge_names_against_relations(&specs, &relations())
            .expect_err("collision must be rejected");
        assert!(
            err.to_string()
                .contains("collides with published relation `members`"),
            "error should name the relation, got: {err}"
        );
    }

    #[test]
    fn an_edge_name_that_matches_nothing_published_is_allowed() {
        let specs = vec![spec("wrote", "articles", "people")];
        validate_edge_names_against_relations(&specs, &relations()).expect("no collision");
    }

    #[test]
    fn an_unknown_from_table_is_reported() {
        let specs = vec![spec("wrote", "pets", "people")];
        let warnings = unmatched_edge_sources(&specs, &relations());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("matches no published relation"));
    }

    #[test]
    fn a_schema_qualified_from_table_says_so() {
        // Never matches, because the relation header carries no schema.
        let specs = vec![spec("wrote", "biz.pets", "people")];
        let warnings = unmatched_edge_sources(&specs, &relations());
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("schema-qualified") && warnings[0].contains("`pets`"),
            "warning should point at the bare name, got: {}",
            warnings[0]
        );
    }

    #[test]
    fn a_matching_from_table_is_quiet() {
        let specs = vec![spec("wrote", "articles", "people")];
        assert!(unmatched_edge_sources(&specs, &relations()).is_empty());
    }
}
