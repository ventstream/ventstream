//! Neo4j-side helper for delete reconciliation.
//!
//! The OS sink's reconciliation pass needs the set of element IDs
//! currently present in Neo4j for a given primary label. This module
//! opens a one-shot Bolt connection, runs `MATCH (n:Label) RETURN
//! elementId(n)`, and returns the IDs as a `HashSet<String>` keyed
//! the same way the sink writes doc IDs (raw element_id suffix).
//!
//! Kept in the sources crate (alongside the existing Bolt/Graph
//! plumbing) so we don't duplicate connection logic in the engine
//! crate. Engine code calls this as a standalone async function.

use std::collections::HashSet;

use neo4rs::{query, ConfigBuilder, Graph};
use tracing::debug;

use super::config::Neo4jCdcConfig;
use crate::error::Neo4jCdcError;

type Result<T> = std::result::Result<T, Neo4jCdcError>;

/// Build a one-shot `Graph` connection from the source config. Same
/// builder logic the `Neo4jCdcSource` uses for its replication
/// connection — duplicated rather than exposed because the source's
/// `connect` lives on a private impl and changing that ripples into
/// the source's invariants.
async fn connect(config: &Neo4jCdcConfig) -> Result<Graph> {
    let mut builder = ConfigBuilder::default()
        .uri(config.uri.as_str())
        .user(config.user.as_str())
        .password(config.password.as_str())
        .db(config.database.as_str());
    if let Some(p) = &config.trust_cert_file {
        builder = builder.with_client_certificate(p);
    }
    let cfg = builder
        .build()
        .map_err(|err| Neo4jCdcError::Connection(format!("neo4rs config: {err}")))?;
    Graph::connect(cfg)
        .await
        .map_err(|err| Neo4jCdcError::Connection(err.to_string()))
}

/// List every `elementId(n)` for nodes carrying `label`. Used by the
/// reconciliation pass to compute the source-of-truth ID set before
/// asking the OS sink to delete orphans.
///
/// The label is interpolated into Cypher via the `$label` literal
/// (Neo4j labels can't be parameterised), so callers MUST ensure the
/// label came from a trusted config — the engine's denormalize spec
/// YAML, not arbitrary user input. Labels are validated against the
/// existing Cypher identifier rules elsewhere; here we belt-and-brace
/// by rejecting any label containing chars outside the allowed range.
pub async fn list_node_element_ids(
    config: &Neo4jCdcConfig,
    label: &str,
) -> Result<HashSet<String>> {
    if !is_safe_label(label) {
        return Err(Neo4jCdcError::Internal(format!(
            "refusing to query node IDs for label {label:?}: contains chars outside the allowed range"
        )));
    }

    let graph = connect(config).await?;
    let cypher = format!("MATCH (n:`{label}`) RETURN elementId(n) AS eid");
    let mut stream = graph.execute(query(&cypher)).await.map_err(|err| {
        Neo4jCdcError::Query(format!("reconciliation query for label {label}: {err}"))
    })?;

    let mut ids = HashSet::with_capacity(1024);
    while let Some(row) = stream.next().await.map_err(|err| {
        Neo4jCdcError::Query(format!("reconciliation stream for label {label}: {err}"))
    })? {
        let eid: String = row
            .get("eid")
            .map_err(|err| Neo4jCdcError::Query(format!("missing eid column: {err}")))?;
        ids.insert(eid);
    }
    debug!(
        label,
        count = ids.len(),
        "listed neo4j node element ids for reconciliation"
    );
    Ok(ids)
}

/// Restrict labels to a safe subset for inline-Cypher interpolation.
/// Neo4j allows backticked identifiers with arbitrary text, but to
/// avoid backtick-injection from a misconfigured YAML we reject
/// anything containing a backtick or control character.
fn is_safe_label(label: &str) -> bool {
    !label.is_empty() && !label.contains('`') && !label.chars().any(|c| c.is_control())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_label() {
        assert!(!is_safe_label(""));
    }

    #[test]
    fn rejects_backtick() {
        assert!(!is_safe_label("Label`Injection"));
    }

    #[test]
    fn rejects_control_chars() {
        assert!(!is_safe_label("Label\n"));
        assert!(!is_safe_label("Label\0"));
    }

    #[test]
    fn allows_normal_label() {
        assert!(is_safe_label("Author"));
        assert!(is_safe_label("User"));
        assert!(is_safe_label("MyLabel_with-special"));
    }
}
