//! Cold-start snapshot for the Neo4j CDC source.
//!
//! Paginates every label and relationship type the configured database
//! exposes, emits each as a synthetic insert event in the same shape
//! the live CDC tail emits for `operation: "c"`, then a sentinel that
//! lets the join engine flush in-memory state to disk in one shot.
//!
//! ### Why the cursor is captured BEFORE the scan
//!
//! Same trade-off the PG source makes:
//! - Capture cursor `C` via `db.cdc.current()`
//! - Scan the graph (non-isolated — concurrent writes during this
//!   window may also appear in the scan)
//! - Tail resumes from `C`
//!
//! Rows mutated during the scan may be emitted twice: once by the
//! scan, once by the tail. Downstream the deterministic `elementId`-keyed
//! doc id makes the sink last-write-wins, so the net effect is correct
//! even though it costs a doc write.

use neo4rs::{query, BoltType, Graph};
use serde_json::Value;
use tracing::{info, warn};
use ventstream_core::SourceContext;

use super::bolt::bolt_to_json;
use super::config::Neo4jCdcConfig;
use super::event_mapper;
use crate::error::Neo4jCdcError;

/// Run the snapshot scan. Returns the cursor captured BEFORE the scan
/// began — that's what the tail must resume from.
pub async fn run_snapshot(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    ctx: &SourceContext,
) -> Result<String, Neo4jCdcError> {
    let batch_size = config
        .bootstrap
        .as_ref()
        .map_or(2_000_i64, |b| b.batch_size);

    // Phase 1 — cursor BEFORE the scan.
    let cursor = fetch_current_cursor(graph).await?;
    info!(
        cursor_prefix = %&cursor[..cursor.len().min(30)],
        "neo4j bootstrap: cursor captured pre-scan"
    );

    // Phase 2 — discover everything to scan.
    let labels = fetch_labels(graph).await?;
    let reltypes = fetch_reltypes(graph).await?;
    info!(
        labels = labels.len(),
        reltypes = reltypes.len(),
        labels_list = ?labels,
        reltypes_list = ?reltypes,
        "neo4j bootstrap: discovered targets"
    );

    let mut total: u64 = 0;

    // Phase 3 — nodes. ONE scan over every node in the graph. For each
    // node we compute the canonical label (priority-aware), apply the
    // label filter, and emit at most one event per node. This is the
    // critical change from per-label scans: a node with composite
    // labels (e.g. `Author:Person`) used to be emitted once per label
    // (so events landed in both `events-author` and `events-person`);
    // now it lands in exactly one place — the same place its live tail
    // events land — so bootstrap and tail are consistent.
    {
        let counts = scan_all_nodes(graph, config, batch_size, ctx).await?;
        for (table, n) in &counts {
            info!(table = %table, rows = n, "neo4j bootstrap: nodes scanned");
            total += n;
        }
    }

    // Phase 4 — relationships per reltype.
    for rt in &reltypes {
        if !config.reltype_allowed(rt) {
            continue;
        }
        let table = config.resolve_reltype_table(rt);
        let n = scan_relationships(graph, config, rt, &table, batch_size, ctx).await?;
        info!(reltype = %rt, table = %table, rows = n, "neo4j bootstrap: relationships scanned");
        total += n;
    }

    // Phase 5 — sentinel.
    let sentinel = event_mapper::snapshot_complete(config)?;
    if ctx.sender.send(sentinel, &ctx.shutdown).await.is_err() {
        warn!("neo4j bootstrap: shutdown raced sentinel publish — proceeding");
    }
    info!(total, "neo4j bootstrap complete");
    Ok(cursor)
}

/// Read `db.cdc.current()` into an opaque cursor string. Public so
/// the source's idle-advance loop can call it too.
pub async fn fetch_current_cursor(graph: &Graph) -> Result<String, Neo4jCdcError> {
    let mut rows = graph
        .execute(query("CALL db.cdc.current() YIELD id RETURN id"))
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.cdc.current: {err}")))?;
    let row = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.cdc.current fetch: {err}")))?
        .ok_or_else(|| Neo4jCdcError::Query("db.cdc.current returned no rows".to_owned()))?;
    row.get::<String>("id")
        .map_err(|err| Neo4jCdcError::Query(format!("db.cdc.current row.get: {err}")))
}

async fn fetch_labels(graph: &Graph) -> Result<Vec<String>, Neo4jCdcError> {
    let mut rows = graph
        .execute(query(
            "CALL db.labels() YIELD label RETURN label ORDER BY label",
        ))
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.labels: {err}")))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.labels iter: {err}")))?
    {
        let label = row
            .get::<String>("label")
            .map_err(|err| Neo4jCdcError::Query(format!("db.labels row.get: {err}")))?;
        out.push(label);
    }
    Ok(out)
}

async fn fetch_reltypes(graph: &Graph) -> Result<Vec<String>, Neo4jCdcError> {
    let mut rows = graph
        .execute(query(
            "CALL db.relationshipTypes() YIELD relationshipType \
             RETURN relationshipType ORDER BY relationshipType",
        ))
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.relationshipTypes: {err}")))?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("db.relationshipTypes iter: {err}")))?
    {
        let rt = row
            .get::<String>("relationshipType")
            .map_err(|err| Neo4jCdcError::Query(format!("db.relationshipTypes row.get: {err}")))?;
        out.push(rt);
    }
    Ok(out)
}

/// One scan over every node in the database. Emits each node at most
/// once, using the priority-aware canonical-label resolver to choose
/// the destination table. Skips nodes whose canonical label is
/// excluded by `label_filter`.
///
/// Returns a per-table count map so callers can log the breakdown.
async fn scan_all_nodes(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    _batch_size: i64,
    ctx: &SourceContext,
) -> Result<std::collections::BTreeMap<String, u64>, Neo4jCdcError> {
    let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    // One streamed scan: the Bolt cursor pulls rows lazily in
    // fetch-size batches, so memory stays bounded without explicit
    // paging. The previous keyset pagination over elementId(n)
    // re-planned a full AllNodesScan plus a top-K sort for every page —
    // elementId() has no index and no native ordering — so each page
    // cost O(total nodes): invisible on the 529k-node graph it was
    // measured on (~10 ms/page), a quadratic collapse at 50M nodes
    // (~650 docs/s observed). One streamed scan is O(n) total, no sort.
    let mut rows = graph
        .execute(query(
            "MATCH (n) RETURN \
                elementId(n) AS eid, labels(n) AS labels, properties(n) AS props",
        ))
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("scan_all_nodes: {err}")))?;

    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("scan_all_nodes iter: {err}")))?
    {
        let eid = row
            .get::<String>("eid")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_all_nodes eid: {err}")))?;
        let labels_bolt: BoltType = row
            .get("labels")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_all_nodes labels: {err}")))?;
        let props_bolt: BoltType = row
            .get("props")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_all_nodes props: {err}")))?;

        // BoltType::List<String> → Vec<String> for canonicalisation.
        let labels_vec = labels_to_vec(&labels_bolt);
        let Some(canonical) = config.canonical_label(&labels_vec) else {
            // No labels on the node — Neo4j 5 generally requires a
            // label per node, so this is anomalous. Skip + warn
            // rather than crash.
            warn!(element_id = %eid, "neo4j bootstrap: node has no labels, skipping");
            continue;
        };
        if !config.label_allowed(canonical) {
            continue;
        }
        let table = config.resolve_label_table(canonical);

        let payload = synthesize_node_payload(&eid, &labels_bolt, &props_bolt);
        let event = event_mapper::synth_node_insert(config, &table, payload)?;
        ctx.sender
            .send(event, &ctx.shutdown)
            .await
            .map_err(|err| Neo4jCdcError::Internal(format!("publish failed: {err}")))?;
        *counts.entry(table).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Extract a `Vec<String>` from a `BoltType::List` of strings — the
/// shape `labels(n)` returns. Non-string entries are skipped (defensive
/// against future Bolt shape changes).
fn labels_to_vec(b: &BoltType) -> Vec<String> {
    if let BoltType::List(list) = b {
        list.value
            .iter()
            .filter_map(|item| match item {
                BoltType::String(s) => Some(s.value.clone()),
                _ => None,
            })
            .collect()
    } else {
        Vec::new()
    }
}

async fn scan_relationships(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    reltype: &str,
    table: &str,
    _batch_size: i64,
    ctx: &SourceContext,
) -> Result<u64, Neo4jCdcError> {
    let mut emitted: u64 = 0;
    // One streamed scan per relationship type — same reasoning as
    // scan_all_nodes: elementId(r) keyset paging re-planned a full
    // expand + sort per page, quadratic in total. The Bolt cursor
    // streams in fetch-size batches, keeping memory bounded.
    let cypher = format!(
        "MATCH (a)-[r:`{reltype}`]->(b) RETURN \
           elementId(r) AS eid, type(r) AS rtype, properties(r) AS props, \
           elementId(a) AS start_eid, labels(a) AS start_labels, \
           elementId(b) AS end_eid,   labels(b) AS end_labels"
    );
    let mut rows = graph.execute(query(&cypher)).await.map_err(|err| {
        Neo4jCdcError::Query(format!("scan_relationships reltype={reltype}: {err}"))
    })?;

    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("scan_relationships iter: {err}")))?
    {
        let eid = row
            .get::<String>("eid")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel eid: {err}")))?;
        let props_bolt: BoltType = row
            .get("props")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel props: {err}")))?;
        let start_eid = row
            .get::<String>("start_eid")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel start_eid: {err}")))?;
        let start_labels_bolt: BoltType = row
            .get("start_labels")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel start_labels: {err}")))?;
        let end_eid = row
            .get::<String>("end_eid")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel end_eid: {err}")))?;
        let end_labels_bolt: BoltType = row
            .get("end_labels")
            .map_err(|err| Neo4jCdcError::Query(format!("scan_rel end_labels: {err}")))?;

        let payload = synthesize_rel_payload(
            &eid,
            reltype,
            &props_bolt,
            &start_eid,
            &start_labels_bolt,
            &end_eid,
            &end_labels_bolt,
        );
        let event = event_mapper::synth_rel_insert(config, table, payload, &start_eid, &end_eid)?;
        ctx.sender
            .send(event, &ctx.shutdown)
            .await
            .map_err(|err| Neo4jCdcError::Internal(format!("publish failed: {err}")))?;
        emitted += 1;
    }
    Ok(emitted)
}

/// Build a payload in the same shape the live CDC tail emits for a
/// node create — so bootstrap and tail events route through the join
/// engine the same way.
fn synthesize_node_payload(element_id: &str, labels: &BoltType, props: &BoltType) -> Value {
    serde_json::json!({
        "elementId": element_id,
        "eventType": "n",
        "operation": "c",
        "labels":    bolt_to_json(labels),
        "keys":      {},
        "state": {
            "before": Value::Null,
            "after":  {
                "labels":     bolt_to_json(labels),
                "properties": bolt_to_json(props),
            },
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn synthesize_rel_payload(
    element_id: &str,
    rtype: &str,
    props: &BoltType,
    start_eid: &str,
    start_labels: &BoltType,
    end_eid: &str,
    end_labels: &BoltType,
) -> Value {
    serde_json::json!({
        "elementId": element_id,
        "eventType": "r",
        "operation": "c",
        "type":      rtype,
        "keys":      [],
        "start": {
            "elementId": start_eid,
            "labels":    bolt_to_json(start_labels),
            "keys":      {},
        },
        "end": {
            "elementId": end_eid,
            "labels":    bolt_to_json(end_labels),
            "keys":      {},
        },
        "state": {
            "before": Value::Null,
            "after":  { "properties": bolt_to_json(props) },
        },
    })
}
