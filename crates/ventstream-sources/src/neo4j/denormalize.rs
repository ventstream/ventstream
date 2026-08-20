//! Real-time denormalisation for Neo4j primaries — generic version.
//!
//! Given a YAML spec listing primary labels and user-supplied Cypher
//! bodies, produces one continuously-updated denormalised document per
//! primary node and writes them to a single OpenSearch index per
//! primary. Same output shape any periodic Cypher dump would produce,
//! delivered continuously from CDC events instead of on a schedule.
//!
//! ### Spec shape
//!
//! ```yaml
//! denormalize:
//!   - primary_label: Author
//!     output_table: parties_denormalized
//!     fan_out_max_hops: 2
//!     cypher: |
//!       WITH p, datetime() AS now
//!       OPTIONAL MATCH (p)-[hn:HAS_NAME]->(name:Name {type: "Display"})
//!         WHERE hn.fromDate <= now AND coalesce(hn.thruDate, datetime("9999-12-31")) > now
//!       ...
//!       RETURN elementId(p) AS primaryEid, { id: p.id, displayName: name.name } AS doc
//!
//!   - primary_label: Document
//!     output_table: docs_denormalized
//!     fan_out_max_hops: 3
//!     cypher: |
//!       ...
//! ```
//!
//! Each primary's `cypher` body MUST end in
//! `RETURN elementId(p) AS primaryEid, { ... } AS doc`.
//!
//! ### How it works
//!
//! - **Bootstrap.** For each spec, keyset-paginate the primaries by
//!   one streamed `MATCH (p:Label) RETURN elementId(p)` key scan,
//!   chunked client-side into body queries — so each body page is
//!   its own short read transaction rather than one long-streaming query.
//!   Emit one event per row, keyed by `ventstream.doc.id =
//!   "{output_table}:{primaryEid}"` so OS upserts in place.
//! - **Tail.** For each CDC event, for each spec, run:
//!
//!   ```cypher
//!   MATCH (p:`{Label}`) WHERE
//!     elementId(p) = $eid OR
//!     EXISTS {
//!       MATCH path = (p)-[*1..N]-(x)
//!       WHERE any(r IN relationships(path) WHERE elementId(r) = $eid)
//!          OR any(n IN nodes(path)         WHERE elementId(n) = $eid)
//!     }
//!   {cypher}
//!   ```
//!
//!   This finds every primary within `N` hops of the changed element
//!   AND recomposes them in the same call. N is configurable per spec
//!   (`fan_out_max_hops`); 2 catches the generic Genre / agent
//!   traversal.
//!
//! ### Genericity guarantees
//!
//! - **No baked-in labels or reltypes.** The Cypher comes from the
//!   user's YAML. Any graph shape works as long as the spec's Cypher
//!   returns `(primaryEid, doc)`.
//! - **Multi-primary.** N specs run independently in the same source;
//!   each writes to its own output_table.
//! - **N-hop fan-out per spec.** Tight fan-out for small graphs;
//!   wider fan-out for deep graphs. Cost scales with hops on big
//!   graphs — tune per spec.

use chrono::Utc;
use futures_util::stream::{FuturesUnordered, StreamExt};
use neo4rs::{query, BoltType, Graph};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::{debug, info};
use ventstream_core::{ContentType, Event, Headers, Payload, SourceContext, SourceUri, Subject};

use super::bolt::bolt_to_json;
use super::bootstrap::fetch_current_cursor;
use super::config::Neo4jCdcConfig;
use super::projection::{build_projection_call_block, extract_projection_paths, ProjectionExtract};
use crate::error::Neo4jCdcError;

/// Env-var opt-out for projection-aware fan-out. Default = enabled. Set
/// `VS_NEO4J_PROJECTION_FAN_OUT=0` to force the original variable-length
/// path scan — useful as a fallback if the projection extractor mis-handles
/// some Cypher shape we haven't anticipated.
const PROJECTION_FAN_OUT_ENV: &str = "VS_NEO4J_PROJECTION_FAN_OUT";

fn projection_fan_out_enabled() -> bool {
    match std::env::var(PROJECTION_FAN_OUT_ENV) {
        Ok(v) => {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
}

/// One denormalisation projection — produced from one entry under
/// `denormalize:` in the YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct DenormalizeSpec {
    /// Neo4j label of the primary node. The Cypher template is wrapped
    /// with `MATCH (p:`{primary_label}`) ...`.
    pub primary_label: String,

    /// OpenSearch index / logical table where the denormalised docs
    /// land. Stamped into `ventstream.cdc.relation` so the standard
    /// index template (`events-${header:...relation}`) routes correctly.
    /// Each spec produces one OS doc per primary, upserted in place.
    pub output_table: String,

    /// Cypher body run after `MATCH (p:Label)`. Must end in
    /// `RETURN elementId(p) AS primaryEid, { ... } AS doc`. The full
    /// Cypher language is available — OPTIONAL MATCH, WITH, CALL { },
    /// collect(), aggregations, multi-hop traversals, etc. We do not
    /// constrain the projection.
    pub cypher: String,

    /// Maximum graph distance from a CDC event's element at which
    /// primaries are recomposed. 0 = only when the primary itself
    /// mutates. 1 = primary OR any direct neighbour / relationship.
    /// 2 = up to two-hop reach. Tune per spec — wider hops catch more
    /// indirect changes but cost more per event.
    #[serde(default = "default_max_hops")]
    pub fan_out_max_hops: usize,
}

fn default_max_hops() -> usize {
    2
}

/// One row of the analyze report — produced by [`analyze_specs`] and
/// rendered by the binary's `analyze` subcommand.
#[derive(Debug, Clone)]
pub struct AnalyzeRow {
    /// Primary label this spec targets.
    pub primary_label: String,
    /// What the spec sets `fan_out_max_hops` to.
    pub configured_hops: usize,
    /// What `estimate_max_hops_in_cypher` infers from the Cypher body.
    pub inferred_min_hops: usize,
    /// True iff `configured_hops < inferred_min_hops` — the spec will
    /// silently miss recompositions for changes at the deepest hops.
    pub warn_too_low: bool,
}

/// Walk every spec, infer the minimum hops needed from its Cypher,
/// compare against `fan_out_max_hops`. Pure transform over the parsed
/// YAML — no graph access required.
pub fn analyze_specs(specs: &DenormalizeSpecs) -> Vec<AnalyzeRow> {
    specs
        .denormalize
        .iter()
        .map(|s| {
            let inferred = estimate_max_hops_in_cypher(&s.cypher);
            AnalyzeRow {
                primary_label: s.primary_label.clone(),
                configured_hops: s.fan_out_max_hops,
                inferred_min_hops: inferred,
                warn_too_low: s.fan_out_max_hops < inferred,
            }
        })
        .collect()
}

/// Wrapper around the list of specs loaded from YAML. Source code
/// passes this around to keep the trait surface clean.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DenormalizeSpecs {
    /// One entry per primary projection. Order doesn't matter — each
    /// spec runs independently against the same graph + same CDC event.
    pub denormalize: Vec<DenormalizeSpec>,
}

impl DenormalizeSpecs {
    /// Convenience: zero specs == denormalize mode is off.
    pub fn is_empty(&self) -> bool {
        self.denormalize.is_empty()
    }

    /// Convenience: number of configured primary projections.
    pub fn len(&self) -> usize {
        self.denormalize.len()
    }

    /// Load and validate a YAML file. Returns an error if the file is
    /// missing, malformed, or any spec's Cypher body is empty.
    pub fn from_yaml_file(path: &Path) -> Result<Self, Neo4jCdcError> {
        let text = std::fs::read_to_string(path).map_err(|err| {
            Neo4jCdcError::Internal(format!(
                "reading denormalize YAML at {}: {err}",
                path.display()
            ))
        })?;
        let specs: Self = serde_yaml::from_str(&text).map_err(|err| {
            Neo4jCdcError::Internal(format!(
                "parsing denormalize YAML at {}: {err}",
                path.display()
            ))
        })?;
        for s in &specs.denormalize {
            if s.primary_label.trim().is_empty() {
                return Err(Neo4jCdcError::Internal(
                    "denormalize spec has empty primary_label".to_owned(),
                ));
            }
            if s.output_table.trim().is_empty() {
                return Err(Neo4jCdcError::Internal(format!(
                    "denormalize spec for '{}' has empty output_table",
                    s.primary_label
                )));
            }
            if s.cypher.trim().is_empty() {
                return Err(Neo4jCdcError::Internal(format!(
                    "denormalize spec for '{}' has empty cypher",
                    s.primary_label
                )));
            }
        }
        Ok(specs)
    }
}

/// Bootstrap every spec. Returns the CDC cursor captured BEFORE the
/// first scan — that's what the tail must resume from to avoid losing
/// changes that happened during bootstrap.
pub async fn run_bootstrap_all(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    specs: &DenormalizeSpecs,
    ctx: &SourceContext,
) -> Result<String, Neo4jCdcError> {
    // Capture cursor BEFORE any scan. Concurrent writes during scan
    // are picked up in the tail; deterministic doc IDs keep the
    // end-state correct.
    let cursor = fetch_current_cursor(graph).await?;
    info!(
        cursor_prefix = %&cursor[..cursor.len().min(30)],
        specs = specs.len(),
        "neo4j denormalize bootstrap: cursor captured pre-scan"
    );

    for spec in &specs.denormalize {
        let started = std::time::Instant::now();
        let count = bootstrap_one(graph, config, spec, ctx).await?;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        info!(
            primary = %spec.primary_label,
            output = %spec.output_table,
            rows = count,
            elapsed_ms,
            "neo4j denormalize bootstrap: spec complete"
        );
    }

    Ok(cursor)
}

/// Build the stage-1 key enumeration: one streamed scan of every
/// primary element-id under the label. The previous per-page keyset
/// (`ORDER BY elementId(p) LIMIT $batch`) re-ran a full label scan plus
/// a top-K sort for every page — `elementId()` has no index and no
/// native ordering — so each page cost O(label cardinality): flat on
/// the graphs it was measured on (~60k primaries), a quadratic
/// collapse at tens of millions (observed ~640 docs/s at 50M nodes).
/// One streamed scan is O(n) total; the Bolt cursor fetches lazily so
/// memory stays bounded by the chunking below. The stream's read
/// transaction stays open for the scan's duration; a mid-stream
/// failure restarts the bootstrap, which deterministic doc ids make
/// idempotent.
fn build_keys_cypher(label: &str) -> String {
    format!("MATCH (p:`{label}`)\nRETURN elementId(p) AS eid")
}

/// Build the stage-2 body query: run the user's projection `body` over EXACTLY
/// the primaries selected by stage 1 (`elementId(p) IN $eids`). The body
/// composes after a `WITH p`, identical to the previous single-query form —
/// only the primary selection changed from a `LIMIT` to an `IN $eids`.
fn build_bootstrap_body_cypher(label: &str, body: &str) -> String {
    format!(
        "MATCH (p:`{label}`)\nWHERE elementId(p) IN $eids\n\
         WITH p ORDER BY elementId(p)\n{body}",
    )
}

async fn bootstrap_one(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    ctx: &SourceContext,
) -> Result<u64, Neo4jCdcError> {
    let batch_size = config
        .bootstrap
        .as_ref()
        .map_or(2_000_i64, |b| b.batch_size);
    let label = &spec.primary_label;
    let mut emitted: u64 = 0;
    let mut page: u64 = 0;

    // PAGINATION IS DRIVEN BY THE PRIMARY KEY STREAM, NOT THE BODY OUTPUT.
    //
    // A previous design ran one query — `MATCH (p) … WITH p LIMIT $batch
    // {body}` — and used the COUNT OF BODY ROWS for both termination and
    // the cursor advance. But `LIMIT $batch` bounds primaries while the
    // body decides how many rows to return: a body using a non-`OPTIONAL
    // MATCH` (or any filter dropping primaries) emits fewer rows than the
    // primaries scanned, so `rows < batch` ended the scan early and every
    // remaining primary was silently never visited. Stage 1 enumerates
    // primaries by itself — its stream is authoritative regardless of the
    // body's row count.
    let mut krows = graph
        .execute(query(&build_keys_cypher(label)))
        .await
        .map_err(|err| {
            Neo4jCdcError::Query(format!("bootstrap_one keys primary={label}: {err}"))
        })?;

    let mut exhausted = false;
    while !exhausted {
        // Collect the next chunk of primary ids off the key stream.
        let mut page_eids: Vec<String> = Vec::new();
        while (page_eids.len() as i64) < batch_size {
            let Some(row) = krows
                .next()
                .await
                .map_err(|err| Neo4jCdcError::Query(format!("bootstrap_one keys iter: {err}")))?
            else {
                exhausted = true;
                break;
            };
            let eid = row
                .get::<String>("eid")
                .map_err(|err| Neo4jCdcError::Query(format!("bootstrap_one keys eid: {err}")))?;
            page_eids.push(eid);
        }
        if page_eids.is_empty() {
            break;
        }
        let page_primary_count = page_eids.len() as i64;

        // Stage 2 runs the user body over EXACTLY this chunk's primaries. The
        // body still composes after a `WITH p`, identical to before — only the
        // primary selection is `elementId(p) IN $eids`, a NodeByElementIdSeek.
        let body_cypher = build_bootstrap_body_cypher(label, &spec.cypher);
        let mut rows = graph
            .execute(query(&body_cypher).param("eids", page_eids.clone()))
            .await
            .map_err(|err| {
                Neo4jCdcError::Query(format!(
                    "bootstrap_one body primary={label} page={page}: {err}"
                ))
            })?;

        let mut page_doc_count: i64 = 0;
        while let Some(row) = rows
            .next()
            .await
            .map_err(|err| Neo4jCdcError::Query(format!("bootstrap_one body iter: {err}")))?
        {
            let primary_eid = row
                .get::<String>("primaryEid")
                .map_err(|err| Neo4jCdcError::Query(format!(
                    "bootstrap_one primaryEid (is your RETURN clause `RETURN elementId(p) AS primaryEid, ... AS doc`?): {err}"
                )))?;
            let doc_bolt: BoltType = row
                .get("doc")
                .map_err(|err| Neo4jCdcError::Query(format!("bootstrap_one doc: {err}")))?;
            let doc_json = bolt_to_json(&doc_bolt);
            let event = build_event(config, spec, &primary_eid, doc_json, true, None)?;
            ctx.sender
                .send(event, &ctx.shutdown)
                .await
                .map_err(|err| Neo4jCdcError::Internal(format!("publish failed: {err}")))?;
            ventstream_telemetry::bump_events_emitted(1);
            emitted += 1;
            page_doc_count += 1;
        }
        page += 1;
        debug!(
            primary = %label, page,
            primaries = page_primary_count, docs = page_doc_count, total = emitted,
            "neo4j denormalize bootstrap: page complete"
        );
    }
    Ok(emitted)
}

/// Handle one CDC event by running every spec's fan-out cypher.
/// Returns the total number of recompositions / deletes emitted.
///
/// Three cases per spec:
///
/// 1. **Node DELETE on a node carrying this spec's primary_label.** Emit
///    an OS delete keyed by `ventstream.doc.id` so the denormalised doc
///    is removed instead of left stale.
/// 2. **Relationship event (any op).** Use `[event.elementId,
///    start.elementId, end.elementId]` as the fan-out anchor set. For
///    DELETEs in particular the relationship is gone from the graph, so
///    a path scan that includes `elementId(r) = $eid` matches nothing;
///    the endpoint node ids are how we reach the affected primaries.
/// 3. **Anything else (node create/update, non-primary node delete).**
///    Standard fan-out on `[event.elementId]`.
#[allow(clippy::too_many_arguments)] // cohesive tail-event context; see runner refactor
pub async fn handle_tail_event_all(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    specs: &DenormalizeSpecs,
    hot: &[super::hot_endpoints::SpecHotEndpoints],
    fan_out_cyphers: &[String],
    event_json: &Value,
    tx_id: i64,
    ctx: &SourceContext,
) -> Result<usize, Neo4jCdcError> {
    // Thin wrapper: process a single event as a one-element batch.
    handle_tail_events_all(
        graph,
        config,
        specs,
        hot,
        fan_out_cyphers,
        &[(event_json, tx_id)],
        ctx,
    )
    .await
}

/// Batched tail handler: process a whole poll's worth of CDC events with
/// ONE recompose query per spec instead of one query per event.
///
/// CDC events are post-commit, so every recompose queries the CURRENT
/// graph — N events touching the same primary otherwise run N identical
/// queries. Here we union the affected anchor element-ids across the
/// batch (per spec, applying the same hot-endpoint filtering as the
/// single-event path), dedup, and run a single `elementId(...) IN $eids`
/// recompose. Primary node-deletes are collected separately and emitted
/// as OpenSearch tombstones.
///
/// Correctness is unchanged — the result reflects the batch's final graph
/// state either way — but throughput at depth improves dramatically,
/// since the per-event multi-hop Cypher overhead is what makes deep
/// projections slow on the live path.
// `recompose[idx]` / `deletes[idx]` are bounds-safe: idx comes from
// `specs.denormalize.iter().enumerate()` and both vecs are sized to
// `specs.denormalize.len()`.
#[allow(clippy::too_many_arguments, clippy::indexing_slicing)]
pub async fn handle_tail_events_all(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    specs: &DenormalizeSpecs,
    hot: &[super::hot_endpoints::SpecHotEndpoints],
    fan_out_cyphers: &[String],
    events: &[(&Value, i64)],
    ctx: &SourceContext,
) -> Result<usize, Neo4jCdcError> {
    if events.is_empty() {
        return Ok(0);
    }
    let batch_started = std::time::Instant::now();
    // Stamp recomposed docs + tombstones with the batch's high-water
    // tx_id (events are ordered); the cursor advances past the whole
    // batch once the sink confirms it.
    let batch_tx_id = events.last().map_or(0, |(_, t)| *t);

    let n_specs = specs.denormalize.len();
    // Per-spec accumulators (BTreeSet => dedup + deterministic order).
    let mut recompose: Vec<std::collections::BTreeSet<String>> =
        vec![std::collections::BTreeSet::new(); n_specs];
    let mut deletes: Vec<std::collections::BTreeSet<String>> =
        vec![std::collections::BTreeSet::new(); n_specs];

    for (event_json, _tx) in events {
        let meta = EventMeta::extract(event_json);
        let Some(element_id) = meta.element_id.as_deref().filter(|s| !s.is_empty()) else {
            continue;
        };
        for (idx, spec) in specs.denormalize.iter().enumerate() {
            // Primary node delete → tombstone; skip the fan-out for this
            // spec (the DETACH DELETE's relationship-delete events recompose
            // any collateral primaries via their own endpoint anchors).
            if meta.is_node_delete() && meta.has_label(&spec.primary_label) {
                deletes[idx].insert(element_id.to_owned());
                continue;
            }
            // Same anchor set + hot-endpoint filtering as the single-event
            // path: the event's own id always anchors; rel endpoints are
            // kept unless they're the low-cardinality far side for this
            // rel type (see `hot_endpoints`).
            let (keep_start, keep_end) = hot.get(idx).map_or((true, true), |h| {
                h.keep_endpoints(
                    meta.rel_type.as_deref(),
                    meta.start_eid.as_deref(),
                    meta.end_eid.as_deref(),
                )
            });
            recompose[idx].insert(element_id.to_owned());
            if keep_start {
                if let Some(s) = meta.start_eid.as_deref() {
                    recompose[idx].insert(s.to_owned());
                }
            }
            if keep_end {
                if let Some(e) = meta.end_eid.as_deref() {
                    recompose[idx].insert(e.to_owned());
                }
            }
        }
    }

    let mut total: usize = 0;
    for (idx, spec) in specs.denormalize.iter().enumerate() {
        // Primary deletes → OpenSearch tombstones.
        for element_id in &deletes[idx] {
            let event = build_delete_event(config, spec, element_id, batch_tx_id)?;
            ctx.sender
                .send(event, &ctx.shutdown)
                .await
                .map_err(|err| Neo4jCdcError::Internal(format!("publish failed: {err}")))?;
            ventstream_telemetry::bump_events_emitted(1);
            total += 1;
        }
        if !deletes[idx].is_empty() {
            info!(
                primary = %spec.primary_label,
                output = %spec.output_table,
                deleted = deletes[idx].len(),
                metric = "denormalize.tail.deleted_batch",
                "neo4j denormalize: primaries deleted, emitted OS deletes"
            );
        }
        // One recompose query for every affected primary in the batch.
        let eids: Vec<String> = recompose[idx].iter().cloned().collect();
        if eids.is_empty() {
            continue;
        }
        let fallback;
        let selector: &str = match fan_out_cyphers.get(idx) {
            Some(c) => c,
            None => {
                fallback = build_fan_out_selector_cypher(spec);
                &fallback
            }
        };
        // Chunk the anchor list. A single huge `elementId(...) IN $eids`
        // degrades the fan-out query plan (at a large list Neo4j can flip
        // the projection branches off their indexed element-id seek onto a
        // scan), so we bound the list size. Chunks still amortize the
        // per-query round-trip + planning across many primaries — the win
        // over one-query-per-event — without the blow-up of one giant list.
        // Stream selector rows directly into bounded projection queries. The
        // old two-stage implementation bounded each projection transaction but
        // retained every affected ID before stage 2; million-primary cascades
        // therefore still grew engine RSS with fan-out size.
        let (n, selected) =
            stream_affected_primaries(graph, config, spec, selector, &eids, batch_tx_id, ctx)
                .await?;
        if n > 0 {
            info!(
                primary = %spec.primary_label,
                output = %spec.output_table,
                recomposed = n,
                anchor_eids = eids.len(),
                affected_primaries = selected,
                metric = "denormalize.tail.recomposed_batch",
                "neo4j denormalize: spec recomposed primaries (streamed)"
            );
        }
        total += n;
    }
    debug!(
        events = events.len(),
        recomposed = total,
        elapsed_ms = batch_started.elapsed().as_millis() as u64,
        metric = "denormalize.tail.batch_total",
        "neo4j denormalize: batch end-to-end"
    );
    Ok(total)
}

/// Pulled-out projection of the bits of a CDC event the fan-out logic
/// cares about. Defensive: every field is optional so an unexpected
/// payload shape doesn't crash the source.
#[derive(Debug, Default)]
struct EventMeta {
    /// `event.elementId` — the node or relationship the event is about.
    element_id: Option<String>,
    /// `event.eventType == "n"`.
    is_node: bool,
    /// `event.operation == "d"`.
    is_delete: bool,
    /// Combined labels from top-level `event.labels` and
    /// `event.state.before.labels` — covers both update and delete
    /// payload shapes.
    labels: Vec<String>,
    /// For relationship events, `event.start.elementId`.
    start_eid: Option<String>,
    /// For relationship events, `event.end.elementId`.
    end_eid: Option<String>,
    /// For relationship events, the relationship `type` (e.g.
    /// `SUPPLIED_BY`). Drives direction-aware hot-endpoint filtering.
    rel_type: Option<String>,
}

impl EventMeta {
    #[allow(clippy::field_reassign_with_default)] // incremental field population reads clearer here
    fn extract(event_json: &Value) -> Self {
        let mut m = Self::default();
        m.element_id = event_json
            .get("elementId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let event_type = event_json
            .get("eventType")
            .and_then(Value::as_str)
            .unwrap_or("");
        let op = event_json
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or("");
        m.is_node = event_type == "n";
        m.is_delete = op == "d";

        // Labels: top-level when present (update / create), plus
        // state.before.labels (delete payloads put it there).
        if let Some(arr) = event_json.get("labels").and_then(Value::as_array) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    m.labels.push(s.to_owned());
                }
            }
        }
        if let Some(arr) = event_json
            .pointer("/state/before/labels")
            .and_then(Value::as_array)
        {
            for v in arr {
                if let Some(s) = v.as_str() {
                    if !m.labels.iter().any(|l| l == s) {
                        m.labels.push(s.to_owned());
                    }
                }
            }
        }

        m.start_eid = event_json
            .pointer("/start/elementId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        m.end_eid = event_json
            .pointer("/end/elementId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        // Relationship type — only present on rel events; drives
        // direction-aware hot-endpoint filtering.
        m.rel_type = event_json
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        m
    }

    fn is_node_delete(&self) -> bool {
        self.is_node && self.is_delete
    }

    fn has_label(&self, want: &str) -> bool {
        self.labels.iter().any(|l| l == want)
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_affected_primaries(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    selector: &str,
    anchor_eids: &[String],
    tx_id: i64,
    ctx: &SourceContext,
) -> Result<(usize, usize), Neo4jCdcError> {
    let chunk_size = config.recompose_chunk.max(1);
    let concurrency = config.recompose_concurrency.max(1);
    let dedup_capacity = chunk_size.saturating_mul(concurrency).saturating_mul(8);
    let recompose_cypher = build_direct_recompose_cypher(spec);
    let mut generation = FanOutDedupGeneration::new(dedup_capacity);
    let mut projection_chunk = Vec::with_capacity(chunk_size);
    let mut in_flight = FuturesUnordered::new();
    let mut selected = 0usize;
    let mut emitted = 0usize;

    // Selector anchor chunks remain sequential so the total projection query
    // concurrency is exactly `recompose_concurrency`, rather than multiplying
    // selector and projection concurrency. The bounded recent-ID set removes
    // ordinary overlap between adjacent anchor chunks. Before the set rolls
    // over, every projection from the current generation is drained. A later
    // duplicate can therefore recompose with the same external version, but
    // can never overlap an older projection and stale-overwrite its result.
    for anchors in anchor_eids.chunks(chunk_size) {
        let cypher_started = std::time::Instant::now();
        let mut rows = graph
            .execute(query(selector).param("eids", anchors.to_vec()))
            .await
            .map_err(|err| {
                Neo4jCdcError::Query(format!(
                    "fan-out selector primary={}: {err}",
                    spec.primary_label
                ))
            })?;
        while let Some(row) = rows
            .next()
            .await
            .map_err(|err| Neo4jCdcError::Query(format!("fan-out selector iter: {err}")))?
        {
            let primary_eid = row.get::<String>("primaryEid").map_err(|err| {
                Neo4jCdcError::Query(format!("fan-out selector primaryEid: {err}"))
            })?;
            match generation.classify(&primary_eid) {
                FanOutDedupAction::Duplicate => continue,
                FanOutDedupAction::Admit => {}
                FanOutDedupAction::Rollover => {
                    if !projection_chunk.is_empty() {
                        let ids = std::mem::replace(
                            &mut projection_chunk,
                            Vec::with_capacity(chunk_size),
                        );
                        in_flight.push(recompose_owned_primary_chunk(
                            graph,
                            config,
                            spec,
                            recompose_cypher.as_str(),
                            ids,
                            tx_id,
                            ctx,
                        ));
                    }
                    while let Some(result) = in_flight.next().await {
                        emitted += result?;
                    }
                    generation.begin_next(primary_eid.clone());
                }
            }
            selected += 1;
            projection_chunk.push(primary_eid);
            if projection_chunk.len() >= chunk_size {
                let ids = std::mem::replace(&mut projection_chunk, Vec::with_capacity(chunk_size));
                in_flight.push(recompose_owned_primary_chunk(
                    graph,
                    config,
                    spec,
                    recompose_cypher.as_str(),
                    ids,
                    tx_id,
                    ctx,
                ));
                if in_flight.len() >= concurrency {
                    emitted += in_flight.next().await.transpose()?.unwrap_or(0);
                }
            }
        }
        debug!(
            primary = %spec.primary_label,
            eid_count = anchors.len(),
            cypher_elapsed_ms = cypher_started.elapsed().as_millis() as u64,
            metric = "neo4j.denormalize.fan_out_selector",
            "fan-out selector chunk streamed"
        );
    }
    if !projection_chunk.is_empty() {
        in_flight.push(recompose_owned_primary_chunk(
            graph,
            config,
            spec,
            recompose_cypher.as_str(),
            projection_chunk,
            tx_id,
            ctx,
        ));
    }
    while let Some(result) = in_flight.next().await {
        emitted += result?;
    }
    Ok((emitted, selected))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FanOutDedupAction {
    Duplicate,
    Admit,
    Rollover,
}

/// Bounds selector deduplication without allowing equal-version projection
/// queries from different generations to overlap. The caller must drain the
/// current generation before calling [`Self::begin_next`] after `Rollover`.
#[derive(Debug)]
struct FanOutDedupGeneration {
    capacity: usize,
    seen: HashSet<String>,
}

impl FanOutDedupGeneration {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            seen: HashSet::with_capacity(capacity),
        }
    }

    fn classify(&mut self, primary_eid: &str) -> FanOutDedupAction {
        if self.seen.contains(primary_eid) {
            return FanOutDedupAction::Duplicate;
        }
        if self.seen.len() >= self.capacity {
            return FanOutDedupAction::Rollover;
        }
        self.seen.insert(primary_eid.to_owned());
        FanOutDedupAction::Admit
    }

    fn begin_next(&mut self, primary_eid: String) {
        self.seen.clear();
        self.seen.insert(primary_eid);
    }
}

#[allow(clippy::too_many_arguments)]
async fn recompose_owned_primary_chunk(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    cypher: &str,
    primary_eids: Vec<String>,
    tx_id: i64,
    ctx: &SourceContext,
) -> Result<usize, Neo4jCdcError> {
    recompose_primary_chunk(graph, config, spec, cypher, &primary_eids, tx_id, ctx).await
}

#[allow(clippy::too_many_arguments)] // cohesive bounded projection context
async fn recompose_primary_chunk(
    graph: &Graph,
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    cypher: &str,
    primary_eids: &[String],
    tx_id: i64,
    ctx: &SourceContext,
) -> Result<usize, Neo4jCdcError> {
    let mut rows = graph
        .execute(query(cypher).param("primaryEids", primary_eids.to_vec()))
        .await
        .map_err(|err| {
            Neo4jCdcError::Query(format!(
                "direct recompose primary={}: {err}",
                spec.primary_label
            ))
        })?;
    let mut emitted = 0usize;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|err| Neo4jCdcError::Query(format!("direct recompose iter: {err}")))?
    {
        let primary_eid = row
            .get::<String>("primaryEid")
            .map_err(|err| Neo4jCdcError::Query(format!("direct recompose primaryEid: {err}")))?;
        let doc_bolt: BoltType = row
            .get("doc")
            .map_err(|err| Neo4jCdcError::Query(format!("direct recompose doc: {err}")))?;
        let doc_json = bolt_to_json(&doc_bolt);
        let event = build_event(config, spec, &primary_eid, doc_json, false, Some(tx_id))?;
        ctx.sender
            .send(event, &ctx.shutdown)
            .await
            .map_err(|err| Neo4jCdcError::Internal(format!("publish failed: {err}")))?;
        ventstream_telemetry::bump_events_emitted(1);
        emitted += 1;
    }
    Ok(emitted)
}

/// Validate every spec's Cypher BEFORE the tail starts:
/// static read-only check, static RETURN-contract check, and live
/// `EXPLAIN` against Neo4j to catch syntax / schema errors.
///
/// Designed to fail fast at startup with an actionable message —
/// the alternative is the source dying on the first CDC event, which
/// is the worst time to discover a typo.
pub async fn validate_specs(
    graph: &Graph,
    specs: &DenormalizeSpecs,
    projection_fan_out: bool,
    hot_node_threshold: usize,
) -> Result<Vec<super::hot_endpoints::SpecHotEndpoints>, Neo4jCdcError> {
    for spec in &specs.denormalize {
        check_no_write_clauses(&spec.cypher).map_err(|e| {
            Neo4jCdcError::Internal(format!(
                "spec primary='{}' has invalid cypher — {}",
                spec.primary_label, e
            ))
        })?;
        check_return_contract(&spec.cypher).map_err(|e| {
            Neo4jCdcError::Internal(format!(
                "spec primary='{}' has invalid cypher — {}",
                spec.primary_label, e
            ))
        })?;
        explain_spec(graph, spec).await?;
        let (_, mode) = build_fan_out_cypher_with_mode_config(spec, projection_fan_out);
        match &mode {
            FanOutMode::Projection { hop_clauses } => {
                info!(
                    primary = %spec.primary_label,
                    output = %spec.output_table,
                    fan_out_mode = "projection",
                    hop_clauses = *hop_clauses,
                    "neo4j denormalize spec validated"
                );
            }
            FanOutMode::PathScan { reason } => {
                // INFO not WARN — operator may have opted out intentionally
                // via env var. But the reason is the interesting bit so
                // it deserves its own field for log queries.
                info!(
                    primary = %spec.primary_label,
                    output = %spec.output_table,
                    fan_out_mode = "path_scan",
                    fallback_reason = %reason,
                    "neo4j denormalize spec validated (path-scan fallback active)"
                );
            }
        }
    }

    // After per-spec validation, probe the graph once for each
    // spec's anchor paths to find low-cardinality endpoints. The
    // resulting per-spec sets feed the tail-loop filter that
    // prevents hot-node fan-out explosions.
    super::hot_endpoints::compute_for_specs(graph, &specs.denormalize, hot_node_threshold).await
}

/// Reject Cypher containing top-level write clauses. VentStream's
/// denormalize source is strictly a read-only consumer of Neo4j —
/// writing back would corrupt the graph.
///
/// Tokenises after stripping string literals so a quoted "CREATE"
/// inside a comparison doesn't false-positive.
pub(crate) fn check_no_write_clauses(cypher: &str) -> Result<(), String> {
    let no_strings = strip_string_literals(cypher);
    let lower = no_strings.to_ascii_lowercase();
    // Replace non-identifier chars with spaces so tokens with
    // surrounding punctuation still match cleanly. Keep `_` as
    // identifier so `create_date` stays one token (no false match).
    let normalised: String = lower
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect();
    const FORBIDDEN: &[&str] = &[
        "create", "merge", "set", "delete", "detach", "remove", "foreach",
    ];
    for token in normalised.split_whitespace() {
        if FORBIDDEN.iter().any(|f| f == &token) {
            return Err(format!(
                "contains write keyword '{token}' (denormalize cypher must be read-only). \
                 If you have a quoted string or property name that contains this word, that \
                 specific case isn't caught by the literal-stripper — please rename or use a \
                 different quoting style."
            ));
        }
    }
    Ok(())
}

/// Verify the user's RETURN clause aliases as `primaryEid` and `doc`.
/// This is the contract every spec must honour for the rest of the
/// pipeline to work (deterministic doc id is derived from primaryEid;
/// payload comes from doc).
pub(crate) fn check_return_contract(cypher: &str) -> Result<(), String> {
    let lower = cypher.to_ascii_lowercase();
    // Cypher `AS` is case-insensitive; alias names are case-sensitive
    // on the wire, so we lowercase the haystack but check for the
    // lowercased alias names directly.
    if !lower.contains("as primaryeid") {
        return Err(
            "RETURN clause must alias the element id as `primaryEid` (no `AS primaryEid` found)."
                .to_owned(),
        );
    }
    if !lower.contains("as doc") {
        return Err(
            "RETURN clause must alias the projection map as `doc` (no `AS doc` found).".to_owned(),
        );
    }
    Ok(())
}

/// Drop the contents of `"..."` and `'...'` string literals (keeping
/// the surrounding quotes' positions). Used before tokenising for
/// keyword checks so quoted text can't false-positive.
fn strip_string_literals(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' || c == '\'' {
            let quote = c;
            // Skip until matching close-quote (handle backslash escape).
            while let Some(c2) = chars.next() {
                if c2 == '\\' {
                    chars.next();
                    continue;
                }
                if c2 == quote {
                    break;
                }
            }
            // Replace the whole literal with a single space so token
            // boundaries are preserved.
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

/// Run `EXPLAIN` against Neo4j for both the bootstrap and fan-out
/// forms of the spec. EXPLAIN parses and plans without executing, so
/// it surfaces syntax errors, missing procedures, and (in some cases)
/// unknown labels at startup rather than at first CDC event.
async fn explain_spec(graph: &Graph, spec: &DenormalizeSpec) -> Result<(), Neo4jCdcError> {
    // Bootstrap form.
    let bs = format!(
        "EXPLAIN MATCH (p:`{label}`)\n{body}",
        label = spec.primary_label,
        body = spec.cypher,
    );
    let _ = graph.execute(query(&bs)).await.map_err(|err| {
        Neo4jCdcError::Internal(format!(
            "spec primary='{}' bootstrap cypher rejected by EXPLAIN: {err}",
            spec.primary_label
        ))
    })?;

    // Fan-out form — same body wrapped with the WHERE clause. Bind a
    // dummy empty list so EXPLAIN can resolve `$eids` to a parameter.
    let fo = format!("EXPLAIN {}", build_fan_out_cypher(spec));
    let _ = graph
        .execute(query(&fo).param("eids", Vec::<String>::new()))
        .await
        .map_err(|err| {
            Neo4jCdcError::Internal(format!(
                "spec primary='{}' fan-out cypher rejected by EXPLAIN: {err}",
                spec.primary_label
            ))
        })?;

    Ok(())
}

/// Best-effort estimate of the maximum relationship-chain depth the
/// user's Cypher walks away from `p` (the primary). Counts the
/// arrow-and-node sequences that follow `(p)` or `(p)<` / `(p)-`. Each
/// arrow (`->` / `<-`) is one hop.
///
/// Used by the analyze subcommand to warn when `fan_out_max_hops` is
/// configured smaller than what the Cypher actually traverses, which
/// would manifest as silent staleness for changes at the deepest hops.
///
/// This is a string-level heuristic — Cypher's full grammar would
/// require an AST, which is overkill for a config-time linter. The
/// heuristic favours false-positives (suggests a slightly higher hop
/// number than strictly needed) over false-negatives, so the warning
/// errs on the side of "tell the user to use more hops."
pub fn estimate_max_hops_in_cypher(cypher: &str) -> usize {
    // Strategy: find every `(p)` anchor, then walk forward counting
    // consecutive `]->(` or `]-(` or `]<-(` until the chain breaks.
    // Each `]->(` or `]<-(` is one hop. We then take the max across
    // all p-anchored chains in the cypher.
    //
    // Caveats this heuristic accepts:
    // - Cypher line continuations inside a single OPTIONAL MATCH are
    //   handled (we scan the raw string, not lines).
    // - WITH / WHERE clauses between hops break the chain — counted
    //   as separate matches, max taken across them. That's the right
    //   semantic since each OPTIONAL MATCH is its own path.
    // - User-named vars other than `p` are NOT followed. So a query
    //   that does WITH p AS q, then traverses from q, will under-count.
    //   Document the convention: use `p` for the primary anchor.

    let mut max_hops: usize = 0;

    // Find every occurrence of the primary anchor `(p)` so we can
    // count outgoing hops from it.
    let anchor_positions: Vec<usize> = cypher
        .match_indices("(p)")
        .map(|(i, _)| i)
        .chain(cypher.match_indices("(p:").map(|(i, _)| i)) // `(p:Label)` form too
        .collect();

    for start in anchor_positions {
        // Scan forward from after the anchor's `)` looking for
        // `-[...]-(` / `-[...]->(` / `<-[...]-(` patterns. We count
        // arrows; relationships ARE the hops.
        let rest = &cypher[start..];
        let hops = count_hops_in_chain(rest);
        if hops > max_hops {
            max_hops = hops;
        }
    }

    max_hops
}

/// Walk a substring starting at a `(p)` or `(p:Label)` anchor and count
/// consecutive relationship hops until the chain breaks. A "chain
/// break" is any token outside the `-[...]-(...)` / `<-[...]-(...)`
/// grammar — including a newline followed by a Cypher keyword.
// Hand-written byte scanner; every index is guarded by an `i < len` check.
#[allow(clippy::indexing_slicing)]
fn count_hops_in_chain(s: &str) -> usize {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    // Skip past the anchor `(p)` or `(p:Label)`.
    while i < bytes.len() && bytes[i] != b')' {
        i += 1;
    }
    if i < bytes.len() {
        i += 1; // past the closing paren
    }

    let mut hops = 0usize;
    loop {
        // Skip whitespace and line breaks.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        // Each hop is either `-[...]->(...)` or `<-[...]-(...)` or
        // `-[...]-(...)`. Look for the opening `-` or `<-`.
        let next_is_dash = i < bytes.len() && bytes[i] == b'-';
        let next_is_back = i + 1 < bytes.len() && bytes[i] == b'<' && bytes[i + 1] == b'-';
        if !next_is_dash && !next_is_back {
            break;
        }
        // Consume the start of the hop.
        if next_is_back {
            i += 2; // past `<-`
        } else {
            i += 1; // past `-`
        }
        // Now expect `[...rel spec...]`.
        if i >= bytes.len() || bytes[i] != b'[' {
            break;
        }
        let mut depth = 0;
        while i < bytes.len() {
            if bytes[i] == b'[' {
                depth += 1;
            }
            if bytes[i] == b']' {
                depth -= 1;
                if depth == 0 {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
        // After `]`, expect `->(...)` or `-(...)`.
        if i < bytes.len() && bytes[i] == b'-' {
            i += 1;
            if i < bytes.len() && bytes[i] == b'>' {
                i += 1;
            }
        }
        // Expect opening paren of the next node.
        if i >= bytes.len() || bytes[i] != b'(' {
            break;
        }
        // Skip past the node's `(...)` body (which may include labels,
        // properties, etc.; we don't parse them).
        let mut paren_depth = 0;
        while i < bytes.len() {
            if bytes[i] == b'(' {
                paren_depth += 1;
            }
            if bytes[i] == b')' {
                paren_depth -= 1;
                if paren_depth == 0 {
                    i += 1;
                    break;
                }
            }
            i += 1;
        }
        hops += 1;
    }
    hops
}

/// Which fan-out form a spec ended up using. Surfaced at startup so
/// operators can confirm the projection path actually applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanOutMode {
    /// Projection-aware: one typed `EXISTS` per sub-path the user's
    /// Cypher walks. Fast on cascade events (~seconds vs minutes).
    Projection {
        /// Number of typed `EXISTS` clauses emitted (one per
        /// extracted sub-path, de-duplicated). Zero when the user's
        /// Cypher is property-only (no hops away from `p`).
        hop_clauses: usize,
    },
    /// Old variable-length path scan. Set when the user opts out via
    /// env var, OR when the projection extractor reports an unsupported
    /// Cypher shape (anonymous rels, variable-length rels, etc.).
    PathScan {
        /// Human-readable explanation of why projection mode wasn't
        /// used. Surfaced in startup logs for operator visibility.
        reason: String,
    },
}

/// Compose the per-spec fan-out + recompose Cypher.
///
/// The `$eids` parameter is a Bolt list. For node events it's
/// `[event.elementId]`; for relationship events it's
/// `[event.elementId, start.elementId, end.elementId]` so the WHERE
/// clause can still find affected primaries even when the relationship
/// itself has been deleted (path scans can't traverse a missing edge).
///
/// Two forms — see [`FanOutMode`]:
///
/// - **Projection-aware** (default when the Cypher parses cleanly). Emits
///   `EXISTS { MATCH (p)-[:TYPE]->(x) WHERE elementId(x) IN $eids }`
///   for each typed sub-path. The planner can start at `x` via the
///   elementId index and walk back to `(p)` through typed edges, so a
///   Department rename touches O(reverse-degree) edges instead of
///   scanning every Author.
///
/// - **Path scan** fallback. Variable-length `MATCH path = (p)-[*1..N]-(x)`.
///   Handles arbitrary user Cypher (anonymous rels, variable-length
///   rels) at the cost of being slow on cascade events at scale.
pub(crate) fn build_fan_out_cypher(spec: &DenormalizeSpec) -> String {
    build_fan_out_cypher_with_mode(spec).0
}

/// Build the memory-bounded stage-1 fan-out query. It returns only affected
/// primary element IDs; full user projections are deliberately excluded.
pub(crate) fn build_fan_out_selector_cypher(spec: &DenormalizeSpec) -> String {
    build_fan_out_selector_cypher_with_config(spec, projection_fan_out_enabled())
}

/// Config-explicit selector builder used by the source after configuration
/// resolution. Keeping the switch explicit avoids an environment override
/// disagreeing with the validated runtime setting.
pub(crate) fn build_fan_out_selector_cypher_with_config(
    spec: &DenormalizeSpec,
    projection_fan_out: bool,
) -> String {
    let n = spec.fan_out_max_hops;
    if projection_fan_out {
        if let ProjectionExtract::Paths(paths) = extract_projection_paths(&spec.cypher) {
            let call_block = build_projection_call_block(&spec.primary_label, &paths, n);
            return format!("{call_block}RETURN elementId(p) AS primaryEid");
        }
    }
    format!(
        "MATCH (p:`{label}`) WHERE {where_clause}\n\
         RETURN elementId(p) AS primaryEid",
        label = spec.primary_label,
        where_clause = path_scan_where_clause(n),
    )
}

/// Build the stage-2 direct projection query for a bounded primary-ID chunk.
fn build_direct_recompose_cypher(spec: &DenormalizeSpec) -> String {
    format!(
        "MATCH (p:`{label}`) WHERE elementId(p) IN $primaryEids\n\
         WITH p\n{body}",
        label = spec.primary_label,
        body = spec.cypher,
    )
}

/// Like [`build_fan_out_cypher`] but also surfaces which form was used,
/// so the source can log it once at startup.
pub(crate) fn build_fan_out_cypher_with_mode(spec: &DenormalizeSpec) -> (String, FanOutMode) {
    build_fan_out_cypher_with_mode_config(spec, projection_fan_out_enabled())
}

fn build_fan_out_cypher_with_mode_config(
    spec: &DenormalizeSpec,
    projection_fan_out: bool,
) -> (String, FanOutMode) {
    let n = spec.fan_out_max_hops;

    if !projection_fan_out {
        return (
            wrap_path_scan(spec, n),
            FanOutMode::PathScan {
                reason: "projection fan-out disabled by configuration".to_owned(),
            },
        );
    }

    match extract_projection_paths(&spec.cypher) {
        ProjectionExtract::Paths(paths) => {
            // Use the CALL { UNION } form. Anchors at `elementId(x) IN
            // $eids` (small indexed list) and walks back to (p:Label)
            // via typed relationships — the planner picks the indexed
            // lookup as the leaf, avoiding the 100k label scan.
            let call_block = build_projection_call_block(&spec.primary_label, &paths, n);
            // Count UNION-joined branches beyond the primary-anchor
            // branch — that's the number of distinct typed paths
            // actually generated, which is what `hop_clauses` measures
            // for operator-facing logs.
            let hop_clauses = call_block.matches("UNION").count();
            let cypher = format!(
                "{call_block}{body}",
                call_block = call_block,
                body = spec.cypher
            );
            (cypher, FanOutMode::Projection { hop_clauses })
        }
        ProjectionExtract::Unsupported { reason } => {
            (wrap_path_scan(spec, n), FanOutMode::PathScan { reason })
        }
    }
}

/// Compose the path-scan fallback envelope around the user's Cypher.
fn wrap_path_scan(spec: &DenormalizeSpec, n: usize) -> String {
    format!(
        "MATCH (p:`{label}`) WHERE {where_clause}\n{body}",
        label = spec.primary_label,
        where_clause = path_scan_where_clause(n),
        body = spec.cypher,
    )
}

/// The original variable-length path-scan WHERE body. Kept as the
/// fallback form for Cypher shapes the projection extractor can't
/// invert (anonymous rels, variable-length rels, …).
fn path_scan_where_clause(n: usize) -> String {
    if n == 0 {
        "elementId(p) IN $eids".to_owned()
    } else {
        format!(
            "elementId(p) IN $eids \
             OR EXISTS {{ \
               MATCH path = (p)-[*1..{n}]-(x) \
               WHERE any(r IN relationships(path) WHERE elementId(r) IN $eids) \
                  OR any(node IN nodes(path)     WHERE elementId(node) IN $eids) \
             }}"
        )
    }
}

/// Build an OS-delete event for a primary that has been removed from
/// the graph. The OS sink keys deletes on the `.delete` suffix in the
/// subject; we also stamp `ventstream.doc.id` so the bulk delete action
/// targets exactly the same doc the upsert path writes.
fn build_delete_event(
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    element_id: &str,
    tx_id: i64,
) -> Result<Event, Neo4jCdcError> {
    let source = SourceUri::new(format!(
        "neo4j://{db}/_/{table}",
        db = percent(&config.database),
        table = percent(&spec.output_table),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;
    let subject = Subject::new(format!(
        "neo4j.{ns}.{table}.delete",
        ns = sanitize_segment(&config.namespace),
        table = sanitize_segment(&spec.output_table),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;

    let mut headers: HashMap<String, String> = HashMap::with_capacity(8);
    headers.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    headers.insert(
        "ventstream.cdc.relation".to_owned(),
        spec.output_table.clone(),
    );
    headers.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    headers.insert(
        "ventstream.doc.id".to_owned(),
        format!("{}:{}", spec.output_table, element_id),
    );
    headers.insert(
        "ventstream.cdc.event_type".to_owned(),
        "denormalized_delete".to_owned(),
    );
    headers.insert(
        "ventstream.cdc.element_id".to_owned(),
        element_id.to_owned(),
    );
    headers.insert(
        "ventstream.denormalize.primary".to_owned(),
        spec.primary_label.clone(),
    );
    headers.insert("ventstream.cdc.tx_id".to_owned(), tx_id.to_string());

    // Bulk delete actions don't carry a body — the OS sink emits the
    // action line only when the subject ends with `.delete`. We still
    // supply an empty {} payload for defensiveness against any sink
    // that wants to log it.
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(headers))
        .build())
}

/// Build one OS-bound event from a denormalised doc. Stamps
/// `ventstream.doc.id = "{output_table}:{primary_eid}"` so the OS sink
/// upserts in place instead of inserting a new doc per recomputation.
#[allow(clippy::needless_pass_by_value)] // `doc` is serialized into the event payload
fn build_event(
    config: &Neo4jCdcConfig,
    spec: &DenormalizeSpec,
    element_id: &str,
    doc: Value,
    bootstrap: bool,
    tx_id: Option<i64>,
) -> Result<Event, Neo4jCdcError> {
    let source = SourceUri::new(format!(
        "neo4j://{db}/_/{table}",
        db = percent(&config.database),
        table = percent(&spec.output_table),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;
    // `neo4j.{ns}.{table}.upsert` — same 4-segment subject grammar the
    // standard mode emits; consumers don't need a separate parser.
    let subject = Subject::new(format!(
        "neo4j.{ns}.{table}.upsert",
        ns = sanitize_segment(&config.namespace),
        table = sanitize_segment(&spec.output_table),
    ))
    .map_err(|err| Neo4jCdcError::Internal(err.to_string()))?;

    let mut headers: HashMap<String, String> = HashMap::with_capacity(8);
    headers.insert(
        "ventstream.cdc.namespace".to_owned(),
        config.namespace.clone(),
    );
    headers.insert(
        "ventstream.cdc.relation".to_owned(),
        spec.output_table.clone(),
    );
    headers.insert(
        "ventstream.cdc.database".to_owned(),
        config.database.clone(),
    );
    headers.insert(
        "ventstream.doc.id".to_owned(),
        format!("{}:{}", spec.output_table, element_id),
    );
    headers.insert(
        "ventstream.cdc.event_type".to_owned(),
        "denormalized".to_owned(),
    );
    headers.insert(
        "ventstream.cdc.element_id".to_owned(),
        element_id.to_owned(),
    );
    headers.insert(
        "ventstream.denormalize.primary".to_owned(),
        spec.primary_label.clone(),
    );
    if bootstrap {
        headers.insert("ventstream.cdc.bootstrap".to_owned(), "snapshot".to_owned());
    }
    if let Some(tx) = tx_id {
        headers.insert("ventstream.cdc.tx_id".to_owned(), tx.to_string());
    }

    let bytes = serde_json::to_vec(&doc)
        .map_err(|err| Neo4jCdcError::Internal(format!("encoding doc: {err}")))?;
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(bytes))
        .content_type(ContentType::Json)
        .occurred_at(Utc::now())
        .headers(Headers::from_map(headers))
        .build())
}

fn percent(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

fn sanitize_segment(input: &str) -> String {
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

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn spec(label: &str, table: &str, max_hops: usize) -> DenormalizeSpec {
        DenormalizeSpec {
            primary_label: label.to_owned(),
            output_table: table.to_owned(),
            cypher: "RETURN elementId(p) AS primaryEid, p AS doc".to_owned(),
            fan_out_max_hops: max_hops,
        }
    }

    #[test]
    fn fan_out_zero_hops_only_matches_primary_itself() {
        let cy = build_fan_out_cypher(&spec("Author", "authors", 0));
        // No traversal whatsoever — only the primary-anchor branch.
        assert!(cy.contains("MATCH (p:`Author`) WHERE elementId(p) IN __eids"));
        assert!(!cy.contains("[*1.."));
        assert!(!cy.contains("UNION"));
    }

    #[test]
    fn fan_out_property_only_projection_emits_no_traversal() {
        // The default test spec has no hops in its Cypher body, so the
        // projection-aware extractor returns zero paths. The generated
        // CALL block should have only the primary-anchor branch — no
        // UNION, no path scan.
        let (cy, mode) = build_fan_out_cypher_with_mode(&spec("Author", "authors", 2));
        assert!(cy.contains("MATCH (p:`Author`) WHERE elementId(p) IN __eids"));
        assert!(!cy.contains("[*1.."));
        assert!(!cy.contains("UNION"));
        assert!(matches!(mode, FanOutMode::Projection { hop_clauses: 0 }));
    }

    #[test]
    fn fan_out_projection_mode_emits_typed_union_branches() {
        let mut s = spec("Author", "authors", 2);
        // Realistic generic traversal — typed hops, mixed depths.
        s.cypher = "
            OPTIONAL MATCH (p)-[:HAS_NAME]->(n:Name)
            OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)-[:IN_GENRE]->(ra:Genre)
            RETURN elementId(p) AS primaryEid, p AS doc
        "
        .to_owned();
        let (cy, mode) = build_fan_out_cypher_with_mode(&s);
        assert!(cy.starts_with("WITH $eids AS __eids"));
        // Anchor branch.
        assert!(cy.contains("MATCH (p:`Author`) WHERE elementId(p) IN __eids"));
        // Depth-1 typed branches.
        assert!(cy.contains("MATCH (p:`Author`)-[:HAS_NAME]->(x) WHERE elementId(x) IN __eids"));
        assert!(cy.contains("MATCH (p:`Author`)-[:HAS_BOOK]->(x) WHERE elementId(x) IN __eids"));
        // Depth-2 typed branch.
        assert!(cy.contains(
            "MATCH (p:`Author`)-[:HAS_BOOK]->()-[:IN_GENRE]->(x) WHERE elementId(x) IN __eids"
        ));
        // WITH DISTINCT p before the user's body.
        assert!(cy.contains("WITH DISTINCT p\n"));
        // No variable-length path scan in projection mode.
        assert!(!cy.contains("[*1..2]"));
        match mode {
            FanOutMode::Projection { hop_clauses } => assert_eq!(hop_clauses, 3),
            FanOutMode::PathScan { .. } => panic!("expected Projection mode"),
        }
    }

    #[test]
    fn fan_out_selector_excludes_the_expensive_projection_body() {
        let mut s = spec("Author", "authors", 2);
        s.cypher = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(b:Book) \
                    RETURN elementId(p) AS primaryEid, collect(b) AS doc"
            .to_owned();
        let selector = build_fan_out_selector_cypher_with_config(&s, true);
        assert!(selector.contains("[:HAS_BOOK]"));
        assert!(selector.ends_with("RETURN elementId(p) AS primaryEid"));
        assert!(!selector.contains("collect(b)"));
        assert!(!selector.contains(" AS doc"));
    }

    #[test]
    fn direct_recompose_is_bounded_by_primary_element_ids() {
        let mut s = spec("Author", "authors", 2);
        s.cypher = "RETURN elementId(p) AS primaryEid, p { .* } AS doc".to_owned();
        let cypher = build_direct_recompose_cypher(&s);
        assert!(cypher.contains("elementId(p) IN $primaryEids"));
        assert!(cypher.contains(&s.cypher));
        assert!(!cypher.contains("$eids"));
    }

    #[test]
    fn fan_out_dedup_rollover_requires_a_generation_barrier() {
        let mut generation = FanOutDedupGeneration::new(2);

        assert_eq!(generation.classify("primary-a"), FanOutDedupAction::Admit);
        assert_eq!(generation.classify("primary-b"), FanOutDedupAction::Admit);
        assert_eq!(
            generation.classify("primary-a"),
            FanOutDedupAction::Duplicate,
            "duplicates in the full generation must not force rollover"
        );
        assert_eq!(
            generation.classify("primary-c"),
            FanOutDedupAction::Rollover,
            "a new ID requires the caller to drain in-flight projections"
        );

        generation.begin_next("primary-c".to_owned());
        assert_eq!(
            generation.classify("primary-c"),
            FanOutDedupAction::Duplicate
        );
        assert_eq!(
            generation.classify("primary-a"),
            FanOutDedupAction::Admit,
            "an older ID may be recomposed only in the drained next generation"
        );
    }

    #[test]
    fn fan_out_dedup_uses_a_minimum_capacity_of_one() {
        let mut generation = FanOutDedupGeneration::new(0);

        assert_eq!(generation.classify("primary-a"), FanOutDedupAction::Admit);
        assert_eq!(
            generation.classify("primary-b"),
            FanOutDedupAction::Rollover
        );
    }

    #[test]
    fn fan_out_falls_back_to_path_scan_on_anonymous_rel() {
        let mut s = spec("Author", "authors", 2);
        // Anonymous rel — extractor can't invert, must fall back.
        s.cypher =
            "OPTIONAL MATCH (p)-[]->(x) RETURN elementId(p) AS primaryEid, p AS doc".to_owned();
        let (cy, mode) = build_fan_out_cypher_with_mode(&s);
        assert!(cy.contains("[*1..2]"));
        assert!(cy.contains("relationships(path)"));
        assert!(matches!(mode, FanOutMode::PathScan { .. }));
    }

    #[test]
    fn fan_out_falls_back_to_path_scan_on_variable_length() {
        let mut s = spec("Author", "authors", 3);
        s.cypher =
            "OPTIONAL MATCH (p)-[*1..3]-(x) RETURN elementId(p) AS primaryEid, p AS doc".to_owned();
        let (cy, mode) = build_fan_out_cypher_with_mode(&s);
        assert!(cy.contains("[*1..3]"));
        assert!(matches!(mode, FanOutMode::PathScan { .. }));
    }

    #[test]
    fn write_keyword_create_rejected() {
        let cy =
            "MATCH (p:Author) CREATE (p)-[:LIKES]->(x) RETURN elementId(p) AS primaryEid, x AS doc";
        let err = check_no_write_clauses(cy).expect_err("should reject");
        assert!(err.contains("'create'"));
    }

    #[test]
    fn write_keyword_set_rejected() {
        let cy = "MATCH (p:Author) SET p.foo = 1 RETURN elementId(p) AS primaryEid, p AS doc";
        assert!(check_no_write_clauses(cy).is_err());
    }

    #[test]
    fn quoted_create_in_string_not_rejected() {
        // The word "CREATE" appears inside a quoted string — should NOT trip the lint.
        let cy = "MATCH (p:Author) WHERE p.action = \"CREATE\" RETURN elementId(p) AS primaryEid, p AS doc";
        assert!(check_no_write_clauses(cy).is_ok());
    }

    #[test]
    fn underscore_identifier_with_keyword_prefix_not_rejected() {
        // `create_date` is a property name, NOT the CREATE keyword.
        let cy = "MATCH (p:Author) WHERE p.create_date > datetime() RETURN elementId(p) AS primaryEid, p AS doc";
        assert!(check_no_write_clauses(cy).is_ok());
    }

    #[test]
    fn return_contract_requires_primary_eid_alias() {
        let cy = "MATCH (p:Author) RETURN p AS doc";
        let err = check_return_contract(cy).expect_err("should reject");
        assert!(err.contains("primaryEid"));
    }

    #[test]
    fn return_contract_requires_doc_alias() {
        let cy = "MATCH (p:Author) RETURN elementId(p) AS primaryEid";
        let err = check_return_contract(cy).expect_err("should reject");
        assert!(err.contains("doc"));
    }

    #[test]
    fn return_contract_accepts_canonical_shape() {
        let cy = "MATCH (p:Author) RETURN elementId(p) AS primaryEid, {id: p.id} AS doc";
        assert!(check_return_contract(cy).is_ok());
    }

    #[test]
    fn event_meta_extracts_relationship_endpoints() {
        let json = serde_json::json!({
            "elementId": "5:rel:1",
            "eventType": "r",
            "operation": "d",
            "start": { "elementId": "4:start:1", "labels": ["Author"] },
            "end":   { "elementId": "4:end:1",   "labels": ["Department"] }
        });
        let meta = EventMeta::extract(&json);
        assert_eq!(meta.element_id.as_deref(), Some("5:rel:1"));
        assert!(!meta.is_node_delete()); // it's a rel, not node
        assert_eq!(meta.start_eid.as_deref(), Some("4:start:1"));
        assert_eq!(meta.end_eid.as_deref(), Some("4:end:1"));
    }

    #[test]
    fn estimate_zero_hops_for_pure_return() {
        let cy = "WITH p RETURN elementId(p) AS primaryEid, p AS doc";
        assert_eq!(estimate_max_hops_in_cypher(cy), 0);
    }

    #[test]
    fn estimate_one_hop_for_single_optional_match() {
        let cy = "OPTIONAL MATCH (p)-[hn:HAS_NAME]->(name:Name) RETURN elementId(p) AS primaryEid, name AS doc";
        assert_eq!(estimate_max_hops_in_cypher(cy), 1);
    }

    #[test]
    fn estimate_two_hops_for_chained_traversal() {
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep:Book)-[:IN_GENRE]->(ra:Genre) RETURN elementId(p) AS primaryEid, ra AS doc";
        assert_eq!(estimate_max_hops_in_cypher(cy), 2);
    }

    #[test]
    fn estimate_three_hops_for_agent_chain() {
        let cy = "OPTIONAL MATCH (p)-[:HAS_BOOK]->(r:Book)<-[:REVIEWS]-(a:Review)<-[:HAS_REVIEW]-(agent:Author) RETURN elementId(p) AS primaryEid, agent AS doc";
        assert_eq!(estimate_max_hops_in_cypher(cy), 3);
    }

    #[test]
    fn estimate_max_across_separate_matches() {
        let cy = "
            OPTIONAL MATCH (p)-[:HAS_NAME]->(n)
            OPTIONAL MATCH (p)-[:HAS_BOOK]->(rep)-[:IN_GENRE]->(ra)
            RETURN elementId(p) AS primaryEid, n AS doc
        ";
        // One match is 1 hop, the other is 2; max wins.
        assert_eq!(estimate_max_hops_in_cypher(cy), 2);
    }

    #[test]
    fn event_meta_detects_node_delete_via_state_before_labels() {
        // Real CDC payload shape for a node delete: state.after is null,
        // state.before.labels carries what the node WAS.
        let json = serde_json::json!({
            "elementId": "4:p:1",
            "eventType": "n",
            "operation": "d",
            "state": {
                "before": { "labels": ["Person", "Author"], "properties": {} },
                "after": null
            }
        });
        let meta = EventMeta::extract(&json);
        assert!(meta.is_node_delete());
        assert!(meta.has_label("Author"));
        assert!(meta.has_label("Person"));
    }

    #[test]
    fn keyset_cypher_paginates_primaries_independently_of_body() {
        // First page: no WHERE, ordered LIMIT on the PRIMARY keyset.
        let first = build_keyset_cypher("Author", false);
        assert!(first.contains("MATCH (p:`Author`)"));
        assert!(!first.contains("WHERE"), "first page has no keyset filter");
        assert!(first.contains("RETURN elementId(p) AS eid"));
        assert!(first.contains("ORDER BY elementId(p) LIMIT $batch"));
        // It must NOT contain any user body / doc projection — it's a pure
        // primary scan, so its row count == primary count.
        assert!(!first.contains("AS doc"));

        // Subsequent pages add the keyset filter.
        let next = build_keyset_cypher("Author", true);
        assert!(next.contains("WHERE elementId(p) > $lastEid"));
    }

    #[test]
    fn bootstrap_body_cypher_binds_exact_page_primaries() {
        let body = "OPTIONAL MATCH (p)-[:IN]->(d)\n\
                    RETURN elementId(p) AS primaryEid, { dept: d.name } AS doc";
        let cy = build_bootstrap_body_cypher("Author", body);
        // Primaries are bound by the page's eid list, not a LIMIT — so a body
        // that drops primaries can't shorten the page and end the scan early.
        assert!(cy.contains("WHERE elementId(p) IN $eids"));
        assert!(!cy.contains("LIMIT"), "body query must not re-limit");
        assert!(cy.contains("WITH p ORDER BY elementId(p)"));
        // User body pasted verbatim after the WITH.
        assert!(cy.ends_with(body));
    }

    #[test]
    fn fan_out_includes_primary_label_quoted() {
        let cy = build_fan_out_cypher(&spec("My-Weird Label", "out", 1));
        // Backtick-quoted so labels with spaces / hyphens work.
        assert!(cy.contains("MATCH (p:`My-Weird Label`)"));
    }

    #[test]
    fn fan_out_pastes_user_cypher_verbatim() {
        let mut s = spec("Author", "authors", 1);
        s.cypher = "WITH p, 42 AS magic\nRETURN elementId(p) AS primaryEid, {} AS doc".to_owned();
        let cy = build_fan_out_cypher(&s);
        assert!(cy.contains("WITH p, 42 AS magic"));
        assert!(cy.ends_with("RETURN elementId(p) AS primaryEid, {} AS doc"));
    }

    #[test]
    fn from_yaml_file_round_trips() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vs-denormalize-test-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            r#"denormalize:
  - primary_label: Author
    output_table: parties_denormalized
    fan_out_max_hops: 2
    cypher: |
      WITH p, datetime() AS now
      RETURN elementId(p) AS primaryEid, { id: p.id } AS doc
  - primary_label: Order
    output_table: orders_denormalized
    cypher: |
      RETURN elementId(p) AS primaryEid, p AS doc
"#,
        )
        .expect("write yaml");
        let specs = DenormalizeSpecs::from_yaml_file(&path).expect("parse");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs.denormalize[0].primary_label, "Author");
        assert_eq!(specs.denormalize[0].fan_out_max_hops, 2);
        assert_eq!(specs.denormalize[1].primary_label, "Order");
        // Default applies.
        assert_eq!(specs.denormalize[1].fan_out_max_hops, 2);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn from_yaml_file_rejects_empty_cypher() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vs-denormalize-bad-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            r#"denormalize:
  - primary_label: Author
    output_table: authors
    cypher: ""
"#,
        )
        .expect("write yaml");
        let err = DenormalizeSpecs::from_yaml_file(&path).expect_err("should reject");
        assert!(err.to_string().contains("empty cypher"));
        std::fs::remove_file(&path).ok();
    }
}
