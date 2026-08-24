//! Delete-reconciliation pass for drain-resume bootstraps.
//!
//! After an agent has been auto-drained (long pause expired, slot
//! dropped, redb wiped) and the operator then resumes, the agent
//! re-bootstraps the primary tables from a fresh snapshot of source
//! state. That re-creates every row that *currently exists* in the
//! source, but it doesn't know about rows that were *deleted* during
//! the pause window — those rows are simply absent from the snapshot.
//! The corresponding OS docs were never told to go away, so they sit
//! as orphans referencing PKs that no longer exist upstream.
//!
//! This module deletes those orphans before the bootstrap runs. The
//! orchestrator queries the source for the set of currently-valid PKs
//! and calls [`reconcile_orphans`] with that set plus the doc-ID
//! prefix the engine writes (e.g. `public.orders:`). We scroll the
//! target index, diff every doc ID against the valid-PK set, and
//! bulk-delete the misses.
//!
//! ### Why before bootstrap, not after
//!
//! After-bootstrap reconciliation either needs deep engine hooks (sink
//! reacts to a snapshot-complete sentinel) or per-doc epoch markers
//! that the sink writes on every snapshot row. Both leak source/sink
//! coupling. Pre-bootstrap reconciliation is a standalone async
//! function called from `main.rs`, run once per drain-resume cycle,
//! with no engine changes. The "wasted re-write" concern doesn't
//! apply — bootstrap was going to rewrite every legitimate doc anyway;
//! we're just removing the stale ones before it does.

use std::collections::HashSet;

use reqwest::{header, StatusCode};
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::config::{AuthMode, OpenSearchConfig};

/// Errors surfaced from a reconciliation pass. Mirrors the
/// granularity of the OpenSearch sink's error type but stays
/// self-contained — reconciliation is a one-shot maintenance op,
/// not a hot-path component, so it owns its own error type.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// Failed to build the HTTP client. Misconfigured TLS / proxy / etc.
    #[error("failed to build reqwest client: {0}")]
    Client(String),
    /// HTTP transport error (DNS, connection refused, timeout, ...).
    #[error("opensearch transport error: {0}")]
    Transport(String),
    /// OpenSearch returned a non-2xx response to one of our requests.
    /// The body (truncated) is included so operators can see the
    /// upstream complaint without re-running with debug logging.
    #[error("opensearch responded with {status}: {body}")]
    BadStatus {
        /// HTTP status code.
        status: u16,
        /// First ~1KB of the response body.
        body: String,
    },
    /// JSON decode error — usually means the OS endpoint isn't what we
    /// think it is (e.g. pointed at a generic JSON service).
    #[error("opensearch returned malformed JSON: {0}")]
    Decode(String),
    /// The mass-deletion guardrail refused the pass — deleting would have
    /// wiped the whole index or more than [`MAX_DELETE_FRACTION`] of it,
    /// almost certainly from a transient/truncated source query. **No docs
    /// were deleted.** Set `OpenSearchConfig::reconcile_allow_full_purge`
    /// to override for an intended full purge.
    #[error("reconcile refused (mass-deletion guardrail): {0}")]
    Refused(String),
}

/// Refuse a reconcile pass that would delete more than this fraction of the
/// examined (prefix-matching) docs — almost always a transient/truncated
/// source query rather than a genuine mass-orphaning. Bypassed by
/// `OpenSearchConfig::reconcile_allow_full_purge`.
pub const MAX_DELETE_FRACTION: f64 = 0.5;

/// Pure decision for the mass-deletion guardrail (unit-tested). `Ok(())`
/// = safe to delete; `Err(reason)` = refuse and delete nothing.
fn check_delete_guard(
    valid_pks_len: usize,
    examined_matching: usize,
    would_delete: usize,
    allow_full_purge: bool,
) -> Result<(), String> {
    if allow_full_purge {
        return Ok(());
    }
    // Catastrophic case: source returned nothing, so every matching doc
    // looks like an orphan.
    if valid_pks_len == 0 {
        return Err(format!(
            "source returned 0 valid PKs but {examined_matching} matching docs exist; \
             refusing to delete all of them (likely a transient/empty source query)"
        ));
    }
    // Mass-orphaning: more than the cap would be deleted.
    if examined_matching > 0 {
        let frac = would_delete as f64 / examined_matching as f64;
        if frac > MAX_DELETE_FRACTION {
            return Err(format!(
                "{would_delete}/{examined_matching} docs ({:.1}%) would be deleted, \
                 over the {:.0}% cap (likely a truncated source query)",
                frac * 100.0,
                MAX_DELETE_FRACTION * 100.0
            ));
        }
    }
    Ok(())
}

/// Describes how the engine encodes the primary-key value into a
/// doc ID for a given source. Both source backends use a `{prefix}…`
/// shape, but the tail differs:
///
/// - [`DocIdFormat::JsonArray`] (Postgres) — IDs look like
///   `public.orders:["abc-uuid"]`. The PK is JSON-encoded inside a
///   one-element array, leaving headroom for composite keys (which
///   v1 reconciliation still skips).
/// - [`DocIdFormat::RawSuffix`] (Neo4j) — IDs look like
///   `parties_denormalized:4:abc-uuid:42`. The element ID is
///   appended verbatim, no escaping. Reconciliation just strips the
///   prefix to recover the upstream key.
///
/// New source backends introduce a new variant rather than changing
/// the parser logic for existing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocIdFormat {
    /// Postgres: `{prefix}["{pk}"]` (JSON-array wrapping).
    JsonArray,
    /// Neo4j: `{prefix}{raw_element_id}` (no wrapping).
    RawSuffix,
}

/// Scroll the target index, diff each doc ID against `valid_pks`,
/// bulk-delete the mismatches. Returns the number of docs removed.
///
/// `doc_id_prefix` is the deterministic prefix the engine uses for
/// doc IDs in this index — `{schema.table}:` for the Postgres source,
/// `{output_table}:` for Neo4j. Doc IDs that don't start with this
/// prefix are skipped: they belong to other primary tables that share
/// the index and have their own reconciliation pass.
///
/// `valid_pks` is the set of primary-key string representations
/// currently in the source. For the Postgres path it's the raw column
/// value (the wrapping is handled by [`DocIdFormat::JsonArray`]). For
/// the Neo4j path it's the raw `elementId(...)` string.
pub async fn reconcile_orphans(
    config: &OpenSearchConfig,
    index: &str,
    doc_id_prefix: &str,
    valid_pks: &HashSet<String>,
    id_format: DocIdFormat,
) -> Result<usize, ReconcileError> {
    // Mass-deletion guardrail — catastrophic case. An empty valid-PK set
    // makes every matching doc look like an orphan, so a transient/empty
    // source query would wipe the whole index. Refuse *before* scrolling.
    if valid_pks.is_empty() && !config.reconcile_allow_full_purge {
        return Err(ReconcileError::Refused(
            "source returned 0 valid PKs; refusing to delete the whole index \
             (set reconcile_allow_full_purge to override)"
                .to_string(),
        ));
    }

    let client = build_client(config)?;
    // Collect orphan candidates across the full scroll BEFORE deleting any,
    // so the ratio guardrail can see the whole picture (we can't know the
    // delete fraction until the scan completes).
    let mut to_delete: Vec<String> = Vec::new();
    let mut scroll_id: Option<String> = None;
    let mut docs_examined = 0usize;
    let mut docs_skipped_prefix = 0usize;

    loop {
        let batch = match &scroll_id {
            Some(sid) => scroll_continue(&client, config, sid).await?,
            None => scroll_open(&client, config, index, doc_id_prefix).await?,
        };
        if batch.ids.is_empty() && batch.scroll_id.is_none() {
            break;
        }

        scroll_id = batch.scroll_id;
        if batch.ids.is_empty() {
            break;
        }

        for id in batch.ids {
            docs_examined += 1;
            let Some(pk) = extract_pk_from_doc_id(&id, doc_id_prefix, id_format) else {
                docs_skipped_prefix += 1;
                continue;
            };
            if !valid_pks.contains(&pk) {
                to_delete.push(id);
            }
        }
    }

    // Best-effort scroll cleanup before we decide/delete.
    if let Some(sid) = scroll_id {
        if let Err(err) = clear_scroll(&client, config, &sid).await {
            debug!(error = %err, "clear_scroll failed (non-fatal)");
        }
    }

    // Mass-deletion guardrail — ratio case. Refuse if more than the cap of
    // the examined (prefix-matching) docs would be deleted.
    let examined_matching = docs_examined - docs_skipped_prefix;
    if let Err(reason) = check_delete_guard(
        valid_pks.len(),
        examined_matching,
        to_delete.len(),
        config.reconcile_allow_full_purge,
    ) {
        warn!(
            index,
            examined_matching,
            would_delete = to_delete.len(),
            valid_pks = valid_pks.len(),
            "reconcile refused by mass-deletion guardrail; no docs deleted"
        );
        return Err(ReconcileError::Refused(reason));
    }

    let mut total_deleted = 0usize;
    for chunk in to_delete.chunks(BULK_DELETE_BATCH) {
        bulk_delete(&client, config, index, chunk).await?;
        total_deleted += chunk.len();
    }

    info!(
        index,
        docs_examined,
        docs_skipped_prefix,
        orphans_deleted = total_deleted,
        valid_pks = valid_pks.len(),
        "reconciliation pass complete"
    );
    Ok(total_deleted)
}

/// Per-batch bulk-delete cap. 1000 is the OS default `max_action_count`
/// for a single `_bulk` body — bigger batches start to risk 413s on
/// constrained clusters. Each delete line is ~80 bytes, so a 1000-doc
/// batch is well under the default 100MB body limit.
const BULK_DELETE_BATCH: usize = 1000;

/// Ceiling on parents returned by a single [`lookup_doc_ids_by_terms`] call.
/// In a 1:many embed each child PK maps to exactly one parent, so the natural
/// upper bound is the number of looked-up values; this clamps a pathological
/// batch from requesting an unbounded page.
const REVERSE_LOOKUP_MAX_HITS: usize = 10_000;

/// Pooled client for sink reverse-index lookups.
///
/// This powers SQL-denormalize 1:many child-delete propagation under Postgres
/// `REPLICA IDENTITY DEFAULT`. On a child-row delete the WAL carries only the
/// child's own primary key, not the parent foreign key, so the recompose path
/// can't tell which parent to rebuild. But the denormalized parent doc already
/// embeds every child *with its PK*, and OpenSearch indexes that PK as a
/// keyword — so the sink IS the reverse index. We query it for the embedded
/// child PK and recompose whatever parent(s) come back: no `REPLICA IDENTITY
/// FULL`, no extra DB index, no resident engine state.
///
/// Owns a single [`reqwest::Client`] so a stream of child deletes reuses
/// pooled connections instead of paying a fresh TLS handshake per lookup.
/// Cheap to clone (the client is internally `Arc`'d).
#[derive(Clone)]
pub struct OsReverseLookup {
    client: reqwest::Client,
    config: OpenSearchConfig,
}

impl OsReverseLookup {
    /// Build the pooled client once from the sink config.
    pub fn new(config: OpenSearchConfig) -> Result<Self, ReconcileError> {
        Ok(Self {
            client: build_client(&config)?,
            config,
        })
    }

    /// The sink index template — callers render it to the concrete index a
    /// given primary's docs live in before looking up.
    pub fn index_template(&self) -> &str {
        &self.config.index_template
    }

    /// Find the `_id`s of parent docs embedding any of `values` at `field`.
    ///
    /// `field` is the keyword field of the embedded child PK (e.g.
    /// `items.item_id.keyword`); `values` are the deleted child PK strings —
    /// a whole batch of them resolves in ONE `terms` query. Returns the
    /// matching parent doc `_id`s (the canonical `{table}:[...]` form the
    /// engine writes), which the caller decodes back to primary keys.
    pub async fn doc_ids_by_terms(
        &self,
        index: &str,
        field: &str,
        values: &[String],
    ) -> Result<Vec<String>, ReconcileError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let size = values.len().clamp(1, REVERSE_LOOKUP_MAX_HITS);
        self.search_doc_ids(index, reverse_lookup_query(field, values, size))
            .await
    }

    /// Composite-child-PK variant: find parents embedding a child whose PK
    /// columns equal one of `tuples`. `fields` are the embedded keyword fields
    /// (one per PK column, e.g. `["items.region", "items.item_id"]`); each
    /// tuple's components align with `fields`.
    ///
    /// On the default `object` (flattened) mapping a `bool/must` can cross-match
    /// columns from different array elements (a doc with items `[{EU,5},{US,9}]`
    /// matches `region=EU AND item_id=9`). That's harmless: the lookup only
    /// SELECTS candidate parents to recompose, and recompose rebuilds the doc
    /// from current DB truth — so the true parent never misses (no false
    /// negatives) and an over-selected parent is just re-emitted correctly.
    pub async fn doc_ids_by_term_tuples(
        &self,
        index: &str,
        fields: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<String>, ReconcileError> {
        if fields.is_empty() || tuples.is_empty() {
            return Ok(Vec::new());
        }
        let size = tuples.len().clamp(1, REVERSE_LOOKUP_MAX_HITS);
        self.search_doc_ids(index, reverse_lookup_tuples_query(fields, tuples, size))
            .await
    }

    async fn search_doc_ids(
        &self,
        index: &str,
        body: serde_json::Value,
    ) -> Result<Vec<String>, ReconcileError> {
        let url = format!(
            "{}/{}/_search",
            self.config.endpoint.trim_end_matches('/'),
            index,
        );
        let req = apply_auth(self.client.post(&url), &self.config.auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
        let res = req
            .send()
            .await
            .map_err(|err| ReconcileError::Transport(err.to_string()))?;
        let status = res.status();
        if !status.is_success() {
            return Err(bad_status(status, res).await);
        }
        let body: SearchResponseBody = res
            .json()
            .await
            .map_err(|err| ReconcileError::Decode(err.to_string()))?;
        Ok(body.hits.hits.into_iter().map(|h| h.id).collect())
    }
}

/// Build the reverse-lookup query body. Matches on BOTH the base field and its
/// `.keyword` subfield so it works whichever way the embedded child PK mapped:
/// a numeric PK maps as `long` (no `.keyword` subfield; OS coerces the string
/// values), while a string PK matches exactly on `.keyword`. A field that
/// doesn't exist simply contributes 0 matches. Callers pass the BASE field
/// (e.g. `items.id`), not `…​.keyword`.
fn reverse_lookup_query(field: &str, values: &[String], size: usize) -> serde_json::Value {
    serde_json::json!({
        "size": size,
        "_source": false,
        "query": field_or_keyword(field, values),
    })
}

/// Match `field` OR `field.keyword` for the given values (numeric PKs map as
/// `long` with no `.keyword`; string PKs match on `.keyword`).
fn field_or_keyword(field: &str, values: &[String]) -> serde_json::Value {
    serde_json::json!({
        "bool": {
            "should": [
                { "terms": { field: values } },
                { "terms": { format!("{field}.keyword"): values } },
            ],
            "minimum_should_match": 1
        }
    })
}

/// Composite reverse-lookup: `bool/should` across tuples, each a `bool/must`
/// over its PK columns, each column a base-or-`.keyword` match. See
/// [`OsReverseLookup::doc_ids_by_term_tuples`] for why cross-element matches
/// are harmless.
fn reverse_lookup_tuples_query(
    fields: &[String],
    tuples: &[Vec<String>],
    size: usize,
) -> serde_json::Value {
    let shoulds: Vec<serde_json::Value> = tuples
        .iter()
        .map(|tuple| {
            let musts: Vec<serde_json::Value> = fields
                .iter()
                .zip(tuple)
                .map(|(field, value)| field_or_keyword(field, std::slice::from_ref(value)))
                .collect();
            serde_json::json!({ "bool": { "must": musts } })
        })
        .collect();
    serde_json::json!({
        "size": size,
        "_source": false,
        "query": { "bool": { "should": shoulds, "minimum_should_match": 1 } },
    })
}

#[derive(Debug, Deserialize)]
struct SearchResponseBody {
    hits: ScrollHits,
}

/// Scroll keep-alive duration. OS holds the scroll context for at
/// least this long after each `_search/scroll` call. Long enough to
/// survive a slow diff pass on a big batch.
const SCROLL_KEEP_ALIVE: &str = "2m";

/// Per-batch scroll size. Trade-off: bigger batches = fewer HTTP
/// round-trips, but more memory + per-batch latency.
const SCROLL_BATCH_SIZE: usize = 1_000;

#[derive(Debug, Default)]
pub(crate) struct ScrollBatch {
    pub(crate) scroll_id: Option<String>,
    pub(crate) ids: Vec<String>,
}

pub(crate) async fn scroll_open(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    index: &str,
    _doc_id_prefix: &str,
) -> Result<ScrollBatch, ReconcileError> {
    let url = format!(
        "{}/{}/_search?scroll={}",
        config.endpoint.trim_end_matches('/'),
        index,
        SCROLL_KEEP_ALIVE,
    );
    // OpenSearch rejects `prefix` queries against `_id` (it's not a
    // keyword or text field), so we can't filter server-side by ID
    // prefix. Scan the whole index instead and let the caller filter
    // each batch in-process via `extract_pk_from_doc_id`. For the
    // common single-primary-table-per-index case the prefix filter
    // would have matched everything anyway, so the only cost is in
    // mixed-source indexes where unrelated docs flow through the
    // diff loop — measured by `docs_skipped_prefix` in the summary
    // log.
    let body = serde_json::json!({
        "size": SCROLL_BATCH_SIZE,
        "_source": false,
        "query": { "match_all": {} },
        // Stable sort across pages — _doc is the cheapest sort and is
        // what the docs recommend for scroll.
        "sort": ["_doc"],
    });
    let req = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string());
    let res = req
        .send()
        .await
        .map_err(|err| ReconcileError::Transport(err.to_string()))?;
    parse_scroll_response(res).await
}

pub(crate) async fn scroll_continue(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    scroll_id: &str,
) -> Result<ScrollBatch, ReconcileError> {
    let url = format!("{}/_search/scroll", config.endpoint.trim_end_matches('/'),);
    let body = serde_json::json!({
        "scroll": SCROLL_KEEP_ALIVE,
        "scroll_id": scroll_id,
    });
    let req = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string());
    let res = req
        .send()
        .await
        .map_err(|err| ReconcileError::Transport(err.to_string()))?;
    parse_scroll_response(res).await
}

async fn parse_scroll_response(response: reqwest::Response) -> Result<ScrollBatch, ReconcileError> {
    let status = response.status();
    if !status.is_success() {
        return Err(bad_status(status, response).await);
    }
    let body: ScrollResponseBody = response
        .json()
        .await
        .map_err(|err| ReconcileError::Decode(err.to_string()))?;
    Ok(ScrollBatch {
        scroll_id: Some(body.scroll_id),
        ids: body.hits.hits.into_iter().map(|h| h.id).collect(),
    })
}

#[derive(Debug, Deserialize)]
struct ScrollResponseBody {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    hits: ScrollHits,
}

#[derive(Debug, Deserialize)]
struct ScrollHits {
    hits: Vec<ScrollHit>,
}

#[derive(Debug, Deserialize)]
struct ScrollHit {
    #[serde(rename = "_id")]
    id: String,
}

pub(crate) async fn clear_scroll(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    scroll_id: &str,
) -> Result<(), ReconcileError> {
    let url = format!("{}/_search/scroll", config.endpoint.trim_end_matches('/'),);
    let body = serde_json::json!({ "scroll_id": [scroll_id] });
    let req = apply_auth(client.delete(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string());
    let res = req
        .send()
        .await
        .map_err(|err| ReconcileError::Transport(err.to_string()))?;
    if !res.status().is_success() {
        return Err(bad_status(res.status(), res).await);
    }
    Ok(())
}

pub(crate) async fn bulk_delete(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    index: &str,
    ids: &[String],
) -> Result<(), ReconcileError> {
    let url = format!("{}/_bulk", config.endpoint.trim_end_matches('/'));
    // NDJSON body: one delete metadata line per ID. Each ID is JSON-
    // escaped via serde_json::to_string so embedded quotes, brackets,
    // and backslashes from the deterministic ID format don't break
    // the parser.
    let mut body = String::with_capacity(ids.len() * 80);
    for id in ids {
        let id_json =
            serde_json::to_string(id).map_err(|err| ReconcileError::Decode(err.to_string()))?;
        body.push_str(&format!(
            "{{\"delete\":{{\"_index\":{index_json},\"_id\":{id_json}}}}}\n",
            index_json = serde_json::to_string(index)
                .map_err(|err| ReconcileError::Decode(err.to_string()))?,
        ));
    }
    let req = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(body);
    let res = req
        .send()
        .await
        .map_err(|err| ReconcileError::Transport(err.to_string()))?;
    let status = res.status();
    if !status.is_success() {
        return Err(bad_status(status, res).await);
    }
    // Per-item failures: we don't fail the whole reconciliation pass
    // for an individual `not_found` (the doc was already gone, which
    // is the goal anyway). We DO surface per-item errors at WARN so
    // operators see real failures (auth changes mid-pass, etc.).
    let body: BulkResponseBody = res
        .json()
        .await
        .map_err(|err| ReconcileError::Decode(err.to_string()))?;
    if body.errors {
        for item in &body.items {
            let Some(del) = &item.delete else { continue };
            if del.status >= 400 && del.status != 404 {
                warn!(
                    id = %del.id,
                    status = del.status,
                    error = ?del.error,
                    "bulk delete item failed during reconciliation"
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct BulkResponseBody {
    #[serde(default)]
    errors: bool,
    #[serde(default)]
    items: Vec<BulkResponseItem>,
}

#[derive(Debug, Deserialize)]
struct BulkResponseItem {
    delete: Option<BulkDeleteResult>,
}

#[derive(Debug, Deserialize)]
struct BulkDeleteResult {
    #[serde(rename = "_id")]
    id: String,
    status: u16,
    #[serde(default)]
    error: serde_json::Value,
}

fn build_client(config: &OpenSearchConfig) -> Result<reqwest::Client, ReconcileError> {
    // Reconcile/reverse-lookup talks to the SAME cluster as the sink, so
    // it needs the same TLS settings. Building a default client here made
    // every lookup fail the handshake on private-CA clusters — and the
    // caller swallows lookup failures as "this row has no parents", so
    // child deletes silently stopped propagating while the sink looked
    // perfectly healthy.
    crate::util::build_http_client(
        config.request_timeout,
        config.verify_tls,
        config.ca_file.as_deref(),
    )
    .map_err(ReconcileError::Client)
}

fn apply_auth(rb: reqwest::RequestBuilder, auth: &AuthMode) -> reqwest::RequestBuilder {
    match auth {
        AuthMode::None => rb,
        AuthMode::Basic { username, password } => rb.basic_auth(username, Some(password)),
        AuthMode::ApiKey(key) => rb.header(header::AUTHORIZATION, format!("ApiKey {key}")),
    }
}

async fn bad_status(status: StatusCode, response: reqwest::Response) -> ReconcileError {
    let body = response.text().await.unwrap_or_default();
    let truncated = if body.len() > 1024 {
        // Slice on a character boundary: `&body[..1024]` panics when byte
        // 1024 lands mid-character, which any proxy error page or a key
        // echoed back with non-ASCII text can produce.
        let cut = body
            .char_indices()
            .nth(1024)
            .map_or(body.len(), |(index, _)| index);
        format!("{}…", &body[..cut])
    } else {
        body
    };
    ReconcileError::BadStatus {
        status: status.as_u16(),
        body: truncated,
    }
}

/// Pull the primary-key text out of a deterministic doc ID. The
/// shape depends on [`DocIdFormat`]:
///
/// - `JsonArray` — expects `{prefix}["{pk}"]`. Parses the suffix as
///   JSON to recover the inner string and skips composite keys
///   (arrays of length ≠ 1) — those are tracked under a v1.1 follow-up.
/// - `RawSuffix` — strips the prefix and returns whatever follows.
///   No parsing, no escaping. The Neo4j source emits raw element
///   IDs (e.g. `4:abc-uuid:42`) so the suffix is itself the lookup key.
///
/// Returns `None` when the ID doesn't match the expected shape — the
/// caller treats that as "not our doc, skip."
/// Render one PK component as the text the source side compares against.
/// A JSON string yields its inner value (no surrounding quotes); any
/// other scalar yields its compact JSON form, which lines up with a
/// `::text` cast on the source column (e.g. `7`, `true`).
fn component_text(v: &serde_json::Value) -> String {
    // Shared with every doc-id producer so the parse can't drift from what the
    // engine / SQL path emit (M3).
    ventstream_core::doc_id::component_text(v)
}

/// Canonical comparison key for a primary key — the single source of
/// truth shared by both sides of reconciliation. The source-side live
/// set and the doc-id parse (`extract_pk_from_doc_id`) must produce
/// identical strings for a row to be recognised as still-present.
///
/// Delegates to [`ventstream_core::doc_id::pk_key`] so the source-side live
/// set, the doc-id parser here, and the doc-id *producers* (in-memory join +
/// SQL denormalize) all share one implementation and cannot drift (M3).
pub fn encode_pk_key(components: &[String]) -> Option<String> {
    ventstream_core::doc_id::pk_key(components)
}

fn extract_pk_from_doc_id(doc_id: &str, prefix: &str, format: DocIdFormat) -> Option<String> {
    let rest = doc_id.strip_prefix(prefix)?;
    match format {
        DocIdFormat::JsonArray => {
            // Doc ids look like `["a"]` (single column) or `["a","b"]`
            // (composite). Parse the array and fold it into the SAME
            // canonical key the source side builds via `encode_pk_key`,
            // so single- and composite-key tables both reconcile.
            let parsed: Vec<serde_json::Value> = serde_json::from_str(rest).ok()?;
            let components: Vec<String> = parsed.iter().map(component_text).collect();
            encode_pk_key(&components)
        }
        DocIdFormat::RawSuffix => {
            // Raw element ID. Empty suffix would mean the prefix
            // matched the whole doc ID — treat as "no key," skip.
            if rest.is_empty() {
                None
            } else {
                Some(rest.to_owned())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// Regression (numeric child PK): the reverse-lookup must query BOTH the
    /// base field and its `.keyword` subfield so a child PK mapped as `long`
    /// (no `.keyword`) still matches. A keyword-only query silently dropped
    /// 1:many child deletes for the common auto-increment-PK case.
    #[test]
    fn reverse_lookup_query_matches_base_and_keyword() {
        let q = reverse_lookup_query("items.id", &["999".to_owned()], 10);
        let should = &q["query"]["bool"]["should"];
        let fields: Vec<&str> = should
            .as_array()
            .expect("should array")
            .iter()
            .filter_map(|c| c["terms"].as_object())
            .filter_map(|m| m.keys().next())
            .map(String::as_str)
            .collect();
        assert!(fields.contains(&"items.id"), "{q}");
        assert!(fields.contains(&"items.id.keyword"), "{q}");
        assert_eq!(q["query"]["bool"]["minimum_should_match"], 1);
    }

    /// Composite child PK: one bool/must per tuple (AND over PK columns), each
    /// column matching base-or-`.keyword`; tuples combined under bool/should.
    #[test]
    fn reverse_lookup_tuples_query_ands_columns_per_tuple() {
        let fields = vec!["items.region".to_owned(), "items.item_id".to_owned()];
        let tuples = vec![vec!["EU".to_owned(), "1000".to_owned()]];
        let q = reverse_lookup_tuples_query(&fields, &tuples, 10);
        let should = q["query"]["bool"]["should"].as_array().expect("should");
        assert_eq!(should.len(), 1);
        let must = should[0]["bool"]["must"].as_array().expect("must");
        assert_eq!(must.len(), 2); // one clause per PK column (AND)
                                   // each column clause matches base OR .keyword
        let col_fields: Vec<&str> = must
            .iter()
            .flat_map(|m| m["bool"]["should"].as_array().expect("col should"))
            .filter_map(|c| c["terms"].as_object())
            .filter_map(|m| m.keys().next())
            .map(String::as_str)
            .collect();
        assert!(col_fields.contains(&"items.region"), "{q}");
        assert!(col_fields.contains(&"items.region.keyword"), "{q}");
        assert!(col_fields.contains(&"items.item_id"), "{q}");
        assert!(col_fields.contains(&"items.item_id.keyword"), "{q}");
    }

    /// Cross-module contract (M3): a doc-id built by the SHARED producer
    /// encoder (`ventstream_core::doc_id::doc_id`, which both the in-memory
    /// engine and the SQL path now call) must parse back through this module's
    /// `extract_pk_from_doc_id` to exactly the source-side `encode_pk_key`.
    /// This is the link the prior tests lacked — they only proved the parser
    /// agreed with itself, not with what the engine actually writes.
    #[test]
    fn producer_doc_id_round_trips_to_source_key() {
        let table = "public.orders";
        let prefix = format!("{table}:");
        for components in [
            vec!["5".to_owned()],                           // single (int rendered as text)
            vec!["abc-123".to_owned()],                     // single (text/uuid)
            vec!["route-1".to_owned(), "evt-1".to_owned()], // composite
        ] {
            let doc_id = ventstream_core::doc_id::doc_id(table, &components);
            let parsed = extract_pk_from_doc_id(&doc_id, &prefix, DocIdFormat::JsonArray);
            let source_key = encode_pk_key(&components);
            assert_eq!(
                parsed, source_key,
                "doc-id {doc_id} must parse to the source key for {components:?}"
            );
        }
    }

    #[test]
    fn extract_pk_single_column() {
        let got = extract_pk_from_doc_id(
            "public.orders:[\"abc-123\"]",
            "public.orders:",
            DocIdFormat::JsonArray,
        );
        assert_eq!(got.as_deref(), Some("abc-123"));
    }

    #[test]
    fn extract_pk_rejects_non_matching_prefix() {
        let got = extract_pk_from_doc_id(
            "public.customers:[\"abc\"]",
            "public.orders:",
            DocIdFormat::JsonArray,
        );
        assert!(got.is_none());
    }

    #[test]
    fn extract_pk_handles_composite_keys() {
        // Composite keys serialise as JSON arrays of length > 1; they now
        // reconcile, keyed on the canonical array form.
        let got = extract_pk_from_doc_id(
            "public.order_lines:[\"route-1\",\"evt-1\"]",
            "public.order_lines:",
            DocIdFormat::JsonArray,
        );
        assert_eq!(got.as_deref(), Some(r#"["route-1","evt-1"]"#));
    }

    #[test]
    fn composite_key_round_trips_between_source_and_doc_id() {
        // The source side (encode_pk_key over ::text components) and the
        // doc-id side (extract_pk_from_doc_id) MUST agree, or a live row
        // looks like an orphan and gets wrongly deleted. Prove it for both
        // arities.
        for components in [
            vec!["route-1".to_string(), "evt-1".to_string()], // composite
            vec!["just-one".to_string()],                     // single
        ] {
            let source_key = encode_pk_key(&components).expect("source key");
            // Build the doc id the engine would emit: `{prefix}{json array}`.
            let arr = serde_json::to_string(&components).unwrap();
            let doc_id = format!("public.order_lines:{arr}");
            let doc_key =
                extract_pk_from_doc_id(&doc_id, "public.order_lines:", DocIdFormat::JsonArray)
                    .expect("doc key");
            assert_eq!(source_key, doc_key, "sides must agree for {components:?}");
        }
    }

    #[test]
    fn extract_pk_composite_with_embedded_quotes_round_trips() {
        let components = vec!["a\"b".to_string(), "c".to_string()];
        let arr = serde_json::to_string(&components).unwrap();
        let doc_id = format!("t.x:{arr}");
        let doc_key = extract_pk_from_doc_id(&doc_id, "t.x:", DocIdFormat::JsonArray).unwrap();
        assert_eq!(doc_key, encode_pk_key(&components).unwrap());
    }

    #[test]
    fn extract_pk_handles_embedded_quotes() {
        let got = extract_pk_from_doc_id(
            "public.orders:[\"abc\\\"def\"]",
            "public.orders:",
            DocIdFormat::JsonArray,
        );
        assert_eq!(got.as_deref(), Some("abc\"def"));
    }

    #[test]
    fn extract_pk_raw_suffix_neo4j_format() {
        // Neo4j element IDs include colons; the extractor returns
        // whatever follows the prefix as a single string.
        let got = extract_pk_from_doc_id(
            "users_test:4:c85c29e1-1fc4-40cf-ad59-f34e67e75c23:280040",
            "users_test:",
            DocIdFormat::RawSuffix,
        );
        assert_eq!(
            got.as_deref(),
            Some("4:c85c29e1-1fc4-40cf-ad59-f34e67e75c23:280040")
        );
    }

    #[test]
    fn extract_pk_raw_suffix_empty_after_prefix_is_none() {
        let got = extract_pk_from_doc_id("users_test:", "users_test:", DocIdFormat::RawSuffix);
        assert!(got.is_none());
    }

    #[test]
    fn extract_pk_raw_suffix_rejects_non_matching_prefix() {
        let got = extract_pk_from_doc_id(
            "parties_denormalized:4:abc:1",
            "users_test:",
            DocIdFormat::RawSuffix,
        );
        assert!(got.is_none());
    }

    // C2: empty valid-PK set must be refused (would wipe the whole index).
    #[test]
    fn guard_refuses_empty_valid_pks() {
        let r = check_delete_guard(0, 1000, 1000, false);
        assert!(r.is_err(), "empty valid_pks must be refused");
    }

    // C2: deleting more than the cap must be refused.
    #[test]
    fn guard_refuses_over_ratio() {
        // 600/1000 = 60% > 50% cap.
        let r = check_delete_guard(400, 1000, 600, false);
        assert!(r.is_err(), "over-cap deletion must be refused");
    }

    // C2: a normal small orphan set proceeds.
    #[test]
    fn guard_allows_small_orphan_set() {
        // 50/1000 = 5%.
        let r = check_delete_guard(950, 1000, 50, false);
        assert!(r.is_ok(), "a small orphan fraction must proceed");
    }

    // C2: the override bypasses both the empty floor and the ratio cap.
    #[test]
    fn guard_override_bypasses_everything() {
        assert!(check_delete_guard(0, 1000, 1000, true).is_ok());
        assert!(check_delete_guard(1, 1000, 999, true).is_ok());
    }

    // C2: exactly at the cap is allowed; just over is refused.
    #[test]
    fn guard_boundary() {
        assert!(
            check_delete_guard(500, 1000, 500, false).is_ok(),
            "50% (== cap) allowed"
        );
        assert!(
            check_delete_guard(499, 1000, 501, false).is_err(),
            "just over cap refused"
        );
    }
}
