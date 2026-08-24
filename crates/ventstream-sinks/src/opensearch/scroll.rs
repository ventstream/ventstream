//! Scroll and bulk-delete primitives shared by the sink's TRUNCATE clear
//! and the reconcile maintenance pass.
//!
//! Both need the same shape — walk an index's document ids, delete a
//! filtered subset — but they answer to different contracts. Reconcile is a
//! best-effort sweep where a stray `not_found` is the goal; a truncate must
//! clear the relation completely or fail. So these primitives report what
//! happened and leave the policy to the caller: `bulk_delete` returns the
//! per-item failures rather than deciding whether they matter.
//!
//! Keeping them here rather than in either caller means neither module
//! depends on the other, and the tuning below belongs to the transport
//! rather than to one workload's expectations.

use reqwest::{header, StatusCode};
use serde::Deserialize;

use super::config::{AuthMode, OpenSearchConfig};

/// How long the server holds a scroll context between pages.
pub(crate) const SCROLL_KEEP_ALIVE: &str = "2m";
/// Document ids fetched per page. Ids only, so a page stays small.
pub(crate) const SCROLL_BATCH_SIZE: usize = 1_000;

/// A failure from one of these primitives. Callers map it onto their own
/// error type and choose the retry classification.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ScrollError {
    #[error("opensearch transport error: {0}")]
    Transport(String),
    #[error("opensearch responded with {status}: {body}")]
    BadStatus {
        /// HTTP status code.
        status: u16,
        /// First ~1KB of the response body.
        body: String,
    },
    #[error("opensearch returned malformed JSON: {0}")]
    Decode(String),
}

/// One page of a scroll.
pub(crate) struct ScrollBatch {
    /// Cursor for the next page.
    pub(crate) scroll_id: Option<String>,
    /// Document ids in this page.
    pub(crate) ids: Vec<String>,
}

/// One document that the server refused to delete.
pub(crate) struct FailedDelete {
    /// Document id.
    pub(crate) id: String,
    /// Per-item HTTP status.
    pub(crate) status: u16,
    /// Reason text, when the server supplied one.
    pub(crate) reason: Option<String>,
}

pub(crate) fn apply_auth(rb: reqwest::RequestBuilder, auth: &AuthMode) -> reqwest::RequestBuilder {
    match auth {
        AuthMode::None => rb,
        AuthMode::Basic { username, password } => rb.basic_auth(username, Some(password)),
        AuthMode::ApiKey(key) => rb.header(header::AUTHORIZATION, format!("ApiKey {key}")),
    }
}

/// Make prior writes visible to search.
///
/// A scroll reads the search view, which lags indexing until a refresh. A
/// clear that scans without refreshing can miss documents written moments
/// earlier and report success having deleted nothing.
pub(crate) async fn refresh(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    index: &str,
) -> Result<(), ScrollError> {
    let url = format!(
        "{}/{}/_refresh",
        config.endpoint.trim_end_matches('/'),
        index
    );
    let res = apply_auth(client.post(&url), &config.auth)
        .send()
        .await
        .map_err(|err| ScrollError::Transport(err.to_string()))?;
    let status = res.status();
    if !status.is_success() {
        return Err(bad_status(status, res).await);
    }
    Ok(())
}

/// Open a scroll over every document id in `index`.
///
/// The query is `match_all` by necessity: `_id` is a metadata field and
/// OpenSearch rejects prefix queries on it, so callers that want a subset
/// filter the returned ids themselves.
pub(crate) async fn scroll_open(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    index: &str,
) -> Result<ScrollBatch, ScrollError> {
    let url = format!(
        "{}/{}/_search?scroll={}",
        config.endpoint.trim_end_matches('/'),
        index,
        SCROLL_KEEP_ALIVE,
    );
    let body = serde_json::json!({
        "size": SCROLL_BATCH_SIZE,
        "_source": false,
        "query": { "match_all": {} },
        // _doc is the cheapest stable sort across pages.
        "sort": ["_doc"],
    });
    let res = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|err| ScrollError::Transport(err.to_string()))?;
    parse_scroll_response(res).await
}

/// Fetch the next page of an open scroll.
pub(crate) async fn scroll_continue(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    scroll_id: &str,
) -> Result<ScrollBatch, ScrollError> {
    let url = format!("{}/_search/scroll", config.endpoint.trim_end_matches('/'));
    let body = serde_json::json!({
        "scroll": SCROLL_KEEP_ALIVE,
        "scroll_id": scroll_id,
    });
    let res = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|err| ScrollError::Transport(err.to_string()))?;
    parse_scroll_response(res).await
}

/// Release a scroll context. Best effort: an orphan expires on its own.
pub(crate) async fn clear_scroll(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    scroll_id: &str,
) -> Result<(), ScrollError> {
    let url = format!("{}/_search/scroll", config.endpoint.trim_end_matches('/'));
    let body = serde_json::json!({ "scroll_id": [scroll_id] });
    let res = apply_auth(client.delete(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .map_err(|err| ScrollError::Transport(err.to_string()))?;
    let status = res.status();
    if !status.is_success() {
        return Err(bad_status(status, res).await);
    }
    Ok(())
}

/// Delete `ids` from `index`, reporting per-item failures rather than
/// judging them.
///
/// A `not_found` means someone else already removed the document. That is
/// the goal for a reconcile sweep and harmless for a truncate, so it is
/// never reported. Everything else is returned for the caller to weigh.
pub(crate) async fn bulk_delete(
    client: &reqwest::Client,
    config: &OpenSearchConfig,
    index: &str,
    ids: &[String],
) -> Result<Vec<FailedDelete>, ScrollError> {
    let url = format!("{}/_bulk", config.endpoint.trim_end_matches('/'));
    let index_json =
        serde_json::to_string(index).map_err(|err| ScrollError::Decode(err.to_string()))?;
    // NDJSON: one delete action per id. Ids are JSON-escaped so quotes and
    // brackets from the deterministic id format cannot break the parser.
    let mut body = String::with_capacity(ids.len() * 80);
    for id in ids {
        let id_json =
            serde_json::to_string(id).map_err(|err| ScrollError::Decode(err.to_string()))?;
        body.push_str(&format!(
            "{{\"delete\":{{\"_index\":{index_json},\"_id\":{id_json}}}}}\n"
        ));
    }
    let res = apply_auth(client.post(&url), &config.auth)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(body)
        .send()
        .await
        .map_err(|err| ScrollError::Transport(err.to_string()))?;
    let status = res.status();
    if !status.is_success() {
        return Err(bad_status(status, res).await);
    }
    let parsed: BulkResponseBody = res
        .json()
        .await
        .map_err(|err| ScrollError::Decode(err.to_string()))?;
    if !parsed.errors {
        return Ok(Vec::new());
    }
    Ok(parsed
        .items
        .into_iter()
        .filter_map(|item| item.delete)
        .filter(|del| del.status >= 400 && del.status != 404)
        .map(|del| FailedDelete {
            id: del.id,
            status: del.status,
            reason: del.error.map(|e| e.reason),
        })
        .collect())
}

async fn parse_scroll_response(response: reqwest::Response) -> Result<ScrollBatch, ScrollError> {
    let status = response.status();
    if !status.is_success() {
        return Err(bad_status(status, response).await);
    }
    let body: ScrollResponseBody = response
        .json()
        .await
        .map_err(|err| ScrollError::Decode(err.to_string()))?;
    Ok(ScrollBatch {
        scroll_id: Some(body.scroll_id),
        ids: body.hits.hits.into_iter().map(|h| h.id).collect(),
    })
}

pub(crate) async fn bad_status(status: StatusCode, response: reqwest::Response) -> ScrollError {
    let body = response.text().await.unwrap_or_default();
    let truncated = if body.len() > 1024 {
        // Slice on a character boundary: `&body[..1024]` panics when byte
        // 1024 lands mid-character.
        let cut = body
            .char_indices()
            .nth(1024)
            .map_or(body.len(), |(index, _)| index);
        format!("{}…", &body[..cut])
    } else {
        body
    };
    ScrollError::BadStatus {
        status: status.as_u16(),
        body: truncated,
    }
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

#[derive(Debug, Deserialize)]
struct BulkResponseBody {
    errors: bool,
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
    error: Option<BulkItemError>,
}

#[derive(Debug, Deserialize)]
struct BulkItemError {
    reason: String,
}
