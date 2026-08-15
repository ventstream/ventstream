//! Reverse lookup against a Meilisearch index: find parent docs embedding a
//! deleted 1:many child's PK, so the denormalizer can recompose them when
//! the source delete image lacks the parent key.
//!
//! Mirrors the OpenSearch `OsReverseLookup` contract: batched terms lookup,
//! composite-tuple variant, canonical `{table}:[...]` ids returned (read
//! from the `_vs_id` field the writer stamps on every document).
//!
//! Meilisearch specifics: filtering requires the queried fields to be in
//! `filterableAttributes`, so the first lookup against an index lazily
//! merges the needed fields into the index settings and waits for the
//! settings task (a one-time reindex cost, logged). Filter values match
//! type-sensitively, so each comparison ORs the raw string with a numeric
//! literal when the value parses as a number.

use std::collections::HashSet;

use reqwest::StatusCode;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use ventstream_core::Event;

use super::config::MeilisearchConfig;
use super::documents::{resolve_index, CANONICAL_ID_FIELD};
use super::sink::build_http_client;

/// Cap on parents returned per lookup — matches the OpenSearch limit's
/// intent: a lookup is a candidate-selection step, not a full scan.
const REVERSE_LOOKUP_MAX_HITS: usize = 1_000;

/// Pooled reverse-lookup client for one Meilisearch sink config.
pub struct MeiliReverseLookup {
    config: MeilisearchConfig,
    client: reqwest::Client,
    base_url: String,
    /// Indexes whose filterable attributes already include the fields we
    /// query, keyed by `{index}::{sorted fields}` to re-ensure on change.
    ensured: Mutex<HashSet<String>>,
}

impl MeiliReverseLookup {
    /// Build the pooled client once from the sink config.
    pub fn new(config: MeilisearchConfig) -> Result<Self, String> {
        let client = build_http_client(&config).map_err(|err| err.to_string())?;
        let base_url = config.endpoint.trim_end_matches('/').to_owned();
        Ok(Self {
            config,
            client,
            base_url,
            ensured: Mutex::new(HashSet::new()),
        })
    }

    /// Resolve the concrete index UID a probe event's documents land in —
    /// the same routing the writer uses, prefix and encoding included.
    pub fn resolve_index_for(&self, probe: &Event) -> Option<String> {
        resolve_index(&self.config, probe).ok()
    }

    /// Find canonical parent doc ids embedding any of `values` at `field`.
    pub async fn doc_ids_by_terms(
        &self,
        index: &str,
        field: &str,
        values: &[String],
    ) -> Result<Vec<String>, String> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_filterable(index, &[field.to_owned()]).await?;
        let clauses: Vec<String> = values.iter().map(|v| comparison(field, v)).collect();
        self.search_ids(index, &clauses.join(" OR ")).await
    }

    /// Composite-child-PK variant: parents embedding a child whose PK
    /// columns equal one of `tuples` (fields align with tuple components).
    pub async fn doc_ids_by_term_tuples(
        &self,
        index: &str,
        fields: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<String>, String> {
        if fields.is_empty() || tuples.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_filterable(index, fields).await?;
        let clauses: Vec<String> = tuples
            .iter()
            .map(|tuple| {
                let ands: Vec<String> = fields
                    .iter()
                    .zip(tuple.iter())
                    .map(|(f, v)| comparison(f, v))
                    .collect();
                format!("({})", ands.join(" AND "))
            })
            .collect();
        self.search_ids(index, &clauses.join(" OR ")).await
    }

    async fn search_ids(&self, index: &str, filter: &str) -> Result<Vec<String>, String> {
        let url = format!("{}/indexes/{}/search", self.base_url, index);
        let body = serde_json::json!({
            "q": "",
            "filter": filter,
            "limit": REVERSE_LOOKUP_MAX_HITS,
            "attributesToRetrieve": [CANONICAL_ID_FIELD],
        });
        let mut request = self.client.post(&url).json(&body);
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        let response = request.send().await.map_err(|err| err.to_string())?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            // Index not created yet — nothing can embed the child.
            return Ok(Vec::new());
        }
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(format!("reverse lookup search HTTP {status}: {text}"));
        }
        let parsed: serde_json::Value = response.json().await.map_err(|err| err.to_string())?;
        let hits = parsed
            .get("hits")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut ids = Vec::with_capacity(hits.len());
        for hit in &hits {
            if let Some(id) = hit
                .get(CANONICAL_ID_FIELD)
                .and_then(serde_json::Value::as_str)
            {
                ids.push(id.to_owned());
            }
        }
        if hits.len() >= REVERSE_LOOKUP_MAX_HITS {
            warn!(
                index,
                limit = REVERSE_LOOKUP_MAX_HITS,
                "meili reverse lookup hit the result cap; some parents may recompose late"
            );
        }
        debug!(index, parents = ids.len(), "meili reverse lookup resolved");
        Ok(ids)
    }

    /// Merge `fields` into the index's filterable attributes if missing and
    /// wait for the settings task. One-time per (index, fields) per process.
    async fn ensure_filterable(&self, index: &str, fields: &[String]) -> Result<(), String> {
        let mut sorted: Vec<&str> = fields.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        let cache_key = format!("{index}::{}", sorted.join(","));
        {
            let ensured = self.ensured.lock().await;
            if ensured.contains(&cache_key) {
                return Ok(());
            }
        }
        let url = format!(
            "{}/indexes/{}/settings/filterable-attributes",
            self.base_url, index
        );
        let mut request = self.client.get(&url);
        if let Some(key) = &self.config.api_key {
            request = request.bearer_auth(key);
        }
        let response = request.send().await.map_err(|err| err.to_string())?;
        if response.status() == StatusCode::NOT_FOUND {
            // Index not created yet; nothing to ensure (and nothing to find).
            return Ok(());
        }
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(format!("read filterable attributes HTTP {status}: {text}"));
        }
        let current: Vec<String> = response.json().await.map_err(|err| err.to_string())?;
        let mut merged: Vec<String> = current.clone();
        let mut missing = false;
        for field in fields {
            if !merged.iter().any(|f| f == field) {
                merged.push(field.clone());
                missing = true;
            }
        }
        if missing {
            info!(
                index,
                fields = ?fields,
                "meili reverse lookup: adding child-PK fields to filterableAttributes \
                 (one-time settings update; large indexes reindex in the background)"
            );
            let mut put = self.client.put(&url).json(&merged);
            if let Some(key) = &self.config.api_key {
                put = put.bearer_auth(key);
            }
            let response = put.send().await.map_err(|err| err.to_string())?;
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(format!(
                    "update filterable attributes HTTP {status}: {text}"
                ));
            }
            let parsed: serde_json::Value = response.json().await.map_err(|err| err.to_string())?;
            if let Some(task_uid) = parsed.get("taskUid").and_then(serde_json::Value::as_u64) {
                self.wait_task(task_uid).await?;
            }
        }
        self.ensured.lock().await.insert(cache_key);
        Ok(())
    }

    async fn wait_task(&self, task_uid: u64) -> Result<(), String> {
        let url = format!("{}/tasks/{task_uid}", self.base_url);
        let deadline = std::time::Instant::now() + self.config.task.deadline;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(format!("settings task {task_uid} timed out"));
            }
            let mut request = self.client.get(&url);
            if let Some(key) = &self.config.api_key {
                request = request.bearer_auth(key);
            }
            let response = request.send().await.map_err(|err| err.to_string())?;
            let parsed: serde_json::Value = response.json().await.map_err(|err| err.to_string())?;
            match parsed.get("status").and_then(serde_json::Value::as_str) {
                Some("succeeded") => return Ok(()),
                Some("failed") | Some("canceled") => {
                    return Err(format!("settings task {task_uid} did not succeed"));
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
    }
}

/// One field comparison that matches both the numeric and string form of a
/// value — embedded documents keep source JSON types, and Meilisearch
/// filters are type-sensitive.
fn comparison(field: &str, value: &str) -> String {
    let quoted = format!("{field} = {:?}", value);
    if value.parse::<f64>().is_ok() {
        format!("({field} = {value} OR {quoted})")
    } else {
        quoted
    }
}

#[cfg(test)]
mod tests {
    use super::comparison;

    #[test]
    fn comparison_covers_numeric_and_string_forms() {
        assert_eq!(
            comparison("items.id", "42"),
            "(items.id = 42 OR items.id = \"42\")"
        );
        assert_eq!(comparison("items.sku", "SKU-1"), "items.sku = \"SKU-1\"");
    }
}
