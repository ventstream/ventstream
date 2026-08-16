//! The [`ventstream_core::Sink`] implementation for OpenSearch / Elasticsearch.
//!
//! Hands one or more size-bounded bulk requests per call to
//! [`write`](OpenSearchSink::write). Retries are driven by
//! [`BackoffSchedule`]; per-request transport is
//! [`reqwest::Client`] (connection pool, gzip request compression, native
//! roots TLS).
//!
//! ### Error classification
//!
//! - Network / DNS / timeout → [`OpenSearchSinkError::Transport`] →
//!   retried with backoff.
//! - HTTP 5xx → [`OpenSearchSinkError::Server`] → retried with backoff.
//! - HTTP 429 → [`OpenSearchSinkError::RateLimited`] → retried with
//!   backoff (caller may wish to use longer delays for this; the current
//!   policy treats it identically to 5xx for simplicity).
//! - HTTP 401 / 403 → [`OpenSearchSinkError::Auth`] → not retried; the
//!   engine should surface this to the operator immediately.
//! - HTTP 4xx other → [`OpenSearchSinkError::Client`] → not retried; the
//!   whole request is blocked without advancing source progress.
//! - Bulk response with `errors: true` → per-item split: transient items
//!   (429 / 5xx) are retried as a shrinking subset (already-acked items are
//!   never re-sent); only known permanent document errors surface as
//!   [`OpenSearchSinkError::PartialFailure`] carrying their original-batch
//!   offsets so the engine can DLQ them. Unknown item failures and transient
//!   exhaustion in an explicitly bounded diagnostic policy fail the batch
//!   closed.
//!
//! ### What the sink does NOT do
//!
//! - Throughput batching: the engine's dispatcher builds the [`SinkBatch`].
//!   The sink only bisects a batch when its exact NDJSON body exceeds the
//!   configured protocol limit.
//! - DLQ writes: the engine handles exact permanent item failures reported by
//!   the sink.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use rand::Rng;
use reqwest::{header, StatusCode};
use tracing::{debug, warn};
use ventstream_core::{
    Sink, SinkBatch, SinkError, SinkFailureGuard, SinkHealth, SinkHealthSnapshot,
};

use super::bulk::{self, BulkResponse};
use super::config::{AuthMode, OpenSearchConfig};
use super::retry::BackoffSchedule;
use crate::error::OpenSearchSinkError;

/// Sink adapter for OpenSearch / Elasticsearch via the Bulk API.
pub struct OpenSearchSink {
    config: OpenSearchConfig,
    client: reqwest::Client,
    bulk_url: String,
    adaptive: AdaptiveConcurrency,
    delivery_health: SinkHealth,
}

struct AdaptiveConcurrency {
    current: AtomicUsize,
    observed_ceiling: AtomicUsize,
    success_streak: AtomicUsize,
    latency_ewma_micros: AtomicU64,
    target_latency_micros: u64,
}

impl AdaptiveConcurrency {
    fn new(request_timeout: Duration) -> Self {
        let target_latency_micros = u64::try_from(request_timeout.as_micros() / 4)
            .unwrap_or(u64::MAX)
            .clamp(100_000, 2_000_000);
        Self {
            current: AtomicUsize::new(4),
            observed_ceiling: AtomicUsize::new(1),
            success_streak: AtomicUsize::new(0),
            latency_ewma_micros: AtomicU64::new(0),
            target_latency_micros,
        }
    }

    fn desired(&self, configured_ceiling: usize) -> usize {
        let ceiling = configured_ceiling.max(1);
        self.observed_ceiling.fetch_max(ceiling, Ordering::Relaxed);
        self.current.load(Ordering::Relaxed).clamp(1, ceiling)
    }

    fn on_success(&self, elapsed: Duration) {
        let sample = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let previous = self.latency_ewma_micros.load(Ordering::Relaxed);
        let ewma = if previous == 0 {
            sample
        } else {
            previous.saturating_mul(7).saturating_add(sample) / 8
        };
        self.latency_ewma_micros.store(ewma, Ordering::Relaxed);
        let streak = self.success_streak.fetch_add(1, Ordering::Relaxed) + 1;
        if !streak.is_multiple_of(16) {
            return;
        }

        let current = self.current.load(Ordering::Relaxed).max(1);
        let ceiling = self.observed_ceiling.load(Ordering::Relaxed).max(1);
        let desired = if ewma <= self.target_latency_micros && current < ceiling {
            current + 1
        } else if ewma > self.target_latency_micros.saturating_mul(2) && current > 1 {
            current - 1
        } else {
            current
        };
        if desired != current
            && self
                .current
                .compare_exchange(current, desired, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            debug!(
                previous = current,
                desired,
                latency_ewma_ms = ewma / 1_000,
                metric = "opensearch.adaptive_concurrency",
                "OpenSearch adaptive concurrency adjusted after successful requests"
            );
        }
    }

    fn on_pressure(&self, reason: &'static str) {
        self.success_streak.store(0, Ordering::Relaxed);
        let previous = self.current.load(Ordering::Relaxed).max(1);
        let desired = (previous / 2).max(1);
        if desired != previous
            && self
                .current
                .compare_exchange(previous, desired, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            warn!(
                previous,
                desired,
                reason,
                metric = "opensearch.adaptive_concurrency",
                "OpenSearch adaptive concurrency reduced under pressure"
            );
        }
    }
}

impl OpenSearchSink {
    /// Construct a sink. Builds the HTTP client eagerly so connection
    /// pooling kicks in on the first call to [`write`](Self::write).
    pub fn new(config: OpenSearchConfig) -> Result<Self, OpenSearchSinkError> {
        let client = build_http_client(&config)?;

        let bulk_url = format!("{}/_bulk", config.endpoint.trim_end_matches('/'));
        let adaptive = AdaptiveConcurrency::new(config.request_timeout);
        let delivery_health = config.delivery_health.clone().unwrap_or_default();
        Ok(Self {
            config,
            client,
            bulk_url,
            adaptive,
            delivery_health,
        })
    }

    /// Send a dispatcher batch, bisecting only when the exact encoded body
    /// exceeds OpenSearch's configured request limit.
    ///
    /// Successful chunks are never re-sent. Failures retain offsets into the
    /// original dispatcher batch so DLQ routing remains precise.
    async fn send_with_retry(&self, batch: &SinkBatch) -> Result<(), OpenSearchSinkError> {
        let events = batch.events();
        if events.is_empty() {
            return Ok(());
        }

        let mut ranges = VecDeque::new();
        ranges.push_back(0..events.len());
        let mut permanent: Vec<ventstream_core::FailedItem> = Vec::new();
        while let Some(range) = ranges.pop_front() {
            let Some(attempt) = events.get(range.clone()) else {
                return Err(OpenSearchSinkError::Internal(
                    "bulk split produced an invalid event range".into(),
                ));
            };
            match self.send_slice_with_retry(attempt).await {
                Ok(()) => {}
                Err(OpenSearchSinkError::RequestTooLarge { .. }) if range.len() > 1 => {
                    let midpoint = range.start + range.len() / 2;
                    // Process the left half first while retaining the right half
                    // at the front. This preserves source order at concurrency=1.
                    ranges.push_front(midpoint..range.end);
                    ranges.push_front(range.start..midpoint);
                }
                Err(err @ OpenSearchSinkError::RequestTooLarge { .. }) => {
                    permanent.push(ventstream_core::FailedItem {
                        offset: range.start,
                        error: err.to_string(),
                    });
                }
                Err(OpenSearchSinkError::PartialFailure { failed_items, .. }) => {
                    for item in failed_items {
                        if item.offset < range.len() {
                            permanent.push(ventstream_core::FailedItem {
                                offset: range.start + item.offset,
                                error: item.error,
                            });
                        }
                    }
                }
                Err(err) => {
                    // A whole-request failure after one or more split chunks
                    // succeeded is still not evidence that the remaining
                    // events are poison. Fail the enclosing dispatcher batch
                    // closed; stable document IDs and external versions make
                    // replay of the successful prefix safe.
                    return Err(err);
                }
            }
        }

        if permanent.is_empty() {
            return Ok(());
        }
        permanent.sort_unstable_by_key(|item| item.offset);
        let sample_error = permanent
            .first()
            .map(|item| item.error.clone())
            .unwrap_or_else(|| "unknown".into());
        Err(OpenSearchSinkError::PartialFailure {
            batch_size: events.len(),
            failed_count: permanent.len(),
            sample_error,
            failed_items: permanent,
        })
    }

    /// Send one size-valid event slice with retry.
    async fn send_slice_with_retry(
        &self,
        events: &[ventstream_core::Event],
    ) -> Result<(), OpenSearchSinkError> {
        if events.is_empty() {
            return Ok(());
        }

        let mut schedule = BackoffSchedule::new(self.config.retry);

        // Offsets (into the original `events`) that still need sending. Starts
        // as the whole batch; a partial transient failure shrinks it to just
        // the retryable subset so already-acked items are never re-sent.
        let mut pending: Vec<usize> = (0..events.len()).collect();
        // Permanent per-item failures accumulate here and surface as a single
        // PartialFailure (→ DLQ) once no transient work remains.
        let mut permanent: Vec<ventstream_core::FailedItem> = Vec::new();
        // Materialized subset for retry attempts; the first attempt borrows
        // the original slice to avoid cloning the whole batch on the hot path.
        let mut subset: Vec<ventstream_core::Event> = Vec::new();
        let mut use_subset = false;
        let rendered_at = Utc::now();
        let mut encoded: Option<(Bytes, Vec<usize>)> = None;
        let mut transient_failure: Option<SinkFailureGuard> = None;

        loop {
            let attempt: &[ventstream_core::Event] = if use_subset {
                subset.as_slice()
            } else {
                events
            };
            if encoded.is_none() {
                encoded = Some(self.encode_attempt(attempt, rendered_at)?);
            }
            let Some((body, item_event_offsets)) = encoded.as_ref().cloned() else {
                return Err(OpenSearchSinkError::Internal(
                    "bulk request body was not prepared".into(),
                ));
            };
            let request_started = Instant::now();
            match self.send_once(body, &item_event_offsets, attempt).await {
                Ok(SendOutcome::AllOk) => {
                    self.adaptive.on_success(request_started.elapsed());
                    self.finish_transient_failure(&mut transient_failure);
                    break;
                }
                Ok(SendOutcome::PerItem {
                    transient,
                    permanent: perm,
                    blocked,
                }) => {
                    if !blocked.is_empty() {
                        let sample_error = blocked
                            .first()
                            .map(|item| item.error.clone())
                            .unwrap_or_else(|| "unknown blocked bulk item".to_owned());
                        return Err(OpenSearchSinkError::BlockedItems {
                            failed_count: blocked.len(),
                            sample_error,
                        });
                    }
                    if transient.is_empty() {
                        self.adaptive.on_success(request_started.elapsed());
                        self.finish_transient_failure(&mut transient_failure);
                    } else {
                        self.adaptive.on_pressure("bulk_item_rejection");
                        self.begin_transient_failure(
                            &mut transient_failure,
                            "OpenSearch rejected bulk items with a transient status",
                        );
                    }
                    // Map attempt-relative offsets back to original-batch
                    // offsets so the DLQ targets the right events.
                    for item in perm {
                        if let Some(&orig) = pending.get(item.offset) {
                            permanent.push(ventstream_core::FailedItem {
                                offset: orig,
                                error: item.error,
                            });
                        }
                    }
                    let mut next_offsets: Vec<usize> = Vec::new();
                    let mut next_failed: Vec<ventstream_core::FailedItem> = Vec::new();
                    for item in transient {
                        if let Some(&orig) = pending.get(item.offset) {
                            next_offsets.push(orig);
                            next_failed.push(ventstream_core::FailedItem {
                                offset: orig,
                                error: item.error,
                            });
                        }
                    }
                    if next_offsets.is_empty() {
                        break;
                    }
                    if let Some(delay) = schedule.next() {
                        ventstream_telemetry::bump_sink_retries(
                            u64::try_from(next_offsets.len()).unwrap_or(u64::MAX),
                        );
                        debug!(
                            sink_id = %self.config.id,
                            attempt = schedule.attempts_so_far(),
                            retry_items = next_offsets.len(),
                            ?delay,
                            "partial bulk failure; retrying transient subset after backoff"
                        );
                        tokio::time::sleep(jittered_delay(delay)).await;
                        subset = next_offsets
                            .iter()
                            .filter_map(|&i| events.get(i).cloned())
                            .collect();
                        // Offsets are always in range, so the filter_map never
                        // drops; assert it so a future regression can't silently
                        // desync `subset` from `pending` and corrupt remapping.
                        debug_assert_eq!(
                            subset.len(),
                            next_offsets.len(),
                            "every pending offset must index a valid event"
                        );
                        pending = next_offsets;
                        use_subset = true;
                        encoded = None;
                        continue;
                    }
                    let sample_error = next_failed
                        .first()
                        .map(|item| item.error.clone())
                        .unwrap_or_else(|| "unknown transient item failure".to_owned());
                    warn!(
                        sink_id = %self.config.id,
                        retry_items = next_offsets.len(),
                        "partial bulk transient retry budget exhausted; failing closed"
                    );
                    return Err(OpenSearchSinkError::TransientItems {
                        failed_count: next_offsets.len(),
                        sample_error,
                    });
                }
                Err(err) if err.is_retryable() => {
                    self.adaptive.on_pressure(pressure_reason(&err));
                    self.begin_transient_failure(&mut transient_failure, err.to_string());
                    // Whole-attempt transient (transport / 5xx / 429 at the
                    // HTTP layer) — retry the same attempt (full or subset).
                    if let Some(base_delay) = schedule.next() {
                        let delay = err
                            .retry_after()
                            .unwrap_or_else(|| jittered_delay(base_delay))
                            .min(self.config.retry.max_backoff);
                        ventstream_telemetry::bump_sink_retries(
                            u64::try_from(attempt.len()).unwrap_or(u64::MAX),
                        );
                        debug!(
                            sink_id = %self.config.id,
                            attempt = schedule.attempts_so_far(),
                            ?delay,
                            error = %err,
                            "bulk request failed; retrying after backoff"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    warn!(
                        sink_id = %self.config.id,
                        error = %err,
                        "bulk request retries exhausted"
                    );
                    return Err(err);
                }
                Err(err) => {
                    // Whole-request authentication, configuration, and
                    // protocol failures are deployment-level blockers, not
                    // event-level poison. Fail closed even when an earlier
                    // attempt already partitioned this batch.
                    return Err(err);
                }
            }
        }

        if permanent.is_empty() {
            return Ok(());
        }
        let failed_count = permanent.len();
        let sample_error = permanent
            .first()
            .map(|f| f.error.clone())
            .unwrap_or_else(|| "unknown".into());
        Err(OpenSearchSinkError::PartialFailure {
            batch_size: events.len(),
            failed_count,
            sample_error,
            failed_items: permanent,
        })
    }

    fn begin_transient_failure(
        &self,
        current: &mut Option<SinkFailureGuard>,
        reason: impl Into<String>,
    ) {
        if current.is_none() {
            *current = Some(self.delivery_health.begin_transient_failure(reason));
            ventstream_telemetry::mark_sink_unavailable();
        }
    }

    fn finish_transient_failure(&self, current: &mut Option<SinkFailureGuard>) {
        current.take();
        if matches!(self.delivery_health.snapshot(), SinkHealthSnapshot::Healthy) {
            ventstream_telemetry::mark_sink_available();
        }
    }

    fn mark_blocked(&self, error: &OpenSearchSinkError) {
        self.delivery_health.mark_blocked(error.to_string());
        ventstream_telemetry::mark_sink_unavailable();
    }

    fn encode_attempt(
        &self,
        events: &[ventstream_core::Event],
        rendered_at: chrono::DateTime<Utc>,
    ) -> Result<(Bytes, Vec<usize>), OpenSearchSinkError> {
        let (body, item_event_offsets) =
            bulk::build_bulk_body(events, &self.config.index_template, rendered_at)?;
        if body.len() > self.config.bulk.max_bytes {
            return Err(OpenSearchSinkError::RequestTooLarge {
                actual_bytes: body.len(),
                max_bytes: self.config.bulk.max_bytes,
            });
        }
        Ok((Bytes::from(body), item_event_offsets))
    }

    /// Single prepared attempt — POST immutable NDJSON, parse response, and
    /// classify. `Bytes` cloning is O(1), so whole-request retries do not
    /// rebuild or copy the body.
    async fn send_once(
        &self,
        body: Bytes,
        item_event_offsets: &[usize],
        attempt_events: &[ventstream_core::Event],
    ) -> Result<SendOutcome, OpenSearchSinkError> {
        let mut http = self
            .client
            .post(&self.bulk_url)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .body(body);

        http = apply_auth(http, &self.config.auth);

        let response = http
            .send()
            .await
            .map_err(|err| OpenSearchSinkError::Transport(err.to_string()))?;

        classify_response(response, item_event_offsets, attempt_events).await
    }
}

fn pressure_reason(error: &OpenSearchSinkError) -> &'static str {
    match error {
        OpenSearchSinkError::RateLimited { .. } => "http_429",
        OpenSearchSinkError::Server { .. } => "http_5xx",
        OpenSearchSinkError::Transport(_) => "transport",
        _ => "retryable",
    }
}

fn jittered_delay(delay: Duration) -> Duration {
    let factor = rand::thread_rng().gen_range(0.5..=1.0);
    delay.mul_f64(factor)
}

/// Outcome of a single bulk attempt that returned an HTTP 2xx.
///
/// A 2xx with `errors: true` is *not* a whole-batch failure — individual
/// items can fail for different reasons. We split them so the caller can
/// retry the transient subset without re-sending the items that already
/// succeeded, route only known permanent document failures to the DLQ, and
/// block delivery for unknown failures.
#[derive(Debug)]
enum SendOutcome {
    /// Every item in the attempt was accepted.
    AllOk,
    /// Some items failed. Offsets are relative to the attempt's event slice.
    PerItem {
        /// 429 / 5xx — retryable; should be re-sent.
        transient: Vec<ventstream_core::FailedItem>,
        /// Known document-level failures — permanent; route to the DLQ.
        permanent: Vec<ventstream_core::FailedItem>,
        /// Unknown or deployment-level item failures — fail closed.
        blocked: Vec<ventstream_core::FailedItem>,
    },
}

/// Partition a parsed bulk response into per-item outcomes.
///
/// Pure (no I/O) so the transient/permanent split is unit-testable without a
/// live HTTP server. Only known document-level errors are permanent. Unknown
/// item failures fail closed instead of being misclassified into the DLQ.
fn partition_bulk_items(
    parsed: &BulkResponse,
    item_event_offsets: &[usize],
    attempt_events: &[ventstream_core::Event],
) -> SendOutcome {
    let mut transient: Vec<ventstream_core::FailedItem> = Vec::new();
    let mut permanent: Vec<ventstream_core::FailedItem> = Vec::new();
    let mut blocked: Vec<ventstream_core::FailedItem> = Vec::new();
    for (item_index, item) in parsed.items.iter().enumerate() {
        // Map the bulk action back to its batch event; a key-changing update
        // contributes two actions for one event.
        let offset = item_event_offsets
            .get(item_index)
            .copied()
            .unwrap_or(item_index);
        let entry = item.action.entry();
        if entry.is_success() {
            continue;
        }
        if item.action.is_delete_not_found() {
            debug!(
                offset,
                "bulk delete target was already absent; treating replay as applied"
            );
            continue;
        }
        // A 409 external-version conflict is not a failure: a newer
        // (>=) write already won, so this stale op is correctly dropped
        // (H18). Treat it as applied — never retry it (the retry would
        // 409 forever) and never DLQ it (it isn't poison).
        if entry.is_version_conflict() {
            debug!(
                offset,
                "bulk item rejected by external-version conflict (stale write \
                 dropped — a newer LSN/tx_id already won); treating as applied"
            );
            continue;
        }
        let error = entry
            .error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("status {}", entry.status));
        let failed = ventstream_core::FailedItem { offset, error };
        if is_transient_item_failure(entry) {
            transient.push(failed);
        } else if is_permanent_document_failure(entry) {
            count_schema_casualty(entry, attempt_events.get(offset));
            permanent.push(failed);
        } else {
            blocked.push(failed);
        }
    }
    if transient.is_empty() && permanent.is_empty() && blocked.is_empty() {
        // `errors: true` but every item reads as a success — defensive.
        SendOutcome::AllOk
    } else {
        SendOutcome::PerItem {
            transient,
            permanent,
            blocked,
        }
    }
}

fn is_transient_item_failure(entry: &super::bulk::BulkResponseEntry) -> bool {
    if entry.is_rate_limited() || entry.status == 408 || entry.status >= 500 {
        return true;
    }
    is_transient_error_type(entry.error_type())
}

fn is_transient_error_type(error_type: Option<&str>) -> bool {
    matches!(
        error_type,
        Some(
            "circuit_breaking_exception"
                | "cluster_block_exception"
                | "es_rejected_execution_exception"
                | "master_not_discovered_exception"
                | "node_not_connected_exception"
                | "snapshot_in_progress_exception"
                | "unavailable_shards_exception"
        )
    )
}

fn response_error_type(body: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    let error = parsed.get("error")?;
    error
        .get("type")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            error
                .get("root_cause")
                .and_then(serde_json::Value::as_array)
                .and_then(|causes| causes.first())
                .and_then(|cause| cause.get("type"))
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_owned)
}

fn is_permanent_document_failure(entry: &super::bulk::BulkResponseEntry) -> bool {
    match entry.error_type() {
        Some(
            "document_parsing_exception"
            | "mapper_parsing_exception"
            | "strict_dynamic_mapping_exception",
        ) => true,
        // A mapping conflict surfaces as `illegal_argument_exception`
        // ("mapper [x] cannot be changed from type [long] to [text]").
        // Only the mapping-conflict shape is permanent; any other
        // illegal_argument stays fail-closed so a real config problem
        // cannot silently drain into the DLQ.
        Some("illegal_argument_exception") => {
            let reason = entry.error_reason().unwrap_or("");
            reason.contains("mapper")
                || reason.contains("cannot be changed")
                || reason.contains("different type")
        }
        _ => false,
    }
}

/// Count a schema-classified DLQ routing so mapping casualties are visible
/// per table instead of only as DLQ file lines. The table label comes from
/// the canonical doc id prefix (`{table}:[…]`).
fn count_schema_casualty(
    entry: &super::bulk::BulkResponseEntry,
    event: Option<&ventstream_core::Event>,
) {
    let table = entry
        .id
        .as_deref()
        .and_then(|id| id.split(":[").next())
        .unwrap_or("unknown")
        .to_owned();
    let error_type = entry.error_type().unwrap_or("unknown").to_owned();
    // Source scheme (postgres / mysql / mongodb / …) so shape drift on
    // schemaless sources is attributable without opening the DLQ.
    let source = event
        .and_then(|e| e.source.as_str().split("://").next())
        .unwrap_or("unknown")
        .to_owned();
    metrics::counter!(
        "vs_schema_casualties_total",
        "table" => table,
        "sink" => "opensearch",
        "source" => source,
        "error_type" => error_type
    )
    .increment(1);
}

/// Build the pooled HTTP client from the sink's TLS and timeout settings.
/// Shared with the read path so readers connect exactly like the writer.
pub(crate) fn build_http_client(
    config: &OpenSearchConfig,
) -> Result<reqwest::Client, OpenSearchSinkError> {
    crate::util::build_http_client(
        config.request_timeout,
        config.verify_tls,
        config.ca_file.as_deref(),
    )
    .map_err(OpenSearchSinkError::Internal)
}

/// Apply the configured auth mode to a request builder.
pub(crate) fn apply_auth(rb: reqwest::RequestBuilder, auth: &AuthMode) -> reqwest::RequestBuilder {
    match auth {
        AuthMode::None => rb,
        AuthMode::Basic { username, password } => rb.basic_auth(username, Some(password)),
        AuthMode::ApiKey(key) => rb.header(header::AUTHORIZATION, format!("ApiKey {key}")),
    }
}

/// Inspect the HTTP response: classify into a typed error or parse the
/// bulk body and surface per-item failures.
async fn classify_response(
    response: reqwest::Response,
    item_event_offsets: &[usize],
    attempt_events: &[ventstream_core::Event],
) -> Result<SendOutcome, OpenSearchSinkError> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    if status.is_success() {
        // 2xx - check each bulk item and partition failures into transient
        // retries, known permanent document errors, or delivery blockers.
        let bytes = response
            .bytes()
            .await
            .map_err(|err| OpenSearchSinkError::Transport(err.to_string()))?;
        let parsed: BulkResponse = serde_json::from_slice(&bytes)
            .map_err(|err| OpenSearchSinkError::MalformedResponse(err.to_string()))?;
        if parsed.items.len() != item_event_offsets.len() {
            return Err(OpenSearchSinkError::MalformedResponse(format!(
                "bulk response item count {} does not match request item count {}",
                parsed.items.len(),
                item_event_offsets.len()
            )));
        }
        return Ok(partition_bulk_items(
            &parsed,
            item_event_offsets,
            attempt_events,
        ));
    }

    // Non-2xx — read at most 1 KiB of the body for the error message so a
    // 5xx HTML error page can't dump megabytes into our logs.
    let body = read_truncated_body(response, 1024).await;
    let error_type = response_error_type(&body);

    match status {
        StatusCode::TOO_MANY_REQUESTS => Err(OpenSearchSinkError::RateLimited {
            message: body,
            retry_after,
        }),
        StatusCode::REQUEST_TIMEOUT => Err(OpenSearchSinkError::Server {
            status: status.as_u16(),
            message: body,
            retry_after,
        }),
        s if s.is_server_error() => Err(OpenSearchSinkError::Server {
            status: status.as_u16(),
            message: body,
            retry_after,
        }),
        s if s.is_client_error()
            && s != StatusCode::UNAUTHORIZED
            && is_transient_error_type(error_type.as_deref()) =>
        {
            Err(OpenSearchSinkError::Server {
                status: status.as_u16(),
                message: body,
                retry_after,
            })
        }
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(OpenSearchSinkError::Auth {
            status: status.as_u16(),
            message: body,
        }),
        s if s.is_client_error() => Err(OpenSearchSinkError::Client {
            status: status.as_u16(),
            message: body,
        }),
        _ => Err(OpenSearchSinkError::Internal(format!(
            "unexpected status {status}: {body}"
        ))),
    }
}

async fn read_truncated_body(response: reqwest::Response, max_bytes: usize) -> String {
    match response.bytes().await {
        Ok(bytes) => {
            // `.get(..max_bytes)` returns `Some` only when the slice
            // fits; falling back to the whole buffer keeps us off the
            // indexing-panics path that clippy denies.
            let slice = bytes.get(..max_bytes).unwrap_or(bytes.as_ref());
            String::from_utf8_lossy(slice).into_owned()
        }
        Err(err) => format!("<body read failed: {err}>"),
    }
}

#[async_trait]
impl Sink for OpenSearchSink {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "opensearch"
    }

    fn estimate_event_bytes(&self, event: &ventstream_core::Event) -> usize {
        // Payload plus conservative NDJSON action framing. Include the source
        // subject because dynamic subject templates can expand beyond the
        // literal template length, and include the stable doc id when present.
        let doc_id_bytes = event
            .headers
            .get("ventstream.doc.id")
            .map(str::len)
            .unwrap_or(26);
        event
            .payload
            .as_slice()
            .len()
            .saturating_add(self.config.index_template.len())
            .saturating_add(event.subject.as_str().len())
            .saturating_add(doc_id_bytes)
            .saturating_add(160)
    }

    fn max_request_bytes(&self) -> Option<usize> {
        Some(self.config.bulk.max_bytes)
    }

    fn recommended_concurrency(&self, configured_ceiling: usize) -> usize {
        self.adaptive.desired(configured_ceiling)
    }

    async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
        let result = self.send_with_retry(&batch).await;
        if let Err(error) = &result {
            if matches!(
                error,
                OpenSearchSinkError::Client { .. }
                    | OpenSearchSinkError::Auth { .. }
                    | OpenSearchSinkError::BlockedItems { .. }
                    | OpenSearchSinkError::IndexTemplate(_)
                    | OpenSearchSinkError::MalformedResponse(_)
                    | OpenSearchSinkError::Internal(_)
            ) {
                self.mark_blocked(error);
            }
        }
        result.map_err(|err| match err {
            OpenSearchSinkError::Transport(msg) => SinkError::Connection(msg),
            OpenSearchSinkError::Server {
                status, message, ..
            } => SinkError::Connection(format!("HTTP {status}: {message}")),
            OpenSearchSinkError::RateLimited { message, .. } => {
                SinkError::Connection(format!("HTTP 429: {message}"))
            }
            OpenSearchSinkError::Client { status, message }
            | OpenSearchSinkError::Auth { status, message } => {
                SinkError::Blocked(format!("HTTP {status}: {message}"))
            }
            OpenSearchSinkError::PartialFailure {
                batch_size,
                failed_count,
                sample_error,
                failed_items,
            } => SinkError::Rejected {
                batch_size,
                rejected_count: failed_count,
                message: sample_error,
                failed_items: Some(failed_items),
            },
            OpenSearchSinkError::TransientItems {
                failed_count,
                sample_error,
            } => SinkError::Connection(format!(
                "{failed_count} bulk items remain transiently unavailable: {sample_error}"
            )),
            OpenSearchSinkError::BlockedItems {
                failed_count,
                sample_error,
            } => SinkError::Blocked(format!(
                "{failed_count} bulk items blocked delivery: {sample_error}"
            )),
            OpenSearchSinkError::RequestTooLarge {
                actual_bytes,
                max_bytes,
            } => SinkError::Internal(format!(
                "single bulk item encoded to {actual_bytes} bytes, exceeding max_bytes={max_bytes}"
            )),
            OpenSearchSinkError::IndexTemplate(msg) => SinkError::Blocked(msg),
            OpenSearchSinkError::MalformedResponse(msg) => SinkError::Blocked(msg),
            OpenSearchSinkError::Internal(msg) => SinkError::Internal(msg),
        })
    }

    async fn flush(&self) -> Result<(), SinkError> {
        // Stateless sink — every write is a complete bulk request.
        Ok(())
    }
}

/// Unused parameter binding to satisfy the const expectations of the
/// internal API for time formatting; kept so the formatter never sees
/// an unused-but-imported error in release builds.
#[allow(dead_code)]
const _UNUSED_DURATION: Duration = Duration::ZERO;

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use std::time::Duration;
    use ventstream_core::{ContentType, Event, Headers, Payload, SourceUri, Subject};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn entry_with_error(error_type: &str, reason: &str) -> super::super::bulk::BulkResponseEntry {
        serde_json::from_value(serde_json::json!({
            "_id": "public.orders:[\"42\"]",
            "status": 400,
            "error": { "type": error_type, "reason": reason }
        }))
        .unwrap()
    }

    #[test]
    fn illegal_argument_is_permanent_only_for_mapping_conflicts() {
        // Mapping conflict shapes route to the DLQ.
        assert!(is_permanent_document_failure(&entry_with_error(
            "illegal_argument_exception",
            "mapper [price] cannot be changed from type [long] to [text]"
        )));
        assert!(is_permanent_document_failure(&entry_with_error(
            "illegal_argument_exception",
            "field [tags] is of different type in other index"
        )));
        // Any other illegal_argument stays fail-closed (retried, not dropped).
        assert!(!is_permanent_document_failure(&entry_with_error(
            "illegal_argument_exception",
            "Limit of total fields [1000] has been exceeded"
        )));
        // Existing permanent types keep their classification.
        assert!(is_permanent_document_failure(&entry_with_error(
            "mapper_parsing_exception",
            "failed to parse field"
        )));
        assert!(!is_permanent_document_failure(&entry_with_error(
            "es_rejected_execution_exception",
            "queue full"
        )));
    }

    #[test]
    fn adaptive_concurrency_halves_on_pressure_and_recovers_additively() {
        let adaptive = AdaptiveConcurrency::new(Duration::from_secs(30));
        assert_eq!(adaptive.desired(16), 4);
        adaptive.on_pressure("test");
        assert_eq!(adaptive.desired(16), 2);
        adaptive.on_pressure("test");
        assert_eq!(adaptive.desired(16), 1);
        for _ in 0..16 {
            adaptive.on_success(Duration::from_millis(20));
        }
        assert_eq!(adaptive.desired(16), 2);
    }

    #[test]
    fn adaptive_concurrency_never_exceeds_operator_ceiling() {
        let adaptive = AdaptiveConcurrency::new(Duration::from_secs(30));
        assert_eq!(adaptive.desired(2), 2);
        for _ in 0..64 {
            adaptive.on_success(Duration::from_millis(20));
        }
        assert_eq!(adaptive.desired(2), 2);
    }

    #[test]
    fn retry_jitter_stays_within_half_to_full_delay() {
        let base = Duration::from_secs(10);
        for _ in 0..1_000 {
            let delay = jittered_delay(base);
            assert!(delay >= Duration::from_secs(5));
            assert!(delay <= base);
        }
    }

    #[test]
    fn rejects_a_custom_ca_with_verification_disabled() {
        let config = OpenSearchConfig::new("test", "https://localhost:9200", "events")
            .with_ca_file("/run/secrets/ca.pem".into())
            .with_insecure_tls();
        assert!(OpenSearchSink::new(config).is_err());
    }

    fn bulk_body_len(
        events: &[ventstream_core::Event],
        template: &str,
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<u8>, OpenSearchSinkError> {
        bulk::build_bulk_body(events, template, now).map(|(body, _)| body)
    }

    fn make_event(subject: &str, payload: &str) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(payload.as_bytes().to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::empty())
            .build()
    }

    fn sink_against(endpoint: &str) -> OpenSearchSink {
        let mut cfg = OpenSearchConfig::new("test-sink", endpoint, "events-${subject:0}-%Y-%m-%d");
        // Tight retry so tests don't sit in sleep loops.
        cfg.retry = super::super::config::RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_factor: 1.5,
        };
        cfg.request_timeout = Duration::from_secs(2);
        OpenSearchSink::new(cfg).expect("sink builds")
    }

    fn live_tls_sink(ca_file: Option<std::path::PathBuf>) -> OpenSearchSink {
        let endpoint = std::env::var("VENTSTREAM_TEST_OPENSEARCH_ENDPOINT")
            .expect("VENTSTREAM_TEST_OPENSEARCH_ENDPOINT is required");
        let mut cfg = OpenSearchConfig::new("tls-test", endpoint, "ventstream-tls-test");
        cfg.retry = super::super::config::RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            backoff_factor: 1.0,
        };
        cfg.request_timeout = Duration::from_secs(10);
        if let Some(path) = ca_file {
            cfg = cfg.with_ca_file(path);
        }
        OpenSearchSink::new(cfg).expect("sink builds")
    }

    async fn probe_tls(sink: &OpenSearchSink) -> Result<(), ventstream_core::SinkError> {
        sink.write(SinkBatch::new(vec![make_event(
            "tls.probe",
            r#"{"probe":true}"#,
        )]))
        .await
    }

    #[tokio::test]
    #[ignore = "requires VENTSTREAM_TEST_OPENSEARCH_ENDPOINT and VENTSTREAM_TEST_OPENSEARCH_CA_FILE"]
    async fn strict_tls_reaches_a_trusted_opensearch_endpoint() {
        let ca = std::path::PathBuf::from(
            std::env::var("VENTSTREAM_TEST_OPENSEARCH_CA_FILE")
                .expect("VENTSTREAM_TEST_OPENSEARCH_CA_FILE is required"),
        );
        let result = probe_tls(&live_tls_sink(Some(ca))).await;
        assert!(
            !matches!(result, Err(ventstream_core::SinkError::Connection(_))),
            "trusted TLS must complete before any HTTP-level rejection: {result:?}"
        );
    }

    #[tokio::test]
    #[ignore = "requires VENTSTREAM_TEST_OPENSEARCH_ENDPOINT and VENTSTREAM_TEST_WRONG_CA_FILE"]
    async fn strict_tls_rejects_an_untrusted_opensearch_ca() {
        let wrong_ca = std::path::PathBuf::from(
            std::env::var("VENTSTREAM_TEST_WRONG_CA_FILE")
                .expect("VENTSTREAM_TEST_WRONG_CA_FILE is required"),
        );
        let result = probe_tls(&live_tls_sink(Some(wrong_ca))).await;
        assert!(
            matches!(result, Err(ventstream_core::SinkError::Connection(_))),
            "an unrelated CA must fail at the transport layer: {result:?}"
        );
    }

    #[tokio::test]
    #[ignore = "requires a hostname-mismatched VENTSTREAM_TEST_OPENSEARCH_ENDPOINT and VENTSTREAM_TEST_OPENSEARCH_CA_FILE"]
    async fn strict_tls_rejects_an_opensearch_hostname_mismatch() {
        let ca = std::path::PathBuf::from(
            std::env::var("VENTSTREAM_TEST_OPENSEARCH_CA_FILE")
                .expect("VENTSTREAM_TEST_OPENSEARCH_CA_FILE is required"),
        );
        let result = probe_tls(&live_tls_sink(Some(ca))).await;
        assert!(
            matches!(result, Err(ventstream_core::SinkError::Connection(_))),
            "a hostname mismatch must fail at the transport layer: {result:?}"
        );
    }

    #[tokio::test]
    async fn successful_bulk_write_completes() {
        let server = MockServer::start().await;
        let success_body = serde_json::json!({
            "took": 3,
            "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("content-type", "application/x-ndjson"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_body))
            .expect(1)
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![make_event("postgres.app.t.insert", r#"{"x":1}"#)]);
        sink.write(batch).await.expect("write");
    }

    #[tokio::test]
    async fn oversized_dispatcher_batch_is_split_into_valid_requests() {
        let server = MockServer::start().await;
        let success_body = serde_json::json!({
            "took": 1,
            "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_body))
            .expect(2)
            .mount(&server)
            .await;

        let events = vec![
            make_event("a.b", &format!(r#"{{"value":"{}"}}"#, "a".repeat(256))),
            make_event("a.b", &format!(r#"{{"value":"{}"}}"#, "b".repeat(256))),
        ];
        let single_bytes = bulk_body_len(
            events.get(..1).expect("single event slice"),
            "static-index",
            Utc::now(),
        )
        .expect("single body")
        .len();
        let both_bytes = bulk_body_len(&events, "static-index", Utc::now())
            .expect("combined body")
            .len();
        assert!(both_bytes > single_bytes + 1);

        let mut cfg = OpenSearchConfig::new("test-sink", server.uri(), "static-index");
        cfg.bulk.max_bytes = single_bytes + 1;
        cfg.retry.max_attempts = 1;
        let sink = OpenSearchSink::new(cfg).expect("sink");
        sink.write(SinkBatch::new(events))
            .await
            .expect("split write succeeds");
    }

    #[tokio::test]
    async fn single_oversized_event_is_reported_as_one_failed_item() {
        let server = MockServer::start().await;
        let event = make_event("a.b", &format!(r#"{{"value":"{}"}}"#, "x".repeat(256)));
        let encoded_bytes = bulk_body_len(std::slice::from_ref(&event), "static-index", Utc::now())
            .expect("body")
            .len();
        let mut cfg = OpenSearchConfig::new("test-sink", server.uri(), "static-index");
        cfg.bulk.max_bytes = encoded_bytes.saturating_sub(1);
        cfg.retry.max_attempts = 1;
        let sink = OpenSearchSink::new(cfg).expect("sink");

        let err = sink
            .write(SinkBatch::new(vec![event]))
            .await
            .expect_err("oversized event must fail");
        match err {
            SinkError::Rejected {
                batch_size,
                rejected_count,
                failed_items,
                ..
            } => {
                assert_eq!(batch_size, 1);
                assert_eq!(rejected_count, 1);
                let items = failed_items.expect("precise failed item");
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].offset, 0);
                assert!(items[0].error.contains("exceeds max_bytes"));
            }
            other => panic!("expected precise rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transient_500_then_success_retries_until_ok() {
        let server = MockServer::start().await;
        // First 500, then 200.
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let success_body = serde_json::json!({
            "took": 1, "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_body))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![make_event("a.b", "{}")]);
        sink.write(batch).await.expect("eventual success");
        assert_eq!(sink.delivery_health.snapshot(), SinkHealthSnapshot::Healthy);
    }

    #[tokio::test]
    async fn cluster_write_block_is_retried_even_when_reported_as_403() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {
                    "type": "cluster_block_exception",
                    "reason": "index blocked"
                },
                "status": 403
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": [{ "index": { "_id": "x", "status": 201 } }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        sink.write(SinkBatch::new(vec![make_event("a.b", "{}")]))
            .await
            .expect("cluster block should recover through retry");
    }

    #[tokio::test]
    async fn transient_retry_is_visible_until_the_write_recovers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": [{ "index": { "_id": "x", "status": 201 } }]
            })))
            .mount(&server)
            .await;

        let health = SinkHealth::new();
        let mut cfg = OpenSearchConfig::new("test-sink", server.uri(), "static-index")
            .with_delivery_health(health.clone());
        cfg.retry = super::super::config::RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_millis(250),
            backoff_factor: 1.0,
        };
        let sink = std::sync::Arc::new(OpenSearchSink::new(cfg).expect("sink"));
        let task_sink = std::sync::Arc::clone(&sink);
        let write = tokio::spawn(async move {
            task_sink
                .write(SinkBatch::new(vec![make_event("a.b", "{}")]))
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(health.snapshot(), SinkHealthSnapshot::Degraded { .. }) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("transient health transition");
        write
            .await
            .expect("write task")
            .expect("write eventually recovers");
        assert_eq!(health.snapshot(), SinkHealthSnapshot::Healthy);
    }

    #[tokio::test]
    async fn auth_failure_is_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(401).set_body_string("nope"))
            .expect(1) // strictly one — no retry
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![make_event("a.b", "{}")]);
        let err = sink.write(batch).await.expect_err("should fail");
        match err {
            SinkError::Blocked(message) => {
                assert!(message.contains("HTTP 401"), "got: {message}");
            }
            other => panic!("expected Blocked, got {other:?}"),
        }
        assert!(matches!(
            sink.delivery_health.snapshot(),
            SinkHealthSnapshot::Blocked { ref reason, .. }
                if reason.contains("authentication failed")
        ));
    }

    #[tokio::test]
    async fn retry_after_delta_seconds_is_captured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/retry"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "7")
                    .set_body_string("slow down"),
            )
            .mount(&server)
            .await;

        let response = reqwest::get(format!("{}/retry", server.uri()))
            .await
            .expect("response");
        let error = classify_response(response, &[], &[])
            .await
            .expect_err("429 must be retryable");
        assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
    }

    #[tokio::test]
    async fn incomplete_bulk_response_blocks_without_advancing_delivery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let error = sink
            .write(SinkBatch::new(vec![make_event("a.b", "{}")]))
            .await
            .expect_err("missing item acknowledgement must block");
        assert!(matches!(error, SinkError::Blocked(_)));
        assert!(matches!(
            sink.delivery_health.snapshot(),
            SinkHealthSnapshot::Blocked { .. }
        ));
    }

    #[tokio::test]
    async fn partial_failure_surfaces_failed_count_and_sample_error() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "took": 5,
            "errors": true,
            "items": [
                { "index": { "_id": "x", "status": 201 } },
                { "index": { "_id": "y", "status": 400, "error": { "type": "mapper_parsing_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![make_event("a.b", "{}"), make_event("a.b", "{}")]);
        let err = sink.write(batch).await.expect_err("should be partial");
        match err {
            SinkError::Rejected {
                batch_size,
                rejected_count,
                message,
                failed_items,
            } => {
                assert_eq!(batch_size, 2);
                assert_eq!(rejected_count, 1);
                assert!(
                    message.contains("mapper_parsing_exception"),
                    "got: {message}"
                );
                let items = failed_items.expect("partial failures must carry per-item details");
                assert_eq!(items.len(), 1);
                assert_eq!(
                    items[0].offset, 1,
                    "the second event in the batch was the one rejected"
                );
                assert!(
                    items[0].error.contains("mapper_parsing_exception"),
                    "per-item error must carry the downstream's message: got {}",
                    items[0].error
                );
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn basic_auth_header_is_sent() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "took": 1, "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("authorization", "Basic dXNlcjpwYXNz")) // "user:pass" base64
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let mut cfg =
            OpenSearchConfig::new("test-sink", server.uri(), "events-${subject:0}-%Y-%m-%d")
                .with_auth(AuthMode::Basic {
                    username: "user".into(),
                    password: "pass".into(),
                });
        cfg.retry = super::super::config::RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_factor: 1.0,
        };
        let sink = OpenSearchSink::new(cfg).expect("sink");
        let batch = SinkBatch::new(vec![make_event("a.b", "{}")]);
        sink.write(batch).await.expect("write succeeds");
    }

    #[tokio::test]
    async fn api_key_header_is_sent() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "took": 1, "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("authorization", "ApiKey abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let mut cfg =
            OpenSearchConfig::new("test-sink", server.uri(), "events-${subject:0}-%Y-%m-%d")
                .with_auth(AuthMode::ApiKey("abc123".into()));
        cfg.retry = super::super::config::RetryConfig {
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_factor: 1.0,
        };
        let sink = OpenSearchSink::new(cfg).expect("sink");
        let batch = SinkBatch::new(vec![make_event("a.b", "{}")]);
        sink.write(batch).await.expect("write succeeds");
    }

    #[test]
    fn partition_splits_transient_from_permanent() {
        let json = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "ok", "status": 201 } },
                { "index": { "_id": "rate", "status": 429,
                    "error": { "type": "es_rejected_execution_exception" } } },
                { "index": { "_id": "srv", "status": 503,
                    "error": { "type": "unavailable_shards_exception" } } },
                { "index": { "_id": "timeout", "status": 408,
                    "error": { "type": "request_timeout" } } },
                { "index": { "_id": "blocked-disk", "status": 403,
                    "error": { "type": "cluster_block_exception" } } },
                { "index": { "_id": "bad", "status": 400,
                    "error": { "type": "mapper_parsing_exception" } } },
                { "index": { "_id": "unknown", "status": 400,
                    "error": { "type": "unexpected_request_failure" } } }
            ]
        });
        let parsed: BulkResponse = serde_json::from_value(json).unwrap();
        match partition_bulk_items(&parsed, &[], &[]) {
            SendOutcome::PerItem {
                transient,
                permanent,
                blocked,
            } => {
                // 429 (offset 1), 503 (offset 2), 408 (offset 3), and a
                // disk-watermark cluster block (offset 4) are retryable.
                let t: Vec<usize> = transient.iter().map(|f| f.offset).collect();
                assert_eq!(
                    t,
                    vec![1, 2, 3, 4],
                    "408, 429, 5xx, and transient error types must retry"
                );
                // The known mapping error (offset 5) is permanent.
                assert_eq!(permanent.len(), 1);
                assert_eq!(permanent[0].offset, 5);
                assert!(permanent[0].error.contains("mapper_parsing_exception"));
                // Unknown 4xx errors are not safe to discard.
                assert_eq!(blocked.len(), 1);
                assert_eq!(blocked[0].offset, 6);
            }
            other => panic!("expected PerItem, got {other:?}"),
        }
    }

    #[test]
    fn partition_treats_already_absent_delete_as_success() {
        let json = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "delete": { "_id": "gone", "status": 404, "result": "not_found" } }
            ]
        });
        let parsed: BulkResponse = serde_json::from_value(json).unwrap();
        assert!(matches!(
            partition_bulk_items(&parsed, &[], &[]),
            SendOutcome::AllOk
        ));
    }

    #[tokio::test]
    async fn unknown_item_failure_blocks_without_becoming_dlq_poison() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": true,
                "items": [{
                    "index": {
                        "_id": "x",
                        "status": 400,
                        "error": { "type": "unexpected_request_failure" }
                    }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let error = sink
            .write(SinkBatch::new(vec![make_event("a.b", "{}")]))
            .await
            .expect_err("unknown item failure must block");
        assert!(matches!(error, SinkError::Blocked(_)));
        assert!(matches!(
            sink.delivery_health.snapshot(),
            SinkHealthSnapshot::Blocked { .. }
        ));
    }

    #[test]
    fn partition_treats_version_conflict_as_success() {
        // H18: with external_gte versioning, a 409 means a newer write
        // already won. The stale op must be dropped (treated as applied),
        // NOT retried (would 409 forever) and NOT DLQ'd (it isn't poison).
        let json = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "ok", "status": 201 } },
                { "index": { "_id": "stale", "status": 409,
                    "error": { "type": "version_conflict_engine_exception",
                               "reason": "current version [5] >= [3]" } } }
            ]
        });
        let parsed: BulkResponse = serde_json::from_value(json).unwrap();
        assert!(
            matches!(partition_bulk_items(&parsed, &[], &[]), SendOutcome::AllOk),
            "a batch whose only non-2xx item is a version conflict is fully applied"
        );
    }

    #[tokio::test]
    async fn version_conflict_item_does_not_fail_the_write() {
        // End-to-end through the sink: a bulk response with a lone 409
        // version conflict must resolve to Ok(()) — the stale write is
        // silently dropped, the source slot advances normally.
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "took": 2,
            "errors": true,
            "items": [
                { "index": { "_id": "ok", "status": 200 } },
                { "index": { "_id": "stale", "status": 409,
                    "error": { "type": "version_conflict_engine_exception",
                               "reason": "current version higher" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1) // no retry — the conflict is not transient
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            make_event("postgres.app.t.update", "{}"),
            make_event("postgres.app.t.update", "{}"),
        ]);
        sink.write(batch)
            .await
            .expect("version conflict must not fail the batch");
    }

    #[test]
    fn partition_treats_only_delete_not_found_as_success() {
        let json = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "delete": { "_id": "already-absent", "status": 404,
                    "error": { "type": "document_missing_exception" } } },
                { "index": { "_id": "bad-index", "status": 404,
                    "error": { "type": "index_not_found_exception" } } }
            ]
        });
        let parsed: BulkResponse = serde_json::from_value(json).unwrap();
        match partition_bulk_items(&parsed, &[], &[]) {
            SendOutcome::PerItem {
                transient,
                permanent,
                blocked,
            } => {
                assert!(transient.is_empty());
                assert!(permanent.is_empty());
                assert_eq!(blocked.len(), 1);
                assert_eq!(blocked[0].offset, 1);
                assert!(blocked[0].error.contains("index_not_found_exception"));
            }
            other => panic!("expected the missing index to block delivery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replayed_delete_not_found_does_not_fail_the_write() {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "delete": { "_id": "orders:missing", "status": 404,
                    "error": { "type": "document_missing_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .expect(1)
            .mount(&server)
            .await;

        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new("postgres.app.orders.delete").expect("subject");
        let mut headers = std::collections::HashMap::new();
        headers.insert("ventstream.doc.id".to_owned(), "orders:missing".to_owned());
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(br#"{"id":"missing"}"#.to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();

        let sink = sink_against(&server.uri());
        sink.write(SinkBatch::new(vec![event]))
            .await
            .expect("an already-absent delete target is applied");
    }

    #[test]
    fn partition_all_success_is_allok() {
        let json = serde_json::json!({
            "took": 1, "errors": false,
            "items": [{ "index": { "_id": "x", "status": 201 } }]
        });
        let parsed: BulkResponse = serde_json::from_value(json).unwrap();
        assert!(matches!(
            partition_bulk_items(&parsed, &[], &[]),
            SendOutcome::AllOk
        ));
    }

    #[tokio::test]
    async fn transient_item_retried_only_permanent_dlqd() {
        let server = MockServer::start().await;
        // First attempt (3 items): ok / 429 (transient) / 400 (permanent).
        let first = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "a", "status": 201 } },
                { "index": { "_id": "b", "status": 429,
                    "error": { "type": "es_rejected_execution_exception" } } },
                { "index": { "_id": "c", "status": 400,
                    "error": { "type": "mapper_parsing_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(first))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Retry of the transient subset (just item "b") succeeds.
        let retry_ok = serde_json::json!({
            "took": 1, "errors": false,
            "items": [{ "index": { "_id": "b", "status": 201 } }]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(retry_ok))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            make_event("a.b", "{}"),
            make_event("a.b", "{}"),
            make_event("a.b", "{}"),
        ]);
        let err = sink.write(batch).await.expect_err("permanent item remains");
        match err {
            SinkError::Rejected {
                batch_size,
                rejected_count,
                failed_items,
                ..
            } => {
                assert_eq!(batch_size, 3);
                assert_eq!(rejected_count, 1, "only the 400 is permanent");
                let items = failed_items.expect("per-item details");
                assert_eq!(items.len(), 1);
                assert_eq!(
                    items[0].offset, 2,
                    "offset must map back to the original batch (the 400 was item 2)"
                );
                assert!(items[0].error.contains("mapper_parsing_exception"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transient_item_retry_budget_exhaustion_fails_closed() {
        let server = MockServer::start().await;
        // First attempt (2 items): ok / 429.
        let first = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "a", "status": 201 } },
                { "index": { "_id": "b", "status": 429,
                    "error": { "type": "es_rejected_execution_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(first))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Every retry of the 1-item subset keeps returning 429.
        let still_429 = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "b", "status": 429,
                    "error": { "type": "es_rejected_execution_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(still_429))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![make_event("a.b", "{}"), make_event("a.b", "{}")]);
        let err = sink
            .write(batch)
            .await
            .expect_err("bounded transient retries must fail closed");
        match err {
            SinkError::Connection(message) => {
                assert!(message.contains("1 bulk items"));
                assert!(message.contains("es_rejected_execution_exception"));
            }
            other => panic!("expected Connection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subset_retry_auth_failure_blocks_the_entire_batch() {
        let server = MockServer::start().await;
        // Attempt 1 (3 items): ok / 429 (transient) / 400 (permanent).
        let first = serde_json::json!({
            "took": 1,
            "errors": true,
            "items": [
                { "index": { "_id": "a", "status": 201 } },
                { "index": { "_id": "b", "status": 429,
                    "error": { "type": "es_rejected_execution_exception" } } },
                { "index": { "_id": "c", "status": 400,
                    "error": { "type": "mapper_parsing_exception" } } }
            ]
        });
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(first))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // The subset retry (just item "b") hits an auth failure — a whole-
        // attempt, non-retryable error.
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(401).set_body_string("token expired"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            make_event("a.b", "{}"),
            make_event("a.b", "{}"),
            make_event("a.b", "{}"),
        ]);
        let err = sink.write(batch).await.expect_err("auth must block");
        match err {
            SinkError::Blocked(message) => {
                assert!(message.contains("HTTP 401"));
                assert!(message.contains("token expired"));
            }
            other => panic!("expected Blocked, got {other:?}"),
        }
    }
}
