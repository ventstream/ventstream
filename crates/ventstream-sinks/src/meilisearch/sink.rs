//! The Meilisearch sink: sequential runs, asynchronous task confirmation.
//!
//! Meilisearch acknowledges writes with a `taskUid` and indexes later, so
//! delivery is only durable once the task reaches a terminal status. Every
//! run therefore enqueues, then polls its task to `succeeded` before the
//! next run starts. Meilisearch has no external document versioning, so
//! the sink pins write concurrency to 1 to preserve per-document order.

use std::time::{Duration, Instant};

use reqwest::StatusCode;
use tracing::{debug, warn};
use ventstream_core::{
    Event, ShutdownToken, Sink, SinkBatch, SinkError, SinkFailureGuard, SinkHealth,
};

use super::config::{MeilisearchConfig, MeilisearchIndexRouting};
use super::documents::{translate_batch, Run, RunKind};
use super::error::{is_transient_task_code, truncate_body, MeilisearchSinkError};
use crate::opensearch::retry::BackoffSchedule;
use crate::util::jittered_delay;

/// Terminal-or-not view of one polled task.
#[derive(Debug)]
enum TaskState {
    Pending,
    Succeeded,
    Failed { code: String, message: String },
    Canceled,
}

/// Meilisearch materialization sink.
pub struct MeilisearchSink {
    config: MeilisearchConfig,
    client: reqwest::Client,
    base_url: String,
    delivery_health: SinkHealth,
}

impl MeilisearchSink {
    /// Build the sink and HTTP client without touching the network.
    pub fn new(config: MeilisearchConfig) -> Result<Self, MeilisearchSinkError> {
        if !config.verify_tls && config.ca_file.is_some() {
            return Err(MeilisearchSinkError::Internal(
                "a custom CA cannot be combined with disabled TLS verification".into(),
            ));
        }
        let mut builder = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(30))
            .gzip(true)
            .user_agent(concat!("ventstream/", env!("CARGO_PKG_VERSION")));
        if !config.verify_tls {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(path) = &config.ca_file {
            let pem = std::fs::read(path).map_err(|err| {
                MeilisearchSinkError::Internal(format!(
                    "read TLS CA bundle {}: {err}",
                    path.display()
                ))
            })?;
            let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|err| {
                MeilisearchSinkError::Internal(format!(
                    "parse TLS CA bundle {}: {err}",
                    path.display()
                ))
            })?;
            if certificates.is_empty() {
                return Err(MeilisearchSinkError::Internal(format!(
                    "TLS CA bundle {} contains no certificates",
                    path.display()
                )));
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder
            .build()
            .map_err(|err| MeilisearchSinkError::Internal(format!("http client build: {err}")))?;
        let base_url = config.endpoint.trim_end_matches('/').to_owned();
        let delivery_health = config.delivery_health.clone().unwrap_or_default();
        Ok(Self {
            config,
            client,
            base_url,
            delivery_health,
        })
    }

    /// Build the sink and run the startup probe, retrying transient
    /// failures until the probe passes or shutdown is signalled.
    pub async fn connect_with_shutdown(
        config: MeilisearchConfig,
        shutdown: &ShutdownToken,
    ) -> Result<Self, MeilisearchSinkError> {
        let sink = Self::new(config)?;
        let mut schedule = BackoffSchedule::new(sink.config.retry);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    return Err(MeilisearchSinkError::Internal(
                        "shutdown during Meilisearch startup probe".into(),
                    ));
                }
                probed = sink.probe() => match probed {
                    Ok(()) => return Ok(sink),
                    Err(err) if err.is_retryable() => {
                        let Some(delay) = schedule.next() else {
                            return Err(err);
                        };
                        warn!(
                            sink_id = %sink.config.id,
                            error = %err,
                            ?delay,
                            "meilisearch startup probe failed; retrying"
                        );
                        tokio::time::sleep(jittered_delay(delay)).await;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    /// Startup probe: reachability + auth, and for fixed routing the
    /// primary-key compatibility of the target index plus any declared
    /// settings. Routed (per-table) indexes are validated lazily because
    /// their names are only known per event.
    async fn probe(&self) -> Result<(), MeilisearchSinkError> {
        let response = self
            .request(reqwest::Method::GET, "/version")
            .send()
            .await
            .map_err(|err| transport_error(&err))?;
        let status = response.status();
        if !status.is_success() {
            return Err(self.classify_status(status, &body_text(response).await));
        }

        let MeilisearchIndexRouting::Fixed(_) = &self.config.index_routing else {
            return Ok(());
        };
        let translated_uid = fixed_index_uid(&self.config);
        let response = self
            .request(reqwest::Method::GET, &format!("/indexes/{translated_uid}"))
            .send()
            .await
            .map_err(|err| transport_error(&err))?;
        match response.status() {
            StatusCode::NOT_FOUND if self.config.auto_create_indexes => {}
            StatusCode::NOT_FOUND => {
                return Err(MeilisearchSinkError::Client {
                    status: 404,
                    message: format!(
                        "index `{translated_uid}` does not exist and auto_create_indexes is disabled"
                    ),
                });
            }
            status if status.is_success() => {
                let body: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| MeilisearchSinkError::MalformedResponse(err.to_string()))?;
                match body.get("primaryKey") {
                    Some(serde_json::Value::Null) | None => {}
                    Some(serde_json::Value::String(existing))
                        if *existing == self.config.primary_key_field => {}
                    Some(other) => {
                        return Err(MeilisearchSinkError::Client {
                            status: 409,
                            message: format!(
                                "index `{translated_uid}` has primaryKey {other} but the sink \
                                 requires `{}`; drain and rebootstrap to change it",
                                self.config.primary_key_field
                            ),
                        });
                    }
                }
            }
            status => return Err(self.classify_status(status, &body_text(response).await)),
        }

        if let Some(settings) = &self.config.settings {
            let mut patch = serde_json::Map::new();
            if !settings.filterable_attributes.is_empty() {
                patch.insert(
                    "filterableAttributes".to_owned(),
                    serde_json::json!(settings.filterable_attributes),
                );
            }
            if !settings.sortable_attributes.is_empty() {
                patch.insert(
                    "sortableAttributes".to_owned(),
                    serde_json::json!(settings.sortable_attributes),
                );
            }
            if !patch.is_empty() {
                let task_uid = self
                    .enqueue(
                        reqwest::Method::PATCH,
                        &format!("/indexes/{translated_uid}/settings"),
                        Some(serde_json::Value::Object(patch)),
                    )
                    .await?;
                self.await_task(task_uid, false).await?;
            }
        }
        Ok(())
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        if let Some(key) = &self.config.api_key {
            builder = builder.bearer_auth(key);
        }
        builder
    }

    /// Send one API call that enqueues a task; return its uid.
    async fn enqueue(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<u64, MeilisearchSinkError> {
        let mut builder = self.request(method, path);
        if let Some(body) = body {
            builder = builder.json(&body);
        }
        let response = builder.send().await.map_err(|err| transport_error(&err))?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = parse_retry_after(&response);
            let body = body_text(response).await;
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(MeilisearchSinkError::RateLimited {
                    message: truncate_body(&body),
                    retry_after,
                });
            }
            return Err(self.classify_status(status, &body));
        }
        let parsed: serde_json::Value = response
            .json()
            .await
            .map_err(|err| MeilisearchSinkError::MalformedResponse(err.to_string()))?;
        parsed
            .get("taskUid")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                MeilisearchSinkError::MalformedResponse("enqueue response has no taskUid".into())
            })
    }

    /// Poll one task to a terminal status within the configured deadline.
    /// `tolerate_missing_index` treats an `index_not_found` failure as
    /// success — deletes against a not-yet-created index are a no-op.
    async fn await_task(
        &self,
        task_uid: u64,
        tolerate_missing_index: bool,
    ) -> Result<(), MeilisearchSinkError> {
        let started = Instant::now();
        let mut interval = self.config.task.poll_initial;
        loop {
            if started.elapsed() >= self.config.task.deadline {
                return Err(MeilisearchSinkError::TaskTimeout {
                    task_uid,
                    millis: u64::try_from(self.config.task.deadline.as_millis())
                        .unwrap_or(u64::MAX),
                });
            }
            match self.fetch_task(task_uid).await? {
                TaskState::Pending => {
                    tokio::time::sleep(interval).await;
                    interval = (interval * 2).min(self.config.task.poll_max);
                }
                TaskState::Succeeded => {
                    debug!(
                        sink_id = %self.config.id,
                        task_uid,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "meilisearch task succeeded"
                    );
                    return Ok(());
                }
                TaskState::Failed { code, message } => {
                    if tolerate_missing_index && code == "index_not_found" {
                        return Ok(());
                    }
                    if is_transient_task_code(&code) {
                        return Err(MeilisearchSinkError::TaskTransient { code, message });
                    }
                    return Err(MeilisearchSinkError::TaskBlocked { code, message });
                }
                TaskState::Canceled => {
                    return Err(MeilisearchSinkError::TaskCanceled { task_uid });
                }
            }
        }
    }

    async fn fetch_task(&self, task_uid: u64) -> Result<TaskState, MeilisearchSinkError> {
        let response = self
            .request(reqwest::Method::GET, &format!("/tasks/{task_uid}"))
            .send()
            .await
            .map_err(|err| transport_error(&err))?;
        let status = response.status();
        if !status.is_success() {
            return Err(self.classify_status(status, &body_text(response).await));
        }
        let parsed: serde_json::Value = response
            .json()
            .await
            .map_err(|err| MeilisearchSinkError::MalformedResponse(err.to_string()))?;
        let state = parsed.get("status").and_then(serde_json::Value::as_str);
        match state {
            Some("enqueued" | "processing") => Ok(TaskState::Pending),
            Some("succeeded") => Ok(TaskState::Succeeded),
            Some("canceled") => Ok(TaskState::Canceled),
            Some("failed") => {
                let code = parsed
                    .pointer("/error/code")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let message = parsed
                    .pointer("/error/message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("meilisearch reported no error message")
                    .to_owned();
                Ok(TaskState::Failed { code, message })
            }
            other => Err(MeilisearchSinkError::MalformedResponse(format!(
                "unexpected task status {other:?}"
            ))),
        }
    }

    fn classify_status(&self, status: StatusCode, body: &str) -> MeilisearchSinkError {
        let message = truncate_body(body);
        match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => MeilisearchSinkError::Auth {
                status: status.as_u16(),
                message,
            },
            StatusCode::REQUEST_TIMEOUT => MeilisearchSinkError::Server {
                status: status.as_u16(),
                message,
            },
            status if status.is_server_error() => MeilisearchSinkError::Server {
                status: status.as_u16(),
                message,
            },
            status => MeilisearchSinkError::Client {
                status: status.as_u16(),
                message,
            },
        }
    }

    /// Execute one run to task completion, retrying transient failures.
    async fn execute_run(&self, run: &Run) -> Result<(), MeilisearchSinkError> {
        let mut schedule = BackoffSchedule::new(self.config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;
        loop {
            let attempt = self.execute_run_once(run).await;
            match attempt {
                Ok(()) => {
                    if transient_failure.take().is_some() {
                        ventstream_telemetry::mark_sink_available();
                    }
                    return Ok(());
                }
                Err(err) if err.is_retryable() => {
                    if transient_failure.is_none() {
                        transient_failure = Some(
                            self.delivery_health
                                .begin_transient_failure(err.to_string()),
                        );
                        ventstream_telemetry::mark_sink_unavailable();
                    }
                    let Some(base_delay) = schedule.next() else {
                        return Err(err);
                    };
                    let delay = err
                        .retry_after()
                        .unwrap_or_else(|| jittered_delay(base_delay))
                        .min(self.config.retry.max_backoff);
                    ventstream_telemetry::bump_sink_retries(
                        u64::try_from(run.offsets.len()).unwrap_or(u64::MAX),
                    );
                    debug!(
                        sink_id = %self.config.id,
                        attempt = schedule.attempts_so_far(),
                        ?delay,
                        error = %err,
                        "meilisearch run failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn execute_run_once(&self, run: &Run) -> Result<(), MeilisearchSinkError> {
        let (task_uid, tolerate_missing_index) = match &run.kind {
            RunKind::Upsert(documents) => {
                let path = format!(
                    "/indexes/{}/documents?primaryKey={}",
                    run.index_uid, self.config.primary_key_field
                );
                let task = self
                    .enqueue(
                        reqwest::Method::POST,
                        &path,
                        Some(serde_json::json!(documents)),
                    )
                    .await?;
                (task, false)
            }
            RunKind::Delete(primary_keys) => {
                let path = format!("/indexes/{}/documents/delete-batch", run.index_uid);
                let task = self
                    .enqueue(
                        reqwest::Method::POST,
                        &path,
                        Some(serde_json::json!(primary_keys)),
                    )
                    .await?;
                (task, true)
            }
            RunKind::Truncate => {
                let path = format!("/indexes/{}/documents", run.index_uid);
                let task = self.enqueue(reqwest::Method::DELETE, &path, None).await?;
                (task, true)
            }
        };
        self.await_task(task_uid, tolerate_missing_index).await
    }

    fn mark_blocked(&self, error: &MeilisearchSinkError) {
        self.delivery_health.mark_blocked(error.to_string());
    }
}

/// Index UID for fixed routing, prefix and encoding applied.
fn fixed_index_uid(config: &MeilisearchConfig) -> String {
    let MeilisearchIndexRouting::Fixed(index) = &config.index_routing else {
        return String::new();
    };
    format!(
        "{}{}",
        config.index_prefix,
        super::documents::encode_index_segment(index)
    )
}

fn transport_error(err: &reqwest::Error) -> MeilisearchSinkError {
    MeilisearchSinkError::Transport(err.to_string())
}

async fn body_text(response: reqwest::Response) -> String {
    response.text().await.unwrap_or_default()
}

fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

#[async_trait::async_trait]
impl Sink for MeilisearchSink {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "meilisearch"
    }

    fn estimate_event_bytes(&self, event: &Event) -> usize {
        // Payload plus primary key, canonical id field, and JSON framing.
        event.payload.as_slice().len() + 160
    }

    fn max_request_bytes(&self) -> Option<usize> {
        Some(self.config.batching.max_bytes)
    }

    fn recommended_concurrency(&self, _configured_ceiling: usize) -> usize {
        // No external document versioning in Meilisearch: parallel batches
        // could reorder same-document writes. Order is preserved by
        // sequential runs at concurrency 1.
        1
    }

    async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
        let events = batch.events();
        if events.is_empty() {
            return Ok(());
        }
        let translated = translate_batch(&self.config, events);

        for run in &translated.runs {
            if let Err(err) = self.execute_run(run).await {
                if matches!(
                    err,
                    MeilisearchSinkError::Auth { .. }
                        | MeilisearchSinkError::Client { .. }
                        | MeilisearchSinkError::TaskBlocked { .. }
                        | MeilisearchSinkError::MalformedResponse(_)
                        | MeilisearchSinkError::Internal(_)
                ) {
                    self.mark_blocked(&err);
                }
                return Err(match err {
                    MeilisearchSinkError::Transport(msg) => SinkError::Connection(msg),
                    MeilisearchSinkError::Server { status, message } => {
                        SinkError::Connection(format!("HTTP {status}: {message}"))
                    }
                    MeilisearchSinkError::RateLimited { message, .. } => {
                        SinkError::Connection(format!("HTTP 429: {message}"))
                    }
                    MeilisearchSinkError::TaskTransient { code, message } => SinkError::Connection(
                        format!("task failed transiently ({code}): {message}"),
                    ),
                    MeilisearchSinkError::TaskCanceled { task_uid } => {
                        SinkError::Connection(format!("task {task_uid} canceled server-side"))
                    }
                    MeilisearchSinkError::TaskTimeout { millis, .. } => {
                        SinkError::Timeout { millis }
                    }
                    MeilisearchSinkError::Auth { status, message }
                    | MeilisearchSinkError::Client { status, message } => {
                        SinkError::Blocked(format!("HTTP {status}: {message}"))
                    }
                    MeilisearchSinkError::TaskBlocked { code, message } => SinkError::Blocked(
                        format!("meilisearch rejected the batch ({code}): {message}"),
                    ),
                    MeilisearchSinkError::MalformedResponse(msg) => SinkError::Blocked(msg),
                    MeilisearchSinkError::PartialFailure { .. } => SinkError::Internal(
                        "unexpected partial failure from a whole-run operation".into(),
                    ),
                    MeilisearchSinkError::Internal(msg) => SinkError::Internal(msg),
                });
            }
        }

        if translated.rejects.is_empty() {
            return Ok(());
        }
        let mut rejects = translated.rejects;
        rejects.sort_unstable_by_key(|item| item.offset);
        rejects.dedup_by_key(|item| item.offset);
        let message = rejects
            .first()
            .map(|item| item.error.clone())
            .unwrap_or_else(|| "unknown".into());
        Err(SinkError::Rejected {
            batch_size: events.len(),
            rejected_count: rejects.len(),
            message,
            failed_items: Some(rejects),
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::time::Duration;

    use ventstream_core::event::{ContentType, Headers, Payload, SourceUri, Subject};
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::super::config::MeilisearchTaskConfig;
    use super::*;
    use crate::opensearch::RetryConfig;

    fn upsert_event(table: &str, doc_id: &str, payload: &str) -> Event {
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new(format!("postgres.public.{table}.update")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(payload.as_bytes().to_vec()))
            .content_type(ContentType::Json)
            .headers(
                Headers::empty()
                    .with_header("ventstream.doc.id".into(), doc_id.into())
                    .with_header("ventstream.cdc.relation".into(), table.into()),
            )
            .build()
    }

    fn delete_event(table: &str, doc_id: &str) -> Event {
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new(format!("postgres.public.{table}.delete")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(Vec::new()))
            .headers(
                Headers::empty()
                    .with_header("ventstream.doc.id".into(), doc_id.into())
                    .with_header("ventstream.cdc.relation".into(), table.into()),
            )
            .build()
    }

    fn sink_against(endpoint: &str) -> MeilisearchSink {
        let mut config = MeilisearchConfig::new("test-meili", endpoint);
        config.retry = RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            backoff_factor: 2.0,
        };
        config.task = MeilisearchTaskConfig {
            poll_initial: Duration::from_millis(1),
            poll_max: Duration::from_millis(5),
            deadline: Duration::from_secs(5),
        };
        MeilisearchSink::new(config).expect("sink")
    }

    fn task_response(uid: u64, status: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "uid": uid,
            "status": status,
        }))
    }

    fn failed_task_response(uid: u64, code: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "uid": uid,
            "status": "failed",
            "error": { "message": "boom", "code": code },
        }))
    }

    fn enqueue_response(uid: u64) -> ResponseTemplate {
        ResponseTemplate::new(202).set_body_json(serde_json::json!({
            "taskUid": uid,
            "indexUid": "vs_orders",
            "status": "enqueued",
        }))
    }

    #[tokio::test]
    async fn upsert_batch_succeeds_after_task_completion() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(7))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/7"))
            .respond_with(task_response(7, "succeeded"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"orders:["1"]"#,
            r#"{"id":1,"total":10}"#,
        )]);
        sink.write(batch).await.expect("write");
    }

    #[tokio::test]
    async fn pending_task_is_polled_to_completion() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(9))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/9"))
            .respond_with(task_response(9, "processing"))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/9"))
            .respond_with(task_response(9, "succeeded"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"orders:["2"]"#,
            r#"{"id":2}"#,
        )]);
        sink.write(batch).await.expect("write");
    }

    #[tokio::test]
    async fn document_task_failure_blocks_delivery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(11))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/11"))
            .respond_with(failed_task_response(11, "invalid_document_fields"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"orders:["3"]"#,
            r#"{"id":3}"#,
        )]);
        let err = sink.write(batch).await.expect_err("must block");
        assert!(matches!(err, SinkError::Blocked(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn transient_task_failure_retries_then_fails_connection() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(13))
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/13"))
            .respond_with(failed_task_response(13, "task_queue_full"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"orders:["4"]"#,
            r#"{"id":4}"#,
        )]);
        let err = sink.write(batch).await.expect_err("must fail");
        assert!(matches!(err, SinkError::Connection(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn delete_against_missing_index_is_a_noop() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents/delete-batch"))
            .respond_with(enqueue_response(17))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/17"))
            .respond_with(failed_task_response(17, "index_not_found"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![delete_event("orders", r#"orders:["5"]"#)]);
        sink.write(batch).await.expect("delete is idempotent");
    }

    #[tokio::test]
    async fn auth_rejection_blocks_delivery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/indexes/.*/documents$"))
            .respond_with(ResponseTemplate::new(403).set_body_string("invalid key"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"orders:["6"]"#,
            r#"{"id":6}"#,
        )]);
        let err = sink.write(batch).await.expect_err("must block");
        assert!(matches!(err, SinkError::Blocked(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unparseable_payload_is_rejected_with_exact_offset() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(19))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/19"))
            .respond_with(task_response(19, "succeeded"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            upsert_event("orders", r#"orders:["7"]"#, r#"{"id":7}"#),
            upsert_event("orders", r#"orders:["8"]"#, "not json"),
        ]);
        let err = sink.write(batch).await.expect_err("must reject");
        let SinkError::Rejected {
            batch_size,
            rejected_count,
            failed_items: Some(items),
            ..
        } = err
        else {
            panic!("expected Rejected, got another variant");
        };
        assert_eq!(batch_size, 2);
        assert_eq!(rejected_count, 1);
        assert_eq!(items[0].offset, 1);
    }

    #[tokio::test]
    async fn mixed_ops_execute_in_order_per_index() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents"))
            .respond_with(enqueue_response(23))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes/vs_orders/documents/delete-batch"))
            .respond_with(enqueue_response(29))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/tasks/\d+$"))
            .respond_with(task_response(23, "succeeded"))
            .mount(&server)
            .await;

        let sink = sink_against(&server.uri());
        // upsert, delete, upsert on one index → three ordered runs.
        let batch = SinkBatch::new(vec![
            upsert_event("orders", r#"orders:["9"]"#, r#"{"id":9}"#),
            delete_event("orders", r#"orders:["9"]"#),
            upsert_event("orders", r#"orders:["10"]"#, r#"{"id":10}"#),
        ]);
        sink.write(batch).await.expect("write");
    }

    #[tokio::test]
    async fn startup_probe_validates_fixed_index_primary_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pkgVersion": "1.10.0"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes/vs_products"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "uid": "vs_products",
                "primaryKey": "someone_elses_key"
            })))
            .mount(&server)
            .await;

        let mut config = MeilisearchConfig::new("test-meili", server.uri());
        config.index_routing = MeilisearchIndexRouting::Fixed("products".into());
        config.retry = RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            backoff_factor: 2.0,
        };
        let shutdown = ShutdownToken::new();
        let Err(err) = MeilisearchSink::connect_with_shutdown(config, &shutdown).await else {
            panic!("primary key mismatch must fail the probe");
        };
        assert!(
            err.to_string().contains("drain and rebootstrap"),
            "got {err}"
        );
    }
}
