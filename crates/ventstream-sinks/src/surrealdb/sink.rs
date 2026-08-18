//! The SurrealDB sink: ordered statement runs over the HTTP RPC protocol.
//!
//! SurrealDB commits synchronously — a successful RPC response IS the
//! delivery confirmation, so there is no asynchronous task layer. Runs
//! execute sequentially at concurrency 1 (no external document
//! versioning; parallel batches could reorder same-record writes). On a
//! retryable failure at run K, execution restarts from K — earlier runs
//! are already committed and replaying them is idempotent by record id.

use reqwest::StatusCode;
use serde_json::{json, Value};
use tracing::{debug, warn};
use ventstream_core::{
    Event, ShutdownToken, Sink, SinkBatch, SinkError, SinkFailureGuard, SinkHealth,
};

use super::config::{is_safe_identifier, SurrealDbConfig};
use super::error::{is_transient_query_error, truncate_body, SurrealSinkError};
use super::statements::{translate_batch, Run, RunKind};
use crate::opensearch::retry::BackoffSchedule;
use crate::util::jittered_delay;

/// SurrealDB materialization sink.
pub struct SurrealDbSink {
    config: SurrealDbConfig,
    client: reqwest::Client,
    rpc_url: String,
    delivery_health: SinkHealth,
}

impl SurrealDbSink {
    /// Build the sink and HTTP client without touching the network.
    pub fn new(config: SurrealDbConfig) -> Result<Self, SurrealSinkError> {
        for index in &config.vector_indexes {
            if !is_safe_identifier(&index.table) || !is_safe_identifier(&index.field) {
                return Err(SurrealSinkError::Internal(format!(
                    "vector index on `{}`.`{}` uses characters outside [A-Za-z0-9_.-]",
                    index.table, index.field
                )));
            }
            if index.dimension == 0 {
                return Err(SurrealSinkError::Internal(format!(
                    "vector index on `{}` declares dimension 0",
                    index.table
                )));
            }
        }
        let client = crate::util::build_http_client(
            config.request_timeout,
            config.verify_tls,
            config.ca_file.as_deref(),
        )
        .map_err(SurrealSinkError::Internal)?;
        let rpc_url = format!("{}/rpc", config.endpoint.trim_end_matches('/'));
        let delivery_health = config.delivery_health.clone().unwrap_or_default();
        Ok(Self {
            config,
            client,
            rpc_url,
            delivery_health,
        })
    }

    /// Build the sink and run the startup probe (reachability, auth,
    /// vector-index DDL), retrying transient failures until the probe
    /// passes or shutdown is signalled.
    pub async fn connect_with_shutdown(
        config: SurrealDbConfig,
        shutdown: &ShutdownToken,
    ) -> Result<Self, SurrealSinkError> {
        let sink = Self::new(config)?;
        let mut schedule = BackoffSchedule::new(sink.config.retry);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    return Err(SurrealSinkError::Internal(
                        "shutdown during SurrealDB startup probe".into(),
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
                            "surrealdb startup probe failed; retrying"
                        );
                        tokio::time::sleep(jittered_delay(delay)).await;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    /// Startup probe: ensure the namespace/database when configured,
    /// prove reachability/auth/scoping with one trivial query, then
    /// ensure declared vector indexes with idempotent DDL.
    async fn probe(&self) -> Result<(), SurrealSinkError> {
        if self.config.auto_create_database {
            if !is_safe_identifier(&self.config.namespace)
                || !is_safe_identifier(&self.config.database)
            {
                return Err(SurrealSinkError::Internal(format!(
                    "auto_create_database requires namespace/database names in                      [A-Za-z0-9_.-]; got `{}`/`{}`",
                    self.config.namespace, self.config.database
                )));
            }
            // Root-scoped: the target database may not exist yet, so this
            // request carries no NS/DB headers.
            let ddl = format!(
                "DEFINE NAMESPACE IF NOT EXISTS ⟨{ns}⟩; USE NS ⟨{ns}⟩;                  DEFINE DATABASE IF NOT EXISTS ⟨{db}⟩;",
                ns = self.config.namespace,
                db = self.config.database
            );
            self.query_with_scope(&ddl, json!({}), false).await?;
        }
        self.query("RETURN 1;", json!({})).await?;
        for index in &self.config.vector_indexes {
            // Identifiers pass the safe-charset gate in `new`; ⟨⟩
            // escaping covers the dots routed table names contain.
            let name = format!(
                "vs_hnsw_{}_{}",
                index.table.replace(['.', '-'], "_"),
                index.field
            );
            let ddl = format!(
                "DEFINE INDEX IF NOT EXISTS ⟨{name}⟩ ON TABLE ⟨{}⟩ FIELDS ⟨{}⟩ \
                 HNSW DIMENSION {} DIST {};",
                index.table,
                index.field,
                index.dimension,
                index.distance.keyword()
            );
            self.query(&ddl, json!({})).await?;
            debug!(
                sink_id = %self.config.id,
                table = %index.table,
                field = %index.field,
                dimension = index.dimension,
                "surrealdb vector index ensured"
            );
        }
        Ok(())
    }

    /// Execute one RPC `query` call and return per-statement results.
    /// Every statement must report `status: "OK"`; the first failure is
    /// classified as transient (conflict/pressure) or blocking.
    async fn query(&self, sql: &str, vars: Value) -> Result<Vec<Value>, SurrealSinkError> {
        self.query_with_scope(sql, vars, true).await
    }

    async fn query_with_scope(
        &self,
        sql: &str,
        vars: Value,
        scoped: bool,
    ) -> Result<Vec<Value>, SurrealSinkError> {
        let body = json!({
            "method": "query",
            "params": [sql, vars],
        });
        let mut request = self
            .client
            .post(&self.rpc_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header(reqwest::header::ACCEPT, "application/json");
        if scoped {
            request = request
                .header("Surreal-NS", &self.config.namespace)
                .header("Surreal-DB", &self.config.database);
        }
        let response = request
            .json(&body)
            .send()
            .await
            .map_err(|err| SurrealSinkError::Transport(err.to_string()))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(classify_status(status, &text));
        }
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|err| SurrealSinkError::MalformedResponse(err.to_string()))?;
        if let Some(error) = parsed.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("rpc error with no message");
            return Err(classify_query_error(message));
        }
        let results = parsed
            .get("result")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                SurrealSinkError::MalformedResponse("rpc response has no result array".into())
            })?;
        for statement in &results {
            let ok = statement
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status == "OK");
            if !ok {
                let message = statement
                    .get("result")
                    .and_then(Value::as_str)
                    .unwrap_or("statement failed with no message");
                return Err(classify_query_error(message));
            }
        }
        Ok(results)
    }

    /// Execute one translated run as a single RPC call.
    async fn execute_run(&self, run: &Run) -> Result<(), SurrealSinkError> {
        let (sql, vars) = match &run.kind {
            RunKind::Upsert(pairs) => {
                let pairs: Vec<Value> = pairs
                    .iter()
                    .map(|(rid, doc)| json!({"rid": rid, "doc": doc}))
                    .collect();
                (
                    "FOR $pair IN $pairs { UPSERT type::record($tb, $pair.rid) CONTENT $pair.doc; };",
                    json!({"tb": run.table, "pairs": pairs}),
                )
            }
            RunKind::Delete(rids) => (
                "FOR $rid IN $rids { DELETE type::record($tb, $rid); };",
                json!({"tb": run.table, "rids": rids}),
            ),
            RunKind::Truncate => ("DELETE type::table($tb);", json!({"tb": run.table})),
        };
        self.query(sql, vars).await.map(|_| ())
    }

    /// Sequential runs with retry-from-failed-run. Committed runs stay
    /// committed; replaying the failed tail is idempotent by record id.
    async fn execute_runs(&self, runs: &[Run]) -> Result<(), SurrealSinkError> {
        if runs.is_empty() {
            return Ok(());
        }
        let mut schedule = BackoffSchedule::new(self.config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;
        let mut from = 0usize;
        loop {
            let mut failure: Option<(usize, SurrealSinkError)> = None;
            for (idx, run) in runs.iter().enumerate().skip(from) {
                if let Err(err) = self.execute_run(run).await {
                    failure = Some((idx, err));
                    break;
                }
            }
            let (idx, err) = match failure {
                None => {
                    if transient_failure.take().is_some() {
                        ventstream_telemetry::mark_sink_available();
                    }
                    return Ok(());
                }
                Some(found) => found,
            };
            if !err.is_retryable() {
                return Err(err);
            }
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
            let delay = jittered_delay(base_delay).min(self.config.retry.max_backoff);
            let retried_events: usize = runs.iter().skip(idx).map(|run| run.offsets.len()).sum();
            ventstream_telemetry::bump_sink_retries(
                u64::try_from(retried_events).unwrap_or(u64::MAX),
            );
            debug!(
                sink_id = %self.config.id,
                restart_from = idx,
                attempt = schedule.attempts_so_far(),
                ?delay,
                error = %err,
                "surrealdb run failed; retrying ordered tail after backoff"
            );
            tokio::time::sleep(delay).await;
            from = idx;
        }
    }

    fn mark_blocked(&self, error: &SurrealSinkError) {
        self.delivery_health.mark_blocked(error.to_string());
    }
}

fn classify_status(status: StatusCode, body: &str) -> SurrealSinkError {
    let message = truncate_body(body);
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => SurrealSinkError::Auth {
            status: status.as_u16(),
            message,
        },
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => SurrealSinkError::Server {
            status: status.as_u16(),
            message,
        },
        status if status.is_server_error() => SurrealSinkError::Server {
            status: status.as_u16(),
            message,
        },
        status => SurrealSinkError::Client {
            status: status.as_u16(),
            message,
        },
    }
}

fn classify_query_error(message: &str) -> SurrealSinkError {
    if is_transient_query_error(message) {
        SurrealSinkError::QueryTransient(truncate_body(message))
    } else {
        SurrealSinkError::QueryBlocked(truncate_body(message))
    }
}

#[async_trait::async_trait]
impl Sink for SurrealDbSink {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "surrealdb"
    }

    fn estimate_event_bytes(&self, event: &Event) -> usize {
        // Payload plus record id, canonical id field, and JSON framing.
        event.payload.as_slice().len() + 128
    }

    fn max_request_bytes(&self) -> Option<usize> {
        Some(self.config.batching.max_bytes)
    }

    fn recommended_concurrency(&self, _configured_ceiling: usize) -> usize {
        // No external document versioning: parallel batches could reorder
        // same-record writes, and concurrent transactions raise SurrealDB
        // conflict rates. Order is preserved by sequential runs.
        1
    }

    async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
        let events = batch.events();
        if events.is_empty() {
            return Ok(());
        }
        let translated = translate_batch(&self.config, events);

        if let Err(err) = self.execute_runs(&translated.runs).await {
            if matches!(
                err,
                SurrealSinkError::Auth { .. }
                    | SurrealSinkError::Client { .. }
                    | SurrealSinkError::QueryBlocked(_)
                    | SurrealSinkError::MalformedResponse(_)
                    | SurrealSinkError::Internal(_)
            ) {
                self.mark_blocked(&err);
            }
            return Err(match err {
                SurrealSinkError::Transport(msg) => SinkError::Connection(msg),
                SurrealSinkError::Server { status, message } => {
                    SinkError::Connection(format!("HTTP {status}: {message}"))
                }
                SurrealSinkError::QueryTransient(msg) => {
                    SinkError::Connection(format!("transient statement failure: {msg}"))
                }
                SurrealSinkError::Auth { status, message }
                | SurrealSinkError::Client { status, message } => {
                    SinkError::Blocked(format!("HTTP {status}: {message}"))
                }
                SurrealSinkError::QueryBlocked(msg) => {
                    SinkError::Blocked(format!("surrealdb rejected the batch: {msg}"))
                }
                SurrealSinkError::MalformedResponse(msg) => SinkError::Blocked(msg),
                SurrealSinkError::PartialFailure { .. } => SinkError::Internal(
                    "unexpected partial failure from a whole-run operation".into(),
                ),
                SurrealSinkError::Internal(msg) => SinkError::Internal(msg),
            });
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
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::super::config::{SurrealVectorDistance, SurrealVectorIndex};
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

    fn sink_against(endpoint: &str) -> SurrealDbSink {
        let mut config =
            SurrealDbConfig::new("test-surreal", endpoint, "vs", "app", "root", "root");
        config.retry = RetryConfig {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            backoff_factor: 2.0,
        };
        SurrealDbSink::new(config).expect("sink")
    }

    fn ok_response() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 1,
            "result": [{"result": null, "status": "OK", "time": "1ms"}],
        }))
    }

    fn err_response(message: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": 1,
            "result": [{"result": message, "status": "ERR", "time": "1ms"}],
        }))
    }

    #[tokio::test]
    async fn upsert_batch_sends_one_rpc_call_with_bound_pairs() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .and(body_partial_json(serde_json::json!({"method": "query"})))
            .respond_with(ok_response())
            .expect(1)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            upsert_event("orders", r#"public.orders:["1"]"#, r#"{"id":1,"item":"a"}"#),
            upsert_event("orders", r#"public.orders:["2"]"#, r#"{"id":2,"item":"b"}"#),
        ]);
        sink.write(batch).await.expect("write");

        let request: &Request = &server.received_requests().await.expect("requests")[0];
        let body: serde_json::Value = serde_json::from_slice(&request.body).expect("json");
        let sql = body["params"][0].as_str().expect("sql");
        assert!(sql.contains("UPSERT type::record($tb, $pair.rid) CONTENT $pair.doc"));
        let pairs = body["params"][1]["pairs"].as_array().expect("pairs");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0]["rid"], serde_json::json!(["1"]));
        // Source `id` column preserved as source_id; canonical id stamped.
        assert_eq!(pairs[0]["doc"]["source_id"], serde_json::json!(1));
        assert_eq!(
            pairs[0]["doc"]["_vs_id"],
            serde_json::json!(r#"public.orders:["1"]"#)
        );
        assert!(pairs[0]["doc"].get("id").is_none());
        // Auth and scoping headers ride every request.
        assert!(request.headers.get("authorization").is_some());
        assert_eq!(request.headers.get("surreal-ns").unwrap(), "vs");
        assert_eq!(request.headers.get("surreal-db").unwrap(), "app");
    }

    #[tokio::test]
    async fn transaction_conflict_retries_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(err_response(
                "The query was not executed due to a failed transaction. Transaction conflict",
            ))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(1)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"public.orders:["1"]"#,
            r#"{"v":1}"#,
        )]);
        sink.write(batch).await.expect("conflict must retry");
    }

    #[tokio::test]
    async fn schema_rejection_blocks_instead_of_retrying() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(err_response(
                "Found 'x' for field `qty`, with record `orders:1`, but expected a int",
            ))
            .expect(1)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![upsert_event(
            "orders",
            r#"public.orders:["1"]"#,
            r#"{"qty":"x"}"#,
        )]);
        let err = sink.write(batch).await.expect_err("must block");
        assert!(matches!(err, SinkError::Blocked(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn auth_failure_maps_to_blocked() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_string("There was a problem with authentication"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![delete_event("orders", r#"public.orders:["1"]"#)]);
        let err = sink.write(batch).await.expect_err("must block");
        assert!(matches!(err, SinkError::Blocked(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn missing_doc_id_rejects_that_event_only() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(1)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new("postgres.public.orders.update").expect("subject");
        let broken = Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .headers(
                Headers::empty().with_header("ventstream.cdc.relation".into(), "orders".into()),
            )
            .build();
        let good = upsert_event("orders", r#"public.orders:["1"]"#, r#"{"v":1}"#);
        let err = sink
            .write(SinkBatch::new(vec![broken, good]))
            .await
            .expect_err("must surface the reject");
        match err {
            SinkError::Rejected {
                batch_size,
                rejected_count,
                failed_items,
                ..
            } => {
                assert_eq!(batch_size, 2);
                assert_eq!(rejected_count, 1);
                assert_eq!(failed_items.expect("items")[0].offset, 0);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_ensures_declared_vector_indexes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(3)
            .mount(&server)
            .await;
        let mut config =
            SurrealDbConfig::new("test-surreal", server.uri(), "vs", "app", "root", "root");
        config.vector_indexes = vec![SurrealVectorIndex {
            table: "public.orders".into(),
            field: "embedding".into(),
            dimension: 384,
            distance: SurrealVectorDistance::Cosine,
        }];
        let sink = SurrealDbSink::new(config).expect("sink");
        sink.probe().await.expect("probe");

        let requests = server.received_requests().await.expect("requests");
        // Request order: ns/db ensure (unscoped) → RETURN 1 → index DDL.
        let ensure: serde_json::Value = serde_json::from_slice(&requests[0].body).expect("json");
        assert!(ensure["params"][0]
            .as_str()
            .expect("sql")
            .contains("DEFINE DATABASE IF NOT EXISTS"));
        assert!(requests[0].headers.get("surreal-ns").is_none());
        let ddl_body: serde_json::Value = serde_json::from_slice(&requests[2].body).expect("json");
        let sql = ddl_body["params"][0].as_str().expect("sql");
        assert!(sql.contains("DEFINE INDEX IF NOT EXISTS"));
        assert!(sql.contains("HNSW DIMENSION 384 DIST COSINE"));
        assert!(sql.contains("⟨public.orders⟩"));
    }

    #[tokio::test]
    async fn relocation_deletes_the_old_record_before_upserting() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(2)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new("postgres.public.orders.update").expect("subject");
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(br#"{"v":2}"#.to_vec()))
            .headers(
                Headers::empty()
                    .with_header("ventstream.doc.id".into(), r#"public.orders:["2"]"#.into())
                    .with_header(
                        "ventstream.doc.old_id".into(),
                        r#"public.orders:["1"]"#.into(),
                    )
                    .with_header("ventstream.cdc.relation".into(), "orders".into()),
            )
            .build();
        sink.write(SinkBatch::new(vec![event]))
            .await
            .expect("write");

        let requests = server.received_requests().await.expect("requests");
        let first: serde_json::Value = serde_json::from_slice(&requests[0].body).expect("json");
        let second: serde_json::Value = serde_json::from_slice(&requests[1].body).expect("json");
        assert!(first["params"][0].as_str().unwrap().contains("DELETE"));
        assert_eq!(first["params"][1]["rids"][0], serde_json::json!(["1"]));
        assert!(second["params"][0].as_str().unwrap().contains("UPSERT"));
    }
}
