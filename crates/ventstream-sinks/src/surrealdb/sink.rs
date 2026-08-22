//! The SurrealDB sink: ordered statement runs over the HTTP RPC protocol.
//!
//! SurrealDB commits synchronously — a successful RPC response IS the
//! delivery confirmation, so there is no asynchronous task layer. Runs
//! execute sequentially at concurrency 1 (no external document
//! versioning; parallel batches could reorder same-record writes). On a
//! retryable failure at run K, execution restarts from K — earlier runs
//! are already committed and replaying them is idempotent by record id.

use reqwest::StatusCode;
use std::sync::Arc;

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
    signin_url: String,
    /// Bearer token from `/signin`. Database- and namespace-scoped users
    /// cannot use HTTP basic auth (Surreal Cloud returns 401); every
    /// request authenticates with a signed-in token, refreshed once on
    /// expiry. Root users sign in the same way, so one path serves all.
    /// `Arc` so per-request access clones a pointer, not the JWT, and
    /// so 401 invalidation can identity-check the exact expired token.
    token: tokio::sync::RwLock<Option<Arc<str>>>,
    delivery_health: SinkHealth,
}

impl SurrealDbSink {
    /// Build the sink and HTTP client without touching the network.
    pub fn new(config: SurrealDbConfig) -> Result<Self, SurrealSinkError> {
        // Namespace/database ride as HTTP header values on every
        // request; a non-visible-ASCII byte would fail each send as a
        // retryable transport error instead of a config error.
        for (label, value) in [
            ("namespace", &config.namespace),
            ("database", &config.database),
        ] {
            if !value.bytes().all(|b| (0x21..=0x7E).contains(&b)) {
                return Err(SurrealSinkError::Internal(format!(
                    "{label} `{value}` contains characters that cannot travel as an \
                     HTTP header value (visible ASCII only)"
                )));
            }
        }
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
        if !config.table_prefix.is_empty() && !is_safe_identifier(&config.table_prefix) {
            // The prefix is concatenated into ⟨⟩-escaped identifier
            // positions (edge DDL, the RELATE arrow); a `⟩` in it would
            // escape the quoting.
            return Err(SurrealSinkError::Internal(format!(
                "table_prefix `{}` uses characters outside [A-Za-z0-9_.-]",
                config.table_prefix
            )));
        }
        if !config.graph_edges.is_empty()
            && matches!(
                config.table_routing,
                super::config::SurrealTableRouting::Fixed(_)
            )
        {
            // Graph nodes need per-relation tables — under fixed
            // routing every record id is fully qualified inside one
            // table and RELATE endpoints would never resolve.
            return Err(SurrealSinkError::Internal(
                "graph_edges requires by_output_relation table routing, not fixed".to_owned(),
            ));
        }
        if !config.graph_edges.is_empty()
            && matches!(
                config.table_routing,
                super::config::SurrealTableRouting::ByProjectionTarget
            )
        {
            // Edge specs match `from_table` against the CDC relation;
            // projection-target routing names tables by a different
            // header, so every spec would silently match nothing.
            return Err(SurrealSinkError::Internal(
                "graph_edges requires by_output_relation table routing; \
                 by_projection_target routes by a header edge specs do not match"
                    .to_owned(),
            ));
        }
        let mut edge_names = std::collections::HashSet::new();
        for spec in &config.graph_edges {
            // Edge table names are embedded in RELATE statements as
            // identifiers; endpoints and ids ride bind variables.
            if !is_safe_identifier(&spec.name)
                || !is_safe_identifier(&spec.from_table)
                || !is_safe_identifier(&spec.to_table)
            {
                return Err(SurrealSinkError::Internal(format!(
                    "graph edge `{}` ({} -> {}) uses characters outside [A-Za-z0-9_.-]",
                    spec.name, spec.from_table, spec.to_table
                )));
            }
            if spec.fk_columns.is_empty() {
                return Err(SurrealSinkError::Internal(format!(
                    "graph edge `{}` declares no fk_columns",
                    spec.name
                )));
            }
            if !edge_names.insert(spec.name.as_str()) {
                return Err(SurrealSinkError::Internal(format!(
                    "graph edge `{}` is declared more than once",
                    spec.name
                )));
            }
        }
        for spec in &config.graph_edges {
            // An edge table sharing a name with a routed document table
            // would interleave edge and document runs on one table —
            // and their lane assignments would disagree.
            if config
                .graph_edges
                .iter()
                .any(|other| spec.name == other.from_table || spec.name == other.to_table)
            {
                return Err(SurrealSinkError::Internal(format!(
                    "graph edge `{}` collides with a document table name",
                    spec.name
                )));
            }
        }
        let client = crate::util::build_http_client(
            config.request_timeout,
            config.verify_tls,
            config.ca_file.as_deref(),
        )
        .map_err(SurrealSinkError::Internal)?;
        let base = config.endpoint.trim_end_matches('/');
        let rpc_url = format!("{base}/rpc");
        let signin_url = format!("{base}/signin");
        let delivery_health = config.delivery_health.clone().unwrap_or_default();
        Ok(Self {
            config,
            client,
            rpc_url,
            signin_url,
            token: tokio::sync::RwLock::new(None),
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

    /// Startup probe: prove reachability/auth/scoping with one trivial
    /// query (optionally ensuring the namespace/database first when the
    /// operator opted in), then ensure declared vector indexes with
    /// idempotent DDL. A missing scope without the opt-in fails with
    /// the exact DDL to run.
    async fn probe(&self) -> Result<(), SurrealSinkError> {
        if self.config.auto_create_database {
            if !is_safe_identifier(&self.config.namespace)
                || !is_safe_identifier(&self.config.database)
            {
                return Err(SurrealSinkError::Internal(format!(
                    "auto_create_database requires namespace/database names in \
                     [A-Za-z0-9_.-]; got `{}`/`{}`",
                    self.config.namespace, self.config.database
                )));
            }
            // Root-scoped: the target database may not exist yet, so this
            // request carries no NS/DB headers.
            let ddl = format!(
                "DEFINE NAMESPACE IF NOT EXISTS ⟨{ns}⟩; USE NS ⟨{ns}⟩; \
                 DEFINE DATABASE IF NOT EXISTS ⟨{db}⟩;",
                ns = self.config.namespace,
                db = self.config.database
            );
            self.query_with_scope(&ddl, json!({}), false).await?;
        }
        if let Err(err) = self.query("RETURN 1;", json!({})).await {
            let text = err.to_string().to_ascii_lowercase();
            if text.contains("does not exist")
                && (text.contains("namespace") || text.contains("database"))
            {
                return Err(SurrealSinkError::QueryBlocked(format!(
                    "namespace `{ns}` / database `{db}` is not provisioned: {err}. Create \
                     them once (DEFINE NAMESPACE IF NOT EXISTS ⟨{ns}⟩; USE NS ⟨{ns}⟩; DEFINE \
                     DATABASE IF NOT EXISTS ⟨{db}⟩;) and grant the sink a database-scoped \
                     user, or set auto_create_database: true with namespace-level credentials",
                    ns = self.config.namespace,
                    db = self.config.database
                )));
            }
            return Err(err);
        }
        for spec in &self.config.graph_edges {
            // TYPE RELATION makes SurrealDB purge attached edges when
            // an endpoint record is deleted — the property the edge
            // lifecycle relies on. Not ENFORCED: snapshot batches may
            // RELATE endpoints whose records arrive later.
            let ddl = format!(
                "DEFINE TABLE IF NOT EXISTS ⟨{}{}⟩ TYPE RELATION;",
                self.config.table_prefix, spec.name
            );
            self.query(&ddl, json!({})).await?;
            debug!(
                sink_id = %self.config.id,
                edge = %spec.name,
                from = %spec.from_table,
                to = %spec.to_table,
                "surrealdb relation table ensured"
            );
        }
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

    /// Sign in and cache the bearer token, trying database-, then
    /// namespace-, then root-level credentials — the narrowest scope
    /// the user's authority satisfies wins.
    async fn signin(&self) -> Result<Arc<str>, SurrealSinkError> {
        let attempts = [
            json!({"user": self.config.username, "pass": self.config.password,
                   "ns": self.config.namespace, "db": self.config.database}),
            json!({"user": self.config.username, "pass": self.config.password,
                   "ns": self.config.namespace}),
            json!({"user": self.config.username, "pass": self.config.password}),
        ];
        let mut last = SurrealSinkError::Internal("no signin attempt ran".into());
        for body in attempts {
            let response = self
                .client
                .post(&self.signin_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|err| SurrealSinkError::Transport(err.to_string()))?;
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            if status.is_success() {
                let parsed: Value = serde_json::from_str(&text)
                    .map_err(|err| SurrealSinkError::MalformedResponse(err.to_string()))?;
                if let Some(token) = parsed.get("token").and_then(Value::as_str) {
                    let token: Arc<str> = Arc::from(token);
                    let mut guard = self.token.write().await;
                    *guard = Some(Arc::clone(&token));
                    return Ok(token);
                }
                return Err(SurrealSinkError::MalformedResponse(
                    "signin response has no token".into(),
                ));
            }
            last = classify_status(status, &text);
            if !matches!(last, SurrealSinkError::Auth { .. }) {
                return Err(last);
            }
        }
        Err(last)
    }

    async fn bearer(&self) -> Result<Arc<str>, SurrealSinkError> {
        if let Some(token) = self.token.read().await.as_ref().map(Arc::clone) {
            return Ok(token);
        }
        self.signin().await
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
        let body = super::cbor::encode(&body)
            .map(bytes::Bytes::from)
            .map_err(|err| SurrealSinkError::Internal(format!("serializing rpc body: {err}")))?;
        self.send_body(body, scoped).await
    }

    /// Send pre-serialized RPC bytes; classify HTTP and per-statement
    /// results. Tokens expire (1h default): one transparent re-signin,
    /// invalidating the cache only if it still holds the exact token
    /// that got the 401 — a sibling lane may already have refreshed it,
    /// and discarding its fresh token would cascade redundant signins.
    async fn send_body(
        &self,
        body: bytes::Bytes,
        scoped: bool,
    ) -> Result<Vec<Value>, SurrealSinkError> {
        let mut refreshed = false;
        let (status, raw) = loop {
            let token = self.bearer().await?;
            let mut request = self
                .client
                .post(&self.rpc_url)
                .bearer_auth(token.as_ref())
                .header(reqwest::header::ACCEPT, "application/cbor")
                .header(reqwest::header::CONTENT_TYPE, "application/cbor");
            if scoped {
                request = request
                    .header("Surreal-NS", &self.config.namespace)
                    .header("Surreal-DB", &self.config.database);
            }
            let response = request
                .body(body.clone())
                .send()
                .await
                .map_err(|err| SurrealSinkError::Transport(err.to_string()))?;
            let status = response.status();
            let raw = response.bytes().await.unwrap_or_default();
            if status == StatusCode::UNAUTHORIZED && !refreshed {
                refreshed = true;
                let mut guard = self.token.write().await;
                if guard
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &token))
                {
                    guard.take();
                }
                drop(guard);
                continue;
            }
            break (status, raw);
        };
        if !status.is_success() {
            // Error bodies may be CBOR or plain text; classification is
            // substring-based, and CBOR text segments are raw UTF-8, so
            // the lossy view preserves every match the classifier needs.
            return Err(classify_status(status, &String::from_utf8_lossy(&raw)));
        }
        let parsed: Value =
            super::cbor::decode_to_json(&raw).map_err(SurrealSinkError::MalformedResponse)?;
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

    /// Serialize one run's RPC body exactly once. Pairs and rids are
    /// borrowed straight into the serializer — no deep copies — and the
    /// returned bytes are reused verbatim across retry attempts.
    fn build_run_body(&self, run: &Run) -> Result<bytes::Bytes, SurrealSinkError> {
        #[derive(serde::Serialize)]
        struct Vars<'a> {
            tb: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            pairs: Option<&'a [Value]>,
            #[serde(skip_serializing_if = "Option::is_none")]
            rids: Option<&'a [Value]>,
            #[serde(skip_serializing_if = "Option::is_none")]
            atb: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            btb: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            edges: Option<&'a [Value]>,
        }
        #[derive(serde::Serialize)]
        struct Body<'a> {
            method: &'static str,
            params: (&'a str, Vars<'a>),
        }
        let mut vars = Vars {
            tb: &run.table,
            pairs: None,
            rids: None,
            atb: None,
            btb: None,
            edges: None,
        };
        // EdgeApply embeds the edge table as an identifier: RELATE's
        // arrow position cannot be parameterized. The name passed the
        // safe-identifier gate at construction; everything else rides
        // bind variables. Delete-then-RELATE is split into two FOR
        // statements because RELATE's uniqueness check does not see a
        // delete issued in the same statement (verified on 3.2).
        let edge_sql;
        let sql: &str = match &run.kind {
            RunKind::Upsert(pairs) => {
                vars.pairs = Some(pairs);
                "FOR $pair IN $pairs { UPSERT type::record($tb, $pair.rid) CONTENT $pair.doc; };"
            }
            RunKind::Delete(rids) => {
                vars.rids = Some(rids);
                "FOR $rid IN $rids { DELETE type::record($tb, $rid); };"
            }
            RunKind::Truncate => "DELETE type::table($tb);",
            RunKind::EdgeApply {
                a_table,
                b_table,
                items,
            } => {
                vars.atb = Some(a_table);
                vars.btb = Some(b_table);
                vars.edges = Some(items);
                edge_sql = format!(
                    "FOR $e IN $edges {{ DELETE type::record($tb, $e.eid); }}; \
                     FOR $e IN $edges {{ RELATE (type::record($atb, $e.a))->⟨{}⟩->\
                     (type::record($btb, $e.b)) CONTENT {{ id: type::record($tb, $e.eid) }}; }};",
                    run.table
                );
                &edge_sql
            }
        };
        let body = Body {
            method: "query",
            params: (sql, vars),
        };
        super::cbor::encode(&body)
            .map(bytes::Bytes::from)
            .map_err(|err| SurrealSinkError::Internal(format!("serializing run body: {err}")))
    }

    /// Execute a batch's runs: strictly ordered within each routed
    /// table, concurrently across tables. Ordering only matters
    /// per-record — and every run for a record lives in its table's
    /// lane — so distinct tables draining in parallel is safe and
    /// collapses the serial per-request round-trip cost that dominates
    /// multi-table pipelines writing over the internet (a 4-projection
    /// Neo4j pipeline pays 1 RTT instead of 4 per flush).
    async fn execute_runs(&self, runs: &[Run]) -> Result<(), SurrealSinkError> {
        if runs.is_empty() {
            return Ok(());
        }
        let mut lanes: Vec<(&str, Vec<&Run>)> = Vec::new();
        for run in runs {
            match lanes
                .iter_mut()
                .find(|(lane_key, _)| *lane_key == run.lane_key())
            {
                Some((_, lane)) => lane.push(run),
                None => lanes.push((run.lane_key(), vec![run])),
            }
        }
        if lanes.len() == 1 {
            let (_, lane) = lanes.remove(0);
            if self.execute_lane(&lane).await? {
                ventstream_telemetry::mark_sink_available();
            }
            return Ok(());
        }
        let outcomes =
            futures_util::future::join_all(lanes.iter().map(|(_, lane)| self.execute_lane(lane)))
                .await;
        // Every lane ran to completion or its own terminal error;
        // committed lanes replay idempotently if the batch retries.
        // Availability flips back only here, once ALL lanes finished —
        // one recovered lane must not mask siblings still failing.
        let recovered = outcomes
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .any(|had_failure| had_failure);
        if recovered {
            ventstream_telemetry::mark_sink_available();
        }
        Ok(())
    }

    /// One lane's runs. Order between runs only matters when they can
    /// touch the same record — so a lane whose runs are all upserts
    /// with pairwise-disjoint id sets (every bootstrap batch, and most
    /// tail batches) executes them concurrently, paying one round-trip
    /// instead of one per run. Any overlap, delete, or truncate falls
    /// back to the strictly sequential path.
    async fn execute_lane(&self, runs: &[&Run]) -> Result<bool, SurrealSinkError> {
        if runs.len() > 1 && upsert_runs_are_disjoint(runs) {
            let outcomes = futures_util::future::join_all(
                runs.iter().map(|run| self.execute_run_with_retry(run)),
            )
            .await;
            let mut had_failure = false;
            for outcome in outcomes {
                had_failure |= outcome?;
            }
            return Ok(had_failure);
        }
        self.execute_lane_sequential(runs).await
    }

    /// One independent run with its own retry budget; used by the
    /// parallel lane path. Retries are idempotent by record id.
    async fn execute_run_with_retry(&self, run: &Run) -> Result<bool, SurrealSinkError> {
        let body = self.build_run_body(run)?;
        let mut schedule = BackoffSchedule::new(self.config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;
        let mut had_failure = false;
        loop {
            match self.send_body(body.clone(), true).await {
                Ok(_) => {
                    drop(transient_failure.take());
                    return Ok(had_failure);
                }
                Err(err) if err.is_retryable() => {
                    had_failure = true;
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
                    ventstream_telemetry::bump_sink_retries(
                        u64::try_from(run.offsets.len()).unwrap_or(u64::MAX),
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Sequential runs with retry-from-failed-run. Committed runs stay
    /// committed; replaying the failed tail is idempotent by record id.
    /// Returns whether the lane went through a transient failure, so
    /// the caller flips process-wide availability only once every lane
    /// has recovered (not while siblings are still backing off).
    async fn execute_lane_sequential(&self, runs: &[&Run]) -> Result<bool, SurrealSinkError> {
        let mut schedule = BackoffSchedule::new(self.config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;
        let mut had_failure = false;
        let mut bodies: Vec<Option<bytes::Bytes>> = vec![None; runs.len()];
        let mut from = 0usize;
        loop {
            let mut failure: Option<(usize, SurrealSinkError)> = None;
            for (idx, run) in runs.iter().enumerate().skip(from) {
                let cached = bodies.get_mut(idx).ok_or_else(|| {
                    SurrealSinkError::Internal("run body cache index out of range".into())
                })?;
                let body = match cached {
                    Some(body) => body.clone(),
                    None => {
                        let body = self.build_run_body(run)?;
                        *cached = Some(body.clone());
                        body
                    }
                };
                if let Err(err) = self.send_body(body, true).await.map(|_| ()) {
                    failure = Some((idx, err));
                    break;
                }
            }
            let (idx, err) = match failure {
                None => {
                    drop(transient_failure.take());
                    return Ok(had_failure);
                }
                Some(found) => found,
            };
            if !err.is_retryable() {
                return Err(err);
            }
            had_failure = true;
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

/// True when every run is an upsert and no record id appears in more
/// than one run — the condition under which run order is vacuously
/// irrelevant and concurrent execution is safe for any source.
fn upsert_runs_are_disjoint(runs: &[&Run]) -> bool {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for run in runs {
        let RunKind::Upsert(pairs) = &run.kind else {
            return false;
        };
        for pair in pairs {
            let Some(rid) = pair.get("rid") else {
                return false;
            };
            if !seen.insert(rid.to_string()) {
                return false;
            }
        }
    }
    true
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
    use wiremock::matchers::{method, path};
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

    async fn rpc_requests(server: &MockServer) -> Vec<Request> {
        server
            .received_requests()
            .await
            .expect("requests")
            .into_iter()
            .filter(|request| request.url.path() == "/rpc")
            .collect()
    }

    async fn mount_signin(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/signin"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"code": 200, "token": "test-token"})),
            )
            .mount(server)
            .await;
    }

    fn cbor_response(body: &serde_json::Value) -> ResponseTemplate {
        let bytes = super::super::cbor::encode(body).expect("cbor response");
        ResponseTemplate::new(200).set_body_raw(bytes, "application/cbor")
    }

    fn ok_response() -> ResponseTemplate {
        cbor_response(&serde_json::json!({
            "id": 1,
            "result": [{"result": null, "status": "OK", "time": "1ms"}],
        }))
    }

    fn err_response(message: &str) -> ResponseTemplate {
        cbor_response(&serde_json::json!({
            "id": 1,
            "result": [{"result": message, "status": "ERR", "time": "1ms"}],
        }))
    }

    #[tokio::test]
    async fn upsert_batch_sends_one_rpc_call_with_bound_pairs() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
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

        let requests = rpc_requests(&server).await;
        let request: &Request = &requests[0];
        let body: serde_json::Value =
            super::super::cbor::decode_to_json(&request.body).expect("cbor");
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
        mount_signin(&server).await;
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
        mount_signin(&server).await;
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
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_string("There was a problem with authentication"),
            )
            // First 401 triggers one transparent token refresh + retry.
            .expect(2)
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
        mount_signin(&server).await;
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
    async fn disjoint_same_table_runs_execute_in_parallel() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response().set_delay(Duration::from_millis(100)))
            .expect(3)
            .mount(&server)
            .await;
        let mut config =
            SurrealDbConfig::new("test-surreal", server.uri(), "vs", "app", "root", "root");
        config.batching.max_docs = 1; // one run per event
        config.retry = RetryConfig {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            backoff_factor: 2.0,
        };
        let sink = SurrealDbSink::new(config).expect("sink");
        let batch = SinkBatch::new(vec![
            upsert_event("orders", r#"o:["1"]"#, r#"{"v":1}"#),
            upsert_event("orders", r#"o:["2"]"#, r#"{"v":2}"#),
            upsert_event("orders", r#"o:["3"]"#, r#"{"v":3}"#),
        ]);
        let started = std::time::Instant::now();
        sink.write(batch).await.expect("write");
        // Three disjoint runs, one table: serial would cost >= 300ms.
        // Generous margin over the ~100ms parallel ideal for loaded CI.
        assert!(
            started.elapsed() < Duration::from_millis(280),
            "disjoint runs did not parallelize: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn overlapping_runs_stay_strictly_sequential() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response().set_delay(Duration::from_millis(60)))
            .expect(2)
            .mount(&server)
            .await;
        let mut config =
            SurrealDbConfig::new("test-surreal", server.uri(), "vs", "app", "root", "root");
        config.batching.max_docs = 1;
        let sink = SurrealDbSink::new(config).expect("sink");
        // Same record twice: order is semantic, must not overlap.
        let batch = SinkBatch::new(vec![
            upsert_event("orders", r#"o:["1"]"#, r#"{"v":1}"#),
            upsert_event("orders", r#"o:["1"]"#, r#"{"v":2}"#),
        ]);
        let started = std::time::Instant::now();
        sink.write(batch).await.expect("write");
        assert!(
            started.elapsed() >= Duration::from_millis(115),
            "overlapping runs overlapped: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn declared_lookup_fields_are_materialized_on_documents() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(1)
            .mount(&server)
            .await;
        let mut config =
            SurrealDbConfig::new("test-surreal", server.uri(), "vs", "app", "root", "root");
        config.lookup_fields = vec![super::super::config::SurrealLookupField {
            table: "orders".into(),
            field: "items.item_id".into(),
        }];
        let sink = SurrealDbSink::new(config).expect("sink");
        let payload = r#"{"order_id":7,"items":[{"item_id":41},{"item_id":52}]}"#;
        sink.write(SinkBatch::new(vec![upsert_event(
            "orders",
            r#"public.orders:["7"]"#,
            payload,
        )]))
        .await
        .expect("write");
        let requests = rpc_requests(&server).await;
        let body: serde_json::Value =
            super::super::cbor::decode_to_json(&requests[0].body).expect("cbor");
        let doc = &body["params"][1]["pairs"][0]["doc"];
        assert_eq!(
            doc["_vs_lx_items_item_id"],
            serde_json::json!(["41", "52"]),
            "materialized lookup values wrong: {doc}"
        );
    }

    #[test]
    fn unsafe_table_prefix_is_rejected_at_construction() {
        // The prefix rides inside ⟨⟩-escaped identifiers for edge DDL
        // and RELATE statements — `⟩` must never reach that position.
        let mut config = SurrealDbConfig::new("s", "http://x", "vs", "app", "u", "p");
        config.table_prefix = "evil⟩; REMOVE TABLE users; --".to_owned();
        let err = match SurrealDbSink::new(config) {
            Err(err) => err,
            Ok(_) => panic!("unsafe table_prefix must be rejected"),
        };
        assert!(err.to_string().contains("table_prefix"), "{err}");
    }

    #[tokio::test]
    async fn probe_ensures_declared_vector_indexes() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(3)
            .mount(&server)
            .await;
        let mut config =
            SurrealDbConfig::new("test-surreal", server.uri(), "vs", "app", "root", "root");
        config.auto_create_database = true;
        config.vector_indexes = vec![SurrealVectorIndex {
            table: "public.orders".into(),
            field: "embedding".into(),
            dimension: 384,
            distance: SurrealVectorDistance::Cosine,
        }];
        let sink = SurrealDbSink::new(config).expect("sink");
        sink.probe().await.expect("probe");

        let requests = rpc_requests(&server).await;
        // Request order: ns/db ensure (unscoped) → RETURN 1 → index DDL.
        let ensure: serde_json::Value =
            super::super::cbor::decode_to_json(&requests[0].body).expect("cbor");
        assert!(ensure["params"][0]
            .as_str()
            .expect("sql")
            .contains("DEFINE DATABASE IF NOT EXISTS"));
        assert!(requests[0].headers.get("surreal-ns").is_none());
        let ddl_body: serde_json::Value =
            super::super::cbor::decode_to_json(&requests[2].body).expect("cbor");
        let sql = ddl_body["params"][0].as_str().expect("sql");
        assert!(sql.contains("DEFINE INDEX IF NOT EXISTS"));
        assert!(sql.contains("HNSW DIMENSION 384 DIST COSINE"));
        assert!(sql.contains("⟨public.orders⟩"));
    }

    #[tokio::test]
    async fn distinct_tables_write_in_parallel_lanes() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response().set_delay(Duration::from_millis(100)))
            .expect(4)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let batch = SinkBatch::new(vec![
            upsert_event("t1", r#"a:["1"]"#, r#"{"v":1}"#),
            upsert_event("t2", r#"b:["1"]"#, r#"{"v":1}"#),
            upsert_event("t3", r#"c:["1"]"#, r#"{"v":1}"#),
            upsert_event("t4", r#"d:["1"]"#, r#"{"v":1}"#),
        ]);
        let started = std::time::Instant::now();
        sink.write(batch).await.expect("write");
        let elapsed = started.elapsed();
        // Serial lanes would cost >= 4 x 100ms; parallel lanes pay ~1 x.
        assert!(
            elapsed < Duration::from_millis(360),
            "lanes did not run concurrently: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn rejected_relocation_event_issues_no_delete() {
        // A key-changing update with a malformed payload must NOT
        // delete the old record on its way to the DLQ — that would
        // destroy data on behalf of a failed delivery.
        let server = MockServer::start().await;
        mount_signin(&server).await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ok_response())
            .expect(0)
            .mount(&server)
            .await;
        let sink = sink_against(&server.uri());
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new("postgres.public.orders.update").expect("subject");
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(b"not json".to_vec()))
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
        let err = sink
            .write(SinkBatch::new(vec![event]))
            .await
            .expect_err("must reject");
        assert!(matches!(err, SinkError::Rejected { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn relocation_deletes_the_old_record_before_upserting() {
        let server = MockServer::start().await;
        mount_signin(&server).await;
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

        let requests = rpc_requests(&server).await;
        let first: serde_json::Value =
            super::super::cbor::decode_to_json(&requests[0].body).expect("cbor");
        let second: serde_json::Value =
            super::super::cbor::decode_to_json(&requests[1].body).expect("cbor");
        assert!(first["params"][0].as_str().unwrap().contains("DELETE"));
        assert_eq!(first["params"][1]["rids"][0], serde_json::json!(["1"]));
        assert!(second["params"][0].as_str().unwrap().contains("UPSERT"));
    }
}
