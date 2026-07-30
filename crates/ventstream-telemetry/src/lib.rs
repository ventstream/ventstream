//! Outbound telemetry to the VentStream control plane.
//!
//! Designed to be **invisible to engine performance**:
//!
//! - All hot-path interactions are atomic counter increments
//!   ([`TelemetryCounters`]). Counter ops are `fetch_add` with
//!   `Relaxed` ordering — nanoseconds per call, lock-free.
//! - HTTP calls happen exclusively on a background tokio task spawned by
//!   [`TelemetryHandle::spawn`] (the layer + handle are built up front by
//!   [`build_telemetry`]). The engine's CDC + dispatcher tasks never block on
//!   the control plane.
//! - Trace events are sampled via a custom [`tracing_subscriber`]
//!   layer (only metric-tagged DEBUG events; default 1% rate). They
//!   are dropped if the in-memory buffer is full, not buffered
//!   unboundedly.
//! - If the control plane is unreachable or slow, the engine keeps
//!   running. We log a `WARN` at most once every 30s about each
//!   failure mode.
//!
//! ### Env vars
//!
//! - `VS_CONTROL_PLANE_URL` — base URL, e.g.
//!   `https://control.ventstream.io`. When unset, the crate becomes
//!   a no-op (the engine logs that telemetry is disabled and moves on).
//! - `VS_CONTROL_PLANE_KEY` — bearer token issued by the UI when an
//!   agent is registered.
//! - `VS_AGENT_NAME` — required only for unbound keys' first
//!   contact. Once bound the control plane resolves by key alone.
//! - `VS_CONTROL_PLANE_INTERVAL_SECS` — metrics post cadence
//!   (default 30).
//! - `VS_CONTROL_PLANE_TRACE_SAMPLE_RATE` — sampling rate for
//!   debug-level metric-tagged events (default 0.01).
//! - `VS_CONTROL_PLANE_TRACE_BUFFER` — bounded buffer size, dropped
//!   when full (default 1024).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::Utc;
use rand::Rng;
use serde::Serialize;
use tracing::{debug, info, warn};
use ventstream_core::ShutdownToken;

mod tracing_layer;
pub use tracing_layer::TelemetryTraceLayer;

/// Shared atomic counters the engine bumps as work happens. The
/// telemetry task reads them every 30s, takes a delta against its
/// last snapshot, and posts rate-shaped values.
///
/// `Arc<TelemetryCounters>` is cheap to clone — pass it into every
/// subsystem that needs to bump. None of the methods allocate.
#[derive(Debug, Default)]
pub struct TelemetryCounters {
    /// Total events emitted from any source to the bus.
    pub events_emitted: AtomicU64,
    /// Total events accepted by the dispatcher for delivery to the sink.
    pub events_received: AtomicU64,
    /// Total events acknowledged by the sink.
    pub events_delivered: AtomicU64,
    /// Total exact permanent item failures reported by the sink.
    pub events_failed: AtomicU64,
    /// Total event delivery retries attempted by a sink.
    pub sink_retries: AtomicU64,
    /// Total DLQ writes that SUCCEEDED (event durably routed to DLQ).
    /// Lifetime-cumulative counter, not a depth gauge.
    pub dlq_writes: AtomicU64,
    /// Total DLQ writes that FAILED (event could not be dead-lettered —
    /// genuine loss). Previously log-only and therefore un-alertable (M16).
    pub dlq_write_failures: AtomicU64,
    /// Total backpressure occurrences (bus was full when source
    /// tried to send).
    pub backpressure_events: AtomicU64,
    /// Latest bulk-write p95 latency in milliseconds (the
    /// dispatcher updates this on each batch).
    pub bulk_p95_ms: AtomicU64,
    /// Latest bulk-write p50 latency in milliseconds.
    pub bulk_p50_ms: AtomicU64,
    /// Bounded recent sink-write durations used to calculate real percentiles.
    bulk_latency_samples: Mutex<VecDeque<u64>>,
    /// Latest cursor age in milliseconds (source-specific; for
    /// Neo4j this is `now - last_event_sampled_at`; for PG it's
    /// `wal_high_water - last_acked_lsn` translated to ms via
    /// LSN-to-time mapping — we use a coarse approximation).
    pub cursor_age_ms: AtomicU64,
    /// Unix millis for the newest event presented to the sink.
    pub last_input_at_ms: AtomicU64,
    /// Unix millis for the newest successful sink acknowledgement.
    pub last_output_at_ms: AtomicU64,
    /// Unix millis when the current sink outage began; zero while available.
    pub sink_unavailable_since_ms: AtomicU64,
    /// Current lifecycle phase as a [`LifecyclePhase`] discriminant.
    /// Source code calls [`set_phase`] when state transitions occur
    /// (snapshot start, snapshot complete, tail mode, fatal error).
    pub current_phase: AtomicU8,
    /// Unix millis when the most recent error was recorded; `0` when
    /// no error has been recorded since process start.
    pub last_error_at_ms: AtomicU64,
    /// The most recent error message. Written via [`record_error`];
    /// cleared via [`clear_error`] when the agent recovers (e.g.
    /// re-enters tail mode after a transient connect failure).
    pub last_error_message: Mutex<Option<String>>,
    /// The source backend this agent runs (`postgres` / `neo4j`). Set
    /// once at startup via [`set_source`]; lets the control plane label
    /// the agent without asking at registration time.
    pub source_kind: Mutex<Option<String>>,
    /// The sink this agent writes to (`opensearch`). Set once at startup
    /// via [`set_target`]; shown next to the source so the control plane
    /// displays the whole pipeline shape.
    pub target_kind: Mutex<Option<String>>,
}

/// Coarse-grained lifecycle phase the agent reports to the control
/// plane on every heartbeat. The values are stable wire identifiers
/// — they appear verbatim in the control plane DB and UI — so don't
/// rename them without coordinating a server-side rename.
///
/// The numeric discriminants are stored in [`TelemetryCounters::current_phase`]
/// as a `u8`; keep them small and contiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LifecyclePhase {
    /// Default value before any source code has reported. Treated as
    /// "process is alive but hasn't gotten far enough to know."
    Starting = 0,
    /// Snapshot bootstrap is in progress (initial table scan to seed
    /// downstream sink + join state).
    Bootstrapping = 1,
    /// Tail mode — consuming logical-replication / cursor events as
    /// they arrive. The healthy steady state.
    Tailing = 2,
    /// A fatal or transient failure was recorded via [`record_error`].
    /// The next successful tail-mode iteration calls [`clear_error`]
    /// and transitions back to [`LifecyclePhase::Tailing`].
    Erroring = 3,
    /// Shutdown signal received; engine is winding down.
    Stopped = 4,
    /// Operator-initiated pause. The source is disconnected but the
    /// upstream cursor (PG slot / Neo4j sequence) is retained so a
    /// later resume picks up exactly where we left off. The control
    /// plane sets a `pauseExpiresAt` (24h default); once that fires
    /// the agent transitions to `Drained`.
    Paused = 5,
    /// Pause exceeded its TTL, so the source cursor has been dropped
    /// (PG: `pg_drop_replication_slot`; Neo4j: cursor file wiped).
    /// Resuming from this state requires a full re-bootstrap and the
    /// frontend confirms via modal before triggering the action.
    Drained = 6,
}

impl LifecyclePhase {
    /// Stable wire name. Used as the JSON value in the telemetry
    /// payload and stored verbatim on `MetricSample.phase`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Bootstrapping => "bootstrapping",
            Self::Tailing => "tailing",
            Self::Erroring => "erroring",
            Self::Stopped => "stopped",
            Self::Paused => "paused",
            Self::Drained => "drained",
        }
    }

    /// Round-trip a `u8` back to the enum. Out-of-range values fall
    /// back to [`LifecyclePhase::Starting`] rather than panicking —
    /// the telemetry path must never crash because a counter held an
    /// unexpected byte.
    pub const fn from_u8(b: u8) -> Self {
        match b {
            1 => Self::Bootstrapping,
            2 => Self::Tailing,
            3 => Self::Erroring,
            4 => Self::Stopped,
            5 => Self::Paused,
            6 => Self::Drained,
            _ => Self::Starting,
        }
    }
}

impl TelemetryCounters {
    /// Allocate a fresh, zeroed counter set behind an `Arc` for sharing
    /// across the engine's tasks.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn snapshot(&self) -> RawSnapshot {
        let error_message = self.last_error_message.lock().ok().and_then(|g| g.clone());
        let source_kind = self.source_kind.lock().ok().and_then(|g| g.clone());
        let target_kind = self.target_kind.lock().ok().and_then(|g| g.clone());
        RawSnapshot {
            events_emitted: self.events_emitted.load(Ordering::Relaxed),
            dlq_writes: self.dlq_writes.load(Ordering::Relaxed),
            dlq_write_failures: self.dlq_write_failures.load(Ordering::Relaxed),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
            bulk_p95_ms: self.bulk_p95_ms.load(Ordering::Relaxed),
            bulk_p50_ms: self.bulk_p50_ms.load(Ordering::Relaxed),
            cursor_age_ms: self.cursor_age_ms.load(Ordering::Relaxed),
            phase: LifecyclePhase::from_u8(self.current_phase.load(Ordering::Relaxed)),
            last_error_at_ms: self.last_error_at_ms.load(Ordering::Relaxed),
            last_error_message: error_message,
            source_kind,
            target_kind,
        }
    }
}

#[derive(Debug, Clone)]
struct RawSnapshot {
    events_emitted: u64,
    dlq_writes: u64,
    dlq_write_failures: u64,
    backpressure_events: u64,
    bulk_p95_ms: u64,
    bulk_p50_ms: u64,
    cursor_age_ms: u64,
    phase: LifecyclePhase,
    last_error_at_ms: u64,
    last_error_message: Option<String>,
    source_kind: Option<String>,
    target_kind: Option<String>,
}

/// Config resolved from env vars at startup.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Base URL of the control plane (trailing slash trimmed).
    pub control_plane_url: String,
    /// API key sent with each telemetry push.
    pub api_key: String,
    /// Optional agent name (`VS_AGENT_NAME`); identifies this agent in the
    /// control plane.
    pub agent_name: Option<String>,
    /// How often metrics/traces are flushed to the control plane.
    pub interval: Duration,
    /// Fraction of traces to sample, clamped to `0.0..=1.0`.
    pub trace_sample_rate: f32,
    /// Bounded capacity of the in-process trace channel.
    pub trace_buffer: usize,
    /// This agent's build version (`CARGO_PKG_VERSION`).
    pub agent_version: String,
}

impl TelemetryConfig {
    /// Resolve config from env. Returns `None` when
    /// `VS_CONTROL_PLANE_URL` or `VS_CONTROL_PLANE_KEY` is unset —
    /// telemetry is opt-in by env presence.
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("VS_CONTROL_PLANE_URL").ok()?;
        let key = std::env::var("VS_CONTROL_PLANE_KEY").ok()?;
        let agent_name = std::env::var("VS_AGENT_NAME").ok();
        let interval_secs = std::env::var("VS_CONTROL_PLANE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let trace_sample_rate = std::env::var("VS_CONTROL_PLANE_TRACE_SAMPLE_RATE")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(0.01)
            .clamp(0.0, 1.0);
        let trace_buffer = std::env::var("VS_CONTROL_PLANE_TRACE_BUFFER")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024);
        let agent_version = env!("CARGO_PKG_VERSION").to_owned();
        Some(Self {
            control_plane_url: url.trim_end_matches('/').to_owned(),
            api_key: key,
            agent_name,
            interval: Duration::from_secs(interval_secs),
            trace_sample_rate,
            trace_buffer,
            agent_version,
        })
    }
}

/// One trace event captured by the [`TelemetryTraceLayer`] and
/// pushed onto the channel for batch upload.
#[derive(Debug, Clone, Serialize)]
pub struct TraceRecord {
    /// RFC 3339 timestamp when the event was sampled.
    #[serde(rename = "sampledAt")]
    pub sampled_at: String,
    /// Metric / event name (the tracing target or `metric=` field).
    pub metric: String,
    /// Log severity (`info`, `warn`, `error`, …).
    pub severity: String,
    /// Operation duration in milliseconds, when the event carries one.
    #[serde(rename = "elapsedMs", skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    /// Source kind (`postgres`, `neo4j`), when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Remaining structured key/value fields captured from the span/event.
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct MetricsPayload<'a> {
    #[serde(rename = "sampledAt")]
    sampled_at: String,
    #[serde(rename = "agentVersion")]
    agent_version: &'a str,
    #[serde(rename = "agentName", skip_serializing_if = "Option::is_none")]
    agent_name: Option<&'a str>,
    metrics: MetricsBody,
}

#[derive(Debug, Serialize)]
struct MetricsBody {
    #[serde(rename = "eventsPerSec")]
    events_per_sec: f64,
    #[serde(rename = "cursorAgeMs", skip_serializing_if = "Option::is_none")]
    cursor_age_ms: Option<u64>,
    /// Lifetime-cumulative count of successful DLQ writes. Renamed from the
    /// misleading `dlqDepth` (M16) — it only ever rises, even as the DLQ
    /// drains, so it was never a "depth" gauge.
    #[serde(rename = "dlqWritesTotal")]
    dlq_writes_total: u64,
    /// Lifetime-cumulative count of FAILED DLQ writes (genuine loss). Lets the
    /// control plane alert on un-dead-letterable events (M16).
    #[serde(rename = "dlqWriteFailuresTotal")]
    dlq_write_failures_total: u64,
    #[serde(rename = "rssMb")]
    rss_mb: u64,
    #[serde(rename = "bulkLatencyP50Ms", skip_serializing_if = "Option::is_none")]
    bulk_latency_p50_ms: Option<u64>,
    #[serde(rename = "bulkLatencyP95Ms", skip_serializing_if = "Option::is_none")]
    bulk_latency_p95_ms: Option<u64>,
    #[serde(rename = "busBackpressureCount")]
    bus_backpressure_count: u64,
    /// Source backend (`postgres` / `neo4j`). Lets the control plane
    /// label the agent without capturing it at registration. Omitted
    /// until the source has reported it.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// Sink the agent writes to (`opensearch`). Omitted until reported.
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
    /// Current lifecycle phase (`starting` / `bootstrapping` /
    /// `tailing` / `erroring` / `stopped`). Always present — see
    /// [`LifecyclePhase::as_str`].
    phase: &'static str,
    /// Most recent error message, if [`record_error`] was called and
    /// not yet cleared by [`clear_error`]. Omitted from the JSON when
    /// absent.
    #[serde(rename = "lastErrorMessage", skip_serializing_if = "Option::is_none")]
    last_error_message: Option<String>,
    /// ISO-8601 timestamp of the most recent error. Present iff
    /// `lastErrorMessage` is present.
    #[serde(rename = "lastErrorAt", skip_serializing_if = "Option::is_none")]
    last_error_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct TracesPayload<'a> {
    #[serde(rename = "agentName", skip_serializing_if = "Option::is_none")]
    agent_name: Option<&'a str>,
    events: Vec<TraceRecord>,
}

/// Deferred export-loop spawn, paired with the trace layer by
/// [`build_telemetry`]. Holds the resolved config + the trace receiver so the
/// loop can be started *after* the tracing subscriber is installed.
pub struct TelemetryHandle {
    cfg: TelemetryConfig,
    trace_rx: tokio::sync::mpsc::Receiver<TraceRecord>,
}

/// Build the trace-sampling layer + a [`TelemetryHandle`] to spawn the export
/// loop later. Returns `None` when telemetry isn't configured (no
/// `VS_CONTROL_PLANE_URL`).
///
/// Needs **no** tokio runtime — call it in `main()` BEFORE installing the
/// tracing subscriber and pass the returned layer into the subscriber stack,
/// so DEBUG metric-tagged events actually feed the trace pipeline. (Previously
/// the layer was created after the subscriber was `.init()`'d and dropped, so
/// the trace path ran with permanently empty batches — M14.) Then call
/// [`TelemetryHandle::spawn`] once the runtime exists.
pub fn build_telemetry() -> Option<(TelemetryTraceLayer, TelemetryHandle)> {
    let cfg = TelemetryConfig::from_env()?;
    let (trace_tx, trace_rx) = tokio::sync::mpsc::channel::<TraceRecord>(cfg.trace_buffer);
    let layer = TelemetryTraceLayer::new(trace_tx, cfg.trace_sample_rate);
    Some((layer, TelemetryHandle { cfg, trace_rx }))
}

impl TelemetryHandle {
    /// Spawn the control-plane export loop. Requires a tokio runtime; the task
    /// lives until `shutdown` fires.
    pub fn spawn(self, counters: Arc<TelemetryCounters>, shutdown: ShutdownToken) {
        info!(
            url = %self.cfg.control_plane_url,
            interval_secs = self.cfg.interval.as_secs(),
            sample_rate = self.cfg.trace_sample_rate,
            "telemetry: control-plane integration enabled"
        );
        tokio::spawn(run_telemetry_loop(
            self.cfg,
            counters,
            self.trace_rx,
            shutdown,
        ));
    }
}

async fn run_telemetry_loop(
    cfg: TelemetryConfig,
    counters: Arc<TelemetryCounters>,
    mut trace_rx: tokio::sync::mpsc::Receiver<TraceRecord>,
    shutdown: ShutdownToken,
) {
    let client = match build_http_client() {
        Ok(c) => c,
        Err(err) => {
            warn!(error = %err, "telemetry: failed to build http client; telemetry disabled for this process");
            return;
        }
    };

    let mut metrics_timer = tokio::time::interval(cfg.interval);
    metrics_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = metrics_timer.tick().await; // drop first immediate tick

    let mut last_snapshot = counters.snapshot();
    let mut last_at = std::time::Instant::now();

    let mut sysinfo = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().ok();

    // Bounded local buffer; flushed each interval.
    let mut trace_batch: Vec<TraceRecord> = Vec::with_capacity(256);

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                debug!("telemetry: shutdown requested");
                // Best-effort final flush.
                if !trace_batch.is_empty() {
                    let _ = post_traces(&client, &cfg, &trace_batch).await;
                }
                return;
            }
            // Drain trace records onto the batch as they arrive.
            recv = trace_rx.recv() => {
                match recv {
                    Some(rec) => {
                        trace_batch.push(rec);
                        // Soft cap: if we somehow buffered a lot, force a flush
                        // mid-cycle to bound memory.
                        if trace_batch.len() >= 200 {
                            let to_send = std::mem::take(&mut trace_batch);
                            if let Err(e) = post_traces(&client, &cfg, &to_send).await {
                                warn!(error = %e, "telemetry: trace POST failed (mid-cycle); dropping batch");
                            }
                        }
                    }
                    None => {
                        debug!("telemetry: trace channel closed");
                        // Continue loop; we still want metrics ticks.
                    }
                }
            }
            _ = metrics_timer.tick() => {
                let now = std::time::Instant::now();
                let cur = counters.snapshot();
                let elapsed_secs = now.duration_since(last_at).as_secs_f64().max(1.0);
                let events_delta = cur.events_emitted.saturating_sub(last_snapshot.events_emitted);
                let events_per_sec = events_delta as f64 / elapsed_secs;

                let rss_mb = if let Some(pid) = pid {
                    sysinfo.refresh_processes_specifics(
                        sysinfo::ProcessesToUpdate::Some(&[pid]),
                        true,
                        sysinfo::ProcessRefreshKind::new().with_memory(),
                    );
                    sysinfo
                        .process(pid)
                        .map(|p| p.memory() / 1024 / 1024)
                        .unwrap_or(0)
                } else {
                    0
                };

                let payload = MetricsPayload {
                    sampled_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    agent_version: &cfg.agent_version,
                    agent_name: cfg.agent_name.as_deref(),
                    metrics: MetricsBody {
                        events_per_sec,
                        cursor_age_ms: if cur.cursor_age_ms > 0 { Some(cur.cursor_age_ms) } else { None },
                        dlq_writes_total: cur.dlq_writes,
                        dlq_write_failures_total: cur.dlq_write_failures,
                        rss_mb,
                        bulk_latency_p50_ms: if cur.bulk_p50_ms > 0 { Some(cur.bulk_p50_ms) } else { None },
                        bulk_latency_p95_ms: if cur.bulk_p95_ms > 0 { Some(cur.bulk_p95_ms) } else { None },
                        bus_backpressure_count: cur.backpressure_events,
                        source: cur.source_kind.clone(),
                        target: cur.target_kind.clone(),
                        phase: cur.phase.as_str(),
                        last_error_message: cur.last_error_message.clone(),
                        last_error_at: if cur.last_error_at_ms > 0 {
                            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(cur.last_error_at_ms as i64)
                                .map(|ts| ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                        } else {
                            None
                        },
                    },
                };

                match post_metrics(&client, &cfg, &payload).await {
                    Ok(response) => {
                        debug!(events_per_sec, rss_mb, "telemetry: metrics posted");
                        // Hand commands off to the agent-command channel
                        // for the orchestrator to act on. Slice 2 just
                        // publishes; slice 3 wires the consumer.
                        if let Some(cmds) = response.commands {
                            publish_commands(cmds);
                        }
                    }
                    Err(e) => warn!(error = %e, "telemetry: metrics POST failed; will retry next interval"),
                }

                if !trace_batch.is_empty() {
                    let to_send = std::mem::take(&mut trace_batch);
                    match post_traces(&client, &cfg, &to_send).await {
                        Ok(_) => debug!(count = to_send.len(), "telemetry: traces posted"),
                        Err(e) => warn!(error = %e, "telemetry: trace POST failed; dropping batch"),
                    }
                }

                last_snapshot = cur;
                last_at = now;
            }
        }
    }
}

fn build_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(3))
        .user_agent(concat!("ventstream-agent/", env!("CARGO_PKG_VERSION")))
        .build()
}

/// Reply the control plane returns from a successful `/agent/metrics`
/// POST. The `commands` block is the control-plane → agent channel:
/// the agent reconciles its local source state to whatever these
/// booleans describe on every heartbeat.
///
/// Both fields are optional on the wire so legacy / partial servers
/// don't break the agent. Missing field = falsy.
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct MetricsResponse {
    #[serde(default)]
    commands: Option<AgentCommands>,
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
struct AgentCommands {
    #[serde(default)]
    pause: bool,
    #[serde(default, rename = "cursorInvalidated")]
    cursor_invalidated: bool,
}

async fn post_metrics(
    client: &reqwest::Client,
    cfg: &TelemetryConfig,
    payload: &MetricsPayload<'_>,
) -> Result<MetricsResponse, reqwest::Error> {
    let url = format!("{}/api/v1/agent/metrics", cfg.control_plane_url);
    let res = client
        .post(url)
        .bearer_auth(&cfg.api_key)
        .json(payload)
        .send()
        .await?
        .error_for_status()?;
    // Parse the body. A server that hasn't been upgraded yet returns
    // `{ok: true}` with no `commands` field — `Default` covers that
    // path so we treat it as "no pending commands."
    let body: MetricsResponse = res.json().await.unwrap_or_default();
    Ok(body)
}

async fn post_traces(
    client: &reqwest::Client,
    cfg: &TelemetryConfig,
    records: &[TraceRecord],
) -> Result<(), reqwest::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let url = format!("{}/api/v1/agent/traces", cfg.control_plane_url);
    let payload = TracesPayload {
        agent_name: cfg.agent_name.as_deref(),
        events: records.to_vec(),
    };
    let res = client
        .post(url)
        .bearer_auth(&cfg.api_key)
        .json(&payload)
        .send()
        .await?;
    res.error_for_status()?;
    Ok(())
}

// ─── Global counter helpers ─────────────────────────────────────
//
// Engine code (source, dispatcher) calls the `bump_*` helpers below
// on hot-path events. When telemetry is disabled, the OnceLock is
// empty and the helpers cost one read + one branch — about as close
// to free as a counter can be.
//
// Telemetry crate sets the global via `set_global_counters` at
// startup; the read side sees the same `Arc<TelemetryCounters>` it
// would have read explicitly.

static GLOBAL_COUNTERS: OnceLock<Arc<TelemetryCounters>> = OnceLock::new();

/// Install the shared counters globally. Idempotent: only the first
/// call wins. Engine `main` calls this once after constructing the
/// shared `Arc<TelemetryCounters>`.
pub fn set_global_counters(counters: Arc<TelemetryCounters>) {
    let _ = GLOBAL_COUNTERS.set(counters);
}

#[inline]
fn with_global<F: FnOnce(&TelemetryCounters)>(f: F) {
    if let Some(c) = GLOBAL_COUNTERS.get() {
        f(c);
    }
}

// ── Prometheus export ────────────────────────────────────────────────
//
// A vendor-neutral scrape endpoint, fully independent of the control
// plane (works with `VS_CONTROL_PLANE_URL` unset). The hot path keeps
// using the raw atomic counters above — on each scrape we bridge the
// latest snapshot into the recorder via `.absolute()`/`.set()` and
// render the text exposition. Labeled / error-path series (DLQ reasons,
// schema drift) are emitted directly via the `metrics` facade at their
// call sites; they accumulate in the same recorder.

/// Opaque handle used to render the Prometheus exposition. Re-exported
/// so callers don't need a direct dependency on the exporter crate.
pub use metrics_exporter_prometheus::PrometheusHandle;

/// Install the global Prometheus recorder. Call once at startup, before
/// anything emits via the `metrics` facade. Returns a handle the health
/// server uses to render `/metrics`.
pub fn install_prometheus() -> Result<PrometheusHandle, String> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .map_err(|err| err.to_string())
}

/// Render the Prometheus text exposition. Bridges the live atomic
/// counters into the recorder first, so a scrape always reflects current
/// state without instrumenting the hot path.
pub fn render_prometheus(handle: &PrometheusHandle) -> String {
    if let Some(c) = GLOBAL_COUNTERS.get() {
        metrics::counter!("vs_events_emitted_total")
            .absolute(c.events_emitted.load(Ordering::Relaxed));
        metrics::counter!("vs_events_received_total")
            .absolute(c.events_received.load(Ordering::Relaxed));
        metrics::counter!("vs_events_delivered_total")
            .absolute(c.events_delivered.load(Ordering::Relaxed));
        metrics::counter!("vs_events_failed_total")
            .absolute(c.events_failed.load(Ordering::Relaxed));
        metrics::counter!("vs_sink_retries_total").absolute(c.sink_retries.load(Ordering::Relaxed));
        metrics::counter!("vs_dlq_writes_total").absolute(c.dlq_writes.load(Ordering::Relaxed));
        metrics::counter!("vs_dlq_write_failures_total")
            .absolute(c.dlq_write_failures.load(Ordering::Relaxed));
        metrics::gauge!("vs_cursor_age_ms").set(c.cursor_age_ms.load(Ordering::Relaxed) as f64);
        metrics::gauge!("vs_last_input_at_unixtime_ms")
            .set(c.last_input_at_ms.load(Ordering::Relaxed) as f64);
        metrics::gauge!("vs_last_output_at_unixtime_ms")
            .set(c.last_output_at_ms.load(Ordering::Relaxed) as f64);
        let unavailable_since = c.sink_unavailable_since_ms.load(Ordering::Relaxed);
        metrics::gauge!("vs_sink_available").set(if unavailable_since == 0 { 1.0 } else { 0.0 });
        metrics::gauge!("vs_sink_outage_seconds").set(if unavailable_since == 0 {
            0.0
        } else {
            now_unix_millis().saturating_sub(unavailable_since) as f64 / 1_000.0
        });
        metrics::gauge!("vs_bulk_write_p50_ms").set(c.bulk_p50_ms.load(Ordering::Relaxed) as f64);
        metrics::gauge!("vs_bulk_write_p95_ms").set(c.bulk_p95_ms.load(Ordering::Relaxed) as f64);
        metrics::gauge!("vs_lifecycle_phase").set(c.current_phase.load(Ordering::Relaxed) as f64);
    }
    handle.render()
}

/// Bump the events-emitted counter. Call after a successful
/// `ctx.sender.send` from any source.
#[inline]
pub fn bump_events_emitted(n: u64) {
    with_global(|c| {
        c.events_emitted.fetch_add(n, Ordering::Relaxed);
    });
}

/// Count events accepted by the dispatcher for sink delivery.
#[inline]
pub fn bump_events_received(n: u64) {
    with_global(|c| {
        c.events_received.fetch_add(n, Ordering::Relaxed);
        c.last_input_at_ms
            .store(now_unix_millis(), Ordering::Relaxed);
    });
}

/// Count events durably acknowledged by the sink.
#[inline]
pub fn bump_events_delivered(n: u64) {
    with_global(|c| {
        c.events_delivered.fetch_add(n, Ordering::Relaxed);
        c.last_output_at_ms
            .store(now_unix_millis(), Ordering::Relaxed);
    });
}

/// Count events rejected or failed during sink delivery.
#[inline]
pub fn bump_events_failed(n: u64) {
    with_global(|c| {
        c.events_failed.fetch_add(n, Ordering::Relaxed);
    });
}

/// Count event-level sink retry attempts.
#[inline]
pub fn bump_sink_retries(n: u64) {
    with_global(|c| {
        c.sink_retries.fetch_add(n, Ordering::Relaxed);
    });
}

/// Mark the downstream sink unavailable without resetting the original outage
/// start time on every retry.
#[inline]
pub fn mark_sink_unavailable() {
    with_global(|c| {
        let _ = c.sink_unavailable_since_ms.compare_exchange(
            0,
            now_unix_millis(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    });
}

/// Mark the downstream sink recovered.
#[inline]
pub fn mark_sink_available() {
    with_global(|c| {
        c.sink_unavailable_since_ms.store(0, Ordering::Relaxed);
    });
}

/// Bump the successful-DLQ-write counter. Call once per event that was
/// durably written to the DLQ (i.e. on success, not on attempt — M16).
#[inline]
pub fn bump_dlq_writes(n: u64) {
    with_global(|c| {
        c.dlq_writes.fetch_add(n, Ordering::Relaxed);
    });
}

/// Bump the failed-DLQ-write counter. Call once per event that could NOT be
/// dead-lettered (genuine loss). Surfaced as `vs_dlq_write_failures_total` and
/// `dlqWriteFailuresTotal` so the loss is alertable, not just logged (M16).
#[inline]
pub fn bump_dlq_write_failures(n: u64) {
    with_global(|c| {
        c.dlq_write_failures.fetch_add(n, Ordering::Relaxed);
    });
}

/// Bump the backpressure counter. Call when a bus send had to await
/// for capacity.
#[inline]
pub fn bump_backpressure() {
    metrics::counter!("vs_backpressure_events_total").increment(1);
    with_global(|c| {
        c.backpressure_events.fetch_add(1, Ordering::Relaxed);
    });
}

/// Record the latest bulk write latency (overwrites the previous
/// value — the telemetry task reads the most recent on each post).
#[inline]
pub fn record_bulk_latency(p50_ms: u64, p95_ms: u64) {
    with_global(|c| {
        c.bulk_p50_ms.store(p50_ms, Ordering::Relaxed);
        c.bulk_p95_ms.store(p95_ms, Ordering::Relaxed);
    });
}

/// Record one acknowledged bulk-write duration and update bounded p50/p95 values.
pub fn record_bulk_latency_sample(milliseconds: u64) {
    const WINDOW: usize = 256;
    with_global(|c| {
        let Ok(mut samples) = c.bulk_latency_samples.lock() else {
            return;
        };
        if samples.len() == WINDOW {
            samples.pop_front();
        }
        samples.push_back(milliseconds);
        let mut ordered = samples.iter().copied().collect::<Vec<_>>();
        ordered.sort_unstable();
        c.bulk_p50_ms
            .store(percentile(&ordered, 50), Ordering::Relaxed);
        c.bulk_p95_ms
            .store(percentile(&ordered, 95), Ordering::Relaxed);
    });
}

fn percentile(ordered: &[u64], percentile: usize) -> u64 {
    if ordered.is_empty() {
        return 0;
    }
    let index = ((ordered.len() - 1) * percentile).div_ceil(100);
    ordered.get(index).copied().unwrap_or_default()
}

fn now_unix_millis() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0)
}

/// Update the cursor-age estimate. Source-specific.
#[inline]
pub fn record_cursor_age(ms: u64) {
    with_global(|c| {
        c.cursor_age_ms.store(ms, Ordering::Relaxed);
    });
}

/// Update the agent's current lifecycle phase. Source code calls this
/// at transition points (snapshot start, snapshot complete, tail
/// mode, shutdown). Cheap — one relaxed atomic store; a no-op when
/// telemetry is disabled.
///
/// Transitioning *out* of [`LifecyclePhase::Erroring`] does NOT
/// automatically clear the last error message; call [`clear_error`]
/// explicitly when the agent has recovered.
#[inline]
pub fn set_phase(phase: LifecyclePhase) {
    with_global(|c| {
        c.current_phase.store(phase as u8, Ordering::Relaxed);
    });
}

/// Record which source backend this agent runs (`postgres` / `neo4j`).
/// Call once at startup. The control plane displays it on the agent
/// list so operators don't have to remember each agent's source.
pub fn set_source(kind: impl Into<String>) {
    with_global(|c| {
        if let Ok(mut guard) = c.source_kind.lock() {
            *guard = Some(kind.into());
        }
    });
}

/// Record which sink this agent writes to (`opensearch`). Call once at
/// startup; shown next to the source on the agent list.
pub fn set_target(kind: impl Into<String>) {
    with_global(|c| {
        if let Ok(mut guard) = c.target_kind.lock() {
            *guard = Some(kind.into());
        }
    });
}

/// Record an error that should be surfaced to operators in the
/// control plane UI. Also sets the phase to
/// [`LifecyclePhase::Erroring`] — the caller doesn't need a separate
/// `set_phase` call.
///
/// The message is stored under a `Mutex<Option<String>>`, so frequent
/// calls are fine but not free; reserve this for actual operator-
/// visible failures (connect loss, snapshot abort, sink rejection
/// stuck on retries), not per-event errors.
pub fn record_error(message: impl Into<String>) {
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    with_global(|c| {
        if let Ok(mut guard) = c.last_error_message.lock() {
            *guard = Some(message.into());
        }
        c.last_error_at_ms.store(now_ms, Ordering::Relaxed);
        c.current_phase
            .store(LifecyclePhase::Erroring as u8, Ordering::Relaxed);
    });
}

/// Clear the last recorded error. Call after a successful recovery
/// — e.g. once the source has re-connected and re-entered tail mode.
/// Does NOT change the phase; the caller is expected to follow with
/// [`set_phase`] for the new healthy state.
pub fn clear_error() {
    with_global(|c| {
        if let Ok(mut guard) = c.last_error_message.lock() {
            *guard = None;
        }
        c.last_error_at_ms.store(0, Ordering::Relaxed);
    });
}

// ─── Control-plane → agent command channel ──────────────────────────
//
// The metrics POST response carries a `commands` block describing the
// desired pause / cursor state. The telemetry loop publishes whatever
// it received into the static below; the orchestrator polls it on
// each engine iteration to reconcile local source state against the
// control plane's intent.
//
// Storage shape: `Mutex<Option<AgentCommand>>` behind a OnceLock. The
// mutex is uncontested in the common case (one writer in the
// telemetry task, one reader in the orchestrator) so contention cost
// is irrelevant.

/// Reconciled agent intent from the control plane. Two booleans
/// describe the entire state space — see the metrics route in the
/// control plane API for the full state-machine truth table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentCommand {
    /// Operator-requested pause is in effect. The agent should
    /// disconnect its source (without dropping the upstream cursor)
    /// and transition to [`LifecyclePhase::Paused`].
    pub pause: bool,
    /// The upstream cursor (PG slot, Neo4j sequence) has been
    /// invalidated server-side. When the agent next runs source code
    /// — either to drain (if `pause` is also true) or to bootstrap
    /// (if `pause` is false) — it must treat its local cursor state
    /// as worthless.
    pub cursor_invalidated: bool,
}

static LATEST_COMMAND: OnceLock<Mutex<Option<AgentCommand>>> = OnceLock::new();

fn command_slot() -> &'static Mutex<Option<AgentCommand>> {
    LATEST_COMMAND.get_or_init(|| Mutex::new(None))
}

fn publish_commands(cmds: AgentCommands) {
    let value = AgentCommand {
        pause: cmds.pause,
        cursor_invalidated: cmds.cursor_invalidated,
    };
    if let Ok(mut guard) = command_slot().lock() {
        *guard = Some(value);
    }
    debug!(
        pause = value.pause,
        cursor_invalidated = value.cursor_invalidated,
        "telemetry: received agent command"
    );
}

/// Read the most recently received command from the control plane.
/// Returns `None` when no command has been delivered yet (e.g. on a
/// freshly-started agent before its first heartbeat).
///
/// Cheap — one mutex acquire + a `Copy` of two booleans. Call freely
/// from the orchestrator loop.
pub fn latest_command() -> Option<AgentCommand> {
    command_slot().lock().ok().and_then(|g| *g)
}

/// Re-export so callers can `use rand::random;` without depending on rand directly.
pub use rand::random;

/// Convenience helper used by [`tracing_layer`] — checks sampling
/// without exposing rand internals to the caller.
#[doc(hidden)]
pub fn should_sample(rate: f32) -> bool {
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    rand::thread_rng().gen::<f32>() < rate
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_uses_the_nearest_rank_without_exceeding_the_window() {
        assert_eq!(percentile(&[], 95), 0);
        assert_eq!(percentile(&[7], 95), 7);
        assert_eq!(percentile(&[1, 2, 3, 4, 100], 50), 3);
        assert_eq!(percentile(&[1, 2, 3, 4, 100], 95), 100);
    }
}
