//! Single, always-on health server shared by every role.
//!
//! This is the **one canonical liveness/readiness target** for the whole
//! binary — `cdc`, `ws`, `graphql`, or any combination all probe the same
//! `/healthz` + `/readyz` here. The role servers (ws/graphql) deliberately
//! do NOT serve their own health route, so a multi-role process exposes
//! exactly one health endpoint on one port (no per-role duplication, no
//! ambiguity about which port to probe).
//!
//! Unlike the optional admin server ([`crate::admin`], gated behind
//! `VS_ADMIN_LISTEN`), this is always on and lives for the whole process —
//! it is not torn down on a pause/resume cycle. Listen address is
//! `VS_HEALTH_LISTEN` (default `0.0.0.0:4043`). A bind failure is logged
//! by the caller and does not take the data pipeline down.
//!
//! ## Endpoints
//!
//! - `GET /healthz` — liveness. 200 once the process is up; semantics
//!   match the gateways' probe (the async runtime is responsive). It is
//!   deliberately independent of capacity: a full pod is still *alive*
//!   (serving its existing connections), so liveness must stay 200 or
//!   k8s would needlessly restart it and drop those connections.
//! - `GET /readyz`  — readiness. Returns 503 until every enabled traffic
//!   gateway has initialized its dependencies and bound its listener, while
//!   the WS gateway is at its connection-capacity threshold, or after a CDC
//!   sink has remained unavailable for the delivery grace period.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use axum::{response::IntoResponse, routing::get, Router};
use tokio::net::TcpListener;
use tracing::info;
use ventstream_core::{ReadinessSignal, ShutdownToken, SinkHealth, SinkHealthSnapshot};
use ventstream_telemetry::PrometheusHandle;

/// Aggregate readiness for the enabled traffic gateways and WS capacity.
#[derive(Clone)]
pub(crate) struct ReadinessGate {
    ws: Option<ReadinessSignal>,
    graphql: Option<ReadinessSignal>,
    mcp: Option<ReadinessSignal>,
    ws_capacity: Option<WsCapacityGate>,
    sink_health: Option<SinkHealth>,
    sink_failure_grace: Duration,
    /// Source-side pipeline stall channel (e.g. the SQL denormalizer's tail
    /// loop retrying a failed batch). A stall past the grace flips readiness
    /// so orchestrators and operators see the pipeline is not making
    /// progress even though the process is alive.
    pipeline_health: Option<SinkHealth>,
    pipeline_stall_grace: Duration,
}

impl ReadinessGate {
    const DEFAULT_SINK_FAILURE_GRACE: Duration = Duration::from_secs(30);
    /// Longer than the sink grace: one tail batch retry is routine; a stall
    /// that persists for a minute is not.
    const DEFAULT_PIPELINE_STALL_GRACE: Duration = Duration::from_secs(60);

    pub(crate) fn new(
        ws: Option<ReadinessSignal>,
        graphql: Option<ReadinessSignal>,
        ws_capacity: Option<(Arc<AtomicUsize>, usize)>,
        sink_health: Option<SinkHealth>,
        pipeline_health: Option<SinkHealth>,
    ) -> Self {
        Self {
            ws,
            graphql,
            mcp: None,
            ws_capacity: ws_capacity.map(|(active, max)| WsCapacityGate { active, max }),
            sink_health,
            sink_failure_grace: Self::DEFAULT_SINK_FAILURE_GRACE,
            pipeline_health,
            pipeline_stall_grace: Self::DEFAULT_PIPELINE_STALL_GRACE,
        }
    }

    /// Gate for the solo mcp role: ready once the target registry is built.
    pub(crate) fn for_mcp(mcp: ReadinessSignal) -> Self {
        Self {
            ws: None,
            graphql: None,
            mcp: Some(mcp),
            ws_capacity: None,
            sink_health: None,
            sink_failure_grace: Self::DEFAULT_SINK_FAILURE_GRACE,
            pipeline_health: None,
            pipeline_stall_grace: Self::DEFAULT_PIPELINE_STALL_GRACE,
        }
    }

    fn status(&self) -> ReadinessStatus {
        let mut waiting_for = Vec::with_capacity(2);
        if self.ws.as_ref().is_some_and(|signal| !signal.is_ready()) {
            waiting_for.push("ws");
        }
        if self
            .graphql
            .as_ref()
            .is_some_and(|signal| !signal.is_ready())
        {
            waiting_for.push("graphql");
        }
        if self.mcp.as_ref().is_some_and(|signal| !signal.is_ready()) {
            waiting_for.push("mcp");
        }
        if !waiting_for.is_empty() {
            return ReadinessStatus::Starting(waiting_for);
        }
        if let Some(health) = &self.sink_health {
            match health.snapshot() {
                SinkHealthSnapshot::Blocked { duration, reason } => {
                    return ReadinessStatus::SinkUnavailable {
                        state: "blocked",
                        duration,
                        reason,
                    };
                }
                SinkHealthSnapshot::Degraded { duration, reason }
                    if duration >= self.sink_failure_grace =>
                {
                    return ReadinessStatus::SinkUnavailable {
                        state: "unavailable",
                        duration,
                        reason,
                    };
                }
                SinkHealthSnapshot::Healthy | SinkHealthSnapshot::Degraded { .. } => {}
            }
        }
        if let Some(health) = &self.pipeline_health {
            match health.snapshot() {
                SinkHealthSnapshot::Blocked { duration, reason } => {
                    return ReadinessStatus::PipelineStalled { duration, reason };
                }
                SinkHealthSnapshot::Degraded { duration, reason }
                    if duration >= self.pipeline_stall_grace =>
                {
                    return ReadinessStatus::PipelineStalled { duration, reason };
                }
                SinkHealthSnapshot::Healthy | SinkHealthSnapshot::Degraded { .. } => {}
            }
        }
        if self
            .ws_capacity
            .as_ref()
            .is_some_and(|gate| !gate.is_ready())
        {
            return ReadinessStatus::AtCapacity;
        }
        ReadinessStatus::Ready
    }
}

#[derive(Clone)]
struct WsCapacityGate {
    active: Arc<AtomicUsize>,
    max: usize,
}

impl WsCapacityGate {
    fn is_ready(&self) -> bool {
        self.active.load(Ordering::Relaxed) < self.max
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReadinessStatus {
    Starting(Vec<&'static str>),
    SinkUnavailable {
        state: &'static str,
        duration: Duration,
        reason: String,
    },
    /// The CDC transform stage has been retrying the same batch past the
    /// stall grace — the process is alive but not making progress.
    PipelineStalled {
        duration: Duration,
        reason: String,
    },
    AtCapacity,
    Ready,
}

/// Run the health server until `shutdown` fires. When a Prometheus handle
/// is supplied, also serves `GET /metrics` (text exposition) on the same
/// port — the vendor-neutral scrape target for every role.
pub(crate) async fn run(
    listen: SocketAddr,
    prometheus: Option<PrometheusHandle>,
    readiness: ReadinessGate,
    shutdown: ShutdownToken,
) -> anyhow::Result<()> {
    let readyz_gate = readiness;
    let mut app = Router::new().route("/healthz", get(healthz)).route(
        "/readyz",
        get(move || {
            let gate = readyz_gate.clone();
            async move { readyz(&gate) }
        }),
    );
    if let Some(handle) = prometheus {
        app = app.route(
            "/metrics",
            get(move || {
                let handle = handle.clone();
                async move {
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        ventstream_telemetry::render_prometheus(&handle),
                    )
                }
            }),
        );
    }

    let listener = TcpListener::bind(listen).await?;
    info!(listen = %listen, "health server listening");
    let shutdown_for_serve = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown_for_serve.cancelled().await })
        .await?;
    info!("health server stopped");
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"ok"}"#,
    )
}

/// Readiness response for gateway startup and WS connection capacity.
fn readyz(gate: &ReadinessGate) -> axum::response::Response {
    match gate.status() {
        ReadinessStatus::Starting(waiting_for) => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "starting",
                "waiting_for": waiting_for,
            })),
        )
            .into_response(),
        ReadinessStatus::AtCapacity => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "status": "at_capacity" })),
        )
            .into_response(),
        ReadinessStatus::SinkUnavailable {
            state,
            duration,
            reason,
        } => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "not_ready",
                "dependency": "sink",
                "state": state,
                "failing_for_ms": u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                "reason": reason,
            })),
        )
            .into_response(),
        ReadinessStatus::PipelineStalled { duration, reason } => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "status": "not_ready",
                "dependency": "pipeline",
                "state": "stalled",
                "failing_for_ms": u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                "reason": reason,
            })),
        )
            .into_response(),
        ReadinessStatus::Ready => (
            StatusCode::OK,
            axum::Json(serde_json::json!({ "status": "ready" })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_gate_waits_for_the_registry() {
        let signal = ReadinessSignal::new();
        let gate = ReadinessGate::for_mcp(signal.clone());
        assert_eq!(gate.status(), ReadinessStatus::Starting(vec!["mcp"]));
        signal.mark_ready();
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn waits_for_every_enabled_gateway() {
        let ws = ReadinessSignal::new();
        let graphql = ReadinessSignal::new();
        let gate = ReadinessGate::new(Some(ws.clone()), Some(graphql.clone()), None, None, None);

        assert_eq!(
            gate.status(),
            ReadinessStatus::Starting(vec!["ws", "graphql"])
        );
        ws.mark_ready();
        assert_eq!(gate.status(), ReadinessStatus::Starting(vec!["graphql"]));
        graphql.mark_ready();
        assert_eq!(gate.status(), ReadinessStatus::Ready);

        ws.mark_not_ready();
        assert_eq!(gate.status(), ReadinessStatus::Starting(vec!["ws"]));
    }

    #[test]
    fn applies_capacity_only_after_gateway_startup() {
        let ws = ReadinessSignal::new();
        let active = Arc::new(AtomicUsize::new(9));
        let gate = ReadinessGate::new(
            Some(ws.clone()),
            None,
            Some((Arc::clone(&active), 9)),
            None,
            None,
        );

        assert_eq!(gate.status(), ReadinessStatus::Starting(vec!["ws"]));
        ws.mark_ready();
        assert_eq!(gate.status(), ReadinessStatus::AtCapacity);
        active.store(8, Ordering::Relaxed);
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn cdc_only_process_has_no_gateway_startup_gate() {
        let gate = ReadinessGate::new(None, None, None, Some(SinkHealth::new()), None);
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn brief_sink_failure_stays_ready_but_sustained_failure_does_not() {
        let health = SinkHealth::new();
        let _failure = health.begin_transient_failure("HTTP 503");
        let mut gate = ReadinessGate::new(None, None, None, Some(health), None);

        assert_eq!(gate.status(), ReadinessStatus::Ready);
        gate.sink_failure_grace = Duration::ZERO;
        assert!(matches!(
            gate.status(),
            ReadinessStatus::SinkUnavailable {
                state: "unavailable",
                ref reason,
                ..
            } if reason == "HTTP 503"
        ));
    }

    #[test]
    fn brief_pipeline_stall_stays_ready_but_sustained_stall_does_not() {
        let health = SinkHealth::new();
        let _stall = health.begin_transient_failure("recompose for public.orders");
        let mut gate = ReadinessGate::new(None, None, None, None, Some(health));

        assert_eq!(gate.status(), ReadinessStatus::Ready);
        gate.pipeline_stall_grace = Duration::ZERO;
        assert!(matches!(
            gate.status(),
            ReadinessStatus::PipelineStalled { ref reason, .. }
                if reason == "recompose for public.orders"
        ));
    }

    #[test]
    fn recovered_pipeline_returns_to_ready() {
        let health = SinkHealth::new();
        let stall = health.begin_transient_failure("recompose for public.orders");
        let mut gate = ReadinessGate::new(None, None, None, None, Some(health));
        gate.pipeline_stall_grace = Duration::ZERO;
        assert!(matches!(
            gate.status(),
            ReadinessStatus::PipelineStalled { .. }
        ));
        drop(stall);
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn blocked_sink_is_immediately_not_ready() {
        let health = SinkHealth::new();
        health.mark_blocked("authentication failed");
        let gate = ReadinessGate::new(None, None, None, Some(health), None);

        assert!(matches!(
            gate.status(),
            ReadinessStatus::SinkUnavailable {
                state: "blocked",
                ref reason,
                ..
            } if reason == "authentication failed"
        ));
    }
}
