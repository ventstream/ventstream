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
//!   gateway has initialized its dependencies and bound its listener, and
//!   also while the WS gateway is at its connection-capacity threshold.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::{response::IntoResponse, routing::get, Router};
use tokio::net::TcpListener;
use tracing::info;
use ventstream_core::{ReadinessSignal, ShutdownToken};
use ventstream_telemetry::PrometheusHandle;

/// Aggregate readiness for the enabled traffic gateways and WS capacity.
#[derive(Clone)]
pub(crate) struct ReadinessGate {
    ws: Option<ReadinessSignal>,
    graphql: Option<ReadinessSignal>,
    ws_capacity: Option<WsCapacityGate>,
}

impl ReadinessGate {
    pub(crate) fn new(
        ws: Option<ReadinessSignal>,
        graphql: Option<ReadinessSignal>,
        ws_capacity: Option<(Arc<AtomicUsize>, usize)>,
    ) -> Self {
        Self {
            ws,
            graphql,
            ws_capacity: ws_capacity.map(|(active, max)| WsCapacityGate { active, max }),
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
        if !waiting_for.is_empty() {
            return ReadinessStatus::Starting(waiting_for);
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
    fn waits_for_every_enabled_gateway() {
        let ws = ReadinessSignal::new();
        let graphql = ReadinessSignal::new();
        let gate = ReadinessGate::new(Some(ws.clone()), Some(graphql.clone()), None);

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
        let gate = ReadinessGate::new(Some(ws.clone()), None, Some((Arc::clone(&active), 9)));

        assert_eq!(gate.status(), ReadinessStatus::Starting(vec!["ws"]));
        ws.mark_ready();
        assert_eq!(gate.status(), ReadinessStatus::AtCapacity);
        active.store(8, Ordering::Relaxed);
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }

    #[test]
    fn cdc_only_process_has_no_gateway_startup_gate() {
        let gate = ReadinessGate::new(None, None, None);
        assert_eq!(gate.status(), ReadinessStatus::Ready);
    }
}
