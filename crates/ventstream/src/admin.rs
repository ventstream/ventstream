//! Optional admin HTTP server for the CDC role.
//!
//! Opt-in via `VS_ADMIN_LISTEN`. Default off — when set, the binary
//! spawns a small axum server alongside the engine.
//!
//! ## Endpoints
//!
//! - `GET  /admin/healthz` — liveness.
//! - `POST /admin/resync`  — full re-sync of every table in the
//!   joins config. Optional query: `?tables=public.shows,public.routes`
//!   to scope the resync. Returns when complete with row counts.
//!
//! ## Concurrency
//!
//! Only one resync at a time. The handler holds a `Mutex<()>`
//! across the SELECT loop; a concurrent request returns `409
//! Conflict`. This is a v1 simplification — we'd accept a queue
//! later if customers ask.
//!
//! ## Security
//!
//! The `/admin/resync` endpoint is destructive (drops a slot, re-bootstraps,
//! and injects synthetic events with attacker-controllable `chunk_size` /
//! `tables`), so it is protected two ways:
//!
//! - **Loopback by default.** A port-only listen string (`:4042` or `4042`)
//!   binds `127.0.0.1` via [`parse_admin_listen`]. A spelled-out address
//!   (`0.0.0.0:4042`) binds that interface.
//! - **Bearer token.** When `VS_ADMIN_TOKEN` (or `VS_CONTROL_PLANE_KEY`) is
//!   set, `/admin/resync` requires `Authorization: Bearer <token>`
//!   (constant-time compared). [`validate_listen_auth`] refuses to start a
//!   **non-loopback** bind that has no token — so the destructive endpoint is
//!   never reachable off-host unauthenticated. `/admin/healthz` stays open
//!   for liveness probes.
//!
//! **Recommendation:** set `VS_ADMIN_TOKEN` even on a loopback bind. Loopback
//! is not a trust boundary on a shared host — a co-tenant process, or an
//! SSRF in any other service on the box, can still reach `127.0.0.1:4042`.
//! The token is the only real authn; the loopback default just keeps the
//! endpoint off the network by default. Loopback-without-token is permitted
//! (it's the safe-enough local-dev case), not endorsed for production.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};
use ventstream_core::{EventSender, ShutdownToken};
use ventstream_sources::postgres::{resync_tables, PostgresCdcConfig, ResyncStats, SnapshotTable};

/// Shared state for the admin server handlers.
#[derive(Clone)]
pub(crate) struct AdminState {
    /// Postgres config (host/port/auth) used to open the resync
    /// connection.
    pub pg: PostgresCdcConfig,
    /// The full set of snapshot-eligible tables — derived from the
    /// joins YAML at startup.
    pub tables: Vec<SnapshotTable>,
    /// Default rows per SELECT chunk during a resync. Operators
    /// override per-request via `?chunk_size=`.
    pub chunk_size: usize,
    /// Source-bus sender. The resync handler builds a
    /// `SourceContext` with a clone so synthetic events land in the
    /// same bus the running source writes to.
    pub source_sender: EventSender,
    /// Shutdown token; surfaced into the resync's `SourceContext`
    /// so a server shutdown also aborts an in-flight resync.
    pub shutdown: ShutdownToken,
    /// Single-flight guard — only one resync at a time.
    pub busy: Arc<Mutex<bool>>,
    /// Optional bearer token for the destructive endpoints. `None` means no
    /// token configured — allowed only because [`validate_listen_auth`]
    /// guarantees the bind is loopback in that case.
    pub token: Option<String>,
}

/// Parse `VS_ADMIN_LISTEN` into a bind address.
///
/// Accepts a full `host:port` ([`SocketAddr`]) verbatim, or a **port-only**
/// form (`:4042` or `4042`) which binds loopback (`127.0.0.1:port`) — the
/// documented default. `SocketAddr::parse(":4042")` errors, so the port-only
/// case is handled explicitly here.
pub(crate) fn parse_admin_listen(s: &str) -> anyhow::Result<SocketAddr> {
    let s = s.trim();
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let port_str = s.strip_prefix(':').unwrap_or(s);
    let port: u16 = port_str.parse().map_err(|_| {
        anyhow::anyhow!("VS_ADMIN_LISTEN must be `host:port`, `:port`, or `port`; got {s:?}")
    })?;
    Ok(SocketAddr::from(([127, 0, 0, 1], port)))
}

/// Refuse to start a non-loopback admin bind that has no bearer token.
///
/// The endpoint is destructive; exposing it off-host unauthenticated is a
/// DoS / data-shape-churn vector, so this is a hard startup error rather than
/// a warning.
pub(crate) fn validate_listen_auth(
    listen: &SocketAddr,
    token: &Option<String>,
) -> anyhow::Result<()> {
    if !listen.ip().is_loopback() && token.is_none() {
        anyhow::bail!(
            "VS_ADMIN_LISTEN binds non-loopback address {listen} but no VS_ADMIN_TOKEN \
             (or VS_CONTROL_PLANE_KEY) is set; /admin/resync is destructive — refusing to \
             start. Set a token, or bind a loopback address (`:PORT`)."
        );
    }
    Ok(())
}

/// Constant-time byte-slice equality. Length difference short-circuits
/// (length is not secret for a bearer token); otherwise no early exit.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Enforce the bearer token on a destructive request. `Ok(())` when no token
/// is configured (loopback-only, guaranteed by [`validate_listen_auth`]) or
/// the `Authorization: Bearer <token>` header matches.
fn check_bearer(headers: &HeaderMap, token: &Option<String>) -> Result<(), StatusCode> {
    let Some(expected) = token.as_deref() else {
        return Ok(());
    };
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Run the admin server until shutdown.
pub(crate) async fn run(
    listen: SocketAddr,
    state: AdminState,
    shutdown: ShutdownToken,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/admin/healthz", get(healthz))
        .route("/admin/resync", post(resync_handler))
        .with_state(state);

    let listener = TcpListener::bind(listen).await?;
    info!(listen = %listen, "admin server listening");
    let shutdown_for_serve = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown_for_serve.cancelled().await })
        .await?;
    info!("admin server stopped");
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"ok"}"#,
    )
}

#[derive(Debug, Deserialize)]
struct ResyncQuery {
    /// Comma-separated `namespace.table` pairs. When absent, all
    /// configured tables are re-synced.
    tables: Option<String>,
    /// Override the per-request chunk size.
    chunk_size: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ResyncResponse {
    tables_processed: Vec<String>,
    stats: ResyncStats,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

async fn resync_handler(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Query(q): Query<ResyncQuery>,
) -> impl IntoResponse {
    // Authenticate before doing anything else (incl. claiming the single-
    // flight slot) so an unauthenticated caller can't even probe liveness of
    // the resync machinery.
    if let Err(status) = check_bearer(&headers, &state.token) {
        return (
            status,
            axum::Json(ErrorResponse {
                error: "missing or invalid bearer token".into(),
            }),
        )
            .into_response();
    }
    // Single-flight guard. parking_lot's Mutex is sync; we hold it
    // across the await — fine because we never await while holding,
    // we drop it as soon as we've claimed the slot.
    {
        let mut busy = state.busy.lock();
        if *busy {
            return (
                StatusCode::CONFLICT,
                axum::Json(ErrorResponse {
                    error: "another resync is already in progress".into(),
                }),
            )
                .into_response();
        }
        *busy = true;
    }
    // Always clear the flag on the way out, even if the handler
    // panics — the panic-handler in the runtime would crash the
    // task. To be safe, we use a guard struct.
    struct Guard(Arc<Mutex<bool>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            *self.0.lock() = false;
        }
    }
    let _guard = Guard(Arc::clone(&state.busy));

    let chunk_size = q.chunk_size.unwrap_or(state.chunk_size);
    let target_tables = filter_tables(&state.tables, q.tables.as_deref());
    if target_tables.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ErrorResponse {
                error: "no matching tables in joins config".into(),
            }),
        )
            .into_response();
    }
    let names: Vec<String> = target_tables
        .iter()
        .map(|t| format!("{}.{}", t.namespace, t.name))
        .collect();

    info!(tables = ?names, chunk_size, "admin resync requested");

    let ctx = ventstream_core::SourceContext::new(state.source_sender, state.shutdown);
    match resync_tables(&state.pg, &target_tables, chunk_size, &ctx).await {
        Ok(stats) => (
            StatusCode::OK,
            axum::Json(ResyncResponse {
                tables_processed: names,
                stats,
            }),
        )
            .into_response(),
        Err(err) => {
            error!(error = %err, "admin resync failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ErrorResponse {
                    error: err.to_string(),
                }),
            )
                .into_response()
        }
    }
}

/// Filter the configured tables down to the ones the request named
/// (or return all when no filter is supplied).
fn filter_tables(all: &[SnapshotTable], filter: Option<&str>) -> Vec<SnapshotTable> {
    let Some(spec) = filter else {
        return all.to_vec();
    };
    let wanted: std::collections::HashSet<String> = spec
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    all.iter()
        .filter(|t| wanted.contains(&format!("{}.{}", t.namespace, t.name)))
        .cloned()
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use axum::http::header::AUTHORIZATION;

    #[test]
    fn parse_listen_port_only_binds_loopback() {
        for s in [":4042", "4042", " :4042 "] {
            let addr = parse_admin_listen(s).expect("port-only parses");
            assert!(
                addr.ip().is_loopback(),
                "{s} must bind loopback, got {addr}"
            );
            assert_eq!(addr.port(), 4042);
        }
    }

    #[test]
    fn parse_listen_full_address_is_verbatim() {
        let addr = parse_admin_listen("0.0.0.0:4042").expect("full parses");
        assert!(!addr.ip().is_loopback());
        assert_eq!(addr.port(), 4042);
        let lo = parse_admin_listen("127.0.0.1:9").expect("full loopback parses");
        assert!(lo.ip().is_loopback());
    }

    #[test]
    fn parse_listen_rejects_garbage() {
        assert!(parse_admin_listen("not-a-port").is_err());
        assert!(parse_admin_listen(":99999").is_err());
    }

    #[test]
    fn validate_refuses_public_bind_without_token() {
        let public: SocketAddr = "0.0.0.0:4042".parse().unwrap();
        let loopback: SocketAddr = "127.0.0.1:4042".parse().unwrap();
        // The one combination that must be refused.
        assert!(validate_listen_auth(&public, &None).is_err());
        // The three that are allowed.
        assert!(validate_listen_auth(&public, &Some("secret".into())).is_ok());
        assert!(validate_listen_auth(&loopback, &None).is_ok());
        assert!(validate_listen_auth(&loopback, &Some("secret".into())).is_ok());
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    fn headers_with_auth(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, value.parse().expect("header value"));
        h
    }

    #[test]
    fn check_bearer_without_configured_token_allows_all() {
        assert!(check_bearer(&HeaderMap::new(), &None).is_ok());
        assert!(check_bearer(&headers_with_auth("Bearer anything"), &None).is_ok());
    }

    #[test]
    fn check_bearer_enforces_configured_token() {
        let token = Some("s3cr3t".to_string());
        assert!(check_bearer(&headers_with_auth("Bearer s3cr3t"), &token).is_ok());
        // Wrong token, wrong scheme, and missing header all 401.
        assert_eq!(
            check_bearer(&headers_with_auth("Bearer nope"), &token),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            check_bearer(&headers_with_auth("Basic s3cr3t"), &token),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            check_bearer(&HeaderMap::new(), &token),
            Err(StatusCode::UNAUTHORIZED)
        );
    }
}
