//! Axum router + entry point for the GraphQL gateway.
//!
//! Routes:
//! - `GET /graphql` / `POST /graphql` — GraphQL queries and mutations
//!   over HTTP. Apollo Sandbox and `graphql-codegen` both speak this.
//! - `GET /graphql/ws` — graphql-transport-ws WebSocket upgrade for
//!   subscriptions.
//!
//! Liveness/readiness is served centrally by the engine binary's health
//! server (`VS_HEALTH_LISTEN`), not on this port.
//!
//! `graphql-transport-ws` is the modern subprotocol Apollo Client
//! uses by default; the legacy `graphql-ws` subprotocol is also
//! supported via the `GraphQLProtocol` axum extractor that
//! async-graphql provides.

use std::sync::Arc;

use async_graphql::http::ALL_WEBSOCKET_PROTOCOLS;
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tracing::{info, warn};
use ventstream_core::{ReadinessSignal, ShutdownToken};
use ventstream_realtime::RealtimeBroker;

use crate::auth::Tenant;

use crate::auth;
use crate::config::{GraphQlConfig, SubjectManifest};
use crate::conn_source::{ConnSourceCell, ResumeCursor};
use crate::dynamic_schema;
use crate::error::GraphQlError;
use crate::manifest::Manifest as SubscriptionsManifest;
use crate::schema::{build_schema, AppSchema, GraphContext};

/// Run the GraphQL gateway until shutdown fires.
pub async fn run(config: GraphQlConfig, shutdown: ShutdownToken) -> Result<(), GraphQlError> {
    run_inner(config, shutdown, None).await
}

/// Run the GraphQL gateway and publish listener readiness to the health server.
///
/// The signal remains false through schema and broker initialization and
/// flips true only after the shared HTTP/WebSocket listener is bound. Shutdown
/// or any exit path resets it.
pub async fn run_with_readiness(
    config: GraphQlConfig,
    shutdown: ShutdownToken,
    readiness: ReadinessSignal,
) -> Result<(), GraphQlError> {
    run_inner(config, shutdown, Some(readiness)).await
}

async fn run_inner(
    config: GraphQlConfig,
    shutdown: ShutdownToken,
    readiness: Option<ReadinessSignal>,
) -> Result<(), GraphQlError> {
    let readiness = readiness.as_ref().map(ReadinessSignal::guard);
    let manifest = if let Some(path) = &config.manifest_path {
        let text = std::fs::read_to_string(path).map_err(|err| {
            GraphQlError::Config(format!("reading manifest {}: {err}", path.display()))
        })?;
        let parsed: SubjectManifest = serde_yaml::from_str(&text)
            .map_err(|err| GraphQlError::Config(format!("parsing manifest: {err}")))?;
        info!(
            path = %path.display(),
            entries = parsed.subjects.len(),
            "subject manifest loaded"
        );
        parsed.subjects
    } else {
        Vec::new()
    };

    let broker: Arc<dyn RealtimeBroker> = if let Some(redis_config) = &config.redis_streams {
        let redis = ventstream_redis::RedisStreamsBroker::connect(redis_config.clone())
            .await
            .map_err(|error| GraphQlError::Broker(error.to_string()))?;
        info!(key_prefix = %redis_config.key_prefix, "graphql gateway connected to Redis Streams");
        Arc::new(redis)
    } else {
        // The stream is bootstrapped by the ws role or an external operator.
        let client = async_nats::connect(&config.nats_url)
            .await
            .map_err(|error| {
                GraphQlError::Nats(format!("connecting to {}: {error}", config.nats_url))
            })?;
        let js = async_nats::jetstream::new(client);
        let mut attempt = 0u32;
        loop {
            match js.get_stream(&config.stream_name).await {
                Ok(_) => break,
                Err(error) => {
                    attempt += 1;
                    if attempt >= 30 {
                        return Err(GraphQlError::Nats(format!(
                            "JetStream stream '{}' not found after {attempt} attempts: {error}",
                            config.stream_name
                        )));
                    }
                    tracing::debug!(stream = %config.stream_name, attempt, "JetStream stream not yet ready");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        let jetstream = ventstream_jetstream::JetStreamBroker::new(
            js.clone(),
            ventstream_jetstream::JetStreamBrokerConfig {
                stream_name: config.stream_name.clone(),
                pod_id: config.pod_id.clone(),
                consumer_inactive_threshold: config.consumer_inactive_threshold,
            },
        );
        let reaper_active = jetstream.active_set();
        let reaper_stream = config.stream_name.clone();
        let reaper_pod = config.pod_id.clone();
        let reaper_interval = config.reaper_interval;
        let reaper_shutdown = shutdown.clone();
        tokio::spawn(async move {
            ventstream_jetstream::run_reaper(
                js,
                reaper_stream,
                "graphql".to_owned(),
                reaper_pod,
                reaper_interval,
                reaper_active,
                reaper_shutdown,
            )
            .await;
        });
        info!(stream = %config.stream_name, pod_id = %config.pod_id, "graphql gateway connected to JetStream");
        Arc::new(jetstream)
    };

    let graph_ctx = GraphContext {
        broker,
        broadcast_capacity: config.broadcast_capacity,
        manifest: Arc::new(manifest),
    };

    // Decide the typed-subscription source, in precedence order:
    //   1. VS_GRAPHQL_SCHEMA  — a GraphQL SDL file with @vsSubscribe /
    //      @source directives (the recommended way).
    //   2. VS_GRAPHQL_SUBSCRIPTIONS — the YAML manifest (legacy).
    //   3. neither — the static schema (generic `events(subject)` only).
    let parsed_manifest: Option<SubscriptionsManifest> =
        if let Some(schema_path) = &config.schema_path {
            let text = std::fs::read_to_string(schema_path).map_err(|err| {
                GraphQlError::Config(format!(
                    "reading SDL schema {}: {err}",
                    schema_path.display()
                ))
            })?;
            let m = crate::sdl::parse_sdl(&text)?;
            info!(
                path = %schema_path.display(),
                entries = m.subscriptions.len(),
                "GraphQL SDL loaded (dynamic schema enabled)"
            );
            Some(m)
        } else if let Some(subs_path) = &config.subscriptions_path {
            let text = std::fs::read_to_string(subs_path).map_err(|err| {
                GraphQlError::Config(format!(
                    "reading subscriptions manifest {}: {err}",
                    subs_path.display()
                ))
            })?;
            let parsed: SubscriptionsManifest = serde_yaml::from_str(&text).map_err(|err| {
                GraphQlError::Config(format!("parsing subscriptions manifest: {err}"))
            })?;
            info!(
                path = %subs_path.display(),
                entries = parsed.subscriptions.len(),
                "subscriptions manifest loaded (dynamic schema enabled)"
            );
            Some(parsed)
        } else {
            None
        };

    let playground = config.playground;
    let expected_tenant: Option<Arc<str>> = config.expected_tenant.as_deref().map(Arc::from);
    let router = if let Some(parsed) = parsed_manifest {
        let dyn_schema = dynamic_schema::build(parsed, graph_ctx)
            .map_err(|err| GraphQlError::Config(format!("building dynamic schema: {err}")))?;
        let state = DynamicAppState {
            schema: Arc::new(dyn_schema),
            expected_tenant: expected_tenant.clone(),
        };
        let mut r = Router::new().route(
            "/graphql",
            get(graphql_handler_dynamic).post(graphql_handler_dynamic),
        );
        if playground {
            r = r.route("/graphiql", get(graphiql));
        }
        r.route("/graphql/ws", get(graphql_ws_handler_dynamic))
            .with_state(state)
    } else {
        let schema = Arc::new(build_schema(graph_ctx));
        let state = AppState {
            schema,
            expected_tenant: expected_tenant.clone(),
        };
        let mut r = Router::new().route("/graphql", get(graphql_handler).post(graphql_handler));
        if playground {
            r = r.route("/graphiql", get(graphiql));
        }
        r.route("/graphql/ws", get(graphql_ws_handler))
            .with_state(state)
    };

    let listener =
        TcpListener::bind(&config.listen)
            .await
            .map_err(|source| GraphQlError::Bind {
                addr: config.listen.to_string(),
                source,
            })?;
    if let Some(readiness) = &readiness {
        readiness.mark_ready();
    }
    info!(
        listen = %config.listen,
        endpoint_http = "/graphql",
        endpoint_ws = "/graphql/ws",
        "graphql gateway listening"
    );

    let shutdown_for_serve = shutdown.clone();
    let readiness_for_shutdown = readiness.as_ref().map(|guard| guard.signal());
    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown_for_serve.cancelled().await;
            if let Some(readiness) = readiness_for_shutdown {
                readiness.mark_not_ready();
            }
        })
        .await;

    if let Err(err) = serve_result {
        return Err(GraphQlError::Serve(err));
    }
    info!("graphql gateway stopped");
    Ok(())
}

#[derive(Clone)]
struct AppState {
    schema: Arc<AppSchema>,
    /// Single tenant this deployment serves; client-asserted tenants are
    /// validated against it (see [`auth::validate_tenant`]). `None` = dev /
    /// legacy (isolation not enforced).
    expected_tenant: Option<Arc<str>>,
}

/// State for the dynamic-schema path (Model 2 / Federation Flavor B).
/// Same axum routes, different schema type — async-graphql's static
/// and dynamic schemas don't share a trait.
#[derive(Clone)]
struct DynamicAppState {
    schema: Arc<async_graphql::dynamic::Schema>,
    /// See [`AppState::expected_tenant`].
    expected_tenant: Option<Arc<str>>,
}

/// In-browser GraphiQL playground at `GET /graphiql`.
///
/// Unlike the stock `GraphiQLSource` (which only bakes in *static*
/// `connection_init` params), this page mounts GraphiQL with a custom
/// `graphql-ws` client that is **resume-aware**:
///
/// - `connectionParams` is a *function*, so on every (re)connect it
///   sends the current cursor as `resume_from_cursor` alongside the demo
///   auth token + tenant.
/// - an `on.message` hook scans each delivered event for a `cursor` field
///   and keeps the latest value in `localStorage`.
///
/// So a reconnect — close the socket, stop/restart a subscription, or a
/// full page reload — transparently replays whatever the client missed,
/// up to the stream's retention window. v1 auth is permissive (any
/// non-empty token), so the demo token is fine; change `tenant` to match
/// what you publish to.
async fn graphiql() -> impl IntoResponse {
    Html(PLAYGROUND_HTML)
}

/// Self-contained GraphiQL page with a resume-aware `graphql-ws` client.
/// Served verbatim from `GET /graphiql`.
const PLAYGROUND_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>VentStream — GraphiQL</title>
    <link rel="stylesheet" href="https://unpkg.com/graphiql@3/graphiql.min.css" />
    <style>
      body { margin: 0; }
      #vsbar {
        position: fixed; top: 0; left: 0; right: 0; height: 36px; z-index: 9999;
        display: flex; align-items: center; gap: 12px; padding: 0 14px;
        background: #0b1220; color: #e6edf3; border-bottom: 1px solid #2563eb;
        font: 13px ui-monospace, SFMono-Regular, Menlo, monospace;
      }
      #vsbar code { color: #7dd3fc; }
      #vsbar button {
        margin-left: auto; background: #1e293b; color: #e6edf3; cursor: pointer;
        border: 1px solid #334155; border-radius: 6px; padding: 4px 10px; font: inherit;
      }
      #graphiql { position: absolute; top: 36px; left: 0; right: 0; bottom: 0; }
    </style>
  </head>
  <body>
    <div id="vsbar">
      <strong>VentStream</strong>
      <span>resume cursor</span>
      <code id="seqbadge">(none)</code>
      <button onclick="vsResetCursor()">reset cursor</button>
    </div>
    <div id="graphiql">Loading…</div>

    <script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script src="https://unpkg.com/graphiql@3/graphiql.min.js"></script>
    <script src="https://unpkg.com/graphql-ws@5/umd/graphql-ws.min.js"></script>
    <script>
      // Surface any init error on the page instead of a silent "Loading…".
      window.addEventListener("error", function (e) {
        var el = document.getElementById("graphiql");
        if (el) el.textContent = "Playground error: " + (e && (e.message || e.error) || e);
      });

      // --- latest provider-neutral resume cursor -----------------------
      var CURSOR_KEY = "vs_resume_cursor";
      var lastCursor = localStorage.getItem(CURSOR_KEY) || null;
      function vsRenderBadge() {
        document.getElementById("seqbadge").textContent = lastCursor == null ? "(none)" : lastCursor;
      }
      function vsResetCursor() {
        lastCursor = null;
        localStorage.removeItem(CURSOR_KEY);
        vsRenderBadge();
      }
      function vsFindCursor(node, acc) {
        if (node && typeof node === "object") {
          for (var k in node) {
            var v = node[k];
            if (k === "cursor" && (typeof v === "string" || typeof v === "number")) {
              acc.v = String(v);
            } else if (v && typeof v === "object") {
              vsFindCursor(v, acc);
            }
          }
        }
        return acc;
      }
      vsRenderBadge();

      var proto = location.protocol === "https:" ? "wss" : "ws";
      var wsClient = graphqlWs.createClient({
        url: proto + "://" + location.host + "/graphql/ws",
        // Function form: re-evaluated on every (re)connect, so the latest
        // cursor rides along in connection_init.
        connectionParams: function () {
          var p = { authToken: "demo", tenant: "acme" };
          if (lastCursor != null) p.resume_from_cursor = lastCursor;
          return p;
        },
        on: {
          message: function (m) {
            if (m && m.type === "next" && m.payload && m.payload.data) {
              var found = vsFindCursor(m.payload.data, { v: null }).v;
              if (found != null) {
                lastCursor = found;
                localStorage.setItem(CURSOR_KEY, found);
                vsRenderBadge();
              }
            }
          },
        },
      });

      var fetcher = GraphiQL.createFetcher({ url: "/graphql", wsClient: wsClient });

      var defaultQuery =
        "# Typed subscriptions expose a provider-neutral `cursor`.\n" +
        "# Watch the cursor (top bar) advance as events arrive, then stop\n" +
        "# and re-run (or reload): missed events replay automatically.\n" +
        "subscription {\n" +
        "  orderStatusChanged(orderId: \"order_1\") {\n" +
        "    id\n    status\n    changedAt\n    cursor\n  }\n}\n";

      ReactDOM.createRoot(document.getElementById("graphiql")).render(
        React.createElement(GraphiQL, { fetcher: fetcher, defaultQuery: defaultQuery })
      );
    </script>
  </body>
</html>
"##;

/// HTTP entry for queries and mutations. Apollo Sandbox + Apollo
/// Studio + `graphql-codegen` all use this route for introspection.
///
/// Tenant on HTTP queries comes from headers (the WS path uses
/// `connection_init` instead): `Authorization: Bearer <token>` plus
/// `X-VS-Tenant: <tenant>`. v1 trusts a non-empty token; JWT
/// validation lands later. Introspection-only queries that don't
/// need tenant context still work without these headers.
async fn graphql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut inner = req.into_inner();
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let tenant = headers.get("x-vs-tenant").and_then(|v| v.to_str().ok());
    if let (Some(token), Some(tenant)) = (token, tenant) {
        // Same gate as the WS path: only attach the tenant when it matches the
        // deployment's configured tenant (when one is set). A mismatch leaves
        // no `Tenant` in the request data, so resolvers see an unauthenticated
        // request rather than the asserted (forged) tenant.
        if !token.is_empty()
            && !tenant.is_empty()
            && auth::validate_tenant(tenant, state.expected_tenant.as_deref()).is_ok()
        {
            inner = inner.data(Tenant(tenant.to_owned()));
        }
    }
    state.schema.execute(inner).await.into()
}

/// WebSocket upgrade for graphql-transport-ws (and the legacy
/// graphql-ws subprotocol). `GraphQLProtocol` is an axum extractor
/// from async-graphql-axum that negotiates from the
/// `Sec-WebSocket-Protocol` header.
async fn graphql_ws_handler(
    State(state): State<AppState>,
    protocol: GraphQLProtocol,
    websocket: WebSocketUpgrade,
) -> impl IntoResponse {
    let schema = (*state.schema).clone();
    let expected = state.expected_tenant;
    websocket
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            GraphQLWebSocket::new(socket, schema, protocol)
                .on_connection_init(move |value| async move {
                    match auth::extract(value, expected.as_deref()) {
                        Ok(init) => {
                            let mut data = async_graphql::Data::default();
                            data.insert(init.tenant);
                            // Lazy source shared by live-only operations.
                            // Resumed operations open isolated replay sources.
                            data.insert(ConnSourceCell::default());
                            // Connection-scoped resume cursor (None → live-only).
                            data.insert(ResumeCursor(init.resume_from_cursor));
                            Ok(data)
                        }
                        Err(msg) => {
                            warn!(error = %msg, "graphql connection_init rejected");
                            Err(async_graphql::Error::new(msg))
                        }
                    }
                })
                .serve()
                .await;
        })
}

// ---------------------------------------------------------------------------
// Dynamic-schema handlers — same surface as the static ones, but driven by
// `async_graphql::dynamic::Schema`.
// ---------------------------------------------------------------------------

async fn graphql_handler_dynamic(
    State(state): State<DynamicAppState>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut inner = req.into_inner();
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let tenant = headers.get("x-vs-tenant").and_then(|v| v.to_str().ok());
    if let (Some(token), Some(tenant)) = (token, tenant) {
        if !token.is_empty()
            && !tenant.is_empty()
            && auth::validate_tenant(tenant, state.expected_tenant.as_deref()).is_ok()
        {
            inner = inner.data(Tenant(tenant.to_owned()));
        }
    }
    state.schema.execute(inner).await.into()
}

async fn graphql_ws_handler_dynamic(
    State(state): State<DynamicAppState>,
    protocol: GraphQLProtocol,
    websocket: WebSocketUpgrade,
) -> impl IntoResponse {
    let schema = (*state.schema).clone();
    let expected = state.expected_tenant;
    websocket
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            GraphQLWebSocket::new(socket, schema, protocol)
                .on_connection_init(move |value| async move {
                    match auth::extract(value, expected.as_deref()) {
                        Ok(init) => {
                            let mut data = async_graphql::Data::default();
                            data.insert(init.tenant);
                            // Lazy source shared by live-only operations.
                            // Resumed operations open isolated replay sources.
                            data.insert(ConnSourceCell::default());
                            // Connection-scoped resume cursor (None → live-only).
                            data.insert(ResumeCursor(init.resume_from_cursor));
                            Ok(data)
                        }
                        Err(msg) => {
                            warn!(error = %msg, "graphql connection_init rejected");
                            Err(async_graphql::Error::new(msg))
                        }
                    }
                })
                .serve()
                .await;
        })
}
