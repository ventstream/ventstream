//! HTTP+WebSocket server entry point.
//!
//! Routes:
//! - `GET /ws` → WebSocket upgrade, handed to
//!   [`run_connection`](crate::connection::run_connection).
//!
//! Liveness/readiness is served centrally by the engine binary's health
//! server (`VS_HEALTH_LISTEN`), not on this port.
//!
//! The server runs alongside one of three broker paths:
//!
//! - **Core mode** (jetstream `None`): a single NATS Core subscriber
//!   fans bus events into every matching local connection.
//! - **JetStream mode** (jetstream `Some(...)`): the stream is
//!   bootstrapped, the orphan reaper is spawned, and each connection
//!   gets its own per-connection durable consumer. There is no
//!   central bus subscriber in this mode.
//! - **Redis Streams mode** (redis_streams `Some(...)`): each tenant has
//!   one shared stream tailer and connections open replay-aware sessions.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use ventstream_core::{ReadinessSignal, ShutdownToken};
use ventstream_realtime::RealtimeBroker;

use crate::bus::{connect_bus, run_bus};
use crate::config::WsConfig;
use crate::connection::run_connection;
use crate::error::WsError;
use crate::jetstream::ensure_stream;
use crate::registry::ConnectionRegistry;

/// Shared application state passed into Axum handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) registry: ConnectionRegistry,
    pub(crate) config: Arc<WsConfig>,
    pub(crate) shutdown: ShutdownToken,
    /// Durable broker selected for per-connection event sessions.
    pub(crate) broker: Option<Arc<dyn RealtimeBroker>>,
}

/// Run the WebSocket gateway until `shutdown` fires.
///
/// `active_gauge` is the shared established-connection counter. The
/// binary owns it so its `/readyz` + `/metrics` handlers can read the
/// same value this gateway maintains (drives readiness gating and the
/// `vs_ws_active_connections` metric). Pass a fresh
/// `Arc::new(AtomicUsize::new(0))` if you don't need to observe it.
pub async fn run(
    config: WsConfig,
    shutdown: ShutdownToken,
    active_gauge: Arc<AtomicUsize>,
) -> Result<(), WsError> {
    run_inner(config, shutdown, active_gauge, None).await
}

/// Run the WebSocket gateway and publish listener readiness to the health server.
///
/// The signal remains false through broker initialization and flips true only
/// after the traffic listener is bound. Shutdown or any exit path resets it.
pub async fn run_with_readiness(
    config: WsConfig,
    shutdown: ShutdownToken,
    active_gauge: Arc<AtomicUsize>,
    readiness: ReadinessSignal,
) -> Result<(), WsError> {
    run_inner(config, shutdown, active_gauge, Some(readiness)).await
}

async fn run_inner(
    config: WsConfig,
    shutdown: ShutdownToken,
    active_gauge: Arc<AtomicUsize>,
    readiness: Option<ReadinessSignal>,
) -> Result<(), WsError> {
    let readiness = readiness.as_ref().map(ReadinessSignal::guard);
    let registry = ConnectionRegistry::with_gauge(active_gauge);

    if config.jetstream.is_some() && config.redis_streams.is_some() {
        return Err(WsError::Broker(
            "JetStream and Redis Streams cannot both be enabled".to_owned(),
        ));
    }

    // Pre-flight the selected broker before binding the traffic listener.
    let (broker, bus_task): (Option<Arc<dyn RealtimeBroker>>, _) = if let Some(redis_cfg) =
        &config.redis_streams
    {
        let redis_broker = ventstream_redis::RedisStreamsBroker::connect(redis_cfg.clone())
            .await
            .map_err(|err| WsError::Broker(err.to_string()))?;
        info!(key_prefix = %redis_cfg.key_prefix, "Redis Streams mode active");
        (Some(Arc::new(redis_broker)), tokio::spawn(async {}))
    } else if let Some(js_cfg) = &config.jetstream {
        let client = async_nats::connect(&config.nats_url)
            .await
            .map_err(|err| WsError::Nats {
                url: config.nats_url.clone(),
                detail: err.to_string(),
            })?;
        let js = async_nats::jetstream::new(client);
        ensure_stream(&js, js_cfg).await?;
        let js_broker = ventstream_jetstream::JetStreamBroker::new(
            js.clone(),
            ventstream_jetstream::JetStreamBrokerConfig {
                stream_name: js_cfg.stream_name.clone(),
                pod_id: js_cfg.pod_id.clone(),
                consumer_inactive_threshold: js_cfg.consumer_inactive_threshold,
            },
        );
        info!(
            stream = %js_cfg.stream_name,
            pod_id = %js_cfg.pod_id,
            "JetStream mode active"
        );

        // Spawn the shared orphan reaper using the adapter's live-session set.
        let reaper_js = js.clone();
        let reaper_active = js_broker.active_set();
        let reaper_stream = js_cfg.stream_name.clone();
        let reaper_pod = js_cfg.pod_id.clone();
        let reaper_interval = js_cfg.reaper_interval;
        let reaper_shutdown = shutdown.clone();
        tokio::spawn(async move {
            ventstream_jetstream::run_reaper(
                reaper_js,
                reaper_stream,
                "ws".to_owned(),
                reaper_pod,
                reaper_interval,
                reaper_active,
                reaper_shutdown,
            )
            .await;
        });

        // No central bus in JetStream mode — each connection has its
        // own consumer. Return a noop task so the join later works.
        (Some(Arc::new(js_broker)), tokio::spawn(async {}))
    } else {
        // Core mode: connect and subscribe before binding the listener so
        // readiness cannot go green while the event bus is still starting.
        let subscriptions = connect_bus(&config.nats_url, &config.bus_subjects).await?;
        let bus_registry = registry.clone();
        let bus_shutdown = shutdown.clone();
        let bus_url = config.nats_url.clone();
        let task = tokio::spawn(async move {
            if let Err(err) =
                run_bus(subscriptions, bus_url, bus_registry, bus_shutdown.clone()).await
            {
                error!(error = %err, "bus subscriber exited with error");
                bus_shutdown.cancel();
            }
        });
        (None, task)
    };

    let state = AppState {
        registry: registry.clone(),
        config: Arc::new(config.clone()),
        shutdown: shutdown.clone(),
        broker,
    };

    let router = Router::new()
        .route("/ws", get(ws_upgrade))
        .with_state(state);

    let listener = TcpListener::bind(&config.listen)
        .await
        .map_err(|source| WsError::Bind {
            addr: config.listen.to_string(),
            source,
        })?;
    if let Some(readiness) = &readiness {
        readiness.mark_ready();
    }
    info!(
        listen = %config.listen,
        subjects = ?config.bus_subjects,
        jetstream = config.jetstream.is_some(),
        redis_streams = config.redis_streams.is_some(),
        "ws gateway listening"
    );

    // Run the HTTP server until shutdown.
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

    // Drain background tasks (bus or noop) tied to the shutdown token.
    let _ = bus_task.await;

    if let Err(err) = serve_result {
        return Err(WsError::Serve(err));
    }
    info!("ws gateway stopped");
    Ok(())
}

async fn ws_upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    // Connection cap (the local OOM backstop). Atomically reserve a slot
    // *before* `on_upgrade` allocates the socket task, the per-connection
    // JetStream consumer, and the outbound mailbox — so a surge can't
    // push the pod past its memory budget. Reserving here (not at the
    // post-Hello insert) makes the cap a HARD ceiling under concurrency:
    // a burst of simultaneous upgrades can't all pass a soft check and
    // then overshoot. On rejection we answer with a standard
    // `503 + Retry-After` rather than a TCP refusal — an explicit,
    // readable "I'm full, come back later" the client backs off on
    // (jittered), distinct from a socket/network failure. The LB should
    // already be routing new connections elsewhere via `/readyz`; this
    // only fires for the readiness-propagation race window.
    let Some(admit) = state.registry.try_admit(state.config.max_connections) else {
        metrics::counter!("vs_ws_connections_rejected_total").increment(1);
        warn!(
            active = state.registry.active_connections(),
            max = state.config.max_connections,
            "connection cap reached; rejecting WS upgrade with 503"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "5")],
            "connection cap reached\n",
        )
            .into_response();
    };

    ws.on_upgrade(move |socket: WebSocket| async move {
        // Hold the admission slot for the whole connection; it releases
        // on drop when `run_connection` returns (any exit path).
        let _admit = admit;
        run_connection(
            socket,
            state.registry,
            (*state.config).clone(),
            state.shutdown,
            state.broker,
        )
        .await;
    })
    .into_response()
}
