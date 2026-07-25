//! Per-connection state machine.
//!
//! Each accepted WebSocket spawns one task that runs [`run_connection`].
//! The task owns the WebSocket and a bounded inbound mailbox; it
//! exits on:
//!
//! - Client `Close` frame
//! - WebSocket-level read or write error
//! - Pong timeout
//! - Detected slow-consumer state (signalled by the dispatcher)
//! - Shutdown
//!
//! On exit, RAII guards first cancel the connection-scoped broker pump,
//! then release provider resources and remove the connection from the registry.
//! The same cleanup order applies to graceful closes and every early return.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{future, SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tracing::{debug, info, trace, warn};
use ventstream_core::ShutdownToken;
use ventstream_protocol::SubjectPattern;
use ventstream_realtime::{
    BrokerError, BrokerKind, Cursor, GatewayRole, RealtimeBroker, SessionRequest,
};

use crate::config::WsConfig;
use crate::jetstream::ConsumerPumpExit;
use crate::protocol::{ClientMessage, ErrorCode, ServerMessage};
use crate::registry::{ConnectionId, ConnectionRegistry, Subscription};

const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// RAII guard that deregisters the connection on drop.
struct ConnectionDropGuard {
    registry: ConnectionRegistry,
    id: Option<ConnectionId>,
}

impl Drop for ConnectionDropGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.registry.remove(&id);
            debug!(connection_id = %id, "connection deregistered");
        }
    }
}

/// Cancels the per-connection consumer pump before its consumer handle drops.
struct PumpShutdownGuard(ShutdownToken);

impl Drop for PumpShutdownGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Entry point for an accepted WebSocket connection.
///
/// When `broker` is `Some`, durable mode is active and a per-connection
/// broker session is wired on. When `None`, NATS Core uses central fan-out.
pub(crate) async fn run_connection(
    socket: WebSocket,
    registry: ConnectionRegistry,
    config: WsConfig,
    shutdown: ShutdownToken,
    broker: Option<Arc<dyn RealtimeBroker>>,
) {
    let (mut ws_writer, mut ws_reader) = socket.split();

    // Phase 1: handshake. We accept exactly one Hello frame before
    // doing anything else.
    let (tenant, resume_cursor) = match timeout(Duration::from_secs(10), ws_reader.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            // The handshake decision (token + single-tenant gate + frame
            // shape) is a pure function so it's unit-testable without a live
            // socket — see `authorize_hello`. A rejection is sent + the socket
            // closed here, BEFORE any connection/consumer resource is created
            // below (the registry insert / broker session). That ordering
            // is what makes a forged-tenant Hello leak nothing.
            match authorize_hello(
                serde_json::from_str::<ClientMessage>(&text),
                config.expected_tenant.as_deref(),
            ) {
                Ok(accepted) => accepted,
                Err(reject) => {
                    warn!(
                        ?reject,
                        "ws handshake rejected before connection established"
                    );
                    let _ = write_with_timeout(&mut ws_writer, json_msg(&reject)).await;
                    close_with_code(&mut ws_writer, 1008, "handshake rejected").await;
                    return;
                }
            }
        }
        _ => {
            // Timed out / closed before Hello / non-text frame.
            close_with_code(&mut ws_writer, 1002, "hello required").await;
            return;
        }
    };
    let provider = broker
        .as_ref()
        .map_or(BrokerKind::NatsCore, |broker| broker.kind());
    if resume_cursor.is_some() && !provider.capabilities().replay {
        metrics::counter!(
            "vs_realtime_resume_attempts_total",
            "transport" => "ws",
            "provider" => provider.as_str(),
            "result" => "unsupported"
        )
        .increment(1);
        let _ = write_with_timeout(
            &mut ws_writer,
            json_msg(&ServerMessage::Error {
                code: ErrorCode::InvalidCursor,
                message: format!("realtime provider {provider} does not support resume cursors"),
            }),
        )
        .await;
        close_with_code(
            &mut ws_writer,
            1008,
            "resume is not supported by this provider",
        )
        .await;
        return;
    }
    // Build the outbound mailbox. The Sender goes to whoever's
    // pushing events at us (registry in Core mode, pump in JS mode);
    // the connection task itself holds only the Receiver — that way,
    // when the producer evicts us, the channel actually closes
    // (otherwise our retained Sender clone would keep it alive).
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<ServerMessage>(config.per_connection_mailbox);
    let registry_sender = outbound_tx.clone();
    let (connection_id, local_subs) = registry.insert(tenant.clone(), registry_sender);
    let _guard = ConnectionDropGuard {
        registry: registry.clone(),
        id: Some(connection_id),
    };

    // Durable mode: open a per-connection broker session and spawn the pump
    // that drains it into the outbound mailbox. The session owns any
    // provider-specific cleanup handle for the lifetime of the pump task.
    //
    // The pump takes ownership of `outbound_tx`; the registry has its own
    // clone. When both drop their senders, the receiver closes.
    let mut pump_shutdown = None;
    let mut pump_task = None;
    if let Some(broker) = broker {
        let request = SessionRequest {
            role: GatewayRole::WebSocket,
            connection_id: Arc::from(connection_id.to_string()),
            tenant: Arc::from(tenant.as_str()),
            subject_filter: None,
            resume_after: resume_cursor.clone(),
        };
        match broker.open_session(request).await {
            Ok(session) => {
                if resume_cursor.is_some() {
                    metrics::counter!(
                        "vs_realtime_resume_attempts_total",
                        "transport" => "ws",
                        "provider" => provider.as_str(),
                        "result" => "accepted"
                    )
                    .increment(1);
                }
                let pump_subs = std::sync::Arc::clone(&local_subs);
                let connection_shutdown = shutdown.child();
                let task_shutdown = connection_shutdown.clone();
                pump_task = Some(tokio::spawn(async move {
                    crate::jetstream::run_consumer_pump(
                        session,
                        outbound_tx,
                        pump_subs,
                        connection_id,
                        task_shutdown,
                        provider,
                    )
                    .await
                }));
                pump_shutdown = Some(connection_shutdown);
            }
            Err(err) => {
                let (code, result) = match &err {
                    BrokerError::ResumeExpired { .. } => (ErrorCode::ResumeExpired, "expired"),
                    BrokerError::CursorAhead { .. } => (ErrorCode::InvalidCursor, "ahead"),
                    BrokerError::CursorProviderMismatch { .. } | BrokerError::InvalidCursor(_) => {
                        (ErrorCode::InvalidCursor, "invalid")
                    }
                    _ => (ErrorCode::Internal, "consumer_error"),
                };
                if resume_cursor.is_some() {
                    metrics::counter!(
                        "vs_realtime_resume_attempts_total",
                        "transport" => "ws",
                        "provider" => provider.as_str(),
                        "result" => result
                    )
                    .increment(1);
                }
                let _ = write_with_timeout(
                    &mut ws_writer,
                    json_msg(&ServerMessage::Error {
                        code,
                        message: format!("could not open broker session: {err}"),
                    }),
                )
                .await;
                close_with_code(&mut ws_writer, 1008, "resume or consumer setup rejected").await;
                return;
            }
        }
    } else {
        // Core mode: the registry already holds its Sender clone;
        // the local `outbound_tx` is unused in this mode. Drop it
        // explicitly so the only remaining Sender is the registry's
        // — when the bus evicts us, that closes the channel.
        drop(outbound_tx);
    }
    let _pump_shutdown = pump_shutdown.map(PumpShutdownGuard);

    // Ack with Ready.
    if !write_with_timeout(
        &mut ws_writer,
        json_msg(&ServerMessage::Ready {
            connection_id: connection_id.to_string(),
            resumed_from: resume_cursor.as_ref().and_then(legacy_jetstream_sequence),
            resumed_from_cursor: resume_cursor.as_ref().map(Cursor::to_compatible_wire),
        }),
    )
    .await
    {
        return;
    }
    info!(
        connection_id = %connection_id,
        tenant = %tenant,
        "connection ready"
    );
    metrics::counter!(
        "vs_realtime_connections_total",
        "transport" => "ws",
        "provider" => provider.as_str(),
        "result" => "accepted"
    )
    .increment(1);

    // Phase 2: pump three concerns in parallel:
    //   - inbound reader (subscribe/unsubscribe/ping/close from client)
    //   - outbound mailbox (events the registry fans into us)
    //   - heartbeat + pong-deadline tracking
    let mut last_pong = Instant::now();
    let mut ping_tick = interval(config.ping_interval);
    ping_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let pump_completion = async move {
        match pump_task {
            Some(task) => Some(task.await),
            None => {
                future::pending::<Option<Result<ConsumerPumpExit, tokio::task::JoinError>>>().await
            }
        }
    };
    tokio::pin!(pump_completion);

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                close_with_code(&mut ws_writer, 1001, "server shutdown").await;
                return;
            }
            pump = &mut pump_completion => {
                let Some(pump) = pump else {
                    return;
                };
                match pump {
                    Ok(ConsumerPumpExit::Shutdown | ConsumerPumpExit::OutboundClosed) => {
                        debug!(connection_id = %connection_id, "consumer pump stopped with connection");
                        close_with_code(&mut ws_writer, 1000, "consumer stopped").await;
                    }
                    Ok(ConsumerPumpExit::SlowConsumer) => {
                        warn!(connection_id = %connection_id, "consumer pump evicted slow connection");
                        let _ = write_with_timeout(
                            &mut ws_writer,
                            json_msg(&ServerMessage::Error {
                                code: ErrorCode::SlowConsumer,
                                message: "outbound mailbox is full; reconnect with the last processed cursor".into(),
                            }),
                        )
                        .await;
                        close_with_code(&mut ws_writer, 1013, "slow consumer").await;
                    }
                    Ok(ConsumerPumpExit::Unrecoverable) => {
                        let _ = write_with_timeout(
                            &mut ws_writer,
                            json_msg(&ServerMessage::Error {
                                code: ErrorCode::Internal,
                                message: "event stream unavailable; reconnect with the last processed cursor".into(),
                            }),
                        )
                        .await;
                        close_with_code(&mut ws_writer, 1011, "event stream unavailable").await;
                    }
                    Err(err) => {
                        metrics::counter!(
                            "vs_realtime_terminal_failures_total",
                            "transport" => "ws",
                            "reason" => "pump_task_failed"
                        )
                        .increment(1);
                        warn!(connection_id = %connection_id, error = %err, "consumer pump task failed; closing connection");
                        let _ = write_with_timeout(
                            &mut ws_writer,
                            json_msg(&ServerMessage::Error {
                                code: ErrorCode::Internal,
                                message: "event stream failed; reconnect with the last processed cursor".into(),
                            }),
                        )
                        .await;
                        close_with_code(&mut ws_writer, 1011, "event stream failed").await;
                    }
                }
                return;
            }
            _ = ping_tick.tick() => {
                if last_pong.elapsed() > config.pong_timeout {
                    warn!(connection_id = %connection_id, "pong timeout; closing");
                    close_with_code(&mut ws_writer, 1001, "pong timeout").await;
                    return;
                }
                if !write_with_timeout(&mut ws_writer, Message::Ping(Vec::new().into())).await {
                    return;
                }
            }
            maybe_msg = outbound_rx.recv() => {
                let Some(msg) = maybe_msg else {
                    // All Senders dropped — typically because the
                    // registry evicted us for being a slow consumer.
                    // Close the WS so the client knows to reconnect.
                    debug!(
                        connection_id = %connection_id,
                        "outbound channel closed; closing connection"
                    );
                    close_with_code(&mut ws_writer, 1001, "outbound closed").await;
                    return;
                };
                if let ServerMessage::Event { cursor, seq, .. } = &msg {
                    trace!(connection_id = %connection_id, ?cursor, ?seq, "event frame writing");
                }
                // `to_text` splices the pre-serialized event JSON for
                // Event frames (the hot fan-out path) instead of
                // re-encoding the envelope per connection.
                if !write_with_timeout(&mut ws_writer, Message::Text(msg.to_text().into())).await {
                    metrics::counter!(
                        "vs_realtime_slow_consumers_total",
                        "transport" => "ws"
                    )
                    .increment(1);
                    warn!(connection_id = %connection_id, "event frame write failed or timed out; closing slow connection");
                    return;
                }
                metrics::counter!(
                    "vs_realtime_frames_written_total",
                    "transport" => "ws"
                )
                .increment(1);
            }
            inbound = ws_reader.next() => {
                let Some(frame) = inbound else { return; };
                let frame = match frame {
                    Ok(f) => f,
                    Err(err) => {
                        debug!(connection_id = %connection_id, error = %err, "ws read error");
                        return;
                    }
                };
                match frame {
                    Message::Text(text) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Subscribe { id, pattern }) => {
                                match SubjectPattern::anchored(&tenant, &pattern) {
                                    Ok(anchored) => {
                                        let anchored_str = anchored.as_str().to_owned();
                                        registry.add_subscription(
                                            &connection_id,
                                            Subscription {
                                                id: id.clone(),
                                                pattern: anchored,
                                            },
                                        );
                                        let _ = write_with_timeout(
                                            &mut ws_writer,
                                            json_msg(&ServerMessage::Subscribed {
                                                id,
                                                anchored_pattern: anchored_str,
                                            }),
                                        )
                                        .await;
                                    }
                                    Err(err) => {
                                        let _ = write_with_timeout(
                                            &mut ws_writer,
                                            json_msg(&ServerMessage::Error {
                                                code: ErrorCode::BadPattern,
                                                message: err.to_string(),
                                            }),
                                        )
                                        .await;
                                    }
                                }
                            }
                            Ok(ClientMessage::Unsubscribe { id }) => {
                                registry.remove_subscription(&connection_id, &id);
                                let _ = write_with_timeout(
                                    &mut ws_writer,
                                    json_msg(&ServerMessage::Unsubscribed { id }),
                                )
                                .await;
                            }
                            Ok(ClientMessage::Ping) => {
                                let _ = write_with_timeout(
                                    &mut ws_writer,
                                    json_msg(&ServerMessage::Pong),
                                )
                                .await;
                            }
                            Ok(ClientMessage::Hello { .. }) => {
                                let _ = write_with_timeout(
                                    &mut ws_writer,
                                    json_msg(&ServerMessage::Error {
                                        code: ErrorCode::NotReady,
                                        message: "duplicate Hello".into(),
                                    }),
                                )
                                .await;
                            }
                            Err(err) => {
                                let _ = write_with_timeout(
                                    &mut ws_writer,
                                    json_msg(&ServerMessage::Error {
                                        code: ErrorCode::BadFrame,
                                        message: err.to_string(),
                                    }),
                                )
                                .await;
                            }
                        }
                    }
                    Message::Binary(_) => {
                        let _ = write_with_timeout(
                            &mut ws_writer,
                            json_msg(&ServerMessage::Error {
                                code: ErrorCode::BadFrame,
                                message: "binary frames not supported".into(),
                            }),
                        )
                        .await;
                    }
                    Message::Ping(payload) => {
                        // axum auto-replies with Pong, but updating
                        // last_pong on the inbound side keeps us
                        // resilient if it ever changes.
                        last_pong = Instant::now();
                        let _ = write_with_timeout(&mut ws_writer, Message::Pong(payload)).await;
                    }
                    Message::Pong(_) => {
                        last_pong = Instant::now();
                    }
                    Message::Close(_) => {
                        debug!(connection_id = %connection_id, "client closed");
                        return;
                    }
                }
            }
        }
    }
}

async fn close_with_code<S>(writer: &mut S, code: u16, reason: &'static str)
where
    S: futures_util::Sink<Message> + Unpin,
{
    let _ = timeout(
        SOCKET_WRITE_TIMEOUT,
        writer.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

async fn write_with_timeout<S>(writer: &mut S, message: Message) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    matches!(
        timeout(SOCKET_WRITE_TIMEOUT, writer.send(message)).await,
        Ok(Ok(()))
    )
}

/// Helper: serialize a [`ServerMessage`] into a `Message::Text`.
/// Serialization can in principle fail (it cannot, for our typed
/// inputs), so we encode a fallback error frame on the off chance.
fn json_msg(msg: &ServerMessage) -> Message {
    match serde_json::to_string(msg) {
        Ok(s) => Message::Text(s.into()),
        Err(err) => Message::Text(format!(r#"{{"type":"error","message":"{err}"}}"#).into()),
    }
}

/// Decide the handshake outcome for the first client frame.
///
/// Pure (no I/O) so the gate is unit-testable without a live socket. Returns
/// `Ok((tenant, resume_cursor))` to proceed, or `Err(ServerMessage::Error)`
/// — the frame to send before closing. `run_connection` calls this BEFORE it
/// creates any connection resource, so a rejected client (forged tenant, empty
/// token, wrong frame, unparseable) never reaches the registry or a consumer.
fn authorize_hello(
    parsed: Result<ClientMessage, serde_json::Error>,
    expected: Option<&str>,
) -> Result<(String, Option<Cursor>), ServerMessage> {
    match parsed {
        Ok(ClientMessage::Hello {
            tenant,
            token,
            resume_from_seq,
            resume_from_cursor,
        }) => {
            if token.is_empty() {
                return Err(ServerMessage::Error {
                    code: ErrorCode::AuthFailed,
                    message: "empty token".into(),
                });
            }
            if !tenant_authorized(&tenant, expected) {
                return Err(ServerMessage::Error {
                    code: ErrorCode::AuthFailed,
                    message: format!("tenant '{tenant}' is not authorized for this deployment"),
                });
            }
            let resume_cursor = match (resume_from_seq, resume_from_cursor) {
                (Some(legacy), Some(current)) if legacy != current => {
                    return Err(ServerMessage::Error {
                        code: ErrorCode::BadFrame,
                        message: "resume_from_seq and resume_from_cursor disagree".to_owned(),
                    });
                }
                (Some(legacy), _) => Some(legacy),
                (_, current) => current,
            };
            Ok((tenant, resume_cursor))
        }
        Ok(_) => Err(ServerMessage::Error {
            code: ErrorCode::NotReady,
            message: "first frame must be Hello".into(),
        }),
        Err(err) => Err(ServerMessage::Error {
            code: ErrorCode::BadFrame,
            message: format!("parse Hello: {err}"),
        }),
    }
}

fn legacy_jetstream_sequence(cursor: &Cursor) -> Option<u64> {
    cursor.legacy_jetstream_value()?.parse().ok()
}

fn tenant_authorized(asserted: &str, expected: Option<&str>) -> bool {
    expected.is_none_or(|configured| configured == asserted)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use ventstream_core::ShutdownToken;
    use ventstream_realtime::Cursor;

    use super::{authorize_hello, tenant_authorized, PumpShutdownGuard};
    use crate::protocol::{ClientMessage, ErrorCode, ServerMessage};

    fn hello(tenant: &str, token: &str) -> Result<ClientMessage, serde_json::Error> {
        Ok(ClientMessage::Hello {
            tenant: tenant.to_owned(),
            token: token.to_owned(),
            resume_from_seq: Some(Cursor::jetstream(7)),
            resume_from_cursor: None,
        })
    }

    #[test]
    fn pump_guard_cancels_only_the_connection_child() {
        let process = ShutdownToken::new();
        let connection = process.child();
        {
            let _guard = PumpShutdownGuard(connection.clone());
            assert!(!connection.is_cancelled());
        }
        assert!(connection.is_cancelled());
        assert!(!process.is_cancelled());
    }

    #[test]
    fn forged_tenant_rejected_at_handshake() {
        // The core C1 drill at the decision boundary: a Hello asserting a
        // tenant other than the one this deployment serves is rejected with
        // AuthFailed, and the message does NOT leak the configured tenant.
        let err = authorize_hello(hello("victim", "nonempty"), Some("acme"))
            .expect_err("forged tenant must be rejected");
        match err {
            ServerMessage::Error { code, message } => {
                assert!(matches!(code, ErrorCode::AuthFailed));
                assert!(message.contains("victim"));
                assert!(!message.contains("acme"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn matching_tenant_accepted_with_resume_seq() {
        let (tenant, resume) =
            authorize_hello(hello("acme", "nonempty"), Some("acme")).expect("matching accepted");
        assert_eq!(tenant, "acme");
        assert_eq!(resume, Some(Cursor::jetstream(7)));
    }

    #[test]
    fn unconfigured_deployment_accepts_asserted_tenant() {
        // VS_TENANT unset (dev/legacy): asserted tenant flows through.
        let (tenant, _) =
            authorize_hello(hello("whatever", "nonempty"), None).expect("unconfigured accepts");
        assert_eq!(tenant, "whatever");
    }

    #[test]
    fn empty_token_is_rejected() {
        let err = authorize_hello(hello("acme", ""), Some("acme")).expect_err("empty token");
        match err {
            ServerMessage::Error { code, message } => {
                assert!(matches!(code, ErrorCode::AuthFailed));
                assert!(message.contains("empty token"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn wrong_first_frame_is_not_ready() {
        let err = authorize_hello(Ok(ClientMessage::Ping), Some("acme"))
            .expect_err("ping is not a valid first frame");
        assert!(matches!(
            err,
            ServerMessage::Error {
                code: ErrorCode::NotReady,
                ..
            }
        ));
    }

    #[test]
    fn unparseable_frame_is_bad_frame() {
        let parsed = serde_json::from_str::<ClientMessage>("{ not valid");
        let err = authorize_hello(parsed, Some("acme")).expect_err("bad json");
        assert!(matches!(
            err,
            ServerMessage::Error {
                code: ErrorCode::BadFrame,
                ..
            }
        ));
    }

    #[test]
    fn tenant_gate_matches_expected_tenant() {
        assert!(tenant_authorized("acme", Some("acme")));
        assert!(!tenant_authorized("other", Some("acme")));
        assert!(tenant_authorized("anything", None));
    }
}
