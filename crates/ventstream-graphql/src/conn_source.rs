//! Provider-neutral event sources for GraphQL WebSocket subscriptions.
//!
//! Live-only operations share one broker session per client connection.
//! Operations that supply a replay cursor use their own subject-filtered
//! session so one operation cannot acknowledge replay events before another
//! operation has attached. Falling behind a bounded local broadcast is
//! terminal and visible to the client; no gap is reported as a clean
//! completion.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Error as GqlError, ErrorExtensions, Result as GqlResult};
use futures_util::{Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::{broadcast, Notify, OnceCell};
use tracing::{info, trace, warn, Instrument};
use ventstream_core::ShutdownToken;
use ventstream_protocol::{Event as ProtocolEvent, SubjectPattern};
use ventstream_realtime::{
    BrokerError, BrokerEvent, Cursor, EventSession, GatewayRole, SessionRequest,
};

use crate::schema::{GraphContext, SubscriptionEvent};

/// Lazy per-connection source inserted during `connection_init`.
pub(crate) type ConnSourceCell = Arc<OnceCell<Arc<ConnEventSource>>>;

/// Provider-neutral cursor parsed from `connection_init`.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResumeCursor(pub(crate) Option<Cursor>);

pub(crate) const RESUME_CURSOR_ARGUMENT: &str = "resumeFromCursor";

static ACTIVE_GRAPHQL_OPERATIONS: AtomicU64 = AtomicU64::new(0);
const BROADCAST_BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(1);
const BROADCAST_BACKPRESSURE_POLL: Duration = Duration::from_millis(1);

struct GraphQlOperationGuard {
    operation_notify: Arc<Notify>,
}

impl GraphQlOperationGuard {
    fn new(operation_notify: Arc<Notify>) -> Self {
        let active = ACTIVE_GRAPHQL_OPERATIONS.fetch_add(1, Ordering::AcqRel) + 1;
        metrics::gauge!(
            "vs_realtime_operations_active",
            "transport" => "graphql"
        )
        .set(active as f64);
        metrics::counter!(
            "vs_realtime_operations_total",
            "transport" => "graphql"
        )
        .increment(1);
        Self { operation_notify }
    }
}

impl Drop for GraphQlOperationGuard {
    fn drop(&mut self) {
        let active = ACTIVE_GRAPHQL_OPERATIONS.fetch_sub(1, Ordering::AcqRel) - 1;
        metrics::gauge!(
            "vs_realtime_operations_active",
            "transport" => "graphql"
        )
        .set(active as f64);
        self.operation_notify.notify_waiters();
    }
}

/// One broker session and local broadcast for a GraphQL socket.
pub(crate) struct ConnEventSource {
    sender: broadcast::Sender<ConnMessage>,
    terminal: Arc<Mutex<Option<TerminalFailure>>>,
    shutdown: ShutdownToken,
    operation_notify: Arc<Notify>,
}

struct PumpControl {
    terminal: Arc<Mutex<Option<TerminalFailure>>>,
    shutdown: ShutdownToken,
    operation_notify: Arc<Notify>,
}

#[derive(Clone)]
enum ConnMessage {
    Event(Arc<SubscriptionEvent>),
    Terminal(TerminalFailure),
}

#[derive(Clone)]
struct TerminalFailure {
    reason: Arc<str>,
    code: &'static str,
}

impl TerminalFailure {
    fn graphql_error(&self) -> GqlError {
        GqlError::new(self.reason.to_string()).extend_with(|_, extensions| {
            extensions.set("code", self.code);
        })
    }
}

/// Converts panic/cancellation mistakes in the detached pump into an explicit
/// operation error instead of leaving subscribers parked forever.
struct PumpTerminalGuard {
    sender: broadcast::Sender<ConnMessage>,
    terminal: Arc<Mutex<Option<TerminalFailure>>>,
    provider: &'static str,
    armed: bool,
}

impl PumpTerminalGuard {
    fn new(
        sender: broadcast::Sender<ConnMessage>,
        terminal: Arc<Mutex<Option<TerminalFailure>>>,
        provider: &'static str,
    ) -> Self {
        Self {
            sender,
            terminal,
            provider,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn terminate(&mut self, code: &'static str, reason: impl Into<Arc<str>>) {
        let failure = TerminalFailure {
            reason: reason.into(),
            code,
        };
        *self.terminal.lock() = Some(failure.clone());
        let _ = self.sender.send(ConnMessage::Terminal(failure));
        self.armed = false;
    }
}

impl Drop for PumpTerminalGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let failure = TerminalFailure {
            reason: Arc::from("GraphQL event pump exited unexpectedly"),
            code: "EVENT_PUMP_FAILED",
        };
        *self.terminal.lock() = Some(failure.clone());
        let _ = self.sender.send(ConnMessage::Terminal(failure));
        metrics::counter!(
            "vs_realtime_terminal_failures_total",
            "transport" => "graphql",
            "provider" => self.provider,
            "reason" => "pump_task_failed"
        )
        .increment(1);
    }
}

impl Drop for ConnEventSource {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn broker_setup_error(error: &BrokerError, resumed: bool, provider: &'static str) -> GqlError {
    let (code, result) = match error {
        BrokerError::ResumeExpired { .. } => ("RESUME_EXPIRED", "expired"),
        BrokerError::CursorAhead { .. }
        | BrokerError::CursorProviderMismatch { .. }
        | BrokerError::InvalidCursor(_) => ("INVALID_CURSOR", "invalid"),
        _ => ("EVENT_STREAM_UNAVAILABLE", "consumer_error"),
    };
    if resumed {
        metrics::counter!(
            "vs_realtime_resume_attempts_total",
            "transport" => "graphql",
            "provider" => provider,
            "result" => result
        )
        .increment(1);
    }
    GqlError::new(error.to_string()).extend_with(|_, extensions| {
        extensions.set("code", code);
    })
}

pub(crate) fn resolve_resume_cursor(
    operation_cursor: Option<&str>,
    connection_cursor: Option<Cursor>,
) -> GqlResult<Option<Cursor>> {
    operation_cursor.map_or(Ok(connection_cursor), |cursor| {
        Cursor::from_wire_compatible(cursor)
            .map(Some)
            .map_err(|error| {
                GqlError::new(format!("invalid operation resume cursor: {error}")).extend_with(
                    |_, extensions| {
                        extensions.set("code", "INVALID_CURSOR");
                    },
                )
            })
    })
}

async fn accept_or_terminate(
    session: &mut dyn EventSession,
    delivery: &BrokerEvent,
    guard: &mut PumpTerminalGuard,
    provider: &'static str,
) -> bool {
    if let Err(error) = session.accepted(delivery).await {
        metrics::counter!(
            "vs_realtime_ack_failures_total",
            "transport" => "graphql",
            "provider" => provider
        )
        .increment(1);
        guard.terminate(
            "EVENT_STREAM_ACCEPT_FAILED",
            format!("event stream acceptance failed: {error}"),
        );
        return false;
    }
    true
}

async fn wait_for_operation_receiver(
    sender: &broadcast::Sender<ConnMessage>,
    operation_notify: &Notify,
    shutdown: &ShutdownToken,
) -> bool {
    loop {
        if sender.receiver_count() > 0 {
            return true;
        }
        let notified = operation_notify.notified();
        tokio::pin!(notified);
        // Register the waiter before the state recheck. `notify_waiters()` does
        // not retain a permit for a future that has not been polled or enabled,
        // so omitting this leaves a lost-wakeup window where an operation has
        // attached but the broker pump sleeps indefinitely.
        notified.as_mut().enable();
        if sender.receiver_count() > 0 {
            return true;
        }
        tokio::select! {
            () = shutdown.cancelled() => return false,
            () = &mut notified => {}
        }
    }
}

async fn run_session_pump(
    mut session: Box<dyn EventSession>,
    sender: broadcast::Sender<ConnMessage>,
    broadcast_capacity: usize,
    control: PumpControl,
    provider: &'static str,
) {
    let PumpControl {
        terminal,
        shutdown,
        operation_notify,
    } = control;
    let mut guard = PumpTerminalGuard::new(sender.clone(), terminal, provider);

    // A resumed session must not drain replay before the operation that
    // created it has attached its broadcast receiver.
    if !wait_for_operation_receiver(&sender, &operation_notify, &shutdown).await {
        guard.disarm();
        return;
    }

    loop {
        // A GraphQL socket can remain connected while its operation is
        // completed and replaced. Do not pull from the retained broker during
        // that receiver-less handoff: accepting such a delivery would advance
        // the consumer while no operation could observe the event.
        if !wait_for_operation_receiver(&sender, &operation_notify, &shutdown).await {
            guard.disarm();
            return;
        }

        // Redis Streams has no broker-side acknowledgement to pace live
        // delivery. Do not let a healthy burst eagerly fill every
        // connection-local broadcast with independently decoded payloads.
        // Wait briefly for the slowest operation, then fail explicitly so the
        // client can resume from its last processed cursor.
        let capacity_deadline = tokio::time::Instant::now() + BROADCAST_BACKPRESSURE_TIMEOUT;
        while sender.len() >= broadcast_capacity {
            if tokio::time::Instant::now() >= capacity_deadline {
                metrics::counter!(
                    "vs_realtime_slow_consumers_total",
                    "transport" => "graphql",
                    "provider" => provider
                )
                .increment(1);
                guard.terminate(
                    "SUBSCRIPTION_LAGGED",
                    "subscription operation did not drain its bounded buffer; reconnect with the last processed cursor",
                );
                return;
            }
            tokio::select! {
                () = shutdown.cancelled() => {
                    guard.disarm();
                    return;
                }
                () = tokio::time::sleep(BROADCAST_BACKPRESSURE_POLL) => {}
            }
        }
        let delivery = tokio::select! {
            () = shutdown.cancelled() => {
                guard.disarm();
                return;
            }
            result = session.next() => match result {
                Ok(Some(delivery)) => delivery,
                Ok(None) => {
                    guard.terminate(
                        "EVENT_STREAM_UNAVAILABLE",
                        "event stream ended; reconnect with the last processed cursor",
                    );
                    return;
                }
                Err(error) => {
                    metrics::counter!(
                        "vs_realtime_terminal_failures_total",
                        "transport" => "graphql",
                        "provider" => provider,
                        "reason" => "broker_unrecoverable"
                    )
                    .increment(1);
                    guard.terminate(
                        "EVENT_STREAM_UNAVAILABLE",
                        format!("event stream unavailable: {error}"),
                    );
                    return;
                }
            }
        };
        metrics::counter!(
            "vs_realtime_broker_messages_total",
            "transport" => "graphql",
            "provider" => provider
        )
        .increment(1);

        let Some(cursor) = delivery.cursor.as_ref() else {
            guard.terminate(
                "EVENT_STREAM_CURSOR_UNAVAILABLE",
                "event stream cursor unavailable; reconnect with the last processed cursor",
            );
            return;
        };
        let envelope = match serde_json::from_slice::<ProtocolEvent>(&delivery.payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                metrics::counter!(
                    "vs_realtime_events_dropped_total",
                    "transport" => "graphql",
                    "provider" => provider,
                    "reason" => "malformed_json"
                )
                .increment(1);
                warn!(subject = %delivery.subject, %error, "dropping malformed broker envelope");
                if !accept_or_terminate(session.as_mut(), &delivery, &mut guard, provider).await {
                    return;
                }
                continue;
            }
        };
        if let Err(error) = envelope.validate() {
            metrics::counter!(
                "vs_realtime_events_dropped_total",
                "transport" => "graphql",
                "provider" => provider,
                "reason" => "invalid_envelope"
            )
            .increment(1);
            warn!(subject = %delivery.subject, %error, "dropping invalid broker envelope");
            if !accept_or_terminate(session.as_mut(), &delivery, &mut guard, provider).await {
                return;
            }
            continue;
        }

        trace!(event_id = %envelope.id, %cursor, "GraphQL broker event validated");
        let event = Arc::new(SubscriptionEvent::from_protocol(
            envelope,
            delivery.subject.to_string(),
            cursor,
        ));
        loop {
            if !wait_for_operation_receiver(&sender, &operation_notify, &shutdown).await {
                guard.disarm();
                return;
            }
            match sender.send(ConnMessage::Event(Arc::clone(&event))) {
                Ok(_) => break,
                Err(_) => {
                    metrics::counter!(
                        "vs_realtime_delivery_retries_total",
                        "transport" => "graphql",
                        "provider" => provider,
                        "reason" => "operation_handoff"
                    )
                    .increment(1);
                }
            }
        }
        metrics::counter!(
            "vs_realtime_events_enqueued_total",
            "transport" => "graphql",
            "provider" => provider
        )
        .increment(1);
        if !accept_or_terminate(session.as_mut(), &delivery, &mut guard, provider).await {
            return;
        }
    }
}

impl ConnEventSource {
    pub(crate) async fn create(
        gctx: &GraphContext,
        tenant: &str,
        subject_filter: Option<SubjectPattern>,
        resume_from_cursor: Option<Cursor>,
    ) -> GqlResult<Arc<Self>> {
        let source_id = ulid::Ulid::new().to_string();
        let resumed = resume_from_cursor.is_some();
        let provider = gctx.broker.kind().as_str();
        let session = gctx
            .broker
            .open_session(SessionRequest {
                role: GatewayRole::GraphQl,
                connection_id: Arc::from(source_id.as_str()),
                tenant: Arc::from(tenant),
                subject_filter: subject_filter.clone(),
                resume_after: resume_from_cursor.clone(),
            })
            .await
            .map_err(|error| broker_setup_error(&error, resumed, provider))?;
        if resumed {
            metrics::counter!(
                "vs_realtime_resume_attempts_total",
                "transport" => "graphql",
                "provider" => provider,
                "result" => "accepted"
            )
            .increment(1);
        }

        let (sender, _receiver) = broadcast::channel(gctx.broadcast_capacity.max(1));
        let broadcast_capacity = gctx.broadcast_capacity.max(1);
        let terminal = Arc::new(Mutex::new(None));
        let shutdown = ShutdownToken::new();
        let operation_notify = Arc::new(Notify::new());
        info!(
            source_id,
            tenant,
            provider = %gctx.broker.kind(),
            subject_filter = subject_filter.as_ref().map(SubjectPattern::as_str),
            resumed_from = ?resume_from_cursor,
            "GraphQL event source started"
        );
        tokio::spawn(
            run_session_pump(
                session,
                sender.clone(),
                broadcast_capacity,
                PumpControl {
                    terminal: Arc::clone(&terminal),
                    shutdown: shutdown.clone(),
                    operation_notify: Arc::clone(&operation_notify),
                },
                provider,
            )
            .instrument(tracing::info_span!(
                "graphql_connection_pump",
                source_id,
                tenant
            )),
        );

        Ok(Arc::new(Self {
            sender,
            terminal,
            shutdown,
            operation_notify,
        }))
    }

    fn subscribe(&self) -> GqlResult<broadcast::Receiver<ConnMessage>> {
        let receiver = self.sender.subscribe();
        self.operation_notify.notify_waiters();
        if let Some(failure) = self.terminal.lock().as_ref() {
            return Err(failure.graphql_error());
        }
        Ok(receiver)
    }
}

fn filtered_stream(
    mut receiver: broadcast::Receiver<ConnMessage>,
    pattern: SubjectPattern,
    provider: &'static str,
) -> impl Stream<Item = GqlResult<SubscriptionEvent>> {
    async_stream::stream! {
        loop {
            match receiver.recv().await {
                Ok(ConnMessage::Event(event)) if pattern.matches_str(&event.subject) => {
                    trace!(event_id = event.id.as_str(), cursor = %event.cursor, "GraphQL subscription event emitted");
                    metrics::counter!(
                        "vs_realtime_events_emitted_total",
                        "transport" => "graphql",
                        "provider" => provider
                    )
                    .increment(1);
                    yield Ok((*event).clone());
                }
                Ok(ConnMessage::Event(_)) => {}
                Ok(ConnMessage::Terminal(failure)) => {
                    yield Err(failure.graphql_error());
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    metrics::counter!(
                        "vs_realtime_slow_consumers_total",
                        "transport" => "graphql",
                        "provider" => provider
                    )
                    .increment(1);
                    yield Err(GqlError::new(format!(
                        "subscription lagged: {skipped} event(s) dropped; reconnect with the last processed cursor"
                    )).extend_with(|_, extensions| {
                        extensions.set("code", "SUBSCRIPTION_LAGGED");
                    }));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

/// Subscribe one operation to an event source.
///
/// Live-only operations share the connection source. A cursor makes the
/// operation replayable and therefore isolated: each operation owns a
/// subject-filtered broker session and cannot advance another operation's
/// replay position.
pub(crate) async fn connection_stream(
    cell: ConnSourceCell,
    gctx: GraphContext,
    tenant: String,
    pattern: SubjectPattern,
    resume_from_cursor: Option<Cursor>,
) -> GqlResult<impl Stream<Item = GqlResult<SubscriptionEvent>>> {
    let provider = gctx.broker.kind().as_str();
    let source = if resume_from_cursor.is_some() {
        ConnEventSource::create(&gctx, &tenant, Some(pattern.clone()), resume_from_cursor).await?
    } else {
        Arc::clone(
            cell.get_or_try_init(|| async {
                ConnEventSource::create(&gctx, &tenant, None, None).await
            })
            .await?,
        )
    };
    let receiver = source.subscribe()?;
    let operation_notify = Arc::clone(&source.operation_notify);
    Ok(async_stream::stream! {
        let _operation_guard = GraphQlOperationGuard::new(operation_notify);
        let _source = source;
        let stream = filtered_stream(receiver, pattern, provider);
        futures_util::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            yield item;
        }
    })
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct SingleEventSession {
        event: Option<BrokerEvent>,
        accepted: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl EventSession for SingleEventSession {
        async fn next(&mut self) -> Result<Option<BrokerEvent>, BrokerError> {
            Ok(self.event.take())
        }

        async fn accepted(&mut self, _event: &BrokerEvent) -> Result<(), BrokerError> {
            self.accepted.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn sub_event(subject: &str) -> SubscriptionEvent {
        let event = ventstream_protocol::Event::publish(
            "acme",
            "verifyEvt",
            "x",
            None,
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("epoch"),
            serde_json::json!({}),
            ventstream_protocol::Metadata::default(),
        )
        .expect("valid event");
        SubscriptionEvent::from_protocol(event, subject.to_owned(), &Cursor::jetstream(1))
    }

    #[tokio::test]
    async fn lag_yields_terminal_error_and_terminates() {
        let (sender, _receiver) = broadcast::channel::<ConnMessage>(4);
        let receiver = sender.subscribe();
        for index in 0..16 {
            let _ = sender.send(ConnMessage::Event(Arc::new(sub_event(&format!(
                "vs.t.acme.verifyEvt.x{index}"
            )))));
        }
        let pattern = SubjectPattern::anchored("acme", ">").expect("pattern");
        let items: Vec<_> = filtered_stream(receiver, pattern, "nats_jetstream")
            .collect()
            .await;
        assert!(matches!(items.last(), Some(Err(_))));
    }

    #[tokio::test]
    async fn matching_events_pass_and_other_events_are_filtered() {
        let (sender, _receiver) = broadcast::channel::<ConnMessage>(16);
        let receiver = sender.subscribe();
        let _ = sender.send(ConnMessage::Event(Arc::new(sub_event(
            "vs.t.acme.verifyEvt.keep",
        ))));
        let _ = sender.send(ConnMessage::Event(Arc::new(sub_event(
            "vs.t.acme.other.drop",
        ))));
        drop(sender);
        let pattern = SubjectPattern::anchored("acme", "verifyEvt.>").expect("pattern");
        let events: Vec<_> = filtered_stream(receiver, pattern, "nats_jetstream")
            .filter_map(|result| async move { result.ok() })
            .collect()
            .await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].subject, "vs.t.acme.verifyEvt.keep");
    }

    #[tokio::test]
    async fn terminal_message_errors_and_ends_operation() {
        let (sender, _receiver) = broadcast::channel::<ConnMessage>(4);
        let receiver = sender.subscribe();
        let _ = sender.send(ConnMessage::Terminal(TerminalFailure {
            reason: Arc::from("broker unavailable"),
            code: "EVENT_STREAM_UNAVAILABLE",
        }));
        let pattern = SubjectPattern::anchored("acme", ">").expect("pattern");
        let items: Vec<_> = filtered_stream(receiver, pattern, "nats_jetstream")
            .collect()
            .await;
        assert_eq!(items.len(), 1);
        assert!(items[0].is_err());
    }

    #[test]
    fn terminal_guard_records_failure_for_late_subscribers() {
        let (sender, _receiver) = broadcast::channel::<ConnMessage>(4);
        let terminal = Arc::new(Mutex::new(None));
        {
            let _guard = PumpTerminalGuard::new(sender, Arc::clone(&terminal), "nats_jetstream");
        }
        let failure = terminal.lock().clone().expect("terminal failure");
        assert_eq!(failure.code, "EVENT_PUMP_FAILED");
    }

    #[test]
    fn operation_cursor_overrides_connection_cursor() {
        assert_eq!(
            resolve_resume_cursor(None, Some(Cursor::jetstream(10))).expect("fallback cursor"),
            Some(Cursor::jetstream(10))
        );
        assert_eq!(
            resolve_resume_cursor(
                Some("25"),
                Some(Cursor::redis_streams(1_700_000_000_000, 1))
            )
            .expect("operation cursor"),
            Some(Cursor::jetstream(25))
        );
        let error = resolve_resume_cursor(Some("invalid"), None).expect_err("invalid cursor");
        assert_eq!(
            error
                .extensions
                .as_ref()
                .and_then(|extensions| extensions.get("code")),
            Some(&async_graphql::Value::String("INVALID_CURSOR".into()))
        );
    }

    #[tokio::test]
    async fn pump_does_not_accept_delivery_during_operation_handoff() {
        let protocol_event = ventstream_protocol::Event::publish(
            "acme",
            "verifyEvt",
            "handoff",
            None,
            chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("epoch"),
            serde_json::json!({}),
            ventstream_protocol::Metadata::default(),
        )
        .expect("valid event");
        let accepted = Arc::new(AtomicBool::new(false));
        let session = SingleEventSession {
            event: Some(BrokerEvent {
                subject: Arc::from(protocol_event.subject().expect("event subject").to_string()),
                payload: serde_json::to_vec(&protocol_event)
                    .expect("serialize event")
                    .into(),
                cursor: Some(Cursor::jetstream(1)),
            }),
            accepted: Arc::clone(&accepted),
        };
        let (sender, receiver) = broadcast::channel(4);
        drop(receiver);
        let terminal = Arc::new(Mutex::new(None));
        let shutdown = ShutdownToken::new();
        let operation_notify = Arc::new(Notify::new());
        let task = tokio::spawn(run_session_pump(
            Box::new(session),
            sender.clone(),
            4,
            PumpControl {
                terminal,
                shutdown: shutdown.clone(),
                operation_notify: Arc::clone(&operation_notify),
            },
            "nats_jetstream",
        ));

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !accepted.load(Ordering::Acquire),
            "delivery was accepted with no GraphQL operation attached"
        );

        let mut replacement = sender.subscribe();
        operation_notify.notify_waiters();
        let delivered = tokio::time::timeout(Duration::from_secs(1), replacement.recv())
            .await
            .expect("replacement operation timed out")
            .expect("replacement operation closed");
        assert!(matches!(delivered, ConnMessage::Event(_)));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !accepted.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker delivery was not accepted after operation attached");

        shutdown.cancel();
        task.await.expect("pump task");
    }
}
