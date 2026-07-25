//! JetStream stream bootstrap and the broker-neutral connection pump.
//!
//! Stream bootstrap remains owned by the native WebSocket role. Delivery,
//! replay, acknowledgement, and consumer cleanup are supplied through the
//! [`ventstream_realtime::EventSession`] contract, allowing the same gateway
//! pump to consume JetStream now and Redis Streams later.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};
use async_nats::jetstream::Context as JsContext;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing::{info, trace, warn};
use ventstream_core::ShutdownToken;
use ventstream_realtime::{BrokerEvent, BrokerKind, CursorProvider, EventSession};

use crate::config::JetStreamConfig;
use crate::error::WsError;

const OUTBOUND_BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(1);

/// Why a connection-scoped broker pump stopped.
#[derive(Debug)]
pub(crate) enum ConsumerPumpExit {
    /// The owning connection or process asked the pump to stop.
    Shutdown,
    /// The connection writer disappeared before the pump.
    OutboundClosed,
    /// The connection could not drain its bounded outbound mailbox.
    SlowConsumer,
    /// Repeated broker failures made forward progress impossible.
    Unrecoverable,
}

/// Ensure the configured stream exists with the right shape.
///
/// Idempotent: re-running with the same config is a no-op. If a stream with
/// the same name exists with drifted subjects, the gateway reports the drift
/// and operates against the existing stream rather than mutating production
/// retention implicitly.
pub(crate) async fn ensure_stream(js: &JsContext, cfg: &JetStreamConfig) -> Result<(), WsError> {
    let storage = match cfg.storage {
        crate::config::StreamStorage::File => StorageType::File,
        crate::config::StreamStorage::Memory => StorageType::Memory,
    };
    let wanted = StreamConfig {
        name: cfg.stream_name.clone(),
        subjects: cfg.stream_subjects.clone(),
        retention: RetentionPolicy::Limits,
        discard: DiscardPolicy::Old,
        storage,
        max_age: cfg.max_age,
        max_bytes: cfg.max_bytes,
        max_messages: cfg.max_msgs,
        num_replicas: cfg.replicas,
        ..Default::default()
    };

    match js.get_stream(&cfg.stream_name).await {
        Ok(stream) => {
            let existing_subjects = &stream.cached_info().config.subjects;
            if existing_subjects != &cfg.stream_subjects {
                warn!(
                    stream = %cfg.stream_name,
                    existing_subjects = ?existing_subjects,
                    wanted_subjects = ?cfg.stream_subjects,
                    "JetStream stream exists with different subjects; operating against existing configuration"
                );
            } else {
                info!(stream = %cfg.stream_name, "JetStream stream already exists");
            }
        }
        Err(_) => {
            js.create_stream(wanted)
                .await
                .map_err(|err| WsError::Nats {
                    url: cfg.stream_name.clone(),
                    detail: format!("creating stream: {err}"),
                })?;
            info!(stream = %cfg.stream_name, "JetStream stream created");
        }
    }
    Ok(())
}

/// Drain one broker session into the native WebSocket connection mailbox.
///
/// The broker receives acceptance only after an event is intentionally
/// discarded or successfully enters the bounded mailbox. A mailbox-full event
/// remains unsettled and the client reconnects from its last processed cursor.
pub(crate) async fn run_consumer_pump(
    mut session: Box<dyn EventSession>,
    outbound: mpsc::Sender<crate::protocol::ServerMessage>,
    local_subs: Arc<Mutex<Vec<crate::registry::Subscription>>>,
    connection_id: crate::registry::ConnectionId,
    shutdown: ShutdownToken,
    broker_kind: BrokerKind,
) -> ConsumerPumpExit {
    let provider = broker_kind.as_str();
    // A resumable session must not drain replay before the first subscription
    // pattern exists. Otherwise those events would be acknowledged as
    // unmatched before the client can install its post-Ready subscription.
    while local_subs.lock().is_empty() {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return ConsumerPumpExit::Shutdown,
            () = outbound.closed() => return ConsumerPumpExit::OutboundClosed,
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
    }

    loop {
        let next = tokio::select! {
            biased;
            () = shutdown.cancelled() => return ConsumerPumpExit::Shutdown,
            () = outbound.closed() => return ConsumerPumpExit::OutboundClosed,
            result = session.next() => result,
        };
        let delivery = match next {
            Ok(Some(delivery)) => delivery,
            Ok(None) => {
                warn!(connection_id = %connection_id, "broker session ended unexpectedly");
                return ConsumerPumpExit::Unrecoverable;
            }
            Err(err) => {
                metrics::counter!(
                    "vs_realtime_terminal_failures_total",
                    "transport" => "ws",
                    "provider" => provider,
                    "reason" => "broker_unrecoverable"
                )
                .increment(1);
                warn!(connection_id = %connection_id, error = %err, "broker session failed");
                return ConsumerPumpExit::Unrecoverable;
            }
        };
        metrics::counter!(
            "vs_realtime_broker_messages_total",
            "transport" => "ws",
            "provider" => provider
        )
        .increment(1);

        let legacy_sequence = match legacy_jetstream_sequence(&delivery) {
            Ok(sequence) => sequence,
            Err(reason) => {
                warn!(connection_id = %connection_id, %reason, "broker event has no compatible cursor");
                return ConsumerPumpExit::Unrecoverable;
            }
        };
        let event = match serde_json::from_slice::<ventstream_protocol::Event>(&delivery.payload) {
            Ok(event) => event,
            Err(err) => {
                metrics::counter!(
                    "vs_realtime_events_dropped_total",
                    "transport" => "ws",
                    "provider" => provider,
                    "reason" => "malformed_json"
                )
                .increment(1);
                warn!(subject = %delivery.subject, error = %err, "dropping malformed broker envelope");
                if !accept_or_log(session.as_mut(), &delivery).await {
                    return ConsumerPumpExit::Unrecoverable;
                }
                continue;
            }
        };
        if let Err(err) = event.validate() {
            metrics::counter!(
                    "vs_realtime_events_dropped_total",
                    "transport" => "ws",
                    "provider" => provider,
                    "reason" => "invalid_envelope"
            )
            .increment(1);
            warn!(subject = %delivery.subject, error = %err, "dropping invalid broker envelope");
            if !accept_or_log(session.as_mut(), &delivery).await {
                return ConsumerPumpExit::Unrecoverable;
            }
            continue;
        }
        trace!(
            connection_id = %connection_id,
            event_id = %event.id,
            cursor = ?delivery.cursor,
            "broker event validated"
        );

        let matched: Vec<String> = {
            let subscriptions = local_subs.lock();
            subscriptions
                .iter()
                .filter(|subscription| subscription.pattern.matches_str(&delivery.subject))
                .map(|subscription| subscription.id.clone())
                .collect()
        };
        if matched.is_empty() {
            if !accept_or_log(session.as_mut(), &delivery).await {
                return ConsumerPumpExit::Unrecoverable;
            }
            continue;
        }

        let event_json: Arc<str> = match std::str::from_utf8(&delivery.payload) {
            Ok(value) => Arc::from(value),
            Err(err) => {
                metrics::counter!(
                    "vs_realtime_events_dropped_total",
                    "transport" => "ws",
                    "provider" => provider,
                    "reason" => "invalid_utf8"
                )
                .increment(1);
                warn!(subject = %delivery.subject, error = %err, "broker payload is not UTF-8");
                if !accept_or_log(session.as_mut(), &delivery).await {
                    return ConsumerPumpExit::Unrecoverable;
                }
                continue;
            }
        };
        let message = crate::protocol::ServerMessage::Event {
            subject: delivery.subject.to_string(),
            matched,
            event_json,
            cursor: delivery.cursor.clone(),
            seq: legacy_sequence,
        };
        match outbound
            .send_timeout(message, OUTBOUND_BACKPRESSURE_TIMEOUT)
            .await
        {
            Ok(()) => {
                metrics::counter!(
                    "vs_realtime_events_enqueued_total",
                    "transport" => "ws",
                    "provider" => provider
                )
                .increment(1);
                if !accept_or_log(session.as_mut(), &delivery).await {
                    return ConsumerPumpExit::Unrecoverable;
                }
            }
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                metrics::counter!(
                    "vs_realtime_slow_consumers_total",
                    "transport" => "ws",
                    "provider" => provider
                )
                .increment(1);
                warn!(connection_id = %connection_id, "slow consumer: outbound mailbox full");
                return ConsumerPumpExit::SlowConsumer;
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                return ConsumerPumpExit::OutboundClosed;
            }
        }
    }
}

fn legacy_jetstream_sequence(delivery: &BrokerEvent) -> Result<Option<u64>, &'static str> {
    let cursor = delivery.cursor.as_ref().ok_or("cursor missing")?;
    if cursor.provider() != CursorProvider::NatsJetStream {
        return Ok(None);
    }
    cursor
        .value()
        .parse::<u64>()
        .map(Some)
        .map_err(|_| "JetStream cursor is not an unsigned decimal sequence")
}

async fn accept_or_log(session: &mut dyn EventSession, delivery: &BrokerEvent) -> bool {
    match session.accepted(delivery).await {
        Ok(()) => true,
        Err(err) => {
            warn!(error = %err, "broker delivery acceptance failed");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use ventstream_realtime::{BrokerEvent, Cursor};

    use super::legacy_jetstream_sequence;

    #[test]
    fn extracts_legacy_sequence_from_generic_cursor() {
        let event = BrokerEvent {
            subject: Arc::from("vs.t.acme.orders.updated.1"),
            payload: Bytes::new(),
            cursor: Some(Cursor::jetstream(u64::MAX)),
        };
        assert_eq!(legacy_jetstream_sequence(&event), Ok(Some(u64::MAX)));
    }

    #[test]
    fn omits_legacy_sequence_for_non_jetstream_cursor() {
        let event = BrokerEvent {
            subject: Arc::from("vs.t.acme.orders.updated.1"),
            payload: Bytes::new(),
            cursor: Some(Cursor::redis_streams(1, 0)),
        };
        assert_eq!(legacy_jetstream_sequence(&event), Ok(None));
    }
}
