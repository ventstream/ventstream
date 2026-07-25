//! NATS subscriber that fans bus events into the connection registry.
//!
//! v1 uses NATS Core (ephemeral, at-most-once). JetStream durable
//! consumers — one per WS connection, with the orphan-cleanup story —
//! land alongside task #7.

use futures_util::StreamExt;
use tracing::{debug, info, warn};
use ventstream_core::ShutdownToken;
use ventstream_protocol::Event;

use crate::error::WsError;
use crate::registry::ConnectionRegistry;

pub(crate) type BusSubscriptions = futures_util::stream::SelectAll<async_nats::Subscriber>;

/// Connect to NATS and establish every configured Core subscription.
///
/// Kept separate from [`run_bus`] so the gateway does not mark its traffic
/// listener ready before the underlying event bus is usable.
pub(crate) async fn connect_bus(
    nats_url: &str,
    subjects: &[String],
) -> Result<BusSubscriptions, WsError> {
    info!(nats_url = %nats_url, subjects = ?subjects, "connecting bus");

    let client = async_nats::connect(nats_url)
        .await
        .map_err(|err| WsError::Nats {
            url: nats_url.to_owned(),
            detail: err.to_string(),
        })?;

    let mut merged = futures_util::stream::SelectAll::new();
    for subject in subjects {
        let sub =
            client
                .subscribe(subject.clone())
                .await
                .map_err(|err| WsError::NatsSubscribe {
                    subject: subject.clone(),
                    detail: err.to_string(),
                })?;
        merged.push(sub);
        info!(subject = %subject, "bus subscribed");
    }
    Ok(merged)
}

/// Forward established NATS subscriptions into the connection registry.
/// Returns once shutdown fires or NATS disconnects fatally.
pub(crate) async fn run_bus(
    mut merged: BusSubscriptions,
    nats_url: String,
    registry: ConnectionRegistry,
    shutdown: ShutdownToken,
) -> Result<(), WsError> {
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("bus subscriber shutting down");
                return Ok(());
            }
            maybe_msg = merged.next() => {
                let Some(msg) = maybe_msg else {
                    warn!("bus subscriptions all closed; exiting");
                    return Err(WsError::Nats {
                        url: nats_url,
                        detail: "all Core subscriptions closed".to_owned(),
                    });
                };
                let subject = msg.subject.to_string();
                let event = match serde_json::from_slice::<Event>(&msg.payload) {
                    Ok(ev) => ev,
                    Err(err) => {
                        warn!(
                            subject = %subject,
                            error = %err,
                            "dropping malformed envelope from bus"
                        );
                        continue;
                    }
                };
                if let Err(err) = event.validate() {
                    warn!(
                        subject = %subject,
                        error = %err,
                        "dropping envelope that failed validation"
                    );
                    continue;
                }
                // Reuse the raw bus payload verbatim as the event JSON —
                // it deserialized cleanly above, so it's valid UTF-8 JSON.
                // This skips re-serializing the envelope on the fan-out.
                let event_json = match std::str::from_utf8(&msg.payload) {
                    Ok(s) => s,
                    Err(err) => {
                        warn!(subject = %subject, error = %err, "bus payload not utf-8; dropping");
                        continue;
                    }
                };
                let slow = registry.dispatch(&subject, &event, event_json);
                for id in slow {
                    // Mailbox overflow → disconnect. Removing the
                    // registry entry drops its outbound Sender, which
                    // causes the connection task's receiver to return
                    // None on next recv; that task then closes the
                    // WebSocket and the RAII drop guard does cleanup.
                    // This is the contract advertised in WsConfig:
                    // a slow client cannot stall a fast publisher.
                    if registry.remove(&id).is_some() {
                        debug!(
                            connection_id = %id,
                            "slow consumer disconnected (mailbox full)"
                        );
                    }
                }
            }
        }
    }
}
