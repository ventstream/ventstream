//! Per-pod connection registry.
//!
//! The registry is the in-memory ground truth for "which sockets are
//! connected to this pod and what they're subscribed to." It is the
//! only crate-internal coordination point — every connection task
//! registers itself here, the bus subscriber reads from here to find
//! delivery targets, and the connection's drop guard deregisters on
//! exit.
//!
//! All operations are O(1) in connection count for lookup/insert/remove
//! (HashMap). Subject matching across the registry is O(connections ×
//! subscriptions-per-connection). At ~1k connections with ~10 subs each
//! that's 10k linear comparisons per event — fine for v1. Trie-backed
//! matching is a follow-up optimization with the same external API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::mpsc;
use ulid::Ulid;
use ventstream_protocol::{Event, SubjectPattern};

use crate::protocol::ServerMessage;

/// Unique identifier for a connection on this pod. ULIDs are sortable,
/// 26-char base32, and unique per pod across restarts (independent of
/// PIDs).
pub(crate) type ConnectionId = Ulid;

/// A subscription installed by a connection. Holds the
/// client-chosen [`SubscriptionId`] and the anchored pattern.
#[derive(Debug, Clone)]
pub(crate) struct Subscription {
    pub(crate) id: String,
    pub(crate) pattern: SubjectPattern,
}

/// What we track per live connection.
pub(crate) struct ConnectionEntry {
    /// Tenant established at `Hello` time. Cannot change for the
    /// lifetime of the connection.
    pub(crate) tenant: String,
    /// Subscriptions currently installed by the client.
    ///
    /// Wrapped in `Arc<Mutex<...>>` so the per-connection JetStream
    /// consumer pump can share it with the registry. In Core mode
    /// the pump doesn't run, and `dispatch` reads via the same lock;
    /// in JetStream mode the pump and `add/remove_subscription` use
    /// the same handle.
    pub(crate) subscriptions: Arc<Mutex<Vec<Subscription>>>,
    /// Outbound mailbox to the connection's writer task. Bounded —
    /// when full, the registry drops the entry and signals a slow
    /// consumer.
    pub(crate) outbound: mpsc::Sender<ServerMessage>,
}

/// Concurrent registry. Cheap to clone (`Arc`).
///
/// `active` is a lock-free count of established connections, kept in
/// lockstep with `inner`'s length by `insert`/`remove`. It exists as a
/// separate atomic (rather than reading `inner.lock().len()`) so two
/// hot, out-of-crate readers never have to take the registry mutex:
/// the accept-path connection cap (every WS upgrade) and the binary's
/// `/readyz` + `/metrics` handlers. The binary injects this `Arc` via
/// [`with_gauge`](Self::with_gauge) so it can read the same counter the
/// gateway mutates.
#[derive(Clone, Default)]
pub(crate) struct ConnectionRegistry {
    inner: Arc<Mutex<HashMap<ConnectionId, ConnectionEntry>>>,
    active: Arc<AtomicUsize>,
}

impl ConnectionRegistry {
    /// A registry with its own private gauge. Used by tests; production
    /// goes through [`with_gauge`](Self::with_gauge) so the binary shares
    /// the counter.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Build a registry whose active-connection count is published into
    /// the supplied shared atomic. The binary holds another clone of
    /// this `Arc` to drive readiness gating and the connection metric.
    pub(crate) fn with_gauge(active: Arc<AtomicUsize>) -> Self {
        Self {
            inner: Arc::default(),
            active,
        }
    }

    /// Count of *admitted* connections — lock-free. A slot is reserved
    /// at the WS upgrade (see [`try_admit`](Self::try_admit)) and held
    /// until the connection task ends, so this counts every connection
    /// occupying memory, including ones still in the `Hello` handshake.
    /// It's the value the cap enforces, the readiness gate reads, and
    /// `vs_ws_active_connections` exports.
    pub(crate) fn active_connections(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// Atomically reserve a connection slot at admission time. Returns an
    /// [`AdmissionGuard`] that releases the slot on drop, or `None` if
    /// the pod is already at `max` (the caller rejects the upgrade).
    ///
    /// Reserving here — *before* `on_upgrade` allocates the socket task,
    /// JetStream consumer, and mailbox — is what makes the cap a HARD
    /// ceiling under concurrency. Counting at `Hello`/insert instead
    /// would let a burst of simultaneous upgrades all pass a `< max`
    /// check and then overshoot the cap once they registered. The
    /// optimistic `fetch_add` + rollback admits exactly `max`: if two
    /// upgrades race from `max-1`, only the one that observes `<= max`
    /// keeps its slot; the other rolls back.
    pub(crate) fn try_admit(&self, max: Option<usize>) -> Option<AdmissionGuard> {
        let n = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(max) = max {
            if n > max {
                self.active.fetch_sub(1, Ordering::AcqRel);
                return None;
            }
        }
        report_active(n);
        Some(AdmissionGuard {
            active: Arc::clone(&self.active),
        })
    }

    /// Register a new connection. Returns the freshly-minted
    /// [`ConnectionId`] and the shared subscriptions handle so the
    /// per-connection consumer pump (JetStream mode) can read the
    /// same Vec the registry mutates.
    pub(crate) fn insert(
        &self,
        tenant: String,
        outbound: mpsc::Sender<ServerMessage>,
    ) -> (ConnectionId, Arc<Mutex<Vec<Subscription>>>) {
        let id = Ulid::new();
        let subs = Arc::new(Mutex::new(Vec::new()));
        let entry = ConnectionEntry {
            tenant,
            subscriptions: Arc::clone(&subs),
            outbound,
        };
        self.inner.lock().insert(id, entry);
        (id, subs)
    }

    /// Remove a connection from the dispatch map. The admission slot is
    /// released separately by dropping the [`AdmissionGuard`] when the
    /// connection task ends, so the count is not touched here.
    pub(crate) fn remove(&self, id: &ConnectionId) -> Option<ConnectionEntry> {
        self.inner.lock().remove(id)
    }

    /// Add a subscription to an existing connection. Returns `false`
    /// if the connection is no longer registered (probably racing a
    /// removal).
    pub(crate) fn add_subscription(&self, id: &ConnectionId, sub: Subscription) -> bool {
        let subs = {
            let guard = self.inner.lock();
            guard.get(id).map(|e| Arc::clone(&e.subscriptions))
        };
        match subs {
            Some(subs) => {
                subs.lock().push(sub);
                true
            }
            None => false,
        }
    }

    /// Remove a subscription by its client-supplied id. Returns
    /// `true` if a matching subscription was found and removed.
    pub(crate) fn remove_subscription(&self, id: &ConnectionId, sub_id: &str) -> bool {
        let subs = {
            let guard = self.inner.lock();
            guard.get(id).map(|e| Arc::clone(&e.subscriptions))
        };
        let Some(subs) = subs else { return false };
        let mut g = subs.lock();
        let before = g.len();
        g.retain(|s| s.id != sub_id);
        before != g.len()
    }

    /// Fan out a bus event to every connection whose subscriptions
    /// match. Returns the list of connections we identified as slow
    /// (mailbox full) — the caller is expected to disconnect them.
    ///
    /// Holds the registry lock briefly to collect targets, then
    /// releases it before sending so a slow `try_send` cannot block
    /// other registry operations.
    pub(crate) fn dispatch(
        &self,
        subject: &str,
        event: &Event,
        event_json: &str,
    ) -> Vec<ConnectionId> {
        // Collect (connection_id, outbound, matched_sub_ids) under
        // the lock; do the send outside the lock.
        struct Target {
            id: ConnectionId,
            outbound: mpsc::Sender<ServerMessage>,
            matched: Vec<String>,
        }
        let targets: Vec<Target> = {
            let guard = self.inner.lock();
            guard
                .iter()
                .filter_map(|(id, entry)| {
                    // Tenant gate: skip outright when the event's
                    // tenant doesn't match the connection's. Defense
                    // in depth — the bus subscription should already
                    // be scoped, but a misconfigured subscription
                    // would otherwise leak across tenants.
                    if entry.tenant != event.tenant {
                        return None;
                    }
                    let subs = entry.subscriptions.lock();
                    let matched: Vec<String> = subs
                        .iter()
                        .filter(|s| s.pattern.matches_str(subject))
                        .map(|s| s.id.clone())
                        .collect();
                    if matched.is_empty() {
                        None
                    } else {
                        Some(Target {
                            id: *id,
                            outbound: entry.outbound.clone(),
                            matched,
                        })
                    }
                })
                .collect()
        };

        // Serialize the event JSON once (it's identical for every
        // target) and share it via `Arc<str>`. A hot subject with N
        // subscribers used to (a) deep-clone the whole envelope and
        // (b) re-serialize it once per connection — N times each, all
        // in this single (serial) bus task / N connection tasks. Now
        // it's one shared allocation and N refcount bumps.
        let shared_json: Arc<str> = Arc::from(event_json);

        let mut slow: Vec<ConnectionId> = Vec::new();
        for target in targets {
            let msg = ServerMessage::Event {
                subject: subject.to_owned(),
                matched: target.matched,
                event_json: Arc::clone(&shared_json),
                // Core mode is unsequenced — no resume cursor.
                cursor: None,
                seq: Some(0),
            };
            if target.outbound.try_send(msg).is_err() {
                slow.push(target.id);
            }
        }
        slow
    }

    /// Current connection count read straight from the map. Kept for
    /// tests as an independent cross-check against [`active_connections`]
    /// (the two must always agree).
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

/// Publish the live connection count to the `vs_ws_active_connections`
/// gauge. A no-op until the binary installs the global Prometheus
/// recorder (e.g. in unit tests), so it's safe to call unconditionally.
fn report_active(n: usize) {
    metrics::gauge!("vs_ws_active_connections").set(n as f64);
    metrics::gauge!(
        "vs_realtime_connections_active",
        "transport" => "ws"
    )
    .set(n as f64);
}

/// RAII handle for one admitted connection slot. Reserved by
/// [`ConnectionRegistry::try_admit`] at the WS upgrade and held for the
/// connection's whole lifetime; `Drop` releases the slot so the count
/// falls whether the connection closed gracefully, failed the `Hello`
/// handshake, or errored out — the same all-paths guarantee the
/// dispatch-map drop guard gives.
pub(crate) struct AdmissionGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let prev = self.active.fetch_sub(1, Ordering::AcqRel);
        report_active(prev.saturating_sub(1));
    }
}

/// The registry *is* the WS gateway's live-consumer set: every live
/// connection owns exactly one consumer keyed by its connection id, so
/// the reaper treats a consumer whose id is absent here as an orphan.
impl ventstream_jetstream::ActiveConsumers for ConnectionRegistry {
    fn snapshot(&self) -> std::collections::HashSet<String> {
        self.inner.lock().keys().map(|id| id.to_string()).collect()
    }
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
    use chrono::Utc;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use ventstream_protocol::Metadata;

    fn sample_event(tenant: &str) -> Event {
        Event::publish(
            tenant,
            "orders.order.statusChanged",
            "order_123",
            None,
            Utc::now(),
            Value::Null,
            Metadata::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn dispatch_matches_pattern_and_sends() {
        let reg = ConnectionRegistry::new();
        let (tx, mut rx) = mpsc::channel(16);
        let (cid, _subs) = reg.insert("acme".into(), tx);
        reg.add_subscription(
            &cid,
            Subscription {
                id: "s1".into(),
                pattern: SubjectPattern::anchored("acme", "orders.order.statusChanged.*").unwrap(),
            },
        );

        let ev = sample_event("acme");
        let raw = serde_json::to_string(&ev).unwrap();
        let slow = reg.dispatch("vs.t.acme.orders.order.statusChanged.order_123", &ev, &raw);
        assert!(slow.is_empty());
        let got = rx.recv().await.unwrap();
        match got {
            ServerMessage::Event { matched, .. } => {
                assert_eq!(matched, vec!["s1".to_string()]);
            }
            _ => panic!("expected Event"),
        }
    }

    #[tokio::test]
    async fn dispatch_skips_other_tenants() {
        let reg = ConnectionRegistry::new();
        let (tx, mut rx) = mpsc::channel(16);
        let (cid, _subs) = reg.insert("acme".into(), tx);
        reg.add_subscription(
            &cid,
            Subscription {
                id: "s1".into(),
                pattern: SubjectPattern::parse("vs.t.>").unwrap(),
            },
        );

        // Event with tenant = "other" — should be skipped even though
        // the pattern would match the subject.
        let ev = sample_event("other");
        let raw = serde_json::to_string(&ev).unwrap();
        reg.dispatch(&ev.subject().unwrap().to_string(), &ev, &raw);
        // Channel should be empty after a brief yield.
        let r = tokio::time::timeout(std::time::Duration::from_millis(20), rx.recv()).await;
        assert!(r.is_err(), "expected timeout — no message should arrive");
    }

    #[tokio::test]
    async fn slow_consumer_reported() {
        let reg = ConnectionRegistry::new();
        let (tx, _rx) = mpsc::channel(1);
        let (cid, _subs) = reg.insert("acme".into(), tx);
        reg.add_subscription(
            &cid,
            Subscription {
                id: "s1".into(),
                pattern: SubjectPattern::parse("vs.t.acme.>").unwrap(),
            },
        );

        let ev = sample_event("acme");
        let raw = serde_json::to_string(&ev).unwrap();
        let subject = ev.subject().unwrap().to_string();
        // First send fills the mailbox.
        let _ = reg.dispatch(&subject, &ev, &raw);
        // Second send overflows -> connection marked slow.
        let slow = reg.dispatch(&subject, &ev, &raw);
        assert_eq!(slow, vec![cid]);
    }

    #[test]
    fn admission_count_tracks_reserve_and_release() {
        let reg = ConnectionRegistry::new();
        assert_eq!(reg.active_connections(), 0);

        let g1 = reg.try_admit(Some(2)).expect("slot 1");
        let g2 = reg.try_admit(Some(2)).expect("slot 2");
        assert_eq!(reg.active_connections(), 2);

        // Releasing one slot frees capacity for a new admission.
        drop(g1);
        assert_eq!(reg.active_connections(), 1);
        let _g3 = reg.try_admit(Some(2)).expect("slot freed");
        assert_eq!(reg.active_connections(), 2);
        drop(g2);
        assert_eq!(reg.active_connections(), 1);
    }

    #[test]
    fn try_admit_enforces_hard_cap_and_rolls_back() {
        let reg = ConnectionRegistry::new();
        let _g1 = reg.try_admit(Some(1)).expect("first admitted");
        // At cap: rejected, and the optimistic fetch_add is rolled back
        // so the count stays exactly at the cap (no leak/overshoot).
        assert!(reg.try_admit(Some(1)).is_none());
        assert_eq!(reg.active_connections(), 1);
    }

    #[test]
    fn try_admit_unlimited_when_no_cap() {
        let reg = ConnectionRegistry::new();
        let _g: Vec<_> = (0..5)
            .map(|_| reg.try_admit(None).expect("no cap"))
            .collect();
        assert_eq!(reg.active_connections(), 5);
    }

    #[test]
    fn with_gauge_publishes_into_shared_atomic() {
        // The binary holds this Arc to read for readiness/metrics; the
        // gateway mutates it via admission. Both views must agree.
        let shared = Arc::new(AtomicUsize::new(0));
        let reg = ConnectionRegistry::with_gauge(Arc::clone(&shared));
        let g = reg.try_admit(Some(10)).expect("admitted");
        assert_eq!(shared.load(Ordering::Relaxed), 1);
        drop(g);
        assert_eq!(shared.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remove_subscription_returns_whether_removed() {
        let reg = ConnectionRegistry::new();
        let (tx, _rx) = mpsc::channel(16);
        let (cid, _subs) = reg.insert("acme".into(), tx);
        let pat = SubjectPattern::parse("vs.t.acme.>").unwrap();
        reg.add_subscription(
            &cid,
            Subscription {
                id: "s1".into(),
                pattern: pat,
            },
        );
        assert!(reg.remove_subscription(&cid, "s1"));
        assert!(!reg.remove_subscription(&cid, "s1"));
    }
}
