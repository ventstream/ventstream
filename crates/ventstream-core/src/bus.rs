//! Bounded MPSC channel that decouples source ingest rate from sink drain rate.
//!
//! Phase 0 uses [`tokio::sync::mpsc`] as the underlying primitive. It
//! provides bounded capacity, async backpressure, and clean closure
//! semantics when all senders are dropped. A custom lock-free ring buffer
//! is on the roadmap for Phase 2+ if profiling shows mpsc overhead is
//! material — abstracting behind [`EventSender`] / [`EventReceiver`] now
//! lets us swap the implementation without churning call sites.
//!
//! Concurrency model:
//! - Multiple producers (sources, gRPC ingress, replay tasks).
//! - Single consumer (the dispatcher that fans events out to sinks and
//!   the WebSocket gateway). If we need multiple consumers later we will
//!   layer a broadcast primitive on top — not multiplex receivers, which
//!   complicates exactly-once semantics.
//! - Backpressure is the channel's `send().await` blocking when full;
//!   non-blocking publishers should use [`EventSender::try_send`] and
//!   surface [`BackpressureError::Full`] to the caller (typical for the
//!   gRPC ingress path so it can return `RESOURCE_EXHAUSTED`).

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::error::BackpressureError;
use crate::event::Event;
use crate::memory::{MemoryAdmission, MemoryBudget};
use crate::shutdown::ShutdownToken;

/// Construct a new bounded event bus with `capacity` slots.
///
/// Returns the sender (cheaply cloneable, distribute one per producer) and
/// the single consumer. Dropping all senders closes the channel and the
/// consumer sees `None`.
pub fn channel(capacity: usize) -> (EventSender, EventReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    metrics::gauge!("vs_bus_capacity").set(capacity as f64);
    metrics::gauge!("vs_bus_depth").set(0.0);
    (
        EventSender {
            inner: tx,
            capacity,
            memory_budget: None,
            memory_admission: Arc::new(AtomicU8::new(MemoryAdmission::Direct as u8)),
        },
        EventReceiver {
            inner: rx,
            capacity,
            single_receives_since_observation: 0,
        },
    )
}

/// Convenience wrapper combining the sender and receiver halves.
///
/// Use [`EventBus::split`] to consume it into its halves before handing
/// the sender to multiple producers and the receiver to the dispatcher.
#[derive(Debug)]
pub struct EventBus {
    sender: EventSender,
    receiver: EventReceiver,
}

impl EventBus {
    /// Construct an [`EventBus`] with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = channel(capacity);
        Self { sender, receiver }
    }

    /// Construct a bus with byte-weighted memory admission.
    pub fn with_memory_budget(
        capacity: usize,
        memory_budget: Arc<MemoryBudget>,
        admission: MemoryAdmission,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        metrics::gauge!("vs_bus_capacity").set(capacity as f64);
        metrics::gauge!("vs_bus_depth").set(0.0);
        Self {
            sender: EventSender {
                inner: tx,
                capacity,
                memory_budget: Some(memory_budget),
                memory_admission: Arc::new(AtomicU8::new(admission as u8)),
            },
            receiver: EventReceiver {
                inner: rx,
                capacity,
                single_receives_since_observation: 0,
            },
        }
    }

    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        self.sender.capacity
    }

    /// Split into independent sender and receiver halves.
    pub fn split(self) -> (EventSender, EventReceiver) {
        (self.sender, self.receiver)
    }
}

/// Producer handle. Cheap to clone; pass one to each source.
#[derive(Debug, Clone)]
pub struct EventSender {
    inner: mpsc::Sender<Event>,
    capacity: usize,
    memory_budget: Option<Arc<MemoryBudget>>,
    memory_admission: Arc<AtomicU8>,
}

impl EventSender {
    /// Publish an event, awaiting available capacity.
    ///
    /// Honours cancellation via the supplied [`ShutdownToken`]; if the
    /// token fires while the publish is pending it returns
    /// [`BackpressureError::Cancelled`] rather than blocking indefinitely.
    ///
    /// Emits a `metric="bus.backpressure"` DEBUG log when the bus is
    /// full at send-time, with how long the send actually waited.
    /// Operators chasing "events look stalled" can grep this metric.
    pub async fn send(
        &self,
        mut event: Event,
        shutdown: &ShutdownToken,
    ) -> Result<(), BackpressureError> {
        if self.inner.is_closed() {
            return Err(BackpressureError::Closed);
        }
        self.reserve_memory(&mut event, shutdown).await?;
        // Fast path — try_send first. Most calls succeed immediately;
        // only when the bus is full do we pay for the await + log.
        match self.inner.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(BackpressureError::Closed),
            Err(mpsc::error::TrySendError::Full(event)) => {
                metrics::counter!("vs_backpressure_events_total").increment(1);
                self.record_depth();
                let blocked_at = std::time::Instant::now();
                tracing::debug!(
                    capacity = self.capacity,
                    metric = "bus.backpressure",
                    "bus full at send — awaiting capacity"
                );
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => Err(BackpressureError::Cancelled),
                    result = self.inner.send(event) => {
                        let waited_ms = blocked_at.elapsed().as_millis() as u64;
                        tracing::debug!(
                            capacity = self.capacity,
                            waited_ms,
                            metric = "bus.backpressure.cleared",
                            "bus capacity available after backpressure wait"
                        );
                        self.record_depth();
                        result.map_err(|_| BackpressureError::Closed)
                    }
                }
            }
        }
    }

    /// Non-blocking publish.
    ///
    /// Returns [`BackpressureError::Full`] immediately if the bus is at
    /// capacity, [`BackpressureError::Closed`] if all receivers have been
    /// dropped. Used by the gRPC ingress so the SDK gets a fast NACK
    /// instead of an unbounded wait.
    pub fn try_send(&self, mut event: Event) -> Result<(), BackpressureError> {
        if self.inner.is_closed() {
            return Err(BackpressureError::Closed);
        }
        self.try_reserve_memory(&mut event)?;
        match self.inner.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("vs_backpressure_events_total").increment(1);
                self.record_depth();
                Err(BackpressureError::Full {
                    capacity: self.capacity,
                })
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(BackpressureError::Closed),
        }
    }

    /// Approximate number of free slots in the bus.
    ///
    /// Useful for adaptive backpressure heuristics. Note the value is a
    /// snapshot — by the time the caller reads it, slots may have been
    /// consumed or freed. Treat as advisory.
    pub fn available_capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Configured capacity of the bus.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether the channel is closed (receiver dropped).
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Change the stage's admission class for this sender and all its clones.
    pub fn set_memory_admission(&self, admission: MemoryAdmission) {
        self.memory_admission
            .store(admission as u8, Ordering::Release);
    }

    fn admission(&self) -> MemoryAdmission {
        MemoryAdmission::from_u8(self.memory_admission.load(Ordering::Acquire))
    }

    async fn reserve_memory(
        &self,
        event: &mut Event,
        shutdown: &ShutdownToken,
    ) -> Result<(), BackpressureError> {
        let Some(budget) = &self.memory_budget else {
            return Ok(());
        };
        if event.payload.has_memory_reservation() {
            return Ok(());
        }
        let reservation = budget
            .reserve(event.estimated_memory_bytes(), self.admission(), shutdown)
            .await?;
        event.payload.set_memory_reservation(reservation);
        Ok(())
    }

    fn try_reserve_memory(&self, event: &mut Event) -> Result<(), BackpressureError> {
        let Some(budget) = &self.memory_budget else {
            return Ok(());
        };
        if event.payload.has_memory_reservation() {
            return Ok(());
        }
        let reservation = budget.try_reserve(event.estimated_memory_bytes(), self.admission())?;
        event.payload.set_memory_reservation(reservation);
        Ok(())
    }

    fn record_depth(&self) {
        metrics::gauge!("vs_bus_capacity").set(self.capacity as f64);
        metrics::gauge!("vs_bus_depth")
            .set(self.capacity.saturating_sub(self.inner.capacity()) as f64);
    }
}

/// Consumer handle. Single-owner: the dispatcher.
#[derive(Debug)]
pub struct EventReceiver {
    inner: mpsc::Receiver<Event>,
    capacity: usize,
    single_receives_since_observation: u8,
}

impl EventReceiver {
    /// Receive the next event, awaiting until one is available.
    ///
    /// Returns `None` when all senders have been dropped (clean shutdown).
    pub async fn recv(&mut self) -> Option<Event> {
        let event = self.inner.recv().await;
        self.single_receives_since_observation =
            self.single_receives_since_observation.wrapping_add(1);
        if self.single_receives_since_observation == 0 || event.is_none() {
            self.record_depth();
        }
        event
    }

    /// Receive up to `limit` events without blocking past the first.
    ///
    /// This is the primary path the dispatcher uses to assemble batches
    /// for sinks: await one event, then opportunistically drain whatever
    /// else has accumulated up to the batch ceiling.
    pub async fn recv_batch(&mut self, limit: usize) -> Vec<Event> {
        let mut batch = Vec::with_capacity(limit);
        self.recv_many(&mut batch, limit).await;
        batch
    }

    /// Append up to `limit` events to a reusable buffer.
    ///
    /// Unlike a `recv` + `try_recv` loop, Tokio's batched receive amortizes
    /// channel wakeups and permits and lets hot-path consumers retain their
    /// allocation between calls. Returns zero only when the channel is closed
    /// and drained, or when `limit` is zero.
    pub async fn recv_many(&mut self, buffer: &mut Vec<Event>, limit: usize) -> usize {
        let received = self.inner.recv_many(buffer, limit).await;
        self.record_depth();
        received
    }

    /// Close the receiver without consuming the remaining buffered events.
    /// Useful in shutdown paths to stop accepting new events while still
    /// allowing the drain loop to flush what is already buffered.
    pub fn close(&mut self) {
        self.inner.close();
    }

    fn record_depth(&self) {
        metrics::gauge!("vs_bus_capacity").set(self.capacity as f64);
        metrics::gauge!("vs_bus_depth")
            .set(self.capacity.saturating_sub(self.inner.capacity()) as f64);
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
    use crate::event::{Event, Payload, SourceUri, Subject};
    use crate::memory::MemoryBudget;

    fn test_event(subject_segment: &str) -> Event {
        let source = SourceUri::new("sdk://test").expect("uri");
        let subject = Subject::new(format!("test.{subject_segment}")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"payload".to_vec()))
            .build()
    }

    #[tokio::test]
    async fn send_and_receive_one_event() {
        let (sender, mut receiver) = channel(4);
        let shutdown = ShutdownToken::new();

        sender
            .send(test_event("one"), &shutdown)
            .await
            .expect("send");
        let received = receiver.recv().await.expect("recv");
        assert_eq!(received.subject.as_str(), "test.one");
    }

    #[tokio::test]
    async fn try_send_returns_full_at_capacity() {
        let (sender, _receiver) = channel(1);
        sender.try_send(test_event("a")).expect("first send");
        let err = sender.try_send(test_event("b")).expect_err("second send");
        match err {
            BackpressureError::Full { capacity } => assert_eq!(capacity, 1),
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_send_returns_closed_when_receiver_dropped() {
        let (sender, receiver) = channel(4);
        drop(receiver);
        let err = sender.try_send(test_event("x")).expect_err("send");
        assert!(matches!(err, BackpressureError::Closed));
    }

    #[tokio::test]
    async fn send_respects_shutdown_cancellation() {
        // Fill the channel so the next send must wait.
        let (sender, _receiver) = channel(1);
        sender.try_send(test_event("a")).expect("prime");

        let shutdown = ShutdownToken::new();
        let waiter_shutdown = shutdown.clone();

        let handle =
            tokio::spawn(async move { sender.send(test_event("b"), &waiter_shutdown).await });

        // Cancel before any receiver drains.
        shutdown.cancel();
        let result = handle.await.expect("join");
        assert!(matches!(result, Err(BackpressureError::Cancelled)));
    }

    #[tokio::test]
    async fn recv_batch_drains_up_to_limit() {
        let (sender, mut receiver) = channel(8);
        for i in 0..5 {
            sender.try_send(test_event(&format!("e{i}"))).expect("send");
        }
        let batch = receiver.recv_batch(3).await;
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn recv_batch_returns_empty_when_closed_and_drained() {
        let (sender, mut receiver) = channel(4);
        drop(sender);
        let batch = receiver.recv_batch(4).await;
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn recv_many_reuses_the_callers_buffer() {
        let (sender, mut receiver) = channel(8);
        for i in 0..5 {
            sender.try_send(test_event(&format!("e{i}"))).expect("send");
        }
        let mut buffer = Vec::with_capacity(8);
        let initial_capacity = buffer.capacity();
        let received = receiver.recv_many(&mut buffer, 5).await;
        assert_eq!(received, 5);
        assert_eq!(buffer.len(), 5);
        buffer.clear();
        assert_eq!(buffer.capacity(), initial_capacity);
    }

    #[tokio::test]
    async fn memory_charge_follows_event_clones_until_the_last_drop() {
        let budget = MemoryBudget::new(4_096, 1_024);
        let bus = EventBus::with_memory_budget(4, Arc::clone(&budget), MemoryAdmission::Direct);
        let (sender, mut receiver) = bus.split();
        sender.try_send(test_event("reserved")).expect("send");
        let charged = budget.used_bytes();
        assert!(charged > 0);

        let received = receiver.recv().await.expect("receive");
        let clone = received.clone();
        drop(received);
        assert_eq!(budget.used_bytes(), charged);
        drop(clone);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn oversized_event_is_rejected_before_entering_the_bus() {
        let budget = MemoryBudget::new(4_096, 600);
        let bus = EventBus::with_memory_budget(4, budget, MemoryAdmission::Direct);
        let (sender, _receiver) = bus.split();
        let source = SourceUri::new("sdk://test").expect("uri");
        let subject = Subject::new("test.large").expect("subject");
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(vec![0; 1_024]))
            .build();
        assert!(matches!(
            sender.try_send(event),
            Err(BackpressureError::EventTooLarge { .. })
        ));
    }
}
