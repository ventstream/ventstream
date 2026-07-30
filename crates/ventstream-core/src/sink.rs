//! The [`Sink`] trait — the contract every event consumer implements.
//!
//! A sink receives batches of events from the dispatcher and writes them
//! to its target (OpenSearch, S3, an HTTP webhook, etc.). The engine
//! drives batching and ordered progress; the sink is responsible for the
//! protocol-specific call and its transient retry policy.

use async_trait::async_trait;

use crate::error::SinkError;
use crate::event::Event;

/// A batch of events handed to a [`Sink`].
///
/// Held by value so the sink owns the events for the duration of the
/// write. Cloning is cheap (events themselves are refcounted internally)
/// but the batch is single-use: a retry constructs a new batch from the
/// engine's pending queue.
#[derive(Debug, Clone)]
pub struct SinkBatch {
    events: Vec<Event>,
}

impl SinkBatch {
    /// Construct from an event vector.
    pub fn new(events: Vec<Event>) -> Self {
        Self { events }
    }

    /// Borrow the events in the batch.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Consume the batch and return the inner events.
    pub fn into_events(self) -> Vec<Event> {
        self.events
    }

    /// Number of events in the batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the batch is empty (engines should never hand an empty
    /// batch to a sink, but we accept it defensively).
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// An event consumer.
///
/// Implementations:
/// - Must be `Send + Sync + 'static`.
/// - Must be idempotent against retries — the engine WILL re-deliver
///   on failure. Sinks should use [`Event::id`](crate::Event::id) as the
///   idempotency key with the downstream (for example, OpenSearch
///   `_id` or an HTTP `Idempotency-Key` header).
/// - Should never `unwrap` or `panic`; return [`SinkError`] instead. The
///   engine fails closed for whole-request errors and only routes explicitly
///   identified permanent item failures to the dead-letter queue.
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// Stable identifier (matches the `id` field in `pipeline.yaml`).
    fn id(&self) -> &str;

    /// Sink kind for telemetry/logging — `"opensearch"`, `"s3"`, etc.
    fn kind(&self) -> &'static str;

    /// Conservative estimate of the encoded bytes this event contributes to
    /// one sink request. Dispatchers use this for early batch boundaries; the
    /// sink remains responsible for enforcing its exact protocol limit.
    fn estimate_event_bytes(&self, event: &Event) -> usize {
        event.payload.as_slice().len()
    }

    /// Exact maximum request-body size accepted by this sink, when bounded.
    ///
    /// The dispatcher combines this with its configured memory limit. A sink
    /// must still enforce the value because templates and protocol framing can
    /// make any estimate imperfect.
    fn max_request_bytes(&self) -> Option<usize> {
        None
    }

    /// Current sink-recommended write concurrency, bounded by the operator's
    /// configured ceiling. Most sinks return the ceiling unchanged; adaptive
    /// sinks can lower it under downstream pressure and recover gradually.
    fn recommended_concurrency(&self, configured_ceiling: usize) -> usize {
        configured_ceiling.max(1)
    }

    /// Write a batch to the downstream target.
    async fn write(&self, batch: SinkBatch) -> Result<(), SinkError>;

    /// Flush any internal buffers. Called on graceful shutdown.
    ///
    /// Default impl is a no-op for sinks that flush per write.
    async fn flush(&self) -> Result<(), SinkError> {
        Ok(())
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
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;
    use crate::event::{Event, Payload, SourceUri, Subject};

    struct CollectingSink {
        id: String,
        received: Arc<Mutex<Vec<Event>>>,
    }

    #[async_trait]
    impl Sink for CollectingSink {
        fn id(&self) -> &str {
            &self.id
        }
        fn kind(&self) -> &'static str {
            "collecting"
        }
        async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
            self.received.lock().extend(batch.into_events());
            Ok(())
        }
    }

    fn make_event(label: &str) -> Event {
        let source = SourceUri::new("sdk://test").expect("uri");
        let subject = Subject::new(format!("test.{label}")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(label.as_bytes().to_vec()))
            .build()
    }

    #[tokio::test]
    async fn sink_receives_full_batch() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectingSink {
            id: "test-sink".to_string(),
            received: Arc::clone(&received),
        };

        let batch = SinkBatch::new(vec![make_event("a"), make_event("b"), make_event("c")]);
        sink.write(batch).await.expect("write");

        assert_eq!(received.lock().len(), 3);
    }

    #[tokio::test]
    async fn sink_batch_round_trips_events() {
        let events = vec![make_event("a"), make_event("b")];
        let batch = SinkBatch::new(events.clone());
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert_eq!(batch.into_events(), events);
    }
}
