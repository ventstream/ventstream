//! The [`Source`] trait — the contract every event producer implements.
//!
//! A source is a long-lived task that reads from some upstream (Postgres
//! WAL, a broker topic, a gRPC ingress port, a file tail) and publishes
//! events into the engine's [`crate::EventBus`] until told to stop.
//!
//! ### Why `async_trait` instead of native `async fn` in traits
//!
//! Rust 1.75+ supports native `async fn` in traits, but the result is not
//! yet `dyn`-compatible for traits that need to be stored as
//! `Box<dyn Source>` (which the engine's source registry requires).
//! `async_trait` works today and the perf overhead is negligible compared
//! to the actual work performed (network I/O). We will revisit when
//! `dyn AsyncFn` lands.

use async_trait::async_trait;

use crate::bus::EventSender;
use crate::error::SourceError;
use crate::shutdown::ShutdownToken;

/// Execution environment passed to each source.
///
/// Holds the handles a source needs to publish events and observe
/// shutdown. Constructed by the engine, never by the source itself.
#[derive(Debug, Clone)]
pub struct SourceContext {
    /// Sender into the engine's event bus.
    pub sender: EventSender,
    /// Token signalling graceful shutdown.
    pub shutdown: ShutdownToken,
}

impl SourceContext {
    /// Construct a context (used by the engine; sources receive one).
    pub fn new(sender: EventSender, shutdown: ShutdownToken) -> Self {
        Self { sender, shutdown }
    }
}

/// A long-lived event producer.
///
/// Implementations:
/// - Must be `Send + Sync + 'static` so the engine can `tokio::spawn`
///   them and store them in a `Vec<Box<dyn Source>>`.
/// - Must terminate promptly when `ctx.shutdown` fires. Best practice is
///   to use `tokio::select!` against `ctx.shutdown.cancelled()` in the
///   main loop rather than polling `is_cancelled`.
/// - Should treat [`SourceError`] returns as fatal; recoverable errors
///   should be retried internally with backoff.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Stable identifier (matches the `id` field in `pipeline.yaml`).
    fn id(&self) -> &str;

    /// Source kind for telemetry/logging — `"postgres_cdc"`, `"kafka"`, etc.
    fn kind(&self) -> &'static str;

    /// Run the source loop until shutdown or fatal error.
    ///
    /// The default contract is at-least-once: a source MUST NOT commit
    /// upstream progress (e.g. advance a Postgres replication slot or
    /// commit a Kafka offset) until events have been acknowledged by the
    /// downstream sinks. See the engine's dispatcher for the ACK signal
    /// path.
    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError>;
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::bus::channel;
    use crate::event::{Event, Payload, SourceUri, Subject};

    /// Minimal source used to exercise the trait contract in isolation.
    struct TestSource {
        id: String,
        emitted: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Source for TestSource {
        fn id(&self) -> &str {
            &self.id
        }

        fn kind(&self) -> &'static str {
            "test"
        }

        async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
            let source_uri = SourceUri::new("test://emitter")
                .map_err(|err| SourceError::Internal(err.to_string()))?;
            let subject =
                Subject::new("test.tick").map_err(|err| SourceError::Internal(err.to_string()))?;

            loop {
                tokio::select! {
                    biased;
                    () = ctx.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_millis(1)) => {
                        let event = Event::builder(source_uri.clone(), subject.clone())
                            .payload(Payload::from_vec(b"tick".to_vec()))
                            .build();
                        ctx.sender.send(event, &ctx.shutdown).await?;
                        self.emitted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn source_emits_until_shutdown() {
        let (sender, mut receiver) = channel(64);
        let shutdown = ShutdownToken::new();
        let ctx = SourceContext::new(sender, shutdown.clone());

        let emitted = Arc::new(AtomicUsize::new(0));
        let source = TestSource {
            id: "test-1".to_string(),
            emitted: Arc::clone(&emitted),
        };

        let handle = tokio::spawn(async move { source.run(ctx).await });

        // Block on the first event with a generous real-time timeout so the
        // test fails fast if the source never emits, instead of hanging.
        let first = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("source emitted within 1s")
            .expect("source produced an event before closing the channel");
        assert_eq!(first.subject.as_str(), "test.tick");

        shutdown.cancel();
        let result = handle.await.expect("join");
        assert!(result.is_ok());
        assert!(emitted.load(Ordering::Relaxed) >= 1);
    }
}
