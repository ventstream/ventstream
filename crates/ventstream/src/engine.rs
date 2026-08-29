//! Top-level engine — owns the source, bus, dispatcher, sink, and DLQ.
//!
//! Phase 0 wires exactly one source, one sink, and one DLQ. Multi-pipe
//! configurations (multiple sources or sinks per engine) land alongside
//! the pipeline.yaml loader.

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use thiserror::Error;
use tokio::task::JoinError;
use tracing::{error, info};
use ventstream_core::{
    EventBus, EventReceiver, EventSender, MemoryAdmission, MemoryBudget, ShutdownToken, Sink,
    Source,
};
use ventstream_joins::{JoinDurability, JoinEngine, PoisonSink};

use crate::dispatcher::{Dispatcher, DispatcherConfig};
use crate::dlq::{DlqError, DlqPoisonSink, DlqWriter};
use crate::memory_controller::{MemoryControllerConfig, MemoryRuntime};

/// Engine-level configuration knobs.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Capacity of the bus channel between source and dispatcher.
    pub bus_capacity: usize,
    /// Dispatcher batching config.
    pub dispatcher: DispatcherConfig,
    /// Absolute or relative path where the dispatcher appends poison
    /// events. Created on startup if missing.
    pub dlq_path: PathBuf,
    /// Adaptive process/cgroup memory protection.
    pub memory: MemoryControllerConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            bus_capacity: 1024,
            dispatcher: DispatcherConfig::default(),
            dlq_path: PathBuf::from("./data/dlq.jsonl"),
            memory: MemoryControllerConfig::default(),
        }
    }
}

/// Errors that can stop the engine.
#[derive(Debug, Error)]
pub enum EngineError {
    /// DLQ could not be opened at startup.
    #[error("opening DLQ: {0}")]
    Dlq(#[from] DlqError),

    /// Source task returned an error.
    #[error("source '{id}' failed: {error}")]
    Source {
        /// Source identifier.
        id: String,
        /// Underlying error message.
        error: String,
    },

    /// Dispatcher task returned an error.
    #[error("dispatcher failed: {0}")]
    Dispatcher(String),

    /// Stateful join processing or persistence failed.
    #[error("join engine failed: {0}")]
    Join(String),

    /// One of the spawned tasks panicked.
    #[error("background task panicked: {0}")]
    TaskPanic(String),
}

impl From<JoinError> for EngineError {
    fn from(value: JoinError) -> Self {
        Self::TaskPanic(value.to_string())
    }
}

/// The engine. Composes one source + (optional) join layer + dispatcher
/// + sink + DLQ behind a single [`run`](Engine::run) call.
pub struct Engine {
    source: Box<dyn Source>,
    sink: Arc<dyn Sink>,
    config: EngineConfig,
    /// When `Some`, events flow `source → join_engine → dispatcher`.
    /// When `None`, events flow `source → dispatcher` directly.
    join_engine: Option<Arc<JoinEngine>>,
    /// Source bus, created at construction so external callers (admin
    /// resync, future side-injection paths) can grab a sender clone
    /// before `run()` consumes the receiver.
    source_sender: EventSender,
    source_receiver: Option<EventReceiver>,
    /// Sink-progress watermark wired between the source and dispatcher.
    /// The source reads this to gate WAL slot advancement so a crash
    /// can't lose events that the sink hadn't durably written. The
    /// dispatcher writes max-LSN per durably-handled batch into it.
    /// Both sides treat `None`-progress as the legacy unsafe behavior.
    sink_progress: Arc<AtomicU64>,
    memory_runtime: Option<MemoryRuntime>,
}

impl Engine {
    /// Construct an engine without any joins.
    pub fn new(source: Box<dyn Source>, sink: Arc<dyn Sink>, config: EngineConfig) -> Self {
        let memory_runtime = MemoryRuntime::detect(&config.memory);
        let source_bus = memory_runtime.as_ref().map_or_else(
            || EventBus::new(config.bus_capacity),
            |runtime| {
                EventBus::with_memory_budget(
                    config.bus_capacity,
                    runtime.budget(),
                    MemoryAdmission::Direct,
                )
            },
        );
        let (source_sender, source_receiver) = source_bus.split();
        Self {
            source,
            sink,
            config,
            join_engine: None,
            source_sender,
            source_receiver: Some(source_receiver),
            sink_progress: Arc::new(AtomicU64::new(0)),
            memory_runtime,
        }
    }

    /// Substitute an external sink-progress watermark for the one
    /// constructed by [`Engine::new`]. The expected usage is:
    /// `main.rs` allocates an Arc, hands a clone to the source via
    /// `Source::with_sink_progress`, and then calls
    /// `Engine::new(...).with_sink_progress(arc)` so both sides share
    /// the same atomic. Calling this without also wiring the source
    /// is harmless — the dispatcher just publishes to an Arc nobody
    /// reads.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = progress;
        self
    }

    /// Clone the source bus's sender. Used by the admin resync
    /// endpoint to inject synthetic INSERT events alongside the
    /// source's normal WAL stream. The clone keeps the channel
    /// alive across source-sender drops — admin callers should
    /// drop the clone when they're done injecting.
    pub fn source_sender_clone(&self) -> EventSender {
        self.source_sender.clone()
    }

    /// Plug in a join engine. Cleared if `join_engine.has_joins()` is
    /// false (no point spawning a no-op transform task).
    #[must_use]
    pub fn with_joins(mut self, join_engine: Arc<JoinEngine>) -> Self {
        if join_engine.has_joins() {
            self.source_sender
                .set_memory_admission(MemoryAdmission::TransformInput);
            self.join_engine = Some(join_engine);
        }
        self
    }

    /// Drive the pipeline until shutdown or a fatal error.
    pub async fn run(mut self, shutdown: ShutdownToken) -> Result<(), EngineError> {
        let dlq = DlqWriter::open(self.config.dlq_path.clone()).await?;
        // The join engine quarantines rows it can never process on the same
        // DLQ the dispatcher uses for sink rejections (#131).
        let poison: Arc<dyn PoisonSink> = Arc::new(DlqPoisonSink::new(dlq.clone()));
        info!(
            source = %self.source.id(),
            sink = %self.sink.id(),
            dlq = %dlq.path().display(),
            joins = self.join_engine.is_some(),
            "engine starting"
        );

        let source = self.source;
        let source_id = source.id().to_owned();
        let transform_progress = self
            .join_engine
            .as_ref()
            .map(|_| Arc::new(AtomicU64::new(0)));
        let mut dispatcher = Dispatcher::new(
            Arc::clone(&self.sink),
            dlq,
            self.config.dispatcher.clone(),
            shutdown.clone(),
        );
        if let Some(progress) = &transform_progress {
            dispatcher = dispatcher.with_transform_progress(Arc::clone(progress));
        } else {
            dispatcher = dispatcher.with_sink_progress(Arc::clone(&self.sink_progress));
        }
        if let Some(memory) = &self.memory_runtime {
            dispatcher = dispatcher.with_memory_budget(memory.budget());
        }

        let memory_shutdown = shutdown.child();
        let memory_monitor = self
            .memory_runtime
            .as_ref()
            .map(|runtime| runtime.spawn(memory_shutdown.clone()));
        let memory_budget = self.memory_runtime.as_ref().map(MemoryRuntime::budget);

        let source_sender = self.source_sender;
        let source_receiver = self.source_receiver.take().ok_or_else(|| {
            EngineError::TaskPanic("Engine::run called twice — source receiver gone".into())
        })?;

        let result = match self.join_engine {
            None => {
                run_without_joins(
                    source,
                    source_id,
                    source_sender,
                    source_receiver,
                    dispatcher,
                    shutdown,
                )
                .await
            }
            Some(join_engine) => {
                let durability = JoinDurability::new(
                    transform_progress.ok_or_else(|| {
                        EngineError::TaskPanic(
                            "join engine configured without transform progress".into(),
                        )
                    })?,
                    Arc::clone(&self.sink_progress),
                );
                run_with_joins(
                    source,
                    source_id,
                    source_sender,
                    source_receiver,
                    join_engine,
                    durability,
                    poison,
                    dispatcher,
                    self.config.bus_capacity,
                    memory_budget,
                    shutdown,
                )
                .await
            }
        };
        memory_shutdown.cancel();
        if let Some(handle) = memory_monitor {
            let _ = handle.await;
        }
        result
    }
}

async fn run_without_joins(
    source: Box<dyn Source>,
    source_id: String,
    sender: EventSender,
    receiver: EventReceiver,
    dispatcher: Dispatcher,
    shutdown: ShutdownToken,
) -> Result<(), EngineError> {
    let source_ctx = ventstream_core::SourceContext::new(sender, shutdown.clone());
    let source_handle = tokio::spawn(async move { source.run(source_ctx).await });
    let dispatcher_handle = tokio::spawn(dispatcher.run(receiver));

    let source_result = source_handle.await?;
    if let Err(err) = &source_result {
        error!(source = %source_id, error = %err, "source returned error");
        shutdown.cancel();
    }
    let dispatcher_result = dispatcher_handle.await?;
    finalize(source_id, source_result, dispatcher_result)
}

#[allow(clippy::too_many_arguments)]
async fn run_with_joins(
    source: Box<dyn Source>,
    source_id: String,
    source_sender: EventSender,
    source_receiver: EventReceiver,
    join_engine: Arc<JoinEngine>,
    durability: JoinDurability,
    poison: Arc<dyn PoisonSink>,
    dispatcher: Dispatcher,
    bus_capacity: usize,
    memory_budget: Option<Arc<MemoryBudget>>,
    shutdown: ShutdownToken,
) -> Result<(), EngineError> {
    // Source → bus1 → JoinEngine → bus2 → Dispatcher.
    let join_bus = memory_budget.map_or_else(
        || EventBus::new(bus_capacity),
        |budget| {
            EventBus::with_memory_budget(bus_capacity, budget, MemoryAdmission::TransformOutput)
        },
    );
    let (join_sender, join_receiver) = join_bus.split();

    let source_ctx = ventstream_core::SourceContext::new(source_sender, shutdown.clone());
    let source_handle = tokio::spawn(async move { source.run(source_ctx).await });

    let join_shutdown = shutdown.clone();
    let join_handle = tokio::spawn(async move {
        join_engine
            .run_with_durability(
                source_receiver,
                join_sender,
                join_shutdown,
                Some(durability),
                Some(poison),
            )
            .await
    });

    let dispatcher_handle = tokio::spawn(dispatcher.run(join_receiver));

    let source_result = source_handle.await?;
    if let Err(err) = &source_result {
        error!(source = %source_id, error = %err, "source returned error");
        shutdown.cancel();
    }
    // Wait for the join engine to drain before the dispatcher closes —
    // its sender dropping is what tells the dispatcher to flush.
    let join_result = join_handle.await?;
    let dispatcher_result = dispatcher_handle.await?;
    if let Err(err) = join_result {
        return Err(EngineError::Join(err.to_string()));
    }
    finalize(source_id, source_result, dispatcher_result)
}

fn finalize(
    source_id: String,
    source_result: Result<(), ventstream_core::SourceError>,
    dispatcher_result: Result<(), crate::dispatcher::DispatcherError>,
) -> Result<(), EngineError> {
    match source_result {
        Ok(()) => {}
        Err(err) => {
            return Err(EngineError::Source {
                id: source_id,
                error: err.to_string(),
            })
        }
    }
    match dispatcher_result {
        Ok(()) => Ok(()),
        Err(err) => Err(EngineError::Dispatcher(err.to_string())),
    }
}

/// Install a SIGINT / SIGTERM handler that fires the supplied shutdown
/// token. The returned task ends when the first signal is received OR
/// when the token is cancelled by some other path.
pub fn spawn_signal_handler(shutdown: ShutdownToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let sigterm = async {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(err) => {
                    error!(error = %err, "failed to install SIGTERM handler");
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let sigterm = std::future::pending::<()>();

        tokio::select! {
            biased;
            () = shutdown.cancelled() => {}
            res = ctrl_c => {
                match res {
                    Ok(()) => info!("received SIGINT; shutting down"),
                    Err(err) => error!(error = %err, "ctrl_c handler error"),
                }
                shutdown.cancel();
            }
            () = sigterm => {
                info!("received SIGTERM; shutting down");
                shutdown.cancel();
            }
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
    use super::*;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use ventstream_core::{
        ContentType, Event, Headers, Payload, SinkBatch, SinkError, SourceContext, SourceError,
        SourceUri, Subject,
    };

    fn make_event(label: &str) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(format!("test.{label}")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::empty())
            .build()
    }

    /// Emits N events then stays idle until shutdown.
    struct ScriptedSource {
        id: String,
        count: usize,
    }

    #[async_trait]
    impl Source for ScriptedSource {
        fn id(&self) -> &str {
            &self.id
        }
        fn kind(&self) -> &'static str {
            "scripted"
        }
        async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
            for i in 0..self.count {
                let event = make_event(&format!("e{i}"));
                if ctx.sender.send(event, &ctx.shutdown).await.is_err() {
                    break;
                }
            }
            ctx.shutdown.cancelled().await;
            Ok(())
        }
    }

    struct InvalidJoinSource;

    #[async_trait]
    impl Source for InvalidJoinSource {
        fn id(&self) -> &str {
            "invalid-join-source"
        }

        fn kind(&self) -> &'static str {
            "scripted"
        }

        async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
            let mut headers = std::collections::HashMap::new();
            headers.insert("ventstream.cdc.namespace".to_owned(), "public".to_owned());
            headers.insert("ventstream.cdc.relation".to_owned(), "orders".to_owned());
            headers.insert("ventstream.cdc.lsn".to_owned(), "42".to_owned());
            let event = Event::builder(
                SourceUri::new("postgres://pub/public.orders").expect("source URI"),
                Subject::new("postgres.public.orders.insert").expect("subject"),
            )
            .payload(Payload::from_vec(b"{invalid-json".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();
            let _ = ctx.sender.send(event, &ctx.shutdown).await;
            ctx.shutdown.cancelled().await;
            Ok(())
        }
    }

    struct CountingSink {
        id: String,
        received: Arc<Mutex<Vec<Vec<Event>>>>,
        total: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sink for CountingSink {
        fn id(&self) -> &str {
            &self.id
        }
        fn kind(&self) -> &'static str {
            "counting"
        }
        async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
            let events = batch.into_events();
            self.total.fetch_add(events.len(), Ordering::SeqCst);
            self.received.lock().push(events);
            Ok(())
        }
    }

    fn temp_dlq(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "ventstream-engine-test-{}-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            name,
        ));
        path
    }

    #[tokio::test]
    async fn end_to_end_source_to_sink() {
        let source = Box::new(ScriptedSource {
            id: "src".into(),
            count: 5,
        });
        let received = Arc::new(Mutex::new(Vec::new()));
        let total = Arc::new(AtomicUsize::new(0));
        let sink = Arc::new(CountingSink {
            id: "sink".into(),
            received: Arc::clone(&received),
            total: Arc::clone(&total),
        });

        let dlq_path = temp_dlq("end_to_end");
        let config = EngineConfig {
            bus_capacity: 16,
            dispatcher: DispatcherConfig {
                max_events: 2,
                max_batch_bytes: 4 * 1024 * 1024,
                flush_interval: Duration::from_millis(50),
                max_parallel_bulks: 4,
            },
            dlq_path: dlq_path.clone(),
            memory: MemoryControllerConfig {
                enabled: false,
                ..MemoryControllerConfig::default()
            },
        };

        let shutdown = ShutdownToken::new();
        let engine = Engine::new(source, Arc::clone(&sink) as Arc<dyn Sink>, config);

        let shutdown_clone = shutdown.clone();
        let trigger = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            shutdown_clone.cancel();
        });

        engine.run(shutdown).await.expect("engine ok");
        let _ = trigger.await;

        assert_eq!(
            total.load(Ordering::SeqCst),
            5,
            "all 5 events should reach the sink"
        );
        assert!(received.lock().len() >= 2, "should be multiple batches");
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }

    /// #131 end to end: a row the join engine can never process is written
    /// to the pipeline's DLQ with the join error as its reason, and the
    /// pipeline keeps running instead of failing the iteration.
    #[tokio::test]
    async fn join_poison_row_is_dead_lettered_and_pipeline_continues() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let total = Arc::new(AtomicUsize::new(0));
        let sink = Arc::new(CountingSink {
            id: "sink".into(),
            received,
            total: Arc::clone(&total),
        });
        let config = EngineConfig {
            bus_capacity: 8,
            dispatcher: DispatcherConfig {
                max_events: 4,
                max_batch_bytes: 1024 * 1024,
                flush_interval: Duration::from_millis(10),
                max_parallel_bulks: 1,
            },
            dlq_path: temp_dlq("join_poison"),
            memory: MemoryControllerConfig {
                enabled: false,
                ..MemoryControllerConfig::default()
            },
        };
        let dlq_path = config.dlq_path.clone();
        let join: ventstream_joins::JoinDefinition = serde_yaml::from_str(
            r#"
name: orders
primary:
  table: public.orders
  pk: id
"#,
        )
        .expect("valid join definition");
        let join_engine = Arc::new(JoinEngine::new(
            vec![join],
            Arc::new(ventstream_joins::fetcher::NoopFetcher),
        ));
        let shutdown = ShutdownToken::new();
        // The source parks until shutdown; before #131 the join failure
        // cancelled it. Now nothing does, so end the run ourselves.
        let shutdown_clone = shutdown.clone();
        let trigger = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            shutdown_clone.cancel();
        });
        let result = Engine::new(Box::new(InvalidJoinSource), sink as Arc<dyn Sink>, config)
            .with_joins(join_engine)
            .run(shutdown)
            .await;
        let _ = trigger.await;

        assert!(
            result.is_ok(),
            "a poison row must not fail the pipeline: {result:?}"
        );
        assert_eq!(
            total.load(Ordering::SeqCst),
            0,
            "the poison row must not reach the sink"
        );
        let dlq = tokio::fs::read_to_string(&dlq_path)
            .await
            .expect("DLQ file written");
        let lines: Vec<&str> = dlq.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "exactly the poison row is dead-lettered: {dlq}"
        );
        let record: serde_json::Value = serde_json::from_str(lines[0]).expect("DLQ record is JSON");
        let reason = record["reason"].as_str().expect("reason");
        assert!(
            reason.starts_with("join engine: ")
                && reason.contains("does not deserialize as a JSON object"),
            "the reason must name the join failure: {reason}"
        );
        assert_eq!(
            record["event"]["subject"], "postgres.public.orders.insert",
            "the full event is kept for replay: {record}"
        );
        let _ = tokio::fs::remove_file(&dlq_path).await;
    }
}
