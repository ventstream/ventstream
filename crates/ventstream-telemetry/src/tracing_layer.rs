//! Tracing-subscriber layer that samples DEBUG events with a
//! `metric=` field and pipes them to the telemetry channel for batch
//! upload.
//!
//! Designed to add **zero meaningful overhead** when sampling at 1%:
//! - Field visitation is cheap (small fixed cost per event).
//! - Sample check is `rng.gen::<f32>()` per event — nanoseconds.
//! - Below sample threshold we return early without allocating the
//!   trace record.
//! - Channel send is `try_send` — never blocks; on overflow we drop
//!   and bump a local "dropped" counter (logged hourly).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::{should_sample, TraceRecord};

/// Subscriber layer that funnels eligible tracing events into the
/// telemetry upload pipeline.
///
/// "Eligible" = DEBUG-level event that carries a `metric` field.
/// That's the engine-side convention enforced in `docs/debugging.md`.
#[derive(Clone)]
pub struct TelemetryTraceLayer {
    tx: mpsc::Sender<TraceRecord>,
    sample_rate: f32,
    dropped: Arc<AtomicU64>,
}

impl TelemetryTraceLayer {
    pub(crate) fn new(tx: mpsc::Sender<TraceRecord>, sample_rate: f32) -> Self {
        Self {
            tx,
            sample_rate,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of events sampled-but-dropped due to channel overflow.
    /// Useful as an internal counter for operators chasing "why don't
    /// I see traces?"
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl<S> Layer<S> for TelemetryTraceLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // We're only interested in DEBUG-level events. Trace events
        // at higher levels (info / warn / error) flow into the
        // upload too — but DEBUG is where the metric= breadcrumbs
        // live, and on a busy engine they vastly outnumber the rest.
        // Filter early to avoid visiting fields for ineligible events.
        let level = event.metadata().level();
        // Allow any level — operator might want WARN/ERROR to flow
        // up at 100%. Sampling rate is the gate; default 1% applies
        // uniformly. For high-volume DEBUG that's plenty.
        if !should_sample(self.sample_rate) && *level == tracing::Level::DEBUG {
            return;
        }
        // Above DEBUG → emit unconditionally (errors should reach
        // the control plane even if we're sampling traces).
        if *level > tracing::Level::DEBUG && !should_sample(1.0) {
            return;
        }

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        // Require a `metric` field to count as a telemetry trace.
        // Without one this is just a debug log; we don't want to
        // ship every untagged log line.
        let metric = match visitor.fields.remove("metric").and_then(value_to_string) {
            Some(m) => m,
            None => return,
        };

        let elapsed_ms = visitor
            .fields
            .get("elapsed_ms")
            .and_then(|v| v.as_u64())
            .or_else(|| {
                // PG side names it differently sometimes.
                visitor
                    .fields
                    .get("cypher_elapsed_ms")
                    .and_then(|v| v.as_u64())
            });

        // Derive `source` from the target module path (eg
        // `ventstream_sources::neo4j::source` → `neo4j`).
        let target = event.metadata().target();
        let source = derive_source(target);

        let severity = match *level {
            tracing::Level::ERROR => "error",
            tracing::Level::WARN => "warn",
            _ => "info",
        };

        let record = TraceRecord {
            sampled_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            metric,
            severity: severity.to_owned(),
            elapsed_ms,
            source,
            fields: visitor.fields,
        };

        // Non-blocking. If the buffer is full (control plane slow or
        // disabled), drop on the floor and bump the dropped counter.
        // This is the line that guarantees telemetry never affects
        // engine throughput.
        if self.tx.try_send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn derive_source(target: &str) -> Option<String> {
    if target.contains("neo4j") {
        Some("neo4j".to_owned())
    } else if target.contains("postgres") || target.contains("pg") {
        Some("postgres".to_owned())
    } else if target.contains("dispatcher") {
        Some("dispatcher".to_owned())
    } else if target.contains("bus") {
        Some("bus".to_owned())
    } else if target.contains("sink") {
        Some("sink".to_owned())
    } else {
        None
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: serde_json::Map<String, Value>,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let s = format!("{:?}", value);
        // Strip surrounding quotes that Debug wraps strings in.
        let trimmed = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            s[1..s.len() - 1].to_owned()
        } else {
            s
        };
        self.fields
            .insert(field.name().to_owned(), Value::String(trimmed));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), Value::String(value.to_owned()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_owned(), Value::Number(n));
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), Value::Bool(value));
    }
}

fn value_to_string(v: Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
