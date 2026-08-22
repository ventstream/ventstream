//! Source impl: Kafka consumer + Debezium/raw unwrap + sink-gated commit.
//!
//! Each consumed message is mapped to an event and published to the bus; its
//! offset is committed only once the sink has durably written everything up to
//! that message's consume-seq (read from the shared `sink_progress` watermark).
//! No capture, no DB connection, no file cursor — resume is the group's
//! committed offsets.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{info, warn};
use ventstream_core::{Event, Source, SourceContext, SourceError};

use super::config::KafkaCdcConfig;
use super::envelope::{self, Mapping};
use super::event_mapper;
use super::offset::OffsetTracker;
use crate::error::KafkaCdcError;

const ACK_SEQUENCE_HEADER: &str = "ventstream.cdc.ack_seq";
const SOURCE_VERSION_HEADER: &str = "ventstream.cdc.source_version";

/// Kafka/Redpanda CDC source.
pub struct KafkaCdcSource {
    config: KafkaCdcConfig,
    sink_progress: Option<Arc<AtomicU64>>,
    /// Consecutive unmappable-message skips; reset by any message that
    /// maps (or intentionally filters). Bounded by
    /// [`MAX_UNMAPPABLE_STREAK`].
    unmappable_streak: AtomicU64,
}

/// Tolerated run of consecutive unmappable messages before the source
/// halts. Far above any real poison-record cluster; far below a whole
/// topic misdecoding.
const MAX_UNMAPPABLE_STREAK: u64 = 256;

/// Owned copy of a message — lets us drop the borrowed message (which is
/// `!Send`) before any `.await`.
struct OwnedMsg {
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

impl KafkaCdcSource {
    /// Construct from configuration.
    pub fn new(config: KafkaCdcConfig) -> Self {
        Self {
            config,
            sink_progress: None,
            unmappable_streak: AtomicU64::new(0),
        }
    }

    /// Attach the sink-progress watermark. Offsets are committed only up to
    /// the seq the sink has durably written. Without it, offsets commit after
    /// publish (at-least-once + idempotent doc-ids), like the Mongo source.
    #[must_use]
    pub fn with_sink_progress(mut self, progress: Arc<AtomicU64>) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    fn build_consumer(&self) -> Result<StreamConsumer, KafkaCdcError> {
        let mut cc = ClientConfig::new();
        cc.set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", &self.config.auto_offset_reset);
        if let Some(p) = &self.config.security_protocol {
            cc.set("security.protocol", p);
        }
        if let Some(m) = &self.config.sasl_mechanism {
            cc.set("sasl.mechanism", m);
        }
        if let Some(u) = &self.config.sasl_username {
            cc.set("sasl.username", u);
        }
        if let Some(p) = &self.config.sasl_password {
            cc.set("sasl.password", p);
        }
        if let Some(ca) = &self.config.ssl_ca_location {
            cc.set("ssl.ca.location", ca);
        }
        cc.create()
            .map_err(|e| KafkaCdcError::Connection(e.to_string()))
    }

    async fn run_inner(&self, ctx: SourceContext) -> Result<(), KafkaCdcError> {
        let consumer = self.build_consumer()?;
        let topics: Vec<&str> = self.config.topics.iter().map(String::as_str).collect();
        if topics.is_empty() {
            return Err(KafkaCdcError::Connection(
                "no topics configured (VS_KAFKA_TOPICS)".to_owned(),
            ));
        }
        consumer
            .subscribe(&topics)
            .map_err(|e| KafkaCdcError::Connection(format!("subscribe failed: {e}")))?;

        info!(
            source = %self.config.id,
            brokers = %self.config.brokers,
            group = %self.config.group_id,
            topics = ?self.config.topics,
            unwrap = ?self.config.unwrap,
            "kafka consumer started"
        );

        let mut tracker = OffsetTracker::new();
        let mut commit =
            tokio::time::interval(self.config.commit_interval.max(Duration::from_millis(50)));
        commit.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    self.commit_now(&consumer, &mut tracker, CommitMode::Sync);
                    return Ok(());
                }
                _ = commit.tick() => {
                    self.commit_now(&consumer, &mut tracker, CommitMode::Async);
                }
                recv = consumer.recv() => {
                    let owned = match recv {
                        Ok(msg) => OwnedMsg {
                            topic: msg.topic().to_owned(),
                            partition: msg.partition(),
                            offset: msg.offset(),
                            key: msg.key().map(<[u8]>::to_vec),
                            payload: msg.payload().map(<[u8]>::to_vec),
                        },
                        // librdkafka reconnects internally; a recv error is
                        // usually transient. Log and keep consuming.
                        Err(e) => {
                            warn!(source = %self.config.id, error = %e, "kafka recv error; continuing");
                            continue;
                        }
                    };
                    if !self.handle(owned, &mut tracker, &ctx).await? {
                        self.commit_now(&consumer, &mut tracker, CommitMode::Sync);
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Map one message and (for a change) publish it. Returns `false` if the
    /// publish was cancelled by shutdown.
    async fn handle(
        &self,
        owned: OwnedMsg,
        tracker: &mut OffsetTracker,
        ctx: &SourceContext,
    ) -> Result<bool, KafkaCdcError> {
        let mapping = envelope::map(
            &self.config,
            &owned.topic,
            owned.key.as_deref(),
            owned.payload.as_deref(),
        );
        match mapping {
            Ok(Mapping::Skip) => {
                // Intentional skip (tombstone/heartbeat/filter) — not an
                // error; resets the unmappable streak like any healthy
                // message would.
                metrics::counter!("vs_kafka_skipped_total", "reason" => "filtered").increment(1);
                self.unmappable_streak
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                tracker.record(&owned.topic, owned.partition, owned.offset, false);
            }
            Ok(Mapping::Event(mapped)) => match event_mapper::to_event(&mapped) {
                Ok(event) => {
                    self.unmappable_streak
                        .store(0, std::sync::atomic::Ordering::Relaxed);
                    // Record as needs-sink only once we're committed to
                    // publishing, so a build/publish failure can't leave a
                    // seq that blocks the commit prefix forever.
                    let seq = tracker.record(&owned.topic, owned.partition, owned.offset, true);
                    let event = stamp_progress(event, seq, owned.offset);
                    if !self.publish(ctx, event).await? {
                        return Ok(false);
                    }
                }
                Err(e) => {
                    self.note_unmappable(&owned, &e)?;
                    tracker.record(&owned.topic, owned.partition, owned.offset, false);
                }
            },
            Err(e) => {
                self.note_unmappable(&owned, &e)?;
                tracker.record(&owned.topic, owned.partition, owned.offset, false);
            }
        }
        Ok(true)
    }

    /// Count and bound unmappable skips. Isolated bad messages skip
    /// with a warning + counter (at-least-once pipelines tolerate a
    /// poison record); an unbroken run of them means EVERY message
    /// fails to map — a converter/serialization misconfiguration where
    /// "skip and commit" is 100% silent data loss on a pipeline that
    /// looks healthy. Halt so the operator sees it immediately.
    fn note_unmappable(
        &self,
        owned: &OwnedMsg,
        error: &KafkaCdcError,
    ) -> Result<(), KafkaCdcError> {
        metrics::counter!("vs_kafka_skipped_total", "reason" => "unmappable").increment(1);
        ventstream_telemetry::record_error(format!(
            "kafka unmappable message skipped ({}/{}@{}): {error}",
            owned.topic, owned.partition, owned.offset
        ));
        let streak = self
            .unmappable_streak
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        if streak >= MAX_UNMAPPABLE_STREAK {
            return Err(KafkaCdcError::MalformedEvent(format!(
                "{streak} consecutive messages failed to map (last: {error}) — this is a                  converter/format misconfiguration, not poison records; halting instead of                  silently committing past the entire topic. Check the connector's                  key/value converter (JSON without schemas) and the topic's format"
            )));
        }
        warn!(
            source = %self.config.id, topic = %owned.topic,
            partition = owned.partition, offset = owned.offset,
            streak,
            error = %error, "unmappable message; skipping"
        );
        Ok(())
    }

    /// Commit the sink-durable offset prefix. Errors are logged, not fatal —
    /// uncommitted offsets just redeliver (idempotent at the sink).
    fn commit_now(&self, consumer: &StreamConsumer, tracker: &mut OffsetTracker, mode: CommitMode) {
        let durable = self
            .sink_progress
            .as_ref()
            .map_or(u64::MAX, |p| p.load(Ordering::Relaxed));
        let commits = tracker.committable(durable);
        if commits.is_empty() {
            return;
        }
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), next) in &commits {
            if let Err(e) = tpl.add_partition_offset(topic, *partition, Offset::Offset(*next)) {
                warn!(topic = %topic, partition = partition, error = %e, "kafka: bad commit offset");
                return;
            }
        }
        if let Err(e) = consumer.commit(&tpl, mode) {
            warn!(source = %self.config.id, error = %e, "kafka offset commit failed (will retry)");
        }
    }

    async fn publish(&self, ctx: &SourceContext, event: Event) -> Result<bool, KafkaCdcError> {
        match ctx.sender.send(event, &ctx.shutdown).await {
            Ok(()) => Ok(true),
            Err(ventstream_core::BackpressureError::Cancelled) => Ok(false),
            Err(e) => Err(KafkaCdcError::Internal(format!("bus publish failed: {e}"))),
        }
    }
}

fn stamp_progress(event: Event, ack_seq: u64, offset: i64) -> Event {
    // The process-local ack sequence gates Kafka commits, but must never be
    // used as an OpenSearch external version because it resets on restart.
    // A keyed Kafka record remains on one partition, where offset+1 is a
    // stable, positive, monotonically increasing document version.
    let source_version = offset.saturating_add(1).max(1);
    Event {
        headers: event
            .headers
            .with_header(ACK_SEQUENCE_HEADER.to_owned(), ack_seq.to_string())
            .with_header(SOURCE_VERSION_HEADER.to_owned(), source_version.to_string()),
        ..event
    }
}

#[async_trait]
impl Source for KafkaCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn kind(&self) -> &'static str {
        "kafka_cdc"
    }
    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
        self.run_inner(ctx)
            .await
            .map_err(|e| SourceError::Internal(e.to_string()))
    }
}
