//! Redis broker providers for VentStream realtime gateways.
//!
//! The initial provider uses Redis Streams. It runs one blocking stream tailer
//! per tenant and gateway process, then multiplexes deliveries through a
//! bounded local broadcast. Reconnecting clients replay lazily from Redis up
//! to a captured live watermark before joining that broadcast, avoiding both
//! gaps and one blocked Redis connection per WebSocket client.

#![deny(missing_docs)]

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::{StreamId, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use tokio::sync::{broadcast, Mutex, OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use ventstream_protocol::SubjectPattern;
use ventstream_realtime::{
    BrokerError, BrokerEvent, BrokerKind, Cursor, CursorProvider, EventSession, RealtimeBroker,
    SessionRequest,
};

const SUBJECT_FIELD: &str = "subject";
const EVENT_FIELD: &str = "event";
const TAILER_FAILURE_THRESHOLD: u32 = 6;
const INITIAL_STREAM_ID: RedisStreamId = RedisStreamId {
    milliseconds: 0,
    sequence: 0,
};

/// Redis Streams provider configuration.
#[derive(Debug, Clone)]
pub struct RedisStreamsConfig {
    /// Redis or TLS Redis URL. Keep credentials in a deployment-side secret.
    pub url: String,
    /// Prefix used to derive `prefix:{tenant}:events` stream keys.
    pub key_prefix: String,
    /// Maximum records returned by each Redis read.
    pub read_batch: usize,
    /// Finite block duration for the shared `XREAD` tailer.
    pub block_timeout: Duration,
    /// Number of live events retained in the per-tenant local broadcast.
    pub broadcast_capacity: usize,
    /// Maximum tenant hubs and blocking tailers held by one gateway process.
    pub max_tenant_hubs: usize,
    /// Target maximum entries retained per tenant stream. The gateway applies
    /// exact periodic trims; `None` leaves retention to publishers or Redis.
    pub max_length: Option<usize>,
    /// Connection establishment timeout.
    pub connect_timeout: Duration,
    /// Response timeout for control/replay calls and the margin added to the
    /// blocking tailer's server-side block duration.
    pub response_timeout: Duration,
}

impl Default for RedisStreamsConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".to_owned(),
            key_prefix: "ventstream".to_owned(),
            read_batch: 256,
            block_timeout: Duration::from_secs(5),
            broadcast_capacity: 2048,
            max_tenant_hubs: 1024,
            max_length: Some(1_000_000),
            connect_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
        }
    }
}

impl RedisStreamsConfig {
    fn validate(&self) -> Result<(), BrokerError> {
        if self.url.trim().is_empty() {
            return Err(BrokerError::Configuration(
                "Redis URL must not be empty".to_owned(),
            ));
        }
        validate_key_segment("Redis key prefix", &self.key_prefix)?;
        if self.read_batch == 0 {
            return Err(BrokerError::Configuration(
                "Redis read_batch must be positive".to_owned(),
            ));
        }
        if self.broadcast_capacity == 0 {
            return Err(BrokerError::Configuration(
                "Redis broadcast_capacity must be positive".to_owned(),
            ));
        }
        if self.max_tenant_hubs == 0 {
            return Err(BrokerError::Configuration(
                "Redis max_tenant_hubs must be positive".to_owned(),
            ));
        }
        if self.max_length == Some(0) {
            return Err(BrokerError::Configuration(
                "Redis max_length must be positive when set".to_owned(),
            ));
        }
        if self.block_timeout.is_zero()
            || self.connect_timeout.is_zero()
            || self.response_timeout.is_zero()
        {
            return Err(BrokerError::Configuration(
                "Redis timeouts must be positive".to_owned(),
            ));
        }
        if self.block_timeout > Duration::from_secs(30) {
            return Err(BrokerError::Configuration(
                "Redis block_timeout must not exceed 30 seconds".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Parsed Redis Stream entry ID.
///
/// IDs are ordered as `(milliseconds, sequence)` numeric pairs. Lexical string
/// comparison is incorrect for values with different digit lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedisStreamId {
    milliseconds: u64,
    sequence: u64,
}

impl RedisStreamId {
    /// Timestamp component assigned by Redis.
    #[must_use]
    pub const fn milliseconds(self) -> u64 {
        self.milliseconds
    }

    /// Sequence component assigned within the same millisecond.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Convert this ID to its versioned client resume cursor.
    #[must_use]
    pub fn to_cursor(self) -> Cursor {
        Cursor::redis_streams(self.milliseconds, self.sequence)
    }
}

impl fmt::Display for RedisStreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.milliseconds, self.sequence)
    }
}

impl FromStr for RedisStreamId {
    type Err = RedisStreamIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (milliseconds, sequence) = value
            .split_once('-')
            .ok_or(RedisStreamIdError::InvalidFormat)?;
        if milliseconds.is_empty() || sequence.is_empty() || sequence.contains('-') {
            return Err(RedisStreamIdError::InvalidFormat);
        }
        Ok(Self {
            milliseconds: milliseconds
                .parse::<u64>()
                .map_err(|_| RedisStreamIdError::InvalidMilliseconds)?,
            sequence: sequence
                .parse::<u64>()
                .map_err(|_| RedisStreamIdError::InvalidSequence)?,
        })
    }
}

/// Redis Stream ID syntax error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RedisStreamIdError {
    /// An ID must contain exactly two non-empty numeric components.
    #[error("Redis Stream ID must have the form '<milliseconds>-<sequence>'")]
    InvalidFormat,
    /// Timestamp component is not an unsigned integer.
    #[error("Redis Stream ID timestamp must be an unsigned integer")]
    InvalidMilliseconds,
    /// Sequence component is not an unsigned integer.
    #[error("Redis Stream ID sequence must be an unsigned integer")]
    InvalidSequence,
}

#[derive(Clone)]
struct TenantHub {
    sender: broadcast::Sender<HubMessage>,
    watermark: Arc<RwLock<Option<RedisStreamId>>>,
    status: Arc<RwLock<TailerStatus>>,
    start_tailer: CancellationToken,
}

#[derive(Clone)]
enum HubMessage {
    Event(Arc<BrokerEvent>),
    Terminal(Arc<str>),
}

#[derive(Clone)]
enum TailerStatus {
    Healthy,
    Unavailable(Arc<str>),
}

struct RedisStreamsInner {
    client: redis::Client,
    control: ConnectionManager,
    config: RedisStreamsConfig,
    hubs: Mutex<HashMap<String, Arc<OnceCell<TenantHub>>>>,
    shutdown: CancellationToken,
}

impl Drop for RedisStreamsInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Redis Streams implementation of the realtime broker contract.
#[derive(Clone)]
pub struct RedisStreamsBroker {
    inner: Arc<RedisStreamsInner>,
}

impl RedisStreamsBroker {
    /// Connect to Redis and create the provider.
    pub async fn connect(config: RedisStreamsConfig) -> Result<Self, BrokerError> {
        config.validate()?;
        let client = redis::Client::open(config.url.as_str())
            .map_err(|err| BrokerError::Configuration(format!("invalid Redis URL: {err}")))?;
        let manager_config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(config.connect_timeout))
            .set_response_timeout(Some(config.response_timeout))
            .set_number_of_retries(6);
        let control = client
            .get_connection_manager_with_config(manager_config)
            .await
            .map_err(|err| BrokerError::Unavailable(format!("connecting to Redis: {err}")))?;
        let broker = Self {
            inner: Arc::new(RedisStreamsInner {
                client,
                control,
                config,
                hubs: Mutex::new(HashMap::new()),
                shutdown: CancellationToken::new(),
            }),
        };
        broker.ready().await?;
        Ok(broker)
    }

    async fn hub_for(&self, tenant: &str) -> Result<(String, TenantHub), BrokerError> {
        let key = stream_key(&self.inner.config.key_prefix, tenant)?;
        let cell = {
            let mut hubs = self.inner.hubs.lock().await;
            if !hubs.contains_key(&key) && hubs.len() >= self.inner.config.max_tenant_hubs {
                return Err(BrokerError::Unavailable(format!(
                    "Redis tenant hub capacity reached ({})",
                    self.inner.config.max_tenant_hubs
                )));
            }
            Arc::clone(
                hubs.entry(key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };
        let hub = cell
            .get_or_try_init(|| async {
                let initial = stream_bounds(&mut self.inner.control.clone(), &key)
                    .await?
                    .map(|bounds| bounds.latest);
                let (sender, _) = broadcast::channel(self.inner.config.broadcast_capacity);
                let hub = TenantHub {
                    sender,
                    watermark: Arc::new(RwLock::new(initial)),
                    status: Arc::new(RwLock::new(TailerStatus::Healthy)),
                    start_tailer: CancellationToken::new(),
                };
                let tail_connection = self
                    .inner
                    .client
                    .get_connection_manager_with_config(
                        ConnectionManagerConfig::new()
                            .set_connection_timeout(Some(self.inner.config.connect_timeout))
                            .set_response_timeout(Some(
                                self.inner.config.block_timeout
                                    + self.inner.config.response_timeout,
                            ))
                            .set_number_of_retries(6),
                    )
                    .await
                    .map_err(|err| {
                        BrokerError::Unavailable(format!("opening Redis stream tailer: {err}"))
                    })?;
                spawn_tailer(
                    tail_connection,
                    key.clone(),
                    hub.clone(),
                    TailerConfig {
                        read_batch: self.inner.config.read_batch,
                        block_timeout: self.inner.config.block_timeout,
                        max_length: self.inner.config.max_length,
                        start: hub.start_tailer.clone(),
                        shutdown: self.inner.shutdown.child_token(),
                    },
                );
                Ok(hub)
            })
            .await?
            .clone();
        Ok((key, hub))
    }
}

#[async_trait]
impl RealtimeBroker for RedisStreamsBroker {
    fn kind(&self) -> BrokerKind {
        BrokerKind::RedisStreams
    }

    async fn ready(&self) -> Result<(), BrokerError> {
        let mut connection = self.inner.control.clone();
        let pong: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(|err| BrokerError::Unavailable(format!("Redis PING failed: {err}")))?;
        if pong != "PONG" {
            return Err(BrokerError::Unavailable(
                "Redis PING returned an unexpected response".to_owned(),
            ));
        }
        Ok(())
    }

    async fn open_session(
        &self,
        request: SessionRequest,
    ) -> Result<Box<dyn EventSession>, BrokerError> {
        let resumed = request.resume_after.is_some();
        let (key, hub) = self.hub_for(&request.tenant).await?;

        // Subscribe first, then capture the hub watermark. Anything published
        // during bounds/replay is either replayed through this cutoff or waits
        // in the local receiver and is de-duplicated at the transition.
        let live = hub.sender.subscribe();
        // The first session releases the shared tailer only after its receiver
        // exists. CancellationToken is used as a persistent one-shot latch, so
        // later sessions can call this harmlessly and no wake-up can be lost.
        hub.start_tailer.cancel();
        if let TailerStatus::Unavailable(reason) = &*hub.status.read().await {
            return Err(BrokerError::Unavailable(reason.to_string()));
        }
        let captured_cutoff = *hub.watermark.read().await;
        let resume_after = request
            .resume_after
            .as_ref()
            .map(redis_id_from_cursor)
            .transpose()?;
        // A brand-new live-only session must consume every event queued after
        // `subscribe()`. Applying the captured watermark as a cutoff would
        // discard an event that arrived between subscribe and this read.
        let live_cutoff = session_live_cutoff(resume_after, captured_cutoff);
        if let Some(requested) = resume_after {
            validate_resume(&mut self.inner.control.clone(), &key, requested).await?;
        }
        let replay = match (resume_after, live_cutoff) {
            (Some(after), Some(through)) if after < through => Some(ReplayState {
                requested: after,
                next_after: after,
                through,
                buffered: VecDeque::new(),
            }),
            _ => None,
        };
        metrics::counter!(
            "vs_realtime_broker_sessions_total",
            "provider" => BrokerKind::RedisStreams.as_str(),
            "resumed" => if resumed { "true" } else { "false" }
        )
        .increment(1);

        Ok(Box::new(RedisStreamsSession {
            connection: self.inner.control.clone(),
            key,
            read_batch: self.inner.config.read_batch,
            replay,
            live,
            live_cutoff,
            subject_filter: request.subject_filter,
        }))
    }
}

fn session_live_cutoff(
    resume_after: Option<RedisStreamId>,
    captured_cutoff: Option<RedisStreamId>,
) -> Option<RedisStreamId> {
    resume_after.and(captured_cutoff)
}

struct ReplayState {
    requested: RedisStreamId,
    next_after: RedisStreamId,
    through: RedisStreamId,
    buffered: VecDeque<BrokerEvent>,
}

struct RedisStreamsSession {
    connection: ConnectionManager,
    key: String,
    read_batch: usize,
    replay: Option<ReplayState>,
    live: broadcast::Receiver<HubMessage>,
    live_cutoff: Option<RedisStreamId>,
    subject_filter: Option<SubjectPattern>,
}

#[async_trait]
impl EventSession for RedisStreamsSession {
    async fn next(&mut self) -> Result<Option<BrokerEvent>, BrokerError> {
        loop {
            if let Some(replay) = &mut self.replay {
                if let Some(event) = replay.buffered.pop_front() {
                    if self
                        .subject_filter
                        .as_ref()
                        .is_some_and(|pattern| !pattern.matches_str(&event.subject))
                    {
                        continue;
                    }
                    metrics::counter!(
                        "vs_realtime_broker_replay_events_total",
                        "provider" => BrokerKind::RedisStreams.as_str()
                    )
                    .increment(1);
                    return Ok(Some(event));
                }
                let batch = read_range(
                    &mut self.connection,
                    &self.key,
                    replay.next_after,
                    replay.through,
                    self.read_batch,
                )
                .await?;
                let Some(last_scanned) = batch.last_scanned else {
                    if replay.next_after < replay.through {
                        let earliest = stream_bounds(&mut self.connection, &self.key)
                            .await?
                            .map_or(replay.through, |bounds| bounds.earliest);
                        return Err(BrokerError::ResumeExpired {
                            requested: replay.requested.to_cursor(),
                            earliest: earliest.to_cursor(),
                        });
                    }
                    self.replay = None;
                    continue;
                };
                replay.next_after = last_scanned;
                replay.buffered.extend(batch.events);
                continue;
            }

            match self.live.recv().await {
                Ok(HubMessage::Event(event)) => {
                    if let Some(cutoff) = self.live_cutoff {
                        let id = event
                            .cursor
                            .as_ref()
                            .map(redis_id_from_cursor)
                            .transpose()?
                            .ok_or_else(|| {
                                BrokerError::InvalidCursor(
                                    "Redis live entry did not carry a cursor".to_owned(),
                                )
                            })?;
                        if id <= cutoff {
                            continue;
                        }
                    }
                    self.live_cutoff = None;
                    if self
                        .subject_filter
                        .as_ref()
                        .is_some_and(|pattern| !pattern.matches_str(&event.subject))
                    {
                        continue;
                    }
                    return Ok(Some((*event).clone()));
                }
                Ok(HubMessage::Terminal(reason)) => {
                    return Err(BrokerError::SessionClosed(reason.to_string()));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(BrokerError::SessionClosed(format!(
                        "local Redis stream fan-out lagged by {skipped} event(s)"
                    )));
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(None),
            }
        }
    }

    async fn accepted(&mut self, _event: &BrokerEvent) -> Result<(), BrokerError> {
        // XREAD has no broker-side pending-entry list. The client cursor is the
        // durable checkpoint, so acceptance requires no Redis command.
        Ok(())
    }
}

struct TailerConfig {
    read_batch: usize,
    block_timeout: Duration,
    max_length: Option<usize>,
    start: CancellationToken,
    shutdown: CancellationToken,
}

fn spawn_tailer(
    mut connection: ConnectionManager,
    key: String,
    hub: TenantHub,
    config: TailerConfig,
) {
    tokio::spawn(async move {
        tokio::select! {
            () = config.start.cancelled() => {}
            () = config.shutdown.cancelled() => return,
        }
        let mut last = hub.watermark.read().await.unwrap_or(INITIAL_STREAM_ID);
        let block_ms = config.block_timeout.as_millis().min(usize::MAX as u128) as usize;
        let options = StreamReadOptions::default()
            .count(config.read_batch)
            .block(block_ms);
        let mut failures = 0u32;
        let mut entries_since_trim = 0usize;
        info!(stream = %key, cursor = %last, "Redis Streams tailer started");

        loop {
            let key_arg = key.as_str();
            let cursor_arg = last.to_string();
            let keys = [key_arg];
            let cursors = [cursor_arg.as_str()];
            let read = connection.xread_options::<_, _, StreamReadReply>(&keys, &cursors, &options);
            let reply = tokio::select! {
                () = config.shutdown.cancelled() => break,
                result = read => result,
            };
            let reply = match reply {
                Ok(reply) => {
                    if failures >= TAILER_FAILURE_THRESHOLD {
                        info!(stream = %key, "Redis Streams tailer recovered");
                    }
                    failures = 0;
                    *hub.status.write().await = TailerStatus::Healthy;
                    reply
                }
                Err(err) => {
                    failures = failures.saturating_add(1);
                    metrics::counter!(
                        "vs_realtime_broker_restarts_total",
                        "provider" => BrokerKind::RedisStreams.as_str(),
                        "reason" => "read_error"
                    )
                    .increment(1);
                    if failures == TAILER_FAILURE_THRESHOLD {
                        let reason: Arc<str> = Arc::from(format!(
                            "Redis Streams tailer unavailable after {failures} consecutive read failures"
                        ));
                        *hub.status.write().await =
                            TailerStatus::Unavailable(Arc::<str>::clone(&reason));
                        let _ = hub.sender.send(HubMessage::Terminal(reason));
                        metrics::counter!(
                            "vs_realtime_broker_terminal_failures_total",
                            "provider" => BrokerKind::RedisStreams.as_str(),
                            "reason" => "read_recovery_exhausted"
                        )
                        .increment(1);
                    }
                    let backoff =
                        Duration::from_millis(200u64.saturating_mul(1u64 << failures.min(5)));
                    warn!(stream = %key, error = %err, failures, "Redis Streams tail read failed; retrying");
                    tokio::select! {
                        () = config.shutdown.cancelled() => break,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    continue;
                }
            };

            for stream in reply.keys {
                for entry in stream.ids {
                    let id = match entry.id.parse::<RedisStreamId>() {
                        Ok(id) => id,
                        Err(err) => {
                            metrics::counter!(
                                "vs_realtime_broker_entries_dropped_total",
                                "provider" => BrokerKind::RedisStreams.as_str(),
                                "reason" => "invalid_stream_id"
                            )
                            .increment(1);
                            warn!(stream = %key, entry_id = %entry.id, error = %err, "dropping Redis entry with invalid ID");
                            continue;
                        }
                    };
                    last = id;
                    *hub.watermark.write().await = Some(id);
                    match broker_event(&entry) {
                        Ok(event) => {
                            let _ = hub.sender.send(HubMessage::Event(Arc::new(event)));
                            metrics::counter!(
                                "vs_realtime_broker_ingress_total",
                                "provider" => BrokerKind::RedisStreams.as_str()
                            )
                            .increment(1);
                        }
                        Err(err) => {
                            metrics::counter!(
                                "vs_realtime_broker_entries_dropped_total",
                                "provider" => BrokerKind::RedisStreams.as_str(),
                                "reason" => "invalid_entry"
                            )
                            .increment(1);
                            warn!(stream = %key, cursor = %id, error = %err, "dropping malformed Redis stream entry");
                        }
                    }
                    entries_since_trim = entries_since_trim.saturating_add(1);
                }
            }
            if let Some(max_length) = config.max_length {
                let trim_every = max_length.clamp(1, 4096);
                if entries_since_trim >= trim_every {
                    let trim = redis::cmd("XTRIM")
                        .arg(&key)
                        .arg("MAXLEN")
                        .arg(max_length)
                        .query_async::<usize>(&mut connection)
                        .await;
                    match trim {
                        Ok(removed) => {
                            metrics::counter!(
                                "vs_realtime_broker_retention_runs_total",
                                "provider" => BrokerKind::RedisStreams.as_str(),
                                "result" => "success"
                            )
                            .increment(1);
                            debug!(stream = %key, removed, max_length, "Redis Stream retention applied");
                            entries_since_trim = 0;
                        }
                        Err(error) => {
                            metrics::counter!(
                                "vs_realtime_broker_retention_runs_total",
                                "provider" => BrokerKind::RedisStreams.as_str(),
                                "result" => "error"
                            )
                            .increment(1);
                            warn!(stream = %key, %error, "Redis Stream retention failed; retrying after more deliveries");
                        }
                    }
                }
            }
        }
        debug!(stream = %key, "Redis Streams tailer stopped");
    });
}

#[derive(Debug, Clone, Copy)]
struct StreamBounds {
    earliest: RedisStreamId,
    latest: RedisStreamId,
}

async fn stream_bounds(
    connection: &mut ConnectionManager,
    key: &str,
) -> Result<Option<StreamBounds>, BrokerError> {
    let earliest: StreamRangeReply = connection
        .xrange_count(key, "-", "+", 1usize)
        .await
        .map_err(redis_unavailable("reading earliest Redis Stream ID"))?;
    let latest: StreamRangeReply = connection
        .xrevrange_count(key, "+", "-", 1usize)
        .await
        .map_err(redis_unavailable("reading latest Redis Stream ID"))?;
    match (earliest.ids.first(), latest.ids.first()) {
        (Some(first), Some(last)) => Ok(Some(StreamBounds {
            earliest: first
                .id
                .parse()
                .map_err(|err| BrokerError::InvalidCursor(format!("earliest Redis ID: {err}")))?,
            latest: last
                .id
                .parse()
                .map_err(|err| BrokerError::InvalidCursor(format!("latest Redis ID: {err}")))?,
        })),
        (None, None) => Ok(None),
        _ => Err(BrokerError::Unavailable(
            "Redis returned inconsistent stream bounds".to_owned(),
        )),
    }
}

async fn validate_resume(
    connection: &mut ConnectionManager,
    key: &str,
    requested: RedisStreamId,
) -> Result<(), BrokerError> {
    let Some(bounds) = stream_bounds(connection, key).await? else {
        if requested == INITIAL_STREAM_ID {
            return Ok(());
        }
        return Err(BrokerError::CursorAhead {
            requested: requested.to_cursor(),
            latest: INITIAL_STREAM_ID.to_cursor(),
        });
    };
    if requested > bounds.latest {
        return Err(BrokerError::CursorAhead {
            requested: requested.to_cursor(),
            latest: bounds.latest.to_cursor(),
        });
    }
    if requested < bounds.earliest {
        return Err(BrokerError::ResumeExpired {
            requested: requested.to_cursor(),
            earliest: bounds.earliest.to_cursor(),
        });
    }
    Ok(())
}

async fn read_range(
    connection: &mut ConnectionManager,
    key: &str,
    after: RedisStreamId,
    through: RedisStreamId,
    count: usize,
) -> Result<ReplayBatch, BrokerError> {
    let start = format!("({after}");
    let end = through.to_string();
    let reply: StreamRangeReply = connection
        .xrange_count(key, start, end, count)
        .await
        .map_err(redis_unavailable("replaying Redis Stream entries"))?;
    let mut batch = ReplayBatch {
        last_scanned: None,
        events: Vec::with_capacity(reply.ids.len()),
    };
    for entry in &reply.ids {
        let id = entry.id.parse::<RedisStreamId>().map_err(|error| {
            BrokerError::InvalidCursor(format!("invalid replay Redis ID: {error}"))
        })?;
        batch.last_scanned = Some(id);
        match broker_event(entry) {
            Ok(event) => batch.events.push(event),
            Err(error) => {
                metrics::counter!(
                    "vs_realtime_broker_entries_dropped_total",
                    "provider" => BrokerKind::RedisStreams.as_str(),
                    "reason" => "invalid_replay_entry"
                )
                .increment(1);
                warn!(stream = %key, cursor = %id, %error, "dropping malformed Redis replay entry");
            }
        }
    }
    Ok(batch)
}

struct ReplayBatch {
    last_scanned: Option<RedisStreamId>,
    events: Vec<BrokerEvent>,
}

fn broker_event(entry: &StreamId) -> Result<BrokerEvent, BrokerError> {
    let id = entry
        .id
        .parse::<RedisStreamId>()
        .map_err(|err| BrokerError::InvalidCursor(err.to_string()))?;
    let subject = entry
        .map
        .get(SUBJECT_FIELD)
        .ok_or_else(|| BrokerError::SessionClosed("Redis entry is missing 'subject'".to_owned()))
        .and_then(redis_string)?;
    let payload = entry
        .map
        .get(EVENT_FIELD)
        .ok_or_else(|| BrokerError::SessionClosed("Redis entry is missing 'event'".to_owned()))
        .and_then(redis_bytes)?;
    Ok(BrokerEvent {
        subject: Arc::from(subject),
        payload: Bytes::from(payload),
        cursor: Some(id.to_cursor()),
    })
}

fn redis_string(value: &redis::Value) -> Result<String, BrokerError> {
    redis::from_redis_value(value.clone())
        .map_err(|err| BrokerError::SessionClosed(format!("invalid Redis string field: {err}")))
}

fn redis_bytes(value: &redis::Value) -> Result<Vec<u8>, BrokerError> {
    redis::from_redis_value(value.clone())
        .map_err(|err| BrokerError::SessionClosed(format!("invalid Redis bytes field: {err}")))
}

fn redis_id_from_cursor(cursor: &Cursor) -> Result<RedisStreamId, BrokerError> {
    if cursor.provider() != CursorProvider::RedisStreams {
        return Err(BrokerError::CursorProviderMismatch {
            actual: cursor.provider(),
            expected: CursorProvider::RedisStreams,
        });
    }
    cursor
        .value()
        .parse::<RedisStreamId>()
        .map_err(|err| BrokerError::InvalidCursor(err.to_string()))
}

fn stream_key(prefix: &str, tenant: &str) -> Result<String, BrokerError> {
    validate_key_segment("Redis key prefix", prefix)?;
    validate_key_segment("tenant", tenant)?;
    Ok(format!("{prefix}:{{{tenant}}}:events"))
}

fn validate_key_segment(name: &str, value: &str) -> Result<(), BrokerError> {
    if value.is_empty() || value.len() > 128 {
        return Err(BrokerError::Configuration(format!(
            "{name} must contain between 1 and 128 bytes"
        )));
    }
    if value.chars().any(|character| {
        character.is_control() || character.is_whitespace() || "{}".contains(character)
    }) {
        return Err(BrokerError::Configuration(format!(
            "{name} contains an invalid character"
        )));
    }
    Ok(())
}

fn redis_unavailable(action: &'static str) -> impl FnOnce(redis::RedisError) -> BrokerError + Copy {
    move |err| BrokerError::Unavailable(format!("{action}: {err}"))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use redis::Value;

    use super::*;

    #[test]
    fn redis_stream_ids_are_parsed_and_ordered_numerically() {
        let small: RedisStreamId = "9-10".parse().expect("valid ID");
        let large: RedisStreamId = "10-0".parse().expect("valid ID");
        assert!(small < large, "must not compare IDs lexically");
        assert_eq!(large.milliseconds(), 10);
        assert_eq!(large.sequence(), 0);
        assert!("10".parse::<RedisStreamId>().is_err());
        assert!("10-a".parse::<RedisStreamId>().is_err());
        assert!("10-1-2".parse::<RedisStreamId>().is_err());
    }

    #[test]
    fn stream_keys_pin_one_tenant_to_one_cluster_slot() {
        assert_eq!(
            stream_key("ventstream", "tenant-a").expect("valid key"),
            "ventstream:{tenant-a}:events"
        );
        assert!(stream_key("ventstream", "bad{tenant}").is_err());
        assert!(stream_key("bad prefix", "tenant-a").is_err());
    }

    #[test]
    fn cursor_conversion_rejects_other_brokers_and_bad_ids() {
        let jetstream = Cursor::jetstream(42);
        assert!(matches!(
            redis_id_from_cursor(&jetstream),
            Err(BrokerError::CursorProviderMismatch { .. })
        ));
        let malformed = Cursor::new(CursorProvider::RedisStreams, Arc::<str>::from("not-an-id"))
            .expect("opaque cursor accepts provider value");
        assert!(matches!(
            redis_id_from_cursor(&malformed),
            Err(BrokerError::InvalidCursor(_))
        ));
    }

    #[test]
    fn stream_entry_decodes_subject_payload_and_cursor() {
        let mut map = HashMap::new();
        map.insert(
            SUBJECT_FIELD.to_owned(),
            Value::BulkString(b"vs.t.acme.orders.updated.42".to_vec()),
        );
        map.insert(
            EVENT_FIELD.to_owned(),
            Value::BulkString(br#"{"id":"event-1"}"#.to_vec()),
        );
        let event = broker_event(&StreamId {
            id: "1712345678901-7".to_owned(),
            map,
            ..StreamId::default()
        })
        .expect("valid stream entry");
        assert_eq!(event.subject.as_ref(), "vs.t.acme.orders.updated.42");
        assert_eq!(event.payload, Bytes::from_static(br#"{"id":"event-1"}"#));
        assert_eq!(
            event.cursor.as_ref().map(Cursor::to_wire).as_deref(),
            Some("rs:1712345678901-7")
        );
    }

    #[test]
    fn configuration_rejects_unbounded_or_unsafe_values() {
        let config = RedisStreamsConfig {
            read_batch: 0,
            ..RedisStreamsConfig::default()
        };
        assert!(config.validate().is_err());
        let config = RedisStreamsConfig {
            read_batch: 1,
            block_timeout: Duration::from_secs(31),
            ..RedisStreamsConfig::default()
        };
        assert!(config.validate().is_err());
        let config = RedisStreamsConfig {
            max_tenant_hubs: 0,
            ..RedisStreamsConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn live_only_session_does_not_discard_the_subscribe_handoff_window() {
        let captured = RedisStreamId {
            milliseconds: 100,
            sequence: 2,
        };

        assert_eq!(session_live_cutoff(None, Some(captured)), None);
        assert_eq!(
            session_live_cutoff(
                Some(RedisStreamId {
                    milliseconds: 100,
                    sequence: 1,
                }),
                Some(captured),
            ),
            Some(captured)
        );
    }
}
