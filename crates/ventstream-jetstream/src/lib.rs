//! Shared JetStream consumer lifecycle for the real-time gateways.
//!
//! Both the native WebSocket gateway (`ventstream-ws`) and the
//! GraphQL gateway (`ventstream-graphql`) use **bespoke** consumers:
//! one durable JetStream consumer per client connection for both native WS
//! and GraphQL — created on demand
//! and deleted when that unit goes away. "Bespoke" = custom-made for
//! one client and disposable, the opposite of a shared/pooled
//! consumer that many clients read from.
//!
//! Because every consumer is disposable, three cleanup layers keep
//! the broker tidy (see [`run_reaper`] and [`ConsumerHandle`]):
//!
//! 1. **Drop guard** ([`ConsumerHandle`]) — async-deletes the consumer
//!    on every owner-task exit path. Wins the common case.
//! 2. **`inactive_threshold`** — JetStream server-side auto-delete when
//!    no client has pulled within the window. Catches `kill -9`.
//! 3. **[`run_reaper`]** — periodic sweep deleting consumers under this
//!    pod's marker whose id is no longer [`ActiveConsumers`]. Catches
//!    the case where the Drop-spawned delete failed.
//!
//! Both gateways previously carried near-identical copies of all of
//! this, coupled only by a hand-written name format. Centralising it
//! here removes that duplication and the silent-drift hazard (change
//! the name in one place and the other's reaper stops matching).
//!
//! Nothing here is on the per-event hot path: consumers are created
//! and torn down at connect/subscribe time, and the reaper runs on a
//! coarse interval. The hot path (event fan-out) lives in the gateway
//! crates.

#![deny(missing_docs)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::{MessagesErrorKind, Stream as PullStream};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::{pull::Config as PullConfig, AckPolicy, DeliverPolicy};
use async_nats::jetstream::Context as JsContext;
use async_trait::async_trait;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tracing::{debug, info, warn};
use ventstream_core::ShutdownToken;
use ventstream_realtime::{
    BrokerError, BrokerEvent, BrokerKind, Cursor, CursorProvider, EventSession, GatewayRole,
    RealtimeBroker, SessionRequest,
};

/// Errors creating a bespoke consumer.
#[derive(Debug, thiserror::Error)]
pub enum JetStreamError {
    /// The configured stream could not be loaded (must exist already;
    /// the `ws` role / an operator bootstraps it).
    #[error("stream '{stream}' not reachable: {detail}")]
    Stream {
        /// Stream name that failed to load.
        stream: String,
        /// Underlying error detail.
        detail: String,
    },
    /// `create_consumer` was rejected by the broker.
    #[error("create_consumer '{name}': {detail}")]
    Create {
        /// Consumer name that failed to create.
        name: String,
        /// Underlying error detail.
        detail: String,
    },
    /// The requested resume point has already fallen out of retention.
    #[error(
        "resume cursor expired: requested start sequence {requested}, earliest available {earliest}, latest {latest}"
    )]
    ResumeExpired {
        /// First sequence the reconnect requested (`last_processed + 1`).
        requested: u64,
        /// Earliest sequence still retained by the stream.
        earliest: u64,
        /// Latest sequence assigned by the stream.
        latest: u64,
    },
    /// The requested resume point is beyond the stream high watermark.
    #[error(
        "resume cursor is ahead of the stream: requested start sequence {requested}, next available sequence is at most {next}"
    )]
    ResumeAhead {
        /// First sequence the reconnect requested (`last_processed + 1`).
        requested: u64,
        /// Sequence immediately after the current stream high watermark.
        next: u64,
    },
}

fn validate_resume_window(
    requested: u64,
    messages: u64,
    first_sequence: u64,
    last_sequence: u64,
) -> Result<(), JetStreamError> {
    let next = last_sequence.saturating_add(1);
    if requested > next {
        return Err(JetStreamError::ResumeAhead { requested, next });
    }

    if messages > 0 && requested < first_sequence {
        return Err(JetStreamError::ResumeExpired {
            requested,
            earliest: first_sequence,
            latest: last_sequence,
        });
    }

    // An empty stream that has assigned sequences before was purged or aged
    // out completely. Only its current tail (`last + 1`) is resumable.
    if messages == 0 && last_sequence > 0 && requested < next {
        return Err(JetStreamError::ResumeExpired {
            requested,
            earliest: next,
            latest: last_sequence,
        });
    }
    Ok(())
}

/// Build the canonical consumer name for a client unit.
///
/// `id` is the per-connection id. `role` ("ws" / "graphql") scopes the
/// reaper: two roles
/// running in the same process (and thus sharing a `pod_id`, e.g. when
/// both `VS_*_POD_ID` env vars are set to the pod name) must NOT reap
/// each other's consumers. The role lives inside the reaper's marker so
/// each role's reaper only ever considers its own consumers, regardless
/// of pod_id collisions. JetStream consumer names cannot contain `.`,
/// so the segments are `-`-joined. The reaper matches on
/// [`pod_owned_marker`], which is a substring of this name.
#[must_use]
pub fn consumer_name(role: &str, pod_id: &str, tenant: &str, id: &str) -> String {
    format!("vs-t-{tenant}-p-{pod_id}-{role}-c-{id}")
}

/// The substring the reaper uses to recognise consumers owned by this
/// role+pod: `-p-{pod_id}-{role}-c-`. Sits between the tenant and the id
/// segments of [`consumer_name`]. Including `role` is what keeps a
/// `ws` reaper from deleting `graphql` consumers (and vice-versa) when
/// the two roles share a `pod_id`.
#[must_use]
pub fn pod_owned_marker(role: &str, pod_id: &str) -> String {
    format!("-p-{pod_id}-{role}-c-")
}

/// Parameters for [`create_consumer`].
#[derive(Debug, Clone)]
pub struct ConsumerParams<'a> {
    /// Stream to attach the consumer to. Must already exist.
    pub stream_name: &'a str,
    /// This pod's id (embedded in the consumer name for the reaper).
    pub pod_id: &'a str,
    /// This role's name ("ws" / "graphql"), embedded in the reaper
    /// marker so each role only reaps its own consumers even when the
    /// roles share a `pod_id`.
    pub role: &'a str,
    /// Tenant scope (embedded in the consumer name).
    pub tenant: &'a str,
    /// Per-unit id — connection id (WS) or subscription id (GraphQL).
    pub id: &'a str,
    /// Subject filter. Native WS and GraphQL connection consumers use the
    /// tenant-wide `vs.t.{tenant}.>` pattern and match operations in-process.
    pub filter_subject: String,
    /// Server-side auto-delete window (cleanup layer 2).
    pub inactive_threshold: Duration,
    /// Resume cursor. `Some(seq)` → deliver starting at this stream
    /// sequence (a client reconnecting to replay the gap it missed);
    /// `None` → `New` (live-only, first connect). The stream is the
    /// durable log and the client supplies its last-processed sequence, so
    /// any pod can serve the resume with **no cross-pod consumer
    /// ownership** — each connection still gets its own fresh consumer.
    pub start_seq: Option<u64>,
}

/// The fixed delivery semantics every bespoke consumer shares:
/// `New` (live-only, no history replay), **cumulative** ack, bounded
/// redelivery.
///
/// `AckPolicy::All` (vs per-message `Explicit`) lets the pumps ack one
/// message and have it cover every earlier message on the consumer —
/// so a pump draining a burst acks once per batch instead of once per
/// message, cutting ack round-trips to the broker by the batch factor.
/// Pumps MUST still ack promptly (every K messages and on a short flush
/// timer) so the tail can't sit unacked past `ack_wait` and redeliver.
fn pull_config(name: String, params: &ConsumerParams<'_>) -> PullConfig {
    PullConfig {
        durable_name: Some(name),
        filter_subject: params.filter_subject.clone(),
        // Resume from the client's cursor when given; otherwise live-only.
        deliver_policy: match params.start_seq {
            Some(seq) => DeliverPolicy::ByStartSequence {
                start_sequence: seq,
            },
            None => DeliverPolicy::New,
        },
        ack_policy: AckPolicy::All,
        max_deliver: 5,
        ack_wait: Duration::from_secs(30),
        inactive_threshold: params.inactive_threshold,
        ..Default::default()
    }
}

/// Create a bespoke pull consumer and return its [`ConsumerHandle`]
/// (whose `Drop` schedules deletion) and the [`PullConsumer`] the
/// caller pulls from.
///
/// When `active` is `Some`, the unit's `id` is inserted into that set
/// on creation and removed by the handle's `Drop` — this is how the
/// GraphQL gateway feeds the reaper its live set. The native WS
/// gateway passes `None` because its [`ConnectionRegistry`] already
/// *is* the live set.
///
/// [`ConnectionRegistry`]: # "in ventstream-ws"
pub async fn create_consumer(
    js: &JsContext,
    params: ConsumerParams<'_>,
    active: Option<ActiveSet>,
) -> Result<(ConsumerHandle, PullConsumer), JetStreamError> {
    let name = consumer_name(params.role, params.pod_id, params.tenant, params.id);
    // Register before the first broker await. Otherwise the reaper can list a
    // newly created consumer in the small window between create_consumer()
    // succeeding and the active-set insert, classify it as orphaned, and
    // delete a live session.
    let reservation = ActiveReservation::new(active, params.id).await;

    let mut stream =
        js.get_stream(params.stream_name)
            .await
            .map_err(|err| JetStreamError::Stream {
                stream: params.stream_name.to_owned(),
                detail: err.to_string(),
            })?;

    if let Some(requested) = params.start_seq {
        let info = stream.info().await.map_err(|err| JetStreamError::Stream {
            stream: params.stream_name.to_owned(),
            detail: format!("reading stream state for resume validation: {err}"),
        })?;
        validate_resume_window(
            requested,
            info.state.messages,
            info.state.first_sequence,
            info.state.last_sequence,
        )?;
    }

    let consumer: PullConsumer = stream
        .create_consumer(pull_config(name.clone(), &params))
        .await
        .map_err(|err| JetStreamError::Create {
            name: name.clone(),
            detail: err.to_string(),
        })?;

    let handle = ConsumerHandle {
        js: js.clone(),
        stream_name: params.stream_name.to_owned(),
        consumer_name: name,
        id: params.id.to_owned(),
        active: reservation.commit(),
        pending: Arc::new(Mutex::new(true)),
    };
    Ok((handle, consumer))
}

/// Provisional reaper registration held across asynchronous consumer setup.
///
/// Setup failures remove the id automatically. Successful setup transfers the
/// active-set handle to [`ConsumerHandle`], which owns removal for the live
/// session.
struct ActiveReservation {
    active: Option<ActiveSet>,
    id: String,
}

impl ActiveReservation {
    async fn new(active: Option<ActiveSet>, id: &str) -> Self {
        if let Some(set) = &active {
            set.reserve(id.to_owned()).await;
        }
        Self {
            active,
            id: id.to_owned(),
        }
    }

    fn commit(mut self) -> Option<ActiveSet> {
        self.active.take()
    }
}

impl Drop for ActiveReservation {
    fn drop(&mut self) {
        if let Some(set) = &self.active {
            set.remove(&self.id);
        }
    }
}

/// RAII handle to a bespoke consumer. Created by [`create_consumer`];
/// dropped on every owner-task exit path. `Drop` is sync while the
/// JetStream delete is async, so it hands the delete to a detached
/// task — if the runtime is gone the reaper / `inactive_threshold`
/// still reclaim it.
pub struct ConsumerHandle {
    js: JsContext,
    stream_name: String,
    consumer_name: String,
    id: String,
    active: Option<ActiveSet>,
    /// Guards against a double-delete if the handle is ever cloned via
    /// shared state; also lets tests disable cleanup.
    pending: Arc<Mutex<bool>>,
}

impl ConsumerHandle {
    /// The consumer's durable name (for logging / diagnostics).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.consumer_name
    }
}

impl Drop for ConsumerHandle {
    fn drop(&mut self) {
        let was_pending = {
            let mut g = self.pending.lock();
            let was = *g;
            *g = false;
            was
        };
        if !was_pending {
            return;
        }
        // Remove from the live set first so a reaper sweep that races
        // the async delete still treats this id as gone.
        if let Some(set) = &self.active {
            set.remove(&self.id);
        }
        let js = self.js.clone();
        let stream = self.stream_name.clone();
        let consumer = self.consumer_name.clone();
        tokio::spawn(async move {
            match js.get_stream(&stream).await {
                Ok(s) => {
                    if let Err(err) = s.delete_consumer(&consumer).await {
                        debug!(
                            consumer = %consumer,
                            error = %err,
                            "delete_consumer failed (falling back to inactive_threshold / reaper)"
                        );
                    } else {
                        debug!(consumer = %consumer, "consumer deleted on drop");
                    }
                }
                Err(err) => {
                    debug!(
                        stream = %stream,
                        error = %err,
                        "could not load stream to delete consumer (falling back to inactive_threshold / reaper)"
                    );
                }
            }
        });
    }
}

/// Configuration for the broker-neutral JetStream adapter.
#[derive(Debug, Clone)]
pub struct JetStreamBrokerConfig {
    /// Existing JetStream stream consumed by realtime sessions.
    pub stream_name: String,
    /// Stable pod identifier embedded in disposable consumer names.
    pub pod_id: String,
    /// Server-side cleanup threshold for abandoned consumers.
    pub consumer_inactive_threshold: Duration,
}

/// JetStream implementation of the broker-neutral realtime contract.
///
/// Each client connection receives a fresh disposable pull consumer. The
/// retained stream, rather than a durable consumer name, owns replay state;
/// this permits reconnecting through any gateway pod.
#[derive(Clone)]
pub struct JetStreamBroker {
    js: JsContext,
    config: JetStreamBrokerConfig,
    active: ActiveSet,
}

impl JetStreamBroker {
    /// Construct an adapter around an established JetStream context.
    #[must_use]
    pub fn new(js: JsContext, config: JetStreamBrokerConfig) -> Self {
        Self {
            js,
            config,
            active: ActiveSet::new(),
        }
    }

    /// Shared live-consumer set suitable for [`run_reaper`].
    #[must_use]
    pub fn active_set(&self) -> ActiveSet {
        self.active.clone()
    }
}

#[async_trait]
impl RealtimeBroker for JetStreamBroker {
    fn kind(&self) -> BrokerKind {
        BrokerKind::NatsJetStream
    }

    async fn ready(&self) -> Result<(), BrokerError> {
        self.js
            .get_stream(&self.config.stream_name)
            .await
            .map(|_| ())
            .map_err(|err| {
                BrokerError::Unavailable(format!(
                    "JetStream stream '{}' is unavailable: {err}",
                    self.config.stream_name
                ))
            })
    }

    async fn open_session(
        &self,
        request: SessionRequest,
    ) -> Result<Box<dyn EventSession>, BrokerError> {
        let tenant_prefix = format!("vs.t.{}.", request.tenant);
        if let Some(pattern) = request.subject_filter.as_ref() {
            if !pattern.as_str().starts_with(&tenant_prefix) {
                return Err(BrokerError::Configuration(
                    "session subject filter is outside the authorized tenant".to_owned(),
                ));
            }
        }
        let start_seq = request
            .resume_after
            .as_ref()
            .map(jetstream_start_sequence)
            .transpose()?;
        let params = ConsumerParams {
            stream_name: &self.config.stream_name,
            pod_id: &self.config.pod_id,
            role: request.role.as_str(),
            tenant: &request.tenant,
            id: &request.connection_id,
            filter_subject: request.subject_filter.as_ref().map_or_else(
                || format!("vs.t.{}.>", request.tenant),
                |pattern| pattern.as_str().to_owned(),
            ),
            inactive_threshold: self.config.consumer_inactive_threshold,
            start_seq,
        };
        let resume = request.resume_after.clone();
        let (handle, consumer) = create_consumer(&self.js, params, Some(self.active.clone()))
            .await
            .map_err(|err| map_consumer_error(err, resume.as_ref()))?;
        Ok(Box::new(JetStreamSession::new(
            handle,
            consumer,
            request.role,
        )))
    }
}

fn jetstream_start_sequence(cursor: &Cursor) -> Result<u64, BrokerError> {
    if cursor.provider() != CursorProvider::NatsJetStream {
        return Err(BrokerError::CursorProviderMismatch {
            actual: cursor.provider(),
            expected: CursorProvider::NatsJetStream,
        });
    }
    cursor
        .value()
        .parse::<u64>()
        .map(|sequence| sequence.saturating_add(1))
        .map_err(|_| {
            BrokerError::InvalidCursor(
                "JetStream cursor must contain an unsigned decimal sequence".to_owned(),
            )
        })
}

fn map_consumer_error(error: JetStreamError, resume: Option<&Cursor>) -> BrokerError {
    match error {
        JetStreamError::ResumeExpired {
            requested,
            earliest,
            ..
        } => BrokerError::ResumeExpired {
            requested: resume
                .cloned()
                .unwrap_or_else(|| Cursor::jetstream(requested.saturating_sub(1))),
            earliest: Cursor::jetstream(earliest),
        },
        JetStreamError::ResumeAhead { requested, next } => BrokerError::CursorAhead {
            requested: resume
                .cloned()
                .unwrap_or_else(|| Cursor::jetstream(requested.saturating_sub(1))),
            latest: Cursor::jetstream(next.saturating_sub(1)),
        },
        JetStreamError::Stream { detail, .. } | JetStreamError::Create { detail, .. } => {
            BrokerError::Unavailable(detail)
        }
    }
}

const SESSION_ACK_EVERY: u32 = 64;
const SESSION_MAX_REOPEN: u32 = 6;
const SESSION_MAX_RECOVERY: Duration = Duration::from_secs(40);

struct JetStreamSession {
    _handle: ConsumerHandle,
    consumer: PullConsumer,
    stream: Option<PullStream>,
    delivered: Option<(Cursor, async_nats::jetstream::Message)>,
    pending: Option<async_nats::jetstream::Message>,
    since_ack: u32,
    last_ack: tokio::time::Instant,
    ack_flush: Duration,
    reopen_attempts: u32,
    recovery_started: Option<tokio::time::Instant>,
    transport: &'static str,
}

impl JetStreamSession {
    fn new(handle: ConsumerHandle, consumer: PullConsumer, role: GatewayRole) -> Self {
        // Preserve the existing role-specific quiet-tail behavior while both
        // gateways migrate to this shared adapter.
        let ack_flush = match role {
            GatewayRole::WebSocket => Duration::from_millis(100),
            GatewayRole::GraphQl => Duration::from_secs(10),
        };
        Self {
            _handle: handle,
            consumer,
            stream: None,
            delivered: None,
            pending: None,
            since_ack: 0,
            last_ack: tokio::time::Instant::now(),
            ack_flush,
            reopen_attempts: 0,
            recovery_started: None,
            transport: role.as_str(),
        }
    }

    async fn open_pull_stream(&self) -> Result<PullStream, BrokerError> {
        self.consumer
            .stream()
            .max_messages_per_batch(64)
            .expires(Duration::from_secs(30))
            .heartbeat(Duration::from_secs(10))
            .messages()
            .await
            .map_err(|err| {
                BrokerError::Unavailable(format!("opening JetStream pull stream: {err}"))
            })
    }

    async fn flush_pending(&mut self) -> bool {
        let Some(message) = self.pending.as_ref() else {
            return true;
        };
        match message.ack().await {
            Ok(()) => {
                self.pending.take();
                self.since_ack = 0;
                self.last_ack = tokio::time::Instant::now();
                true
            }
            Err(err) => {
                metrics::counter!(
                    "vs_realtime_ack_failures_total",
                    "transport" => self.transport,
                    "provider" => BrokerKind::NatsJetStream.as_str()
                )
                .increment(1);
                warn!(error = %err, "JetStream session acknowledgement failed; retaining delivery for retry");
                false
            }
        }
    }

    fn recovery_exhausted(&self) -> bool {
        self.reopen_attempts > SESSION_MAX_REOPEN
            || self
                .recovery_started
                .is_some_and(|started| started.elapsed() >= SESSION_MAX_RECOVERY)
    }

    async fn recovery_backoff(&self) {
        if self.reopen_attempts == 0 {
            return;
        }
        let millis = 200u64.saturating_mul(1u64 << self.reopen_attempts.min(5));
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }

    fn record_stream_failure(&mut self, reason: &'static str) {
        self.stream = None;
        self.reopen_attempts = self.reopen_attempts.saturating_add(1);
        self.recovery_started
            .get_or_insert_with(tokio::time::Instant::now);
        metrics::counter!(
            "vs_realtime_pull_restarts_total",
            "transport" => self.transport,
            "provider" => BrokerKind::NatsJetStream.as_str(),
            "reason" => reason
        )
        .increment(1);
    }
}

#[async_trait]
impl EventSession for JetStreamSession {
    async fn next(&mut self) -> Result<Option<BrokerEvent>, BrokerError> {
        if self.delivered.is_some() {
            return Err(BrokerError::SessionClosed(
                "JetStream session next() called before the prior delivery was accepted".to_owned(),
            ));
        }

        loop {
            if self.recovery_exhausted() {
                return Err(BrokerError::SessionClosed(
                    "JetStream pull stream could not recover".to_owned(),
                ));
            }
            if self.stream.is_none() {
                self.recovery_backoff().await;
                match self.open_pull_stream().await {
                    Ok(stream) => self.stream = Some(stream),
                    Err(err) => {
                        warn!(error = %err, attempt = self.reopen_attempts, "JetStream pull stream open failed; retrying");
                        self.record_stream_failure("open_error");
                        continue;
                    }
                }
            }

            let elapsed = self.last_ack.elapsed();
            let flush_after = self.ack_flush.saturating_sub(elapsed);
            let stream = self.stream.as_mut().ok_or_else(|| {
                BrokerError::SessionClosed("JetStream pull stream was not initialized".to_owned())
            })?;
            let polled = if self.pending.is_some() {
                tokio::select! {
                    message = stream.next() => Some(message),
                    () = tokio::time::sleep(flush_after) => None,
                }
            } else {
                Some(stream.next().await)
            };

            match polled {
                None => {
                    self.flush_pending().await;
                }
                Some(Some(Ok(message))) => {
                    let sequence = message
                        .info()
                        .map_err(|err| {
                            BrokerError::SessionClosed(format!(
                                "JetStream delivery has no stream sequence: {err}"
                            ))
                        })?
                        .stream_sequence;
                    let cursor = Cursor::jetstream(sequence);
                    let event = BrokerEvent {
                        subject: Arc::from(message.subject.to_string()),
                        payload: message.payload.clone(),
                        cursor: Some(cursor.clone()),
                    };
                    self.delivered = Some((cursor, message));
                    self.reopen_attempts = 0;
                    self.recovery_started = None;
                    return Ok(Some(event));
                }
                Some(Some(Err(err))) => {
                    self.flush_pending().await;
                    if matches!(err.kind(), MessagesErrorKind::ConsumerDeleted) {
                        return Err(BrokerError::SessionClosed(
                            "JetStream consumer was deleted".to_owned(),
                        ));
                    }
                    warn!(error = %err, "JetStream pull stream interrupted; reopening");
                    self.record_stream_failure("stream_error");
                }
                Some(None) => {
                    self.flush_pending().await;
                    self.record_stream_failure("stream_ended");
                }
            }
        }
    }

    async fn accepted(&mut self, event: &BrokerEvent) -> Result<(), BrokerError> {
        let Some(event_cursor) = event.cursor.as_ref() else {
            return Err(BrokerError::InvalidCursor(
                "JetStream delivery acceptance requires a cursor".to_owned(),
            ));
        };
        let Some((delivered_cursor, message)) = self.delivered.take() else {
            return Err(BrokerError::SessionClosed(
                "JetStream delivery was accepted more than once".to_owned(),
            ));
        };
        if event_cursor != &delivered_cursor {
            self.delivered = Some((delivered_cursor, message));
            return Err(BrokerError::InvalidCursor(
                "JetStream acceptance cursor does not match the pending delivery".to_owned(),
            ));
        }
        self.pending = Some(message);
        self.since_ack = self.since_ack.saturating_add(1);
        if self.since_ack >= SESSION_ACK_EVERY {
            self.flush_pending().await;
        }
        Ok(())
    }
}

/// A snapshot source of live consumer ids for the reaper.
///
/// The reaper extracts each consumer's id (the segment after `-c-`)
/// and deletes the consumer if that id is not in this set. Implement
/// it over whatever the gateway already tracks as "live"; the WS
/// gateway implements it directly on its connection registry, the
/// GraphQL gateway uses [`ActiveSet`].
pub trait ActiveConsumers: Send + Sync {
    /// The set of currently-live unit ids (bare connection/subscription
    /// ids, matching the `id` passed to [`create_consumer`]).
    fn snapshot(&self) -> HashSet<String>;

    /// Whether one unit id is currently live.
    ///
    /// Implementations should override this when they can avoid allocating a
    /// complete snapshot for the reaper's destructive-boundary check.
    fn contains(&self, id: &str) -> bool {
        self.snapshot().contains(id)
    }
}

/// A simple shared set of live consumer ids, for gateways that don't
/// already track their units centrally (the GraphQL gateway). Cheap to
/// clone (`Arc`); inserts on consumer create and removes on handle
/// drop.
#[derive(Clone, Default)]
pub struct ActiveSet {
    inner: Arc<Mutex<HashSet<String>>>,
    deletion_gate: Arc<tokio::sync::Mutex<()>>,
}

impl ActiveSet {
    /// Create an empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark an id live.
    pub fn insert(&self, id: String) {
        self.inner.lock().insert(id);
    }

    async fn reserve(&self, id: String) {
        let _gate = self.deletion_gate.lock().await;
        self.insert(id);
    }

    async fn deletion_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.deletion_gate.lock().await
    }

    /// Mark an id gone.
    pub fn remove(&self, id: &str) {
        self.inner.lock().remove(id);
    }

    /// Current number of live ids (diagnostics / tests).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

impl ActiveConsumers for ActiveSet {
    fn snapshot(&self) -> HashSet<String> {
        self.inner.lock().clone()
    }

    fn contains(&self, id: &str) -> bool {
        self.inner.lock().contains(id)
    }
}

/// Run the orphan reaper until `shutdown` fires.
///
/// Every `interval`, lists the stream's consumers, keeps only those
/// owned by this role+pod ([`pod_owned_marker`]), and deletes any whose
/// id is not in `active`. This is cleanup layer 3 — the backstop behind
/// the Drop guard and `inactive_threshold`.
///
/// `role` ("ws" / "graphql") scopes the sweep: a reaper only ever
/// considers consumers created by its own role, so co-located roles
/// that share a `pod_id` cannot reap each other's live consumers.
#[allow(clippy::too_many_arguments)]
pub async fn run_reaper(
    js: JsContext,
    stream_name: String,
    role: String,
    pod_id: String,
    interval: Duration,
    active: ActiveSet,
    shutdown: ShutdownToken,
) {
    let marker = pod_owned_marker(&role, &pod_id);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        stream = %stream_name,
        pod_id = %pod_id,
        interval_ms = interval.as_millis() as u64,
        "consumer reaper started"
    );

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                info!("consumer reaper stopping");
                return;
            }
            _ = tick.tick() => {
                if let Err(err) = reap_once(&js, &stream_name, &marker, &active).await {
                    warn!(error = %err, "reaper sweep failed");
                }
            }
        }
    }
}

/// One reaper sweep. Returns the number of orphan consumers deleted.
async fn reap_once(
    js: &JsContext,
    stream_name: &str,
    marker: &str,
    active: &ActiveSet,
) -> Result<usize, String> {
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|err| format!("get_stream: {err}"))?;

    let live = active.snapshot();

    use futures_util::StreamExt;
    let mut names = stream.consumer_names();

    let mut considered = 0usize;
    let mut deleted = 0usize;
    while let Some(result) = names.next().await {
        let name = match result {
            Ok(n) => n,
            Err(_) => continue,
        };
        if !name.contains(marker) {
            continue;
        }
        considered += 1;
        let Some((_, id)) = name.rsplit_once("-c-") else {
            continue;
        };
        if live.contains(id) {
            continue;
        }
        // Serialize the final check and broker deletion with session
        // reservation. A reservation that wins the gate is visible here; a
        // deletion that wins completes before the new consumer is created.
        let _deletion_guard = active.deletion_guard().await;
        if active.contains(id) {
            continue;
        }
        match stream.delete_consumer(&name).await {
            Ok(_) => {
                deleted += 1;
                debug!(consumer = %name, "reaper deleted orphan");
            }
            Err(err) => {
                // Usually a race with inactive_threshold — harmless.
                debug!(consumer = %name, error = %err, "reaper delete failed");
            }
        }
    }
    if considered > 0 || deleted > 0 {
        debug!(considered, deleted, "reaper sweep done");
    }
    Ok(deleted)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn consumer_name_round_trips_through_marker() {
        let name = consumer_name("ws", "pod7", "acme", "01JCONN");
        assert_eq!(name, "vs-t-acme-p-pod7-ws-c-01JCONN");
        // The reaper's marker is a substring of every name from this role+pod.
        assert!(name.contains(&pod_owned_marker("ws", "pod7")));
        assert!(!name.contains(&pod_owned_marker("ws", "pod8")));
        // And the id is recoverable the way the reaper recovers it.
        let (_, id) = name.rsplit_once("-c-").expect("has -c-");
        assert_eq!(id, "01JCONN");
    }

    #[test]
    fn marker_does_not_match_other_pods() {
        // A consumer owned by pod8 must not be swept by pod7's reaper.
        let other = consumer_name("ws", "pod8", "acme", "x");
        assert!(!other.contains(&pod_owned_marker("ws", "pod7")));
    }

    #[test]
    fn marker_does_not_match_other_roles_on_same_pod() {
        // Regression: co-located ws+graphql roles share a pod_id (e.g. both
        // VS_*_POD_ID = pod name). The ws reaper must NOT match a graphql
        // consumer on the same pod, or it deletes live graphql consumers
        // (and vice-versa) — the cross-role reap that collapsed delivery.
        let pod = "vs-gw-abc123";
        let ws = consumer_name("ws", pod, "acme", "01JCONN");
        let gql = consumer_name("graphql", pod, "acme", "01JSUB");
        assert!(ws.contains(&pod_owned_marker("ws", pod)));
        assert!(gql.contains(&pod_owned_marker("graphql", pod)));
        // Cross-role: neither marker matches the other role's consumer.
        assert!(!ws.contains(&pod_owned_marker("graphql", pod)));
        assert!(!gql.contains(&pod_owned_marker("ws", pod)));
    }

    #[test]
    fn active_set_insert_remove_snapshot() {
        let set = ActiveSet::new();
        assert!(set.is_empty());
        set.insert("a".to_owned());
        set.insert("b".to_owned());
        assert_eq!(set.len(), 2);
        let snap = set.snapshot();
        assert!(snap.contains("a") && snap.contains("b"));
        set.remove("a");
        assert_eq!(set.len(), 1);
        assert!(!set.snapshot().contains("a"));
    }

    #[test]
    fn active_set_clone_shares_state() {
        let set = ActiveSet::new();
        let clone = set.clone();
        set.insert("shared".to_owned());
        // The clone sees the insert — both point at the same Arc.
        assert!(clone.snapshot().contains("shared"));
        clone.remove("shared");
        assert!(set.is_empty());
    }

    #[tokio::test]
    async fn active_reservation_registers_before_setup_and_rolls_back() {
        let set = ActiveSet::new();
        {
            let _reservation = ActiveReservation::new(Some(set.clone()), "creating").await;
            assert!(set.snapshot().contains("creating"));
        }
        assert!(!set.snapshot().contains("creating"));
    }

    #[tokio::test]
    async fn active_reservation_transfers_cleanup_to_consumer_handle() {
        let set = ActiveSet::new();
        let reservation = ActiveReservation::new(Some(set.clone()), "live").await;
        let committed = reservation.commit().expect("active set transferred");
        assert!(set.snapshot().contains("live"));
        committed.remove("live");
        assert!(set.is_empty());
    }

    #[test]
    fn broker_cursor_maps_last_processed_to_next_sequence() {
        assert_eq!(jetstream_start_sequence(&Cursor::jetstream(41)), Ok(42));
        assert_eq!(
            jetstream_start_sequence(&Cursor::jetstream(u64::MAX)),
            Ok(u64::MAX),
            "legacy saturating behavior remains backward compatible"
        );
    }

    #[test]
    fn broker_cursor_rejects_other_providers_and_invalid_values() {
        let redis = Cursor::redis_streams(1, 0);
        assert!(matches!(
            jetstream_start_sequence(&redis),
            Err(BrokerError::CursorProviderMismatch { .. })
        ));
        let malformed = Cursor::new(
            CursorProvider::NatsJetStream,
            Arc::<str>::from("not-a-sequence"),
        )
        .expect("opaque cursor accepts provider-owned syntax");
        assert!(matches!(
            jetstream_start_sequence(&malformed),
            Err(BrokerError::InvalidCursor(_))
        ));
    }

    #[test]
    fn resume_window_accepts_retained_and_current_tail() {
        assert!(validate_resume_window(40, 61, 40, 100).is_ok());
        assert!(validate_resume_window(75, 61, 40, 100).is_ok());
        assert!(validate_resume_window(101, 61, 40, 100).is_ok());
        assert!(validate_resume_window(1, 0, 0, 0).is_ok());
    }

    #[test]
    fn resume_window_rejects_expired_cursor() {
        assert!(matches!(
            validate_resume_window(39, 61, 40, 100),
            Err(JetStreamError::ResumeExpired {
                requested: 39,
                earliest: 40,
                latest: 100,
            })
        ));
        assert!(matches!(
            validate_resume_window(90, 0, 101, 100),
            Err(JetStreamError::ResumeExpired {
                requested: 90,
                earliest: 101,
                latest: 100,
            })
        ));
    }

    #[test]
    fn resume_window_rejects_cursor_ahead_of_stream() {
        assert!(matches!(
            validate_resume_window(102, 61, 40, 100),
            Err(JetStreamError::ResumeAhead {
                requested: 102,
                next: 101,
            })
        ));
    }

    /// The exact orphan-vs-live decision the reaper makes, in pure form.
    #[test]
    fn reaper_decision_keeps_live_deletes_orphans() {
        let marker = pod_owned_marker("ws", "podA");
        let live: HashSet<String> = ["conn1".to_owned()].into_iter().collect();
        let names = [
            consumer_name("ws", "podA", "acme", "conn1"), // live  -> keep
            consumer_name("ws", "podA", "acme", "conn2"), // orphan-> delete
            consumer_name("ws", "podB", "acme", "conn3"), // other pod -> skip
            consumer_name("graphql", "podA", "acme", "conn9"), // other role, same pod -> skip
        ];
        let mut would_delete = Vec::new();
        for name in &names {
            if !name.contains(&marker) {
                continue; // not ours
            }
            if let Some((_, id)) = name.rsplit_once("-c-") {
                if !live.contains(id) {
                    would_delete.push(id.to_owned());
                }
            }
        }
        assert_eq!(would_delete, vec!["conn2".to_owned()]);
    }
}
