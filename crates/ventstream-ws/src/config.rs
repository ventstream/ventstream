//! Configuration for the WebSocket gateway.

use std::net::SocketAddr;
use std::time::Duration;

use ventstream_redis::RedisStreamsConfig;

/// Configuration for the WebSocket fan-out gateway.
#[derive(Debug, Clone)]
pub struct WsConfig {
    /// HTTP+WS listener address.
    pub listen: SocketAddr,

    /// NATS connection URL (e.g. `nats://127.0.0.1:4222`).
    pub nats_url: String,

    /// NATS subject patterns this gateway subscribes to.
    ///
    /// Typical value: `["vs.t.>"]` — every tenant. For multi-tenant
    /// deployments where pods are sharded by tenant, set this to the
    /// per-pod tenant slice.
    pub bus_subjects: Vec<String>,

    /// Per-connection outbound mailbox capacity. When a client cannot
    /// drain at the rate the bus produces, the mailbox fills, the
    /// connection is closed, and the client must reconnect. This is
    /// the textbook "no slow client takes down the publisher" guard.
    pub per_connection_mailbox: usize,

    /// Ping interval — the server sends a WebSocket ping every tick.
    pub ping_interval: Duration,

    /// Pong timeout — if the client doesn't reply within this window,
    /// the connection is closed.
    pub pong_timeout: Duration,

    /// JetStream configuration. When `Some`, the gateway uses per-WS
    /// durable consumers backed by a JetStream stream and the
    /// orphan-cleanup machinery is wired on. When `None`, the
    /// gateway falls back to ephemeral NATS Core pub/sub (v1 demo
    /// path).
    pub jetstream: Option<JetStreamConfig>,

    /// Redis Streams configuration. When `Some`, the gateway opens
    /// provider-neutral durable sessions against Redis instead of NATS.
    /// This is mutually exclusive with `jetstream`.
    pub redis_streams: Option<RedisStreamsConfig>,

    /// Hard ceiling on established connections per pod. `None` =
    /// unlimited (the default — preserves existing behaviour). When set,
    /// a WS upgrade that would push the pod past this count is rejected
    /// at the HTTP handshake with `503 Service Unavailable` + a
    /// `Retry-After` header, *before* the expensive per-connection
    /// JetStream consumer + mailbox are allocated. This is the local,
    /// fast backstop against OOM under a connection surge; readiness
    /// gating + the HPA are the slower, global scale-out levers.
    ///
    /// Size it to the pod's memory limit: roughly
    /// `(mem_limit - base) / ~165 KiB-per-conn`.
    pub max_connections: Option<usize>,

    /// The single tenant this deployment serves (the "single tenant per
    /// deployment" isolation model). When `Some`, a client's Hello-asserted
    /// tenant MUST equal this value — otherwise the handshake is rejected.
    /// This is what closes cross-tenant access: the server, not the client,
    /// decides the tenant, so a client can't assert `victim` and stream its
    /// firehose. When `None`, tenant isolation is NOT enforced (dev / legacy);
    /// the binary logs a loud warning at startup. Set via `VS_TENANT`.
    pub expected_tenant: Option<String>,
}

/// Where JetStream keeps the stream's messages.
///
/// The live fan-out path consumes with `deliver_policy=New` (no replay),
/// so the stream is a short delivery buffer, not a durable log —
/// `Memory` is a perfectly safe, faster choice here, and a NATS restart
/// just means "new events from now," which is already the semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStorage {
    /// Disk-backed: survives a NATS restart. The default.
    File,
    /// RAM-backed: faster, bounded by `max_bytes`; cleared on a NATS
    /// restart (fine for live fan-out — there's no replay to lose).
    Memory,
}

/// JetStream stream + consumer configuration.
#[derive(Debug, Clone)]
pub struct JetStreamConfig {
    /// Stream name (e.g. `ventstream`). Created idempotently on
    /// startup if it doesn't exist; refused if it exists with a
    /// drifted config.
    pub stream_name: String,

    /// Subject patterns the stream captures. Should be a superset of
    /// every tenant's traffic — typically just `vs.t.>`.
    pub stream_subjects: Vec<String>,

    /// Storage backend for the stream. Defaults to `File`; set `Memory`
    /// for higher throughput (the buffer is self-bounding, see below).
    pub storage: StreamStorage,

    /// How long messages live in the stream. Because the gateways
    /// consume with `deliver_policy=New`, this is the *live buffer*
    /// window — how long an event sticks around for currently-connected
    /// consumers — NOT a replay horizon. Kept short by default so the
    /// stream self-trims and can't bloat. Eviction is `discard: old`.
    pub max_age: Duration,

    /// Max stream size in bytes — a hard ceiling JetStream enforces
    /// continuously by evicting the oldest messages (`discard: old`).
    /// This is what keeps the stream bounded with zero operator input;
    /// it is a *limit*, never a reservation.
    pub max_bytes: i64,

    /// Max message count, or `-1` for unlimited (bounded by age + bytes).
    pub max_msgs: i64,

    /// Replica count for the stream. 1 for dev/single-node; 3+ for
    /// production clusters.
    pub replicas: usize,

    /// `inactive_threshold` set on every per-connection consumer.
    /// JetStream auto-deletes a consumer if no client has pulled
    /// from it in this window. This is the safety net for kill -9.
    pub consumer_inactive_threshold: Duration,

    /// Identifier for this pod. Embedded in every consumer name as
    /// `vs.t.{tenant}.p.{pod_id}.c.{connection_id}` so the reaper
    /// only touches consumers it owns. Use the k8s pod name in
    /// production; a process-startup ULID is fine for dev.
    pub pod_id: String,

    /// How often the reaper task lists consumers under this pod's
    /// namespace and deletes any that don't have a live in-process
    /// connection. Layer 3 of the cleanup hierarchy.
    pub reaper_interval: Duration,
}

impl Default for JetStreamConfig {
    fn default() -> Self {
        Self {
            stream_name: "ventstream".into(),
            stream_subjects: vec!["vs.t.>".into()],
            storage: StreamStorage::File,
            // Self-bounding live-buffer defaults: ~recent + undelivered.
            // No operator sizing required; can't bloat. Both are limits,
            // not reservations, so a Memory stream can't OOM NATS either.
            max_age: Duration::from_secs(600), // 10 minutes
            max_bytes: 512 * 1024 * 1024,      // 512 MiB
            max_msgs: -1,                      // unlimited (bounded by age + bytes)
            replicas: 1,
            consumer_inactive_threshold: Duration::from_secs(300),
            pod_id: ulid::Ulid::new().to_string(),
            reaper_interval: Duration::from_secs(60),
        }
    }
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            listen: ([0, 0, 0, 0], 4040).into(),
            nats_url: "nats://127.0.0.1:4222".into(),
            bus_subjects: vec!["vs.t.>".into()],
            per_connection_mailbox: 256,
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            jetstream: None,
            redis_streams: None,
            max_connections: None,
            expected_tenant: None,
        }
    }
}
