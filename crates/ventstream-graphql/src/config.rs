//! Configuration for the GraphQL gateway.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use ventstream_redis::RedisStreamsConfig;

/// Configuration for the GraphQL subscription gateway.
#[derive(Debug, Clone)]
pub struct GraphQlConfig {
    /// HTTP+WS listener address. The HTTP path is `/graphql` and the
    /// WS upgrade endpoint is `/graphql/ws` — both standard for
    /// Apollo Client.
    pub listen: SocketAddr,

    /// NATS connection URL.
    pub nats_url: String,

    /// JetStream stream name. Must already exist; the gateway does
    /// not create the stream (the native WS gateway / engine owns
    /// stream bootstrap).
    pub stream_name: String,

    /// Identifier for this pod. Embedded in every consumer name
    /// (`vs-t-{tenant}-p-{pod_id}-c-{stream_id}`) so the reaper from
    /// the native WS path only sweeps its own consumers.
    pub pod_id: String,

    /// `inactive_threshold` set on every per-connection consumer.
    /// Safety net behind the stream-Drop cleanup.
    pub consumer_inactive_threshold: Duration,

    /// How often the orphan reaper sweeps this pod's consumers. The
    /// GraphQL role runs its own reaper (cleanup layer 3) — it does
    /// not depend on the `ws` role's reaper, whose `pod_id` differs.
    pub reaper_interval: Duration,

    /// Redis Streams provider configuration. When set, GraphQL subscriptions
    /// use Redis instead of JetStream. The NATS fields remain ignored.
    pub redis_streams: Option<RedisStreamsConfig>,

    /// Capacity of the per-connection in-process fan-out buffer. A
    /// subscription that falls further behind than this is terminated
    /// with a lag error (the client resubscribes/resumes). Larger =
    /// more tolerance for bursty/slow clients at higher per-connection
    /// memory; smaller = shed sooner. Clamped to at least 1.
    pub broadcast_capacity: usize,

    /// Path to the subject manifest YAML. Optional — when absent,
    /// `availableSubjects` returns an empty list and any pattern is
    /// permitted (subject to tenant scoping).
    pub manifest_path: Option<PathBuf>,

    /// Path to the **subscriptions** manifest YAML. When present, the
    /// gateway builds a dynamic schema with the declared typed
    /// subscription fields (Model 2 + Federation Flavor B). When
    /// absent, the static schema is used and only the generic
    /// `events(subject)` field is exposed.
    pub subscriptions_path: Option<PathBuf>,

    /// Path to a GraphQL **SDL** file declaring typed subscriptions via
    /// `@vsSubscribe` / `@source` directives. The recommended way to
    /// author typed subscriptions; takes precedence over
    /// `subscriptions_path` when both are set.
    pub schema_path: Option<PathBuf>,

    /// Serve the in-browser GraphiQL playground at `GET /graphiql`.
    /// Off by default — don't expose it in production.
    pub playground: bool,

    /// The single tenant this deployment serves (the "single tenant per
    /// deployment" isolation model). When `Some`, a client-asserted tenant
    /// MUST equal this value — otherwise it's rejected, which is what closes
    /// cross-tenant access (the server, not the client, decides the tenant).
    /// When `None`, tenant isolation is NOT enforced (dev / legacy); the
    /// binary logs a loud warning at startup. Set via `VS_TENANT`.
    pub expected_tenant: Option<String>,
}

impl Default for GraphQlConfig {
    fn default() -> Self {
        Self {
            listen: ([0, 0, 0, 0], 4041).into(),
            nats_url: "nats://127.0.0.1:4222".into(),
            stream_name: "ventstream".into(),
            pod_id: ulid::Ulid::new().to_string(),
            consumer_inactive_threshold: Duration::from_secs(300),
            reaper_interval: Duration::from_secs(60),
            redis_streams: None,
            broadcast_capacity: 1024,
            manifest_path: None,
            subscriptions_path: None,
            schema_path: None,
            playground: false,
            expected_tenant: None,
        }
    }
}

/// One row of the subject manifest. Returned (filtered to the
/// current tenant) by the `availableSubjects` query so clients can
/// discover what they can subscribe to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectDescriptor {
    /// NATS subject pattern in *unanchored* form (no `vs.t.{tenant}.`
    /// prefix). Clients pass this as the `subject` argument to the
    /// `events` subscription.
    pub pattern: String,

    /// Human-readable description shown in introspection / discovery
    /// UIs.
    #[serde(default)]
    pub description: Option<String>,

    /// Example `type` field value events on this pattern carry —
    /// helps clients work out the shape of `data` they'll receive.
    #[serde(default)]
    pub example_event_type: Option<String>,

    /// Optional per-tenant allowlist. When set, only the listed
    /// tenants see this descriptor and can subscribe to it. When
    /// unset, the descriptor is global (any tenant on the gateway).
    #[serde(default)]
    pub tenants: Option<Vec<String>>,
}

impl SubjectDescriptor {
    /// Whether this descriptor is visible to the given tenant.
    pub fn visible_to(&self, tenant: &str) -> bool {
        match &self.tenants {
            None => true,
            Some(list) => list.iter().any(|t| t == tenant),
        }
    }
}

/// On-disk shape of the manifest file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SubjectManifest {
    #[serde(default)]
    pub(crate) subjects: Vec<SubjectDescriptor>,
}
