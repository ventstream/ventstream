//! GraphQL schema: types + Query / Subscription roots.
//!
//! Schema deliberately narrow: a single subscription field
//! (`events`), a discovery query (`availableSubjects`), a liveness
//! probe (`health`). Apollo Client and any other graphql-transport-ws
//! client introspects this and sees exactly what's available; field
//! selection works through async-graphql's normal projection.

use std::sync::Arc;

use async_graphql::{
    types::Json, Context, EmptyMutation, Error as GqlError, Object, Result as GqlResult, Schema,
    SimpleObject, Subscription, ID,
};
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use tracing::warn;
use ventstream_protocol::{Actor as ProtoActor, SubjectPattern};
use ventstream_realtime::{Cursor, RealtimeBroker};

use crate::auth::Tenant;
use crate::config::SubjectDescriptor as ConfigSubjectDescriptor;
use crate::conn_source::{connection_stream, resolve_resume_cursor, ConnSourceCell, ResumeCursor};

/// The full schema type, fully built. Stored in axum app state and
/// handed to async-graphql's HTTP and WS handlers.
pub(crate) type AppSchema = Schema<QueryRoot, EmptyMutation, SubscriptionRoot>;

/// What the resolvers need to do their job. Inserted into the
/// schema's data on build; resolvers read via `ctx.data::<...>()?`.
#[derive(Clone)]
pub(crate) struct GraphContext {
    /// Provider-neutral broker used to open shared live or isolated replay
    /// sessions.
    pub(crate) broker: Arc<dyn RealtimeBroker>,
    /// Capacity of each connection's in-process fan-out broadcast buffer.
    pub(crate) broadcast_capacity: usize,
    /// Loaded subject manifest. The `availableSubjects` query filters
    /// this per-tenant.
    pub(crate) manifest: Arc<Vec<ConfigSubjectDescriptor>>,
}

// =====================================================================
// Types (mirrors of the protocol envelope, exposed as GraphQL types).
// =====================================================================

/// Who or what caused the event.
#[derive(SimpleObject, Clone, Serialize)]
pub(crate) struct Actor {
    /// Actor type (`user`, `system`, `service`, …).
    pub kind: String,
    /// Actor identifier.
    pub id: String,
}

impl From<ProtoActor> for Actor {
    fn from(p: ProtoActor) -> Self {
        Self {
            kind: p.kind,
            id: p.id,
        }
    }
}

/// Tracing/correlation hints. All fields optional.
#[derive(SimpleObject, Clone, Default, Serialize)]
pub(crate) struct Metadata {
    /// OpenTelemetry trace id.
    pub trace_id: Option<String>,
    /// Application correlation id.
    pub correlation_id: Option<String>,
    /// Causal-parent event id.
    pub causation_id: Option<String>,
}

/// The event delivered to subscribers — the on-the-wire envelope
/// plus the routing subject. `data` is opaque JSON; clients are
/// responsible for casting it to their expected type.
#[derive(SimpleObject, Clone, Serialize)]
#[graphql(name = "Event")]
pub(crate) struct SubscriptionEvent {
    /// Unique event id (ULID).
    pub id: ID,
    /// The event name — e.g. `deskMutated` or `orders.order.statusChanged`.
    pub event: String,
    /// Tenant scope.
    pub tenant: String,
    /// The instance id this event is about (the routing subject's last
    /// segment).
    pub entity_id: String,
    /// Principal that caused it, if any.
    pub actor: Option<Actor>,
    /// When the business event happened.
    pub occurred_at: DateTime<Utc>,
    /// When the SDK shipped it.
    pub received_at: DateTime<Utc>,
    /// Envelope schema version.
    pub schema_version: i32,
    /// Developer-defined payload, opaque to the gateway.
    pub data: Json<serde_json::Value>,
    /// Tracing hints.
    pub metadata: Metadata,
    /// The canonical broker subject this event arrived on (anchored form).
    pub subject: String,
    /// Compatibility alias for `cursor`. Existing JetStream clients may pass
    /// it back as `resume_from_seq`; new clients should use `cursor` and
    /// `resume_from_cursor`.
    pub seq: String,
    /// Provider-neutral resume cursor. Persist it only after processing the
    /// event successfully, then return it as `resume_from_cursor` during
    /// `connection_init`. Cursors are opaque strings and must not be ordered
    /// or modified by clients.
    pub cursor: String,
}

impl SubscriptionEvent {
    pub(crate) fn from_protocol(
        ev: ventstream_protocol::Event,
        subject: String,
        cursor: &Cursor,
    ) -> Self {
        let cursor = cursor.to_compatible_wire();
        Self {
            id: ID(ev.id.to_string()),
            event: ev.event,
            tenant: ev.tenant,
            entity_id: ev.entity_id,
            actor: ev.actor.map(Into::into),
            occurred_at: ev.occurred_at,
            received_at: ev.received_at,
            schema_version: ev.schema_version as i32,
            data: Json(ev.data),
            metadata: Metadata {
                trace_id: ev.metadata.trace_id,
                correlation_id: ev.metadata.correlation_id,
                causation_id: ev.metadata.causation_id,
            },
            subject,
            seq: cursor.clone(),
            cursor,
        }
    }
}

/// Liveness payload — also echoes the tenant the connection
/// resolved to, which is useful for debugging auth on the client.
#[derive(SimpleObject)]
pub(crate) struct Health {
    /// Always `"ok"` if the gateway is reachable and tenant
    /// resolved.
    pub status: String,
    /// The tenant the current connection is scoped to.
    pub tenant: String,
}

/// One row of the manifest, exposed as a GraphQL type.
#[derive(SimpleObject)]
pub(crate) struct SubjectDescriptor {
    /// Subject pattern in unanchored form.
    pub pattern: String,
    /// Human description.
    pub description: Option<String>,
    /// Example `type` value events on this pattern carry.
    pub example_event_type: Option<String>,
}

// =====================================================================
// Query root
// =====================================================================

/// Top-level query type. Named `Query` in the GraphQL schema
/// (federation convention) so it merges with other subgraphs'
/// Query types in a composed supergraph.
pub(crate) struct QueryRoot;

#[Object(name = "Query")]
impl QueryRoot {
    /// Liveness + tenant echo. Hit this from your client startup to
    /// verify auth before opening subscriptions.
    async fn health(&self, ctx: &Context<'_>) -> GqlResult<Health> {
        let tenant = ctx
            .data::<Tenant>()
            .map_err(|_| GqlError::new("no tenant on context — did connection_init resolve?"))?;
        Ok(Health {
            status: "ok".into(),
            tenant: tenant.0.clone(),
        })
    }

    /// Subject patterns the current tenant is permitted to subscribe
    /// to. The manifest is backend-defined; an empty result means no
    /// patterns are declared (clients can still try patterns
    /// directly — the gateway's tenant gate enforces isolation).
    async fn available_subjects(&self, ctx: &Context<'_>) -> GqlResult<Vec<SubjectDescriptor>> {
        let tenant = ctx
            .data::<Tenant>()
            .map_err(|_| GqlError::new("no tenant on context"))?;
        let gctx = ctx
            .data::<GraphContext>()
            .map_err(|_| GqlError::new("no graph context"))?;
        let out: Vec<SubjectDescriptor> = gctx
            .manifest
            .iter()
            .filter(|d| d.visible_to(&tenant.0))
            .map(|d| SubjectDescriptor {
                pattern: d.pattern.clone(),
                description: d.description.clone(),
                example_event_type: d.example_event_type.clone(),
            })
            .collect();
        Ok(out)
    }
}

// =====================================================================
// Subscription root
// =====================================================================

/// Top-level subscription type. Named `Subscription` in the GraphQL
/// schema (federation convention).
pub(crate) struct SubscriptionRoot;

#[Subscription(name = "Subscription")]
impl SubscriptionRoot {
    /// Subscribe to events matching `subject`. Pattern is unanchored
    /// (no `vs.t.{tenant}.` prefix); the gateway prepends the tenant
    /// resolved at connection_init time. NATS wildcards `*` and `>`
    /// are supported.
    ///
    /// Live-only operations on a GraphQL socket share one ordered broker
    /// session. Supplying `resumeFromCursor` gives this operation an isolated
    /// replay session, allowing independent cursor advancement on a
    /// multiplexed socket.
    async fn events(
        &self,
        ctx: &Context<'_>,
        subject: String,
        resume_from_cursor: Option<String>,
    ) -> impl Stream<Item = async_graphql::Result<SubscriptionEvent>> {
        // Validate inputs synchronously. The `#[Subscription]` macro
        // doesn't peel `Result<impl Stream>`, so setup errors are surfaced
        // by yielding a terminal `Err` into the stream — the client sees a
        // GraphQL error rather than a silent empty completion.
        let tenant = ctx.data::<Tenant>().ok().cloned();
        let gctx = ctx.data::<GraphContext>().ok().cloned();
        let cell = ctx.data::<ConnSourceCell>().ok().cloned();
        let connection_cursor = ctx
            .data::<ResumeCursor>()
            .ok()
            .and_then(|cursor| cursor.0.clone());
        let resume_from_cursor =
            resolve_resume_cursor(resume_from_cursor.as_deref(), connection_cursor);
        let anchored = match (&tenant, &gctx) {
            (Some(t), Some(_)) => SubjectPattern::anchored(&t.0, &subject)
                .map_err(|err| format!("invalid subject pattern: {err}")),
            (None, _) => Err("connection context missing tenant".into()),
            (_, None) => Err("connection context missing graph context".into()),
        };

        async_stream::stream! {
            let pattern = match anchored {
                Ok(p) => p,
                Err(err) => {
                    warn!(subject = %subject, error = %err, "subscription setup rejected");
                    yield Err(async_graphql::Error::new(err));
                    return;
                }
            };
            let resume_from_cursor = match resume_from_cursor {
                Ok(cursor) => cursor,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };
            let (Some(tenant), Some(gctx), Some(cell)) = (tenant, gctx, cell) else {
                yield Err(async_graphql::Error::new("connection context unavailable"));
                return;
            };

            // Live-only operations share the connection source. Resumed
            // operations use a subject-filtered source. A terminal `Err`
            // means the operation lagged; pass it through.
            let stream = match connection_stream(cell, gctx, tenant.0, pattern, resume_from_cursor).await {
                Ok(s) => s,
                Err(err) => {
                    warn!(subject = %subject, error = %err.message, "connection source init failed");
                    yield Err(err);
                    return;
                }
            };
            let mut stream = Box::pin(stream);
            while let Some(item) = stream.next().await {
                yield item;
            }
        }
    }
}

/// Build the schema. The context (broker / manifest) is injected here
/// so resolvers can read it from `ctx.data::<GraphContext>()`.
pub(crate) fn build_schema(graph_ctx: GraphContext) -> AppSchema {
    Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
        .data(graph_ctx)
        .finish()
}
