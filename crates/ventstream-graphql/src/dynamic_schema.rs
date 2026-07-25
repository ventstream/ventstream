//! Build an `async_graphql::dynamic::Schema` from a manifest.
//!
//! Used when the developer supplies a subscription manifest
//! (`VS_GRAPHQL_SUBSCRIPTIONS`). The static schema in `schema.rs`
//! is still the default when no manifest is configured.
//!
//! Layered choices for federation:
//!
//! - **Inline return types** are registered as full GraphQL types,
//!   resolvers read the event payload via the source expressions
//!   from the manifest, and the gateway emits the entire response
//!   shape. No federation cooperation required.
//!
//! - **Entity-ref return types** emit only the `@key` fields of an
//!   external type. The router stitches the rest from the
//!   entity-owning subgraph. We register a stub type with only the
//!   key fields and mark it as a federated entity in the SDL.
//!
//! - The standard `events(subject)` subscription is *always* added,
//!   so non-federated clients can still use the generic firehose.
//!
//! Federation directives (`@key`, `_service { sdl }`,
//! `_entities`) are emitted via async-graphql's
//! `SchemaBuilder::enable_federation()`.

use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, Scalar, Schema, SchemaError, Subscription,
    SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use futures_util::StreamExt;
use serde_json::Value as JsonValue;
use tracing::warn;
use ventstream_protocol::SubjectPattern;

use crate::auth::Tenant;
use crate::config::SubjectDescriptor as ConfigSubjectDescriptor;
use crate::conn_source::{
    connection_stream, resolve_resume_cursor, ConnSourceCell, ResumeCursor, RESUME_CURSOR_ARGUMENT,
};
use crate::manifest::{InlineFieldDef, Manifest, ReturnTypeDef, SubscriptionDef};
use crate::schema::GraphContext;
use crate::template::{expand_subject, resolve_source};

/// Build the dynamic schema from a manifest + graph context.
pub(crate) fn build(manifest: Manifest, graph_ctx: GraphContext) -> Result<Schema, SchemaError> {
    if let Some(subscription) = manifest.subscriptions.iter().find(|subscription| {
        subscription
            .args
            .iter()
            .any(|argument| argument.name == RESUME_CURSOR_ARGUMENT)
    }) {
        return Err(SchemaError(format!(
            "subscription '{}' declares reserved argument '{RESUME_CURSOR_ARGUMENT}'",
            subscription.name
        )));
    }
    let manifest = Arc::new(manifest);
    let graph_ctx = Arc::new(graph_ctx);

    // === Scalars ===========================================================
    let json_scalar = Scalar::new("JSON");
    let datetime_scalar = Scalar::new("DateTime");

    // === Common types (Actor, Metadata, Event, Health, SubjectDescriptor) ===
    let actor_type = Object::new("Actor")
        .field(Field::new(
            "kind",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "kind"),
        ))
        .field(Field::new(
            "id",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "id"),
        ));

    let metadata_type = Object::new("Metadata")
        .field(Field::new(
            "traceId",
            TypeRef::named(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "trace_id"),
        ))
        .field(Field::new(
            "correlationId",
            TypeRef::named(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "correlation_id"),
        ))
        .field(Field::new(
            "causationId",
            TypeRef::named(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "causation_id"),
        ));

    let event_type = Object::new("Event")
        .field(Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
            field_from_parent(ctx, "id")
        }))
        .field(Field::new(
            "event",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "event"),
        ))
        .field(Field::new(
            "tenant",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "tenant"),
        ))
        .field(Field::new(
            "entityId",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "entity_id"),
        ))
        .field(Field::new("actor", TypeRef::named("Actor"), |ctx| {
            field_object_from_parent(ctx, "actor")
        }))
        .field(Field::new(
            "occurredAt",
            TypeRef::named_nn("DateTime"),
            |ctx| field_from_parent(ctx, "occurred_at"),
        ))
        .field(Field::new(
            "receivedAt",
            TypeRef::named_nn("DateTime"),
            |ctx| field_from_parent(ctx, "received_at"),
        ))
        .field(Field::new(
            "schemaVersion",
            TypeRef::named_nn(TypeRef::INT),
            |ctx| field_from_parent(ctx, "schema_version"),
        ))
        .field(Field::new("data", TypeRef::named("JSON"), |ctx| {
            field_from_parent(ctx, "data")
        }))
        .field(Field::new(
            "metadata",
            TypeRef::named_nn("Metadata"),
            |ctx| field_object_from_parent(ctx, "metadata"),
        ))
        .field(Field::new(
            "subject",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "subject"),
        ))
        // Compatibility alias retained for existing JetStream clients.
        .field(Field::new("seq", TypeRef::named_nn(TypeRef::STRING), |ctx| {
            field_from_parent(ctx, "seq")
        }))
        .field(Field::new(
            "cursor",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "cursor"),
        ));

    let health_type = Object::new("Health")
        .field(Field::new(
            "status",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "status"),
        ))
        .field(Field::new(
            "tenant",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "tenant"),
        ));

    let subject_desc_type = Object::new("SubjectDescriptor")
        .field(Field::new(
            "pattern",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "pattern"),
        ))
        .field(Field::new(
            "description",
            TypeRef::named(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "description"),
        ))
        .field(Field::new(
            "exampleEventType",
            TypeRef::named(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "example_event_type"),
        ));

    // === Inline types declared in the manifest ============================
    let mut schema_types: Vec<Object> = Vec::new();
    for sub in &manifest.subscriptions {
        if let ReturnTypeDef::Inline { name, fields } = &sub.return_type {
            let obj = build_inline_type(name, fields);
            schema_types.push(obj);
        }
    }

    // === Entity-ref types: emit a stub Object with @key directive =========
    // Each unique entity_ref `type` gets one stub registration.
    let mut entity_stubs: HashMap<String, Object> = HashMap::new();
    for sub in &manifest.subscriptions {
        if let ReturnTypeDef::EntityRef { type_name, key } = &sub.return_type {
            let entry = entity_stubs.entry(type_name.clone()).or_insert_with(|| {
                let mut obj = Object::new(type_name);
                // Each key field is exposed as a scalar field on the stub.
                for k in key.keys() {
                    obj = obj.field(Field::new(k, TypeRef::named_nn(TypeRef::ID), {
                        let key_name = k.clone();
                        move |ctx| {
                            let k = key_name.clone();
                            field_from_parent(ctx, &k)
                        }
                    }));
                }
                // Mark as a federation entity by adding the @key
                // directive in the SDL via the `key` builder method.
                obj.key(key.keys().cloned().collect::<Vec<_>>().join(" "))
            });
            // Ignore — we just want it registered once.
            let _ = entry;
        }
    }

    // === Query root =======================================================
    // Federation convention: the root types are named `Query` and
    // `Subscription` (not the async-graphql default `QueryRoot`/
    // `SubscriptionRoot`) so they merge cleanly with other
    // subgraphs' equivalents in the composed supergraph.
    let query_root = Object::new("Query")
        .field(Field::new("health", TypeRef::named_nn("Health"), {
            let _ctx = Arc::clone(&graph_ctx);
            move |ctx| {
                FieldFuture::new(async move {
                    let tenant = ctx
                        .data::<Tenant>()
                        .map_err(|_| async_graphql::Error::new("no tenant on context"))?;
                    let mut obj = serde_json::Map::new();
                    obj.insert("status".into(), JsonValue::String("ok".into()));
                    obj.insert("tenant".into(), JsonValue::String(tenant.0.clone()));
                    Ok(Some(FieldValue::owned_any(JsonValue::Object(obj))))
                })
            }
        }))
        .field(Field::new(
            "availableSubjects",
            TypeRef::named_nn_list_nn("SubjectDescriptor"),
            {
                let ctx_outer = Arc::clone(&graph_ctx);
                move |ctx| {
                    let gctx = Arc::clone(&ctx_outer);
                    FieldFuture::new(async move {
                        let tenant = ctx
                            .data::<Tenant>()
                            .map_err(|_| async_graphql::Error::new("no tenant on context"))?;
                        let out: Vec<JsonValue> = gctx
                            .manifest
                            .iter()
                            .filter(|d| d.visible_to(&tenant.0))
                            .map(subject_desc_to_json)
                            .collect();
                        Ok(Some(FieldValue::list(
                            out.into_iter().map(FieldValue::owned_any),
                        )))
                    })
                }
            },
        ));

    // === Subscription root ================================================
    let mut subscription_root = Subscription::new("Subscription");

    // Generic events(subject) field — still here in dynamic mode.
    subscription_root = subscription_root.field(generic_events_field(Arc::clone(&graph_ctx)));

    // Add a field per manifest declaration.
    for sub in &manifest.subscriptions {
        let sub = Arc::new(sub.clone());
        let gctx_for_field = Arc::clone(&graph_ctx);
        let field = subscription_field_from_def(sub, gctx_for_field);
        subscription_root = subscription_root.field(field);
    }

    // === Build the schema =================================================
    let mut builder = Schema::build("Query", None, Some("Subscription"))
        .register(json_scalar)
        .register(datetime_scalar)
        .register(actor_type)
        .register(metadata_type)
        .register(event_type)
        .register(health_type)
        .register(subject_desc_type)
        .register(query_root)
        .register(subscription_root);
    for ty in schema_types {
        builder = builder.register(ty);
    }
    for (_, stub) in entity_stubs {
        builder = builder.register(stub);
    }

    // Federation v2 — emits `_service { sdl }` so routers introspect
    // us at compose time, and the @key directives on entity-ref
    // stubs above end up in the published SDL.
    builder = builder.enable_federation();
    builder.data((*graph_ctx).clone()).finish()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::needless_pass_by_value)]
fn field_from_parent<'a>(
    ctx: async_graphql::dynamic::ResolverContext<'a>,
    key: &str,
) -> FieldFuture<'a> {
    let key = key.to_owned();
    FieldFuture::new(async move {
        let json: &JsonValue = ctx
            .parent_value
            .downcast_ref::<JsonValue>()
            .ok_or_else(|| async_graphql::Error::new("parent value missing"))?;
        let v = match json {
            JsonValue::Object(map) => map.get(&key).cloned().unwrap_or(JsonValue::Null),
            _ => JsonValue::Null,
        };
        Ok(Some(json_to_field_value(v)))
    })
}

#[allow(clippy::needless_pass_by_value)]
fn field_object_from_parent<'a>(
    ctx: async_graphql::dynamic::ResolverContext<'a>,
    key: &str,
) -> FieldFuture<'a> {
    let key = key.to_owned();
    FieldFuture::new(async move {
        let json: &JsonValue = ctx
            .parent_value
            .downcast_ref::<JsonValue>()
            .ok_or_else(|| async_graphql::Error::new("parent value missing"))?;
        let v = match json {
            JsonValue::Object(map) => map.get(&key).cloned().unwrap_or(JsonValue::Null),
            _ => JsonValue::Null,
        };
        Ok(Some(FieldValue::owned_any(v)))
    })
}

fn json_to_field_value(v: JsonValue) -> FieldValue<'static> {
    match v {
        JsonValue::Null => FieldValue::NULL,
        other => FieldValue::value(json_to_gql_value(other)),
    }
}

fn json_to_gql_value(v: JsonValue) -> GqlValue {
    match v {
        JsonValue::Null => GqlValue::Null,
        JsonValue::Bool(b) => GqlValue::Boolean(b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                GqlValue::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::Number::from_f64(f)
                    .map(GqlValue::Number)
                    .unwrap_or(GqlValue::Null)
            } else {
                GqlValue::Null
            }
        }
        JsonValue::String(s) => GqlValue::String(s),
        JsonValue::Array(arr) => GqlValue::List(arr.into_iter().map(json_to_gql_value).collect()),
        JsonValue::Object(map) => GqlValue::Object(
            map.into_iter()
                .map(|(k, v)| (Name::new(k), json_to_gql_value(v)))
                .collect(),
        ),
    }
}

fn subject_desc_to_json(d: &ConfigSubjectDescriptor) -> JsonValue {
    let mut o = serde_json::Map::new();
    o.insert("pattern".into(), JsonValue::String(d.pattern.clone()));
    o.insert(
        "description".into(),
        d.description
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    o.insert(
        "example_event_type".into(),
        d.example_event_type
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    JsonValue::Object(o)
}

/// Build an Object for an inline return type.
fn build_inline_type(name: &str, fields: &[InlineFieldDef]) -> Object {
    let mut obj = Object::new(name);
    for f in fields {
        let key = f.name.clone();
        let type_ref = scalar_type_ref(&f.type_name, f.required);
        obj = obj.field(Field::new(&f.name, type_ref, move |ctx| {
            let key = key.clone();
            field_from_parent(ctx, &key)
        }));
    }
    // Auto-expose the provider-neutral cursor and legacy `seq` alias on every
    // typed subscription. Skip either field when the SDL author defines it.
    if !fields.iter().any(|f| f.name == "seq") {
        obj = obj.field(Field::new(
            "seq",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "seq"),
        ));
    }
    if !fields.iter().any(|f| f.name == "cursor") {
        obj = obj.field(Field::new(
            "cursor",
            TypeRef::named_nn(TypeRef::STRING),
            |ctx| field_from_parent(ctx, "cursor"),
        ));
    }
    obj
}

/// Map a manifest scalar-type name to a `TypeRef`.
fn scalar_type_ref(name: &str, required: bool) -> TypeRef {
    let base = match name {
        "ID" => TypeRef::named(TypeRef::ID),
        "String" => TypeRef::named(TypeRef::STRING),
        "Int" => TypeRef::named(TypeRef::INT),
        "Float" => TypeRef::named(TypeRef::FLOAT),
        "Boolean" => TypeRef::named(TypeRef::BOOLEAN),
        "DateTime" => TypeRef::named("DateTime"),
        "JSON" => TypeRef::named("JSON"),
        other => TypeRef::named(other),
    };
    if required {
        match base {
            TypeRef::Named(n) => TypeRef::NonNull(Box::new(TypeRef::Named(n))),
            other => other,
        }
    } else {
        base
    }
}

/// The generic `events(subject: String!): Event!` field — kept in
/// the dynamic schema for catch-all subscriptions.
fn generic_events_field(graph_ctx: Arc<GraphContext>) -> SubscriptionField {
    SubscriptionField::new("events", TypeRef::named_nn("Event"), move |ctx| {
        let gctx = Arc::clone(&graph_ctx);
        SubscriptionFieldFuture::new(async move {
            let tenant = ctx
                .data::<Tenant>()
                .map_err(|_| async_graphql::Error::new("no tenant on context"))?
                .clone();
            let subject: String = ctx.args.try_get("subject")?.string()?.to_owned();
            let anchored = SubjectPattern::anchored(&tenant.0, &subject)
                .map_err(|err| async_graphql::Error::new(format!("invalid pattern: {err}")))?;
            let connection_cursor = ctx
                .data::<ResumeCursor>()
                .ok()
                .and_then(|cursor| cursor.0.clone());
            let operation_cursor = ctx
                .args
                .try_get(RESUME_CURSOR_ARGUMENT)
                .ok()
                .and_then(|value| value.string().ok());
            let resume_from_cursor = resolve_resume_cursor(operation_cursor, connection_cursor)?;
            let cell = ctx
                .data::<ConnSourceCell>()
                .map_err(|_| async_graphql::Error::new("no connection source on context"))?;
            let cell = Arc::clone(cell);
            let stream = Box::pin(
                connection_stream(
                    cell,
                    (*gctx).clone(),
                    tenant.0.clone(),
                    anchored,
                    resume_from_cursor,
                )
                .await?,
            );
            Ok(stream.map(|item| {
                // A terminal `Err` (operation lagged, H14) propagates to the
                // client; otherwise convert the event into the JsonValue the
                // dynamic resolvers expect.
                item.map(|ev| {
                    let json = serde_json::to_value(&ev).unwrap_or(JsonValue::Null);
                    FieldValue::owned_any(json)
                })
            }))
        })
    })
    .argument(InputValue::new(
        "subject",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        RESUME_CURSOR_ARGUMENT,
        TypeRef::named(TypeRef::STRING),
    ))
}

fn subscription_field_from_def(
    def: Arc<SubscriptionDef>,
    graph_ctx: Arc<GraphContext>,
) -> SubscriptionField {
    let return_type_ref = match &def.return_type {
        ReturnTypeDef::Envelope => TypeRef::named_nn("Event"),
        ReturnTypeDef::Inline { name, .. } => TypeRef::named_nn(name.clone()),
        ReturnTypeDef::EntityRef { type_name, .. } => TypeRef::named_nn(type_name.clone()),
    };

    let def_for_args = Arc::clone(&def);
    let field_name = def.name.clone();

    let mut field = SubscriptionField::new(field_name, return_type_ref, move |ctx| {
        let def = Arc::clone(&def);
        let gctx = Arc::clone(&graph_ctx);
        SubscriptionFieldFuture::new(async move {
            let tenant = ctx
                .data::<Tenant>()
                .map_err(|_| async_graphql::Error::new("no tenant on context"))?
                .clone();

            // Collect args into a HashMap<String, String>.
            let mut args_map: HashMap<String, String> = HashMap::new();
            for arg in &def.args {
                let v = ctx.args.try_get(&arg.name).ok();
                let s = match v {
                    Some(av) => {
                        // ID / String → string; numeric → string repr.
                        match av.string() {
                            Ok(s) => s.to_owned(),
                            Err(_) => match av.i64() {
                                Ok(n) => n.to_string(),
                                Err(_) => match av.f64() {
                                    Ok(n) => n.to_string(),
                                    Err(_) => match av.boolean() {
                                        Ok(b) => b.to_string(),
                                        Err(_) => String::new(),
                                    },
                                },
                            },
                        }
                    }
                    None => String::new(),
                };
                args_map.insert(arg.name.clone(), s);
            }

            // Expand the subject template.
            let unanchored = expand_subject(&def.subject, &args_map)
                .map_err(|err| async_graphql::Error::new(format!("subject template: {err}")))?;
            let anchored = SubjectPattern::anchored(&tenant.0, &unanchored)
                .map_err(|err| async_graphql::Error::new(format!("pattern: {err}")))?;

            let connection_cursor = ctx
                .data::<ResumeCursor>()
                .ok()
                .and_then(|cursor| cursor.0.clone());
            let operation_cursor = ctx
                .args
                .try_get(RESUME_CURSOR_ARGUMENT)
                .ok()
                .and_then(|value| value.string().ok());
            let resume_from_cursor = resolve_resume_cursor(operation_cursor, connection_cursor)?;
            let cell = ctx
                .data::<ConnSourceCell>()
                .map_err(|_| async_graphql::Error::new("no connection source on context"))?;
            let cell = Arc::clone(cell);
            let stream = Box::pin(
                connection_stream(
                    cell,
                    (*gctx).clone(),
                    tenant.0.clone(),
                    anchored,
                    resume_from_cursor,
                )
                .await?,
            );

            let mapper = SubscriptionMapper {
                def,
                args: args_map,
            };
            Ok(stream.filter_map(move |item| {
                let mapper = mapper.clone();
                async move {
                    match item {
                        // Terminal lag error (H14) → surface to the client.
                        Err(err) => Some(Err(err)),
                        Ok(ev) => match mapper.build_response(&ev) {
                            Ok(json) => Some(Ok(FieldValue::owned_any(json))),
                            Err(err) => {
                                warn!(error = %err, "subscription response build failed");
                                None
                            }
                        },
                    }
                }
            }))
        })
    });

    for arg in &def_for_args.args {
        field = field.argument(InputValue::new(
            &arg.name,
            scalar_type_ref(&arg.type_name, arg.required),
        ));
    }
    field.argument(InputValue::new(
        RESUME_CURSOR_ARGUMENT,
        TypeRef::named(TypeRef::STRING),
    ))
}

/// Holds the data needed to project each event into the manifest's
/// declared response shape. Cloned per-event in the stream's
/// filter_map; cheap because everything is `Arc`/`String`.
#[derive(Clone)]
struct SubscriptionMapper {
    def: Arc<SubscriptionDef>,
    args: HashMap<String, String>,
}

impl SubscriptionMapper {
    fn build_response(&self, ev: &crate::schema::SubscriptionEvent) -> Result<JsonValue, String> {
        // Reconstitute a `ventstream_protocol::Event` from the
        // SubscriptionEvent so the template engine can read it.
        let protocol_ev = reverse_to_protocol(ev);

        match &self.def.return_type {
            ReturnTypeDef::Envelope => {
                // Return the whole event envelope as the `Event` type.
                // SubscriptionEvent serializes to exactly the keys the
                // `Event` object's field resolvers read.
                serde_json::to_value(ev).map_err(|e| e.to_string())
            }
            ReturnTypeDef::Inline { name, fields } => {
                let mut out = serde_json::Map::new();
                for f in fields {
                    let v = resolve_source(&f.source, &self.args, &protocol_ev, &ev.subject)
                        .map_err(|e| e.to_string())?;
                    out.insert(f.name.clone(), v);
                }
                // Match the auto-exposed compatibility `seq` field added in
                // `build_inline_type` unless the SDL already mapped its own.
                if !fields.iter().any(|f| f.name == "seq") {
                    out.insert("seq".into(), JsonValue::String(ev.seq.clone()));
                }
                if !fields.iter().any(|f| f.name == "cursor") {
                    out.insert("cursor".into(), JsonValue::String(ev.cursor.clone()));
                }
                // Stash type name for federation traces if we ever
                // need it; harmless for inline types.
                out.insert("__typename".into(), JsonValue::String(name.clone()));
                Ok(JsonValue::Object(out))
            }
            ReturnTypeDef::EntityRef { type_name, key } => {
                let mut out = serde_json::Map::new();
                for (k, expr) in key {
                    let v = resolve_source(expr, &self.args, &protocol_ev, &ev.subject)
                        .map_err(|e| e.to_string())?;
                    out.insert(k.clone(), v);
                }
                out.insert("__typename".into(), JsonValue::String(type_name.clone()));
                Ok(JsonValue::Object(out))
            }
        }
    }
}

/// Convert a `SubscriptionEvent` back into a
/// `ventstream_protocol::Event` for template evaluation. We could
/// avoid this round-trip by passing the protocol Event through the
/// consumer stream instead of the SubscriptionEvent — opportunity
/// for a small refactor in v1.1.
fn reverse_to_protocol(ev: &crate::schema::SubscriptionEvent) -> ventstream_protocol::Event {
    // Best-effort reconstruction. The template engine only reads a
    // handful of fields, so missing optional bits are fine.
    let id = ev.id.as_str().parse::<ulid::Ulid>().unwrap_or_default();
    ventstream_protocol::Event {
        id,
        event: ev.event.clone(),
        tenant: ev.tenant.clone(),
        entity_id: ev.entity_id.clone(),
        actor: ev
            .actor
            .as_ref()
            .and_then(|a| ventstream_protocol::Actor::new(a.kind.clone(), a.id.clone()).ok()),
        occurred_at: ev.occurred_at,
        received_at: ev.received_at,
        schema_version: ev.schema_version as u32,
        data: ev.data.0.clone(),
        metadata: ventstream_protocol::Metadata {
            trace_id: ev.metadata.trace_id.clone(),
            correlation_id: ev.metadata.correlation_id.clone(),
            causation_id: ev.metadata.causation_id.clone(),
        },
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
    use super::*;
    use crate::manifest::{ArgDef, InlineFieldDef, ReturnTypeDef, SubscriptionDef};
    use crate::schema::SubscriptionEvent;

    fn inline_field(name: &str, source: &str, ty: &str) -> InlineFieldDef {
        InlineFieldDef {
            name: name.into(),
            type_name: ty.into(),
            required: true,
            source: source.into(),
        }
    }

    fn order_def(fields: Vec<InlineFieldDef>) -> Arc<SubscriptionDef> {
        Arc::new(SubscriptionDef {
            name: "orderStatusChanged".into(),
            description: None,
            args: vec![ArgDef {
                name: "orderId".into(),
                type_name: "ID".into(),
                required: true,
            }],
            return_type: ReturnTypeDef::Inline {
                name: "OrderStatusChange".into(),
                fields,
            },
            subject: "orderStatusChanged.{args.orderId}".into(),
        })
    }

    fn sample_event(seq: u64) -> SubscriptionEvent {
        let ts = chrono::DateTime::from_timestamp(0, 0).unwrap();
        let ev = ventstream_protocol::Event {
            id: ulid::Ulid::default(),
            event: "orderStatusChanged".into(),
            tenant: "acme".into(),
            entity_id: "order_1".into(),
            actor: None,
            occurred_at: ts,
            received_at: ts,
            schema_version: 2,
            data: serde_json::json!({ "status": "confirmed" }),
            metadata: ventstream_protocol::Metadata {
                trace_id: None,
                correlation_id: None,
                causation_id: None,
            },
        };
        SubscriptionEvent::from_protocol(
            ev,
            "vs.t.acme.orderStatusChanged.order_1".into(),
            &ventstream_realtime::Cursor::jetstream(seq),
        )
    }

    fn mapper(def: Arc<SubscriptionDef>) -> SubscriptionMapper {
        let mut args = HashMap::new();
        args.insert("orderId".into(), "order_1".into());
        SubscriptionMapper { def, args }
    }

    // A typed subscription carries both the provider-neutral cursor and the
    // legacy `seq` alias automatically, without the SDL declaring either.
    #[test]
    fn seq_auto_injected_into_typed_response() {
        let def = order_def(vec![
            inline_field("id", "$event.entityId", "ID"),
            inline_field("status", "$data.status", "String"),
        ]);
        let out = mapper(def).build_response(&sample_event(42)).unwrap();
        let obj = out.as_object().unwrap();
        assert_eq!(obj.get("seq"), Some(&JsonValue::String("42".into())));
        assert_eq!(obj.get("cursor"), Some(&JsonValue::String("42".into())));
        // declared fields still resolve as before
        assert_eq!(
            obj.get("status"),
            Some(&JsonValue::String("confirmed".into()))
        );
        assert_eq!(obj.get("id"), Some(&JsonValue::String("order_1".into())));
    }

    // If the SDL declares its own `seq` field, the author's mapping wins
    // and we do not overwrite it with the stream sequence.
    #[test]
    fn author_declared_seq_is_not_overwritten() {
        let def = order_def(vec![inline_field("seq", "$event.entityId", "String")]);
        let out = mapper(def).build_response(&sample_event(42)).unwrap();
        // author mapped seq <- entityId ("order_1"), NOT the stream seq "42"
        assert_eq!(
            out.as_object().unwrap().get("seq"),
            Some(&JsonValue::String("order_1".into()))
        );
    }

    // Both auto-exposed cursor fields land in the published SDL.
    #[test]
    fn typed_type_publishes_seq_as_non_null_string() {
        let obj = build_inline_type(
            "OrderStatusChange",
            &[inline_field("status", "$data.status", "String")],
        );
        // Reference the type from a Query field so the schema doesn't prune it.
        let query = Object::new("Query").field(Field::new(
            "probe",
            TypeRef::named_nn("OrderStatusChange"),
            |_| FieldFuture::new(async { Ok::<_, async_graphql::Error>(None::<FieldValue<'_>>) }),
        ));
        let schema = Schema::build("Query", None, None)
            .register(query)
            .register(obj)
            .finish()
            .unwrap();
        let sdl = schema.sdl();
        assert!(
            sdl.contains("seq: String!"),
            "published SDL missing `seq: String!`:\n{sdl}"
        );
        assert!(
            sdl.contains("cursor: String!"),
            "published SDL missing `cursor: String!`:\n{sdl}"
        );
    }
}
