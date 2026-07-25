//! The minimal developer-facing publish contract.
//!
//! Hand-building a full [`Event`](crate::Event) to publish is heavy.
//! A publisher only really needs to say **what happened** and **to
//! which instance** — everything else the SDK fills in. [`PublishInput`]
//! is that minimal shape:
//!
//! - `event` — the event name, e.g. `"deskMutated"` (or a dotted name
//!   like `"orders.order.statusChanged"`). Generic; no id baked in.
//! - `id` — the unique id of the instance the event is about, e.g.
//!   `"desk_123"`.
//! - `data` — optional opaque payload (the engine never inspects it).
//!
//! [`PublishInput::into_event`] expands that into the canonical
//! envelope, composing the routing subject `vs.t.{tenant}.{event}.{id}`
//! — id **last**, so subscribers can target one instance
//! (`deskMutated.desk_123`) or wildcard the id for all of them
//! (`deskMutated.*`), and any number of clients can share either.
//!
//! `tenant` is **not** part of the input — it comes from the
//! publisher's authenticated context, never caller-supplied data.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ProtocolError;
use crate::{Actor, Event, Metadata};

/// Minimal input to publish an event. Everything not listed here is
/// derived (the routing subject) or defaulted (`id`/ULID, timestamps,
/// `schema_version`, `metadata`) by [`Self::into_event`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublishInput {
    /// Event name — what happened. `"deskMutated"`, or dotted like
    /// `"orders.order.statusChanged"`. No id.
    pub event: String,

    /// Unique id of the instance this event concerns. Becomes the final
    /// (id) segment of the routing subject.
    pub id: String,

    /// Optional opaque developer payload. Defaults to JSON null.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub data: Value,

    /// Optional actor (who/what caused it). Omitted by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,

    /// Optional business-event time. Defaults to publish time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurred_at: Option<DateTime<Utc>>,

    /// Optional tracing/correlation hints. Defaults to empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

impl PublishInput {
    /// The happy path: `event` name + instance `id`, no payload.
    pub fn new(event: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            id: id.into(),
            data: Value::Null,
            actor: None,
            occurred_at: None,
            metadata: None,
        }
    }

    /// Attach an opaque payload.
    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }

    /// Attach an actor (who/what caused it).
    #[must_use]
    pub fn with_actor(mut self, actor: Actor) -> Self {
        self.actor = Some(actor);
        self
    }

    /// Set the business-event time (defaults to publish time).
    #[must_use]
    pub fn with_occurred_at(mut self, occurred_at: DateTime<Utc>) -> Self {
        self.occurred_at = Some(occurred_at);
        self
    }

    /// Attach tracing/correlation metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Expand into the canonical, routed [`Event`]. `tenant` comes from
    /// the publisher's authenticated context. Returns [`ProtocolError`]
    /// if `event`/`id` are malformed.
    pub fn into_event(self, tenant: impl Into<String>) -> Result<Event, ProtocolError> {
        let occurred_at = self.occurred_at.unwrap_or_else(Utc::now);
        let metadata = self.metadata.unwrap_or_default();
        Event::publish(
            tenant,
            self.event,
            self.id,
            self.actor,
            occurred_at,
            self.data,
            metadata,
        )
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

    #[test]
    fn minimal_input_expands_to_routed_event() {
        let ev = PublishInput::new("deskMutated", "desk_123")
            .into_event("acme")
            .expect("expand");
        assert_eq!(ev.event, "deskMutated");
        assert_eq!(ev.entity_id, "desk_123");
        assert_eq!(ev.tenant, "acme");
        assert!(ev.actor.is_none());
        assert_eq!(ev.schema_version, crate::CURRENT_SCHEMA_VERSION);
        assert!(ev.data.is_null());
        ev.validate().expect("valid");
    }

    #[test]
    fn id_is_the_last_subject_segment() {
        let ev = PublishInput::new("deskMutated", "desk_123")
            .into_event("acme")
            .unwrap();
        assert_eq!(
            ev.subject().unwrap().to_string(),
            "vs.t.acme.deskMutated.desk_123"
        );
    }

    #[test]
    fn dotted_event_name_keeps_id_last() {
        let ev = PublishInput::new("orders.order.statusChanged", "order_123")
            .into_event("acme")
            .unwrap();
        assert_eq!(
            ev.subject().unwrap().to_string(),
            "vs.t.acme.orders.order.statusChanged.order_123"
        );
    }

    #[test]
    fn optional_data_round_trips() {
        let data = serde_json::json!({"status": "active"});
        let ev = PublishInput::new("deskMutated", "desk_123")
            .with_data(data.clone())
            .into_event("acme")
            .unwrap();
        assert_eq!(ev.data, data);
    }

    #[test]
    fn actor_override_is_carried() {
        let ev = PublishInput::new("deskMutated", "desk_123")
            .with_actor(Actor::new("service", "billing").unwrap())
            .into_event("acme")
            .unwrap();
        let actor = ev.actor.unwrap();
        assert_eq!(actor.kind, "service");
        assert_eq!(actor.id, "billing");
    }

    #[test]
    fn malformed_event_name_is_rejected() {
        PublishInput::new("desk mutated", "desk_123")
            .into_event("acme")
            .unwrap_err();
    }

    #[test]
    fn deserializes_from_minimal_json() {
        let v = serde_json::json!({
            "event": "deskMutated",
            "id": "desk_123",
            "data": {"status": "active"}
        });
        let input: PublishInput = serde_json::from_value(v).unwrap();
        assert_eq!(input.event, "deskMutated");
        assert_eq!(input.id, "desk_123");
        assert!(input.actor.is_none());
        input.into_event("acme").unwrap();
    }

    #[test]
    fn serializes_compactly_when_minimal() {
        let json = serde_json::to_value(PublishInput::new("deskMutated", "desk_123")).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"event": "deskMutated", "id": "desk_123"})
        );
    }
}
