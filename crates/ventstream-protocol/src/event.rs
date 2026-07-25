//! The on-the-wire event envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::actor::Actor;
use crate::error::ProtocolError;
use crate::identifier::{validate_entity_id, validate_event_name, validate_strict};
use crate::metadata::Metadata;
use crate::subject::Subject;

/// Current envelope schema version. Bumped only on breaking changes
/// to the envelope shape (not to the `data` payload — that's the
/// developer's concern).
pub const CURRENT_SCHEMA_VERSION: u32 = 2;

/// The event envelope — the structure every event has on the bus.
///
/// The routing identity is just `event` + `id`: the event **name**
/// (e.g. `deskMutated`, possibly dotted) and the **instance id** it
/// concerns. They compose the subject `vs.t.{tenant}.{event}.{id}`
/// (see [`Event::subject`]). Everything else is envelope metadata; the
/// developer payload rides in the opaque [`Event::data`].
///
/// Build with [`crate::PublishInput`] (the ergonomic path) or
/// [`Event::publish`]; validate inbound events with [`Event::validate`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Unique id for this specific event instance (ULID). Idempotency
    /// key for sinks that need it. Distinct from [`Self::entity_id`].
    pub id: Ulid,

    /// The event name — what happened, independent of which instance.
    /// One or more `.`-joined segments, e.g. `"deskMutated"` or
    /// `"orders.order.statusChanged"`.
    pub event: String,

    /// Tenant scope. Authoritative source is the publisher's auth
    /// context — set from the token, not caller input.
    pub tenant: String,

    /// The instance id this event is about — becomes the final subject
    /// segment, so it's individually addressable and wildcard-able.
    pub entity_id: String,

    /// When the underlying business event happened — set by the
    /// publisher, may predate `received_at`.
    pub occurred_at: DateTime<Utc>,

    /// When the SDK handed this event to the bus.
    pub received_at: DateTime<Utc>,

    /// Envelope schema version. Equals [`CURRENT_SCHEMA_VERSION`] for
    /// events built by an SDK on the same major version.
    pub schema_version: u32,

    /// Optional principal that caused the event. Omitted by default
    /// (most publishes don't carry one); set it for audit trails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,

    /// Developer-defined payload. Opaque to the engine.
    #[serde(default)]
    pub data: serde_json::Value,

    /// Tracing/correlation hints. All sub-fields optional.
    #[serde(default)]
    pub metadata: Metadata,
}

impl Event {
    /// Build an event for publication.
    ///
    /// `id` is freshly minted; `received_at` is set to `now()`;
    /// `schema_version` is pinned. The caller supplies `occurred_at`
    /// separately so delayed publishes can preserve the business-event
    /// time.
    #[allow(clippy::too_many_arguments)]
    pub fn publish(
        tenant: impl Into<String>,
        event: impl Into<String>,
        entity_id: impl Into<String>,
        actor: Option<Actor>,
        occurred_at: DateTime<Utc>,
        data: serde_json::Value,
        metadata: Metadata,
    ) -> Result<Self, ProtocolError> {
        let tenant = tenant.into();
        let event = event.into();
        let entity_id = entity_id.into();
        validate_strict("tenant", &tenant)?;
        validate_event_name("event", &event)?;
        validate_entity_id("id", &entity_id)?;
        if let Some(a) = &actor {
            a.validate()?;
        }
        Ok(Self {
            id: Ulid::new(),
            event,
            tenant,
            entity_id,
            occurred_at,
            received_at: Utc::now(),
            schema_version: CURRENT_SCHEMA_VERSION,
            actor,
            data,
            metadata,
        })
    }

    /// Compose the routing subject `vs.t.{tenant}.{event}.{id}`.
    pub fn subject(&self) -> Result<Subject, ProtocolError> {
        Subject::new(&self.tenant, &self.event, &self.entity_id)
    }

    /// Validate an event received off the wire — identifier character
    /// classes and schema-version compatibility.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_strict("tenant", &self.tenant)?;
        validate_event_name("event", &self.event)?;
        validate_entity_id("id", &self.entity_id)?;
        if let Some(a) = &self.actor {
            a.validate()?;
        }
        if self.schema_version != CURRENT_SCHEMA_VERSION {
            return Err(ProtocolError::InvalidEvent {
                reason: format!(
                    "schema_version {} not supported (expected {})",
                    self.schema_version, CURRENT_SCHEMA_VERSION
                ),
            });
        }
        Ok(())
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
    fn publish_sets_envelope_fields() {
        let occurred = Utc::now();
        let ev = Event::publish(
            "acme",
            "deskMutated",
            "desk_123",
            None,
            occurred,
            serde_json::json!({"status": "active"}),
            Metadata::default(),
        )
        .unwrap();
        assert_eq!(ev.event, "deskMutated");
        assert_eq!(ev.tenant, "acme");
        assert_eq!(ev.entity_id, "desk_123");
        assert_eq!(ev.schema_version, CURRENT_SCHEMA_VERSION);
        assert!(ev.actor.is_none());
        ev.validate().unwrap();
    }

    #[test]
    fn subject_composes_correctly() {
        let ev = Event::publish(
            "acme",
            "deskMutated",
            "desk_123",
            None,
            Utc::now(),
            serde_json::Value::Null,
            Metadata::default(),
        )
        .unwrap();
        assert_eq!(
            ev.subject().unwrap().to_string(),
            "vs.t.acme.deskMutated.desk_123"
        );
    }

    #[test]
    fn validate_rejects_future_schema_version() {
        let mut ev = Event::publish(
            "acme",
            "deskMutated",
            "desk_123",
            None,
            Utc::now(),
            serde_json::Value::Null,
            Metadata::default(),
        )
        .unwrap();
        ev.schema_version = 999;
        ev.validate().unwrap_err();
    }

    #[test]
    fn javascript_sdk_fixture_matches_wire_contract() {
        let json = include_str!("../../../testdata/realtime-event-v2.json");
        let ev: Event = serde_json::from_str(json).expect("deserialize SDK fixture");
        ev.validate().expect("validate SDK fixture");
        assert_eq!(
            ev.subject().unwrap().to_string(),
            "vs.t.acme.orders.order.status_changed.order_123"
        );
    }

    #[test]
    fn rejects_uppercase_tenant() {
        Event::publish(
            "Acme",
            "deskMutated",
            "desk_123",
            None,
            Utc::now(),
            serde_json::Value::Null,
            Metadata::default(),
        )
        .unwrap_err();
    }
}
