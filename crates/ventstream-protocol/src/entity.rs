//! The thing an event is about.

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::identifier::{validate_entity_id, validate_strict};

/// The entity this event is about — the addressable subject of the
/// change. Pairs a free-form `kind` (the entity type) with an `id`.
///
/// For events that aren't bound to a single business entity (system
/// events, batch operations), use `kind: "system"` and an id that
/// names the process (`"billing-cron"`, `"nightly-reindex"`). The
/// subject grammar requires both — there is no "entity-less" event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    /// The entity type. Lowercase snake: `[a-z0-9_-]+`, max 64 chars.
    pub kind: String,
    /// The entity identifier. ULID/UUID-friendly: `[A-Za-z0-9_-]+`,
    /// max 128 chars.
    pub id: String,
}

impl Entity {
    /// Construct an entity, validating both `kind` and `id`.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self, ProtocolError> {
        let kind = kind.into();
        let id = id.into();
        validate_strict("entity.kind", &kind)?;
        validate_entity_id("entity.id", &id)?;
        Ok(Self { kind, id })
    }

    /// Validate an entity after deserialization. `serde` cannot enforce
    /// the character classes on its own.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_strict("entity.kind", &self.kind)?;
        validate_entity_id("entity.id", &self.id)?;
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
    fn accepts_valid() {
        let e = Entity::new("order", "order_123").unwrap();
        assert_eq!(e.kind, "order");
        assert_eq!(e.id, "order_123");
    }

    #[test]
    fn rejects_uppercase_kind() {
        Entity::new("Order", "order_123").unwrap_err();
    }

    #[test]
    fn accepts_uppercase_id() {
        Entity::new("order", "Order_ABC123").unwrap();
    }

    #[test]
    fn serde_roundtrip() {
        let e = Entity::new("order", "order_123").unwrap();
        let j = serde_json::to_string(&e).unwrap();
        let back: Entity = serde_json::from_str(&j).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn serde_rejects_unknown_field() {
        let j = r#"{"kind":"order","id":"o1","extra":"x"}"#;
        let r: Result<Entity, _> = serde_json::from_str(j);
        assert!(r.is_err());
    }
}
