//! Who or what caused this event.

use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::identifier::{validate_entity_id, validate_strict};

/// Identifies the principal that caused this event. Always required —
/// audit logs are useless without it. For automated processes, use
/// `kind: "system"` and a descriptive id (`"billing-cron"`,
/// `"deduper-v2"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    /// Actor type. Common values: `user`, `system`, `service`,
    /// `anonymous`. Lowercase snake: `[a-z0-9_-]+`, max 64 chars.
    pub kind: String,
    /// Actor identifier. ULID/UUID-friendly: `[A-Za-z0-9_-]+`, max
    /// 128 chars. For `kind: "system"`, use a descriptive process name.
    pub id: String,
}

impl Actor {
    /// Construct an actor, validating both `kind` and `id`.
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Result<Self, ProtocolError> {
        let kind = kind.into();
        let id = id.into();
        validate_strict("actor.kind", &kind)?;
        validate_entity_id("actor.id", &id)?;
        Ok(Self { kind, id })
    }

    /// Validate an actor after deserialization.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_strict("actor.kind", &self.kind)?;
        validate_entity_id("actor.id", &self.id)?;
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
    fn user_actor() {
        let a = Actor::new("user", "user_456").unwrap();
        assert_eq!(a.kind, "user");
    }

    #[test]
    fn system_actor() {
        Actor::new("system", "billing-cron").unwrap();
    }

    #[test]
    fn rejects_bad_kind() {
        Actor::new("USER", "x").unwrap_err();
    }
}
