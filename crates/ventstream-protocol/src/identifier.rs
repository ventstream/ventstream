//! Character-class validators for the identifier slots in the
//! protocol — tenant, domain, entity kind/id, action.
//!
//! Two classes: a strict lowercase-snake class used for system-defined
//! slots (tenant, domain, entity kind, action), and a slightly more
//! permissive class used for entity IDs (which are often ULIDs / UUIDs
//! / mixed-case opaque keys generated outside our control).
//!
//! No regex dependency — the character classes are simple enough that
//! a hand-written byte scan is faster, has no compile-time overhead,
//! and avoids one more dependency.

use crate::error::ProtocolError;

const MAX_IDENTIFIER_LEN: usize = 128;

/// Strict lowercase identifier: `[a-z0-9_-]+`, 1..=64 chars. Used for
/// tenant, domain, entity kind, and action.
pub(crate) fn validate_strict(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
            reason: "must be non-empty",
        });
    }
    if value.len() > 64 {
        return Err(ProtocolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
            reason: "max 64 chars",
        });
    }
    for &b in value.as_bytes() {
        let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-';
        if !ok {
            return Err(ProtocolError::InvalidIdentifier {
                field,
                value: value.to_owned(),
                reason: "only [a-z0-9_-] allowed",
            });
        }
    }
    Ok(())
}

/// Validate an event *name*: one or more `.`-joined segments, each
/// `[A-Za-z0-9_-]+` and ≤64 chars. Mixed case is allowed so the
/// common `camelCase` convention works (`deskMutated`,
/// `orders.order.statusChanged`). The name must not be empty and must
/// not have empty segments (`a..b`).
pub(crate) fn validate_event_name(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
            reason: "must be non-empty",
        });
    }
    for segment in value.split('.') {
        if segment.is_empty() {
            return Err(ProtocolError::InvalidIdentifier {
                field,
                value: value.to_owned(),
                reason: "empty segment (e.g. leading/trailing/double '.')",
            });
        }
        if segment.len() > 64 {
            return Err(ProtocolError::InvalidIdentifier {
                field,
                value: value.to_owned(),
                reason: "each segment max 64 chars",
            });
        }
        for &b in segment.as_bytes() {
            let ok = b.is_ascii_alphanumeric() || b == b'_' || b == b'-';
            if !ok {
                return Err(ProtocolError::InvalidIdentifier {
                    field,
                    value: value.to_owned(),
                    reason: "only [A-Za-z0-9_-] allowed per segment",
                });
            }
        }
    }
    Ok(())
}

/// Looser identifier for entity IDs: `[A-Za-z0-9_-]+`, 1..=128 chars.
/// Accepts ULIDs (uppercase) and UUIDs (mixed case with hyphens).
pub(crate) fn validate_entity_id(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
            reason: "must be non-empty",
        });
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(ProtocolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
            reason: "max 128 chars",
        });
    }
    for &b in value.as_bytes() {
        let ok = b.is_ascii_alphanumeric() || b == b'_' || b == b'-';
        if !ok {
            return Err(ProtocolError::InvalidIdentifier {
                field,
                value: value.to_owned(),
                reason: "only [A-Za-z0-9_-] allowed",
            });
        }
    }
    Ok(())
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
    fn strict_accepts_lowercase_snake() {
        validate_strict("domain", "orders").unwrap();
        validate_strict("domain", "order_history").unwrap();
        validate_strict("domain", "abc-123").unwrap();
    }

    #[test]
    fn strict_rejects_uppercase() {
        let err = validate_strict("domain", "Orders").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidIdentifier { .. }));
    }

    #[test]
    fn strict_rejects_dot() {
        let err = validate_strict("domain", "orders.archive").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidIdentifier { .. }));
    }

    #[test]
    fn strict_rejects_empty() {
        let err = validate_strict("domain", "").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidIdentifier { .. }));
    }

    #[test]
    fn entity_id_accepts_ulid_shape() {
        validate_entity_id("entity.id", "01KS9FJZ8H3K4M2N0P1Q2R3S4T").unwrap();
    }

    #[test]
    fn entity_id_accepts_uuid_shape() {
        validate_entity_id("entity.id", "550e8400-e29b-41d4-a716-446655440000").unwrap();
    }

    #[test]
    fn entity_id_accepts_mixed_case() {
        validate_entity_id("entity.id", "Order_ABC123").unwrap();
    }

    #[test]
    fn entity_id_rejects_dot() {
        validate_entity_id("entity.id", "order.123").unwrap_err();
    }

    #[test]
    fn entity_id_rejects_space() {
        validate_entity_id("entity.id", "order 123").unwrap_err();
    }
}
