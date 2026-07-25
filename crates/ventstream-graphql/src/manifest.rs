//! Subscription manifest — how developers declare typed subscription
//! fields and map them to subject patterns.
//!
//! When a manifest is configured (`VS_GRAPHQL_SUBSCRIPTIONS`), the
//! gateway builds an `async_graphql::dynamic::Schema` from it
//! instead of the generic-only static schema. The generic
//! `events(subject)` field is still present for catch-all
//! subscriptions; the manifest adds typed fields on top.
//!
//! ## Format
//!
//! ```yaml
//! subscriptions:
//!   - name: orderStatusChanged
//!     description: "Fires when an order's status changes"
//!     args:
//!       - { name: orderId, type: ID, required: true }
//!     return_type:
//!       kind: inline
//!       name: OrderStatusChange
//!       fields:
//!         - { name: id,        type: ID,       required: true, source: "$event.entityId" }
//!         - { name: status,    type: String,   required: true, source: "$data.status" }
//!         - { name: changedAt, type: DateTime, required: true, source: "$event.occurredAt" }
//!     subject: "orderStatusChanged.{args.orderId}"          # id last
//!
//!   - name: orderUpdated
//!     description: "Federated entity reference — router stitches the rest"
//!     args:
//!       - { name: orderId, type: ID, required: true }
//!     return_type:
//!       kind: entity_ref
//!       type: Order
//!       key:
//!         order_id: "{args.orderId}"
//!     subject: "orderUpdated.{args.orderId}"
//! ```
//!
//! ## Source expressions
//!
//! Values in `subject` templates and `source` fields use a tiny
//! syntax:
//! - `{args.NAME}` — value from the subscription's args
//! - `$data.PATH` — extract from the published event's `data` JSON
//!   (`$data` alone yields the whole payload)
//! - `$event.FIELD` — extract from the event envelope itself
//!   (supported: `id`, `tenant`, `event`, `entityId`, `occurredAt`,
//!   `receivedAt`, `subject`)

use serde::{Deserialize, Serialize};

/// Top-level manifest file shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// Declared typed subscription fields.
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionDef>,
}

/// One typed subscription field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubscriptionDef {
    /// GraphQL field name (camelCase by convention).
    pub name: String,

    /// Human-readable description (surfaced via introspection).
    #[serde(default)]
    pub description: Option<String>,

    /// Args this subscription field takes.
    #[serde(default)]
    pub args: Vec<ArgDef>,

    /// Return type definition.
    pub return_type: ReturnTypeDef,

    /// NATS subject pattern. May reference `{args.NAME}` placeholders
    /// that get filled in from `args` at subscription time.
    pub subject: String,
}

/// One argument on a subscription field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArgDef {
    /// Argument name as it appears in GraphQL queries.
    pub name: String,
    /// GraphQL scalar type. v1 supports: `ID`, `String`, `Int`,
    /// `Float`, `Boolean`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// When `true`, the arg is `!` in GraphQL (NON_NULL).
    #[serde(default = "default_true")]
    pub required: bool,
}

fn default_true() -> bool {
    true
}

/// Either an inline type defined in the manifest, or a reference to
/// an entity owned by another federated subgraph.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ReturnTypeDef {
    /// The built-in event envelope (`Event`) — `id`, `event`,
    /// `entityId`, `occurredAt`, opaque `data: JSON`, etc. No field
    /// mapping; the developer reads `data` client-side. This is the
    /// minimal case (`field(key): Event!`).
    Envelope,

    /// An inline type defined entirely in this manifest. The
    /// gateway registers the type with the schema and resolves each
    /// field from its declared `source` expression.
    Inline {
        /// GraphQL type name (PascalCase).
        name: String,
        /// Field definitions. Each carries a `source` expression
        /// that determines how the value is extracted at delivery
        /// time.
        fields: Vec<InlineFieldDef>,
    },

    /// A reference to a federated entity owned by another subgraph.
    /// The gateway emits a representation with the `@key` fields
    /// filled in; the router stitches the remaining fields from the
    /// entity-owning subgraph.
    EntityRef {
        /// Name of the entity type (must exist in another subgraph
        /// with a matching `@key`).
        #[serde(rename = "type")]
        type_name: String,
        /// The key fields and the source expression for each. Maps
        /// directly to the `@key(fields: "...")` directive.
        key: std::collections::BTreeMap<String, String>,
    },
}

/// One field on an inline return type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InlineFieldDef {
    /// GraphQL field name (camelCase by convention).
    pub name: String,
    /// GraphQL scalar type. v1: `ID`, `String`, `Int`, `Float`,
    /// `Boolean`, `DateTime`, `JSON`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// `!` modifier in GraphQL.
    #[serde(default = "default_true")]
    pub required: bool,
    /// Source expression — see module docs.
    pub source: String,
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

    const SAMPLE: &str = r#"
subscriptions:
  - name: orderStatusChanged
    description: "Order status changes"
    args:
      - { name: orderId, type: ID, required: true }
    return_type:
      kind: inline
      name: OrderStatusChange
      fields:
        - { name: id, type: ID, required: true, source: "$event.entityId" }
        - { name: status, type: String, required: true, source: "$data.status" }
        - { name: changedAt, type: DateTime, required: true, source: "$event.occurredAt" }
    subject: "orderStatusChanged.{args.orderId}"

  - name: orderUpdated
    args:
      - { name: orderId, type: ID, required: true }
    return_type:
      kind: entity_ref
      type: Order
      key: { order_id: "{args.orderId}" }
    subject: "orderUpdated.{args.orderId}"
"#;

    #[test]
    fn parses_sample_manifest() {
        let m: Manifest = serde_yaml::from_str(SAMPLE).unwrap();
        assert_eq!(m.subscriptions.len(), 2);
        assert_eq!(m.subscriptions[0].name, "orderStatusChanged");
        match &m.subscriptions[0].return_type {
            ReturnTypeDef::Inline { name, fields } => {
                assert_eq!(name, "OrderStatusChange");
                assert_eq!(fields.len(), 3);
            }
            _ => panic!("expected Inline"),
        }
        match &m.subscriptions[1].return_type {
            ReturnTypeDef::EntityRef { type_name, key } => {
                assert_eq!(type_name, "Order");
                assert_eq!(key.get("order_id").unwrap(), "{args.orderId}");
            }
            _ => panic!("expected EntityRef"),
        }
    }
}
