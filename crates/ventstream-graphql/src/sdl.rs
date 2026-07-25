//! Load typed subscriptions from a GraphQL **SDL** file.
//!
//! An alternative (and the recommended way) to the YAML manifest:
//! developers declare the `Subscription` type in GraphQL SDL and
//! annotate each field with `@vsSubscribe(subject: "...")` — the
//! event/subject it maps to (id last, e.g. `orderStatusChanged.{orderId}`).
//!
//! The return type is either:
//! - the built-in **`Event`** (opaque envelope — `data` as JSON), or
//! - a custom object type whose fields carry an optional
//!   `@source(from: "...")` directive (defaulting to `$data.{fieldName}`).
//!
//! This parses the SDL into the same [`Manifest`] the YAML path produces,
//! so the dynamic-schema builder is unchanged.
//!
//! ```graphql
//! type Subscription {
//!   orderStatusChanged(orderId: ID!): Event!
//!     @vsSubscribe(subject: "orderStatusChanged.{orderId}")
//! }
//! ```

use std::collections::BTreeMap;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{
    BaseType, ConstDirective, ObjectType, Type, TypeKind, TypeSystemDefinition,
};
use async_graphql::parser::Positioned;
use async_graphql::Value as ConstValue;

use crate::error::GraphQlError;
use crate::manifest::{ArgDef, InlineFieldDef, Manifest, ReturnTypeDef, SubscriptionDef};

const SUBSCRIBE_DIRECTIVE: &str = "vsSubscribe";
const SOURCE_DIRECTIVE: &str = "source";
const ENVELOPE_TYPE: &str = "Event";

/// Parse a GraphQL SDL document into the subscription [`Manifest`].
pub(crate) fn parse_sdl(sdl: &str) -> Result<Manifest, GraphQlError> {
    let doc =
        parse_schema(sdl).map_err(|e| GraphQlError::Config(format!("parsing SDL schema: {e}")))?;

    // Index object types by name; find the subscription root name.
    let mut objects: BTreeMap<String, &ObjectType> = BTreeMap::new();
    let mut sub_root = "Subscription".to_owned();
    for def in &doc.definitions {
        match def {
            TypeSystemDefinition::Schema(s) => {
                if let Some(sub) = &s.node.subscription {
                    sub_root = sub.node.as_str().to_owned();
                }
            }
            TypeSystemDefinition::Type(t) => {
                if let TypeKind::Object(obj) = &t.node.kind {
                    objects.insert(t.node.name.node.as_str().to_owned(), obj);
                }
            }
            TypeSystemDefinition::Directive(_) => {}
        }
    }

    let sub_obj = objects.get(&sub_root).ok_or_else(|| {
        GraphQlError::Config(format!(
            "SDL has no `{sub_root}` type to read subscriptions from"
        ))
    })?;

    let mut subscriptions = Vec::with_capacity(sub_obj.fields.len());
    for f in &sub_obj.fields {
        let field = &f.node;
        let name = field.name.node.as_str().to_owned();

        // @vsSubscribe(subject: "...") — required.
        let raw_subject = directive_string_arg(&field.directives, SUBSCRIBE_DIRECTIVE, "subject")
            .ok_or_else(|| {
            GraphQlError::Config(format!(
                "subscription field `{name}` is missing @{SUBSCRIBE_DIRECTIVE}(subject: \"...\")"
            ))
        })?;

        let description = field.description.as_ref().map(|d| d.node.clone());

        let args: Vec<ArgDef> = field
            .arguments
            .iter()
            .map(|a| {
                let (type_name, required) = named_type(&a.node.ty.node);
                ArgDef {
                    name: a.node.name.node.as_str().to_owned(),
                    type_name,
                    required,
                }
            })
            .collect();

        // SDL uses the friendly `{argName}` placeholder; the template
        // engine expects `{args.argName}`. Normalize each declared arg
        // (leaves an already-qualified `{args.x}` untouched, since it
        // contains no bare `{x}` substring).
        let mut subject = raw_subject;
        for a in &args {
            subject = subject.replace(&format!("{{{}}}", a.name), &format!("{{args.{}}}", a.name));
        }

        let (ret_name, _) = named_type(&field.ty.node);
        let return_type = if ret_name == ENVELOPE_TYPE {
            ReturnTypeDef::Envelope
        } else {
            let ret_obj = objects.get(&ret_name).ok_or_else(|| {
                GraphQlError::Config(format!(
                    "subscription field `{name}` returns `{ret_name}`, which is not defined in the SDL \
                     (use `Event` for the opaque envelope, or define an object type)"
                ))
            })?;
            let fields = ret_obj
                .fields
                .iter()
                .map(|rf| {
                    let rfd = &rf.node;
                    let fname = rfd.name.node.as_str().to_owned();
                    let (type_name, required) = named_type(&rfd.ty.node);
                    // `@source(from:)` is optional — default to the
                    // same-named key in the event's opaque `data`.
                    let source = directive_string_arg(&rfd.directives, SOURCE_DIRECTIVE, "from")
                        .unwrap_or_else(|| format!("$data.{fname}"));
                    InlineFieldDef {
                        name: fname,
                        type_name,
                        required,
                        source,
                    }
                })
                .collect();
            ReturnTypeDef::Inline {
                name: ret_name,
                fields,
            }
        };

        subscriptions.push(SubscriptionDef {
            name,
            description,
            args,
            return_type,
            subject,
        });
    }

    Ok(Manifest { subscriptions })
}

/// Pull a string-valued argument out of a named directive on a field.
fn directive_string_arg(
    directives: &[Positioned<ConstDirective>],
    directive: &str,
    arg: &str,
) -> Option<String> {
    directives
        .iter()
        .find(|d| d.node.name.node.as_str() == directive)
        .and_then(|d| d.node.get_argument(arg))
        .and_then(|v| match &v.node {
            ConstValue::String(s) => Some(s.clone()),
            _ => None,
        })
}

/// Resolve a GraphQL type to its base name + whether it's non-null (`!`).
/// Lists are unwrapped to their element's base name (we don't model list
/// fields in the manifest; the inner name is the useful part).
fn named_type(ty: &Type) -> (String, bool) {
    let required = !ty.nullable;
    let name = base_name(&ty.base);
    (name, required)
}

fn base_name(base: &BaseType) -> String {
    match base {
        BaseType::Named(n) => n.as_str().to_owned(),
        BaseType::List(inner) => base_name(&inner.base),
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
    fn parses_envelope_form() {
        let sdl = r#"
            type Subscription {
              orderStatusChanged(orderId: ID!): Event!
                @vsSubscribe(subject: "orderStatusChanged.{orderId}")
            }
        "#;
        let m = parse_sdl(sdl).unwrap();
        assert_eq!(m.subscriptions.len(), 1);
        let s = &m.subscriptions[0];
        assert_eq!(s.name, "orderStatusChanged");
        // `{orderId}` is normalized to the template engine's `{args.orderId}`.
        assert_eq!(s.subject, "orderStatusChanged.{args.orderId}");
        assert_eq!(s.args.len(), 1);
        assert_eq!(s.args[0].name, "orderId");
        assert_eq!(s.args[0].type_name, "ID");
        assert!(s.args[0].required);
        assert!(matches!(s.return_type, ReturnTypeDef::Envelope));
    }

    #[test]
    fn parses_inline_form_with_default_and_explicit_sources() {
        let sdl = r#"
            type Subscription {
              orderStatusChanged(orderId: ID!): OrderStatusChange!
                @vsSubscribe(subject: "orderStatusChanged.{orderId}")
            }
            type OrderStatusChange {
              id: ID! @source(from: "$event.entityId")
              status: String!
              changedAt: DateTime @source(from: "$event.occurredAt")
            }
        "#;
        let m = parse_sdl(sdl).unwrap();
        let s = &m.subscriptions[0];
        match &s.return_type {
            ReturnTypeDef::Inline { name, fields } => {
                assert_eq!(name, "OrderStatusChange");
                assert_eq!(fields.len(), 3);
                // explicit @source
                assert_eq!(fields[0].name, "id");
                assert_eq!(fields[0].source, "$event.entityId");
                // default source -> $data.status
                assert_eq!(fields[1].name, "status");
                assert_eq!(fields[1].source, "$data.status");
                assert_eq!(fields[2].source, "$event.occurredAt");
            }
            other => panic!("expected Inline, got {other:?}"),
        }
    }

    #[test]
    fn missing_subscribe_directive_is_an_error() {
        let sdl = r#"
            type Subscription {
              orderStatusChanged(orderId: ID!): Event!
            }
        "#;
        assert!(parse_sdl(sdl).is_err());
    }

    #[test]
    fn unknown_return_type_is_an_error() {
        let sdl = r#"
            type Subscription {
              x(id: ID!): Nope! @vsSubscribe(subject: "x.{id}")
            }
        "#;
        assert!(parse_sdl(sdl).is_err());
    }
}
