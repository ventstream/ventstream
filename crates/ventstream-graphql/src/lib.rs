//! Apollo-Client-compatible GraphQL subscription gateway.
//!
//! This crate exposes a `graphql-transport-ws` endpoint that
//! standard GraphQL clients (Apollo Client, urql, Relay) speak
//! natively. The schema is a single subscription field —
//! `events(subject: String!)` — that delivers the same envelope the
//! native VentStream WS pipeline uses, with `data` carried opaquely
//! as a JSON scalar.
//!
//! The crate doesn't run resolvers or do field projection. The
//! GraphQL surface is thin on purpose: introspection + connection
//! lifecycle + protocol framing is what GraphQL clients expect, and
//! that's what we provide. Event shape is the developer's
//! responsibility, same as anywhere else they emit events.
//!
//! Live-only operations on a GraphQL socket share one provider-neutral broker
//! session. Resumed operations use subject-filtered sessions so multiplexed
//! operations can advance and replay their cursors independently. Provider
//! adapters own replay, acceptance, recovery, and resource cleanup.

#![deny(missing_docs)]

mod auth;
mod config;
mod conn_source;
mod dynamic_schema;
mod error;
mod manifest;
mod schema;
mod sdl;
mod server;
mod template;

pub use config::{GraphQlConfig, SubjectDescriptor};
pub use error::GraphQlError;
pub use server::{run, run_with_readiness};
