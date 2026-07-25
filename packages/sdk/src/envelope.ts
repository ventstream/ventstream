// Envelope types — the on-the-wire shape of every event.
//
// These mirror the Rust `ventstream-protocol` crate. When the Rust
// envelope evolves, bump `SCHEMA_VERSION` here and in the engine.

export const SCHEMA_VERSION = 2;

/**
 * The thing an event is about. Pairs a free-form `kind` with an `id`.
 *
 * For events not tied to a single business entity, use
 * `{ kind: "system", id: "<descriptor>" }`.
 */
export interface Entity {
  kind: string;
  id: string;
}

/**
 * Who or what caused the event.
 *
 * For automated actors, use `{ kind: "system", id: "<process>" }`.
 */
export interface Actor {
  kind: string;
  id: string;
}

/**
 * Optional tracing/correlation hints.
 */
export interface Metadata {
  trace_id?: string;
  correlation_id?: string;
  causation_id?: string;
}

/**
 * The wire envelope every event carries.
 */
export interface Event {
  id: string;
  event: string;
  tenant: string;
  entity_id: string;
  actor?: Actor;
  occurred_at: string;
  received_at: string;
  schema_version: number;
  data: unknown;
  metadata: Metadata;
}
