export { VentStreamClient } from "./client.js";
export type {
  EventHandler,
  Subscription,
  VentStreamClientConfig,
} from "./client.js";
export type { Actor, Entity, Event, Metadata } from "./envelope.js";
export { SCHEMA_VERSION } from "./envelope.js";
export type {
  ClientMessage,
  ServerMessage,
  EventServerMessage,
  ErrorServerMessage,
} from "./protocol.js";
