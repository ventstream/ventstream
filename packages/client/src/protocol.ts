// Wire protocol between WS client and the VentStream gateway.
// Must match `crates/ventstream-ws/src/protocol.rs`.

import type { Event } from "./envelope.js";

export interface HelloMessage {
  type: "hello";
  tenant: string;
  token: string;
  resume_from_seq?: string;
  resume_from_cursor?: string;
}

export interface SubscribeMessage {
  type: "subscribe";
  id: string;
  pattern: string;
}

export interface UnsubscribeMessage {
  type: "unsubscribe";
  id: string;
}

export interface PingMessage {
  type: "ping";
}

export type ClientMessage =
  | HelloMessage
  | SubscribeMessage
  | UnsubscribeMessage
  | PingMessage;

export interface ReadyServerMessage {
  type: "ready";
  connection_id: string;
  resumed_from?: number;
  resumed_from_cursor?: string;
}

export interface SubscribedServerMessage {
  type: "subscribed";
  id: string;
  anchored_pattern: string;
}

export interface UnsubscribedServerMessage {
  type: "unsubscribed";
  id: string;
}

export interface EventServerMessage {
  type: "event";
  subject: string;
  matched: string[];
  /** Exact provider-neutral resume cursor; absent for NATS Core delivery. */
  cursor?: string;
  /** Legacy JetStream field; absent for Redis and future providers. */
  seq?: number;
  event: Event;
}

export interface ErrorServerMessage {
  type: "error";
  code:
    | "bad_frame"
    | "not_ready"
    | "auth_failed"
    | "bad_pattern"
    | "slow_consumer"
    | "resume_expired"
    | "invalid_cursor"
    | "internal";
  message: string;
}

export interface PongServerMessage {
  type: "pong";
}

export type ServerMessage =
  | ReadyServerMessage
  | SubscribedServerMessage
  | UnsubscribedServerMessage
  | EventServerMessage
  | ErrorServerMessage
  | PongServerMessage;
