// The publisher client.
//
// Wraps `nats.js` with VentStream-shaped publish() and a thin
// lifecycle (connect, drain, close). Auth refresh and JetStream
// support land in v1.1; v1 talks NATS Core only.

import { connect, type NatsConnection } from "nats";
import { ulid } from "ulid";

import type { Actor, Entity, Event, Metadata } from "./envelope.js";
import { SCHEMA_VERSION } from "./envelope.js";
import { buildEventType, buildSubject } from "./subject.js";

/**
 * Backpressure behavior when the underlying NATS connection cannot
 * accept new bytes immediately.
 *
 * - `"block"` — the publish promise awaits flush; safest, lowest
 *   throughput.
 * - `"buffer"` — let nats.js buffer in memory (default); fastest,
 *   risks unbounded memory growth under sustained outage.
 * - `"drop"` — if the connection is closed, drop the publish silently
 *   and resolve. For fire-and-forget telemetry where loss is tolerable.
 */
export type BackpressurePolicy = "block" | "buffer" | "drop";

export interface VentStreamConfig {
  /** NATS server URLs (e.g. `["nats://localhost:4222"]`). */
  servers: string | string[];
  /** Token for `nats.js` auth (e.g. NATS JWT). Optional in v1. */
  token?: string;
  /** Username for basic auth. Optional. */
  user?: string;
  /** Password for basic auth. Optional. */
  pass?: string;
  /** Application name shown in NATS server logs. */
  name?: string;
  /** Default backpressure policy if a publish doesn't override. */
  backpressure?: BackpressurePolicy;
}

export interface PublishArgs {
  tenant: string;
  domain: string;
  /** e.g. "status_changed", "created", "deleted". */
  action: string;
  entity: Entity;
  actor?: Actor;
  /** Developer-defined payload. Anything JSON-serializable. */
  data: unknown;
  /** When the underlying business event happened. Defaults to now. */
  occurredAt?: Date;
  /** Optional tracing/correlation hints. */
  metadata?: Metadata;
  /** Override the default backpressure policy for this publish. */
  backpressure?: BackpressurePolicy;
}

/**
 * The VentStream publisher.
 *
 * Lifecycle:
 *
 * ```ts
 * const vs = new VentStream({ servers: "nats://localhost:4222" });
 * await vs.connect();
 * await vs.publish({ ... });
 * await vs.close();
 * ```
 */
export class VentStream {
  private readonly cfg: VentStreamConfig;
  private nc: NatsConnection | undefined;

  constructor(cfg: VentStreamConfig) {
    this.cfg = cfg;
  }

  /** Open the NATS connection. Idempotent. */
  async connect(): Promise<void> {
    if (this.nc) return;
    this.nc = await connect({
      servers: this.cfg.servers,
      ...(this.cfg.name !== undefined ? { name: this.cfg.name } : {}),
      ...(this.cfg.token !== undefined ? { token: this.cfg.token } : {}),
      ...(this.cfg.user !== undefined ? { user: this.cfg.user } : {}),
      ...(this.cfg.pass !== undefined ? { pass: this.cfg.pass } : {}),
    });
  }

  /** Drain in-flight publishes and close the connection. */
  async close(): Promise<void> {
    if (!this.nc) return;
    await this.nc.drain();
    this.nc = undefined;
  }

  /**
   * Build and publish one event.
   *
   * The SDK mints `id` (ULID), `received_at` (now), and pins
   * `schema_version`. Required fields are validated by character
   * class — bad inputs throw `ProtocolError` before any bytes hit
   * the network.
   */
  async publish(args: PublishArgs): Promise<{ id: string; subject: string }> {
    if (!this.nc) {
      throw new Error("VentStream.publish: call connect() first");
    }

    const { envelope, subject } = buildPublishMessage(args);

    const policy = args.backpressure ?? this.cfg.backpressure ?? "buffer";
    const payload = JSON.stringify(envelope);

    if (policy === "drop" && this.nc.isClosed()) {
      // Connection is gone — drop silently per policy.
      return { id: envelope.id, subject };
    }

    this.nc.publish(subject, payload);

    if (policy === "block") {
      await this.nc.flush();
    }
    return { id: envelope.id, subject };
  }
}

interface BuildPublishMessageOptions {
  id?: string;
  now?: Date;
}

/** Build the canonical wire message. Exported for protocol contract tests. */
export function buildPublishMessage(
  args: PublishArgs,
  options: BuildPublishMessageOptions = {},
): { envelope: Event; subject: string } {
  const subject = buildSubject({
    tenant: args.tenant,
    domain: args.domain,
    entityKind: args.entity.kind,
    entityId: args.entity.id,
    action: args.action,
  });
  const event = buildEventType({
    domain: args.domain,
    entityKind: args.entity.kind,
    action: args.action,
  });
  const now = options.now ?? new Date();
  const occurredAt = args.occurredAt ?? now;
  const envelope: Event = {
    id: options.id ?? ulid(),
    event,
    tenant: args.tenant,
    entity_id: args.entity.id,
    ...(args.actor !== undefined ? { actor: args.actor } : {}),
    occurred_at: occurredAt.toISOString(),
    received_at: now.toISOString(),
    schema_version: SCHEMA_VERSION,
    data: args.data,
    metadata: args.metadata ?? {},
  };
  return { envelope, subject };
}
