import { createClient } from "redis";

import { buildPublishMessage, type PublishArgs } from "./client.js";

const DEFAULT_KEY_PREFIX = "ventstream";
const SAFE_PREFIX = /^[A-Za-z0-9:_-]+$/;

export interface RedisVentStreamConfig {
  /** Redis connection URL. Use `rediss://` for TLS. */
  url: string;
  /** Prefix for `<prefix>:{<tenant>}:events` stream keys. */
  keyPrefix?: string;
  /** Approximate stream entry ceiling applied atomically with XADD. */
  maxLength?: number;
}

export interface RedisPublishResult {
  id: string;
  subject: string;
  /** Redis stream entry ID returned by XADD. */
  streamId: string;
  /** Provider-neutral cursor accepted by VentStream gateways. */
  cursor: string;
}

/** Publisher for the Redis Streams realtime provider. */
export class RedisVentStream {
  private readonly cfg: RedisVentStreamConfig;
  private readonly client;

  constructor(cfg: RedisVentStreamConfig) {
    if (cfg.url.trim().length === 0) {
      throw new Error("RedisVentStream: url must not be empty");
    }
    const keyPrefix = cfg.keyPrefix ?? DEFAULT_KEY_PREFIX;
    if (!SAFE_PREFIX.test(keyPrefix)) {
      throw new Error(
        "RedisVentStream: keyPrefix may contain only letters, numbers, colon, underscore, and hyphen",
      );
    }
    if (
      cfg.maxLength !== undefined &&
      (!Number.isSafeInteger(cfg.maxLength) || cfg.maxLength <= 0)
    ) {
      throw new Error("RedisVentStream: maxLength must be a positive safe integer");
    }
    this.cfg = { ...cfg, keyPrefix };
    this.client = createClient({ url: cfg.url });
  }

  /** Open the Redis connection. Idempotent. */
  async connect(): Promise<void> {
    if (!this.client.isOpen) {
      await this.client.connect();
    }
  }

  /** Flush Redis protocol writes and close the connection. */
  async close(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }

  /** Build and append one canonical VentStream event to its tenant stream. */
  async publish(args: PublishArgs): Promise<RedisPublishResult> {
    if (!this.client.isReady) {
      throw new Error("RedisVentStream.publish: call connect() first");
    }
    const { envelope, subject } = buildPublishMessage(args);
    const key = redisStreamKey(this.cfg.keyPrefix ?? DEFAULT_KEY_PREFIX, args.tenant);
    const command = buildRedisXAddCommand(
      key,
      subject,
      JSON.stringify(envelope),
      this.cfg.maxLength,
    );
    const streamId = await this.client.sendCommand(command);
    if (typeof streamId !== "string") {
      throw new Error("RedisVentStream.publish: XADD returned no stream ID");
    }
    return {
      id: envelope.id,
      subject,
      streamId,
      cursor: `rs:${streamId}`,
    };
  }
}

/** Derive the Redis Cluster-safe stream key consumed by the gateways. */
export function redisStreamKey(prefix: string, tenant: string): string {
  return `${prefix}:{${tenant}}:events`;
}

/** Build the exact XADD command. Exported for protocol contract tests. */
export function buildRedisXAddCommand(
  key: string,
  subject: string,
  eventJson: string,
  maxLength?: number,
): string[] {
  const command = ["XADD", key];
  if (maxLength !== undefined) {
    command.push("MAXLEN", "~", String(maxLength));
  }
  command.push("*", "subject", subject, "event", eventJson);
  return command;
}
