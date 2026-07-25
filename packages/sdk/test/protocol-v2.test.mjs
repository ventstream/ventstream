import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { buildPublishMessage } from "../dist/client.js";
import {
  RedisVentStream,
  buildRedisXAddCommand,
  redisStreamKey,
} from "../dist/redis-client.js";

const fixture = JSON.parse(
  readFileSync(
    new URL("../../../testdata/realtime-event-v2.json", import.meta.url),
    "utf8",
  ),
);

test("builds the Rust protocol-v2 envelope and ID-last subject", () => {
  const { envelope, subject } = buildPublishMessage(
    {
      tenant: "acme",
      domain: "orders",
      action: "status_changed",
      entity: { kind: "order", id: "order_123" },
      data: { from: "pending", to: "confirmed" },
    },
    {
      id: "01KSA000000000000000000000",
      now: new Date("2026-05-23T03:55:01.000Z"),
    },
  );

  assert.equal(subject, "vs.t.acme.orders.order.status_changed.order_123");
  assert.deepEqual(envelope, fixture);
  assert.equal("type" in envelope, false);
  assert.equal("entity" in envelope, false);
  assert.equal("actor" in envelope, false);
});

test("includes optional actor and preserves business-event time", () => {
  const { envelope } = buildPublishMessage(
    {
      tenant: "acme",
      domain: "orders",
      action: "created",
      entity: { kind: "order", id: "order_456" },
      actor: { kind: "user", id: "user_1" },
      occurredAt: new Date("2026-05-23T03:54:00.000Z"),
      data: {},
    },
    {
      id: "01KSA000000000000000000001",
      now: new Date("2026-05-23T03:55:01.000Z"),
    },
  );

  assert.deepEqual(envelope.actor, { kind: "user", id: "user_1" });
  assert.equal(envelope.occurred_at, "2026-05-23T03:54:00.000Z");
});

test("builds the Redis Streams key and atomic bounded XADD command", () => {
  const key = redisStreamKey("ventstream", "acme");
  assert.equal(key, "ventstream:{acme}:events");
  assert.deepEqual(
    buildRedisXAddCommand(
      key,
      "vs.t.acme.orders.order.created.order_1",
      '{"id":"event-1"}',
      100_000,
    ),
    [
      "XADD",
      "ventstream:{acme}:events",
      "MAXLEN",
      "~",
      "100000",
      "*",
      "subject",
      "vs.t.acme.orders.order.created.order_1",
      "event",
      '{"id":"event-1"}',
    ],
  );
});

test("rejects unsafe Redis publisher configuration before connecting", () => {
  assert.throws(
    () => new RedisVentStream({ url: "" }),
    /url must not be empty/,
  );
  assert.throws(
    () =>
      new RedisVentStream({
        url: "redis://127.0.0.1:6379",
        keyPrefix: "bad{prefix}",
      }),
    /keyPrefix/,
  );
  assert.throws(
    () =>
      new RedisVentStream({
        url: "redis://127.0.0.1:6379",
        maxLength: 0,
      }),
    /maxLength/,
  );
});
