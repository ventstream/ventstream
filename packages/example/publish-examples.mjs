// Realistic developer-facing examples of publishing with @ventstream/sdk.
//
// This file is documentation-as-code — every example below is something
// an application server might actually emit. The interesting bits are
// the *shape* of each call, not the data.

import { VentStream } from "@ventstream/sdk";
import { VentStreamClient } from "@ventstream/client";

// Connect once at startup. The instance is safe to reuse across the
// whole app lifetime; nats.js handles reconnects internally.
const vs = new VentStream({
  servers: "nats://127.0.0.1:4222",
  name: "orders-api", // shows up in NATS server logs
});
await vs.connect();

// We also open a WS subscriber so we can see what lands on the bus.
const client = new VentStreamClient({
  url: "ws://127.0.0.1:4040/ws",
  tenant: "acme",
  token: "demo",
});
await client.connect();
client.subscribe(">", (event, ctx) => {
  console.log(`     ← ${ctx.subject}`);
  console.log(`        event=${event.event}`);
  console.log(`        entity_id=${event.entity_id}`);
  console.log(`        actor=${JSON.stringify(event.actor)}`);
  console.log(`        data=${JSON.stringify(event.data)}`);
});
await new Promise((r) => setTimeout(r, 200));

// ============================================================
// Example 1: a user creates an order.
// ============================================================
console.log("\n— Example 1: user creates an order —");
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "created",
  entity: { kind: "order", id: "ord_01HZX" },
  actor: { kind: "user", id: "usr_alice" },
  data: {
    total_cents: 12_995,
    currency: "USD",
    line_items: [
      { sku: "WIDGET-001", qty: 2, unit_cents: 4995 },
      { sku: "WIDGET-002", qty: 1, unit_cents: 3005 },
    ],
  },
});
// Subject: vs.t.acme.orders.order.created.ord_01HZX
// Event:   orders.order.created

// ============================================================
// Example 2: the same order moves to "confirmed."
// ============================================================
console.log("\n— Example 2: order status changes —");
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "status_changed",
  entity: { kind: "order", id: "ord_01HZX" },
  actor: { kind: "user", id: "usr_alice" },
  data: { from: "pending", to: "confirmed" },
});

// ============================================================
// Example 3: a *system* actor — a nightly cron emitting a summary.
// Audit logs need to know what caused this; `actor.kind = "system"`
// with a descriptive id makes it explicit.
// ============================================================
console.log("\n— Example 3: system actor (nightly cron) —");
await vs.publish({
  tenant: "acme",
  domain: "billing",
  action: "summary_generated",
  // No single business "entity" for a global summary — we model the
  // run itself as the entity so the subject grammar stays uniform.
  entity: { kind: "report", id: "billing_2026_05_23" },
  actor: { kind: "system", id: "nightly-billing-cron" },
  data: {
    orders_processed: 1247,
    revenue_cents: 9_823_440,
    period_start: "2026-05-22T00:00:00Z",
    period_end: "2026-05-23T00:00:00Z",
  },
});

// ============================================================
// Example 4: an event with trace correlation. The SDK accepts a
// `metadata` block; consumers (audit log, distributed tracing) read
// from it.
// ============================================================
console.log("\n— Example 4: event carrying trace context —");
await vs.publish({
  tenant: "acme",
  domain: "users",
  action: "profile_updated",
  entity: { kind: "user", id: "usr_alice" },
  actor: { kind: "user", id: "usr_alice" }, // self-action
  data: {
    fields_changed: ["email", "marketing_opt_in"],
  },
  metadata: {
    trace_id: "0af7651916cd43dd8448eb211c80319c",
    correlation_id: "req_abc123",
    causation_id: "01KSAH5XYZ", // a previous event that triggered this
  },
});

// ============================================================
// Example 5: occurredAt vs received_at — a backfill or delayed
// publish. occurred_at records *when the business event actually
// happened*; received_at is when the SDK shipped it.
// ============================================================
console.log("\n— Example 5: backfill event from yesterday —");
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "refunded",
  entity: { kind: "order", id: "ord_01HYY" },
  actor: { kind: "system", id: "refund-reconciler" },
  occurredAt: new Date("2026-05-22T14:30:00Z"), // ← yesterday
  data: { amount_cents: 4_995, reason: "duplicate_charge" },
});

// ============================================================
// Example 6: what gets rejected at publish time.
// The SDK enforces the subject grammar before any bytes hit the bus.
// ============================================================
console.log("\n— Example 6: rejected at the SDK boundary —");
try {
  await vs.publish({
    tenant: "ACME", // ← uppercase rejected
    domain: "orders",
    action: "created",
    entity: { kind: "order", id: "x" },
    actor: { kind: "user", id: "u" },
    data: {},
  });
  console.log("        UNEXPECTED: bad tenant accepted");
} catch (err) {
  console.log(`        ✓ rejected: ${err.message}`);
}

try {
  await vs.publish({
    tenant: "acme",
    domain: "orders",
    action: "Status Changed", // ← spaces + capitals rejected
    entity: { kind: "order", id: "x" },
    actor: { kind: "user", id: "u" },
    data: {},
  });
  console.log("        UNEXPECTED: bad action accepted");
} catch (err) {
  console.log(`        ✓ rejected: ${err.message}`);
}

try {
  await vs.publish({
    tenant: "acme",
    domain: "orders",
    action: "created",
    entity: { kind: "order", id: "ord.with.dots" }, // ← dots forbidden
    actor: { kind: "user", id: "u" },
    data: {},
  });
  console.log("        UNEXPECTED: bad entity id accepted");
} catch (err) {
  console.log(`        ✓ rejected: ${err.message}`);
}

// Wait for delivery, then close.
await new Promise((r) => setTimeout(r, 600));
await vs.close();
await client.close();
console.log("\ndone.");
