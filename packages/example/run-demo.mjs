// End-to-end demo using the published SDKs:
//
//   @ventstream/sdk     -> publishes events to NATS
//   @ventstream/client  -> subscribes via WS gateway
//
// Run (with engine + NATS already up):
//   node packages/example/run-demo.mjs

import { VentStream } from "@ventstream/sdk";
import { VentStreamClient } from "@ventstream/client";

const tenant = "acme";

// 1. WS client subscribes
const client = new VentStreamClient({
  url: process.env.VS_WS_URL || "ws://127.0.0.1:4040/ws",
  tenant,
  token: "demo",
  onError: (e) => console.error("[client] error:", e),
  onClose: (r) => console.log("[client] closed:", r),
});

await client.connect();
console.log("[client] connected and ready");

const received = [];
const done = new Promise((resolve) => {
  let n = 0;
  client.subscribe("orders.order.status_changed.*", (event, ctx) => {
    console.log(
      `[client] event on ${ctx.subject}:`,
      JSON.stringify({
        id: event.id,
        entity_id: event.entity_id,
        data: event.data,
      }),
    );
    received.push(event);
    n += 1;
    if (n >= 3) resolve();
  });
});

// Give the subscribe ack a beat
await new Promise((r) => setTimeout(r, 200));

// 2. Publisher emits three events
const vs = new VentStream({
  servers: process.env.VS_NATS_URL || "nats://127.0.0.1:4222",
  name: "demo-publisher",
});
await vs.connect();
console.log("[publisher] connected");

for (let i = 1; i <= 3; i++) {
  const { id, subject } = await vs.publish({
    tenant,
    domain: "orders",
    action: "status_changed",
    entity: { kind: "order", id: `order_${i}` },
    actor: { kind: "user", id: "user_456" },
    data: { from: "pending", to: "confirmed", attempt: i },
  });
  console.log(`[publisher] sent id=${id} subject=${subject}`);
}

await done;
await vs.close();
await client.close();
console.log(`\n[demo] received ${received.length} events end-to-end ✓`);
process.exit(0);
