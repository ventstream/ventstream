// Deterministic scenario tests for the WS gateway.
//
// Each scenario prints PASS / FAIL with a one-line reason; the script
// exits non-zero if any scenario fails. Run with NATS + engine already up.

import { VentStream } from "@ventstream/sdk";
import { VentStreamClient } from "@ventstream/client";
import WebSocket from "ws";

const WS_URL = process.env.VS_WS_URL || "ws://127.0.0.1:4040/ws";
const NATS_URL = process.env.VS_NATS_URL || "nats://127.0.0.1:4222";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
const record = (name, ok, detail) => {
  results.push({ name, ok, detail });
  console.log(`${ok ? "✅" : "❌"} ${name}${detail ? "  — " + detail : ""}`);
};

// Helper: open a client and return a handle. Tracks all received events.
async function makeClient(tenant = "acme") {
  const events = [];
  const errors = [];
  const closes = [];
  const c = new VentStreamClient({
    url: WS_URL,
    tenant,
    token: "demo",
    onError: (e) => errors.push(e),
    onClose: (r) => closes.push(r),
  });
  await c.connect();
  return {
    client: c,
    events,
    errors,
    closes,
    subscribe(pattern) {
      return c.subscribe(pattern, (ev, ctx) =>
        events.push({ subject: ctx.subject, event: ev }),
      );
    },
    async close() {
      await c.close();
    },
  };
}

async function makePublisher() {
  const p = new VentStream({ servers: NATS_URL, name: "scenario-publisher" });
  await p.connect();
  return p;
}

function makeEvent(suffix = "x", action = "status_changed") {
  return {
    tenant: "acme",
    domain: "orders",
    action,
    entity: { kind: "order", id: `order_${suffix}` },
    actor: { kind: "user", id: "user_456" },
    data: { suffix },
  };
}

// ---------------------------------------------------------------------------

async function scenarioFanOutMultipleClients() {
  const name = "1. fan-out: 3 clients each receive the same event";
  const a = await makeClient();
  const b = await makeClient();
  const c = await makeClient();
  a.subscribe("orders.>");
  b.subscribe("orders.>");
  c.subscribe("orders.>");
  await sleep(200);

  const p = await makePublisher();
  await p.publish(makeEvent("multi"));
  await sleep(400);

  const ok =
    a.events.length === 1 && b.events.length === 1 && c.events.length === 1;
  record(
    name,
    ok,
    ok ? null : `got a=${a.events.length} b=${b.events.length} c=${c.events.length}`,
  );

  await p.close();
  await a.close();
  await b.close();
  await c.close();
}

async function scenarioUnsubscribeStopsDelivery() {
  const name = "2. unsubscribe: events stop after unsub";
  const h = await makeClient();
  const sub = h.subscribe("orders.>");
  await sleep(150);

  const p = await makePublisher();
  await p.publish(makeEvent("u1"));
  await sleep(200);
  const beforeUnsub = h.events.length;

  sub.unsubscribe();
  await sleep(150);
  await p.publish(makeEvent("u2"));
  await sleep(300);
  const afterUnsub = h.events.length;

  const ok = beforeUnsub === 1 && afterUnsub === 1;
  record(
    name,
    ok,
    ok ? null : `beforeUnsub=${beforeUnsub} afterUnsub=${afterUnsub}`,
  );

  await p.close();
  await h.close();
}

async function scenarioMultipleSubsOnOneConnection() {
  const name = "3. multiple subs on one connection — each fires independently";
  const h = await makeClient();
  const createdEvents = [];
  const deletedEvents = [];
  h.client.subscribe("orders.order.created.*", (ev) => createdEvents.push(ev));
  h.client.subscribe("orders.order.deleted.*", (ev) => deletedEvents.push(ev));
  await sleep(200);

  const p = await makePublisher();
  await p.publish(makeEvent("c1", "created"));
  await p.publish(makeEvent("d1", "deleted"));
  await p.publish(makeEvent("c2", "created"));
  await sleep(400);

  const ok = createdEvents.length === 2 && deletedEvents.length === 1;
  record(
    name,
    ok,
    ok ? null : `created=${createdEvents.length} deleted=${deletedEvents.length}`,
  );

  await p.close();
  await h.close();
}

async function scenarioReconnectAndReplay() {
  const name = "4. reconnect + subscription replay (rip the WS, expect resume)";
  const h = await makeClient();
  h.subscribe("orders.>");
  await sleep(200);

  // Forcefully tear down the underlying socket. The client will
  // reconnect on its own (default) and replay the subscription.
  // We access the private field intentionally for this test only.
  const internalWs = h.client.ws;
  if (internalWs) internalWs.close();
  await sleep(800); // backoff + reconnect

  const p = await makePublisher();
  await p.publish(makeEvent("rec"));
  await sleep(500);

  const ok = h.events.length === 1;
  record(name, ok, ok ? null : `events received after reconnect=${h.events.length}`);

  await p.close();
  await h.close();
}

async function scenarioBurstThroughput() {
  const name = "5. burst: 500 events delivered without loss";
  const h = await makeClient();
  h.subscribe("orders.>");
  await sleep(200);

  const p = await makePublisher();
  const N = 500;
  for (let i = 0; i < N; i++) {
    await p.publish(makeEvent(`b${i}`));
  }
  // Allow drain
  await sleep(1500);

  const ok = h.events.length === N;
  record(name, ok, ok ? null : `got ${h.events.length} of ${N}`);

  await p.close();
  await h.close();
}

async function scenarioWrongTenantBlocked() {
  const name = "6. tenant gate: event for other tenant not delivered";
  const h = await makeClient("acme");
  h.subscribe("orders.>");
  await sleep(200);

  const p = await makePublisher();
  await p.publish({
    tenant: "other",
    domain: "orders",
    action: "status_changed",
    entity: { kind: "order", id: "x" },
    actor: { kind: "user", id: "u" },
    data: {},
  });
  await sleep(400);

  const ok = h.events.length === 0;
  record(name, ok, ok ? null : `leaked ${h.events.length} events`);

  await p.close();
  await h.close();
}

async function scenarioHelloMissing() {
  // Drop straight into a raw WS to confirm the server rejects a
  // non-Hello first frame. SDK won't let us do this, so go raw.
  const name = "7. protocol: first frame must be Hello — non-Hello rejected";
  return new Promise((resolve) => {
    const ws = new WebSocket(WS_URL);
    let gotError = false;
    let gotClose = false;
    ws.addEventListener("open", () => {
      ws.send(
        JSON.stringify({ type: "subscribe", id: "x", pattern: "orders.>" }),
      );
    });
    ws.addEventListener("message", (evt) => {
      const raw = typeof evt.data === "string" ? evt.data : String(evt.data);
      const msg = JSON.parse(raw);
      if (msg.type === "error") gotError = true;
    });
    ws.addEventListener("close", () => {
      gotClose = true;
      const ok = gotError && gotClose;
      record(name, ok, ok ? null : `error=${gotError} close=${gotClose}`);
      resolve();
    });
  });
}

async function scenarioSlowConsumerDisconnected() {
  // Raw WS so we can pause reading to stall the client. With
  // VS_WS_MAILBOX=64 we flood enough events that even after OS-level
  // TCP/WS buffers fill, the engine's per-connection mailbox
  // overflows and the connection is force-closed.
  const name = "9. slow consumer: paused reader gets force-closed";
  return new Promise((resolve) => {
    const ws = new WebSocket(WS_URL);
    const start = Date.now();
    let recorded = false;
    let subAcked = false;
    const finish = (ok, detail) => {
      if (recorded) return;
      recorded = true;
      record(name, ok, detail);
      try { ws.terminate(); } catch {}
      resolve();
    };

    ws.on("open", () =>
      ws.send(JSON.stringify({ type: "hello", tenant: "acme", token: "demo" })),
    );
    ws.on("message", async (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "ready") {
        ws.send(
          JSON.stringify({ type: "subscribe", id: "s", pattern: "orders.>" }),
        );
      }
      if (msg.type === "subscribed") {
        subAcked = true;
        ws._socket.pause(); // stop draining frames
        const p = await makePublisher();
        for (let i = 0; i < 2000; i++) await p.publish(makeEvent(`slow${i}`));
        await p.close();
        // Give the engine ~1s to detect mailbox overflow and emit
        // a close. Then resume the socket so the close frame and
        // TCP FIN are actually read by our event loop. Without this
        // resume, the close sits in the OS buffer forever.
        setTimeout(() => {
          try { ws._socket.resume(); } catch {}
        }, 1000);
      }
    });
    ws.on("close", () => {
      const elapsed = Date.now() - start;
      finish(subAcked, subAcked ? `closed in ${elapsed}ms` : "never subscribed");
    });
    // Safety net at 30s.
    setTimeout(() => finish(false, "server did not close within 30s"), 30_000);
  });
}

async function scenarioCleanClose() {
  const name = "8. clean close: client.close() exits without dangling timers";
  const h = await makeClient();
  h.subscribe("orders.>");
  await sleep(150);

  const beforeClose = Date.now();
  await h.close();
  const elapsed = Date.now() - beforeClose;

  const ok = elapsed < 500;
  record(name, ok, ok ? `closed in ${elapsed}ms` : `close took ${elapsed}ms`);
}

// ---------------------------------------------------------------------------

const all = [
  scenarioFanOutMultipleClients,
  scenarioUnsubscribeStopsDelivery,
  scenarioMultipleSubsOnOneConnection,
  scenarioReconnectAndReplay,
  scenarioBurstThroughput,
  scenarioWrongTenantBlocked,
  scenarioHelloMissing,
  scenarioSlowConsumerDisconnected,
  scenarioCleanClose,
];

for (const s of all) {
  try {
    await s();
  } catch (err) {
    record(s.name, false, `threw: ${err.message}`);
  }
}

const failures = results.filter((r) => !r.ok);
console.log(`\n${results.length - failures.length}/${results.length} scenarios passed`);
if (failures.length > 0) {
  for (const f of failures) console.log(`  FAIL: ${f.name}  ${f.detail ?? ""}`);
  process.exit(1);
}
process.exit(0);
