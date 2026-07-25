// Proves VentStream WS JetStream-mode reconnect behaviour with valid envelopes
// published straight to NATS. Each event carries data.seq.
import { connect } from "nats";
import { VentStreamClient } from "@ventstream/client";

const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random() * 8)]; for (let i = 0; i < 25; i++) s += B32[Math.floor(Math.random() * 32)]; return s; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const nc = await connect({ servers: "nats://127.0.0.1:4222" });
async function pub(seq) {
  const now = new Date().toISOString();
  const env = { id: ulid(), event: "orders.order.created", tenant: "acme", entity_id: `ord_${seq}`, occurred_at: now, received_at: now, schema_version: 2, data: { seq } };
  nc.publish(`vs.t.acme.orders.order.created.ord_${seq}`, enc.encode(JSON.stringify(env)));
  await nc.flush();
}

const got1 = new Set(), got2 = new Set();

const c1 = new VentStreamClient({ url: "ws://127.0.0.1:4040/ws", tenant: "acme", token: "demo", reconnect: false });
await c1.connect();
c1.subscribe("orders.>", (ev) => got1.add(ev.data.seq));
await sleep(700);

console.log("Phase 1: publish seq 1-5 WHILE CONNECTED");
for (let s = 1; s <= 5; s++) await pub(s);
await sleep(1500);
console.log("  client1 received:", [...got1].sort((a, b) => a - b));

console.log("Phase 2: DISCONNECT client1");
await c1.close();
await sleep(700);

console.log("Phase 3 (GAP): publish seq 6-10 WHILE DISCONNECTED");
for (let s = 6; s <= 10; s++) await pub(s);
await sleep(1500);

console.log("Phase 4: RECONNECT as fresh connection (client2) + subscribe");
const c2 = new VentStreamClient({ url: "ws://127.0.0.1:4040/ws", tenant: "acme", token: "demo", reconnect: false });
await c2.connect();
c2.subscribe("orders.>", (ev) => got2.add(ev.data.seq));
await sleep(900);

console.log("Phase 5: publish seq 11-15 WHILE RECONNECTED");
for (let s = 11; s <= 15; s++) await pub(s);
await sleep(1500);
console.log("  client2 received:", [...got2].sort((a, b) => a - b));

const all = new Set([...got1, ...got2]);
const steady = [1, 2, 3, 4, 5, 11, 12, 13, 14, 15].filter((s) => all.has(s));
const gapLost = [6, 7, 8, 9, 10].filter((s) => !all.has(s));
console.log("\n=== RESULT ===");
console.log(`steady-state delivered (expect 10/10): ${steady.length}/10  ${JSON.stringify(steady)}`);
console.log(`gap-window LOST (expect [6,7,8,9,10]):  ${JSON.stringify(gapLost)}`);
await nc.drain(); await c2.close();
process.exit(0);
