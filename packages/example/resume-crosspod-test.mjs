// CROSS-POD resume test (raw protocol, no SDK). Client connects to pod A,
// disconnects, gap is published, client reconnects to a DIFFERENT pod B and
// must recover the gap — proving any pod serves the resume from the shared
// JetStream stream (no cross-pod consumer ownership).
import { connect } from "nats";

const POD_A = "ws://127.0.0.1:6040/ws";   // port-forward → pod A
const POD_B = "ws://127.0.0.1:6041/ws";   // port-forward → pod B (different!)
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random() * 8)]; for (let i = 0; i < 25; i++) s += B32[Math.floor(Math.random() * 32)]; return s; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const nc = await connect({ servers: "nats://127.0.0.1:6222" });
async function pub(seq) {
  const now = new Date().toISOString();
  const env = { id: ulid(), event: "orders.order.created", tenant: "acme", entity_id: `ord_${seq}`, occurred_at: now, received_at: now, schema_version: 2, data: { seq } };
  nc.publish(`vs.t.acme.orders.order.created.ord_${seq}`, enc.encode(JSON.stringify(env)));
  await nc.flush();
}

const state = { lastSeq: 0, seen: new Set(), received: [] };
function open(url) {
  return new Promise((resolve) => {
    const ws = new WebSocket(url); state.ws = ws;
    ws.addEventListener("open", () => {
      const h = { type: "hello", tenant: "acme", token: "demo" };
      if (state.lastSeq > 0) h.resume_from_seq = state.lastSeq;
      ws.send(JSON.stringify(h));
    });
    ws.addEventListener("message", (e) => {
      const m = JSON.parse(String(e.data));
      if (m.type === "ready") { ws.send(JSON.stringify({ type: "subscribe", id: "s1", pattern: "orders.>" })); resolve(m); }
      else if (m.type === "event") {
        if (typeof m.seq === "number" && m.seq > state.lastSeq) state.lastSeq = m.seq;
        const id = m.event?.id; if (id) { if (state.seen.has(id)) return; state.seen.add(id); }
        state.received.push(m.event.data.seq);
      }
    });
    ws.addEventListener("error", () => {});
  });
}
const closeSocket = () => new Promise((res) => { if (!state.ws) return res(); state.ws.addEventListener("close", () => res()); state.ws.close(); });

console.log("Phase 1: connect to POD A, publish 1-5");
const r1 = await open(POD_A); await sleep(700);
for (let s = 1; s <= 5; s++) await pub(s);
await sleep(1500);
console.log("  received on A:", [...state.received].sort((a, b) => a - b), "lastSeq:", state.lastSeq);

console.log("Phase 2: DISCONNECT from A");
await closeSocket(); await sleep(500);

console.log("Phase 3 (GAP): publish 6-10 while disconnected");
for (let s = 6; s <= 10; s++) await pub(s);
await sleep(1500);

console.log(`Phase 4: RECONNECT to POD B (different pod) with resume_from_seq=${state.lastSeq}`);
const r2 = await open(POD_B); await sleep(1800);

console.log("Phase 5: publish 11-15 (now on B)");
for (let s = 11; s <= 15; s++) await pub(s);
await sleep(1500);

const all = [...new Set(state.received)].sort((a, b) => a - b);
const recovered = [6, 7, 8, 9, 10].filter((s) => all.includes(s));
const dupes = state.received.length - new Set(state.received).size;
console.log("\n=== RESULT (cross-pod) ===");
console.log("all received:", all);
console.log(`gap RECOVERED on a different pod (expect [6,7,8,9,10]): ${JSON.stringify(recovered)}`);
console.log(`duplicates (expect 0): ${dupes}`);
await nc.drain(); process.exit(0);
