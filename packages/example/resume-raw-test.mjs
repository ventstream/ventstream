// Raw protocol resume test — NO @ventstream SDK. Plain WebSocket + raw NATS,
// exactly how a real client/publisher would talk to the gateway. Proves that
// after the resume fix, a reconnecting client RECOVERS the gap-window it
// missed (deliver via resume_from_seq → ByStartSequence), deduped.
import { connect } from "nats";

const WS = "ws://127.0.0.1:4040/ws";
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

// Raw resume-capable client: tracks last stream seq, sends it on reconnect,
// dedups by event id.
class RawClient {
  constructor(url) { this.url = url; this.lastSeq = 0; this.seen = new Set(); this.received = []; }
  open() {
    return new Promise((resolve) => {
      const ws = new WebSocket(this.url); this.ws = ws;
      ws.addEventListener("open", () => {
        const h = { type: "hello", tenant: "acme", token: "demo" };
        if (this.lastSeq > 0) h.resume_from_seq = this.lastSeq;  // ← resume
        ws.send(JSON.stringify(h));
      });
      ws.addEventListener("message", (e) => {
        const m = JSON.parse(String(e.data));
        if (m.type === "ready") {
          ws.send(JSON.stringify({ type: "subscribe", id: "s1", pattern: "orders.>" }));
          resolve(m);
        } else if (m.type === "event") {
          if (typeof m.seq === "number" && m.seq > this.lastSeq) this.lastSeq = m.seq;
          const id = m.event?.id;
          if (id) { if (this.seen.has(id)) return; this.seen.add(id); }
          this.received.push(m.event.data.seq);
        }
      });
      ws.addEventListener("error", () => {});
    });
  }
  closeSocket() { return new Promise((res) => { if (!this.ws) return res(); this.ws.addEventListener("close", () => res()); this.ws.close(); }); }
}

const c = new RawClient(WS);
await c.open(); await sleep(700);

console.log("Phase 1: publish 1-5 (connected)");
for (let s = 1; s <= 5; s++) await pub(s);
await sleep(1500);
console.log("  received:", [...c.received].sort((a, b) => a - b), "lastSeq:", c.lastSeq);

console.log("Phase 2: DISCONNECT");
await c.closeSocket(); await sleep(500);

console.log("Phase 3 (GAP): publish 6-10 (disconnected)");
for (let s = 6; s <= 10; s++) await pub(s);
await sleep(1500);

console.log(`Phase 4: RECONNECT with resume_from_seq=${c.lastSeq}`);
await c.open(); await sleep(1800);

console.log("Phase 5: publish 11-15 (reconnected)");
for (let s = 11; s <= 15; s++) await pub(s);
await sleep(1500);

const all = [...new Set(c.received)].sort((a, b) => a - b);
const recovered = [6, 7, 8, 9, 10].filter((s) => all.includes(s));
const dupes = c.received.length - new Set(c.received).size;
console.log("\n=== RESULT ===");
console.log("all received:", all);
console.log(`gap RECOVERED (expect [6,7,8,9,10]): ${JSON.stringify(recovered)}`);
console.log(`duplicates delivered to app (expect 0): ${dupes}`);
await nc.drain(); process.exit(0);
