// H13 repro: quiet GraphQL subscription must NOT get duplicate deliveries.
// A connection that receives < ACK_EVERY (64) messages then goes idle left
// its tail unacked; after the consumer's ack_wait (30s) JetStream redelivered
// → duplicates. The idle-ack flush fix should keep each event delivered once.
//
// Subscribe orderEvents(orderId:"42") -> subject vs.t.acme.orderStatusChanged.42.
// Publish 10 unique events, then idle 40s (> ack_wait). PASS = 0 duplicates.
import { connect } from "nats";

const GQL = process.env.GQL_URL || "ws://127.0.0.1:4041/graphql/ws";
const NATS = process.env.NATS_URL || "nats://127.0.0.1:4222";
const N = parseInt(process.env.N || "10", 10);          // < 64 so the tail never hits ACK_EVERY
const IDLE_MS = parseInt(process.env.IDLE_MS || "40000", 10); // > 30s ack_wait
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random()*8)]; for (let i=0;i<25;i++) s+=B32[Math.floor(Math.random()*32)]; return s; };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const counts = new Map(); // event id -> times delivered
let acked = false;

const ws = new WebSocket(GQL, "graphql-transport-ws");
await new Promise((res, rej) => {
  ws.addEventListener("open", () => ws.send(JSON.stringify({ type: "connection_init", payload: { authToken: "demo", tenant: "acme" } })));
  ws.addEventListener("error", rej);
  ws.addEventListener("message", (e) => {
    const m = JSON.parse(String(e.data));
    if (m.type === "connection_ack") {
      acked = true;
      ws.send(JSON.stringify({ type: "subscribe", id: "1", payload: { query: 'subscription { orderEvents(orderId: "42") { id } }' } }));
      res();
    } else if (m.type === "ping") {
      ws.send(JSON.stringify({ type: "pong" }));
    } else if (m.type === "next" && m.id === "1") {
      const id = m.payload?.data?.orderEvents?.id;
      if (id) counts.set(id, (counts.get(id) || 0) + 1);
    } else if (m.type === "error") {
      console.log("SUBSCRIBE ERROR:", JSON.stringify(m.payload));
    }
  });
  setTimeout(() => acked ? res() : rej(new Error("no connection_ack")), 8000);
});
console.log("connected + subscribed (orderEvents/42)");
await sleep(800); // let the subscription register before publishing

const nc = await connect({ servers: NATS });
const ids = [];
for (let i = 0; i < N; i++) {
  const id = ulid(); ids.push(id);
  const now = new Date().toISOString();
  const env = { id, event: "orderStatusChanged", tenant: "acme", entity_id: `42`, occurred_at: now, received_at: now, schema_version: 2, data: { status: `s${i}` } };
  nc.publish(`vs.t.acme.orderStatusChanged.42`, enc.encode(JSON.stringify(env)));
}
await nc.flush();
console.log(`published ${N} events; waiting ${IDLE_MS/1000}s idle (> 30s ack_wait) to expose redelivery...`);
await sleep(IDLE_MS);

const delivered = [...counts.values()].reduce((a,b)=>a+b,0);
const dupes = [...counts.entries()].filter(([,c]) => c > 1);
console.log(JSON.stringify({
  published: N,
  unique_ids_delivered: counts.size,
  total_deliveries: delivered,
  duplicate_ids: dupes.length,
  sample_dupes: dupes.slice(0,3),
  result: (counts.size === N && dupes.length === 0) ? "PASS (each event once, no redelivery)" : "FAIL (duplicates or missing)"
}, null, 0));
ws.close(); await nc.drain(); process.exit(0);
