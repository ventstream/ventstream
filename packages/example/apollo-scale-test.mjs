// Apollo-path scale test: N graphql-transport-ws connections (the exact wire
// protocol Apollo Client's graphql-ws link speaks), each with a subscription,
// then publish EVENTS — verify every connection receives all of them.
// Raw protocol (no Apollo lib). env: TARGET, EVENTS.
import { connect } from "nats";

const GQL = "ws://127.0.0.1:4041/graphql/ws";
const TARGET = parseInt(process.env.TARGET || "1000", 10);
const EVENTS = parseInt(process.env.EVENTS || "20", 10);
const SUBS = parseInt(process.env.SUBS || "0", 10); // extra noise graphql subs per conn (each = its own consumer)
const BATCH = 100;
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random() * 8)]; for (let i = 0; i < 25; i++) s += B32[Math.floor(Math.random() * 32)]; return s; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const nc = await connect({ servers: "nats://127.0.0.1:4222" });
async function pubVerify(k) {
  const now = new Date().toISOString();
  const env = { id: ulid(), event: "verify.evt", tenant: "acme", entity_id: `v${k}`, occurred_at: now, received_at: now, schema_version: 2, data: { k } };
  nc.publish(`vs.t.acme.verify.evt.v${k}`, enc.encode(JSON.stringify(env)));
}

let ack = 0, errors = 0;
const conns = [];
const QUERY = 'subscription { events(subject: "verify.>") { id event entityId data } }';
function mk() {
  return new Promise((resolve) => {
    let ws; try { ws = new WebSocket(GQL, "graphql-transport-ws"); } catch { errors++; return resolve(); }
    const st = { got: new Set(), ws }; let settled = false;
    const done = () => { if (!settled) { settled = true; resolve(); } };
    ws.addEventListener("open", () => ws.send(JSON.stringify({ type: "connection_init", payload: { authToken: "demo", tenant: "acme" } })));
    ws.addEventListener("message", (e) => {
      const m = JSON.parse(String(e.data));
      if (m.type === "connection_ack") {
        ws.send(JSON.stringify({ type: "subscribe", id: "v", payload: { query: QUERY } }));
        // extra noise graphql subscriptions — each becomes its own consumer
        for (let s = 0; s < SUBS; s++) {
          ws.send(JSON.stringify({ type: "subscribe", id: `n${s}`, payload: { query: `subscription { events(subject: "n${s}.>") { id event } }` } }));
        }
        ack++; conns.push(st); done();
      }
      else if (m.type === "next") { const ev = m.payload?.data?.events; if (ev && ev.event === "verify.evt") st.got.add(ev.data?.k); }
      else if (m.type === "error" || m.type === "connection_error") { errors++; if (process.env.DEBUG) console.log("gql err:", JSON.stringify(m)); done(); }
    });
    ws.addEventListener("error", () => { errors++; done(); });
    ws.addEventListener("close", () => done());
    setTimeout(done, 8000);
  });
}

console.log(`ramping ${TARGET} graphql-transport-ws conns ...`);
const t0 = Date.now();
for (let b = 0; b * BATCH < TARGET; b++) {
  const ps = []; for (let k = 0; k < BATCH && b * BATCH + k < TARGET; k++) ps.push(mk());
  await Promise.all(ps); await sleep(60);
}
console.log(`ramp ${((Date.now() - t0) / 1000).toFixed(1)}s — connection_ack=${ack}/${TARGET} errors=${errors}`);
await sleep(2000);

console.log(`publishing ${EVENTS} verify events ...`);
for (let k = 0; k < EVENTS; k++) { await pubVerify(k); await nc.flush(); }
await sleep(5000);

const counts = conns.map((c) => c.got.size).sort((a, b) => a - b);
const full = counts.filter((n) => n >= EVENTS).length;
const pct = (q) => counts.length ? counts[Math.min(counts.length - 1, Math.floor(q * counts.length))] : 0;
console.log("\n=== RESULT (Apollo / graphql-transport-ws) ===");
console.log(`connection_ack: ${ack}/${TARGET}  errors: ${errors}`);
console.log(`events per conn (expect ${EVENTS}): min=${counts[0] ?? 0} p50=${pct(0.5)} p99=${pct(0.99)} max=${counts[counts.length - 1] ?? 0}`);
console.log(`conns that got ALL ${EVENTS}: ${full}/${ack} (${ack ? (100 * full / ack).toFixed(1) : 0}%)`);
await nc.drain(); process.exit(0);
