// "Onyx at scale": N connections, each installing MANY subscriptions (one
// shared `verify.>` + SUBS noise patterns), then a publisher emits EVENTS on
// `verify.*`. Every connection should receive every verify event. Measures
// delivery completeness + ramp at thousands of connections × many subs.
// Raw protocol (no SDK). Run: bun onyx-scale-test.mjs   (env: TARGET, SUBS, EVENTS)
import { connect } from "nats";

const WS = "ws://127.0.0.1:4040/ws";
const TARGET = parseInt(process.env.TARGET || "1000", 10);
const SUBS = parseInt(process.env.SUBS || "50", 10);   // noise subs per connection
const EVENTS = parseInt(process.env.EVENTS || "20", 10);
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

let ready = 0, errors = 0;
const conns = []; // {got:Set}
function mk(i) {
  return new Promise((resolve) => {
    let ws; try { ws = new WebSocket(WS); } catch { errors++; return resolve(); }
    const st = { got: new Set(), ws };
    let settled = false;
    ws.addEventListener("open", () => {
      ws.send(JSON.stringify({ type: "hello", tenant: "acme", token: "demo" }));
      // one shared verify sub + SUBS noise subs (the "many subscriptions")
      ws.send(JSON.stringify({ type: "subscribe", id: "v", pattern: "verify.>" }));
      for (let s = 0; s < SUBS; s++) ws.send(JSON.stringify({ type: "subscribe", id: `n${s}`, pattern: `n${s}.order.>` }));
    });
    ws.addEventListener("message", (e) => {
      const m = JSON.parse(String(e.data));
      if (m.type === "ready") { ready++; conns.push(st); if (!settled) { settled = true; resolve(); } }
      else if (m.type === "event") { if (m.event?.event === "verify.evt") st.got.add(m.event.data.k); }
    });
    ws.addEventListener("error", () => { errors++; if (!settled) { settled = true; resolve(); } });
    ws.addEventListener("close", () => { if (!settled) { settled = true; resolve(); } });
    setTimeout(() => { if (!settled) { settled = true; resolve(); } }, 8000);
  });
}

console.log(`ramping ${TARGET} conns × ${SUBS + 1} subs each ...`);
const t0 = Date.now();
for (let b = 0; b * BATCH < TARGET; b++) {
  const ps = [];
  for (let k = 0; k < BATCH && b * BATCH + k < TARGET; k++) ps.push(mk(b * BATCH + k));
  await Promise.all(ps);
  await sleep(60);
}
console.log(`ramp done in ${((Date.now() - t0) / 1000).toFixed(1)}s — ready=${ready} errors=${errors}, total subs installed≈${ready * (SUBS + 1)}`);
await sleep(2000); // let all subscribes settle server-side

console.log(`publishing ${EVENTS} verify events (each should reach all ${ready} conns)...`);
for (let k = 0; k < EVENTS; k++) { await pubVerify(k); await nc.flush(); }
await sleep(5000); // let fan-out + matching complete

const counts = conns.map((c) => c.got.size);
const full = counts.filter((n) => n >= EVENTS).length;
counts.sort((a, b) => a - b);
const pct = (q) => counts[Math.min(counts.length - 1, Math.floor(q * counts.length))];
console.log("\n=== RESULT ===");
console.log(`connections ready: ${ready}/${TARGET}  errors: ${errors}`);
console.log(`verify events per conn (expect ${EVENTS}): min=${counts[0]} p50=${pct(0.5)} p99=${pct(0.99)} max=${counts[counts.length - 1]}`);
console.log(`conns that got ALL ${EVENTS}: ${full}/${ready} (${(100 * full / ready).toFixed(1)}%)`);
await nc.drain(); process.exit(0);
