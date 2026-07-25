// 4-hour soak driver (in-cluster). Maintains a pool of resume-capable raw-WS
// connections to the gateway (via ClusterIP → spread across pods), and cycles
// stages: STEADY fan-out, DISCONNECT+RESUME (gap recovery), SURGE, IDLE.
// Emits one JSON line per cycle so `kubectl logs` is the monitor feed.
import { connect } from "nats";

const WS = process.env.WS_URL || "ws://vs-gw.vsr.svc:4040/ws";
const NATS = process.env.NATS_URL || "nats://nats.vsr.svc:4222";
const POOL = parseInt(process.env.POOL || "300", 10);
const DURATION_MS = parseInt(process.env.DURATION_MS || String(4 * 3600 * 1000), 10);
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random()*8)]; for (let i=0;i<25;i++) s+=B32[Math.floor(Math.random()*32)]; return s; };
const sleep = (ms) => new Promise(r=>setTimeout(r,ms));

// Retry the initial connect — the driver may start before NATS DNS/endpoints
// are ready (reconnect only applies after a successful first connect).
let nc;
for (let attempt = 0; ; attempt++) {
  try { nc = await connect({ servers: NATS, reconnect: true, maxReconnectAttempts: -1 }); break; }
  catch (e) {
    if (attempt >= 60) throw e;
    console.log(JSON.stringify({ evt: "nats_connect_retry", attempt, err: String(e?.code || e) }));
    await sleep(2000);
  }
}
let kCounter = 0;
async function pubBatch(n) {
  const ks = [];
  for (let i = 0; i < n; i++) {
    const k = ++kCounter; ks.push(k);
    const now = new Date().toISOString();
    const env = { id: ulid(), event: "verify.evt", tenant: "acme", entity_id: `v${k}`, occurred_at: now, received_at: now, schema_version: 2, data: { k } };
    nc.publish(`vs.t.acme.verify.evt.v${k}`, enc.encode(JSON.stringify(env)));
  }
  await nc.flush();
  return ks;
}

class Conn {
  constructor(i) { this.i = i; this.lastSeq = 0; this.seen = new Set(); this.recv = new Set(); this.dupes = 0; this.open = false; this.ws = null; }
  connect() {
    return new Promise((resolve) => {
      let settled = false;
      let ws; try { ws = new WebSocket(WS); } catch { return resolve(); }
      this.ws = ws;
      ws.addEventListener("open", () => {
        const h = { type: "hello", tenant: "acme", token: "demo" };
        if (this.lastSeq > 0) h.resume_from_seq = this.lastSeq;
        ws.send(JSON.stringify(h));
      });
      ws.addEventListener("message", (e) => {
        let m; try { m = JSON.parse(String(e.data)); } catch { return; }
        if (m.type === "ready") { ws.send(JSON.stringify({ type: "subscribe", id: "v", pattern: "verify.>" })); this.open = true; if (!settled){settled=true;resolve();} }
        else if (m.type === "event") {
          if (typeof m.seq === "number" && m.seq > this.lastSeq) this.lastSeq = m.seq;
          const id = m.event?.id;
          if (id) { if (this.seen.has(id)) { this.dupes++; return; } this.seen.add(id); }
          const k = m.event?.data?.k; if (k !== undefined) this.recv.add(k);
        }
      });
      ws.addEventListener("error", () => { this.open = false; if (!settled){settled=true;resolve();} });
      ws.addEventListener("close", () => { this.open = false; if (!settled){settled=true;resolve();} });
      setTimeout(() => { if (!settled){settled=true;resolve();} }, 8000);
    });
  }
  close() { return new Promise((res) => { if (!this.ws) return res(); this.ws.addEventListener("close", () => res()); try { this.ws.close(); } catch {} this.open = false; setTimeout(res, 3000); }); }
}

const pool = Array.from({ length: POOL }, (_, i) => new Conn(i));
// initial ramp
for (let b = 0; b < POOL; b += 50) { await Promise.all(pool.slice(b, b + 50).map(c => c.connect())); await sleep(80); }
const alive = () => pool.filter(c => c.open).length;
const fracGotAll = (conns, ks) => { if (!conns.length) return 1; let ok = 0; for (const c of conns) if (ks.every(k => c.recv.has(k))) ok++; return ok / conns.length; };
console.log(JSON.stringify({ evt: "ramp_done", pool: POOL, alive: alive() }));

const start = Date.now();
let cycle = 0;
while (Date.now() - start < DURATION_MS) {
  cycle++;
  const r = { cycle, elapsed_min: +(((Date.now() - start) / 60000).toFixed(1)) };
  // STEADY
  let ks = await pubBatch(5); await sleep(3000);
  const aliveConns = pool.filter(c => c.open);
  r.alive = aliveConns.length;
  r.steady = +(fracGotAll(aliveConns, ks) * 100).toFixed(1);
  // DISCONNECT + RESUME
  const cohort = pool.filter(c => c.open).slice(0, 50);
  await Promise.all(cohort.map(c => c.close())); await sleep(500);
  ks = await pubBatch(5); await sleep(800);                 // gap published while cohort down
  await Promise.all(cohort.map(c => c.connect())); await sleep(3000);
  r.resume_recovered = +(fracGotAll(cohort.filter(c => c.open), ks) * 100).toFixed(1);
  r.resume_cohort_alive = cohort.filter(c => c.open).length;
  // SURGE
  ks = await pubBatch(100); await sleep(5000);
  const aliveNow = pool.filter(c => c.open);
  r.surge = +(fracGotAll(aliveNow, ks) * 100).toFixed(1);
  // dupes total
  r.dupes = pool.reduce((s, c) => s + c.dupes, 0);
  console.log(JSON.stringify(r));
  // IDLE
  await sleep(30000);
}
console.log(JSON.stringify({ evt: "soak_done", cycles: cycle, alive: alive() }));
await nc.drain();
