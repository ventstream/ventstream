// Repro the post-burst delivery stall. CONNS connections, each subscribes
// verify.>. Phases: warmup(5) -> burst(BURST) -> post(5). If post-burst events
// don't arrive, the pull loop stalled after the burst. env: CONNS, BURST.
import { connect } from "nats";
const WS = "ws://127.0.0.1:4040/ws";
const CONNS = parseInt(process.env.CONNS || "1", 10);
const BURST = parseInt(process.env.BURST || "100", 10);
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random()*8)]; for (let i=0;i<25;i++) s+=B32[Math.floor(Math.random()*32)]; return s; };
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const nc = await connect({ servers: "nats://127.0.0.1:4222" });
let k=0;
async function pub(n){ const ks=[]; for(let i=0;i<n;i++){ k++; ks.push(k); const now=new Date().toISOString(); const env={id:ulid(),event:"verify.evt",tenant:"acme",entity_id:`v${k}`,occurred_at:now,received_at:now,schema_version:2,data:{k}}; nc.publish(`vs.t.acme.verify.evt.v${k}`, enc.encode(JSON.stringify(env))); } await nc.flush(); return ks; }

const conns=[];
function mk(){ return new Promise(res=>{ const ws=new WebSocket(WS); const st={got:new Set(),ws}; let s=false;
  ws.addEventListener("open",()=>ws.send(JSON.stringify({type:"hello",tenant:"acme",token:"demo"})));
  ws.addEventListener("message",e=>{ const m=JSON.parse(String(e.data)); if(m.type==="ready"){ws.send(JSON.stringify({type:"subscribe",id:"v",pattern:"verify.>"})); conns.push(st); if(!s){s=true;res();}} else if(m.type==="event"){ const kk=m.event?.data?.k; if(kk!==undefined) st.got.add(kk);} });
  ws.addEventListener("error",()=>{if(!s){s=true;res();}}); setTimeout(()=>{if(!s){s=true;res();}},6000);
}); }
for(let b=0;b<CONNS;b+=50){ await Promise.all(Array.from({length:Math.min(50,CONNS-b)},mk)); await sleep(60); }
await sleep(600);
const frac=(ks)=>{ let ok=0; for(const c of conns) if(ks.every(x=>c.got.has(x))) ok++; return (100*ok/conns.length).toFixed(1); };

const w=await pub(5); await sleep(2000); console.log(`warmup(5):  delivered to ${frac(w)}% of ${conns.length} conns`);
const burst=await pub(BURST); await sleep(4000); console.log(`burst(${BURST}): delivered to ${frac(burst)}%`);
const post=await pub(5); await sleep(3000); console.log(`POST-burst(5): delivered to ${frac(post)}%  <-- if <100, pull stalled after burst`);
await nc.drain(); process.exit(0);
