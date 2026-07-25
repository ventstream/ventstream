// Repro idle-at-scale stall: CONNS conns subscribe verify.>, then we publish a
// small batch, IDLE, publish again, IDLE, publish again — checking delivery
// after each idle. If delivery decays across idles, the pull renewal stalls at
// scale during quiet periods. env: CONNS, IDLE_S, ROUNDS.
import { connect } from "nats";
const WS = "ws://127.0.0.1:4040/ws";
const CONNS = parseInt(process.env.CONNS || "300", 10);
const IDLE_S = parseInt(process.env.IDLE_S || "35", 10);
const ROUNDS = parseInt(process.env.ROUNDS || "4", 10);
const BATCH = parseInt(process.env.BATCH || "3", 10);
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
  ws.addEventListener("error",()=>{if(!s){s=true;res();}}); setTimeout(()=>{if(!s){s=true;res();}},6000); }); }
for(let b=0;b<CONNS;b+=50){ await Promise.all(Array.from({length:Math.min(50,CONNS-b)},mk)); await sleep(60); }
await sleep(600);
const frac=(ks)=>{ let ok=0; for(const c of conns) if(ks.every(x=>c.got.has(x))) ok++; return (100*ok/conns.length).toFixed(1); };
console.log(`${conns.length} conns ready`);
for(let r=1;r<=ROUNDS;r++){
  const ks=await pub(BATCH); await sleep(2500);
  console.log(`round ${r} (after ${(r-1)*IDLE_S}s idle): delivered to ${frac(ks)}%`);
  if(r<ROUNDS) await sleep(IDLE_S*1000);
}
await nc.drain(); process.exit(0);
