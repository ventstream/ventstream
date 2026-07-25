// Isolate the soak's distinguishing variable: per-round disconnect/resume churn
// of a cohort, and whether it breaks the STABLE (never-churned) connections.
// env: CONNS, COHORT, ROUNDS.
import { connect } from "nats";
const WS = "ws://127.0.0.1:4040/ws";
const CONNS = parseInt(process.env.CONNS || "300", 10);
const COHORT = parseInt(process.env.COHORT || "50", 10);
const ROUNDS = parseInt(process.env.ROUNDS || "8", 10);
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random()*8)]; for (let i=0;i<25;i++) s+=B32[Math.floor(Math.random()*32)]; return s; };
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const nc = await connect({ servers: "nats://127.0.0.1:4222" });
let k=0;
async function pub(n){ const ks=[]; for(let i=0;i<n;i++){ k++; ks.push(k); const now=new Date().toISOString(); const env={id:ulid(),event:"verify.evt",tenant:"acme",entity_id:`v${k}`,occurred_at:now,received_at:now,schema_version:2,data:{k}}; nc.publish(`vs.t.acme.verify.evt.v${k}`, enc.encode(JSON.stringify(env))); } await nc.flush(); return ks; }
class Conn{ constructor(){this.lastSeq=0;this.seen=new Set();this.recv=new Set();this.open=false;this.ws=null;}
  connect(){ return new Promise(res=>{ let s=false; const ws=new WebSocket(WS); this.ws=ws;
    ws.addEventListener("open",()=>{ const h={type:"hello",tenant:"acme",token:"demo"}; if(this.lastSeq>0)h.resume_from_seq=this.lastSeq; ws.send(JSON.stringify(h)); });
    ws.addEventListener("message",e=>{ const m=JSON.parse(String(e.data)); if(m.type==="ready"){ws.send(JSON.stringify({type:"subscribe",id:"v",pattern:"verify.>"}));this.open=true;if(!s){s=true;res();}} else if(m.type==="event"){ if(typeof m.seq==="number"&&m.seq>this.lastSeq)this.lastSeq=m.seq; const id=m.event?.id; if(id){if(this.seen.has(id))return;this.seen.add(id);} const kk=m.event?.data?.k; if(kk!==undefined)this.recv.add(kk);} });
    ws.addEventListener("error",()=>{this.open=false;if(!s){s=true;res();}}); ws.addEventListener("close",()=>{this.open=false;if(!s){s=true;res();}}); setTimeout(()=>{if(!s){s=true;res();}},6000); }); }
  close(){ return new Promise(r=>{ if(!this.ws)return r(); this.ws.addEventListener("close",()=>r()); try{this.ws.close();}catch{} this.open=false; setTimeout(r,3000); }); }
}
const pool=Array.from({length:CONNS},()=>new Conn());
for(let b=0;b<CONNS;b+=50){ await Promise.all(pool.slice(b,b+50).map(c=>c.connect())); await sleep(60); }
await sleep(600);
const frac=(conns,ks)=>{ const a=conns.filter(c=>c.open); if(!a.length)return"0"; let ok=0; for(const c of a) if(ks.every(x=>c.recv.has(x)))ok++; return (100*ok/a.length).toFixed(1); };
console.log(`${pool.filter(c=>c.open).length} conns ready; cohort=${COHORT} churns each round`);
const cohort=pool.slice(0,COHORT); const stable=pool.slice(COHORT);
for(let r=1;r<=ROUNDS;r++){
  await Promise.all(cohort.map(c=>c.close())); await sleep(500);
  const gap=await pub(3); await sleep(800);
  await Promise.all(cohort.map(c=>c.connect())); await sleep(1500);
  const probe=await pub(3); await sleep(2500);
  console.log(`round ${r}: STABLE delivered=${frac(stable,probe)}%  cohort gap-recovered=${frac(cohort,gap)}%  stable_alive=${stable.filter(c=>c.open).length}`);
}
await nc.drain(); process.exit(0);
