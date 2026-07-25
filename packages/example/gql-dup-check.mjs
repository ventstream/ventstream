// Dup/isolation check for the per-connection graphql model: one connection
// with TWO overlapping subscriptions that BOTH match the published events.
// Correct = each operation receives each event exactly once (per-op delivery),
// never twice on the same operation.
import { connect } from "nats";
const enc = new TextEncoder();
const B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ulid = () => { let s = "01234567"[Math.floor(Math.random()*8)]; for (let i=0;i<25;i++) s+=B32[Math.floor(Math.random()*32)]; return s; };
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const nc = await connect({ servers: "nats://127.0.0.1:4222" });
async function pub(k){ const now=new Date().toISOString(); const env={id:ulid(),event:"verify.evt",tenant:"acme",entity_id:`v${k}`,occurred_at:now,received_at:now,schema_version:2,data:{k}}; nc.publish(`vs.t.acme.verify.evt.v${k}`, enc.encode(JSON.stringify(env))); await nc.flush(); }

const perOp = { a: [], b: [] };
const ws = new WebSocket("ws://127.0.0.1:4041/graphql/ws", "graphql-transport-ws");
await new Promise((resolve)=>{
  ws.addEventListener("open",()=>ws.send(JSON.stringify({type:"connection_init",payload:{authToken:"demo",tenant:"acme"}})));
  ws.addEventListener("message",(e)=>{ const m=JSON.parse(String(e.data));
    if(m.type==="connection_ack"){
      // two OVERLAPPING subscriptions on the SAME connection
      ws.send(JSON.stringify({type:"subscribe",id:"a",payload:{query:'subscription { events(subject:"verify.>"){ data } }'}}));
      ws.send(JSON.stringify({type:"subscribe",id:"b",payload:{query:'subscription { events(subject:"verify.evt.*"){ data } }'}}));
      resolve();
    } else if(m.type==="next"){ const k=m.payload?.data?.events?.data?.k; if(k!==undefined && perOp[m.id]) perOp[m.id].push(k); }
  });
});
await sleep(800);
for(let k=0;k<5;k++) await pub(k);
await sleep(2000);
const dupA = perOp.a.length - new Set(perOp.a).size;
const dupB = perOp.b.length - new Set(perOp.b).size;
console.log("=== per-connection multi-op dup check ===");
console.log(`op a (verify.>):      received ${perOp.a.length} (expect 5)  dupes=${dupA}`);
console.log(`op b (verify.evt.*):  received ${perOp.b.length} (expect 5)  dupes=${dupB}`);
console.log(dupA===0 && dupB===0 && perOp.a.length===5 && perOp.b.length===5 ? "PASS: each op got each event exactly once" : "FAIL");
await nc.drain(); process.exit(0);
