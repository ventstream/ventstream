#!/usr/bin/env bash
# WebSocket cap + HPA test against the AWS stack. Ramps TARGET connections at
# the gateway's public NLB; expect ~ws_max_conns per pod established, the rest
# 503-rejected, /readyz to flip, and the memory HPA to scale out. Mirrors the
# minikube validation from PR #7. Requires: bun, kubeconfig.
set -euo pipefail
NS=ventstream
TARGET="${1:-8000}"

LB=$(kubectl -n "$NS" get svc -l app.kubernetes.io/name=ventstream-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}')
[ -n "$LB" ] || { echo "LB not provisioned yet — retry in a minute"; exit 1; }
URL="ws://${LB}:4040/ws"
echo "WS endpoint: $URL  target=$TARGET"

cat >/tmp/ws-ramp.ts <<'TS'
const URL=process.env.WS_URL!, TARGET=parseInt(process.env.TARGET||"8000",10);
let ready=0, rejected=0, retries=0; const held:WebSocket[]=[];
function attempt(i:number,tries:number){
  let ws:WebSocket; try{ws=new WebSocket(URL);}catch{return sched(i,tries);}
  let s=false;
  ws.addEventListener("open",()=>{ws.send(JSON.stringify({type:"hello",tenant:"acme",token:"demo"}));ws.send(JSON.stringify({type:"subscribe",id:"s"+i,pattern:">"}));});
  ws.addEventListener("message",e=>{try{if(JSON.parse(String((e as MessageEvent).data)).type==="ready"&&!s){s=true;ready++;held.push(ws);}}catch{}});
  ws.addEventListener("error",()=>{if(!s){s=true;sched(i,tries);}});
  ws.addEventListener("close",()=>{if(!s){s=true;sched(i,tries);}});
}
function sched(i:number,t:number){rejected++; if(t>=8)return; retries++; const b=500*2**Math.min(t,5); setTimeout(()=>attempt(i,t+1), b*(0.7+Math.random()*0.6));}
(async()=>{for(let i=0;i<TARGET;i++){attempt(i,0); if(i%60===0)await new Promise(r=>setTimeout(r,200));}
 setInterval(()=>console.log(JSON.stringify({ready,rejected,retries})),5000);})();
TS

echo "watch in another shell: kubectl -n $NS get hpa,pods -w"
WS_URL="$URL" TARGET="$TARGET" bun /tmp/ws-ramp.ts
