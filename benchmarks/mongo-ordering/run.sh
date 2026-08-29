#!/usr/bin/env bash
# MongoDB → OpenSearch write-ordering benchmark (#117).
#
# Measures two things for a hot-document workload — many updates to the same
# small set of documents, which is exactly what parallel sink bulks reorder:
#
#   correctness  documents whose final sink state is NOT the last write the
#                source applied (stale), after the pipeline has fully drained;
#   throughput   change events per second from first update to convergence.
#
# Run against any engine binaries and parallelism settings, e.g. the pre-fix
# binary (no source version on Mongo events) and the fixed one, at
# VS_DISPATCH_PARALLEL_BULKS=4 and =1 (the stopgap the issue proposed).
#
# Usage:
#   benchmarks/mongo-ordering/run.sh <label>=<binary> [<label>=<binary> ...]
# Env:
#   DOCS      hot documents (default 200)
#   ROUNDS    full update passes over every document (default 200)
#   PARALLEL  comma-separated VS_DISPATCH_PARALLEL_BULKS values (default 4,1)
#   BATCH     VS_DISPATCH_MAX_EVENTS (default 100 — small batches so several
#             are in flight at once, which is the reordering window)
#   JITTER_MS route sink writes through jitter-proxy.py, which holds each
#             request for a random 0..JITTER_MS before forwarding (default 0:
#             direct). On a local single-node OpenSearch bulks arrive and
#             apply in send order, so without jitter the reordering hazard
#             does not show; with it, in-flight bulks overtake each other as
#             they can across a real network or a multi-node cluster.
#
# Needs Docker (mongo:7.0, opensearchproject/opensearch:2.17.1), curl, python3.
set -euo pipefail

DOCS="${DOCS:-200}"
ROUNDS="${ROUNDS:-200}"
PARALLEL="${PARALLEL:-4,1}"
BATCH="${BATCH:-100}"
JITTER_MS="${JITTER_MS:-0}"
MONGO_PORT="${MONGO_PORT:-27117}"
OS_PORT="${OS_PORT:-9299}"
PROXY_PORT="${PROXY_PORT:-9298}"
INDEX="bench_orders"
WORK="${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/vs-mongo-ordering.XXXXXX")}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RUN_SEQ=0
mkdir -p "$WORK"

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <label>=<binary> [...]" >&2
  exit 2
fi

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

mongosh() { docker exec vsbench-mongo mongosh --quiet --norc "$@"; }
os() { curl -sS "http://127.0.0.1:${OS_PORT}$1" "${@:2}"; }

PROXY_PID=""
cleanup() {
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
  docker rm -f vsbench-mongo vsbench-os >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- containers -------------------------------------------------------------
cleanup
log "starting mongo:7.0 (replica set) and opensearch:2.17.1"
docker run -d --name vsbench-mongo -p "${MONGO_PORT}:27017" mongo:7.0 \
  mongod --replSet rs0 --bind_ip_all >/dev/null
docker run -d --name vsbench-os -p "${OS_PORT}:9200" \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e 'OPENSEARCH_INITIAL_ADMIN_PASSWORD=Vent$tr3am!Pass' \
  -e bootstrap.memory_lock=false -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' \
  opensearchproject/opensearch:2.17.1 >/dev/null

for _ in $(seq 1 60); do
  mongosh --eval 'db.adminCommand({ping:1}).ok' >/dev/null 2>&1 && break
  sleep 1
done
mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})' >/dev/null
for _ in $(seq 1 60); do
  [ "$(mongosh --eval 'db.hello().isWritablePrimary')" = "true" ] && break
  sleep 1
done
for _ in $(seq 1 180); do
  os / >/dev/null 2>&1 && break
  sleep 1
done
log "containers ready"

MONGO_URI="mongodb://127.0.0.1:${MONGO_PORT}/?directConnection=true&replicaSet=rs0"
SINK_ENDPOINT="http://127.0.0.1:${OS_PORT}"
if [ "$JITTER_MS" -gt 0 ]; then
  python3 "${HERE}/jitter-proxy.py" "$PROXY_PORT" "$OS_PORT" "$JITTER_MS" "${WORK}/bulk-trace.log" >"${WORK}/proxy.log" 2>&1 &
  PROXY_PID=$!
  for _ in $(seq 1 20); do
    curl -sS "http://127.0.0.1:${PROXY_PORT}/" >/dev/null 2>&1 && break
    sleep 0.25
  done
  SINK_ENDPOINT="http://127.0.0.1:${PROXY_PORT}"
  log "sink writes routed through jitter proxy (0..${JITTER_MS} ms)"
fi

# ---- helpers ----------------------------------------------------------------
os_sum_round() {
  os "/${INDEX}/_search?size=0" -H 'content-type: application/json' \
    -d '{"aggs":{"s":{"sum":{"field":"round"}}}}' 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(int(d["aggregations"]["s"]["value"]))' 2>/dev/null || echo -1
}
os_count_round() {
  os "/${INDEX}/_count" -H 'content-type: application/json' \
    -d "{\"query\":{\"term\":{\"round\":$1}}}" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null || echo -1
}
os_count() {
  os "/${INDEX}/_count" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null || echo -1
}

run_config() {
  local label="$1" binary="$2" parallel="$3"
  RUN_SEQ=$((RUN_SEQ + 1))
  # A fresh state dir per run: a reused resume token would skip the bootstrap
  # and resume into the previous run's stream.
  local dir="${WORK}/${RUN_SEQ}-${label}-p${parallel}"
  mkdir -p "$dir"

  # Fresh source + sink for every configuration.
  mongosh --eval "db.getSiblingDB('bench').orders.drop()" >/dev/null
  mongosh --eval "db.getSiblingDB('bench').orders.insertMany(Array.from({length:${DOCS}},(_,i)=>({_id:i,round:0,payload:'x'.repeat(64)})))" >/dev/null
  os "/${INDEX}" -X DELETE >/dev/null 2>&1 || true

  VS_ROLES=cdc VS_CDC_SOURCE=mongodb VS_MONGO_URI="$MONGO_URI" VS_MONGO_DATABASE=bench \
  VS_MONGO_COLLECTIONS=orders VS_MONGO_STATE_DIR="$dir" VS_MONGO_BOOTSTRAP_MODE=snapshot \
  VS_MONGO_TOKEN_FLUSH_MS=100 VS_OS_ENDPOINT="$SINK_ENDPOINT" \
  VS_INDEX_TEMPLATE="$INDEX" VS_DLQ_PATH="$dir/dlq.jsonl" \
  VS_DISPATCH_PARALLEL_BULKS="$parallel" VS_DISPATCH_MAX_EVENTS="$BATCH" \
  VS_DISPATCH_FLUSH_MS=50 RUST_LOG=info,ventstream_sinks::opensearch=debug \
    "$binary" >"$dir/engine.log" 2>&1 &
  local pid=$!

  # Bootstrap: every seed document indexed.
  for _ in $(seq 1 120); do
    [ "$(os_count)" = "$DOCS" ] && break
    sleep 1
  done
  if [ "$(os_count)" != "$DOCS" ]; then
    log "  bootstrap did not complete (count=$(os_count)); see $dir/engine.log"
    kill "$pid" 2>/dev/null || true
    return 1
  fi
  # Make writes visible to the sampler promptly (default refresh is 1s).
  os "/${INDEX}/_settings" -X PUT -H 'content-type: application/json' \
    -d '{"index":{"refresh_interval":"50ms"}}' >/dev/null

  # Workload: ROUNDS full passes, each pass one write per document, so every
  # document's writes are DOCS events apart in the oplog — a few batches at
  # BATCH events each, well inside the parallel in-flight window.
  #
  # `replaceOne`, not `$set`: a replace event's `fullDocument` is the image
  # written by THAT operation. An update event under `updateLookup` carries
  # whatever the document holds when the event is *read*, so a backlog of
  # old events all arrive carrying the newest state — which both hides
  # reordering at the end and makes the pipeline look finished while
  # thousands of events are still queued.
  [ -n "$PROXY_PID" ] && : >"${WORK}/bulk-trace.log"
  local started
  started=$(python3 -c 'import time; print(time.time())')
  mongosh --eval "const c=db.getSiblingDB('bench').orders; for (let r=1; r<=${ROUNDS}; r++) { c.bulkWrite(Array.from({length:${DOCS}},(_,i)=>({replaceOne:{filter:{_id:i},replacement:{round:r,payload:'x'.repeat(64)}}})), {ordered:true}); }" >/dev/null

  # Sample the index every 50 ms until it stops changing. Two things come
  # out: the moment of the last change (pipeline time), and how many times a
  # document's `round` went BACKWARDS between samples — an older write
  # applied after a newer one. A final-state check alone misses that: a
  # later round almost always lands eventually, so only the last pair of
  # bulks would ever leave a document stale.
  local sampled finished regressions
  sampled=$(python3 - "http://127.0.0.1:${OS_PORT}" "$INDEX" "$DOCS" <<'EOF'
import json, sys, time, urllib.request
base, index, docs = sys.argv[1], sys.argv[2], int(sys.argv[3])
seen, regressions, last_sum, last_change, stable = {}, 0, None, time.time(), 0
body = json.dumps({"size": docs, "_source": ["round"]}).encode()
while stable < 60:  # 3 s without any change
    time.sleep(0.05)
    try:
        req = urllib.request.Request(f"{base}/{index}/_search", data=body,
                                     headers={"content-type": "application/json"})
        hits = json.load(urllib.request.urlopen(req, timeout=10))["hits"]["hits"]
    except Exception:
        continue
    total = 0
    for hit in hits:
        r = hit["_source"].get("round", 0)
        total += r
        prev = seen.get(hit["_id"])
        if prev is not None and r < prev:
            regressions += 1
        seen[hit["_id"]] = max(prev or 0, r)
    if total != last_sum:
        last_sum, last_change, stable = total, time.time(), 0
    else:
        stable += 1
print(f"{last_change:.6f} {regressions}")
EOF
)
  finished="${sampled%% *}"
  regressions="${sampled##* }"
  os "/${INDEX}/_refresh" -X POST >/dev/null

  local final stale elapsed rate versions conflicts
  final=$(os_count_round "$ROUNDS")
  stale=$((DOCS - final))
  elapsed=$(python3 -c "print(round(${finished} - ${started}, 2))")
  rate=$(python3 -c "print(round(${DOCS} * ${ROUNDS} / max(${finished} - ${started}, 0.001)))")
  versions=$(os "/${INDEX}/_doc/bench.orders%3A%5B%220%22%5D" 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("_version"))' 2>/dev/null || echo "?")
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  # Stale writes the sink refused because a newer version was already
  # stored — the fix engaging. Always 0 for an unversioned binary.
  conflicts=$(grep -c 'external-version conflict' "$dir/engine.log" || true)
  # Bulks the proxy forwarded ahead of an earlier bulk carrying strictly
  # older rounds — writes that reached OpenSearch out of source order. The
  # same for both binaries; only a versioned one refuses them.
  local reordered="-" bulks="-"
  if [ -n "$PROXY_PID" ]; then
    read -r bulks reordered <<<"$(python3 - "${WORK}/bulk-trace.log" <<'EOF'
import sys
rows = sorted(tuple(float(x) for x in l.split()[:2]) + tuple(int(x) for x in l.split()[2:])
              for l in open(sys.argv[1]) if l.strip())
print(len(rows), sum(1 for i in range(len(rows)) for j in range(i + 1, min(i + 16, len(rows)))
                      if rows[j][1] < rows[i][1] and rows[j][2] > rows[i][3]))
EOF
)"
  fi

  printf '%-8s %-8s %-9s %5s %6s %7s %9s %6s %10s %11s %9s %5s %20s\n' \
    "$label" "$parallel" "$JITTER_MS" "$DOCS" "$ROUNDS" "$elapsed" "$rate" "$bulks" "$reordered" "$regressions" "$stale" "$conflicts" "$versions"
}

printf '\n%-8s %-8s %-9s %5s %6s %7s %9s %6s %10s %11s %9s %5s %20s\n' \
  "binary" "parallel" "jitter-ms" "docs" "rounds" "secs" "events/s" "bulks" "ooo-bulks" "regressions" "stale" "409s" "doc0 _version"
printf '%-8s %-8s %-9s %5s %6s %7s %9s %6s %10s %11s %9s %5s %20s\n' \
  "------" "--------" "---------" "----" "------" "----" "--------" "-----" "---------" "-----------" "-----" "----" "-------------"
IFS=',' read -r -a parallels <<<"$PARALLEL"
for spec in "$@"; do
  label="${spec%%=*}"
  binary="${spec#*=}"
  for p in "${parallels[@]}"; do
    log "running ${label} (${binary}) parallel_bulks=${p}"
    run_config "$label" "$binary" "$p" || true
  done
done
log "logs and state under ${WORK}"
