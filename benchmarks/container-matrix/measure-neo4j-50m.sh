#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "$0")/run-sources.sh"
ROOT=$HOME/projects/ventstream
RECORDS=50000000
RESULTS="$ROOT/target/benchmarks/container-matrix/bootstrap-50m-neo4j"
mkdir -p "$RESULTS/neo4j-bootstrap"
ensure_network
docker network connect vsbench vsbench-neo4j 2>/dev/null || true
start_opensearch
delete_index vsbench-neo4j
create_index vsbench-neo4j
cleanup_engine
read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values throughput)"
t0=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
start_engine neo4j throughput \
  -e VS_NEO4J_URI=bolt://vsbench-neo4j:7687 -e VS_NEO4J_USER=neo4j \
  -e VS_NEO4J_PASSWORD=ventstream -e VS_NEO4J_DATABASE=neo4j \
  -e VS_NEO4J_BOOTSTRAP_MODE=snapshot -e VS_NEO4J_POLL_INTERVAL_MS=10 \
  -e VS_NEO4J_DENORMALIZE_YAML=/specs/neo4j.yaml \
  -e "VS_NEO4J_RECOMPOSE_CHUNK=$chunk" -e "VS_NEO4J_RECOMPOSE_CONCURRENCY=$concurrency" \
  -e VS_NEO4J_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-neo4j
"$BENCH_DIR/sample-container.sh" "$ENGINE" "$RESULTS/neo4j-bootstrap" &
monitor_pid=$!
port=$(wait_engine_metrics)
TIMEOUT_SECS=10800 wait_delivered "$port" 0 "$RECORDS"
t1=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
kill "$monitor_pid" >/dev/null 2>&1 || true; wait "$monitor_pid" 2>/dev/null || true
boots=$(awk -v s="$t0" -v e="$t1" 'BEGIN {printf "%.3f", (e-s)/1000000000}')
tput=$(awk -v n="$RECORDS" -v s="$boots" 'BEGIN {printf "%.2f", n/s}')
docs=$(verified_count vsbench-neo4j)
docker logs "$ENGINE" >"$RESULTS/neo4j-bootstrap/engine.log" 2>&1 || true
samples=$("$BENCH_DIR/summarize-samples.sh" "$RESULTS/neo4j-bootstrap/docker-stats.tsv" "$RESULTS/neo4j-bootstrap/process-memory.tsv")
printf 'neo4j,%s,64,reused,%s,%s,%s,%s\n' "$RECORDS" "$boots" "$tput" "$samples" "$docs" | tee -a "$RESULTS/bootstrap.csv"
[ "$docs" = "$RECORDS" ] && echo "VERIFIED 50M" || echo "COUNT MISMATCH: $docs"
cleanup_engine
docker rm -f vsbench-neo4j vsbench-opensearch >/dev/null 2>&1 || true
