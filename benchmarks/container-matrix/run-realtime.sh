#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
BENCH_DIR="$ROOT/benchmarks/container-matrix"
RUN_ID=${VS_BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${VS_BENCH_RESULTS:-$ROOT/target/benchmarks/container-matrix/$RUN_ID}
IMAGE=${VS_BENCH_IMAGE:-ventstream-engine:bench}
NETWORK=vsbench-realtime
ENGINE=vsbench-realtime-engine
NATS=vsbench-nats
REDIS=vsbench-redis
ENGINE_CPUS=${VS_BENCH_ENGINE_CPUS:-2}
ENGINE_MEMORY=${VS_BENCH_ENGINE_MEMORY:-1g}
REDIS_MEMORY=${VS_BENCH_REDIS_MEMORY:-1g}
CLIENT_IMAGE=${VS_BENCH_CLIENT_IMAGE:-node:26-alpine}
PROBE_IMAGE=${VS_BENCH_PROBE_IMAGE:-busybox:1.37}
WS_MAILBOX_CORE=${VS_BENCH_WS_MAILBOX_CORE:-${VS_BENCH_WS_MAILBOX:-65536}}
WS_MAILBOX_DURABLE=${VS_BENCH_WS_MAILBOX_DURABLE:-${VS_BENCH_WS_MAILBOX:-1024}}
GRAPHQL_BROADCAST_NATS=${VS_BENCH_GRAPHQL_BROADCAST_NATS:-${VS_BENCH_GRAPHQL_BROADCAST:-1024}}
GRAPHQL_BROADCAST_REDIS=${VS_BENCH_GRAPHQL_BROADCAST_REDIS:-${VS_BENCH_GRAPHQL_BROADCAST:-1024}}
REDIS_READ_BATCH_RAW=${VS_BENCH_REDIS_READ_BATCH_RAW:-${VS_BENCH_REDIS_READ_BATCH:-1000}}
REDIS_READ_BATCH_GRAPHQL=${VS_BENCH_REDIS_READ_BATCH_GRAPHQL:-${VS_BENCH_REDIS_READ_BATCH:-100}}
JS_REAPER_INTERVAL_MS=${VS_BENCH_JS_REAPER_INTERVAL_MS:-60000}

mkdir -p "$RESULTS"
CSV="$RESULTS/realtime.csv"
printf '%s\n' 'transport,provider,clients,published_events,verified_deliveries,elapsed_s,published_eps,deliveries_eps,gaps,duplicates,cpu_mean_pct,cpu_p95_pct,cpu_peak_pct,cgroup_peak_mib,rss_peak_mib,rss_hwm_mib,mailbox,broadcast_capacity,redis_read_batch' >"$CSV"

remove_container() {
  docker rm -fv "$1" >/dev/null 2>&1 || true
}

cleanup() {
  remove_container "$ENGINE"
  remove_container "$NATS"
  remove_container "$REDIS"
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  trap cleanup EXIT INT TERM
fi

wait_for() {
  local description=$1
  shift
  local deadline=$((SECONDS + 180))
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $description" >&2
      return 1
    fi
    sleep 1
  done
}

ensure_network() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null
}

start_nats() {
  remove_container "$NATS"
  docker run -d --name "$NATS" --network "$NETWORK" --cpus 1 --memory 512m \
    nats:2.10-alpine -js -sd /tmp/nats -m 8222 >/dev/null
  sleep 2
}

start_redis() {
  remove_container "$REDIS"
  docker run -d --name "$REDIS" --network "$NETWORK" --cpus 1 --memory "$REDIS_MEMORY" \
    redis:7.4-alpine redis-server --save '' --appendonly no >/dev/null
  wait_for Redis docker exec "$REDIS" redis-cli ping
}

probe_http() {
  docker run --rm --network "$NETWORK" "$PROBE_IMAGE" \
    wget -q -T 2 -O /dev/null "$1"
}

probe_tcp() {
  docker run --rm --network "$NETWORK" "$PROBE_IMAGE" \
    nc -z -w 2 "$1" "$2"
}

run_realtime_client() {
  local transport=$1 provider=$2 clients=$3 events=$4
  local service_port=4041 service_path=/graphql/ws
  if [[ "$transport" == raw ]]; then
    service_port=4040
    service_path=/ws
  fi
  local args=(
    --rm --network "$NETWORK"
    -v "$ROOT:/workspace:ro" -w /workspace/packages/example
    -e "VS_BENCH_TRANSPORT=$transport"
    -e "VS_BENCH_PROVIDER=$provider"
    -e "VS_BENCH_WS_URL=ws://$ENGINE:$service_port$service_path"
    -e "VS_BENCH_CLIENTS=$clients"
    -e "VS_BENCH_EVENTS=$events"
    -e VS_BENCH_PAYLOAD_BYTES=256
  )
  if [[ "$provider" == redis_streams ]]; then
    args+=(-e "VS_BENCH_REDIS_URL=redis://$REDIS:6379")
  else
    args+=(-e "VS_BENCH_NATS_URL=nats://$NATS:4222")
  fi
  docker run "${args[@]}" "$CLIENT_IMAGE" node benchmark-realtime.mjs
}

start_engine() {
  local transport=$1 provider=$2
  remove_container "$ENGINE"
  local role listen_port redis_read_batch=$REDIS_READ_BATCH_RAW ws_mailbox=$WS_MAILBOX_DURABLE
  if [[ "$transport" == raw ]]; then role=ws; listen_port=4040; else role=graphql; listen_port=4041; fi
  if [[ "$transport" == graphql ]]; then redis_read_batch=$REDIS_READ_BATCH_GRAPHQL; fi
  if [[ "$provider" == nats_core ]]; then ws_mailbox=$WS_MAILBOX_CORE; fi
  local args=(
    -d --name "$ENGINE" --network "$NETWORK"
    --cpus "$ENGINE_CPUS" --memory "$ENGINE_MEMORY"
    -v "$BENCH_DIR:/specs:ro"
    -e "VS_ROLES=$role" -e VS_TENANT=acme
    -e VS_HEALTH_LISTEN=0.0.0.0:4043 -e RUST_LOG=warn
  )
  if [[ "$transport" == raw ]]; then
    args+=(
      -e VS_WS_LISTEN=0.0.0.0:4040 -e VS_WS_SUBJECTS='vs.t.>'
      -e "VS_WS_PROVIDER=$provider" -e "VS_WS_MAILBOX=$ws_mailbox" -e VS_WS_MAX_CONNS=1000
    )
    if [[ "$provider" == nats_core || "$provider" == nats_jetstream ]]; then
      args+=(-e VS_WS_NATS_URL=nats://vsbench-nats:4222)
    fi
    if [[ "$provider" == nats_jetstream ]]; then
      args+=(
        -e VS_WS_JETSTREAM=1 -e VS_WS_JS_STREAM=vsbench
        -e VS_WS_JS_STORAGE=memory -e VS_WS_JS_MAX_MSGS=1000000
        -e "VS_WS_JS_REAPER_INTERVAL_MS=$JS_REAPER_INTERVAL_MS"
      )
    fi
    if [[ "$provider" == redis_streams ]]; then
      args+=(
        -e VS_WS_REDIS_URL=redis://vsbench-redis:6379
        -e "VS_WS_REDIS_READ_BATCH=$redis_read_batch" -e VS_WS_REDIS_BLOCK_TIMEOUT_MS=10
        -e VS_WS_REDIS_BROADCAST_CAPACITY=65536 -e VS_WS_REDIS_MAX_LENGTH=1000000
      )
    fi
  else
    local graphql_broadcast=$GRAPHQL_BROADCAST_NATS
    if [[ "$provider" == redis_streams ]]; then graphql_broadcast=$GRAPHQL_BROADCAST_REDIS; fi
    args+=(
      -e VS_GRAPHQL_LISTEN=0.0.0.0:4041 -e "VS_GRAPHQL_PROVIDER=$provider"
      -e VS_GRAPHQL_SCHEMA=/specs/subscriptions.graphql -e "VS_GRAPHQL_BROADCAST_CAP=$graphql_broadcast"
    )
    if [[ "$provider" == nats_jetstream ]]; then
      args+=(
        -e VS_GRAPHQL_NATS_URL=nats://vsbench-nats:4222 -e VS_GRAPHQL_STREAM=vsbench
        -e "VS_GRAPHQL_REAPER_INTERVAL_MS=$JS_REAPER_INTERVAL_MS"
      )
    else
      args+=(
        -e VS_GRAPHQL_REDIS_URL=redis://vsbench-redis:6379
        -e "VS_GRAPHQL_REDIS_READ_BATCH=$redis_read_batch" -e VS_GRAPHQL_REDIS_BLOCK_TIMEOUT_MS=10
        -e VS_GRAPHQL_REDIS_BROADCAST_CAPACITY=65536 -e VS_GRAPHQL_REDIS_MAX_LENGTH=1000000
      )
    fi
  fi
  docker run "${args[@]}" "$IMAGE" >/dev/null
  wait_for 'realtime engine readiness' probe_http "http://$ENGINE:4043/readyz"
  wait_for 'realtime listener' probe_tcp "$ENGINE" "$listen_port"
  sleep 1
}

bootstrap_jetstream() {
  start_engine raw nats_jetstream
  remove_container "$ENGINE"
}

run_case() {
  local transport=$1 provider=$2 clients=$3 events=$4
  local graphql_broadcast=$GRAPHQL_BROADCAST_NATS
  local redis_read_batch=$REDIS_READ_BATCH_RAW
  local ws_mailbox=$WS_MAILBOX_DURABLE
  if [[ "$provider" == redis_streams ]]; then graphql_broadcast=$GRAPHQL_BROADCAST_REDIS; fi
  if [[ "$transport" == graphql ]]; then redis_read_batch=$REDIS_READ_BATCH_GRAPHQL; fi
  if [[ "$provider" == nats_core ]]; then ws_mailbox=$WS_MAILBOX_CORE; fi
  if [[ "$provider" == nats_jetstream && "$transport" == graphql ]]; then
    # GraphQL consumers replay retained JetStream data. Give every measured
    # case a fresh stream so earlier matrix entries cannot contaminate it.
    start_nats
    bootstrap_jetstream
  fi
  start_engine "$transport" "$provider"
  local result_dir monitor_pid output samples
  result_dir="$RESULTS/$transport-$provider-$clients"
  mkdir -p "$result_dir"
  "$BENCH_DIR/sample-container.sh" "$ENGINE" "$result_dir" &
  monitor_pid=$!
  output=$(run_realtime_client "$transport" "$provider" "$clients" "$events")
  kill "$monitor_pid" >/dev/null 2>&1 || true
  wait "$monitor_pid" >/dev/null 2>&1 || true
  printf '%s\n' "$output" >"$result_dir/result.json"
  if [[ $(jq -r '.gaps + .duplicates' <<<"$output") != 0 ]]; then
    echo "realtime correctness failure: $output" >&2
    return 1
  fi
  samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/docker-stats.tsv" "$result_dir/process-memory.tsv")
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$transport" "$provider" "$clients" "$events" \
    "$(jq -r '.received' <<<"$output")" "$(jq -r '.elapsed_seconds' <<<"$output")" \
    "$(jq -r '.published_eps' <<<"$output")" "$(jq -r '.deliveries_eps' <<<"$output")" \
    "$(jq -r '.gaps' <<<"$output")" "$(jq -r '.duplicates' <<<"$output")" \
    "$samples" "$ws_mailbox" "$graphql_broadcast" "$redis_read_batch" | tee -a "$CSV"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
  remove_container "$ENGINE"
}

run_provider_matrix() {
  local transport=$1 provider=$2
  local client_count event_count
  for client_count in ${VS_BENCH_REALTIME_CLIENTS:-1 50 200}; do
    case "$client_count" in
      1) event_count=${VS_BENCH_REALTIME_EVENTS_1:-100000} ;;
      50) event_count=${VS_BENCH_REALTIME_EVENTS_50:-10000} ;;
      200) event_count=${VS_BENCH_REALTIME_EVENTS_200:-5000} ;;
      *) echo "unsupported realtime client count $client_count" >&2; return 2 ;;
    esac
    run_case "$transport" "$provider" "$client_count" "$event_count"
  done
}

main() {
  ensure_network
  local requested=${1:-all}
  if [[ "$requested" == all || "$requested" == nats || "$requested" == nats_raw ]]; then
    start_nats
    run_provider_matrix raw nats_core
    run_provider_matrix raw nats_jetstream
  fi
  if [[ "$requested" == all || "$requested" == nats || "$requested" == nats_graphql ]]; then
    start_nats
    run_provider_matrix graphql nats_jetstream
    remove_container "$NATS"
  fi
  if [[ "$requested" == all || "$requested" == redis || "$requested" == redis_raw ]]; then
    start_redis
    run_provider_matrix raw redis_streams
  fi
  if [[ "$requested" == all || "$requested" == redis || "$requested" == redis_graphql ]]; then
    start_redis
    run_provider_matrix graphql redis_streams
    remove_container "$REDIS"
  fi
  echo "realtime benchmark results: $CSV"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  main "$@"
fi
