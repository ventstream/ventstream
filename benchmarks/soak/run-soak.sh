#!/usr/bin/env bash
set -uo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
DURATION_SECS=${VS_SOAK_DURATION_SECS:-86400}
RUN_ID=${VS_SOAK_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RUN_DIR=${VS_SOAK_RUN_DIR:-$ROOT/target/soak/$RUN_ID}
IMAGE=${VS_SOAK_IMAGE:-ventstream-engine:adaptive-memory}
MIN_FREE_GIB=${VS_SOAK_MIN_FREE_GIB:-12}
MIN_DOCKER_FREE_GIB=${VS_SOAK_MIN_DOCKER_FREE_GIB:-8}
COMPLEX_EVERY=${VS_SOAK_COMPLEX_EVERY:-3}
OUTAGE_SECS=${VS_SOAK_OUTAGE_SECS:-8}
INJECTION_DELAY_SECS=${VS_SOAK_INJECTION_DELAY_SECS:-5}

mkdir -p "$RUN_DIR/phases"
EVENTS="$RUN_DIR/events.jsonl"
RESOURCES="$RUN_DIR/resources.tsv"
PID_FILE="$RUN_DIR/soak.pid"
LATEST_FILE="$ROOT/target/soak/latest"
printf '%s\n' "$$" >"$PID_FILE"
printf '%s\n' "$RUN_DIR" >"$LATEST_FILE"
printf '%s\n' 'timestamp_utc\telapsed_s\tdisk_free_gib\tcontainer\tcpu\tmemory\tpids' >"$RESOURCES"

START_EPOCH=$(date +%s)
END_EPOCH=$((START_EPOCH + DURATION_SECS))
PHASE_PID=''
INJECTOR_PID=''
MONITOR_PID=''

emit() {
  local kind=$1
  shift
  jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg kind "$kind" \
    --argjson elapsed_s "$(( $(date +%s) - START_EPOCH ))" \
    --arg detail "$*" \
    '{timestamp:$timestamp,elapsed_s:$elapsed_s,kind:$kind,detail:$detail}' \
    | tee -a "$EVENTS"
}

free_gib() {
  df -Pk "$ROOT" | awk 'NR==2 {printf "%d", $4/1024/1024}'
}

docker_free_gib() {
  docker run --rm --network none alpine:3.22 \
    sh -c "df -Pk / | awk 'NR==2 {printf \"%d\", \$4/1024/1024}'" 2>/dev/null
}

resource_monitor() {
  while (( $(date +%s) < END_EPOCH )); do
    local now elapsed free names
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    elapsed=$(( $(date +%s) - START_EPOCH ))
    free=$(free_gib)
    names=$(docker ps --format '{{.Names}}' --filter name=vsbench 2>/dev/null | paste -sd ' ' -)
    if [[ -n "$names" ]]; then
      docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}' $names 2>/dev/null \
        | while IFS= read -r row; do printf '%s\t%s\t%s\t%s\n' "$now" "$elapsed" "$free" "$row"; done \
        >>"$RESOURCES"
    else
      printf '%s\t%s\t%s\t-\t-\t-\t-\n' "$now" "$elapsed" "$free" >>"$RESOURCES"
    fi
    sleep 30
  done
}

unpause_if_needed() {
  local container
  for container in vsbench-opensearch vsbench-postgres vsbench-mysql vsbench-mongo vsbench-redpanda vsbench-neo4j vsbench-nats vsbench-redis; do
    if [[ $(docker inspect -f '{{.State.Paused}}' "$container" 2>/dev/null || true) == true ]]; then
      docker unpause "$container" >/dev/null 2>&1 || true
    fi
  done
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  [[ -n "$INJECTOR_PID" ]] && kill "$INJECTOR_PID" >/dev/null 2>&1 || true
  [[ -n "$MONITOR_PID" ]] && kill "$MONITOR_PID" >/dev/null 2>&1 || true
  if [[ -n "$PHASE_PID" ]]; then
    kill -TERM "$PHASE_PID" >/dev/null 2>&1 || true
    wait "$PHASE_PID" >/dev/null 2>&1 || true
  fi
  unpause_if_needed
  emit soak_stopped "exit_code=$rc"
  exit "$rc"
}
trap cleanup EXIT INT TERM

wait_and_pause() {
  local container=$1 reason=$2
  local trigger=vsbench-engine
  if [[ "$container" == vsbench-nats || "$container" == vsbench-redis ]]; then
    trigger=vsbench-realtime-engine
  fi
  local deadline=$(( $(date +%s) + 180 ))
  while (( $(date +%s) < deadline )); do
    if [[ $(docker inspect -f '{{.State.Running}}' "$trigger" 2>/dev/null || true) == true ]]; then
      sleep "$INJECTION_DELAY_SECS"
      if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true) == true ]] \
        && docker pause "$container" >/dev/null 2>&1; then
        emit injection_started "$reason container=$container duration_s=$OUTAGE_SECS"
        sleep "$OUTAGE_SECS"
        docker unpause "$container" >/dev/null 2>&1 || true
        emit injection_finished "$reason container=$container"
      fi
      return
    fi
    sleep 1
  done
  emit injection_skipped "$reason container=$container was_not_running"
}

run_phase() {
  local cycle=$1 name=$2 injector=$3
  shift 3
  local started ended rc log
  log="$RUN_DIR/phases/$(printf '%04d' "$cycle")-$name.log"

  if (( $(free_gib) < MIN_FREE_GIB )); then
    emit soak_aborted "disk_floor free_gib=$(free_gib) required_gib=$MIN_FREE_GIB"
    exit 90
  fi
  local docker_free
  docker_free=$(docker_free_gib || echo 0)
  if (( docker_free < MIN_DOCKER_FREE_GIB )); then
    emit soak_aborted "docker_disk_floor free_gib=$docker_free required_gib=$MIN_DOCKER_FREE_GIB"
    exit 91
  fi

  emit phase_started "cycle=$cycle phase=$name injector=$injector log=$log"
  started=$(date +%s)
  INJECTOR_PID=''
  case "$injector" in
    sink) wait_and_pause vsbench-opensearch sink_unavailable & INJECTOR_PID=$! ;;
    postgres) wait_and_pause vsbench-postgres source_unavailable & INJECTOR_PID=$! ;;
    mysql) wait_and_pause vsbench-mysql source_unavailable & INJECTOR_PID=$! ;;
    mongo) wait_and_pause vsbench-mongo source_unavailable & INJECTOR_PID=$! ;;
    kafka) wait_and_pause vsbench-redpanda broker_unavailable & INJECTOR_PID=$! ;;
    neo4j) wait_and_pause vsbench-neo4j source_unavailable & INJECTOR_PID=$! ;;
    nats) wait_and_pause vsbench-nats broker_unavailable & INJECTOR_PID=$! ;;
    redis) wait_and_pause vsbench-redis broker_unavailable & INJECTOR_PID=$! ;;
  esac

  "$@" >"$log" 2>&1 &
  PHASE_PID=$!
  wait "$PHASE_PID"
  rc=$?
  PHASE_PID=''
  if [[ -n "$INJECTOR_PID" ]]; then
    wait "$INJECTOR_PID" >/dev/null 2>&1 || true
    INJECTOR_PID=''
  fi
  unpause_if_needed
  ended=$(date +%s)
  if (( rc == 0 )); then
    emit phase_passed "cycle=$cycle phase=$name duration_s=$((ended-started))"
  else
    emit phase_failed "cycle=$cycle phase=$name duration_s=$((ended-started)) exit_code=$rc log=$log"
  fi
  return "$rc"
}

source_phase() {
  local cycle=$1 source=$2 injector=$3 records_var=$4 records=$5
  run_phase "$cycle" "cdc-$source" "$injector" env \
    VS_BENCH_IMAGE="$IMAGE" \
    VS_BENCH_RUN_ID="soak-$RUN_ID-$cycle-$source" \
    VS_BENCH_PROFILES=throughput \
    VS_BENCH_ENGINE_MEMORY=512m \
    VS_BENCH_PAYLOAD_BYTES=1024 \
    "$records_var=$records" \
    "$ROOT/benchmarks/container-matrix/run-sources.sh" "$source"
}

realtime_phase() {
  local cycle=$1 target=$2 injector=$3
  run_phase "$cycle" "realtime-$target" "$injector" env \
    VS_BENCH_IMAGE="$IMAGE" \
    VS_BENCH_RUN_ID="soak-$RUN_ID-$cycle-realtime-$target" \
    VS_BENCH_REALTIME_CLIENTS=200 \
    VS_BENCH_REALTIME_EVENTS_200=5000 \
    "$ROOT/benchmarks/container-matrix/run-realtime.sh" "$target"
}

complex_phase() {
  local cycle=$1
  run_phase "$cycle" complex-all sink env \
    VS_BENCH_IMAGE="$IMAGE" \
    VS_BENCH_RUN_ID="soak-$RUN_ID-$cycle-complex" \
    VS_BENCH_PROFILES=throughput \
    VS_BENCH_ENGINE_MEMORY=512m \
    VS_BENCH_PAYLOAD_BYTES=1024 \
    VS_BENCH_POSTGRES_PER_TABLE=25000 \
    VS_BENCH_MYSQL_PER_TABLE=10000 \
    VS_BENCH_MONGODB_PER_COLLECTION=25000 \
    VS_BENCH_NEO4J_PER_SPEC=2500 \
    VS_BENCH_RELATION_FANOUT=1 \
    VS_BENCH_MONGODB_UPDATES=1 \
    VS_BENCH_NEO4J_FANOUT=1 \
    "$ROOT/benchmarks/container-matrix/run-complex.sh" all
}

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  emit soak_aborted "image_not_found image=$IMAGE"
  exit 2
fi

emit soak_started "run_id=$RUN_ID duration_s=$DURATION_SECS image=$IMAGE min_free_gib=$MIN_FREE_GIB min_docker_free_gib=$MIN_DOCKER_FREE_GIB"
resource_monitor &
MONITOR_PID=$!

cycle=0
while (( $(date +%s) < END_EPOCH )); do
  cycle=$((cycle + 1))
  emit cycle_started "cycle=$cycle"

  source_phase "$cycle" postgres sink VS_BENCH_POSTGRES_RECORDS 200000 || true
  (( $(date +%s) >= END_EPOCH )) && break
  source_phase "$cycle" mysql mysql VS_BENCH_MYSQL_RECORDS 100000 || true
  (( $(date +%s) >= END_EPOCH )) && break
  source_phase "$cycle" mongodb mongo VS_BENCH_MONGODB_RECORDS 200000 || true
  (( $(date +%s) >= END_EPOCH )) && break
  source_phase "$cycle" kafka kafka VS_BENCH_KAFKA_RECORDS 400000 || true
  (( $(date +%s) >= END_EPOCH )) && break
  source_phase "$cycle" neo4j neo4j VS_BENCH_NEO4J_RECORDS 20000 || true
  (( $(date +%s) >= END_EPOCH )) && break

  realtime_phase "$cycle" nats_graphql nats || true
  (( $(date +%s) >= END_EPOCH )) && break
  realtime_phase "$cycle" redis_graphql redis || true
  (( $(date +%s) >= END_EPOCH )) && break
  realtime_phase "$cycle" nats_raw none || true
  (( $(date +%s) >= END_EPOCH )) && break
  realtime_phase "$cycle" redis_raw none || true

  if (( cycle % COMPLEX_EVERY == 0 && $(date +%s) < END_EPOCH )); then
    complex_phase "$cycle" || true
  fi
  emit cycle_finished "cycle=$cycle"
done

emit soak_completed "cycles=$cycle duration_s=$(( $(date +%s) - START_EPOCH ))"
exit 0
