#!/usr/bin/env bash
set -uo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
DURATION_SECS=${VS_SOAK_DURATION_SECS:-86400}
RUN_ID=${VS_SOAK_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RUN_DIR=${VS_SOAK_RUN_DIR:-$ROOT/target/soak/$RUN_ID}
IMAGE=${VS_SOAK_IMAGE:-ventstream-engine:adaptive-memory}
MIN_FREE_GIB=${VS_SOAK_MIN_FREE_GIB:-12}
MIN_DOCKER_FREE_GIB=${VS_SOAK_MIN_DOCKER_FREE_GIB:-8}
PHASES=(postgres mysql mongodb kafka neo4j nats_graphql redis_graphql nats_raw redis_raw)
BLOCK_SECS=${VS_SOAK_BLOCK_SECS:-$(( DURATION_SECS / ${#PHASES[@]} ))}

mkdir -p "$RUN_DIR/blocks"
EVENTS="$RUN_DIR/events.jsonl"
RESOURCES="$RUN_DIR/resources.tsv"
printf '%s\n' "$$" >"$RUN_DIR/soak.pid"
printf '%s\n' "$RUN_DIR" >"$ROOT/target/soak/latest"
printf '%s\n' 'timestamp_utc\telapsed_s\thost_free_gib\tdocker_free_gib\tcontainer\tcpu\tmemory\tpids' >"$RESOURCES"

START_EPOCH=$(date +%s)
END_EPOCH=$((START_EPOCH + DURATION_SECS))
printf '%s\n' "$START_EPOCH" >"$RUN_DIR/started.epoch"
MONITOR_PID=''
PHASE_PID=''
COMPLETED=false

emit() {
  jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg kind "$1" \
    --argjson elapsed_s "$(( $(date +%s) - START_EPOCH ))" \
    --arg detail "${2:-}" \
    '{timestamp:$timestamp,elapsed_s:$elapsed_s,kind:$kind,detail:$detail}' | tee -a "$EVENTS"
}

host_free_gib() {
  df -Pk "$ROOT" | awk 'NR==2 {printf "%d", $4/1024/1024}'
}

docker_free_gib() {
  docker run --rm --network none alpine:3.22 sh -c "df -Pk / | awk 'NR==2 {printf \"%d\", \$4/1024/1024}'" 2>/dev/null
}

resource_monitor() {
  while (( $(date +%s) < END_EPOCH )); do
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    elapsed=$(( $(date +%s) - START_EPOCH ))
    host_free=$(host_free_gib)
    docker_free=$(docker_free_gib || echo unavailable)
    names=$(docker ps --format '{{.Names}}' --filter name=vsbench 2>/dev/null | paste -sd ' ' -)
    if [[ -n $names ]]; then
      docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}' $names 2>/dev/null \
        | while IFS= read -r row; do printf '%s\t%s\t%s\t%s\t%s\n' "$now" "$elapsed" "$host_free" "$docker_free" "$row"; done >>"$RESOURCES"
    else
      printf '%s\t%s\t%s\t%s\t-\t-\t-\t-\n' "$now" "$elapsed" "$host_free" "$docker_free" >>"$RESOURCES"
    fi
    sleep 30
  done
}

cleanup() {
  rc=$?
  trap - EXIT INT TERM
  [[ -n $PHASE_PID ]] && kill -TERM "$PHASE_PID" >/dev/null 2>&1 || true
  [[ -n $PHASE_PID ]] && wait "$PHASE_PID" >/dev/null 2>&1 || true
  [[ -n $MONITOR_PID ]] && kill "$MONITOR_PID" >/dev/null 2>&1 || true
  if [[ $COMPLETED != true ]]; then
    emit soak_stopped "exit_code=$rc"
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  emit soak_aborted "image_not_found image=$IMAGE"
  exit 2
fi

emit soak_started "run_id=$RUN_ID duration_s=$DURATION_SECS block_s=$BLOCK_SECS image=$IMAGE topology=long_lived"
resource_monitor &
MONITOR_PID=$!

round=0
while (( $(date +%s) < END_EPOCH )); do
  for phase in "${PHASES[@]}"; do
    (( $(date +%s) >= END_EPOCH )) && break 2
    round=$((round + 1))
    phase_end=$(( $(date +%s) + BLOCK_SECS ))
    (( phase_end > END_EPOCH )) && phase_end=$END_EPOCH
    block_dir="$RUN_DIR/blocks/$(printf '%02d' "$round")-$phase"
    mkdir -p "$block_dir"

    host_free=$(host_free_gib)
    docker_free=$(docker_free_gib || echo 0)
    if (( host_free < MIN_FREE_GIB || docker_free < MIN_DOCKER_FREE_GIB )); then
      emit soak_aborted "disk_floor phase=$phase host_free_gib=$host_free docker_free_gib=$docker_free"
      exit 90
    fi

    emit phase_started "round=$round phase=$phase block_end_epoch=$phase_end"
    if [[ $phase == postgres || $phase == mysql || $phase == mongodb || $phase == kafka || $phase == neo4j ]]; then
      env VS_SOAK_IMAGE="$IMAGE" VS_BENCH_IMAGE="$IMAGE" VS_SOAK_BLOCK_END_EPOCH="$phase_end" VS_SOAK_BLOCK_DIR="$block_dir" \
        "$ROOT/benchmarks/soak/run-cdc-block.sh" "$phase" >"$block_dir/supervisor.log" 2>&1 &
    else
      env VS_SOAK_IMAGE="$IMAGE" VS_BENCH_IMAGE="$IMAGE" VS_SOAK_BLOCK_END_EPOCH="$phase_end" VS_SOAK_BLOCK_DIR="$block_dir" \
        "$ROOT/benchmarks/soak/run-realtime-block.sh" "$phase" >"$block_dir/supervisor.log" 2>&1 &
    fi
    PHASE_PID=$!
    wait "$PHASE_PID"
    rc=$?
    PHASE_PID=''
    if (( rc == 0 )); then
      emit phase_passed "round=$round phase=$phase"
    else
      emit phase_failed "round=$round phase=$phase exit_code=$rc log=$block_dir/supervisor.log"
    fi
  done
done

emit soak_completed "phases=$round duration_s=$(( $(date +%s) - START_EPOCH ))"
COMPLETED=true
exit 0
