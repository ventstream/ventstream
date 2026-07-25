#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TARGET=${1:?usage: run-realtime-block.sh nats_graphql|nats_raw|redis_graphql|redis_raw}
BLOCK_END_EPOCH=${VS_SOAK_BLOCK_END_EPOCH:-$(( $(date +%s) + 3600 ))}
OUT=${VS_SOAK_BLOCK_DIR:-$ROOT/target/soak/realtime-$TARGET-$(date -u +%Y%m%dT%H%M%SZ)}
OUTAGE_SECS=${VS_SOAK_OUTAGE_SECS:-8}
OUTAGE_EVERY=${VS_SOAK_OUTAGE_EVERY:-10}
RESTART_EVERY=${VS_SOAK_RESTART_EVERY:-20}
ROUND_SLEEP_SECS=${VS_SOAK_BATCH_SLEEP_SECS:-15}
CLIENTS=${VS_SOAK_REALTIME_CLIENTS:-200}
PUBLISHED_EVENTS=${VS_SOAK_REALTIME_EVENTS:-5000}
VS_BENCH_RESULTS=$OUT/benchmark
export VS_BENCH_RESULTS

source "$ROOT/benchmarks/container-matrix/run-realtime.sh"

mkdir -p "$OUT/rounds"
EVENTS="$OUT/events.jsonl"

finish() {
  local rc=$?
  trap - EXIT INT TERM
  if (( rc != 0 )); then
    docker logs "$ENGINE" >"$OUT/engine-failure.log" 2>&1 || true
    docker inspect "$ENGINE" "$BROKER" >"$OUT/docker-inspect-failure.json" 2>&1 || true
    emit_block block_failed "target=$TARGET exit_code=$rc" || true
  fi
  cleanup
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

emit_block() {
  jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg kind "$1" \
    --arg detail "${2:-}" \
    '{timestamp:$timestamp,kind:$kind,detail:$detail}' | tee -a "$EVENTS"
}

case "$TARGET" in
  nats_graphql) TRANSPORT=graphql; PROVIDER=nats_jetstream; BROKER=$NATS ;;
  nats_raw) TRANSPORT=raw; PROVIDER=nats_core; BROKER=$NATS ;;
  redis_graphql) TRANSPORT=graphql; PROVIDER=redis_streams; BROKER=$REDIS ;;
  redis_raw) TRANSPORT=raw; PROVIDER=redis_streams; BROKER=$REDIS ;;
  *) echo "unsupported realtime target: $TARGET" >&2; exit 2 ;;
esac

ensure_network
if [[ $PROVIDER == redis_streams ]]; then
  start_redis
else
  start_nats
  if [[ $PROVIDER == nats_jetstream ]]; then bootstrap_jetstream; fi
fi
start_engine "$TRANSPORT" "$PROVIDER"

if [[ $TRANSPORT == raw ]]; then SERVICE_PORT=4040; else SERVICE_PORT=4041; fi

emit_block block_started "target=$TARGET clients=$CLIENTS events=$PUBLISHED_EVENTS end_epoch=$BLOCK_END_EPOCH"
round=0
while (( $(date +%s) < BLOCK_END_EPOCH )); do
  round=$((round + 1))
  if (( RESTART_EVERY > 0 && round > 1 && round % RESTART_EVERY == 0 )); then
    emit_block engine_restart_started "round=$round"
    docker restart "$ENGINE" >/dev/null
    wait_for 'realtime engine readiness after restart' probe_http "http://$ENGINE:4043/readyz"
    wait_for 'realtime listener after restart' probe_tcp "$ENGINE" "$SERVICE_PORT"
    sleep 1
    emit_block engine_restart_finished "round=$round"
  fi
  if (( OUTAGE_EVERY > 0 && round % OUTAGE_EVERY == 0 )); then
    emit_block outage_started "round=$round target=$BROKER"
    docker pause "$BROKER" >/dev/null
    sleep "$OUTAGE_SECS"
    docker unpause "$BROKER" >/dev/null
    sleep 2
    emit_block outage_finished "round=$round target=$BROKER"
  fi

  log="$OUT/rounds/$(printf '%05d' "$round").log"
  started=$(date +%s)
  if ! output=$(run_realtime_client "$TRANSPORT" "$PROVIDER" "$CLIENTS" "$PUBLISHED_EVENTS" 2>"$log"); then
    client_error=$(grep -m1 '^Error:' "$log" || tail -n 1 "$log")
    emit_block round_failed "round=$round client_error=$client_error"
    exit 1
  fi
  printf '%s\n' "$output" >>"$log"
  if [[ $(jq -r '.gaps + .duplicates' <<<"$output") != 0 ]] || [[ $(jq -r '.received' <<<"$output") != $((CLIENTS * PUBLISHED_EVENTS)) ]]; then
    emit_block round_failed "round=$round result=$output"
    docker logs "$ENGINE" >"$OUT/engine-failure.log" 2>&1 || true
    exit 1
  fi
  elapsed=$(( $(date +%s) - started ))
  emit_block round_passed "round=$round duration_s=$elapsed result=$output"
  sleep "$ROUND_SLEEP_SECS"
done

docker logs "$ENGINE" >"$OUT/engine.log" 2>&1 || true
emit_block block_completed "target=$TARGET rounds=$round"
