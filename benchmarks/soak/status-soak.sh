#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
RUN_DIR=${1:-}
if [[ -z "$RUN_DIR" && -f "$ROOT/target/soak/latest" ]]; then
  RUN_DIR=$(<"$ROOT/target/soak/latest")
fi
if [[ -z "$RUN_DIR" || ! -d "$RUN_DIR" ]]; then
  echo 'no soak run found' >&2
  exit 1
fi

PID=$(<"$RUN_DIR/soak.pid")
if kill -0 "$PID" >/dev/null 2>&1; then state=running; else state=stopped; fi
START=$(jq -r 'select(.kind=="soak_started") | .timestamp' "$RUN_DIR/events.jsonl" | head -1)
LAST=$(tail -1 "$RUN_DIR/events.jsonl")
PASSED=$(jq -s '[.[] | select(.kind=="phase_passed")] | length' "$RUN_DIR/events.jsonl")
FAILED=$(jq -s '[.[] | select(.kind=="phase_failed")] | length' "$RUN_DIR/events.jsonl")
if [[ -f "$RUN_DIR/started.epoch" ]]; then
  START_EPOCH=$(<"$RUN_DIR/started.epoch")
  ELAPSED=$(( $(date +%s) - START_EPOCH ))
else
  ELAPSED=$(jq -s 'map(.elapsed_s // 0) | max' "$RUN_DIR/events.jsonl")
fi
CURRENT=$(jq -r 'select(.kind=="phase_started") | .detail' "$RUN_DIR/events.jsonl" | tail -1)
CURRENT_DIR=$(find "$RUN_DIR/blocks" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | sort | tail -1)
VALIDATIONS=0
CURRENT_EVENT='none'
if [[ -n "$CURRENT_DIR" && -f "$CURRENT_DIR/events.jsonl" ]]; then
  VALIDATIONS=$(jq -s '[.[] | select(.kind=="batch_passed" or .kind=="round_passed")] | length' "$CURRENT_DIR/events.jsonl")
  CURRENT_EVENT=$(tail -1 "$CURRENT_DIR/events.jsonl")
fi

printf 'state: %s\n' "$state"
printf 'pid: %s\n' "$PID"
printf 'started: %s\n' "$START"
printf 'elapsed: %02d:%02d:%02d\n' "$((ELAPSED/3600))" "$(((ELAPSED%3600)/60))" "$((ELAPSED%60))"
printf 'run directory: %s\n' "$RUN_DIR"
printf 'phases passed: %s\n' "$PASSED"
printf 'phases failed: %s\n' "$FAILED"
printf 'current phase: %s\n' "$CURRENT"
printf 'current phase validations: %s\n' "$VALIDATIONS"
printf 'current validation event: %s\n' "$CURRENT_EVENT"
printf 'latest event: %s\n' "$LAST"
printf '\nRecent events:\n'
tail -10 "$RUN_DIR/events.jsonl"
