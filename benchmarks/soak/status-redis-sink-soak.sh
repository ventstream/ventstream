#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
RUN_DIR=${1:-}
if [[ -z $RUN_DIR && -f "$ROOT/target/soak/latest-redis-sink" ]]; then
  RUN_DIR=$(<"$ROOT/target/soak/latest-redis-sink")
fi
if [[ -z $RUN_DIR || ! -d $RUN_DIR ]]; then
  echo 'no Redis sink soak found' >&2
  exit 1
fi

PID=$(<"$RUN_DIR/soak.pid")
if kill -0 "$PID" >/dev/null 2>&1; then state=running; else state=stopped; fi
START_EPOCH=$(<"$RUN_DIR/started.epoch")
ELAPSED=$(( $(date +%s) - START_EPOCH ))
EVENTS="$RUN_DIR/blocks/01-redis-sink/events.jsonl"
TOP_EVENTS="$RUN_DIR/events.jsonl"
RESOURCES="$RUN_DIR/resources.tsv"
PASSED=$(jq -s '[.[] | select(.kind=="batch_passed")] | length' "$EVENTS")
FAILED=$(jq -s '[.[] | select(.kind=="invariant_failed")] | length' "$EVENTS")
INJECTIONS=$(jq -s '[.[] | select(.kind=="injection_finished")] | length' "$EVENTS")
LAST_BATCH=$(jq -c 'select(.kind=="batch_passed")' "$EVENTS" | tail -n1)
LAST_EVENT=$(tail -n1 "$EVENTS")
ENGINE_SAMPLE=$(awk -F '\t' '$3 ~ /-engine$/ {line=$0} END {print line}' "$RESOURCES")
PEAK_MEMORY=$(awk -F '\t' '$3 ~ /-engine$/ {print $5}' "$RESOURCES" | sort -h | tail -n1)
LATEST_TOP=$(tail -n1 "$TOP_EVENTS")

printf 'state: %s\n' "$state"
printf 'pid: %s\n' "$PID"
printf 'elapsed: %02d:%02d:%02d\n' "$((ELAPSED/3600))" "$(((ELAPSED%3600)/60))" "$((ELAPSED%60))"
printf 'run directory: %s\n' "$RUN_DIR"
printf 'validated batches: %s\n' "$PASSED"
printf 'completed injections: %s\n' "$INJECTIONS"
printf 'invariant failures: %s\n' "$FAILED"
printf 'engine peak memory sample: %s\n' "${PEAK_MEMORY:-unavailable}"
printf 'latest engine sample: %s\n' "${ENGINE_SAMPLE:-unavailable}"
printf 'latest batch: %s\n' "${LAST_BATCH:-none}"
printf 'latest event: %s\n' "$LAST_EVENT"
printf 'latest supervisor event: %s\n' "$LATEST_TOP"
printf '\nRecent events:\n'
tail -12 "$EVENTS"
