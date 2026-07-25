#!/usr/bin/env bash
set -euo pipefail

container=${1:?container name is required}
output=${2:?output directory is required}
mkdir -p "$output"

stats_file="$output/docker-stats.tsv"
rss_file="$output/process-memory.tsv"
: >"$stats_file"
: >"$rss_file"

(
  while docker inspect "$container" >/dev/null 2>&1; do
    docker stats --no-stream --format '{{.CPUPerc}}\t{{.MemUsage}}' "$container" 2>/dev/null || break
  done
) >"$stats_file" &
stats_pid=$!

monitor_name="${container}-proc-monitor"
docker rm -f "$monitor_name" >/dev/null 2>&1 || true
docker run --rm --name "$monitor_name" --pid "container:$container" alpine:3.22 \
  sh -c 'while kill -0 1 2>/dev/null; do
    now=$(date +%s)
    awk -v now="$now" '\''
      /^VmRSS:/ { rss=$2 }
      /^VmHWM:/ { hwm=$2 }
      END { printf "%s\t%s\t%s\n", now, rss+0, hwm+0 }
    '\'' /proc/1/status
    sleep 1
  done' >"$rss_file" 2>/dev/null &
rss_pid=$!

cleanup() {
  kill "$stats_pid" "$rss_pid" >/dev/null 2>&1 || true
  wait "$stats_pid" "$rss_pid" >/dev/null 2>&1 || true
  docker rm -f "$monitor_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 0' INT TERM

while docker inspect "$container" >/dev/null 2>&1; do
  sleep 1
done
