#!/usr/bin/env bash
set -euo pipefail

container=${1:?container name is required}
output=${2:?output directory is required}
mkdir -p "$output"

stats_file="$output/docker-stats.tsv"
rss_file="$output/process-memory.tsv"
: >"$stats_file"
: >"$rss_file"

trap 'exit 0' INT TERM
while docker inspect "$container" >/dev/null 2>&1; do
  docker stats --no-stream --format '{{.CPUPerc}}\t{{.MemUsage}}' "$container" 2>/dev/null || break
done >"$stats_file"
