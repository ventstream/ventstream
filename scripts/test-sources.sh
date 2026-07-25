#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SOURCE=${1:-all}

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 2
  fi
}

run_source() {
  source=$1
  echo "Running local $source source integration suite"
  cargo test -p ventstream --test "it_$source" -- --ignored --test-threads=1 --nocapture
}

require cargo
require docker
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not reachable" >&2
  exit 2
fi

cd "$ROOT_DIR"
case "$SOURCE" in
  postgres | neo4j | mongodb | mysql | kafka)
    run_source "$SOURCE"
    ;;
  all)
    # Keep OpenSearch and source containers sequential to bound local memory.
    for source in postgres neo4j mongodb mysql kafka; do
      run_source "$source"
    done
    ;;
  *)
    echo "usage: $0 [postgres|neo4j|mongodb|mysql|kafka|all]" >&2
    exit 2
    ;;
esac
