#!/bin/sh
set -eu

SCRIPT_DIR=$(dirname -- "$0")
ROOT_DIR=$(CDPATH='' cd -- "$SCRIPT_DIR/.." && pwd)
SUITE=${1:-all}
REDIS_IMAGE=${VS_TEST_REDIS_IMAGE:-redis:7.4-alpine}
REDISJSON_IMAGE=${VS_TEST_REDISJSON_IMAGE:-redis/redis-stack-server:7.4.0-v3}
HAPROXY_IMAGE=${VS_TEST_HAPROXY_IMAGE:-haproxy:3.1-alpine}
RUN_ID="vs-redis-$$-$(date +%s)"
NETWORK="$RUN_ID"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/$RUN_ID.XXXXXX")
CONTAINERS=""
AUTH_PASSWORD="ventstream-integration-secret"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 2
  fi
}

remember_container() {
  CONTAINERS="$CONTAINERS $1"
}

cleanup() {
  if [ "${VS_TEST_REDIS_KEEP:-0}" = "1" ]; then
    echo "Redis test infrastructure retained: $CONTAINERS"
    echo "TLS material retained at $TEMP_DIR"
    return
  fi
  if [ -n "$CONTAINERS" ]; then
    # shellcheck disable=SC2086
    docker rm --force $CONTAINERS >/dev/null 2>&1 || true
  fi
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$TEMP_DIR"
}

published_port() {
  docker port "$1" 6379/tcp | sed -n 's/.*://p' | head -n 1
}

random_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_for_redis() {
  container=$1
  password=${2:-}
  attempts=0
  while [ "$attempts" -lt 150 ]; do
    if [ -n "$password" ]; then
      if docker exec "$container" redis-cli --no-auth-warning -a "$password" PING 2>/dev/null | grep -q PONG; then
        return
      fi
    elif docker exec "$container" redis-cli PING 2>/dev/null | grep -q PONG; then
      return
    fi
    attempts=$((attempts + 1))
    sleep 0.1
  done
  echo "$container did not become ready" >&2
  docker logs "$container" >&2 || true
  exit 1
}

wait_for_proxy() {
  container=$1
  attempts=0
  while [ "$attempts" -lt 150 ]; do
    if docker run --rm --network "$NETWORK" "$REDIS_IMAGE" \
      redis-cli -h "$container" PING 2>/dev/null | grep -q PONG; then
      return
    fi
    attempts=$((attempts + 1))
    sleep 0.1
  done
  echo "$container did not expose a writable Redis endpoint" >&2
  docker logs "$container" >&2 || true
  exit 1
}

start_redis() {
  name=$1
  shift
  port=$(random_port)
  remember_container "$name"
  docker run --detach --name "$name" --network "$NETWORK" \
    --publish "127.0.0.1:$port:6379" \
    "$REDIS_IMAGE" redis-server "$@" >/dev/null
  wait_for_redis "$name"
}

start_authenticated_redis() {
  name=$1
  port=$(random_port)
  remember_container "$name"
  docker run --detach --name "$name" --network "$NETWORK" \
    --publish "127.0.0.1:$port:6379" \
    "$REDIS_IMAGE" redis-server --requirepass "$AUTH_PASSWORD" >/dev/null
  wait_for_redis "$name" "$AUTH_PASSWORD"
}

generate_tls_material() {
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 \
    -subj '/CN=VentStream Redis Test CA' \
    -keyout "$TEMP_DIR/ca.key" -out "$TEMP_DIR/ca.crt" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=127.0.0.1' \
    -keyout "$TEMP_DIR/server.key" -out "$TEMP_DIR/server.csr" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=IP:127.0.0.1,DNS:localhost' 'extendedKeyUsage=serverAuth' \
    >"$TEMP_DIR/server.ext"
  openssl x509 -req -sha256 -days 1 \
    -in "$TEMP_DIR/server.csr" -CA "$TEMP_DIR/ca.crt" -CAkey "$TEMP_DIR/ca.key" \
    -CAcreateserial -extfile "$TEMP_DIR/server.ext" -out "$TEMP_DIR/server.crt" \
    >/dev/null 2>&1
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=ventstream-test-client' \
    -keyout "$TEMP_DIR/client.key" -out "$TEMP_DIR/client.csr" >/dev/null 2>&1
  printf '%s\n' 'extendedKeyUsage=clientAuth' >"$TEMP_DIR/client.ext"
  openssl x509 -req -sha256 -days 1 \
    -in "$TEMP_DIR/client.csr" -CA "$TEMP_DIR/ca.crt" -CAkey "$TEMP_DIR/ca.key" \
    -CAcreateserial -extfile "$TEMP_DIR/client.ext" -out "$TEMP_DIR/client.crt" \
    >/dev/null 2>&1
  chmod 644 "$TEMP_DIR"/*.crt "$TEMP_DIR"/*.key
}

start_tls_redis() {
  name=$1
  port=$(random_port)
  remember_container "$name"
  docker run --detach --name "$name" --network "$NETWORK" \
    --publish "127.0.0.1:$port:6379" \
    --volume "$TEMP_DIR:/tls:ro" \
    "$REDIS_IMAGE" redis-server \
    --port 0 \
    --tls-port 6379 \
    --tls-cert-file /tls/server.crt \
    --tls-key-file /tls/server.key \
    --tls-ca-cert-file /tls/ca.crt \
    --tls-auth-clients yes >/dev/null
  attempts=0
  while [ "$attempts" -lt 150 ]; do
    if docker exec "$name" redis-cli --tls \
      --cacert /tls/ca.crt --cert /tls/client.crt --key /tls/client.key \
      PING 2>/dev/null | grep -q PONG; then
      return
    fi
    attempts=$((attempts + 1))
    sleep 0.1
  done
  echo "$name did not become ready with mutual TLS" >&2
  docker logs "$name" >&2 || true
  exit 1
}

start_contract_infrastructure() {
  STANDALONE="$RUN_ID-standalone"
  AUTH="$RUN_ID-auth"
  JSON="$RUN_ID-json"
  PRESSURE="$RUN_ID-pressure"
  REPL_PRIMARY="$RUN_ID-repl-primary"
  REPL_REPLICA="$RUN_ID-repl-replica"
  AOF_PRIMARY="$RUN_ID-aof-primary"
  AOF_REPLICA="$RUN_ID-aof-replica"
  FAIL_PRIMARY="$RUN_ID-fail-primary"
  FAIL_REPLICA="$RUN_ID-fail-replica"
  FAIL_PROXY="$RUN_ID-fail-proxy"
  TLS="$RUN_ID-tls"

  start_redis "$STANDALONE" --appendonly no
  start_authenticated_redis "$AUTH"

  remember_container "$JSON"
  json_port=$(random_port)
  docker run --detach --name "$JSON" --network "$NETWORK" \
    --publish "127.0.0.1:$json_port:6379" "$REDISJSON_IMAGE" >/dev/null
  wait_for_redis "$JSON"

  start_redis "$PRESSURE" --appendonly no
  start_redis "$REPL_PRIMARY" --appendonly no
  start_redis "$REPL_REPLICA" --appendonly no --replicaof "$REPL_PRIMARY" 6379
  start_redis "$AOF_PRIMARY" --appendonly yes --appendfsync everysec
  start_redis "$AOF_REPLICA" --appendonly yes --appendfsync everysec --replicaof "$AOF_PRIMARY" 6379
  start_redis "$FAIL_PRIMARY" --appendonly no
  start_redis "$FAIL_REPLICA" --appendonly no --replicaof "$FAIL_PRIMARY" 6379
  sed \
    -e "s/ventstream-redis-sink-primary/$FAIL_PRIMARY/g" \
    -e "s/ventstream-redis-sink-replica/$FAIL_REPLICA/g" \
    "$ROOT_DIR/crates/ventstream-sinks/tests/fixtures/redis-failover-haproxy.cfg" \
    >"$TEMP_DIR/redis-failover-haproxy.cfg"
  fail_proxy_port=$(random_port)
  remember_container "$FAIL_PROXY"
  docker run --detach --name "$FAIL_PROXY" --network "$NETWORK" \
    --publish "127.0.0.1:$fail_proxy_port:6379" \
    --volume "$TEMP_DIR/redis-failover-haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro" \
    "$HAPROXY_IMAGE" >/dev/null
  wait_for_proxy "$FAIL_PROXY"
  generate_tls_material
  start_tls_redis "$TLS"

  STANDALONE_PORT=$(published_port "$STANDALONE")
  AUTH_PORT=$(published_port "$AUTH")
  JSON_PORT=$(published_port "$JSON")
  PRESSURE_PORT=$(published_port "$PRESSURE")
  REPL_PRIMARY_PORT=$(published_port "$REPL_PRIMARY")
  AOF_PRIMARY_PORT=$(published_port "$AOF_PRIMARY")
  FAIL_PROXY_PORT=$(published_port "$FAIL_PROXY")
  FAIL_REPLICA_PORT=$(published_port "$FAIL_REPLICA")
  TLS_PORT=$(published_port "$TLS")
}

run_contracts() {
  echo "Running Redis sink contracts against standalone, authenticated, RedisJSON, pressure, replication, failover, and mTLS servers"
  start_contract_infrastructure
  env \
    VS_TEST_REDIS_SINK_URL="redis://127.0.0.1:$STANDALONE_PORT/" \
    VS_TEST_REDIS_RESTART_CONTAINER="$STANDALONE" \
    VS_TEST_REDIS_AUTH_URL="redis://127.0.0.1:$AUTH_PORT/" \
    VS_TEST_REDIS_AUTH_PASSWORD="$AUTH_PASSWORD" \
    VS_TEST_REDISJSON_URL="redis://127.0.0.1:$JSON_PORT/" \
    VS_TEST_REDIS_PRESSURE_URL="redis://127.0.0.1:$PRESSURE_PORT/" \
    VS_TEST_REDIS_REPLICATED_URL="redis://127.0.0.1:$REPL_PRIMARY_PORT/" \
    VS_TEST_REDIS_AOF_URL="redis://127.0.0.1:$AOF_PRIMARY_PORT/" \
    VS_TEST_REDIS_FAILOVER_URL="redis://127.0.0.1:$FAIL_PROXY_PORT/" \
    VS_TEST_REDIS_FAILOVER_PRIMARY_CONTAINER="$FAIL_PRIMARY" \
    VS_TEST_REDIS_FAILOVER_REPLICA_CONTAINER="$FAIL_REPLICA" \
    VS_TEST_REDIS_FAILOVER_REPLICA_URL="redis://127.0.0.1:$FAIL_REPLICA_PORT/" \
    VS_TEST_REDIS_MTLS_URL="rediss://127.0.0.1:$TLS_PORT/" \
    VS_TEST_REDIS_MTLS_CA_FILE="$TEMP_DIR/ca.crt" \
    VS_TEST_REDIS_MTLS_CLIENT_CERT_FILE="$TEMP_DIR/client.crt" \
    VS_TEST_REDIS_MTLS_CLIENT_KEY_FILE="$TEMP_DIR/client.key" \
    cargo test -p ventstream-sinks --test redis_sink_integration -- \
      --include-ignored --test-threads=1 --nocapture
  VS_TEST_REDIS_SINK_URL="redis://127.0.0.1:$STANDALONE_PORT/" \
    cargo test -p ventstream-sinks --test redis_metrics_integration -- \
      --include-ignored --test-threads=1 --nocapture
}

run_topologies() {
  echo "Running Redis Sentinel and Cluster topology contracts"
  cargo test -p ventstream-sinks --test redis_topology_integration -- \
    --ignored --test-threads=1 --nocapture
}

require cargo
require docker
require openssl
require python3
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not reachable" >&2
  exit 2
fi

case "$SUITE" in
  contracts | topology | all) ;;
  *)
    echo "usage: $0 [contracts|topology|all]" >&2
    exit 2
    ;;
esac

trap cleanup EXIT INT TERM
docker network create "$NETWORK" >/dev/null
cd "$ROOT_DIR"

case "$SUITE" in
  contracts)
    run_contracts
    ;;
  topology)
    run_topologies
    ;;
  all)
    run_contracts
    run_topologies
    ;;
esac
