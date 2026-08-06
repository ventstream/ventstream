#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
DURATION_SECS=${VS_SOAK_DURATION_SECS:-86400}
RUN_ID=${VS_SOAK_RUN_ID:-redis-sink-$(date -u +%Y%m%dT%H%M%SZ)}
RUN_DIR=${VS_SOAK_RUN_DIR:-$ROOT/target/soak/$RUN_ID}
IMAGE=${VS_SOAK_IMAGE:-ventstream-engine:redis-sink-candidate}
WORKING_SET=${VS_SOAK_WORKING_SET:-20000}
BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-2000}
DELETE_RECORDS=${VS_SOAK_DELETE_RECORDS:-200}
CYCLE_SLEEP_SECS=${VS_SOAK_CYCLE_SLEEP_SECS:-10}
INJECTION_EVERY=${VS_SOAK_INJECTION_EVERY:-30}
SOURCE_OUTAGE_SECS=${VS_SOAK_SOURCE_OUTAGE_SECS:-8}
REDIS_OUTAGE_SECS=${VS_SOAK_REDIS_OUTAGE_SECS:-42}
RECONCILE_TIMEOUT_SECS=${VS_SOAK_RECONCILE_TIMEOUT_SECS:-180}
PREFIX=${VS_SOAK_REDIS_PREFIX:-ventstream:soak:orders}
SERVICE=${VS_SOAK_SENTINEL_SERVICE:-ventstream-soak-primary}
INITIAL_PASSWORD=${VS_SOAK_REDIS_PASSWORD:-ventstream-soak-a}
MIN_FREE_GIB=${VS_SOAK_MIN_FREE_GIB:-12}
RUST_LOG_FILTER=${VS_SOAK_RUST_LOG:-info}
CHECK_SLOT_EACH_CYCLE=${VS_SOAK_CHECK_SLOT_EACH_CYCLE:-true}

SHORT_ID=$(printf '%s' "$RUN_ID" | shasum -a 256 | cut -c1-10)
NAME="vsr-$SHORT_ID"
NETWORK="$NAME"
POSTGRES="$NAME-postgres"
REDIS_A="$NAME-redis-a"
REDIS_B="$NAME-redis-b"
SENTINEL_A="$NAME-sentinel-a"
SENTINEL_B="$NAME-sentinel-b"
SENTINEL_C="$NAME-sentinel-c"
ENGINE="$NAME-engine"
STATE_VOLUME="$NAME-engine-state"
REDIS_A_VOLUME="$NAME-redis-a-data"
REDIS_B_VOLUME="$NAME-redis-b-data"
CONFIG_DIR="$RUN_DIR/config"
SECRETS_DIR="$RUN_DIR/secrets"
BLOCK_DIR="$RUN_DIR/blocks/01-redis-sink"
EVENTS="$BLOCK_DIR/events.jsonl"
TOP_EVENTS="$RUN_DIR/events.jsonl"
RESOURCES="$RUN_DIR/resources.tsv"
ENGINE_LOG="$BLOCK_DIR/engine.log"
START_EPOCH=$(date +%s)
END_EPOCH=$((START_EPOCH + DURATION_SECS))
CURRENT_PASSWORD=$INITIAL_PASSWORD
MONITOR_PID=''
COMPLETED=false

mkdir -p "$CONFIG_DIR/redis-a" "$CONFIG_DIR/redis-b" "$SECRETS_DIR" "$BLOCK_DIR"
chmod 700 "$SECRETS_DIR"
printf '%s\n' "$$" >"$RUN_DIR/soak.pid"
printf '%s\n' "$START_EPOCH" >"$RUN_DIR/started.epoch"
printf '%s\n' "$RUN_DIR" >"$ROOT/target/soak/latest-redis-sink"
printf '%s\n' 'timestamp_utc\telapsed_s\tcontainer\tcpu\tmemory\tpids\treadiness\tdlq_bytes' >"$RESOURCES"

emit_to() {
  local file=$1 kind=$2 detail=${3:-}
  jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg kind "$kind" \
    --argjson elapsed_s "$(( $(date +%s) - START_EPOCH ))" \
    --arg detail "$detail" \
    '{timestamp:$timestamp,elapsed_s:$elapsed_s,kind:$kind,detail:$detail}' | tee -a "$file"
}

emit() {
  emit_to "$EVENTS" "$1" "${2:-}"
}

emit_top() {
  emit_to "$TOP_EVENTS" "$1" "${2:-}"
}

remove_container() {
  docker rm -fv "$1" >/dev/null 2>&1 || true
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  [[ -n $MONITOR_PID ]] && kill "$MONITOR_PID" >/dev/null 2>&1 || true
  [[ -n $MONITOR_PID ]] && wait "$MONITOR_PID" >/dev/null 2>&1 || true
  docker logs "$ENGINE" >"$ENGINE_LOG" 2>&1 || true
  docker inspect "$ENGINE" "$POSTGRES" "$REDIS_A" "$REDIS_B" \
    >"$BLOCK_DIR/docker-inspect.json" 2>&1 || true
  for container in "$ENGINE" "$SENTINEL_A" "$SENTINEL_B" "$SENTINEL_C" "$REDIS_A" "$REDIS_B" "$POSTGRES"; do
    docker logs "$container" >"$BLOCK_DIR/$container.log" 2>&1 || true
    if [[ $(docker inspect -f '{{.State.Paused}}' "$container" 2>/dev/null || true) == true ]]; then
      docker unpause "$container" >/dev/null 2>&1 || true
    fi
    remove_container "$container"
  done
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  docker volume rm -f "$STATE_VOLUME" "$REDIS_A_VOLUME" "$REDIS_B_VOLUME" >/dev/null 2>&1 || true
  if [[ $COMPLETED != true ]]; then
    emit_top soak_stopped "exit_code=$rc"
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

wait_for() {
  local description=$1 timeout=$2
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      emit invariant_failed "wait_timeout=$description timeout_s=$timeout"
      return 1
    fi
    sleep 1
  done
}

host_free_gib() {
  df -Pk "$ROOT" | awk 'NR==2 {printf "%d", $4/1024/1024}'
}

engine_port() {
  docker port "$ENGINE" 4043/tcp | awk -F: 'NR==1 {print $NF}'
}

ready() {
  curl -fsS "http://127.0.0.1:$(engine_port)/readyz"
}

metrics() {
  curl -fsS "http://127.0.0.1:$(engine_port)/metrics"
}

dlq_bytes() {
  docker run --rm --network none -v "$STATE_VOLUME:/state:ro" alpine:3.22 \
    sh -c 'wc -c </state/dlq.jsonl 2>/dev/null || printf 0' 2>/dev/null | tr -d '[:space:]'
}

resource_monitor() {
  while (( $(date +%s) < END_EPOCH )); do
    local now elapsed readiness dlq name
    local names=()
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    elapsed=$(( $(date +%s) - START_EPOCH ))
    if ready >/dev/null 2>&1; then readiness=ready; else readiness=unready; fi
    dlq=$(dlq_bytes || echo unavailable)
    while IFS= read -r name; do
      [[ -n $name ]] && names+=("$name")
    done < <(docker ps --format '{{.Names}}' --filter "name=$NAME")
    if (( ${#names[@]} > 0 )); then
      docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}' "${names[@]}" 2>/dev/null \
        | while IFS= read -r row; do
            printf '%s\t%s\t%s\t%s\t%s\n' "$now" "$elapsed" "$row" "$readiness" "$dlq"
          done >>"$RESOURCES"
    fi
    sleep 30
  done
}

write_runtime_files() {
  printf '%s\n' "$INITIAL_PASSWORD" >"$SECRETS_DIR/redis-password"
  chmod 600 "$SECRETS_DIR/redis-password"

  cat >"$CONFIG_DIR/joins.yaml" <<'YAML'
joins:
  - name: soak_orders
    primary:
      table: bench.orders
      pk: id
    target:
      index: redis-soak-orders
    related: []
    state:
      backend: memory
    backfill:
      mode: none
YAML

  cat >"$CONFIG_DIR/ventstream.yaml" <<'YAML'
schema_version: 1
roles: [cdc]

source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    port: 5432
    user_ref: env:VS_PG_USER
    password_ref: env:VS_PG_PASSWORD
    database_ref: env:VS_PG_DATABASE
    publication_ref: env:VS_PG_PUBLICATION
    slot_ref: env:VS_PG_SLOT
    bootstrap:
      mode: none
      chunk_size: 1000
    denormalize_mode: sql
    sink_reverse_lookup: false
    tls:
      mode: disabled

sink:
  kind: redis
  redis:
    topology:
      mode: sentinel
      service_name: ventstream-soak-primary
      endpoints:
        - env:VS_REDIS_SENTINEL_A
        - env:VS_REDIS_SENTINEL_B
        - env:VS_REDIS_SENTINEL_C
      data_node_tls: false
    auth:
      mode: password
      password_ref: file:/secrets/redis-password
    keyspace:
      prefix: ventstream:soak:orders
      ownership: exclusive
      routing:
        strategy: fixed
        name: orders
    document:
      format: string
    contract:
      mode: materialized_view
    acknowledgement:
      mode: replicated
      replicas: 1
      timeout_ms: 1000
    writer:
      id_ref: env:VS_REDIS_SINK_WRITER_ID
      lease_ms: 9000
    connect_timeout_ms: 3000
    response_timeout_ms: 5000

specs:
  joins: /config/joins.yaml

runtime:
  health_listen: 0.0.0.0:4043
  bus_capacity: 16384
  dlq_path: /var/lib/ventstream/dlq.jsonl
  dispatch:
    max_events: 2000
    max_batch_bytes: 8388608
    flush_ms: 20
    parallel_bulks: 4
  memory:
    enabled: true
    budget_bytes: 402653184
    max_event_bytes: 33554432
    sample_ms: 250
    recovery_ms: 1000
  joins:
    state_dir: /var/lib/ventstream/joins
YAML

  sed -i.bak \
    -e "s/service_name: ventstream-soak-primary/service_name: $SERVICE/" \
    -e "s/prefix: ventstream:soak:orders/prefix: $PREFIX/" \
    "$CONFIG_DIR/ventstream.yaml"
  rm -f "$CONFIG_DIR/ventstream.yaml.bak"

  cat >"$CONFIG_DIR/aggregate.lua" <<'LUA'
local cursor = '0'
local count = 0
local id_sum = 0
local version_sum = 0
repeat
  local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 1000)
  cursor = result[1]
  for _, key in ipairs(result[2]) do
    local raw = redis.call('GET', key)
    if raw then
      local ok, document = pcall(cjson.decode, raw)
      if not ok then
        return redis.error_reply('invalid JSON at ' .. key)
      end
      count = count + 1
      id_sum = id_sum + tonumber(document.id)
      version_sum = version_sum + tonumber(document.version)
    end
  end
until cursor == '0'
return {count, id_sum, version_sum}
LUA

  cat >"$CONFIG_DIR/dump-documents.lua" <<'LUA'
local cursor = '0'
local rows = {}
repeat
  local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 1000)
  cursor = result[1]
  for _, key in ipairs(result[2]) do
    local raw = redis.call('GET', key)
    if raw then
      local ok, document = pcall(cjson.decode, raw)
      if not ok then
        rows[#rows + 1] = 'invalid\tinvalid\t' .. key
      else
        rows[#rows + 1] = tostring(document.id) .. '\t' .. tostring(document.version) .. '\t' .. key
      end
    end
  end
until cursor == '0'
return rows
LUA

  write_redis_config "$REDIS_A" "$CONFIG_DIR/redis-a/redis.conf" ''
  write_redis_config "$REDIS_B" "$CONFIG_DIR/redis-b/redis.conf" "replicaof $REDIS_A 6379"
}

write_redis_config() {
  local node=$1 path=$2 replica_line=$3
  cat >"$path" <<EOF
bind 0.0.0.0
protected-mode no
port 6379
dir /data
appendonly yes
appendfsync everysec
requirepass $INITIAL_PASSWORD
masterauth $INITIAL_PASSWORD
$replica_line
EOF
  chmod 666 "$path"
  chmod 777 "$(dirname "$path")"
}

start_postgres() {
  docker run -d --name "$POSTGRES" --network "$NETWORK" \
    --cpus 1 --memory 768m \
    -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=ventstream -e POSTGRES_DB=soak \
    postgres:16-alpine postgres -c wal_level=logical -c max_wal_senders=4 \
    -c max_replication_slots=4 -c shared_buffers=192MB >/dev/null
  wait_for PostgreSQL 90 docker exec "$POSTGRES" pg_isready -U ventstream -d soak
  docker exec "$POSTGRES" psql -U ventstream -d soak -v ON_ERROR_STOP=1 -c \
    "CREATE SCHEMA bench;
     CREATE TABLE bench.orders(
       id bigint PRIMARY KEY,
       version bigint NOT NULL,
       status text NOT NULL,
       payload text NOT NULL
     );
     CREATE PUBLICATION ventstream_soak_pub FOR TABLE bench.orders;" >/dev/null
}

start_redis_node() {
  local node=$1 config_dir=$2 volume=$3
  docker run -d --name "$node" --network "$NETWORK" \
    --cpus 0.5 --memory 256m \
    -v "$config_dir:/config" -v "$CONFIG_DIR:/soak:ro" -v "$volume:/data" \
    redis:7.4-alpine redis-server /config/redis.conf >/dev/null
  wait_for "$node" 60 redis_ping "$node"
}

redis_ping() {
  docker exec "$1" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" PING | grep -q PONG
}

write_sentinel_config() {
  local path=$1
  cat >"$path" <<EOF
bind 0.0.0.0
protected-mode no
port 26379
dir /tmp
sentinel monitor $SERVICE $REDIS_A 6379 2
sentinel auth-pass $SERVICE $CURRENT_PASSWORD
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel down-after-milliseconds $SERVICE 2000
sentinel failover-timeout $SERVICE 10000
sentinel parallel-syncs $SERVICE 1
EOF
  chmod 666 "$path"
}

start_sentinel() {
  local node=$1
  local directory="$CONFIG_DIR/$node"
  mkdir -p "$directory"
  chmod 777 "$directory"
  write_sentinel_config "$directory/sentinel.conf"
  docker run -d --name "$node" --network "$NETWORK" \
    --cpus 0.15 --memory 64m -v "$directory:/config" \
    redis:7.4-alpine redis-sentinel /config/sentinel.conf >/dev/null
  wait_for "$node" 60 sentinel_ping "$node"
}

sentinel_ping() {
  docker exec "$1" redis-cli -p 26379 PING | grep -q PONG
}

master_container() {
  local node role
  for node in "$REDIS_A" "$REDIS_B"; do
    role=$(docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" --raw ROLE 2>/dev/null | head -n1 || true)
    if [[ $role == master ]]; then
      printf '%s\n' "$node"
      return 0
    fi
  done
  return 1
}

replica_container() {
  local master
  master=$(master_container) || return 1
  if [[ $master == "$REDIS_A" ]]; then printf '%s\n' "$REDIS_B"; else printf '%s\n' "$REDIS_A"; fi
}

replica_ready() {
  local replica
  replica=$(replica_container) || return 1
  docker exec "$replica" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" INFO replication 2>/dev/null \
    | tr -d '\r' | grep -q '^master_link_status:up$'
}

start_engine() {
  remove_container "$ENGINE"
  docker run -d --name "$ENGINE" --network "$NETWORK" \
    --cpus 2 --memory 768m --log-opt max-size=50m --log-opt max-file=3 \
    -p 127.0.0.1::4043 \
    -v "$STATE_VOLUME:/var/lib/ventstream" \
    -v "$CONFIG_DIR:/config:ro" -v "$SECRETS_DIR:/secrets:ro" \
    -e VS_ENGINE_CONFIG=/config/ventstream.yaml \
    -e VS_PG_HOST="$POSTGRES" -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream \
    -e VS_PG_DATABASE=soak -e VS_PG_PUBLICATION=ventstream_soak_pub -e VS_PG_SLOT=ventstream_soak_slot \
    -e VS_REDIS_SENTINEL_A="redis://$SENTINEL_A:26379" \
    -e VS_REDIS_SENTINEL_B="redis://$SENTINEL_B:26379" \
    -e VS_REDIS_SENTINEL_C="redis://$SENTINEL_C:26379" \
    -e VS_REDIS_SINK_WRITER_ID=redis-sink-soak-candidate \
    -e RUST_LOG="$RUST_LOG_FILTER" -e VS_LOG_FORMAT=json \
    "$IMAGE" >/dev/null
  wait_for engine-readiness 120 ready
}

source_aggregate() {
  docker exec "$POSTGRES" psql -U ventstream -d soak -At -F '|' -c \
    'SELECT count(*), coalesce(sum(id),0), coalesce(sum(version),0) FROM bench.orders'
}

redis_aggregate() {
  local node=$1 pattern="$PREFIX:{orders}:*"
  docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" --raw \
    --eval /soak/aggregate.lua , "$pattern" 2>/dev/null | paste -sd '|' -
}

source_documents() {
  docker exec "$POSTGRES" psql -U ventstream -d soak -At -F $'\t' -c \
    'SELECT id, version FROM bench.orders ORDER BY id'
}

redis_documents() {
  local node=$1 pattern="$PREFIX:{orders}:*"
  docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" --raw \
    --eval /soak/dump-documents.lua , "$pattern" 2>/dev/null | LC_ALL=C sort -n -k1,1
}

capture_mismatch() {
  local cycle=$1 master=$2 replica=$3
  local directory="$BLOCK_DIR/mismatch-cycle-$cycle" mismatch_count
  mkdir -p "$directory"
  source_documents >"$directory/source.tsv" 2>"$directory/source.err" || true
  redis_documents "$master" >"$directory/primary.tsv" 2>"$directory/primary.err" || true
  redis_documents "$replica" >"$directory/replica.tsv" 2>"$directory/replica.err" || true
  join -t $'\t' -a 1 -a 2 -e '<missing>' -o '0,1.2,2.2,2.3' \
    "$directory/source.tsv" "$directory/primary.tsv" \
    >"$directory/comparison.tsv" 2>"$directory/comparison.err" || true
  awk -F $'\t' '$2 != $3' "$directory/comparison.tsv" \
    >"$directory/mismatches.tsv"
  mismatch_count=$(wc -l <"$directory/mismatches.tsv" | tr -d '[:space:]')
  emit mismatch_captured \
    "cycle=$cycle records=$mismatch_count artifact=$directory/mismatches.tsv"
}

slot_caught_up() {
  local commit_lsn=$1
  [[ $(docker exec "$POSTGRES" psql -U ventstream -d soak -At -c \
    "SELECT confirmed_flush_lsn >= '$commit_lsn'::pg_lsn FROM pg_replication_slots WHERE slot_name='ventstream_soak_slot'") == t ]]
}

slot_reconciled() {
  local commit_lsn=$1
  [[ $CHECK_SLOT_EACH_CYCLE != true ]] || slot_caught_up "$commit_lsn"
}

assert_slot_behind() {
  local commit_lsn=$1
  if slot_caught_up "$commit_lsn"; then
    emit invariant_failed "source_progress_advanced_before_sink_ack commit_lsn=$commit_lsn"
    return 1
  fi
  emit source_progress_blocked "commit_lsn=$commit_lsn"
}

assert_dlq_empty() {
  local bytes
  bytes=$(dlq_bytes)
  if [[ $bytes != 0 ]]; then
    emit invariant_failed "dlq_bytes=$bytes"
    return 1
  fi
}

reconcile() {
  local cycle=$1 commit_lsn=$2 expected master replica primary_state replica_state deadline
  expected=$(source_aggregate)
  deadline=$((SECONDS + RECONCILE_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    if [[ $(docker inspect -f '{{.State.Running}}' "$ENGINE" 2>/dev/null || true) != true ]]; then
      emit invariant_failed "engine_exited cycle=$cycle"
      return 1
    fi
    if master=$(master_container 2>/dev/null) && replica=$(replica_container 2>/dev/null) && replica_ready; then
      primary_state=$(redis_aggregate "$master" || true)
      replica_state=$(redis_aggregate "$replica" || true)
      if [[ $primary_state == "$expected" && $replica_state == "$expected" ]] \
        && slot_reconciled "$commit_lsn" && ready >/dev/null 2>&1; then
        assert_dlq_empty
        emit batch_passed "cycle=$cycle source=$expected primary=$primary_state replica=$replica_state commit_lsn=$commit_lsn"
        return 0
      fi
    fi
    sleep 1
  done
  emit invariant_failed "reconcile_timeout cycle=$cycle source=$expected primary=${primary_state:-unavailable} replica=${replica_state:-unavailable} commit_lsn=$commit_lsn"
  if [[ -n ${master:-} && -n ${replica:-} ]]; then
    capture_mismatch "$cycle" "$master" "$replica"
  fi
  metrics >"$BLOCK_DIR/metrics-failure.txt" 2>&1 || true
  return 1
}

mutate_source() {
  local cycle=$1 offset delete_start delete_end result
  offset=$(( ((cycle - 1) * BATCH_RECORDS) % WORKING_SET ))
  delete_start=$(( ((cycle * DELETE_RECORDS) % WORKING_SET) + 1 ))
  delete_end=$((delete_start + DELETE_RECORDS - 1))
  result=$(docker exec "$POSTGRES" psql -U ventstream -d soak -At -F '|' -v ON_ERROR_STOP=1 -c \
    "WITH upserted AS (
       INSERT INTO bench.orders(id, version, status, payload)
       SELECT ((g - 1) % $WORKING_SET) + 1,
              $cycle,
              CASE WHEN g % 3 = 0 THEN 'processing' ELSE 'pending' END,
              repeat('x', 256)
       FROM generate_series($((offset + 1)), $((offset + BATCH_RECORDS))) AS g
       ON CONFLICT (id) DO UPDATE SET
         version=EXCLUDED.version,
         status=EXCLUDED.status,
         payload=EXCLUDED.payload
       RETURNING 1
     ), deleted AS (
       DELETE FROM bench.orders
       WHERE $cycle % 5 = 0
         AND id BETWEEN $delete_start AND LEAST($delete_end, $WORKING_SET)
       RETURNING 1
     )
     SELECT (SELECT count(*) FROM upserted),
            (SELECT count(*) FROM deleted),
            pg_current_wal_lsn();")
  printf '%s\n' "$result"
}

rotate_credentials() {
  local next node sentinel replacement
  if [[ $CURRENT_PASSWORD == ventstream-soak-a ]]; then next=ventstream-soak-b; else next=ventstream-soak-a; fi
  emit injection_started "type=credential_rotation"
  for sentinel in "$SENTINEL_A" "$SENTINEL_B" "$SENTINEL_C"; do
    docker exec "$sentinel" redis-cli -p 26379 SENTINEL SET "$SERVICE" auth-pass "$next" >/dev/null
  done
  for node in "$REDIS_A" "$REDIS_B"; do
    docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" CONFIG SET masterauth "$next" >/dev/null
    docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" CONFIG SET requirepass "$next" >/dev/null
    docker exec "$node" redis-cli --no-auth-warning -a "$next" CONFIG REWRITE >/dev/null
  done
  replacement="$SECRETS_DIR/redis-password.next"
  printf '%s\n' "$next" >"$replacement"
  chmod 600 "$replacement"
  mv "$replacement" "$SECRETS_DIR/redis-password"
  CURRENT_PASSWORD=$next
  for node in "$REDIS_A" "$REDIS_B"; do
    docker exec "$node" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" CLIENT KILL TYPE normal SKIPME yes >/dev/null 2>&1 || true
  done
  wait_for replica-after-credential-rotation 60 replica_ready
  emit injection_finished "type=credential_rotation"
}

failover_with_backlog() {
  local cycle=$1 old_master new_master mutation commit_lsn
  old_master=$(master_container)
  emit injection_started "type=sentinel_failover old_master=$old_master"
  docker stop -t 0 "$old_master" >/dev/null
  mutation=$(mutate_source "$cycle")
  commit_lsn=${mutation##*|}
  wait_for sentinel-promotion 60 master_container
  new_master=$(master_container)
  if [[ $new_master == "$old_master" ]]; then
    emit invariant_failed "sentinel_did_not_promote_a_new_master old_master=$old_master"
    return 1
  fi
  docker start "$old_master" >/dev/null
  wait_for restarted-redis-node 60 redis_ping "$old_master"
  docker exec "$old_master" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" \
    REPLICAOF "$new_master" 6379 >/dev/null
  docker exec "$old_master" redis-cli --no-auth-warning -a "$CURRENT_PASSWORD" CONFIG REWRITE >/dev/null
  wait_for replica-after-failover 90 replica_ready
  emit injection_finished "type=sentinel_failover new_master=$new_master mutation=$mutation"
  printf '%s\n' "$commit_lsn"
}

run_cycle() {
  local cycle=$1 injection=none mutation commit_lsn check_at
  if (( INJECTION_EVERY > 0 && cycle % INJECTION_EVERY == 0 )); then
    case $(( (cycle / INJECTION_EVERY - 1) % 5 )) in
      0) injection=engine_restart ;;
      1) injection=source_outage ;;
      2) injection=redis_outage ;;
      3) injection=credential_rotation ;;
      4) injection=sentinel_failover ;;
    esac
  fi

  case "$injection" in
    engine_restart)
      emit injection_started "type=engine_restart"
      docker stop -t 5 "$ENGINE" >/dev/null
      mutation=$(mutate_source "$cycle")
      commit_lsn=${mutation##*|}
      assert_slot_behind "$commit_lsn"
      docker start "$ENGINE" >/dev/null
      wait_for engine-after-restart 120 ready
      emit injection_finished "type=engine_restart mutation=$mutation"
      ;;
    source_outage)
      mutation=$(mutate_source "$cycle")
      commit_lsn=${mutation##*|}
      emit injection_started "type=source_outage duration_s=$SOURCE_OUTAGE_SECS"
      docker pause "$POSTGRES" >/dev/null
      sleep "$SOURCE_OUTAGE_SECS"
      docker unpause "$POSTGRES" >/dev/null
      emit injection_finished "type=source_outage mutation=$mutation"
      ;;
    redis_outage)
      emit injection_started "type=redis_outage duration_s=$REDIS_OUTAGE_SECS"
      docker pause "$REDIS_A" "$REDIS_B" >/dev/null
      mutation=$(mutate_source "$cycle")
      commit_lsn=${mutation##*|}
      check_at=$((REDIS_OUTAGE_SECS - 1))
      sleep "$check_at"
      assert_slot_behind "$commit_lsn"
      if ready >/dev/null 2>&1; then
        emit invariant_failed "readiness_stayed_healthy_during_sustained_sink_failure"
        return 1
      fi
      emit readiness_degraded "type=redis_outage"
      sleep "$((REDIS_OUTAGE_SECS - check_at))"
      docker unpause "$REDIS_A" "$REDIS_B" >/dev/null
      emit injection_finished "type=redis_outage mutation=$mutation"
      ;;
    credential_rotation)
      mutation=$(mutate_source "$cycle")
      commit_lsn=${mutation##*|}
      rotate_credentials
      ;;
    sentinel_failover)
      commit_lsn=$(failover_with_backlog "$cycle" | tail -n1)
      ;;
    *)
      mutation=$(mutate_source "$cycle")
      commit_lsn=${mutation##*|}
      ;;
  esac
  reconcile "$cycle" "$commit_lsn"
}

for command in docker curl jq shasum; do
  command -v "$command" >/dev/null 2>&1 || { echo "$command is required" >&2; exit 2; }
done
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "soak image not found: $IMAGE" >&2
  exit 2
fi
if (( $(host_free_gib) < MIN_FREE_GIB )); then
  echo "less than ${MIN_FREE_GIB} GiB is free" >&2
  exit 2
fi
if (( REDIS_OUTAGE_SECS < 38 )); then
  echo 'Redis outage must be at least 38 seconds to cross the 30-second readiness grace period' >&2
  exit 2
fi

write_runtime_files
docker network create "$NETWORK" >/dev/null
docker volume create "$STATE_VOLUME" >/dev/null
docker volume create "$REDIS_A_VOLUME" >/dev/null
docker volume create "$REDIS_B_VOLUME" >/dev/null
start_postgres
start_redis_node "$REDIS_A" "$CONFIG_DIR/redis-a" "$REDIS_A_VOLUME"
start_redis_node "$REDIS_B" "$CONFIG_DIR/redis-b" "$REDIS_B_VOLUME"
wait_for initial-replication 60 replica_ready
start_sentinel "$SENTINEL_A"
start_sentinel "$SENTINEL_B"
start_sentinel "$SENTINEL_C"
wait_for sentinel-master-discovery 60 master_container

docker run --rm --network "$NETWORK" \
  -v "$CONFIG_DIR:/config:ro" -v "$SECRETS_DIR:/secrets:ro" \
  -e VS_ENGINE_CONFIG=/config/ventstream.yaml \
  -e VS_PG_HOST="$POSTGRES" -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream \
  -e VS_PG_DATABASE=soak -e VS_PG_PUBLICATION=ventstream_soak_pub -e VS_PG_SLOT=ventstream_soak_slot \
  -e VS_REDIS_SENTINEL_A="redis://$SENTINEL_A:26379" \
  -e VS_REDIS_SENTINEL_B="redis://$SENTINEL_B:26379" \
  -e VS_REDIS_SENTINEL_C="redis://$SENTINEL_C:26379" \
  -e VS_REDIS_SINK_WRITER_ID=redis-sink-soak-candidate \
  "$IMAGE" --validate-config >"$BLOCK_DIR/config-validation.log" 2>&1

start_engine
emit_top soak_started "run_id=$RUN_ID duration_s=$DURATION_SECS image=$IMAGE topology=postgres_to_redis_sentinel"
emit_top phase_started "round=1 phase=redis-sink block_end_epoch=$END_EPOCH"
emit block_started "working_set=$WORKING_SET batch_records=$BATCH_RECORDS injection_every=$INJECTION_EVERY"
resource_monitor &
MONITOR_PID=$!

cycle=0
while (( $(date +%s) < END_EPOCH )); do
  cycle=$((cycle + 1))
  run_cycle "$cycle"
  sleep "$CYCLE_SLEEP_SECS"
done

assert_dlq_empty
metrics >"$BLOCK_DIR/final-metrics.txt"
emit block_completed "cycles=$cycle"
emit_top phase_passed "round=1 phase=redis-sink cycles=$cycle"
emit_top soak_completed "phases=1 duration_s=$(( $(date +%s) - START_EPOCH ))"
COMPLETED=true
exit 0
