#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SOURCE=${1:?usage: run-cdc-block.sh postgres|mysql|mongodb|kafka|neo4j}
BLOCK_END_EPOCH=${VS_SOAK_BLOCK_END_EPOCH:-$(( $(date +%s) + 3600 ))}
OUT=${VS_SOAK_BLOCK_DIR:-$ROOT/target/soak/cdc-$SOURCE-$(date -u +%Y%m%dT%H%M%SZ)}
OUTAGE_SECS=${VS_SOAK_OUTAGE_SECS:-8}
OUTAGE_EVERY=${VS_SOAK_OUTAGE_EVERY:-10}
RESTART_EVERY=${VS_SOAK_RESTART_EVERY:-20}
BATCH_SLEEP_SECS=${VS_SOAK_BATCH_SLEEP_SECS:-15}
VS_BENCH_RESULTS=$OUT/benchmark
export VS_BENCH_RESULTS

# Reuse the benchmark's production-like containers and engine configuration,
# but retain them for this entire block instead of recreating them per batch.
source "$ROOT/benchmarks/container-matrix/run-sources.sh"

mkdir -p "$OUT/batches"
EVENTS="$OUT/events.jsonl"

finish() {
  local rc=$?
  trap - EXIT INT TERM
  if (( rc != 0 )); then
    docker logs "$ENGINE" >"$OUT/engine-failure.log" 2>&1 || true
    docker inspect "$ENGINE" "$SOURCE_CONTAINER" "$OS" >"$OUT/docker-inspect-failure.json" 2>&1 || true
    emit_block block_failed "source=$SOURCE exit_code=$rc" || true
  fi
  cleanup_all
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

load_postgres_window() {
  local records=$1
  docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
    "INSERT INTO bench.events SELECT g, g % 1000, repeat('x',$PAYLOAD_BYTES) FROM generate_series(1,$records) g ON CONFLICT (id) DO UPDATE SET value=bench.events.value+1, payload=EXCLUDED.payload;" >/dev/null
}

load_mysql_window() {
  local records=$1
  docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e \
    "SET SESSION cte_max_recursion_depth=$((records + 1)); INSERT INTO events WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n < $records) SELECT n, MOD(n,1000), REPEAT('x',$PAYLOAD_BYTES) FROM seq ON DUPLICATE KEY UPDATE value=value+1,payload=VALUES(payload);" >/dev/null
}

load_mongodb_window() {
  local records=$1
  docker exec vsbench-mongo mongosh --quiet bench --eval \
    "const n=$records,p='x'.repeat($PAYLOAD_BYTES); for(let s=1;s<=n;s+=1000){const a=[];for(let i=s;i<=Math.min(n,s+999);i++)a.push({updateOne:{filter:{_id:i},update:{\$inc:{value:1},\$set:{payload:p}},upsert:true}});db.events.bulkWrite(a,{ordered:false});}" >/dev/null
}

load_kafka_window() {
  local records=$1
  awk -v n="$records" -v bytes="$PAYLOAD_BYTES" 'BEGIN { p=""; for(i=0;i<bytes;i++)p=p"x"; for(i=1;i<=n;i++) printf "{\"id\":%d,\"value\":%d,\"payload\":\"%s\"}\n",i,i%1000,p }' \
    | docker exec -i vsbench-redpanda rpk topic produce events >/dev/null
}

load_neo4j_window() {
  local records=$1 start end batch=2500
  for ((start=1; start<=records; start+=batch)); do
    end=$((start + batch - 1)); (( end > records )) && end=$records
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
      "UNWIND range($start,$end) AS id MERGE (n:BenchmarkEvent {id:id}) SET n.value=coalesce(n.value,0)+1,n.payload=substring('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',0,$PAYLOAD_BYTES)" >/dev/null
  done
}

configure_source() {
  local chunk concurrency
  read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values throughput)"
  case "$SOURCE" in
    postgres)
      BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-10000}; SOURCE_CONTAINER=vsbench-postgres; INDEX=vsbench-postgres; LOADER=load_postgres_window
      start_postgres
      start_engine postgres throughput \
        -e VS_PG_HOST=vsbench-postgres -e VS_PG_PORT=5432 -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream -e VS_PG_DATABASE=bench \
        -e VS_PG_PUBLICATION=vsbench_pub -e VS_PG_SLOT=vsbench_slot -e VS_PG_BOOTSTRAP_MODE=none -e "VS_PG_BOOTSTRAP_CHUNK_SIZE=$chunk" \
        -e VS_PG_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/postgres.yaml -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state
      ;;
    mysql)
      BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-10000}; SOURCE_CONTAINER=vsbench-mysql; INDEX=vsbench-mysql; LOADER=load_mysql_window
      start_mysql
      start_engine mysql throughput \
        -e VS_MYSQL_HOST=vsbench-mysql -e VS_MYSQL_PORT=3306 -e VS_MYSQL_USER=ventstream -e VS_MYSQL_PASSWORD=ventstream -e VS_MYSQL_DATABASE=bench \
        -e VS_MYSQL_TABLES=events -e VS_MYSQL_SERVER_ID=4000000001 -e VS_MYSQL_BOOTSTRAP_MODE=none -e VS_MYSQL_POS_FLUSH_MS=1000 \
        -e VS_MYSQL_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/mysql.yaml -e "VS_MYSQL_RECOMPOSE_CHUNK=$chunk" \
        -e "VS_MYSQL_RECOMPOSE_CONCURRENCY=$concurrency" -e VS_MYSQL_STATE_DIR=/var/lib/ventstream/state -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state
      ;;
    mongodb)
      BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-10000}; SOURCE_CONTAINER=vsbench-mongo; INDEX=vsbench-mongodb; LOADER=load_mongodb_window
      start_mongo
      start_engine mongodb throughput \
        -e 'VS_MONGO_URI=mongodb://vsbench-mongo:27017/?replicaSet=rs0' -e VS_MONGO_DATABASE=bench -e VS_MONGO_COLLECTIONS=events \
        -e VS_MONGO_BOOTSTRAP_MODE=none -e "VS_MONGO_BOOTSTRAP_CHUNK_SIZE=$chunk" -e VS_MONGO_FULL_DOCUMENT=update_lookup \
        -e VS_MONGO_TOKEN_FLUSH_MS=1000 -e VS_MONGO_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-mongodb
      ;;
    kafka)
      BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-20000}; SOURCE_CONTAINER=vsbench-redpanda; INDEX=vsbench-kafka; LOADER=load_kafka_window
      start_redpanda
      docker exec vsbench-redpanda rpk topic create events -p 1 >/dev/null
      start_engine kafka throughput \
        -e VS_KAFKA_BROKERS=vsbench-redpanda:9092 -e VS_KAFKA_TOPICS=events -e VS_KAFKA_GROUP_ID=vsbench-soak \
        -e VS_KAFKA_NAMESPACE=bench -e VS_KAFKA_UNWRAP=raw -e VS_KAFKA_RAW_KEY_FIELD=id \
        -e VS_KAFKA_AUTO_OFFSET_RESET=earliest -e VS_KAFKA_COMMIT_MS=1000 -e VS_INDEX_TEMPLATE=vsbench-kafka
      ;;
    neo4j)
      BATCH_RECORDS=${VS_SOAK_BATCH_RECORDS:-2500}; SOURCE_CONTAINER=vsbench-neo4j; INDEX=vsbench-neo4j; LOADER=load_neo4j_window
      start_neo4j
      start_engine neo4j throughput \
        -e VS_NEO4J_URI=bolt://vsbench-neo4j:7687 -e VS_NEO4J_USER=neo4j -e VS_NEO4J_PASSWORD=ventstream -e VS_NEO4J_DATABASE=neo4j \
        -e VS_NEO4J_BOOTSTRAP_MODE=none -e VS_NEO4J_POLL_INTERVAL_MS=10 -e VS_NEO4J_DENORMALIZE_YAML=/specs/neo4j.yaml \
        -e "VS_NEO4J_RECOMPOSE_CHUNK=$chunk" -e "VS_NEO4J_RECOMPOSE_CONCURRENCY=$concurrency" \
        -e VS_NEO4J_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-neo4j
      ;;
    *) echo "unsupported source: $SOURCE" >&2; exit 2 ;;
  esac
}

ensure_network
start_opensearch
configure_source
delete_index "$INDEX"
create_index "$INDEX"
PORT=$(wait_engine_metrics)
emit_block block_started "source=$SOURCE batch_records=$BATCH_RECORDS end_epoch=$BLOCK_END_EPOCH"

cycle=0
while (( $(date +%s) < BLOCK_END_EPOCH )); do
  cycle=$((cycle + 1))
  if (( RESTART_EVERY > 0 && cycle > 1 && cycle % RESTART_EVERY == 0 )); then
    emit_block engine_restart_started "cycle=$cycle"
    docker restart "$ENGINE" >/dev/null
    PORT=$(wait_engine_metrics)
    emit_block engine_restart_finished "cycle=$cycle"
  fi

  injection=none
  if (( OUTAGE_EVERY > 0 && cycle % OUTAGE_EVERY == 0 )); then
    if (( (cycle / OUTAGE_EVERY) % 2 == 0 )); then
      injection=source
      docker pause "$SOURCE_CONTAINER" >/dev/null
      emit_block outage_started "cycle=$cycle target=$SOURCE_CONTAINER"
      sleep "$OUTAGE_SECS"
      docker unpause "$SOURCE_CONTAINER" >/dev/null
      emit_block outage_finished "cycle=$cycle target=$SOURCE_CONTAINER"
    else
      injection=sink
      docker pause "$OS" >/dev/null
      emit_block outage_started "cycle=$cycle target=$OS"
    fi
  fi

  baseline=$(metric_value "$PORT" vs_events_delivered_total)
  started=$(date +%s)
  "$LOADER" "$BATCH_RECORDS"
  if [[ $injection == sink ]]; then
    sleep "$OUTAGE_SECS"
    docker unpause "$OS" >/dev/null
    emit_block outage_finished "cycle=$cycle target=$OS"
  fi
  wait_delivered "$PORT" "$baseline" "$BATCH_RECORDS"
  docs=$(verified_count "$INDEX")
  if [[ $docs != "$BATCH_RECORDS" ]]; then
    emit_block batch_failed "cycle=$cycle expected_docs=$BATCH_RECORDS actual_docs=$docs"
    exit 1
  fi
  elapsed=$(( $(date +%s) - started ))
  emit_block batch_passed "cycle=$cycle events=$BATCH_RECORDS docs=$docs duration_s=$elapsed injection=$injection"
  sleep "$BATCH_SLEEP_SECS"
done

docker logs "$ENGINE" >"$OUT/engine.log" 2>&1 || true
emit_block block_completed "source=$SOURCE cycles=$cycle"
