#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
BENCH_DIR="$ROOT/benchmarks/container-matrix"
RUN_ID=${VS_BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${VS_BENCH_RESULTS:-$ROOT/target/benchmarks/container-matrix/$RUN_ID}
IMAGE=${VS_BENCH_IMAGE:-ventstream-engine:bench}
NETWORK=vsbench
ENGINE=vsbench-engine
OS=vsbench-opensearch
ENGINE_CPUS=${VS_BENCH_ENGINE_CPUS:-2}
ENGINE_MEMORY=${VS_BENCH_ENGINE_MEMORY:-1g}
PAYLOAD_BYTES=${VS_BENCH_PAYLOAD_BYTES:-256}
TIMEOUT_SECS=${VS_BENCH_TIMEOUT_SECS:-600}
ALLOCATOR_CONF=${VS_BENCH_ALLOCATOR_CONF:-background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:1000}
MEMORY_CONTROLLER_ENABLED=${VS_BENCH_MEMORY_CONTROLLER_ENABLED:-true}

mkdir -p "$RESULTS"
CSV="$RESULTS/sources.csv"
printf '%s\n' 'source,profile,records,elapsed_s,throughput_eps,cpu_mean_pct,cpu_p95_pct,cpu_peak_pct,cgroup_peak_mib,rss_peak_mib,rss_hwm_mib,verified_docs,bus_capacity,batch_events,batch_bytes,flush_ms,parallel_bulks,source_chunk,source_concurrency' >"$CSV"

if [[ -n ${VS_BENCH_PROFILES:-} ]]; then
  read -r -a profiles <<<"$VS_BENCH_PROFILES"
else
  profiles=(balanced throughput maximum)
fi

profile_values() {
  case "$1" in
    balanced)   printf '%s\n' '8192 2000 4194304 50 8 128 8' ;;
    throughput) printf '%s\n' '32768 5000 16777216 10 16 512 16' ;;
    maximum)    printf '%s\n' '65536 10000 33554432 5 32 1024 32' ;;
    *) echo "unknown profile $1" >&2; return 2 ;;
  esac
}

remove_container() {
  docker rm -fv "$1" >/dev/null 2>&1 || true
}

remove_volume() {
  docker volume rm -f "$1" >/dev/null 2>&1 || true
}

cleanup_engine() {
  remove_container "$ENGINE"
  remove_volume vsbench-engine-state
}

cleanup_all() {
  cleanup_engine
  for name in vsbench-postgres vsbench-mysql vsbench-mongo vsbench-redpanda vsbench-neo4j; do
    remove_container "$name"
  done
  remove_container "$OS"
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  trap cleanup_all EXIT INT TERM
fi

wait_for() {
  local description=$1
  shift
  local deadline=$((SECONDS + TIMEOUT_SECS))
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for $description" >&2
      return 1
    fi
    sleep 1
  done
}

ensure_network() {
  docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK" >/dev/null
}

start_opensearch() {
  remove_container "$OS"
  docker run -d --name "$OS" --network "$NETWORK" \
    --cpus 2 --memory 2304m \
    -e discovery.type=single-node \
    -e bootstrap.memory_lock=false \
    -e OPENSEARCH_JAVA_OPTS='-Xms1024m -Xmx1024m' \
    -e DISABLE_SECURITY_PLUGIN=true \
    -e DISABLE_INSTALL_DEMO_CONFIG=true \
    opensearchproject/opensearch:2.17.1 >/dev/null
  wait_for OpenSearch docker exec "$OS" curl -fsS http://127.0.0.1:9200/_cluster/health
  docker exec "$OS" curl -fsS -XPUT http://127.0.0.1:9200/_cluster/settings \
    -H 'content-type: application/json' \
    -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false}}' >/dev/null
}

delete_index() {
  docker exec "$OS" curl -fsS -XDELETE "http://127.0.0.1:9200/$1" >/dev/null 2>&1 || true
}

create_index() {
  local index=$1
  docker exec "$OS" curl -fsS -XPUT "http://127.0.0.1:9200/$index" \
    -H 'content-type: application/json' \
    -d '{"settings":{"index":{"refresh_interval":"-1","number_of_replicas":0}}}' >/dev/null
}

engine_port() {
  docker port "$ENGINE" 4043/tcp | awk -F: 'NR==1 {print $NF}'
}

metric_value() {
  local port=$1 metric=$2 body
  body=$(curl -fsS "http://127.0.0.1:$port/metrics") || return 1
  awk -v metric="$metric" '$1==metric {v=$2} END {print v+0}' <<<"$body"
}

wait_engine_metrics() {
  local port
  port=$(engine_port)
  wait_for 'engine metrics endpoint' curl -fsS "http://127.0.0.1:$port/metrics"
  sleep 2
  printf '%s\n' "$port"
}

start_engine() {
  local source=$1 profile=$2
  shift 2
  read -r bus batch batch_bytes flush parallel source_chunk source_concurrency <<<"$(profile_values "$profile")"
  cleanup_engine
  docker volume create vsbench-engine-state >/dev/null
  docker run -d --name "$ENGINE" --network "$NETWORK" \
    --cpus "$ENGINE_CPUS" --memory "$ENGINE_MEMORY" \
    -p 127.0.0.1::4043 \
    -v vsbench-engine-state:/var/lib/ventstream \
    -v "$BENCH_DIR:/specs:ro" \
    -e VS_ROLES=cdc \
    -e "VS_CDC_SOURCE=$source" \
    -e VS_OS_ENDPOINT=http://vsbench-opensearch:9200 \
    -e VS_INDEX_TEMPLATE='${header:ventstream.target.index}' \
    -e "VS_BUS_CAPACITY=$bus" \
    -e "VS_DISPATCH_MAX_EVENTS=$batch" \
    -e "VS_DISPATCH_MAX_BATCH_BYTES=$batch_bytes" \
    -e "VS_DISPATCH_FLUSH_MS=$flush" \
    -e "VS_DISPATCH_PARALLEL_BULKS=$parallel" \
    -e VS_HEALTH_LISTEN=0.0.0.0:4043 \
    -e VS_DLQ_PATH=/var/lib/ventstream/dlq.jsonl \
    -e "_RJEM_MALLOC_CONF=$ALLOCATOR_CONF" \
    -e "VS_MEMORY_CONTROLLER_ENABLED=$MEMORY_CONTROLLER_ENABLED" \
    -e RUST_LOG=warn \
    "$@" "$IMAGE" >/dev/null
}

wait_delivered() {
  local port=$1 baseline=$2 expected=$3
  local deadline=$((SECONDS + TIMEOUT_SECS)) current
  while :; do
    if [[ $(docker inspect -f '{{.State.Running}}' "$ENGINE" 2>/dev/null || echo false) != true ]]; then
      echo "engine exited before delivery completed" >&2
      docker logs --tail 100 "$ENGINE" >&2 || true
      return 1
    fi
    current=$(metric_value "$port" vs_events_delivered_total || echo 0)
    if (( current - baseline >= expected )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "delivery timeout: baseline=$baseline current=$current expected=$expected" >&2
      docker logs --tail 100 "$ENGINE" >&2 || true
      return 1
    fi
    sleep 1
  done
}

verified_count() {
  local index=$1
  docker exec "$OS" curl -fsS -XPOST "http://127.0.0.1:9200/$index/_refresh" >/dev/null
  docker exec "$OS" curl -fsS "http://127.0.0.1:9200/$index/_count" | jq -r '.count'
}

run_measurement() {
  local source=$1 profile=$2 records=$3 index=$4 load_function=$5 port=$6
  local result_dir="$RESULTS/$source-$profile"
  mkdir -p "$result_dir"
  local baseline start_ns end_ns elapsed throughput docs samples monitor_pid
  baseline=$(metric_value "$port" vs_events_delivered_total)
  "$BENCH_DIR/sample-container.sh" "$ENGINE" "$result_dir" &
  monitor_pid=$!
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  "$load_function" "$records"
  wait_delivered "$port" "$baseline" "$records"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  kill "$monitor_pid" >/dev/null 2>&1 || true
  wait "$monitor_pid" >/dev/null 2>&1 || true
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  throughput=$(awk -v n="$records" -v s="$elapsed" 'BEGIN {printf "%.2f", n/s}')
  docs=$(verified_count "$index")
  if [[ "$docs" != "$records" ]]; then
    echo "$source/$profile correctness failure: expected $records docs, got $docs" >&2
    return 1
  fi
  samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/docker-stats.tsv" "$result_dir/process-memory.tsv")
  read -r bus batch batch_bytes flush parallel source_chunk source_concurrency <<<"$(profile_values "$profile")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$source" "$profile" "$records" "$elapsed" "$throughput" "$samples" "$docs" \
    "$bus" "$batch" "$batch_bytes" "$flush" "$parallel" "$source_chunk" "$source_concurrency" \
    | tee -a "$CSV"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

start_postgres() {
  remove_container vsbench-postgres
  docker run -d --name vsbench-postgres --network "$NETWORK" \
    --cpus 1.5 --memory 1g \
    -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=ventstream -e POSTGRES_DB=bench \
    postgres:16-alpine postgres -c wal_level=logical -c max_wal_senders=4 -c max_replication_slots=8 \
    -c shared_buffers=256MB -c synchronous_commit=off >/dev/null
  wait_for PostgreSQL docker exec vsbench-postgres psql -U ventstream -d bench -Atc 'SELECT 1'
  docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
    "CREATE SCHEMA bench; CREATE TABLE bench.events(id bigint PRIMARY KEY, value bigint NOT NULL, payload text NOT NULL); CREATE PUBLICATION vsbench_pub FOR TABLE bench.events;" >/dev/null
}

load_postgres() {
  local records=$1
  docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
    "INSERT INTO bench.events SELECT g, g % 1000, repeat('x',$PAYLOAD_BYTES) FROM generate_series(1,$records) g;" >/dev/null
}

bench_postgres() {
  local records=${VS_BENCH_POSTGRES_RECORDS:-100000}
  start_postgres
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-postgres psql -U ventstream -d bench -c 'TRUNCATE bench.events' >/dev/null
    docker exec vsbench-postgres psql -U ventstream -d bench -Atc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='vsbench_slot'" >/dev/null
    delete_index vsbench-postgres
    read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$profile")"
    start_engine postgres "$profile" \
      -e VS_PG_HOST=vsbench-postgres -e VS_PG_PORT=5432 \
      -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream -e VS_PG_DATABASE=bench \
      -e VS_PG_PUBLICATION=vsbench_pub -e VS_PG_SLOT=vsbench_slot \
      -e VS_PG_BOOTSTRAP_MODE=none -e "VS_PG_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_PG_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/postgres.yaml \
      -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state
    port=$(wait_engine_metrics)
    load_postgres 1
    wait_delivered "$port" 0 1
    docker exec vsbench-postgres psql -U ventstream -d bench -c 'TRUNCATE bench.events' >/dev/null
    delete_index vsbench-postgres
    create_index vsbench-postgres
    run_measurement postgres "$profile" "$records" vsbench-postgres load_postgres "$port"
  done
  cleanup_engine
  remove_container vsbench-postgres
}

start_mysql() {
  remove_container vsbench-mysql
  docker run -d --name vsbench-mysql --network "$NETWORK" \
    --cpus 1.5 --memory 1g \
    -e MYSQL_ROOT_PASSWORD=ventstream -e MYSQL_ROOT_HOST=% \
    mysql:8.4 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --sync-binlog=0 --innodb-flush-log-at-trx-commit=2 --innodb-buffer-pool-size=256M >/dev/null
  wait_for MySQL docker exec vsbench-mysql mysqladmin ping -h127.0.0.1 -uroot -pventstream --silent
  docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream -e \
    "CREATE DATABASE bench; CREATE USER 'ventstream'@'%' IDENTIFIED BY 'ventstream'; GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ventstream'@'%'; CREATE TABLE bench.events(id BIGINT PRIMARY KEY, value BIGINT NOT NULL, payload TEXT NOT NULL);" >/dev/null
}

load_mysql() {
  local records=$1
  docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e \
    "SET SESSION cte_max_recursion_depth=$((records + 1)); INSERT INTO events WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n < $records) SELECT n, MOD(n,1000), REPEAT('x',$PAYLOAD_BYTES) FROM seq;" >/dev/null
}

bench_mysql() {
  local records=${VS_BENCH_MYSQL_RECORDS:-50000}
  start_mysql
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream -e 'TRUNCATE bench.events' >/dev/null
    delete_index vsbench-mysql
    create_index vsbench-mysql
    read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$profile")"
    start_engine mysql "$profile" \
      -e VS_MYSQL_HOST=vsbench-mysql -e VS_MYSQL_PORT=3306 \
      -e VS_MYSQL_USER=ventstream -e VS_MYSQL_PASSWORD=ventstream -e VS_MYSQL_DATABASE=bench \
      -e VS_MYSQL_TABLES=events -e VS_MYSQL_SERVER_ID=4000000001 \
      -e VS_MYSQL_BOOTSTRAP_MODE=none -e VS_MYSQL_POS_FLUSH_MS=1000 \
      -e VS_MYSQL_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/mysql.yaml \
      -e "VS_MYSQL_RECOMPOSE_CHUNK=$chunk" -e "VS_MYSQL_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_MYSQL_STATE_DIR=/var/lib/ventstream/state -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state
    port=$(wait_engine_metrics)
    run_measurement mysql "$profile" "$records" vsbench-mysql load_mysql "$port"
  done
  cleanup_engine
  remove_container vsbench-mysql
}

start_mongo() {
  remove_container vsbench-mongo
  docker run -d --name vsbench-mongo --network "$NETWORK" --hostname vsbench-mongo \
    --cpus 1.5 --memory 1g mongo:7.0 mongod --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB 0.25 >/dev/null
  wait_for MongoDB docker exec vsbench-mongo mongosh --quiet --eval 'db.adminCommand({ping:1}).ok'
  docker exec vsbench-mongo mongosh --quiet --eval \
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"vsbench-mongo:27017"}]})' >/dev/null
  wait_for 'MongoDB primary' docker exec vsbench-mongo mongosh --quiet --eval \
    'if (!db.hello().isWritablePrimary) quit(1)'
}

load_mongodb() {
  local records=$1
  docker exec vsbench-mongo mongosh --quiet bench --eval \
    "const n=$records,p='x'.repeat($PAYLOAD_BYTES); for(let s=1;s<=n;s+=1000){const a=[];for(let i=s;i<=Math.min(n,s+999);i++)a.push({_id:i,value:i%1000,payload:p});db.events.insertMany(a,{ordered:false});}" >/dev/null
}

bench_mongodb() {
  local records=${VS_BENCH_MONGODB_RECORDS:-100000}
  start_mongo
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-mongo mongosh --quiet bench --eval 'db.events.drop()' >/dev/null
    delete_index vsbench-mongodb
    create_index vsbench-mongodb
    read -r _ _ _ _ _ chunk _ <<<"$(profile_values "$profile")"
    start_engine mongodb "$profile" \
      -e 'VS_MONGO_URI=mongodb://vsbench-mongo:27017/?replicaSet=rs0' \
      -e VS_MONGO_DATABASE=bench -e VS_MONGO_COLLECTIONS=events \
      -e VS_MONGO_BOOTSTRAP_MODE=none -e "VS_MONGO_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_MONGO_FULL_DOCUMENT=update_lookup -e VS_MONGO_TOKEN_FLUSH_MS=1000 \
      -e VS_MONGO_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-mongodb
    port=$(wait_engine_metrics)
    run_measurement mongodb "$profile" "$records" vsbench-mongodb load_mongodb "$port"
  done
  cleanup_engine
  remove_container vsbench-mongo
}

start_redpanda() {
  remove_container vsbench-redpanda
  docker run -d --name vsbench-redpanda --network "$NETWORK" --hostname vsbench-redpanda \
    --cpus 1.5 --memory 1g redpandadata/redpanda:v24.3.10 \
    redpanda start --overprovisioned --smp=1 --memory=768M --reserve-memory=0M --node-id=0 --check=false \
    --kafka-addr=PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr=PLAINTEXT://vsbench-redpanda:9092 >/dev/null
  wait_for Redpanda docker exec vsbench-redpanda rpk cluster health --exit-when-healthy
}

load_kafka() {
  local records=$1
  awk -v n="$records" -v bytes="$PAYLOAD_BYTES" 'BEGIN { p=""; for(i=0;i<bytes;i++)p=p"x"; for(i=1;i<=n;i++) printf "{\"id\":%d,\"value\":%d,\"payload\":\"%s\"}\n",i,i%1000,p }' \
    | docker exec -i vsbench-redpanda rpk topic produce events >/dev/null
}

bench_kafka() {
  local records=${VS_BENCH_KAFKA_RECORDS:-200000}
  start_redpanda
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-redpanda rpk topic delete events >/dev/null 2>&1 || true
    docker exec vsbench-redpanda rpk topic create events -p 1 >/dev/null
    delete_index vsbench-kafka
    create_index vsbench-kafka
    start_engine kafka "$profile" \
      -e VS_KAFKA_BROKERS=vsbench-redpanda:9092 -e VS_KAFKA_TOPICS=events \
      -e "VS_KAFKA_GROUP_ID=vsbench-$profile" -e VS_KAFKA_NAMESPACE=bench \
      -e VS_KAFKA_UNWRAP=raw -e VS_KAFKA_RAW_KEY_FIELD=id \
      -e VS_KAFKA_AUTO_OFFSET_RESET=earliest -e VS_KAFKA_COMMIT_MS=1000 \
      -e VS_INDEX_TEMPLATE=vsbench-kafka
    port=$(wait_engine_metrics)
    run_measurement kafka "$profile" "$records" vsbench-kafka load_kafka "$port"
  done
  cleanup_engine
  remove_container vsbench-redpanda
}

start_neo4j() {
  remove_container vsbench-neo4j
  docker run -d --name vsbench-neo4j --network "$NETWORK" --hostname vsbench-neo4j \
    --cpus 2 --memory 2g \
    -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes -e NEO4J_AUTH=neo4j/ventstream \
    -e NEO4J_server_memory_heap_initial__size=512m -e NEO4J_server_memory_heap_max__size=512m \
    -e NEO4J_server_memory_pagecache_size=512m \
    neo4j:5.26-enterprise >/dev/null
  wait_for Neo4j docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream 'RETURN 1'
  docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream -d system \
    "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL'" >/dev/null
}

load_neo4j() {
  local records=$1 start end batch=5000
  for ((start=1; start<=records; start+=batch)); do
    end=$((start + batch - 1)); (( end > records )) && end=$records
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
      "UNWIND range($start,$end) AS id CREATE (:BenchmarkEvent {id:id,value:id%1000,payload:substring('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',0,$PAYLOAD_BYTES)})" >/dev/null
  done
}

bench_neo4j() {
  local records=${VS_BENCH_NEO4J_RECORDS:-50000}
  start_neo4j
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream 'MATCH (n) DETACH DELETE n' >/dev/null
    delete_index vsbench-neo4j
    create_index vsbench-neo4j
    read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$profile")"
    start_engine neo4j "$profile" \
      -e VS_NEO4J_URI=bolt://vsbench-neo4j:7687 -e VS_NEO4J_USER=neo4j \
      -e VS_NEO4J_PASSWORD=ventstream -e VS_NEO4J_DATABASE=neo4j \
      -e VS_NEO4J_BOOTSTRAP_MODE=none -e VS_NEO4J_POLL_INTERVAL_MS=10 \
      -e VS_NEO4J_DENORMALIZE_YAML=/specs/neo4j.yaml \
      -e "VS_NEO4J_RECOMPOSE_CHUNK=$chunk" -e "VS_NEO4J_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_NEO4J_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-neo4j
    port=$(wait_engine_metrics)
    run_measurement neo4j "$profile" "$records" vsbench-neo4j load_neo4j "$port"
  done
  cleanup_engine
  remove_container vsbench-neo4j
}

main() {
  ensure_network
  start_opensearch
  local requested=${1:-all}
  case "$requested" in
    postgres) bench_postgres ;;
    mysql) bench_mysql ;;
    mongodb) bench_mongodb ;;
    kafka) bench_kafka ;;
    neo4j) bench_neo4j ;;
    all)
      bench_postgres
      bench_mysql
      bench_mongodb
      bench_kafka
      bench_neo4j
      ;;
    *) echo "usage: $0 [postgres|mysql|mongodb|kafka|neo4j|all]" >&2; return 2 ;;
  esac
  echo "source benchmark results: $CSV"
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then
  main "$@"
fi
