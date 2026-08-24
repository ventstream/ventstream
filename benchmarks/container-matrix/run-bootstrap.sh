#!/usr/bin/env bash
# 50M-row snapshot-bootstrap benchmark: seed the source completely, then
# measure the engine bootstrapping the full dataset into OpenSearch.
# Runs one source at a time (shared laptop resources) and tears each
# source down before the next to stay inside local disk.
set -euo pipefail

BOOT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
# shellcheck source=run-sources.sh
source "$BOOT_ROOT/benchmarks/container-matrix/run-sources.sh"

RECORDS=${VS_BENCH_RECORDS:-50000000}
PROFILE=${VS_BENCH_PROFILE:-throughput}
SINK=${VS_BENCH_SINK:-opensearch}
MIN_FREE_GB=${VS_BENCH_MIN_FREE_GB:-12}
BOOT_CSV="$RESULTS/bootstrap-$SINK.csv"
printf '%s\n' 'source,records,payload_bytes,seed_s,bootstrap_s,throughput_eps,cpu_mean_pct,cpu_p95_pct,cpu_peak_pct,cgroup_peak_mib,rss_peak_mib,rss_hwm_mib,verified_docs' >"$BOOT_CSV"

now_ns() { perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000'; }

elapsed_s() { awk -v s="$1" -v e="$2" 'BEGIN {printf "%.3f", (e-s)/1000000000}'; }

check_disk() {
  local free_gb
  free_gb=$(df -g / | awk 'NR==2 {print $4}')
  if (( free_gb < MIN_FREE_GB )); then
    echo "aborting $1: only ${free_gb}GB free (< ${MIN_FREE_GB}GB)" >&2
    return 1
  fi
}

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }

SURREAL=vsbench-surreal

surreal_rpc() {
  local port=$1 sql=$2
  curl -fsS -X POST "http://127.0.0.1:$port/rpc" -u root:root \
    -H 'Accept: application/json' -H 'Content-Type: application/json' \
    -H 'surreal-ns: bench' -H 'surreal-db: bench' \
    --data-binary @<(python3 -c 'import json,sys; print(json.dumps({"method":"query","params":[sys.argv[1]]}))' "$sql")
}

start_surrealdb() {
  remove_container "$SURREAL"
  docker run -d --name "$SURREAL" --network "$NETWORK" \
    --cpus "${VS_BENCH_SURREAL_CPUS:-2}" --memory 2304m \
    -p 127.0.0.1::8000 \
    surrealdb/surrealdb:v3.2.4 start --user root --pass root rocksdb:/tmp/bench.db >/dev/null
  local port
  port=$(docker port "$SURREAL" 8000/tcp | awk -F: 'NR==1 {print $NF}')
  wait_for SurrealDB curl -fsS "http://127.0.0.1:$port/health"
  curl -fsS -X POST "http://127.0.0.1:$port/rpc" -u root:root \
    -H 'Accept: application/json' -H 'Content-Type: application/json' \
    -d "{\"method\":\"query\",\"params\":[\"DEFINE NAMESPACE bench; USE NS bench; DEFINE DATABASE bench; USE DB bench; DEFINE USER vs ON DATABASE PASSWORD 'vs' ROLES OWNER;\"]}" >/dev/null
  printf '%s\n' "$port"
}

# Elasticsearch runs under the same container name as OpenSearch: the
# bulk API, index helpers, and the engine's VS_OS_ENDPOINT are identical.
start_elasticsearch() {
  remove_container "$OS"
  docker run -d --name "$OS" --network "$NETWORK" \
    --cpus 2 --memory 2304m \
    -e discovery.type=single-node \
    -e xpack.security.enabled=false \
    -e bootstrap.memory_lock=false \
    -e ES_JAVA_OPTS='-Xms1024m -Xmx1024m' \
    docker.elastic.co/elasticsearch/elasticsearch:8.15.2 >/dev/null
  wait_for Elasticsearch docker exec "$OS" curl -fsS http://127.0.0.1:9200/_cluster/health
  docker exec "$OS" curl -fsS -XPUT http://127.0.0.1:9200/_cluster/settings \
    -H 'content-type: application/json' \
    -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false}}' >/dev/null
}

REDIS=vsbench-redis

# Redis is RAM-resident: size runs with VS_BENCH_RECORDS to fit the local
# VM (the sink stores one doc key + one version key per document).
start_redis() {
  remove_container "$REDIS"
  docker run -d --name "$REDIS" --network "$NETWORK" \
    --cpus 2 --memory "${VS_BENCH_REDIS_MEMORY:-4608m}" \
    -p 127.0.0.1::6379 \
    redis:7.4-alpine redis-server --save '' --appendonly no >/dev/null
  local port
  port=$(docker port "$REDIS" 6379/tcp | awk -F: 'NR==1 {print $NF}')
  wait_for Redis docker exec "$REDIS" redis-cli ping
  printf '%s\n' "$port"
}

MEILI=vsbench-meili

start_meilisearch() {
  remove_container "$MEILI"
  docker run -d --name "$MEILI" --network "$NETWORK" \
    --cpus 2 --memory 2304m \
    -p 127.0.0.1::7700 \
    getmeili/meilisearch:v1.12 meilisearch --master-key vsbenchmaster --no-analytics >/dev/null
  local port
  port=$(docker port "$MEILI" 7700/tcp | awk -F: 'NR==1 {print $NF}')
  wait_for Meilisearch curl -fsS "http://127.0.0.1:$port/health"
  printf '%s\n' "$port"
}

boot_prepare() {
  if [[ $SINK == surrealdb ]]; then
    SURREAL_PORT=$(start_surrealdb)
  elif [[ $SINK == meilisearch ]]; then
    MEILI_PORT=$(start_meilisearch)
  elif [[ $SINK == redis ]]; then
    REDIS_PORT=$(start_redis)
  fi
}

reset_target() {
  local target=$1
  if [[ $SINK == redis ]]; then
    docker exec "$REDIS" redis-cli flushall >/dev/null 2>&1 || true
  elif [[ $SINK == surrealdb ]]; then
    surreal_rpc "$SURREAL_PORT" "REMOVE TABLE IF EXISTS ${target//-/_};" >/dev/null 2>&1 || true
  elif [[ $SINK == meilisearch ]]; then
    curl -fsS -X DELETE "http://127.0.0.1:$MEILI_PORT/indexes/vs_$target" \
      -H "Authorization: Bearer vsbenchmaster" >/dev/null 2>&1 || true
    sleep 2
    # Pre-create with lean searchable attributes: default ["*"] tokenizes
    # the unique _vs_pk/_vs_id of every doc — a 50M-entry word dictionary
    # that pins one core. Realistic deployments restrict searchables.
    curl -fsS -X POST "http://127.0.0.1:$MEILI_PORT/indexes" \
      -H "Authorization: Bearer vsbenchmaster" -H 'Content-Type: application/json' \
      -d "{\"uid\":\"vs_$target\",\"primaryKey\":\"_vs_pk\"}" >/dev/null 2>&1 || true
    sleep 1
    curl -fsS -X PUT "http://127.0.0.1:$MEILI_PORT/indexes/vs_$target/settings/searchable-attributes" \
      -H "Authorization: Bearer vsbenchmaster" -H 'Content-Type: application/json' \
      -d '["value"]' >/dev/null 2>&1 || true
    sleep 2
  else
    delete_index "$target"
    create_index "$target"
  fi
}

verified_target() {
  local target=$1
  if [[ $SINK == surrealdb ]]; then
    surreal_rpc "$SURREAL_PORT" "SELECT count() FROM ${target//-/_} GROUP ALL;" \
      | python3 -c "
import json, sys
d = json.load(sys.stdin)
r = d.get('result', [{}])[0].get('result')
if isinstance(r, list) and r and isinstance(r[0], dict):
    print(r[0].get('count', 0))
else:
    print(0)
    print(f'surreal count query returned: {r!r}', file=sys.stderr)"
  elif [[ $SINK == redis ]]; then
    docker exec "$REDIS" sh -c "redis-cli --scan --pattern 'vs:*' | grep -cv ':__ventstream:'"
  elif [[ $SINK == meilisearch ]]; then
    local stats
    stats=$(curl -fsS "http://127.0.0.1:$MEILI_PORT/indexes/vs_$target/stats" \
      -H "Authorization: Bearer vsbenchmaster" 2>/dev/null) || {
      echo "meili stats failed for '$target'; existing indexes:" >&2
      curl -fsS "http://127.0.0.1:$MEILI_PORT/indexes" \
        -H "Authorization: Bearer vsbenchmaster" >&2 || true
      echo 0
      return
    }
    printf '%s' "$stats" | python3 -c "import json,sys;print(json.load(sys.stdin).get('numberOfDocuments',0))"
  else
    verified_count "$target"
  fi
}

sink_env_args() {
  local target=$1
  if [[ $SINK == redis ]]; then
    printf '%s\n' \
      "-e" "VS_SINK=redis" \
      "-e" "VS_REDIS_SINK_URL=redis://$REDIS:6379" \
      "-e" "VS_REDIS_SINK_KEY_PREFIX=vs"
    return
  fi
  if [[ $SINK == elasticsearch ]]; then
    printf '%s\n' "-e" "VS_SINK=elasticsearch"
    return
  fi
  if [[ $SINK == meilisearch ]]; then
    printf '%s\n' \
      "-e" "VS_SINK=meilisearch" \
      "-e" "VS_MEILI_ENDPOINT=http://$MEILI:7700" \
      "-e" "VS_MEILI_API_KEY=vsbenchmaster" \
      "-e" "VS_MEILI_INDEX=$target" \
      "-e" "VS_MEILI_MAX_BATCH_DOCS=250000" \
      "-e" "VS_MEILI_MAX_BATCH_BYTES=100000000" \
      "-e" "VS_MEILI_TASK_DEADLINE_MS=1800000" \
      "-e" "VS_DISPATCH_MAX_EVENTS=250000" \
      "-e" "VS_DISPATCH_MAX_BATCH_BYTES=104857600" \
      "-e" "VS_BUS_CAPACITY=400000"
    return
  fi
  if [[ $SINK == surrealdb ]]; then
    printf '%s\n' \
      "-e" "VS_SINK=surrealdb" \
      "-e" "VS_SURREAL_ENDPOINT=http://$SURREAL:8000" \
      "-e" "VS_SURREAL_NAMESPACE=bench" \
      "-e" "VS_SURREAL_DATABASE=bench" \
      "-e" "VS_SURREAL_USERNAME=vs" \
      "-e" "VS_SURREAL_PASSWORD=vs" \
      "-e" "VS_SURREAL_TABLE=${target//-/_}"
  fi
}

boot_measure() {
  local source=$1 index=$2 seed_s=$3 start_engine_fn=$4
  local result_dir="$RESULTS/$source-bootstrap"
  mkdir -p "$result_dir"
  local t0 t1 boots tput docs samples port
  cleanup_engine
  reset_target "$index"
  t0=$(now_ns)
  "$start_engine_fn"
  "$BENCH_DIR/sample-container.sh" "$ENGINE" "$result_dir" &
  local monitor_pid=$!
  port=$(wait_engine_metrics)
  wait_delivered "$port" 0 "$RECORDS"
  t1=$(now_ns)
  kill "$monitor_pid" >/dev/null 2>&1 || true
  wait "$monitor_pid" >/dev/null 2>&1 || true
  boots=$(elapsed_s "$t0" "$t1")
  tput=$(awk -v n="$RECORDS" -v s="$boots" 'BEGIN {printf "%.2f", n/s}')
  docs=$(verified_target "$index")
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
  if [[ "$docs" != "$RECORDS" ]]; then
    echo "$source bootstrap correctness failure: expected $RECORDS docs, got $docs" >&2
    return 1
  fi
  samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/docker-stats.tsv" "$result_dir/process-memory.tsv")
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$source" "$RECORDS" "$PAYLOAD_BYTES" "$seed_s" "$boots" "$tput" "$samples" "$docs" \
    | tee -a "$BOOT_CSV"
  cleanup_engine
  if [[ $SINK == surrealdb ]]; then
    remove_container "$SURREAL"
  elif [[ $SINK == meilisearch ]]; then
    remove_container "$MEILI"
  else
    delete_index "$index"
  fi
}

# ── postgres ──
seed_postgres() {
  local batch=2500000 lo=1 hi t0 t1
  t0=$(now_ns)
  while (( lo <= RECORDS )); do
    hi=$((lo + batch - 1)); (( hi > RECORDS )) && hi=$RECORDS
    docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
      "INSERT INTO bench.events SELECT g, g % 1000, repeat('x',$PAYLOAD_BYTES) FROM generate_series($lo,$hi) g;" >/dev/null
    log "postgres seeded $hi/$RECORDS"
    lo=$((hi + 1))
  done
  docker exec vsbench-postgres psql -U ventstream -d bench -c 'CHECKPOINT' >/dev/null
  t1=$(now_ns); elapsed_s "$t0" "$t1"
}

boot_postgres() {
  check_disk postgres
  boot_prepare
  remove_container vsbench-postgres
  docker run -d --name vsbench-postgres --network "$NETWORK" \
    --cpus 2 --memory 2g \
    -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=ventstream -e POSTGRES_DB=bench \
    postgres:16-alpine postgres -c wal_level=logical -c max_wal_senders=4 -c max_replication_slots=8 \
    -c shared_buffers=512MB -c synchronous_commit=off -c max_wal_size=4GB >/dev/null
  wait_for PostgreSQL docker exec vsbench-postgres psql -U ventstream -d bench -Atc 'SELECT 1'
  docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
    "CREATE SCHEMA bench; CREATE TABLE bench.events(id bigint PRIMARY KEY, value bigint NOT NULL, payload text NOT NULL); CREATE PUBLICATION vsbench_pub FOR TABLE bench.events;" >/dev/null
  local seed_s; seed_s=$(seed_postgres)
  log "postgres seed done in ${seed_s}s"
  # Seeding 50M rows plus the CHECKPOINT can bounce postgres; it then
  # rejects connections while recovering. Wait for it to accept again
  # before handing over to the engine, or the run dies on 57P03.
  wait_for "PostgreSQL (post-seed)" docker exec vsbench-postgres \
    psql -U ventstream -d bench -Atc 'SELECT 1'
  read -r _ _ _ _ _ chunk _ <<<"$(profile_values "$PROFILE")"
  engine_pg() {
    local sink_envs=(); while IFS= read -r line; do sink_envs+=("$line"); done < <(sink_env_args vsbench-postgres)
    start_engine postgres "$PROFILE" ${sink_envs[@]+"${sink_envs[@]}"} \
      -e VS_PG_HOST=vsbench-postgres -e VS_PG_PORT=5432 \
      -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream -e VS_PG_DATABASE=bench \
      -e VS_PG_PUBLICATION=vsbench_pub -e VS_PG_SLOT=vsbench_slot \
      -e VS_PG_BOOTSTRAP_MODE=snapshot -e "VS_PG_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_PG_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/postgres.yaml \
      -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state \
      -e VS_INDEX_TEMPLATE=vsbench-postgres
  }
  boot_measure postgres vsbench-postgres "$seed_s" engine_pg
  remove_container vsbench-postgres
}

# ── mysql ──
seed_mysql() {
  local batch=1000000 lo=0 hi t0 t1
  t0=$(now_ns)
  while (( lo < RECORDS )); do
    hi=$((lo + batch)); (( hi > RECORDS )) && hi=$RECORDS
    docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e \
      "SET SESSION cte_max_recursion_depth=$((batch + 1)); INSERT INTO events WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n < $((hi - lo))) SELECT n + $lo, MOD(n + $lo,1000), REPEAT('x',$PAYLOAD_BYTES) FROM seq;" 2>/dev/null
    log "mysql seeded $hi/$RECORDS"
    lo=$hi
  done
  t1=$(now_ns); elapsed_s "$t0" "$t1"
}

boot_mysql() {
  check_disk mysql
  boot_prepare
  remove_container vsbench-mysql
  docker run -d --name vsbench-mysql --network "$NETWORK" \
    --cpus 2 --memory 2g \
    -e MYSQL_ROOT_PASSWORD=ventstream -e MYSQL_ROOT_HOST=% \
    mysql:8.4 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --sync-binlog=0 --innodb-flush-log-at-trx-commit=2 --innodb-buffer-pool-size=768M >/dev/null
  wait_for MySQL docker exec vsbench-mysql mysqladmin ping -h127.0.0.1 -uroot -pventstream --silent
  docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream -e \
    "CREATE DATABASE bench; CREATE USER 'ventstream'@'%' IDENTIFIED BY 'ventstream'; GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ventstream'@'%'; CREATE TABLE bench.events(id BIGINT PRIMARY KEY, value BIGINT NOT NULL, payload TEXT NOT NULL);" 2>/dev/null
  local seed_s; seed_s=$(seed_mysql)
  log "mysql seed done in ${seed_s}s"
  read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$PROFILE")"
  engine_my() {
    local sink_envs=(); while IFS= read -r line; do sink_envs+=("$line"); done < <(sink_env_args vsbench-mysql)
    start_engine mysql "$PROFILE" ${sink_envs[@]+"${sink_envs[@]}"} \
      -e VS_MYSQL_HOST=vsbench-mysql -e VS_MYSQL_PORT=3306 \
      -e VS_MYSQL_USER=ventstream -e VS_MYSQL_PASSWORD=ventstream -e VS_MYSQL_DATABASE=bench \
      -e VS_MYSQL_TABLES=events -e VS_MYSQL_SERVER_ID=4000000001 \
      -e VS_MYSQL_BOOTSTRAP_MODE=snapshot -e VS_MYSQL_POS_FLUSH_MS=1000 \
      -e VS_MYSQL_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/mysql.yaml \
      -e "VS_MYSQL_RECOMPOSE_CHUNK=$chunk" -e "VS_MYSQL_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_MYSQL_STATE_DIR=/var/lib/ventstream/state -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state \
      -e VS_INDEX_TEMPLATE=vsbench-mysql
  }
  boot_measure mysql vsbench-mysql "$seed_s" engine_my
  remove_container vsbench-mysql
}

# ── mongodb ──
seed_mongodb() {
  local batch=5000000 lo=1 hi t0 t1
  t0=$(now_ns)
  while (( lo <= RECORDS )); do
    hi=$((lo + batch - 1)); (( hi > RECORDS )) && hi=$RECORDS
    docker exec vsbench-mongo mongosh --quiet bench --eval \
      "const p='x'.repeat($PAYLOAD_BYTES); for(let s=$lo;s<=$hi;s+=5000){const a=[];for(let i=s;i<=Math.min($hi,s+4999);i++)a.push({_id:i,value:i%1000,payload:p});db.events.insertMany(a,{ordered:false});}" >/dev/null
    log "mongodb seeded $hi/$RECORDS"
    lo=$((hi + 1))
  done
  t1=$(now_ns); elapsed_s "$t0" "$t1"
}

boot_mongodb() {
  check_disk mongodb
  boot_prepare
  remove_container vsbench-mongo
  docker run -d --name vsbench-mongo --network "$NETWORK" --hostname vsbench-mongo \
    --cpus 2 --memory 2g mongo:7.0 mongod --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB 0.5 >/dev/null
  wait_for MongoDB docker exec vsbench-mongo mongosh --quiet --eval 'db.adminCommand({ping:1}).ok'
  docker exec vsbench-mongo mongosh --quiet --eval \
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"vsbench-mongo:27017"}]})' >/dev/null
  wait_for 'MongoDB primary' docker exec vsbench-mongo mongosh --quiet --eval \
    'if (!db.hello().isWritablePrimary) quit(1)'
  local seed_s; seed_s=$(seed_mongodb)
  log "mongodb seed done in ${seed_s}s"
  read -r _ _ _ _ _ chunk _ <<<"$(profile_values "$PROFILE")"
  engine_mg() {
    local sink_envs=(); while IFS= read -r line; do sink_envs+=("$line"); done < <(sink_env_args vsbench-mongodb)
    start_engine mongodb "$PROFILE" ${sink_envs[@]+"${sink_envs[@]}"} \
      -e 'VS_MONGO_URI=mongodb://vsbench-mongo:27017/?replicaSet=rs0' \
      -e VS_MONGO_DATABASE=bench -e VS_MONGO_COLLECTIONS=events \
      -e VS_MONGO_BOOTSTRAP_MODE=snapshot -e "VS_MONGO_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_MONGO_FULL_DOCUMENT=update_lookup -e VS_MONGO_TOKEN_FLUSH_MS=1000 \
      -e VS_MONGO_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-mongodb
  }
  boot_measure mongodb vsbench-mongodb "$seed_s" engine_mg
  remove_container vsbench-mongo
}

# ── kafka ──
seed_kafka() {
  local t0 t1
  t0=$(now_ns)
  docker exec vsbench-redpanda rpk topic create events -p 1 >/dev/null
  awk -v n="$RECORDS" -v bytes="$PAYLOAD_BYTES" 'BEGIN { p=""; for(i=0;i<bytes;i++)p=p"x"; for(i=1;i<=n;i++) printf "{\"id\":%d,\"value\":%d,\"payload\":\"%s\"}\n",i,i%1000,p }' \
    | docker exec -i vsbench-redpanda rpk topic produce events >/dev/null
  t1=$(now_ns); elapsed_s "$t0" "$t1"
}

boot_kafka() {
  check_disk kafka
  boot_prepare
  start_redpanda
  local seed_s; seed_s=$(seed_kafka)
  log "kafka seed done in ${seed_s}s"
  engine_kf() {
    local sink_envs=(); while IFS= read -r line; do sink_envs+=("$line"); done < <(sink_env_args vsbench-kafka)
    start_engine kafka "$PROFILE" ${sink_envs[@]+"${sink_envs[@]}"} \
      -e VS_KAFKA_BROKERS=vsbench-redpanda:9092 -e VS_KAFKA_TOPICS=events \
      -e VS_KAFKA_GROUP_ID=vsbench-bootstrap -e VS_KAFKA_NAMESPACE=bench \
      -e VS_KAFKA_UNWRAP=raw -e VS_KAFKA_RAW_KEY_FIELD=id \
      -e VS_KAFKA_AUTO_OFFSET_RESET=earliest -e VS_KAFKA_COMMIT_MS=1000 \
      -e VS_INDEX_TEMPLATE=vsbench-kafka
  }
  boot_measure kafka vsbench-kafka "$seed_s" engine_kf
  remove_container vsbench-redpanda
}

# ── neo4j ──
seed_neo4j() {
  local batch=50000 lo=1 hi t0 t1
  t0=$(now_ns)
  while (( lo <= RECORDS )); do
    hi=$((lo + batch - 1)); (( hi > RECORDS )) && hi=$RECORDS
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
      "UNWIND range($lo,$hi) AS id CREATE (:BenchmarkEvent {id:id,value:id%1000,payload:left(reduce(s='', x IN range(1,8) | s + 'xxxxxxxx'),$PAYLOAD_BYTES)})" >/dev/null
    if (( (hi / batch) % 20 == 0 || hi == RECORDS )); then log "neo4j seeded $hi/$RECORDS"; fi
    lo=$((hi + 1))
  done
  t1=$(now_ns); elapsed_s "$t0" "$t1"
}

boot_neo4j() {
  check_disk neo4j
  boot_prepare
  remove_container vsbench-neo4j
  docker run -d --name vsbench-neo4j --network "$NETWORK" --hostname vsbench-neo4j \
    --cpus 2 --memory 3g \
    -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes -e NEO4J_AUTH=neo4j/ventstream \
    -e NEO4J_server_memory_heap_initial__size=1g -e NEO4J_server_memory_heap_max__size=1g \
    -e NEO4J_server_memory_pagecache_size=1g \
    -e NEO4J_db_tx__log_rotation_retention__policy="100M size" \
    neo4j:5.26-enterprise >/dev/null
  wait_for Neo4j docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream 'RETURN 1'
  docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream -d system \
    "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL'" >/dev/null
  docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
    'CREATE INDEX bench_id IF NOT EXISTS FOR (n:BenchmarkEvent) ON (n.id)' >/dev/null
  local seed_s; seed_s=$(seed_neo4j)
  log "neo4j seed done in ${seed_s}s"
  read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$PROFILE")"
  engine_n4() {
    local sink_envs=(); while IFS= read -r line; do sink_envs+=("$line"); done < <(sink_env_args vsbench-neo4j)
    start_engine neo4j "$PROFILE" ${sink_envs[@]+"${sink_envs[@]}"} \
      -e VS_NEO4J_URI=bolt://vsbench-neo4j:7687 -e VS_NEO4J_USER=neo4j \
      -e VS_NEO4J_PASSWORD=ventstream -e VS_NEO4J_DATABASE=neo4j \
      -e VS_NEO4J_BOOTSTRAP_MODE=snapshot -e VS_NEO4J_POLL_INTERVAL_MS=10 \
      -e VS_NEO4J_DENORMALIZE_YAML=/specs/neo4j.yaml \
      -e "VS_NEO4J_RECOMPOSE_CHUNK=$chunk" -e "VS_NEO4J_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_NEO4J_STATE_DIR=/var/lib/ventstream/state -e VS_INDEX_TEMPLATE=vsbench-neo4j
  }
  boot_measure neo4j vsbench-neo4j "$seed_s" engine_n4
  remove_container vsbench-neo4j
}

boot_main() {
  ensure_network
  if [[ $SINK == opensearch ]]; then
    start_opensearch
  elif [[ $SINK == elasticsearch ]]; then
    start_elasticsearch
  fi
  local requested=${1:-all}
  case "$requested" in
    postgres) boot_postgres ;;
    mysql) boot_mysql ;;
    mongodb) boot_mongodb ;;
    kafka) boot_kafka ;;
    neo4j) boot_neo4j ;;
    all)
      boot_postgres
      boot_mysql
      boot_mongodb
      boot_kafka
      boot_neo4j
      ;;
    *) echo "usage: $0 [postgres|mysql|mongodb|kafka|neo4j|all]" >&2; return 2 ;;
  esac
  echo "bootstrap benchmark results: $BOOT_CSV"
}

cleanup_all_with_sink() {
  remove_container "$SURREAL"
  remove_container "$REDIS"
  remove_container "$MEILI"
  cleanup_all
}
trap cleanup_all_with_sink EXIT INT TERM
boot_main "$@"
