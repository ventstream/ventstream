#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
BENCH_DIR="$ROOT/benchmarks/container-matrix"
RUN_ID=${VS_BENCH_RUN_ID:-complex-$(date -u +%Y%m%dT%H%M%SZ)}
RESULTS=${VS_BENCH_RESULTS:-$ROOT/target/benchmarks/container-matrix/$RUN_ID}
IMAGE=${VS_BENCH_IMAGE:-ventstream-engine:bench}
NETWORK=vsbench
ENGINE=vsbench-engine
OS=vsbench-opensearch
ENGINE_CPUS=${VS_BENCH_ENGINE_CPUS:-2}
ENGINE_MEMORY=${VS_BENCH_ENGINE_MEMORY:-1g}
PAYLOAD_BYTES=${VS_BENCH_PAYLOAD_BYTES:-256}
TIMEOUT_SECS=${VS_BENCH_TIMEOUT_SECS:-1200}
SQL_LOAD_CHUNK=${VS_BENCH_SQL_LOAD_CHUNK:-1000}
NEO4J_LOAD_CHUNK=${VS_BENCH_NEO4J_LOAD_CHUNK:-250}

mkdir -p "$RESULTS"
CSV="$RESULTS/complex.csv"
printf '%s\n' 'source,profile,final_documents,source_changes,sink_writes,elapsed_s,documents_eps,sink_writes_eps,engine_cpu_mean_pct,engine_cpu_p95_pct,engine_cpu_peak_pct,engine_cgroup_peak_mib,engine_rss_peak_mib,engine_rss_hwm_mib,source_cpu_mean_pct,source_cpu_peak_pct,source_cgroup_peak_mib,opensearch_cpu_mean_pct,opensearch_cpu_peak_pct,opensearch_cgroup_peak_mib,bus_capacity,batch_events,batch_bytes,flush_ms,parallel_bulks,recompose_chunk,recompose_concurrency' >"$CSV"

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
  remove_container "${ENGINE}-proc-monitor"
  remove_volume vsbench-engine-state
}

cleanup_all() {
  cleanup_engine
  for name in vsbench-postgres vsbench-mysql vsbench-mongo vsbench-neo4j; do
    remove_container "$name"
    remove_container "${name}-proc-monitor"
  done
  remove_container "$OS"
  remove_container "${OS}-proc-monitor"
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup_all EXIT INT TERM

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

reset_index() {
  local index=$1
  docker exec "$OS" curl -fsS -XDELETE "http://127.0.0.1:9200/$index" >/dev/null 2>&1 || true
  docker exec "$OS" curl -fsS -XPUT "http://127.0.0.1:9200/$index" \
    -H 'content-type: application/json' \
    -d '{"settings":{"index":{"refresh_interval":"-1","number_of_replicas":0}}}' >/dev/null
}

index_count() {
  local index=$1
  docker exec "$OS" curl -sS "http://127.0.0.1:9200/$index/_count" | jq -r '.count // 0'
}

refresh_indices() {
  local index
  for index in "$@"; do
    docker exec "$OS" curl -fsS -XPOST "http://127.0.0.1:9200/$index/_refresh" >/dev/null
  done
}

assert_index_counts() {
  local expected=$1
  shift
  local index count
  for index in "$@"; do
    count=$(index_count "$index")
    if [[ "$count" != "$expected" ]]; then
      echo "correctness failure: $index expected $expected docs, got $count" >&2
      return 1
    fi
  done
}

assert_customer_tier() {
  local expected=$1 index value
  shift
  for index in "$@"; do
    value=$(docker exec "$OS" curl -fsS "http://127.0.0.1:9200/$index/_search?size=1" | jq -r '.hits.hits[0]._source.customer.tier // ""')
    if [[ "$value" != "$expected" ]]; then
      echo "correctness failure: $index customer.tier expected $expected, got $value" >&2
      return 1
    fi
  done
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
  read -r bus batch batch_bytes flush parallel _ _ <<<"$(profile_values "$profile")"
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
    -e "VS_BUS_CAPACITY=$bus" \
    -e "VS_DISPATCH_MAX_EVENTS=$batch" \
    -e "VS_DISPATCH_MAX_BATCH_BYTES=$batch_bytes" \
    -e "VS_DISPATCH_FLUSH_MS=$flush" \
    -e "VS_DISPATCH_PARALLEL_BULKS=$parallel" \
    -e VS_HEALTH_LISTEN=0.0.0.0:4043 \
    -e VS_DLQ_PATH=/var/lib/ventstream/dlq.jsonl \
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

wait_indices_ready() {
  local expected=$1
  shift
  local deadline=$((SECONDS + TIMEOUT_SECS)) index count all_ready
  while :; do
    refresh_indices "$@"
    all_ready=1
    for index in "$@"; do
      count=$(index_count "$index")
      if [[ "$count" != "$expected" ]]; then
        all_ready=0
      fi
    done
    if (( all_ready == 1 )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for final index counts" >&2
      docker logs --tail 100 "$ENGINE" >&2 || true
      return 1
    fi
    sleep 1
  done
}

wait_sink_quiet() {
  local port=$1 deadline=$((SECONDS + TIMEOUT_SECS)) current previous=-1 quiet=0
  while :; do
    current=$(metric_value "$port" vs_events_delivered_total || echo 0)
    if (( current == previous )); then
      quiet=$((quiet + 1))
      if (( quiet >= 3 )); then
        return 0
      fi
    else
      quiet=0
    fi
    previous=$current
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for a quiet sink" >&2
      return 1
    fi
    sleep 1
  done
}

start_monitors() {
  local result_dir=$1 source_container=$2
  "$BENCH_DIR/sample-container.sh" "$ENGINE" "$result_dir/engine" &
  ENGINE_MONITOR=$!
  "$BENCH_DIR/sample-docker-stats.sh" "$source_container" "$result_dir/source" &
  SOURCE_MONITOR=$!
  "$BENCH_DIR/sample-docker-stats.sh" "$OS" "$result_dir/opensearch" &
  OS_MONITOR=$!
}

stop_monitors() {
  local pid
  for pid in "$ENGINE_MONITOR" "$SOURCE_MONITOR" "$OS_MONITOR"; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
}

append_result() {
  local source=$1 profile=$2 docs=$3 changes=$4 sink_writes=$5 elapsed=$6 result_dir=$7
  local documents_eps sink_eps engine_samples source_samples os_samples
  local source_mean source_peak source_mem os_mean os_peak os_mem
  documents_eps=$(awk -v n="$docs" -v s="$elapsed" 'BEGIN {printf "%.2f", n/s}')
  sink_eps=$(awk -v n="$sink_writes" -v s="$elapsed" 'BEGIN {printf "%.2f", n/s}')
  engine_samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/engine/docker-stats.tsv" "$result_dir/engine/process-memory.tsv")
  source_samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/source/docker-stats.tsv" "$result_dir/source/process-memory.tsv")
  os_samples=$("$BENCH_DIR/summarize-samples.sh" "$result_dir/opensearch/docker-stats.tsv" "$result_dir/opensearch/process-memory.tsv")
  IFS=, read -r source_mean _ source_peak source_mem _ _ <<<"$source_samples"
  IFS=, read -r os_mean _ os_peak os_mem _ _ <<<"$os_samples"
  read -r bus batch batch_bytes flush parallel chunk concurrency <<<"$(profile_values "$profile")"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$source" "$profile" "$docs" "$changes" "$sink_writes" "$elapsed" \
    "$documents_eps" "$sink_eps" "$engine_samples" \
    "$source_mean" "$source_peak" "$source_mem" "$os_mean" "$os_peak" "$os_mem" \
    "$bus" "$batch" "$batch_bytes" "$flush" "$parallel" "$chunk" "$concurrency" \
    | tee -a "$CSV"
}

run_sql_measurement() {
  local source=$1 profile=$2 per_table=$3 source_container=$4 loader=$5 port=$6
  local docs=$((per_table * 4)) result_dir="$RESULTS/$source-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" "$source_container"
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  "$loader" "$per_table"
  wait_delivered "$port" "$baseline" "$docs"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_indices_ready "$per_table" \
    vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  assert_index_counts "$per_table" \
    vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  append_result "$source" "$profile" "$docs" "$docs" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

run_sql_fanout_measurement() {
  local source=$1 profile=$2 per_table=$3 source_container=$4 updater=$5 port=$6
  local docs=$((per_table * 4)) result_dir="$RESULTS/${source}_fanout-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" "$source_container"
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  "$updater" "$per_table"
  wait_delivered "$port" "$baseline" "$docs"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  assert_index_counts "$per_table" \
    vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  assert_customer_tier fanout-updated \
    vsbench-complex-orders vsbench-complex-shipments vsbench-complex-invoices vsbench-complex-support-cases
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  local changed=$per_table
  (( changed > 10000 )) && changed=10000
  append_result "${source}_fanout" "$profile" "$docs" "$changed" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

reset_relational_indices() {
  reset_index vsbench-complex-orders
  reset_index vsbench-complex-shipments
  reset_index vsbench-complex-invoices
  reset_index vsbench-complex-support-cases
}

start_postgres() {
  remove_container vsbench-postgres
  docker run -d --name vsbench-postgres --network "$NETWORK" \
    --cpus 2 --memory 1536m \
    -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=ventstream -e POSTGRES_DB=bench \
    postgres:16-alpine postgres -c wal_level=logical -c max_wal_senders=4 -c max_replication_slots=8 \
    -c shared_buffers=384MB -c synchronous_commit=off >/dev/null
  wait_for PostgreSQL docker exec vsbench-postgres psql -U ventstream -d bench -Atc 'SELECT 1'
  docker exec -i vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE SCHEMA bench;
CREATE TABLE bench.customers(id bigint PRIMARY KEY, name text, tier text);
CREATE TABLE bench.products(id bigint PRIMARY KEY, sku text, category text);
CREATE TABLE bench.regions(id bigint PRIMARY KEY, name text, zone text);
CREATE TABLE bench.warehouses(id bigint PRIMARY KEY, name text, capacity bigint);
CREATE TABLE bench.carriers(id bigint PRIMARY KEY, name text, service_level text);
CREATE TABLE bench.accounts(id bigint PRIMARY KEY, name text, segment text);
CREATE TABLE bench.currencies(id bigint PRIMARY KEY, code text, precision_digits int);
CREATE TABLE bench.agents(id bigint PRIMARY KEY, name text, level int);
CREATE TABLE bench.support_queues(id bigint PRIMARY KEY, name text, priority int);
CREATE TABLE bench.orders(id bigint PRIMARY KEY, customer_id bigint, product_id bigint, region_id bigint, status text, amount bigint, payload text);
CREATE TABLE bench.shipments(id bigint PRIMARY KEY, customer_id bigint, warehouse_id bigint, carrier_id bigint, status text, payload text);
CREATE TABLE bench.invoices(id bigint PRIMARY KEY, customer_id bigint, account_id bigint, currency_id bigint, status text, amount bigint, payload text);
CREATE TABLE bench.support_cases(id bigint PRIMARY KEY, customer_id bigint, agent_id bigint, queue_id bigint, status text, payload text);
CREATE INDEX orders_customer ON bench.orders(customer_id); CREATE INDEX orders_product ON bench.orders(product_id); CREATE INDEX orders_region ON bench.orders(region_id);
CREATE INDEX shipments_customer ON bench.shipments(customer_id); CREATE INDEX shipments_warehouse ON bench.shipments(warehouse_id); CREATE INDEX shipments_carrier ON bench.shipments(carrier_id);
CREATE INDEX invoices_customer ON bench.invoices(customer_id); CREATE INDEX invoices_account ON bench.invoices(account_id); CREATE INDEX invoices_currency ON bench.invoices(currency_id);
CREATE INDEX cases_customer ON bench.support_cases(customer_id); CREATE INDEX cases_agent ON bench.support_cases(agent_id); CREATE INDEX cases_queue ON bench.support_cases(queue_id);
INSERT INTO bench.customers SELECT g, 'customer-'||g, CASE WHEN g%10=0 THEN 'gold' ELSE 'standard' END FROM generate_series(1,10000) g;
INSERT INTO bench.products SELECT g, 'sku-'||g, 'category-'||(g%100) FROM generate_series(1,10000) g;
INSERT INTO bench.regions SELECT g, 'region-'||g, 'zone-'||(g%10) FROM generate_series(1,100) g;
INSERT INTO bench.warehouses SELECT g, 'warehouse-'||g, 10000+g FROM generate_series(1,1000) g;
INSERT INTO bench.carriers SELECT g, 'carrier-'||g, CASE WHEN g%2=0 THEN 'express' ELSE 'standard' END FROM generate_series(1,100) g;
INSERT INTO bench.accounts SELECT g, 'account-'||g, 'segment-'||(g%20) FROM generate_series(1,5000) g;
INSERT INTO bench.currencies SELECT g, 'C'||g, 2 FROM generate_series(1,10) g;
INSERT INTO bench.agents SELECT g, 'agent-'||g, 1+(g%5) FROM generate_series(1,5000) g;
INSERT INTO bench.support_queues SELECT g, 'queue-'||g, 1+(g%5) FROM generate_series(1,100) g;
CREATE PUBLICATION vsbench_complex_pub FOR ALL TABLES;
SQL
}

load_postgres_complex() {
  local per_table=$1 start end
  for ((start=1; start<=per_table; start+=SQL_LOAD_CHUNK)); do
    end=$((start + SQL_LOAD_CHUNK - 1)); (( end > per_table )) && end=$per_table
    docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c "
      BEGIN;
      INSERT INTO bench.orders SELECT g,1+(g%10000),1+(g%10000),1+(g%100),'open',g%100000,repeat('x',$PAYLOAD_BYTES) FROM generate_series($start,$end) g;
      INSERT INTO bench.shipments SELECT g,1+(g%10000),1+(g%1000),1+(g%100),'in_transit',repeat('x',$PAYLOAD_BYTES) FROM generate_series($start,$end) g;
      INSERT INTO bench.invoices SELECT g,1+(g%10000),1+(g%5000),1+(g%10),'issued',g%100000,repeat('x',$PAYLOAD_BYTES) FROM generate_series($start,$end) g;
      INSERT INTO bench.support_cases SELECT g,1+(g%10000),1+(g%5000),1+(g%100),'open',repeat('x',$PAYLOAD_BYTES) FROM generate_series($start,$end) g;
      COMMIT;" >/dev/null
  done
}

update_postgres_customers() {
  local per_table=$1 changed=$per_table first=1 last start end update_chunk=500
  (( changed > 10000 )) && changed=10000
  if (( per_table < 10000 )); then
    first=2
  fi
  last=$((first + changed - 1))
  for ((start=first; start<=last; start+=update_chunk)); do
    end=$((start + update_chunk - 1)); (( end > last )) && end=$last
    docker exec vsbench-postgres psql -U ventstream -d bench -v ON_ERROR_STOP=1 -c \
      "UPDATE bench.customers SET tier='fanout-updated' WHERE id BETWEEN $start AND $end" >/dev/null
  done
}

bench_postgres() {
  local per_table=${VS_BENCH_POSTGRES_PER_TABLE:-10000} profile port
  start_postgres
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-postgres psql -U ventstream -d bench -c 'TRUNCATE bench.orders, bench.shipments, bench.invoices, bench.support_cases' >/dev/null
    docker exec vsbench-postgres psql -U ventstream -d bench -Atc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='vsbench_complex_slot'" >/dev/null
    reset_relational_indices
    read -r _ _ _ _ _ chunk _ <<<"$(profile_values "$profile")"
    start_engine postgres "$profile" \
      -e VS_PG_HOST=vsbench-postgres -e VS_PG_PORT=5432 \
      -e VS_PG_USER=ventstream -e VS_PG_PASSWORD=ventstream -e VS_PG_DATABASE=bench \
      -e VS_PG_PUBLICATION=vsbench_complex_pub -e VS_PG_SLOT=vsbench_complex_slot \
      -e VS_PG_BOOTSTRAP_MODE=none -e "VS_PG_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_PG_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/relational-complex.yaml \
      -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state \
      -e 'VS_INDEX_TEMPLATE=${header:ventstream.target.index}'
    port=$(wait_engine_metrics)
    run_sql_measurement postgres_complex "$profile" "$per_table" vsbench-postgres load_postgres_complex "$port"
    if [[ ${VS_BENCH_RELATION_FANOUT:-0} == 1 ]]; then
      run_sql_fanout_measurement postgres_complex "$profile" "$per_table" \
        vsbench-postgres update_postgres_customers "$port"
    fi
  done
  cleanup_engine
  remove_container vsbench-postgres
}

start_mysql() {
  remove_container vsbench-mysql
  docker run -d --name vsbench-mysql --network "$NETWORK" \
    --cpus 2 --memory 1536m \
    -e MYSQL_ROOT_PASSWORD=ventstream -e MYSQL_ROOT_HOST=% \
    mysql:8.4 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
    --sync-binlog=0 --innodb-flush-log-at-trx-commit=2 --innodb-buffer-pool-size=384M >/dev/null
  wait_for MySQL docker exec vsbench-mysql mysqladmin ping -h127.0.0.1 -uroot -pventstream --silent
  docker exec -i vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream <<'SQL' >/dev/null
CREATE DATABASE bench;
CREATE USER 'ventstream'@'%' IDENTIFIED BY 'ventstream';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ventstream'@'%';
USE bench;
CREATE TABLE customers(id BIGINT PRIMARY KEY, name VARCHAR(64), tier VARCHAR(16));
CREATE TABLE products(id BIGINT PRIMARY KEY, sku VARCHAR(64), category VARCHAR(64));
CREATE TABLE regions(id BIGINT PRIMARY KEY, name VARCHAR(64), zone VARCHAR(32));
CREATE TABLE warehouses(id BIGINT PRIMARY KEY, name VARCHAR(64), capacity BIGINT);
CREATE TABLE carriers(id BIGINT PRIMARY KEY, name VARCHAR(64), service_level VARCHAR(32));
CREATE TABLE accounts(id BIGINT PRIMARY KEY, name VARCHAR(64), segment VARCHAR(32));
CREATE TABLE currencies(id BIGINT PRIMARY KEY, code VARCHAR(16), precision_digits INT);
CREATE TABLE agents(id BIGINT PRIMARY KEY, name VARCHAR(64), level INT);
CREATE TABLE support_queues(id BIGINT PRIMARY KEY, name VARCHAR(64), priority INT);
CREATE TABLE orders(id BIGINT PRIMARY KEY, customer_id BIGINT, product_id BIGINT, region_id BIGINT, status VARCHAR(32), amount BIGINT, payload TEXT, INDEX(customer_id), INDEX(product_id), INDEX(region_id));
CREATE TABLE shipments(id BIGINT PRIMARY KEY, customer_id BIGINT, warehouse_id BIGINT, carrier_id BIGINT, status VARCHAR(32), payload TEXT, INDEX(customer_id), INDEX(warehouse_id), INDEX(carrier_id));
CREATE TABLE invoices(id BIGINT PRIMARY KEY, customer_id BIGINT, account_id BIGINT, currency_id BIGINT, status VARCHAR(32), amount BIGINT, payload TEXT, INDEX(customer_id), INDEX(account_id), INDEX(currency_id));
CREATE TABLE support_cases(id BIGINT PRIMARY KEY, customer_id BIGINT, agent_id BIGINT, queue_id BIGINT, status VARCHAR(32), payload TEXT, INDEX(customer_id), INDEX(agent_id), INDEX(queue_id));
SET SESSION cte_max_recursion_depth=10001;
INSERT INTO customers WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<10000) SELECT n,CONCAT('customer-',n),IF(MOD(n,10)=0,'gold','standard') FROM s;
INSERT INTO products WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<10000) SELECT n,CONCAT('sku-',n),CONCAT('category-',MOD(n,100)) FROM s;
INSERT INTO regions WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n,CONCAT('region-',n),CONCAT('zone-',MOD(n,10)) FROM s;
INSERT INTO warehouses WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<1000) SELECT n,CONCAT('warehouse-',n),10000+n FROM s;
INSERT INTO carriers WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n,CONCAT('carrier-',n),IF(MOD(n,2)=0,'express','standard') FROM s;
INSERT INTO accounts WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<5000) SELECT n,CONCAT('account-',n),CONCAT('segment-',MOD(n,20)) FROM s;
INSERT INTO currencies WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<10) SELECT n,CONCAT('C',n),2 FROM s;
INSERT INTO agents WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<5000) SELECT n,CONCAT('agent-',n),1+MOD(n,5) FROM s;
INSERT INTO support_queues WITH RECURSIVE s AS (SELECT 1 n UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n,CONCAT('queue-',n),1+MOD(n,5) FROM s;
SQL
}

load_mysql_complex() {
  local per_table=$1 start end
  for ((start=1; start<=per_table; start+=SQL_LOAD_CHUNK)); do
    end=$((start + SQL_LOAD_CHUNK - 1)); (( end > per_table )) && end=$per_table
    docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e "
      SET SESSION cte_max_recursion_depth=$((SQL_LOAD_CHUNK + 1)); START TRANSACTION;
      INSERT INTO orders WITH RECURSIVE s AS (SELECT $start n UNION ALL SELECT n+1 FROM s WHERE n<$end) SELECT n,1+MOD(n,10000),1+MOD(n,10000),1+MOD(n,100),'open',MOD(n,100000),REPEAT('x',$PAYLOAD_BYTES) FROM s;
      INSERT INTO shipments WITH RECURSIVE s AS (SELECT $start n UNION ALL SELECT n+1 FROM s WHERE n<$end) SELECT n,1+MOD(n,10000),1+MOD(n,1000),1+MOD(n,100),'in_transit',REPEAT('x',$PAYLOAD_BYTES) FROM s;
      INSERT INTO invoices WITH RECURSIVE s AS (SELECT $start n UNION ALL SELECT n+1 FROM s WHERE n<$end) SELECT n,1+MOD(n,10000),1+MOD(n,5000),1+MOD(n,10),'issued',MOD(n,100000),REPEAT('x',$PAYLOAD_BYTES) FROM s;
      INSERT INTO support_cases WITH RECURSIVE s AS (SELECT $start n UNION ALL SELECT n+1 FROM s WHERE n<$end) SELECT n,1+MOD(n,10000),1+MOD(n,5000),1+MOD(n,100),'open',REPEAT('x',$PAYLOAD_BYTES) FROM s;
      COMMIT;" >/dev/null
  done
}

update_mysql_customers() {
  local per_table=$1 changed=$per_table first=1 last start end update_chunk=500
  (( changed > 10000 )) && changed=10000
  if (( per_table < 10000 )); then
    first=2
  fi
  last=$((first + changed - 1))
  for ((start=first; start<=last; start+=update_chunk)); do
    end=$((start + update_chunk - 1)); (( end > last )) && end=$last
    docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e \
      "UPDATE customers SET tier='fanout-updated' WHERE id BETWEEN $start AND $end" >/dev/null
  done
}

bench_mysql() {
  local per_table=${VS_BENCH_MYSQL_PER_TABLE:-5000} profile port tables
  tables=customers,products,regions,warehouses,carriers,accounts,currencies,agents,support_queues,orders,shipments,invoices,support_cases
  start_mysql
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-mysql mysql -h127.0.0.1 -uroot -pventstream bench -e 'TRUNCATE orders; TRUNCATE shipments; TRUNCATE invoices; TRUNCATE support_cases' >/dev/null
    reset_relational_indices
    read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$profile")"
    start_engine mysql "$profile" \
      -e VS_MYSQL_HOST=vsbench-mysql -e VS_MYSQL_PORT=3306 \
      -e VS_MYSQL_USER=ventstream -e VS_MYSQL_PASSWORD=ventstream -e VS_MYSQL_DATABASE=bench \
      -e "VS_MYSQL_TABLES=$tables" -e VS_MYSQL_SERVER_ID=4000000001 \
      -e VS_MYSQL_BOOTSTRAP_MODE=none -e VS_MYSQL_POS_FLUSH_MS=1000 \
      -e VS_MYSQL_DENORMALIZE_MODE=sql -e VS_JOINS_YAML=/specs/relational-complex.yaml \
      -e "VS_MYSQL_RECOMPOSE_CHUNK=$chunk" -e "VS_MYSQL_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_MYSQL_STATE_DIR=/var/lib/ventstream/state -e VS_JOINS_STATE_DIR=/var/lib/ventstream/state \
      -e 'VS_INDEX_TEMPLATE=${header:ventstream.target.index}'
    port=$(wait_engine_metrics)
    run_sql_measurement mysql_complex "$profile" "$per_table" vsbench-mysql load_mysql_complex "$port"
    if [[ ${VS_BENCH_RELATION_FANOUT:-0} == 1 ]]; then
      run_sql_fanout_measurement mysql_complex "$profile" "$per_table" \
        vsbench-mysql update_mysql_customers "$port"
    fi
  done
  cleanup_engine
  remove_container vsbench-mysql
}

reset_mongodb_indices() {
  reset_index vsbench-complex-mongo-orders
  reset_index vsbench-complex-mongo-shipments
  reset_index vsbench-complex-mongo-invoices
  reset_index vsbench-complex-mongo-support_cases
}

start_mongodb() {
  remove_container vsbench-mongo
  docker run -d --name vsbench-mongo --network "$NETWORK" --hostname vsbench-mongo \
    --cpus 2 --memory 2g \
    mongo:7.0 mongod --replSet rs0 --bind_ip_all --wiredTigerCacheSizeGB 0.5 >/dev/null
  wait_for MongoDB docker exec vsbench-mongo mongosh --quiet --eval 'db.adminCommand({ping:1}).ok'
  docker exec vsbench-mongo mongosh --quiet --eval \
    'rs.initiate({_id:"rs0",members:[{_id:0,host:"vsbench-mongo:27017"}]})' >/dev/null
  wait_for 'MongoDB primary' docker exec vsbench-mongo mongosh --quiet --eval \
    'if (!db.hello().isWritablePrimary) quit(1)'
}

reset_mongodb_collections() {
  docker exec vsbench-mongo mongosh --quiet bench --eval \
    "for (const name of ['orders','shipments','invoices','support_cases']) db.getCollection(name).drop()" >/dev/null
}

load_mongodb_complex() {
  local per_collection=$1
  docker exec vsbench-mongo mongosh --quiet bench --eval \
    "const count=$per_collection, bytes=$PAYLOAD_BYTES, payload='x'.repeat(bytes);
     for (const name of ['orders','shipments','invoices','support_cases']) {
       for (let start=1; start<=count; start+=1000) {
         const docs=[];
         for (let id=start; id<=Math.min(count,start+999); id++) {
           docs.push({_id:id,status:'created',value:id%1000,payload,collection:name});
         }
         db.getCollection(name).insertMany(docs,{ordered:false});
       }
     }" >/dev/null
}

update_mongodb_complex() {
  docker exec vsbench-mongo mongosh --quiet bench --eval \
    "for (const name of ['orders','shipments','invoices','support_cases']) {
       db.getCollection(name).updateMany({},{\$set:{status:'fanout-updated',update_sequence:1}});
     }" >/dev/null
}

assert_mongodb_status() {
  local index value
  for index in \
    vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases; do
    value=$(docker exec "$OS" curl -fsS "http://127.0.0.1:9200/$index/_search?size=1" | jq -r '.hits.hits[0]._source.status // ""')
    if [[ "$value" != fanout-updated ]]; then
      echo "MongoDB update verification failed: $index status=$value" >&2
      return 1
    fi
  done
}

run_mongodb_measurement() {
  local profile=$1 per_collection=$2 port=$3
  local docs=$((per_collection * 4)) result_dir="$RESULTS/mongodb_complex-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" vsbench-mongo
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  load_mongodb_complex "$per_collection"
  wait_delivered "$port" "$baseline" "$docs"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_indices_ready "$per_collection" \
    vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases
  assert_index_counts "$per_collection" \
    vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  append_result mongodb_complex "$profile" "$docs" "$docs" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

run_mongodb_update_measurement() {
  local profile=$1 per_collection=$2 port=$3
  local docs=$((per_collection * 4)) result_dir="$RESULTS/mongodb_complex_updates-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" vsbench-mongo
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  update_mongodb_complex
  wait_delivered "$port" "$baseline" "$docs"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases
  assert_index_counts "$per_collection" \
    vsbench-complex-mongo-orders vsbench-complex-mongo-shipments \
    vsbench-complex-mongo-invoices vsbench-complex-mongo-support_cases
  assert_mongodb_status
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  append_result mongodb_complex_updates "$profile" "$docs" "$docs" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

bench_mongodb() {
  local per_collection=${VS_BENCH_MONGODB_PER_COLLECTION:-10000} profile port
  start_mongodb
  for profile in "${profiles[@]}"; do
    cleanup_engine
    reset_mongodb_collections
    reset_mongodb_indices
    read -r _ _ _ _ _ chunk _ <<<"$(profile_values "$profile")"
    start_engine mongodb "$profile" \
      -e 'VS_MONGO_URI=mongodb://vsbench-mongo:27017/?replicaSet=rs0' \
      -e VS_MONGO_DATABASE=bench \
      -e VS_MONGO_COLLECTIONS=orders,shipments,invoices,support_cases \
      -e VS_MONGO_BOOTSTRAP_MODE=none -e "VS_MONGO_BOOTSTRAP_CHUNK_SIZE=$chunk" \
      -e VS_MONGO_FULL_DOCUMENT=update_lookup -e VS_MONGO_TOKEN_FLUSH_MS=1000 \
      -e VS_MONGO_STATE_DIR=/var/lib/ventstream/state \
      -e 'VS_INDEX_TEMPLATE=vsbench-complex-mongo-${header:ventstream.cdc.relation}'
    port=$(wait_engine_metrics)
    run_mongodb_measurement "$profile" "$per_collection" "$port"
    if [[ ${VS_BENCH_MONGODB_UPDATES:-0} == 1 ]]; then
      run_mongodb_update_measurement "$profile" "$per_collection" "$port"
    fi
  done
  cleanup_engine
  remove_container vsbench-mongo
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

reset_neo4j_indices() {
  reset_index vsbench-complex-customer_orders
  reset_index vsbench-complex-seller_catalogs
  reset_index vsbench-complex-shipment_routes
  reset_index vsbench-complex-support_assignments
}

load_neo4j_complex() {
  local per_spec=$1 start end
  for ((start=1; start<=per_spec; start+=NEO4J_LOAD_CHUNK)); do
    end=$((start + NEO4J_LOAD_CHUNK - 1)); (( end > per_spec )) && end=$per_spec
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
      "UNWIND range($start,$end) AS id
       CREATE (customer:Customer {id:id,name:'customer-'+toString(id)})-[:PLACED]->(order:Order {id:id,status:'open'})-[:CONTAINS]->(:Product {id:id,sku:'sku-'+toString(id),category:'category-'+toString(id%100)})
       CREATE (seller:Seller {id:id,name:'seller-'+toString(id)})-[:LISTS]->(listing:Listing {id:id,price:id%10000})-[:IN_CATEGORY]->(:Category {id:id,name:'category-'+toString(id%100)})
       CREATE (shipment:Shipment {id:id,tracking_number:'track-'+toString(id)})-[:HANDLED_BY]->(carrier:Carrier {id:id,name:'carrier-'+toString(id%100)})-[:BASED_IN]->(:Region {id:id,name:'region-'+toString(id%100),zone:'zone-'+toString(id%10)})
       CREATE (support:SupportCase {id:id,subject:'case-'+toString(id)})-[:ASSIGNED_TO]->(agent:Agent {id:id,name:'agent-'+toString(id)})-[:MEMBER_OF]->(:Team {id:id,name:'team-'+toString(id%100),tier:'tier-'+toString(id%5)})" >/dev/null
  done
}

assert_neo4j_shape() {
  local hits
  hits=$(docker exec "$OS" curl -fsS 'http://127.0.0.1:9200/vsbench-complex-customer_orders/_search?size=1' | jq -r '.hits.hits[0]._source | select(.order.id != null and .product.sku != null) | 1')
  [[ "$hits" == 1 ]] || { echo 'Neo4j two-hop document shape verification failed' >&2; return 1; }
}

update_neo4j_second_hop_nodes() {
  docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream \
    "MATCH (n:Product) SET n.category='fanout-updated';
     MATCH (n:Category) SET n.name='fanout-updated';
     MATCH (n:Region) SET n.zone='fanout-updated';
     MATCH (n:Team) SET n.tier='fanout-updated'" >/dev/null
}

assert_neo4j_fanout_shape() {
  local index path value
  while IFS=' ' read -r index path; do
    value=$(docker exec "$OS" curl -fsS "http://127.0.0.1:9200/$index/_search?size=1" | jq -r ".hits.hits[0]._source.$path // \"\"")
    if [[ "$value" != fanout-updated ]]; then
      echo "Neo4j fan-out verification failed: $index $path=$value" >&2
      return 1
    fi
  done <<'CHECKS'
vsbench-complex-customer_orders product.category
vsbench-complex-seller_catalogs category.name
vsbench-complex-shipment_routes region.zone
vsbench-complex-support_assignments team.tier
CHECKS
}

run_neo4j_measurement() {
  local profile=$1 per_spec=$2 port=$3
  local docs=$((per_spec * 4))
  local changes=$((docs * 5))
  local result_dir="$RESULTS/neo4j_multihop-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" vsbench-neo4j
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  load_neo4j_complex "$per_spec"
  wait_indices_ready "$per_spec" \
    vsbench-complex-customer_orders vsbench-complex-seller_catalogs \
    vsbench-complex-shipment_routes vsbench-complex-support_assignments
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-customer_orders vsbench-complex-seller_catalogs \
    vsbench-complex-shipment_routes vsbench-complex-support_assignments
  assert_index_counts "$per_spec" \
    vsbench-complex-customer_orders vsbench-complex-seller_catalogs \
    vsbench-complex-shipment_routes vsbench-complex-support_assignments
  assert_neo4j_shape
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  append_result neo4j_multihop "$profile" "$docs" "$changes" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

run_neo4j_fanout_measurement() {
  local profile=$1 per_spec=$2 port=$3
  local docs=$((per_spec * 4)) result_dir="$RESULTS/neo4j_two_hop_fanout-$profile"
  local baseline final start_ns end_ns elapsed
  mkdir -p "$result_dir"
  baseline=$(metric_value "$port" vs_events_delivered_total)
  start_monitors "$result_dir" vsbench-neo4j
  start_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  update_neo4j_second_hop_nodes
  wait_delivered "$port" "$baseline" "$docs"
  end_ns=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000')
  stop_monitors
  wait_sink_quiet "$port"
  refresh_indices vsbench-complex-customer_orders vsbench-complex-seller_catalogs \
    vsbench-complex-shipment_routes vsbench-complex-support_assignments
  assert_index_counts "$per_spec" \
    vsbench-complex-customer_orders vsbench-complex-seller_catalogs \
    vsbench-complex-shipment_routes vsbench-complex-support_assignments
  assert_neo4j_fanout_shape
  final=$(metric_value "$port" vs_events_delivered_total)
  elapsed=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN {printf "%.3f", (end-start)/1000000000}')
  append_result neo4j_two_hop_fanout "$profile" "$docs" "$docs" "$((final - baseline))" "$elapsed" "$result_dir"
  docker logs "$ENGINE" >"$result_dir/engine.log" 2>&1 || true
}

bench_neo4j() {
  local per_spec=${VS_BENCH_NEO4J_PER_SPEC:-1000} profile port
  start_neo4j
  for profile in "${profiles[@]}"; do
    cleanup_engine
    docker exec vsbench-neo4j cypher-shell -u neo4j -p ventstream 'MATCH (n) DETACH DELETE n' >/dev/null
    reset_neo4j_indices
    read -r _ _ _ _ _ chunk concurrency <<<"$(profile_values "$profile")"
    start_engine neo4j "$profile" \
      -e VS_NEO4J_URI=bolt://vsbench-neo4j:7687 -e VS_NEO4J_USER=neo4j \
      -e VS_NEO4J_PASSWORD=ventstream -e VS_NEO4J_DATABASE=neo4j \
      -e VS_NEO4J_BOOTSTRAP_MODE=none -e VS_NEO4J_POLL_INTERVAL_MS=10 \
      -e VS_NEO4J_DENORMALIZE_YAML=/specs/neo4j-multihop.yaml \
      -e VS_NEO4J_PROJECTION_FAN_OUT=true \
      -e "VS_NEO4J_RECOMPOSE_CHUNK=$chunk" -e "VS_NEO4J_RECOMPOSE_CONCURRENCY=$concurrency" \
      -e VS_NEO4J_STATE_DIR=/var/lib/ventstream/state \
      -e 'VS_INDEX_TEMPLATE=vsbench-complex-${header:ventstream.cdc.relation}'
    port=$(wait_engine_metrics)
    run_neo4j_measurement "$profile" "$per_spec" "$port"
    if [[ ${VS_BENCH_NEO4J_FANOUT:-0} == 1 ]]; then
      run_neo4j_fanout_measurement "$profile" "$per_spec" "$port"
    fi
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
    neo4j) bench_neo4j ;;
    all)
      bench_postgres
      bench_mysql
      bench_mongodb
      bench_neo4j
      ;;
    *) echo "usage: $0 [postgres|mysql|mongodb|neo4j|all]" >&2; return 2 ;;
  esac
  echo "complex benchmark results: $CSV"
}

main "$@"
