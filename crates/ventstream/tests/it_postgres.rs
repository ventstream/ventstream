//! Postgres CDC integration tests — real engine binary against real
//! Postgres + OpenSearch containers. Run with: `cargo test -p ventstream
//! --test it_postgres -- --test-threads=1` (each test owns its stack).
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::time::{Duration, Instant};

use redis::AsyncCommands;
use serde_json::Value;
use ventstream_joins::{PkValue, RelatedFetcher};
use ventstream_sources::postgres::{PostgresCdcConfig, PostgresFetcher};

const ORDERS_SPEC: &str = r#"
joins:
  - name: orders
    primary:
      table: shop.orders
      pk: order_id
    target:
      index: it_pg_sql_mode_orders
    related:
      - id: customer
        table: shop.customers
        pk: customer_id
        join_on: { from: customer_id, to: customer_id }
        embed_as: customer
        cardinality: one
        select: [customer_id, name, tier]
      - id: items
        table: shop.order_items
        pk: item_id
        join_on: { from: order_id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: item_id
        select: [item_id, sku, qty, price]
    state:
      backend: memory
    backfill:
      mode: sync_on_miss
"#;

const SEED: &str = r#"
CREATE SCHEMA shop;
CREATE TABLE shop.customers(customer_id text PRIMARY KEY, name text, tier text);
CREATE TABLE shop.orders(order_id text PRIMARY KEY, customer_id text REFERENCES shop.customers, status text, total numeric);
CREATE TABLE shop.order_items(item_id text PRIMARY KEY, order_id text REFERENCES shop.orders, sku text, qty int, price numeric);
INSERT INTO shop.customers VALUES ('c1','Ada','gold');
INSERT INTO shop.orders VALUES ('ord-1','c1','open',50);
INSERT INTO shop.order_items VALUES ('i1','ord-1','SKU-A',1,10),('i2','ord-1','SKU-B',1,40);
CREATE PUBLICATION ventstream_shop FOR TABLE shop.orders, shop.customers, shop.order_items;
"#;

const NUMERIC_PK_SPEC: &str = r#"
joins:
  - name: numeric_events
    primary:
      table: benchmark.events
      pk: id
    target:
      index: it_pg_empty_numeric_pk
    related: []
    state:
      backend: memory
    backfill:
      mode: none
"#;

fn doc_id(order: &str) -> String {
    format!("shop.orders:[\"{order}\"]")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn sql_mode_materializes_to_redis_and_recovers_from_outage() {
    const PREFIX: &str = "ventstream:it:postgres";
    const SPEC: &str = "joins: []\n";
    let stack = common::start_pg_redis().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA redis_it;
         CREATE TABLE redis_it.orders(
           id text PRIMARY KEY,
           status text NOT NULL,
           total bigint NOT NULL
         );
         INSERT INTO redis_it.orders VALUES ('ord-1', 'created', 100);
         CREATE PUBLICATION ventstream_redis FOR TABLE redis_it.orders;",
    )
    .await
    .expect("seed Redis sink source");

    let dir = common::state_dir("postgres-redis", stack.pg_port);
    let spec = common::write_spec(&dir, SPEC);
    let opts = common::PgRedisEngine {
        pg_port: stack.pg_port,
        redis_port: stack.redis_port,
        slot: "redis_sink_slot",
        publication: "ventstream_redis",
        spec_path: &spec,
        state_dir: &dir,
        key_prefix: PREFIX,
        key_routing: "by_output_relation",
        keyspace_ownership: "exclusive",
    };
    let mut engine = common::spawn_pg_redis_engine(&opts);
    let key = format!("{PREFIX}:{{orders}}:redis_it.orders%3A%5B%22ord-1%22%5D");

    common::wait_until(Duration::from_secs(60), "Redis snapshot", || {
        let key = key.clone();
        async move {
            let Ok(mut redis) = common::redis_connection(stack.redis_port).await else {
                return false;
            };
            redis
                .get::<_, Vec<u8>>(&key)
                .await
                .ok()
                .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                .is_some_and(|doc| doc["status"] == "created" && doc["total"] == 100)
        }
    })
    .await;

    pg.batch_execute("UPDATE redis_it.orders SET status='paid', total=125 WHERE id='ord-1';")
        .await
        .expect("update Redis sink source");
    common::wait_until(Duration::from_secs(30), "Redis update", || {
        let key = key.clone();
        async move {
            let Ok(mut redis) = common::redis_connection(stack.redis_port).await else {
                return false;
            };
            redis
                .get::<_, Vec<u8>>(&key)
                .await
                .ok()
                .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                .is_some_and(|doc| doc["status"] == "paid" && doc["total"] == 125)
        }
    })
    .await;

    let transaction_client = common::pg_client(stack.pg_port).await;
    let committing = tokio::spawn(async move {
        transaction_client
            .batch_execute(
                "BEGIN;
                 UPDATE redis_it.orders
                    SET status='committing', total=140
                  WHERE id='ord-1';
                 SELECT pg_sleep(2);
                 COMMIT;",
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(750)).await;
    let before_commit = common::redis_value(stack.redis_port, &key)
        .await
        .expect("read Redis while PostgreSQL transaction is open")
        .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
        .expect("materialized document before commit");
    assert_eq!(before_commit["status"], "paid");
    assert_eq!(before_commit["total"], 125);
    committing
        .await
        .expect("transaction task")
        .expect("commit source transaction");
    common::wait_until(Duration::from_secs(30), "Redis committed update", || {
        let key = key.clone();
        async move {
            common::redis_value(stack.redis_port, &key)
                .await
                .ok()
                .flatten()
                .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                .is_some_and(|doc| doc["status"] == "committing" && doc["total"] == 140)
        }
    })
    .await;

    stack.redis.pause().await.expect("pause Redis");
    pg.batch_execute("UPDATE redis_it.orders SET status='shipped', total=150 WHERE id='ord-1';")
        .await
        .expect("write while Redis is unavailable");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        std::path::Path::new(&format!("{dir}/dlq.jsonl"))
            .metadata()
            .map_or(true, |metadata| metadata.len() == 0),
        "transient Redis outages must not write to the DLQ"
    );
    stack.redis.unpause().await.expect("resume Redis");
    common::wait_until(Duration::from_secs(60), "Redis outage recovery", || {
        let key = key.clone();
        async move {
            let Ok(mut redis) = common::redis_connection(stack.redis_port).await else {
                return false;
            };
            redis
                .get::<_, Vec<u8>>(&key)
                .await
                .ok()
                .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                .is_some_and(|doc| doc["status"] == "shipped" && doc["total"] == 150)
        }
    })
    .await;

    pg.batch_execute(
        "BEGIN;
         TRUNCATE redis_it.orders;
         INSERT INTO redis_it.orders(id, status, total)
           VALUES ('ord-2', 'created-after-truncate', 200);
         COMMIT;",
    )
    .await
    .expect("truncate and insert in one transaction");
    let post_truncate_key = format!("{PREFIX}:{{orders}}:redis_it.orders%3A%5B%22ord-2%22%5D");
    common::wait_until(
        Duration::from_secs(60),
        "Redis transactional truncate and insert",
        || {
            let key = post_truncate_key.clone();
            async move {
                let keys = common::redis_keys(stack.redis_port, &format!("{PREFIX}:{{orders}}:*"))
                    .await
                    .unwrap_or_default();
                keys.len() == 1
                    && common::redis_value(stack.redis_port, &key)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                        .is_some_and(|doc| doc["status"] == "created-after-truncate")
            }
        },
    )
    .await;

    engine.terminate();
    let _restarted = common::spawn_pg_redis_engine(&opts);
    pg.batch_execute("DELETE FROM redis_it.orders WHERE id='ord-2';")
        .await
        .expect("delete after engine restart");
    common::wait_until(
        Duration::from_secs(60),
        "Redis delete after restart",
        || {
            let key = post_truncate_key.clone();
            async move {
                let Ok(mut redis) = common::redis_connection(stack.redis_port).await else {
                    return false;
                };
                redis.exists::<_, bool>(&key).await == Ok(false)
            }
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn sql_mode_redis_handles_primary_and_related_truncates() {
    const PREFIX: &str = "ventstream:it:postgres:truncate";
    const TARGET: &str = "orders_projection";
    const SPEC: &str = r#"
joins:
  - name: orders_projection
    primary:
      table: redis_join.orders
      pk: id
    target:
      index: orders_projection
    related:
      - id: items
        table: redis_join.order_items
        pk: id
        join_on: { from: id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: id
        select: [id, sku]
"#;
    let stack = common::start_pg_redis().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA redis_join;
         CREATE TABLE redis_join.orders(
           id text PRIMARY KEY,
           status text NOT NULL
         );
         CREATE TABLE redis_join.order_items(
           id text PRIMARY KEY,
           order_id text NOT NULL,
           sku text NOT NULL
         );
         CREATE INDEX redis_join_order_items_order_id
           ON redis_join.order_items(order_id);
         INSERT INTO redis_join.orders VALUES
           ('ord-1', 'open'),
           ('ord-2', 'open');
         INSERT INTO redis_join.order_items VALUES
           ('item-1', 'ord-1', 'SKU-1'),
           ('item-2', 'ord-1', 'SKU-2'),
           ('item-3', 'ord-2', 'SKU-3');
         CREATE PUBLICATION ventstream_redis_join
           FOR TABLE redis_join.orders, redis_join.order_items;",
    )
    .await
    .expect("seed joined Redis sink source");

    let dir = common::state_dir("postgres-redis-truncate", stack.pg_port);
    let spec = common::write_spec(&dir, SPEC);
    let opts = common::PgRedisEngine {
        pg_port: stack.pg_port,
        redis_port: stack.redis_port,
        slot: "redis_join_sink_slot",
        publication: "ventstream_redis_join",
        spec_path: &spec,
        state_dir: &dir,
        key_prefix: PREFIX,
        key_routing: "by_projection_target",
        keyspace_ownership: "exclusive",
    };
    let _engine = common::spawn_pg_redis_engine(&opts);
    let key_pattern = format!("{PREFIX}:{{{TARGET}}}:*");

    common::wait_until(Duration::from_secs(60), "joined Redis snapshot", || async {
        let Ok(keys) = common::redis_keys(stack.redis_port, &key_pattern).await else {
            return false;
        };
        if keys.len() != 2 {
            return false;
        }
        for key in keys {
            let Some(payload) = common::redis_value(stack.redis_port, &key)
                .await
                .ok()
                .flatten()
            else {
                return false;
            };
            let Ok(document) = serde_json::from_slice::<Value>(&payload) else {
                return false;
            };
            if document["items"].as_array().is_none_or(Vec::is_empty) {
                return false;
            }
        }
        true
    })
    .await;

    pg.batch_execute("TRUNCATE redis_join.order_items;")
        .await
        .expect("truncate related table");
    common::wait_until(
        Duration::from_secs(60),
        "related truncate recomposition",
        || async {
            let Ok(keys) = common::redis_keys(stack.redis_port, &key_pattern).await else {
                return false;
            };
            if keys.len() != 2 {
                return false;
            }
            for key in keys {
                let Some(payload) = common::redis_value(stack.redis_port, &key)
                    .await
                    .ok()
                    .flatten()
                else {
                    return false;
                };
                let Ok(document) = serde_json::from_slice::<Value>(&payload) else {
                    return false;
                };
                if !document["items"].as_array().is_some_and(Vec::is_empty) {
                    return false;
                }
            }
            true
        },
    )
    .await;

    pg.batch_execute("TRUNCATE redis_join.orders;")
        .await
        .expect("truncate primary table");
    common::wait_until(
        Duration::from_secs(60),
        "primary truncate target clear",
        || async {
            common::redis_keys(stack.redis_port, &key_pattern)
                .await
                .is_ok_and(|keys| keys.is_empty())
        },
    )
    .await;

    pg.batch_execute("INSERT INTO redis_join.orders VALUES ('ord-3', 'after-truncate');")
        .await
        .expect("insert after primary truncate");
    common::wait_until(
        Duration::from_secs(60),
        "joined insert after truncate",
        || async {
            let Ok(keys) = common::redis_keys(stack.redis_port, &key_pattern).await else {
                return false;
            };
            keys.len() == 1
                && common::redis_value(stack.redis_port, &keys[0])
                    .await
                    .ok()
                    .flatten()
                    .and_then(|payload| serde_json::from_slice::<Value>(&payload).ok())
                    .is_some_and(|document| document["status"] == "after-truncate")
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn sql_mode_projection_materializes_declarative_redis_views() {
    const PREFIX: &str = "ventstream:it:postgres:views";
    const TARGET: &str = "orders_projection";
    const SPEC: &str = r#"
joins:
  - name: orders_projection
    primary:
      table: redis_views.orders
      pk: id
    target:
      index: orders_projection
    related:
      - id: items
        table: redis_views.order_items
        pk: id
        join_on: { from: id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: id
        select: [id, sku]
"#;

    let stack = common::start_pg_redis().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA redis_views;
         CREATE TABLE redis_views.orders(
           id text PRIMARY KEY,
           customer_id text NOT NULL,
           status text NOT NULL
         );
         CREATE TABLE redis_views.order_items(
           id text PRIMARY KEY,
           order_id text NOT NULL,
           sku text NOT NULL
         );
         CREATE INDEX redis_views_order_items_order_id
           ON redis_views.order_items(order_id);
         INSERT INTO redis_views.orders VALUES
           ('ord-1', 'customer-1', 'open');
         INSERT INTO redis_views.order_items VALUES
           ('item-1', 'ord-1', 'SKU-1');
         CREATE PUBLICATION ventstream_redis_views
           FOR TABLE redis_views.orders, redis_views.order_items;",
    )
    .await
    .expect("seed PostgreSQL Redis views source");

    let state_dir = common::state_dir("postgres-redis-views", stack.pg_port);
    let spec_path = common::write_spec(&state_dir, SPEC);
    let config_path = format!("{state_dir}/ventstream.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    host: 127.0.0.1
    port: {pg_port}
    user: {pg_user}
    password_ref: env:VS_PG_PASSWORD
    database: {pg_database}
    publication: ventstream_redis_views
    slot: redis_views_sink_slot
    bootstrap:
      mode: snapshot
      chunk_size: 100
    denormalize_mode: sql
    tls:
      mode: disabled
specs:
  joins: {spec_path}
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_SINK_URL
    keyspace:
      prefix: {PREFIX}
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: active_order_by_customer
            source:
              projection_target: {TARGET}
            key:
              template: "customer:${{json:/customer_id}}:order:${{json:/id}}"
            filter:
              conditions:
                - path: /status
                  operator: not_equals
                  value: cancelled
            value:
              mode: document
runtime:
  dlq_path: {state_dir}/dlq.jsonl
  joins:
    state_dir: {state_dir}
    lsn_flush_ms: 100
"#,
            pg_port = stack.pg_port,
            pg_user = common::PG_USER,
            pg_database = common::PG_DB,
        ),
    )
    .expect("write PostgreSQL Redis view config");

    let _engine = common::spawn_engine_with_config(
        &state_dir,
        &config_path,
        &[
            ("VS_PG_PASSWORD", common::PG_PASSWORD.to_owned()),
            ("VS_REDIS_SINK_URL", common::redis_url(stack.redis_port)),
        ],
    );
    let first_key =
        format!("{PREFIX}:{{active_order_by_customer}}:customer%3Acustomer-1%3Aorder%3Aord-1");
    let second_key =
        format!("{PREFIX}:{{active_order_by_customer}}:customer%3Acustomer-2%3Aorder%3Aord-1");

    common::wait_until(
        Duration::from_secs(60),
        "PostgreSQL Redis view snapshot",
        || async {
            common::redis_value(stack.redis_port, &first_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
                .is_some_and(|document| {
                    document["status"] == "open"
                        && document["items"]
                            .as_array()
                            .is_some_and(|items| items.len() == 1)
                })
        },
    )
    .await;

    pg.batch_execute("INSERT INTO redis_views.order_items VALUES ('item-2', 'ord-1', 'SKU-2');")
        .await
        .expect("insert PostgreSQL related row");
    common::wait_until(
        Duration::from_secs(30),
        "PostgreSQL Redis view recomposition",
        || async {
            common::redis_value(stack.redis_port, &first_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
                .is_some_and(|document| {
                    document["items"]
                        .as_array()
                        .is_some_and(|items| items.len() == 2)
                })
        },
    )
    .await;

    pg.batch_execute(
        "UPDATE redis_views.orders
         SET customer_id='customer-2'
         WHERE id='ord-1';",
    )
    .await
    .expect("move PostgreSQL Redis view");
    common::wait_until(
        Duration::from_secs(30),
        "PostgreSQL Redis view move",
        || async {
            common::redis_value(stack.redis_port, &first_key).await == Ok(None)
                && common::redis_value(stack.redis_port, &second_key)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
                    .is_some_and(|document| document["customer_id"] == "customer-2")
        },
    )
    .await;

    pg.batch_execute("UPDATE redis_views.orders SET status='cancelled' WHERE id='ord-1';")
        .await
        .expect("filter PostgreSQL Redis view");
    common::wait_until(
        Duration::from_secs(30),
        "PostgreSQL Redis view filter transition",
        || async { common::redis_value(stack.redis_port, &second_key).await == Ok(None) },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn redis_rebootstrap_requires_exclusive_ownership_and_restores_the_live_set() {
    const PREFIX: &str = "ventstream:it:postgres:rebootstrap";
    const TARGET: &str = "orders";
    const SLOT: &str = "redis_rebootstrap_slot";
    const SPEC: &str = "joins: []\n";
    let stack = common::start_pg_redis().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA redis_rebootstrap;
         CREATE TABLE redis_rebootstrap.orders(
           id text PRIMARY KEY,
           status text NOT NULL
         );
         INSERT INTO redis_rebootstrap.orders VALUES
           ('ord-1', 'open'),
           ('ord-2', 'paid');
         CREATE PUBLICATION ventstream_redis_rebootstrap
           FOR TABLE redis_rebootstrap.orders;",
    )
    .await
    .expect("seed Redis rebootstrap source");

    let dir = common::state_dir("postgres-redis-rebootstrap", stack.pg_port);
    let spec = common::write_spec(&dir, SPEC);
    let shared = common::PgRedisEngine {
        pg_port: stack.pg_port,
        redis_port: stack.redis_port,
        slot: SLOT,
        publication: "ventstream_redis_rebootstrap",
        spec_path: &spec,
        state_dir: &dir,
        key_prefix: PREFIX,
        key_routing: "by_output_relation",
        keyspace_ownership: "shared",
    };
    let mut engine = common::spawn_pg_redis_engine(&shared);
    let owned_pattern = format!("{PREFIX}:{{{TARGET}}}:*");
    let stale_key = format!("{PREFIX}:{{{TARGET}}}:stale");
    let neighboring_key = format!("{PREFIX}:{{unrelated_projection}}:keep");

    common::wait_until(
        Duration::from_secs(60),
        "initial Redis live set",
        || async {
            common::redis_keys(stack.redis_port, &owned_pattern)
                .await
                .is_ok_and(|keys| keys.len() == 2)
        },
    )
    .await;

    let mut redis = common::redis_connection(stack.redis_port)
        .await
        .expect("connect to Redis");
    redis
        .set::<_, _, ()>(&stale_key, br#"{"stale":true}"#)
        .await
        .expect("seed stale owned key");
    redis
        .set::<_, _, ()>(&neighboring_key, br#"{"keep":true}"#)
        .await
        .expect("seed neighboring key");
    engine.terminate();

    let shared_output = common::pg_redis_engine_command(&shared)
        .arg("--fleet-rebootstrap")
        .output()
        .expect("run shared-keyspace rebootstrap");
    assert!(
        !shared_output.status.success(),
        "shared-keyspace rebootstrap must be refused"
    );
    assert!(
        String::from_utf8_lossy(&shared_output.stderr).contains("ownership=exclusive"),
        "unexpected rebootstrap error: {}",
        String::from_utf8_lossy(&shared_output.stderr)
    );
    let slot_exists: bool = pg
        .query_one(
            "SELECT EXISTS(
               SELECT 1 FROM pg_replication_slots WHERE slot_name = $1
             )",
            &[&SLOT],
        )
        .await
        .expect("query replication slot after refused rebootstrap")
        .get(0);
    assert!(
        slot_exists,
        "ownership validation must happen before source checkpoint removal"
    );
    assert_eq!(
        common::redis_keys(stack.redis_port, &owned_pattern)
            .await
            .expect("read owned keys after refused rebootstrap")
            .len(),
        3,
        "refused rebootstrap must leave the sink untouched"
    );

    let exclusive = common::PgRedisEngine {
        keyspace_ownership: "exclusive",
        ..shared
    };
    let exclusive_output = common::pg_redis_engine_command(&exclusive)
        .arg("--fleet-rebootstrap")
        .output()
        .expect("run exclusive-keyspace rebootstrap");
    assert!(
        exclusive_output.status.success(),
        "exclusive rebootstrap failed: {}",
        String::from_utf8_lossy(&exclusive_output.stderr)
    );
    assert!(
        common::redis_keys(stack.redis_port, &owned_pattern)
            .await
            .expect("read owned keys after rebootstrap")
            .is_empty(),
        "rebootstrap must clear the exclusively owned target"
    );
    assert!(
        common::redis_value(stack.redis_port, &neighboring_key)
            .await
            .expect("read neighboring key")
            .is_some(),
        "rebootstrap must not clear another target under the same prefix"
    );

    let _engine = common::spawn_pg_redis_engine_with_forced_bootstrap(&exclusive);
    common::wait_until(
        Duration::from_secs(60),
        "Redis live set after forced bootstrap",
        || async {
            common::redis_keys(stack.redis_port, &owned_pattern)
                .await
                .is_ok_and(|keys| keys.len() == 2 && !keys.contains(&stale_key))
        },
    )
    .await;

    pg.batch_execute("INSERT INTO redis_rebootstrap.orders VALUES ('ord-3', 'after-rebootstrap');")
        .await
        .expect("insert after Redis rebootstrap");
    common::wait_until(
        Duration::from_secs(60),
        "Redis tail after rebootstrap",
        || async {
            common::redis_keys(stack.redis_port, &owned_pattern)
                .await
                .is_ok_and(|keys| keys.len() == 3)
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn sql_mode_discovers_numeric_pk_type_before_first_insert() {
    const INDEX: &str = "it_pg_empty_numeric_pk";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA benchmark;
         CREATE TABLE benchmark.events(
           id bigint PRIMARY KEY,
           value bigint NOT NULL
         );
         CREATE PUBLICATION ventstream_numeric FOR TABLE benchmark.events;",
    )
    .await
    .expect("create empty numeric table");

    let dir = common::state_dir("empty-numeric", stack.pg_port);
    let spec = common::write_spec(&dir, NUMERIC_PK_SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "empty_numeric_slot",
        publication: "ventstream_numeric",
        spec_path: &spec,
        state_dir: &dir,
        index_template: "${header:ventstream.target.index}",
        denormalize_mode: "sql",
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    pg.execute(
        "INSERT INTO benchmark.events(id, value) VALUES ($1, $2)",
        &[&42_i64, &7_i64],
    )
    .await
    .expect("insert first numeric row after engine preparation");

    common::wait_until(Duration::from_secs(30), "numeric document", || async {
        common::os_doc(stack.os_port, INDEX, "benchmark.events:[\"42\"]")
            .await
            .is_some_and(|doc| doc["value"] == 7)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn sql_mode_without_joins_materializes_publication_tables() {
    const INDEX: &str = "it_pg_direct_sql_orders";
    const SPEC: &str = "joins: []\n";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA direct;
         CREATE TABLE direct.orders(
           id text PRIMARY KEY,
           status text NOT NULL,
           total bigint NOT NULL
         );
         INSERT INTO direct.orders VALUES
           ('ord-1', 'created', 100),
           ('ord-2', 'paid', 200);
         CREATE PUBLICATION ventstream_direct FOR TABLE direct.orders;",
    )
    .await
    .expect("seed direct table");

    let dir = common::state_dir("direct-sql", stack.pg_port);
    let spec = common::write_spec(&dir, SPEC);
    let opts = common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "direct_sql_slot",
        publication: "ventstream_direct",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "sql",
    };
    let mut engine = common::spawn_pg_engine(&opts);

    common::wait_until(Duration::from_secs(60), "direct SQL bootstrap", || async {
        common::os_count(stack.os_port, INDEX).await == 2
    })
    .await;
    let id1 = r#"direct.orders:["ord-1"]"#;
    let id2 = r#"direct.orders:["ord-2"]"#;
    let id3 = r#"direct.orders:["ord-3"]"#;
    let doc = common::os_doc(stack.os_port, INDEX, id1)
        .await
        .expect("stable bootstrap document");
    assert_eq!(doc["status"], "created");
    assert!(doc.get("new").is_none(), "sink document must be a flat row");

    pg.batch_execute("UPDATE direct.orders SET status='shipped', total=150 WHERE id='ord-1';")
        .await
        .expect("update direct row");
    common::wait_until(Duration::from_secs(30), "direct SQL update", || async {
        common::os_doc(stack.os_port, INDEX, id1)
            .await
            .is_some_and(|doc| doc["status"] == "shipped" && doc["total"] == 150)
            && common::os_count(stack.os_port, INDEX).await == 2
    })
    .await;

    pg.batch_execute("INSERT INTO direct.orders VALUES ('ord-3', 'created', 300);")
        .await
        .expect("insert direct row");
    common::wait_until(Duration::from_secs(30), "direct SQL insert", || async {
        common::os_doc(stack.os_port, INDEX, id3).await.is_some()
            && common::os_count(stack.os_port, INDEX).await == 3
    })
    .await;

    pg.batch_execute("DELETE FROM direct.orders WHERE id='ord-2';")
        .await
        .expect("delete direct row");
    common::wait_until(Duration::from_secs(30), "direct SQL delete", || async {
        common::os_doc(stack.os_port, INDEX, id2).await.is_none()
            && common::os_count(stack.os_port, INDEX).await == 2
    })
    .await;

    engine.terminate();
    let _restarted = common::spawn_pg_engine(&opts);
    common::wait_until(
        Duration::from_secs(60),
        "restart preserves stable direct projection",
        || async {
            common::os_count(stack.os_port, INDEX).await == 2
                && common::os_doc(stack.os_port, INDEX, id1)
                    .await
                    .is_some_and(|doc| doc["status"] == "shipped")
                && common::os_doc(stack.os_port, INDEX, id2).await.is_none()
                && common::os_doc(stack.os_port, INDEX, id3).await.is_some()
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local benchmark: requires Docker; run with scripts/test-sources.sh postgres"]
async fn related_fetcher_batches_real_postgres_lookups() {
    const KEY_COUNT: usize = 1_024;
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA benchmark;
         CREATE TABLE benchmark.children(
           child_id bigint PRIMARY KEY,
           parent_id bigint NOT NULL,
           payload text NOT NULL
         );
         CREATE INDEX children_parent_id_idx ON benchmark.children(parent_id);
         INSERT INTO benchmark.children(child_id, parent_id, payload)
         SELECT parent_id * 4 + child_offset, parent_id, repeat('x', 128)
         FROM generate_series(1, 1024) parent_id
         CROSS JOIN generate_series(1, 4) child_offset;",
    )
    .await
    .expect("seed fetch benchmark");

    let mut source = PostgresCdcConfig::new(
        "bench-fetch",
        "127.0.0.1",
        common::PG_USER,
        common::PG_PASSWORD,
        common::PG_DB,
        "bench_pub",
        "bench_slot",
    );
    source.port = stack.pg_port;
    let fetcher = PostgresFetcher::connect_config_with_pool_size(source, 4)
        .await
        .expect("connect fetcher");
    let keys = (1..=KEY_COUNT)
        .map(|key| PkValue::from_single(&Value::from(key as u64)))
        .collect::<Vec<_>>();
    let columns = vec!["parent_id".to_owned()];
    let select = vec![
        "child_id".to_owned(),
        "parent_id".to_owned(),
        "payload".to_owned(),
    ];

    // Warm the catalog type cache and connection before either measurement.
    fetcher
        .fetch_many("benchmark.children", &columns, &keys[0], &select)
        .await
        .expect("warm fetcher");

    let sequential_started = Instant::now();
    let mut sequential_rows = 0;
    for key in &keys {
        sequential_rows += fetcher
            .fetch_many("benchmark.children", &columns, key, &select)
            .await
            .expect("sequential fetch")
            .len();
    }
    let sequential_elapsed = sequential_started.elapsed();

    let batch_started = Instant::now();
    let batch = fetcher
        .fetch_many_batch("benchmark.children", &columns, &keys, &select)
        .await
        .expect("batch fetch");
    let batch_elapsed = batch_started.elapsed();
    let batch_rows = batch.iter().map(|(_, rows)| rows.len()).sum::<usize>();

    assert_eq!(sequential_rows, KEY_COUNT * 4);
    assert_eq!(batch_rows, sequential_rows);
    assert_eq!(batch.len(), KEY_COUNT);
    println!(
        "postgres related fetch benchmark: keys={KEY_COUNT} rows={batch_rows} sequential_ms={} batch_ms={} speedup={:.2}x",
        sequential_elapsed.as_millis(),
        batch_elapsed.as_millis(),
        sequential_elapsed.as_secs_f64() / batch_elapsed.as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn bootstrap_projects_order_with_embedded_children() {
    const INDEX: &str = "it_pg_bootstrap_orders";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SEED).await.expect("seed");

    let dir = common::state_dir("smoke", stack.pg_port);
    let spec = common::write_spec(&dir, ORDERS_SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "smoke_slot",
        publication: "ventstream_shop",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "",
    });

    common::wait_until(Duration::from_secs(60), "order doc to appear", || async {
        common::os_count(stack.os_port, INDEX).await == 1
    })
    .await;

    let doc = common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
        .await
        .expect("ord-1 doc");
    assert_eq!(doc["customer"]["name"], "Ada");
    let items: Vec<String> = doc["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|i| i["item_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(items, vec!["i1", "i2"]);
}

/// Guards commit 7f9e5d5: deleting a child row under the DEFAULT replica
/// identity (PK-only old tuple) must still drop it from the parent doc —
/// the engine recovers the FK from its join state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn child_row_delete_propagates_on_default_replica_identity() {
    const INDEX: &str = "it_pg_child_delete_orders";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SEED).await.expect("seed");
    // SEED leaves order_items at the Postgres default replica identity.

    let dir = common::state_dir("childdel", stack.pg_port);
    let spec = common::write_spec(&dir, ORDERS_SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "childdel_slot",
        publication: "ventstream_shop",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "",
    });
    common::wait_until(Duration::from_secs(60), "bootstrap", || async {
        common::os_count(stack.os_port, INDEX).await == 1
    })
    .await;

    // Delete ONE line item; the order stays.
    pg.execute("DELETE FROM shop.order_items WHERE item_id='i1'", &[])
        .await
        .expect("delete item");

    common::wait_until(Duration::from_secs(30), "i1 to leave the doc", || async {
        match common::os_doc(stack.os_port, INDEX, &doc_id("ord-1")).await {
            Some(d) => {
                let ids: Vec<String> = d["items"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|i| i["item_id"].as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                ids == vec!["i2".to_string()]
            }
            None => false,
        }
    })
    .await;
}

const TAGS_SPEC: &str = r#"
joins:
  - name: order_tags
    primary:
      table: shop.order_tags
      pk: [order_id, tag]
    related: []
    state:
      backend: memory
    backfill:
      mode: sync_on_miss
"#;

const TAGS_SEED: &str = r#"
CREATE SCHEMA shop;
CREATE TABLE shop.order_tags(
  order_id text NOT NULL,
  tag text NOT NULL,
  added_by text NOT NULL DEFAULT 'system',
  PRIMARY KEY (order_id, tag)
);
INSERT INTO shop.order_tags(order_id, tag) VALUES
  ('ord-1','urgent'), ('ord-1','gift'), ('ord-2','fragile'), ('ord-2','rush');
CREATE PUBLICATION ventstream_order_tags FOR TABLE shop.order_tags;
"#;

/// Guards commit e6309c7: a composite-PK primary table bootstraps with
/// composite doc-ids, and the orphan-reconcile pass sweeps composite-keyed
/// docs whose rows are gone (the previously-skipped case).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn composite_pk_bootstraps_and_reconciles_orphans() {
    const INDEX: &str = "it_pg_composite_order_tags";
    use ventstream_sinks::opensearch::{
        encode_pk_key, reconcile_orphans, DocIdFormat, OpenSearchConfig,
    };

    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(TAGS_SEED).await.expect("seed");

    let dir = common::state_dir("tags", stack.pg_port);
    let spec = common::write_spec(&dir, TAGS_SPEC);
    let mut engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "tags_slot",
        publication: "ventstream_order_tags",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "",
    });
    common::wait_until(Duration::from_secs(60), "composite bootstrap", || async {
        common::os_count(stack.os_port, INDEX).await == 4
    })
    .await;

    // Composite doc-ids are JSON arrays of the PK components.
    assert!(
        common::os_doc(
            stack.os_port,
            INDEX,
            "shop.order_tags:[\"ord-1\",\"urgent\"]",
        )
        .await
        .is_some(),
        "composite doc id must exist"
    );

    // Stop the engine, THEN delete — this is the drained-window case the
    // reconcile pass exists for (live CDC never sees the delete, so the
    // doc is left orphaned and only reconcile can clean it).
    engine.kill();
    pg.execute(
        "DELETE FROM shop.order_tags WHERE order_id='ord-1' AND tag='urgent'",
        &[],
    )
    .await
    .expect("delete composite row");

    // Build the live PK set from PG, keyed canonically (== pg_table_pk_keys).
    let rows = pg
        .query("SELECT order_id::text, tag::text FROM shop.order_tags", &[])
        .await
        .expect("list pks");
    let mut valid = std::collections::HashSet::new();
    for r in &rows {
        let c0: String = r.get(0);
        let c1: String = r.get(1);
        if let Some(k) = encode_pk_key(&[c0, c1]) {
            valid.insert(k);
        }
    }
    assert_eq!(valid.len(), 3, "3 composite rows remain in PG");

    let os = OpenSearchConfig::new("os", common::os_url(stack.os_port), INDEX.to_string());
    let deleted = reconcile_orphans(
        &os,
        INDEX,
        "shop.order_tags:",
        &valid,
        DocIdFormat::JsonArray,
    )
    .await
    .expect("reconcile");
    assert_eq!(
        deleted, 1,
        "exactly the one orphaned composite doc is swept"
    );

    // Orphan gone, a surviving composite doc kept.
    assert!(
        common::os_doc(
            stack.os_port,
            INDEX,
            "shop.order_tags:[\"ord-1\",\"urgent\"]",
        )
        .await
        .is_none(),
        "orphan composite doc must be deleted"
    );
    assert!(
        common::os_doc(
            stack.os_port,
            INDEX,
            "shop.order_tags:[\"ord-2\",\"fragile\"]",
        )
        .await
        .is_some(),
        "valid composite doc must survive"
    );

    // Idempotent: a second pass deletes nothing. Refresh first so the
    // scroll sees the delete from the first pass (search is near-real-time).
    common::os_count(stack.os_port, INDEX).await;
    let again = reconcile_orphans(
        &os,
        INDEX,
        "shop.order_tags:",
        &valid,
        DocIdFormat::JsonArray,
    )
    .await
    .expect("reconcile again");
    assert_eq!(again, 0, "reconcile is idempotent");
}

/// Crash recovery: kill the engine, apply create/update/child-delete/
/// primary-delete while it's down, restart. The slot replays the missed
/// WAL and redb reloads the join state, so every offline change lands —
/// including the child delete (FK recovered from reloaded state). Guards
/// the lifecycle path the upcoming runner refactor will touch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn crash_recovery_replays_offline_create_update_delete() {
    const INDEX: &str = "it_pg_crash_recovery_orders";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SEED).await.expect("seed"); // ord-1 (i1,i2)
    pg.batch_execute(
        "INSERT INTO shop.orders VALUES ('ord-2','c1','open',60);
         INSERT INTO shop.order_items VALUES ('i3','ord-2','SKU-C',1,20),('i4','ord-2','SKU-D',1,30);",
    )
    .await
    .expect("seed ord-2");

    let dir = common::state_dir("recovery", stack.pg_port);
    let spec = common::write_spec(&dir, ORDERS_SPEC);
    let opts = common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "recovery_slot",
        publication: "ventstream_shop",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "",
    };

    let mut engine = common::spawn_pg_engine(&opts);
    common::wait_until(Duration::from_secs(60), "initial bootstrap", || async {
        common::os_count(stack.os_port, INDEX).await == 2
    })
    .await;

    // Crash.
    engine.kill();

    // Offline: create, update-primary, child-delete, primary-delete.
    pg.batch_execute(
        "INSERT INTO shop.orders VALUES ('ord-3','c1','new',10);
         INSERT INTO shop.order_items VALUES ('i5','ord-3','SKU-E',1,5);
         UPDATE shop.orders SET status='recovered' WHERE order_id='ord-1';
         DELETE FROM shop.order_items WHERE item_id='i1';
         DELETE FROM shop.order_items WHERE order_id='ord-2';
         DELETE FROM shop.orders WHERE order_id='ord-2';",
    )
    .await
    .expect("offline mutations");

    // Restart with the SAME slot + state dir.
    let engine2 = common::spawn_pg_engine(&opts);

    // CREATE replayed.
    common::wait_until(Duration::from_secs(30), "ord-3 to appear", || async {
        common::os_doc(stack.os_port, INDEX, &doc_id("ord-3"))
            .await
            .is_some()
    })
    .await;
    // PRIMARY delete replayed (tombstone).
    common::wait_until(Duration::from_secs(30), "ord-2 to tombstone", || async {
        common::os_doc(stack.os_port, INDEX, &doc_id("ord-2"))
            .await
            .is_none()
    })
    .await;
    // UPDATE + CHILD delete replayed on ord-1.
    common::wait_until(
        Duration::from_secs(30),
        "ord-1 update+child-del",
        || async {
            match common::os_doc(stack.os_port, INDEX, &doc_id("ord-1")).await {
                Some(d) => {
                    let items: Vec<String> = d["items"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .map(|i| i["item_id"].as_str().unwrap_or("").to_string())
                                .collect()
                        })
                        .unwrap_or_default();
                    d["status"] == "recovered" && items == vec!["i2".to_string()]
                }
                None => false,
            }
        },
    )
    .await;

    // It RESUMED from the slot — did not re-bootstrap from scratch.
    let log = engine2.log();
    assert!(
        log.contains("snapshot bootstrap skipped") || log.contains("persistent state replayed"),
        "restart must resume from the slot/state, not re-bootstrap; log was:\n{log}"
    );
}

/// Resilience: the engine reconnects in-process when its replication
/// connection is killed mid-stream — the "LB kills the connection"
/// scenario. Postgres stays up (the slot persists); we terminate the
/// engine's walsender backend from another session, then write a new
/// row. The reconnect supervisor must re-attach to the slot and deliver
/// it, with NO process restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn engine_reconnects_after_replication_connection_killed() {
    const INDEX: &str = "it_pg_reconnect_orders";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SEED).await.expect("seed");

    let dir = common::state_dir("reconnect", stack.pg_port);
    let spec = common::write_spec(&dir, ORDERS_SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "reconnect_slot",
        publication: "ventstream_shop",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "",
    });
    common::wait_until(Duration::from_secs(60), "initial bootstrap", || async {
        common::os_count(stack.os_port, INDEX).await == 1
    })
    .await;

    // Kill the engine's replication connection from another session —
    // exactly what an LB / proxy does to an idle connection. Postgres
    // stays up and the slot persists, so recovery is in-process.
    let killed = pg
        .query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'walsender'",
            &[],
        )
        .await
        .expect("terminate walsender");
    assert!(
        !killed.is_empty(),
        "expected the engine's walsender (replication conn) to be present and terminated"
    );

    // A row written after the kill can only reach OS if the engine
    // noticed the drop, reconnected to the slot, and resumed tailing.
    pg.batch_execute("INSERT INTO shop.orders VALUES ('ord-after-kill','c1','open',88);")
        .await
        .expect("post-kill insert");

    common::wait_until(
        Duration::from_secs(60),
        "post-kill row to propagate (engine reconnected to the slot)",
        || async {
            common::os_doc(stack.os_port, INDEX, &doc_id("ord-after-kill"))
                .await
                .is_some()
        },
    )
    .await;
}

/// Same `orders` projection, executed in bounded SQL-denormalize mode
/// (`VS_PG_DENORMALIZE_MODE=sql`): bootstrap via a keyset-chunked SQL join,
/// tail recompose per change. The doc id matches the in-memory mode, so the
/// shared `doc_id` helper applies. Covers bootstrap + 1:many child fan-out +
/// many:1 foreign fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh postgres"]
async fn sql_mode_bootstraps_and_recomposes_on_cdc() {
    const INDEX: &str = "it_pg_sql_mode_orders";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SEED).await.expect("seed");

    let dir = common::state_dir("sqlmode", stack.pg_port);
    let spec = common::write_spec(&dir, ORDERS_SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "sqlmode_slot",
        publication: "ventstream_shop",
        spec_path: &spec,
        state_dir: &dir,
        index_template: "${header:ventstream.target.index}",
        denormalize_mode: "sql",
    });

    // Bootstrap: the order doc appears, same doc id + shape as memory mode.
    common::wait_until(Duration::from_secs(60), "sql bootstrap doc", || async {
        common::os_count(stack.os_port, INDEX).await == 1
    })
    .await;
    let doc = common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
        .await
        .expect("ord-1 doc");
    assert_eq!(doc["customer"]["name"], "Ada");
    let items: Vec<String> = doc["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|i| i["item_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(items, vec!["i1", "i2"]);

    // 1:many child change → recompose adds the new item.
    pg.batch_execute("INSERT INTO shop.order_items VALUES ('i3','ord-1','SKU-C',2,5);")
        .await
        .expect("insert item");
    common::wait_until(Duration::from_secs(30), "child recompose", || async {
        common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
            .await
            .and_then(|d| d["items"].as_array().map(|a| a.len()))
            == Some(3)
    })
    .await;

    // many:1 foreign change → recompose updates the embedded customer.
    pg.batch_execute("UPDATE shop.customers SET name='Ada Lovelace' WHERE customer_id='c1';")
        .await
        .expect("update customer");
    common::wait_until(Duration::from_secs(30), "foreign recompose", || async {
        common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
            .await
            .map(|d| d["customer"]["name"] == "Ada Lovelace")
            == Some(true)
    })
    .await;

    // DEFAULT replica identity exposes only the deleted child PK. SQL mode
    // must render the projection target on its reverse-lookup probe before it
    // can discover and recompose the owning order.
    pg.batch_execute("DELETE FROM shop.order_items WHERE item_id='i1';")
        .await
        .expect("delete child item");
    common::wait_until(
        Duration::from_secs(30),
        "child delete recompose",
        || async {
            common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
                .await
                .and_then(|doc| {
                    doc["items"].as_array().map(|items| {
                        items
                            .iter()
                            .all(|item| item["item_id"].as_str() != Some("i1"))
                    })
                })
                == Some(true)
        },
    )
    .await;

    // A primary delete emits a tombstone. It must carry the same projection
    // target or the dispatcher cannot delete the document from its index.
    pg.batch_execute(
        "DELETE FROM shop.order_items WHERE order_id='ord-1';
         DELETE FROM shop.orders WHERE order_id='ord-1';",
    )
    .await
    .expect("delete order and remaining children");
    common::wait_until(Duration::from_secs(30), "projection tombstone", || async {
        common::os_doc(stack.os_port, INDEX, &doc_id("ord-1"))
            .await
            .is_none()
    })
    .await;
}
