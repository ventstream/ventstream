//! MySQL CDC integration test: row binlog, SQL denormalization, persisted
//! position restart, and a real OpenSearch sink.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::time::Duration;

use mysql_async::prelude::Queryable;
use redis::AsyncCommands;

const INDEX: &str = "it_mysql_orders";
const COMPOSITE_INDEX: &str = "it_mysql_composite_items";
const MEMORY_INDEX: &str = "it_mysql_memory_orders";
const DOC_ID: &str = r#"shop.orders:["ord-1"]"#;
const BADGES_INDEX: &str = "it_mysql_badges";
const BADGES_SPEC: &str = r#"
joins:
  - name: badges
    primary:
      table: shop.badges
      pk: [kind, owner]
    target:
      index: it_mysql_badges
    related: []
    state:
      backend: memory
"#;

const ORDERS_SPEC: &str = r#"
joins:
  - name: orders
    primary:
      table: shop.orders
      pk: id
    target:
      index: it_mysql_orders
    related: []
    state:
      backend: memory
  - name: composite_items
    primary:
      table: shop.composite_items
      pk: [tenant_id, item_id]
    target:
      index: it_mysql_composite_items
    related: []
    state:
      backend: memory
"#;
const MEMORY_ORDERS_SPEC: &str = r#"
joins:
  - name: memory_orders
    primary:
      table: shop.orders
      pk: id
    target:
      index: it_mysql_memory_orders
    related:
      - id: customer
        table: shop.customers
        pk: id
        join_on: { from: customer_id, to: id }
        embed_as: customer
        cardinality: one
        select: [id, name]
    state:
      backend: memory
    backfill:
      mode: sync_on_miss
"#;

const REDIS_ORDERS_SPEC: &str = r#"
joins:
  - name: redis_orders
    primary:
      table: shop.orders
      pk: id
    target:
      index: redis_orders
    related: []
    state:
      backend: memory
"#;

const REDIS_JOINED_ORDERS_SPEC: &str = r#"
joins:
  - name: redis_joined_orders
    primary:
      table: shop.orders
      pk: id
    target:
      index: redis_joined_orders
    related:
      - id: items
        table: shop.order_items
        pk: id
        join_on: { from: id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: id
    state:
      backend: memory
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mysql_sql_mode_materializes_updates_and_deletes_in_redis() {
    const PREFIX: &str = "ventstream:it:mysql";

    let stack = common::start_mysql().await;
    let (_redis, redis_port) = common::start_redis().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.orders (id VARCHAR(64) PRIMARY KEY, status VARCHAR(64) NOT NULL, total INT NOT NULL); \
             INSERT INTO {}.orders (id, status, total) VALUES ('redis-1', 'created', 10) \
             ON DUPLICATE KEY UPDATE status=VALUES(status), total=VALUES(total)",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed mysql");

    let state_dir = common::state_dir("mysql-redis", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, REDIS_ORDERS_SPEC);
    let mut engine = common::spawn_mysql_redis_engine(
        stack.mysql_port,
        redis_port,
        &spec_path,
        &state_dir,
        PREFIX,
    );
    let pattern = format!("{PREFIX}:{{orders}}:*");
    common::wait_until(Duration::from_secs(180), "MySQL Redis snapshot", || {
        let pattern = pattern.clone();
        async move {
            let Ok(keys) = common::redis_keys(redis_port, &pattern).await else {
                return false;
            };
            let Some(key) = keys.first() else {
                return false;
            };
            common::redis_value(redis_port, key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                .is_some_and(|document| document["status"] == "created")
        }
    })
    .await;
    let keys = common::redis_keys(redis_port, &pattern)
        .await
        .expect("MySQL Redis keys");
    assert_eq!(keys.len(), 1);
    let key = keys.first().expect("MySQL Redis key").clone();

    mysql
        .query_drop("UPDATE shop.orders SET status='live-updated' WHERE id='redis-1'")
        .await
        .expect("update mysql order");
    common::wait_until(Duration::from_secs(30), "MySQL Redis update", || async {
        common::redis_value(redis_port, &key)
            .await
            .ok()
            .flatten()
            .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
            .is_some_and(|document| document["status"] == "live-updated")
    })
    .await;

    let binlog_connection: Option<u64> = mysql
        .exec_first(
            "SELECT ID FROM information_schema.PROCESSLIST \
             WHERE USER = ? AND COMMAND LIKE 'Binlog Dump%' \
             ORDER BY ID LIMIT 1",
            (common::MYSQL_USER,),
        )
        .await
        .expect("find MySQL binlog connection");
    let binlog_connection = binlog_connection.expect("active MySQL binlog connection");
    mysql
        .query_drop(format!("KILL CONNECTION {binlog_connection}"))
        .await
        .expect("kill MySQL binlog connection");
    mysql
        .query_drop("UPDATE shop.orders SET status='after-binlog-reconnect' WHERE id='redis-1'")
        .await
        .expect("update mysql after binlog disconnect");
    common::wait_until(
        Duration::from_secs(45),
        "MySQL Redis binlog reconnect",
        || async {
            common::redis_value(redis_port, &key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                .is_some_and(|document| document["status"] == "after-binlog-reconnect")
        },
    )
    .await;
    assert!(
        engine
            .log()
            .contains("reconnecting from sink-confirmed cursor"),
        "MySQL source did not report the exercised reconnect path"
    );

    engine.kill();
    mysql
        .query_drop("UPDATE shop.orders SET status='updated-while-stopped' WHERE id='redis-1'")
        .await
        .expect("update mysql while stopped");
    let mut restarted = common::spawn_mysql_redis_engine(
        stack.mysql_port,
        redis_port,
        &spec_path,
        &state_dir,
        PREFIX,
    );
    common::wait_until(Duration::from_secs(45), "MySQL Redis restart", || async {
        common::redis_value(redis_port, &key)
            .await
            .ok()
            .flatten()
            .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
            .is_some_and(|document| document["status"] == "updated-while-stopped")
    })
    .await;

    let cursor_path = format!("{state_dir}/mysql_binlog_pos");
    common::wait_until(Duration::from_secs(10), "MySQL binlog position", || async {
        std::fs::metadata(&cursor_path).is_ok()
    })
    .await;
    let owned_pattern = format!("{PREFIX}:{{orders}}:*");
    let stale_key = format!("{PREFIX}:{{orders}}:stale");
    let neighboring_key = format!("{PREFIX}:{{inventory}}:keep");
    let mut redis = common::redis_connection(redis_port)
        .await
        .expect("connect to Redis");
    redis
        .set::<_, _, ()>(&stale_key, br#"{"stale":true}"#)
        .await
        .expect("seed stale MySQL target key");
    redis
        .set::<_, _, ()>(&neighboring_key, br#"{"keep":true}"#)
        .await
        .expect("seed neighboring Redis target key");
    restarted.terminate();

    let shared_output = common::mysql_redis_engine_command(&common::MySqlRedisEngine {
        mysql_port: stack.mysql_port,
        redis_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        key_prefix: PREFIX,
        tables: "orders",
        keyspace_ownership: "shared",
    })
    .arg("--fleet-rebootstrap")
    .output()
    .expect("run shared MySQL rebootstrap");
    assert!(
        !shared_output.status.success(),
        "shared MySQL keyspace rebootstrap must be refused"
    );
    assert!(
        std::fs::metadata(&cursor_path).is_ok(),
        "ownership validation must preserve the MySQL binlog position"
    );
    assert_eq!(
        common::redis_keys(redis_port, &owned_pattern)
            .await
            .expect("read MySQL target after refused rebootstrap")
            .len(),
        2,
        "refused rebootstrap must preserve materialized and stale keys"
    );

    let exclusive_output = common::mysql_redis_engine_command(&common::MySqlRedisEngine {
        mysql_port: stack.mysql_port,
        redis_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        key_prefix: PREFIX,
        tables: "orders",
        keyspace_ownership: "exclusive",
    })
    .arg("--fleet-rebootstrap")
    .output()
    .expect("run exclusive MySQL rebootstrap");
    assert!(
        exclusive_output.status.success(),
        "exclusive MySQL rebootstrap failed: {}",
        String::from_utf8_lossy(&exclusive_output.stderr)
    );
    assert!(
        std::fs::metadata(&cursor_path).is_err(),
        "MySQL rebootstrap must remove the binlog position"
    );
    assert!(
        common::redis_keys(redis_port, &owned_pattern)
            .await
            .expect("read MySQL target after rebootstrap")
            .is_empty(),
        "MySQL rebootstrap must clear the exclusively owned target"
    );
    assert!(
        common::redis_value(redis_port, &neighboring_key)
            .await
            .expect("read neighboring Redis target")
            .is_some(),
        "MySQL rebootstrap must preserve neighboring targets"
    );

    let _rebuilt = common::spawn_mysql_redis_engine_with_options(&common::MySqlRedisEngine {
        mysql_port: stack.mysql_port,
        redis_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        key_prefix: PREFIX,
        tables: "orders",
        keyspace_ownership: "exclusive",
    });
    common::wait_until(
        Duration::from_secs(180),
        "MySQL Redis live set after rebootstrap",
        || async {
            common::redis_keys(redis_port, &owned_pattern)
                .await
                .is_ok_and(|keys| keys.len() == 1 && !keys.contains(&stale_key))
        },
    )
    .await;

    mysql
        .query_drop("DELETE FROM shop.orders WHERE id='redis-1'")
        .await
        .expect("delete mysql order");
    common::wait_until(Duration::from_secs(30), "MySQL Redis delete", || async {
        common::redis_value(redis_port, &key).await == Ok(None)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mysql_sql_mode_recomposes_join_transitions_in_redis() {
    const PREFIX: &str = "ventstream:it:mysql:joins";

    let stack = common::start_mysql().await;
    let (_redis, redis_port) = common::start_redis().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.orders (
               id VARCHAR(64) PRIMARY KEY,
               status VARCHAR(64) NOT NULL
             ); \
             CREATE TABLE IF NOT EXISTS {}.order_items (
               id VARCHAR(64) PRIMARY KEY,
               order_id VARCHAR(64) NOT NULL,
               product VARCHAR(64) NOT NULL
             ); \
             DELETE FROM {}.order_items; \
             DELETE FROM {}.orders; \
             INSERT INTO {}.orders (id, status) VALUES
               ('join-order-1', 'created'),
               ('join-order-2', 'created'),
               ('pk-old', 'created'); \
             INSERT INTO {}.order_items (id, order_id, product) VALUES
               ('item-1', 'join-order-1', 'book'),
               ('item-2', 'join-order-1', 'pen')",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed joined mysql tables");

    let state_dir = common::state_dir("mysql-redis-joins", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, REDIS_JOINED_ORDERS_SPEC);
    let mut engine = common::spawn_mysql_redis_engine_with_tables(
        stack.mysql_port,
        redis_port,
        &spec_path,
        &state_dir,
        PREFIX,
        "orders,order_items",
    );
    let pattern = format!("{PREFIX}:{{orders}}:*");

    common::wait_until(
        Duration::from_secs(60),
        "MySQL Redis joined snapshot",
        || {
            let pattern = pattern.clone();
            async move {
                redis_document(redis_port, &pattern, "join-order-1")
                    .await
                    .is_some_and(|document| {
                        item_ids(&document) == vec!["item-1".to_owned(), "item-2".to_owned()]
                    })
            }
        },
    )
    .await;

    mysql
        .query_drop("DELETE FROM shop.order_items WHERE id='item-1'")
        .await
        .expect("delete joined child");
    common::wait_until(Duration::from_secs(30), "MySQL Redis child delete", || {
        let pattern = pattern.clone();
        async move {
            redis_document(redis_port, &pattern, "join-order-1")
                .await
                .is_some_and(|document| item_ids(&document) == vec!["item-2".to_owned()])
        }
    })
    .await;

    mysql
        .query_drop("UPDATE shop.order_items SET order_id='join-order-2' WHERE id='item-2'")
        .await
        .expect("reparent joined child");
    common::wait_until(
        Duration::from_secs(30),
        "MySQL Redis child reparent",
        || {
            let pattern = pattern.clone();
            async move {
                let old_parent = redis_document(redis_port, &pattern, "join-order-1").await;
                let new_parent = redis_document(redis_port, &pattern, "join-order-2").await;
                old_parent.is_some_and(|document| item_ids(&document).is_empty())
                    && new_parent
                        .is_some_and(|document| item_ids(&document) == vec!["item-2".to_owned()])
            }
        },
    )
    .await;

    mysql
        .query_drop("UPDATE shop.orders SET id='pk-new' WHERE id='pk-old'")
        .await
        .expect("update primary key");
    common::wait_until(
        Duration::from_secs(30),
        "MySQL Redis primary-key transition",
        || {
            let pattern = pattern.clone();
            async move {
                redis_document(redis_port, &pattern, "pk-old")
                    .await
                    .is_none()
                    && redis_document(redis_port, &pattern, "pk-new")
                        .await
                        .is_some()
            }
        },
    )
    .await;

    engine.kill();
    mysql
        .query_drop(
            "INSERT INTO shop.order_items (id, order_id, product) \
             VALUES ('item-3', 'join-order-2', 'pencil')",
        )
        .await
        .expect("insert joined child while stopped");
    let _restarted = common::spawn_mysql_redis_engine_with_tables(
        stack.mysql_port,
        redis_port,
        &spec_path,
        &state_dir,
        PREFIX,
        "orders,order_items",
    );
    common::wait_until(
        Duration::from_secs(45),
        "MySQL Redis joined restart",
        || {
            let pattern = pattern.clone();
            async move {
                redis_document(redis_port, &pattern, "join-order-2")
                    .await
                    .is_some_and(|document| {
                        item_ids(&document) == vec!["item-2".to_owned(), "item-3".to_owned()]
                    })
            }
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mysql_sql_mode_projection_materializes_declarative_redis_views() {
    const PREFIX: &str = "ventstream:it:mysql:views";
    const TARGET: &str = "mysql_orders_projection";
    const SPEC: &str = r#"
joins:
  - name: mysql_orders_projection
    primary:
      table: shop.orders
      pk: id
    target:
      index: mysql_orders_projection
    related:
      - id: items
        table: shop.order_items
        pk: id
        join_on: { from: id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: id
        select: [id, product]
"#;

    let stack = common::start_mysql().await;
    let (_redis, redis_port) = common::start_redis().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.orders (
               id VARCHAR(64) PRIMARY KEY,
               customer_id VARCHAR(64) NOT NULL,
               status VARCHAR(64) NOT NULL
             ); \
             CREATE TABLE IF NOT EXISTS {}.order_items (
               id VARCHAR(64) PRIMARY KEY,
               order_id VARCHAR(64) NOT NULL,
               product VARCHAR(64) NOT NULL
             ); \
             DELETE FROM {}.order_items; \
             DELETE FROM {}.orders; \
             INSERT INTO {}.orders (id, customer_id, status)
               VALUES ('view-order-1', 'customer-1', 'open'); \
             INSERT INTO {}.order_items (id, order_id, product)
               VALUES ('view-item-1', 'view-order-1', 'book')",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed MySQL Redis views source");

    let state_dir = common::state_dir("mysql-redis-views", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, SPEC);
    let config_path = format!("{state_dir}/ventstream.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mysql
  mysql:
    host: 127.0.0.1
    port: {mysql_port}
    user: {mysql_user}
    password_ref: env:VS_MYSQL_PASSWORD
    database: {mysql_database}
    namespace: {mysql_database}
    server_id: 4000000102
    state_dir: {state_dir}
    tables: [orders, order_items]
    bootstrap:
      mode: snapshot
      chunk_size: 100
    pos_flush_ms: 100
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
    contract:
      mode: materialized_view
runtime:
  dlq_path: {state_dir}/dlq.jsonl
  joins:
    state_dir: {state_dir}
"#,
            mysql_port = stack.mysql_port,
            mysql_user = common::MYSQL_USER,
            mysql_database = common::MYSQL_DB,
        ),
    )
    .expect("write MySQL Redis view config");

    let _engine = common::spawn_engine_with_config(
        &state_dir,
        &config_path,
        &[
            ("VS_MYSQL_PASSWORD", common::MYSQL_PASSWORD.to_owned()),
            ("VS_REDIS_SINK_URL", common::redis_url(redis_port)),
        ],
    );
    let first_key = format!(
        "{PREFIX}:{{active_order_by_customer}}:customer%3Acustomer-1%3Aorder%3Aview-order-1"
    );
    let second_key = format!(
        "{PREFIX}:{{active_order_by_customer}}:customer%3Acustomer-2%3Aorder%3Aview-order-1"
    );

    common::wait_until(
        Duration::from_secs(60),
        "MySQL Redis view snapshot",
        || async {
            common::redis_value(redis_port, &first_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                .is_some_and(|document| {
                    document["status"] == "open"
                        && document["items"]
                            .as_array()
                            .is_some_and(|items| items.len() == 1)
                })
        },
    )
    .await;

    mysql
        .query_drop(
            "INSERT INTO shop.order_items (id, order_id, product)
             VALUES ('view-item-2', 'view-order-1', 'pen')",
        )
        .await
        .expect("insert MySQL related row");
    common::wait_until(
        Duration::from_secs(30),
        "MySQL Redis view recomposition",
        || async {
            common::redis_value(redis_port, &first_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                .is_some_and(|document| {
                    document["items"]
                        .as_array()
                        .is_some_and(|items| items.len() == 2)
                })
        },
    )
    .await;

    mysql
        .query_drop(
            "UPDATE shop.orders
             SET customer_id='customer-2'
             WHERE id='view-order-1'",
        )
        .await
        .expect("move MySQL Redis view");
    common::wait_until(Duration::from_secs(30), "MySQL Redis view move", || async {
        common::redis_value(redis_port, &first_key).await == Ok(None)
            && common::redis_value(redis_port, &second_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                .is_some_and(|document| document["customer_id"] == "customer-2")
    })
    .await;

    mysql
        .query_drop("UPDATE shop.orders SET status='cancelled' WHERE id='view-order-1'")
        .await
        .expect("filter MySQL Redis view");
    common::wait_until(
        Duration::from_secs(30),
        "MySQL Redis view filter transition",
        || async { common::redis_value(redis_port, &second_key).await == Ok(None) },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mysql_sql_mode_rejects_incomplete_join_images() {
    const PREFIX: &str = "ventstream:it:mysql:minimal";

    let stack = common::start_mysql_with_row_image("MINIMAL").await;
    let (_redis, redis_port) = common::start_redis().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.orders (
               id VARCHAR(64) PRIMARY KEY,
               status VARCHAR(64) NOT NULL
             ); \
             CREATE TABLE IF NOT EXISTS {}.order_items (
               id VARCHAR(64) PRIMARY KEY,
               order_id VARCHAR(64) NOT NULL,
               product VARCHAR(64) NOT NULL
             )",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("prepare minimal-image mysql tables");

    let state_dir = common::state_dir("mysql-redis-minimal", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, REDIS_JOINED_ORDERS_SPEC);
    let engine = common::spawn_mysql_redis_engine_with_tables(
        stack.mysql_port,
        redis_port,
        &spec_path,
        &state_dir,
        PREFIX,
        "orders,order_items",
    );

    common::wait_until(
        Duration::from_secs(20),
        "MySQL incomplete row-image rejection",
        || {
            let log = engine.log();
            async move {
                log.contains("binlog_row_image=FULL") && log.contains("server reports MINIMAL")
            }
        },
    )
    .await;
}

async fn redis_document(redis_port: u16, pattern: &str, id: &str) -> Option<serde_json::Value> {
    let keys = common::redis_keys(redis_port, pattern).await.ok()?;
    for key in keys {
        let value = common::redis_value(redis_port, &key).await.ok().flatten()?;
        let document = serde_json::from_slice::<serde_json::Value>(&value).ok()?;
        if document.get("id").and_then(serde_json::Value::as_str) == Some(id) {
            return Some(document);
        }
    }
    None
}

fn item_ids(document: &serde_json::Value) -> Vec<String> {
    document
        .get("items")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("id").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh mysql"]
async fn mysql_sql_bootstrap_live_change_and_binlog_restart() {
    let stack = common::start_mysql_os().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.orders (id VARCHAR(64) PRIMARY KEY, status VARCHAR(64) NOT NULL, total INT NOT NULL); \
             INSERT INTO {}.orders (id, status, total) VALUES ('ord-1', 'created', 10) \
             ON DUPLICATE KEY UPDATE status=VALUES(status), total=VALUES(total); \
             CREATE TABLE IF NOT EXISTS {}.composite_items (tenant_id VARCHAR(64) NOT NULL, item_id INT NOT NULL, status VARCHAR(64) NOT NULL, PRIMARY KEY (tenant_id, item_id)); \
             INSERT INTO {}.composite_items (tenant_id, item_id, status) \
             WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 300) \
             SELECT 'tenant-a', n, 'created' FROM seq \
             ON DUPLICATE KEY UPDATE status=VALUES(status)",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed mysql");

    let state_dir = common::state_dir("mysql", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, ORDERS_SPEC);
    let options = common::MySqlEngine {
        mysql_port: stack.mysql_port,
        os_port: stack.os_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        index_template: "${header:ventstream.target.index}",
        denormalize_mode: "sql",
        bootstrap_mode: "snapshot",
    };
    let mut engine = common::spawn_mysql_engine(&options);

    common::wait_until(
        Duration::from_secs(60),
        "mysql SQL snapshot document",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "created")
        },
    )
    .await;

    common::wait_until(
        Duration::from_secs(60),
        "mysql composite SQL snapshot documents",
        || async { common::os_count(stack.os_port, COMPOSITE_INDEX).await == 300 },
    )
    .await;

    let recompose_started = std::time::Instant::now();
    mysql
        .query_drop("UPDATE shop.composite_items SET status='batch-updated'")
        .await
        .expect("batch update composite mysql rows");
    common::wait_until(
        Duration::from_secs(45),
        "mysql composite batch recomposition",
        || async {
            common::os_term_count(
                stack.os_port,
                COMPOSITE_INDEX,
                "status.keyword",
                "batch-updated",
            )
            .await
                == 300
        },
    )
    .await;
    println!(
        "mysql_composite_recompose_300_elapsed_ms={}",
        recompose_started.elapsed().as_millis()
    );

    mysql
        .query_drop("UPDATE shop.orders SET status='live-updated' WHERE id='ord-1'")
        .await
        .expect("update mysql order");
    common::wait_until(Duration::from_secs(30), "mysql live update", || async {
        common::os_doc(stack.os_port, INDEX, DOC_ID)
            .await
            .is_some_and(|doc| doc["status"] == "live-updated")
    })
    .await;

    // A transient server restart must reconnect in-process from the last
    // sink-confirmed binlog position. This catches the old behavior where EOF
    // looked like a graceful source completion and left the agent alive but
    // permanently idle.
    drop(mysql);
    stack
        .mysql
        .stop_with_timeout(Some(30))
        .await
        .expect("stop mysql for reconnect drill");
    assert!(
        !stack
            .mysql
            .is_running()
            .await
            .expect("inspect stopped mysql"),
        "mysql should be stopped before the reconnect drill continues"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    stack.mysql.start().await.expect("restart mysql");
    assert!(
        stack
            .mysql
            .is_running()
            .await
            .expect("inspect restarted mysql"),
        "mysql should be running after restart"
    );
    common::wait_mysql_ready(stack.mysql_port).await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql after restart");
    mysql
        .query_drop("UPDATE shop.orders SET status='reconnected' WHERE id='ord-1'")
        .await
        .expect("update after mysql restart");
    common::wait_until(
        Duration::from_secs(45),
        "mysql in-process reconnect",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "reconnected")
        },
    )
    .await;

    common::wait_until(Duration::from_secs(10), "mysql binlog cursor", || async {
        std::fs::metadata(format!("{state_dir}/mysql_binlog_pos"))
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
    })
    .await;

    engine.kill();
    mysql
        .query_drop("UPDATE shop.orders SET status='updated-while-stopped' WHERE id='ord-1'")
        .await
        .expect("update mysql order while engine stopped");
    let _restarted = common::spawn_mysql_engine(&options);
    common::wait_until(
        Duration::from_secs(45),
        "mysql resume after restart",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "updated-while-stopped")
        },
    )
    .await;

    mysql
        .query_drop("DELETE FROM shop.orders WHERE id='ord-1'")
        .await
        .expect("delete mysql order");
    common::wait_until(
        Duration::from_secs(30),
        "mysql projection tombstone",
        || async { common::os_doc(stack.os_port, INDEX, DOC_ID).await.is_none() },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh mysql"]
async fn mysql_memory_join_rebuilds_when_join_state_is_lost() {
    let stack = common::start_mysql_os().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE {}.customers (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64) NOT NULL); \
             CREATE TABLE {}.orders (id VARCHAR(64) PRIMARY KEY, customer_id VARCHAR(64) NOT NULL, status VARCHAR(64) NOT NULL); \
             INSERT INTO {}.customers VALUES ('customer-1', 'Ada'); \
             INSERT INTO {}.orders VALUES \
               ('order-1', 'customer-1', 'created'), \
               ('deleted-during-rebuild', 'customer-1', 'created')",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed mysql memory join");

    let state_dir = common::state_dir("mysql-memory", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, MEMORY_ORDERS_SPEC);
    let options = common::MySqlEngine {
        mysql_port: stack.mysql_port,
        os_port: stack.os_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        index_template: "${header:ventstream.target.index}",
        denormalize_mode: "memory",
        bootstrap_mode: "snapshot",
    };
    let mut engine = common::spawn_mysql_engine(&options);
    let document_id = r#"shop.orders:["order-1"]"#;
    let deleted_document_id = r#"shop.orders:["deleted-during-rebuild"]"#;

    common::wait_until(Duration::from_secs(60), "mysql memory snapshot", || async {
        common::os_doc(stack.os_port, MEMORY_INDEX, document_id)
            .await
            .is_some_and(|doc| doc["customer"]["name"] == "Ada")
    })
    .await;
    common::wait_until(
        Duration::from_secs(60),
        "mysql memory snapshot deletion candidate",
        || async {
            common::os_doc(stack.os_port, MEMORY_INDEX, deleted_document_id)
                .await
                .is_some()
        },
    )
    .await;
    common::wait_until(Duration::from_secs(15), "mysql state pair", || async {
        std::fs::metadata(format!("{state_dir}/mysql_binlog_pos"))
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
            && std::fs::metadata(format!("{state_dir}/ventstream-joins.redb"))
                .map(|metadata| metadata.len() > 0)
                .unwrap_or(false)
    })
    .await;

    engine.kill();
    std::fs::remove_file(format!("{state_dir}/ventstream-joins.redb"))
        .expect("remove only join state");
    std::fs::write(
        format!("{state_dir}/mysql_binlog_pos.incomplete"),
        "state-rebuild",
    )
    .expect("simulate interrupted state rebuild");
    mysql
        .query_drop(
            "UPDATE shop.customers SET name='Grace' WHERE id='customer-1'; \
             DELETE FROM shop.orders WHERE id='deleted-during-rebuild'",
        )
        .await
        .expect("update while engine stopped");

    let rebuild_options = common::MySqlEngine {
        bootstrap_mode: "none",
        ..options
    };
    let mut rebuilt = common::spawn_mysql_engine(&rebuild_options);
    common::wait_until(
        Duration::from_secs(60),
        "mysql memory state rebuild",
        || async {
            common::os_doc(stack.os_port, MEMORY_INDEX, document_id)
                .await
                .is_some_and(|doc| doc["customer"]["name"] == "Grace")
        },
    )
    .await;
    common::wait_until(
        Duration::from_secs(60),
        "mysql retained cursor replays deletion after interrupted rebuild",
        || async {
            common::os_doc(stack.os_port, MEMORY_INDEX, deleted_document_id)
                .await
                .is_none()
        },
    )
    .await;

    rebuilt.kill();
    mysql
        .query_drop("UPDATE shop.orders SET status='resumed' WHERE id='order-1'")
        .await
        .expect("update while rebuilt engine stopped");
    let _resumed = common::spawn_mysql_engine(&rebuild_options);
    common::wait_until(
        Duration::from_secs(45),
        "mysql memory normal resume",
        || async {
            common::os_doc(stack.os_port, MEMORY_INDEX, document_id)
                .await
                .is_some_and(|doc| doc["status"] == "resumed")
        },
    )
    .await;
}

/// ENUM/SET primary keys: the bootstrap path renders labels (SELECT),
/// the binlog path historically rendered indexes — so a DELETE's doc id
/// never matched the INSERT's and the sink document was orphaned
/// forever. The normalization must make both paths render labels.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn mysql_enum_pk_delete_matches_bootstrap_doc_id() {
    let stack = common::start_mysql_os().await;
    let mut mysql = common::mysql_root_conn(stack.mysql_port)
        .await
        .expect("mysql root connection");
    mysql
        .query_drop(format!(
            "CREATE USER IF NOT EXISTS '{}'@'%' IDENTIFIED BY '{}'; \
             GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{}'@'%'; \
             CREATE TABLE IF NOT EXISTS {}.badges ( \
                 kind ENUM('gold','silver','bronze') NOT NULL, \
                 owner VARCHAR(64) NOT NULL, \
                 perks SET('a','b','c') NOT NULL DEFAULT '', \
                 PRIMARY KEY (kind, owner)); \
             INSERT INTO {}.badges (kind, owner, perks) VALUES ('silver', 'ada', 'a,c') \
             ON DUPLICATE KEY UPDATE perks=VALUES(perks)",
            common::MYSQL_USER,
            common::MYSQL_PASSWORD,
            common::MYSQL_USER,
            common::MYSQL_DB,
            common::MYSQL_DB,
        ))
        .await
        .expect("seed mysql badges");

    let state_dir = common::state_dir("mysql-enum", stack.mysql_port);
    let spec_path = common::write_spec(&state_dir, BADGES_SPEC);
    let options = common::MySqlEngine {
        mysql_port: stack.mysql_port,
        os_port: stack.os_port,
        spec_path: &spec_path,
        state_dir: &state_dir,
        index_template: "${header:ventstream.target.index}",
        denormalize_mode: "sql",
        bootstrap_mode: "snapshot",
    };
    let mut engine = common::spawn_mysql_engine(&options);

    // Bootstrap doc id uses the LABEL text.
    let doc_id = r#"shop.badges:["silver","ada"]"#;
    common::wait_until(Duration::from_secs(60), "enum badge doc", || async {
        common::os_doc(stack.os_port, BADGES_INDEX, doc_id)
            .await
            .is_some()
    })
    .await;

    // Live insert through the binlog must land under a label id too —
    // and its SET payload must render labels, not a bitmask.
    mysql
        .query_drop(format!(
            "INSERT INTO {}.badges (kind, owner, perks) VALUES ('gold', 'grace', 'b')",
            common::MYSQL_DB
        ))
        .await
        .expect("live insert");
    let live_id = r#"shop.badges:["gold","grace"]"#;
    common::wait_until(Duration::from_secs(60), "live enum badge doc", || async {
        common::os_doc(stack.os_port, BADGES_INDEX, live_id)
            .await
            .is_some()
    })
    .await;

    // THE regression case: the DELETE's before-image doc id (binlog
    // path) must match the bootstrap doc id (SELECT path) — pre-fix
    // the tombstone targeted shop.badges:["2","ada"] and this doc
    // stayed behind forever.
    mysql
        .query_drop(format!(
            "DELETE FROM {}.badges WHERE kind='silver' AND owner='ada'",
            common::MYSQL_DB
        ))
        .await
        .expect("delete badge");
    common::wait_until(
        Duration::from_secs(60),
        "enum badge doc deleted",
        || async {
            common::os_doc(stack.os_port, BADGES_INDEX, doc_id)
                .await
                .is_none()
        },
    )
    .await;

    engine.kill();
    let _ = std::fs::remove_dir_all(&state_dir);
}
