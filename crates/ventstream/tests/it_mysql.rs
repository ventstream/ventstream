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

const INDEX: &str = "it_mysql_orders";
const COMPOSITE_INDEX: &str = "it_mysql_composite_items";
const MEMORY_INDEX: &str = "it_mysql_memory_orders";
const DOC_ID: &str = r#"shop.orders:["ord-1"]"#;
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
