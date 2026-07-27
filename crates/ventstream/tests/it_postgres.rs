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

use serde_json::Value;
use ventstream_joins::{PkValue, RelatedFetcher};
use ventstream_sources::postgres::PostgresFetcher;

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

    let connection_string = format!(
        "host=127.0.0.1 port={} user={} password={} dbname={}",
        stack.pg_port,
        common::PG_USER,
        common::PG_PASSWORD,
        common::PG_DB
    );
    let fetcher = PostgresFetcher::connect_with_pool_size(connection_string, 4)
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
