//! End-to-end: Postgres (SQL-denormalize joins) → SurrealDB.
//!
//! Drives the real engine binary against real containers: composed-doc
//! bootstrap parity, live CRUD with join fan-out, 1:many child deletes
//! resolved through the sink reverse lookup, primary-key relocation, and
//! declared HNSW vector-index DDL.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod common;

use std::time::{Duration, Instant};

const JOINS: &str = r#"
joins:
  - name: orders
    primary:
      table: public.orders
      pk: order_id
    related:
      - id: customer
        table: public.customers
        pk: customer_id
        join_on: { from: customer_id, to: customer_id }
        embed_as: customer
        cardinality: one
      - id: items
        table: public.order_items
        pk: item_id
        join_on: { from: order_id, to: order_id }
        embed_as: items
        cardinality: many
        sort_by: item_id
    target:
      index: orders
"#;

fn engine_yaml(pg_port: u16, state_dir: &str) -> String {
    format!(
        r#"
schema_version: 1
roles: [cdc]

source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    port: {pg_port}
    user_ref: env:VS_PG_USER
    password_ref: env:VS_PG_PASSWORD
    database_ref: env:VS_PG_DATABASE
    publication_ref: env:VS_PG_PUBLICATION
    slot_ref: env:VS_PG_SLOT
    denormalize_mode: sql
    sink_reverse_lookup: true
    bootstrap:
      mode: snapshot

specs:
  joins: {state_dir}/joins.yaml

sink:
  kind: surrealdb
  surrealdb:
    endpoint_ref: env:VS_SURREAL_ENDPOINT
    namespace: vs
    database: app
    username_ref: env:VS_SURREAL_USERNAME
    password_ref: env:VS_SURREAL_PASSWORD
    auto_create_database: true
    vector_indexes:
      - table: orders
        field: embedding
        dimension: 3
        distance: cosine

runtime:
  health_listen: 127.0.0.1:0
  dlq_path: {state_dir}/dlq.jsonl
"#
    )
}

/// Count query that treats "not there yet" (namespace/table still being
/// created by the engine) as zero, so polls can retry instead of panic.
async fn count(surreal_port: u16, sql: &str) -> i64 {
    match common::surreal_rpc(surreal_port, sql).await {
        Ok(result) => result[0]["result"][0]["count"].as_i64().unwrap_or(0),
        Err(_) => -1,
    }
}

async fn wait_for(deadline_s: u64, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(deadline_s);
    loop {
        if check().await {
            return;
        }
        assert!(Instant::now() < deadline, "condition not met in time");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn composed_docs_reach_surrealdb_and_stay_consistent() {
    let stack = common::start_pg_surreal().await;
    let client = common::pg_client(stack.pg_port).await;
    client
        .batch_execute(
            "CREATE TABLE customers(customer_id bigint PRIMARY KEY, name text NOT NULL, \
             tier text NOT NULL DEFAULT 'standard');
             CREATE TABLE orders(order_id bigint PRIMARY KEY, customer_id bigint NOT NULL, \
             status text NOT NULL DEFAULT 'open', embedding jsonb);
             CREATE TABLE order_items(item_id bigint PRIMARY KEY, order_id bigint NOT NULL, \
             sku text NOT NULL);
             INSERT INTO customers SELECT g, 'customer-'||g, 'standard' \
             FROM generate_series(1,50) g;
             INSERT INTO orders SELECT g, (g%50)+1, 'open', \
             to_jsonb(ARRAY[0.1,0.2,0.3]) FROM generate_series(1,500) g;
             INSERT INTO order_items SELECT g, ((g-1)%500)+1, 'sku-'||g \
             FROM generate_series(1,1500) g;
             CREATE PUBLICATION vs_it_pub FOR TABLE orders, customers, order_items;",
        )
        .await
        .expect("seed schema");

    let state_dir = format!(
        "{}/vs-it-surreal-{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    std::fs::create_dir_all(&state_dir).expect("state dir");
    std::fs::write(format!("{state_dir}/joins.yaml"), JOINS).expect("joins");
    let config_path = format!("{state_dir}/engine.yaml");
    std::fs::write(&config_path, engine_yaml(stack.pg_port, &state_dir)).expect("engine yaml");

    let env = [
        ("VS_PG_HOST", "127.0.0.1".to_owned()),
        ("VS_PG_USER", common::PG_USER.to_owned()),
        ("VS_PG_PASSWORD", common::PG_PASSWORD.to_owned()),
        ("VS_PG_DATABASE", common::PG_DB.to_owned()),
        ("VS_PG_PUBLICATION", "vs_it_pub".to_owned()),
        ("VS_PG_SLOT", "vs_it_surreal_slot".to_owned()),
        (
            "VS_SURREAL_ENDPOINT",
            format!("http://127.0.0.1:{}", stack.surreal_port),
        ),
        ("VS_SURREAL_USERNAME", "root".to_owned()),
        ("VS_SURREAL_PASSWORD", "root".to_owned()),
    ];
    let mut engine = common::spawn_engine_with_config(&state_dir, &config_path, &env);

    // Bootstrap parity: every composed order document lands.
    let surreal_port = stack.surreal_port;
    wait_for(120, async || {
        count(surreal_port, "SELECT count() FROM orders GROUP ALL;").await == 500
    })
    .await;

    // The declared HNSW vector index exists on the routed table.
    let info = common::surreal_rpc(surreal_port, "INFO FOR TABLE orders;")
        .await
        .expect("table info");
    assert!(
        info[0]["result"]["indexes"]
            .as_object()
            .is_some_and(|indexes| !indexes.is_empty()),
        "vector index missing: {info}"
    );

    // Live insert composes customer + items; join fan-out recomposes.
    client
        .batch_execute(
            "INSERT INTO orders VALUES (9001, 7, 'open', to_jsonb(ARRAY[0.9,0.9,0.9]));
             INSERT INTO order_items VALUES (99001, 9001, 'sku-live');
             UPDATE customers SET tier='gold' WHERE customer_id=7;",
        )
        .await
        .expect("live changes");
    wait_for(60, async || {
        let Ok(result) = common::surreal_rpc(
            surreal_port,
            "SELECT customer.tier AS tier, array::len(items) AS n FROM orders:[\"9001\"];",
        )
        .await
        else {
            return false;
        };
        result[0]["result"][0]["tier"] == "gold" && result[0]["result"][0]["n"] == 1
    })
    .await;

    // 1:many child delete under REPLICA IDENTITY DEFAULT resolves through
    // the sink reverse lookup and recomposes the parent.
    client
        .batch_execute("DELETE FROM order_items WHERE item_id=99001;")
        .await
        .expect("child delete");
    wait_for(60, async || {
        let Ok(result) = common::surreal_rpc(
            surreal_port,
            "SELECT array::len(items) AS n FROM orders:[\"9001\"];",
        )
        .await
        else {
            return false;
        };
        result[0]["result"][0]["n"] == 0
    })
    .await;

    // Primary-key relocation deletes the old identity and writes the new.
    client
        .batch_execute("UPDATE orders SET order_id=9002 WHERE order_id=9001;")
        .await
        .expect("relocate pk");
    wait_for(60, async || {
        let old = count(
            surreal_port,
            "SELECT count() FROM orders:[\"9001\"] GROUP ALL;",
        )
        .await;
        let new = count(
            surreal_port,
            "SELECT count() FROM orders:[\"9002\"] GROUP ALL;",
        )
        .await;
        old == 0 && new == 1
    })
    .await;

    // Primary delete tombstones the document.
    client
        .batch_execute("DELETE FROM orders WHERE order_id=9002;")
        .await
        .expect("primary delete");
    wait_for(60, async || {
        count(surreal_port, "SELECT count() FROM orders GROUP ALL;").await == 500
    })
    .await;

    engine.kill();
    let _ = std::fs::remove_dir_all(&state_dir);
}

fn relate_engine_yaml(pg_port: u16, state_dir: &str) -> String {
    format!(
        r#"
schema_version: 1
roles: [cdc]

source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    port: {pg_port}
    user_ref: env:VS_PG_USER
    password_ref: env:VS_PG_PASSWORD
    database_ref: env:VS_PG_DATABASE
    publication_ref: env:VS_PG_PUBLICATION
    slot_ref: env:VS_PG_SLOT
    denormalize_mode: sql
    bootstrap:
      mode: snapshot

sink:
  kind: surrealdb
  surrealdb:
    endpoint_ref: env:VS_SURREAL_ENDPOINT
    namespace: vs
    database: app
    username_ref: env:VS_SURREAL_USERNAME
    password_ref: env:VS_SURREAL_PASSWORD
    auto_create_database: true
    graph_edges:
      - name: wrote
        from_table: articles
        fk_columns: [author_id]
        to_table: people
        reversed: true

runtime:
  health_listen: 127.0.0.1:0
  dlq_path: {state_dir}/dlq.jsonl
"#
    )
}

/// Edge count reachable from one person via `->wrote->articles`.
async fn wrote_from(surreal_port: u16, person: &str) -> i64 {
    let sql = format!("SELECT array::len(->wrote->articles) AS n FROM people:[\"{person}\"];");
    match common::surreal_rpc(surreal_port, &sql).await {
        Ok(result) => result[0]["result"][0]["n"].as_i64().unwrap_or(-1),
        Err(_) => -1,
    }
}

/// FK-derived RELATE edges: bootstrap materializes the graph, live CDC
/// moves/removes edges (REPLICA IDENTITY DEFAULT throughout), endpoint
/// deletion auto-purges, and a full re-bootstrap is idempotent — the
/// deterministic edge ids overwrite instead of duplicating.
#[tokio::test]
#[ignore = "local integration: requires Docker; run explicitly"]
async fn fk_edges_materialize_and_stay_consistent() {
    let stack = common::start_pg_surreal().await;
    let client = common::pg_client(stack.pg_port).await;
    // Logical FK only (no constraint): the test deletes a referenced
    // person to prove SurrealDB's edge auto-purge.
    client
        .batch_execute(
            "CREATE TABLE people(person_id bigint PRIMARY KEY, name text NOT NULL);
             CREATE TABLE articles(article_id bigint PRIMARY KEY, author_id bigint, \
             title text NOT NULL DEFAULT 'untitled');
             INSERT INTO people SELECT g, 'person-'||g FROM generate_series(1,20) g;
             INSERT INTO articles SELECT g, (g%20)+1, 'article-'||g \
             FROM generate_series(1,100) g;
             CREATE PUBLICATION vs_relate_pub FOR TABLE people, articles;",
        )
        .await
        .expect("seed schema");

    let state_dir = format!(
        "{}/vs-it-relate-{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    std::fs::create_dir_all(&state_dir).expect("state dir");
    let config_path = format!("{state_dir}/engine.yaml");
    std::fs::write(&config_path, relate_engine_yaml(stack.pg_port, &state_dir)).expect("yaml");

    let env = [
        ("VS_PG_HOST", "127.0.0.1".to_owned()),
        ("VS_PG_USER", common::PG_USER.to_owned()),
        ("VS_PG_PASSWORD", common::PG_PASSWORD.to_owned()),
        ("VS_PG_DATABASE", common::PG_DB.to_owned()),
        ("VS_PG_PUBLICATION", "vs_relate_pub".to_owned()),
        ("VS_PG_SLOT", "vs_relate_slot".to_owned()),
        (
            "VS_SURREAL_ENDPOINT",
            format!("http://127.0.0.1:{}", stack.surreal_port),
        ),
        ("VS_SURREAL_USERNAME", "root".to_owned()),
        ("VS_SURREAL_PASSWORD", "root".to_owned()),
    ];
    let mut engine = common::spawn_engine_with_config(&state_dir, &config_path, &env);
    let surreal_port = stack.surreal_port;

    // Bootstrap: documents AND the full edge set land.
    wait_for(120, async || {
        count(surreal_port, "SELECT count() FROM articles GROUP ALL;").await == 100
            && count(surreal_port, "SELECT count() FROM people GROUP ALL;").await == 20
            && count(surreal_port, "SELECT count() FROM wrote GROUP ALL;").await == 100
    })
    .await;
    // Graph traversal works: each person wrote 5 of the 100 articles.
    assert_eq!(wrote_from(surreal_port, "7").await, 5);

    // Live insert grows the graph.
    client
        .batch_execute("INSERT INTO articles VALUES (9001, 3, 'live');")
        .await
        .expect("live insert");
    wait_for(60, async || wrote_from(surreal_port, "3").await == 6).await;

    // FK update MOVES the edge — no old-row image needed under
    // REPLICA IDENTITY DEFAULT.
    client
        .batch_execute("UPDATE articles SET author_id=5 WHERE article_id=9001;")
        .await
        .expect("fk move");
    wait_for(60, async || {
        wrote_from(surreal_port, "3").await == 5 && wrote_from(surreal_port, "5").await == 6
    })
    .await;

    // NULL FK removes the edge; the document stays.
    client
        .batch_execute("UPDATE articles SET author_id=NULL WHERE article_id=9001;")
        .await
        .expect("null fk");
    wait_for(60, async || {
        wrote_from(surreal_port, "5").await == 5
            && count(surreal_port, "SELECT count() FROM wrote GROUP ALL;").await == 100
            && count(surreal_port, "SELECT count() FROM articles GROUP ALL;").await == 101
    })
    .await;

    // Row delete drops its edge bookkeeping too (the edge was already
    // gone here; the delete path must not resurrect or fail it).
    client
        .batch_execute(
            "DELETE FROM articles WHERE article_id=9001;
             INSERT INTO articles VALUES (9002, 7, 'second-wind');",
        )
        .await
        .expect("delete + reinsert");
    wait_for(60, async || {
        wrote_from(surreal_port, "7").await == 6
            && count(surreal_port, "SELECT count() FROM articles GROUP ALL;").await == 101
    })
    .await;

    // Deleting a referenced endpoint auto-purges its edges (TYPE
    // RELATION) — person 7 leaves with all 6 of their edges.
    client
        .batch_execute("DELETE FROM people WHERE person_id=7;")
        .await
        .expect("delete person");
    wait_for(60, async || {
        count(surreal_port, "SELECT count() FROM people GROUP ALL;").await == 19
            && count(surreal_port, "SELECT count() FROM wrote GROUP ALL;").await == 95
    })
    .await;

    // Full re-bootstrap over the survivors: deterministic edge ids make
    // the RELATE pass idempotent — exactly one edge per FK row, never
    // duplicates. (Rows still referencing the deleted person re-relate
    // as dangling edges, matching source truth.)
    engine.kill();
    let state_dir2 = format!("{state_dir}-re");
    std::fs::create_dir_all(&state_dir2).expect("state dir 2");
    let config_path2 = format!("{state_dir2}/engine.yaml");
    std::fs::write(
        &config_path2,
        relate_engine_yaml(stack.pg_port, &state_dir2),
    )
    .expect("yaml");
    let mut env2 = env.clone();
    env2[5] = ("VS_PG_SLOT", "vs_relate_slot2".to_owned());
    let mut engine2 = common::spawn_engine_with_config(&state_dir2, &config_path2, &env2);
    wait_for(120, async || {
        count(surreal_port, "SELECT count() FROM wrote GROUP ALL;").await == 101
    })
    .await;
    // Stability probe: nothing keeps growing after the snapshot.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        count(surreal_port, "SELECT count() FROM wrote GROUP ALL;").await,
        101
    );
    assert_eq!(wrote_from(surreal_port, "5").await, 5);

    engine2.kill();
    let _ = std::fs::remove_dir_all(&state_dir);
    let _ = std::fs::remove_dir_all(&state_dir2);
}
