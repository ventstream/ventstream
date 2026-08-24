//! OpenSearch sink integration tests — real engine binary against real
//! Postgres + OpenSearch containers. Run with: `cargo test -p ventstream
//! --test it_opensearch -- --ignored --test-threads=1`.
//!
//! The other suites cover OpenSearch incidentally, as the sink behind a
//! source. These cover the sink's own behaviour, and in particular the
//! TRUNCATE path added in #164: a source truncate now issues a scoped
//! the truncated relation's documents instead of indexing the truncate
//! event as a document. That is a destructive path, so it needs coverage
//! against a real cluster rather than unit tests over the request builder.
//!
//! These run in SQL-denormalize mode deliberately. The in-memory join
//! engine handles a primary-table truncate by purging its own state and
//! emitting per-row tombstones, so the documents disappear whether or not
//! the sink supports target clears — a truncate test in that mode passes
//! against a build that has no clear path at all. Only the
//! sql-denormalize path emits the target-clear event the sink acts on.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::time::Duration;

/// Two relations projected into one shared index. A truncate of either must
/// scope its delete by document-id prefix and leave the other alone.
///
/// The index name is a parameter because the OpenSearch container is
/// process-scoped: tests sharing an index name would see each other's
/// documents and race.
fn shared_index_spec(index: &str) -> String {
    format!(
        r#"
joins:
  - name: orders
    primary:
      table: os_it.orders
      pk: id
    target:
      index: {index}
    state:
      backend: memory
  - name: customers
    primary:
      table: os_it.customers
      pk: id
    target:
      index: {index}
    state:
      backend: memory
"#
    )
}

const SHARED_INDEX_SEED: &str = "
CREATE SCHEMA os_it;
CREATE TABLE os_it.orders(id text PRIMARY KEY, status text NOT NULL);
CREATE TABLE os_it.customers(id text PRIMARY KEY, name text NOT NULL);
INSERT INTO os_it.orders VALUES ('o1', 'open'), ('o2', 'open');
INSERT INTO os_it.customers VALUES ('c1', 'Ada'), ('c2', 'Grace');
CREATE PUBLICATION ventstream_os_it FOR TABLES IN SCHEMA os_it;
";

fn order_id(id: &str) -> String {
    format!("os_it.orders:[\"{id}\"]")
}

fn customer_id(id: &str) -> String {
    format!("os_it.customers:[\"{id}\"]")
}

/// #112: TRUNCATE clears the target, scoped to the truncated relation so
/// co-tenant relations in a shared index are untouched. `_id` cannot be
/// prefix-matched server side, so the sink scans and filters locally. Before the
/// fix the truncate event was indexed as a junk document and nothing was
/// ever removed; a naive fix would have used match_all and destroyed the
/// customers alongside the orders.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn truncate_clears_only_the_truncated_relation() {
    const INDEX: &str = "it_os_scoped_clear";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SHARED_INDEX_SEED).await.expect("seed");

    let dir = common::state_dir("os-truncate-scoped", stack.pg_port);
    let spec = common::write_spec(&dir, &shared_index_spec(INDEX));
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "os_truncate_scoped",
        publication: "ventstream_os_it",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "sql",
    });

    common::wait_until(Duration::from_secs(90), "all four docs to land", || async {
        common::os_count(stack.os_port, INDEX).await == 4
    })
    .await;

    pg.batch_execute("TRUNCATE os_it.orders;")
        .await
        .expect("truncate orders");

    common::wait_until(
        Duration::from_secs(90),
        "orders to be cleared from the shared index",
        || async { common::os_count(stack.os_port, INDEX).await == 2 },
    )
    .await;

    // The customers must survive: this is the assertion that fails if the
    // clear ever regresses to match_all.
    assert!(
        common::os_doc(stack.os_port, INDEX, &customer_id("c1"))
            .await
            .is_some(),
        "c1 must survive a truncate of a different relation"
    );
    assert!(
        common::os_doc(stack.os_port, INDEX, &customer_id("c2"))
            .await
            .is_some(),
        "c2 must survive a truncate of a different relation"
    );
    assert!(
        common::os_doc(stack.os_port, INDEX, &order_id("o1"))
            .await
            .is_none(),
        "o1 must be gone after truncating its relation"
    );
}

/// #164 review: clears are applied at their position in the stream rather
/// than hoisted ahead of the batch. A write that lands after a truncate in
/// the same transaction must survive it — hoisting would delete the row the
/// truncate was supposed to precede.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn a_write_after_a_truncate_in_one_transaction_survives() {
    const INDEX: &str = "it_os_clear_ordering";
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(SHARED_INDEX_SEED).await.expect("seed");

    let dir = common::state_dir("os-truncate-ordering", stack.pg_port);
    let spec = common::write_spec(&dir, &shared_index_spec(INDEX));
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "os_truncate_ordering",
        publication: "ventstream_os_it",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "sql",
    });

    common::wait_until(Duration::from_secs(90), "seed docs to land", || async {
        common::os_count(stack.os_port, INDEX).await == 4
    })
    .await;

    // write, TRUNCATE, write — all in one transaction, so the engine sees
    // them in one batch and the clear sits in the middle of it.
    pg.batch_execute(
        "BEGIN;
         INSERT INTO os_it.orders VALUES ('o3', 'before-truncate');
         TRUNCATE os_it.orders;
         INSERT INTO os_it.orders VALUES ('o4', 'after-truncate');
         COMMIT;",
    )
    .await
    .expect("write/truncate/write");

    common::wait_until(
        Duration::from_secs(90),
        "only the trailing write to remain",
        || async {
            common::os_doc(stack.os_port, INDEX, &order_id("o4"))
                .await
                .is_some()
        },
    )
    .await;

    for gone in ["o1", "o2", "o3"] {
        assert!(
            common::os_doc(stack.os_port, INDEX, &order_id(gone))
                .await
                .is_none(),
            "{gone} was written before the truncate and must not survive it"
        );
    }
    assert!(
        common::os_doc(stack.os_port, INDEX, &customer_id("c1"))
            .await
            .is_some(),
        "customers are a different relation and must be untouched"
    );
}

/// A truncate against an index that was never created must be a no-op. The
/// sink's first request against a missing index answers 404; treating that
/// as an error would kill the pipeline on an empty table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn truncating_an_empty_relation_does_not_stall_the_pipeline() {
    const INDEX: &str = "it_os_missing_index";
    const SPEC: &str = r#"
joins:
  - name: orders
    primary:
      table: os_empty.orders
      pk: id
    target:
      index: it_os_missing_index
    state:
      backend: memory
"#;
    let stack = common::start_pg_os().await;
    let pg = common::pg_client(stack.pg_port).await;
    pg.batch_execute(
        "CREATE SCHEMA os_empty;
         CREATE TABLE os_empty.orders(id text PRIMARY KEY, status text NOT NULL);
         CREATE PUBLICATION ventstream_os_empty FOR TABLES IN SCHEMA os_empty;",
    )
    .await
    .expect("empty schema");

    let dir = common::state_dir("os-truncate-missing", stack.pg_port);
    let spec = common::write_spec(&dir, SPEC);
    let _engine = common::spawn_pg_engine(&common::PgEngine {
        pg_port: stack.pg_port,
        os_port: stack.os_port,
        slot: "os_truncate_missing",
        publication: "ventstream_os_empty",
        spec_path: &spec,
        state_dir: &dir,
        index_template: INDEX,
        denormalize_mode: "sql",
    });

    // Truncate before anything has ever been indexed: the index does not exist.
    pg.batch_execute("TRUNCATE os_empty.orders;")
        .await
        .expect("truncate empty relation");

    // The pipeline must still be alive: a subsequent insert has to arrive.
    pg.batch_execute("INSERT INTO os_empty.orders VALUES ('o1', 'after-empty-truncate');")
        .await
        .expect("insert after truncate");

    common::wait_until(
        Duration::from_secs(90),
        "pipeline to keep working after truncating a never-created index",
        || async { common::os_count(stack.os_port, INDEX).await == 1 },
    )
    .await;
}
