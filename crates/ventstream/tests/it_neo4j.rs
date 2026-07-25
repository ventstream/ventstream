//! Neo4j CDC integration test — real engine binary against a real
//! Neo4j-enterprise (CDC enrichment) + OpenSearch, container-local plain
//! bolt (no TLS). One comprehensive test to amortize the heavy boot.
//!
//! Fixtures use a neutral `User`-[:HAS_ROLE]->`Role` graph (no
//! domain-specific names).
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::process::Command;
use std::time::{Duration, Instant};

const USER_SPEC: &str = r#"
denormalize:
  - primary_label: User
    output_table: users
    fan_out_max_hops: 1
    cypher: |
      OPTIONAL MATCH (p)-[:HAS_ROLE]->(role:Role)
      WITH p, collect(DISTINCT role.name) AS activeRoles
      RETURN elementId(p) AS primaryEid, {
        id:          p.id,
        name:        p.name,
        activeRoles: activeRoles
      } AS doc
"#;

fn has_role(doc: &serde_json::Value, role: &str) -> bool {
    doc["activeRoles"]
        .as_array()
        .map(|a| a.iter().any(|r| r == role))
        .unwrap_or(false)
}

fn process_rss_kib(pid: u32) -> u64 {
    Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|rss| rss.trim().parse().ok())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local stress benchmark: set VS_RUN_NEO4J_STRESS=1 and run explicitly"]
async fn neo4j_streams_one_hundred_thousand_fan_out_ids() {
    if std::env::var("VS_RUN_NEO4J_STRESS").as_deref() != Ok("1") {
        eprintln!("skipped; set VS_RUN_NEO4J_STRESS=1 to run Neo4j fan-out stress");
        return;
    }

    const USER_COUNT: u64 = 100_000;
    const SEED_BATCH: u64 = 10_000;
    let stack = common::start_neo4j_os().await;
    common::neo4j_exec(&stack.neo4j, "CREATE (:Role {name:'editor'})").await;
    for start in (1..=USER_COUNT).step_by(SEED_BATCH as usize) {
        let end = (start + SEED_BATCH - 1).min(USER_COUNT);
        common::neo4j_exec(
            &stack.neo4j,
            &format!(
                "MATCH (role:Role {{name:'editor'}}) \
                 UNWIND range({start}, {end}) AS id \
                 CREATE (:User {{id:'u' + toString(id), name:'User ' + toString(id)}})-[:HAS_ROLE]->(role)"
            ),
        )
        .await;
    }

    let dir = common::state_dir("neo4j-fanout-stress", stack.neo4j_port);
    let spec = common::write_spec(&dir, USER_SPEC);
    let engine = common::spawn_neo4j_engine(&common::Neo4jEngine {
        neo4j_port: stack.neo4j_port,
        os_port: stack.os_port,
        spec_path: &spec,
        state_dir: &dir,
        index_template: "users",
        max_parallel_bulks: Some(16),
    });
    let pid = engine.pid();
    let mut peak_rss_kib = process_rss_kib(pid);
    let bootstrap_started = Instant::now();
    common::wait_until(Duration::from_secs(240), "100k Neo4j snapshot", || {
        peak_rss_kib = peak_rss_kib.max(process_rss_kib(pid));
        async { common::os_count(stack.os_port, "users").await == USER_COUNT }
    })
    .await;
    let bootstrap_elapsed = bootstrap_started.elapsed();

    let fan_out_started = Instant::now();
    common::neo4j_exec(
        &stack.neo4j,
        "MATCH (role:Role {name:'editor'}) SET role.name='administrator'",
    )
    .await;
    common::wait_until(Duration::from_secs(240), "100k Neo4j fan-out", || {
        peak_rss_kib = peak_rss_kib.max(process_rss_kib(pid));
        async {
            common::os_term_count(
                stack.os_port,
                "users",
                "activeRoles.keyword",
                "administrator",
            )
            .await
                == USER_COUNT
        }
    })
    .await;
    let fan_out_elapsed = fan_out_started.elapsed();
    let concurrency_adjustments = engine
        .log()
        .matches("OpenSearch adaptive concurrency adjusted")
        .count();
    assert!(
        concurrency_adjustments > 0,
        "real OpenSearch writes should ramp above the initial concurrency"
    );

    println!(
        "neo4j fan-out benchmark: primaries={USER_COUNT} bootstrap_ms={} fan_out_ms={} peak_rss_mib={:.1} adaptive_adjustments={concurrency_adjustments}",
        bootstrap_elapsed.as_millis(),
        fan_out_elapsed.as_millis(),
        peak_rss_kib as f64 / 1024.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh neo4j"]
async fn neo4j_cdc_bootstrap_edge_delete_node_delete() {
    let stack = common::start_neo4j_os().await;

    // Seed a User + role BEFORE the engine starts so the bootstrap scan
    // picks it up.
    common::neo4j_exec(
        &stack.neo4j,
        "CREATE (u:User {id:'u1', name:'Ada'})-[:HAS_ROLE]->(:Role {name:'editor'})",
    )
    .await;
    let eid = common::neo4j_scalar(
        &common::neo4j_exec(&stack.neo4j, "MATCH (u:User {id:'u1'}) RETURN elementId(u)").await,
    );
    let doc_id = format!("users:{eid}");

    let dir = common::state_dir("neo4j", stack.neo4j_port);
    let spec = common::write_spec(&dir, USER_SPEC);
    let _engine = common::spawn_neo4j_engine(&common::Neo4jEngine {
        neo4j_port: stack.neo4j_port,
        os_port: stack.os_port,
        spec_path: &spec,
        state_dir: &dir,
        index_template: "users",
        max_parallel_bulks: None,
    });

    // BOOTSTRAP: the user doc appears with its role embedded.
    common::wait_until(Duration::from_secs(90), "bootstrap user doc", || async {
        match common::os_doc(stack.os_port, "users", &doc_id).await {
            Some(d) => d["name"] == "Ada" && has_role(&d, "editor"),
            None => false,
        }
    })
    .await;

    // EDGE DELETE: drop HAS_ROLE — activeRoles empties, the user survives.
    common::neo4j_exec(
        &stack.neo4j,
        "MATCH (:User {id:'u1'})-[r:HAS_ROLE]->() DELETE r",
    )
    .await;
    common::wait_until(Duration::from_secs(45), "role to leave the doc", || async {
        match common::os_doc(stack.os_port, "users", &doc_id).await {
            Some(d) => d["activeRoles"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(false),
            None => false,
        }
    })
    .await;

    // NODE DELETE: detach delete the user — doc tombstoned.
    common::neo4j_exec(&stack.neo4j, "MATCH (u:User {id:'u1'}) DETACH DELETE u").await;
    common::wait_until(Duration::from_secs(45), "user doc tombstone", || async {
        common::os_doc(stack.os_port, "users", &doc_id)
            .await
            .is_none()
    })
    .await;
}
