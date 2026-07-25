//! MongoDB CDC integration test: a real replica set, engine process, and
//! OpenSearch sink. Run locally through `scripts/test-sources.sh mongodb`.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::time::Duration;

use mongodb::bson::{doc, Document};

const INDEX: &str = "it_mongodb_orders";
const DOC_ID: &str = r#"shop.orders:["ord-1"]"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh mongodb"]
async fn mongodb_bootstrap_live_change_and_resume_token_restart() {
    let stack = common::start_mongodb_os().await;
    let client = mongodb::Client::with_uri_str(&stack.uri)
        .await
        .expect("mongodb client");
    let orders = client
        .database(common::MONGO_DB)
        .collection::<Document>("orders");
    orders
        .insert_one(doc! {"_id": "ord-1", "status": "created", "total": 10})
        .await
        .expect("seed mongodb order");

    let state_dir = common::state_dir("mongodb", stack.mongo_port);
    let options = common::MongoEngine {
        uri: &stack.uri,
        os_port: stack.os_port,
        state_dir: &state_dir,
        index_template: INDEX,
    };
    let mut engine = common::spawn_mongodb_engine(&options);

    common::wait_until(
        Duration::from_secs(60),
        "mongodb snapshot document",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "created")
        },
    )
    .await;

    orders
        .update_one(
            doc! {"_id": "ord-1"},
            doc! {"$set": {"status": "live-updated"}},
        )
        .await
        .expect("update mongodb order");
    common::wait_until(Duration::from_secs(30), "mongodb live update", || async {
        common::os_doc(stack.os_port, INDEX, DOC_ID)
            .await
            .is_some_and(|doc| doc["status"] == "live-updated")
    })
    .await;
    common::wait_until(Duration::from_secs(10), "mongodb resume token", || async {
        std::fs::metadata(format!("{state_dir}/mongo_resume_token"))
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
    })
    .await;

    engine.kill();
    orders
        .update_one(
            doc! {"_id": "ord-1"},
            doc! {"$set": {"status": "updated-while-stopped"}},
        )
        .await
        .expect("update mongodb order while engine stopped");
    let _restarted = common::spawn_mongodb_engine(&options);
    common::wait_until(
        Duration::from_secs(45),
        "mongodb resume after restart",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "updated-while-stopped")
        },
    )
    .await;
}
