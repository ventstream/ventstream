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
use redis::AsyncCommands;

const INDEX: &str = "it_mongodb_orders";
const DOC_ID: &str = r#"shop.orders:["ord-1"]"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mongodb_materializes_updates_and_deletes_in_redis() {
    const PREFIX: &str = "ventstream:it:mongodb";

    let stack = common::start_mongodb_os().await;
    let (_redis, redis_port) = common::start_redis().await;
    let client = mongodb::Client::with_uri_str(&stack.uri)
        .await
        .expect("mongodb client");
    let orders = client
        .database(common::MONGO_DB)
        .collection::<Document>("orders");
    orders
        .insert_one(doc! {"_id": "redis-1", "status": "created", "total": 10})
        .await
        .expect("seed mongodb order");

    let state_dir = common::state_dir("mongodb-redis", stack.mongo_port);
    let mut engine = common::spawn_mongodb_redis_engine(&stack.uri, redis_port, &state_dir, PREFIX);
    let pattern = format!("{PREFIX}:{{orders}}:*");
    common::wait_until(Duration::from_secs(60), "MongoDB Redis snapshot", || {
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
        .expect("MongoDB Redis keys");
    assert_eq!(keys.len(), 1);
    let key = keys.first().expect("MongoDB Redis key").clone();

    orders
        .update_one(
            doc! {"_id": "redis-1"},
            doc! {"$set": {"status": "live-updated"}},
        )
        .await
        .expect("update mongodb order");
    common::wait_until(Duration::from_secs(30), "MongoDB Redis update", || async {
        common::redis_value(redis_port, &key)
            .await
            .ok()
            .flatten()
            .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
            .is_some_and(|document| document["status"] == "live-updated")
    })
    .await;

    engine.kill();
    orders
        .update_one(
            doc! {"_id": "redis-1"},
            doc! {"$set": {"status": "updated-while-stopped"}},
        )
        .await
        .expect("update mongodb order while stopped");
    let mut restarted =
        common::spawn_mongodb_redis_engine(&stack.uri, redis_port, &state_dir, PREFIX);
    common::wait_until(Duration::from_secs(45), "MongoDB Redis restart", || async {
        common::redis_value(redis_port, &key)
            .await
            .ok()
            .flatten()
            .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
            .is_some_and(|document| document["status"] == "updated-while-stopped")
    })
    .await;

    let cursor_path = format!("{state_dir}/mongo_resume_token");
    common::wait_until(Duration::from_secs(10), "MongoDB resume token", || async {
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
        .expect("seed stale MongoDB target key");
    redis
        .set::<_, _, ()>(&neighboring_key, br#"{"keep":true}"#)
        .await
        .expect("seed neighboring Redis target key");
    restarted.terminate();

    let shared_output =
        common::mongodb_redis_engine_command(&stack.uri, redis_port, &state_dir, PREFIX, "shared")
            .arg("--fleet-rebootstrap")
            .output()
            .expect("run shared MongoDB rebootstrap");
    assert!(
        !shared_output.status.success(),
        "shared MongoDB keyspace rebootstrap must be refused"
    );
    assert!(
        std::fs::metadata(&cursor_path).is_ok(),
        "ownership validation must preserve the MongoDB resume token"
    );
    assert_eq!(
        common::redis_keys(redis_port, &owned_pattern)
            .await
            .expect("read MongoDB target after refused rebootstrap")
            .len(),
        2,
        "refused rebootstrap must preserve materialized and stale keys"
    );

    let exclusive_output = common::mongodb_redis_engine_command(
        &stack.uri,
        redis_port,
        &state_dir,
        PREFIX,
        "exclusive",
    )
    .arg("--fleet-rebootstrap")
    .output()
    .expect("run exclusive MongoDB rebootstrap");
    assert!(
        exclusive_output.status.success(),
        "exclusive MongoDB rebootstrap failed: {}",
        String::from_utf8_lossy(&exclusive_output.stderr)
    );
    assert!(
        std::fs::metadata(&cursor_path).is_err(),
        "MongoDB rebootstrap must remove the resume token"
    );
    assert!(
        common::redis_keys(redis_port, &owned_pattern)
            .await
            .expect("read MongoDB target after rebootstrap")
            .is_empty(),
        "MongoDB rebootstrap must clear the exclusively owned target"
    );
    assert!(
        common::redis_value(redis_port, &neighboring_key)
            .await
            .expect("read neighboring Redis target")
            .is_some(),
        "MongoDB rebootstrap must preserve neighboring targets"
    );

    let _rebuilt = common::spawn_mongodb_redis_engine_with_ownership(
        &stack.uri,
        redis_port,
        &state_dir,
        PREFIX,
        "exclusive",
    );
    common::wait_until(
        Duration::from_secs(60),
        "MongoDB Redis live set after rebootstrap",
        || async {
            common::redis_keys(redis_port, &owned_pattern)
                .await
                .is_ok_and(|keys| keys.len() == 1 && !keys.contains(&stale_key))
        },
    )
    .await;

    orders
        .delete_one(doc! {"_id": "redis-1"})
        .await
        .expect("delete mongodb order");
    common::wait_until(Duration::from_secs(30), "MongoDB Redis delete", || async {
        common::redis_value(redis_port, &key).await == Ok(None)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker"]
async fn mongodb_materializes_declarative_redis_views() {
    const PREFIX: &str = "ventstream:it:mongodb:views";

    let stack = common::start_mongodb_os().await;
    let (_redis, redis_port) = common::start_redis().await;
    let client = mongodb::Client::with_uri_str(&stack.uri)
        .await
        .expect("mongodb client");
    let orders = client
        .database(common::MONGO_DB)
        .collection::<Document>("orders");
    orders
        .insert_one(doc! {
            "_id": "redis-view-1",
            "customer_id": "customer-1",
            "status": "pending",
            "total": 10
        })
        .await
        .expect("seed MongoDB view source");

    let state_dir = common::state_dir("mongodb-redis-views", stack.mongo_port);
    let config_path = format!("{state_dir}/ventstream.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: {database}
    namespace: {database}
    state_dir: {state_dir}
    collections: [orders]
    full_document: update_lookup
    bootstrap:
      mode: snapshot
      chunk_size: 100
    token_flush_ms: 100
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
          - name: open_order_by_id
            source:
              namespace: {database}
              relation: orders
            key:
              template: "order:${{json:/id}}"
            filter:
              conditions:
                - path: /status
                  operator: in
                  values: [pending, processing]
            value:
              mode: fields
              fields:
                id: /id
                status: /status
          - name: order_by_customer
            source:
              namespace: {database}
              relation: orders
            key:
              template: "customer:${{json:/customer_id}}:order:${{json:/id}}"
            value:
              mode: document
    contract:
      mode: materialized_view
runtime:
  dlq_path: {state_dir}/dlq.jsonl
"#,
            database = common::MONGO_DB
        ),
    )
    .expect("write MongoDB Redis view config");

    let _engine = common::spawn_engine_with_config(
        &state_dir,
        &config_path,
        &[
            ("VS_MONGO_URI", stack.uri.clone()),
            ("VS_REDIS_SINK_URL", common::redis_url(redis_port)),
        ],
    );
    let open_key = format!("{PREFIX}:{{open_order_by_id}}:order%3Aredis-view-1");
    let first_customer_key =
        format!("{PREFIX}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3Aredis-view-1");
    let second_customer_key =
        format!("{PREFIX}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3Aredis-view-1");

    common::wait_until(
        Duration::from_secs(60),
        "MongoDB Redis view snapshot",
        || async {
            let open = common::redis_value(redis_port, &open_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok());
            let customer = common::redis_value(redis_port, &first_customer_key)
                .await
                .ok()
                .flatten()
                .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok());
            open.is_some_and(|document| {
                document["id"] == "redis-view-1" && document["status"] == "pending"
            }) && customer.is_some_and(|document| document["total"] == 10)
        },
    )
    .await;

    orders
        .update_one(
            doc! {"_id": "redis-view-1"},
            doc! {"$set": {"customer_id": "customer-2", "status": "shipped"}},
        )
        .await
        .expect("move MongoDB view");
    common::wait_until(
        Duration::from_secs(30),
        "MongoDB Redis view move",
        || async {
            common::redis_value(redis_port, &open_key).await == Ok(None)
                && common::redis_value(redis_port, &first_customer_key).await == Ok(None)
                && common::redis_value(redis_port, &second_customer_key)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|value| serde_json::from_slice::<serde_json::Value>(&value).ok())
                    .is_some_and(|document| document["status"] == "shipped")
        },
    )
    .await;

    orders
        .delete_one(doc! {"_id": "redis-view-1"})
        .await
        .expect("delete MongoDB view source");
    common::wait_until(
        Duration::from_secs(30),
        "MongoDB Redis view delete",
        || async { common::redis_value(redis_port, &second_customer_key).await == Ok(None) },
    )
    .await;
}

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
