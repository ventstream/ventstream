//! Kafka/Redpanda CDC integration test: raw events, durable consumer-group
//! offsets, engine restart, and a real OpenSearch sink.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod common;

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

const INDEX: &str = "it_kafka_orders";
const DOC_ID: &str = r#"shop.orders:["ord-1"]"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "local integration: requires Docker; run with scripts/test-sources.sh kafka"]
async fn kafka_live_change_and_consumer_offset_restart() {
    let stack = common::start_kafka_os().await;
    let brokers = format!("127.0.0.1:{}", stack.kafka_port);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create kafka admin client");
    admin
        .create_topics(
            &[NewTopic::new("orders", 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create kafka topic")
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("kafka topic result");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create kafka producer");
    publish(&producer, "created").await;

    let state_dir = common::state_dir("kafka", stack.kafka_port);
    let options = common::KafkaEngine {
        kafka_port: stack.kafka_port,
        os_port: stack.os_port,
        state_dir: &state_dir,
        index_template: INDEX,
        group_id: "ventstream-it-orders",
    };
    let mut engine = common::spawn_kafka_engine(&options);
    common::wait_until(
        Duration::from_secs(60),
        "kafka replayed document",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "created")
        },
    )
    .await;

    publish(&producer, "live-updated").await;
    common::wait_until(Duration::from_secs(30), "kafka live update", || async {
        common::os_doc(stack.os_port, INDEX, DOC_ID)
            .await
            .is_some_and(|doc| doc["status"] == "live-updated")
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    engine.terminate();
    publish(&producer, "updated-while-stopped").await;
    let _restarted = common::spawn_kafka_engine(&options);
    common::wait_until(
        Duration::from_secs(90),
        "kafka resume after restart",
        || async {
            common::os_doc(stack.os_port, INDEX, DOC_ID)
                .await
                .is_some_and(|doc| doc["status"] == "updated-while-stopped")
        },
    )
    .await;
}

async fn publish(producer: &FutureProducer, status: &str) {
    let payload = format!(r#"{{"id":"ord-1","status":"{status}"}}"#);
    producer
        .send(
            FutureRecord::to("orders")
                .key(r#""ord-1""#)
                .payload(&payload),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .expect("publish kafka record");
}
