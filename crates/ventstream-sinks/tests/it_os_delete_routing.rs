//! #124 against a real OpenSearch: with a time-rolled index template, a
//! delete must remove the document from the index that actually holds it —
//! not report success against today's index while the document lives on.
//!
//! Start the server with:
//!
//! ```text
//! docker run -d --name vstest-os -p 9399:9200 \
//!   -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
//!   -e 'OPENSEARCH_INITIAL_ADMIN_PASSWORD=Vent$tr3am!Pass' \
//!   -e bootstrap.memory_lock=false \
//!   -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' \
//!   opensearchproject/opensearch:2.17.1
//! ```
//!
//! Then: `cargo test -p ventstream-sinks --test it_os_delete_routing -- --ignored`
//! Override the endpoint with `VS_TEST_OS_URL`.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::HashMap;
use std::time::Duration;

use ventstream_core::{ContentType, Event, Headers, Payload, Sink, SinkBatch, SourceUri, Subject};
use ventstream_sinks::opensearch::{OpenSearchConfig, OpenSearchSink};

fn endpoint() -> String {
    std::env::var("VS_TEST_OS_URL").unwrap_or_else(|_| "http://127.0.0.1:9399".to_owned())
}

fn delete_event(doc_id: &str) -> Event {
    let mut headers = HashMap::new();
    headers.insert("ventstream.doc.id".to_owned(), doc_id.to_owned());
    Event::builder(
        SourceUri::new("test://x").expect("uri"),
        Subject::new("postgres.app.orders.delete").expect("subject"),
    )
    .payload(Payload::from_vec(b"{}".to_vec()))
    .content_type(ContentType::Json)
    .headers(Headers::from_map(headers))
    .build()
}

#[tokio::test]
#[ignore = "local: requires the vstest-os container"]
async fn delete_reaches_the_dated_index_holding_the_document() {
    let base = endpoint();
    let http = reqwest::Client::new();
    let doc_id = "app.orders:[\"rolled-1\"]";
    // Two copies across periods — a cross-day update's leftovers. Both
    // must go.
    for index in ["events-2026-08-20", "events-2026-08-21"] {
        let response = http
            .put(format!("{base}/{index}/_doc/{}", urlencode(doc_id)))
            .query(&[("refresh", "true")])
            .json(&serde_json::json!({"status": "stale"}))
            .send()
            .await
            .expect("seed doc — see this file's header for how to start OpenSearch");
        assert!(
            response.status().is_success(),
            "seeding failed: {}",
            response.status()
        );
    }

    let config = OpenSearchConfig::new("it-sink", &base, "events-%Y-%m-%d")
        .with_tombstone_retention(Duration::ZERO);
    let sink = OpenSearchSink::new(config).expect("sink builds");
    sink.write(SinkBatch::new(vec![delete_event(doc_id)]))
        .await
        .expect("delete write");

    for index in ["events-2026-08-20", "events-2026-08-21"] {
        let status = http
            .get(format!("{base}/{index}/_doc/{}", urlencode(doc_id)))
            .send()
            .await
            .expect("lookup")
            .status();
        assert_eq!(
            status.as_u16(),
            404,
            "the document in {index} must be gone — before #124 it survived forever"
        );
    }
}

fn urlencode(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}
