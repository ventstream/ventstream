//! Reproducible OpenSearch NDJSON serialization throughput harness.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use chrono::{TimeZone, Utc};
use ventstream_core::{ContentType, Event, Headers, Payload, SourceUri, Subject};
use ventstream_sinks::opensearch::{bulk, index_template};

const DEFAULT_BATCH_EVENTS: usize = 2_000;
const DEFAULT_PAYLOAD_BYTES: usize = 512;
const DEFAULT_ROUNDS: usize = 2_000;

fn arg_usize(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn make_events(count: usize, payload_bytes: usize) -> Vec<Event> {
    let source = SourceUri::new("bench://bulk-builder").unwrap_or_else(|error| {
        eprintln!("invalid benchmark source URI: {error}");
        std::process::exit(2);
    });
    let subject = Subject::new("postgres.public.orders.update").unwrap_or_else(|error| {
        eprintln!("invalid benchmark subject: {error}");
        std::process::exit(2);
    });
    (0..count)
        .map(|position| {
            let mut headers = HashMap::new();
            headers.insert(
                "ventstream.doc.id".to_owned(),
                format!("public.orders:[\"order-{position}\"]"),
            );
            headers.insert(
                "ventstream.cdc.lsn".to_owned(),
                (24_000_000_u64 + position as u64).to_string(),
            );
            let payload = format!(
                "{{\"id\":{position},\"padding\":\"{}\"}}",
                "x".repeat(payload_bytes.saturating_sub(32))
            );
            Event::builder(source.clone(), subject.clone())
                .payload(Payload::from_vec(payload.into_bytes()))
                .content_type(ContentType::Json)
                .headers(Headers::from_map(headers))
                .build()
        })
        .collect()
}

fn legacy_bulk_body(events: &[Event], template: &str) -> Vec<u8> {
    let now = Utc
        .with_ymd_and_hms(2026, 7, 20, 12, 0, 0)
        .single()
        .unwrap_or_else(|| {
            eprintln!("invalid fixed benchmark timestamp");
            std::process::exit(2);
        });
    let mut body = Vec::with_capacity(events.len() * 256);
    let mut indices = Vec::with_capacity(events.len());
    for event in events {
        let index = index_template::render(template, event, now).unwrap_or_else(|error| {
            eprintln!("index render failed: {error}");
            std::process::exit(2);
        });
        let id = event
            .headers
            .get("ventstream.doc.id")
            .map(str::to_owned)
            .unwrap_or_else(|| event.id.to_string());
        let version = event
            .headers
            .get("ventstream.cdc.lsn")
            .and_then(|raw| raw.parse::<u64>().ok());
        let mut metadata = serde_json::Map::with_capacity(4);
        metadata.insert("_index".into(), serde_json::Value::String(index.clone()));
        metadata.insert("_id".into(), serde_json::Value::String(id));
        if let Some(version) = version {
            metadata.insert("version".into(), serde_json::Value::from(version));
            metadata.insert(
                "version_type".into(),
                serde_json::Value::String("external_gte".into()),
            );
        }
        let action = serde_json::json!({ "index": metadata });
        if let Err(error) = serde_json::to_writer(&mut body, &action) {
            eprintln!("legacy action serialization failed: {error}");
            std::process::exit(2);
        }
        body.push(b'\n');
        body.extend_from_slice(event.payload.as_slice());
        body.push(b'\n');
        indices.push(index);
    }
    black_box(indices);
    body
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "body".into());
    let batch_events = arg_usize(2, DEFAULT_BATCH_EVENTS).max(1);
    let payload_bytes = arg_usize(3, DEFAULT_PAYLOAD_BYTES);
    let rounds = arg_usize(4, DEFAULT_ROUNDS).max(1);
    let template = std::env::args()
        .nth(5)
        .unwrap_or_else(|| "orders-current".into());
    let events = make_events(batch_events, payload_bytes);
    let now = Utc
        .with_ymd_and_hms(2026, 7, 20, 12, 0, 0)
        .single()
        .unwrap_or_else(|| {
            eprintln!("invalid fixed benchmark timestamp");
            std::process::exit(2);
        });

    let started = Instant::now();
    let mut encoded_bytes = 0usize;
    for _ in 0..rounds {
        let body = match mode.as_str() {
            "legacy" => legacy_bulk_body(&events, &template),
            "request" => {
                bulk::build_bulk_request(&events, &template, now)
                    .unwrap_or_else(|error| {
                        eprintln!("request build failed: {error}");
                        std::process::exit(2);
                    })
                    .body
            }
            "body" => bulk::build_bulk_body(&events, &template, now).unwrap_or_else(|error| {
                eprintln!("body build failed: {error}");
                std::process::exit(2);
            }),
            other => {
                eprintln!("unknown mode '{other}'; expected legacy, request, or body");
                std::process::exit(2);
            }
        };
        encoded_bytes = encoded_bytes.wrapping_add(black_box(body.len()));
    }

    let elapsed = started.elapsed();
    let documents = batch_events.saturating_mul(rounds);
    let documents_per_second = documents as f64 / elapsed.as_secs_f64();
    let mib_per_second = encoded_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "mode={mode} documents={documents} batch_events={batch_events} payload_bytes={payload_bytes} \
         rounds={rounds} elapsed_s={:.6} documents_per_s={:.0} encoded_mib_per_s={:.1} checksum={encoded_bytes}",
        elapsed.as_secs_f64(),
        documents_per_second,
        mib_per_second,
    );
}
