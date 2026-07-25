#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

//! Live edge-case verification for the Postgres CDC source.
//!
//! Drives a publication that includes:
//! - A table in a custom schema (`app.products`)
//! - A table in a schema whose name contains a dot, and whose own name
//!   contains a space (`"weird.schema"."odd table"`) — Postgres allows
//!   these via double-quoted identifiers.
//!
//! Asserts that:
//! - Subjects are sanitized (no embedded dots, no spaces).
//! - Source URIs percent-encode unsafe characters but keep dots literal
//!   (per RFC 3986 unreserved).
//! - Raw identifiers + relation oid are preserved verbatim in headers.

use std::time::Duration;

use ventstream_core::{EventBus, ShutdownToken, Source, SourceContext};
use ventstream_sources::postgres::{PostgresCdcConfig, PostgresCdcSource};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let config = PostgresCdcConfig::new(
        "edge",
        "127.0.0.1",
        "vstest",
        "vstest",
        "vsdb",
        "vspub",
        "vsslot",
    )
    .with_port(5433)
    .with_status_interval(Duration::from_secs(2));

    let source = PostgresCdcSource::new(config);
    let bus = EventBus::new(64);
    let (sender, mut receiver) = bus.split();
    let shutdown = ShutdownToken::new();
    let ctx = SourceContext::new(sender, shutdown.clone());

    let source_handle = tokio::spawn(async move { source.run(ctx).await });

    let driver = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Err(err) = exercise_tables().await {
            eprintln!("driver error: {err}");
        }
    });

    let watch_shutdown = shutdown.clone();
    let watcher = tokio::spawn(async move {
        let mut events: Vec<ventstream_core::Event> = Vec::new();
        loop {
            tokio::select! {
                () = watch_shutdown.cancelled() => break,
                event = receiver.recv() => match event {
                    Some(event) => {
                        let payload = String::from_utf8_lossy(event.payload.as_slice()).to_string();
                        println!(
                            "event: subject={} source={} payload={}",
                            event.subject, event.source, payload
                        );
                        events.push(event);
                    }
                    None => break,
                }
            }
        }
        events
    });

    tokio::time::sleep(Duration::from_secs(5)).await;
    shutdown.cancel();
    let _ = driver.await;
    let events = watcher.await.unwrap_or_default();
    let source_result = source_handle.await;

    println!("---");
    println!("observed {} event(s)", events.len());
    match source_result {
        Ok(Ok(())) => println!("source exited cleanly"),
        Ok(Err(err)) => println!("source returned error: {err}"),
        Err(join_err) => println!("source task panicked: {join_err}"),
    }

    verify_edge_cases(&events);
    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,ventstream_sources=info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn exercise_tables() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let statements = [
        "INSERT INTO app.products (name, price) VALUES ('widget', 9.99);",
        r#"INSERT INTO "weird.schema"."odd table" (label) VALUES ('hello');"#,
        r#"UPDATE "weird.schema"."odd table" SET label = 'world' WHERE label = 'hello';"#,
        r#"DELETE FROM "weird.schema"."odd table" WHERE label = 'world';"#,
    ];
    for sql in statements {
        let status = tokio::process::Command::new("psql")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                "5433",
                "-U",
                "postgres",
                "-d",
                "vsdb",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ])
            .status()
            .await?;
        if !status.success() {
            return Err(format!("psql exited with status {status} for `{sql}`").into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

fn verify_edge_cases(events: &[ventstream_core::Event]) {
    println!();
    println!("== edge case verification ==");

    // 1. Custom schema `app.products` insert
    let app_insert = events
        .iter()
        .find(|e| e.subject.as_str() == "postgres.app.products.insert");
    match app_insert {
        Some(e) => {
            println!(
                "PASS: custom schema → subject={} source={} header_ns={:?} header_oid={:?}",
                e.subject,
                e.source,
                e.headers.get("ventstream.cdc.namespace"),
                e.headers.get("ventstream.cdc.relation_oid"),
            );
            assert!(e
                .source
                .as_str()
                .starts_with("postgres://vspub/app/products"));
            assert_eq!(e.headers.get("ventstream.cdc.namespace"), Some("app"));
            assert_eq!(e.headers.get("ventstream.cdc.relation"), Some("products"));
        }
        None => println!("FAIL: missing app.products.insert event"),
    }

    // 2. Dotted schema + spaced table name → sanitized
    let weird_subjects: Vec<_> = events
        .iter()
        .filter(|e| {
            e.headers.get("ventstream.cdc.namespace") == Some("weird.schema")
                && e.headers.get("ventstream.cdc.relation") == Some("odd table")
        })
        .collect();

    if weird_subjects.is_empty() {
        println!("FAIL: no events from 'weird.schema'.'odd table' observed");
    } else {
        for e in &weird_subjects {
            let subject = e.subject.as_str();
            let source = e.source.as_str();
            println!("event: subject={subject} source={source}");

            // Subject must be sanitized — no dots inside identifier segments, no spaces.
            let parts: Vec<&str> = subject.split('.').collect();
            assert_eq!(parts.len(), 4, "subject must have exactly 4 segments");
            assert_eq!(parts[0], "postgres");
            assert_eq!(
                parts[1], "weird_schema",
                "dotted schema must sanitize to underscore"
            );
            assert_eq!(
                parts[2], "odd_table",
                "spaced table name must sanitize to underscore"
            );

            // Source URI: dots are unreserved (kept), spaces are encoded as %20.
            assert!(
                source.contains("/weird.schema/odd%20table"),
                "source URI keeps literal dot, percent-encodes space: {source}"
            );

            // Headers preserve the raw values verbatim.
            assert_eq!(
                e.headers.get("ventstream.cdc.namespace"),
                Some("weird.schema")
            );
            assert_eq!(e.headers.get("ventstream.cdc.relation"), Some("odd table"));
            assert!(e.headers.get("ventstream.cdc.relation_oid").is_some());
        }
        println!("PASS: dotted schema + spaced table → subject sanitized, URI percent-encoded, raw values preserved");
    }
}
