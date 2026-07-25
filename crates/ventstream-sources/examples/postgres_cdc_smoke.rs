#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

//! End-to-end smoke test for the Postgres CDC source.
//!
//! This run-once binary verifies three things against a live Postgres:
//!
//! 1. The full INSERT / UPDATE / DELETE / TRUNCATE pipeline (decode →
//!    schema cache → event mapper → bus).
//! 2. `REPLICA IDENTITY DEFAULT` vs `REPLICA IDENTITY FULL` behavior —
//!    the `users` table has DEFAULT, `orders` has FULL. UPDATEs/DELETEs
//!    on `orders` should carry the complete old row; on `users` only
//!    the primary-key column.
//! 3. `confirmed_flush_lsn` advance — we probe the slot mid-run (well
//!    past the configured status interval) and again after shutdown,
//!    so we can tell whether LSN advance is working at all and whether
//!    shutdown is dropping the last update.
//!
//! Expects a pre-configured cluster on `127.0.0.1:5433` — see the
//! `setup_cluster.sh` block printed at the bottom of the source if the
//! test fails to connect.

use std::time::Duration;

use ventstream_core::{EventBus, ShutdownToken, Source, SourceContext};
use ventstream_sources::postgres::{PostgresCdcConfig, PostgresCdcSource};

const STATUS_INTERVAL: Duration = Duration::from_secs(2);
const MID_RUN_PROBE_DELAY: Duration = Duration::from_secs(6);
const TOTAL_RUNTIME: Duration = Duration::from_secs(12);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let initial = probe_slot().await?;
    println!("== initial slot state ==");
    println!("{initial}");

    let config = PostgresCdcConfig::new(
        "smoke",
        "127.0.0.1",
        "vstest",
        "vstest",
        "vsdb",
        "vspub",
        "vsslot",
    )
    .with_port(5433)
    .with_status_interval(STATUS_INTERVAL);

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
        let mut events = Vec::new();
        loop {
            tokio::select! {
                () = watch_shutdown.cancelled() => break,
                event = receiver.recv() => match event {
                    Some(event) => {
                        let payload = String::from_utf8_lossy(event.payload.as_slice()).to_string();
                        println!("event: subject={} payload={}", event.subject, payload);
                        events.push((event.subject.as_str().to_owned(), payload));
                    }
                    None => break,
                }
            }
        }
        events
    });

    tokio::time::sleep(MID_RUN_PROBE_DELAY).await;
    println!("== mid-run slot state (well past one status interval, source still alive) ==");
    println!("{}", probe_slot().await?);

    tokio::time::sleep(TOTAL_RUNTIME - MID_RUN_PROBE_DELAY).await;
    shutdown.cancel();

    let _ = driver.await;
    let events = watcher.await.unwrap_or_default();
    let source_result = source_handle.await;

    // After shutdown — give Postgres a moment to register the final flush,
    // then probe again so we can compare mid-run vs post-shutdown LSNs.
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("== post-shutdown slot state ==");
    println!("{}", probe_slot().await?);

    println!("---");
    println!("observed {} event(s)", events.len());
    match source_result {
        Ok(Ok(())) => println!("source exited cleanly"),
        Ok(Err(err)) => println!("source returned error: {err}"),
        Err(join_err) => println!("source task panicked: {join_err}"),
    }

    verify_replica_identity_behaviour(&events);

    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,ventstream_sources=info,pgwire_replication=info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn probe_slot() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let output = tokio::process::Command::new("psql")
        .args([
            "-h",
            "127.0.0.1",
            "-p",
            "5433",
            "-U",
            "postgres",
            "-d",
            "vsdb",
            "-At",
            "-F",
            ",",
            "-c",
            "SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='vsslot';",
        ])
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn exercise_tables() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let statements = [
        "INSERT INTO users (email, full_name) VALUES ('alice@example.com', 'Alice');",
        "INSERT INTO users (email, full_name) VALUES ('bob@example.com', NULL);",
        "UPDATE users SET full_name = 'Robert' WHERE email = 'bob@example.com';",
        "DELETE FROM users WHERE email = 'alice@example.com';",
        // Now `orders`, which has REPLICA IDENTITY FULL — UPDATE/DELETE
        // should carry the full old row.
        "INSERT INTO orders (user_id, total) VALUES (1, 99.99);",
        "UPDATE orders SET total = 149.99 WHERE user_id = 1;",
        "DELETE FROM orders WHERE user_id = 1;",
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

fn verify_replica_identity_behaviour(events: &[(String, String)]) {
    println!();
    println!("== replica identity check ==");

    let orders_update = events
        .iter()
        .find(|(subject, _)| subject == "postgres.public.orders.update");
    match orders_update {
        Some((_, payload)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(payload).expect("orders.update payload is JSON");
            let old = &parsed["old"];
            let total_in_old = old.get("total").cloned();
            if total_in_old.is_some() && !old["total"].is_null() {
                println!(
                    "PASS: orders.update old.total = {} (REPLICA IDENTITY FULL works)",
                    old["total"]
                );
            } else {
                println!("FAIL: orders.update old.total missing or null: {old}");
            }
        }
        None => println!("WARN: no orders.update event observed"),
    }

    let users_update = events
        .iter()
        .find(|(subject, _)| subject == "postgres.public.users.update");
    match users_update {
        Some((_, payload)) => {
            let parsed: serde_json::Value =
                serde_json::from_str(payload).expect("users.update payload is JSON");
            let old = &parsed["old"];
            if old.is_null() {
                println!("PASS: users.update old is null (REPLICA IDENTITY DEFAULT, no PK change)");
            } else {
                println!(
                    "INFO: users.update old = {old}  (key column captured under DEFAULT identity)"
                );
            }
        }
        None => println!("WARN: no users.update event observed"),
    }
}
