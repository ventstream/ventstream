//! Confirms the related-row fetcher surfaces what Postgres actually said
//! when a SELECT is refused: SQLSTATE and message, not the bare `db error`
//! that tokio-postgres's `Display` collapses to (#184).
//!
//! Needs a Postgres plus a role that can see the schema but has no SELECT
//! on the table. Start one with:
//!
//! ```text
//! docker run -d --name vstest-pg -p 55999:5432 \
//!   -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=x -e POSTGRES_DB=bench \
//!   postgres:16-alpine -c wal_level=logical
//! docker exec vstest-pg psql -U ventstream -d bench \
//!   -c "CREATE ROLE vs_norepl LOGIN PASSWORD 'x';" \
//!   -c "CREATE SCHEMA direct; CREATE TABLE direct.orders(id text PRIMARY KEY, \
//!       status text NOT NULL); GRANT USAGE ON SCHEMA direct TO vs_norepl;"
//! ```
//!
//! Then: `cargo test -p ventstream-sources --test it_fetcher_error -- --ignored`
//! Override the connection with `VS_TEST_PG_HOST` / `VS_TEST_PG_PORT`.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use serde_json::json;
use ventstream_joins::{FetchError, PkValue, RelatedFetcher};
use ventstream_sources::postgres::{PostgresCdcConfig, PostgresFetcher};

fn restricted_role_config() -> PostgresCdcConfig {
    let host = std::env::var("VS_TEST_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("VS_TEST_PG_PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .unwrap_or(55999);
    let mut config =
        PostgresCdcConfig::new("pg", host, "vs_norepl", "x", "bench", "unused", "unused");
    config.port = port;
    config
}

/// A role without SELECT on the table: the fetcher's query is refused
/// with 42501. The error must carry the code and the server's message so
/// the operator sees which grant is missing — and must stay retryable,
/// since a grant applied moments later clears it.
#[tokio::test]
#[ignore = "local: requires the vstest-pg container and the vs_norepl role"]
async fn refused_select_surfaces_sqlstate_and_stays_retryable() {
    let fetcher = PostgresFetcher::connect_config(restricted_role_config())
        .await
        .expect("connect as vs_norepl — see this file's header");

    let err = fetcher
        .fetch_one(
            "direct.orders",
            &["id".to_owned()],
            &PkValue::from_single(&json!("ord-1")),
            &["status".to_owned()],
        )
        .await
        .expect_err("a role without SELECT must be refused");

    let FetchError::Query { table, message } = &err else {
        panic!("expected a query failure, got: {err}");
    };
    assert_eq!(table, "direct.orders");
    assert!(
        message.contains("SQLSTATE 42501"),
        "code missing: {message}"
    );
    assert!(
        message.contains("permission denied for table orders"),
        "server message missing: {message}"
    );
    // The bare Display is what the fetcher used to emit; pin that it
    // really does hide the code, so the change is not redundant.
    assert!(
        !message.trim().eq_ignore_ascii_case("db error"),
        "still the collapsed Display: {message}"
    );
    // 42501 on a table read is a grant that may land later: never terminal.
    assert!(
        !ventstream_sources::credential::is_crash_fast_text(&err.to_string()),
        "a table permission error must stay retryable: {err}"
    );
}

/// The batch path (`fetch_many_batch`) takes its own query branch; it must
/// render the same refusal the same way.
#[tokio::test]
#[ignore = "local: requires the vstest-pg container and the vs_norepl role"]
async fn refused_batch_select_surfaces_sqlstate() {
    let fetcher = PostgresFetcher::connect_config(restricted_role_config())
        .await
        .expect("connect as vs_norepl — see this file's header");

    let err = fetcher
        .fetch_many_batch(
            "direct.orders",
            &["id".to_owned()],
            &[
                PkValue::from_single(&json!("ord-1")),
                PkValue::from_single(&json!("ord-2")),
            ],
            &["status".to_owned()],
        )
        .await
        .expect_err("a role without SELECT must be refused");

    let text = err.to_string();
    assert!(text.contains("SQLSTATE 42501"), "code missing: {text}");
    assert!(
        text.contains("permission denied for table orders"),
        "server message missing: {text}"
    );
}
