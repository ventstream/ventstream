//! Confirms the slot-creation error path surfaces what Postgres actually
//! said: SQLSTATE, message and HINT, and a terminal classification.
//!
//! Needs a Postgres with logical replication enabled. Start one with:
//!
//! ```text
//! docker run -d --name vstest-pg -p 55999:5432 \
//!   -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=x -e POSTGRES_DB=bench \
//!   postgres:16-alpine -c wal_level=logical
//! ```
//!
//! Then: `cargo test -p ventstream-sources --test it_slot_error -- --ignored`
//! Override the connection with `VS_TEST_PG_URL` if you have one already.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[tokio::test]
#[ignore = "local: requires the vstest-pg container"]
async fn invalid_slot_name_surfaces_sqlstate_message_and_hint() {
    let url = std::env::var("VS_TEST_PG_URL").unwrap_or_else(|_| {
        "host=127.0.0.1 port=55999 user=ventstream password=x dbname=bench".to_owned()
    });
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect — see this file's header for how to start Postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let err = client
        .batch_execute("SELECT pg_create_logical_replication_slot('vs_slotS', 'pgoutput')")
        .await
        .expect_err("invalid slot name must fail");
    let described = ventstream_sources::postgres::connection::describe_db_error(&err);
    assert!(described.contains("SQLSTATE 42602"), "got: {described}");
    assert!(
        described.contains("contains invalid character"),
        "got: {described}"
    );
    assert!(
        described.contains("lower case letters"),
        "hint missing: {described}"
    );
    assert!(
        ventstream_sources::credential::is_crash_fast_text(&described),
        "must classify terminal: {described}"
    );
}
