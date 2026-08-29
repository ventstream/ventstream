//! Confirms the slot-creation error path surfaces what Postgres actually
//! said: SQLSTATE, message, DETAIL and HINT, and a terminal classification.
//!
//! Needs a Postgres with logical replication enabled, plus a role without
//! the REPLICATION attribute for the 42501 case. Start one with:
//!
//! ```text
//! docker run -d --name vstest-pg -p 55999:5432 \
//!   -e POSTGRES_USER=ventstream -e POSTGRES_PASSWORD=x -e POSTGRES_DB=bench \
//!   postgres:16-alpine -c wal_level=logical
//! docker exec vstest-pg psql -U ventstream -d bench \
//!   -c "CREATE ROLE vs_norepl LOGIN PASSWORD 'x';"
//! ```
//!
//! Then: `cargo test -p ventstream-sources --test it_slot_error -- --ignored`
//! Override the connections with `VS_TEST_PG_URL` / `VS_TEST_PG_NOREPL_URL`
//! if you have them already.
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

/// The helper both slot-creating paths render through (snapshot bootstrap
/// and the SQL-denormalize `ensure_replication_slot`). With a real server
/// error it must produce the text the unit tests assume: slot name, SQLSTATE,
/// message and HINT, and a terminal classification.
#[tokio::test]
#[ignore = "local: requires the vstest-pg container"]
async fn slot_creation_helper_renders_real_refusal_as_terminal() {
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
    let message = ventstream_sources::postgres::describe_slot_creation_error("vs_slotS", &err);
    assert!(
        message.starts_with("creating slot vs_slotS: db error (SQLSTATE 42602): "),
        "got: {message}"
    );
    assert!(
        message.contains("lower case letters"),
        "hint missing: {message}"
    );
    assert!(
        ventstream_sources::credential::is_crash_fast_text(&message),
        "must classify terminal: {message}"
    );
    // The raw Display is what the SQL-denormalize path used to emit; pin
    // that it really does hide the code, so the helper is not redundant.
    assert!(
        !ventstream_sources::credential::is_crash_fast_text(&err.to_string()),
        "raw Display unexpectedly carries the SQLSTATE now: {err}"
    );
}

/// #177: a role without REPLICATION is refused with 42501 and a DETAIL
/// naming the attribute. The helper classifies it terminal at the slot
/// site and carries the remedy; the plain description — the same code as
/// it would surface anywhere else — stays unclassified, because 42501 is
/// deliberately not matched globally.
#[tokio::test]
#[ignore = "local: requires the vstest-pg container and the vs_norepl role"]
async fn role_without_replication_is_terminal_at_the_slot_site_only() {
    let url = std::env::var("VS_TEST_PG_NOREPL_URL").unwrap_or_else(|_| {
        "host=127.0.0.1 port=55999 user=vs_norepl password=x dbname=bench".to_owned()
    });
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect as vs_norepl — see this file's header for the CREATE ROLE");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let err = client
        .batch_execute("SELECT pg_create_logical_replication_slot('vs_norepl_slot', 'pgoutput')")
        .await
        .expect_err("a role without REPLICATION must be refused");
    assert_eq!(
        ventstream_sources::postgres::sqlstate(&err),
        Some("42501"),
        "expected insufficient_privilege: {err}"
    );

    let described = ventstream_sources::postgres::describe_db_error(&err);
    assert!(
        described.contains("permission denied to use replication slots"),
        "got: {described}"
    );
    assert!(
        described.contains("REPLICATION attribute"),
        "DETAIL missing: {described}"
    );
    assert!(
        !ventstream_sources::credential::is_crash_fast_text(&described),
        "42501 must not be terminal globally: {described}"
    );

    let message =
        ventstream_sources::postgres::describe_slot_creation_error("vs_norepl_slot", &err);
    assert!(
        message.starts_with("creating slot vs_norepl_slot: db error (SQLSTATE 42501): "),
        "got: {message}"
    );
    assert!(
        message.contains("ALTER ROLE <role> REPLICATION"),
        "remedy missing: {message}"
    );
    assert!(
        ventstream_sources::credential::is_crash_fast_text(&message),
        "must classify terminal at the slot site: {message}"
    );
}
