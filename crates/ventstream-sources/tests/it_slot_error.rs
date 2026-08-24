//! Confirms the slot-creation error path surfaces what Postgres actually
//! said. Needs a local Postgres with logical replication on port 55999.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[tokio::test]
#[ignore = "local: requires the vstest-pg container"]
async fn invalid_slot_name_surfaces_sqlstate_message_and_hint() {
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=55999 user=ventstream password=x dbname=bench",
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
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
