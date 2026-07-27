//! Live PostgreSQL TLS verification tests.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use tokio_postgres::config::Host;
use ventstream_sources::postgres::{connect_client, PostgresCdcConfig};
use ventstream_sources::{DatabaseTlsConfig, DatabaseTlsMode};

fn source_from_env() -> PostgresCdcConfig {
    let url = std::env::var("VENTSTREAM_TEST_POSTGRES_URL").expect("VENTSTREAM_TEST_POSTGRES_URL");
    let connection_url = url.split_once('?').map_or(url.as_str(), |(base, _)| base);
    let parsed: tokio_postgres::Config = connection_url.parse().expect("parse PostgreSQL test URL");
    let host = parsed
        .get_hosts()
        .first()
        .and_then(|host| match host {
            Host::Tcp(host) => Some(host.clone()),
            Host::Unix(_) => None,
        })
        .expect("TCP host");
    let user = parsed.get_user().expect("user").to_owned();
    let password = parsed
        .get_password()
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .expect("password");
    let database = parsed.get_dbname().expect("database").to_owned();
    let mut source = PostgresCdcConfig::new(
        "tls-test", host, user, password, database, "unused", "unused",
    );
    source.port = parsed.get_ports().first().copied().unwrap_or(5432);
    source.tls = Some(DatabaseTlsConfig {
        mode: DatabaseTlsMode::VerifyFull,
        ca_file: Some(PathBuf::from(
            std::env::var("VENTSTREAM_TEST_POSTGRES_CA_FILE")
                .expect("VENTSTREAM_TEST_POSTGRES_CA_FILE"),
        )),
    });
    source
}

#[tokio::test]
#[ignore = "requires a TLS-enabled PostgreSQL endpoint"]
async fn strict_tls_accepts_the_expected_server() {
    let source = source_from_env();
    let client = connect_client(&source, "TLS integration test")
        .await
        .expect("strict TLS connection");
    let row = client
        .query_one("SELECT 1::int4", &[])
        .await
        .expect("query over strict TLS");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test]
#[ignore = "requires a TLS-enabled PostgreSQL endpoint"]
async fn strict_tls_rejects_an_untrusted_ca() {
    let mut source = source_from_env();
    source.tls.as_mut().expect("TLS").ca_file = Some(PathBuf::from(
        std::env::var("VENTSTREAM_TEST_WRONG_CA_FILE").expect("VENTSTREAM_TEST_WRONG_CA_FILE"),
    ));
    assert!(connect_client(&source, "wrong CA test").await.is_err());
}

#[tokio::test]
#[ignore = "requires a TLS-enabled PostgreSQL endpoint"]
async fn strict_tls_rejects_a_hostname_mismatch() {
    let mut source = source_from_env();
    let address = tokio::net::lookup_host((source.host.as_str(), source.port))
        .await
        .expect("resolve PostgreSQL host")
        .next()
        .expect("resolved address");
    source.host = address.ip().to_string();
    assert!(connect_client(&source, "hostname mismatch test")
        .await
        .is_err());
}
