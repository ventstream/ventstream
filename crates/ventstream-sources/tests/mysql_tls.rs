//! Live MySQL TLS verification tests.

#![allow(clippy::expect_used)]

use std::path::PathBuf;

use mysql_async::prelude::Queryable;
use ventstream_sources::{DatabaseTlsConfig, DatabaseTlsMode, MySqlCdcConfig};

fn source(host: &str, ca_file: &str) -> MySqlCdcConfig {
    let mut source = MySqlCdcConfig::new(
        "tls-test",
        host,
        std::env::var("VENTSTREAM_TEST_MYSQL_USER").expect("VENTSTREAM_TEST_MYSQL_USER"),
        std::env::var("VENTSTREAM_TEST_MYSQL_PASSWORD").expect("VENTSTREAM_TEST_MYSQL_PASSWORD"),
        std::env::var("VENTSTREAM_TEST_MYSQL_DATABASE").expect("VENTSTREAM_TEST_MYSQL_DATABASE"),
        "/tmp/mysql-tls-test",
    );
    source.port = std::env::var("VENTSTREAM_TEST_MYSQL_PORT")
        .expect("VENTSTREAM_TEST_MYSQL_PORT")
        .parse()
        .expect("MySQL port");
    source.tls = Some(DatabaseTlsConfig {
        mode: DatabaseTlsMode::VerifyFull,
        ca_file: Some(PathBuf::from(ca_file)),
    });
    source
}

#[tokio::test]
#[ignore = "requires a TLS-enabled MySQL endpoint"]
async fn strict_tls_accepts_the_expected_server() {
    let source = source(
        "localhost",
        &std::env::var("VENTSTREAM_TEST_MYSQL_CA_FILE").expect("VENTSTREAM_TEST_MYSQL_CA_FILE"),
    );
    let mut connection = mysql_async::Conn::new(source.opts())
        .await
        .expect("strict TLS connection");
    let status: Option<(String, String)> = connection
        .query_first("SHOW STATUS LIKE 'Ssl_cipher'")
        .await
        .expect("read TLS status");
    assert!(!status.expect("TLS status row").1.is_empty());
}

#[tokio::test]
#[ignore = "requires a TLS-enabled MySQL endpoint"]
async fn strict_tls_rejects_an_untrusted_ca() {
    let source = source(
        "localhost",
        &std::env::var("VENTSTREAM_TEST_WRONG_CA_FILE").expect("VENTSTREAM_TEST_WRONG_CA_FILE"),
    );
    assert!(mysql_async::Conn::new(source.opts()).await.is_err());
}

#[tokio::test]
#[ignore = "requires a TLS-enabled MySQL endpoint"]
async fn strict_tls_rejects_a_hostname_mismatch() {
    let source = source(
        "127.0.0.1",
        &std::env::var("VENTSTREAM_TEST_MYSQL_CA_FILE").expect("VENTSTREAM_TEST_MYSQL_CA_FILE"),
    );
    assert!(mysql_async::Conn::new(source.opts()).await.is_err());
}
