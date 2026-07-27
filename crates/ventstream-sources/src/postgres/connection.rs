//! TLS-aware PostgreSQL connections shared by CDC auxiliary paths.

use std::fs::File;
use std::io::BufReader;

use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;

use super::config::PostgresCdcConfig;
use crate::error::PostgresCdcError;
use crate::tls::{ensure_crypto_provider, DatabaseTlsMode};

/// Open and drive a PostgreSQL client using the source's transport policy.
pub async fn connect_client(
    source: &PostgresCdcConfig,
    purpose: &'static str,
) -> Result<Client, PostgresCdcError> {
    let mut config: tokio_postgres::Config = source.connection_string().parse().map_err(|err| {
        PostgresCdcError::Connection(format!("{purpose}: invalid postgres config: {err}"))
    })?;

    match source.tls.as_ref().map(|tls| tls.mode) {
        None | Some(DatabaseTlsMode::Disabled) => {
            config.ssl_mode(SslMode::Disable);
            let (client, connection) = config
                .connect(NoTls)
                .await
                .map_err(|err| PostgresCdcError::Connection(format!("{purpose}: {err}")))?;
            tokio::spawn(async move {
                if let Err(err) = connection.await {
                    warn!(error = %err, purpose, "postgres connection ended");
                }
            });
            Ok(client)
        }
        Some(DatabaseTlsMode::VerifyFull) => {
            config.ssl_mode(SslMode::Require);
            let connector = strict_tls_connector(source, purpose)?;
            let (client, connection) = config
                .connect(connector)
                .await
                .map_err(|err| PostgresCdcError::Connection(format!("{purpose}: {err}")))?;
            tokio::spawn(async move {
                if let Err(err) = connection.await {
                    warn!(error = %err, purpose, "postgres TLS connection ended");
                }
            });
            Ok(client)
        }
    }
}

fn strict_tls_connector(
    source: &PostgresCdcConfig,
    purpose: &'static str,
) -> Result<MakeRustlsConnect, PostgresCdcError> {
    ensure_crypto_provider();
    let native = rustls_native_certs::load_native_certs();
    if native.certs.is_empty() && !native.errors.is_empty() {
        let detail = native
            .errors
            .first()
            .map(ToString::to_string)
            .unwrap_or_else(|| "unknown system certificate error".to_owned());
        return Err(PostgresCdcError::Connection(format!(
            "{purpose}: failed to load system CA certificates: {detail}"
        )));
    }

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(native.certs);
    if let Some(path) = source.tls.as_ref().and_then(|tls| tls.ca_file.as_ref()) {
        let file = File::open(path).map_err(|err| {
            PostgresCdcError::Connection(format!(
                "{purpose}: open TLS CA bundle {}: {err}",
                path.display()
            ))
        })?;
        let certs = rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                PostgresCdcError::Connection(format!(
                    "{purpose}: parse TLS CA bundle {}: {err}",
                    path.display()
                ))
            })?;
        if certs.is_empty() {
            return Err(PostgresCdcError::Connection(format!(
                "{purpose}: TLS CA bundle {} contains no certificates",
                path.display()
            )));
        }
        let (added, ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(PostgresCdcError::Connection(format!(
                "{purpose}: TLS CA bundle {} contains no usable certificates ({ignored} ignored)",
                path.display()
            )));
        }
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::tls::{DatabaseTlsConfig, DatabaseTlsMode};

    #[test]
    fn strict_tls_rejects_a_missing_ca_bundle() {
        let mut source =
            PostgresCdcConfig::new("pg", "db.example.com", "u", "p", "app", "pub", "slot");
        source.tls = Some(DatabaseTlsConfig {
            mode: DatabaseTlsMode::VerifyFull,
            ca_file: Some(std::env::temp_dir().join("ventstream-missing-ca.pem")),
        });
        let result = strict_tls_connector(&source, "test");
        assert!(
            result
                .as_ref()
                .is_err_and(|err| err.to_string().contains("open TLS CA bundle")),
            "missing CA must fail with a file-open error"
        );
    }
}
