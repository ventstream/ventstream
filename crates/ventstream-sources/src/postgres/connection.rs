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

/// SQLSTATE of the underlying server error, when present. tokio-postgres
/// collapses server errors to a bare "db error" in Display, hiding it.
pub fn sqlstate(error: &tokio_postgres::Error) -> Option<&str> {
    error.as_db_error().map(|db| db.code().code())
}

/// Credential SQLSTATEs that cannot heal in-process: 28000
/// invalid_authorization_specification, 28P01 invalid_password.
pub fn is_credential_sqlstate(code: &str) -> bool {
    matches!(code, "28000" | "28P01")
}

/// True when the structured error carries a credential SQLSTATE.
pub fn is_credential_db_error(error: &tokio_postgres::Error) -> bool {
    sqlstate(error).is_some_and(is_credential_sqlstate)
}

/// Expand the collapsed "db error" Display with SQLSTATE and message so
/// downstream string classifiers and operators see the real cause.
pub fn describe_db_error(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => {
            // The HINT usually carries the actionable half — for an invalid
            // slot name it names the allowed characters — so dropping it
            // leaves the operator with a complaint and no remedy.
            let hint = db
                .hint()
                .map_or_else(String::new, |hint| format!(" (hint: {hint})"));
            format!(
                "db error (SQLSTATE {}): {}{hint}",
                db.code().code(),
                db.message()
            )
        }
        None => error.to_string(),
    }
}

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
            let (client, connection) = match config.connect(NoTls).await {
                Ok(pair) => pair,
                Err(err) => return Err(connect_error(source, purpose, &err).await),
            };
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
            let (client, connection) = match config.connect(connector).await {
                Ok(pair) => pair,
                Err(err) => return Err(connect_error(source, purpose, &err).await),
            };
            tokio::spawn(async move {
                if let Err(err) = connection.await {
                    warn!(error = %err, purpose, "postgres TLS connection ended");
                }
            });
            Ok(client)
        }
    }
}

/// Expand a connect failure and, when the target is IPv6-only, append
/// the routing hint — this path sees every non-replication connect
/// (preflight, slot creation, fetcher), which on some platforms is
/// where an IPv4-only network fails first.
async fn connect_error(
    source: &PostgresCdcConfig,
    purpose: &str,
    err: &tokio_postgres::Error,
) -> PostgresCdcError {
    let mut detail = format!("{purpose}: {}", describe_db_error(err));
    if let Some(hint) = super::preflight::unreachable_hint(&source.host, source.port, &detail).await
    {
        detail = format!("{detail} ({hint})");
    }
    PostgresCdcError::Connection(detail)
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
    } else if crate::tls::implicit_trust_provider(&source.host).is_some() {
        // Known provider on a private CA (Supabase) and no operator
        // override — trust the packaged root so verify_full works
        // without a manual certificate download.
        let certs = rustls_pemfile::certs(&mut BufReader::new(crate::tls::SUPABASE_CA_PEM))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| {
                PostgresCdcError::Connection(format!(
                    "{purpose}: parse packaged Supabase CA: {err}"
                ))
            })?;
        roots.add_parsable_certificates(certs);
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
