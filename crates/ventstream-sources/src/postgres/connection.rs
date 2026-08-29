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
            // The DETAIL and HINT usually carry the actionable half — for
            // an invalid slot name the HINT names the allowed characters,
            // for a missing REPLICATION attribute the DETAIL names the
            // attribute — so dropping them leaves the operator with a
            // complaint and no remedy.
            let detail = db
                .detail()
                .map_or_else(String::new, |detail| format!(" (detail: {detail})"));
            let hint = db
                .hint()
                .map_or_else(String::new, |hint| format!(" (hint: {hint})"));
            format!(
                "db error (SQLSTATE {}): {}{detail}{hint}",
                db.code().code(),
                db.message()
            )
        }
        None => error.to_string(),
    }
}

/// Message for a failed `pg_create_logical_replication_slot`, rendered from
/// an already-described server error. Split from
/// [`describe_slot_creation_error`] so the shape can be pinned by a unit
/// test without a live server (a `tokio_postgres::Error` cannot be built
/// by hand).
pub fn slot_creation_message(slot_name: &str, detail: &str) -> String {
    format!("creating slot {slot_name}: {detail}")
}

/// Classify a slot-creation refusal from its *typed* SQLSTATE.
///
/// This is the one seat for the decision. Two codes are terminal here, and
/// only here, because at `pg_create_logical_replication_slot` each can mean
/// exactly one thing:
///
/// - **42602** (invalid_name): the slot name is outside the allowed charset.
///   Reproduced on Postgres 16; reserved-prefix and over-long names were
///   tried and do NOT raise it. The server's HINT names the allowed
///   characters.
/// - **42501** (insufficient_privilege): the connecting role lacks the
///   REPLICATION attribute — a fixed grant. The same code on a table read
///   may be a grant that lands moments later, where retrying is right, which
///   is why this is classified at the site rather than by a global code list.
///
/// Everything else stays a retryable [`PostgresCdcError::Connection`]. The
/// result is a type, not a marker in the text: it maps to
/// [`SourceError::Unrecoverable`](ventstream_core::SourceError::Unrecoverable)
/// and the supervisor walks the error chain for that variant, so reformatting
/// the message cannot silently turn a terminal refusal back into an infinite
/// retry.
///
/// Pure so the classification can be pinned without a live server.
pub fn classify_slot_refusal(sqlstate: Option<&str>, message: String) -> PostgresCdcError {
    match sqlstate {
        Some("42602") => PostgresCdcError::Unrecoverable(message),
        Some("42501") => PostgresCdcError::Unrecoverable(format!(
            "{message} — the connecting role needs the REPLICATION attribute \
             (ALTER ROLE <role> REPLICATION), then restart"
        )),
        _ => PostgresCdcError::Connection(message),
    }
}

/// Error for a failed `pg_create_logical_replication_slot`: the SQLSTATE,
/// message, DETAIL and HINT that tokio-postgres's `Display` discards,
/// classified by [`classify_slot_refusal`].
///
/// Both slot-creating paths — the snapshot bootstrap and the SQL-denormalize
/// `ensure_replication_slot` — go through this one function, so a refusal
/// the server will repeat forever is terminal on both, by type.
pub fn describe_slot_creation_error(
    slot_name: &str,
    error: &tokio_postgres::Error,
) -> PostgresCdcError {
    classify_slot_refusal(
        sqlstate(error),
        slot_creation_message(slot_name, &describe_db_error(error)),
    )
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
#[allow(clippy::expect_used, clippy::panic)]
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

    use ventstream_core::SourceError;

    /// Detail as `describe_db_error` renders Postgres 16's refusal of a slot
    /// name outside the allowed charset (reproduced in
    /// `tests/it_slot_error.rs`).
    const INVALID_NAME_DETAIL: &str =
        "db error (SQLSTATE 42602): replication slot name \"vs_slotS\" contains invalid \
         character (hint: Replication slot names may only contain lower case letters, \
         numbers, and the underscore character.)";

    /// Detail as `describe_db_error` renders Postgres 16's refusal for a
    /// role without REPLICATION (reproduced in `tests/it_slot_error.rs`).
    const NO_REPLICATION_DETAIL: &str =
        "db error (SQLSTATE 42501): permission denied to use replication slots (detail: \
         Only roles with the REPLICATION attribute may use replication slots.)";

    /// The rendered detail passes through untouched — the HINT is what the
    /// operator acts on — and the slot is named.
    #[test]
    fn slot_creation_message_keeps_the_detail_and_names_the_slot() {
        assert_eq!(
            slot_creation_message("vs_slotS", INVALID_NAME_DETAIL),
            format!("creating slot vs_slotS: {INVALID_NAME_DETAIL}")
        );
    }

    /// An invalid slot name is terminal by *type*: the server refuses it
    /// identically on every attempt, so the supervisor must stop rather than
    /// back off forever. The classification survives the hop to the runtime's
    /// error type, which is what the supervisor inspects.
    #[test]
    fn an_invalid_slot_name_is_unrecoverable_by_type() {
        let message = slot_creation_message("vs_slotS", INVALID_NAME_DETAIL);
        let err = classify_slot_refusal(Some("42602"), message.clone());
        let PostgresCdcError::Unrecoverable(text) = &err else {
            panic!("expected Unrecoverable, got: {err}");
        };
        assert_eq!(text, &message, "the message is carried through intact");
        assert!(
            matches!(SourceError::from(err), SourceError::Unrecoverable(_)),
            "the type must survive the conversion the supervisor sees"
        );
    }

    /// A role without REPLICATION is a fixed grant: terminal at this site,
    /// with the remedy appended.
    #[test]
    fn a_role_without_replication_is_unrecoverable_at_the_slot_site() {
        let message = slot_creation_message("vs_slot", NO_REPLICATION_DETAIL);
        let err = classify_slot_refusal(Some("42501"), message.clone());
        let PostgresCdcError::Unrecoverable(text) = &err else {
            panic!("expected Unrecoverable, got: {err}");
        };
        assert!(text.starts_with(&message), "got: {text}");
        assert!(
            text.contains("ALTER ROLE <role> REPLICATION"),
            "remedy missing: {text}"
        );
    }

    /// The direction that matters more: every other code stays a retryable
    /// connection failure with its message untouched. Misclassifying a
    /// recovering server would halt a pipeline that would have healed.
    #[test]
    fn other_codes_stay_retryable_connection_failures() {
        let starting = "db error (SQLSTATE 57P03): the database system is starting up";
        let err = classify_slot_refusal(Some("57P03"), starting.to_owned());
        assert!(
            matches!(&err, PostgresCdcError::Connection(text) if text == starting),
            "got: {err}"
        );
        assert!(matches!(SourceError::from(err), SourceError::Connection(_)));

        let reset = classify_slot_refusal(None, "connection reset".to_owned());
        assert!(
            matches!(&reset, PostgresCdcError::Connection(text) if text == "connection reset"),
            "got: {reset}"
        );
    }
}
