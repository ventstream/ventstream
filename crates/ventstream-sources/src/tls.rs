//! Shared database transport-security policy.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Authoritative source for the AWS RDS global CA bundle packaged with VentStream.
pub const AWS_RDS_GLOBAL_CA_SOURCE: &str =
    "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem";

/// SHA-256 of the packaged AWS RDS global CA bundle.
pub const AWS_RDS_GLOBAL_CA_SHA256: &str =
    "e5bb2084ccf45087bda1c9bffdea0eb15ee67f0b91646106e466714f9de3c7e3";

const AWS_RDS_GLOBAL_CA_PEM: &[u8] = include_bytes!("../certs/aws-rds-global-bundle.pem");
static AWS_RDS_GLOBAL_CA_PATH: OnceLock<Result<PathBuf, String>> = OnceLock::new();
static TRUST_BUNDLE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Provenance of the Supabase root CA packaged with VentStream. Every
/// Supabase database serves it as the last certificate of its chain; it is
/// also downloadable from the project's database settings page.
pub const SUPABASE_CA_SOURCE: &str = "Supabase Root 2021 CA (expires 2031-04-26)";

/// SHA-256 of the packaged Supabase root CA.
pub const SUPABASE_CA_SHA256: &str =
    "700723581420dd1ac98fd7e9ac529f0ef210eadcaf87fc868a3ad7d114c2f3b7";

/// The packaged Supabase root CA, exposed so in-memory TLS paths can add it
/// to their root store without a filesystem round trip.
pub const SUPABASE_CA_PEM: &[u8] = include_bytes!("../certs/supabase-root-2021.pem");
static SUPABASE_CA_PATH: OnceLock<Result<PathBuf, String>> = OnceLock::new();

/// Provider whose packaged CA should be trusted implicitly for `host` when
/// the operator supplied no `ca_file` or `trust` of their own. Supabase
/// signs with a private CA no system store carries; without this,
/// verify_full would force every Supabase user through a manual
/// certificate download.
pub fn implicit_trust_provider(host: &str) -> Option<DatabaseTlsTrustProvider> {
    host.to_ascii_lowercase()
        .ends_with(".supabase.co")
        .then_some(DatabaseTlsTrustProvider::Supabase)
}

/// TLS policy applied consistently to every connection opened by a source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseTlsConfig {
    /// Transport mode.
    pub mode: DatabaseTlsMode,
    /// Optional PEM CA bundle for a private certificate authority.
    pub ca_file: Option<PathBuf>,
}

/// Database TLS modes supported by the engine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DatabaseTlsMode {
    /// Require TLS and validate both the certificate chain and hostname.
    #[default]
    VerifyFull,
    /// Use an unencrypted connection.
    Disabled,
}

/// Maintained provider trust bundles packaged with the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseTlsTrustProvider {
    /// Amazon RDS global CA bundle.
    AwsRds,
    /// Supabase root CA.
    Supabase,
}

/// Return a filesystem path for a packaged provider trust bundle.
///
/// Some connector libraries accept in-memory roots, while PostgreSQL logical
/// replication currently accepts only a path. Materializing the public bundle
/// once gives every connection path the same trust policy.
pub fn materialize_provider_ca_bundle(
    provider: DatabaseTlsTrustProvider,
) -> Result<PathBuf, String> {
    match provider {
        DatabaseTlsTrustProvider::AwsRds => AWS_RDS_GLOBAL_CA_PATH
            .get_or_init(|| {
                materialize_bundle(
                    "aws-rds-global",
                    AWS_RDS_GLOBAL_CA_SHA256,
                    AWS_RDS_GLOBAL_CA_PEM,
                )
            })
            .clone(),
        DatabaseTlsTrustProvider::Supabase => SUPABASE_CA_PATH
            .get_or_init(|| {
                materialize_bundle("supabase-root-2021", SUPABASE_CA_SHA256, SUPABASE_CA_PEM)
            })
            .clone(),
    }
}

fn materialize_bundle(
    file_stem: &str,
    sha256: &str,
    pem: &'static [u8],
) -> Result<PathBuf, String> {
    let directory = std::env::temp_dir().join("ventstream").join("trust");
    fs::create_dir_all(&directory).map_err(|err| {
        format!(
            "create provider trust directory {}: {err}",
            directory.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).map_err(|err| {
            format!(
                "secure provider trust directory {}: {err}",
                directory.display()
            )
        })?;
    }

    let path = directory.join(format!("{file_stem}-{}.pem", &sha256[..16]));
    if fs::read(&path).is_ok_and(|current| current == pem) {
        return Ok(path);
    }

    let sequence = TRUST_BUNDLE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary_path = directory.join(format!(
        ".{file_stem}-{}-{}-{sequence}.tmp",
        &sha256[..16],
        std::process::id()
    ));
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|err| {
                format!(
                    "create temporary {file_stem} trust bundle {}: {err}",
                    temporary_path.display()
                )
            })?;
        file.write_all(pem).map_err(|err| {
            format!(
                "write temporary {file_stem} trust bundle {}: {err}",
                temporary_path.display()
            )
        })?;
        file.sync_all().map_err(|err| {
            format!(
                "sync temporary {file_stem} trust bundle {}: {err}",
                temporary_path.display()
            )
        })?;
        fs::rename(&temporary_path, &path)
            .map_err(|err| format!("install {file_stem} trust bundle {}: {err}", path.display()))
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    write_result?;

    Ok(path)
}

/// Select the workspace's Rustls provider before a connector builds a TLS client.
///
/// Installing an already-selected provider is harmless. Keeping this beside the
/// shared policy also makes connector crates safe to use outside the engine
/// binary, where `main` has not initialized Rustls for them.
pub(crate) fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::io::BufReader;

    use super::*;

    #[test]
    fn packaged_aws_rds_bundle_contains_certificates() {
        let certificates = rustls_pemfile::certs(&mut BufReader::new(AWS_RDS_GLOBAL_CA_PEM))
            .collect::<Result<Vec<_>, _>>()
            .expect("parse packaged AWS RDS CA bundle");
        assert_eq!(certificates.len(), 108);
    }

    #[test]
    fn packaged_supabase_root_parses_and_materializes() {
        let certificates = rustls_pemfile::certs(&mut BufReader::new(SUPABASE_CA_PEM))
            .collect::<Result<Vec<_>, _>>()
            .expect("parse packaged Supabase root CA");
        assert_eq!(certificates.len(), 1);
        let path = materialize_provider_ca_bundle(DatabaseTlsTrustProvider::Supabase)
            .expect("materialize Supabase CA");
        assert_eq!(
            fs::read(path).expect("read materialized Supabase CA"),
            SUPABASE_CA_PEM
        );
    }

    #[test]
    fn implicit_trust_is_scoped_to_supabase_hosts() {
        assert_eq!(
            implicit_trust_provider("db.abc.supabase.co"),
            Some(DatabaseTlsTrustProvider::Supabase)
        );
        assert_eq!(
            implicit_trust_provider("DB.ABC.SUPABASE.CO"),
            Some(DatabaseTlsTrustProvider::Supabase)
        );
        assert_eq!(implicit_trust_provider("db.example.com"), None);
        assert_eq!(implicit_trust_provider("evil-supabase.co"), None);
    }

    #[test]
    fn materialized_aws_rds_bundle_matches_packaged_bytes() {
        let path = materialize_provider_ca_bundle(DatabaseTlsTrustProvider::AwsRds)
            .expect("materialize AWS RDS CA bundle");
        assert_eq!(
            fs::read(path).expect("read materialized AWS RDS CA bundle"),
            AWS_RDS_GLOBAL_CA_PEM
        );
    }
}
