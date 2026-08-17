//! Loader for Fleet-staged non-secret engine configuration.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::info;

const MAX_FLEET_CONFIG_BYTES: u64 = 96 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct FleetAppliedConfig {
    pub(crate) path: PathBuf,
    pub(crate) revision: u64,
    pub(crate) schema_version: u64,
    pub(crate) content_digest: String,
    pub(crate) document: Value,
}

impl FleetAppliedConfig {
    pub(crate) fn text_at(&self, pointer: &str) -> Result<Option<&str>> {
        let Some(value) = self.document.pointer(pointer) else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        value
            .as_str()
            .map(Some)
            .ok_or_else(|| anyhow!("Fleet-applied config {pointer} must be a string"))
    }

    pub(crate) fn materialize_text(
        &self,
        pointer: &str,
        filename: &str,
    ) -> Result<Option<PathBuf>> {
        let Some(text) = self.text_at(pointer)? else {
            return Ok(None);
        };
        if text.is_empty() {
            return Err(anyhow!("Fleet-applied config {pointer} must not be empty"));
        }
        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| anyhow!("Fleet-applied config path has no parent directory"))?;
        let path = parent.join(format!("generated-r{}-{filename}", self.revision));
        write_private_file(&path, text.as_bytes())
            .with_context(|| format!("materializing Fleet-applied config {pointer}"))?;
        Ok(Some(path))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetAppliedConfigEnvelope {
    revision: u64,
    schema_version: u64,
    content_digest: String,
    document: Value,
}

pub(crate) fn load_from_env() -> Result<Option<FleetAppliedConfig>> {
    let Some(path) = optional_env_path("VS_FLEET_APPLIED_CONFIG_PATH")? else {
        return Ok(None);
    };
    let config = load_from_path(&path)?;
    let document = config.document.as_object().ok_or_else(|| {
        anyhow!("Fleet-applied config document must be a JSON object after validation")
    })?;
    info!(
        revision = config.revision,
        schema_version = config.schema_version,
        digest = %config.content_digest,
        path = %config.path.display(),
        source = document.contains_key("source"),
        sink = document.contains_key("sink"),
        runtime = document.contains_key("runtime"),
        "Fleet-applied engine configuration loaded"
    );
    Ok(Some(config))
}

fn optional_env_path(name: &str) -> Result<Option<PathBuf>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                return Err(anyhow!("{name} must be an absolute path"));
            }
            Ok(Some(path))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading env var {name}")),
    }
}

fn load_from_path(path: &Path) -> Result<FleetAppliedConfig> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("reading Fleet-applied config {}", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_FLEET_CONFIG_BYTES {
        return Err(anyhow!(
            "Fleet-applied config {} must be a non-empty regular file no larger than {} bytes",
            path.display(),
            MAX_FLEET_CONFIG_BYTES
        ));
    }
    let encoded = fs::read(path)
        .with_context(|| format!("reading Fleet-applied config {}", path.display()))?;
    let envelope: FleetAppliedConfigEnvelope = serde_json::from_slice(&encoded)
        .with_context(|| format!("decoding Fleet-applied config {}", path.display()))?;
    validate_envelope(path, envelope)
}

fn validate_envelope(
    path: &Path,
    envelope: FleetAppliedConfigEnvelope,
) -> Result<FleetAppliedConfig> {
    if envelope.revision == 0 {
        return Err(anyhow!("Fleet-applied config revision must be positive"));
    }
    if envelope.schema_version != 1 {
        return Err(anyhow!(
            "Fleet-applied config schema_version {} is not supported",
            envelope.schema_version
        ));
    }
    validate_digest_shape(&envelope.content_digest)?;
    let Some(document) = envelope.document.as_object() else {
        return Err(anyhow!(
            "Fleet-applied config document must be a JSON object"
        ));
    };
    if document.contains_key("secrets") {
        return Err(anyhow!(
            "Fleet-applied config document must not contain top-level secrets"
        ));
    }
    let document_schema_version = document
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("Fleet-applied config document.schema_version is required"))?;
    if document_schema_version != envelope.schema_version {
        return Err(anyhow!(
            "Fleet-applied config document.schema_version does not match envelope schema_version"
        ));
    }
    let computed_digest = content_digest(&envelope.document)?;
    if computed_digest != envelope.content_digest {
        return Err(anyhow!("Fleet-applied config content digest mismatch"));
    }
    Ok(FleetAppliedConfig {
        path: path.to_path_buf(),
        revision: envelope.revision,
        schema_version: envelope.schema_version,
        content_digest: envelope.content_digest,
        document: envelope.document,
    })
}

fn validate_digest_shape(value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(anyhow!(
            "Fleet-applied config content_digest must use sha256:<hex>"
        ));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!(
            "Fleet-applied config content_digest must use sha256:<64 hex chars>"
        ));
    }
    Ok(())
}

fn content_digest(document: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(document)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("sha256:{digest:x}"))
}

fn write_private_file(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("generated Fleet config path has no parent directory"))?;
    let temporary = parent.join(format!(
        ".generated-fleet-config-{}-{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let result = write_private_file_and_replace(&temporary, path, parent, content);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn write_private_file_and_replace(
    temporary: &Path,
    destination: &Path,
    parent: &Path,
    content: &[u8],
) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_private_file_mode(&mut options);
    let mut file = options.open(temporary)?;
    file.write_all(content)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, destination)?;
    sync_directory(parent)?;
    Ok(())
}

/// Durability barrier on the containing directory after a rename. Windows
/// cannot open directories via `File::open`; NTFS journals the metadata
/// update, so this is a no-op there.
#[cfg(unix)]
fn sync_directory(parent: &Path) -> std::io::Result<()> {
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_parent: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_file_mode(_options: &mut OpenOptions) {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self> {
            let suffix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ventstream-fleet-config-{}-{suffix}",
                std::process::id()
            ));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_config(path: &Path, document: &Value) -> Result<()> {
        let envelope = json!({
            "revision": 7,
            "schema_version": 1,
            "content_digest": content_digest(document)?,
            "document": document,
        });
        let mut file = fs::File::create(path)?;
        file.write_all(serde_json::to_vec(&envelope)?.as_slice())?;
        Ok(())
    }

    #[test]
    fn valid_fleet_config_envelope_loads() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.path("applied.json");
        write_config(
            &path,
            &json!({
                "schema_version": 1,
                "source": {"kind": "postgres"},
                "sink": {"kind": "opensearch"},
            }),
        )?;

        let config = load_from_path(&path)?;

        assert_eq!(config.revision, 7);
        assert_eq!(config.schema_version, 1);
        assert_eq!(
            config
                .document
                .pointer("/source/kind")
                .and_then(Value::as_str),
            Some("postgres")
        );
        Ok(())
    }

    #[test]
    fn digest_mismatch_fails_closed() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.path("bad-digest.json");
        let envelope = json!({
            "revision": 7,
            "schema_version": 1,
            "content_digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "document": {"schema_version": 1},
        });
        fs::write(&path, serde_json::to_vec(&envelope)?)?;

        assert!(load_from_path(&path).is_err());
        Ok(())
    }

    #[test]
    fn secrets_are_rejected() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.path("secrets.json");
        write_config(
            &path,
            &json!({
                "schema_version": 1,
                "secrets": {"password": "not-allowed"},
            }),
        )?;

        assert!(load_from_path(&path).is_err());
        Ok(())
    }

    #[test]
    fn inline_spec_text_materializes_next_to_applied_config() -> Result<()> {
        let directory = TestDirectory::new()?;
        let path = directory.path("applied.json");
        write_config(
            &path,
            &json!({
                "schema_version": 1,
                "specs": {"joins_yaml": "joins: []\n"},
            }),
        )?;
        let config = load_from_path(&path)?;

        let materialized = config
            .materialize_text("/specs/joins_yaml", "joins.yaml")?
            .ok_or_else(|| anyhow!("materialized path missing"))?;

        assert_eq!(fs::read_to_string(&materialized)?, "joins: []\n".to_owned());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&materialized)?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        Ok(())
    }
}
