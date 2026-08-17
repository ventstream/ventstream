use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::error::AgentError;
use crate::model::{AgentScope, ManagementState, STATE_SCHEMA_VERSION};

const MAX_STATE_BYTES: u64 = 1024 * 1024;

/// Atomic, private persistence for one deployment's management state.
#[derive(Clone, Debug)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    /// Creates a store for the given state-file path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Returns the configured state-file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn load_or_initialize(
        &self,
        expected_scope: &AgentScope,
    ) -> Result<ManagementState, AgentError> {
        self.prepare_parent()?;

        match fs::symlink_metadata(&self.path) {
            Ok(metadata) => self.load_existing(expected_scope, &metadata),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let state = ManagementState::new(expected_scope.clone());
                self.persist(&state)?;
                Ok(state)
            }
            Err(error) => Err(AgentError::io(&self.path, error)),
        }
    }

    pub(crate) fn persist(&self, state: &ManagementState) -> Result<(), AgentError> {
        self.prepare_parent()?;
        validate_state(&self.path, state, &state.scope)?;

        let encoded = serde_json::to_vec(state).map_err(|error| {
            AgentError::invalid_state(&self.path, format!("serialization failed: {error}"))
        })?;
        let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
            AgentError::invalid_state(&self.path, "serialized state length overflowed")
        })?;
        if encoded_len > MAX_STATE_BYTES {
            return Err(AgentError::invalid_state(
                &self.path,
                "serialized state exceeds the 1 MiB limit",
            ));
        }

        if let Ok(metadata) = fs::symlink_metadata(&self.path) {
            validate_regular_private_file(&self.path, &metadata)?;
        }

        let parent = self.parent()?;
        let temporary = parent.join(format!(".state-{}.tmp", Uuid::now_v7()));
        let result = write_and_replace(&temporary, &self.path, parent, &encoded);
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    fn load_existing(
        &self,
        expected_scope: &AgentScope,
        metadata: &fs::Metadata,
    ) -> Result<ManagementState, AgentError> {
        validate_regular_private_file(&self.path, metadata)?;
        if metadata.len() > MAX_STATE_BYTES {
            return Err(AgentError::invalid_state(
                &self.path,
                "state exceeds the 1 MiB limit",
            ));
        }

        let encoded = fs::read(&self.path).map_err(|error| AgentError::io(&self.path, error))?;
        let state: ManagementState = serde_json::from_slice(&encoded).map_err(|error| {
            AgentError::invalid_state(&self.path, format!("JSON decoding failed: {error}"))
        })?;
        validate_state(&self.path, &state, expected_scope)?;
        Ok(state)
    }

    fn prepare_parent(&self) -> Result<(), AgentError> {
        let parent = self.parent()?;
        create_private_directory(parent)?;
        let metadata =
            fs::symlink_metadata(parent).map_err(|error| AgentError::io(parent, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AgentError::invalid_state(
                parent,
                "state parent must be a real directory",
            ));
        }
        validate_private_mode(parent, &metadata, "state parent")
    }

    fn parent(&self) -> Result<&Path, AgentError> {
        self.path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or_else(|| {
                AgentError::invalid_state(&self.path, "state path must have a parent directory")
            })
    }
}

fn validate_state(
    path: &Path,
    state: &ManagementState,
    expected_scope: &AgentScope,
) -> Result<(), AgentError> {
    if state.schema_version != STATE_SCHEMA_VERSION {
        return Err(AgentError::invalid_state(
            path,
            "unsupported state schema version",
        ));
    }
    if &state.scope != expected_scope {
        return Err(AgentError::invalid_state(
            path,
            "state is bound to a different pipeline or deployment",
        ));
    }
    if let Some(desired) = &state.desired
        && (desired.pipeline_id != state.scope.pipeline_id
            || desired.deployment_id != state.scope.deployment_id)
    {
        return Err(AgentError::invalid_state(
            path,
            "desired state does not match the state-file scope",
        ));
    }

    let mut operation_ids = HashSet::with_capacity(state.operation_receipts.len());
    for receipt in &state.operation_receipts {
        if receipt.sequence > state.last_operation_sequence {
            return Err(AgentError::invalid_state(
                path,
                "operation receipt exceeds the last accepted sequence",
            ));
        }
        if !operation_ids.insert(receipt.operation_id) {
            return Err(AgentError::invalid_state(
                path,
                "duplicate operation receipt",
            ));
        }
    }
    Ok(())
}

fn write_and_replace(
    temporary: &Path,
    destination: &Path,
    parent: &Path,
    encoded: &[u8],
) -> Result<(), AgentError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_private_file_mode(&mut options);

    let mut file = options
        .open(temporary)
        .map_err(|error| AgentError::io(temporary, error))?;
    file.write_all(encoded)
        .map_err(|error| AgentError::io(temporary, error))?;
    file.write_all(b"\n")
        .map_err(|error| AgentError::io(temporary, error))?;
    file.sync_all()
        .map_err(|error| AgentError::io(temporary, error))?;
    drop(file);

    fs::rename(temporary, destination).map_err(|error| AgentError::io(destination, error))?;
    sync_directory(parent).map_err(|error| AgentError::io(parent, error))
}

/// Directory durability barrier; no-op on Windows, which cannot open
/// directories via `File::open` and journals renames in NTFS.
#[cfg(unix)]
fn sync_directory(parent: &Path) -> std::io::Result<()> {
    fs::File::open(parent).and_then(|directory| directory.sync_all())
}

#[cfg(not(unix))]
fn sync_directory(_parent: &Path) -> std::io::Result<()> {
    Ok(())
}

fn validate_regular_private_file(path: &Path, metadata: &fs::Metadata) -> Result<(), AgentError> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AgentError::invalid_state(
            path,
            "state must be a regular file",
        ));
    }
    validate_private_mode(path, metadata, "state file")
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<(), AgentError> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder
        .create(path)
        .map_err(|error| AgentError::io(path, error))
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<(), AgentError> {
    fs::create_dir_all(path).map_err(|error| AgentError::io(path, error))
}

#[cfg(unix)]
fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn validate_private_mode(
    path: &Path,
    metadata: &fs::Metadata,
    description: &str,
) -> Result<(), AgentError> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(AgentError::invalid_state(
            path,
            format!("{description} must not be accessible by group or other users"),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_mode(
    _path: &Path,
    _metadata: &fs::Metadata,
    _description: &str,
) -> Result<(), AgentError> {
    Ok(())
}
