//! File-backed resume-token persistence.
//!
//! MongoDB change streams resume from an opaque **resume token** (the
//! `_data` string carried on every change event). MongoDB has no
//! server-side acknowledgment primitive for change streams, so — exactly
//! like the Neo4j source — the agent owns the cursor entirely and persists
//! the token to a file after every sink-confirmed batch.
//!
//! File layout: one line, UTF-8, no trailing newline. Atomic-write via
//! `tmp + rename` so a crash mid-write can't leave a half-written token.
//! Absence of the file means "cold start — do a snapshot bootstrap", so a
//! corrupt/partial write defaulting to recovery is the right safety story.
//! An expired token (oplog rotated past it) is handled the same way: delete
//! and re-bootstrap.

use std::path::{Path, PathBuf};

use crate::error::MongoCdcError;

/// Persistent resume token anchored to a file.
#[derive(Debug, Clone)]
pub struct CursorFile {
    path: PathBuf,
}

impl CursorFile {
    /// Construct a cursor handle. Creates the parent directory if it
    /// doesn't exist; does not create the file itself.
    pub fn new(state_dir: &Path) -> Result<Self, MongoCdcError> {
        std::fs::create_dir_all(state_dir).map_err(|err| {
            MongoCdcError::CursorIo(format!("creating state dir {}: {err}", state_dir.display()))
        })?;
        Ok(Self {
            path: state_dir.join("mongo_resume_token"),
        })
    }

    /// Read the persisted resume token, or `None` if the file doesn't exist.
    pub fn read(&self) -> Result<Option<String>, MongoCdcError> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_owned()))
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MongoCdcError::CursorIo(format!(
                "reading {}: {err}",
                self.path.display()
            ))),
        }
    }

    /// Atomically replace the token file. Writes to a `*.tmp` sibling first,
    /// then renames over the live path so a crash mid-write can't leave a
    /// truncated token.
    pub fn write(&self, token: &str) -> Result<(), MongoCdcError> {
        let tmp_path = self.path.with_extension("tmp");
        std::fs::write(&tmp_path, token).map_err(|err| {
            MongoCdcError::CursorIo(format!("writing tmp {}: {err}", tmp_path.display()))
        })?;
        std::fs::rename(&tmp_path, &self.path).map_err(|err| {
            MongoCdcError::CursorIo(format!(
                "renaming {} -> {}: {err}",
                tmp_path.display(),
                self.path.display()
            ))
        })
    }

    /// Remove the token file so the next start re-bootstraps. Idempotent —
    /// a missing file is success. Used when MongoDB rejects the persisted
    /// token (expired / oplog rotated past it).
    pub fn delete(&self) -> Result<(), MongoCdcError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MongoCdcError::CursorIo(format!(
                "removing {}: {err}",
                self.path.display()
            ))),
        }
    }

    /// Path used for tests / log lines.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path of the "bootstrap incomplete" sentinel sibling.
    fn incomplete_path(&self) -> PathBuf {
        self.path.with_extension("bootstrap_incomplete")
    }

    /// Mark that a bootstrap snapshot is underway but not yet confirmed
    /// durable by the sink. Written *before* the scan; while it exists, the
    /// persisted token must NOT be trusted on restart — the snapshot events
    /// may only be on the bus, not sink-durable.
    pub fn mark_incomplete(&self) -> Result<(), MongoCdcError> {
        let p = self.incomplete_path();
        std::fs::write(&p, b"1")
            .map_err(|err| MongoCdcError::CursorIo(format!("writing {}: {err}", p.display())))
    }

    /// Clear the sentinel once the snapshot is confirmed sink-durable.
    /// Idempotent — a missing sentinel is success.
    pub fn clear_incomplete(&self) -> Result<(), MongoCdcError> {
        let p = self.incomplete_path();
        match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MongoCdcError::CursorIo(format!(
                "removing {}: {err}",
                p.display()
            ))),
        }
    }

    /// Whether a prior bootstrap was left unconfirmed (sentinel present).
    /// When true the source must re-bootstrap rather than resume.
    pub fn is_incomplete(&self) -> bool {
        self.incomplete_path().exists()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A directory unique to this call, removed when the guard drops.
    ///
    /// Keying on the clock alone is not enough: unit tests run in parallel
    /// and the timer is coarse enough on macOS that two calls can land on
    /// the same value, at which point two tests share a directory and
    /// overwrite each other's cursor files. The counter makes uniqueness
    /// independent of timer resolution; the pid keeps concurrent test
    /// binaries apart, matching the pattern used elsewhere in the tree.
    ///
    /// Cleanup matters more now than it did: guaranteeing a fresh directory
    /// per call removes the accidental reuse that used to cap how many of
    /// these accumulated in the temp dir.
    struct TempDir(PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl std::ops::Deref for TempDir {
        type Target = Path;
        fn deref(&self) -> &Path {
            &self.0
        }
    }

    fn tempdir() -> TempDir {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "vs-mongo-cursor-test-{}-{nanos}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("mkdir");
        TempDir(path)
    }

    #[test]
    fn read_missing_returns_none() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        assert!(cf.read().expect("read").is_none());
    }

    #[test]
    fn write_then_read_round_trips() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        cf.write("826...hex").expect("write");
        assert_eq!(cf.read().expect("read"), Some("826...hex".to_owned()));
    }

    #[test]
    fn delete_then_cold_start() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        cf.write("tok").expect("write");
        cf.delete().expect("delete");
        assert!(cf.read().expect("read").is_none());
        cf.delete().expect("idempotent");
    }

    #[test]
    fn incomplete_sentinel_independent_of_token() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        assert!(!cf.is_incomplete());
        cf.mark_incomplete().expect("mark");
        cf.write("tok").expect("write");
        assert!(cf.is_incomplete(), "token write leaves sentinel");
        cf.clear_incomplete().expect("clear");
        assert!(!cf.is_incomplete());
        assert_eq!(cf.read().expect("read"), Some("tok".to_owned()));
    }
}
