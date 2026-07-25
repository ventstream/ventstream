//! File-backed cursor persistence.
//!
//! The Neo4j CDC source persists its last-seen change-id after every
//! batch so a restart resumes from that point rather than from
//! `db.cdc.current()`. This is the equivalent of the PG source's
//! `update_applied_lsn` — except we own the storage entirely (Neo4j
//! has no server-side acknowledgment primitive for CDC).
//!
//! File layout: one line, UTF-8, no trailing newline. Atomic-write via
//! `tmp + rename` so a crash mid-write can't leave a half-written
//! cursor. The source treats the absence of the file as "cold start —
//! do a snapshot bootstrap", so partial corruption defaulting to
//! recovery is the right safety story.

use std::path::{Path, PathBuf};

use crate::error::Neo4jCdcError;

/// Persistent cursor anchored to a file.
#[derive(Debug, Clone)]
pub struct CursorFile {
    path: PathBuf,
}

impl CursorFile {
    /// Construct a cursor handle. Creates the parent directory if it
    /// doesn't exist; does not create the file itself.
    pub fn new(state_dir: &Path) -> Result<Self, Neo4jCdcError> {
        std::fs::create_dir_all(state_dir).map_err(|err| {
            Neo4jCdcError::CursorIo(format!("creating state dir {}: {err}", state_dir.display()))
        })?;
        Ok(Self {
            path: state_dir.join("neo4j_cursor"),
        })
    }

    /// Read the persisted cursor, or `None` if the file doesn't exist.
    pub fn read(&self) -> Result<Option<String>, Neo4jCdcError> {
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
            Err(err) => Err(Neo4jCdcError::CursorIo(format!(
                "reading {}: {err}",
                self.path.display()
            ))),
        }
    }

    /// Atomically replace the cursor file with `cursor`. Writes to a
    /// `*.tmp` sibling first, then renames over the live path so a
    /// crash mid-write doesn't leave a truncated cursor.
    pub fn write(&self, cursor: &str) -> Result<(), Neo4jCdcError> {
        let tmp_path = self.path.with_extension("tmp");
        std::fs::write(&tmp_path, cursor).map_err(|err| {
            Neo4jCdcError::CursorIo(format!("writing tmp {}: {err}", tmp_path.display()))
        })?;
        std::fs::rename(&tmp_path, &self.path).map_err(|err| {
            Neo4jCdcError::CursorIo(format!(
                "renaming {} -> {}: {err}",
                tmp_path.display(),
                self.path.display()
            ))
        })
    }

    /// Remove the cursor file so the next start re-bootstraps from a
    /// fresh `db.cdc.current()`. Idempotent — a missing file is success.
    /// Used by the auto-heal path when the persisted cursor is rejected
    /// by Neo4j (invalid / aged-out change identifier).
    pub fn delete(&self) -> Result<(), Neo4jCdcError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Neo4jCdcError::CursorIo(format!(
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
    /// durable by the sink. Written *before* the snapshot scan; while it
    /// exists, the persisted cursor must NOT be trusted on restart — the
    /// snapshot events may only be on the bus, not sink-durable.
    pub fn mark_incomplete(&self) -> Result<(), Neo4jCdcError> {
        let p = self.incomplete_path();
        std::fs::write(&p, b"1")
            .map_err(|err| Neo4jCdcError::CursorIo(format!("writing {}: {err}", p.display())))
    }

    /// Clear the sentinel once the snapshot is confirmed sink-durable.
    /// Idempotent — a missing sentinel is success.
    pub fn clear_incomplete(&self) -> Result<(), Neo4jCdcError> {
        let p = self.incomplete_path();
        match std::fs::remove_file(&p) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Neo4jCdcError::CursorIo(format!(
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
        cf.write("abc-123").expect("write");
        assert_eq!(cf.read().expect("read"), Some("abc-123".to_owned()));
    }

    #[test]
    fn empty_file_treated_as_no_cursor() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        cf.write("   ").expect("write whitespace");
        assert!(cf.read().expect("read").is_none());
    }

    #[test]
    fn incomplete_sentinel_marks_clears_and_is_independent_of_cursor() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        assert!(!cf.is_incomplete(), "absent by default");
        cf.mark_incomplete().expect("mark");
        assert!(cf.is_incomplete(), "present after mark");
        // Writing/reading the cursor doesn't touch the sentinel.
        cf.write("cursor-1").expect("write");
        assert_eq!(cf.read().expect("read"), Some("cursor-1".to_owned()));
        assert!(cf.is_incomplete(), "cursor write leaves sentinel");
        cf.clear_incomplete().expect("clear");
        assert!(!cf.is_incomplete(), "absent after clear");
        cf.clear_incomplete().expect("clear is idempotent");
        // Cursor survived independently.
        assert_eq!(cf.read().expect("read"), Some("cursor-1".to_owned()));
    }

    #[test]
    fn delete_removes_cursor_and_is_idempotent() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("create");
        cf.write("abc-123").expect("write");
        assert!(cf.read().expect("read").is_some());
        cf.delete().expect("delete");
        assert!(cf.read().expect("read").is_none()); // gone → cold start
        cf.delete().expect("delete again is a no-op"); // idempotent
    }

    /// Make a unique temp dir under /tmp without pulling in the
    /// `tempfile` crate (kept dep-free).
    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "vs-cursor-test-{}-{nanos}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("mkdir");
        path
    }
}
