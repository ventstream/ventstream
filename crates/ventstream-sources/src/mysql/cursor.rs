//! File-backed binlog-position persistence.
//!
//! MySQL has no server-side ack for a binlog client — the client owns the
//! position `(file, pos)`. Persisted atomically (tmp + rename); absence means
//! cold start (snapshot bootstrap). An `.incomplete` sentinel distinguishes a
//! cold bootstrap from a local-state rebuild that must retain its old cursor.

use std::path::{Path, PathBuf};

use crate::error::MySqlCdcError;

const BOOTSTRAP_INCOMPLETE: &str = "bootstrap";
const STATE_REBUILD_INCOMPLETE: &str = "state-rebuild";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncompleteKind {
    Bootstrap,
    StateRebuild,
}

/// A binlog coordinate: file name + byte offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinlogPos {
    pub(crate) file: String,
    pub(crate) pos: u64,
}

impl BinlogPos {
    /// `"{file}:{pos}"`. Binlog file names never contain `:`.
    pub(crate) fn encode(&self) -> String {
        format!("{}:{}", self.file, self.pos)
    }

    pub(crate) fn decode(s: &str) -> Option<Self> {
        let (file, pos) = s.trim().rsplit_once(':')?;
        Some(Self {
            file: file.to_owned(),
            pos: pos.parse().ok()?,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CursorFile {
    path: PathBuf,
}

impl CursorFile {
    pub(crate) fn new(state_dir: &Path) -> Result<Self, MySqlCdcError> {
        std::fs::create_dir_all(state_dir).map_err(|err| {
            MySqlCdcError::CursorIo(format!("creating state dir {}: {err}", state_dir.display()))
        })?;
        Ok(Self {
            path: state_dir.join("mysql_binlog_pos"),
        })
    }

    pub(crate) fn read(&self) -> Result<Option<BinlogPos>, MySqlCdcError> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) if s.trim().is_empty() => Ok(None),
            Ok(s) => Ok(BinlogPos::decode(&s)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MySqlCdcError::CursorIo(format!(
                "reading {}: {err}",
                self.path.display()
            ))),
        }
    }

    pub(crate) fn write(&self, pos: &BinlogPos) -> Result<(), MySqlCdcError> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, pos.encode())
            .map_err(|err| MySqlCdcError::CursorIo(format!("writing {}: {err}", tmp.display())))?;
        std::fs::rename(&tmp, &self.path).map_err(|err| {
            MySqlCdcError::CursorIo(format!("renaming into {}: {err}", self.path.display()))
        })
    }

    pub(crate) fn delete(&self) -> Result<(), MySqlCdcError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MySqlCdcError::CursorIo(format!(
                "removing {}: {err}",
                self.path.display()
            ))),
        }
    }

    fn incomplete_path(&self) -> PathBuf {
        self.path.with_extension("incomplete")
    }

    pub(crate) fn mark_incomplete(&self, kind: IncompleteKind) -> Result<(), MySqlCdcError> {
        let p = self.incomplete_path();
        let value = match kind {
            IncompleteKind::Bootstrap => BOOTSTRAP_INCOMPLETE,
            IncompleteKind::StateRebuild => STATE_REBUILD_INCOMPLETE,
        };
        std::fs::write(&p, value)
            .map_err(|err| MySqlCdcError::CursorIo(format!("writing {}: {err}", p.display())))
    }

    pub(crate) fn clear_incomplete(&self) -> Result<(), MySqlCdcError> {
        let path = self.incomplete_path();
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(MySqlCdcError::CursorIo(format!(
                "removing {}: {err}",
                path.display()
            ))),
        }
    }

    pub(crate) fn incomplete_kind(&self) -> Result<Option<IncompleteKind>, MySqlCdcError> {
        let path = self.incomplete_path();
        match std::fs::read_to_string(&path) {
            Ok(value) => match value.trim() {
                // `1` is the sentinel written by releases before v0.1.11.
                BOOTSTRAP_INCOMPLETE | "1" => Ok(Some(IncompleteKind::Bootstrap)),
                STATE_REBUILD_INCOMPLETE => Ok(Some(IncompleteKind::StateRebuild)),
                other => Err(MySqlCdcError::CursorIo(format!(
                    "invalid incomplete-bootstrap marker {}: {other:?}",
                    path.display()
                ))),
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MySqlCdcError::CursorIo(format!(
                "reading {}: {err}",
                path.display()
            ))),
        }
    }
}

/// Whether the state directory contains a syntactically valid cursor that may
/// be replayed. A state-rebuild marker deliberately preserves that cursor;
/// a cold-bootstrap marker does not.
pub fn checkpoint_is_resumable(state_dir: &Path) -> Result<bool, MySqlCdcError> {
    let cursor = CursorFile::new(state_dir)?;
    if cursor.incomplete_kind()? == Some(IncompleteKind::Bootstrap) {
        return Ok(false);
    }
    Ok(cursor.read()?.is_some())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tempdir() -> PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("vs-mysql-cursor-{n}"));
        std::fs::create_dir_all(&p).expect("mkdir");
        p
    }

    #[test]
    fn pos_round_trips() {
        let p = BinlogPos {
            file: "mysql-bin.000003".into(),
            pos: 1543,
        };
        assert_eq!(BinlogPos::decode(&p.encode()), Some(p));
    }

    #[test]
    fn file_round_trips() {
        let cf = CursorFile::new(&tempdir()).expect("new");
        assert!(cf.read().expect("read").is_none());
        let p = BinlogPos {
            file: "binlog.000001".into(),
            pos: 4,
        };
        cf.write(&p).expect("write");
        assert_eq!(cf.read().expect("read"), Some(p));
        cf.delete().expect("delete");
        assert!(cf.read().expect("read").is_none());
    }

    #[test]
    fn incomplete_sentinel() {
        let cf = CursorFile::new(&tempdir()).expect("new");
        assert_eq!(cf.incomplete_kind().expect("read"), None);
        cf.mark_incomplete(IncompleteKind::Bootstrap).expect("mark");
        assert_eq!(
            cf.incomplete_kind().expect("read"),
            Some(IncompleteKind::Bootstrap)
        );
        cf.clear_incomplete().expect("clear");
        assert_eq!(cf.incomplete_kind().expect("read"), None);
    }

    #[test]
    fn interrupted_state_rebuild_retains_resumable_cursor() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("new");
        cf.write(&BinlogPos {
            file: "binlog.000001".into(),
            pos: 42,
        })
        .expect("cursor");
        cf.mark_incomplete(IncompleteKind::StateRebuild)
            .expect("mark");

        assert!(checkpoint_is_resumable(&dir).expect("resumable"));
        assert_eq!(
            cf.incomplete_kind().expect("read"),
            Some(IncompleteKind::StateRebuild)
        );
    }

    #[test]
    fn interrupted_cold_bootstrap_is_not_resumable() {
        let dir = tempdir();
        let cf = CursorFile::new(&dir).expect("new");
        cf.write(&BinlogPos {
            file: "binlog.000001".into(),
            pos: 42,
        })
        .expect("cursor");
        cf.mark_incomplete(IncompleteKind::Bootstrap).expect("mark");

        assert!(!checkpoint_is_resumable(&dir).expect("not resumable"));
    }

    #[test]
    fn clear_incomplete_propagates_removal_failure() {
        let cf = CursorFile::new(&tempdir()).expect("new");
        std::fs::create_dir(cf.incomplete_path()).expect("marker directory");
        assert!(cf.clear_incomplete().is_err());
    }
}
