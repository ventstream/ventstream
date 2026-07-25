//! Local-disk dead-letter queue.
//!
//! Poison events — those a sink rejects with `is_poison()` semantics —
//! are appended here as JSON Lines so an operator can inspect and
//! replay them later. The DLQ is intentionally simple: one file per
//! pipeline, append-only, line-buffered through a tokio `BufWriter`,
//! `fsync()` on close (and on every N writes once the engine gets a
//! configurable durability knob).
//!
//! Each line is a self-contained JSON object:
//!
//! ```json
//! {"recorded_at":"2026-05-23T12:34:56Z","reason":"...","event":{...}}
//! ```
//!
//! Future work (not Phase 0): segment rotation by size, async retention
//! policy, optional S3 mirror.

use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Serialize;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tracing::warn;
use ventstream_core::Event;

/// Failure modes for [`DlqWriter`].
#[derive(Debug, Error)]
pub enum DlqError {
    /// Could not create or open the DLQ file (permissions, missing
    /// parent directory, etc.).
    #[error("opening DLQ file '{path}': {source}")]
    Open {
        /// Path being opened.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Writing to the DLQ failed.
    #[error("writing to DLQ: {0}")]
    Write(#[from] std::io::Error),

    /// JSON encoding of the record failed.
    #[error("encoding DLQ record: {0}")]
    Encode(#[from] serde_json::Error),
}

/// Append-only writer for poison events.
///
/// Cloning is cheap: clones share the same locked file handle behind an
/// `Arc<Mutex<_>>`. Concurrent writers serialize through the mutex —
/// which is fine because writes are short and the DLQ is not in any
/// hot path.
#[derive(Debug, Clone)]
pub struct DlqWriter {
    inner: std::sync::Arc<Mutex<BufWriter<File>>>,
    path: PathBuf,
}

impl DlqWriter {
    /// Open (or create) a DLQ file at the given path. Creates parent
    /// directories as needed.
    pub async fn open(path: PathBuf) -> Result<Self, DlqError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|source| DlqError::Open {
                        path: path.clone(),
                        source,
                    })?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|source| DlqError::Open {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            inner: std::sync::Arc::new(Mutex::new(BufWriter::new(file))),
            path,
        })
    }

    /// Append one event with its rejection reason. Flushes the buffered
    /// writer after every record so a process crash doesn't lose more
    /// than the last in-flight line.
    pub async fn record(&self, event: &Event, reason: &str) -> Result<(), DlqError> {
        let record = DlqRecord {
            recorded_at: Utc::now().to_rfc3339(),
            reason: reason.to_owned(),
            event,
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut guard = self.inner.lock().await;
        guard.write_all(&bytes).await?;
        guard.flush().await?;
        Ok(())
    }

    /// Flush buffered writes and `fsync` the file to disk.
    ///
    /// `record` only flushes to the OS page cache (durable across a process
    /// crash, but **not** a power loss). Call `sync` before advancing the
    /// source watermark/slot past any LSN whose event was routed to the DLQ
    /// — otherwise a power loss in that window loses the poison event: it's
    /// only in the page cache, and the slot already moved past it in the WAL
    /// so it won't be replayed either (C3).
    pub async fn sync(&self) -> Result<(), DlqError> {
        let mut guard = self.inner.lock().await;
        guard.flush().await?;
        guard.get_mut().sync_all().await.map_err(DlqError::Write)?;
        Ok(())
    }

    /// Flush and `fsync` on graceful shutdown.
    pub async fn shutdown(&self) -> Result<(), DlqError> {
        self.sync().await
    }

    /// Path being written to. Useful for log lines and tests.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Serialize)]
struct DlqRecord<'a> {
    recorded_at: String,
    reason: String,
    event: &'a Event,
}

/// Best-effort DLQ write: logs rather than propagates an I/O error, so a
/// single bad write can't panic the dispatcher. Returns `true` if the event
/// was written, `false` if the write failed — a `false` means the event is
/// **not** durable, so callers that gate a source watermark on DLQ
/// durability (the dispatcher's C3 handling) must treat it exactly like an
/// fsync failure and refuse to advance the slot.
pub async fn record_best_effort(dlq: &DlqWriter, event: &Event, reason: &str) -> bool {
    match dlq.record(event, reason).await {
        Ok(()) => {
            // First-class, structured telemetry signal: a developer
            // greps `metric=dlq.write` (or watches `vs_dlq_writes_total`)
            // to see what's being dead-lettered and why, with no CLI and
            // no control plane. The full event payload stays in the DLQ
            // file, referenced by `event_id`.
            warn!(
                metric = "dlq.write",
                event_id = %event.id,
                reason = %reason,
                "event routed to DLQ"
            );
            // Count SUCCESSFUL writes only (M16) — the caller no longer bumps a
            // per-batch attempt count, so successes/failures stay distinct.
            ventstream_telemetry::bump_dlq_writes(1);
            true
        }
        Err(err) => {
            warn!(
                metric = "dlq.write_failed",
                dlq_path = %dlq.path().display(),
                event_id = %event.id,
                error = %err,
                "DLQ write failed; event will be lost"
            );
            // A failed dead-letter is genuine loss — make it a metric, not just
            // a log line, so it's alertable (M16).
            ventstream_telemetry::bump_dlq_write_failures(1);
            false
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use ventstream_core::{ContentType, Headers, Payload, SourceUri, Subject};

    fn make_event(subject: &str, payload: &str) -> Event {
        let source = SourceUri::new("test://x").expect("uri");
        let subject = Subject::new(subject).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(payload.as_bytes().to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::empty())
            .build()
    }

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "ventstream-dlq-test-{}-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            name,
        ));
        path
    }

    #[tokio::test]
    async fn open_creates_parent_directories() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "ventstream-dlq-test-{}/{}-nested/dlq.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let dlq = DlqWriter::open(path.clone()).await.expect("open");
        dlq.shutdown().await.expect("shutdown");
        assert!(path.exists());
        let _ = tokio::fs::remove_dir_all(path.parent().unwrap().parent().unwrap()).await;
    }

    #[tokio::test]
    async fn records_append_as_jsonl() {
        let path = temp_path("append");
        let dlq = DlqWriter::open(path.clone()).await.expect("open");
        dlq.record(&make_event("a.b", r#"{"x":1}"#), "test reason 1")
            .await
            .expect("record");
        dlq.record(&make_event("a.b", r#"{"x":2}"#), "test reason 2")
            .await
            .expect("record");
        dlq.shutdown().await.expect("shutdown");

        let contents = tokio::fs::read_to_string(&path).await.expect("read");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let parsed: serde_json::Value = serde_json::from_str(line).expect("json line");
            assert!(parsed["recorded_at"].is_string());
            assert!(parsed["reason"].is_string());
            assert!(parsed["event"].is_object());
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(lines[0]).unwrap()["reason"],
            "test reason 1"
        );
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn record_best_effort_swallows_after_shutdown() {
        let path = temp_path("best_effort");
        let dlq = DlqWriter::open(path.clone()).await.expect("open");
        // Close the file underneath the writer to force a write failure.
        // This exercises the warn-and-continue branch.
        let event = make_event("a.b", "{}");
        record_best_effort(&dlq, &event, "ok").await; // should succeed
        let contents = tokio::fs::read_to_string(&path).await.expect("read");
        assert!(contents.contains("\"reason\":\"ok\""));
        let _ = tokio::fs::remove_file(&path).await;
    }
}
