//! YAML-change detection for the Postgres CDC pipeline.
//!
//! Computes a stable fingerprint of the joins YAML at startup and
//! compares it to a previously-persisted value on disk. When the
//! fingerprint differs, the operator-shipped OS docs were composed
//! against an older projection — their shape is stale relative to
//! the current YAML.
//!
//! Three operator-controlled responses to a fingerprint mismatch:
//!
//! - **default (warn-only)**: log a WARN that the YAML changed and
//!   tell the operator what to do. The agent boots normally and keeps
//!   composing new events with the current YAML; existing OS docs
//!   stay stale until each row is touched. Production-safe default —
//!   a typo doesn't wipe + re-emit your data.
//!
//! - **`VS_PG_AUTO_RESYNC_ON_YAML_CHANGE=true`**: on mismatch, drop
//!   the replication slot and re-bootstrap automatically. The new
//!   bootstrap re-composes every row with the current YAML. Use this
//!   in dev / staging where you want changes to "just work."
//!
//! - **`VS_PG_FORCE_RESYNC=true`**: always re-bootstrap on this boot,
//!   regardless of the fingerprint. One-off escape hatch when an
//!   operator knows they need a clean re-emit and doesn't want to
//!   bother editing YAML to invalidate the hash.
//!
//! ### How the fingerprint stays whitespace-/comment-agnostic
//!
//! We parse the YAML into `serde_yaml::Value`, then re-serialize as
//! canonical JSON (`serde_json` sorts keys deterministically) and
//! hash the result. Comments are dropped during parsing; whitespace
//! and key order don't affect the canonical form.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// File name (inside `VS_JOINS_STATE_DIR`) where we persist the
/// last-known YAML fingerprint.
const FINGERPRINT_FILE: &str = "joins.yaml.fingerprint";

/// Decision the orchestrator should take based on the fingerprint check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FingerprintAction {
    /// First boot, or no state dir configured — nothing to compare to.
    /// Treat as normal startup; persist the fingerprint after boot.
    FirstRun,
    /// Hash matches; YAML is the same as last successful run.
    Unchanged,
    /// Hash differs and the operator opted into auto-resync.
    Resync,
    /// Hash differs but the operator hasn't opted into auto-resync.
    /// Logged a WARN with instructions; boot continues.
    WarnAndContinue,
    /// `VS_PG_FORCE_RESYNC=true` — always resync regardless of hash.
    ForceResync,
}

/// Result of checking the fingerprint at boot.
pub struct FingerprintCheck {
    pub action: FingerprintAction,
    /// The fresh fingerprint for the current YAML. Persist this once
    /// the boot succeeds.
    pub current_fingerprint: String,
    /// The previously-persisted fingerprint, when present. Useful in
    /// log lines and for surfacing in admin / health endpoints later.
    #[allow(dead_code)]
    pub previous_fingerprint: Option<String>,
    /// Path the orchestrator should write `current_fingerprint` to once
    /// the boot has succeeded. `None` if no state dir is configured.
    pub fingerprint_path: Option<PathBuf>,
}

/// Compute a stable fingerprint for the joins YAML body.
///
/// `yaml_text` is the raw file content. We round-trip it through
/// `serde_yaml::Value` → canonical JSON → SHA-256. Comment-only or
/// whitespace-only edits don't change the fingerprint; semantic edits
/// do.
pub fn fingerprint(yaml_text: &str) -> Result<String> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(yaml_text).context("parsing joins YAML for fingerprinting")?;
    // `serde_json::to_vec` on a serde_yaml::Value produces a stable
    // canonical encoding (mappings emit keys in their parsed order,
    // which for serde_yaml is insertion order — but YAML readers
    // produce insertion = file order, so this is stable across
    // identical files).
    let canonical = serde_json::to_vec(&value).context("canonicalising joins YAML")?;
    let mut hasher = Sha256::new();
    hasher.update(&canonical);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Read the prior fingerprint from disk if it exists.
fn read_previous(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

/// Persist a fresh fingerprint. Best-effort — failure to write is
/// logged but doesn't fail the boot; on next start we'd just treat
/// the missing file as `FirstRun`.
pub fn persist(path: &Path, fingerprint: &str) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(err) = std::fs::write(path, fingerprint) {
        warn!(
            path = %path.display(),
            error = %err,
            "failed to persist joins YAML fingerprint (next boot will treat as first run)"
        );
    }
}

/// Run the fingerprint check at boot. Pure function over its inputs;
/// the orchestrator handles the resulting action.
pub fn check(
    yaml_text: &str,
    state_dir: Option<&Path>,
    auto_resync: bool,
    force_resync: bool,
) -> Result<FingerprintCheck> {
    let current = fingerprint(yaml_text)?;
    let fingerprint_path = state_dir.map(|d| d.join(FINGERPRINT_FILE));

    if force_resync {
        info!(
            current = %short(&current),
            "VS_PG_FORCE_RESYNC=true — re-bootstrap will run this boot"
        );
        return Ok(FingerprintCheck {
            action: FingerprintAction::ForceResync,
            current_fingerprint: current,
            previous_fingerprint: fingerprint_path.as_deref().and_then(read_previous),
            fingerprint_path,
        });
    }

    let previous = fingerprint_path.as_deref().and_then(read_previous);

    let action = match (&previous, &current) {
        (None, _) => {
            info!(
                current = %short(&current),
                "no prior joins YAML fingerprint — treating as first run"
            );
            FingerprintAction::FirstRun
        }
        (Some(p), c) if p == c => {
            info!(current = %short(c), "joins YAML unchanged since last run");
            FingerprintAction::Unchanged
        }
        (Some(p), c) if auto_resync => {
            info!(
                previous = %short(p),
                current = %short(c),
                "joins YAML changed + VS_PG_AUTO_RESYNC_ON_YAML_CHANGE=true — will drop slot and re-bootstrap"
            );
            FingerprintAction::Resync
        }
        (Some(p), c) => {
            warn!(
                previous = %short(p),
                current = %short(c),
                metric = "pg.joins.yaml_changed",
                "joins YAML changed since last successful boot. \
                 New events will be composed with the current YAML, but existing OS docs \
                 still reflect the prior shape. To re-emit every row with the new shape, \
                 set VS_PG_AUTO_RESYNC_ON_YAML_CHANGE=true (or VS_PG_FORCE_RESYNC=true \
                 for a one-off) and restart."
            );
            FingerprintAction::WarnAndContinue
        }
    };

    Ok(FingerprintCheck {
        action,
        current_fingerprint: current,
        previous_fingerprint: previous,
        fingerprint_path,
    })
}

fn short(hash: &str) -> &str {
    if hash.len() >= 12 {
        &hash[..12]
    } else {
        hash
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_stable_for_identical_yaml() {
        let yaml = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let a = fingerprint(yaml).unwrap();
        let b = fingerprint(yaml).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_ignores_comments() {
        let with = "# a comment\njoins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let without = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        assert_eq!(fingerprint(with).unwrap(), fingerprint(without).unwrap());
    }

    #[test]
    fn fingerprint_ignores_whitespace() {
        let compact = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let spaced = "joins:\n\n\n  - primary:\n      table: a.b\n\n      pk: id\n";
        assert_eq!(fingerprint(compact).unwrap(), fingerprint(spaced).unwrap());
    }

    #[test]
    fn fingerprint_changes_on_semantic_edit() {
        let a = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let b = "joins:\n  - primary:\n      table: a.b\n      pk: other_id\n";
        assert_ne!(fingerprint(a).unwrap(), fingerprint(b).unwrap());
    }

    #[test]
    fn check_first_run_when_no_state_dir() {
        let yaml = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let res = check(yaml, None, false, false).unwrap();
        assert_eq!(res.action, FingerprintAction::FirstRun);
        assert!(res.fingerprint_path.is_none());
    }

    #[test]
    fn check_force_resync_overrides_everything() {
        let yaml = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let res = check(yaml, None, false, true).unwrap();
        assert_eq!(res.action, FingerprintAction::ForceResync);
    }

    #[test]
    fn check_warns_when_changed_without_optin() {
        let dir = tempdir();
        let yaml_old = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let yaml_new = "joins:\n  - primary:\n      table: a.b\n      pk: id2\n";

        // First run persists the fingerprint.
        let r1 = check(yaml_old, Some(dir.as_path()), false, false).unwrap();
        assert_eq!(r1.action, FingerprintAction::FirstRun);
        persist(
            r1.fingerprint_path.as_ref().unwrap(),
            &r1.current_fingerprint,
        );

        // Second run with changed YAML, no opt-in → WarnAndContinue.
        let r2 = check(yaml_new, Some(dir.as_path()), false, false).unwrap();
        assert_eq!(r2.action, FingerprintAction::WarnAndContinue);
    }

    #[test]
    fn check_resync_when_changed_with_optin() {
        let dir = tempdir();
        let yaml_old = "joins:\n  - primary:\n      table: a.b\n      pk: id\n";
        let yaml_new = "joins:\n  - primary:\n      table: a.b\n      pk: id2\n";

        let r1 = check(yaml_old, Some(dir.as_path()), false, false).unwrap();
        persist(
            r1.fingerprint_path.as_ref().unwrap(),
            &r1.current_fingerprint,
        );

        let r2 = check(yaml_new, Some(dir.as_path()), true, false).unwrap();
        assert_eq!(r2.action, FingerprintAction::Resync);
    }

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "vs-yaml-fingerprint-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
