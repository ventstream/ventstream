//! Exponential-backoff retry for every Neo4j call site.
//!
//! Production Neo4j has rolling restarts, cluster failovers, brief
//! network blips, and node-eviction events. A single failed Cypher
//! call shouldn't cascade into engine shutdown — instead we retry the
//! operation with exponential backoff for ~100 seconds total, then
//! bubble up so Kubernetes can decide to restart the pod.
//!
//! Retry policy (intentional choices, not defaults):
//!
//! - **Initial delay**: 100ms — short enough that a brief network
//!   blip recovers within the first attempt; long enough not to
//!   hammer a recovering server.
//! - **Cap**: 30s per sleep — past this, Kubernetes pod restart is
//!   probably the right move anyway.
//! - **Attempts**: 10 — total wall-clock is roughly
//!   100 + 200 + 400 + 800 + 1.6s + 3.2s + 6.4s + 12.8s + 25.6s = ~50s,
//!   then a final attempt. ~1 minute of "stay up through the blip".
//! - **Almost no error classification**: we retry every error type
//!   except two that can never succeed by retrying — a shutdown
//!   cancellation and an invalid/aged-out CDC cursor
//!   (`ChangeDataCapture.InvalidIdentifier`). Both short-circuit
//!   immediately (see below). Cypher syntax errors are caught at startup
//!   by validate_specs(); the rest (timeouts, broken connections,
//!   transient unavailable) all benefit from retry.
//!
//! Logging: every retry attempt logs at WARN with the op name, attempt
//! number, and remaining delay. The final failure logs at ERROR.

use std::future::Future;
use std::time::Duration;
use tracing::warn;

use crate::error::Neo4jCdcError;

const MAX_ATTEMPTS: u32 = 10;
const INITIAL_DELAY: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(30);

/// Run a Neo4j operation with bounded exponential-backoff retry.
///
/// `op_name` is a stable string for logs (e.g. "poll_cdc_query",
/// "fetch_current_cursor"). `op` is a closure returning a future —
/// called fresh each attempt so it can re-establish state if needed.
///
/// **Shutdown short-circuit:** if the error message contains the
/// shutdown-cancellation sentinel ("cancelled by shutdown"), we bail
/// immediately. Without this, a SIGTERM during a batch would cause
/// every in-flight event to retry through the full 50s budget,
/// dragging the shutdown out for minutes. Caught during the 25k load
/// test — the binary refused to die when killed mid-burst.
pub async fn with_backoff<F, Fut, T>(op_name: &str, mut op: F) -> Result<T, Neo4jCdcError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Neo4jCdcError>>,
{
    let mut delay = INITIAL_DELAY;
    for attempt in 1..=MAX_ATTEMPTS {
        match op().await {
            Ok(v) => {
                if attempt > 1 {
                    // We recovered — say so. The operator sees a
                    // matching WARN + INFO pair so they know the blip
                    // self-healed instead of cascading.
                    tracing::info!(op = op_name, attempts = attempt, "neo4j call recovered");
                }
                return Ok(v);
            }
            Err(err) => {
                // Shutdown-induced errors should NEVER retry. The
                // engine has signalled the source to stop — looping
                // here delays the SIGTERM response and clogs the log
                // with WARNs. Sentinel substring keeps this resilient
                // to upstream error-wrapping changes.
                if err.to_string().contains("cancelled by shutdown") {
                    return Err(err);
                }
                // An invalid / aged-out CDC cursor is permanent — no
                // amount of retrying makes a rejected change identifier
                // valid. Bail immediately so the source's auto-heal
                // (wipe cursor + re-bootstrap) kicks in without burning
                // the full ~50s backoff budget first.
                if err.to_string().contains("InvalidIdentifier") {
                    return Err(err);
                }
                if attempt == MAX_ATTEMPTS {
                    return Err(err);
                }
                warn!(
                    op = op_name,
                    attempt,
                    max = MAX_ATTEMPTS,
                    delay_ms = delay.as_millis() as u64,
                    error = %err,
                    "neo4j call failed, retrying"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
        }
    }
    // Loop body always returns on attempt == MAX_ATTEMPTS.
    unreachable!("with_backoff retry loop terminates inside the match")
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn returns_value_on_first_success() {
        let result: Result<i32, _> = with_backoff("test", || async { Ok(42) }).await;
        assert_eq!(result.expect("ok"), 42);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let result: Result<i32, _> = with_backoff("test", move || {
            let c = Arc::clone(&calls2);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(Neo4jCdcError::Internal(format!("attempt {n} failed")))
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(result.expect("ok"), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bails_immediately_on_shutdown_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let result: Result<i32, _> = with_backoff("test", move || {
            let c = Arc::clone(&calls2);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(Neo4jCdcError::Internal(
                    "publish failed: publish cancelled by shutdown".to_owned(),
                ))
            }
        })
        .await;
        assert!(result.is_err());
        // Critical: no retries — one attempt, immediate bail.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bails_immediately_on_invalid_cursor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let result: Result<i32, _> = with_backoff("poll_cdc_query", move || {
            let c = Arc::clone(&calls2);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(Neo4jCdcError::Query(
                    "db.cdc.query iter: ... Neo.ClientError.ChangeDataCapture.InvalidIdentifier"
                        .to_owned(),
                ))
            }
        })
        .await;
        assert!(result.is_err());
        // No wasted retries — straight to the auto-heal path.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = Arc::clone(&calls);
        let result: Result<i32, _> = with_backoff("test", move || {
            let c = Arc::clone(&calls2);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(Neo4jCdcError::Internal("always fails".to_owned()))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS as usize);
    }
}
