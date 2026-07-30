//! Shared downstream-sink health state used by delivery and readiness paths.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Cloneable health state for one process-local sink.
#[derive(Clone, Debug, Default)]
pub struct SinkHealth {
    inner: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    active_transient_failures: usize,
    degraded_since: Option<Instant>,
    last_transient_error: Option<String>,
    blocked: Option<BlockedState>,
}

#[derive(Debug)]
struct BlockedState {
    since: Instant,
    reason: String,
}

/// Point-in-time sink availability for health checks and diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkHealthSnapshot {
    /// No active downstream failures.
    Healthy,
    /// One or more writes are retrying a transient downstream failure.
    Degraded {
        /// How long the oldest active transient failure has persisted.
        duration: Duration,
        /// Most recently observed transient error.
        reason: String,
    },
    /// Delivery cannot continue without an operator or configuration change.
    Blocked {
        /// How long delivery has been blocked.
        duration: Duration,
        /// Operator-facing failure reason.
        reason: String,
    },
}

impl SinkHealth {
    /// Create a healthy sink state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one sink write as actively retrying a transient failure.
    ///
    /// The returned guard must be retained for that write's retry lifetime.
    /// Dropping it marks that write recovered without clearing other concurrent
    /// failures.
    #[must_use]
    pub fn begin_transient_failure(&self, reason: impl Into<String>) -> SinkFailureGuard {
        let mut state = self.inner.lock();
        state.active_transient_failures = state.active_transient_failures.saturating_add(1);
        state.degraded_since.get_or_insert_with(Instant::now);
        state.last_transient_error = Some(reason.into());
        SinkFailureGuard {
            health: self.clone(),
            active: true,
        }
    }

    /// Record a deployment-level failure that cannot be resolved by retrying
    /// the same request.
    pub fn mark_blocked(&self, reason: impl Into<String>) {
        let mut state = self.inner.lock();
        state.blocked = Some(BlockedState {
            since: Instant::now(),
            reason: reason.into(),
        });
    }

    /// Return the current sink health.
    #[must_use]
    pub fn snapshot(&self) -> SinkHealthSnapshot {
        let state = self.inner.lock();
        if let Some(blocked) = &state.blocked {
            return SinkHealthSnapshot::Blocked {
                duration: blocked.since.elapsed(),
                reason: blocked.reason.clone(),
            };
        }
        if state.active_transient_failures > 0 {
            return SinkHealthSnapshot::Degraded {
                duration: state
                    .degraded_since
                    .map_or(Duration::ZERO, |at| at.elapsed()),
                reason: state
                    .last_transient_error
                    .clone()
                    .unwrap_or_else(|| "downstream sink unavailable".to_owned()),
            };
        }
        SinkHealthSnapshot::Healthy
    }

    fn finish_transient_failure(&self) {
        let mut state = self.inner.lock();
        state.active_transient_failures = state.active_transient_failures.saturating_sub(1);
        if state.active_transient_failures == 0 {
            state.degraded_since = None;
            state.last_transient_error = None;
        }
    }
}

/// Recovery guard for one retrying sink write.
#[derive(Debug)]
pub struct SinkFailureGuard {
    health: SinkHealth,
    active: bool,
}

impl Drop for SinkFailureGuard {
    fn drop(&mut self) {
        if self.active {
            self.health.finish_transient_failure();
            self.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_failure_guards_recover_independently() {
        let health = SinkHealth::new();
        let first = health.begin_transient_failure("timeout");
        let second = health.begin_transient_failure("429");

        assert!(matches!(
            health.snapshot(),
            SinkHealthSnapshot::Degraded { ref reason, .. } if reason == "429"
        ));
        drop(first);
        assert!(matches!(
            health.snapshot(),
            SinkHealthSnapshot::Degraded { .. }
        ));
        drop(second);
        assert_eq!(health.snapshot(), SinkHealthSnapshot::Healthy);
    }

    #[test]
    fn blocked_state_takes_precedence_over_transient_recovery() {
        let health = SinkHealth::new();
        let transient = health.begin_transient_failure("timeout");
        health.mark_blocked("authentication failed");
        drop(transient);

        assert!(matches!(
            health.snapshot(),
            SinkHealthSnapshot::Blocked { ref reason, .. }
                if reason == "authentication failed"
        ));
    }
}
