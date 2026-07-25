//! Shared readiness signal for long-lived runtime roles.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Cloneable readiness flag shared between a runtime role and the health server.
///
/// Roles mark the signal ready only after their dependencies are initialized and
/// their traffic listener is bound. They must mark it not-ready when shutdown
/// begins or the role exits.
#[derive(Clone, Debug, Default)]
pub struct ReadinessSignal {
    ready: Arc<AtomicBool>,
}

impl ReadinessSignal {
    /// Create a signal in the not-ready state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Report whether the role currently accepts traffic.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Mark the role ready to accept traffic.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Mark the role unable to accept new traffic.
    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    /// Create an ownership guard that resets this signal when dropped.
    ///
    /// Long-lived roles keep the guard for their full runtime so errors,
    /// cancellation, and panic unwinding all return readiness to false.
    #[must_use]
    pub fn guard(&self) -> ReadinessGuard {
        self.mark_not_ready();
        ReadinessGuard {
            signal: self.clone(),
        }
    }
}

/// Resets a [`ReadinessSignal`] when its owning runtime role exits.
pub struct ReadinessGuard {
    signal: ReadinessSignal,
}

impl ReadinessGuard {
    /// Mark the guarded role ready.
    pub fn mark_ready(&self) {
        self.signal.mark_ready();
    }

    /// Clone the underlying signal for a shutdown callback.
    #[must_use]
    pub fn signal(&self) -> ReadinessSignal {
        self.signal.clone()
    }
}

impl Drop for ReadinessGuard {
    fn drop(&mut self) {
        self.signal.mark_not_ready();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_observe_transitions() {
        let signal = ReadinessSignal::new();
        let observer = signal.clone();

        assert!(!observer.is_ready());
        signal.mark_ready();
        assert!(observer.is_ready());
        signal.mark_not_ready();
        assert!(!observer.is_ready());
    }

    #[test]
    fn guard_resets_signal_on_drop() {
        let signal = ReadinessSignal::new();
        {
            let guard = signal.guard();
            guard.mark_ready();
            assert!(signal.is_ready());
        }
        assert!(!signal.is_ready());
    }
}
