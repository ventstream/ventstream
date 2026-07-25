//! Retry policy with exponential backoff.
//!
//! Pure schedule generator — no I/O, no async, no clock. The caller
//! drives the actual delay (`tokio::time::sleep`). This split keeps the
//! policy deterministically testable.

use std::time::Duration;

use super::config::RetryConfig;

/// Iterator-style schedule over backoff delays.
///
/// Yields the delay to wait *before* the next attempt. `next()` returns
/// `None` once `max_attempts` has been reached (i.e. the caller should
/// give up).
pub struct BackoffSchedule {
    config: RetryConfig,
    attempt: u32,
    next_delay: Duration,
}

impl BackoffSchedule {
    /// Construct a fresh schedule. The first call to [`next`](Self::next)
    /// yields `initial_backoff`; subsequent calls multiply by
    /// `backoff_factor` up to `max_backoff`.
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            attempt: 0,
            next_delay: config.initial_backoff,
        }
    }

    /// Number of attempts taken so far. Useful for logging.
    pub fn attempts_so_far(&self) -> u32 {
        self.attempt
    }
}

impl Iterator for BackoffSchedule {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        // `max_attempts` includes the initial attempt, so a value of 5
        // permits 4 retries (4 delays from this iterator).
        if self.attempt + 1 >= self.config.max_attempts {
            return None;
        }
        let delay = self.next_delay;
        self.attempt += 1;
        // Saturating multiply so a runaway factor cannot overflow.
        let next = mul_duration_f64_saturating(delay, self.config.backoff_factor);
        self.next_delay = next.min(self.config.max_backoff);
        Some(delay)
    }
}

/// Multiply a `Duration` by an `f64`, saturating at `Duration::MAX` for
/// non-finite or extreme values. `Duration::mul_f64` panics on those —
/// our `clippy::panic = deny` lint forbids that path entirely.
fn mul_duration_f64_saturating(d: Duration, factor: f64) -> Duration {
    if !factor.is_finite() || factor < 0.0 {
        return Duration::ZERO;
    }
    let nanos = d.as_nanos() as f64 * factor;
    if nanos.is_nan() || nanos < 0.0 {
        return Duration::ZERO;
    }
    if nanos >= u64::MAX as f64 {
        return Duration::MAX;
    }
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    {
        Duration::from_nanos(nanos as u64)
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

    fn cfg(max_attempts: u32) -> RetryConfig {
        RetryConfig {
            max_attempts,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_factor: 2.0,
        }
    }

    #[test]
    fn schedule_yields_initial_then_doubles() {
        let mut s = BackoffSchedule::new(cfg(5));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
        assert_eq!(s.next(), Some(Duration::from_millis(200)));
        assert_eq!(s.next(), Some(Duration::from_millis(400)));
        assert_eq!(s.next(), Some(Duration::from_millis(800)));
        // 5 attempts = 4 retries = 4 delays. Fifth call yields None.
        assert_eq!(s.next(), None);
    }

    #[test]
    fn schedule_caps_at_max_backoff() {
        let mut s = BackoffSchedule::new(RetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(8),
            backoff_factor: 4.0,
        });
        assert_eq!(s.next(), Some(Duration::from_secs(5)));
        // Next would be 20s before cap → clamped to 8s.
        assert_eq!(s.next(), Some(Duration::from_secs(8)));
        assert_eq!(s.next(), Some(Duration::from_secs(8)));
    }

    #[test]
    fn schedule_with_one_max_attempt_yields_no_delays() {
        let mut s = BackoffSchedule::new(cfg(1));
        assert_eq!(s.next(), None);
    }

    #[test]
    fn mul_duration_handles_non_finite_factor_safely() {
        assert_eq!(
            mul_duration_f64_saturating(Duration::from_secs(1), f64::NAN),
            Duration::ZERO
        );
        assert_eq!(
            mul_duration_f64_saturating(Duration::from_secs(1), f64::INFINITY),
            Duration::ZERO
        );
        assert_eq!(
            mul_duration_f64_saturating(Duration::from_secs(1), -1.0),
            Duration::ZERO
        );
    }
}
