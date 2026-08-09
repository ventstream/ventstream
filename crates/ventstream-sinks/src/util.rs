//! Small helpers shared across sinks.

use std::time::Duration;

use rand::Rng as _;

/// Randomize a backoff delay to 50–100% of the scheduled value so
/// concurrent retriers do not synchronize against the same target.
pub(crate) fn jittered_delay(delay: Duration) -> Duration {
    if delay.is_zero() {
        return delay;
    }
    let factor = rand::thread_rng().gen_range(0.5..=1.0);
    delay.mul_f64(factor)
}
