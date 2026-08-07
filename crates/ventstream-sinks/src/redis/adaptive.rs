use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tracing::{debug, warn};

const MIN_BATCH_BYTES: usize = 64 * 1024;
const MIN_PIPELINE_COMMANDS: usize = 32;
const RECOVERY_SUCCESS_INTERVAL: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RedisOperationLimits {
    pub(super) max_batch_bytes: usize,
    pub(super) max_commands: usize,
}

pub(super) struct RedisAdaptiveController {
    configured_batch_bytes: usize,
    configured_commands: usize,
    pool_ceiling: usize,
    concurrency: AtomicUsize,
    observed_concurrency_ceiling: AtomicUsize,
    batch_bytes: AtomicUsize,
    commands: AtomicUsize,
    success_streak: AtomicUsize,
    latency_ewma_micros: AtomicU64,
}

impl RedisAdaptiveController {
    pub(super) fn new(
        configured_batch_bytes: usize,
        configured_commands: usize,
        pool_ceiling: usize,
    ) -> Self {
        let pool_ceiling = pool_ceiling.max(1);
        let controller = Self {
            configured_batch_bytes: configured_batch_bytes.max(1),
            configured_commands: configured_commands.max(1),
            pool_ceiling,
            concurrency: AtomicUsize::new(pool_ceiling),
            observed_concurrency_ceiling: AtomicUsize::new(1),
            batch_bytes: AtomicUsize::new(configured_batch_bytes.max(1)),
            commands: AtomicUsize::new(configured_commands.max(1)),
            success_streak: AtomicUsize::new(0),
            latency_ewma_micros: AtomicU64::new(0),
        };
        controller.record_state();
        controller
    }

    pub(super) fn desired_concurrency(&self, configured_ceiling: usize) -> usize {
        let ceiling = configured_ceiling.clamp(1, self.pool_ceiling);
        self.observed_concurrency_ceiling
            .fetch_max(ceiling, Ordering::Relaxed);
        self.concurrency.load(Ordering::Relaxed).clamp(1, ceiling)
    }

    pub(super) fn operation_limits(&self) -> RedisOperationLimits {
        RedisOperationLimits {
            max_batch_bytes: self
                .batch_bytes
                .load(Ordering::Relaxed)
                .clamp(1, self.configured_batch_bytes),
            max_commands: self
                .commands
                .load(Ordering::Relaxed)
                .clamp(1, self.configured_commands),
        }
    }

    pub(super) fn on_success(&self, elapsed: Duration) {
        self.update_latency(elapsed);
        let streak = self.success_streak.fetch_add(1, Ordering::Relaxed) + 1;
        if !streak.is_multiple_of(RECOVERY_SUCCESS_INTERVAL) {
            return;
        }

        let mut changed = false;
        let ceiling = self
            .observed_concurrency_ceiling
            .load(Ordering::Relaxed)
            .clamp(1, self.pool_ceiling);
        changed |= increase_towards(&self.concurrency, ceiling, |current| current + 1);
        changed |= increase_towards(&self.batch_bytes, self.configured_batch_bytes, |current| {
            current.saturating_mul(2)
        });
        changed |= increase_towards(&self.commands, self.configured_commands, |current| {
            current.saturating_mul(2)
        });

        if changed {
            ventstream_telemetry::bump_redis_adaptive_recoveries();
            self.record_state();
            debug!(
                concurrency = self.concurrency.load(Ordering::Relaxed),
                batch_bytes = self.batch_bytes.load(Ordering::Relaxed),
                commands = self.commands.load(Ordering::Relaxed),
                latency_ewma_ms = self.latency_ewma_micros.load(Ordering::Relaxed) / 1_000,
                metric = "redis.adaptive_limits",
                "Redis adaptive limits increased after sustained successful writes"
            );
        }
    }

    pub(super) fn on_transient_failure(&self, capacity_pressure: bool) {
        self.success_streak.store(0, Ordering::Relaxed);
        let concurrency_changed = reduce_half(&self.concurrency, 1);
        let batch_changed = capacity_pressure
            && reduce_half(
                &self.batch_bytes,
                self.configured_batch_bytes.clamp(1, MIN_BATCH_BYTES),
            );
        let commands_changed = capacity_pressure
            && reduce_half(
                &self.commands,
                self.configured_commands.clamp(1, MIN_PIPELINE_COMMANDS),
            );

        if concurrency_changed || batch_changed || commands_changed {
            ventstream_telemetry::bump_redis_adaptive_reductions(capacity_pressure);
            self.record_state();
            warn!(
                concurrency = self.concurrency.load(Ordering::Relaxed),
                batch_bytes = self.batch_bytes.load(Ordering::Relaxed),
                commands = self.commands.load(Ordering::Relaxed),
                capacity_pressure,
                metric = "redis.adaptive_limits",
                "Redis adaptive limits reduced after a transient failure"
            );
        }
    }

    fn update_latency(&self, elapsed: Duration) {
        let sample = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let previous = self.latency_ewma_micros.load(Ordering::Relaxed);
        let ewma = if previous == 0 {
            sample
        } else {
            previous.saturating_mul(7).saturating_add(sample) / 8
        };
        self.latency_ewma_micros.store(ewma, Ordering::Relaxed);
    }

    fn record_state(&self) {
        ventstream_telemetry::record_redis_adaptive_limits(
            self.concurrency.load(Ordering::Relaxed),
            self.batch_bytes.load(Ordering::Relaxed),
            self.commands.load(Ordering::Relaxed),
        );
    }
}

fn reduce_half(value: &AtomicUsize, floor: usize) -> bool {
    let mut current = value.load(Ordering::Relaxed).max(floor);
    loop {
        let desired = (current / 2).max(floor);
        if desired == current {
            return false;
        }
        match value.compare_exchange_weak(current, desired, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => current = observed.max(floor),
        }
    }
}

fn increase_towards(value: &AtomicUsize, ceiling: usize, next: impl Fn(usize) -> usize) -> bool {
    let mut current = value.load(Ordering::Relaxed).min(ceiling);
    loop {
        let desired = next(current).min(ceiling);
        if desired == current {
            return false;
        }
        match value.compare_exchange_weak(current, desired, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => current = observed.min(ceiling),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_reduces_limits_and_success_recovers_them() {
        let controller = RedisAdaptiveController::new(16 * 1024 * 1024, 1_000, 8);
        assert_eq!(controller.desired_concurrency(16), 8);

        controller.on_transient_failure(true);
        assert_eq!(controller.desired_concurrency(16), 4);
        assert_eq!(
            controller.operation_limits(),
            RedisOperationLimits {
                max_batch_bytes: 8 * 1024 * 1024,
                max_commands: 500,
            }
        );

        for _ in 0..RECOVERY_SUCCESS_INTERVAL {
            controller.on_success(Duration::from_millis(2));
        }
        assert_eq!(controller.desired_concurrency(16), 5);
        assert_eq!(
            controller.operation_limits(),
            RedisOperationLimits {
                max_batch_bytes: 16 * 1024 * 1024,
                max_commands: 1_000,
            }
        );
    }

    #[test]
    fn transport_failure_only_reduces_concurrency() {
        let controller = RedisAdaptiveController::new(1024 * 1024, 128, 8);
        controller.on_transient_failure(false);
        assert_eq!(controller.desired_concurrency(8), 4);
        assert_eq!(
            controller.operation_limits(),
            RedisOperationLimits {
                max_batch_bytes: 1024 * 1024,
                max_commands: 128,
            }
        );
    }

    #[test]
    fn limits_stop_at_floors_and_operator_ceiling() {
        let controller = RedisAdaptiveController::new(16 * 1024 * 1024, 1_000, 8);
        assert_eq!(controller.desired_concurrency(2), 2);
        for _ in 0..16 {
            controller.on_transient_failure(true);
        }
        assert_eq!(controller.desired_concurrency(2), 1);
        assert_eq!(
            controller.operation_limits().max_batch_bytes,
            MIN_BATCH_BYTES
        );
        assert_eq!(
            controller.operation_limits().max_commands,
            MIN_PIPELINE_COMMANDS
        );
    }
}
