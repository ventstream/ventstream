//! Byte-weighted admission control for events retained by the engine.
//!
//! Event-count channel bounds are necessary but insufficient: one event can
//! be a few bytes or many megabytes. [`MemoryBudget`] adds a process-local
//! byte ceiling that follows an event through bus queues, sink batches,
//! retries, and zero-copy clones.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;

use crate::{BackpressureError, ShutdownToken};

/// Runtime memory pressure reported by the engine's process/cgroup monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MemoryPressure {
    /// Memory is below the configured target.
    Normal = 0,
    /// Memory crossed the target; trim admission and request sizes.
    Constrained = 1,
    /// Memory is close to the container limit.
    High = 2,
    /// Memory is at immediate OOM risk.
    Critical = 3,
}

impl MemoryPressure {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Constrained,
            2 => Self::High,
            3 => Self::Critical,
            _ => Self::Normal,
        }
    }

    /// Stable numeric representation exported through metrics.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Where an event is entering the processing graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MemoryAdmission {
    /// Source events flow directly to a sink; the full event budget is usable.
    Direct = 0,
    /// Source events enter a transform stage. Capacity is reserved for output.
    TransformInput = 1,
    /// Newly allocated transform output may consume the reserved headroom.
    TransformOutput = 2,
}

impl MemoryAdmission {
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::TransformInput,
            2 => Self::TransformOutput,
            _ => Self::Direct,
        }
    }
}

/// Shared byte budget for all retained events in one engine process.
#[derive(Debug)]
pub struct MemoryBudget {
    hard_limit_bytes: u64,
    max_event_bytes: u64,
    used_bytes: AtomicU64,
    pressure: AtomicU8,
    notify: Notify,
}

impl MemoryBudget {
    /// Build a budget. The individual-event limit is capped at one quarter of
    /// the total so a transform can always make progress from reserved
    /// headroom even when its input queue is full.
    pub fn new(hard_limit_bytes: u64, max_event_bytes: u64) -> Arc<Self> {
        let hard_limit_bytes = hard_limit_bytes.max(1);
        let max_from_headroom = (hard_limit_bytes / 4).max(1);
        let max_event_bytes = max_event_bytes.max(1).min(max_from_headroom);
        let budget = Arc::new(Self {
            hard_limit_bytes,
            max_event_bytes,
            used_bytes: AtomicU64::new(0),
            pressure: AtomicU8::new(MemoryPressure::Normal.as_u8()),
            notify: Notify::new(),
        });
        budget.record_metrics();
        budget
    }

    /// Maximum bytes retained by admitted events.
    pub const fn hard_limit_bytes(&self) -> u64 {
        self.hard_limit_bytes
    }

    /// Maximum charge accepted for one event.
    pub const fn max_event_bytes(&self) -> u64 {
        self.max_event_bytes
    }

    /// Current bytes retained by live event reservations.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    /// Current process/cgroup pressure state.
    pub fn pressure(&self) -> MemoryPressure {
        MemoryPressure::from_u8(self.pressure.load(Ordering::Acquire))
    }

    /// Update pressure and wake blocked producers if the effective limit grew.
    pub fn set_pressure(&self, pressure: MemoryPressure) -> MemoryPressure {
        let previous =
            MemoryPressure::from_u8(self.pressure.swap(pressure.as_u8(), Ordering::AcqRel));
        if previous != pressure {
            self.notify.notify_waiters();
        }
        self.record_metrics();
        previous
    }

    /// Constrain sink concurrency while composing with sink-local adaptation.
    pub fn recommended_concurrency(&self, configured_ceiling: usize) -> usize {
        let ceiling = configured_ceiling.max(1);
        match self.pressure() {
            MemoryPressure::Normal => ceiling,
            MemoryPressure::Constrained => ceiling.div_ceil(2).max(1),
            MemoryPressure::High => ceiling.div_ceil(4).max(1),
            MemoryPressure::Critical => 1,
        }
    }

    /// Reduce temporary request-body allocations under pressure.
    pub fn recommended_batch_bytes(&self, configured_bytes: usize) -> usize {
        let configured = configured_bytes.max(1);
        let scaled = match self.pressure() {
            MemoryPressure::Normal => configured,
            MemoryPressure::Constrained => configured / 2,
            MemoryPressure::High => configured / 4,
            MemoryPressure::Critical => configured / 8,
        };
        scaled.max(1)
    }

    /// Acquire an event reservation, waiting for byte capacity or shutdown.
    pub(crate) async fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        admission: MemoryAdmission,
        shutdown: &ShutdownToken,
    ) -> Result<Arc<MemoryReservation>, BackpressureError> {
        let charge = bytes.max(1);
        self.validate_event_size(charge)?;
        let blocked_at = Instant::now();
        let mut recorded_wait = false;

        loop {
            // Register before checking so a release between the check and await
            // is retained by Notify rather than becoming a lost wakeup.
            let notified = self.notify.notified();
            match self.try_reserve_inner(charge, admission) {
                Ok(reservation) => {
                    if recorded_wait {
                        metrics::histogram!("vs_memory_admission_wait_seconds")
                            .record(blocked_at.elapsed().as_secs_f64());
                    }
                    return Ok(reservation);
                }
                Err(BackpressureError::MemoryFull { .. }) => {
                    if !recorded_wait {
                        recorded_wait = true;
                        metrics::counter!("vs_memory_throttle_total").increment(1);
                        tracing::debug!(
                            requested_bytes = charge,
                            used_bytes = self.used_bytes(),
                            admission_limit_bytes = self.admission_limit(admission),
                            pressure = ?self.pressure(),
                            metric = "memory.admission.blocked",
                            "event admission waiting for memory budget"
                        );
                    }
                }
                Err(other) => return Err(other),
            }

            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Err(BackpressureError::Cancelled),
                () = notified => {}
            }
        }
    }

    /// Attempt a reservation without waiting.
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: u64,
        admission: MemoryAdmission,
    ) -> Result<Arc<MemoryReservation>, BackpressureError> {
        let charge = bytes.max(1);
        self.validate_event_size(charge)?;
        self.try_reserve_inner(charge, admission)
    }

    /// Refresh budget gauges from a low-frequency monitor task.
    pub fn record_metrics(&self) {
        metrics::gauge!("vs_memory_budget_bytes").set(self.hard_limit_bytes as f64);
        metrics::gauge!("vs_memory_event_max_bytes").set(self.max_event_bytes as f64);
        metrics::gauge!("vs_memory_reserved_bytes").set(self.used_bytes() as f64);
        metrics::gauge!("vs_memory_pressure_state").set(self.pressure().as_u8() as f64);
    }

    fn validate_event_size(&self, charge: u64) -> Result<(), BackpressureError> {
        if charge > self.max_event_bytes {
            metrics::counter!("vs_memory_oversized_events_total").increment(1);
            return Err(BackpressureError::EventTooLarge {
                estimated_bytes: charge,
                max_bytes: self.max_event_bytes,
            });
        }
        Ok(())
    }

    fn try_reserve_inner(
        self: &Arc<Self>,
        charge: u64,
        admission: MemoryAdmission,
    ) -> Result<Arc<MemoryReservation>, BackpressureError> {
        let limit = self.admission_limit(admission);
        let mut used = self.used_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(charge) else {
                return Err(BackpressureError::MemoryFull {
                    requested_bytes: charge,
                    used_bytes: used,
                    limit_bytes: limit,
                });
            };
            if next > limit {
                return Err(BackpressureError::MemoryFull {
                    requested_bytes: charge,
                    used_bytes: used,
                    limit_bytes: limit,
                });
            }
            match self.used_bytes.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Arc::new(MemoryReservation {
                        budget: Arc::clone(self),
                        bytes: charge,
                    }));
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn admission_limit(&self, admission: MemoryAdmission) -> u64 {
        let percent = match (self.pressure(), admission) {
            (MemoryPressure::Normal, MemoryAdmission::TransformInput) => 70,
            (MemoryPressure::Normal, _) => 100,
            (MemoryPressure::Constrained, MemoryAdmission::TransformInput) => 35,
            (MemoryPressure::Constrained, _) => 65,
            (MemoryPressure::High, MemoryAdmission::TransformInput) => 15,
            (MemoryPressure::High, MemoryAdmission::TransformOutput) => 50,
            (MemoryPressure::High, MemoryAdmission::Direct) => 45,
            (MemoryPressure::Critical, MemoryAdmission::TransformInput) => 5,
            (MemoryPressure::Critical, MemoryAdmission::TransformOutput) => 50,
            (MemoryPressure::Critical, MemoryAdmission::Direct) => 25,
        };
        self.hard_limit_bytes
            .saturating_mul(percent)
            .div_ceil(100)
            .max(self.max_event_bytes)
            .min(self.hard_limit_bytes)
    }
}

/// Shared by all zero-copy clones of one payload. The charge is released only
/// after the last clone leaves the sink/retry/DLQ path.
#[derive(Debug)]
pub(crate) struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: u64,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.budget
            .used_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.notify.notify_one();
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn transform_input_preserves_output_headroom() {
        let budget = MemoryBudget::new(1_000, 250);
        let first = budget
            .try_reserve(250, MemoryAdmission::TransformInput)
            .expect("first");
        let second = budget
            .try_reserve(250, MemoryAdmission::TransformInput)
            .expect("second");
        assert!(budget
            .try_reserve(250, MemoryAdmission::TransformInput)
            .is_err());
        let output = budget
            .try_reserve(250, MemoryAdmission::TransformOutput)
            .expect("reserved transform headroom");
        assert_eq!(budget.used_bytes(), 750);
        drop((first, second, output));
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn pressure_reduces_concurrency_and_batch_size() {
        let budget = MemoryBudget::new(1_000, 100);
        assert_eq!(budget.recommended_concurrency(8), 8);
        assert_eq!(budget.recommended_batch_bytes(1_000), 1_000);
        budget.set_pressure(MemoryPressure::High);
        assert_eq!(budget.recommended_concurrency(8), 2);
        assert_eq!(budget.recommended_batch_bytes(1_000), 250);
        budget.set_pressure(MemoryPressure::Critical);
        assert_eq!(budget.recommended_concurrency(8), 1);
        assert_eq!(budget.recommended_batch_bytes(1_000), 125);
    }

    #[test]
    fn critical_pressure_still_preserves_one_event_of_transform_headroom() {
        let budget = MemoryBudget::new(1_000, 250);
        budget.set_pressure(MemoryPressure::Critical);
        let input = budget
            .try_reserve(250, MemoryAdmission::TransformInput)
            .expect("one input");
        let output = budget
            .try_reserve(250, MemoryAdmission::TransformOutput)
            .expect("one replacement output");
        assert_eq!(budget.used_bytes(), 500);
        drop((input, output));
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn blocked_reservation_resumes_after_release() {
        let budget = MemoryBudget::new(1_000, 250);
        let held = budget
            .try_reserve(250, MemoryAdmission::Direct)
            .expect("held");
        let held_two = budget
            .try_reserve(250, MemoryAdmission::Direct)
            .expect("held two");
        let held_three = budget
            .try_reserve(250, MemoryAdmission::Direct)
            .expect("held three");
        let held_four = budget
            .try_reserve(250, MemoryAdmission::Direct)
            .expect("held four");
        let waiter_budget = Arc::clone(&budget);
        let shutdown = ShutdownToken::new();
        let waiter_shutdown = shutdown.clone();
        let waiter = tokio::spawn(async move {
            waiter_budget
                .reserve(250, MemoryAdmission::Direct, &waiter_shutdown)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(held);
        let reservation = waiter.await.expect("join").expect("reservation");
        drop((held_two, held_three, held_four, reservation));
        assert_eq!(budget.used_bytes(), 0);
    }
}
