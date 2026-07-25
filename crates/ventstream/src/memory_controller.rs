//! Adaptive memory protection for CDC engine roles.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use ventstream_core::{MemoryBudget, MemoryPressure, ShutdownToken};

const AUTO_EVENT_BUDGET_PERCENT: u64 = 30;

/// Validated runtime settings loaded from YAML/environment variables.
#[derive(Debug, Clone)]
pub struct MemoryControllerConfig {
    pub enabled: bool,
    pub budget_bytes: Option<u64>,
    pub max_event_bytes: u64,
    pub sample_interval: Duration,
    pub recovery_interval: Duration,
    pub target_percent: u8,
    pub high_percent: u8,
    pub critical_percent: u8,
    pub hysteresis_percent: u8,
}

impl Default for MemoryControllerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            budget_bytes: None,
            max_event_bytes: 32 * 1024 * 1024,
            sample_interval: Duration::from_millis(100),
            recovery_interval: Duration::from_secs(1),
            target_percent: 65,
            high_percent: 75,
            critical_percent: 85,
            hysteresis_percent: 5,
        }
    }
}

/// One controller instance and its byte budget.
#[derive(Debug, Clone)]
pub struct MemoryRuntime {
    budget: Arc<MemoryBudget>,
    probe: Option<CgroupProbe>,
    config: MemoryControllerConfig,
}

impl MemoryRuntime {
    /// Activate when explicitly budgeted or when a finite container cgroup
    /// limit is visible. Bare-metal processes retain legacy behavior unless
    /// operators opt in with `runtime.memory.budget_bytes`.
    pub fn detect(config: &MemoryControllerConfig) -> Option<Self> {
        if !config.enabled {
            info!(metric = "memory.controller", "memory controller disabled");
            return None;
        }

        let probe = CgroupProbe::detect();
        let cgroup_limit = probe.as_ref().map(|probe| probe.limit_bytes);
        let hard_limit = config.budget_bytes.or_else(|| {
            cgroup_limit.map(|limit| limit.saturating_mul(AUTO_EVENT_BUDGET_PERCENT) / 100)
        })?;
        let budget = MemoryBudget::new(hard_limit, config.max_event_bytes);
        if budget.max_event_bytes() < config.max_event_bytes {
            warn!(
                requested_max_event_bytes = config.max_event_bytes,
                effective_max_event_bytes = budget.max_event_bytes(),
                event_budget_bytes = budget.hard_limit_bytes(),
                metric = "memory.controller.event_limit_clamped",
                "max event size was clamped to preserve transform headroom"
            );
        }
        info!(
            event_budget_bytes = budget.hard_limit_bytes(),
            max_event_bytes = budget.max_event_bytes(),
            cgroup_limit_bytes = cgroup_limit,
            sample_ms = config.sample_interval.as_millis() as u64,
            recovery_ms = config.recovery_interval.as_millis() as u64,
            target_percent = config.target_percent,
            high_percent = config.high_percent,
            critical_percent = config.critical_percent,
            metric = "memory.controller",
            "adaptive memory controller enabled"
        );
        Some(Self {
            budget,
            probe,
            config: config.clone(),
        })
    }

    pub fn budget(&self) -> Arc<MemoryBudget> {
        Arc::clone(&self.budget)
    }

    pub fn spawn(&self, shutdown: ShutdownToken) -> JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move { runtime.run(shutdown).await })
    }

    async fn run(self, shutdown: ShutdownToken) {
        let mut ticker = interval(self.config.sample_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut recovery_started = None;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => self.sample(&mut recovery_started).await,
            }
        }
    }

    async fn sample(&self, recovery_started: &mut Option<Instant>) {
        self.budget.record_metrics();
        if let Some(rss) = read_process_rss_bytes().await {
            metrics::gauge!("vs_memory_process_rss_bytes").set(rss as f64);
        }

        let Some(probe) = &self.probe else {
            return;
        };
        let Some(current) = probe.current_bytes().await else {
            metrics::counter!("vs_memory_probe_errors_total").increment(1);
            return;
        };
        metrics::gauge!("vs_memory_cgroup_current_bytes").set(current as f64);
        metrics::gauge!("vs_memory_cgroup_limit_bytes").set(probe.limit_bytes as f64);
        let percent = current.saturating_mul(100) / probe.limit_bytes.max(1);
        metrics::gauge!("vs_memory_cgroup_utilization_percent").set(percent as f64);

        let previous = self.budget.pressure();
        let desired = next_pressure(previous, percent, &self.config);
        let next = stabilize_recovery(
            previous,
            desired,
            recovery_started,
            Instant::now(),
            self.config.recovery_interval,
        );
        if next != previous {
            self.budget.set_pressure(next);
            match next {
                MemoryPressure::Normal => info!(
                    previous = ?previous,
                    current = ?next,
                    utilization_percent = percent,
                    current_bytes = current,
                    limit_bytes = probe.limit_bytes,
                    metric = "memory.pressure.transition",
                    "memory pressure recovered"
                ),
                _ => warn!(
                    previous = ?previous,
                    current = ?next,
                    utilization_percent = percent,
                    current_bytes = current,
                    limit_bytes = probe.limit_bytes,
                    reserved_event_bytes = self.budget.used_bytes(),
                    metric = "memory.pressure.transition",
                    "memory pressure controls adjusted"
                ),
            }
        }
    }
}

fn stabilize_recovery(
    previous: MemoryPressure,
    desired: MemoryPressure,
    recovery_started: &mut Option<Instant>,
    now: Instant,
    recovery_interval: Duration,
) -> MemoryPressure {
    if desired.as_u8() >= previous.as_u8() {
        *recovery_started = None;
        return desired;
    }

    let started = recovery_started.get_or_insert(now);
    if now.saturating_duration_since(*started) < recovery_interval {
        return previous;
    }

    *recovery_started = None;
    desired
}

#[derive(Debug, Clone)]
struct CgroupProbe {
    current_path: PathBuf,
    limit_bytes: u64,
}

impl CgroupProbe {
    fn detect() -> Option<Self> {
        let mut candidates = vec![
            (
                PathBuf::from("/sys/fs/cgroup/memory.current"),
                PathBuf::from("/sys/fs/cgroup/memory.max"),
            ),
            (
                PathBuf::from("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
                PathBuf::from("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            ),
        ];
        if let Ok(cgroup) = std::fs::read_to_string("/proc/self/cgroup") {
            if let Some(relative) = cgroup_path(&cgroup, None) {
                let base = PathBuf::from("/sys/fs/cgroup").join(relative);
                candidates.push((base.join("memory.current"), base.join("memory.max")));
            }
            if let Some(relative) = cgroup_path(&cgroup, Some("memory")) {
                let base = PathBuf::from("/sys/fs/cgroup/memory").join(relative);
                candidates.push((
                    base.join("memory.usage_in_bytes"),
                    base.join("memory.limit_in_bytes"),
                ));
            }
        }
        candidates
            .into_iter()
            .find_map(|(current_path, limit_path)| {
                let raw = std::fs::read_to_string(limit_path).ok()?;
                let limit_bytes = parse_finite_limit(&raw)?;
                current_path.exists().then_some(Self {
                    current_path,
                    limit_bytes,
                })
            })
    }

    async fn current_bytes(&self) -> Option<u64> {
        let raw = tokio::fs::read_to_string(&self.current_path).await.ok()?;
        raw.trim().parse().ok()
    }
}

fn cgroup_path(contents: &str, controller: Option<&str>) -> Option<PathBuf> {
    contents.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?.trim_start_matches('/');
        let matches = match controller {
            None => hierarchy == "0" && controllers.is_empty(),
            Some(expected) => controllers.split(',').any(|item| item == expected),
        };
        matches.then(|| PathBuf::from(path))
    })
}

fn parse_finite_limit(raw: &str) -> Option<u64> {
    let value = raw.trim();
    if value == "max" {
        return None;
    }
    let parsed = value.parse::<u64>().ok()?;
    // cgroup v1 uses near-u64::MAX sentinels to mean unlimited.
    (parsed > 0 && parsed < (1u64 << 60)).then_some(parsed)
}

async fn read_process_rss_bytes() -> Option<u64> {
    let status = tokio::fs::read_to_string("/proc/self/status").await.ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kib.saturating_mul(1024))
}

fn next_pressure(
    previous: MemoryPressure,
    utilization_percent: u64,
    config: &MemoryControllerConfig,
) -> MemoryPressure {
    let target = u64::from(config.target_percent);
    let high = u64::from(config.high_percent);
    let critical = u64::from(config.critical_percent);
    let hysteresis = u64::from(config.hysteresis_percent);
    match previous {
        MemoryPressure::Normal if utilization_percent >= critical => MemoryPressure::Critical,
        MemoryPressure::Normal if utilization_percent >= high => MemoryPressure::High,
        MemoryPressure::Normal if utilization_percent >= target => MemoryPressure::Constrained,
        MemoryPressure::Normal => MemoryPressure::Normal,
        MemoryPressure::Constrained if utilization_percent >= critical => MemoryPressure::Critical,
        MemoryPressure::Constrained if utilization_percent >= high => MemoryPressure::High,
        MemoryPressure::Constrained if utilization_percent < target.saturating_sub(hysteresis) => {
            MemoryPressure::Normal
        }
        MemoryPressure::Constrained => MemoryPressure::Constrained,
        MemoryPressure::High if utilization_percent >= critical => MemoryPressure::Critical,
        MemoryPressure::High if utilization_percent < high.saturating_sub(hysteresis) => {
            MemoryPressure::Constrained
        }
        MemoryPressure::High => MemoryPressure::High,
        MemoryPressure::Critical if utilization_percent < critical.saturating_sub(hysteresis) => {
            MemoryPressure::High
        }
        MemoryPressure::Critical => MemoryPressure::Critical,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn pressure_uses_hysteresis_when_recovering() {
        let config = MemoryControllerConfig::default();
        assert_eq!(
            next_pressure(MemoryPressure::Normal, 65, &config),
            MemoryPressure::Constrained
        );
        assert_eq!(
            next_pressure(MemoryPressure::Constrained, 63, &config),
            MemoryPressure::Constrained
        );
        assert_eq!(
            next_pressure(MemoryPressure::Constrained, 59, &config),
            MemoryPressure::Normal
        );
        assert_eq!(
            next_pressure(MemoryPressure::High, 85, &config),
            MemoryPressure::Critical
        );
        assert_eq!(
            next_pressure(MemoryPressure::Normal, 93, &config),
            MemoryPressure::Critical
        );
        assert_eq!(
            next_pressure(MemoryPressure::Critical, 82, &config),
            MemoryPressure::Critical
        );
        assert_eq!(
            next_pressure(MemoryPressure::Critical, 79, &config),
            MemoryPressure::High
        );
    }

    #[test]
    fn pressure_recovers_only_after_a_stable_interval() {
        let now = Instant::now();
        let hold = Duration::from_secs(1);
        let mut recovery_started = None;

        assert_eq!(
            stabilize_recovery(
                MemoryPressure::High,
                MemoryPressure::Constrained,
                &mut recovery_started,
                now,
                hold,
            ),
            MemoryPressure::High
        );
        assert_eq!(
            stabilize_recovery(
                MemoryPressure::High,
                MemoryPressure::Constrained,
                &mut recovery_started,
                now + Duration::from_millis(999),
                hold,
            ),
            MemoryPressure::High
        );
        assert_eq!(
            stabilize_recovery(
                MemoryPressure::High,
                MemoryPressure::Constrained,
                &mut recovery_started,
                now + hold,
                hold,
            ),
            MemoryPressure::Constrained
        );
    }

    #[test]
    fn pressure_escalation_is_immediate_and_cancels_recovery() {
        let now = Instant::now();
        let mut recovery_started = Some(now);

        assert_eq!(
            stabilize_recovery(
                MemoryPressure::Constrained,
                MemoryPressure::Critical,
                &mut recovery_started,
                now + Duration::from_millis(10),
                Duration::from_secs(1),
            ),
            MemoryPressure::Critical
        );
        assert!(recovery_started.is_none());
    }

    #[test]
    fn cgroup_unlimited_sentinels_are_rejected() {
        assert_eq!(parse_finite_limit("max\n"), None);
        assert_eq!(parse_finite_limit("9223372036854771712"), None);
        assert_eq!(parse_finite_limit("1073741824"), Some(1_073_741_824));
    }

    #[test]
    fn cgroup_paths_support_v2_and_v1_layouts() {
        assert_eq!(
            cgroup_path("0::/user.slice/service.scope\n", None),
            Some(PathBuf::from("user.slice/service.scope"))
        );
        assert_eq!(
            cgroup_path("5:cpu,memory:/docker/abc\n", Some("memory")),
            Some(PathBuf::from("docker/abc"))
        );
    }
}
