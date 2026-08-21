//! Always-on process resource sampler.
//!
//! Publishes the engine's own CPU and memory footprint as gauges every
//! [`SAMPLE_INTERVAL`], regardless of role or memory-controller state —
//! the managed telemetry loop forwards them to the control plane so the
//! dashboard can show per-agent `cpu% · rss / limit` live. Rates are
//! computed HERE against the sampler's own cadence: downstream
//! consumers must never derive a rate from two counters they fetched at
//! unknown times.
//!
//! Readings come from `sysinfo` (safe wrappers over /proc, Mach, and
//! Win32 — matching the platforms we ship binaries for). Container
//! limits are read from cgroup v2/v1 control files on Linux so the UI
//! can render utilization against the actual budget. A platform that
//! fails to produce a reading simply skips the gauge — absence, never
//! zero, so the UI can distinguish "old engine / unsupported" from
//! "idle".

use std::time::Duration;

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use ventstream_core::ShutdownToken;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// Spawn the sampler. Cheap enough to run unconditionally.
pub(crate) fn spawn(shutdown: ShutdownToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Static limits are read once: a container's cgroup limits do
        // not change under a running process (a pod resize replaces it).
        if let Some(limit) = memory_limit_bytes() {
            metrics::gauge!("vs_resource_memory_limit_bytes").set(limit as f64);
        }
        if let Some(millicores) = cpu_limit_millicores() {
            metrics::gauge!("vs_resource_cpu_limit_millicores").set(millicores as f64);
        }

        let Ok(pid) = sysinfo::get_current_pid() else {
            return;
        };
        let refresh = ProcessRefreshKind::nothing().with_cpu().with_memory();
        let mut system = System::new();
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First refresh primes the CPU accounting; percentages start on
        // the second tick.
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = tick.tick() => {}
            }
            system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh);
            let Some(process) = system.process(pid) else {
                continue;
            };
            let rss = process.memory();
            if rss > 0 {
                metrics::gauge!("vs_process_rss_bytes").set(rss as f64);
            }
            // Percent of ONE core; may exceed 100 on multicore. The UI
            // normalizes against the reported cpu limit.
            let cpu = f64::from(process.cpu_usage());
            if cpu.is_finite() && cpu >= 0.0 {
                metrics::gauge!("vs_process_cpu_percent").set(cpu);
            }
        }
    })
}

/// Container memory limit, if one is imposed (cgroup v2 then v1).
#[cfg(target_os = "linux")]
fn memory_limit_bytes() -> Option<u64> {
    for path in cgroup_candidates("memory.max", "memory", "memory.limit_in_bytes") {
        if let Ok(raw) = std::fs::read_to_string(&path) {
            let raw = raw.trim();
            if raw == "max" {
                continue;
            }
            if let Ok(value) = raw.parse::<u64>() {
                // v1 reports an enormous sentinel when unlimited.
                if value < u64::MAX / 2 {
                    return Some(value);
                }
            }
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn memory_limit_bytes() -> Option<u64> {
    None
}

/// Container CPU quota in millicores (cgroup v2 `cpu.max`, v1 quota/period).
#[cfg(target_os = "linux")]
fn cpu_limit_millicores() -> Option<u64> {
    for path in cgroup_candidates("cpu.max", "cpu", "cpu.cfs_quota_us") {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let raw = raw.trim();
        if path.ends_with("cpu.max") {
            let mut parts = raw.split_ascii_whitespace();
            let quota = parts.next()?;
            if quota == "max" {
                continue;
            }
            let quota: u64 = quota.parse().ok()?;
            let period: u64 = parts.next().unwrap_or("100000").parse().ok()?;
            if period > 0 {
                return Some(quota.saturating_mul(1000) / period);
            }
        } else {
            // v1: pair with cpu.cfs_period_us alongside.
            let quota: i64 = raw.parse().ok()?;
            if quota <= 0 {
                continue;
            }
            let period_path = path.replace("cpu.cfs_quota_us", "cpu.cfs_period_us");
            let period: u64 = std::fs::read_to_string(period_path)
                .ok()?
                .trim()
                .parse()
                .ok()?;
            if period > 0 {
                return Some((quota as u64).saturating_mul(1000) / period);
            }
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn cpu_limit_millicores() -> Option<u64> {
    None
}

/// Candidate file paths for a cgroup control file: unified root, the
/// process's own v2 relative path, then the v1 controller hierarchy —
/// the same discovery order the memory controller uses.
#[cfg(target_os = "linux")]
fn cgroup_candidates(v2_file: &str, v1_controller: &str, v1_file: &str) -> Vec<String> {
    let mut out = vec![format!("/sys/fs/cgroup/{v2_file}")];
    if let Ok(cgroup) = std::fs::read_to_string("/proc/self/cgroup") {
        for line in cgroup.lines() {
            let mut parts = line.splitn(3, ':');
            let (Some(_), Some(controllers), Some(path)) =
                (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let path = path.trim_start_matches('/');
            if controllers.is_empty() {
                out.push(format!("/sys/fs/cgroup/{path}/{v2_file}"));
            } else if controllers.split(',').any(|c| c == v1_controller) {
                out.push(format!("/sys/fs/cgroup/{v1_controller}/{path}/{v1_file}"));
            }
        }
    }
    out.push(format!("/sys/fs/cgroup/{v1_controller}/{v1_file}"));
    out
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "test assertions may panic to fail the test"
)]
mod tests {
    use super::*;

    #[test]
    fn own_process_reports_rss_and_cpu_fields() {
        let pid = sysinfo::get_current_pid().expect("pid");
        let refresh = ProcessRefreshKind::nothing().with_cpu().with_memory();
        let mut system = System::new();
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh);
        std::thread::sleep(std::time::Duration::from_millis(120));
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh);
        let process = system.process(pid).expect("own process visible");
        assert!(
            process.memory() > 1024 * 1024,
            "test process RSS should exceed 1 MiB, got {}",
            process.memory()
        );
        let cpu = f64::from(process.cpu_usage());
        assert!(
            cpu.is_finite() && cpu >= 0.0,
            "cpu usage must be a sane percent"
        );
    }
}
