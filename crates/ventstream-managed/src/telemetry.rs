use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ventstream_fleet_client::AgentRuntimeStatus;

const MAX_METRICS_BYTES: usize = 128 * 1024;
const MAX_CONFIGURATION_BYTES: u64 = 128 * 1024;
const EVENT_RATE_WINDOW: Duration = Duration::from_secs(60);
const MAX_EVENT_RATE_SAMPLES: usize = 16;

/// Samples the supervised engine through its loopback-only health server.
pub struct EngineTelemetrySampler {
    client: reqwest::Client,
    ready_url: reqwest::Url,
    metrics_url: reqwest::Url,
    applied_configuration_path: PathBuf,
    input_samples: VecDeque<(u64, Instant)>,
    output_samples: VecDeque<(u64, Instant)>,
    broker_observation_samples: VecDeque<(u64, Instant)>,
}

impl EngineTelemetrySampler {
    /// Creates a bounded local sampler from the already validated readiness URL.
    pub fn new(
        ready_url: reqwest::Url,
        applied_configuration_path: PathBuf,
    ) -> Result<Self, String> {
        let mut metrics_url = ready_url.clone();
        metrics_url.set_path("/metrics");
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "building engine telemetry client failed".to_owned())?;
        Ok(Self {
            client,
            ready_url,
            metrics_url,
            applied_configuration_path,
            input_samples: VecDeque::with_capacity(MAX_EVENT_RATE_SAMPLES),
            output_samples: VecDeque::with_capacity(MAX_EVENT_RATE_SAMPLES),
            broker_observation_samples: VecDeque::with_capacity(MAX_EVENT_RATE_SAMPLES),
        })
    }

    /// Returns a fresh readiness and delivery sample without surfacing connector secrets.
    pub async fn sample(&mut self) -> AgentRuntimeStatus {
        let (source_kind, sink_kind) = configuration_kinds(&self.applied_configuration_path);
        let ready = self
            .client
            .get(self.ready_url.clone())
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if !ready {
            return AgentRuntimeStatus {
                source_kind,
                sink_kind,
                engine_ready: Some(false),
                error_code: "engine_not_ready".to_owned(),
                safe_error_message: "Supervised engine readiness probe failed".to_owned(),
                ..AgentRuntimeStatus::default()
            };
        }

        let metrics = match self.read_metrics().await {
            Ok(metrics) => metrics,
            Err(()) => {
                return AgentRuntimeStatus {
                    source_kind,
                    sink_kind,
                    engine_ready: Some(true),
                    error_code: "engine_metrics_unavailable".to_owned(),
                    safe_error_message: "Supervised engine metrics are unavailable".to_owned(),
                    ..AgentRuntimeStatus::default()
                };
            }
        };
        let now = Instant::now();
        let realtime = sink_kind == "graphql" || sink_kind == "websocket";
        let input_total = if realtime {
            realtime_input_total(&metrics)
        } else {
            metric_u64(&metrics, "vs_events_received_total")
                .or_else(|| metric_u64(&metrics, "vs_events_emitted_total"))
        };
        let output_total = if realtime {
            metric_u64(&metrics, "vs_realtime_events_emitted_total")
                .or_else(|| metric_u64(&metrics, "vs_realtime_frames_written_total"))
        } else {
            metric_u64(&metrics, "vs_events_delivered_total")
        };
        let events_per_second = observe_rate(&mut self.input_samples, input_total, now);
        let events_delivered_per_second = observe_rate(&mut self.output_samples, output_total, now);
        let broker_observations_total = realtime
            .then(|| metric_u64(&metrics, "vs_realtime_broker_messages_total"))
            .flatten();
        let realtime_broker_observations_per_second = observe_rate(
            &mut self.broker_observation_samples,
            broker_observations_total,
            now,
        );
        let lifecycle_phase = metric_u64(&metrics, "vs_lifecycle_phase").unwrap_or(0);
        let pipeline_error = lifecycle_phase == 3;

        AgentRuntimeStatus {
            source_kind,
            sink_kind,
            cursor_age_milliseconds: metric_u64(&metrics, "vs_cursor_age_ms")
                .filter(|value| *value > 0),
            events_per_second,
            events_received_total: input_total,
            events_delivered_total: output_total,
            events_delivered_per_second,
            events_failed_total: metric_u64(&metrics, "vs_events_failed_total"),
            sink_retries_total: metric_u64(&metrics, "vs_sink_retries_total"),
            dlq_writes_total: metric_u64(&metrics, "vs_dlq_writes_total"),
            dlq_write_failures_total: metric_u64(&metrics, "vs_dlq_write_failures_total"),
            backpressure_total: metric_u64(&metrics, "vs_backpressure_events_total"),
            sink_latency_p95_milliseconds: metric_u64(&metrics, "vs_bulk_write_p95_ms")
                .filter(|value| *value > 0),
            queue_depth: metric_u64(&metrics, "vs_bus_depth"),
            queue_capacity: metric_u64(&metrics, "vs_bus_capacity"),
            active_connections: metric_u64(&metrics, "vs_realtime_connections_active")
                .or_else(|| metric_u64(&metrics, "vs_ws_active_connections")),
            active_subscriptions: metric_u64(&metrics, "vs_realtime_operations_active"),
            realtime_broker_observations_total: broker_observations_total,
            realtime_broker_observations_per_second,
            realtime_dropped_total: metric_u64(&metrics, "vs_realtime_events_dropped_total"),
            realtime_terminal_failures_total: metric_u64(
                &metrics,
                "vs_realtime_terminal_failures_total",
            ),
            schema_drift_total: metric_u64(&metrics, "vs_schema_drift_total"),
            schema_casualties_total: metric_u64(&metrics, "vs_schema_casualties_total"),
            truncate_events_total: metric_u64(&metrics, "vs_truncate_events_total"),
            schema_drift_detail: drift_detail(&metrics),
            last_input_at_unix_milliseconds: metric_u64(&metrics, "vs_last_input_at_unixtime_ms"),
            last_output_at_unix_milliseconds: metric_u64(&metrics, "vs_last_output_at_unixtime_ms"),
            engine_ready: Some(!pipeline_error),
            error_code: if pipeline_error {
                "engine_pipeline_error".to_owned()
            } else {
                String::new()
            },
            safe_error_message: if pipeline_error {
                "Supervised engine reported a pipeline error".to_owned()
            } else {
                String::new()
            },
        }
    }

    async fn read_metrics(&self) -> Result<String, ()> {
        let mut response = self
            .client
            .get(self.metrics_url.clone())
            .send()
            .await
            .map_err(|_| ())?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > MAX_METRICS_BYTES as u64)
        {
            return Err(());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
            if body.len().saturating_add(chunk.len()) > MAX_METRICS_BYTES {
                return Err(());
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).map_err(|_| ())
    }
}

fn event_rate(samples: &VecDeque<(u64, Instant)>) -> f64 {
    let (Some((first_total, first_at)), Some((last_total, last_at))) =
        (samples.front(), samples.back())
    else {
        return 0.0;
    };
    let elapsed = last_at.saturating_duration_since(*first_at).as_secs_f64();
    if last_total < first_total || elapsed <= 0.0 {
        return 0.0;
    }
    (*last_total - *first_total) as f64 / elapsed
}

fn observe_rate(
    samples: &mut VecDeque<(u64, Instant)>,
    total: Option<u64>,
    now: Instant,
) -> Option<f64> {
    let total = total?;
    if samples
        .back()
        .is_some_and(|(previous, _)| total < *previous)
    {
        samples.clear();
    }
    samples.push_back((total, now));
    while samples.len() > 1
        && samples.front().is_some_and(|(_, sampled_at)| {
            now.saturating_duration_since(*sampled_at) > EVENT_RATE_WINDOW
        })
    {
        samples.pop_front();
    }
    while samples.len() > MAX_EVENT_RATE_SAMPLES {
        samples.pop_front();
    }
    Some(event_rate(samples))
}

/// Bounded per-table drift summary from the labeled
/// `vs_schema_drift_total{table=…,kind=…}` samples, e.g.
/// `shop.orders added=1 type_change=2`. Empty when no drift observed.
fn drift_detail(exposition: &str) -> String {
    let mut per_table: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for line in exposition.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("vs_schema_drift_total{") else {
            continue;
        };
        let Some((labels, value)) = rest.split_once('}') else {
            continue;
        };
        let value = value.trim();
        let mut table = None;
        let mut kind = None;
        for pair in labels.split(',') {
            match pair.split_once('=') {
                Some(("table", v)) => table = Some(v.trim_matches('"')),
                Some(("kind", v)) => kind = Some(v.trim_matches('"')),
                _ => {}
            }
        }
        if let (Some(table), Some(kind)) = (table, kind) {
            per_table
                .entry(table.to_owned())
                .or_default()
                .push(format!("{kind}={value}"));
        }
    }
    let mut out = String::new();
    for (table, kinds) in per_table {
        let entry = format!("{table} {}", kinds.join(" "));
        if out.is_empty() {
            out = entry;
        } else if out.len() + entry.len() + 2 <= 240 {
            out.push_str("; ");
            out.push_str(&entry);
        } else {
            out.push_str("; …");
            break;
        }
    }
    out.truncate(256);
    out
}

fn metric_u64(exposition: &str, name: &str) -> Option<u64> {
    let values = exposition.lines().filter_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let mut fields = line.split_ascii_whitespace();
        let metric = fields.next()?.split('{').next()?;
        (metric == name).then(|| fields.next()).flatten()
    });
    let mut found = false;
    let mut total = 0u64;
    for value in values {
        let value = value.parse::<f64>().ok()?;
        if !value.is_finite() || value < 0.0 || value > u64::MAX as f64 {
            return None;
        }
        found = true;
        total = total.saturating_add(value as u64);
    }
    found.then_some(total)
}

fn realtime_input_total(exposition: &str) -> Option<u64> {
    metric_u64(exposition, "vs_realtime_broker_ingress_total")
        .or_else(|| metric_u64(exposition, "vs_realtime_broker_messages_total"))
}

fn configuration_kinds(path: &Path) -> (String, String) {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.len() <= MAX_CONFIGURATION_BYTES =>
        {
            metadata
        }
        _ => return (String::new(), String::new()),
    };
    if metadata.len() > MAX_CONFIGURATION_BYTES {
        return (String::new(), String::new());
    }
    let envelope = match std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
    {
        Some(envelope) => envelope,
        None => return (String::new(), String::new()),
    };
    let Some(yaml) = envelope
        .pointer("/document/engine_config_yaml")
        .and_then(serde_json::Value::as_str)
    else {
        return (String::new(), String::new());
    };
    let config = match serde_yaml::from_str::<serde_yaml::Value>(yaml) {
        Ok(config) => config,
        Err(_) => return (String::new(), String::new()),
    };
    let source = yaml_kind(&config, "source");
    let sink = yaml_kind(&config, "sink");
    let roles = config.get("roles").and_then(serde_yaml::Value::as_sequence);
    let has_role = |role: &str| {
        roles.is_some_and(|roles| {
            roles
                .iter()
                .any(|value| value.as_str().is_some_and(|value| value == role))
        })
    };
    if has_role("mcp") {
        return (sink, "mcp".to_owned());
    }
    if !source.is_empty() || !sink.is_empty() {
        return (source, sink);
    }

    let provider = yaml_path_string(&config, &["runtime", "realtime", "provider"])
        .or_else(|| yaml_path_string(&config, &["runtime", "graphql", "provider"]))
        .or_else(|| yaml_path_string(&config, &["runtime", "ws", "provider"]))
        .unwrap_or_default();
    if has_role("graphql") {
        return (provider, "graphql".to_owned());
    }
    if has_role("ws") {
        return (provider, "websocket".to_owned());
    }
    (String::new(), String::new())
}

fn yaml_kind(config: &serde_yaml::Value, section: &str) -> String {
    config
        .get(section)
        .and_then(|value| value.get("kind"))
        .and_then(serde_yaml::Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .unwrap_or_default()
        .to_owned()
}

fn yaml_path_string(config: &serde_yaml::Value, path: &[&str]) -> Option<String> {
    let mut value = config;
    for segment in path {
        value = value.get(*segment)?;
    }
    value
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    use super::{configuration_kinds, event_rate, metric_u64, observe_rate, realtime_input_total};

    #[test]
    fn parses_only_the_requested_prometheus_sample() {
        let metrics = "# TYPE vs_events_emitted_total counter\n\
                       vs_events_emitted_total 202\n\
                       vs_bulk_write_p95_ms 9\n";
        assert_eq!(metric_u64(metrics, "vs_events_emitted_total"), Some(202));
        assert_eq!(metric_u64(metrics, "vs_bulk_write_p95_ms"), Some(9));
        assert_eq!(metric_u64(metrics, "vs_missing"), None);
    }

    #[test]
    fn accepts_labeled_samples_and_rejects_invalid_values() {
        assert_eq!(metric_u64("metric{kind=\"cdc\"} 12\n", "metric"), Some(12));
        assert_eq!(metric_u64("metric NaN\n", "metric"), None);
        assert_eq!(metric_u64("metric -1\n", "metric"), None);
    }

    #[test]
    fn sums_labeled_metric_series() {
        let metrics = "metric{transport=\"graphql\"} 3\nmetric{transport=\"raw\"} 4\n";
        assert_eq!(metric_u64(metrics, "metric"), Some(7));
    }

    #[test]
    fn realtime_input_prefers_unique_broker_ingress() {
        let metrics = "vs_realtime_broker_ingress_total{provider=\"redis_streams\"} 12\n\
                       vs_realtime_broker_messages_total{provider=\"redis_streams\"} 300\n";
        assert_eq!(realtime_input_total(metrics), Some(12));
    }

    #[test]
    fn realtime_input_falls_back_for_older_engines() {
        let metrics = "vs_realtime_broker_messages_total{provider=\"nats\"} 300\n";
        assert_eq!(realtime_input_total(metrics), Some(300));
    }

    #[test]
    fn rolling_rate_keeps_short_bursts_visible_to_heartbeats() {
        let start = Instant::now();
        let samples = VecDeque::from([
            (200, start),
            (220, start + Duration::from_secs(5)),
            (220, start + Duration::from_secs(20)),
        ]);
        assert!((event_rate(&samples) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rolling_rate_resets_after_process_counter_restart() {
        let start = Instant::now();
        let mut samples = VecDeque::from([(200, start), (220, start + Duration::from_secs(5))]);
        assert_eq!(
            observe_rate(&mut samples, Some(2), start + Duration::from_secs(10)),
            Some(0.0)
        );
        assert_eq!(samples.len(), 1);
    }

    #[test]
    fn reads_only_connector_kinds_from_the_applied_envelope() -> Result<(), String> {
        let directory = std::env::temp_dir().join(format!(
            "ventstream-telemetry-config-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&directory).map_err(|error| error.to_string())?;
        let path = directory.join("applied-envelope.json");
        let envelope = serde_json::json!({
            "document": {
                "engine_config_yaml": "source:\n  kind: postgres\nsink:\n  kind: opensearch\n"
            }
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&envelope).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        assert_eq!(
            configuration_kinds(&path),
            ("postgres".to_owned(), "opensearch".to_owned())
        );
        std::fs::remove_dir_all(directory).map_err(|error| error.to_string())?;
        Ok(())
    }

    #[test]
    fn derives_realtime_connector_kinds_from_runtime_configuration() -> Result<(), String> {
        let directory = std::env::temp_dir().join(format!(
            "ventstream-telemetry-realtime-config-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&directory).map_err(|error| error.to_string())?;
        let path = directory.join("applied-envelope.json");
        let envelope = serde_json::json!({
            "document": {
                "engine_config_yaml": "roles: [graphql]\nruntime:\n  realtime:\n    provider: redis_streams\n"
            }
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&envelope).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        assert_eq!(
            configuration_kinds(&path),
            ("redis_streams".to_owned(), "graphql".to_owned())
        );
        std::fs::remove_dir_all(directory).map_err(|error| error.to_string())?;
        Ok(())
    }

    #[test]
    fn derives_mcp_role_kind_from_configuration() -> Result<(), String> {
        let directory = std::env::temp_dir().join(format!(
            "ventstream-telemetry-mcp-config-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&directory).map_err(|error| error.to_string())?;
        let path = directory.join("applied-envelope.json");
        let envelope = serde_json::json!({
            "document": {
                "engine_config_yaml": "roles: [mcp]\nsink:\n  kind: meilisearch\nruntime:\n  mcp:\n    listen: \"0.0.0.0:4044\"\n"
            }
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&envelope).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        assert_eq!(
            configuration_kinds(&path),
            ("meilisearch".to_owned(), "mcp".to_owned())
        );
        std::fs::remove_dir_all(directory).map_err(|error| error.to_string())?;
        Ok(())
    }
}
