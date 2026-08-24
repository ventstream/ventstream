//! Fresh-process Redis metrics contract.
#![allow(clippy::expect_used)]

use chrono::Utc;
use ventstream_core::{ContentType, Event, Headers, Payload, Sink, SinkBatch, SourceUri, Subject};
use ventstream_sinks::{RedisConfig, RedisKeyRouting, RedisSink};

fn sample_line<'a>(rendered: &'a str, name: &str) -> Option<&'a str> {
    rendered
        .lines()
        .find(|line| line.starts_with(name) && line.as_bytes().get(name.len()) == Some(&b' '))
}

#[ignore = "needs VS_TEST_REDIS_SINK_URL; run with --ignored"]
#[tokio::test(flavor = "current_thread")]
async fn live_sink_metrics_survive_recorder_installation_after_sink_startup() {
    let url = std::env::var("VS_TEST_REDIS_SINK_URL")
        .expect("VS_TEST_REDIS_SINK_URL must be set; this test is #[ignore]d by default");
    let prefix = format!(
        "ventstream:test:metrics:{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(RedisConfig::new(
        "redis-metrics-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    ))
    .await
    .expect("connect Redis sink before installing the recorder");

    let counters = ventstream_telemetry::TelemetryCounters::new();
    ventstream_telemetry::set_global_counters(counters);
    let handle = ventstream_telemetry::install_prometheus().expect("install Prometheus recorder");

    let headers = Headers::empty()
        .with_header(
            "ventstream.cdc.relation".to_owned(),
            "public.orders".to_owned(),
        )
        .with_header(
            "ventstream.doc.id".to_owned(),
            r#"public.orders:["metrics-1"]"#.to_owned(),
        );
    let event = Event::builder(
        SourceUri::new("postgres://metrics-contract").expect("source URI"),
        Subject::new("postgres.public.orders.insert").expect("subject"),
    )
    .content_type(ContentType::Json)
    .headers(headers)
    .payload(Payload::from_vec(
        br#"{"id":"metrics-1","status":"pending"}"#.to_vec(),
    ))
    .build();

    ventstream_telemetry::bump_events_received(1);
    sink.write(SinkBatch::new(vec![event]))
        .await
        .expect("acknowledged Redis write");
    ventstream_telemetry::bump_events_delivered(1);

    let rendered = ventstream_telemetry::render_prometheus(&handle);
    for expected in [
        "vs_events_received_total 1",
        "vs_events_delivered_total 1",
        "vs_events_failed_total 0",
        "vs_dlq_writes_total 0",
        "vs_redis_upserts_total 1",
        "vs_redis_deletes_total 0",
        "vs_redis_writer_leased_targets 1",
    ] {
        assert!(
            rendered.lines().any(|line| line == expected),
            "missing {expected}:\n{rendered}"
        );
    }
    assert!(
        sample_line(&rendered, "vs_redis_pipeline_bytes_total")
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|value| value > 0),
        "Redis pipeline byte metric was not positive:\n{rendered}"
    );
    assert!(
        rendered
            .lines()
            .any(|line| line == "vs_redis_writer_lease_acquisitions_total{result=\"acquired\"} 1"),
        "writer acquisition metric missing:\n{rendered}"
    );
}
