//! Real Redis Streams contract test used by local and release verification.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use redis::aio::ConnectionManager;
use ventstream_protocol::SubjectPattern;
use ventstream_realtime::{BrokerError, Cursor, GatewayRole, RealtimeBroker, SessionRequest};
use ventstream_redis::{RedisStreamId, RedisStreamsBroker, RedisStreamsConfig};

const SUBJECT: &str = "vs.t.integration.orders.updated.order-1";

fn request(connection_id: &str, resume_after: Option<Cursor>) -> SessionRequest {
    SessionRequest {
        role: GatewayRole::WebSocket,
        connection_id: Arc::from(connection_id),
        tenant: Arc::from("integration"),
        subject_filter: None,
        resume_after,
    }
}

fn filtered_request(
    connection_id: &str,
    resume_after: Option<Cursor>,
    subject: &str,
) -> SessionRequest {
    SessionRequest {
        subject_filter: Some(SubjectPattern::parse(subject).expect("valid subject filter")),
        ..request(connection_id, resume_after)
    }
}

async fn publish(
    connection: &mut ConnectionManager,
    stream: &str,
    event_id: &str,
) -> Result<RedisStreamId, Box<dyn std::error::Error>> {
    let payload = format!(r#"{{"id":"{event_id}","tenant":"integration"}}"#);
    let id: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("subject")
        .arg(SUBJECT)
        .arg("event")
        .arg(payload)
        .query_async(connection)
        .await?;
    Ok(id.parse()?)
}

async fn publish_subject(
    connection: &mut ConnectionManager,
    stream: &str,
    subject: &str,
    event_id: &str,
) -> Result<RedisStreamId, Box<dyn std::error::Error>> {
    let payload = format!(r#"{{"id":"{event_id}","tenant":"integration"}}"#);
    let id: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("subject")
        .arg(subject)
        .arg("event")
        .arg(payload)
        .query_async(connection)
        .await?;
    Ok(id.parse()?)
}

async fn next_event(
    session: &mut Box<dyn ventstream_realtime::EventSession>,
) -> ventstream_realtime::BrokerEvent {
    tokio::time::timeout(Duration::from_secs(5), session.next())
        .await
        .expect("event delivery timed out")
        .expect("broker session failed")
        .expect("broker session closed")
}

#[tokio::test]
#[ignore = "requires VS_TEST_REDIS_URL pointing at an isolated Redis server"]
async fn live_replay_and_cursor_failures_use_real_redis() {
    let url = std::env::var("VS_TEST_REDIS_URL").expect("VS_TEST_REDIS_URL must be set");
    let stream = "ventstream:{integration}:events";
    let client = redis::Client::open(url.as_str()).expect("valid Redis URL");
    let mut publisher = client
        .get_connection_manager()
        .await
        .expect("connect publisher");
    let _: usize = redis::cmd("DEL")
        .arg(stream)
        .query_async(&mut publisher)
        .await
        .expect("clear test stream");

    let broker = RedisStreamsBroker::connect(RedisStreamsConfig {
        url,
        block_timeout: Duration::from_millis(100),
        response_timeout: Duration::from_secs(2),
        ..RedisStreamsConfig::default()
    })
    .await
    .expect("connect Redis Streams broker");

    let mut live = broker
        .open_session(request("live", None))
        .await
        .expect("open live session");
    let first_id = publish(&mut publisher, stream, "event-1")
        .await
        .expect("publish first event");
    let first = next_event(&mut live).await;
    assert_eq!(first.subject.as_ref(), SUBJECT);
    let first_wire = first_id.to_cursor().to_wire();
    assert_eq!(
        first.cursor.as_ref().map(Cursor::to_wire).as_deref(),
        Some(first_wire.as_str())
    );
    live.accepted(&first).await.expect("accept live event");
    let first_cursor = first.cursor.clone().expect("durable event cursor");
    drop(live);

    let _: String = redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("subject")
        .arg(SUBJECT)
        .query_async(&mut publisher)
        .await
        .expect("publish malformed entry between replay events");
    let second_id = publish(&mut publisher, stream, "event-2")
        .await
        .expect("publish second event");
    let third_id = publish(&mut publisher, stream, "event-3")
        .await
        .expect("publish third event");
    let mut replay = broker
        .open_session(request("replay", Some(first_cursor.clone())))
        .await
        .expect("open replay session");
    let second = next_event(&mut replay).await;
    let third = next_event(&mut replay).await;
    let second_wire = second_id.to_cursor().to_wire();
    let third_wire = third_id.to_cursor().to_wire();
    assert_eq!(
        second.cursor.as_ref().map(Cursor::to_wire).as_deref(),
        Some(second_wire.as_str())
    );
    assert_eq!(
        third.cursor.as_ref().map(Cursor::to_wire).as_deref(),
        Some(third_wire.as_str())
    );

    let wrong_provider = broker
        .open_session(request("wrong-provider", Some(Cursor::jetstream(1))))
        .await;
    assert!(matches!(
        wrong_provider,
        Err(BrokerError::CursorProviderMismatch { .. })
    ));

    let ahead = Cursor::redis_streams(u64::MAX, 0);
    let ahead_result = broker.open_session(request("ahead", Some(ahead))).await;
    assert!(matches!(ahead_result, Err(BrokerError::CursorAhead { .. })));

    let _: usize = redis::cmd("XTRIM")
        .arg(stream)
        .arg("MINID")
        .arg(second_id.to_string())
        .query_async(&mut publisher)
        .await
        .expect("trim first event");
    let expired = broker
        .open_session(request("expired", Some(first_cursor)))
        .await;
    assert!(matches!(expired, Err(BrokerError::ResumeExpired { .. })));

    let filter_start = third.cursor.clone().expect("third replay cursor");
    publish_subject(
        &mut publisher,
        stream,
        "vs.t.integration.audits.updated.audit-1",
        "filtered-out",
    )
    .await
    .expect("publish non-matching event");
    let matching_id = publish(&mut publisher, stream, "filtered-match")
        .await
        .expect("publish matching event");
    let mut filtered = broker
        .open_session(filtered_request(
            "filtered-replay",
            Some(filter_start),
            "vs.t.integration.orders.updated.*",
        ))
        .await
        .expect("open filtered replay session");
    let matching = next_event(&mut filtered).await;
    assert_eq!(matching.subject.as_ref(), SUBJECT);
    assert_eq!(
        matching.cursor.as_ref().map(Cursor::to_wire),
        Some(matching_id.to_cursor().to_wire())
    );

    let _: usize = redis::cmd("DEL")
        .arg(stream)
        .query_async(&mut publisher)
        .await
        .expect("remove test stream");

    let retention_stream = "retention-test:{integration}:events";
    let _: usize = redis::cmd("DEL")
        .arg(retention_stream)
        .query_async(&mut publisher)
        .await
        .expect("clear retention stream");
    let retention_broker = RedisStreamsBroker::connect(RedisStreamsConfig {
        url: std::env::var("VS_TEST_REDIS_URL").expect("test Redis URL"),
        key_prefix: "retention-test".to_owned(),
        max_length: Some(3),
        block_timeout: Duration::from_millis(50),
        ..RedisStreamsConfig::default()
    })
    .await
    .expect("connect retention broker");
    let mut retention_session = retention_broker
        .open_session(request("retention", None))
        .await
        .expect("open retention session");
    for index in 0..6 {
        publish(
            &mut publisher,
            retention_stream,
            &format!("retention-{index}"),
        )
        .await
        .expect("publish retention event");
        let _ = next_event(&mut retention_session).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let retained: usize = redis::cmd("XLEN")
        .arg(retention_stream)
        .query_async(&mut publisher)
        .await
        .expect("read retained stream length");
    assert!(retained <= 3, "retained {retained} entries");
    let _: usize = redis::cmd("DEL")
        .arg(retention_stream)
        .query_async(&mut publisher)
        .await
        .expect("remove retention stream");
}

#[tokio::test]
#[ignore = "requires VS_TEST_REDIS_URL pointing at an isolated Redis server"]
async fn shared_tailer_fans_out_ordered_bursts_without_loss() {
    const SESSION_COUNT: usize = 32;
    const EVENT_COUNT: usize = 1_000;

    let url = std::env::var("VS_TEST_REDIS_URL").expect("VS_TEST_REDIS_URL must be set");
    let stream = "fanout-test:{integration}:events";
    let client = redis::Client::open(url.as_str()).expect("valid Redis URL");
    let mut publisher = client
        .get_connection_manager()
        .await
        .expect("connect publisher");
    let _: usize = redis::cmd("DEL")
        .arg(stream)
        .query_async(&mut publisher)
        .await
        .expect("clear fan-out stream");

    let broker = RedisStreamsBroker::connect(RedisStreamsConfig {
        url,
        key_prefix: "fanout-test".to_owned(),
        block_timeout: Duration::from_millis(50),
        broadcast_capacity: EVENT_COUNT * 2,
        max_length: None,
        ..RedisStreamsConfig::default()
    })
    .await
    .expect("connect fan-out broker");
    let mut sessions = Vec::with_capacity(SESSION_COUNT);
    for index in 0..SESSION_COUNT {
        sessions.push(
            broker
                .open_session(request(&format!("fanout-{index}"), None))
                .await
                .expect("open fan-out session"),
        );
    }

    let mut pipeline = redis::pipe();
    for index in 0..EVENT_COUNT {
        pipeline
            .cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("subject")
            .arg(SUBJECT)
            .arg("event")
            .arg(format!(r#"{{"id":"burst-{index}"}}"#));
    }
    let published: Vec<String> = pipeline
        .query_async(&mut publisher)
        .await
        .expect("publish fan-out burst");
    let expected_last: RedisStreamId = published
        .last()
        .expect("published IDs")
        .parse()
        .expect("valid final Redis ID");

    for session in &mut sessions {
        let mut prior = None;
        for _ in 0..EVENT_COUNT {
            let event = next_event(session).await;
            let current: RedisStreamId = event
                .cursor
                .as_ref()
                .expect("fan-out cursor")
                .value()
                .parse()
                .expect("valid fan-out Redis ID");
            assert!(prior.is_none_or(|previous| current > previous));
            prior = Some(current);
            session
                .accepted(&event)
                .await
                .expect("accept fan-out event");
        }
        assert_eq!(prior, Some(expected_last));
    }

    let _: usize = redis::cmd("DEL")
        .arg(stream)
        .query_async(&mut publisher)
        .await
        .expect("remove fan-out stream");
}
