//! GraphQL operation-handoff regression test backed by NATS JetStream.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic
)]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use async_nats::jetstream::stream::{Config as StreamConfig, StorageType};
use futures_util::{SinkExt as _, StreamExt as _, TryStreamExt as _};
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::Message;
use ventstream_core::ShutdownToken;
use ventstream_graphql::GraphQlConfig;
use ventstream_protocol::{Event, Metadata};

const TENANT: &str = "resubscribe";
const EVENT_COUNT: usize = 300;
const TRIALS: usize = 20;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn available_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve test address");
    listener.local_addr().expect("read test address")
}

async fn connect(address: SocketAddr) -> Socket {
    let url = format!("ws://{address}/graphql/ws");
    for _ in 0..100 {
        let mut request = url
            .clone()
            .into_client_request()
            .expect("WebSocket request");
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            "graphql-transport-ws".parse().expect("protocol header"),
        );
        match tokio_tungstenite::connect_async(request).await {
            Ok((socket, _)) => return socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("GraphQL gateway did not start at {url}");
}

async fn text_frame(socket: &mut Socket) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(30), socket.next())
            .await
            .expect("GraphQL frame timed out")
            .expect("GraphQL socket closed")
            .expect("GraphQL frame failed");
        match frame {
            Message::Text(text) => return serde_json::from_str(&text).expect("valid JSON frame"),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            Message::Close(_) => panic!("GraphQL socket closed"),
            _ => {}
        }
    }
}

async fn initialize(socket: &mut Socket, resume_from_cursor: Option<&str>) {
    let mut payload = serde_json::json!({
        "authToken": "integration-token",
        "tenant": TENANT
    });
    if let Some(cursor) = resume_from_cursor {
        payload["resume_from_cursor"] = serde_json::Value::String(cursor.to_owned());
    }
    socket
        .send(Message::Text(
            serde_json::json!({
                "type": "connection_init",
                "payload": payload
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send connection_init");
    assert_eq!(text_frame(socket).await["type"], "connection_ack");
}

async fn subscribe(socket: &mut Socket, id: &str) {
    subscribe_pattern(socket, id, "orders.updated.*").await;
}

async fn subscribe_pattern(socket: &mut Socket, id: &str, pattern: &str) {
    subscribe_pattern_from(socket, id, pattern, None).await;
}

async fn subscribe_pattern_from(
    socket: &mut Socket,
    id: &str,
    pattern: &str,
    resume_from_cursor: Option<&str>,
) {
    let query = format!(
        "subscription Operation($resumeFromCursor: String) {{ events(subject: \"{pattern}\", resumeFromCursor: $resumeFromCursor) {{ entityId cursor }} }}"
    );
    socket
        .send(Message::Text(
            serde_json::json!({
                "id": id,
                "type": "subscribe",
                "payload": {
                    "query": query,
                    "variables": {
                        "resumeFromCursor": resume_from_cursor
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send subscribe");
}

async fn complete(socket: &mut Socket, id: &str) {
    socket
        .send(Message::Text(
            serde_json::json!({"id": id, "type": "complete"})
                .to_string()
                .into(),
        ))
        .await
        .expect("send complete");
}

#[tokio::test]
#[ignore = "requires VS_TEST_NATS_URL pointing at an isolated JetStream server"]
async fn rapid_operation_replacement_does_not_drop_jetstream_events() {
    let nats_url = std::env::var("VS_TEST_NATS_URL").expect("VS_TEST_NATS_URL must be set");
    let client = async_nats::connect(&nats_url).await.expect("connect NATS");
    let js = async_nats::jetstream::new(client);
    let mut existing_streams = js.stream_names();
    let mut stale_streams = Vec::new();
    while let Some(name) = existing_streams
        .try_next()
        .await
        .expect("list existing streams")
    {
        if name.starts_with("VS_REPRO_") {
            stale_streams.push(name);
        }
    }
    for name in stale_streams {
        js.delete_stream(name)
            .await
            .expect("delete stale repro stream");
    }
    let suffix = ulid::Ulid::new().to_string();
    let stream_name = format!("VS_REPRO_{suffix}");
    let subject_pattern = format!("vs.t.{TENANT}.>");
    js.create_stream(StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject_pattern],
        storage: StorageType::Memory,
        ..Default::default()
    })
    .await
    .expect("create stream");

    let address = available_address();
    let shutdown = ShutdownToken::new();
    let server_shutdown = shutdown.clone();
    let server_url = nats_url.clone();
    let server_stream = stream_name.clone();
    let server = tokio::spawn(async move {
        ventstream_graphql::run(
            GraphQlConfig {
                listen: address,
                expected_tenant: Some(TENANT.to_owned()),
                nats_url: server_url,
                stream_name: server_stream,
                consumer_inactive_threshold: Duration::from_secs(30),
                // Sweep aggressively so consumer creation overlaps reaper
                // listings and exercises the live-registration race.
                reaper_interval: Duration::from_millis(10),
                ..GraphQlConfig::default()
            },
            server_shutdown,
        )
        .await
    });

    // Control: the same server, stream and client without operation churn.
    let mut stable_socket = connect(address).await;
    initialize(&mut stable_socket, None).await;
    subscribe(&mut stable_socket, "stable").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for sequence in 1..=EVENT_COUNT {
        let entity_id = format!("stable-event-{sequence}");
        let event = Event::publish(
            TENANT,
            "orders.updated",
            &entity_id,
            None,
            chrono::Utc::now(),
            serde_json::json!({"sequence": sequence}),
            Metadata::default(),
        )
        .expect("valid stable event");
        js.publish(
            event.subject().expect("stable subject").to_string(),
            serde_json::to_vec(&event)
                .expect("serialize stable event")
                .into(),
        )
        .await
        .expect("start stable JetStream publish")
        .await
        .expect("stable JetStream publish acknowledgement");
    }
    let mut stable_received = HashSet::with_capacity(EVENT_COUNT);
    let stable_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while stable_received.len() < EVENT_COUNT && tokio::time::Instant::now() < stable_deadline {
        let remaining = stable_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(frame) = tokio::time::timeout(remaining, text_frame(&mut stable_socket)).await
        else {
            break;
        };
        if let Some(entity_id) = frame["payload"]["data"]["events"]["entityId"].as_str() {
            if entity_id.starts_with("stable-event-") {
                stable_received.insert(entity_id.to_owned());
            }
        }
    }
    eprintln!(
        "VentStream stable control: received={}/{}",
        stable_received.len(),
        EVENT_COUNT
    );
    assert_eq!(
        stable_received.len(),
        EVENT_COUNT,
        "stable GraphQL subscription lost events"
    );
    stable_socket
        .close(None)
        .await
        .expect("close stable socket");

    let mut failed_trials = Vec::new();
    let mut first_failure_consumer = None;
    for trial in 0..TRIALS {
        let mut socket = connect(address).await;
        initialize(&mut socket, None).await;
        let mut operation = format!("op-{trial}-0");
        subscribe(&mut socket, &operation).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut received = HashSet::with_capacity(EVENT_COUNT);
        for sequence in 1..=EVENT_COUNT {
            let entity_id = format!("trial-{trial}-event-{sequence}");
            let event = Event::publish(
                TENANT,
                "orders.updated",
                &entity_id,
                None,
                chrono::Utc::now(),
                serde_json::json!({"trial": trial, "sequence": sequence}),
                Metadata::default(),
            )
            .expect("valid event");
            let subject = event.subject().expect("event subject").to_string();
            let payload = serde_json::to_vec(&event).expect("serialize event");
            let publish = js.publish(subject, payload.into());

            complete(&mut socket, &operation).await;
            operation = format!("op-{trial}-{sequence}");
            subscribe(&mut socket, &operation).await;
            publish
                .await
                .expect("start JetStream publish")
                .await
                .expect("JetStream publish acknowledgement");
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while received.len() < EVENT_COUNT && tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Ok(frame) = tokio::time::timeout(remaining, text_frame(&mut socket)).await else {
                break;
            };
            if frame["type"] != "next" {
                continue;
            }
            if let Some(entity_id) = frame["payload"]["data"]["events"]["entityId"].as_str() {
                if entity_id.starts_with(&format!("trial-{trial}-")) {
                    received.insert(entity_id.to_owned());
                }
            }
        }

        eprintln!(
            "VentStream rapid-resubscribe trial {trial}: received={}/{}",
            received.len(),
            EVENT_COUNT
        );
        if received.len() != EVENT_COUNT {
            if first_failure_consumer.is_none() {
                // The GraphQL role flushes its cumulative JetStream ack after
                // ten quiet seconds. Capture the resulting durable state while
                // this socket and its disposable consumer still exist.
                tokio::time::sleep(Duration::from_secs(11)).await;
                let stream = js.get_stream(&stream_name).await.expect("get stream");
                let mut names = stream.consumer_names();
                if let Some(name) = names.try_next().await.expect("list GraphQL consumers") {
                    let mut consumer: async_nats::jetstream::consumer::PullConsumer = stream
                        .get_consumer(&name)
                        .await
                        .expect("get GraphQL consumer");
                    let info = consumer.info().await.expect("GraphQL consumer info");
                    first_failure_consumer = Some((
                        info.ack_floor.stream_sequence,
                        info.num_ack_pending,
                        info.num_pending,
                    ));
                }
            }
            failed_trials.push((trial, received.len()));
        }
        socket.close(None).await.expect("close socket");
    }

    // One client, two active operations, and one connection-wide checkpoint.
    let mut multi_socket = connect(address).await;
    initialize(&mut multi_socket, None).await;
    subscribe_pattern(&mut multi_socket, "multi-orders", "orders.updated.*").await;
    subscribe_pattern(&mut multi_socket, "multi-audits", "audits.updated.*").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    const MULTI_EVENTS_PER_OPERATION: usize = 100;
    for sequence in 1..=MULTI_EVENTS_PER_OPERATION {
        for (event_type, entity_id) in [
            ("orders.updated", format!("multi-order-{sequence}")),
            ("audits.updated", format!("multi-audit-{sequence}")),
        ] {
            let event = Event::publish(
                TENANT,
                event_type,
                &entity_id,
                None,
                chrono::Utc::now(),
                serde_json::json!({"sequence": sequence}),
                Metadata::default(),
            )
            .expect("valid multi-operation event");
            js.publish(
                event
                    .subject()
                    .expect("multi-operation subject")
                    .to_string(),
                serde_json::to_vec(&event)
                    .expect("serialize multi-operation event")
                    .into(),
            )
            .await
            .expect("start multi-operation publish")
            .await
            .expect("multi-operation publish acknowledgement");
        }
    }

    let expected_multi = MULTI_EVENTS_PER_OPERATION * 2;
    let mut multi_received = BTreeMap::<u64, (String, String)>::new();
    let multi_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while multi_received.len() < expected_multi && tokio::time::Instant::now() < multi_deadline {
        let remaining = multi_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(frame) = tokio::time::timeout(remaining, text_frame(&mut multi_socket)).await else {
            break;
        };
        if frame["type"] != "next" {
            continue;
        }
        let Some(operation_id) = frame["id"].as_str() else {
            continue;
        };
        let event = &frame["payload"]["data"]["events"];
        let Some(entity_id) = event["entityId"].as_str() else {
            continue;
        };
        let cursor = event["cursor"]
            .as_str()
            .expect("JetStream cursor")
            .parse::<u64>()
            .expect("numeric JetStream cursor");
        if entity_id.starts_with("multi-order-") {
            assert_eq!(operation_id, "multi-orders");
        } else if entity_id.starts_with("multi-audit-") {
            assert_eq!(operation_id, "multi-audits");
        } else {
            continue;
        }
        multi_received.insert(cursor, (operation_id.to_owned(), entity_id.to_owned()));
    }
    eprintln!(
        "VentStream multi-operation live delivery: received={}/{}",
        multi_received.len(),
        expected_multi
    );

    // Simulate asynchronous handlers: orders finish first while audit events
    // remain in flight. The safe checkpoint may advance only through the
    // highest contiguous set of completed stream cursors.
    let first_multi_cursor = *multi_received.keys().next().expect("first multi cursor");
    let last_multi_cursor = *multi_received
        .keys()
        .next_back()
        .expect("last multi cursor");
    let mut completed = BTreeSet::new();
    let mut checkpoint = first_multi_cursor.saturating_sub(1);
    for (cursor, (_, entity_id)) in &multi_received {
        if entity_id.starts_with("multi-order-") {
            completed.insert(*cursor);
        }
    }
    while completed.remove(&checkpoint.saturating_add(1)) {
        checkpoint = checkpoint.saturating_add(1);
    }
    let orders_only_checkpoint = checkpoint;
    for (cursor, (_, entity_id)) in &multi_received {
        if entity_id.starts_with("multi-audit-") {
            completed.insert(*cursor);
        }
    }
    while completed.remove(&checkpoint.saturating_add(1)) {
        checkpoint = checkpoint.saturating_add(1);
    }
    eprintln!(
        "VentStream coordinated checkpoint: orders_only={orders_only_checkpoint} all_operations={checkpoint}"
    );
    multi_socket
        .close(None)
        .await
        .expect("close multi-operation socket");

    for (event_type, entity_id) in [
        ("orders.updated", "offline-order"),
        ("audits.updated", "offline-audit"),
    ] {
        let event = Event::publish(
            TENANT,
            event_type,
            entity_id,
            None,
            chrono::Utc::now(),
            serde_json::json!({"offline": true}),
            Metadata::default(),
        )
        .expect("valid offline event");
        js.publish(
            event.subject().expect("offline subject").to_string(),
            serde_json::to_vec(&event)
                .expect("serialize offline event")
                .into(),
        )
        .await
        .expect("start offline publish")
        .await
        .expect("offline publish acknowledgement");
    }

    let mut resumed_socket = connect(address).await;
    initialize(&mut resumed_socket, Some(&checkpoint.to_string())).await;
    subscribe_pattern(&mut resumed_socket, "resumed-orders", "orders.updated.*").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    subscribe_pattern(&mut resumed_socket, "resumed-audits", "audits.updated.*").await;
    let mut resumed_entities = HashSet::new();
    let resume_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while resumed_entities.len() < 2 && tokio::time::Instant::now() < resume_deadline {
        let remaining = resume_deadline.saturating_duration_since(tokio::time::Instant::now());
        let Ok(frame) = tokio::time::timeout(remaining, text_frame(&mut resumed_socket)).await
        else {
            break;
        };
        if let Some(entity_id) = frame["payload"]["data"]["events"]["entityId"].as_str() {
            if entity_id.starts_with("offline-") {
                resumed_entities.insert(entity_id.to_owned());
            }
        }
    }
    eprintln!("VentStream multi-operation resume: received={resumed_entities:?}");
    resumed_socket
        .close(None)
        .await
        .expect("close resumed socket");

    // Advance two independent operation cursors on one socket. Every event is
    // published while its operation is absent, so delivery must come from
    // that operation's subject-filtered replay rather than live timing.
    let mut dual_socket = connect(address).await;
    initialize(&mut dual_socket, None).await;
    subscribe_pattern(&mut dual_socket, "dual-orders-0", "orders.updated.*").await;
    subscribe_pattern(&mut dual_socket, "dual-audits-0", "audits.updated.*").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (event_type, entity_id) in [
        ("orders.updated", "dual-order-seed"),
        ("audits.updated", "dual-audit-seed"),
    ] {
        let event = Event::publish(
            TENANT,
            event_type,
            entity_id,
            None,
            chrono::Utc::now(),
            serde_json::json!({"seed": true}),
            Metadata::default(),
        )
        .expect("valid dual-operation seed");
        js.publish(
            event
                .subject()
                .expect("dual-operation seed subject")
                .to_string(),
            serde_json::to_vec(&event)
                .expect("serialize dual-operation seed")
                .into(),
        )
        .await
        .expect("start dual-operation seed publish")
        .await
        .expect("dual-operation seed publish acknowledgement");
    }
    let mut dual_cursors = BTreeMap::<&'static str, String>::new();
    while dual_cursors.len() < 2 {
        let frame = text_frame(&mut dual_socket).await;
        let event = &frame["payload"]["data"]["events"];
        match event["entityId"].as_str() {
            Some("dual-order-seed") => {
                assert_eq!(frame["id"], "dual-orders-0");
                dual_cursors.insert(
                    "orders",
                    event["cursor"]
                        .as_str()
                        .expect("dual order seed cursor")
                        .to_owned(),
                );
            }
            Some("dual-audit-seed") => {
                assert_eq!(frame["id"], "dual-audits-0");
                dual_cursors.insert(
                    "audits",
                    event["cursor"]
                        .as_str()
                        .expect("dual audit seed cursor")
                        .to_owned(),
                );
            }
            _ => {}
        }
    }

    const DUAL_CURSOR_ROUNDS: usize = 100;
    let mut dual_received = 0usize;
    for round in 1..=DUAL_CURSOR_ROUNDS {
        complete(&mut dual_socket, &format!("dual-orders-{}", round - 1)).await;
        complete(&mut dual_socket, &format!("dual-audits-{}", round - 1)).await;
        for (event_type, entity_id) in [
            ("orders.updated", format!("dual-order-{round}")),
            ("audits.updated", format!("dual-audit-{round}")),
        ] {
            let event = Event::publish(
                TENANT,
                event_type,
                &entity_id,
                None,
                chrono::Utc::now(),
                serde_json::json!({"round": round}),
                Metadata::default(),
            )
            .expect("valid dual-operation event");
            js.publish(
                event.subject().expect("dual-operation subject").to_string(),
                serde_json::to_vec(&event)
                    .expect("serialize dual-operation event")
                    .into(),
            )
            .await
            .expect("start dual-operation publish")
            .await
            .expect("dual-operation publish acknowledgement");
        }

        let order_operation = format!("dual-orders-{round}");
        let audit_operation = format!("dual-audits-{round}");
        subscribe_pattern_from(
            &mut dual_socket,
            &order_operation,
            "orders.updated.*",
            dual_cursors.get("orders").map(String::as_str),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        subscribe_pattern_from(
            &mut dual_socket,
            &audit_operation,
            "audits.updated.*",
            dual_cursors.get("audits").map(String::as_str),
        )
        .await;

        let mut round_received = HashSet::new();
        while round_received.len() < 2 {
            let frame = text_frame(&mut dual_socket).await;
            let event = &frame["payload"]["data"]["events"];
            let Some(entity_id) = event["entityId"].as_str() else {
                continue;
            };
            let (kind, expected_operation) = if entity_id == format!("dual-order-{round}") {
                ("orders", order_operation.as_str())
            } else if entity_id == format!("dual-audit-{round}") {
                ("audits", audit_operation.as_str())
            } else {
                continue;
            };
            assert_eq!(frame["id"], expected_operation);
            let next_cursor = event["cursor"]
                .as_str()
                .expect("dual-operation cursor")
                .to_owned();
            let previous = dual_cursors
                .get(kind)
                .expect("previous dual-operation cursor")
                .parse::<u64>()
                .expect("numeric previous cursor");
            let next = next_cursor.parse::<u64>().expect("numeric next cursor");
            assert!(next > previous, "{kind} cursor did not advance");
            dual_cursors.insert(kind, next_cursor);
            round_received.insert(kind);
            dual_received += 1;
        }
    }
    eprintln!(
        "VentStream independent operation cursors: received={}/{} final={dual_cursors:?}",
        dual_received,
        DUAL_CURSOR_ROUNDS * 2
    );
    dual_socket
        .close(None)
        .await
        .expect("close independent-cursor socket");

    // Keep an unrelated operation alive while repeatedly replacing the orders
    // operation. Publish each order while no order operation exists, then
    // resume the replacement from that operation's last processed cursor.
    let mut churn_socket = connect(address).await;
    initialize(&mut churn_socket, None).await;
    subscribe_pattern(&mut churn_socket, "persistent-audits", "audits.updated.*").await;
    let mut churn_operation = "churn-orders-0".to_owned();
    subscribe(&mut churn_socket, &churn_operation).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let seed = Event::publish(
        TENANT,
        "orders.updated",
        "churn-order-seed",
        None,
        chrono::Utc::now(),
        serde_json::json!({"seed": true}),
        Metadata::default(),
    )
    .expect("valid churn seed event");
    js.publish(
        seed.subject().expect("churn seed subject").to_string(),
        serde_json::to_vec(&seed)
            .expect("serialize churn seed event")
            .into(),
    )
    .await
    .expect("start churn seed publish")
    .await
    .expect("churn seed publish acknowledgement");
    let mut operation_cursor = loop {
        let frame = text_frame(&mut churn_socket).await;
        let event = &frame["payload"]["data"]["events"];
        if event["entityId"] == "churn-order-seed" {
            break event["cursor"]
                .as_str()
                .expect("churn seed cursor")
                .to_owned();
        }
    };

    const CHURN_EVENTS: usize = 300;
    let mut churn_received = HashSet::new();
    for sequence in 1..=CHURN_EVENTS {
        let entity_id = format!("churn-order-{sequence}");
        let event = Event::publish(
            TENANT,
            "orders.updated",
            &entity_id,
            None,
            chrono::Utc::now(),
            serde_json::json!({"sequence": sequence}),
            Metadata::default(),
        )
        .expect("valid churn event");
        complete(&mut churn_socket, &churn_operation).await;
        js.publish(
            event.subject().expect("churn subject").to_string(),
            serde_json::to_vec(&event)
                .expect("serialize churn event")
                .into(),
        )
        .await
        .expect("start churn publish")
        .await
        .expect("churn publish acknowledgement");
        churn_operation = format!("churn-orders-{sequence}");
        subscribe_pattern_from(
            &mut churn_socket,
            &churn_operation,
            "orders.updated.*",
            Some(&operation_cursor),
        )
        .await;

        loop {
            let frame = text_frame(&mut churn_socket).await;
            let event = &frame["payload"]["data"]["events"];
            if event["entityId"].as_str() == Some(&entity_id) {
                assert_eq!(frame["id"], churn_operation);
                operation_cursor = event["cursor"]
                    .as_str()
                    .expect("replacement operation cursor")
                    .to_owned();
                churn_received.insert(entity_id);
                break;
            }
        }
    }
    eprintln!(
        "VentStream cross-operation handoff: received={}/{}",
        churn_received.len(),
        CHURN_EVENTS
    );
    churn_socket.close(None).await.expect("close churn socket");

    let mut stream = js.get_stream(&stream_name).await.expect("get stream");
    let info = stream.info().await.expect("stream info");
    eprintln!(
        "VentStream summary: failed_trials={failed_trials:?} stream_messages={} first_failure_consumer={first_failure_consumer:?}",
        info.state.messages,
    );

    shutdown.cancel();
    server
        .await
        .expect("GraphQL server task")
        .expect("GraphQL server result");
    js.delete_stream(&stream_name).await.expect("delete stream");
    assert!(
        failed_trials.is_empty(),
        "events disappeared during same-socket GraphQL operation replacement: {failed_trials:?}"
    );
    assert_eq!(
        multi_received.len(),
        expected_multi,
        "interleaved multi-operation delivery lost events"
    );
    assert!(
        orders_only_checkpoint < last_multi_cursor,
        "checkpoint advanced past unfinished audit operation"
    );
    assert_eq!(
        checkpoint, last_multi_cursor,
        "coordinated checkpoint did not reach the last fully processed cursor"
    );
    assert_eq!(
        resumed_entities,
        HashSet::from(["offline-order".to_owned(), "offline-audit".to_owned()]),
        "multi-operation resume lost an offline event"
    );
    assert_eq!(
        dual_received,
        DUAL_CURSOR_ROUNDS * 2,
        "independent operation cursors lost events"
    );
    assert_eq!(
        churn_received.len(),
        CHURN_EVENTS,
        "one operation lost events while another operation remained active"
    );
}
