//! GraphQL-over-WebSocket contract test backed by a real Redis server.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic
)]

use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use redis::aio::ConnectionManager;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::Message;
use ventstream_core::ShutdownToken;
use ventstream_graphql::GraphQlConfig;
use ventstream_protocol::{Event, Metadata};
use ventstream_redis::RedisStreamsConfig;

const TENANT: &str = "integration";
const STREAM: &str = "ventstream:{integration}:events";

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn available_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve test address");
    listener.local_addr().expect("read test address")
}

async fn connect(address: SocketAddr) -> Socket {
    let url = format!("ws://{address}/graphql/ws");
    for _ in 0..50 {
        let mut request = url
            .clone()
            .into_client_request()
            .expect("WebSocket request");
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            "graphql-transport-ws".parse().expect("protocol header"),
        );
        match tokio_tungstenite::connect_async(request).await {
            Ok((socket, response)) => {
                assert_eq!(
                    response
                        .headers()
                        .get(SEC_WEBSOCKET_PROTOCOL)
                        .and_then(|value| value.to_str().ok()),
                    Some("graphql-transport-ws")
                );
                return socket;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("GraphQL gateway did not start at {url}");
}

async fn text_frame(socket: &mut Socket) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("GraphQL frame timed out")
            .expect("GraphQL socket closed")
            .expect("GraphQL frame failed");
        match frame {
            Message::Text(text) => return serde_json::from_str(&text).expect("valid JSON frame"),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected GraphQL WebSocket frame: {other:?}"),
        }
    }
}

async fn initialize(socket: &mut Socket) {
    let payload = serde_json::json!({
        "authToken": "integration-token",
        "tenant": TENANT
    });
    socket
        .send(Message::Text(
            serde_json::json!({"type": "connection_init", "payload": payload})
                .to_string()
                .into(),
        ))
        .await
        .expect("send connection_init");
    assert_eq!(text_frame(socket).await["type"], "connection_ack");
}

async fn subscribe(socket: &mut Socket, id: &str, pattern: &str, cursor: Option<&str>) {
    socket
        .send(Message::Text(
            serde_json::json!({
                "id": id,
                "type": "subscribe",
                "payload": {
                    "query": "subscription Events($pattern: String!, $cursor: String) { events(subject: $pattern, resumeFromCursor: $cursor) { entityId cursor seq } }",
                    "variables": {
                        "pattern": pattern,
                        "cursor": cursor
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send GraphQL subscription");
    // graphql-transport-ws has no operation-level subscribed acknowledgement;
    // allow the resolver to open its live-only broker session before publish.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

async fn publish(
    connection: &mut ConnectionManager,
    event_name: &str,
    entity_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = Event::publish(
        TENANT,
        event_name,
        entity_id,
        None,
        chrono::Utc::now(),
        serde_json::json!({"status": "ready"}),
        Metadata::default(),
    )?;
    let _: String = redis::cmd("XADD")
        .arg(STREAM)
        .arg("*")
        .arg("subject")
        .arg(event.subject()?.to_string())
        .arg("event")
        .arg(serde_json::to_vec(&event)?)
        .query_async(connection)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires VS_TEST_REDIS_URL pointing at an isolated Redis server"]
async fn graphql_multiplexes_and_replays_independent_redis_operation_cursors() {
    let redis_url = std::env::var("VS_TEST_REDIS_URL").expect("VS_TEST_REDIS_URL must be set");
    let client = redis::Client::open(redis_url.as_str()).expect("valid Redis URL");
    let mut publisher = client
        .get_connection_manager()
        .await
        .expect("connect Redis publisher");
    let _: usize = redis::cmd("DEL")
        .arg(STREAM)
        .query_async(&mut publisher)
        .await
        .expect("clear test stream");

    let address = available_address();
    let shutdown = ShutdownToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        ventstream_graphql::run(
            GraphQlConfig {
                listen: address,
                expected_tenant: Some(TENANT.to_owned()),
                redis_streams: Some(RedisStreamsConfig {
                    url: redis_url,
                    block_timeout: Duration::from_millis(100),
                    response_timeout: Duration::from_secs(2),
                    ..RedisStreamsConfig::default()
                }),
                ..GraphQlConfig::default()
            },
            server_shutdown,
        )
        .await
    });

    let mut first_socket = connect(address).await;
    initialize(&mut first_socket).await;
    subscribe(&mut first_socket, "orders", "orders.updated.*", None).await;
    subscribe(&mut first_socket, "audits", "audits.recorded.*", None).await;
    publish(&mut publisher, "orders.updated", "order_1")
        .await
        .expect("publish live order");
    publish(&mut publisher, "audits.recorded", "audit_1")
        .await
        .expect("publish live audit");

    let mut order_cursor = None;
    let mut audit_cursor = None;
    for _ in 0..2 {
        let frame = text_frame(&mut first_socket).await;
        assert_eq!(frame["type"], "next");
        let operation = frame["id"].as_str().expect("operation id");
        let event = &frame["payload"]["data"]["events"];
        let cursor = event["cursor"]
            .as_str()
            .expect("Redis GraphQL cursor")
            .to_owned();
        assert!(cursor.starts_with("rs:"));
        assert_eq!(event["seq"], event["cursor"]);
        match operation {
            "orders" => {
                assert_eq!(event["entityId"], "order_1");
                order_cursor = Some(cursor);
            }
            "audits" => {
                assert_eq!(event["entityId"], "audit_1");
                audit_cursor = Some(cursor);
            }
            other => panic!("unexpected operation {other}"),
        }
    }
    first_socket.close(None).await.expect("close first socket");

    publish(&mut publisher, "orders.updated", "order_2")
        .await
        .expect("publish offline order");
    publish(&mut publisher, "audits.recorded", "audit_2")
        .await
        .expect("publish offline audit");
    publish(&mut publisher, "profiles.updated", "profile_1")
        .await
        .expect("publish non-matching event");

    let mut resumed_socket = connect(address).await;
    initialize(&mut resumed_socket).await;
    subscribe(
        &mut resumed_socket,
        "orders-resumed",
        "orders.updated.*",
        order_cursor.as_deref(),
    )
    .await;
    subscribe(
        &mut resumed_socket,
        "audits-resumed",
        "audits.recorded.*",
        audit_cursor.as_deref(),
    )
    .await;

    let mut replayed = std::collections::HashSet::new();
    for _ in 0..2 {
        let replay = text_frame(&mut resumed_socket).await;
        assert_eq!(replay["type"], "next");
        let operation = replay["id"].as_str().expect("resumed operation id");
        let entity = replay["payload"]["data"]["events"]["entityId"]
            .as_str()
            .expect("replayed entity id");
        replayed.insert((operation.to_owned(), entity.to_owned()));
    }
    assert_eq!(
        replayed,
        std::collections::HashSet::from([
            ("orders-resumed".to_owned(), "order_2".to_owned()),
            ("audits-resumed".to_owned(), "audit_2".to_owned()),
        ])
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(250), text_frame(&mut resumed_socket))
            .await
            .is_err(),
        "subject-filtered operations received an unrelated event"
    );

    resumed_socket
        .close(None)
        .await
        .expect("close resumed socket");
    shutdown.cancel();
    server
        .await
        .expect("GraphQL server task")
        .expect("GraphQL server result");
    let _: usize = redis::cmd("DEL")
        .arg(STREAM)
        .query_async(&mut publisher)
        .await
        .expect("remove test stream");
}
