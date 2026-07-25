//! Full native WebSocket contract test backed by a real Redis server.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic
)]

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use redis::aio::ConnectionManager;
use tokio_tungstenite::tungstenite::Message;
use ventstream_core::ShutdownToken;
use ventstream_protocol::{Event, Metadata};
use ventstream_ws::{RedisStreamsConfig, WsConfig};

const TENANT: &str = "integration";
const STREAM: &str = "ventstream:{integration}:events";

fn available_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve test address");
    listener.local_addr().expect("read test address")
}

async fn connect(
    address: SocketAddr,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://{address}/ws");
    for _ in 0..50 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((socket, _)) => return socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("WebSocket gateway did not start at {url}");
}

async fn text_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("WebSocket frame timed out")
            .expect("WebSocket closed")
            .expect("WebSocket frame failed");
        match frame {
            Message::Text(text) => return serde_json::from_str(&text).expect("valid JSON frame"),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected WebSocket frame: {other:?}"),
        }
    }
}

async fn handshake(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    cursor: Option<&str>,
) {
    let mut hello = serde_json::json!({
        "type": "hello",
        "tenant": TENANT,
        "token": "integration-token"
    });
    if let Some(cursor) = cursor {
        hello["resume_from_cursor"] = serde_json::Value::String(cursor.to_owned());
    }
    socket
        .send(Message::Text(hello.to_string().into()))
        .await
        .expect("send hello");
    let ready = text_frame(socket).await;
    assert_eq!(ready["type"], "ready");

    socket
        .send(Message::Text(
            serde_json::json!({
                "type": "subscribe",
                "id": "orders",
                "pattern": "orders.updated.*"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send subscription");
    let subscribed = text_frame(socket).await;
    assert_eq!(subscribed["type"], "subscribed");
}

async fn publish(
    connection: &mut ConnectionManager,
    event_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = Event::publish(
        TENANT,
        "orders.updated",
        event_id,
        None,
        chrono::Utc::now(),
        serde_json::json!({"status": "ready"}),
        Metadata::default(),
    )?;
    let subject = event.subject()?.to_string();
    let payload = serde_json::to_vec(&event)?;
    let _: String = redis::cmd("XADD")
        .arg(STREAM)
        .arg("*")
        .arg("subject")
        .arg(subject)
        .arg("event")
        .arg(payload)
        .query_async(connection)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires VS_TEST_REDIS_URL pointing at an isolated Redis server"]
async fn native_websocket_delivers_live_and_replays_from_redis_cursor() {
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
        ventstream_ws::run(
            WsConfig {
                listen: address,
                expected_tenant: Some(TENANT.to_owned()),
                redis_streams: Some(RedisStreamsConfig {
                    url: redis_url,
                    block_timeout: Duration::from_millis(100),
                    response_timeout: Duration::from_secs(2),
                    ..RedisStreamsConfig::default()
                }),
                ..WsConfig::default()
            },
            server_shutdown,
            Arc::new(AtomicUsize::new(0)),
        )
        .await
    });

    let mut first_socket = connect(address).await;
    handshake(&mut first_socket, None).await;
    publish(&mut publisher, "order_1")
        .await
        .expect("publish live event");
    let first = text_frame(&mut first_socket).await;
    assert_eq!(first["type"], "event");
    assert_eq!(first["event"]["entity_id"], "order_1");
    assert!(first.get("seq").is_none());
    let cursor = first["cursor"].as_str().expect("Redis resume cursor");
    assert!(cursor.starts_with("rs:"));
    first_socket.close(None).await.expect("close first socket");

    publish(&mut publisher, "order_2")
        .await
        .expect("publish replay event");
    let mut resumed_socket = connect(address).await;
    handshake(&mut resumed_socket, Some(cursor)).await;
    let replay = text_frame(&mut resumed_socket).await;
    assert_eq!(replay["type"], "event");
    assert_eq!(replay["event"]["entity_id"], "order_2");
    assert!(replay["cursor"] != first["cursor"]);

    resumed_socket
        .close(None)
        .await
        .expect("close resumed socket");
    shutdown.cancel();
    server
        .await
        .expect("WebSocket server task")
        .expect("WebSocket server result");
    let _: usize = redis::cmd("DEL")
        .arg(STREAM)
        .query_async(&mut publisher)
        .await
        .expect("remove test stream");
}
