//! Live Redis sink contract tests.
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::{path::PathBuf, process::Command, sync::Arc};

use redis::{AsyncCommands, IntoConnectionInfo};
use ventstream_core::{
    ContentType, Event, Headers, Payload, Sink, SinkBatch, SinkHealth, SinkHealthSnapshot,
    SourceUri, Subject,
};
use ventstream_sinks::{
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDocumentFormat, RedisKeyRouting,
    RedisKeyspaceOwnership, RedisSink, RedisTlsConfig, RedisView, RedisViewCondition,
    RedisViewConditionOperator, RedisViewFilter, RedisViewFilterMode, RedisViewKey,
    RedisViewMissingBehavior, RedisViewSource, RedisViewValue, RetryConfig,
};

fn redis_url() -> Option<String> {
    std::env::var("VS_TEST_REDIS_SINK_URL").ok()
}

fn authenticated_redis() -> Option<(String, String)> {
    let url = std::env::var("VS_TEST_REDIS_AUTH_URL").ok()?;
    let password = std::env::var("VS_TEST_REDIS_AUTH_PASSWORD").ok()?;
    Some((url, password))
}

fn redis_json_url() -> Option<String> {
    std::env::var("VS_TEST_REDISJSON_URL").ok()
}

fn pressure_redis_url() -> Option<String> {
    std::env::var("VS_TEST_REDIS_PRESSURE_URL").ok()
}

fn replicated_redis_url() -> Option<String> {
    std::env::var("VS_TEST_REDIS_REPLICATED_URL").ok()
}

fn aof_redis_url() -> Option<String> {
    std::env::var("VS_TEST_REDIS_AOF_URL").ok()
}

fn redis_failover() -> Option<(String, String, String, String)> {
    Some((
        std::env::var("VS_TEST_REDIS_FAILOVER_URL").ok()?,
        std::env::var("VS_TEST_REDIS_FAILOVER_PRIMARY_CONTAINER").ok()?,
        std::env::var("VS_TEST_REDIS_FAILOVER_REPLICA_CONTAINER").ok()?,
        std::env::var("VS_TEST_REDIS_FAILOVER_REPLICA_URL").ok()?,
    ))
}

struct FailoverTopologyGuard {
    primary: String,
    replica: String,
}

struct RestartContainerGuard {
    container: String,
}

impl RestartContainerGuard {
    fn new(container: String) -> Self {
        Self { container }
    }
}

impl Drop for RestartContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["unpause", &self.container])
            .output();
        let _ = Command::new("docker")
            .args(["start", &self.container])
            .output();
    }
}

impl FailoverTopologyGuard {
    fn acquire(primary: String, replica: String) -> Self {
        assert!(
            restore_failover_topology(&primary, &replica),
            "restore Redis failover topology"
        );
        Self { primary, replica }
    }
}

impl Drop for FailoverTopologyGuard {
    fn drop(&mut self) {
        let _ = restore_failover_topology(&self.primary, &self.replica);
    }
}

fn restore_failover_topology(primary: &str, replica: &str) -> bool {
    for container in [primary, replica] {
        let Ok(output) = Command::new("docker").args(["start", container]).output() else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
    }

    let primary_ready = (0..100).any(|_| {
        let ready = Command::new("docker")
            .args(["exec", primary, "redis-cli", "PING"])
            .output()
            .is_ok_and(|output| output.status.success() && output.stdout.starts_with(b"PONG"));
        if !ready {
            std::thread::sleep(Duration::from_millis(50));
        }
        ready
    });
    if !primary_ready {
        return false;
    }

    let Ok(output) = Command::new("docker")
        .args(["exec", replica, "redis-cli", "REPLICAOF", primary, "6379"])
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }

    (0..200).any(|_| {
        let ready = Command::new("docker")
            .args(["exec", replica, "redis-cli", "--raw", "ROLE"])
            .output()
            .is_ok_and(|output| output.status.success() && output.stdout.starts_with(b"slave\n"));
        if !ready {
            std::thread::sleep(Duration::from_millis(50));
        }
        ready
    })
}

fn redis_mtls() -> Option<(String, PathBuf, PathBuf, PathBuf)> {
    Some((
        std::env::var("VS_TEST_REDIS_MTLS_URL").ok()?,
        PathBuf::from(std::env::var("VS_TEST_REDIS_MTLS_CA_FILE").ok()?),
        PathBuf::from(std::env::var("VS_TEST_REDIS_MTLS_CLIENT_CERT_FILE").ok()?),
        PathBuf::from(std::env::var("VS_TEST_REDIS_MTLS_CLIENT_KEY_FILE").ok()?),
    ))
}

fn process_rss_kib() -> u64 {
    Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|rss| rss.trim().parse().ok())
        .unwrap_or(0)
}

async fn authenticated_connection(url: &str, password: &str) -> redis::aio::ConnectionManager {
    let mut connection_info = url
        .into_connection_info()
        .expect("authenticated Redis connection info");
    let settings = connection_info
        .redis_settings()
        .clone()
        .set_password(password);
    connection_info = connection_info.set_redis_settings(settings);
    redis::Client::open(connection_info)
        .expect("authenticated Redis client")
        .get_connection_manager()
        .await
        .expect("authenticated Redis connection")
}

fn event(operation: &str, doc_id: &str, payload: &str) -> Event {
    event_for_relation("public.orders", operation, doc_id, payload)
}

fn versioned_event(operation: &str, doc_id: &str, payload: &str, version: &str) -> Event {
    let mut event = event(operation, doc_id, payload);
    event.headers = event
        .headers
        .clone()
        .with_header("ventstream.cdc.lsn".to_owned(), version.to_owned());
    event
}

fn event_for_relation(relation: &str, operation: &str, doc_id: &str, payload: &str) -> Event {
    let headers = Headers::empty()
        .with_header("ventstream.cdc.relation".to_owned(), relation.to_owned())
        .with_header("ventstream.doc.id".to_owned(), doc_id.to_owned());
    Event::builder(
        SourceUri::new("postgres://integration").expect("source"),
        Subject::new(format!("postgres.{relation}.{operation}")).expect("subject"),
    )
    .content_type(ContentType::Json)
    .headers(headers)
    .payload(Payload::from_vec(payload.as_bytes().to_vec()))
    .build()
}

fn test_pool_index(target: &str) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    target.hash(&mut hasher);
    usize::try_from(hasher.finish()).unwrap_or(usize::MAX) % 8
}

fn truncate_event(relation: &str) -> Event {
    let headers =
        Headers::empty().with_header("ventstream.cdc.relation".to_owned(), relation.to_owned());
    Event::builder(
        SourceUri::new("postgres://integration").expect("source"),
        Subject::new(format!("postgres.public.{relation}.truncate")).expect("subject"),
    )
    .content_type(ContentType::Json)
    .headers(headers)
    .payload(Payload::from_vec(
        br#"{"cascade":false,"restart_identity":false}"#.to_vec(),
    ))
    .build()
}

fn lookup_view_config(url: &str, prefix: &str, contract: RedisContract) -> RedisConfig {
    let mut open_order_fields = BTreeMap::new();
    open_order_fields.insert("id".to_owned(), "/id".to_owned());
    open_order_fields.insert("status".to_owned(), "/status".to_owned());
    RedisConfig::new(
        "redis-view-test",
        url,
        prefix,
        RedisKeyRouting::Views(vec![
            RedisView {
                name: "open_order_by_id".to_owned(),
                source: RedisViewSource {
                    namespace: None,
                    relation: Some("public.orders".to_owned()),
                    projection_target: None,
                },
                key: RedisViewKey {
                    template: "order:${json:/id}".to_owned(),
                    on_missing: RedisViewMissingBehavior::Block,
                },
                filter: Some(RedisViewFilter {
                    mode: RedisViewFilterMode::All,
                    conditions: vec![RedisViewCondition {
                        path: "/status".to_owned(),
                        operator: RedisViewConditionOperator::In(vec![
                            serde_json::json!("pending"),
                            serde_json::json!("processing"),
                        ]),
                    }],
                }),
                value: RedisViewValue::Fields(open_order_fields),
            },
            RedisView {
                name: "order_by_customer".to_owned(),
                source: RedisViewSource {
                    namespace: None,
                    relation: Some("public.orders".to_owned()),
                    projection_target: None,
                },
                key: RedisViewKey {
                    template: "customer:${json:/customer_id}:order:${json:/id}".to_owned(),
                    on_missing: RedisViewMissingBehavior::Block,
                },
                filter: None,
                value: RedisViewValue::Document,
            },
        ]),
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_writer_lease(Duration::from_secs(3))
    .with_contract(contract)
}

async fn matching_keys(
    connection: &mut redis::aio::ConnectionManager,
    pattern: &str,
) -> Vec<String> {
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (next, mut page) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(500)
            .query_async::<(u64, Vec<String>)>(&mut *connection)
            .await
            .expect("scan Redis keys");
        keys.append(&mut page);
        cursor = next;
        if cursor == 0 {
            return keys;
        }
    }
}

#[tokio::test]
async fn lookup_views_fan_out_move_filter_and_delete_without_tombstone_payload() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:views:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let open_key = format!("{prefix}:{{open_order_by_id}}:order%3A1");
    let first_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3A1");
    let second_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3A1");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");

    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending","total":42}"#,
    )]))
    .await
    .expect("fan out initial views");
    assert_eq!(
        connection.get::<_, String>(&open_key).await,
        Ok(r#"{"id":"1","status":"pending"}"#.to_owned())
    );
    assert_eq!(
        connection.get::<_, String>(&first_customer_key).await,
        Ok(r#"{"id":"1","customer_id":"customer-1","status":"pending","total":42}"#.to_owned())
    );

    sink.write(SinkBatch::new(vec![event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-2","status":"shipped","total":42}"#,
    )]))
    .await
    .expect("move derived key and remove filtered view");
    assert_eq!(connection.exists::<_, bool>(&open_key).await, Ok(false));
    assert_eq!(
        connection.exists::<_, bool>(&first_customer_key).await,
        Ok(false)
    );
    assert_eq!(
        connection.get::<_, String>(&second_customer_key).await,
        Ok(r#"{"id":"1","customer_id":"customer-2","status":"shipped","total":42}"#.to_owned())
    );

    sink.write(SinkBatch::new(vec![event(
        "delete",
        r#"public.orders:["1"]"#,
        "{}",
    )]))
    .await
    .expect("delete from manifests");
    assert_eq!(
        connection.exists::<_, bool>(&second_customer_key).await,
        Ok(false)
    );
    assert!(matching_keys(
        &mut connection,
        &format!("{prefix}:__ventstream:manifest:*")
    )
    .await
    .is_empty());
    assert!(
        matching_keys(&mut connection, &format!("{prefix}:__ventstream:owner:*"))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn lookup_views_reject_stale_moves_and_keep_versioned_tombstones() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-source-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let customer_two = format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3A1");
    let customer_three =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-3%3Aorder%3A1");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");

    sink.write(SinkBatch::new(vec![versioned_event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
        "100",
    )]))
    .await
    .expect("initial view");
    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-2","status":"processing"}"#,
        "102",
    )]))
    .await
    .expect("newer view move");
    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-3","status":"processing"}"#,
        "101",
    )]))
    .await
    .expect("stale view move is acknowledged as a no-op");
    assert_eq!(connection.exists::<_, bool>(&customer_two).await, Ok(true));
    assert_eq!(
        connection.exists::<_, bool>(&customer_three).await,
        Ok(false)
    );

    sink.write(SinkBatch::new(vec![versioned_event(
        "delete",
        r#"public.orders:["1"]"#,
        "{}",
        "103",
    )]))
    .await
    .expect("versioned view delete");
    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-2","status":"processing"}"#,
        "102",
    )]))
    .await
    .expect("stale view resurrection is acknowledged as a no-op");
    assert_eq!(connection.exists::<_, bool>(&customer_two).await, Ok(false));
    let manifests = matching_keys(
        &mut connection,
        &format!("{prefix}:__ventstream:manifest:*"),
    )
    .await;
    assert_eq!(manifests.len(), 2, "one version tombstone per matched view");
    for manifest in manifests {
        let fields: BTreeMap<String, String> = connection
            .hgetall(manifest)
            .await
            .expect("version tombstone");
        assert_eq!(fields.get("version").map(String::as_str), Some("103"));
        assert!(!fields.contains_key("key"));
        assert!(!fields.contains_key("owner"));
    }
}

#[tokio::test]
async fn lookup_view_transformation_failure_preserves_every_previous_materialization() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-fail-closed:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let open_key = format!("{prefix}:{{open_order_by_id}}:order%3A1");
    let first_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3A1");
    let second_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3A1");
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial views");

    let result = sink
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["1"]"#,
            r#"{"id":"1","status":"processing"}"#,
        )]))
        .await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("key template could not resolve")
    ));

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(
        connection.get::<_, String>(&open_key).await,
        Ok(r#"{"id":"1","status":"pending"}"#.to_owned())
    );
    assert_eq!(
        connection.get::<_, String>(&first_customer_key).await,
        Ok(r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#.to_owned())
    );

    sink.write(SinkBatch::new(vec![event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-2","status":"processing"}"#,
    )]))
    .await
    .expect("valid recovery update");
    assert_eq!(
        connection.get::<_, String>(&open_key).await,
        Ok(r#"{"id":"1","status":"processing"}"#.to_owned())
    );
    assert_eq!(
        connection.exists::<_, bool>(&first_customer_key).await,
        Ok(false)
    );
    assert_eq!(
        connection.get::<_, String>(&second_customer_key).await,
        Ok(r#"{"id":"1","customer_id":"customer-2","status":"processing"}"#.to_owned())
    );
}

#[tokio::test]
async fn lookup_view_manifests_survive_sink_restart() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-restart:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3A1");
    let config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let first = RedisSink::connect(config.clone())
        .await
        .expect("first sink");
    first
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["1"]"#,
            r#"{"id":"1","customer_id":"customer-1","status":"shipped"}"#,
        )]))
        .await
        .expect("initial view");
    drop(first);

    let second = RedisSink::connect(config).await.expect("replacement sink");
    second
        .write(SinkBatch::new(vec![event(
            "delete",
            r#"public.orders:["1"]"#,
            "{}",
        )]))
        .await
        .expect("restart delete");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(connection.exists::<_, bool>(&key).await, Ok(false));
}

#[tokio::test]
async fn lookup_view_schema_changes_require_rebootstrap_and_clear_removed_targets() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-schema:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let initial = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let sink = RedisSink::connect(initial)
        .await
        .expect("initial view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial materialization");
    drop(sink);

    let mut revised = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let RedisKeyRouting::Views(views) = &mut revised.key_routing else {
        panic!("expected views routing");
    };
    views.truncate(1);
    views[0].key.template = "order-v2:${json:/id}".to_owned();

    let result = RedisSink::connect(revised.clone()).await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("run an exclusive drain/rebootstrap")
    ));

    RedisSink::reset_owned_targets(revised.clone(), &["open_order_by_id".to_owned()])
        .await
        .expect("view rebootstrap");
    let replacement = RedisSink::connect(revised)
        .await
        .expect("revised view sink");
    replacement
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["1"]"#,
            r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
        )]))
        .await
        .expect("revised materialization");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(
        connection
            .get::<_, String>(format!("{prefix}:{{open_order_by_id}}:order-v2%3A1"))
            .await,
        Ok(r#"{"id":"1","status":"pending"}"#.to_owned())
    );
    assert!(
        matching_keys(
            &mut connection,
            &format!("{prefix}:{{order_by_customer}}:*")
        )
        .await
        .is_empty(),
        "rebootstrap did not clear a removed view target"
    );
}

#[tokio::test]
async fn lookup_view_schema_is_order_independent_and_tracks_contract_changes() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-schema-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let initial = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let first = RedisSink::connect(initial.clone())
        .await
        .expect("initial view sink");
    drop(first);

    let mut reordered = initial.clone();
    let RedisKeyRouting::Views(views) = &mut reordered.key_routing else {
        panic!("expected views routing");
    };
    views.reverse();
    RedisSink::connect(reordered)
        .await
        .expect("view declaration order must not change the schema");

    let mut cache = initial;
    cache.contract = RedisContract::Cache {
        ttl: Duration::from_secs(60),
    };
    let result = RedisSink::connect(cache).await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("run an exclusive drain/rebootstrap")
    ));

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("UNLINK")
        .arg(format!("{prefix}:__ventstream:view-schema:current"))
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove view schema metadata");
}

#[tokio::test]
async fn lookup_view_schema_tracks_document_format_changes() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-schema-format:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let initial = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let first = RedisSink::connect(initial.clone())
        .await
        .expect("initial view sink");
    drop(first);

    let revised = initial.with_document_format(RedisDocumentFormat::Json);
    let result = RedisSink::connect(revised).await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("run an exclusive drain/rebootstrap")
    ));

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("UNLINK")
        .arg(format!("{prefix}:__ventstream:view-schema:current"))
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove view schema metadata");
}

#[tokio::test]
async fn lookup_view_rebootstrap_rejects_malformed_schema_metadata_before_clearing_data() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-schema-invalid:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let sink = RedisSink::connect(config.clone())
        .await
        .expect("initial view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial materialization");
    drop(sink);

    let data_key = format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3A1");
    let metadata_key = format!("{prefix}:__ventstream:view-schema:current");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("SET")
        .arg(&metadata_key)
        .arg(r#"{"version":1,"digest":"bad","targets":["order_by_customer"]}"#)
        .query_async::<String>(&mut connection)
        .await
        .expect("corrupt view schema metadata");

    let result = RedisSink::reset_owned_targets(config, &["order_by_customer".to_owned()]).await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("stored Redis view schema metadata is invalid")
    ));
    assert_eq!(connection.exists::<_, bool>(&data_key).await, Ok(true));

    redis::cmd("UNLINK")
        .arg(&[metadata_key, data_key])
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove malformed metadata test keys");
    let remaining = matching_keys(&mut connection, &format!("{prefix}:*")).await;
    if !remaining.is_empty() {
        redis::cmd("UNLINK")
            .arg(remaining)
            .query_async::<usize>(&mut connection)
            .await
            .expect("remove malformed metadata test manifests");
    }
}

#[tokio::test]
async fn lookup_view_collision_blocks_before_any_event_is_materialized() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-collision:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    config.retry.max_attempts = 1;
    let sink = RedisSink::connect(config).await.expect("view sink");
    let result = sink
        .write(SinkBatch::new(vec![
            event(
                "insert",
                r#"public.orders:["source-1"]"#,
                r#"{"id":"shared","customer_id":"customer-1","status":"pending"}"#,
            ),
            event(
                "insert",
                r#"public.orders:["source-2"]"#,
                r#"{"id":"shared","customer_id":"customer-1","status":"pending"}"#,
            ),
        ]))
        .await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("VENTSTREAM_VIEW_KEY_COLLISION")
    ));

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert!(
        matching_keys(&mut connection, &format!("{prefix}:{{*}}:*"))
            .await
            .is_empty(),
        "collision caused a partial visible write"
    );
    assert!(
        matching_keys(
            &mut connection,
            &format!("{prefix}:__ventstream:manifest:*")
        )
        .await
        .is_empty(),
        "collision caused a partial manifest write"
    );
}

#[tokio::test]
async fn lookup_view_blocks_an_existing_key_without_ownership_metadata() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-missing-owner:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let unowned_key = format!("{prefix}:{{open_order_by_id}}:order%3Aunowned");
    let customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-1%3Aorder%3Aunowned");
    let mut config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    config.retry.max_attempts = 1;
    let sink = RedisSink::connect(config).await.expect("view sink");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    connection
        .set::<_, _, ()>(&unowned_key, r#"{"source":"external"}"#)
        .await
        .expect("seed key without ownership metadata");

    let result = sink
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["source-1"]"#,
            r#"{"id":"unowned","customer_id":"customer-1","status":"pending"}"#,
        )]))
        .await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("VENTSTREAM_VIEW_OWNERSHIP_MISSING")
    ));
    assert_eq!(
        connection.get::<_, String>(&unowned_key).await,
        Ok(r#"{"source":"external"}"#.to_owned())
    );
    assert_eq!(connection.exists::<_, bool>(&customer_key).await, Ok(false));
    assert!(
        matching_keys(
            &mut connection,
            &format!("{prefix}:__ventstream:manifest:*")
        )
        .await
        .is_empty(),
        "missing ownership caused a partial manifest write"
    );
}

#[tokio::test]
async fn lookup_view_batch_can_handoff_a_key_released_by_an_earlier_event() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-handoff:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["source-a"]"#,
        r#"{"id":"shared","customer_id":"customer-1","status":"pending","source":"a"}"#,
    )]))
    .await
    .expect("initial owner");

    sink.write(SinkBatch::new(vec![
        event(
            "update",
            r#"public.orders:["source-a"]"#,
            r#"{"id":"moved","customer_id":"customer-1","status":"pending","source":"a"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["source-b"]"#,
            r#"{"id":"shared","customer_id":"customer-2","status":"pending","source":"b"}"#,
        ),
    ]))
    .await
    .expect("ordered key handoff");

    let moved_key = format!("{prefix}:{{open_order_by_id}}:order%3Amoved");
    let shared_key = format!("{prefix}:{{open_order_by_id}}:order%3Ashared");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(
        connection.get::<_, String>(&moved_key).await,
        Ok(r#"{"id":"moved","status":"pending"}"#.to_owned())
    );
    assert_eq!(
        connection.get::<_, String>(&shared_key).await,
        Ok(r#"{"id":"shared","status":"pending"}"#.to_owned())
    );
}

#[tokio::test]
async fn redis_json_lookup_view_batch_can_handoff_a_key_in_event_order() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-json-handoff:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(
        lookup_view_config(&url, &prefix, RedisContract::MaterializedView)
            .with_document_format(RedisDocumentFormat::Json),
    )
    .await
    .expect("RedisJSON view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["source-a"]"#,
        r#"{"id":"shared","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial owner");

    sink.write(SinkBatch::new(vec![
        event(
            "update",
            r#"public.orders:["source-a"]"#,
            r#"{"id":"moved","customer_id":"customer-1","status":"pending"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["source-b"]"#,
            r#"{"id":"shared","customer_id":"customer-2","status":"processing"}"#,
        ),
    ]))
    .await
    .expect("ordered RedisJSON key handoff");

    let moved_key = format!("{prefix}:{{open_order_by_id}}:order%3Amoved");
    let shared_key = format!("{prefix}:{{open_order_by_id}}:order%3Ashared");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    for (key, expected) in [
        (
            moved_key,
            serde_json::json!({"id": "moved", "status": "pending"}),
        ),
        (
            shared_key,
            serde_json::json!({"id": "shared", "status": "processing"}),
        ),
    ] {
        let stored = redis::cmd("JSON.GET")
            .arg(key)
            .query_async::<String>(&mut connection)
            .await
            .expect("JSON.GET handed-off view");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
            expected
        );
    }
}

#[tokio::test]
async fn lookup_view_batch_rejects_a_handoff_before_the_current_owner_releases_the_key() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-handoff-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    config.retry.max_attempts = 1;
    let sink = RedisSink::connect(config).await.expect("view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["source-a"]"#,
        r#"{"id":"shared","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial owner");

    let result = sink
        .write(SinkBatch::new(vec![
            event(
                "insert",
                r#"public.orders:["source-b"]"#,
                r#"{"id":"shared","customer_id":"customer-2","status":"processing"}"#,
            ),
            event(
                "update",
                r#"public.orders:["source-a"]"#,
                r#"{"id":"moved","customer_id":"customer-1","status":"pending"}"#,
            ),
        ]))
        .await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("VENTSTREAM_VIEW_KEY_COLLISION")
    ));

    let moved_key = format!("{prefix}:{{open_order_by_id}}:order%3Amoved");
    let shared_key = format!("{prefix}:{{open_order_by_id}}:order%3Ashared");
    let second_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3Ashared");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(connection.exists::<_, bool>(&moved_key).await, Ok(false));
    assert_eq!(
        connection.exists::<_, bool>(&second_customer_key).await,
        Ok(false)
    );
    assert_eq!(
        connection.get::<_, String>(&shared_key).await,
        Ok(r#"{"id":"shared","status":"pending"}"#.to_owned())
    );
}

#[tokio::test]
async fn lookup_view_batch_releases_an_intermediate_key_after_multiple_source_transitions() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-multiple-transitions:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["source-a"]"#,
        r#"{"id":"shared","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("initial owner");

    sink.write(SinkBatch::new(vec![
        event(
            "update",
            r#"public.orders:["source-a"]"#,
            r#"{"id":"temporary","customer_id":"customer-1","status":"pending"}"#,
        ),
        event(
            "update",
            r#"public.orders:["source-a"]"#,
            r#"{"id":"final","customer_id":"customer-1","status":"pending"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["source-b"]"#,
            r#"{"id":"temporary","customer_id":"customer-2","status":"processing"}"#,
        ),
    ]))
    .await
    .expect("multiple ordered key transitions");

    let final_key = format!("{prefix}:{{open_order_by_id}}:order%3Afinal");
    let temporary_key = format!("{prefix}:{{open_order_by_id}}:order%3Atemporary");
    let shared_key = format!("{prefix}:{{open_order_by_id}}:order%3Ashared");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(connection.exists::<_, bool>(&shared_key).await, Ok(false));
    assert_eq!(
        connection.get::<_, String>(&final_key).await,
        Ok(r#"{"id":"final","status":"pending"}"#.to_owned())
    );
    assert_eq!(
        connection.get::<_, String>(&temporary_key).await,
        Ok(r#"{"id":"temporary","status":"processing"}"#.to_owned())
    );
}

#[tokio::test]
async fn lookup_view_cache_expires_data_owner_and_manifest_together() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-cache:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::Cache {
            ttl: Duration::from_millis(150),
        },
    ))
    .await
    .expect("view cache sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("cache fanout");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert!(
        matching_keys(&mut connection, &format!("{prefix}:*"))
            .await
            .len()
            >= 6
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    let retained = matching_keys(&mut connection, &format!("{prefix}:*"))
        .await
        .into_iter()
        .filter(|key| {
            !key.contains(":__ventstream:writer:")
                && !key.ends_with(":__ventstream:view-schema:current")
        })
        .collect::<Vec<_>>();
    assert!(
        retained.is_empty(),
        "cache metadata outlived view data: {retained:?}"
    );
}

#[tokio::test]
async fn lookup_view_truncate_clears_all_matching_view_state() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-truncate:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view sink");
    sink.write(SinkBatch::new(vec![
        event(
            "insert",
            r#"public.orders:["1"]"#,
            r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["2"]"#,
            r#"{"id":"2","customer_id":"customer-1","status":"processing"}"#,
        ),
    ]))
    .await
    .expect("seed views");
    sink.write(SinkBatch::new(vec![truncate_event("public.orders")]))
        .await
        .expect("clear views");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let retained = matching_keys(&mut connection, &format!("{prefix}:*"))
        .await
        .into_iter()
        .filter(|key| {
            !key.contains(":__ventstream:writer:")
                && !key.ends_with(":__ventstream:view-schema:current")
        })
        .collect::<Vec<_>>();
    assert!(
        retained.is_empty(),
        "truncate left view state: {retained:?}"
    );
}

#[tokio::test]
async fn redis_json_lookup_views_publish_selected_documents_atomically() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-json:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{open_order_by_id}}:order%3A1");
    let sink = RedisSink::connect(
        lookup_view_config(&url, &prefix, RedisContract::MaterializedView)
            .with_document_format(RedisDocumentFormat::Json),
    )
    .await
    .expect("RedisJSON view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending"}"#,
    )]))
    .await
    .expect("RedisJSON view write");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let stored = redis::cmd("JSON.GET")
        .arg(&key)
        .query_async::<String>(&mut connection)
        .await
        .expect("JSON.GET selected view");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
        serde_json::json!({"id": "1", "status": "pending"})
    );
}

#[tokio::test]
async fn lookup_views_reject_incomplete_acl_permissions_at_startup() {
    let Some((url, admin_password)) = authenticated_redis() else {
        return;
    };
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let prefix = format!("ventstream:test:view-acl:{suffix}");
    let username = format!("ventstream_view_incomplete_{suffix}");
    let user_password = "ventstream-view-incomplete";
    let mut admin = authenticated_connection(&url, &admin_password).await;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{user_password}"))
        .arg(format!("~{prefix}:*"))
        .arg("+ping")
        .arg("+evalsha")
        .arg("+script|load")
        .arg("+scan")
        .arg("+get")
        .arg("+set")
        .arg("+exists")
        .arg("+type")
        .arg("+del")
        .arg("+unlink")
        .query_async::<()>(&mut admin)
        .await
        .expect("create incomplete view ACL user");

    let result = RedisSink::connect(
        lookup_view_config(&url, &prefix, RedisContract::MaterializedView)
            .with_auth(Some(username.clone()), Some(user_password.to_owned())),
    )
    .await;
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove incomplete view ACL user");

    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("permission")
                || reason.to_ascii_lowercase().contains("noperm")
    ));
    assert!(
        matching_keys(&mut admin, &format!("{prefix}:*"))
            .await
            .is_empty(),
        "failed capability probe left temporary keys"
    );
}

#[tokio::test]
async fn string_sink_upserts_expires_and_deletes() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%221%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");

    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_writer_id("materialized-contract"),
    )
    .await
    .expect("sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","status":"created"}"#,
    )]))
    .await
    .expect("insert");
    let stored: Vec<u8> = connection.get(&key).await.expect("get insert");
    assert_eq!(stored, br#"{"id":"1","status":"created"}"#);

    sink.write(SinkBatch::new(vec![event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","status":"paid"}"#,
    )]))
    .await
    .expect("update");
    let stored: Vec<u8> = connection.get(&key).await.expect("get update");
    assert_eq!(stored, br#"{"id":"1","status":"paid"}"#);

    sink.write(SinkBatch::new(vec![event(
        "delete",
        r#"public.orders:["1"]"#,
        "{}",
    )]))
    .await
    .expect("delete");
    let exists: bool = connection.exists(&key).await.expect("exists after delete");
    assert!(!exists);
    sink.write(SinkBatch::new(vec![event(
        "delete",
        r#"public.orders:["1"]"#,
        "{}",
    )]))
    .await
    .expect("idempotent delete");

    let cache = RedisSink::connect(
        RedisConfig::new(
            "redis-cache-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_writer_id("cache-contract")
        .with_writer_takeover_from("materialized-contract")
        .with_contract(RedisContract::Cache {
            ttl: Duration::from_millis(100),
        }),
    )
    .await
    .expect("cache sink");
    cache
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["1"]"#,
            r#"{"id":"1"}"#,
        )]))
        .await
        .expect("cache write");
    let exists: bool = connection.exists(&key).await.expect("exists before TTL");
    assert!(exists);
    tokio::time::sleep(Duration::from_millis(175)).await;
    let exists: bool = connection.exists(&key).await.expect("exists after TTL");
    assert!(!exists);
}

#[tokio::test]
async fn direct_materialization_rejects_stale_replays_and_preserves_delete_versions() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:source-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%221%22%5D");
    let version_key =
        format!("{prefix}:__ventstream:version:{{public.orders}}:public.orders%3A%5B%221%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let sink = RedisSink::connect(RedisConfig::new(
        "redis-source-order-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    ))
    .await
    .expect("sink");

    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","version":"new"}"#,
        "90071992547409931",
    )]))
    .await
    .expect("newer write");
    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","version":"stale"}"#,
        "90071992547409930",
    )]))
    .await
    .expect("stale replay is acknowledged as a no-op");
    assert_eq!(
        connection.get::<_, String>(&key).await,
        Ok(r#"{"id":"1","version":"new"}"#.to_owned())
    );

    sink.write(SinkBatch::new(vec![versioned_event(
        "delete",
        r#"public.orders:["1"]"#,
        "{}",
        "90071992547409932",
    )]))
    .await
    .expect("versioned delete");
    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","version":"resurrected"}"#,
        "90071992547409931",
    )]))
    .await
    .expect("stale resurrection is acknowledged as a no-op");
    assert_eq!(connection.exists::<_, bool>(&key).await, Ok(false));
    assert_eq!(
        connection.get::<_, String>(&version_key).await,
        Ok("90071992547409932".to_owned())
    );

    sink.write(SinkBatch::new(vec![versioned_event(
        "update",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","version":"restored"}"#,
        "90071992547409933",
    )]))
    .await
    .expect("newer write after delete");
    assert_eq!(
        connection.get::<_, String>(&key).await,
        Ok(r#"{"id":"1","version":"restored"}"#.to_owned())
    );
}

#[tokio::test]
async fn script_cache_flush_reloads_on_the_active_primary() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:script-reload:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(RedisConfig::new(
        "redis-script-reload-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    ))
    .await
    .expect("sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["before-flush"]"#,
        r#"{"id":"before-flush"}"#,
    )]))
    .await
    .expect("write before script flush");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async::<String>(&mut connection)
        .await
        .expect("flush scripts");

    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["after-flush"]"#,
        r#"{"id":"after-flush"}"#,
    )]))
    .await
    .expect("write after script flush");
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22after-flush%22%5D");
    assert_eq!(
        connection.get::<_, String>(key).await,
        Ok(r#"{"id":"after-flush"}"#.to_owned())
    );
}

#[tokio::test]
async fn online_diagnostic_checks_live_capabilities_without_claiming_a_writer() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:diagnostic:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let report = RedisSink::diagnose(
        RedisConfig::new(
            "redis-diagnostic-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive),
    )
    .await
    .expect("Redis diagnostic");

    assert_eq!(report.topology, "standalone");
    assert!(!report.tls);
    assert!(!report.authenticated);
    assert!(report.server_version.is_some());
    assert_eq!(report.server_role.as_deref(), Some("master"));
    assert_eq!(report.required_replica_acks, 0);
    assert_eq!(report.observed_replica_acks, None);
    assert!(!report.required_local_aof);
    assert_eq!(report.observed_local_aof_acks, None);
    assert_eq!(report.view_count, 0);

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let keys = matching_keys(&mut connection, &format!("{prefix}:*")).await;
    assert!(keys.is_empty(), "diagnostic left Redis keys behind");
}

#[tokio::test]
async fn online_diagnostic_rejects_incomplete_acl_permissions() {
    let Some((url, admin_password)) = authenticated_redis() else {
        return;
    };
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let username = format!("ventstream_diagnostic_denied_{suffix}");
    let prefix = format!("ventstream:test:diagnostic-acl:{suffix}");
    let user_password = "ventstream-diagnostic-denied";
    let mut admin = authenticated_connection(&url, &admin_password).await;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{user_password}"))
        .arg(format!("~{prefix}:*"))
        .arg("+ping")
        .query_async::<()>(&mut admin)
        .await
        .expect("create restricted diagnostic user");

    let result = RedisSink::diagnose(
        RedisConfig::new(
            "redis-diagnostic-acl-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_auth(Some(username.clone()), Some(user_password.to_owned())),
    )
    .await;
    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove restricted diagnostic user");

    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("permission")
                || reason.to_ascii_lowercase().contains("noperm")
    ));
    assert!(
        matching_keys(&mut admin, &format!("{prefix}:*"))
            .await
            .is_empty(),
        "failed diagnostic left Redis keys behind"
    );
}

#[tokio::test]
async fn online_diagnostic_verifies_replica_acknowledgement() {
    let Some(url) = replicated_redis_url() else {
        return;
    };
    let report = RedisSink::diagnose(
        RedisConfig::new(
            "redis-diagnostic-replica-test",
            &url,
            "ventstream:test:diagnostic-replica",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_acknowledgement(RedisAcknowledgement::Replicated {
            replicas: 1,
            timeout: Duration::from_secs(2),
        }),
    )
    .await
    .expect("replica-acknowledged diagnostic");

    assert_eq!(report.required_replica_acks, 1);
    assert!(report.observed_replica_acks.is_some_and(|acks| acks >= 1));
}

#[tokio::test]
async fn online_diagnostic_does_not_initialize_view_schema() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:diagnostic-view:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let report = RedisSink::diagnose(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view diagnostic");

    assert_eq!(
        report.view_schema,
        ventstream_sinks::redis::RedisViewSchemaStatus::Missing
    );
    assert_eq!(report.view_count, 2);
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert!(
        matching_keys(&mut connection, &format!("{prefix}:*"))
            .await
            .is_empty(),
        "view diagnostic initialized Redis metadata"
    );
}

#[tokio::test]
async fn drift_inspection_is_bounded_and_detects_direct_staging_leaks() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:drift-direct:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let config = RedisConfig::new(
        "redis-drift-direct-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    );
    let sink = RedisSink::connect(config.clone()).await.expect("sink");
    sink.write(SinkBatch::new(vec![
        event(
            "insert",
            r#"public.orders:["1"]"#,
            r#"{"id":"1","status":"created"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["2"]"#,
            r#"{"id":"2","status":"created"}"#,
        ),
    ]))
    .await
    .expect("materialize rows");

    let targets = vec!["public.orders".to_owned()];
    let report = RedisSink::inspect_drift(config.clone(), &targets, 100)
        .await
        .expect("inspect direct materialization");
    assert!(report.complete);
    assert!(report.consistent);
    assert!(report.source_comparison_required);
    assert_eq!(report.targets[0].data_keys, 2);
    assert_eq!(report.targets[0].staging_keys, 0);

    let bounded = RedisSink::inspect_drift(config.clone(), &targets, 1)
        .await
        .expect("bounded inspection");
    assert!(!bounded.complete);
    assert!(!bounded.consistent);

    let staging_key = format!("{prefix}:__ventstream:stage:{{public.orders}}:leaked");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    connection
        .set::<_, _, ()>(&staging_key, "temporary")
        .await
        .expect("seed leaked staging key");
    let report = RedisSink::inspect_drift(config, &targets, 100)
        .await
        .expect("inspect staging leak");
    assert!(!report.consistent);
    assert_eq!(report.targets[0].staging_keys, 1);
    assert!(report.targets[0].requires_rebootstrap);
}

#[tokio::test]
async fn drift_inspection_detects_broken_view_ownership_without_mutating_data() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:drift-view:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    let sink = RedisSink::connect(config.clone()).await.expect("view sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["1"]"#,
        r#"{"id":"1","customer_id":"customer-1","status":"pending","total":42}"#,
    )]))
    .await
    .expect("materialize view");

    let target = "open_order_by_id";
    let targets = vec![target.to_owned()];
    let report = RedisSink::inspect_drift(config.clone(), &targets, 100)
        .await
        .expect("inspect consistent view");
    assert!(report.complete);
    assert!(report.consistent);
    assert_eq!(report.targets[0].data_keys, 1);
    assert_eq!(report.targets[0].manifest_keys, 1);
    assert_eq!(report.targets[0].owner_keys, 1);

    let data_key = format!("{prefix}:{{{target}}}:order%3A1");
    let orphan_owner = format!("{prefix}:__ventstream:owner:{{{target}}}:orphan");
    let unowned_value = format!("{prefix}:{{{target}}}:unowned");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("UNLINK")
        .arg(&data_key)
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove view value");
    connection
        .set::<_, _, ()>(&orphan_owner, r#"public.orders:["missing"]"#)
        .await
        .expect("seed orphan owner");
    connection
        .set::<_, _, ()>(&unowned_value, r#"{"id":"unowned"}"#)
        .await
        .expect("seed unowned value");

    let report = RedisSink::inspect_drift(config, &targets, 100)
        .await
        .expect("inspect broken view");
    assert!(!report.consistent);
    assert_eq!(report.targets[0].missing_values, 1);
    assert_eq!(report.targets[0].orphan_owners, 1);
    assert_eq!(report.targets[0].unowned_values, 1);
    assert!(report.targets[0].requires_rebootstrap);
    assert_eq!(
        connection.get::<_, String>(&unowned_value).await,
        Ok(r#"{"id":"unowned"}"#.to_owned())
    );
}

#[tokio::test]
async fn pooled_writes_serialize_each_target_and_open_an_independent_lane() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:pooled-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let first_target = "public.orders";
    let second_target = (0..100)
        .map(|index| format!("public.customers_{index}"))
        .find(|target| test_pool_index(target) != test_pool_index(first_target))
        .expect("target on a different connection stripe");
    let sink = Arc::new(
        RedisSink::connect(RedisConfig::new(
            "redis-pooled-order-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        ))
        .await
        .expect("sink"),
    );
    assert_eq!(sink.recommended_concurrency(100), 1);
    let mut admin = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");

    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(800)
        .arg("WRITE")
        .query_async::<String>(&mut admin)
        .await
        .expect("pause Redis writes");
    let first = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event_for_relation(
                first_target,
                "update",
                r#"public.orders:["pooled"]"#,
                r#"{"id":"pooled","version":1}"#,
            )]))
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(75)).await;
    let second = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event_for_relation(
                first_target,
                "update",
                r#"public.orders:["pooled"]"#,
                r#"{"id":"pooled","version":2}"#,
            )]))
            .await
        })
    };
    let unrelated = {
        let sink = Arc::clone(&sink);
        let second_target = second_target.clone();
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event_for_relation(
                &second_target,
                "update",
                r#"public.customers:["independent"]"#,
                r#"{"id":"independent","version":1}"#,
            )]))
            .await
        })
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let info = redis::cmd("INFO")
                .arg("clients")
                .query_async::<String>(&mut admin)
                .await
                .expect("client info");
            let connected = info
                .lines()
                .find_map(|line| line.strip_prefix("connected_clients:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or_default();
            if connected >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("independent Redis pool connection");

    first.await.expect("first task").expect("first write");
    second.await.expect("second task").expect("second write");
    unrelated
        .await
        .expect("unrelated task")
        .expect("unrelated write");
    let first_key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22pooled%22%5D");
    let stored = redis::cmd("GET")
        .arg(first_key)
        .query_async::<Vec<u8>>(&mut admin)
        .await
        .expect("ordered value");
    assert_eq!(stored, br#"{"id":"pooled","version":2}"#.to_vec());
}

#[tokio::test]
async fn truncate_is_bounded_target_scoped_and_ordered() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:truncate:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let orders_pattern = format!("{prefix}:{{public.orders}}:*");
    let customers_key = format!("{prefix}:{{public.customers}}:customer-1");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");

    for start in (0..2_500).step_by(500) {
        let mut seed = redis::pipe();
        for id in start..start + 500 {
            seed.cmd("SET")
                .arg(format!("{prefix}:{{public.orders}}:seed-{id}"))
                .arg("{}")
                .ignore();
        }
        seed.query_async::<()>(&mut connection)
            .await
            .expect("seed target page");
    }
    connection
        .set::<_, _, ()>(&customers_key, r#"{"id":"customer-1"}"#)
        .await
        .expect("seed unrelated target");

    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-truncate-test",
            &url,
            &prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive),
    )
    .await
    .expect("sink");
    sink.write(SinkBatch::new(vec![
        event(
            "insert",
            r#"public.orders:["before"]"#,
            r#"{"id":"before"}"#,
        ),
        truncate_event("public.orders"),
        event("insert", r#"public.orders:["after"]"#, r#"{"id":"after"}"#),
    ]))
    .await
    .expect("ordered truncate");

    let order_keys = matching_keys(&mut connection, &orders_pattern).await;
    assert_eq!(order_keys.len(), 1, "truncate left stale order keys");
    assert!(
        order_keys[0].ends_with("public.orders%3A%5B%22after%22%5D"),
        "post-truncate write was not preserved: {order_keys:?}"
    );
    let customer_exists: bool = connection
        .exists(&customers_key)
        .await
        .expect("unrelated target exists");
    assert!(customer_exists, "truncate crossed the target boundary");
}

#[tokio::test]
async fn exclusive_reset_is_deduplicated_and_target_scoped() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:reset:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let orders_key = format!("{prefix}:{{orders}}:order-1");
    let customers_key = format!("{prefix}:{{customers}}:customer-1");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    connection
        .set::<_, _, ()>(&orders_key, r#"{"id":"order-1"}"#)
        .await
        .expect("seed owned target");
    connection
        .set::<_, _, ()>(&customers_key, r#"{"id":"customer-1"}"#)
        .await
        .expect("seed neighboring target");

    let config = RedisConfig::new(
        "redis-reset-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive);
    RedisSink::reset_owned_targets(config, &["orders".to_owned(), "orders".to_owned()])
        .await
        .expect("exclusive reset");

    assert_eq!(
        connection.exists::<_, bool>(&orders_key).await,
        Ok(false),
        "owned target was not cleared"
    );
    assert_eq!(
        connection.exists::<_, bool>(&customers_key).await,
        Ok(true),
        "reset crossed the target boundary"
    );
}

#[tokio::test]
async fn shared_reset_is_refused_before_connecting() {
    let config = RedisConfig::new(
        "redis-shared-reset-test",
        "redis://127.0.0.1:1",
        "ventstream:test",
        RedisKeyRouting::Fixed("orders".to_owned()),
    );
    let result = RedisSink::reset_owned_targets(config, &["orders".to_owned()]).await;

    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("ownership=exclusive")
    ));
}

#[tokio::test]
async fn invalid_reset_target_cannot_partially_clear_valid_targets() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:reset-validation:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let orders_key = format!("{prefix}:{{orders}}:order-1");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    connection
        .set::<_, _, ()>(&orders_key, r#"{"id":"order-1"}"#)
        .await
        .expect("seed valid target");

    let config = RedisConfig::new(
        "redis-reset-validation-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive);
    let result =
        RedisSink::reset_owned_targets(config, &["orders".to_owned(), "bad\nname".to_owned()])
            .await;

    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.contains("contain no control characters")
    ));
    assert_eq!(
        connection.exists::<_, bool>(&orders_key).await,
        Ok(true),
        "validation failure caused a partial reset"
    );
}

#[tokio::test]
async fn invalid_json_is_an_exact_item_rejection() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let mut config = RedisConfig::new(
        "redis-json-test",
        url,
        "ventstream:test:json",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_document_format(RedisDocumentFormat::Json);
    config.retry = RetryConfig {
        max_attempts: 1,
        ..RetryConfig::default()
    };
    let sink = RedisSink::connect(config).await.expect("sink");
    let error = sink
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["bad"]"#,
            "{",
        )]))
        .await
        .expect_err("invalid JSON");
    assert!(matches!(
        error,
        ventstream_core::SinkError::Rejected {
            rejected_count: 1,
            failed_items: Some(ref items),
            ..
        } if items.len() == 1 && items[0].offset == 0
    ));
}

#[tokio::test]
async fn redis_json_sink_upserts_and_deletes() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:json:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22json%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-json-test",
            &url,
            prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_document_format(RedisDocumentFormat::Json),
    )
    .await
    .expect("RedisJSON sink");

    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["json"]"#,
        r#"{"id":"json","total":42}"#,
    )]))
    .await
    .expect("JSON.SET");
    let stored = redis::cmd("JSON.GET")
        .arg(&key)
        .query_async::<String>(&mut connection)
        .await
        .expect("JSON.GET");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
        serde_json::json!({"id": "json", "total": 42})
    );

    sink.write(SinkBatch::new(vec![event(
        "delete",
        r#"public.orders:["json"]"#,
        "{}",
    )]))
    .await
    .expect("JSON delete");
    let exists: bool = connection.exists(&key).await.expect("exists after delete");
    assert!(!exists);
}

#[tokio::test]
async fn redis_json_wrong_type_rejects_only_the_conflicting_event() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:json-wrong-type:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let conflicting_key =
        format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22conflict%22%5D");
    let valid_key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22valid%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    connection
        .set::<_, _, ()>(&conflicting_key, "owned-by-another-type")
        .await
        .expect("seed conflicting Redis type");

    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-json-wrong-type-test",
            &url,
            prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_document_format(RedisDocumentFormat::Json),
    )
    .await
    .expect("RedisJSON sink");
    let error = sink
        .write(SinkBatch::new(vec![
            event(
                "insert",
                r#"public.orders:["invalid"]"#,
                r#"{"id":"invalid""#,
            ),
            event(
                "insert",
                r#"public.orders:["conflict"]"#,
                r#"{"id":"conflict"}"#,
            ),
            event("insert", r#"public.orders:["valid"]"#, r#"{"id":"valid"}"#),
        ]))
        .await
        .expect_err("wrong Redis type must reject one event");

    assert!(
        matches!(
        &error,
        ventstream_core::SinkError::Rejected {
            batch_size: 3,
            rejected_count: 2,
            failed_items: Some(items),
            ..
        } if items.len() == 2
            && items[0].offset == 0
            && items[0].error.contains("not valid JSON")
            && items[1].offset == 1
            && items[1].error.contains("wrong Redis type")
        ),
        "unexpected RedisJSON conflict result: {error:?}"
    );
    assert_eq!(
        connection.get::<_, String>(&conflicting_key).await,
        Ok("owned-by-another-type".to_owned())
    );
    let stored = redis::cmd("JSON.GET")
        .arg(&valid_key)
        .query_async::<String>(&mut connection)
        .await
        .expect("read successful event");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
        serde_json::json!({"id": "valid"})
    );
}

#[tokio::test]
async fn redis_json_cache_refreshes_expiration_with_the_value() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:json-cache:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22cache%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-json-cache-test",
            &url,
            prefix,
            RedisKeyRouting::ByOutputRelation,
        )
        .with_document_format(RedisDocumentFormat::Json)
        .with_contract(RedisContract::Cache {
            ttl: Duration::from_millis(500),
        }),
    )
    .await
    .expect("RedisJSON cache sink");

    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["cache"]"#,
        r#"{"id":"cache","version":1}"#,
    )]))
    .await
    .expect("initial cache write");
    tokio::time::sleep(Duration::from_millis(175)).await;
    sink.write(SinkBatch::new(vec![event(
        "update",
        r#"public.orders:["cache"]"#,
        r#"{"id":"cache","version":2}"#,
    )]))
    .await
    .expect("cache refresh");

    let ttl = redis::cmd("PTTL")
        .arg(&key)
        .query_async::<i64>(&mut connection)
        .await
        .expect("PTTL after refresh");
    assert!(ttl > 300 && ttl <= 500, "unexpected refreshed TTL: {ttl}");
    let stored = redis::cmd("JSON.GET")
        .arg(&key)
        .query_async::<String>(&mut connection)
        .await
        .expect("JSON.GET refreshed value");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
        serde_json::json!({"id": "cache", "version": 2})
    );

    tokio::time::sleep(Duration::from_millis(550)).await;
    let exists: bool = connection.exists(&key).await.expect("exists after TTL");
    assert!(!exists);
}

#[tokio::test]
async fn redis_json_cache_keeps_the_visible_value_unchanged_when_expiry_is_denied() {
    let Some(url) = redis_json_url() else {
        return;
    };
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let prefix = format!("ventstream:test:json-cache-acl:{suffix}");
    let username = format!("ventstream_json_cache_no_expiry_{suffix}");
    let password = "ventstream-json-cache-no-expiry";
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22cache-acl%22%5D");
    let staging_pattern = format!("{prefix}:__ventstream:stage:{{public.orders}}:*");
    let mut admin = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{password}"))
        .arg(format!("~{prefix}:*"))
        .arg("+ping")
        .arg("+get")
        .arg("+set")
        .arg("+del")
        .arg("+evalsha")
        .arg("+script|load")
        .arg("+type")
        .arg("+json.set")
        .arg("+rename")
        .query_async::<()>(&mut admin)
        .await
        .expect("create RedisJSON cache ACL user");
    redis::cmd("JSON.SET")
        .arg(&key)
        .arg("$")
        .arg(r#"{"id":"cache-acl","version":1}"#)
        .query_async::<String>(&mut admin)
        .await
        .expect("seed visible RedisJSON cache value");
    redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(60_000)
        .query_async::<bool>(&mut admin)
        .await
        .expect("expire seeded RedisJSON value");

    let mut config = RedisConfig::new(
        "redis-json-cache-acl-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_auth(Some(username.clone()), Some(password.to_owned()))
    .with_document_format(RedisDocumentFormat::Json)
    .with_contract(RedisContract::Cache {
        ttl: Duration::from_secs(60),
    });
    config.retry.max_attempts = 1;
    let sink = RedisSink::connect(config)
        .await
        .expect("restricted RedisJSON cache sink");
    let result = sink
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["cache-acl"]"#,
            r#"{"id":"cache-acl","version":2}"#,
        )]))
        .await;

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove RedisJSON cache ACL user");
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("permission")
                || reason.to_ascii_lowercase().contains("noperm")
    ));
    let stored = redis::cmd("JSON.GET")
        .arg(&key)
        .query_async::<String>(&mut admin)
        .await
        .expect("read visible RedisJSON cache value");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).expect("stored JSON"),
        serde_json::json!({"id": "cache-acl", "version": 1})
    );
    let ttl = redis::cmd("PTTL")
        .arg(&key)
        .query_async::<i64>(&mut admin)
        .await
        .expect("read visible RedisJSON cache TTL");
    assert!(ttl > 0, "the existing cache TTL was removed");
    assert!(
        matching_keys(&mut admin, &staging_pattern).await.is_empty(),
        "failed RedisJSON cache write left a staging key"
    );
}

#[tokio::test]
async fn redis_json_mode_blocks_when_the_module_is_unavailable() {
    let Some(url) = redis_url() else {
        return;
    };
    let result = RedisSink::connect(
        RedisConfig::new(
            "redis-json-module-test",
            url,
            "ventstream:test:json-module",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_document_format(RedisDocumentFormat::Json),
    )
    .await;
    let Err(error) = result else {
        panic!("plain Redis must reject JSON.SET at startup");
    };
    assert!(matches!(error, ventstream_core::SinkError::Blocked(_)));
}

#[tokio::test]
async fn password_auth_is_not_required_in_the_endpoint_url() {
    let Some((url, password)) = authenticated_redis() else {
        return;
    };
    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-auth-test",
            &url,
            "ventstream:test:auth",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_auth(None, Some(password)),
    )
    .await
    .expect("authenticated sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["auth"]"#,
        r#"{"id":"auth"}"#,
    )]))
    .await
    .expect("authenticated write");
}

#[tokio::test]
async fn mounted_password_rotation_reconnects_without_restarting_the_sink() {
    let Some((url, admin_password)) = authenticated_redis() else {
        return;
    };
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let username = format!("ventstream_rotate_{suffix}");
    let first_password = "ventstream-rotation-first";
    let second_password = "ventstream-rotation-second";
    let prefix = format!("ventstream:test:rotation:{suffix}");
    let directory = std::env::temp_dir().join(format!("ventstream-redis-rotation-{suffix}"));
    std::fs::create_dir_all(&directory).expect("create credential directory");
    let password_file = directory.join("password");
    std::fs::write(&password_file, format!("{first_password}\n"))
        .expect("write initial credential");

    let mut admin = authenticated_connection(&url, &admin_password).await;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{first_password}"))
        .arg("~*")
        .arg("+@all")
        .query_async::<()>(&mut admin)
        .await
        .expect("create rotation ACL user");

    let mut config = RedisConfig::new(
        "redis-credential-rotation-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_auth_sources(
        Some(username.clone()),
        None,
        None,
        Some(password_file.clone()),
    );
    config.retry.max_attempts = 20;
    let sink = RedisSink::connect(config)
        .await
        .expect("connect with mounted credential");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["rotation-before"]"#,
        r#"{"id":"rotation-before"}"#,
    )]))
    .await
    .expect("write before credential rotation");

    let replacement = directory.join("password.next");
    std::fs::write(&replacement, format!("{second_password}\n"))
        .expect("write replacement credential");
    std::fs::rename(&replacement, &password_file).expect("replace mounted credential atomically");
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("resetpass")
        .arg(format!(">{second_password}"))
        .query_async::<()>(&mut admin)
        .await
        .expect("rotate Redis ACL password");
    redis::cmd("CLIENT")
        .arg("KILL")
        .arg("USER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("disconnect old authenticated clients");

    tokio::time::sleep(Duration::from_secs(6)).await;
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["rotation-after"]"#,
        r#"{"id":"rotation-after"}"#,
    )]))
    .await
    .expect("write after credential rotation");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove rotation ACL user");
    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test]
async fn incorrect_password_blocks_without_retrying_forever() {
    let Some((url, _)) = authenticated_redis() else {
        return;
    };
    let mut config = RedisConfig::new(
        "redis-invalid-auth-test",
        &url,
        "ventstream:test:invalid-auth",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_auth(None, Some("incorrect-password".to_owned()));
    config.retry.max_attempts = 0;

    let result = tokio::time::timeout(Duration::from_secs(2), RedisSink::connect(config))
        .await
        .expect("authentication failure must not retry indefinitely");
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("auth")
    ));
}

#[tokio::test]
async fn acl_write_denial_blocks_delivery_without_retrying() {
    let Some((url, admin_password)) = authenticated_redis() else {
        return;
    };
    let username = format!(
        "ventstream_no_write_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let user_password = "ventstream-no-write-test";
    let mut admin = authenticated_connection(&url, &admin_password).await;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{user_password}"))
        .arg("~*")
        .arg("+ping")
        .query_async::<()>(&mut admin)
        .await
        .expect("create restricted ACL user");

    let health = SinkHealth::new();
    let mut config = RedisConfig::new(
        "redis-acl-denied-test",
        &url,
        "ventstream:test:acl-denied",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_auth(Some(username.clone()), Some(user_password.to_owned()))
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_delivery_health(health.clone());
    config.retry.max_attempts = 0;
    let sink = RedisSink::connect(config)
        .await
        .expect("restricted user can connect");
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        sink.write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["acl-denied"]"#,
            r#"{"id":"acl-denied"}"#,
        )])),
    )
    .await
    .expect("ACL denial must not retry indefinitely");

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove restricted ACL user");
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("permission")
                || reason.to_ascii_lowercase().contains("noperm")
    ));
    assert!(matches!(
        health.snapshot(),
        SinkHealthSnapshot::Blocked { .. }
    ));
}

#[tokio::test]
async fn acl_without_keyspace_cleanup_permissions_blocks_truncate() {
    let Some((url, admin_password)) = authenticated_redis() else {
        return;
    };
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let username = format!("ventstream_no_clear_{suffix}");
    let prefix = format!("ventstream:test:acl-clear:{suffix}");
    let user_password = "ventstream-no-clear-test";
    let mut admin = authenticated_connection(&url, &admin_password).await;
    redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&username)
        .arg("reset")
        .arg("on")
        .arg(format!(">{user_password}"))
        .arg(format!("~{prefix}:*"))
        .arg("+ping")
        .arg("+evalsha")
        .arg("+script|load")
        .arg("+get")
        .arg("+set")
        .arg("+del")
        .arg("+pexpire")
        .query_async::<()>(&mut admin)
        .await
        .expect("create point-write-only ACL user");

    let health = SinkHealth::new();
    let mut config = RedisConfig::new(
        "redis-acl-clear-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_auth(Some(username.clone()), Some(user_password.to_owned()))
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_delivery_health(health.clone());
    config.retry.max_attempts = 0;
    let sink = RedisSink::connect(config)
        .await
        .expect("point-write-only user can connect");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["acl-clear"]"#,
        r#"{"id":"acl-clear"}"#,
    )]))
    .await
    .expect("point write");
    let result = sink
        .write(SinkBatch::new(vec![truncate_event("public.orders")]))
        .await;

    redis::cmd("ACL")
        .arg("DELUSER")
        .arg(&username)
        .query_async::<usize>(&mut admin)
        .await
        .expect("remove restricted ACL user");
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("permission")
                || reason.to_ascii_lowercase().contains("noperm")
    ));
    assert!(matches!(
        health.snapshot(),
        SinkHealthSnapshot::Blocked { .. }
    ));
}

#[tokio::test]
async fn active_writer_lease_blocks_duplicates_and_expires_after_owner_stops() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:writer-lease:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22leased%22%5D");
    let mut first_config = RedisConfig::new(
        "redis-writer-lease-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_writer_id("lease-owner-a")
    .with_writer_lease(Duration::from_secs(3));
    first_config.retry.max_attempts = 1;
    let first = RedisSink::connect(first_config.clone())
        .await
        .expect("first writer");
    first
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["leased"]"#,
            r#"{"id":"leased","writer":"first"}"#,
        )]))
        .await
        .expect("first writer claims lease");

    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let mut second_config = first_config.with_writer_id("lease-owner-b");
    second_config.retry.max_attempts = 1;
    let second = RedisSink::connect(second_config)
        .await
        .expect("duplicate process connects without claiming");
    let error = second
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["leased"]"#,
            r#"{"id":"leased","writer":"duplicate"}"#,
        )]))
        .await
        .expect_err("active writer lease must reject a duplicate");
    assert!(matches!(
        error,
        ventstream_core::SinkError::Connection(reason)
            if reason.contains("owned by another active VentStream writer")
    ));

    drop(first);
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    second
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["leased"]"#,
            r#"{"id":"leased","writer":"replacement"}"#,
        )]))
        .await
        .expect("replacement claims expired lease");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(
        connection.get::<_, String>(key).await,
        Ok(r#"{"id":"leased","writer":"replacement"}"#.to_owned())
    );
}

#[tokio::test]
async fn newer_writer_fences_stale_mutations_per_target() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:fencing:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let order_key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22fenced%22%5D");
    let customer_key =
        format!("{prefix}:{{public.customers}}:public.customers%3A%5B%22independent%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut config = RedisConfig::new(
        "redis-fencing-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_writer_id("orders-revision-a");
    config.retry.max_attempts = 1;
    let first = RedisSink::connect(config.clone())
        .await
        .expect("first writer");
    let second = RedisSink::connect(
        config
            .clone()
            .with_writer_id("orders-revision-b")
            .with_writer_takeover_from("orders-revision-a"),
    )
    .await
    .expect("second writer");

    first
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["fenced"]"#,
            r#"{"id":"fenced","writer":"first"}"#,
        )]))
        .await
        .expect("first writer claims target");

    let impostor = RedisSink::connect(
        config
            .clone()
            .with_writer_id("orders-revision-impostor")
            .with_writer_takeover_from("unexpected-revision"),
    )
    .await
    .expect("impostor connects without claiming a target");
    let error = impostor
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["fenced"]"#,
            r#"{"id":"fenced","writer":"impostor"}"#,
        )]))
        .await
        .expect_err("a mismatched handoff must not replace the current writer");
    assert!(matches!(
        error,
        ventstream_core::SinkError::Connection(reason)
            if reason.contains("owned by another active VentStream writer")
    ));

    second
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["fenced"]"#,
            r#"{"id":"fenced","writer":"second"}"#,
        )]))
        .await
        .expect("replacement writer claims target");

    for stale_event in [
        event(
            "update",
            r#"public.orders:["fenced"]"#,
            r#"{"id":"fenced","writer":"stale"}"#,
        ),
        event("delete", r#"public.orders:["fenced"]"#, "{}"),
        truncate_event("public.orders"),
    ] {
        let error = first
            .write(SinkBatch::new(vec![stale_event]))
            .await
            .expect_err("stale writer must be fenced");
        assert!(
            matches!(
                error,
                ventstream_core::SinkError::Connection(ref reason)
                    if reason.contains("fenced by a newer VentStream process")
            ),
            "unexpected stale-writer error: {error:?}"
        );
    }
    let stored: String = connection.get(&order_key).await.expect("replacement value");
    assert_eq!(stored, r#"{"id":"fenced","writer":"second"}"#);

    let mut customer_event = event(
        "insert",
        r#"public.customers:["independent"]"#,
        r#"{"id":"independent","writer":"first"}"#,
    );
    customer_event.subject =
        Subject::new("postgres.public.customers.insert").expect("customer subject");
    customer_event.headers = customer_event.headers.clone().with_header(
        "ventstream.cdc.relation".to_owned(),
        "public.customers".to_owned(),
    );
    first
        .write(SinkBatch::new(vec![customer_event]))
        .await
        .expect("uncontested target remains writable");
    let customer: String = connection
        .get(&customer_key)
        .await
        .expect("independent target value");
    assert_eq!(customer, r#"{"id":"independent","writer":"first"}"#);
}

#[tokio::test]
async fn newer_writer_fences_stale_lookup_view_manifests_and_values_atomically() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:view-fencing:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut config = lookup_view_config(&url, &prefix, RedisContract::MaterializedView);
    config.writer_id = "view-revision-a".to_owned();
    config.retry.max_attempts = 1;
    let first = RedisSink::connect(config.clone())
        .await
        .expect("first view writer");
    let second = RedisSink::connect(
        config
            .with_writer_id("view-revision-b")
            .with_writer_takeover_from("view-revision-a"),
    )
    .await
    .expect("replacement view writer");

    first
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["source-1"]"#,
            r#"{"id":"first","customer_id":"customer-1","status":"pending"}"#,
        )]))
        .await
        .expect("first writer claims view targets");
    second
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["source-1"]"#,
            r#"{"id":"second","customer_id":"customer-2","status":"processing"}"#,
        )]))
        .await
        .expect("replacement writer claims view targets");

    let error = first
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["source-1"]"#,
            r#"{"id":"stale","customer_id":"customer-3","status":"pending"}"#,
        )]))
        .await
        .expect_err("stale view writer must be fenced");
    assert!(
        matches!(
            error,
            ventstream_core::SinkError::Connection(ref reason)
                if reason.contains("fenced by a newer VentStream process")
        ),
        "unexpected stale-writer error: {error:?}"
    );

    let second_order_key = format!("{prefix}:{{open_order_by_id}}:order%3Asecond");
    let second_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-2%3Aorder%3Asecond");
    let stale_order_key = format!("{prefix}:{{open_order_by_id}}:order%3Astale");
    let stale_customer_key =
        format!("{prefix}:{{order_by_customer}}:customer%3Acustomer-3%3Aorder%3Astale");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    assert_eq!(
        connection.get::<_, String>(&second_order_key).await,
        Ok(r#"{"id":"second","status":"processing"}"#.to_owned())
    );
    assert_eq!(
        connection.get::<_, String>(&second_customer_key).await,
        Ok(r#"{"id":"second","customer_id":"customer-2","status":"processing"}"#.to_owned())
    );
    assert_eq!(
        connection.exists::<_, bool>(&stale_order_key).await,
        Ok(false)
    );
    assert_eq!(
        connection.exists::<_, bool>(&stale_customer_key).await,
        Ok(false)
    );
}

#[tokio::test]
async fn stale_multi_target_batch_is_rejected_before_any_target_is_mutated() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:fencing:atomic:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let order_key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22order-1%22%5D");
    let customer_key =
        format!("{prefix}:{{public.customers}}:public.customers%3A%5B%22customer-1%22%5D");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut config = RedisConfig::new(
        "redis-atomic-fencing-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_writer_id("atomic-revision-a");
    config.retry.max_attempts = 1;
    let first = RedisSink::connect(config.clone())
        .await
        .expect("first writer");
    let second = RedisSink::connect(
        config
            .with_writer_id("atomic-revision-b")
            .with_writer_takeover_from("atomic-revision-a"),
    )
    .await
    .expect("second writer");

    let mut first_customer = event(
        "insert",
        r#"public.customers:["customer-1"]"#,
        r#"{"id":"customer-1","writer":"first"}"#,
    );
    first_customer.subject =
        Subject::new("postgres.public.customers.insert").expect("customer subject");
    first_customer.headers = first_customer.headers.clone().with_header(
        "ventstream.cdc.relation".to_owned(),
        "public.customers".to_owned(),
    );
    first
        .write(SinkBatch::new(vec![
            event(
                "insert",
                r#"public.orders:["order-1"]"#,
                r#"{"id":"order-1","writer":"first"}"#,
            ),
            first_customer,
        ]))
        .await
        .expect("first writer claims both targets");
    second
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["order-1"]"#,
            r#"{"id":"order-1","writer":"second"}"#,
        )]))
        .await
        .expect("replacement writer fences the order target");

    let mut stale_customer = event(
        "update",
        r#"public.customers:["customer-1"]"#,
        r#"{"id":"customer-1","writer":"stale"}"#,
    );
    stale_customer.subject =
        Subject::new("postgres.public.customers.update").expect("customer subject");
    stale_customer.headers = stale_customer.headers.clone().with_header(
        "ventstream.cdc.relation".to_owned(),
        "public.customers".to_owned(),
    );
    let error = first
        .write(SinkBatch::new(vec![
            event(
                "update",
                r#"public.orders:["order-1"]"#,
                r#"{"id":"order-1","writer":"stale"}"#,
            ),
            stale_customer,
        ]))
        .await
        .expect_err("one stale target must reject the entire batch");
    assert!(
        matches!(
            error,
            ventstream_core::SinkError::Connection(ref reason)
                if reason.contains("fenced by a newer VentStream process")
        ),
        "unexpected stale-writer error: {error:?}"
    );

    let order: String = connection.get(&order_key).await.expect("order value");
    let customer: String = connection.get(&customer_key).await.expect("customer value");
    assert_eq!(order, r#"{"id":"order-1","writer":"second"}"#);
    assert_eq!(customer, r#"{"id":"customer-1","writer":"first"}"#);
}

#[tokio::test]
async fn missing_writer_fence_recovers_only_while_the_writer_lineage_is_current() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:fence-loss:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{public.orders}}:public.orders%3A%5B%22fence-loss%22%5D");
    let fence_key = format!("{prefix}:__ventstream:writer:{{public.orders}}:current");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut config = RedisConfig::new(
        "redis-fence-loss-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_writer_id("revision-a");
    config.retry.max_attempts = 1;
    let first = RedisSink::connect(config.clone())
        .await
        .expect("first writer");
    first
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["fence-loss"]"#,
            r#"{"id":"fence-loss","revision":1}"#,
        )]))
        .await
        .expect("initial write");

    redis::cmd("UNLINK")
        .arg(&fence_key)
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove writer fence");
    first
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["fence-loss"]"#,
            r#"{"id":"fence-loss","revision":2}"#,
        )]))
        .await
        .expect("current writer recovers an expired lease");
    let recovered: String = connection.get(&key).await.expect("recovered value");
    assert_eq!(recovered, r#"{"id":"fence-loss","revision":2}"#);

    let replacement = RedisSink::connect(
        config
            .clone()
            .with_writer_id("revision-b")
            .with_writer_takeover_from("revision-a"),
    )
    .await
    .expect("replacement writer");
    replacement
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["fence-loss"]"#,
            r#"{"id":"fence-loss","revision":3}"#,
        )]))
        .await
        .expect("replacement takes over the live writer");

    redis::cmd("UNLINK")
        .arg(&fence_key)
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove replacement writer fence");
    let error = first
        .write(SinkBatch::new(vec![event(
            "update",
            r#"public.orders:["fence-loss"]"#,
            r#"{"id":"fence-loss","revision":4}"#,
        )]))
        .await
        .expect_err("superseded writer must not recover the replacement lineage");
    assert!(matches!(
        error,
        ventstream_core::SinkError::Connection(reason)
            if reason.contains("fenced by a newer VentStream process")
    ));
    let unchanged: String = connection.get(&key).await.expect("replacement value");
    assert_eq!(unchanged, r#"{"id":"fence-loss","revision":3}"#);
}

#[tokio::test]
async fn a_lost_target_fence_does_not_expire_unrelated_idle_targets() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:fence-independence:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let stale_target = "a_stale";
    let live_target = "z_live";
    let stale_fence = format!("{prefix}:__ventstream:writer:{{{stale_target}}}:current");
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut owner_config = RedisConfig::new(
        "redis-independent-fences-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_writer_id("revision-a")
    .with_writer_lease(Duration::from_secs(3));
    owner_config.retry.max_attempts = 1;
    let owner = RedisSink::connect(owner_config.clone())
        .await
        .expect("owner");

    owner
        .write(SinkBatch::new(vec![
            event_for_relation(stale_target, "insert", "stale:1", r#"{"id":1}"#),
            event_for_relation(live_target, "insert", "live:1", r#"{"id":1}"#),
        ]))
        .await
        .expect("claim both targets");
    redis::cmd("UNLINK")
        .arg(stale_fence)
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove one target fence");

    tokio::time::sleep(Duration::from_millis(3_500)).await;

    let contender = RedisSink::connect(owner_config.with_writer_id("revision-b"))
        .await
        .expect("contender");
    let error = contender
        .write(SinkBatch::new(vec![event_for_relation(
            live_target,
            "update",
            "live:1",
            r#"{"id":1,"writer":"contender"}"#,
        )]))
        .await
        .expect_err("the unrelated live target must remain leased");
    assert!(matches!(
        error,
        ventstream_core::SinkError::Connection(reason)
            if reason.contains("owned by another active VentStream writer")
    ));
}

#[tokio::test]
async fn custom_ca_and_mutual_tls_are_supported() {
    let Some((url, ca_file, client_cert_file, client_key_file)) = redis_mtls() else {
        return;
    };
    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-mtls-test",
            &url,
            "ventstream:test:mtls",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_tls(RedisTlsConfig {
            ca_file: Some(ca_file),
            client_cert_file: Some(client_cert_file),
            client_key_file: Some(client_key_file),
        }),
    )
    .await
    .expect("mutual TLS sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["mtls"]"#,
        r#"{"id":"mtls","status":"secured"}"#,
    )]))
    .await
    .expect("mutual TLS write");
}

#[tokio::test]
async fn unmet_replica_acknowledgement_fails_closed() {
    let Some(url) = redis_url() else {
        return;
    };
    let mut config = RedisConfig::new(
        "redis-ack-test",
        url,
        "ventstream:test:ack",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_acknowledgement(RedisAcknowledgement::Replicated {
        replicas: 1,
        timeout: Duration::from_millis(25),
    });
    config.retry = RetryConfig {
        max_attempts: 1,
        ..RetryConfig::default()
    };
    let sink = RedisSink::connect(config).await.expect("sink");
    let error = sink
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["ack"]"#,
            r#"{"id":"ack"}"#,
        )]))
        .await
        .expect_err("standalone Redis has no replica acknowledgement");
    assert!(matches!(error, ventstream_core::SinkError::Connection(_)));
}

#[tokio::test]
async fn replicated_acknowledgement_advances_after_replica_confirmation() {
    let Some(url) = replicated_redis_url() else {
        return;
    };
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let info = redis::cmd("INFO")
                .arg("replication")
                .query_async::<String>(&mut connection)
                .await
                .expect("replication info");
            if info.lines().any(|line| line.trim() == "connected_slaves:1") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("replica connection");

    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-replicated-test",
            url,
            "ventstream:test:replicated",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_acknowledgement(RedisAcknowledgement::Replicated {
            replicas: 1,
            timeout: Duration::from_secs(2),
        }),
    )
    .await
    .expect("replicated sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["replicated"]"#,
        r#"{"id":"replicated","status":"confirmed"}"#,
    )]))
    .await
    .expect("replica-acknowledged write");
}

#[tokio::test]
async fn aof_acknowledgement_requires_enabled_persistence() {
    let Some(url) = redis_url() else {
        return;
    };
    let sink = RedisSink::connect(
        RedisConfig::new(
            "redis-aof-disabled-test",
            url,
            "ventstream:test:aof-disabled",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_acknowledgement(RedisAcknowledgement::Aof {
            local: true,
            replicas: 0,
            timeout: Duration::from_secs(2),
        }),
    )
    .await
    .expect("sink connects before its first acknowledgement");
    let result = sink
        .write(SinkBatch::new(vec![event(
            "insert",
            r#"public.orders:["aof-disabled"]"#,
            r#"{"id":"aof-disabled"}"#,
        )]))
        .await;
    assert!(matches!(
        result,
        Err(ventstream_core::SinkError::Blocked(reason))
            if reason.to_ascii_lowercase().contains("aof")
    ));
}

#[tokio::test]
async fn aof_acknowledgement_confirms_local_and_replica_fsync() {
    let Some(url) = aof_redis_url() else {
        return;
    };
    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let info = redis::cmd("INFO")
                .arg("replication")
                .query_async::<String>(&mut connection)
                .await
                .expect("replication info");
            if info.lines().any(|line| line.trim() == "connected_slaves:1") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("AOF replica connection");

    let config = RedisConfig::new(
        "redis-aof-test",
        &url,
        "ventstream:test:aof",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_acknowledgement(RedisAcknowledgement::Aof {
        local: true,
        replicas: 1,
        timeout: Duration::from_secs(3),
    });
    let report = RedisSink::diagnose(config.clone())
        .await
        .expect("AOF diagnostic");
    assert!(report.required_local_aof);
    assert_eq!(report.required_replica_acks, 1);
    assert_eq!(report.observed_local_aof_acks, Some(1));
    assert!(report.observed_replica_acks.is_some_and(|acks| acks >= 1));

    let sink = RedisSink::connect(config).await.expect("AOF sink");
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["aof"]"#,
        r#"{"id":"aof","status":"durable"}"#,
    )]))
    .await
    .expect("AOF-acknowledged write");
}

#[tokio::test]
async fn managed_endpoint_recovers_across_primary_failover() {
    let Some((url, primary, replica, replica_url)) = redis_failover() else {
        return;
    };
    let _topology = FailoverTopologyGuard::acquire(primary.clone(), replica.clone());
    let health = SinkHealth::new();
    let mut config = RedisConfig::new(
        "redis-failover-test",
        &url,
        "ventstream:test:failover",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_delivery_health(health.clone());
    config.retry = RetryConfig {
        max_attempts: 0,
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        backoff_factor: 2.0,
    };
    let sink = Arc::new(RedisSink::connect(config).await.expect("sink"));
    sink.write(SinkBatch::new(vec![event(
        "insert",
        r#"public.orders:["failover"]"#,
        r#"{"id":"failover","status":"created"}"#,
    )]))
    .await
    .expect("initial primary write");

    let output = Command::new("docker")
        .args(["stop", "-t", "0", &primary])
        .output()
        .expect("stop Redis primary");
    assert!(output.status.success());

    let write = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event(
                "update",
                r#"public.orders:["failover"]"#,
                r#"{"id":"failover","status":"promoted"}"#,
            )]))
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !write.is_finished(),
        "write must remain pending while the failover target is read-only"
    );
    assert!(matches!(
        health.snapshot(),
        SinkHealthSnapshot::Degraded { .. }
    ));

    let output = Command::new("docker")
        .args(["exec", &replica, "redis-cli", "REPLICAOF", "NO", "ONE"])
        .output()
        .expect("promote Redis replica");
    assert!(output.status.success());
    tokio::time::timeout(Duration::from_secs(10), write)
        .await
        .expect("failover recovery timeout")
        .expect("write task")
        .expect("write after promotion");
    assert_eq!(health.snapshot(), SinkHealthSnapshot::Healthy);

    let mut promoted = redis::Client::open(replica_url)
        .expect("promoted client")
        .get_connection_manager()
        .await
        .expect("promoted connection");
    let value = redis::cmd("GET")
        .arg("ventstream:test:failover:{public.orders}:public.orders%3A%5B%22failover%22%5D")
        .query_async::<Vec<u8>>(&mut promoted)
        .await
        .expect("promoted value");
    assert_eq!(value, br#"{"id":"failover","status":"promoted"}"#.to_vec());
}

#[tokio::test]
async fn write_recovers_after_redis_restarts() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(container) = std::env::var("VS_TEST_REDIS_RESTART_CONTAINER") else {
        return;
    };
    let _container_guard = RestartContainerGuard::new(container.clone());
    let mut config = RedisConfig::new(
        "redis-restart-test",
        url,
        "ventstream:test:restart",
        RedisKeyRouting::ByOutputRelation,
    );
    config.retry = RetryConfig {
        max_attempts: 40,
        initial_backoff: Duration::from_millis(25),
        max_backoff: Duration::from_millis(100),
        backoff_factor: 2.0,
    };
    let sink = Arc::new(RedisSink::connect(config).await.expect("sink"));

    let output = Command::new("docker")
        .args(["stop", "-t", "0", &container])
        .output()
        .expect("stop Redis container");
    assert!(output.status.success());

    let write = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event(
                "insert",
                r#"public.orders:["restart"]"#,
                r#"{"id":"restart"}"#,
            )]))
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let output = Command::new("docker")
        .args(["start", &container])
        .output()
        .expect("restart Redis container");
    assert!(output.status.success());

    tokio::time::timeout(Duration::from_secs(15), write)
        .await
        .expect("write recovery timeout")
        .expect("write task")
        .expect("write after recovery");
}

#[tokio::test]
async fn truncate_backpressures_and_recovers_after_a_transient_outage() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(container) = std::env::var("VS_TEST_REDIS_RESTART_CONTAINER") else {
        return;
    };
    let _container_guard = RestartContainerGuard::new(container.clone());
    let suffix = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let prefix = format!("ventstream:test:truncate-recovery:{suffix}");
    let health = SinkHealth::new();
    let mut config = RedisConfig::new(
        "redis-truncate-recovery-test",
        &url,
        &prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_delivery_health(health.clone());
    config.response_timeout = Duration::from_millis(100);
    config.retry = RetryConfig {
        max_attempts: 40,
        initial_backoff: Duration::from_millis(25),
        max_backoff: Duration::from_millis(100),
        backoff_factor: 2.0,
    };
    let sink = Arc::new(RedisSink::connect(config).await.expect("sink"));
    sink.write(SinkBatch::new(vec![
        event(
            "insert",
            r#"public.orders:["truncate-recovery-1"]"#,
            r#"{"id":"truncate-recovery-1"}"#,
        ),
        event(
            "insert",
            r#"public.orders:["truncate-recovery-2"]"#,
            r#"{"id":"truncate-recovery-2"}"#,
        ),
    ]))
    .await
    .expect("seed truncate target");

    let output = Command::new("docker")
        .args(["pause", &container])
        .output()
        .expect("pause Redis container");
    assert!(output.status.success());
    let clear = {
        let sink = Arc::clone(&sink);
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![truncate_event("public.orders")]))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !clear.is_finished(),
        "truncate must wait for Redis recovery"
    );
    assert!(matches!(
        health.snapshot(),
        SinkHealthSnapshot::Degraded { .. }
    ));

    let output = Command::new("docker")
        .args(["unpause", &container])
        .output()
        .expect("unpause Redis container");
    assert!(output.status.success());
    tokio::time::timeout(Duration::from_secs(15), clear)
        .await
        .expect("truncate recovery timeout")
        .expect("truncate task")
        .expect("truncate after recovery");
    assert_eq!(health.snapshot(), SinkHealthSnapshot::Healthy);

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let keys = matching_keys(&mut connection, &format!("{prefix}:{{public.orders}}:*")).await;
    assert!(keys.is_empty(), "truncate left stale keys after recovery");
}

#[tokio::test]
async fn connect_recovers_when_redis_starts_late() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(container) = std::env::var("VS_TEST_REDIS_RESTART_CONTAINER") else {
        return;
    };
    let _container_guard = RestartContainerGuard::new(container.clone());
    let output = Command::new("docker")
        .args(["stop", "-t", "0", &container])
        .output()
        .expect("stop Redis container");
    assert!(output.status.success());

    let connect = tokio::spawn(async move {
        let mut config = RedisConfig::new(
            "redis-startup-recovery-test",
            url,
            "ventstream:test:startup-recovery",
            RedisKeyRouting::ByOutputRelation,
        );
        config.retry = RetryConfig {
            max_attempts: 40,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_millis(100),
            backoff_factor: 2.0,
        };
        RedisSink::connect(config).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let output = Command::new("docker")
        .args(["start", &container])
        .output()
        .expect("start Redis container");
    assert!(output.status.success());

    tokio::time::timeout(Duration::from_secs(15), connect)
        .await
        .expect("connect recovery timeout")
        .expect("connect task")
        .expect("connect after recovery");
}

#[tokio::test]
async fn memory_pressure_backpressures_until_capacity_recovers() {
    let Some(url) = pressure_redis_url() else {
        return;
    };
    let mut admin = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let info = redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut admin)
        .await
        .expect("memory info");
    let used_memory = info
        .lines()
        .find_map(|line| line.strip_prefix("used_memory:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .expect("used_memory");
    let maxmemory = used_memory.saturating_add(128 * 1024);

    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("noeviction")
        .query_async::<()>(&mut admin)
        .await
        .expect("noeviction");
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(maxmemory)
        .query_async::<()>(&mut admin)
        .await
        .expect("maxmemory");

    let filler = vec![b'x'; 32 * 1024];
    let mut observed_oom = false;
    for index in 0..64 {
        let result = redis::cmd("SET")
            .arg(format!("ventstream:pressure:filler:{index}"))
            .arg(&filler)
            .query_async::<()>(&mut admin)
            .await;
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error.code() == Some("OOM"))
        {
            observed_oom = true;
            break;
        }
        result.expect("pressure filler");
    }
    assert!(observed_oom, "Redis did not enter noeviction OOM pressure");

    let health = SinkHealth::new();
    let mut config = RedisConfig::new(
        "redis-pressure-test",
        &url,
        "ventstream:test:pressure",
        RedisKeyRouting::ByOutputRelation,
    )
    .with_delivery_health(health.clone());
    config.retry = RetryConfig {
        max_attempts: 0,
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(50),
        backoff_factor: 2.0,
    };
    let sink = Arc::new(RedisSink::connect(config).await.expect("sink"));
    let write = {
        let sink = Arc::clone(&sink);
        let payload = format!(r#"{{"id":"pressure","blob":"{}"}}"#, "x".repeat(64 * 1024));
        tokio::spawn(async move {
            sink.write(SinkBatch::new(vec![event(
                "insert",
                r#"public.orders:["pressure"]"#,
                &payload,
            )]))
            .await
        })
    };

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !write.is_finished(),
        "OOM pressure must keep the write pending"
    );
    assert!(matches!(
        health.snapshot(),
        SinkHealthSnapshot::Degraded { ref reason, .. } if reason.contains("OOM")
    ));
    assert_eq!(sink.recommended_concurrency(8), 1);

    redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(0)
        .query_async::<()>(&mut admin)
        .await
        .expect("remove maxmemory pressure");
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("pressure recovery timeout")
        .expect("write task")
        .expect("write after pressure recovery");
    assert_eq!(health.snapshot(), SinkHealthSnapshot::Healthy);
}

#[tokio::test]
async fn split_pipelines_preserve_same_key_order() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = format!(
        "ventstream:test:split-order:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let key = format!("{prefix}:{{orders}}:orders%3A%5B%22ordered%22%5D");
    let mut config = RedisConfig::new(
        "redis-split-order-test",
        &url,
        prefix,
        RedisKeyRouting::Fixed("orders".to_owned()),
    );
    config.max_batch_bytes = 350;
    let sink = RedisSink::connect(config).await.expect("sink");
    sink.write(SinkBatch::new(vec![
        event(
            "update",
            r#"orders:["ordered"]"#,
            r#"{"id":"ordered","version":1}"#,
        ),
        event(
            "update",
            r#"orders:["ordered"]"#,
            r#"{"id":"ordered","version":2}"#,
        ),
    ]))
    .await
    .expect("ordered split write");

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let stored = redis::cmd("GET")
        .arg(&key)
        .query_async::<Vec<u8>>(&mut connection)
        .await
        .expect("ordered value");
    assert_eq!(stored, br#"{"id":"ordered","version":2}"#.to_vec());
}

#[tokio::test]
async fn ordered_pipeline_throughput_probe() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(event_count) = std::env::var("VS_TEST_REDIS_BENCH_EVENTS") else {
        return;
    };
    let event_count = event_count.parse::<usize>().expect("benchmark event count");
    let prefix = format!(
        "ventstream:test:bench:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(RedisConfig::new(
        "redis-benchmark",
        &url,
        &prefix,
        RedisKeyRouting::Fixed("orders".to_owned()),
    ))
    .await
    .expect("sink");
    let started = std::time::Instant::now();
    for start in (0..event_count).step_by(2_000) {
        let end = start.saturating_add(2_000).min(event_count);
        let events = (start..end)
            .map(|index| {
                event(
                    "insert",
                    &format!("orders:{index}"),
                    &format!(r#"{{"id":{index},"status":"created","total":42}}"#),
                )
            })
            .collect();
        sink.write(SinkBatch::new(events))
            .await
            .expect("benchmark batch");
    }
    let elapsed = started.elapsed();
    let throughput = event_count as f64 / elapsed.as_secs_f64();
    println!(
        "redis sink: {event_count} writes in {:.3}s ({throughput:.0} writes/s)",
        elapsed.as_secs_f64()
    );

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut cursor = 0u64;
    let mut matched = 0usize;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{prefix}:{{orders}}:*"))
            .arg("COUNT")
            .arg(10_000)
            .query_async(&mut connection)
            .await
            .expect("scan benchmark keys");
        matched = matched.saturating_add(keys.len());
        if !keys.is_empty() {
            let _: usize = redis::cmd("UNLINK")
                .arg(keys)
                .query_async(&mut connection)
                .await
                .expect("remove benchmark keys");
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(matched, event_count);
    redis::cmd("UNLINK")
        .arg(format!("{prefix}:__ventstream:writer:{{orders}}:current"))
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove benchmark writer fence");
}

#[tokio::test]
async fn bounded_keyspace_sustained_throughput_probe() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(duration_secs) = std::env::var("VS_TEST_REDIS_SOAK_SECS") else {
        return;
    };
    let duration = Duration::from_secs(
        duration_secs
            .parse::<u64>()
            .expect("Redis soak duration seconds"),
    );
    let cardinality = std::env::var("VS_TEST_REDIS_SOAK_KEYS")
        .ok()
        .map(|value| value.parse::<usize>().expect("Redis soak key count"))
        .unwrap_or(10_000);
    assert!(cardinality > 0);

    let prefix = format!(
        "ventstream:test:soak:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(RedisConfig::new(
        "redis-soak",
        &url,
        &prefix,
        RedisKeyRouting::Fixed("orders".to_owned()),
    ))
    .await
    .expect("sink");
    let started = std::time::Instant::now();
    let mut writes = 0usize;
    let mut peak_rss_kib = process_rss_kib();
    while started.elapsed() < duration {
        let events = (0..2_000)
            .map(|offset| {
                let index = writes.saturating_add(offset) % cardinality;
                event(
                    "update",
                    &format!("orders:{index}"),
                    &format!(
                        r#"{{"id":{index},"revision":{},"status":"active"}}"#,
                        writes.saturating_add(offset)
                    ),
                )
            })
            .collect();
        sink.write(SinkBatch::new(events))
            .await
            .expect("sustained benchmark batch");
        writes = writes.saturating_add(2_000);
        peak_rss_kib = peak_rss_kib.max(process_rss_kib());
    }

    let elapsed = started.elapsed();
    let throughput = writes as f64 / elapsed.as_secs_f64();
    println!(
        "redis sink sustained: {writes} writes in {:.3}s ({throughput:.0} writes/s), keyspace={cardinality}, peak_rss_mib={:.1}",
        elapsed.as_secs_f64(),
        peak_rss_kib as f64 / 1024.0
    );

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let mut cursor = 0u64;
    let mut matched = 0usize;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{prefix}:{{orders}}:*"))
            .arg("COUNT")
            .arg(10_000)
            .query_async(&mut connection)
            .await
            .expect("scan soak keys");
        matched = matched.saturating_add(keys.len());
        if !keys.is_empty() {
            let _: usize = redis::cmd("UNLINK")
                .arg(keys)
                .query_async(&mut connection)
                .await
                .expect("remove soak keys");
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(matched, cardinality);
    redis::cmd("UNLINK")
        .arg(format!("{prefix}:__ventstream:writer:{{orders}}:current"))
        .query_async::<usize>(&mut connection)
        .await
        .expect("remove soak writer fence");
}

#[tokio::test]
async fn lookup_views_sustained_throughput_probe() {
    let Some(url) = redis_url() else {
        return;
    };
    let Ok(duration_secs) = std::env::var("VS_TEST_REDIS_VIEW_SOAK_SECS") else {
        return;
    };
    let duration = Duration::from_secs(
        duration_secs
            .parse::<u64>()
            .expect("Redis view soak duration seconds"),
    );
    let cardinality = std::env::var("VS_TEST_REDIS_VIEW_SOAK_KEYS")
        .ok()
        .map(|value| value.parse::<usize>().expect("Redis view soak key count"))
        .unwrap_or(10_000);
    assert!(cardinality > 0);

    let prefix = format!(
        "ventstream:test:view-soak:{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let sink = RedisSink::connect(lookup_view_config(
        &url,
        &prefix,
        RedisContract::MaterializedView,
    ))
    .await
    .expect("view soak sink");
    let started = std::time::Instant::now();
    let baseline_rss_kib = process_rss_kib();
    let mut peak_rss_kib = baseline_rss_kib;
    let mut source_events = 0usize;
    while started.elapsed() < duration {
        let events = (0..1_000)
            .map(|offset| {
                let revision = source_events.saturating_add(offset);
                let index = revision % cardinality;
                let customer = (revision / cardinality) % 16;
                event(
                    "update",
                    &format!(r#"public.orders:["{index}"]"#),
                    &format!(
                        r#"{{"id":"{index}","customer_id":"customer-{customer}","status":"pending","revision":{revision}}}"#
                    ),
                )
            })
            .collect();
        sink.write(SinkBatch::new(events))
            .await
            .expect("sustained view benchmark batch");
        source_events = source_events.saturating_add(1_000);
        peak_rss_kib = peak_rss_kib.max(process_rss_kib());
    }

    let elapsed = started.elapsed();
    let source_throughput = source_events as f64 / elapsed.as_secs_f64();
    println!(
        "redis views sustained: {source_events} source events / {} materializations in {:.3}s ({source_throughput:.0} source events/s), keyspace={cardinality}, baseline_rss_mib={:.1}, peak_rss_mib={:.1}, rss_growth_mib={:.1}",
        source_events.saturating_mul(2),
        elapsed.as_secs_f64(),
        baseline_rss_kib as f64 / 1024.0,
        peak_rss_kib as f64 / 1024.0,
        peak_rss_kib.saturating_sub(baseline_rss_kib) as f64 / 1024.0
    );

    let mut connection = redis::Client::open(url.as_str())
        .expect("client")
        .get_connection_manager()
        .await
        .expect("connection");
    let expected_keys = source_events.min(cardinality);
    assert_eq!(
        matching_keys(&mut connection, &format!("{prefix}:{{open_order_by_id}}:*"))
            .await
            .len(),
        expected_keys
    );
    assert_eq!(
        matching_keys(
            &mut connection,
            &format!("{prefix}:{{order_by_customer}}:*")
        )
        .await
        .len(),
        expected_keys
    );

    let keys = matching_keys(&mut connection, &format!("{prefix}:*")).await;
    for page in keys.chunks(5_000) {
        redis::cmd("UNLINK")
            .arg(page)
            .query_async::<usize>(&mut connection)
            .await
            .expect("remove view soak keys");
    }
}
