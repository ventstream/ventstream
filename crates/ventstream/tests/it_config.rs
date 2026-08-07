//! Black-box startup validation for canonical engine configuration files.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ventstream-config-integration-{}-{suffix}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn validate_config(path: &Path) -> Result<String, String> {
    let output = Command::new(env!("CARGO_BIN_EXE_ventstream"))
        .arg("--validate-config")
        .current_dir(std::env::temp_dir())
        .env_clear()
        .env("VS_ENGINE_CONFIG", path)
        .env("VS_PG_HOST", "postgres.example.com")
        .env("VS_PG_USER", "ventstream")
        .env("VS_PG_PASSWORD", "postgres-secret")
        .env("VS_PG_DATABASE", "shop")
        .env("VS_PG_PUBLICATION", "ventstream_shop")
        .env("VS_PG_SLOT", "ventstream_shop_slot")
        .env("VS_NEO4J_PASSWORD", "neo4j-secret")
        .env("VS_MONGO_URI", "mongodb://mongo:27017")
        .env("VS_MONGO_DATABASE", "shop")
        .env("VS_MYSQL_PASSWORD", "mysql-secret")
        .env("VS_KAFKA_SASL_PASSWORD", "kafka-secret")
        .env("VS_OS_ENDPOINT", "https://search.example.com")
        .env("VS_REDIS_SINK_URL", "rediss://redis.example.com:6379/")
        .env("VS_REDIS_SINK_USERNAME", "ventstream")
        .env("VS_REDIS_SINK_PASSWORD", "redis-secret")
        .env(
            "VS_REDIS_SENTINEL_A",
            "redis://sentinel-a.example.com:26379",
        )
        .env(
            "VS_REDIS_SENTINEL_B",
            "redis://sentinel-b.example.com:26379",
        )
        .env(
            "VS_REDIS_SENTINEL_C",
            "redis://sentinel-c.example.com:26379",
        )
        .env("VS_REDIS_SENTINEL_PASSWORD", "sentinel-secret")
        .env("VS_REDIS_CLUSTER_A", "redis://redis-a.example.com:6379")
        .env("VS_REDIS_CLUSTER_B", "redis://redis-b.example.com:6379")
        .env("VS_REDIS_CLUSTER_C", "redis://redis-c.example.com:6379")
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "configuration validation failed for {}: stdout={} stderr={}",
            path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| error.to_string())
}

#[test]
fn validates_every_managed_source_without_opening_connector_sockets(
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = TestDirectory::new()?;
    fs::write(
        directory.path("joins.yaml"),
        r#"
joins:
  - name: orders
    primary:
      table: public.orders
      pk: id
    target:
      index: orders
    related: []
"#,
    )?;

    let sources = [
        (
            "postgres",
            r#"
source:
  kind: postgres
  postgres:
    host: postgres
    user: replicator
    password_ref: env:VS_PG_PASSWORD
    database: shop
    publication: ventstream_pub
    slot: ventstream_slot
    bootstrap:
      mode: snapshot
      chunk_size: 500
specs:
  joins: joins.yaml
"#,
        ),
        (
            "neo4j",
            r#"
source:
  kind: neo4j
  neo4j:
    uri: bolt://neo4j:7687
    user: neo4j
    password_ref: env:VS_NEO4J_PASSWORD
    database: neo4j
    namespace: catalog
    bootstrap:
      mode: none
"#,
        ),
        (
            "mongodb",
            r#"
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: shop
    collections: [orders]
    bootstrap:
      mode: snapshot
      chunk_size: 500
"#,
        ),
        (
            "mysql",
            r#"
source:
  kind: mysql
  mysql:
    host: mysql
    user: replicator
    password_ref: env:VS_MYSQL_PASSWORD
    database: shop
    server_id: 42
    tables: [orders]
    denormalize_mode: sql
specs:
  joins: joins.yaml
"#,
        ),
        (
            "kafka",
            r#"
source:
  kind: kafka
  kafka:
    brokers: redpanda:9092
    topics: [orders]
    group_id: ventstream-orders
    unwrap: debezium
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: ventstream
    sasl_password_ref: env:VS_KAFKA_SASL_PASSWORD
"#,
        ),
    ];

    for (name, source) in sources {
        let path = directory.path(&format!("ventstream-{name}.yaml"));
        let routing = if matches!(name, "postgres" | "mysql") {
            "by_projection_target"
        } else {
            "by_output_relation"
        };
        fs::write(
            &path,
            format!(
                r#"
schema_version: 1
roles: [cdc]
{source}
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: {routing}
runtime:
  dlq_path: /tmp/ventstream-dlq.jsonl
"#
            ),
        )?;
        let stdout = validate_config(&path).map_err(|error| format!("{name}: {error}"))?;
        assert!(stdout
            .lines()
            .any(|line| line == "configuration valid: roles=cdc"));
    }
    Ok(())
}

#[test]
fn validates_redis_sink_example() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/standalone/ventstream.redis.yaml");
    let stdout = validate_config(&path)?;
    assert!(stdout
        .lines()
        .any(|line| line == "configuration valid: roles=cdc"));
    Ok(())
}

#[test]
fn validates_redis_lookup_views_example() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/standalone/ventstream.redis-views.yaml");
    let stdout = validate_config(&path)?;
    assert!(stdout
        .lines()
        .any(|line| line == "configuration valid: roles=cdc"));
    Ok(())
}

#[test]
fn validates_redis_topology_examples() -> Result<(), Box<dyn std::error::Error>> {
    for file in [
        "ventstream.redis-sentinel.yaml",
        "ventstream.redis-cluster.yaml",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/standalone")
            .join(file);
        let stdout = validate_config(&path).map_err(|error| format!("{file}: {error}"))?;
        assert!(stdout
            .lines()
            .any(|line| line == "configuration valid: roles=cdc"));
    }
    Ok(())
}

#[test]
fn validates_redis_lookup_views_through_the_runtime_loader(
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = TestDirectory::new()?;
    let path = directory.path("ventstream-redis-views.yaml");
    fs::write(
        &path,
        r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: shop
    collections: [orders]
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_REDIS_SINK_URL
    keyspace:
      prefix: ventstream:shop:views
      ownership: exclusive
      routing:
        strategy: views
        views:
          - name: pending_order_by_id
            source:
              namespace: shop
              relation: orders
            key:
              template: "order:${json:/id}"
            filter:
              conditions:
                - path: /status
                  operator: equals
                  value: pending
            value:
              mode: fields
              fields:
                id: /id
                status: /status
"#,
    )?;

    let stdout = validate_config(&path)?;
    assert!(stdout
        .lines()
        .any(|line| line == "configuration valid: roles=cdc"));
    Ok(())
}

#[test]
fn validates_combined_websocket_and_graphql_roles() -> Result<(), Box<dyn std::error::Error>> {
    let directory = TestDirectory::new()?;
    let path = directory.path("ventstream-realtime.yaml");
    fs::write(
        &path,
        r#"
schema_version: 1
roles: [ws, graphql]
runtime:
  tenant: tenant_a
  ws:
    listen: 0.0.0.0:4040
    nats_url: nats://nats:4222
    subjects: [vs.t.tenant_a.>]
    mailbox: 512
    jetstream:
      stream: ventstream
      storage: memory
      max_age_secs: 60
      max_bytes: 1048576
      max_msgs: -1
  graphql:
    listen: 0.0.0.0:4041
    nats_url: nats://nats:4222
    stream: ventstream
    broadcast_capacity: 2048
"#,
    )?;

    let stdout = validate_config(&path)?;
    assert!(stdout
        .lines()
        .any(|line| line == "configuration valid: roles=graphql,ws"));
    Ok(())
}
