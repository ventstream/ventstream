//! Shared integration-test harness.
//!
//! Environment-agnostic: each test spins its own Postgres + OpenSearch
//! (and, in the Neo4j suite, Neo4j) via testcontainers, then drives the
//! REAL engine binary (`CARGO_BIN_EXE_ventstream`) end-to-end. No
//! pre-staged stack, no fixed ports/creds — just a Docker daemon.
#![allow(dead_code, unreachable_pub)]
// shared harness across test bins
// Test harness: expect/unwrap/panic/indexing are how tests assert.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::clone_on_ref_ptr
)]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mongodb::bson::doc;
use mysql_async::{Conn, OptsBuilder};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const PG_USER: &str = "ventstream";
pub const PG_PASSWORD: &str = "ventstream";
pub const PG_DB: &str = "shop";

/// A running Postgres instance plus the process-scoped OpenSearch test
/// instance. Postgres is torn down when this drops; Testcontainers cleans
/// up OpenSearch when the test process exits.
pub struct PgOsStack {
    pub pg: ContainerAsync<GenericImage>,
    pub pg_port: u16,
    pub os_port: u16,
}

pub struct PgRedisStack {
    pub pg: ContainerAsync<GenericImage>,
    pub redis: ContainerAsync<GenericImage>,
    pub pg_port: u16,
    pub redis_port: u16,
}

static POSTGRES_TEST_OS: tokio::sync::OnceCell<(ContainerAsync<GenericImage>, u16)> =
    tokio::sync::OnceCell::const_new();

/// Start Postgres (logical replication enabled) + OpenSearch (security
/// off) and wait until both accept connections.
pub async fn start_pg_os() -> PgOsStack {
    let (pg, pg_port) = start_postgres().await;

    // OpenSearch is comparatively expensive to start and leaves enough JVM
    // teardown pressure to stall Docker when every test creates a fresh one.
    // The Postgres suite uses isolated indices, so one process-scoped instance
    // is both faster and more reliable.
    let (_, os_port) = POSTGRES_TEST_OS.get_or_init(start_os).await;
    PgOsStack {
        pg,
        pg_port,
        os_port: *os_port,
    }
}

pub async fn start_pg_redis() -> PgRedisStack {
    let (pg, pg_port) = start_postgres().await;
    let (redis, redis_port) = start_redis().await;
    PgRedisStack {
        pg,
        redis,
        pg_port,
        redis_port,
    }
}

pub async fn start_redis() -> (ContainerAsync<GenericImage>, u16) {
    let redis = GenericImage::new("redis", "7.4-alpine")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_cmd([
            "redis-server",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
        ])
        .start()
        .await
        .expect("start Redis container");
    let redis_port = redis
        .get_host_port_ipv4(6379.tcp())
        .await
        .expect("Redis host port");
    wait_redis_ready(redis_port).await;
    (redis, redis_port)
}

async fn start_postgres() -> (ContainerAsync<GenericImage>, u16) {
    let pg = GenericImage::new("postgres", "16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", PG_USER)
        .with_env_var("POSTGRES_PASSWORD", PG_PASSWORD)
        .with_env_var("POSTGRES_DB", PG_DB)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
        ])
        .start()
        .await
        .expect("start postgres container");
    let pg_port = pg
        .get_host_port_ipv4(5432.tcp())
        .await
        .expect("pg host port");
    wait_pg_ready(pg_port).await;
    (pg, pg_port)
}

/// Start an OpenSearch container (security disabled) and wait until its
/// HTTP API is healthy. Shared by the PG and Neo4j stacks.
async fn start_os() -> (ContainerAsync<GenericImage>, u16) {
    // No log-based wait — the ready log line is version-dependent and can
    // precede HTTP readiness. We poll /_cluster/health for availability.
    let start = GenericImage::new("opensearchproject/opensearch", "2.17.1")
        .with_exposed_port(9200.tcp())
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "Vent$tr3am!Pass")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start();
    let os = tokio::time::timeout(Duration::from_secs(120), start)
        .await
        .expect("timed out starting opensearch container")
        .expect("start opensearch container");
    let os_port = os
        .get_host_port_ipv4(9200.tcp())
        .await
        .expect("os host port");
    wait_os_ready(os_port).await;
    (os, os_port)
}

async fn wait_pg_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match try_pg_connect(port).await {
            Ok(_) => return,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(e) => panic!("postgres never became ready: {e}"),
        }
    }
}

async fn wait_os_ready(port: u16) {
    let client = os_client();
    let url = format!("http://127.0.0.1:{port}/_cluster/health");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match client.get(&url).send().await {
            Ok(r) if r.status().is_success() => return,
            _ if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            _ => panic!("opensearch never became ready on port {port}"),
        }
    }
}

async fn wait_redis_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ready = redis_connection(port).await.is_ok();
        if ready {
            return;
        }
        if Instant::now() >= deadline {
            panic!("Redis never became ready on port {port}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn try_pg_connect(port: u16) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, conn) = tokio_postgres::connect(&pg_conn_str(port), tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

pub fn pg_conn_str(port: u16) -> String {
    format!("host=127.0.0.1 port={port} user={PG_USER} password={PG_PASSWORD} dbname={PG_DB}")
}

/// Open a PG client (connection task spawned in the background).
pub async fn pg_client(port: u16) -> tokio_postgres::Client {
    try_pg_connect(port).await.expect("connect to postgres")
}

// ---- OpenSearch helpers -------------------------------------------------

pub fn os_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

pub fn redis_url(port: u16) -> String {
    format!("redis://127.0.0.1:{port}/")
}

pub async fn redis_connection(port: u16) -> redis::RedisResult<redis::aio::ConnectionManager> {
    redis::Client::open(redis_url(port))?
        .get_connection_manager()
        .await
}

pub async fn redis_keys(port: u16, pattern: &str) -> redis::RedisResult<Vec<String>> {
    let mut connection = redis_connection(port).await?;
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (next, page): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1_000)
            .query_async(&mut connection)
            .await?;
        keys.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    keys.sort();
    Ok(keys)
}

pub async fn redis_value(port: u16, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
    let mut connection = redis_connection(port).await?;
    redis::cmd("GET")
        .arg(key)
        .query_async::<Option<Vec<u8>>>(&mut connection)
        .await
}

/// Doc count for an index (0 if the index doesn't exist yet). Refreshes
/// first so the count reflects all indexed docs.
pub async fn os_count(port: u16, index: &str) -> u64 {
    let client = os_client();
    let _ = client
        .post(format!("{}/{index}/_refresh", os_url(port)))
        .send()
        .await;
    let Ok(resp) = client
        .get(format!("{}/{index}/_count", os_url(port)))
        .send()
        .await
    else {
        return 0;
    };
    if !resp.status().is_success() {
        return 0;
    }
    let Ok(v): Result<serde_json::Value, _> = resp.json().await else {
        return 0;
    };
    v.get("count").and_then(|c| c.as_u64()).unwrap_or(0)
}

/// Count documents whose keyword field exactly matches `value`.
pub async fn os_term_count(port: u16, index: &str, field: &str, value: &str) -> u64 {
    let client = os_client();
    let _ = client
        .post(format!("{}/{index}/_refresh", os_url(port)))
        .send()
        .await;
    let Ok(resp) = client
        .post(format!("{}/{index}/_count", os_url(port)))
        .json(&serde_json::json!({
            "query": { "term": { (field): value } }
        }))
        .send()
        .await
    else {
        return 0;
    };
    if !resp.status().is_success() {
        return 0;
    }
    let Ok(v): Result<serde_json::Value, _> = resp.json().await else {
        return 0;
    };
    v.get("count").and_then(|count| count.as_u64()).unwrap_or(0)
}

/// Fetch a doc's `_source` by id, or `None` if it doesn't exist (404).
pub async fn os_doc(port: u16, index: &str, id: &str) -> Option<serde_json::Value> {
    let client = os_client();
    let enc: String = url_encode(id);
    let resp = client
        .get(format!("{}/{index}/_doc/{enc}", os_url(port)))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    if v.get("found").and_then(|f| f.as_bool()) == Some(true) {
        v.get("_source").cloned()
    } else {
        None
    }
}

fn os_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build opensearch test client")
}

/// Minimal percent-encoding for doc-id path segments (covers the chars
/// our deterministic ids use: `[`, `]`, `"`, `,`, space).
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b':' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---- Engine process -----------------------------------------------------

/// Options for spawning the engine against a Postgres source.
pub struct PgEngine<'a> {
    pub pg_port: u16,
    pub os_port: u16,
    pub slot: &'a str,
    pub publication: &'a str,
    pub spec_path: &'a str,
    pub state_dir: &'a str,
    pub index_template: &'a str,
    /// `""` = engine default (in-memory join). `"sql"` = SQL-denormalize.
    pub denormalize_mode: &'a str,
}

pub struct PgRedisEngine<'a> {
    pub pg_port: u16,
    pub redis_port: u16,
    pub slot: &'a str,
    pub publication: &'a str,
    pub spec_path: &'a str,
    pub state_dir: &'a str,
    pub key_prefix: &'a str,
    pub key_routing: &'a str,
    pub keyspace_ownership: &'a str,
}

/// A spawned engine; killed on drop.
pub struct EngineHandle {
    child: Child,
    pub log_path: String,
}

impl EngineHandle {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
    pub fn terminate(&mut self) {
        let signalled = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .is_ok_and(|status| status.success());
        if signalled {
            let deadline = Instant::now() + Duration::from_secs(15);
            while Instant::now() < deadline {
                if self.child.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        self.kill();
    }
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for EngineHandle {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Spawn the real engine binary as a Postgres CDC agent.
pub fn spawn_pg_engine(opts: &PgEngine<'_>) -> EngineHandle {
    let bin = env!("CARGO_BIN_EXE_ventstream");
    let log_path = format!("{}/engine.log", opts.state_dir);
    std::fs::create_dir_all(opts.state_dir).ok();
    let log = std::fs::File::create(&log_path).expect("engine log file");
    let err = log.try_clone().expect("clone log fd");
    let mut cmd = Command::new(bin);
    cmd.env("VS_ROLES", "cdc")
        .env("VS_CDC_SOURCE", "postgres")
        .env("VS_PG_HOST", "127.0.0.1")
        .env("VS_PG_PORT", opts.pg_port.to_string())
        .env("VS_PG_USER", PG_USER)
        .env("VS_PG_PASSWORD", PG_PASSWORD)
        .env("VS_PG_DATABASE", PG_DB)
        .env("VS_PG_PUBLICATION", opts.publication)
        .env("VS_PG_SLOT", opts.slot)
        .env("VS_PG_BOOTSTRAP_MODE", "snapshot")
        .env("VS_JOINS_YAML", opts.spec_path)
        .env("VS_JOINS_STATE_DIR", opts.state_dir)
        .env("VS_OS_ENDPOINT", os_url(opts.os_port))
        .env("VS_INDEX_TEMPLATE", opts.index_template)
        // Keep the DLQ inside the test's temp dir — otherwise the engine
        // writes ./data/dlq.jsonl into the repo working tree.
        .env("VS_DLQ_PATH", format!("{}/dlq.jsonl", opts.state_dir))
        .env("RUST_LOG", "info,ventstream_joins=debug")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    if !opts.denormalize_mode.is_empty() {
        cmd.env("VS_PG_DENORMALIZE_MODE", opts.denormalize_mode);
    }
    let child = cmd.spawn().expect("spawn engine binary");
    EngineHandle { child, log_path }
}

pub fn spawn_pg_redis_engine(opts: &PgRedisEngine<'_>) -> EngineHandle {
    spawn_pg_redis_engine_with_bootstrap_override(opts, false)
}

pub fn spawn_pg_redis_engine_with_forced_bootstrap(opts: &PgRedisEngine<'_>) -> EngineHandle {
    spawn_pg_redis_engine_with_bootstrap_override(opts, true)
}

fn spawn_pg_redis_engine_with_bootstrap_override(
    opts: &PgRedisEngine<'_>,
    force_bootstrap: bool,
) -> EngineHandle {
    let log_path = format!("{}/engine.log", opts.state_dir);
    std::fs::create_dir_all(opts.state_dir).ok();
    let log = std::fs::File::create(&log_path).expect("engine log file");
    let err = log.try_clone().expect("clone log fd");
    let mut command = pg_redis_engine_command(opts);
    if force_bootstrap {
        command.env("VS_FLEET_FORCE_BOOTSTRAP", "1");
    }
    let child = command
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn Redis sink engine");
    EngineHandle { child, log_path }
}

pub fn pg_redis_engine_command(opts: &PgRedisEngine<'_>) -> Command {
    let bin = env!("CARGO_BIN_EXE_ventstream");
    let mut command = Command::new(bin);
    command
        .env("VS_ROLES", "cdc")
        .env("VS_CDC_SOURCE", "postgres")
        .env("VS_PG_HOST", "127.0.0.1")
        .env("VS_PG_PORT", opts.pg_port.to_string())
        .env("VS_PG_USER", PG_USER)
        .env("VS_PG_PASSWORD", PG_PASSWORD)
        .env("VS_PG_DATABASE", PG_DB)
        .env("VS_PG_PUBLICATION", opts.publication)
        .env("VS_PG_SLOT", opts.slot)
        .env("VS_PG_BOOTSTRAP_MODE", "snapshot")
        .env("VS_PG_DENORMALIZE_MODE", "sql")
        .env("VS_JOINS_YAML", opts.spec_path)
        .env("VS_JOINS_STATE_DIR", opts.state_dir)
        .env("VS_SINK", "redis")
        .env("VS_REDIS_SINK_URL", redis_url(opts.redis_port))
        .env("VS_REDIS_SINK_KEY_PREFIX", opts.key_prefix)
        .env("VS_REDIS_SINK_KEY_ROUTING", opts.key_routing)
        .env("VS_REDIS_SINK_KEYSPACE_OWNERSHIP", opts.keyspace_ownership)
        .env("VS_REDIS_SINK_DOCUMENT_FORMAT", "string")
        .env("VS_REDIS_SINK_CONTRACT", "materialized_view")
        .env("VS_DLQ_PATH", format!("{}/dlq.jsonl", opts.state_dir))
        .env("RUST_LOG", "info,ventstream_sinks=debug");
    command
}

/// Write a join spec to a temp file and return its path.
pub fn write_spec(dir: &str, yaml: &str) -> String {
    std::fs::create_dir_all(dir).ok();
    let path = format!("{dir}/spec.yaml");
    std::fs::write(&path, yaml).expect("write spec");
    path
}

/// Unique temp dir for a test's engine state.
pub fn state_dir(name: &str, port: u16) -> String {
    let dir = std::env::temp_dir().join(format!("vs-it-{name}-{port}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("state dir");
    dir.to_string_lossy().into_owned()
}

// ---- polling ------------------------------------------------------------

/// Poll `f` until it returns true or the timeout elapses. Panics with
/// `msg` on timeout.
pub async fn wait_until<F, Fut>(timeout: Duration, msg: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for: {msg}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---- Neo4j -------------------------------------------------------------

pub const NEO4J_USER: &str = "neo4j";
pub const NEO4J_PASSWORD: &str = "ventstreamtest";

/// A running Neo4j (enterprise, CDC enrichment enabled) + OpenSearch pair.
pub struct Neo4jOsStack {
    pub neo4j: ContainerAsync<GenericImage>,
    pub _os: ContainerAsync<GenericImage>,
    pub neo4j_port: u16,
    pub os_port: u16,
}

/// Start Neo4j enterprise (container-local plain bolt — no TLS) + OpenSearch,
/// wait for bolt, and enable CDC enrichment on the default database.
pub async fn start_neo4j_os() -> Neo4jOsStack {
    // No log-based wait — bolt readiness lags the log line; we poll with a
    // trivial query below.
    let neo4j = GenericImage::new("neo4j", "5.26-enterprise")
        .with_exposed_port(7687.tcp())
        .with_env_var("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env_var("NEO4J_AUTH", format!("{NEO4J_USER}/{NEO4J_PASSWORD}"))
        .with_env_var("NEO4J_dbms_security_procedures_unrestricted", "db.cdc.*")
        .with_env_var("NEO4J_server_memory_heap_max__size", "512m")
        .with_env_var("NEO4J_server_memory_pagecache_size", "256m")
        .start()
        .await
        .expect("start neo4j container");
    let neo4j_port = neo4j
        .get_host_port_ipv4(7687.tcp())
        .await
        .expect("neo4j bolt port");

    let (os, os_port) = start_os().await;
    let stack = Neo4jOsStack {
        neo4j,
        _os: os,
        neo4j_port,
        os_port,
    };

    wait_neo4j_ready(&stack.neo4j).await;
    // CDC enrichment is a system-database setting; must precede the engine
    // so its bootstrap captures a valid CDC cursor.
    neo4j_exec_db(
        &stack.neo4j,
        "system",
        "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL'",
    )
    .await;
    stack
}

async fn wait_neo4j_ready(c: &ContainerAsync<GenericImage>) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some((0, _)) = neo4j_try_exec(c, "neo4j", "RETURN 1").await {
            return;
        }
        if Instant::now() >= deadline {
            panic!("neo4j bolt never became ready");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Run a Cypher statement against `db` via cypher-shell inside the
/// container. Panics on non-zero exit. Returns plain stdout.
pub async fn neo4j_exec_db(c: &ContainerAsync<GenericImage>, db: &str, query: &str) -> String {
    let (code, out) = neo4j_try_exec(c, db, query)
        .await
        .expect("exec cypher-shell");
    assert_eq!(code, 0, "cypher-shell exit {code} for `{query}`:\n{out}");
    out
}

/// Convenience: run against the default `neo4j` database.
pub async fn neo4j_exec(c: &ContainerAsync<GenericImage>, query: &str) -> String {
    neo4j_exec_db(c, "neo4j", query).await
}

async fn neo4j_try_exec(
    c: &ContainerAsync<GenericImage>,
    db: &str,
    query: &str,
) -> Option<(i64, String)> {
    let cmd = testcontainers::core::ExecCommand::new([
        "cypher-shell",
        "-u",
        NEO4J_USER,
        "-p",
        NEO4J_PASSWORD,
        "-d",
        db,
        "--format",
        "plain",
        query,
    ]);
    let mut res = c.exec(cmd).await.ok()?;
    let bytes = res.stdout_to_vec().await.ok()?;
    let code = res.exit_code().await.ok().flatten().unwrap_or(-1);
    Some((code, String::from_utf8_lossy(&bytes).into_owned()))
}

/// Parse a single scalar from cypher-shell `--format plain` output: the
/// last non-empty line, with surrounding quotes/whitespace stripped.
pub fn neo4j_scalar(out: &str) -> String {
    out.lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty())
        .unwrap_or("")
        .trim_matches('"')
        .to_owned()
}

/// Options for spawning the engine against a Neo4j source.
pub struct Neo4jEngine<'a> {
    pub neo4j_port: u16,
    pub os_port: u16,
    pub spec_path: &'a str,
    pub state_dir: &'a str,
    pub index_template: &'a str,
    pub max_parallel_bulks: Option<usize>,
}

/// Spawn the real engine binary as a Neo4j CDC agent.
pub fn spawn_neo4j_engine(opts: &Neo4jEngine<'_>) -> EngineHandle {
    let bin = env!("CARGO_BIN_EXE_ventstream");
    let log_path = format!("{}/engine.log", opts.state_dir);
    std::fs::create_dir_all(opts.state_dir).ok();
    let log = std::fs::File::create(&log_path).expect("engine log file");
    let err = log.try_clone().expect("clone log fd");
    let mut command = Command::new(bin);
    command
        .env("VS_ROLES", "cdc")
        .env("VS_CDC_SOURCE", "neo4j")
        .env(
            "VS_NEO4J_URI",
            format!("bolt://127.0.0.1:{}", opts.neo4j_port),
        )
        .env("VS_NEO4J_USER", NEO4J_USER)
        .env("VS_NEO4J_PASSWORD", NEO4J_PASSWORD)
        .env("VS_NEO4J_DATABASE", "neo4j")
        .env("VS_NEO4J_DENORMALIZE_YAML", opts.spec_path)
        .env("VS_NEO4J_STATE_DIR", opts.state_dir)
        .env("VS_OS_ENDPOINT", os_url(opts.os_port))
        .env("VS_INDEX_TEMPLATE", opts.index_template)
        .env("VS_DLQ_PATH", format!("{}/dlq.jsonl", opts.state_dir))
        .env("RUST_LOG", "info,ventstream_sinks=debug")
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    if let Some(max_parallel_bulks) = opts.max_parallel_bulks {
        command.env("VS_DISPATCH_PARALLEL_BULKS", max_parallel_bulks.to_string());
    }
    let child = command.spawn().expect("spawn engine binary");
    EngineHandle { child, log_path }
}

pub fn spawn_neo4j_redis_engine(
    neo4j_port: u16,
    redis_port: u16,
    spec_path: &str,
    state_dir: &str,
    key_prefix: &str,
) -> EngineHandle {
    spawn_neo4j_redis_engine_with_ownership(
        neo4j_port, redis_port, spec_path, state_dir, key_prefix, "shared",
    )
}

pub fn spawn_neo4j_redis_engine_with_ownership(
    neo4j_port: u16,
    redis_port: u16,
    spec_path: &str,
    state_dir: &str,
    key_prefix: &str,
    keyspace_ownership: &str,
) -> EngineHandle {
    let command = neo4j_redis_engine_command(
        neo4j_port,
        redis_port,
        spec_path,
        state_dir,
        key_prefix,
        keyspace_ownership,
    );
    spawn_engine_command(command, state_dir)
}

pub fn neo4j_redis_engine_command(
    neo4j_port: u16,
    redis_port: u16,
    spec_path: &str,
    state_dir: &str,
    key_prefix: &str,
    keyspace_ownership: &str,
) -> Command {
    engine_command(
        state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "neo4j".to_owned()),
            ("VS_NEO4J_URI", format!("bolt://127.0.0.1:{neo4j_port}")),
            ("VS_NEO4J_USER", NEO4J_USER.to_owned()),
            ("VS_NEO4J_PASSWORD", NEO4J_PASSWORD.to_owned()),
            ("VS_NEO4J_DATABASE", "neo4j".to_owned()),
            ("VS_NEO4J_DENORMALIZE_YAML", spec_path.to_owned()),
            ("VS_NEO4J_STATE_DIR", state_dir.to_owned()),
            ("VS_SINK", "redis".to_owned()),
            ("VS_REDIS_SINK_URL", redis_url(redis_port)),
            ("VS_REDIS_SINK_KEY_PREFIX", key_prefix.to_owned()),
            ("VS_REDIS_SINK_KEY_ROUTING", "by_output_relation".to_owned()),
            (
                "VS_REDIS_SINK_KEYSPACE_OWNERSHIP",
                keyspace_ownership.to_owned(),
            ),
        ],
    )
}

// ---- MongoDB -----------------------------------------------------------

pub const MONGO_DB: &str = "shop";

pub struct MongoOsStack {
    pub mongo: ContainerAsync<GenericImage>,
    pub _os: ContainerAsync<GenericImage>,
    pub mongo_port: u16,
    pub os_port: u16,
    pub uri: String,
}

pub async fn start_mongodb_os() -> MongoOsStack {
    let mongo = GenericImage::new("mongo", "7.0")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_cmd(["mongod", "--replSet", "rs0", "--bind_ip_all"])
        .start()
        .await
        .expect("start mongodb container");
    let mongo_port = mongo
        .get_host_port_ipv4(27017.tcp())
        .await
        .expect("mongodb host port");
    let init_uri = format!("mongodb://127.0.0.1:{mongo_port}/?directConnection=true");
    let client = mongodb::Client::with_uri_str(&init_uri)
        .await
        .expect("connect to mongodb");
    client
        .database("admin")
        .run_command(doc! {
            "replSetInitiate": {
                "_id": "rs0",
                "members": [{"_id": 0, "host": "localhost:27017"}]
            }
        })
        .await
        .expect("init mongodb replica set");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let primary = client
            .database("admin")
            .run_command(doc! {"hello": 1})
            .await
            .ok()
            .and_then(|reply| reply.get_bool("isWritablePrimary").ok())
            .unwrap_or(false);
        if primary {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "mongodb replica set never elected a primary"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let uri = format!("{init_uri}&replicaSet=rs0");
    let (os, os_port) = start_os().await;
    MongoOsStack {
        mongo,
        _os: os,
        mongo_port,
        os_port,
        uri,
    }
}

pub struct MongoEngine<'a> {
    pub uri: &'a str,
    pub os_port: u16,
    pub state_dir: &'a str,
    pub index_template: &'a str,
}

pub fn spawn_mongodb_engine(opts: &MongoEngine<'_>) -> EngineHandle {
    spawn_engine(
        opts.state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "mongodb".to_owned()),
            ("VS_MONGO_URI", opts.uri.to_owned()),
            ("VS_MONGO_DATABASE", MONGO_DB.to_owned()),
            ("VS_MONGO_COLLECTIONS", "orders".to_owned()),
            ("VS_MONGO_STATE_DIR", opts.state_dir.to_owned()),
            ("VS_MONGO_BOOTSTRAP_MODE", "snapshot".to_owned()),
            ("VS_MONGO_TOKEN_FLUSH_MS", "100".to_owned()),
            ("VS_OS_ENDPOINT", os_url(opts.os_port)),
            ("VS_INDEX_TEMPLATE", opts.index_template.to_owned()),
        ],
    )
}

pub fn spawn_mongodb_redis_engine(
    uri: &str,
    redis_port: u16,
    state_dir: &str,
    key_prefix: &str,
) -> EngineHandle {
    spawn_mongodb_redis_engine_with_ownership(uri, redis_port, state_dir, key_prefix, "shared")
}

pub fn spawn_mongodb_redis_engine_with_ownership(
    uri: &str,
    redis_port: u16,
    state_dir: &str,
    key_prefix: &str,
    keyspace_ownership: &str,
) -> EngineHandle {
    let command =
        mongodb_redis_engine_command(uri, redis_port, state_dir, key_prefix, keyspace_ownership);
    spawn_engine_command(command, state_dir)
}

pub fn mongodb_redis_engine_command(
    uri: &str,
    redis_port: u16,
    state_dir: &str,
    key_prefix: &str,
    keyspace_ownership: &str,
) -> Command {
    engine_command(
        state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "mongodb".to_owned()),
            ("VS_MONGO_URI", uri.to_owned()),
            ("VS_MONGO_DATABASE", MONGO_DB.to_owned()),
            ("VS_MONGO_COLLECTIONS", "orders".to_owned()),
            ("VS_MONGO_STATE_DIR", state_dir.to_owned()),
            ("VS_MONGO_BOOTSTRAP_MODE", "snapshot".to_owned()),
            ("VS_MONGO_TOKEN_FLUSH_MS", "100".to_owned()),
            ("VS_SINK", "redis".to_owned()),
            ("VS_REDIS_SINK_URL", redis_url(redis_port)),
            ("VS_REDIS_SINK_KEY_PREFIX", key_prefix.to_owned()),
            ("VS_REDIS_SINK_KEY_ROUTING", "by_output_relation".to_owned()),
            (
                "VS_REDIS_SINK_KEYSPACE_OWNERSHIP",
                keyspace_ownership.to_owned(),
            ),
        ],
    )
}

// ---- MySQL -------------------------------------------------------------

pub const MYSQL_USER: &str = "ventstream";
pub const MYSQL_PASSWORD: &str = "ventstream";
pub const MYSQL_DB: &str = "shop";

pub struct MySqlOsStack {
    pub mysql: ContainerAsync<GenericImage>,
    pub _os: ContainerAsync<GenericImage>,
    pub mysql_port: u16,
    pub os_port: u16,
}

pub struct MySqlStack {
    pub mysql: ContainerAsync<GenericImage>,
    pub mysql_port: u16,
}

pub async fn start_mysql() -> MySqlStack {
    start_mysql_with_row_image("FULL").await
}

pub async fn start_mysql_with_row_image(row_image: &str) -> MySqlStack {
    let mysql_port = reserve_local_port();
    let mysql = GenericImage::new("mysql", "8.4")
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_PASSWORD)
        .with_env_var("MYSQL_ROOT_HOST", "%")
        .with_env_var("MYSQL_DATABASE", MYSQL_DB)
        .with_cmd(vec![
            "mysqld".to_owned(),
            "--server-id=1".to_owned(),
            "--log-bin=mysql-bin".to_owned(),
            "--binlog-format=ROW".to_owned(),
            format!("--binlog-row-image={row_image}"),
        ])
        .with_mapped_port(mysql_port, 3306.tcp())
        .start()
        .await
        .expect("start mysql container");
    wait_mysql_ready(mysql_port).await;
    MySqlStack { mysql, mysql_port }
}

pub async fn start_mysql_os() -> MySqlOsStack {
    start_mysql_os_with_row_image("FULL").await
}

pub async fn start_mysql_os_with_row_image(row_image: &str) -> MySqlOsStack {
    let mysql = start_mysql_with_row_image(row_image).await;
    let (os, os_port) = start_os().await;
    MySqlOsStack {
        mysql: mysql.mysql,
        _os: os,
        mysql_port: mysql.mysql_port,
        os_port,
    }
}

pub async fn wait_mysql_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match mysql_root_conn(port).await {
            Ok(conn) => {
                let _ = conn.disconnect().await;
                return;
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(error) => panic!("mysql never became ready: {error}"),
        }
    }
}

pub async fn mysql_root_conn(port: u16) -> Result<Conn, mysql_async::Error> {
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(MYSQL_PASSWORD)),
    )
    .await
}

pub struct MySqlEngine<'a> {
    pub mysql_port: u16,
    pub os_port: u16,
    pub spec_path: &'a str,
    pub state_dir: &'a str,
    pub index_template: &'a str,
    pub denormalize_mode: &'a str,
    pub bootstrap_mode: &'a str,
}

pub fn spawn_mysql_engine(opts: &MySqlEngine<'_>) -> EngineHandle {
    spawn_engine(
        opts.state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "mysql".to_owned()),
            ("VS_MYSQL_HOST", "127.0.0.1".to_owned()),
            ("VS_MYSQL_PORT", opts.mysql_port.to_string()),
            ("VS_MYSQL_USER", MYSQL_USER.to_owned()),
            ("VS_MYSQL_PASSWORD", MYSQL_PASSWORD.to_owned()),
            ("VS_MYSQL_DATABASE", MYSQL_DB.to_owned()),
            ("VS_MYSQL_TABLES", "orders".to_owned()),
            ("VS_MYSQL_SERVER_ID", "4000000001".to_owned()),
            ("VS_MYSQL_STATE_DIR", opts.state_dir.to_owned()),
            ("VS_MYSQL_BOOTSTRAP_MODE", opts.bootstrap_mode.to_owned()),
            ("VS_MYSQL_POS_FLUSH_MS", "100".to_owned()),
            (
                "VS_MYSQL_DENORMALIZE_MODE",
                opts.denormalize_mode.to_owned(),
            ),
            ("VS_JOINS_YAML", opts.spec_path.to_owned()),
            ("VS_JOINS_STATE_DIR", opts.state_dir.to_owned()),
            ("VS_OS_ENDPOINT", os_url(opts.os_port)),
            ("VS_INDEX_TEMPLATE", opts.index_template.to_owned()),
        ],
    )
}

pub fn spawn_mysql_redis_engine(
    mysql_port: u16,
    redis_port: u16,
    spec_path: &str,
    state_dir: &str,
    key_prefix: &str,
) -> EngineHandle {
    spawn_mysql_redis_engine_with_options(&MySqlRedisEngine {
        mysql_port,
        redis_port,
        spec_path,
        state_dir,
        key_prefix,
        tables: "orders",
        keyspace_ownership: "shared",
    })
}

pub fn spawn_mysql_redis_engine_with_tables(
    mysql_port: u16,
    redis_port: u16,
    spec_path: &str,
    state_dir: &str,
    key_prefix: &str,
    tables: &str,
) -> EngineHandle {
    spawn_mysql_redis_engine_with_options(&MySqlRedisEngine {
        mysql_port,
        redis_port,
        spec_path,
        state_dir,
        key_prefix,
        tables,
        keyspace_ownership: "shared",
    })
}

pub struct MySqlRedisEngine<'a> {
    pub mysql_port: u16,
    pub redis_port: u16,
    pub spec_path: &'a str,
    pub state_dir: &'a str,
    pub key_prefix: &'a str,
    pub tables: &'a str,
    pub keyspace_ownership: &'a str,
}

pub fn spawn_mysql_redis_engine_with_options(opts: &MySqlRedisEngine<'_>) -> EngineHandle {
    let command = mysql_redis_engine_command(opts);
    spawn_engine_command(command, opts.state_dir)
}

pub fn mysql_redis_engine_command(opts: &MySqlRedisEngine<'_>) -> Command {
    engine_command(
        opts.state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "mysql".to_owned()),
            ("VS_MYSQL_HOST", "127.0.0.1".to_owned()),
            ("VS_MYSQL_PORT", opts.mysql_port.to_string()),
            ("VS_MYSQL_USER", MYSQL_USER.to_owned()),
            ("VS_MYSQL_PASSWORD", MYSQL_PASSWORD.to_owned()),
            ("VS_MYSQL_DATABASE", MYSQL_DB.to_owned()),
            ("VS_MYSQL_TABLES", opts.tables.to_owned()),
            ("VS_MYSQL_SERVER_ID", "4000000002".to_owned()),
            ("VS_MYSQL_STATE_DIR", opts.state_dir.to_owned()),
            ("VS_MYSQL_BOOTSTRAP_MODE", "snapshot".to_owned()),
            ("VS_MYSQL_POS_FLUSH_MS", "100".to_owned()),
            ("VS_MYSQL_DENORMALIZE_MODE", "sql".to_owned()),
            ("VS_JOINS_YAML", opts.spec_path.to_owned()),
            ("VS_JOINS_STATE_DIR", opts.state_dir.to_owned()),
            ("VS_SINK", "redis".to_owned()),
            ("VS_REDIS_SINK_URL", redis_url(opts.redis_port)),
            ("VS_REDIS_SINK_KEY_PREFIX", opts.key_prefix.to_owned()),
            ("VS_REDIS_SINK_KEY_ROUTING", "by_output_relation".to_owned()),
            (
                "VS_REDIS_SINK_KEYSPACE_OWNERSHIP",
                opts.keyspace_ownership.to_owned(),
            ),
        ],
    )
}

// ---- Kafka / Redpanda --------------------------------------------------

pub struct KafkaOsStack {
    pub redpanda: ContainerAsync<GenericImage>,
    pub _os: ContainerAsync<GenericImage>,
    pub kafka_port: u16,
    pub os_port: u16,
}

pub async fn start_kafka_os() -> KafkaOsStack {
    let kafka_port = reserve_local_port();
    let advertised = format!("PLAINTEXT://127.0.0.1:{kafka_port}");
    let redpanda = GenericImage::new("redpandadata/redpanda", "v24.3.9")
        .with_cmd(vec![
            "redpanda".to_owned(),
            "start".to_owned(),
            "--overprovisioned".to_owned(),
            "--smp=1".to_owned(),
            "--memory=512M".to_owned(),
            "--reserve-memory=0M".to_owned(),
            "--node-id=0".to_owned(),
            "--check=false".to_owned(),
            "--kafka-addr=PLAINTEXT://0.0.0.0:9092".to_owned(),
            format!("--advertise-kafka-addr={advertised}"),
        ])
        .with_mapped_port(kafka_port, 9092.tcp())
        .start()
        .await
        .expect("start redpanda container");
    wait_tcp_ready("redpanda", kafka_port, Duration::from_secs(60)).await;
    let (os, os_port) = start_os().await;
    KafkaOsStack {
        redpanda,
        _os: os,
        kafka_port,
        os_port,
    }
}

async fn wait_tcp_ready(name: &str, port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "{name} never became ready");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn reserve_local_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve local port")
        .local_addr()
        .expect("reserved local address")
        .port()
}

pub struct KafkaEngine<'a> {
    pub kafka_port: u16,
    pub os_port: u16,
    pub state_dir: &'a str,
    pub index_template: &'a str,
    pub group_id: &'a str,
}

pub fn spawn_kafka_engine(opts: &KafkaEngine<'_>) -> EngineHandle {
    spawn_engine(
        opts.state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "kafka".to_owned()),
            ("VS_KAFKA_BROKERS", format!("127.0.0.1:{}", opts.kafka_port)),
            ("VS_KAFKA_TOPICS", "orders".to_owned()),
            ("VS_KAFKA_GROUP_ID", opts.group_id.to_owned()),
            ("VS_KAFKA_NAMESPACE", "shop".to_owned()),
            ("VS_KAFKA_UNWRAP", "raw".to_owned()),
            ("VS_KAFKA_AUTO_OFFSET_RESET", "earliest".to_owned()),
            ("VS_KAFKA_COMMIT_MS", "100".to_owned()),
            ("VS_OS_ENDPOINT", os_url(opts.os_port)),
            ("VS_INDEX_TEMPLATE", opts.index_template.to_owned()),
        ],
    )
}

pub fn spawn_kafka_redis_engine(
    kafka_port: u16,
    redis_port: u16,
    state_dir: &str,
    group_id: &str,
    key_prefix: &str,
) -> EngineHandle {
    spawn_engine(
        state_dir,
        &[
            ("VS_ROLES", "cdc".to_owned()),
            ("VS_CDC_SOURCE", "kafka".to_owned()),
            ("VS_KAFKA_BROKERS", format!("127.0.0.1:{kafka_port}")),
            ("VS_KAFKA_TOPICS", "orders".to_owned()),
            ("VS_KAFKA_GROUP_ID", group_id.to_owned()),
            ("VS_KAFKA_NAMESPACE", "shop".to_owned()),
            ("VS_KAFKA_UNWRAP", "raw".to_owned()),
            ("VS_KAFKA_AUTO_OFFSET_RESET", "earliest".to_owned()),
            ("VS_KAFKA_COMMIT_MS", "100".to_owned()),
            ("VS_SINK", "redis".to_owned()),
            ("VS_REDIS_SINK_URL", redis_url(redis_port)),
            ("VS_REDIS_SINK_KEY_PREFIX", key_prefix.to_owned()),
            ("VS_REDIS_SINK_KEY_ROUTING", "by_output_relation".to_owned()),
        ],
    )
}
fn engine_command(state_dir: &str, env: &[(&str, String)]) -> Command {
    let bin = env!("CARGO_BIN_EXE_ventstream");
    let mut command = Command::new(bin);
    command
        .envs(env.iter().map(|(key, value)| (*key, value)))
        .env("VS_DLQ_PATH", format!("{state_dir}/dlq.jsonl"))
        .env("RUST_LOG", "info");
    command
}

fn spawn_engine(state_dir: &str, env: &[(&str, String)]) -> EngineHandle {
    let command = engine_command(state_dir, env);
    spawn_engine_command(command, state_dir)
}

pub fn spawn_engine_with_config(
    state_dir: &str,
    config_path: &str,
    env: &[(&str, String)],
) -> EngineHandle {
    let mut command = engine_command(state_dir, env);
    command.env("VS_ENGINE_CONFIG", config_path);
    spawn_engine_command(command, state_dir)
}

fn spawn_engine_command(mut command: Command, state_dir: &str) -> EngineHandle {
    let log_path = format!("{state_dir}/engine.log");
    std::fs::create_dir_all(state_dir).ok();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .expect("engine log file");
    let err = log.try_clone().expect("clone log fd");
    command.stdout(Stdio::from(log)).stderr(Stdio::from(err));
    let child = command.spawn().expect("spawn engine binary");
    EngineHandle { child, log_path }
}
