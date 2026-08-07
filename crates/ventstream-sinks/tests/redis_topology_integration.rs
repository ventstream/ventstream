//! Redis Sentinel and Cluster topology contract tests.
#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use ventstream_core::{ContentType, Event, Headers, Payload, Sink, SinkBatch, SourceUri, Subject};
use ventstream_sinks::{
    RedisAcknowledgement, RedisConfig, RedisKeyRouting, RedisKeyspaceOwnership,
    RedisSentinelTopology, RedisSink, RedisTlsConfig, RedisTopology, RetryConfig,
};

const REDIS_IMAGE: &str = "redis:7.4-alpine";

struct DockerTopology {
    network: String,
    containers: Vec<String>,
    directory: PathBuf,
}

struct LocalRedisTopology {
    children: Vec<Child>,
    directory: PathBuf,
}

impl LocalRedisTopology {
    fn new(label: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "vs-{label}-{}-{}",
            std::process::id(),
            random_port()
        ));
        fs::create_dir_all(&directory).expect("create local topology directory");
        Self {
            children: Vec::new(),
            directory,
        }
    }

    fn spawn(&mut self, program: &str, config: &PathBuf) {
        self.children.push(
            Command::new(program)
                .arg(config)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|error| panic!("start {program}: {error}")),
        );
    }
}

impl Drop for LocalRedisTopology {
    fn drop(&mut self) {
        for child in self.children.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

impl DockerTopology {
    fn new(label: &str) -> Self {
        let nonce = format!("{}-{}", std::process::id(), random_port());
        let network = format!("vs-{label}-{nonce}");
        docker(&["network", "create", &network]);
        let directory = std::env::temp_dir().join(&network);
        fs::create_dir_all(&directory).expect("create topology directory");
        Self {
            network,
            containers: Vec::new(),
            directory,
        }
    }

    fn name(&self, suffix: &str) -> String {
        format!("{}-{suffix}", self.network)
    }

    fn track(&mut self, name: String) -> String {
        self.containers.push(name.clone());
        name
    }
}

impl Drop for DockerTopology {
    fn drop(&mut self) {
        for container in self.containers.iter().rev() {
            let _ = Command::new("docker")
                .args(["rm", "--force", container])
                .output();
        }
        let _ = Command::new("docker")
            .args(["network", "rm", &self.network])
            .output();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn random_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind temporary port")
        .local_addr()
        .expect("temporary port address")
        .port()
}

fn docker(args: &[&str]) -> Output {
    let output = Command::new("docker")
        .args(args)
        .output()
        .expect("run docker command");
    assert!(
        output.status.success(),
        "docker {} failed: {}{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn docker_ok(args: &[&str]) -> bool {
    Command::new("docker")
        .args(args)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn cluster_cli(container: &str, password: &str, args: &[&str]) -> Output {
    let mut command = vec![
        "exec",
        container,
        "redis-cli",
        "--no-auth-warning",
        "-a",
        password,
    ];
    command.extend_from_slice(args);
    docker(&command)
}

fn cluster_cli_ok(container: &str, password: &str, args: &[&str]) -> bool {
    let mut command = vec![
        "exec",
        container,
        "redis-cli",
        "--no-auth-warning",
        "-a",
        password,
    ];
    command.extend_from_slice(args);
    docker_ok(&command)
}

fn start_cluster_node(network: &str, name: &str, password: &str) -> u16 {
    const PORT_ATTEMPTS: usize = 16;

    for _ in 0..PORT_ATTEMPTS {
        let port = random_port();
        let port_string = port.to_string();
        let published_port = format!("{port}:6379");
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--name",
                name,
                "--network",
                network,
                "--add-host",
                "host.docker.internal:host-gateway",
                "-p",
                &published_port,
                REDIS_IMAGE,
                "redis-server",
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                "nodes.conf",
                "--cluster-node-timeout",
                "1000",
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--requirepass",
                password,
                "--masterauth",
                password,
                "--cluster-announce-hostname",
                "127.0.0.1",
                "--cluster-preferred-endpoint-type",
                "hostname",
                "--cluster-announce-port",
                &port_string,
                "--cluster-announce-bus-port",
                "16379",
            ])
            .output()
            .expect("start Redis Cluster node");
        if output.status.success() {
            wait_for_redis(port, Some(password));
            return port;
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let retryable = stderr.contains("Ports are not available")
            || stderr.contains("address already in use")
            || stderr.contains("port is already allocated");
        let _ = Command::new("docker")
            .args(["rm", "--force", name])
            .output();
        assert!(
            retryable,
            "unable to start Redis Cluster node {name}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            stderr
        );
    }

    panic!("unable to allocate a host port for Redis Cluster node {name}");
}

fn authenticate(
    connection: &mut redis::Connection,
    password: Option<&str>,
) -> redis::RedisResult<()> {
    if let Some(password) = password {
        redis::cmd("AUTH").arg(password).query(connection)
    } else {
        Ok(())
    }
}

fn wait_for_redis(port: u16, password: Option<&str>) {
    let endpoint = format!("redis://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if redis::Client::open(endpoint.as_str())
            .and_then(|client| client.get_connection())
            .and_then(|mut connection| {
                authenticate(&mut connection, password)?;
                redis::cmd("PING").query::<String>(&mut connection)
            })
            .is_ok_and(|response| response == "PONG")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("Redis on port {port} did not become ready");
}

fn sentinel_master_port(sentinel_port: u16, password: &str) -> Option<u16> {
    let client = redis::Client::open(format!("redis://127.0.0.1:{sentinel_port}")).ok()?;
    let mut connection = client.get_connection().ok()?;
    authenticate(&mut connection, Some(password)).ok()?;
    redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg("mymaster")
        .query::<Vec<String>>(&mut connection)
        .ok()?
        .get(1)?
        .parse()
        .ok()
}

fn wait_for_replica_online(primary_port: u16, password: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let ready = redis::Client::open(format!("redis://127.0.0.1:{primary_port}"))
            .and_then(|client| client.get_connection())
            .and_then(|mut connection| {
                authenticate(&mut connection, Some(password))?;
                redis::cmd("INFO")
                    .arg("replication")
                    .query::<String>(&mut connection)
            })
            .is_ok_and(|info| info.contains("connected_slaves:1") && info.contains("state=online"));
        if ready {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("Redis replica did not become online");
}

fn retry_policy() -> RetryConfig {
    RetryConfig {
        max_attempts: 80,
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(500),
        backoff_factor: 1.5,
    }
}

fn event(relation: &str, id: &str, version: usize) -> Event {
    let headers = Headers::empty()
        .with_header("ventstream.cdc.relation".to_owned(), relation.to_owned())
        .with_header("ventstream.doc.id".to_owned(), id.to_owned());
    Event::builder(
        SourceUri::new("postgres://topology-test").expect("source"),
        Subject::new(format!("postgres.public.{relation}.update")).expect("subject"),
    )
    .content_type(ContentType::Json)
    .headers(headers)
    .payload(Payload::from_vec(
        format!(r#"{{"id":"{id}","version":{version}}}"#).into_bytes(),
    ))
    .build()
}

fn truncate_event(relation: &str) -> Event {
    let headers =
        Headers::empty().with_header("ventstream.cdc.relation".to_owned(), relation.to_owned());
    Event::builder(
        SourceUri::new("postgres://topology-test").expect("source"),
        Subject::new(format!("postgres.public.{relation}.truncate")).expect("subject"),
    )
    .content_type(ContentType::Json)
    .headers(headers)
    .payload(Payload::from_vec(
        br#"{"cascade":false,"restart_identity":false}"#.to_vec(),
    ))
    .build()
}

fn key(prefix: &str, target: &str, id: &str) -> String {
    format!("{prefix}:{{{target}}}:{id}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local redis-server and redis-sentinel binaries"]
async fn sentinel_rediscovers_the_writable_primary_after_failover() {
    let mut topology = LocalRedisTopology::new("sentinel");
    let data_password = "sentinel-data-secret";
    let sentinel_password = "sentinel-control-secret";
    let primary_port = random_port();
    let replica_port = random_port();
    let primary_directory = topology.directory.join("primary");
    let replica_directory = topology.directory.join("replica");
    fs::create_dir_all(&primary_directory).expect("create primary directory");
    fs::create_dir_all(&replica_directory).expect("create replica directory");
    let primary_config = topology.directory.join("primary.conf");
    let replica_config = topology.directory.join("replica.conf");
    fs::write(
        &primary_config,
        format!(
            "port {primary_port}\nbind 127.0.0.1\nprotected-mode no\ndir {}\nappendonly yes\nrequirepass {data_password}\nmasterauth {data_password}\n",
            primary_directory.display(),
        ),
    )
    .expect("write primary config");
    fs::write(
        &replica_config,
        format!(
            "port {replica_port}\nbind 127.0.0.1\nprotected-mode no\ndir {}\nreplicaof 127.0.0.1 {primary_port}\nreplica-announce-ip 127.0.0.1\nreplica-announce-port {replica_port}\nappendonly yes\nrequirepass {data_password}\nmasterauth {data_password}\n",
            replica_directory.display(),
        ),
    )
    .expect("write replica config");
    topology.spawn("redis-server", &primary_config);
    topology.spawn("redis-server", &replica_config);
    wait_for_redis(primary_port, Some(data_password));
    wait_for_redis(replica_port, Some(data_password));
    wait_for_replica_online(primary_port, data_password);

    let mut sentinel_endpoints = Vec::new();
    let mut sentinel_ports = Vec::new();
    for index in 0..3 {
        let port = random_port();
        let sentinel_directory = topology.directory.join(format!("sentinel-{index}"));
        fs::create_dir_all(&sentinel_directory).expect("create Sentinel directory");
        let config = topology.directory.join(format!("sentinel-{index}.conf"));
        fs::write(
            &config,
            format!(
                "port {port}\nbind 127.0.0.1\nprotected-mode no\ndir {}\nrequirepass {sentinel_password}\nsentinel monitor mymaster 127.0.0.1 {primary_port} 2\nsentinel auth-pass mymaster {data_password}\nsentinel down-after-milliseconds mymaster 1000\nsentinel failover-timeout mymaster 10000\nsentinel parallel-syncs mymaster 1\n",
                sentinel_directory.display(),
            ),
        )
        .expect("write Sentinel config");
        topology.spawn("redis-sentinel", &config);
        sentinel_endpoints.push(format!("redis://127.0.0.1:{port}"));
        sentinel_ports.push(port);
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline
        && sentinel_master_port(sentinel_ports[0], sentinel_password).is_none()
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sentinel_endpoints.insert(0, format!("redis://127.0.0.1:{}", random_port()));

    let prefix = "ventstream:sentinel-test";
    let mut config = RedisConfig::new(
        "redis-sentinel-test",
        sentinel_endpoints[0].clone(),
        prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_topology(RedisTopology::Sentinel(RedisSentinelTopology {
        endpoints: sentinel_endpoints,
        service_name: "mymaster".to_owned(),
        data_node_tls: false,
        username: None,
        password: Some(sentinel_password.to_owned()),
        username_file: None,
        password_file: None,
        tls: RedisTlsConfig::default(),
    }))
    .with_auth(None, Some(data_password.to_owned()))
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_acknowledgement(RedisAcknowledgement::Replicated {
        replicas: 1,
        timeout: Duration::from_secs(5),
    });
    config.retry = retry_policy();
    let sink = RedisSink::connect(config.clone())
        .await
        .expect("connect through Sentinel");
    sink.write(SinkBatch::new(vec![event("orders", "order-1", 1)]))
        .await
        .expect("write before failover");
    let drift = RedisSink::inspect_drift(config, &["orders".to_owned()], 100)
        .await
        .expect("inspect Sentinel target");
    assert!(drift.complete);
    assert!(drift.consistent);
    assert_eq!(drift.targets[0].data_keys, 1);

    let sentinel_client = redis::Client::open(format!("redis://127.0.0.1:{}", sentinel_ports[0]))
        .expect("Sentinel client");
    let mut sentinel_connection = sentinel_client
        .get_multiplexed_async_connection()
        .await
        .expect("Sentinel connection");
    redis::cmd("AUTH")
        .arg(sentinel_password)
        .query_async::<()>(&mut sentinel_connection)
        .await
        .expect("authenticate Sentinel connection");
    let failover_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match redis::cmd("SENTINEL")
            .arg("FAILOVER")
            .arg("mymaster")
            .query_async::<String>(&mut sentinel_connection)
            .await
        {
            Ok(_) => break,
            Err(error)
                if error.to_string().contains("NOGOODSLAVE")
                    && Instant::now() < failover_deadline =>
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("request Sentinel failover: {error}"),
        }
    }
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if sentinel_master_port(sentinel_ports[0], sentinel_password) == Some(replica_port) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Sentinel failover did not complete"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    sink.write(SinkBatch::new(vec![event("orders", "order-1", 2)]))
        .await
        .expect("write after failover");
    let client = redis::Client::open(format!("redis://127.0.0.1:{replica_port}"))
        .expect("new primary client");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("new primary connection");
    redis::cmd("AUTH")
        .arg(data_password)
        .query_async::<()>(&mut connection)
        .await
        .expect("authenticate new primary connection");
    let value: String = connection
        .get(key(prefix, "orders", "order-1"))
        .await
        .expect("read new primary value");
    assert!(value.contains(r#""version":2"#));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cluster_handles_ask_then_moved_without_cross_slot_writes() {
    let mut topology = DockerTopology::new("cluster");
    let cluster_password = "cluster-data-secret";
    let mut names = Vec::new();
    let mut ports = Vec::new();
    for index in 0..3 {
        let container_name = topology.name(&format!("node-{index}"));
        let name = topology.track(container_name);
        let port = start_cluster_node(&topology.network, &name, cluster_password);
        names.push(name);
        ports.push(port);
    }
    let mut create_args = vec![
        "run".to_owned(),
        "--rm".to_owned(),
        "--network".to_owned(),
        topology.network.clone(),
        "--add-host".to_owned(),
        "host.docker.internal:host-gateway".to_owned(),
        REDIS_IMAGE.to_owned(),
        "redis-cli".to_owned(),
        "--no-auth-warning".to_owned(),
        "-a".to_owned(),
        cluster_password.to_owned(),
        "--cluster".to_owned(),
        "create".to_owned(),
    ];
    create_args.extend(names.iter().map(|name| format!("{name}:6379")));
    create_args.extend([
        "--cluster-replicas".to_owned(),
        "0".to_owned(),
        "--cluster-yes".to_owned(),
    ]);
    let create_refs = create_args.iter().map(String::as_str).collect::<Vec<_>>();
    docker(&create_refs);

    let seeds = ports
        .iter()
        .map(|port| format!("redis://127.0.0.1:{port}"))
        .collect::<Vec<_>>();
    let prefix = "ventstream:cluster-test";
    let mut config = RedisConfig::new(
        "redis-cluster-test",
        seeds[0].clone(),
        prefix,
        RedisKeyRouting::ByOutputRelation,
    )
    .with_topology(RedisTopology::Cluster {
        endpoints: seeds.clone(),
    })
    .with_auth(None, Some(cluster_password.to_owned()))
    .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    .with_acknowledgement(RedisAcknowledgement::Aof {
        local: true,
        replicas: 0,
        timeout: Duration::from_secs(3),
    });
    config.retry = retry_policy();
    config.retry.max_attempts = 20;
    config.response_timeout = Duration::from_secs(5);
    let sink = Arc::new(
        RedisSink::connect(config.clone())
            .await
            .expect("connect to Cluster"),
    );
    sink.write(SinkBatch::new(vec![
        event("customers", "customer-1", 1),
        event("products", "product-1", 1),
        event("orders", "order-to-clear", 1),
    ]))
    .await
    .expect("write mixed-target Cluster batch");
    let drift = RedisSink::inspect_drift(
        config,
        &[
            "customers".to_owned(),
            "products".to_owned(),
            "orders".to_owned(),
        ],
        100,
    )
    .await
    .expect("inspect Cluster targets");
    assert!(drift.complete);
    assert!(drift.consistent);
    assert_eq!(
        drift
            .targets
            .iter()
            .map(|target| target.data_keys)
            .sum::<usize>(),
        3
    );

    let target = "orders";
    let target_key = key(prefix, target, "order-ask");
    let slot_output = cluster_cli(
        &names[0],
        cluster_password,
        &["CLUSTER", "KEYSLOT", &target_key],
    );
    let slot = String::from_utf8_lossy(&slot_output.stdout)
        .trim()
        .parse::<u16>()
        .expect("cluster key slot");
    let ids = names
        .iter()
        .map(|name| {
            let output = cluster_cli(name, cluster_password, &["CLUSTER", "MYID"]);
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        })
        .collect::<Vec<_>>();
    let mut source_index = None;
    for (index, name) in names.iter().enumerate() {
        if cluster_cli_ok(
            name,
            cluster_password,
            &[
                "CLUSTER",
                "SETSLOT",
                &slot.to_string(),
                "MIGRATING",
                &ids[(index + 1) % ids.len()],
            ],
        ) {
            source_index = Some(index);
            break;
        }
    }
    let source_index = source_index.expect("slot owner");
    let target_index = (source_index + 1) % names.len();
    cluster_cli(
        &names[source_index],
        cluster_password,
        &[
            "CLUSTER",
            "SETSLOT",
            &slot.to_string(),
            "MIGRATING",
            &ids[target_index],
        ],
    );
    cluster_cli(
        &names[target_index],
        cluster_password,
        &[
            "CLUSTER",
            "SETSLOT",
            &slot.to_string(),
            "IMPORTING",
            &ids[source_index],
        ],
    );

    let probe_client = redis::cluster::ClusterClientBuilder::new(seeds.clone())
        .password(cluster_password)
        .response_timeout(Duration::from_secs(2))
        .overall_response_timeout(Some(Duration::from_secs(2)))
        .build()
        .expect("ASK probe client");
    let mut probe = probe_client
        .get_async_connection()
        .await
        .expect("ASK probe connection");
    let _: () = probe
        .set(key(prefix, target, "ask-probe"), "ready")
        .await
        .expect("redis-rs follows ASK redirect");
    let pending_sink = Arc::clone(&sink);
    let pending_clear = tokio::spawn(async move {
        pending_sink
            .write(SinkBatch::new(vec![truncate_event("orders")]))
            .await
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !pending_clear.is_finished(),
        "target cleanup must backpressure while the slot is migrating"
    );

    let keys_output = cluster_cli(
        &names[source_index],
        cluster_password,
        &["CLUSTER", "GETKEYSINSLOT", &slot.to_string(), "1000"],
    );
    let migrating_keys = String::from_utf8_lossy(&keys_output.stdout)
        .lines()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !migrating_keys.is_empty(),
        "source slot has keys to migrate"
    );
    let mut migrate_args = vec![
        "MIGRATE".to_owned(),
        names[target_index].clone(),
        "6379".to_owned(),
        String::new(),
        "0".to_owned(),
        "5000".to_owned(),
        "AUTH".to_owned(),
        cluster_password.to_owned(),
        "KEYS".to_owned(),
    ];
    migrate_args.extend(migrating_keys);
    let migrate_refs = migrate_args.iter().map(String::as_str).collect::<Vec<_>>();
    cluster_cli(&names[source_index], cluster_password, &migrate_refs);

    for name in &names {
        cluster_cli(
            name,
            cluster_password,
            &[
                "CLUSTER",
                "SETSLOT",
                &slot.to_string(),
                "NODE",
                &ids[target_index],
            ],
        );
    }
    pending_clear
        .await
        .expect("Cluster clear task")
        .expect("clear after slot ownership converges");
    sink.write(SinkBatch::new(vec![event("orders", "order-ask", 2)]))
        .await
        .expect("write through MOVED redirect");

    let client = redis::cluster::ClusterClientBuilder::new(seeds)
        .password(cluster_password)
        .build()
        .expect("verification Cluster client");
    let mut connection = client
        .get_async_connection()
        .await
        .expect("verification Cluster connection");
    let stored: String = connection
        .get(target_key)
        .await
        .expect("read Cluster materialization");
    assert!(stored.contains(r#""version":2"#));
    let cleared: Option<String> = connection
        .get(key(prefix, "orders", "order-to-clear"))
        .await
        .expect("read cleared Cluster materialization");
    assert!(cleared.is_none());
    let customer: String = connection
        .get(key(prefix, "customers", "customer-1"))
        .await
        .expect("read customer materialization");
    let product: String = connection
        .get(key(prefix, "products", "product-1"))
        .await
        .expect("read product materialization");
    assert!(customer.contains(r#""version":1"#));
    assert!(product.contains(r#""version":1"#));
}
