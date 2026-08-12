// Binary-crate modules don't have a public surface; the lint exists for
// library hygiene and fires on every `pub` item we introduce. Tools
// reading these as a library is a Phase 1+ concern.
#![allow(unreachable_pub)]

//! VentStream engine entrypoint.
//!
//! The binary supports two independent pipelines that can be enabled
//! together or separately via `VS_ROLES`:
//!
//! - `cdc`: Postgres logical replication → optional join engine → sink
//!   (currently OpenSearch). Single-leader by design — replication slots
//!   are single-consumer.
//! - `ws`: NATS bus subscriber → WebSocket fan-out to connected clients.
//!   Horizontally scalable — every pod can run this role.
//!
//! Common deployment shapes:
//! - `VS_ROLES=cdc` on a 1-active + N-standby StatefulSet (Pipeline A)
//! - `VS_ROLES=ws` on a HPA-scaled Deployment (Pipeline B)
//! - `VS_ROLES=cdc,ws` for single-node demos and small-tenant deployments

// Global allocator: jemalloc. Tuned at runtime via the `_RJEM_MALLOC_CONF`
// env var (set in the image — note the `_RJEM_` prefix: tikv-jemalloc
// builds jemalloc prefixed, so the unprefixed `MALLOC_CONF` is ignored):
//   `background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:1000`
// `background_thread` runs a thread that returns freed pages to the OS off
// the hot path (Linux only — no-op on macOS), so RSS tracks the live
// working set rather than sticking at the high-water mark. That matters
// for the WS gateway, where connection churn frees large amounts the
// system allocator would otherwise retain. Config lives in env (not an
// exported `malloc_conf` symbol) because the workspace forbids unsafe.
// Verified: 5000 WS conns ~842 MiB → ~30 MiB within ~5s of disconnect.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod admin;
mod dispatcher;
mod dlq;
mod engine;
mod fleet_config;
mod health;
mod mcp;
mod memory_controller;
mod mysql_sql_denormalize;
mod sql_denormalize;
mod yaml_fingerprint;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use ventstream_config::{
    BootstrapMode as ConfigBootstrapMode, EngineConfig as EngineFileConfig,
    JetStreamStorage as ConfigJetStreamStorage, KafkaUnwrapMode as ConfigKafkaUnwrapMode,
    LogFormat as ConfigLogFormat, MeilisearchIndexRoutingConfig as FileMeilisearchIndexRouting,
    MongodbFullDocumentMode as ConfigMongodbFullDocumentMode, OpenSearchAuthConfig,
    OpenSearchIndexRouting, RealtimeBrokerProvider,
    RedisAcknowledgementConfig as FileRedisAcknowledgement, RedisAuthConfig as FileRedisAuth,
    RedisContractConfig as FileRedisContract, RedisDocumentFormatConfig as FileRedisDocumentFormat,
    RedisKeyRoutingConfig as FileRedisKeyRouting,
    RedisKeyspaceOwnershipConfig as FileRedisKeyspaceOwnership,
    RedisTlsConfig as FileRedisTlsConfig, RedisTopologyConfig as FileRedisTopology,
    RedisViewConditionConfig as FileRedisViewCondition,
    RedisViewFilterModeConfig as FileRedisViewFilterMode,
    RedisViewMissingBehaviorConfig as FileRedisViewMissingBehavior,
    RedisViewValueConfig as FileRedisViewValue, Role as ConfigRole, SinkKind, SourceKind,
    SqlDenormalizeMode, TlsConfig as FileTlsConfig, TlsMode as FileTlsMode,
    TlsTrustProvider as FileTlsTrustProvider, ValueRef,
};
use ventstream_core::{MemoryAdmission, ReadinessSignal, ShutdownToken};
use ventstream_graphql::GraphQlConfig;
use ventstream_joins::{JoinDefinition, JoinEngine, JoinState, PersistentBackend};
use ventstream_sinks::opensearch::{AuthMode, OpenSearchConfig, OpenSearchSink};
use ventstream_sinks::{
    MeilisearchConfig, MeilisearchIndexRouting, MeilisearchSettings, MeilisearchSink,
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDocumentFormat, RedisKeyRouting,
    RedisKeyspaceOwnership, RedisSentinelTopology, RedisSink, RedisTlsConfig, RedisTopology,
    RedisView, RedisViewCondition, RedisViewConditionOperator, RedisViewFilter,
    RedisViewFilterMode, RedisViewKey, RedisViewMissingBehavior, RedisViewSource, RedisViewValue,
};
use ventstream_sources::kafka::{KafkaCdcConfig, KafkaCdcSource, UnwrapMode};
use ventstream_sources::mongodb::{
    FullDocument as MongoFullDocument, MongoCdcConfig, MongoCdcSource,
};
use ventstream_sources::mysql::{MySqlCdcConfig, MySqlCdcSource, MySqlFetcher};
use ventstream_sources::neo4j::{
    analyze_specs as analyze_neo4j_specs, DenormalizeSpecs as Neo4jDenormalizeSpecs,
    Neo4jBootstrap, Neo4jCdcConfig, Neo4jCdcSource,
};
use ventstream_sources::postgres::{
    PostgresCdcConfig, PostgresCdcSource, PostgresFetcher, SnapshotBootstrap, SnapshotTable,
};
use ventstream_sources::{
    materialize_provider_ca_bundle, DatabaseTlsConfig, DatabaseTlsMode, DatabaseTlsTrustProvider,
};
use ventstream_ws::{JetStreamConfig, RedisStreamsConfig, WsConfig};

use crate::dispatcher::DispatcherConfig;
use crate::engine::{spawn_signal_handler, Engine, EngineConfig};
use crate::fleet_config::FleetAppliedConfig;
use crate::memory_controller::{MemoryControllerConfig, MemoryRuntime};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Role {
    Cdc,
    Ws,
    GraphQl,
    Mcp,
}

impl Role {
    fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cdc" => Ok(Self::Cdc),
            "ws" => Ok(Self::Ws),
            "graphql" => Ok(Self::GraphQl),
            "mcp" => Ok(Self::Mcp),
            other => Err(anyhow!(
                "unknown role '{other}' (expected 'cdc', 'ws', 'graphql', or 'mcp')"
            )),
        }
    }
}

fn run_healthcheck(address: &str, path: &str) -> Result<()> {
    use std::io::{BufRead as _, Write as _};
    use std::net::{TcpStream, ToSocketAddrs as _};

    if !path.starts_with('/') {
        return Err(anyhow!("healthcheck path must start with '/': {path}"));
    }

    let endpoint = address
        .to_socket_addrs()
        .with_context(|| format!("resolving healthcheck address {address}"))?
        .next()
        .ok_or_else(|| anyhow!("healthcheck address resolved to no endpoints: {address}"))?;
    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&endpoint, timeout)
        .with_context(|| format!("connecting to health endpoint {address}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .context("setting healthcheck read timeout")?;
    stream
        .set_write_timeout(Some(timeout))
        .context("setting healthcheck write timeout")?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .context("writing healthcheck request")?;

    let mut status_line = String::new();
    std::io::BufReader::new(stream)
        .read_line(&mut status_line)
        .context("reading healthcheck response")?;
    let mut parts = status_line.split_whitespace();
    let protocol = parts.next().unwrap_or_default();
    let status = parts
        .next()
        .ok_or_else(|| anyhow!("health endpoint returned an invalid HTTP status line"))?
        .parse::<u16>()
        .context("parsing health endpoint status")?;
    if !protocol.starts_with("HTTP/") || !(200..300).contains(&status) {
        return Err(anyhow!(
            "health endpoint returned an unsuccessful status: {}",
            status_line.trim()
        ));
    }
    println!("healthy: {address}{path} ({status})");
    Ok(())
}

fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    match argv.as_slice() {
        [_, flag] if flag == "--version" || flag == "-V" => {
            println!("ventstream {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        [_, flag, address, path] if flag == "--healthcheck" => {
            return run_healthcheck(address, path);
        }
        _ if argv.iter().any(|arg| arg == "--healthcheck") => {
            return Err(anyhow!("usage: ventstream --healthcheck ADDRESS /PATH"));
        }
        _ => {}
    }

    // `ventstream mcp` speaks JSON-RPC on stdout; logs must go to stderr,
    // so this path installs its own subscriber and skips install_tracing.
    if argv.get(1).map(String::as_str) == Some("mcp") {
        install_stderr_tracing();
        install_crypto_provider();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("ventstream-worker")
            .build()
            .context("building tokio runtime")?;
        return runtime.block_on(mcp::run(&argv));
    }

    let early_engine_config = load_engine_config_from_env()?;
    // Build the telemetry trace layer BEFORE installing tracing so it's part
    // of the subscriber stack. Previously the layer was created later (after
    // `.init()`) and dropped, so the trace-export path ran with permanently
    // empty batches (M14). `build_telemetry` needs no runtime; the export loop
    // is spawned inside `run()` once the runtime exists.
    let (telemetry_layer, telemetry_handle) = match ventstream_telemetry::build_telemetry() {
        Some((layer, handle)) => (Some(layer), Some(handle)),
        None => (None, None),
    };
    install_tracing(
        telemetry_layer,
        early_engine_config
            .as_ref()
            .and_then(|config| config.runtime.log_format),
    );
    install_crypto_provider();

    // Top-level dispatch. `--analyze-denormalize <yaml>` runs the
    // hop-depth linter and exits without spinning up tokio or any
    // network roles. Anything else falls through to the normal
    // pipeline entrypoint.
    if let Some(idx) = argv.iter().position(|a| a == "--analyze-denormalize") {
        let yaml_path = argv
            .get(idx + 1)
            .ok_or_else(|| anyhow!("--analyze-denormalize requires a YAML path"))?;
        return run_analyze_denormalize(yaml_path);
    }
    let drain_local_state = argv.iter().any(|arg| arg == "--fleet-drain-local-state");
    let fleet_reconcile = argv.iter().any(|arg| arg == "--fleet-reconcile");
    let fleet_rebootstrap = argv.iter().any(|arg| arg == "--fleet-rebootstrap");
    let validate_config = argv.iter().any(|arg| arg == "--validate-config");
    let check_redis_sink = argv.iter().any(|arg| arg == "--check-redis-sink");
    let check_redis_drift = argv.iter().any(|arg| arg == "--check-redis-drift");
    let redis_drift_targets = repeated_argument_values(&argv, "--redis-target")?;
    let redis_drift_scan_limit =
        optional_usize_argument(&argv, "--redis-drift-scan-limit", 100_000)?;
    let fleet_delete_orphans = argv.iter().any(|arg| arg == "--delete-orphans");
    let one_shot_command_count = [
        drain_local_state,
        fleet_reconcile,
        fleet_rebootstrap,
        validate_config,
        check_redis_sink,
        check_redis_drift,
    ]
    .into_iter()
    .filter(|enabled| *enabled)
    .count();
    if one_shot_command_count > 1 {
        return Err(anyhow!(
            "only one maintenance or validation command may be supplied at a time"
        ));
    }

    info!(version = env!("CARGO_PKG_VERSION"), "ventstream booting");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ventstream-worker")
        .build()
        .context("building tokio runtime")?;
    if validate_config {
        validate_startup_config(early_engine_config.as_ref())
    } else if check_redis_sink {
        runtime.block_on(run_check_redis_sink(early_engine_config.as_ref()))
    } else if check_redis_drift {
        runtime.block_on(run_check_redis_drift(
            early_engine_config.as_ref(),
            redis_drift_targets,
            redis_drift_scan_limit,
        ))
    } else if drain_local_state {
        runtime.block_on(run_fleet_drain_local_state())
    } else if fleet_reconcile {
        runtime.block_on(run_fleet_reconcile(fleet_delete_orphans))
    } else if fleet_rebootstrap {
        runtime.block_on(run_fleet_rebootstrap())
    } else {
        runtime.block_on(run(telemetry_handle, early_engine_config))
    }
}

fn repeated_argument_values(argv: &[String], flag: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut arguments = argv.iter().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == flag {
            let value = arguments
                .next()
                .filter(|value| !value.starts_with("--"))
                .ok_or_else(|| anyhow!("{flag} requires a value"))?;
            values.push(value.clone());
        }
    }
    Ok(values)
}

fn optional_usize_argument(argv: &[String], flag: &str, default: usize) -> Result<usize> {
    let values = repeated_argument_values(argv, flag)?;
    match values.as_slice() {
        [] => Ok(default),
        [value] => value
            .parse::<usize>()
            .with_context(|| format!("{flag} must be a positive integer")),
        _ => Err(anyhow!("{flag} may only be supplied once")),
    }
}

async fn run_check_redis_sink(engine_config: Option<&EngineFileConfig>) -> Result<()> {
    let sink = load_sink_config(engine_config)?;
    let SinkRuntimeConfig::Redis(config) = sink else {
        return Err(anyhow!(
            "--check-redis-sink requires sink.kind=redis or VS_SINK=redis"
        ));
    };
    let report = RedisSink::diagnose(*config)
        .await
        .context("Redis sink preflight failed")?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("encoding Redis sink preflight report")?
    );
    Ok(())
}

async fn run_check_redis_drift(
    engine_config: Option<&EngineFileConfig>,
    mut targets: Vec<String>,
    scan_limit: usize,
) -> Result<()> {
    let sink = load_sink_config(engine_config)?;
    let SinkRuntimeConfig::Redis(config) = sink else {
        return Err(anyhow!(
            "--check-redis-drift requires sink.kind=redis or VS_SINK=redis"
        ));
    };
    if targets.is_empty() {
        targets = match &config.key_routing {
            RedisKeyRouting::Fixed(target) => vec![target.clone()],
            RedisKeyRouting::Views(views) => views.iter().map(|view| view.name.clone()).collect(),
            RedisKeyRouting::ByOutputRelation | RedisKeyRouting::ByProjectionTarget => {
                return Err(anyhow!(
                    "dynamic Redis routing requires one or more --redis-target values"
                ));
            }
        };
    }
    let report = RedisSink::inspect_drift(*config, &targets, scan_limit)
        .await
        .context("Redis drift inspection failed")?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("encoding Redis drift report")?
    );
    Ok(())
}

/// Resolve the complete startup configuration without opening connector sockets.
fn validate_startup_config(engine_config: Option<&EngineFileConfig>) -> Result<()> {
    let fleet_config = fleet_config::load_from_env()?;
    let config = PipelineEnv::load(fleet_config.as_ref(), engine_config)?;
    let mut roles: Vec<&str> = config
        .roles
        .iter()
        .map(|role| match role {
            Role::Cdc => "cdc",
            Role::Ws => "ws",
            Role::GraphQl => "graphql",
            Role::Mcp => "mcp",
        })
        .collect();
    roles.sort_unstable();
    println!("configuration valid: roles={}", roles.join(","));
    Ok(())
}

/// Idempotently release connector resume state without starting a data path.
///
/// This command is intentionally local-only: the Fleet supervisor invokes it
/// after the engine child has stopped, so cursor files and embedded state have
/// no live process handles. It does not contact the Fleet control plane.
async fn run_fleet_drain_local_state() -> Result<()> {
    let fleet_config = fleet_config::load_from_env()?;
    let engine_config = load_engine_config_from_env()?;
    let roles = load_runtime_roles(engine_config.as_ref())?;
    if !roles.contains(&Role::Cdc) {
        info!("fleet drain requested for a realtime-only deployment; no cursor state to release");
        return Ok(());
    }

    let cdc = load_cdc_bundle(fleet_config.as_ref(), engine_config.as_ref())?;
    let redis_targets = cdc.validate_redis_drain().await?;
    match cdc.source {
        CdcSourceConfig::Postgres(config) => {
            drain_pg_local_state(&config, engine_config.as_ref()).await?
        }
        CdcSourceConfig::Neo4j(config) => drain_neo4j_local_state(&config)?,
        CdcSourceConfig::Mongo(config) => drain_mongodb_local_state(&config)?,
        CdcSourceConfig::Mysql(config) => drain_mysql_local_state(&config, engine_config.as_ref())?,
        CdcSourceConfig::Kafka(_) => {
            info!("fleet drain: kafka offsets are server-side; no local state to release");
        }
    }
    cdc.runtime
        .sink
        .reset_redis_targets_if_configured(&redis_targets)
        .await?;
    info!("fleet-managed local drain completed");
    Ok(())
}

/// Run a one-shot source/sink reconciliation command for the Fleet supervisor.
///
/// The current reconcilers are destructive orphan-delete passes, so the command
/// is intentionally a no-op unless Fleet passes `--delete-orphans`.
async fn run_fleet_reconcile(delete_orphans: bool) -> Result<()> {
    let fleet_config = fleet_config::load_from_env()?;
    let engine_config = load_engine_config_from_env()?;
    if !delete_orphans {
        info!("fleet reconcile requested without --delete-orphans; no cleanup performed");
        return Ok(());
    }

    let roles = load_runtime_roles(engine_config.as_ref())?;
    if !roles.contains(&Role::Cdc) {
        info!(
            "fleet reconcile requested for a realtime-only deployment; no cdc state to reconcile"
        );
        return Ok(());
    }

    let cdc = load_cdc_bundle(fleet_config.as_ref(), engine_config.as_ref())?;
    if cdc.runtime.sink.redis().is_some() {
        return Err(anyhow!(
            "Redis live-set reconciliation is not implemented; use an explicit rebootstrap for an exclusively owned keyspace"
        ));
    }
    let deleted = match cdc.source {
        CdcSourceConfig::Postgres(config) => {
            let os = cdc.runtime.sink.open_search().ok_or_else(|| {
                anyhow!("Redis orphan reconciliation is not available in this release")
            })?;
            reconcile_orphan_docs_pg(&config, os, &cdc.joins).await?
        }
        CdcSourceConfig::Neo4j(config) => {
            let os = cdc.runtime.sink.open_search().ok_or_else(|| {
                anyhow!("Redis orphan reconciliation is not available in this release")
            })?;
            reconcile_orphan_docs_neo4j(&config, os).await?
        }
        CdcSourceConfig::Mongo(_) => {
            info!("fleet reconcile: mongodb live-set reconciliation is not implemented yet");
            0
        }
        CdcSourceConfig::Mysql(_) => {
            info!("fleet reconcile: mysql live-set reconciliation is not implemented yet");
            0
        }
        CdcSourceConfig::Kafka(_) => {
            info!("fleet reconcile: kafka tombstones are handled from the stream; no local pass");
            0
        }
    };
    info!(
        orphans_deleted = deleted,
        "fleet-managed reconciliation completed"
    );
    Ok(())
}

/// Prepare this pipeline for a full rebuild on the next normal engine start.
///
/// For cursor-file backends, removing local state is enough. Postgres also
/// needs Fleet to set `VS_FLEET_FORCE_BOOTSTRAP=1` on the next child start so
/// the freshly-created slot is preceded by a table snapshot.
async fn run_fleet_rebootstrap() -> Result<()> {
    let fleet_config = fleet_config::load_from_env()?;
    let engine_config = load_engine_config_from_env()?;
    let roles = load_runtime_roles(engine_config.as_ref())?;
    if !roles.contains(&Role::Cdc) {
        info!(
            "fleet rebootstrap requested for a realtime-only deployment; no cdc state to rebuild"
        );
        return Ok(());
    }

    let cdc = load_cdc_bundle(fleet_config.as_ref(), engine_config.as_ref())?;
    let redis_targets = cdc.validate_redis_drain().await?;
    match cdc.source {
        CdcSourceConfig::Postgres(config) => {
            drain_pg_local_state(&config, engine_config.as_ref()).await?;
            info!(
                "fleet rebootstrap prepared postgres state; next start must set VS_FLEET_FORCE_BOOTSTRAP=1"
            );
        }
        CdcSourceConfig::Neo4j(config) => drain_neo4j_local_state(&config)?,
        CdcSourceConfig::Mongo(config) => drain_mongodb_local_state(&config)?,
        CdcSourceConfig::Mysql(config) => drain_mysql_local_state(&config, engine_config.as_ref())?,
        CdcSourceConfig::Kafka(_) => {
            info!(
                "fleet rebootstrap: kafka offsets are server-side; reset the consumer group externally"
            );
        }
    }
    cdc.runtime
        .sink
        .reset_redis_targets_if_configured(&redis_targets)
        .await?;
    info!("fleet-managed rebootstrap preparation completed");
    Ok(())
}

/// rustls 0.23+ requires the application to install a process-level
/// CryptoProvider before any TLS handshake — neither neo4rs nor
/// reqwest does this on its own, so without this call the first TLS
/// connection (Aura bolt+s, HTTPS to OS, etc.) panics. We pick `ring`
/// because that's what `tokio-rustls` is built with in our workspace.
/// `install_default` returns Err if a provider is already installed —
/// safe to ignore so a transitive dep that also installed `ring`
/// doesn't crash us.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Inspect a denormalize YAML, infer the maximum hop depth each
/// spec's Cypher actually walks, compare it against the spec's
/// `fan_out_max_hops`, and print a report. Exit 1 if any spec is
/// under-configured (so this can gate CI).
fn run_analyze_denormalize(yaml_path: &str) -> Result<()> {
    let path = PathBuf::from(yaml_path);
    let specs = Neo4jDenormalizeSpecs::from_yaml_file(&path)
        .with_context(|| format!("loading denormalize YAML at {yaml_path}"))?;
    if specs.is_empty() {
        println!("(no denormalize specs in {yaml_path})");
        return Ok(());
    }
    let rows = analyze_neo4j_specs(&specs);
    println!(
        "{:<20}  {:>10}  {:>10}  status",
        "primary", "configured", "inferred"
    );
    println!("{}", "─".repeat(60));
    let mut any_warn = false;
    for r in &rows {
        let status = if r.warn_too_low {
            any_warn = true;
            format!(
                "WARN — bump fan_out_max_hops to at least {}",
                r.inferred_min_hops
            )
        } else if r.inferred_min_hops < r.configured_hops {
            "ok (wider than needed — costs CPU, no correctness impact)".to_owned()
        } else {
            "ok".to_owned()
        };
        println!(
            "{:<20}  {:>10}  {:>10}  {}",
            r.primary_label, r.configured_hops, r.inferred_min_hops, status
        );
    }
    if any_warn {
        std::process::exit(1);
    }
    Ok(())
}

async fn run(
    telemetry: Option<ventstream_telemetry::TelemetryHandle>,
    engine_config: Option<EngineFileConfig>,
) -> Result<()> {
    let fleet_config = fleet_config::load_from_env()?;
    // Fleet supervisor contract: the engine child is spawned with NO argv.
    // Role selection comes from the loaded config (VS_ENGINE_CONFIG, which
    // the supervisor points at the staged ventstream.yaml of the applied
    // fleet envelope), readiness is polled on /readyz of VS_HEALTH_LISTEN,
    // and shutdown arrives as "shutdown\n" on stdin. Config validation
    // guarantees mcp is a solo role, so this branch owns the process.
    if load_runtime_roles(engine_config.as_ref())?.contains(&Role::Mcp) {
        let shutdown = ShutdownToken::new();
        let _signal_handle = spawn_signal_handler(shutdown.clone());
        let _supervisor_handle = bool_env("VS_FLEET_SUPERVISED", false)
            .then(|| spawn_supervisor_stdin_handler(shutdown.clone()));
        return mcp::run_role(engine_config, shutdown).await;
    }
    let mut cfg = PipelineEnv::load(fleet_config.as_ref(), engine_config.as_ref())?;
    info!(roles = ?cfg.roles, "pipeline roles configured");

    let shutdown = ShutdownToken::new();
    let _signal_handle = spawn_signal_handler(shutdown.clone());
    let _supervisor_handle = bool_env("VS_FLEET_SUPERVISED", false)
        .then(|| spawn_supervisor_stdin_handler(shutdown.clone()));

    // Wire telemetry to the control plane (opt-in via env vars). The
    // counters are shared globally so source/dispatcher/etc. can bump
    // them with zero allocation; when no `VS_CONTROL_PLANE_URL` is set
    // the global stays unset and the bumps no-op.
    let telemetry_counters = ventstream_telemetry::TelemetryCounters::new();
    ventstream_telemetry::set_global_counters(std::sync::Arc::clone(&telemetry_counters));
    // Spawn the export loop now that the runtime exists. The trace LAYER was
    // already wired into the subscriber back in `main()` (M14) — this just
    // starts the consumer that drains sampled traces to the control plane.
    if let Some(handle) = telemetry {
        handle.spawn(telemetry_counters, shutdown.clone());
    }

    // Spawn each enabled role on its own supervised task. They share
    // the shutdown token but their data paths are independent — a
    // failure in one cancels the other only because the token fires.
    let mut handles = Vec::new();

    // Traffic-role readiness starts false. WS / GraphQL flip their signal
    // only after dependencies are initialized and their listener is bound.
    // The health server starts first and aggregates the enabled role signals.
    let ws_role_readiness = ReadinessSignal::new();
    let graphql_role_readiness = ReadinessSignal::new();
    let ws_enabled = cfg.ws.is_some();
    let graphql_enabled = cfg.graphql.is_some();
    let cdc_sink_health = cfg.cdc.as_mut().map(|cdc| {
        let health = ventstream_core::SinkHealth::new();
        cdc.runtime.sink.attach_health(health.clone());
        health
    });

    // Shared established-connection counter for the WS gateway. The gateway
    // mutates it; the health server reads it for capacity readiness.
    let ws_active = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Readiness trips at 90% of the connection cap — *below* the hard
    // 503 reject threshold on purpose. The LB stops routing new
    // connections to the pod while it still has ~10% headroom, so by the
    // time it would actually reject, new traffic is already being sent
    // elsewhere. The hard cap then only fires in the small window before
    // the readiness change propagates. Existing connections are never
    // touched by either. Only armed when the ws role is on AND a cap is
    // configured.
    let ws_capacity = cfg.ws.as_ref().and_then(|w| w.max_connections).map(|max| {
        (
            std::sync::Arc::clone(&ws_active),
            // 90% of cap, but at least 1 and never the cap itself.
            ((max * 9) / 10).clamp(1, max.saturating_sub(1).max(1)),
        )
    });
    let readiness = health::ReadinessGate::new(
        ws_enabled.then(|| ws_role_readiness.clone()),
        graphql_enabled.then(|| graphql_role_readiness.clone()),
        ws_capacity,
        cdc_sink_health,
    );

    // Single, always-on health server shared by every role — one
    // `/healthz` + `/readyz` on `VS_HEALTH_LISTEN` (default 0.0.0.0:4043)
    // is the canonical probe target regardless of which roles run (cdc,
    // ws, graphql, or any combo). Tied to the outer shutdown so it stays
    // up across pause/resume. A bind failure is logged but does NOT take
    // the data pipeline down — k8s will notice the missing probe.
    // A malformed VS_HEALTH_LISTEN must NOT take the pipeline down — the bind
    // is already soft-failure, and the *parse* must match that stance (M15).
    // Skip the health server with a warning instead of propagating `?`.
    let health_listen: Option<SocketAddr> =
        if cfg.cdc.is_some() || cfg.ws.is_some() || cfg.graphql.is_some() {
            resolve_health_listen(engine_config.as_ref())
        } else {
            None
        };
    if let Some(listen) = health_listen {
        // Install the Prometheus recorder so /metrics can render. A
        // failure here just disables /metrics — it must not stop the
        // pipeline (matches the health server's soft-failure stance).
        let prometheus = match ventstream_telemetry::install_prometheus() {
            Ok(handle) => Some(handle),
            Err(err) => {
                warn!(error = %err, "Prometheus recorder unavailable; /metrics disabled");
                None
            }
        };
        let shutdown = shutdown.clone();
        let readiness = readiness.clone();
        handles.push(tokio::spawn(async move {
            if let Err(err) = health::run(listen, prometheus, readiness, shutdown).await {
                error!(error = %err, "health server stopped; continuing without it");
            }
        }));
    }

    // Terminal cdc failures (e.g. a credential error that exhausted its
    // budget) must exit the process nonzero so the supervisor restarts
    // the pod; the slot carries the error past the task join below.
    let cdc_failure: std::sync::Arc<parking_lot::Mutex<Option<anyhow::Error>>> =
        std::sync::Arc::default();

    if let Some(cdc) = cfg.cdc {
        let shutdown = shutdown.clone();
        let cdc_failure = std::sync::Arc::clone(&cdc_failure);
        handles.push(tokio::spawn(async move {
            let result = match cdc.source {
                CdcSourceConfig::Postgres(pg) => {
                    run_cdc_postgres(
                        *pg,
                        cdc.runtime,
                        cdc.joins,
                        cdc.joins_yaml_text,
                        shutdown.clone(),
                    )
                    .await
                }
                CdcSourceConfig::Neo4j(n4j) => {
                    run_cdc_neo4j(
                        *n4j,
                        cdc.runtime.sink,
                        cdc.runtime.engine_config,
                        shutdown.clone(),
                    )
                    .await
                }
                CdcSourceConfig::Mongo(mongo) => {
                    run_cdc_mongodb(
                        *mongo,
                        cdc.runtime.sink,
                        cdc.runtime.engine_config,
                        shutdown.clone(),
                    )
                    .await
                }
                CdcSourceConfig::Mysql(my) => {
                    run_cdc_mysql(*my, cdc.runtime, cdc.joins, shutdown.clone()).await
                }
                CdcSourceConfig::Kafka(k) => {
                    run_cdc_kafka(
                        *k,
                        cdc.runtime.sink,
                        cdc.runtime.engine_config,
                        shutdown.clone(),
                    )
                    .await
                }
            };
            if let Err(err) = result {
                error!(error = %format!("{err:#}"), "cdc pipeline failed");
                *cdc_failure.lock() = Some(err);
                shutdown.cancel();
            }
        }));
    }

    if let Some(ws_cfg) = cfg.ws {
        let shutdown = shutdown.clone();
        let active = std::sync::Arc::clone(&ws_active);
        let readiness = ws_role_readiness;
        handles.push(tokio::spawn(async move {
            if let Err(err) =
                ventstream_ws::run_with_readiness(ws_cfg, shutdown.clone(), active, readiness).await
            {
                error!(error = %err, "ws pipeline failed");
                shutdown.cancel();
            }
        }));
    }

    if let Some(gql_cfg) = cfg.graphql {
        let shutdown = shutdown.clone();
        let readiness = graphql_role_readiness;
        handles.push(tokio::spawn(async move {
            if let Err(err) =
                ventstream_graphql::run_with_readiness(gql_cfg, shutdown.clone(), readiness).await
            {
                error!(error = %err, "graphql pipeline failed");
                shutdown.cancel();
            }
        }));
    }

    if handles.is_empty() {
        return Err(anyhow!(
            "VS_ROLES is empty; nothing to run (set to 'cdc', 'ws', 'graphql', or a comma-list)"
        ));
    }

    for handle in handles {
        if let Err(err) = handle.await {
            warn!(error = %err, "pipeline task join error");
        }
    }
    if let Some(err) = cdc_failure.lock().take() {
        return Err(err.context("cdc pipeline failed"));
    }
    info!("ventstream stopped");
    Ok(())
}

/// Resolve the shared health listener address. Parse failures disable
/// the health server with a warning; they never stop the data path.
fn resolve_health_listen(engine_config: Option<&EngineFileConfig>) -> Option<SocketAddr> {
    let listen_str = engine_config
        .and_then(|config| config.runtime.health_listen.clone())
        .unwrap_or_else(|| {
            std::env::var("VS_HEALTH_LISTEN").unwrap_or_else(|_| "0.0.0.0:4043".to_string())
        });
    match listen_str.parse::<SocketAddr>() {
        Ok(addr) => Some(addr),
        Err(err) => {
            warn!(
                value = %listen_str,
                error = %err,
                "VS_HEALTH_LISTEN is not a valid host:port; health/metrics server disabled (pipeline continues)"
            );
            None
        }
    }
}

/// Accept one private lifecycle command from the parent process.
///
/// EOF also stops the engine so a supervisor crash cannot leave an orphaned
/// data process running without its workload identity or control stream.
fn spawn_supervisor_stdin_handler(shutdown: ShutdownToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        wait_for_supervisor_shutdown(BufReader::new(stdin)).await;
        info!("fleet supervisor requested engine shutdown");
        shutdown.cancel();
    })
}

async fn wait_for_supervisor_shutdown<R>(mut reader: R)
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => return,
            Ok(_) if line.trim() == "shutdown" => return,
            Ok(_) => warn!("ignored unknown fleet supervisor command"),
            Err(err) => {
                warn!(error = %err, "fleet supervisor stdin failed; stopping engine");
                return;
            }
        }
    }
}

/// Outcome of one engine iteration inside [`run_cdc_postgres`]'s
/// pause-aware loop. The orchestrator uses this to decide whether
/// to terminate or restart. Setup / engine errors are surfaced via
/// `Result::Err` instead of a third variant.
enum EngineIterationOutcome {
    /// Outer shutdown was signalled; the process should exit.
    Shutdown,
    /// An operator pause arrived; the orchestrator should idle until
    /// the control plane reports `pause = false`, then rebuild.
    Paused,
}

/// Construct the configured sink. `VS_SINK` selects the target backend
/// (default `opensearch`). Adding a target = implement
/// `ventstream_core::Sink` and add a match arm here; the rest of the
/// pipeline is sink-agnostic (it only ever sees `Arc<dyn Sink>`).
async fn build_sink(
    config: SinkRuntimeConfig,
    shutdown: &ShutdownToken,
) -> Result<std::sync::Arc<dyn ventstream_core::Sink>> {
    match config {
        SinkRuntimeConfig::OpenSearch(os) => Ok(std::sync::Arc::new(
            OpenSearchSink::new(*os).context("building OpenSearch sink")?,
        )),
        SinkRuntimeConfig::Redis(redis) => Ok(std::sync::Arc::new(
            RedisSink::connect_with_shutdown(*redis, shutdown)
                .await
                .context("building Redis sink")?,
        )),
        SinkRuntimeConfig::Meilisearch(meili) => Ok(std::sync::Arc::new(
            MeilisearchSink::connect_with_shutdown(*meili, shutdown)
                .await
                .context("building Meilisearch sink")?,
        )),
    }
}

/// One CDC source plugged into the shared orchestrator. A backend owns
/// its source/transform/sink config and exposes the four
/// source-specific steps the pause/resume/drain loop dispatches to;
/// everything else (the loop, pause handling, the bootstrap-after-drain
/// decision) lives once in [`run_cdc_loop`]. Add a source by
/// implementing this trait and handing the backend to `run_cdc_loop`.
#[async_trait::async_trait]
trait CdcBackend: Send {
    /// Reject a drain before any cursor or local state is removed when the
    /// configured sink cannot be rebuilt safely.
    async fn validate_drain(&self) -> Result<()>;
    /// Drop local resume state (PG slot bookkeeping / Neo4j cursor file)
    /// so the next iteration re-bootstraps cleanly.
    async fn drain_local(&self) -> Result<()>;
    /// Sweep OS docs whose source rows no longer exist (deletes missed
    /// during a drained window). No-op when there's nothing to reconcile.
    async fn reconcile_orphans(&self) -> Result<()>;
    /// Force a bootstrap on the next iteration after a drain. PG flips
    /// snapshot mode on; Neo4j re-bootstraps implicitly once its cursor
    /// file is gone, so it's a no-op there.
    fn prepare_bootstrap(&mut self) -> Result<()>;
    /// Build + run the engine for one iteration until it completes
    /// (shutdown) or is cancelled by a pause.
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome>;
}

/// The pause/resume/drain orchestrator, shared by every source. Each
/// iteration either idles (paused) or builds + runs the engine; on a
/// drain-resume it reconciles orphans and forces a bootstrap. All
/// source specifics are dispatched through [`CdcBackend`], so operators
/// get one consistent pause/resume/drain UX across backends.
async fn run_cdc_loop<B: CdcBackend>(mut backend: B, shutdown: ShutdownToken) -> Result<()> {
    // Iteration-level retry discipline, matching the tail reconnect: an
    // engine that cancels its own child token on an internal failure used
    // to be indistinguishable from a pause and was retried instantly
    // forever (observed at ~5 attempts/s on a stale MySQL password).
    const ITERATION_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
    const ITERATION_MAX_BACKOFF: Duration = Duration::from_secs(30);
    const ITERATION_HEALTHY_THRESHOLD: Duration = Duration::from_secs(30);

    let mut already_drained_locally = false;
    let mut last_iteration_error: Option<anyhow::Error> = None;
    let mut iteration_backoff = ITERATION_INITIAL_BACKOFF;
    let mut credential_budget = ventstream_sources::credential::CredentialFailureBudget::new();

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if let Some(err) = last_iteration_error.take() {
            ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Stopped);
            return Err(err);
        }

        let cmd = ventstream_telemetry::latest_command().unwrap_or_default();

        if cmd.pause {
            // Paused with a valid cursor → idle; paused with
            // cursor_invalidated → drain locally + idle as Drained.
            if cmd.cursor_invalidated && !already_drained_locally {
                backend.validate_drain().await?;
                backend
                    .drain_local()
                    .await
                    .context("pause-drain failed; local state was not safely reset")?;
                already_drained_locally = true;
                ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Drained);
            } else if !already_drained_locally {
                ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Paused);
            }
            // Re-evaluate when pause flips off, or when server-side
            // auto-drain sets cursor_invalidated while we're still paused.
            wait_for_command_change(&shutdown, |c| {
                !c.pause || (c.cursor_invalidated && !already_drained_locally)
            })
            .await;
            continue;
        }

        // Not paused. If we drained during the pause window (or the
        // server reports cursor_invalidated for a process that restarted
        // between drain + resume), reconcile orphans and bootstrap fresh.
        let needs_bootstrap_after_drain = already_drained_locally || cmd.cursor_invalidated;
        if needs_bootstrap_after_drain {
            if !already_drained_locally && cmd.cursor_invalidated {
                backend.validate_drain().await?;
                backend
                    .drain_local()
                    .await
                    .context("post-restart drain failed; refusing an unsafe bootstrap")?;
            }
            backend
                .reconcile_orphans()
                .await
                .context("drain-resume reconciliation failed; refusing an unsafe bootstrap")?;
            backend.prepare_bootstrap()?;
            already_drained_locally = false;
        }

        // Child shutdown for this iteration: a pause cancels it without
        // touching the outer token, so the engine returns cleanly while
        // the orchestrator keeps running.
        let inner_shutdown = shutdown.child();
        let (pause_watcher, pause_requested) = spawn_pause_watcher(inner_shutdown.clone());

        let iteration_started = std::time::Instant::now();
        let iteration_outcome = backend
            .run_iteration(inner_shutdown.clone(), shutdown.clone())
            .await;

        pause_watcher.abort();

        match iteration_outcome {
            Ok(EngineIterationOutcome::Shutdown) => break,
            Ok(EngineIterationOutcome::Paused) => continue,
            Err(_) if shutdown.is_cancelled() => break,
            // Graceful pause: the watcher cancelled the child token.
            Err(_) if pause_requested.load(std::sync::atomic::Ordering::Acquire) => continue,
            // The engine cancelled its own child token on an internal
            // failure. Crash-fast texts and an exhausted credential
            // budget are terminal; everything else retries with backoff.
            Err(err) if inner_shutdown.is_cancelled() => {
                if iteration_started.elapsed() >= ITERATION_HEALTHY_THRESHOLD {
                    credential_budget.record_success();
                    iteration_backoff = ITERATION_INITIAL_BACKOFF;
                }
                let text = format!("{err:#}");
                if ventstream_sources::credential::is_crash_fast_text(&text) {
                    last_iteration_error = Some(err);
                    continue;
                }
                if ventstream_sources::credential::is_credential_error_text(&text)
                    && credential_budget.record_credential_failure()
                {
                    error!(error = %text, "credential failure budget exhausted across engine iterations");
                    last_iteration_error = Some(anyhow!(
                        ventstream_sources::credential::exhausted_message(&text)
                    ));
                    continue;
                }
                let delay = jittered_iteration_backoff(iteration_backoff);
                warn!(
                    error = %text,
                    backoff_ms = delay.as_millis() as u64,
                    "engine iteration failed; retrying after backoff"
                );
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(delay) => {}
                }
                iteration_backoff = iteration_backoff
                    .saturating_mul(2)
                    .min(ITERATION_MAX_BACKOFF);
            }
            Err(err) => last_iteration_error = Some(err),
        }
    }

    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Stopped);
    Ok(())
}

/// Postgres source plugged into [`run_cdc_loop`].
struct PgBackend {
    pg: PostgresCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
}

#[async_trait::async_trait]
impl CdcBackend for PgBackend {
    async fn validate_drain(&self) -> Result<()> {
        let targets =
            postgres_redis_drain_targets(&self.runtime.sink, &self.pg, &self.joins).await?;
        self.runtime.sink.validate_redis_drain(true, &targets)
    }

    async fn drain_local(&self) -> Result<()> {
        drain_pg_local_state(&self.pg, self.runtime.engine_file_config.as_ref()).await
    }
    async fn reconcile_orphans(&self) -> Result<()> {
        let targets =
            postgres_redis_drain_targets(&self.runtime.sink, &self.pg, &self.joins).await?;
        if self
            .runtime
            .sink
            .reset_redis_targets_if_configured(&targets)
            .await?
        {
            return Ok(());
        }
        let os = self.runtime.sink.open_search().ok_or_else(|| {
            anyhow!("Redis orphan reconciliation is not available; drain-resume is blocked")
        })?;
        reconcile_orphan_docs_pg(&self.pg, os, &self.joins)
            .await
            .map(|_| ())
    }
    fn prepare_bootstrap(&mut self) -> Result<()> {
        // SQL-denormalize mode does its own SQL-join bootstrap; the source
        // must NOT run a table snapshot (and the slot is pre-created by the
        // engine). Leave bootstrap = None.
        if pg_sql_denormalize_enabled(self.runtime.engine_file_config.as_ref()) {
            return Ok(());
        }
        if self.pg.bootstrap.is_none() {
            let chunk_size: usize = postgres_bootstrap_chunk_size(
                self.runtime.engine_file_config.as_ref(),
                "VS_PG_BOOTSTRAP_CHUNK_SIZE",
                10_000,
            )?;
            self.pg = self.pg.clone().with_bootstrap(SnapshotBootstrap {
                tables: build_bootstrap_tables(&self.joins),
                chunk_size,
            });
        }
        Ok(())
    }
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome> {
        if pg_sql_denormalize_enabled(self.runtime.engine_file_config.as_ref()) {
            return build_and_run_pg_sql_denormalize_engine(
                self.pg.clone(),
                self.runtime.clone(),
                self.joins.clone(),
                inner,
                outer,
            )
            .await;
        }
        build_and_run_pg_engine(
            self.pg.clone(),
            self.runtime.clone(),
            self.joins.clone(),
            inner,
            outer,
        )
        .await
    }
}

/// Neo4j source plugged into [`run_cdc_loop`].
struct Neo4jBackend {
    config: Neo4jCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
}

#[async_trait::async_trait]
impl CdcBackend for Neo4jBackend {
    async fn validate_drain(&self) -> Result<()> {
        let targets = neo4j_redis_drain_targets(&self.sink, &self.config)?;
        self.sink
            .validate_redis_drain(self.config.bootstrap.is_some(), &targets)
    }

    async fn drain_local(&self) -> Result<()> {
        drain_neo4j_local_state(&self.config)
    }
    async fn reconcile_orphans(&self) -> Result<()> {
        let targets = neo4j_redis_drain_targets(&self.sink, &self.config)?;
        if self
            .sink
            .reset_redis_targets_if_configured(&targets)
            .await?
        {
            return Ok(());
        }
        let os = self.sink.open_search().ok_or_else(|| {
            anyhow!("Redis orphan reconciliation is not available; drain-resume is blocked")
        })?;
        reconcile_orphan_docs_neo4j(&self.config, os)
            .await
            .map(|_| ())
    }
    fn prepare_bootstrap(&mut self) -> Result<()> {
        // Neo4j re-bootstraps implicitly once its cursor file is gone.
        Ok(())
    }
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome> {
        build_and_run_neo4j_engine(
            self.config.clone(),
            self.sink.clone(),
            self.engine_config.clone(),
            inner,
            outer,
        )
        .await
    }
}

/// MongoDB source plugged into [`run_cdc_loop`].
struct MongoBackend {
    config: MongoCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
}

#[async_trait::async_trait]
impl CdcBackend for MongoBackend {
    async fn validate_drain(&self) -> Result<()> {
        let targets = mongodb_redis_drain_targets(&self.sink, &self.config)?;
        self.sink
            .validate_redis_drain(self.config.bootstrap, &targets)
    }

    async fn drain_local(&self) -> Result<()> {
        drain_mongodb_local_state(&self.config)
    }
    async fn reconcile_orphans(&self) -> Result<()> {
        let targets = mongodb_redis_drain_targets(&self.sink, &self.config)?;
        if self
            .sink
            .reset_redis_targets_if_configured(&targets)
            .await?
        {
            return Ok(());
        }
        self.sink.ensure_drain_reconciliation_supported()?;
        // Phase 1 (raw 1:1) writes deterministic doc IDs and does not yet
        // run a collection-`_id` live-set reconcile pass; deletes are caught
        // live as tombstones. Orphan reconciliation lands with join mode.
        Ok(())
    }
    fn prepare_bootstrap(&mut self) -> Result<()> {
        // Mongo re-bootstraps implicitly once its resume-token file is gone
        // (same as Neo4j), so there's nothing to flip here.
        Ok(())
    }
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome> {
        build_and_run_mongodb_engine(
            self.config.clone(),
            self.sink.clone(),
            self.engine_config.clone(),
            inner,
            outer,
        )
        .await
    }
}

/// MySQL/MariaDB source plugged into [`run_cdc_loop`].
struct MysqlBackend {
    config: MySqlCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
}

#[async_trait::async_trait]
impl CdcBackend for MysqlBackend {
    async fn validate_drain(&self) -> Result<()> {
        let targets = mysql_redis_drain_targets(&self.runtime.sink, &self.config, &self.joins)?;
        self.runtime
            .sink
            .validate_redis_drain(self.config.bootstrap, &targets)
    }

    async fn drain_local(&self) -> Result<()> {
        drain_mysql_local_state(&self.config, self.runtime.engine_file_config.as_ref())
    }
    async fn reconcile_orphans(&self) -> Result<()> {
        let targets = mysql_redis_drain_targets(&self.runtime.sink, &self.config, &self.joins)?;
        if self
            .runtime
            .sink
            .reset_redis_targets_if_configured(&targets)
            .await?
        {
            return Ok(());
        }
        self.runtime.sink.ensure_drain_reconciliation_supported()?;
        // Phase 1: deletes are caught live as tombstones; no live-set
        // reconcile pass yet (lands later, shared with the join modes).
        Ok(())
    }
    fn prepare_bootstrap(&mut self) -> Result<()> {
        // Re-bootstraps once the binlog-position file is gone (like Neo4j/Mongo).
        Ok(())
    }
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome> {
        // Bounded-memory SQL-denormalize mode when joins are configured and
        // VS_MYSQL_DENORMALIZE_MODE=sql; otherwise the in-memory join engine
        // (or raw 1:1 when there are no joins).
        if !self.joins.is_empty()
            && mysql_sql_denormalize_enabled(self.runtime.engine_file_config.as_ref())
        {
            return build_and_run_mysql_sql_denormalize_engine(
                self.config.clone(),
                self.runtime.clone(),
                self.joins.clone(),
                inner,
                outer,
            )
            .await;
        }
        build_and_run_mysql_engine(
            self.config.clone(),
            self.runtime.clone(),
            self.joins.clone(),
            inner,
            outer,
        )
        .await
    }
}

/// `VS_MYSQL_DENORMALIZE_MODE=sql` selects the bounded-memory SQL path.
fn mysql_sql_denormalize_enabled(engine_config: Option<&EngineFileConfig>) -> bool {
    match engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.mysql.as_ref())
        .and_then(|mysql| mysql.denormalize_mode)
    {
        Some(SqlDenormalizeMode::Sql) => true,
        Some(SqlDenormalizeMode::Memory) => false,
        None => std::env::var("VS_MYSQL_DENORMALIZE_MODE")
            .map(|v| v.eq_ignore_ascii_case("sql"))
            .unwrap_or(false),
    }
}

/// Kafka/Redpanda source plugged into [`run_cdc_loop`].
struct KafkaBackend {
    config: KafkaCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
}

#[async_trait::async_trait]
impl CdcBackend for KafkaBackend {
    async fn validate_drain(&self) -> Result<()> {
        if self.sink.redis().is_some() {
            return Err(anyhow!(
                "Redis drain/rebuild is not supported for Kafka because consumer offsets cannot be reset atomically by the engine"
            ));
        }
        Ok(())
    }

    async fn drain_local(&self) -> Result<()> {
        // Kafka resume lives in the consumer group's committed offsets
        // (server-side), not a local file. There's nothing to wipe here; to
        // re-consume from the start, reset the group's offsets externally or
        // set VS_KAFKA_AUTO_OFFSET_RESET=earliest with a fresh group id.
        info!("drain: kafka offsets are server-side; no local state to wipe");
        Ok(())
    }
    async fn reconcile_orphans(&self) -> Result<()> {
        self.sink.ensure_drain_reconciliation_supported()?;
        // Phase 1 (raw 1:1): deletes arrive as Debezium op=d tombstones; no
        // live-set reconcile pass (lands with join mode).
        Ok(())
    }
    fn prepare_bootstrap(&mut self) -> Result<()> {
        Ok(())
    }
    async fn run_iteration(
        &mut self,
        inner: ShutdownToken,
        outer: ShutdownToken,
    ) -> Result<EngineIterationOutcome> {
        build_and_run_kafka_engine(
            self.config.clone(),
            self.sink.clone(),
            self.engine_config.clone(),
            inner,
            outer,
        )
        .await
    }
}

async fn run_cdc_postgres(
    mut pg: PostgresCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    joins_yaml_text: Option<String>,
    shutdown: ShutdownToken,
) -> Result<()> {
    // Report Starting up front so the control plane shows the new
    // process as "starting" until the source moves into bootstrap or
    // tail mode. Without this, the agent would heartbeat as the
    // default phase (also Starting) — explicit set is cheap and makes
    // the intent obvious.
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Starting);
    ventstream_telemetry::set_source("postgres");
    ventstream_telemetry::set_target(runtime.sink.kind());

    info!(
        pg_host = %pg.host,
        pg_db = %pg.database,
        pg_slot = %pg.slot_name,
        sink = runtime.sink.kind(),
        sink_endpoint = %runtime.sink.endpoint(),
        joins_count = joins.len(),
        "cdc pipeline configured"
    );

    // YAML-change detection. Runs before the engine starts so a
    // detected change can drop the slot + re-bootstrap atomically.
    // No-op when no joins YAML is configured.
    let yaml_fp = if let Some(text) = joins_yaml_text.as_deref() {
        let state_dir = join_state_dir(runtime.engine_file_config.as_ref())?;
        let auto_resync = config_bool_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.auto_resync_on_yaml_change),
            "VS_PG_AUTO_RESYNC_ON_YAML_CHANGE",
            false,
        );
        let force_resync = config_bool_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.force_resync),
            "VS_PG_FORCE_RESYNC",
            false,
        );
        let check = yaml_fingerprint::check(text, state_dir.as_deref(), auto_resync, force_resync)?;
        Some(check)
    } else {
        None
    };

    if let Some(fp) = &yaml_fp {
        use yaml_fingerprint::FingerprintAction;
        if matches!(
            fp.action,
            FingerprintAction::Resync | FingerprintAction::ForceResync
        ) {
            // Drop the slot so the source's first connect re-bootstraps.
            // The OS index isn't touched — deterministic doc IDs make
            // the upcoming snapshot upsert every doc with the new shape;
            // OS replaces (not merges) on `index` action so stale
            // fields from the prior projection disappear automatically.
            drop_replication_slot(&pg, &pg.slot_name).await?;
            // Wipe the persistent join state. A resync means the prior
            // projection is no longer authoritative, so the in-memory
            // state we'd otherwise replay is stale by definition. Two
            // concrete reasons:
            //   (1) The new snapshot would merge against old state,
            //       producing docs whose joined fields reflect the old
            //       YAML shape until each row is touched again.
            //   (2) Replay + cascade against an already-populated state
            //       slows bootstrap from a few seconds to many minutes
            //       — every snapshot row triggers re-emits for the
            //       77k+ rows already cached.
            // We delete only the redb file; the fingerprint file lives
            // alongside it and is rewritten by `yaml_fingerprint::persist`
            // a few lines below.
            if let Some(dir) = join_state_dir(runtime.engine_file_config.as_ref())? {
                let redb_path = std::path::PathBuf::from(&dir).join("ventstream-joins.redb");
                match std::fs::remove_file(&redb_path) {
                    Ok(()) => info!(
                        path = %redb_path.display(),
                        "resync: wiped persistent join state"
                    ),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // First-ever resync with no prior persisted state — fine.
                    }
                    Err(err) => warn!(
                        path = %redb_path.display(),
                        error = %err,
                        "resync: failed to remove redb (bootstrap will merge against stale state)"
                    ),
                }
            }
            // Force bootstrap on this run regardless of what env said.
            if pg.bootstrap.is_none() {
                let chunk_size: usize = postgres_bootstrap_chunk_size(
                    runtime.engine_file_config.as_ref(),
                    "VS_PG_BOOTSTRAP_CHUNK_SIZE",
                    10_000,
                )?;
                pg = pg.with_bootstrap(SnapshotBootstrap {
                    tables: build_bootstrap_tables(&joins),
                    chunk_size,
                });
            }
            info!(
                slot = %pg.slot_name,
                "resync: slot dropped + state wiped + bootstrap mode enabled for this boot"
            );
        }
    }

    // Persist the YAML fingerprint right after the boot-time resync
    // decision was made. The agent's pause loop below may rebuild the
    // engine many times across the process lifetime — fingerprint
    // logic should run once, here, so subsequent iterations don't
    // re-resync on the same already-known YAML.
    if let Some(fp) = &yaml_fp {
        if let Some(path) = &fp.fingerprint_path {
            yaml_fingerprint::persist(path, &fp.current_fingerprint);
        }
    }

    // Hand off to the shared orchestrator. `pg` already carries any
    // boot-time resync decision made above.
    run_cdc_loop(PgBackend { pg, runtime, joins }, shutdown).await
}

/// Drive a delete-reconciliation pass against OpenSearch for every
/// primary table in the current joins config. Queries the source for
/// the live set of primary keys, then asks the sink to delete any OS
/// docs whose IDs don't reference one of those PKs.
///
/// Single- and composite-key primary tables are both handled — the
/// live PK set is keyed canonically (see `encode_pk_key`) so it matches
/// the `{table}:["a","b"]` doc-id form. Embedded relations (`related`
/// entries in a join) don't have their own OS docs so they don't need a
/// pass.
///
/// Returns the total number of orphan docs deleted across all tables.
async fn reconcile_orphan_docs_pg(
    pg: &PostgresCdcConfig,
    os: &OpenSearchConfig,
    joins: &[JoinDefinition],
) -> Result<usize> {
    if joins.is_empty() {
        return Ok(0);
    }
    let client = ventstream_sources::postgres::connect_client(pg, "reconciliation")
        .await
        .context("connecting to postgres for reconciliation")?;

    let mut total = 0usize;
    for join in joins {
        let pk_cols = join.primary.pk.columns();
        let pks = match pg_table_pk_keys(&client, &join.primary.table, pk_cols).await {
            Ok(set) => set,
            Err(err) => {
                warn!(
                    table = %join.primary.table,
                    error = %err,
                    "reconciliation: failed to list source PKs; skipping table"
                );
                continue;
            }
        };
        let (_, table_name) = split_table(&join.primary.table);
        let prefix = format!("{}:", join.primary.table);
        match ventstream_sinks::opensearch::reconcile_orphans(
            os,
            &table_name,
            &prefix,
            &pks,
            ventstream_sinks::opensearch::DocIdFormat::JsonArray,
        )
        .await
        {
            Ok(n) => total += n,
            Err(err) => warn!(
                table = %join.primary.table,
                error = %err,
                "reconciliation: bulk delete pass failed; orphans may remain"
            ),
        }
    }

    drop(client);
    info!(
        orphans_deleted = total,
        "reconciliation pass complete (all tables)"
    );
    Ok(total)
}

/// Stream every primary-key value from `table` into a `HashSet` of
/// canonical key strings (see `ventstream_sinks::opensearch::encode_pk_key`)
/// — one entry per row, handling single- and composite-column primary
/// keys alike. Uses a server-side cursor (`DECLARE … CURSOR` + `FETCH`)
/// so the pull doesn't materialise the full result set in PG memory.
async fn pg_table_pk_keys(
    client: &tokio_postgres::Client,
    table: &str,
    pk_columns: &[String],
) -> Result<std::collections::HashSet<String>> {
    // The table + columns are configured-via-YAML and not user-supplied
    // at runtime, so they're not SQL-injection vectors. We still
    // double-quote them so identifiers containing dots / unusual case
    // (`public.orders`, `MyTable`) survive the round-trip.
    let qualified = quote_qualified_ident(table);
    // Project each PK column `::text` in declared order; the row's text
    // components fold into the same canonical key the doc-id side parses.
    let select_list = pk_columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let cursor_name = format!(
        "vs_reconcile_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );

    client
        .batch_execute(&format!(
            "BEGIN; \
             DECLARE {cursor_name} CURSOR FOR \
               SELECT {select_list} FROM {qualified};"
        ))
        .await
        .with_context(|| format!("declaring reconciliation cursor for {table}"))?;

    let n_cols = pk_columns.len();
    let mut pks = std::collections::HashSet::with_capacity(1024);
    const FETCH_BATCH: usize = 10_000;
    loop {
        let rows = client
            .query(&format!("FETCH {FETCH_BATCH} FROM {cursor_name}"), &[])
            .await
            .with_context(|| format!("fetching from reconciliation cursor for {table}"))?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let mut components = Vec::with_capacity(n_cols);
            let mut complete = true;
            for i in 0..n_cols {
                match row.get::<_, Option<String>>(i) {
                    Some(v) => components.push(v),
                    // A NULL PK component can't form a usable key — skip
                    // the row (true primary keys are NOT NULL anyway).
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                if let Some(key) = ventstream_sinks::opensearch::encode_pk_key(&components) {
                    pks.insert(key);
                }
            }
        }
    }
    let _ = client.batch_execute("COMMIT").await;
    Ok(pks)
}

/// Quote a `schema.table` identifier into `"schema"."table"`. The
/// engine config disallows quote chars in identifiers, so the simple
/// double-quote-each-segment scheme is safe.
fn quote_qualified_ident(qualified: &str) -> String {
    qualified
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Drain PG-side local state during a pause: drop the replication
/// slot and remove the persistent redb file. Idempotent — safe to
/// call repeatedly if the first attempt only partially succeeded.
///
/// The redb file MUST be free of in-process handles when this runs,
/// which the orchestrator guarantees by only invoking this between
/// engine iterations (when the previous iteration's engine + its
/// `PersistentBackend` have already dropped).
async fn drain_pg_local_state(
    pg: &PostgresCdcConfig,
    engine_config: Option<&EngineFileConfig>,
) -> Result<()> {
    drop_replication_slot(pg, &pg.slot_name).await?;
    if let Some(dir) = join_state_dir(engine_config)? {
        let redb_path = dir.join("ventstream-joins.redb");
        remove_join_state_database(&redb_path)?;
    }
    Ok(())
}

/// Background task that polls the latest control-plane command and
/// cancels the supplied inner shutdown the moment a `pause = true`
/// arrives. The orchestrator owns the inner token and observes the
/// cancellation by `engine.run().await` returning.
///
/// Cancellation of the child token doesn't propagate to the outer
/// token, so the process keeps running even after the watcher fires.
/// Randomize a backoff to 50-100% of the scheduled value, matching the
/// tail reconnect's jitter.
fn jittered_iteration_backoff(delay: Duration) -> Duration {
    use rand::Rng as _;
    delay.mul_f64(rand::thread_rng().gen_range(0.5..=1.0))
}

/// Watch for a pause command; the returned flag distinguishes a pause
/// cancellation of the child token from an engine-internal failure that
/// cancelled the same token.
fn spawn_pause_watcher(
    inner_shutdown: ShutdownToken,
) -> (
    tokio::task::JoinHandle<()>,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let pause_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&pause_requested);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = inner_shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(500)) => {
                    let cmd = ventstream_telemetry::latest_command().unwrap_or_default();
                    if cmd.pause {
                        info!("pause command received — cancelling engine for graceful pause");
                        flag.store(true, std::sync::atomic::Ordering::Release);
                        inner_shutdown.cancel();
                        return;
                    }
                }
            }
        }
    });
    (handle, pause_requested)
}

/// Idle the orchestrator while the control plane's latest command
/// still fails the `done` predicate. Exits early on outer shutdown.
async fn wait_for_command_change<F>(outer_shutdown: &ShutdownToken, done: F)
where
    F: Fn(ventstream_telemetry::AgentCommand) -> bool,
{
    loop {
        tokio::select! {
            () = outer_shutdown.cancelled() => return,
            () = tokio::time::sleep(Duration::from_millis(500)) => {
                let cmd = ventstream_telemetry::latest_command().unwrap_or_default();
                if done(cmd) {
                    return;
                }
            }
        }
    }
}

/// Build the engine for one iteration and run it until the supplied
/// inner shutdown cancels (either from outer shutdown or from a pause
/// watcher). The returned variant tells the orchestrator how to
/// proceed.
async fn build_and_run_pg_engine(
    mut pg: PostgresCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    let mut bootstrap_existing_slot = false;
    // Snapshot the inputs the optional admin server would need, before
    // either gets moved into the engine. Cheap clones (PostgresCdcConfig
    // is a few short strings; the tables list is small).
    let pg_for_admin = pg.clone();
    let bootstrap_tables_for_admin = build_bootstrap_tables(&joins);

    let join_engine = if joins.is_empty() {
        None
    } else {
        let fetcher_pool_size = config_usize_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.source.as_ref())
                .and_then(|source| source.postgres.as_ref())
                .and_then(|postgres| postgres.related_fetch_pool_size),
            "VS_PG_RELATED_FETCH_POOL_SIZE",
            4,
        )?;
        let fetcher = PostgresFetcher::connect_config_with_pool_size(pg.clone(), fetcher_pool_size)
            .await
            .context("opening sync-on-miss fetcher connection")?;

        // Memory-mode joins require durable local state because the source
        // checkpoint advances only after the corresponding state transaction.
        let path = required_join_state_dir(runtime.engine_file_config.as_ref())?;
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating state dir {}", path.display()))?;
        let db_path = path.join("ventstream-joins.redb");
        let persist_batch = config_usize_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.persist_batch_ops),
            "VS_PERSIST_BATCH_OPS",
            5_000,
        )?;
        let identity = postgres_join_state_identity(&pg);
        let mut backend = PersistentBackend::open_with_batch_size(&db_path, persist_batch)
            .with_context(|| format!("opening persistent state at {}", db_path.display()))?;
        let stored_identity = backend
            .state_identity()
            .context("reading persistent join-state identity")?;
        let source_checkpoint_exists = replication_slot_exists(&pg, &pg.slot_name).await?;
        let recoverable = join_checkpoint_recoverable(
            stored_identity.as_deref(),
            &identity,
            source_checkpoint_exists,
        );
        if !recoverable {
            drop(backend);
            remove_join_state_database(&db_path)?;
            if pg.bootstrap.is_none() {
                pg = pg.with_bootstrap(SnapshotBootstrap {
                    tables: build_bootstrap_tables(&joins),
                    chunk_size: postgres_bootstrap_chunk_size(
                        runtime.engine_file_config.as_ref(),
                        "VS_PG_BOOTSTRAP_CHUNK_SIZE",
                        10_000,
                    )?,
                });
            }
            if source_checkpoint_exists {
                bootstrap_existing_slot = true;
            }
            backend = PersistentBackend::open_with_batch_size(&db_path, persist_batch)
                .with_context(|| {
                    format!("reopening reset persistent state at {}", db_path.display())
                })?;
            warn!(
                path = %db_path.display(),
                slot = %pg.slot_name,
                previous_identity = ?stored_identity,
                retained_slot = source_checkpoint_exists,
                "join state/source checkpoint mismatch; rebuilding state from snapshot"
            );
        }
        let mut state = JoinState::new();
        let stats = state
            .load_from_backend(&backend)
            .context("replaying persistent join state")?;
        info!(
            path = %db_path.display(),
            foreign_rows = stats.foreign_rows,
            primary_rows = stats.primary_rows,
            primary_reverse = stats.primary_reverse,
            foreign_by_fk = stats.foreign_by_fk,
            "persistent join state replayed"
        );
        let state = state
            .with_backend(backend)
            .with_persistence_identity(identity);

        let idle_flush = config_duration_ms_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.idle_flush_ms),
            "VS_JOIN_IDLE_FLUSH_MS",
            Duration::from_secs(1),
        )?;
        Some(std::sync::Arc::new(
            JoinEngine::with_state(joins, std::sync::Arc::new(fetcher), state)
                .with_idle_flush_interval(idle_flush),
        ))
    };

    // Shared watermark between the CDC source and the dispatcher.
    // The source uses it to gate WAL slot advancement; the dispatcher
    // publishes the highest LSN it has durably handled per batch.
    // One Arc, two readers — both ends know nothing about each other.
    let sink_progress = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let lsn_flush = config_duration_ms_or_env(
        runtime
            .engine_file_config
            .as_ref()
            .and_then(|config| config.runtime.joins.lsn_flush_ms),
        "VS_LSN_FLUSH_MS",
        Duration::from_millis(200),
    )?;
    let source = PostgresCdcSource::new(pg)
        .with_sink_progress(std::sync::Arc::clone(&sink_progress))
        .with_lsn_flush_interval(lsn_flush)
        .with_existing_slot_bootstrap(bootstrap_existing_slot);
    let sink = build_sink(runtime.sink.clone(), &inner_shutdown).await?;

    info!(
        bus_capacity = runtime.engine_config.bus_capacity,
        disp_max_events = runtime.engine_config.dispatcher.max_events,
        disp_max_bytes = runtime.engine_config.dispatcher.max_batch_bytes,
        disp_flush_ms = runtime.engine_config.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = runtime.engine_config.dispatcher.max_parallel_bulks,
        lsn_flush_ms = lsn_flush.as_millis() as u64,
        "engine knobs"
    );

    let mut engine = Engine::new(Box::new(source), sink, runtime.engine_config)
        .with_sink_progress(sink_progress);
    if let Some(je) = join_engine {
        engine = engine.with_joins(je);
    }

    // Optional admin HTTP server for on-demand resync. Tied to the
    // inner shutdown so it tears down + rebuilds on each pause cycle.
    // That's fine — admin is a low-traffic operator surface, and not
    // restarting it on pause means pause/resume wouldn't be reflected
    // by the admin endpoint's connectivity.
    // The admin server is OPTIONAL — a malformed VS_ADMIN_LISTEN, or an
    // insecure (non-loopback + tokenless) config, must NOT take the data
    // pipeline down (M15). Skip the server with a loud log instead of
    // propagating `?`. Skipping is also the safe outcome for the insecure
    // case: the destructive endpoint simply doesn't come up.
    let admin_listen_value = runtime
        .engine_file_config
        .as_ref()
        .and_then(|config| config.runtime.admin.listen.clone())
        .or(opt("VS_ADMIN_LISTEN")?);
    let admin_listen = match admin_listen_value {
        Some(listen_str) => match admin::parse_admin_listen(&listen_str) {
            Ok(addr) => Some(addr),
            Err(err) => {
                warn!(
                    value = %listen_str,
                    error = %format!("{err:#}"),
                    "VS_ADMIN_LISTEN invalid; admin server disabled (pipeline continues)"
                );
                None
            }
        },
        None => None,
    };
    let admin_task = if let Some(listen) = admin_listen {
        // Token from VS_ADMIN_TOKEN, falling back to the control-plane key.
        // A blank value counts as unset (env vars can't truly be "empty").
        let admin_token = match runtime
            .engine_file_config
            .as_ref()
            .and_then(|config| config.runtime.admin.token_ref.as_ref())
        {
            Some(reference) => Some(resolve_value_ref(reference)?),
            None => opt("VS_ADMIN_TOKEN")?.or(opt("VS_CONTROL_PLANE_KEY")?),
        }
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
        if let Err(err) = admin::validate_listen_auth(&listen, &admin_token) {
            // Insecure config — don't start the destructive endpoint, but
            // keep the pipeline running (loud error so the operator notices).
            error!(
                error = %format!("{err:#}"),
                "admin server NOT started (insecure listener config); pipeline continues"
            );
            None
        } else {
            let admin_state = admin::AdminState {
                pg: pg_for_admin,
                tables: bootstrap_tables_for_admin,
                chunk_size: 10_000,
                source_sender: engine.source_sender_clone(),
                shutdown: inner_shutdown.clone(),
                busy: std::sync::Arc::new(parking_lot::Mutex::new(false)),
                token: admin_token,
            };
            let admin_shutdown = inner_shutdown.clone();
            Some(tokio::spawn(async move {
                if let Err(err) = admin::run(listen, admin_state, admin_shutdown.clone()).await {
                    error!(error = %err, "admin server exited with error");
                    admin_shutdown.cancel();
                }
            }))
        }
    } else {
        None
    };

    let engine_outcome = engine.run(inner_shutdown.clone()).await;
    if let Err(err) = &engine_outcome {
        ventstream_telemetry::record_error(format!("engine run failed: {err:#}"));
    }

    if let Some(handle) = admin_task {
        let _ = handle.await;
    }

    // Distinguish "outer process shutting down" from "pause command
    // cancelled the inner token." Engine errors get bubbled up.
    match engine_outcome {
        Ok(()) if outer_shutdown.is_cancelled() => Ok(EngineIterationOutcome::Shutdown),
        Ok(()) => Ok(EngineIterationOutcome::Paused),
        Err(err) => Err(anyhow!(err).context("cdc engine run")),
    }
}

/// True when `VS_PG_DENORMALIZE_MODE=sql` — use the bounded SQL-join
/// denormalizer instead of the in-memory join engine.
fn pg_sql_denormalize_enabled(engine_config: Option<&EngineFileConfig>) -> bool {
    match engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.postgres.as_ref())
        .and_then(|postgres| postgres.denormalize_mode)
    {
        Some(SqlDenormalizeMode::Sql) => true,
        Some(SqlDenormalizeMode::Memory) => false,
        None => std::env::var("VS_PG_DENORMALIZE_MODE")
            .map(|v| v.eq_ignore_ascii_case("sql"))
            .unwrap_or(false),
    }
}

/// Create the logical replication slot if it doesn't exist. In SQL-
/// denormalize mode the source doesn't run a snapshot bootstrap (so it
/// never creates the slot), but the tail still needs it — and creating it
/// *before* the SQL-join bootstrap means WAL retains any change made
/// during the bootstrap window for the tail to replay.
async fn ensure_replication_slot(pg: &PostgresCdcConfig, slot: &str) -> Result<()> {
    // This connect authenticates with the same rotating credentials as the
    // tail; a credential rejection here is terminal for the iteration, so
    // classify it (SQLSTATE now visible via describe_db_error) instead of
    // surfacing a bare "db error".
    let client = ventstream_sources::postgres::connect_client(pg, "create replication slot")
        .await
        .map_err(|err| {
            let text = err.to_string();
            if ventstream_sources::postgres::is_credential_message(&text) {
                anyhow!(
                    "credential error; exiting so the supervisor can restart with fresh \
                     credentials (last: {text})"
                )
            } else {
                anyhow!(err)
            }
        })
        .context("connect to create replication slot")?;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .await
        .context("checking replication slot")?
        .get(0);
    if exists {
        info!(slot, "replication slot already exists (sql-denormalize)");
        return Ok(());
    }
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
            slot.replace('\'', "''")
        ))
        .await
        .context("creating replication slot")?;
    info!(slot, "replication slot created (sql-denormalize)");
    Ok(())
}

/// Bounded-memory PG pipeline: `source(tail) → SqlDenormalizer → dispatcher
/// → sink`. The denormalizer SQL-join-bootstraps (O(chunk) memory) then
/// recomposes affected primaries per tail event — no resident join state.
async fn build_and_run_pg_sql_denormalize_engine(
    pg: PostgresCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use ventstream_core::{EventBus, Source, SourceContext};

    ensure_replication_slot(&pg, &pg.slot_name).await?;

    let chunk = postgres_bootstrap_chunk_size(
        runtime.engine_file_config.as_ref(),
        "VS_PG_BOOTSTRAP_CHUNK_SIZE",
        5_000,
    )? as i64;
    let mut denorm = sql_denormalize::SqlDenormalizer::connect(&pg, &pg.publication, joins, chunk)
        .await
        .context("building SQL denormalizer")?;
    if matches!(runtime.sink.kind(), "redis" | "meilisearch") {
        denorm = denorm.with_target_clears();
    }
    // OpenSearch can serve as a reverse index when the WAL delete pre-image
    // lacks the parent key. Other sinks require complete replica identity.
    if postgres_sink_reverse_lookup_enabled(
        runtime.engine_file_config.as_ref(),
        runtime.sink.kind() == "opensearch",
    ) {
        let os = runtime.sink.open_search().ok_or_else(|| {
            anyhow!(
                "Postgres sink reverse lookup requires OpenSearch; set \
                 source.postgres.sink_reverse_lookup=false after configuring child tables \
                 with a complete replica identity"
            )
        })?;
        denorm = denorm.with_reverse_lookup(os.clone());
    }

    let sink_progress = Arc::new(AtomicU64::new(0));
    let transform_progress = Arc::new(AtomicU64::new(0));
    let denormalize_durability = sql_denormalize::SqlDenormalizeDurability::new(
        Arc::clone(&transform_progress),
        Arc::clone(&sink_progress),
    );
    let lsn_flush = config_duration_ms_or_env(
        runtime
            .engine_file_config
            .as_ref()
            .and_then(|config| config.runtime.joins.lsn_flush_ms),
        "VS_LSN_FLUSH_MS",
        Duration::from_millis(200),
    )?;
    let source = PostgresCdcSource::new(pg)
        .with_sink_progress(Arc::clone(&sink_progress))
        .with_lsn_flush_interval(lsn_flush);
    let sink = build_sink(runtime.sink.clone(), &inner_shutdown).await?;

    info!(
        bus_capacity = runtime.engine_config.bus_capacity,
        chunk,
        disp_max_events = runtime.engine_config.dispatcher.max_events,
        disp_max_bytes = runtime.engine_config.dispatcher.max_batch_bytes,
        disp_flush_ms = runtime.engine_config.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = runtime.engine_config.dispatcher.max_parallel_bulks,
        "engine knobs (pg sql-denormalize)"
    );

    let dlq = crate::dlq::DlqWriter::open(runtime.engine_config.dlq_path.clone()).await?;
    let memory_runtime = MemoryRuntime::detect(&runtime.engine_config.memory);
    let memory_shutdown = inner_shutdown.child();
    let memory_monitor = memory_runtime
        .as_ref()
        .map(|memory| memory.spawn(memory_shutdown.clone()));
    let mut dispatcher = crate::dispatcher::Dispatcher::new(
        Arc::clone(&sink),
        dlq,
        runtime.engine_config.dispatcher.clone(),
        inner_shutdown.clone(),
    )
    .with_transform_progress(Arc::clone(&transform_progress));
    if let Some(memory) = &memory_runtime {
        dispatcher = dispatcher.with_memory_budget(memory.budget());
    }

    // source → bus1 → denormalizer (bootstrap + tail) → bus2 → dispatcher.
    let source_bus = memory_runtime.as_ref().map_or_else(
        || EventBus::new(runtime.engine_config.bus_capacity),
        |memory| {
            EventBus::with_memory_budget(
                runtime.engine_config.bus_capacity,
                memory.budget(),
                MemoryAdmission::TransformInput,
            )
        },
    );
    let join_bus = memory_runtime.as_ref().map_or_else(
        || EventBus::new(runtime.engine_config.bus_capacity),
        |memory| {
            EventBus::with_memory_budget(
                runtime.engine_config.bus_capacity,
                memory.budget(),
                MemoryAdmission::TransformOutput,
            )
        },
    );
    let (source_sender, source_receiver) = source_bus.split();
    let (join_sender, join_receiver) = join_bus.split();

    let src_ctx = SourceContext::new(source_sender, inner_shutdown.clone());
    let source_handle = tokio::spawn(async move { source.run(src_ctx).await });
    let denorm_shutdown = inner_shutdown.clone();
    let denorm_handle = tokio::spawn(async move {
        denorm
            .run(
                source_receiver,
                join_sender,
                denorm_shutdown,
                denormalize_durability,
            )
            .await;
    });
    let dispatcher_handle = tokio::spawn(dispatcher.run(join_receiver));

    let source_result = source_handle.await;
    if let Ok(Err(err)) = &source_result {
        error!(error = %err, "pg source (sql-denormalize) returned error");
        inner_shutdown.cancel();
    }
    let _ = denorm_handle.await;
    let _ = dispatcher_handle.await;
    memory_shutdown.cancel();
    if let Some(handle) = memory_monitor {
        let _ = handle.await;
    }

    match source_result {
        Ok(Ok(())) => {
            if outer_shutdown.is_cancelled() {
                Ok(EngineIterationOutcome::Shutdown)
            } else {
                Ok(EngineIterationOutcome::Paused)
            }
        }
        Ok(Err(err)) => Err(anyhow!(err).context("pg sql-denormalize source")),
        Err(join_err) => Err(anyhow!(join_err).context("pg sql-denormalize task panicked")),
    }
}

/// The Neo4j CDC path. Streamlined v1: source → bus → sink. No join
/// engine yet (would need a `Neo4jFetcher` for sync-on-miss), no admin
/// resync, no extra knobs beyond the dispatcher tunables that already
/// apply to every source.
async fn run_cdc_neo4j(
    config: Neo4jCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
    shutdown: ShutdownToken,
) -> Result<()> {
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Starting);
    ventstream_telemetry::set_source("neo4j");
    ventstream_telemetry::set_target(sink.kind());

    info!(
        neo4j_uri = %config.uri,
        neo4j_database = %config.database,
        sink = sink.kind(),
        sink_endpoint = %sink.endpoint(),
        "cdc pipeline configured (neo4j source)"
    );

    // Hand off to the same orchestrator the PG path uses, so operators
    // get one consistent pause/resume/drain UX across backends.
    run_cdc_loop(
        Neo4jBackend {
            config,
            sink,
            engine_config,
        },
        shutdown,
    )
    .await
}

/// Drive a delete-reconciliation pass against OpenSearch for every
/// primary label in the Neo4j denormalize spec. For each spec, fetch
/// the live set of element IDs from Neo4j, then call the sink's
/// reconciler with the `output_table` as the index name and the
/// element IDs as the valid-PK set.
///
/// No-op when no denormalize specs are configured — the simple
/// "emit one event per node/rel" path doesn't write its own primary
/// docs to OS (it relies on the index template + headers), so there's
/// nothing to reconcile.
async fn reconcile_orphan_docs_neo4j(
    config: &Neo4jCdcConfig,
    os: &OpenSearchConfig,
) -> Result<usize> {
    let Some(specs) = &config.denormalize else {
        info!("neo4j reconciliation: no denormalize specs configured, skipping");
        return Ok(0);
    };
    if specs.denormalize.is_empty() {
        return Ok(0);
    }

    let mut total = 0usize;
    for spec in &specs.denormalize {
        let eids =
            match ventstream_sources::neo4j::list_node_element_ids(config, &spec.primary_label)
                .await
            {
                Ok(set) => set,
                Err(err) => {
                    warn!(
                        label = %spec.primary_label,
                        error = %err,
                        "neo4j reconciliation: failed to list source element IDs; skipping spec"
                    );
                    continue;
                }
            };
        let prefix = format!("{}:", spec.output_table);
        match ventstream_sinks::opensearch::reconcile_orphans(
            os,
            &spec.output_table,
            &prefix,
            &eids,
            ventstream_sinks::opensearch::DocIdFormat::RawSuffix,
        )
        .await
        {
            Ok(n) => total += n,
            Err(err) => warn!(
                label = %spec.primary_label,
                output_table = %spec.output_table,
                error = %err,
                "neo4j reconciliation: bulk delete pass failed; orphans may remain"
            ),
        }
    }
    info!(
        orphans_deleted = total,
        "neo4j reconciliation pass complete (all specs)"
    );
    Ok(total)
}

/// Drain Neo4j-side local state during a pause: remove the cursor file
/// so the next source build kicks off a fresh bootstrap.
///
/// Idempotent — a missing file is fine (first drain after a process
/// that never wrote a cursor). Errors only on filesystem issues that
/// would actually prevent recovery.
fn drain_neo4j_local_state(config: &Neo4jCdcConfig) -> Result<()> {
    let cursor_path = config.state_dir.join("neo4j_cursor");
    match std::fs::remove_file(&cursor_path) {
        Ok(()) => {
            info!(
                path = %cursor_path.display(),
                "drain: wiped neo4j cursor file"
            );
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow!(err))
            .with_context(|| format!("removing neo4j cursor file {}", cursor_path.display())),
    }
}

/// Build the Neo4j engine for one iteration and run it. The shape
/// mirrors [`build_and_run_pg_engine`] — the orchestrator uses the
/// return value to decide whether to terminate, idle, or restart.
async fn build_and_run_neo4j_engine(
    config: Neo4jCdcConfig,
    sink_config: SinkRuntimeConfig,
    engine_cfg: EngineConfig,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    // Shared watermark between the Neo4j CDC source and the dispatcher.
    // Mirrors the Postgres setup: source defers cursor file writes,
    // dispatcher pushes the highest confirmed tx_id per batch. Same
    // crash-safety guarantee for Neo4j as PG already has.
    let sink_progress = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let source =
        Neo4jCdcSource::new(config).with_sink_progress(std::sync::Arc::clone(&sink_progress));
    let sink = build_sink(sink_config, &inner_shutdown).await?;

    info!(
        bus_capacity = engine_cfg.bus_capacity,
        disp_max_events = engine_cfg.dispatcher.max_events,
        disp_max_bytes = engine_cfg.dispatcher.max_batch_bytes,
        disp_flush_ms = engine_cfg.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = engine_cfg.dispatcher.max_parallel_bulks,
        "engine knobs (neo4j)"
    );

    let engine = Engine::new(Box::new(source), sink, engine_cfg).with_sink_progress(sink_progress);
    let engine_outcome = engine.run(inner_shutdown.clone()).await;
    if let Err(err) = &engine_outcome {
        ventstream_telemetry::record_error(format!("neo4j engine run failed: {err:#}"));
    }

    match engine_outcome {
        Ok(()) if outer_shutdown.is_cancelled() => Ok(EngineIterationOutcome::Shutdown),
        Ok(()) => Ok(EngineIterationOutcome::Paused),
        Err(err) => Err(anyhow!(err).context("cdc engine run (neo4j)")),
    }
}

async fn run_cdc_mongodb(
    config: MongoCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
    shutdown: ShutdownToken,
) -> Result<()> {
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Starting);
    ventstream_telemetry::set_source("mongodb");
    ventstream_telemetry::set_target(sink.kind());

    info!(
        mongo_database = %config.database,
        mongo_namespace = %config.namespace,
        sink = sink.kind(),
        sink_endpoint = %sink.endpoint(),
        "cdc pipeline configured (mongodb source)"
    );

    // Same orchestrator as PG/Neo4j → one consistent pause/resume/drain UX.
    run_cdc_loop(
        MongoBackend {
            config,
            sink,
            engine_config,
        },
        shutdown,
    )
    .await
}

/// Wipe the Mongo resume-token cursor file so the next iteration
/// re-bootstraps cleanly (the drain path).
fn drain_mongodb_local_state(config: &MongoCdcConfig) -> Result<()> {
    let cursor_path = config.state_dir.join("mongo_resume_token");
    match std::fs::remove_file(&cursor_path) {
        Ok(()) => {
            info!(path = %cursor_path.display(), "drain: wiped mongodb resume-token file");
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow!(err))
            .with_context(|| format!("removing mongodb cursor file {}", cursor_path.display())),
    }
}

/// Build the MongoDB engine for one iteration and run it. Mirrors
/// [`build_and_run_neo4j_engine`] without the sink-progress watermark —
/// Phase 1 persists the resume token after publish (at-least-once;
/// idempotent doc-id upserts make re-emits harmless on restart).
async fn build_and_run_mongodb_engine(
    config: MongoCdcConfig,
    sink_config: SinkRuntimeConfig,
    engine_cfg: EngineConfig,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    let source = MongoCdcSource::new(config);
    let sink = build_sink(sink_config, &inner_shutdown).await?;

    info!(
        bus_capacity = engine_cfg.bus_capacity,
        disp_max_events = engine_cfg.dispatcher.max_events,
        disp_max_bytes = engine_cfg.dispatcher.max_batch_bytes,
        disp_flush_ms = engine_cfg.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = engine_cfg.dispatcher.max_parallel_bulks,
        "engine knobs (mongodb)"
    );

    let engine = Engine::new(Box::new(source), sink, engine_cfg);
    let engine_outcome = engine.run(inner_shutdown.clone()).await;
    if let Err(err) = &engine_outcome {
        ventstream_telemetry::record_error(format!("mongodb engine run failed: {err:#}"));
    }

    match engine_outcome {
        Ok(()) if outer_shutdown.is_cancelled() => Ok(EngineIterationOutcome::Shutdown),
        Ok(()) => Ok(EngineIterationOutcome::Paused),
        Err(err) => Err(anyhow!(err).context("cdc engine run (mongodb)")),
    }
}

async fn run_cdc_mysql(
    config: MySqlCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    shutdown: ShutdownToken,
) -> Result<()> {
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Starting);
    ventstream_telemetry::set_source("mysql");
    ventstream_telemetry::set_target(runtime.sink.kind());

    info!(
        mysql_host = %config.host,
        mysql_database = %config.database,
        sink = runtime.sink.kind(),
        sink_endpoint = %runtime.sink.endpoint(),
        joins_count = joins.len(),
        "cdc pipeline configured (mysql source)"
    );

    run_cdc_loop(
        MysqlBackend {
            config,
            runtime,
            joins,
        },
        shutdown,
    )
    .await
}

fn remove_mysql_cursor_state(config: &MySqlCdcConfig) -> Result<()> {
    let cursor_path = config.state_dir.join("mysql_binlog_pos");
    let incomplete_path = cursor_path.with_extension("incomplete");
    for path in [&cursor_path, &incomplete_path] {
        match std::fs::remove_file(path) {
            Ok(()) => info!(path = %path.display(), "removed mysql cursor state"),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(anyhow!(err))
                    .with_context(|| format!("removing mysql cursor file {}", path.display()));
            }
        }
    }
    Ok(())
}

fn mysql_cursor_is_resumable(config: &MySqlCdcConfig) -> Result<bool> {
    ventstream_sources::mysql::mysql_checkpoint_is_resumable(&config.state_dir)
        .context("reading MySQL checkpoint state")
}

/// Wipe the binlog cursor and memory-join state as one recovery unit.
fn drain_mysql_local_state(
    config: &MySqlCdcConfig,
    engine_config: Option<&EngineFileConfig>,
) -> Result<()> {
    remove_mysql_cursor_state(config)?;
    if let Some(dir) = join_state_dir(engine_config)? {
        remove_join_state_database(&dir.join("ventstream-joins.redb"))?;
    }
    Ok(())
}

/// MySQL binlog positions are durable only after their acknowledgement
/// barriers commit. Keep sink batches serial as well so two updates to the
/// same document cannot land out of order across parallel bulk requests.
fn mysql_dispatcher_config(mut config: DispatcherConfig) -> DispatcherConfig {
    if config.max_parallel_bulks != 1 {
        info!(
            configured_parallel = config.max_parallel_bulks,
            effective_parallel = 1,
            "serializing MySQL sink batches to preserve document order"
        );
        config.max_parallel_bulks = 1;
    }
    config
}

/// Build the MySQL engine for one iteration. Binlog positions advance only
/// after the dispatcher's ordered durable sink watermark confirms the source's
/// acknowledgement barrier.
/// When `joins` is non-empty, a `MySqlFetcher`-backed `JoinEngine` is wired
/// between the source and the dispatcher (sync-on-miss backfill, like Postgres).
async fn build_and_run_mysql_engine(
    config: MySqlCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    let mut force_snapshot_from_cursor = false;
    // Build the join engine (if any) before `config` is moved into the source.
    let join_engine = if joins.is_empty() {
        None
    } else {
        let fetcher = MySqlFetcher::connect(&config);
        let path = required_join_state_dir(runtime.engine_file_config.as_ref())?;
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating state dir {}", path.display()))?;
        let db_path = path.join("ventstream-joins.redb");
        let persist_batch = config_usize_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.persist_batch_ops),
            "VS_PERSIST_BATCH_OPS",
            5_000,
        )?;
        let identity = mysql_join_state_identity(&config);
        let mut backend = PersistentBackend::open_with_batch_size(&db_path, persist_batch)
            .with_context(|| format!("opening persistent state at {}", db_path.display()))?;
        let stored_identity = backend
            .state_identity()
            .context("reading persistent join-state identity")?;
        let source_checkpoint_exists = mysql_cursor_is_resumable(&config)?;
        let recoverable = join_checkpoint_recoverable(
            stored_identity.as_deref(),
            &identity,
            source_checkpoint_exists,
        );
        if !recoverable {
            drop(backend);
            remove_join_state_database(&db_path)?;
            if !config.bootstrap && !source_checkpoint_exists {
                return Err(anyhow!(
                    "join state and MySQL binlog cursor do not form a recoverable checkpoint; \
                     enable snapshot bootstrap to rebuild them"
                ));
            }
            force_snapshot_from_cursor = source_checkpoint_exists;
            backend = PersistentBackend::open_with_batch_size(&db_path, persist_batch)
                .with_context(|| {
                    format!("reopening reset persistent state at {}", db_path.display())
                })?;
            warn!(
                path = %db_path.display(),
                previous_identity = ?stored_identity,
                retained_cursor = source_checkpoint_exists,
                "join state/source checkpoint mismatch; rebuilding state from snapshot"
            );
        }
        let mut state = JoinState::new();
        let stats = state
            .load_from_backend(&backend)
            .context("replaying persistent join state")?;
        info!(
            path = %db_path.display(),
            foreign_rows = stats.foreign_rows,
            primary_rows = stats.primary_rows,
            "persistent join state replayed (mysql)"
        );
        let state = state
            .with_backend(backend)
            .with_persistence_identity(identity);
        let idle_flush = config_duration_ms_or_env(
            runtime
                .engine_file_config
                .as_ref()
                .and_then(|config| config.runtime.joins.idle_flush_ms),
            "VS_JOIN_IDLE_FLUSH_MS",
            Duration::from_secs(1),
        )?;
        Some(std::sync::Arc::new(
            JoinEngine::with_state(joins, std::sync::Arc::new(fetcher), state)
                .with_idle_flush_interval(idle_flush),
        ))
    };

    let sink_progress = Arc::new(AtomicU64::new(0));
    let source = MySqlCdcSource::new(config)
        .with_sink_progress(Arc::clone(&sink_progress))
        .with_snapshot_completion_marker(join_engine.is_some())
        .with_forced_snapshot_from_cursor(force_snapshot_from_cursor);
    let sink = build_sink(runtime.sink.clone(), &inner_shutdown).await?;
    let mut engine_config = runtime.engine_config;
    engine_config.dispatcher = mysql_dispatcher_config(engine_config.dispatcher);

    info!(
        bus_capacity = engine_config.bus_capacity,
        disp_max_events = engine_config.dispatcher.max_events,
        disp_max_bytes = engine_config.dispatcher.max_batch_bytes,
        disp_flush_ms = engine_config.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = engine_config.dispatcher.max_parallel_bulks,
        "engine knobs (mysql)"
    );

    let mut engine =
        Engine::new(Box::new(source), sink, engine_config).with_sink_progress(sink_progress);
    if let Some(je) = join_engine {
        engine = engine.with_joins(je);
    }
    let engine_outcome = engine.run(inner_shutdown.clone()).await;
    if let Err(err) = &engine_outcome {
        ventstream_telemetry::record_error(format!("mysql engine run failed: {err:#}"));
    }

    match engine_outcome {
        Ok(()) if outer_shutdown.is_cancelled() => Ok(EngineIterationOutcome::Shutdown),
        Ok(()) => Ok(EngineIterationOutcome::Paused),
        Err(err) => Err(anyhow!(err).context("cdc engine run (mysql)")),
    }
}

/// Bounded-memory MySQL pipeline: `source(tail) → MySqlDenormalizer → dispatcher
/// → sink`. SQL-join-bootstraps (O(chunk) memory) then recomposes affected
/// primaries per tail event — no resident join state. Mirrors the PG path.
async fn build_and_run_mysql_sql_denormalize_engine(
    config: MySqlCdcConfig,
    runtime: CdcRuntime,
    joins: Vec<JoinDefinition>,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use ventstream_core::{EventBus, Source, SourceContext};

    // The denormalizer does its own SQL-join bootstrap; the binlog source must
    // NOT also snapshot (it would re-emit raw rows the denormalizer reprocesses).
    // The source still tails from the current position for live changes.
    let mut config = config;
    config.bootstrap = false;

    let chunk = mysql_bootstrap_chunk_size(
        runtime.engine_file_config.as_ref(),
        "VS_MYSQL_BOOTSTRAP_CHUNK_SIZE",
        5_000,
    )? as u64;
    let mysql_file = runtime
        .engine_file_config
        .as_ref()
        .and_then(|engine| engine.source.as_ref())
        .and_then(|source| source.mysql.as_ref());
    let recompose_chunk = config_usize_or_env(
        mysql_file.and_then(|mysql| mysql.recompose_chunk),
        "VS_MYSQL_RECOMPOSE_CHUNK",
        256,
    )?;
    let recompose_concurrency = config_usize_or_env(
        mysql_file.and_then(|mysql| mysql.recompose_concurrency),
        "VS_MYSQL_RECOMPOSE_CONCURRENCY",
        4,
    )?;
    let requires_full_row_image = mysql_joins_require_full_row_image(&joins);
    let mut denorm = mysql_sql_denormalize::MySqlDenormalizer::connect(&config, joins, chunk)
        .await
        .context("building MySQL SQL denormalizer")?
        .with_recompose_limits(recompose_chunk, recompose_concurrency);
    if requires_full_row_image {
        denorm
            .require_full_binlog_row_image()
            .await
            .context("validating MySQL joined-delete pre-images")?;
    }
    // OpenSearch remains a fallback for reduced delete images. Other sinks
    // consume the MySQL before-image and require binlog_row_image=FULL.
    if mysql_sink_reverse_lookup_enabled(
        runtime.engine_file_config.as_ref(),
        runtime.sink.kind() == "opensearch",
    ) {
        let os = runtime.sink.open_search().ok_or_else(|| {
            anyhow!(
                "MySQL sink reverse lookup requires OpenSearch; set \
                 source.mysql.sink_reverse_lookup=false only when child deletes carry the \
                 parent key"
            )
        })?;
        denorm = denorm.with_reverse_lookup(os.clone());
    }

    let sink_progress = Arc::new(AtomicU64::new(0));
    let source = MySqlCdcSource::new(config)
        .with_sink_progress(Arc::clone(&sink_progress))
        .with_transition_images(true);
    let sink = build_sink(runtime.sink.clone(), &inner_shutdown).await?;
    let dispatcher_config = mysql_dispatcher_config(runtime.engine_config.dispatcher.clone());

    info!(
        bus_capacity = runtime.engine_config.bus_capacity,
        chunk,
        recompose_chunk,
        recompose_concurrency,
        disp_max_events = dispatcher_config.max_events,
        disp_max_bytes = dispatcher_config.max_batch_bytes,
        disp_flush_ms = dispatcher_config.flush_interval.as_millis() as u64,
        disp_parallel = dispatcher_config.max_parallel_bulks,
        "engine knobs (mysql sql-denormalize)"
    );

    let dlq = crate::dlq::DlqWriter::open(runtime.engine_config.dlq_path.clone()).await?;
    let memory_runtime = MemoryRuntime::detect(&runtime.engine_config.memory);
    let memory_shutdown = inner_shutdown.child();
    let memory_monitor = memory_runtime
        .as_ref()
        .map(|memory| memory.spawn(memory_shutdown.clone()));
    let mut dispatcher = crate::dispatcher::Dispatcher::new(
        std::sync::Arc::clone(&sink),
        dlq,
        dispatcher_config,
        inner_shutdown.clone(),
    )
    .with_sink_progress(Arc::clone(&sink_progress));
    if let Some(memory) = &memory_runtime {
        dispatcher = dispatcher.with_memory_budget(memory.budget());
    }

    // source → bus1 → denormalizer (bootstrap + tail) → bus2 → dispatcher.
    let source_bus = memory_runtime.as_ref().map_or_else(
        || EventBus::new(runtime.engine_config.bus_capacity),
        |memory| {
            EventBus::with_memory_budget(
                runtime.engine_config.bus_capacity,
                memory.budget(),
                MemoryAdmission::TransformInput,
            )
        },
    );
    let join_bus = memory_runtime.as_ref().map_or_else(
        || EventBus::new(runtime.engine_config.bus_capacity),
        |memory| {
            EventBus::with_memory_budget(
                runtime.engine_config.bus_capacity,
                memory.budget(),
                MemoryAdmission::TransformOutput,
            )
        },
    );
    let (source_sender, source_receiver) = source_bus.split();
    let (join_sender, join_receiver) = join_bus.split();

    let src_ctx = SourceContext::new(source_sender, inner_shutdown.clone());
    let source_handle = tokio::spawn(async move { source.run(src_ctx).await });
    let denorm_shutdown = inner_shutdown.clone();
    let denorm_handle = tokio::spawn(async move {
        denorm
            .run(source_receiver, join_sender, denorm_shutdown)
            .await;
    });
    let dispatcher_handle = tokio::spawn(dispatcher.run(join_receiver));

    let source_result = source_handle.await;
    if let Ok(Err(err)) = &source_result {
        error!(error = %err, "mysql source (sql-denormalize) returned error");
        inner_shutdown.cancel();
    }
    let _ = denorm_handle.await;
    let _ = dispatcher_handle.await;
    memory_shutdown.cancel();
    if let Some(handle) = memory_monitor {
        let _ = handle.await;
    }

    match source_result {
        Ok(Ok(())) => {
            if outer_shutdown.is_cancelled() {
                Ok(EngineIterationOutcome::Shutdown)
            } else {
                Ok(EngineIterationOutcome::Paused)
            }
        }
        Ok(Err(err)) => Err(anyhow!(err).context("mysql sql-denormalize source")),
        Err(join_err) => Err(anyhow!(join_err).context("mysql sql-denormalize task panicked")),
    }
}

async fn run_cdc_kafka(
    config: KafkaCdcConfig,
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
    shutdown: ShutdownToken,
) -> Result<()> {
    ventstream_telemetry::set_phase(ventstream_telemetry::LifecyclePhase::Starting);
    ventstream_telemetry::set_source("kafka");
    ventstream_telemetry::set_target(sink.kind());

    info!(
        kafka_brokers = %config.brokers,
        kafka_group = %config.group_id,
        sink = sink.kind(),
        sink_endpoint = %sink.endpoint(),
        "cdc pipeline configured (kafka source)"
    );

    run_cdc_loop(
        KafkaBackend {
            config,
            sink,
            engine_config,
        },
        shutdown,
    )
    .await
}

/// Build the Kafka engine for one iteration. Wires the sink-progress watermark
/// (like Postgres): the source commits consumer-group offsets only up to the
/// seq the sink has durably written — no-loss at-least-once.
async fn build_and_run_kafka_engine(
    config: KafkaCdcConfig,
    sink_config: SinkRuntimeConfig,
    engine_cfg: EngineConfig,
    inner_shutdown: ShutdownToken,
    outer_shutdown: ShutdownToken,
) -> Result<EngineIterationOutcome> {
    let sink_progress = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let source =
        KafkaCdcSource::new(config).with_sink_progress(std::sync::Arc::clone(&sink_progress));
    let sink = build_sink(sink_config, &inner_shutdown).await?;

    info!(
        bus_capacity = engine_cfg.bus_capacity,
        disp_max_events = engine_cfg.dispatcher.max_events,
        disp_max_bytes = engine_cfg.dispatcher.max_batch_bytes,
        disp_flush_ms = engine_cfg.dispatcher.flush_interval.as_millis() as u64,
        disp_parallel = engine_cfg.dispatcher.max_parallel_bulks,
        "engine knobs (kafka)"
    );

    let engine = Engine::new(Box::new(source), sink, engine_cfg).with_sink_progress(sink_progress);
    let engine_outcome = engine.run(inner_shutdown.clone()).await;
    if let Err(err) = &engine_outcome {
        ventstream_telemetry::record_error(format!("kafka engine run failed: {err:#}"));
    }

    match engine_outcome {
        Ok(()) if outer_shutdown.is_cancelled() => Ok(EngineIterationOutcome::Shutdown),
        Ok(()) => Ok(EngineIterationOutcome::Paused),
        Err(err) => Err(anyhow!(err).context("cdc engine run (kafka)")),
    }
}

/// Everything the CDC role needs, loaded together so that
/// presence/absence is decided once at startup.
struct CdcBundle {
    /// Which source backend feeds this CDC pipeline. Selected at
    /// startup by `VS_CDC_SOURCE=postgres|neo4j`.
    source: CdcSourceConfig,
    runtime: CdcRuntime,
    /// Join definitions are only consumed by the Postgres path today;
    /// the Neo4j path passes events straight through to the sink in v1.
    joins: Vec<JoinDefinition>,
    /// Raw joins YAML text — passed through so the PG entrypoint can
    /// fingerprint it for YAML-change detection. `None` when no joins
    /// YAML was configured.
    joins_yaml_text: Option<String>,
}

impl CdcBundle {
    async fn validate_redis_drain(&self) -> Result<Vec<String>> {
        let (targets, replayable) = match &self.source {
            CdcSourceConfig::Postgres(config) => (
                postgres_redis_drain_targets(&self.runtime.sink, config, &self.joins).await?,
                true,
            ),
            CdcSourceConfig::Neo4j(config) => (
                neo4j_redis_drain_targets(&self.runtime.sink, config)?,
                config.bootstrap.is_some(),
            ),
            CdcSourceConfig::Mongo(config) => (
                mongodb_redis_drain_targets(&self.runtime.sink, config)?,
                config.bootstrap,
            ),
            CdcSourceConfig::Mysql(config) => (
                mysql_redis_drain_targets(&self.runtime.sink, config, &self.joins)?,
                config.bootstrap,
            ),
            CdcSourceConfig::Kafka(_) => {
                if self.runtime.sink.redis().is_some() {
                    return Err(anyhow!(
                        "Redis drain/rebuild is not supported for Kafka because consumer offsets cannot be reset atomically by the engine"
                    ));
                }
                (Vec::new(), true)
            }
        };
        self.runtime
            .sink
            .validate_redis_drain(replayable, &targets)?;
        Ok(targets)
    }
}

#[derive(Clone)]
struct CdcRuntime {
    sink: SinkRuntimeConfig,
    engine_config: EngineConfig,
    engine_file_config: Option<EngineFileConfig>,
}

#[derive(Clone)]
enum SinkRuntimeConfig {
    OpenSearch(Box<OpenSearchConfig>),
    Redis(Box<RedisConfig>),
    Meilisearch(Box<MeilisearchConfig>),
}

impl SinkRuntimeConfig {
    fn kind(&self) -> &'static str {
        match self {
            Self::OpenSearch(_) => "opensearch",
            Self::Redis(_) => "redis",
            Self::Meilisearch(_) => "meilisearch",
        }
    }

    fn endpoint(&self) -> &str {
        match self {
            Self::OpenSearch(config) => &config.endpoint,
            Self::Redis(config) => config
                .discovery_endpoint()
                .unwrap_or("redis://unconfigured"),
            Self::Meilisearch(config) => &config.endpoint,
        }
    }

    fn open_search(&self) -> Option<&OpenSearchConfig> {
        match self {
            Self::OpenSearch(config) => Some(config),
            Self::Redis(_) | Self::Meilisearch(_) => None,
        }
    }

    fn redis(&self) -> Option<&RedisConfig> {
        match self {
            Self::OpenSearch(_) | Self::Meilisearch(_) => None,
            Self::Redis(config) => Some(config),
        }
    }

    fn validate_redis_drain(&self, replayable: bool, targets: &[String]) -> Result<()> {
        let Some(redis) = self.redis() else {
            return Ok(());
        };
        if redis.keyspace_ownership != RedisKeyspaceOwnership::Exclusive {
            return Err(anyhow!(
                "Redis drain/rebuild requires sink.redis.keyspace.ownership=exclusive; no local cursor state was removed"
            ));
        }
        if !replayable {
            return Err(anyhow!(
                "Redis drain/rebuild requires snapshot bootstrap to be enabled; no local cursor state was removed"
            ));
        }
        if targets.is_empty() {
            return Err(anyhow!(
                "Redis drain/rebuild requires a finite, statically known target set; no local cursor state was removed"
            ));
        }
        Ok(())
    }

    async fn reset_redis_targets_if_configured(&self, targets: &[String]) -> Result<bool> {
        let Some(redis) = self.redis() else {
            return Ok(false);
        };
        RedisSink::reset_owned_targets(redis.clone(), targets)
            .await
            .context("resetting exclusively owned Redis targets")?;
        Ok(true)
    }

    fn ensure_drain_reconciliation_supported(&self) -> Result<()> {
        match self {
            Self::OpenSearch(_) => Ok(()),
            Self::Redis(_) => Err(anyhow!(
                "Redis orphan reconciliation is not available; drain-resume is blocked"
            )),
            Self::Meilisearch(_) => Err(anyhow!(
                "Meilisearch orphan reconciliation is not available; drain-resume is blocked"
            )),
        }
    }

    fn attach_health(&mut self, health: ventstream_core::SinkHealth) {
        match self {
            Self::OpenSearch(config) => config.delivery_health = Some(health),
            Self::Redis(config) => config.delivery_health = Some(health),
            Self::Meilisearch(config) => config.delivery_health = Some(health),
        }
    }
}

fn routed_redis_drain_targets(
    sink: &SinkRuntimeConfig,
    relations: Vec<String>,
    projection_targets: Vec<Option<String>>,
) -> Result<Vec<String>> {
    let Some(redis) = sink.redis() else {
        return Ok(Vec::new());
    };
    let targets = match &redis.key_routing {
        RedisKeyRouting::Fixed(target) => vec![target.clone()],
        RedisKeyRouting::ByOutputRelation => relations,
        RedisKeyRouting::ByProjectionTarget => {
            if projection_targets.iter().any(Option::is_none) {
                return Err(anyhow!(
                    "Redis drain/rebuild with by_projection_target routing requires every projection to declare target.index"
                ));
            }
            projection_targets.into_iter().flatten().collect()
        }
        RedisKeyRouting::Views(views) => views.iter().map(|view| view.name.clone()).collect(),
    };
    let targets = targets
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(anyhow!(
            "Redis drain/rebuild cannot infer a finite target set for the configured routing"
        ));
    }
    Ok(targets)
}

async fn postgres_redis_drain_targets(
    sink: &SinkRuntimeConfig,
    config: &PostgresCdcConfig,
    joins: &[JoinDefinition],
) -> Result<Vec<String>> {
    let relations = if joins.is_empty() {
        let configured = config
            .bootstrap
            .as_ref()
            .map(|bootstrap| {
                bootstrap
                    .tables
                    .iter()
                    .map(|table| table.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !configured.is_empty()
            || !matches!(
                sink.redis().map(|redis| &redis.key_routing),
                Some(RedisKeyRouting::ByOutputRelation)
            )
        {
            configured
        } else {
            let client = ventstream_sources::postgres::connect_client(
                config,
                "Redis drain target discovery",
            )
            .await
            .context("connecting for Redis drain target discovery")?;
            sql_denormalize::discover_direct_projections(&client, &config.publication)
                .await?
                .iter()
                .map(|projection| split_table(&projection.primary.table).1)
                .collect()
        }
    } else {
        joins
            .iter()
            .map(|join| split_table(&join.primary.table).1)
            .collect()
    };
    let projection_targets = joins
        .iter()
        .map(|join| join.target_index().map(str::to_owned))
        .collect();
    routed_redis_drain_targets(sink, relations, projection_targets)
}

fn mysql_redis_drain_targets(
    sink: &SinkRuntimeConfig,
    config: &MySqlCdcConfig,
    joins: &[JoinDefinition],
) -> Result<Vec<String>> {
    let relations = if joins.is_empty() {
        config.tables.clone()
    } else {
        joins
            .iter()
            .map(|join| split_table(&join.primary.table).1)
            .collect()
    };
    let projection_targets = joins
        .iter()
        .map(|join| join.target_index().map(str::to_owned))
        .collect();
    routed_redis_drain_targets(sink, relations, projection_targets)
}

fn mongodb_redis_drain_targets(
    sink: &SinkRuntimeConfig,
    config: &MongoCdcConfig,
) -> Result<Vec<String>> {
    routed_redis_drain_targets(sink, config.collections.clone(), Vec::new())
}

fn neo4j_redis_drain_targets(
    sink: &SinkRuntimeConfig,
    config: &Neo4jCdcConfig,
) -> Result<Vec<String>> {
    let relations = if let Some(specs) = &config.denormalize {
        specs
            .denormalize
            .iter()
            .map(|spec| spec.output_table.clone())
            .collect()
    } else {
        config
            .label_filter
            .iter()
            .map(|label| {
                config
                    .label_table_map
                    .get(label)
                    .cloned()
                    .unwrap_or_else(|| label.clone())
            })
            .chain(config.reltype_filter.iter().map(|rel_type| {
                config
                    .reltype_table_map
                    .get(rel_type)
                    .cloned()
                    .unwrap_or_else(|| rel_type.clone())
            }))
            .collect()
    };
    routed_redis_drain_targets(sink, relations, Vec::new())
}

/// Discriminated union of source-specific configs, picked by env.
/// Boxed because `PostgresCdcConfig` is much larger than
/// `Neo4jCdcConfig` and we don't want clippy `large_enum_variant`
/// noise as more backends land.
enum CdcSourceConfig {
    Postgres(Box<PostgresCdcConfig>),
    Neo4j(Box<Neo4jCdcConfig>),
    Mongo(Box<MongoCdcConfig>),
    Mysql(Box<MySqlCdcConfig>),
    Kafka(Box<KafkaCdcConfig>),
}

struct PipelineEnv {
    roles: HashSet<Role>,
    cdc: Option<CdcBundle>,
    ws: Option<WsConfig>,
    graphql: Option<GraphQlConfig>,
}

/// YAML file shape for the joins config. Lives at the path pointed to
/// by `VS_JOINS_YAML`. Optional — when absent, no joins are wired.
#[derive(Debug, Deserialize)]
struct JoinsFile {
    #[serde(default)]
    joins: Vec<JoinDefinition>,
}

fn fleet_materialized_spec(
    fleet_config: Option<&FleetAppliedConfig>,
    pointer: &str,
    filename: &str,
) -> Result<Option<PathBuf>> {
    match fleet_config {
        Some(config) => config.materialize_text(pointer, filename),
        None => Ok(None),
    }
}

fn load_engine_config_from_env() -> Result<Option<EngineFileConfig>> {
    let Some(path) = opt("VS_ENGINE_CONFIG")? else {
        return Ok(None);
    };
    load_engine_config_from_path(&path).map(Some)
}

fn load_engine_config_from_path(path: &str) -> Result<EngineFileConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading engine config at {path}"))?;
    let mut config = EngineFileConfig::from_yaml_str(&text)
        .with_context(|| format!("validating engine config at {path}"))?;
    resolve_relative_spec_paths(&mut config, Path::new(path))?;
    info!(path = %path, schema_version = config.schema_version, "engine config loaded");
    Ok(config)
}

fn resolve_relative_spec_paths(config: &mut EngineFileConfig, config_path: &Path) -> Result<()> {
    let absolute_config_path = if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolving current directory for engine config")?
            .join(config_path)
    };
    let base = absolute_config_path
        .parent()
        .ok_or_else(|| anyhow!("engine config path has no parent directory"))?;
    for path in [
        &mut config.specs.joins,
        &mut config.specs.neo4j_denormalize,
        &mut config.specs.graphql_schema,
        &mut config.specs.graphql_subscriptions,
        &mut config.specs.graphql_manifest,
    ]
    .into_iter()
    .flatten()
    {
        if path.is_relative() {
            *path = base.join(&*path);
        }
    }
    Ok(())
}

fn roles_from_config(config: Option<&EngineFileConfig>) -> Option<HashSet<Role>> {
    config.map(|config| {
        config
            .roles
            .iter()
            .map(|role| match role {
                ConfigRole::Cdc => Role::Cdc,
                ConfigRole::Ws => Role::Ws,
                ConfigRole::Graphql => Role::GraphQl,
                ConfigRole::Mcp => Role::Mcp,
            })
            .collect()
    })
}

fn load_runtime_roles(engine_config: Option<&EngineFileConfig>) -> Result<HashSet<Role>> {
    match roles_from_config(engine_config) {
        Some(roles) => Ok(roles),
        None => parse_roles(&opt("VS_ROLES")?.unwrap_or_else(|| "cdc".to_string())),
    }
}

fn config_spec_path(
    config: Option<&EngineFileConfig>,
    selector: fn(&EngineFileConfig) -> Option<&PathBuf>,
) -> Option<PathBuf> {
    config.and_then(selector).cloned()
}

fn load_joins_yaml(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<(Vec<JoinDefinition>, Option<String>)> {
    if let Some(config) = fleet_config {
        if let Some(text) = config.text_at("/specs/joins_yaml")? {
            let parsed: JoinsFile =
                serde_yaml::from_str(text).context("parsing Fleet-applied joins YAML")?;
            if parsed.joins.is_empty() {
                warn!("Fleet-applied joins YAML had no `joins:` entries");
            }
            return Ok((parsed.joins, Some(text.to_owned())));
        }
    }

    if let Some(path) = config_spec_path(engine_config, |config| config.specs.joins.as_ref()) {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading joins YAML at {}", path.display()))?;
        let parsed: JoinsFile = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing joins YAML at {}", path.display()))?;
        if parsed.joins.is_empty() {
            warn!(path = %path.display(), "joins YAML had no `joins:` entries");
        }
        return Ok((parsed.joins, Some(text)));
    }

    match opt("VS_JOINS_YAML")? {
        None => Ok((Vec::new(), None)),
        Some(path) => {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading joins YAML at {path}"))?;
            let parsed: JoinsFile = serde_yaml::from_str(&text)
                .with_context(|| format!("parsing joins YAML at {path}"))?;
            if parsed.joins.is_empty() {
                warn!(path = %path, "joins YAML had no `joins:` entries");
            }
            Ok((parsed.joins, Some(text)))
        }
    }
}

fn validate_projection_target_indexes(
    engine_config: Option<&EngineFileConfig>,
    joins: &[JoinDefinition],
) -> Result<()> {
    let by_projection_target = engine_config
        .and_then(|config| config.sink.as_ref())
        .and_then(|sink| sink.opensearch.as_ref())
        .is_some_and(|opensearch| {
            matches!(
                opensearch.index_routing,
                OpenSearchIndexRouting::ByProjectionTarget
            )
        });
    if !by_projection_target {
        return Ok(());
    }

    let missing: Vec<&str> = joins
        .iter()
        .filter(|definition| definition.target_index().is_none())
        .map(JoinDefinition::effective_name)
        .collect();
    if joins.is_empty() || !missing.is_empty() {
        let detail = if joins.is_empty() {
            "no join definitions were loaded".to_owned()
        } else {
            format!("missing target.index for: {}", missing.join(", "))
        };
        return Err(anyhow!(
            "by_projection_target routing requires target.index on every join definition ({detail})"
        ));
    }
    Ok(())
}

impl PipelineEnv {
    fn load(
        fleet_config: Option<&FleetAppliedConfig>,
        engine_config: Option<&EngineFileConfig>,
    ) -> Result<Self> {
        let roles = load_runtime_roles(engine_config)?;

        let cdc = if roles.contains(&Role::Cdc) {
            Some(load_cdc_bundle(fleet_config, engine_config)?)
        } else {
            None
        };

        let ws = if roles.contains(&Role::Ws) {
            Some(load_ws_config(engine_config)?)
        } else {
            None
        };

        let graphql = if roles.contains(&Role::GraphQl) {
            Some(load_graphql_config(fleet_config, engine_config)?)
        } else {
            None
        };

        Ok(Self {
            roles,
            cdc,
            ws,
            graphql,
        })
    }
}

fn load_graphql_config(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<GraphQlConfig> {
    let runtime_config = engine_config.map(|config| &config.runtime.graphql);
    let shared_realtime = engine_config.map(|config| &config.runtime.realtime);
    let listen: SocketAddr = runtime_config
        .and_then(|config| config.listen.clone())
        .or(opt("VS_GRAPHQL_LISTEN")?)
        .unwrap_or_else(|| "0.0.0.0:4041".to_string())
        .parse()
        .context("parsing VS_GRAPHQL_LISTEN as host:port")?;
    let nats_url = runtime_config
        .and_then(|config| config.nats_url.clone())
        .or(opt("VS_GRAPHQL_NATS_URL")?)
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
    let stream_name = runtime_config
        .and_then(|config| config.stream.clone())
        .or(opt("VS_GRAPHQL_STREAM")?)
        .unwrap_or_else(|| "ventstream".to_string());
    let mut cfg = GraphQlConfig {
        listen,
        nats_url,
        stream_name,
        ..GraphQlConfig::default()
    };
    let configured_provider = runtime_config
        .and_then(|config| config.provider)
        .or_else(|| shared_realtime.and_then(|config| config.provider));
    let role_environment_provider = opt("VS_GRAPHQL_PROVIDER")?
        .map(|value| parse_realtime_provider(&value))
        .transpose()?;
    let shared_environment_provider = opt("VS_REALTIME_PROVIDER")?
        .map(|value| parse_realtime_provider(&value))
        .transpose()?;
    if let (Some(role), Some(shared)) = (role_environment_provider, shared_environment_provider) {
        if role != shared {
            anyhow::bail!("VS_GRAPHQL_PROVIDER and VS_REALTIME_PROVIDER disagree");
        }
    }
    let environment_provider = role_environment_provider.or(shared_environment_provider);
    if let (Some(configured), Some(environment)) = (configured_provider, environment_provider) {
        if configured != environment {
            anyhow::bail!(
                "runtime.graphql.provider and VS_GRAPHQL_PROVIDER select different realtime brokers"
            );
        }
    }
    let redis_config = runtime_config
        .and_then(|config| config.redis_streams.as_ref())
        .or_else(|| shared_realtime.and_then(|config| config.redis_streams.as_ref()));
    let inferred_provider = if redis_config.is_some() {
        RealtimeBrokerProvider::RedisStreams
    } else {
        RealtimeBrokerProvider::NatsJetstream
    };
    let provider = configured_provider
        .or(environment_provider)
        .unwrap_or(inferred_provider);
    if provider == RealtimeBrokerProvider::NatsCore {
        anyhow::bail!("GraphQL subscriptions require nats_jetstream or redis_streams");
    }
    if provider != inferred_provider && redis_config.is_some() {
        anyhow::bail!(
            "selected runtime.graphql provider conflicts with provider-specific settings"
        );
    }
    if provider == RealtimeBrokerProvider::RedisStreams {
        let mut redis = RedisStreamsConfig::default();
        if let Some(reference) = redis_config.and_then(|config| config.url_ref.as_ref()) {
            redis.url = resolve_value_ref(reference)?;
        } else if let Some(url) = opt("VS_GRAPHQL_REDIS_URL")?.or(opt("VS_REDIS_URL")?) {
            redis.url = url;
        }
        if let Some(prefix) = redis_config
            .and_then(|config| config.key_prefix.clone())
            .or(opt("VS_GRAPHQL_REDIS_KEY_PREFIX")?.or(opt("VS_REDIS_KEY_PREFIX")?))
        {
            redis.key_prefix = prefix;
        }
        if let Some(value) = redis_config.and_then(|config| config.read_batch) {
            redis.read_batch = value;
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_READ_BATCH")?.or(opt("VS_REDIS_READ_BATCH")?)
        {
            redis.read_batch = lenient_int("VS_GRAPHQL_REDIS_READ_BATCH", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.block_timeout_ms) {
            redis.block_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_BLOCK_TIMEOUT_MS")?.or(opt("VS_REDIS_BLOCK_TIMEOUT_MS")?)
        {
            redis.block_timeout =
                Duration::from_millis(lenient_int("VS_GRAPHQL_REDIS_BLOCK_TIMEOUT_MS", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.broadcast_capacity) {
            redis.broadcast_capacity = value;
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_BROADCAST_CAPACITY")?.or(opt("VS_REDIS_BROADCAST_CAPACITY")?)
        {
            redis.broadcast_capacity = lenient_int("VS_GRAPHQL_REDIS_BROADCAST_CAPACITY", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.max_tenant_hubs) {
            redis.max_tenant_hubs = value;
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_MAX_TENANT_HUBS")?.or(opt("VS_REDIS_MAX_TENANT_HUBS")?)
        {
            redis.max_tenant_hubs = lenient_int("VS_GRAPHQL_REDIS_MAX_TENANT_HUBS", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.max_length) {
            redis.max_length = Some(value);
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_MAX_LENGTH")?.or(opt("VS_REDIS_MAX_LENGTH")?)
        {
            redis.max_length = Some(lenient_int("VS_GRAPHQL_REDIS_MAX_LENGTH", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.connect_timeout_ms) {
            redis.connect_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_CONNECT_TIMEOUT_MS")?.or(opt("VS_REDIS_CONNECT_TIMEOUT_MS")?)
        {
            redis.connect_timeout =
                Duration::from_millis(lenient_int("VS_GRAPHQL_REDIS_CONNECT_TIMEOUT_MS", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.response_timeout_ms) {
            redis.response_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_GRAPHQL_REDIS_RESPONSE_TIMEOUT_MS")?.or(opt("VS_REDIS_RESPONSE_TIMEOUT_MS")?)
        {
            redis.response_timeout =
                Duration::from_millis(lenient_int("VS_GRAPHQL_REDIS_RESPONSE_TIMEOUT_MS", &value)?);
        }
        cfg.redis_streams = Some(redis);
    }
    if let Some(p) = runtime_config
        .and_then(|config| config.pod_id.clone())
        .or(opt("VS_GRAPHQL_POD_ID")?)
    {
        cfg.pod_id = p;
    }
    if let Some(ms) = runtime_config.and_then(|config| config.inactive_threshold_ms) {
        cfg.consumer_inactive_threshold = Duration::from_millis(ms);
    } else if let Some(s) = opt("VS_GRAPHQL_INACTIVE_THRESHOLD_MS")? {
        let ms: u64 = lenient_int("VS_GRAPHQL_INACTIVE_THRESHOLD_MS", &s)?;
        cfg.consumer_inactive_threshold = Duration::from_millis(ms);
    }
    if let Some(ms) = runtime_config.and_then(|config| config.reaper_interval_ms) {
        cfg.reaper_interval = Duration::from_millis(ms);
    } else if let Some(s) = opt("VS_GRAPHQL_REAPER_INTERVAL_MS")? {
        let ms: u64 = lenient_int("VS_GRAPHQL_REAPER_INTERVAL_MS", &s)?;
        cfg.reaper_interval = Duration::from_millis(ms);
    }
    if let Some(cap) = runtime_config.and_then(|config| config.broadcast_capacity) {
        cfg.broadcast_capacity = cap.max(1);
    } else if let Some(s) = opt("VS_GRAPHQL_BROADCAST_CAP")? {
        // Clamp to >= 1 — a zero-capacity broadcast channel panics.
        let cap: usize = lenient_int("VS_GRAPHQL_BROADCAST_CAP", &s)?;
        cfg.broadcast_capacity = cap.max(1);
    }
    if let Some(path) = fleet_materialized_spec(
        fleet_config,
        "/specs/graphql_manifest_yaml",
        "graphql-manifest.yaml",
    )? {
        cfg.manifest_path = Some(path);
    } else if let Some(path) = config_spec_path(engine_config, |config| {
        config.specs.graphql_manifest.as_ref()
    }) {
        cfg.manifest_path = Some(path);
    } else if let Some(path) = opt("VS_GRAPHQL_MANIFEST")? {
        cfg.manifest_path = Some(PathBuf::from(path));
    }
    if let Some(path) = fleet_materialized_spec(
        fleet_config,
        "/specs/graphql_subscriptions_yaml",
        "graphql-subscriptions.yaml",
    )? {
        cfg.subscriptions_path = Some(path);
    } else if let Some(path) = config_spec_path(engine_config, |config| {
        config.specs.graphql_subscriptions.as_ref()
    }) {
        cfg.subscriptions_path = Some(path);
    } else if let Some(path) = opt("VS_GRAPHQL_SUBSCRIPTIONS")? {
        cfg.subscriptions_path = Some(PathBuf::from(path));
    }
    if let Some(path) = fleet_materialized_spec(
        fleet_config,
        "/specs/graphql_schema",
        "graphql-schema.graphql",
    )? {
        cfg.schema_path = Some(path);
    } else if let Some(path) =
        config_spec_path(engine_config, |config| config.specs.graphql_schema.as_ref())
    {
        cfg.schema_path = Some(path);
    } else if let Some(path) = opt("VS_GRAPHQL_SCHEMA")? {
        cfg.schema_path = Some(PathBuf::from(path));
    }
    cfg.playground = runtime_config
        .and_then(|config| config.playground)
        .unwrap_or(opt("VS_GRAPHQL_PLAYGROUND")?.as_deref() == Some("1"));
    cfg.expected_tenant = load_expected_tenant(engine_config, "graphql")?;
    Ok(cfg)
}

/// Read the deployment's single tenant (`VS_TENANT`) for the gateway tenant
/// gate (the "single tenant per deployment" cross-tenant-isolation model).
/// Blank value → `None`.
///
/// When `None`, the gateway does NOT enforce tenant isolation — any
/// client-asserted tenant is accepted. That's the dev / legacy posture, so we
/// warn loudly here to make a misconfigured production visible in logs.
fn load_expected_tenant(
    engine_config: Option<&EngineFileConfig>,
    role: &str,
) -> Result<Option<String>> {
    let tenant = engine_config
        .and_then(|config| config.runtime.tenant.clone())
        .or(opt("VS_TENANT")?)
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    match &tenant {
        Some(t) => {
            info!(role, tenant = %t, "tenant isolation enforced (single tenant per deployment)")
        }
        None => warn!(
            role,
            "VS_TENANT not set — {role} gateway tenant isolation is NOT enforced; any \
             client-asserted tenant is accepted. Set VS_TENANT to the tenant this \
             deployment serves to close cross-tenant access."
        ),
    }
    Ok(tenant)
}

fn load_cdc_bundle(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<CdcBundle> {
    // Pick the source backend. Default `postgres` preserves the old
    // single-source behavior; `neo4j` switches to the Bolt-based CDC.
    let backend = engine_config
        .and_then(|config| config.source.as_ref())
        .map(|source| source.kind);
    match backend {
        Some(SourceKind::Postgres) => load_cdc_bundle_postgres(fleet_config, engine_config),
        Some(SourceKind::Neo4j) => load_cdc_bundle_neo4j(fleet_config, engine_config),
        Some(SourceKind::Mongo | SourceKind::Mongodb) => load_cdc_bundle_mongodb(engine_config),
        Some(SourceKind::Mysql) => load_cdc_bundle_mysql(fleet_config, engine_config),
        Some(SourceKind::Kafka | SourceKind::Redpanda) => load_cdc_bundle_kafka(engine_config),
        None => match opt("VS_CDC_SOURCE")?
            .unwrap_or_else(|| "postgres".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "postgres" | "pg" => load_cdc_bundle_postgres(fleet_config, engine_config),
            "neo4j" | "neo" => load_cdc_bundle_neo4j(fleet_config, engine_config),
            "mongodb" | "mongo" => load_cdc_bundle_mongodb(engine_config),
            "mysql" | "mariadb" => load_cdc_bundle_mysql(fleet_config, engine_config),
            "kafka" | "redpanda" => load_cdc_bundle_kafka(engine_config),
            other => Err(anyhow!(
                "unknown VS_CDC_SOURCE '{other}' (expected 'postgres', 'neo4j', 'mongodb', 'mysql', or 'kafka')"
            )),
        },
    }
}

fn load_sink_config(engine_config: Option<&EngineFileConfig>) -> Result<SinkRuntimeConfig> {
    let configured_kind = engine_config
        .and_then(|config| config.sink.as_ref())
        .map(|sink| sink.kind);
    let env_kind = if configured_kind.is_none() {
        opt("VS_SINK")?.unwrap_or_else(|| "opensearch".to_owned())
    } else {
        String::new()
    };
    match configured_kind {
        Some(SinkKind::Opensearch | SinkKind::Elasticsearch) => {
            load_opensearch_config(engine_config)
                .map(Box::new)
                .map(SinkRuntimeConfig::OpenSearch)
        }
        Some(SinkKind::Redis) => load_redis_sink_config(engine_config)
            .map(Box::new)
            .map(SinkRuntimeConfig::Redis),
        Some(SinkKind::Meilisearch) => load_meilisearch_config(engine_config)
            .map(Box::new)
            .map(SinkRuntimeConfig::Meilisearch),
        None => match env_kind.trim().to_ascii_lowercase().as_str() {
            "opensearch" | "elasticsearch" | "es" => load_opensearch_config(engine_config)
                .map(Box::new)
                .map(SinkRuntimeConfig::OpenSearch),
            "redis" => load_redis_sink_config(engine_config)
                .map(Box::new)
                .map(SinkRuntimeConfig::Redis),
            "meilisearch" | "meili" => load_meilisearch_config(engine_config)
                .map(Box::new)
                .map(SinkRuntimeConfig::Meilisearch),
            other => Err(anyhow!(
                "unknown VS_SINK '{other}' (expected 'opensearch', 'elasticsearch', 'redis', or 'meilisearch')"
            )),
        },
    }
}

fn load_meilisearch_config(engine_config: Option<&EngineFileConfig>) -> Result<MeilisearchConfig> {
    let file = engine_config
        .and_then(|config| config.sink.as_ref())
        .and_then(|sink| sink.meilisearch.as_ref());
    let Some(meili) = file else {
        return load_meilisearch_config_from_env();
    };
    let endpoint = resolve_value_ref(&meili.endpoint_ref)?;
    let mut config = MeilisearchConfig::new("meilisearch", endpoint);
    config.api_key = meili
        .api_key_ref
        .as_ref()
        .map(resolve_value_ref)
        .transpose()?;
    config.index_routing = match &meili.index_routing {
        FileMeilisearchIndexRouting::ByOutputRelation => MeilisearchIndexRouting::ByOutputRelation,
        FileMeilisearchIndexRouting::ByProjectionTarget => {
            MeilisearchIndexRouting::ByProjectionTarget
        }
        FileMeilisearchIndexRouting::Fixed { index } => {
            MeilisearchIndexRouting::Fixed(index.clone())
        }
    };
    if let Some(prefix) = &meili.index_prefix {
        config.index_prefix = prefix.clone();
    }
    if let Some(auto_create) = meili.auto_create_indexes {
        config.auto_create_indexes = auto_create;
    }
    if let Some(field) = &meili.primary_key_field {
        config.primary_key_field = field.clone();
    }
    if let Some(docs) = meili.max_batch_docs {
        config.batching.max_docs = docs;
    }
    if let Some(bytes) = meili.max_batch_bytes {
        config.batching.max_bytes = bytes;
    }
    if let Some(deadline) = meili.task_deadline_ms {
        config.task.deadline = Duration::from_millis(deadline);
    }
    if let Some(timeout) = meili.request_timeout_ms {
        config.request_timeout = Duration::from_millis(timeout);
    }
    config.settings = meili.settings.as_ref().map(|settings| MeilisearchSettings {
        filterable_attributes: settings.filterable_attributes.clone(),
        sortable_attributes: settings.sortable_attributes.clone(),
    });
    let tls = database_tls_or_env(
        meili.tls.as_ref(),
        "VS_MEILI_TLS_MODE",
        "VS_MEILI_TLS_CA_FILE",
        None,
    )?;
    let insecure_tls = config_bool_or_env(meili.insecure_tls, "VS_INSECURE_TLS", false);
    if tls.is_some() && insecure_tls {
        return Err(anyhow!(
            "strict Meilisearch TLS and VS_INSECURE_TLS=true are mutually exclusive"
        ));
    }
    if let Some(tls) = tls {
        match tls.mode {
            DatabaseTlsMode::VerifyFull => {
                if !config.endpoint.starts_with("https://") {
                    return Err(anyhow!(
                        "sink.meilisearch.tls.mode=verify_full requires an https:// endpoint"
                    ));
                }
                if let Some(path) = tls.ca_file {
                    config.ca_file = Some(path);
                }
            }
            DatabaseTlsMode::Disabled => {
                if !config.endpoint.starts_with("http://") {
                    return Err(anyhow!(
                        "sink.meilisearch.tls.mode=disabled requires an http:// endpoint"
                    ));
                }
            }
        }
    }
    if insecure_tls {
        config.verify_tls = false;
    }
    Ok(config)
}

fn load_meilisearch_config_from_env() -> Result<MeilisearchConfig> {
    let endpoint = req("VS_MEILI_ENDPOINT")?;
    let mut config = MeilisearchConfig::new("meilisearch", endpoint);
    config.api_key = opt("VS_MEILI_API_KEY")?;
    if let Some(prefix) = opt("VS_MEILI_INDEX_PREFIX")? {
        config.index_prefix = prefix;
    }
    if let Some(index) = opt("VS_MEILI_INDEX")? {
        config.index_routing = MeilisearchIndexRouting::Fixed(index);
    }
    if opt("VS_INSECURE_TLS")?.as_deref() == Some("true") {
        config.verify_tls = false;
    }
    Ok(config)
}

fn load_redis_sink_config(engine_config: Option<&EngineFileConfig>) -> Result<RedisConfig> {
    let file = engine_config
        .and_then(|config| config.sink.as_ref())
        .and_then(|sink| sink.redis.as_ref());
    let topology = match file {
        Some(config) => match &config.topology {
            None => RedisTopology::Standalone {
                endpoint: resolve_value_ref(config.endpoint_ref.as_ref().ok_or_else(|| {
                    anyhow!("sink.redis.endpoint_ref is required for standalone topology")
                })?)?,
            },
            Some(FileRedisTopology::Cluster { endpoints }) => RedisTopology::Cluster {
                endpoints: endpoints
                    .iter()
                    .map(resolve_value_ref)
                    .collect::<Result<Vec<_>>>()?,
            },
            Some(FileRedisTopology::Sentinel {
                service_name,
                endpoints,
                data_node_tls,
                sentinel_auth,
                sentinel_tls,
            }) => {
                let ResolvedRedisAuth {
                    username,
                    password,
                    username_file,
                    password_file,
                } = resolve_redis_auth(sentinel_auth.as_ref())?;
                RedisTopology::Sentinel(RedisSentinelTopology {
                    endpoints: endpoints
                        .iter()
                        .map(resolve_value_ref)
                        .collect::<Result<Vec<_>>>()?,
                    service_name: service_name.clone(),
                    data_node_tls: *data_node_tls,
                    username,
                    password,
                    username_file,
                    password_file,
                    tls: load_redis_tls(
                        sentinel_tls.as_ref(),
                        "VS_REDIS_SINK_SENTINEL_TLS_CA_FILE",
                        "VS_REDIS_SINK_SENTINEL_TLS_CLIENT_CERT_FILE",
                        "VS_REDIS_SINK_SENTINEL_TLS_CLIENT_KEY_FILE",
                    )?,
                })
            }
        },
        None => match opt("VS_REDIS_SINK_TOPOLOGY")?
            .unwrap_or_else(|| "standalone".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "standalone" => RedisTopology::Standalone {
                endpoint: req("VS_REDIS_SINK_URL")?,
            },
            "cluster" => RedisTopology::Cluster {
                endpoints: redis_endpoint_list_env("VS_REDIS_SINK_CLUSTER_URLS")?,
            },
            "sentinel" => RedisTopology::Sentinel(RedisSentinelTopology {
                endpoints: redis_endpoint_list_env("VS_REDIS_SINK_SENTINEL_URLS")?,
                service_name: req("VS_REDIS_SINK_SENTINEL_SERVICE")?,
                data_node_tls: bool_env("VS_REDIS_SINK_SENTINEL_DATA_NODE_TLS", false),
                username: std::env::var("VS_REDIS_SINK_SENTINEL_USERNAME").ok(),
                password: std::env::var("VS_REDIS_SINK_SENTINEL_PASSWORD").ok(),
                username_file: opt("VS_REDIS_SINK_SENTINEL_USERNAME_FILE")?.map(PathBuf::from),
                password_file: opt("VS_REDIS_SINK_SENTINEL_PASSWORD_FILE")?.map(PathBuf::from),
                tls: load_redis_tls(
                    None,
                    "VS_REDIS_SINK_SENTINEL_TLS_CA_FILE",
                    "VS_REDIS_SINK_SENTINEL_TLS_CLIENT_CERT_FILE",
                    "VS_REDIS_SINK_SENTINEL_TLS_CLIENT_KEY_FILE",
                )?,
            }),
            other => {
                return Err(anyhow!(
                    "unknown VS_REDIS_SINK_TOPOLOGY '{other}' (expected standalone, sentinel, or cluster)"
                ))
            }
        },
    };
    let key_prefix = match file {
        Some(config) => config.keyspace.prefix.clone(),
        None => req("VS_REDIS_SINK_KEY_PREFIX")?,
    };
    let key_routing = match file.map(|config| &config.keyspace.routing) {
        Some(FileRedisKeyRouting::ByOutputRelation) => RedisKeyRouting::ByOutputRelation,
        Some(FileRedisKeyRouting::ByProjectionTarget) => RedisKeyRouting::ByProjectionTarget,
        Some(FileRedisKeyRouting::Fixed { name }) => RedisKeyRouting::Fixed(name.clone()),
        Some(FileRedisKeyRouting::Views { views }) => RedisKeyRouting::Views(
            views
                .iter()
                .map(|view| RedisView {
                    name: view.name.clone(),
                    source: RedisViewSource {
                        namespace: view.source.namespace.clone(),
                        relation: view.source.relation.clone(),
                        projection_target: view.source.projection_target.clone(),
                    },
                    key: RedisViewKey {
                        template: view.key.template.clone(),
                        on_missing: match view.key.on_missing {
                            FileRedisViewMissingBehavior::Block => RedisViewMissingBehavior::Block,
                            FileRedisViewMissingBehavior::Skip => RedisViewMissingBehavior::Skip,
                        },
                    },
                    filter: view.filter.as_ref().map(|filter| RedisViewFilter {
                        mode: match filter.mode {
                            FileRedisViewFilterMode::All => RedisViewFilterMode::All,
                            FileRedisViewFilterMode::Any => RedisViewFilterMode::Any,
                        },
                        conditions: filter
                            .conditions
                            .iter()
                            .map(|condition| match condition {
                                FileRedisViewCondition::Equals { path, value } => {
                                    RedisViewCondition {
                                        path: path.clone(),
                                        operator: RedisViewConditionOperator::Equals(value.clone()),
                                    }
                                }
                                FileRedisViewCondition::NotEquals { path, value } => {
                                    RedisViewCondition {
                                        path: path.clone(),
                                        operator: RedisViewConditionOperator::NotEquals(
                                            value.clone(),
                                        ),
                                    }
                                }
                                FileRedisViewCondition::In { path, values } => RedisViewCondition {
                                    path: path.clone(),
                                    operator: RedisViewConditionOperator::In(values.clone()),
                                },
                                FileRedisViewCondition::NotIn { path, values } => {
                                    RedisViewCondition {
                                        path: path.clone(),
                                        operator: RedisViewConditionOperator::NotIn(values.clone()),
                                    }
                                }
                                FileRedisViewCondition::Exists { path } => RedisViewCondition {
                                    path: path.clone(),
                                    operator: RedisViewConditionOperator::Exists,
                                },
                                FileRedisViewCondition::NotExists { path } => RedisViewCondition {
                                    path: path.clone(),
                                    operator: RedisViewConditionOperator::NotExists,
                                },
                            })
                            .collect(),
                    }),
                    value: match &view.value {
                        FileRedisViewValue::Document => RedisViewValue::Document,
                        FileRedisViewValue::Pointer { path } => {
                            RedisViewValue::Pointer(path.clone())
                        }
                        FileRedisViewValue::Fields { fields } => {
                            RedisViewValue::Fields(fields.clone())
                        }
                    },
                })
                .collect(),
        ),
        None => match opt("VS_REDIS_SINK_KEY_ROUTING")?
            .unwrap_or_else(|| "by_output_relation".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "by_output_relation" | "relation" => RedisKeyRouting::ByOutputRelation,
            "by_projection_target" | "projection" => RedisKeyRouting::ByProjectionTarget,
            "fixed" => RedisKeyRouting::Fixed(req("VS_REDIS_SINK_FIXED_TARGET")?),
            other => {
                return Err(anyhow!(
                    "unknown VS_REDIS_SINK_KEY_ROUTING '{other}' \
                     (expected by_output_relation, by_projection_target, or fixed)"
                ))
            }
        },
    };
    let keyspace_ownership = match file.map(|config| config.keyspace.ownership) {
        Some(FileRedisKeyspaceOwnership::Shared) => RedisKeyspaceOwnership::Shared,
        Some(FileRedisKeyspaceOwnership::Exclusive) => RedisKeyspaceOwnership::Exclusive,
        None => match opt("VS_REDIS_SINK_KEYSPACE_OWNERSHIP")?
            .unwrap_or_else(|| "shared".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "shared" => RedisKeyspaceOwnership::Shared,
            "exclusive" => RedisKeyspaceOwnership::Exclusive,
            other => {
                return Err(anyhow!(
                    "unknown VS_REDIS_SINK_KEYSPACE_OWNERSHIP '{other}' \
                     (expected shared or exclusive)"
                ))
            }
        },
    };
    let ResolvedRedisAuth {
        username,
        password,
        username_file,
        password_file,
    } = match file {
        None => ResolvedRedisAuth {
            username: std::env::var("VS_REDIS_SINK_USERNAME").ok(),
            password: std::env::var("VS_REDIS_SINK_PASSWORD").ok(),
            username_file: opt("VS_REDIS_SINK_USERNAME_FILE")?.map(PathBuf::from),
            password_file: opt("VS_REDIS_SINK_PASSWORD_FILE")?.map(PathBuf::from),
        },
        Some(config) => resolve_redis_auth(config.auth.as_ref())?,
    };
    let document_format = match file.map(|config| config.document.format) {
        Some(FileRedisDocumentFormat::String) => RedisDocumentFormat::String,
        Some(FileRedisDocumentFormat::Json) => RedisDocumentFormat::Json,
        None => match opt("VS_REDIS_SINK_DOCUMENT_FORMAT")?
            .unwrap_or_else(|| "string".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "string" => RedisDocumentFormat::String,
            "json" | "redis_json" => RedisDocumentFormat::Json,
            other => {
                return Err(anyhow!(
                    "unknown VS_REDIS_SINK_DOCUMENT_FORMAT '{other}' (expected string or json)"
                ))
            }
        },
    };
    let contract = match file.map(|config| &config.contract) {
        Some(FileRedisContract::MaterializedView) => RedisContract::MaterializedView,
        Some(FileRedisContract::Cache { ttl_ms }) => RedisContract::Cache {
            ttl: Duration::from_millis(*ttl_ms),
        },
        None => match opt("VS_REDIS_SINK_CONTRACT")?
            .unwrap_or_else(|| "materialized_view".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "materialized_view" | "materialized" => RedisContract::MaterializedView,
            "cache" => RedisContract::Cache {
                ttl: Duration::from_millis(lenient_int::<u64>(
                    "VS_REDIS_SINK_TTL_MS",
                    &req("VS_REDIS_SINK_TTL_MS")?,
                )?),
            },
            other => {
                return Err(anyhow!(
                    "unknown VS_REDIS_SINK_CONTRACT '{other}' (expected materialized_view or cache)"
                ))
            }
        },
    };
    let acknowledgement = match file.map(|config| &config.acknowledgement) {
        Some(FileRedisAcknowledgement::Primary) => RedisAcknowledgement::Primary,
        Some(FileRedisAcknowledgement::Replicated {
            replicas,
            timeout_ms,
        }) => RedisAcknowledgement::Replicated {
            replicas: *replicas,
            timeout: Duration::from_millis(*timeout_ms),
        },
        Some(FileRedisAcknowledgement::Aof {
            local,
            replicas,
            timeout_ms,
        }) => RedisAcknowledgement::Aof {
            local: *local,
            replicas: *replicas,
            timeout: Duration::from_millis(*timeout_ms),
        },
        None => load_redis_acknowledgement_from_env()?,
    };
    let tls = load_redis_tls(
        file.and_then(|config| config.tls.as_ref()),
        "VS_REDIS_SINK_TLS_CA_FILE",
        "VS_REDIS_SINK_TLS_CLIENT_CERT_FILE",
        "VS_REDIS_SINK_TLS_CLIENT_KEY_FILE",
    )?;
    let writer_id = match file.and_then(|config| config.writer.id_ref.as_ref()) {
        Some(reference) => resolve_value_ref(reference)?,
        None => opt("VS_REDIS_SINK_WRITER_ID")?
            .or_else(|| std::env::var("VS_FLEET_DEPLOYMENT_ID").ok())
            .unwrap_or_else(|| "standalone".to_owned()),
    };
    let writer_takeover_from =
        match file.and_then(|config| config.writer.takeover_from_ref.as_ref()) {
            Some(reference) => Some(resolve_value_ref(reference)?),
            None => opt("VS_REDIS_SINK_WRITER_TAKEOVER_FROM")?,
        };

    let bootstrap_endpoint = match &topology {
        RedisTopology::Standalone { endpoint } => endpoint.clone(),
        RedisTopology::Sentinel(sentinel) => sentinel
            .endpoints
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Redis Sentinel requires at least one endpoint"))?,
        RedisTopology::Cluster { endpoints } => endpoints
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Redis Cluster requires at least one endpoint"))?,
    };
    let mut config = RedisConfig::new("redis", bootstrap_endpoint, key_prefix, key_routing)
        .with_topology(topology)
        .with_keyspace_ownership(keyspace_ownership)
        .with_auth_sources(username, password, username_file, password_file)
        .with_tls(tls)
        .with_document_format(document_format)
        .with_contract(contract)
        .with_acknowledgement(acknowledgement)
        .with_writer_id(writer_id);
    if let Some(previous) = writer_takeover_from {
        config = config.with_writer_takeover_from(previous);
    }
    config.writer_lease = config_duration_ms_or_env(
        file.and_then(|config| config.writer.lease_ms),
        "VS_REDIS_SINK_WRITER_LEASE_MS",
        config.writer_lease,
    )?;
    config.max_batch_bytes = config_usize_or_env(
        file.and_then(|config| config.max_batch_bytes),
        "VS_REDIS_SINK_MAX_BATCH_BYTES",
        config.max_batch_bytes,
    )?;
    config.max_key_bytes = config_usize_or_env(
        file.and_then(|config| config.max_key_bytes),
        "VS_REDIS_SINK_MAX_KEY_BYTES",
        config.max_key_bytes,
    )?;
    config.max_value_bytes = config_usize_or_env(
        file.and_then(|config| config.max_value_bytes),
        "VS_REDIS_SINK_MAX_VALUE_BYTES",
        config.max_value_bytes,
    )?;
    config.connect_timeout = config_duration_ms_or_env(
        file.and_then(|config| config.connect_timeout_ms),
        "VS_REDIS_SINK_CONNECT_TIMEOUT_MS",
        config.connect_timeout,
    )?;
    config.response_timeout = config_duration_ms_or_env(
        file.and_then(|config| config.response_timeout_ms),
        "VS_REDIS_SINK_RESPONSE_TIMEOUT_MS",
        config.response_timeout,
    )?;
    Ok(config)
}

fn load_redis_acknowledgement_from_env() -> Result<RedisAcknowledgement> {
    let mode = opt("VS_REDIS_SINK_ACK_MODE")?.map(|value| value.trim().to_ascii_lowercase());
    let replicas = || -> Result<usize> {
        match opt("VS_REDIS_SINK_ACK_REPLICAS")? {
            Some(value) => lenient_int("VS_REDIS_SINK_ACK_REPLICAS", &value),
            None => Ok(0),
        }
    };
    let timeout = || opt_duration_ms("VS_REDIS_SINK_ACK_TIMEOUT_MS", Duration::from_secs(1));
    match mode.as_deref() {
        Some("primary") => Ok(RedisAcknowledgement::Primary),
        Some("replicated") => {
            let replicas = replicas()?;
            if replicas == 0 {
                return Err(anyhow!(
                    "VS_REDIS_SINK_ACK_REPLICAS must be positive when VS_REDIS_SINK_ACK_MODE=replicated"
                ));
            }
            Ok(RedisAcknowledgement::Replicated {
                replicas,
                timeout: timeout()?,
            })
        }
        Some("aof") => Ok(RedisAcknowledgement::Aof {
            local: strict_bool_env("VS_REDIS_SINK_ACK_LOCAL_AOF", true)?,
            replicas: replicas()?,
            timeout: timeout()?,
        }),
        Some(other) => Err(anyhow!(
            "unknown VS_REDIS_SINK_ACK_MODE '{other}' (expected primary, replicated, or aof)"
        )),
        None => match opt("VS_REDIS_SINK_ACK_REPLICAS")? {
            Some(value) => Ok(RedisAcknowledgement::Replicated {
                replicas: lenient_int("VS_REDIS_SINK_ACK_REPLICAS", &value)?,
                timeout: timeout()?,
            }),
            None => Ok(RedisAcknowledgement::Primary),
        },
    }
}

fn strict_bool_env(name: &str, default: bool) -> Result<bool> {
    let Some(value) = opt(name)? else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(anyhow!(
            "env var {name} must be true, false, 1, 0, yes, no, on, or off"
        )),
    }
}

struct ResolvedRedisAuth {
    username: Option<String>,
    password: Option<String>,
    username_file: Option<PathBuf>,
    password_file: Option<PathBuf>,
}

fn resolve_redis_auth(auth: Option<&FileRedisAuth>) -> Result<ResolvedRedisAuth> {
    match auth {
        None | Some(FileRedisAuth::None) => Ok(ResolvedRedisAuth {
            username: None,
            password: None,
            username_file: None,
            password_file: None,
        }),
        Some(FileRedisAuth::Acl {
            username_ref,
            password_ref,
        }) => {
            let (username, username_file) = resolve_redis_credential_ref(username_ref)?;
            let (password, password_file) = resolve_redis_credential_ref(password_ref)?;
            Ok(ResolvedRedisAuth {
                username,
                password,
                username_file,
                password_file,
            })
        }
        Some(FileRedisAuth::Password { password_ref }) => {
            let (password, password_file) = resolve_redis_credential_ref(password_ref)?;
            Ok(ResolvedRedisAuth {
                username: None,
                password,
                username_file: None,
                password_file,
            })
        }
    }
}

fn resolve_redis_credential_ref(reference: &ValueRef) -> Result<(Option<String>, Option<PathBuf>)> {
    match reference {
        ValueRef::Env(name) => Ok((Some(req(name)?), None)),
        ValueRef::File(path) => Ok((None, Some(path.clone()))),
    }
}

fn redis_endpoint_list_env(name: &str) -> Result<Vec<String>> {
    let value = req(name)?;
    let endpoints = value
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(anyhow!("{name} must contain at least one Redis endpoint"));
    }
    Ok(endpoints)
}

fn load_redis_tls(
    config: Option<&FileRedisTlsConfig>,
    ca_env: &str,
    cert_env: &str,
    key_env: &str,
) -> Result<RedisTlsConfig> {
    Ok(RedisTlsConfig {
        ca_file: redis_tls_path(config, ca_env, |tls| tls.ca_file.clone())?,
        client_cert_file: redis_tls_path(config, cert_env, |tls| tls.client_cert_file.clone())?,
        client_key_file: redis_tls_path(config, key_env, |tls| tls.client_key_file.clone())?,
    })
}

fn redis_tls_path(
    config: Option<&FileRedisTlsConfig>,
    env_name: &str,
    from_config: impl FnOnce(&FileRedisTlsConfig) -> Option<PathBuf>,
) -> Result<Option<PathBuf>> {
    if let Some(path) = config.and_then(from_config) {
        return Ok(Some(path));
    }
    Ok(opt(env_name)?
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from))
}

fn load_opensearch_config(engine_config: Option<&EngineFileConfig>) -> Result<OpenSearchConfig> {
    let Some(config) = engine_config.and_then(|config| config.sink.as_ref()) else {
        return load_opensearch_config_from_env();
    };
    let Some(opensearch) = config.opensearch.as_ref() else {
        return load_opensearch_config_from_env();
    };
    let endpoint = resolve_value_ref(&opensearch.endpoint_ref)?;
    let index_template = opensearch.index_routing.as_legacy_template().to_owned();
    let auth = match opensearch.auth.as_ref() {
        None | Some(OpenSearchAuthConfig::None) => AuthMode::None,
        Some(OpenSearchAuthConfig::Basic {
            username_ref,
            password_ref,
        }) => AuthMode::Basic {
            username: resolve_value_ref(username_ref)?,
            password: resolve_value_ref(password_ref)?,
        },
        Some(OpenSearchAuthConfig::ApiKey { api_key_ref }) => {
            AuthMode::ApiKey(resolve_value_ref(api_key_ref)?)
        }
    };
    let mut os = OpenSearchConfig::new("opensearch", endpoint, index_template)
        .with_auth(auth)
        .with_reconcile_allow_full_purge(config_bool_or_env(
            opensearch.reconcile_allow_full_purge,
            "VS_OS_RECONCILE_ALLOW_FULL_PURGE",
            false,
        ));
    let tls = database_tls_or_env(
        opensearch.tls.as_ref(),
        "VS_OS_TLS_MODE",
        "VS_OS_TLS_CA_FILE",
        None,
    )?;
    let insecure_tls = config_bool_or_env(opensearch.insecure_tls, "VS_INSECURE_TLS", false);
    if tls.is_some() && insecure_tls {
        return Err(anyhow!(
            "strict OpenSearch TLS and VS_INSECURE_TLS=true are mutually exclusive"
        ));
    }
    if let Some(tls) = tls {
        match tls.mode {
            DatabaseTlsMode::VerifyFull => {
                if !os.endpoint.starts_with("https://") {
                    return Err(anyhow!(
                        "sink.opensearch.tls.mode=verify_full requires an https:// endpoint"
                    ));
                }
                if let Some(path) = tls.ca_file {
                    os = os.with_ca_file(path);
                }
            }
            DatabaseTlsMode::Disabled => {
                if !os.endpoint.starts_with("http://") {
                    return Err(anyhow!(
                        "sink.opensearch.tls.mode=disabled requires an http:// endpoint"
                    ));
                }
            }
        }
    }
    if insecure_tls {
        os = os.with_insecure_tls();
    }
    Ok(os)
}

fn load_opensearch_config_from_env() -> Result<OpenSearchConfig> {
    let os_endpoint = req("VS_OS_ENDPOINT")?;
    let index_template = req("VS_INDEX_TEMPLATE")?;
    let os_auth = match (
        std::env::var("VS_OS_USERNAME").ok(),
        std::env::var("VS_OS_PASSWORD").ok(),
        std::env::var("VS_OS_API_KEY").ok(),
    ) {
        (_, _, Some(key)) => AuthMode::ApiKey(key),
        (Some(username), Some(password), _) => AuthMode::Basic { username, password },
        _ => AuthMode::None,
    };
    let mut os = OpenSearchConfig::new("opensearch", os_endpoint, index_template)
        .with_auth(os_auth)
        .with_reconcile_allow_full_purge(bool_env("VS_OS_RECONCILE_ALLOW_FULL_PURGE", false));
    let tls = database_tls_or_env(None, "VS_OS_TLS_MODE", "VS_OS_TLS_CA_FILE", None)?;
    let insecure_tls = bool_env("VS_INSECURE_TLS", false);
    if tls.is_some() && insecure_tls {
        return Err(anyhow!(
            "VS_OS_TLS_MODE and VS_INSECURE_TLS=true are mutually exclusive"
        ));
    }
    if let Some(tls) = tls {
        match tls.mode {
            DatabaseTlsMode::VerifyFull => {
                if !os.endpoint.starts_with("https://") {
                    return Err(anyhow!(
                        "VS_OS_TLS_MODE=verify_full requires an https:// endpoint"
                    ));
                }
                if let Some(path) = tls.ca_file {
                    os = os.with_ca_file(path);
                }
            }
            DatabaseTlsMode::Disabled => {
                if !os.endpoint.starts_with("http://") {
                    return Err(anyhow!(
                        "VS_OS_TLS_MODE=disabled requires an http:// endpoint"
                    ));
                }
            }
        }
    }
    if insecure_tls {
        os = os.with_insecure_tls();
    }
    Ok(os)
}

fn resolve_value_ref(reference: &ValueRef) -> Result<String> {
    match reference {
        ValueRef::Env(name) => req(name),
        ValueRef::File(path) => read_value_ref_file(path),
    }
}

fn read_value_ref_file(path: &Path) -> Result<String> {
    const MAX_VALUE_REF_BYTES: u64 = 1024 * 1024;

    let metadata = std::fs::metadata(path)
        .with_context(|| format!("unable to inspect value reference {}", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_VALUE_REF_BYTES {
        return Err(anyhow!(
            "value reference {} must be a regular file of 1 to {MAX_VALUE_REF_BYTES} bytes",
            path.display()
        ));
    }
    let value = std::fs::read_to_string(path)
        .with_context(|| format!("unable to read value reference {}", path.display()))?;
    let value = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_owned();
    if value.is_empty() {
        return Err(anyhow!(
            "value reference {} must not be empty",
            path.display()
        ));
    }
    Ok(value)
}

fn database_tls_or_env(
    config: Option<&FileTlsConfig>,
    mode_env: &str,
    ca_file_env: &str,
    trust_provider_env: Option<&str>,
) -> Result<Option<DatabaseTlsConfig>> {
    let env_mode = if config.is_none() {
        opt(mode_env)?
    } else {
        None
    };
    let env_ca = if config.is_none() {
        opt(ca_file_env)?.filter(|value| !value.trim().is_empty())
    } else {
        None
    };
    let env_trust_provider = if config.is_none() {
        trust_provider_env
            .map(opt)
            .transpose()?
            .flatten()
            .filter(|value| !value.trim().is_empty())
    } else {
        None
    };
    if config.is_none() && env_mode.is_none() && env_ca.is_none() && env_trust_provider.is_none() {
        return Ok(None);
    }

    let mode = match config.map(|tls| tls.mode) {
        Some(FileTlsMode::VerifyFull) => DatabaseTlsMode::VerifyFull,
        Some(FileTlsMode::Disabled) => DatabaseTlsMode::Disabled,
        None => match env_mode
            .as_deref()
            .unwrap_or("verify_full")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "verify_full" | "verify-full" | "strict" => DatabaseTlsMode::VerifyFull,
            "disabled" | "disable" | "off" => DatabaseTlsMode::Disabled,
            other => {
                return Err(anyhow!(
                    "{mode_env} must be 'verify_full' or 'disabled', got '{other}'"
                ))
            }
        },
    };
    let mut ca_file = config
        .and_then(|tls| tls.ca_file.clone())
        .or_else(|| env_ca.map(PathBuf::from));
    let trust_provider = match config.and_then(|tls| tls.trust.as_ref()) {
        Some(trust) => Some(match trust.provider {
            FileTlsTrustProvider::AwsRds => DatabaseTlsTrustProvider::AwsRds,
        }),
        None => env_trust_provider
            .as_deref()
            .map(
                |provider| match provider.trim().to_ascii_lowercase().as_str() {
                    "aws_rds" | "aws-rds" => Ok(DatabaseTlsTrustProvider::AwsRds),
                    other => Err(anyhow!(
                        "{} must be 'aws_rds', got '{other}'",
                        trust_provider_env.unwrap_or("TLS trust provider")
                    )),
                },
            )
            .transpose()?,
    };
    if mode == DatabaseTlsMode::Disabled && (ca_file.is_some() || trust_provider.is_some()) {
        return Err(anyhow!("TLS trust settings require {mode_env}=verify_full"));
    }
    if ca_file.is_some() && trust_provider.is_some() {
        return Err(anyhow!(
            "{ca_file_env} and {} are mutually exclusive",
            trust_provider_env.unwrap_or("the configured TLS trust provider")
        ));
    }
    if let Some(provider) = trust_provider {
        ca_file = Some(
            materialize_provider_ca_bundle(provider)
                .map_err(|err| anyhow!("prepare provider TLS trust bundle: {err}"))?,
        );
    }
    Ok(Some(DatabaseTlsConfig { mode, ca_file }))
}

fn apply_neo4j_tls_mode(uri: &str, tls: Option<&DatabaseTlsConfig>) -> Result<String> {
    let Some(tls) = tls else {
        return Ok(uri.to_owned());
    };
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| anyhow!("Neo4j URI must include a supported scheme"))?;
    let scheme = match (tls.mode, scheme) {
        (DatabaseTlsMode::VerifyFull, "bolt") => "bolt+s",
        (DatabaseTlsMode::VerifyFull, "neo4j") => "neo4j+s",
        (DatabaseTlsMode::VerifyFull, "bolt+s" | "neo4j+s") => scheme,
        (DatabaseTlsMode::VerifyFull, "bolt+ssc" | "neo4j+ssc") => {
            return Err(anyhow!(
                "Neo4j +ssc URI schemes are not accepted with tls.mode=verify_full"
            ))
        }
        (DatabaseTlsMode::Disabled, "bolt" | "neo4j") => scheme,
        (DatabaseTlsMode::Disabled, "bolt+s" | "bolt+ssc") => "bolt",
        (DatabaseTlsMode::Disabled, "neo4j+s" | "neo4j+ssc") => "neo4j",
        (_, other) => return Err(anyhow!("unsupported Neo4j URI scheme '{other}'")),
    };
    Ok(format!("{scheme}://{rest}"))
}

fn config_value_or_env(
    value: Option<&String>,
    reference: Option<&ValueRef>,
    env_name: &str,
) -> Result<String> {
    if let Some(value) = value {
        return Ok(value.clone());
    }
    if let Some(reference) = reference {
        return resolve_value_ref(reference);
    }
    req(env_name)
}

fn config_value_or_env_default(
    value: Option<&String>,
    reference: Option<&ValueRef>,
    env_name: &str,
    default: &str,
) -> Result<String> {
    if let Some(value) = value {
        return Ok(value.clone());
    }
    if let Some(reference) = reference {
        return resolve_value_ref(reference);
    }
    Ok(opt(env_name)?.unwrap_or_else(|| default.to_owned()))
}

fn config_optional_value_or_env(
    value: Option<&String>,
    reference: Option<&ValueRef>,
    env_name: &str,
) -> Result<Option<String>> {
    if let Some(value) = value {
        return Ok(Some(value.clone()));
    }
    if let Some(reference) = reference {
        return Ok(Some(resolve_value_ref(reference)?));
    }
    opt(env_name)
}

fn config_ref_or_env(reference: Option<&ValueRef>, env_name: &str) -> Result<String> {
    match reference {
        Some(reference) => resolve_value_ref(reference),
        None => req(env_name),
    }
}

fn config_bool_or_env(value: Option<bool>, env_name: &str, default: bool) -> bool {
    value.unwrap_or_else(|| bool_env(env_name, default))
}

fn config_usize_or_env(value: Option<usize>, env_name: &str, default: usize) -> Result<usize> {
    match value {
        Some(value) => Ok(value),
        None => opt_usize(env_name, default),
    }
}

fn config_optional_usize_or_env(value: Option<usize>, env_name: &str) -> Result<Option<usize>> {
    match value {
        Some(value) => Ok(Some(value)),
        None => opt(env_name)?
            .map(|raw| lenient_int(env_name, &raw))
            .transpose(),
    }
}

fn config_duration_ms_or_env(
    value: Option<u64>,
    env_name: &str,
    default: Duration,
) -> Result<Duration> {
    match value {
        Some(ms) => Ok(Duration::from_millis(ms)),
        None => opt_duration_ms(env_name, default),
    }
}

fn config_optional_path_or_env(path: Option<&PathBuf>, env_name: &str) -> Result<Option<PathBuf>> {
    match path {
        Some(path) => Ok(Some(path.clone())),
        None => Ok(opt(env_name)?.filter(|s| !s.is_empty()).map(PathBuf::from)),
    }
}

fn join_state_dir(engine_config: Option<&EngineFileConfig>) -> Result<Option<PathBuf>> {
    config_optional_path_or_env(
        engine_config.and_then(|config| config.runtime.joins.state_dir.as_ref()),
        "VS_JOINS_STATE_DIR",
    )
}

fn postgres_join_state_identity(config: &PostgresCdcConfig) -> String {
    format!(
        "postgres://{}:{}/{}#slot={}",
        config.host, config.port, config.database, config.slot_name
    )
}

fn mysql_join_state_identity(config: &MySqlCdcConfig) -> String {
    format!(
        "mysql://{}:{}/{}#server-id={}",
        config.host, config.port, config.database, config.server_id
    )
}

fn join_checkpoint_recoverable(
    stored_identity: Option<&str>,
    expected_identity: &str,
    source_checkpoint_exists: bool,
) -> bool {
    stored_identity == Some(expected_identity) && source_checkpoint_exists
}

fn remove_join_state_database(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            info!(path = %path.display(), "removed persistent join-state database");
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(anyhow!(err)).with_context(|| format!("removing join state {}", path.display()))
        }
    }
}

fn required_join_state_dir(engine_config: Option<&EngineFileConfig>) -> Result<PathBuf> {
    join_state_dir(engine_config)?.ok_or_else(|| {
        anyhow!(
            "memory-mode joins require durable state; configure runtime.joins.state_dir \
             or VS_JOINS_STATE_DIR"
        )
    })
}

fn postgres_bootstrap_chunk_size(
    engine_config: Option<&EngineFileConfig>,
    env_name: &str,
    default: usize,
) -> Result<usize> {
    config_usize_or_env(
        engine_config
            .and_then(|config| config.source.as_ref())
            .and_then(|source| source.postgres.as_ref())
            .and_then(|postgres| postgres.bootstrap.chunk_size),
        env_name,
        default,
    )
}

fn mysql_bootstrap_chunk_size(
    engine_config: Option<&EngineFileConfig>,
    env_name: &str,
    default: usize,
) -> Result<usize> {
    config_usize_or_env(
        engine_config
            .and_then(|config| config.source.as_ref())
            .and_then(|source| source.mysql.as_ref())
            .and_then(|mysql| mysql.bootstrap.chunk_size),
        env_name,
        default,
    )
}

fn postgres_sink_reverse_lookup_enabled(
    engine_config: Option<&EngineFileConfig>,
    default: bool,
) -> bool {
    config_bool_or_env(
        engine_config
            .and_then(|config| config.source.as_ref())
            .and_then(|source| source.postgres.as_ref())
            .and_then(|postgres| postgres.sink_reverse_lookup),
        "VS_PG_SINK_REVERSE_LOOKUP",
        default,
    )
}

fn mysql_sink_reverse_lookup_enabled(
    engine_config: Option<&EngineFileConfig>,
    default: bool,
) -> bool {
    config_bool_or_env(
        engine_config
            .and_then(|config| config.source.as_ref())
            .and_then(|source| source.mysql.as_ref())
            .and_then(|mysql| mysql.sink_reverse_lookup),
        "VS_MYSQL_SINK_REVERSE_LOOKUP",
        default,
    )
}

fn mysql_joins_require_full_row_image(joins: &[JoinDefinition]) -> bool {
    joins.iter().any(|definition| {
        definition.related.iter().any(|related| {
            let related_pk = related.pk.columns();
            related
                .join_on
                .to
                .columns()
                .iter()
                .any(|column| !related_pk.contains(column))
        })
    })
}

fn load_dlq_path(engine_config: Option<&EngineFileConfig>) -> Result<PathBuf> {
    if let Some(path) = config_spec_runtime_dlq_path(engine_config) {
        return Ok(path);
    }
    Ok(opt("VS_DLQ_PATH")?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/dlq.jsonl")))
}

fn config_spec_runtime_dlq_path(engine_config: Option<&EngineFileConfig>) -> Option<PathBuf> {
    engine_config.and_then(|config| config.runtime.dlq_path.clone())
}

fn load_engine_runtime_config(
    engine_config: Option<&EngineFileConfig>,
    dlq_path: PathBuf,
) -> Result<EngineConfig> {
    let runtime = engine_config.map(|config| &config.runtime);
    let bus_capacity = match runtime.and_then(|runtime| runtime.bus_capacity) {
        Some(value) => value,
        None => opt_usize("VS_BUS_CAPACITY", 1024)?,
    };
    let dispatch = runtime.map(|runtime| &runtime.dispatch);
    let max_events = match dispatch.and_then(|dispatch| dispatch.max_events) {
        Some(value) => value,
        None => opt_usize("VS_DISPATCH_MAX_EVENTS", 2_000)?,
    };
    let max_batch_bytes = match dispatch.and_then(|dispatch| dispatch.max_batch_bytes) {
        Some(value) => value,
        None => opt_usize("VS_DISPATCH_MAX_BATCH_BYTES", 4 * 1024 * 1024)?,
    };
    let flush_interval = match dispatch.and_then(|dispatch| dispatch.flush_ms) {
        Some(ms) => Duration::from_millis(ms),
        None => opt_duration_ms("VS_DISPATCH_FLUSH_MS", Duration::from_millis(500))?,
    };
    let max_parallel_bulks = match dispatch.and_then(|dispatch| dispatch.parallel_bulks) {
        Some(value) => value,
        None => opt_usize("VS_DISPATCH_PARALLEL_BULKS", 4)?,
    };
    let memory = runtime.map(|runtime| &runtime.memory);
    let memory_enabled = config_bool_or_env(
        memory.and_then(|memory| memory.enabled),
        "VS_MEMORY_CONTROLLER_ENABLED",
        true,
    );
    let memory_budget_bytes = config_optional_usize_or_env(
        memory.and_then(|memory| memory.budget_bytes),
        "VS_MEMORY_BUDGET_BYTES",
    )?
    .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX));
    let max_event_bytes = config_usize_or_env(
        memory.and_then(|memory| memory.max_event_bytes),
        "VS_MEMORY_MAX_EVENT_BYTES",
        32 * 1024 * 1024,
    )?;
    let memory_sample_interval = config_duration_ms_or_env(
        memory.and_then(|memory| memory.sample_ms),
        "VS_MEMORY_SAMPLE_MS",
        Duration::from_millis(100),
    )?;
    let memory_recovery_interval = config_duration_ms_or_env(
        memory.and_then(|memory| memory.recovery_ms),
        "VS_MEMORY_RECOVERY_MS",
        Duration::from_secs(1),
    )?;
    let target_percent = u8::try_from(config_usize_or_env(
        memory.and_then(|memory| memory.target_percent.map(usize::from)),
        "VS_MEMORY_TARGET_PERCENT",
        65,
    )?)
    .context("VS_MEMORY_TARGET_PERCENT must fit in u8")?;
    let high_percent = u8::try_from(config_usize_or_env(
        memory.and_then(|memory| memory.high_percent.map(usize::from)),
        "VS_MEMORY_HIGH_PERCENT",
        75,
    )?)
    .context("VS_MEMORY_HIGH_PERCENT must fit in u8")?;
    let critical_percent = u8::try_from(config_usize_or_env(
        memory.and_then(|memory| memory.critical_percent.map(usize::from)),
        "VS_MEMORY_CRITICAL_PERCENT",
        85,
    )?)
    .context("VS_MEMORY_CRITICAL_PERCENT must fit in u8")?;
    let hysteresis_percent = u8::try_from(config_usize_or_env(
        memory.and_then(|memory| memory.hysteresis_percent.map(usize::from)),
        "VS_MEMORY_HYSTERESIS_PERCENT",
        5,
    )?)
    .context("VS_MEMORY_HYSTERESIS_PERCENT must fit in u8")?;
    if target_percent == 0
        || target_percent >= high_percent
        || high_percent >= critical_percent
        || critical_percent >= 100
        || hysteresis_percent == 0
        || hysteresis_percent >= target_percent
    {
        return Err(anyhow!(
            "memory thresholds require 0 < target < high < critical < 100 and 0 < hysteresis < target"
        ));
    }
    Ok(EngineConfig {
        bus_capacity,
        dispatcher: DispatcherConfig {
            max_events,
            max_batch_bytes,
            flush_interval,
            max_parallel_bulks,
        },
        dlq_path,
        memory: MemoryControllerConfig {
            enabled: memory_enabled,
            budget_bytes: memory_budget_bytes,
            max_event_bytes: u64::try_from(max_event_bytes).unwrap_or(u64::MAX),
            sample_interval: memory_sample_interval,
            recovery_interval: memory_recovery_interval,
            target_percent,
            high_percent,
            critical_percent,
            hysteresis_percent,
        },
    })
}

/// Load a MongoDB CDC bundle from `VS_MONGO_*` env. Phase 1: raw 1:1
/// (no joins) — one document → one sink doc.
fn load_cdc_bundle_mongodb(engine_config: Option<&EngineFileConfig>) -> Result<CdcBundle> {
    let source_config = engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.mongodb.as_ref());

    let uri = config_ref_or_env(
        source_config.and_then(|config| config.uri_ref.as_ref()),
        "VS_MONGO_URI",
    )?;
    let database = config_value_or_env(
        source_config.and_then(|config| config.database.as_ref()),
        source_config.and_then(|config| config.database_ref.as_ref()),
        "VS_MONGO_DATABASE",
    )?;
    let namespace = source_config
        .and_then(|config| config.namespace.clone())
        .or(opt("VS_MONGO_NAMESPACE")?)
        .unwrap_or_else(|| database.clone());
    let state_dir = config_optional_path_or_env(
        source_config.and_then(|config| config.state_dir.as_ref()),
        "VS_MONGO_STATE_DIR",
    )?
    .unwrap_or_else(|| PathBuf::from("./data/mongo-state"));
    let collections = match source_config {
        Some(config) if !config.collections.is_empty() => config.collections.clone(),
        _ => parse_csv(opt("VS_MONGO_COLLECTIONS")?.as_deref()),
    };
    let full_document = match source_config.and_then(|config| config.full_document) {
        Some(ConfigMongodbFullDocumentMode::Default) => MongoFullDocument::Default,
        Some(ConfigMongodbFullDocumentMode::UpdateLookup) => MongoFullDocument::UpdateLookup,
        None => opt("VS_MONGO_FULL_DOCUMENT")?
            .map(|s| MongoFullDocument::from_str_lenient(&s))
            .unwrap_or(MongoFullDocument::UpdateLookup),
    };
    let bootstrap = match source_config.and_then(|config| config.bootstrap.mode) {
        Some(ConfigBootstrapMode::Snapshot) => true,
        Some(ConfigBootstrapMode::None) => false,
        None => !matches!(
            opt("VS_MONGO_BOOTSTRAP_MODE")?
                .map(|s| s.trim().to_ascii_lowercase())
                .as_deref(),
            Some("none") | Some("off") | Some("disabled")
        ),
    };
    let bootstrap_chunk_size = config_usize_or_env(
        source_config.and_then(|config| config.bootstrap.chunk_size),
        "VS_MONGO_BOOTSTRAP_CHUNK_SIZE",
        1000,
    )?;
    let id = opt("VS_AGENT_NAME")?.unwrap_or_else(|| "mongodb-cdc".to_owned());

    let mut config = MongoCdcConfig::new(id, uri, database, state_dir);
    config.namespace = namespace;
    config.collections = collections;
    config.full_document = full_document;
    config.bootstrap = bootstrap;
    config.bootstrap_chunk_size = bootstrap_chunk_size;
    config.token_flush_interval = config_duration_ms_or_env(
        source_config.and_then(|config| config.token_flush_ms),
        "VS_MONGO_TOKEN_FLUSH_MS",
        Duration::from_millis(1000),
    )?;
    config.tls = database_tls_or_env(
        source_config.and_then(|config| config.tls.as_ref()),
        "VS_MONGO_TLS_MODE",
        "VS_MONGO_TLS_CA_FILE",
        None,
    )?;

    // Sink + DLQ — same OpenSearch config the other sources load.
    let sink = load_sink_config(engine_config)?;
    let dlq_path = load_dlq_path(engine_config)?;
    let engine_runtime = load_engine_runtime_config(engine_config, dlq_path)?;

    Ok(CdcBundle {
        source: CdcSourceConfig::Mongo(Box::new(config)),
        runtime: CdcRuntime {
            sink,
            engine_config: engine_runtime,
            engine_file_config: engine_config.cloned(),
        },
        joins: Vec::new(),
        joins_yaml_text: None,
    })
}

/// Load a MySQL/MariaDB CDC bundle from `VS_MYSQL_*` env. Phase 1: raw 1:1.
fn load_cdc_bundle_mysql(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<CdcBundle> {
    let source_config = engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.mysql.as_ref());

    let host = config_value_or_env(
        source_config.and_then(|config| config.host.as_ref()),
        source_config.and_then(|config| config.host_ref.as_ref()),
        "VS_MYSQL_HOST",
    )?;
    let database = config_value_or_env(
        source_config.and_then(|config| config.database.as_ref()),
        source_config.and_then(|config| config.database_ref.as_ref()),
        "VS_MYSQL_DATABASE",
    )?;
    let user = config_value_or_env_default(
        source_config.and_then(|config| config.user.as_ref()),
        source_config.and_then(|config| config.user_ref.as_ref()),
        "VS_MYSQL_USER",
        "root",
    )?;
    let password = match source_config.and_then(|config| config.password_ref.as_ref()) {
        Some(reference) => resolve_value_ref(reference)?,
        None => opt("VS_MYSQL_PASSWORD")?.unwrap_or_default(),
    };
    let namespace = source_config
        .and_then(|config| config.namespace.clone())
        .or(opt("VS_MYSQL_NAMESPACE")?)
        .unwrap_or_else(|| database.clone());
    let state_dir = config_optional_path_or_env(
        source_config.and_then(|config| config.state_dir.as_ref()),
        "VS_MYSQL_STATE_DIR",
    )?
    .unwrap_or_else(|| PathBuf::from("./data/mysql-state"));
    let port: u16 = match source_config.and_then(|config| config.port) {
        Some(port) => port,
        None => opt_usize("VS_MYSQL_PORT", 3306)?
            .try_into()
            .context("VS_MYSQL_PORT out of range")?,
    };
    let server_id: u32 = match source_config.and_then(|config| config.server_id) {
        Some(server_id) => server_id,
        None => opt_usize("VS_MYSQL_SERVER_ID", 4_000_000_000)?
            .try_into()
            .context("VS_MYSQL_SERVER_ID out of u32 range")?,
    };
    let tables = match source_config {
        Some(config) if !config.tables.is_empty() => config.tables.clone(),
        _ => parse_csv(opt("VS_MYSQL_TABLES")?.as_deref()),
    };
    let bootstrap = match source_config.and_then(|config| config.bootstrap.mode) {
        Some(ConfigBootstrapMode::Snapshot) => true,
        Some(ConfigBootstrapMode::None) => false,
        None => !matches!(
            opt("VS_MYSQL_BOOTSTRAP_MODE")?
                .map(|s| s.trim().to_ascii_lowercase())
                .as_deref(),
            Some("none") | Some("off") | Some("disabled")
        ),
    };
    let id = opt("VS_AGENT_NAME")?.unwrap_or_else(|| "mysql-cdc".to_owned());

    let mut config = MySqlCdcConfig::new(id, host, user, password, database, state_dir);
    config.port = port;
    config.namespace = namespace;
    config.server_id = server_id;
    config.tables = tables;
    config.bootstrap = bootstrap;
    config.bootstrap_chunk_size =
        mysql_bootstrap_chunk_size(engine_config, "VS_MYSQL_BOOTSTRAP_CHUNK_SIZE", 1000)?;
    config.pos_flush_interval = config_duration_ms_or_env(
        source_config.and_then(|config| config.pos_flush_ms),
        "VS_MYSQL_POS_FLUSH_MS",
        Duration::from_millis(1000),
    )?;
    config.tls = database_tls_or_env(
        source_config.and_then(|config| config.tls.as_ref()),
        "VS_MYSQL_TLS_MODE",
        "VS_MYSQL_TLS_CA_FILE",
        Some("VS_MYSQL_TLS_TRUST_PROVIDER"),
    )?;

    // Optional denormalization joins (shared spec + engine with Postgres).
    let (joins, joins_yaml_text) = load_joins_yaml(fleet_config, engine_config)?;
    validate_projection_target_indexes(engine_config, &joins)?;
    // Ensure every primary + related relation is watched/snapshotted. Only
    // matters when VS_MYSQL_TABLES filters — an empty list already watches all.
    if !joins.is_empty() && !config.tables.is_empty() {
        for j in &joins {
            let related = j.related.iter().map(|r| &r.table);
            for t in std::iter::once(&j.primary.table).chain(related) {
                let (_ns, name) = split_table(t);
                if !config.tables.contains(&name) {
                    config.tables.push(name);
                }
            }
        }
    }

    let sink = load_sink_config(engine_config)?;
    let dlq_path = load_dlq_path(engine_config)?;
    let engine_runtime = load_engine_runtime_config(engine_config, dlq_path)?;

    Ok(CdcBundle {
        source: CdcSourceConfig::Mysql(Box::new(config)),
        runtime: CdcRuntime {
            sink,
            engine_config: engine_runtime,
            engine_file_config: engine_config.cloned(),
        },
        joins,
        joins_yaml_text,
    })
}

/// Load a Kafka/Redpanda CDC bundle from `VS_KAFKA_*` env. Phase 1: raw 1:1,
/// JSON values, Debezium-unwrap (or raw).
fn load_cdc_bundle_kafka(engine_config: Option<&EngineFileConfig>) -> Result<CdcBundle> {
    let source_config = engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.kafka.as_ref());

    let brokers = config_value_or_env(
        source_config.and_then(|config| config.brokers.as_ref()),
        source_config.and_then(|config| config.brokers_ref.as_ref()),
        "VS_KAFKA_BROKERS",
    )?;
    let topics = match source_config {
        Some(config) if !config.topics.is_empty() => config.topics.clone(),
        _ => parse_csv(opt("VS_KAFKA_TOPICS")?.as_deref()),
    };
    if topics.is_empty() {
        return Err(anyhow!(
            "VS_KAFKA_TOPICS is required (CSV of topics, or a single ^regex)"
        ));
    }
    let group_id = config_optional_value_or_env(
        source_config.and_then(|config| config.group_id.as_ref()),
        source_config.and_then(|config| config.group_id_ref.as_ref()),
        "VS_KAFKA_GROUP_ID",
    )?
    .or(opt("VS_AGENT_NAME")?)
    .unwrap_or_else(|| "ventstream-kafka".to_owned());
    let id = opt("VS_AGENT_NAME")?.unwrap_or_else(|| "kafka-cdc".to_owned());

    let mut config = KafkaCdcConfig::new(id, brokers, group_id);
    config.topics = topics;
    config.namespace_override = source_config
        .and_then(|config| config.namespace.clone())
        .or(opt("VS_KAFKA_NAMESPACE")?);
    config.unwrap = match source_config.and_then(|config| config.unwrap) {
        Some(ConfigKafkaUnwrapMode::Raw) => UnwrapMode::Raw,
        Some(ConfigKafkaUnwrapMode::Debezium) => UnwrapMode::Debezium,
        None => opt("VS_KAFKA_UNWRAP")?
            .map(|s| UnwrapMode::from_str_lenient(&s))
            .unwrap_or(UnwrapMode::Debezium),
    };
    if let Some(reset) = source_config
        .and_then(|config| config.auto_offset_reset.clone())
        .or(opt("VS_KAFKA_AUTO_OFFSET_RESET")?)
    {
        config.auto_offset_reset = reset;
    }
    config.security_protocol = source_config
        .and_then(|config| config.security_protocol.clone())
        .or(opt("VS_KAFKA_SECURITY_PROTOCOL")?);
    config.sasl_mechanism = source_config
        .and_then(|config| config.sasl_mechanism.clone())
        .or(opt("VS_KAFKA_SASL_MECHANISM")?);
    config.sasl_username = config_optional_value_or_env(
        source_config.and_then(|config| config.sasl_username.as_ref()),
        source_config.and_then(|config| config.sasl_username_ref.as_ref()),
        "VS_KAFKA_SASL_USERNAME",
    )?;
    config.sasl_password = match source_config.and_then(|config| config.sasl_password_ref.as_ref())
    {
        Some(reference) => Some(resolve_value_ref(reference)?),
        None => opt("VS_KAFKA_SASL_PASSWORD")?,
    };
    config.ssl_ca_location = source_config
        .and_then(|config| {
            config
                .ssl_ca_location
                .as_ref()
                .map(|path| path.display().to_string())
        })
        .or(opt("VS_KAFKA_SSL_CA_LOCATION")?);
    config.raw_key_field = source_config
        .and_then(|config| config.raw_key_field.clone())
        .or(opt("VS_KAFKA_RAW_KEY_FIELD")?);
    config.commit_interval = config_duration_ms_or_env(
        source_config.and_then(|config| config.commit_ms),
        "VS_KAFKA_COMMIT_MS",
        Duration::from_millis(1000),
    )?;

    let sink = load_sink_config(engine_config)?;
    let dlq_path = load_dlq_path(engine_config)?;
    let engine_runtime = load_engine_runtime_config(engine_config, dlq_path)?;

    Ok(CdcBundle {
        source: CdcSourceConfig::Kafka(Box::new(config)),
        runtime: CdcRuntime {
            sink,
            engine_config: engine_runtime,
            engine_file_config: engine_config.cloned(),
        },
        joins: Vec::new(),
        joins_yaml_text: None,
    })
}

fn load_cdc_bundle_neo4j(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<CdcBundle> {
    use std::collections::HashMap;

    let source_config = engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.neo4j.as_ref());

    let uri = config_value_or_env(
        source_config.and_then(|config| config.uri.as_ref()),
        source_config.and_then(|config| config.uri_ref.as_ref()),
        "VS_NEO4J_URI",
    )?;
    let user = config_value_or_env(
        source_config.and_then(|config| config.user.as_ref()),
        source_config.and_then(|config| config.user_ref.as_ref()),
        "VS_NEO4J_USER",
    )?;
    let password = match source_config.and_then(|config| config.password_ref.as_ref()) {
        Some(reference) => resolve_value_ref(reference)?,
        None => req("VS_NEO4J_PASSWORD")?,
    };
    let database = config_value_or_env_default(
        source_config.and_then(|config| config.database.as_ref()),
        source_config.and_then(|config| config.database_ref.as_ref()),
        "VS_NEO4J_DATABASE",
        "neo4j",
    )?;
    let namespace = match source_config.and_then(|config| config.namespace.as_ref()) {
        Some(namespace) => namespace.clone(),
        None => opt("VS_NEO4J_NAMESPACE")?.unwrap_or_else(|| "neo4j".to_owned()),
    };
    let state_dir = config_optional_path_or_env(
        source_config.and_then(|config| config.state_dir.as_ref()),
        "VS_NEO4J_STATE_DIR",
    )?
    .unwrap_or_else(|| PathBuf::from("./data/neo4j-state"));
    let poll_interval = match source_config.and_then(|config| config.poll_interval_ms) {
        Some(ms) => Duration::from_millis(ms),
        None => opt_duration_ms("VS_NEO4J_POLL_INTERVAL_MS", Duration::from_millis(500))?,
    };
    let idle_after: u32 = match source_config.and_then(|config| config.idle_advance_after_polls) {
        Some(value) => value,
        None => opt_usize("VS_NEO4J_IDLE_ADVANCE_AFTER_POLLS", 20)?
            .try_into()
            .context("VS_NEO4J_IDLE_ADVANCE_AFTER_POLLS out of u32 range")?,
    };

    // Mapping env vars use the shape `Label1:table1,Label2:table2`.
    let label_table_map = match source_config {
        Some(config) if !config.label_tables.is_empty() => config
            .label_tables
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        _ => parse_kv_map(opt("VS_NEO4J_LABEL_TABLES")?.as_deref())?,
    };
    let reltype_table_map = match source_config {
        Some(config) if !config.reltype_tables.is_empty() => config
            .reltype_tables
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        _ => parse_kv_map(opt("VS_NEO4J_RELTYPE_TABLES")?.as_deref())?,
    };
    let label_filter = match source_config {
        Some(config) if !config.label_filter.is_empty() => config.label_filter.clone(),
        _ => parse_csv(opt("VS_NEO4J_LABEL_FILTER")?.as_deref()),
    };
    let reltype_filter = match source_config {
        Some(config) if !config.reltype_filter.is_empty() => config.reltype_filter.clone(),
        _ => parse_csv(opt("VS_NEO4J_RELTYPE_FILTER")?.as_deref()),
    };
    let label_priority = match source_config {
        Some(config) if !config.label_priority.is_empty() => config.label_priority.clone(),
        _ => parse_csv(opt("VS_NEO4J_LABEL_PRIORITY")?.as_deref()),
    };
    let bootstrap_mode = source_config.and_then(|config| config.bootstrap.mode);
    let bootstrap = match bootstrap_mode {
        Some(ConfigBootstrapMode::Snapshot) => {
            let batch_size: i64 = source_config
                .and_then(|config| config.bootstrap.chunk_size)
                .unwrap_or(2_000)
                .try_into()
                .context("source.neo4j.bootstrap.chunk_size out of i64 range")?;
            Some(Neo4jBootstrap { batch_size })
        }
        Some(ConfigBootstrapMode::None) => None,
        None => {
            let env_bootstrap_mode =
                opt("VS_NEO4J_BOOTSTRAP_MODE")?.unwrap_or_else(|| "snapshot".to_owned());
            match env_bootstrap_mode.trim().to_ascii_lowercase().as_str() {
                "snapshot" => {
                    let batch_size: i64 = match opt("VS_NEO4J_BOOTSTRAP_BATCH_SIZE")? {
                        Some(s) => lenient_int("VS_NEO4J_BOOTSTRAP_BATCH_SIZE", &s)?,
                        None => 2_000,
                    };
                    Some(Neo4jBootstrap { batch_size })
                }
                "none" | "off" | "" => None,
                other => {
                    return Err(anyhow!(
                        "unknown VS_NEO4J_BOOTSTRAP_MODE '{other}' (expected 'snapshot' or 'none')"
                    ))
                }
            }
        }
    };

    let sink = load_sink_config(engine_config)?;
    let dlq_path = load_dlq_path(engine_config)?;
    let engine_runtime = load_engine_runtime_config(engine_config, dlq_path)?;

    let mut neo = Neo4jCdcConfig::new("neo4j-cdc", uri, user, password, database, state_dir);
    neo.namespace = namespace;
    neo.poll_interval = poll_interval;
    neo.idle_advance_after_polls = idle_after.max(1);
    neo.recompose_chunk = match source_config.and_then(|config| config.recompose_chunk) {
        Some(value) => value,
        None => opt_usize("VS_NEO4J_RECOMPOSE_CHUNK", 128)?,
    }
    .max(1);
    neo.recompose_concurrency = match source_config.and_then(|config| config.recompose_concurrency)
    {
        Some(value) => value,
        None => opt_usize("VS_NEO4J_RECOMPOSE_CONCURRENCY", 8)?,
    }
    .max(1);
    neo.projection_fan_out = config_bool_or_env(
        source_config.and_then(|config| config.projection_fan_out),
        "VS_NEO4J_PROJECTION_FAN_OUT",
        true,
    );
    neo.hot_node_threshold = config_usize_or_env(
        source_config.and_then(|config| config.hot_node_threshold),
        "VS_NEO4J_HOT_NODE_THRESHOLD",
        ventstream_sources::neo4j::hot_endpoints::DEFAULT_HOT_NODE_THRESHOLD,
    )?;
    neo.label_table_map = label_table_map;
    neo.reltype_table_map = reltype_table_map;
    neo.label_filter = label_filter;
    neo.reltype_filter = reltype_filter;
    neo.label_priority = label_priority;
    neo.bootstrap = bootstrap;
    if let Some(path) = config_optional_path_or_env(
        source_config.and_then(|config| config.trust_cert_file.as_ref()),
        "VS_NEO4J_TRUST_CERT_FILE",
    )? {
        neo.trust_cert_file = Some(path);
    }
    neo.tls = database_tls_or_env(
        source_config.and_then(|config| config.tls.as_ref()),
        "VS_NEO4J_TLS_MODE",
        "VS_NEO4J_TLS_CA_FILE",
        None,
    )?;
    neo.uri = apply_neo4j_tls_mode(&neo.uri, neo.tls.as_ref())?;
    if let Some(path) = neo.tls.as_ref().and_then(|tls| tls.ca_file.clone()) {
        if neo.trust_cert_file.is_some() {
            return Err(anyhow!(
                "configure only one of VS_NEO4J_TLS_CA_FILE and VS_NEO4J_TRUST_CERT_FILE"
            ));
        }
        neo.trust_cert_file = Some(path);
    }

    // Optional denormalize mode. Driven by a YAML file describing one
    // or more primary projections — see
    // crates/ventstream-sources/src/neo4j/denormalize.rs for the
    // schema and `demo/stack/specs/products.yaml` for a working
    // example against the product-catalog graph shape.
    let denormalize_path = match fleet_materialized_spec(
        fleet_config,
        "/specs/neo4j_denormalize_yaml",
        "neo4j-denormalize.yaml",
    )? {
        Some(path) => Some(path),
        None => match config_spec_path(engine_config, |config| {
            config.specs.neo4j_denormalize.as_ref()
        }) {
            Some(path) => Some(path),
            None => opt("VS_NEO4J_DENORMALIZE_YAML")?
                .filter(|s| !s.is_empty())
                .map(PathBuf::from),
        },
    };
    if let Some(path) = denormalize_path {
        let specs = Neo4jDenormalizeSpecs::from_yaml_file(&path)
            .with_context(|| format!("loading denormalize YAML at {}", path.display()))?;
        if specs.is_empty() {
            warn!(path = %path.display(), "denormalize YAML had no specs");
        }
        // Lint at startup. Under-configured fan_out_max_hops doesn't
        // break correctness, just produces silent staleness for the
        // deepest hops — exactly the kind of issue that's hard to
        // diagnose at 3am. Surface it loudly here.
        for row in analyze_neo4j_specs(&specs) {
            if row.warn_too_low {
                warn!(
                    primary = %row.primary_label,
                    configured = row.configured_hops,
                    inferred_minimum = row.inferred_min_hops,
                    "denormalize spec fan_out_max_hops is lower than the Cypher's deepest \
                     traversal — changes at hops {} > {} won't propagate until something \
                     closer to the primary also mutates. Bump fan_out_max_hops to at least {} \
                     to fix.",
                    row.inferred_min_hops, row.configured_hops, row.inferred_min_hops
                );
            }
        }
        neo.denormalize = Some(specs);
    }
    let _ = HashMap::<String, String>::new(); // keep HashMap import used if both maps are empty

    Ok(CdcBundle {
        source: CdcSourceConfig::Neo4j(Box::new(neo)),
        runtime: CdcRuntime {
            sink,
            engine_config: engine_runtime,
            engine_file_config: engine_config.cloned(),
        },
        joins: Vec::new(),
        joins_yaml_text: None,
    })
}

/// Parse `Key1:Val1,Key2:Val2` into a HashMap. Empty input → empty map.
/// Whitespace around keys/values is trimmed. Entries without a colon
/// are reported as a config error.
fn parse_kv_map(input: Option<&str>) -> Result<std::collections::HashMap<String, String>> {
    let mut out = std::collections::HashMap::new();
    let Some(s) = input else {
        return Ok(out);
    };
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (k, v) = entry
            .split_once(':')
            .ok_or_else(|| anyhow!("malformed mapping entry '{entry}' (expected 'Key:value')"))?;
        out.insert(k.trim().to_owned(), v.trim().to_owned());
    }
    Ok(out)
}

/// Parse a comma-separated list, trimming each entry. Empty input →
/// empty Vec (== "no filter").
fn parse_csv(input: Option<&str>) -> Vec<String> {
    input
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn load_cdc_bundle_postgres(
    fleet_config: Option<&FleetAppliedConfig>,
    engine_config: Option<&EngineFileConfig>,
) -> Result<CdcBundle> {
    let source_config = engine_config
        .and_then(|config| config.source.as_ref())
        .and_then(|source| source.postgres.as_ref());

    let pg_host = config_value_or_env(
        source_config.and_then(|config| config.host.as_ref()),
        source_config.and_then(|config| config.host_ref.as_ref()),
        "VS_PG_HOST",
    )?;
    let pg_port: u16 = match source_config.and_then(|config| config.port) {
        Some(port) => port,
        None => match opt("VS_PG_PORT")? {
            Some(s) => lenient_int("VS_PG_PORT", &s)?,
            None => 5432,
        },
    };
    let pg_user = config_value_or_env(
        source_config.and_then(|config| config.user.as_ref()),
        source_config.and_then(|config| config.user_ref.as_ref()),
        "VS_PG_USER",
    )?;
    let pg_password = match source_config.and_then(|config| config.password_ref.as_ref()) {
        Some(reference) => resolve_value_ref(reference)?,
        None => req("VS_PG_PASSWORD")?,
    };
    let pg_database = config_value_or_env(
        source_config.and_then(|config| config.database.as_ref()),
        source_config.and_then(|config| config.database_ref.as_ref()),
        "VS_PG_DATABASE",
    )?;
    let pg_publication = config_value_or_env(
        source_config.and_then(|config| config.publication.as_ref()),
        source_config.and_then(|config| config.publication_ref.as_ref()),
        "VS_PG_PUBLICATION",
    )?;
    let pg_slot = config_value_or_env(
        source_config.and_then(|config| config.slot.as_ref()),
        source_config.and_then(|config| config.slot_ref.as_ref()),
        "VS_PG_SLOT",
    )?;

    let sink = load_sink_config(engine_config)?;
    let dlq_path = load_dlq_path(engine_config)?;
    let engine_runtime = load_engine_runtime_config(engine_config, dlq_path)?;

    let mut pg = PostgresCdcConfig::new(
        "postgres-cdc",
        pg_host,
        pg_user,
        pg_password,
        pg_database,
        pg_publication,
        pg_slot,
    )
    .with_port(pg_port);
    pg.tls = database_tls_or_env(
        source_config.and_then(|config| config.tls.as_ref()),
        "VS_PG_TLS_MODE",
        "VS_PG_TLS_CA_FILE",
        Some("VS_PG_TLS_TRUST_PROVIDER"),
    )?;
    let transaction_spool_dir =
        match source_config.and_then(|config| config.transaction_spool_dir.clone()) {
            Some(directory) => Some(directory),
            None => opt("VS_PG_TRANSACTION_SPOOL_DIR")?
                .filter(|directory| !directory.trim().is_empty())
                .map(PathBuf::from),
        };
    pg.transaction_spool_dir = transaction_spool_dir;

    let (joins, joins_yaml_text) = load_joins_yaml(fleet_config, engine_config)?;
    validate_projection_target_indexes(engine_config, &joins)?;

    // Snapshot bootstrap: opt-in via VS_PG_BOOTSTRAP_MODE=snapshot, or
    // one-shot forced by Fleet after a rebootstrap operation. Derives the
    // table list from the joins config so the operator doesn't have to
    // maintain two lists. The bootstrap runs only if the slot doesn't yet
    // exist (the source checks at runtime).
    let force_fleet_bootstrap = bool_env("VS_FLEET_FORCE_BOOTSTRAP", false);
    let configured_bootstrap_mode = source_config.and_then(|config| config.bootstrap.mode);
    let pg_bootstrap_mode = if configured_bootstrap_mode.is_none() {
        opt("VS_PG_BOOTSTRAP_MODE")?
    } else {
        None
    };
    let should_bootstrap = force_fleet_bootstrap
        || matches!(
            configured_bootstrap_mode,
            Some(ConfigBootstrapMode::Snapshot)
        )
        || matches!(pg_bootstrap_mode.as_deref(), Some("snapshot"));
    let pg = if should_bootstrap {
        if joins.is_empty() {
            info!(
                "postgres snapshot bootstrap requested without joins YAML; publication tables will be discovered at snapshot time"
            );
        }
        if force_fleet_bootstrap {
            info!("fleet force bootstrap enabled for this postgres start");
        }
        let chunk_size: usize = match source_config.and_then(|config| config.bootstrap.chunk_size) {
            Some(value) => value,
            None => opt_usize("VS_PG_BOOTSTRAP_CHUNK_SIZE", 10_000)?,
        };
        let bootstrap = SnapshotBootstrap {
            tables: build_bootstrap_tables(&joins),
            chunk_size,
        };
        pg.with_bootstrap(bootstrap)
    } else {
        pg
    };

    Ok(CdcBundle {
        source: CdcSourceConfig::Postgres(Box::new(pg)),
        runtime: CdcRuntime {
            sink,
            engine_config: engine_runtime,
            engine_file_config: engine_config.cloned(),
        },
        joins,
        joins_yaml_text,
    })
}

/// Derive the snapshot table list from the joins config. Emit
/// `related` tables before each join's primary table so dependency
/// leaves arrive at the join engine before the rows that reference
/// them. Sync-on-miss covers the rest if the order misses something.
fn build_bootstrap_tables(joins: &[JoinDefinition]) -> Vec<SnapshotTable> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<SnapshotTable> = Vec::new();
    for j in joins {
        for r in &j.related {
            let key = r.table.clone();
            if seen.insert(key.clone()) {
                let (ns, name) = split_table(&r.table);
                out.push(SnapshotTable {
                    namespace: ns,
                    name,
                    primary_key: r.pk.columns().to_vec(),
                });
            }
        }
        let key = j.primary.table.clone();
        if seen.insert(key) {
            let (ns, name) = split_table(&j.primary.table);
            out.push(SnapshotTable {
                namespace: ns,
                name,
                primary_key: j.primary.pk.columns().to_vec(),
            });
        }
    }
    out
}

/// Split a `schema.relation` into its two parts. Falls back to
/// `("public", whole)` when no schema is given.
fn split_table(qualified: &str) -> (String, String) {
    match qualified.split_once('.') {
        Some((ns, name)) => (ns.to_owned(), name.to_owned()),
        None => ("public".to_owned(), qualified.to_owned()),
    }
}

fn load_ws_config(engine_config: Option<&EngineFileConfig>) -> Result<WsConfig> {
    let runtime_config = engine_config.map(|config| &config.runtime.ws);
    let shared_realtime = engine_config.map(|config| &config.runtime.realtime);
    let listen: SocketAddr = runtime_config
        .and_then(|config| config.listen.clone())
        .or(opt("VS_WS_LISTEN")?)
        .unwrap_or_else(|| "0.0.0.0:4040".to_string())
        .parse()
        .context("parsing VS_WS_LISTEN as host:port")?;
    let nats_url = runtime_config
        .and_then(|config| config.nats_url.clone())
        .or(opt("VS_WS_NATS_URL")?)
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
    let bus_subjects: Vec<String> = match runtime_config {
        Some(config) if !config.subjects.is_empty() => config.subjects.clone(),
        _ => opt("VS_WS_SUBJECTS")?
            .unwrap_or_else(|| "vs.t.>".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    };
    let mut ws_cfg = WsConfig {
        listen,
        nats_url,
        bus_subjects,
        ..WsConfig::default()
    };
    if let Some(mailbox) = runtime_config.and_then(|config| config.mailbox) {
        ws_cfg.per_connection_mailbox = mailbox;
    } else if let Some(s) = opt("VS_WS_MAILBOX")? {
        ws_cfg.per_connection_mailbox = lenient_int("VS_WS_MAILBOX", &s)?;
    }
    if let Some(ms) = runtime_config.and_then(|config| config.ping_interval_ms) {
        ws_cfg.ping_interval = Duration::from_millis(ms);
    } else if let Some(s) = opt("VS_WS_PING_INTERVAL_MS")? {
        let ms: u64 = lenient_int("VS_WS_PING_INTERVAL_MS", &s)?;
        ws_cfg.ping_interval = Duration::from_millis(ms);
    }
    if let Some(ms) = runtime_config.and_then(|config| config.pong_timeout_ms) {
        ws_cfg.pong_timeout = Duration::from_millis(ms);
    } else if let Some(s) = opt("VS_WS_PONG_TIMEOUT_MS")? {
        let ms: u64 = lenient_int("VS_WS_PONG_TIMEOUT_MS", &s)?;
        ws_cfg.pong_timeout = Duration::from_millis(ms);
    }
    // Per-pod connection cap (the OOM backstop). Unset = unlimited.
    if let Some(max) = runtime_config.and_then(|config| config.max_connections) {
        ws_cfg.max_connections = Some(max);
    } else if let Some(s) = opt("VS_WS_MAX_CONNS")? {
        let max: usize = lenient_int("VS_WS_MAX_CONNS", &s)?;
        ws_cfg.max_connections = (max > 0).then_some(max);
    }

    let configured_provider = runtime_config
        .and_then(|config| config.provider)
        .or_else(|| shared_realtime.and_then(|config| config.provider));
    let role_environment_provider = opt("VS_WS_PROVIDER")?
        .map(|value| parse_realtime_provider(&value))
        .transpose()?;
    let shared_environment_provider = opt("VS_REALTIME_PROVIDER")?
        .map(|value| parse_realtime_provider(&value))
        .transpose()?;
    if let (Some(role), Some(shared)) = (role_environment_provider, shared_environment_provider) {
        if role != shared {
            anyhow::bail!("VS_WS_PROVIDER and VS_REALTIME_PROVIDER disagree");
        }
    }
    let environment_provider = role_environment_provider.or(shared_environment_provider);
    if let (Some(configured), Some(environment)) = (configured_provider, environment_provider) {
        if configured != environment {
            anyhow::bail!(
                "runtime.ws.provider and VS_WS_PROVIDER select different realtime brokers"
            );
        }
    }
    let jetstream_config = runtime_config.and_then(|config| config.jetstream.as_ref());
    let redis_config = runtime_config
        .and_then(|config| config.redis_streams.as_ref())
        .or_else(|| shared_realtime.and_then(|config| config.redis_streams.as_ref()));
    let legacy_jetstream = std::env::var("VS_WS_JETSTREAM")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let inferred_provider = if redis_config.is_some() {
        RealtimeBrokerProvider::RedisStreams
    } else if jetstream_config.is_some() || legacy_jetstream {
        RealtimeBrokerProvider::NatsJetstream
    } else {
        RealtimeBrokerProvider::NatsCore
    };
    let provider = configured_provider
        .or(environment_provider)
        .unwrap_or(inferred_provider);
    if provider != inferred_provider
        && (jetstream_config.is_some() || redis_config.is_some() || legacy_jetstream)
    {
        anyhow::bail!("selected runtime.ws provider conflicts with provider-specific settings");
    }

    // JetStream remains opt-in and retains the legacy VS_WS_JETSTREAM switch.
    if provider == RealtimeBrokerProvider::NatsJetstream {
        let mut js_cfg = JetStreamConfig::default();
        if let Some(s) = jetstream_config
            .and_then(|config| config.stream.clone())
            .or(opt("VS_WS_JS_STREAM")?)
        {
            js_cfg.stream_name = s;
        }
        if let Some(s) = jetstream_config
            .and_then(|config| config.pod_id.clone())
            .or(opt("VS_WS_JS_POD_ID")?)
        {
            js_cfg.pod_id = s;
        }
        if let Some(ms) = jetstream_config.and_then(|config| config.inactive_threshold_ms) {
            js_cfg.consumer_inactive_threshold = Duration::from_millis(ms);
        } else if let Some(s) = opt("VS_WS_JS_INACTIVE_THRESHOLD_MS")? {
            let ms: u64 = lenient_int("VS_WS_JS_INACTIVE_THRESHOLD_MS", &s)?;
            js_cfg.consumer_inactive_threshold = Duration::from_millis(ms);
        }
        if let Some(ms) = jetstream_config.and_then(|config| config.reaper_interval_ms) {
            js_cfg.reaper_interval = Duration::from_millis(ms);
        } else if let Some(s) = opt("VS_WS_JS_REAPER_INTERVAL_MS")? {
            let ms: u64 = lenient_int("VS_WS_JS_REAPER_INTERVAL_MS", &s)?;
            js_cfg.reaper_interval = Duration::from_millis(ms);
        }
        if let Some(replicas) = jetstream_config.and_then(|config| config.replicas) {
            js_cfg.replicas = replicas;
        } else if let Some(s) = opt("VS_WS_JS_REPLICAS")? {
            js_cfg.replicas = lenient_int("VS_WS_JS_REPLICAS", &s)?;
        }
        // Stream sizing — all optional. The defaults are self-bounding
        // (10m age, 512 MiB), so an operator can leave every one unset
        // and the stream still can't bloat.
        if let Some(storage) = jetstream_config.and_then(|config| config.storage) {
            js_cfg.storage = match storage {
                ConfigJetStreamStorage::Memory => ventstream_ws::StreamStorage::Memory,
                ConfigJetStreamStorage::File => ventstream_ws::StreamStorage::File,
            };
        } else if let Some(s) = opt("VS_WS_JS_STORAGE")? {
            js_cfg.storage = match s.to_ascii_lowercase().as_str() {
                "memory" | "mem" => ventstream_ws::StreamStorage::Memory,
                "file" => ventstream_ws::StreamStorage::File,
                other => {
                    anyhow::bail!("VS_WS_JS_STORAGE must be 'file' or 'memory', got '{other}'")
                }
            };
        }
        if let Some(secs) = jetstream_config.and_then(|config| config.max_age_secs) {
            js_cfg.max_age = Duration::from_secs(secs);
        } else if let Some(s) = opt("VS_WS_JS_MAX_AGE_SECS")? {
            let secs: u64 = lenient_int("VS_WS_JS_MAX_AGE_SECS", &s)?;
            js_cfg.max_age = Duration::from_secs(secs);
        }
        if let Some(max_bytes) = jetstream_config.and_then(|config| config.max_bytes) {
            js_cfg.max_bytes = max_bytes;
        } else if let Some(s) = opt("VS_WS_JS_MAX_BYTES")? {
            js_cfg.max_bytes = lenient_int("VS_WS_JS_MAX_BYTES", &s)?;
        }
        if let Some(max_msgs) = jetstream_config.and_then(|config| config.max_msgs) {
            js_cfg.max_msgs = max_msgs;
        } else if let Some(s) = opt("VS_WS_JS_MAX_MSGS")? {
            js_cfg.max_msgs = lenient_int("VS_WS_JS_MAX_MSGS", &s)?;
        }
        ws_cfg.jetstream = Some(js_cfg);
    }

    if provider == RealtimeBrokerProvider::RedisStreams {
        let mut redis = RedisStreamsConfig::default();
        if let Some(reference) = redis_config.and_then(|config| config.url_ref.as_ref()) {
            redis.url = resolve_value_ref(reference)?;
        } else if let Some(url) = opt("VS_WS_REDIS_URL")?.or(opt("VS_REDIS_URL")?) {
            redis.url = url;
        }
        if let Some(prefix) = redis_config
            .and_then(|config| config.key_prefix.clone())
            .or(opt("VS_WS_REDIS_KEY_PREFIX")?.or(opt("VS_REDIS_KEY_PREFIX")?))
        {
            redis.key_prefix = prefix;
        }
        if let Some(value) = redis_config.and_then(|config| config.read_batch) {
            redis.read_batch = value;
        } else if let Some(value) = opt("VS_WS_REDIS_READ_BATCH")?.or(opt("VS_REDIS_READ_BATCH")?) {
            redis.read_batch = lenient_int("VS_WS_REDIS_READ_BATCH", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.block_timeout_ms) {
            redis.block_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_WS_REDIS_BLOCK_TIMEOUT_MS")?.or(opt("VS_REDIS_BLOCK_TIMEOUT_MS")?)
        {
            redis.block_timeout =
                Duration::from_millis(lenient_int("VS_WS_REDIS_BLOCK_TIMEOUT_MS", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.broadcast_capacity) {
            redis.broadcast_capacity = value;
        } else if let Some(value) =
            opt("VS_WS_REDIS_BROADCAST_CAPACITY")?.or(opt("VS_REDIS_BROADCAST_CAPACITY")?)
        {
            redis.broadcast_capacity = lenient_int("VS_WS_REDIS_BROADCAST_CAPACITY", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.max_tenant_hubs) {
            redis.max_tenant_hubs = value;
        } else if let Some(value) =
            opt("VS_WS_REDIS_MAX_TENANT_HUBS")?.or(opt("VS_REDIS_MAX_TENANT_HUBS")?)
        {
            redis.max_tenant_hubs = lenient_int("VS_WS_REDIS_MAX_TENANT_HUBS", &value)?;
        }
        if let Some(value) = redis_config.and_then(|config| config.max_length) {
            redis.max_length = Some(value);
        } else if let Some(value) = opt("VS_WS_REDIS_MAX_LENGTH")?.or(opt("VS_REDIS_MAX_LENGTH")?) {
            redis.max_length = Some(lenient_int("VS_WS_REDIS_MAX_LENGTH", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.connect_timeout_ms) {
            redis.connect_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_WS_REDIS_CONNECT_TIMEOUT_MS")?.or(opt("VS_REDIS_CONNECT_TIMEOUT_MS")?)
        {
            redis.connect_timeout =
                Duration::from_millis(lenient_int("VS_WS_REDIS_CONNECT_TIMEOUT_MS", &value)?);
        }
        if let Some(value) = redis_config.and_then(|config| config.response_timeout_ms) {
            redis.response_timeout = Duration::from_millis(value);
        } else if let Some(value) =
            opt("VS_WS_REDIS_RESPONSE_TIMEOUT_MS")?.or(opt("VS_REDIS_RESPONSE_TIMEOUT_MS")?)
        {
            redis.response_timeout =
                Duration::from_millis(lenient_int("VS_WS_REDIS_RESPONSE_TIMEOUT_MS", &value)?);
        }
        ws_cfg.redis_streams = Some(redis);
    }

    ws_cfg.expected_tenant = load_expected_tenant(engine_config, "ws")?;
    Ok(ws_cfg)
}

fn parse_realtime_provider(value: &str) -> Result<RealtimeBrokerProvider> {
    match value.trim().to_ascii_lowercase().as_str() {
        "nats_core" | "core" => Ok(RealtimeBrokerProvider::NatsCore),
        "nats_jetstream" | "jetstream" => Ok(RealtimeBrokerProvider::NatsJetstream),
        "redis_streams" | "redis" => Ok(RealtimeBrokerProvider::RedisStreams),
        other => anyhow::bail!(
            "unknown WebSocket realtime provider '{other}' (expected nats_core, nats_jetstream, or redis_streams)"
        ),
    }
}

fn parse_roles(s: &str) -> Result<HashSet<Role>> {
    let mut out = HashSet::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        out.insert(Role::parse(p)?);
    }
    if out.is_empty() {
        return Err(anyhow!("VS_ROLES is empty"));
    }
    Ok(out)
}

fn req(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("required env var {key} is not set"))
}

fn opt(key: &str) -> Result<Option<String>> {
    match std::env::var(key) {
        Ok(v) => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading env var {key}")),
    }
}

/// Parse a `VS_*` integer env value leniently into `i128` for the caller to
/// range-check. Accepts plain ints, underscore separators (`536_870_912`),
/// and integral float/scientific forms (`5.36e+08`, `5e8`, `1024.0`). The
/// control-plane UI renders large numbers in scientific notation, and a bare
/// `str::parse::<u64>()` on `"5.36e+08"` errors — which would crash the agent
/// at boot (a fleet-wide outage) instead of degrading. Rejects non-integral
/// (`1.5`), non-finite, non-numeric, and values too large for `f64` to
/// represent exactly.
fn parse_lenient_int(raw: &str) -> std::result::Result<i128, String> {
    let s: String = raw.trim().chars().filter(|c| *c != '_').collect();
    if s.is_empty() {
        return Err("empty value".to_string());
    }
    if let Ok(v) = s.parse::<i128>() {
        return Ok(v);
    }
    match s.parse::<f64>() {
        Ok(f) if !f.is_finite() => Err(format!("{raw:?} is not finite")),
        Ok(f) if f.fract() != 0.0 => Err(format!("{raw:?} is not an integer")),
        // f64 represents integers exactly only up to 2^53.
        Ok(f) if f.abs() >= 9_007_199_254_740_992.0 => {
            Err(format!("{raw:?} is too large to parse precisely"))
        }
        Ok(f) => Ok(f as i128),
        Err(_) => Err(format!("{raw:?} is not a valid integer")),
    }
}

/// Leniently parse an integer env value and range-check it into `T`. The one
/// place every numeric `VS_*` var is parsed — see [`parse_lenient_int`].
fn lenient_int<T: TryFrom<i128>>(key: &str, s: &str) -> Result<T> {
    let v = parse_lenient_int(s).map_err(|e| anyhow::anyhow!("env var {key}: {e}"))?;
    T::try_from(v).map_err(|_| anyhow::anyhow!("env var {key}: {s:?} out of range"))
}

fn opt_usize(key: &str, default: usize) -> Result<usize> {
    match opt(key)? {
        Some(s) => lenient_int(key, &s),
        None => Ok(default),
    }
}

/// Read an optional boolean env var. `true`/`1`/`yes`/`on` (any case)
/// counts as true; everything else (including absent) returns `default`.
/// Parse errors are silent by design — operators routinely set these to
/// the empty string to mean "unset," and a noisy failure would be worse
/// than a forgiving fallback.
fn bool_env(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" | "" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

/// Drop a logical replication slot if it exists. Used by the YAML
/// fingerprint resync path so the source's next connect re-bootstraps
/// from a clean snapshot.
///
/// `WHERE EXISTS` makes this a no-op when the slot is absent — useful
/// because operators may have already dropped it by hand. We connect
/// using the same transport policy as the replication connection.
async fn replication_slot_exists(pg: &PostgresCdcConfig, slot: &str) -> Result<bool> {
    let client = ventstream_sources::postgres::connect_client(pg, "inspect replication slot")
        .await
        .with_context(|| format!("connecting to {} to inspect slot {slot}", pg.host))?;
    let exists = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .await
        .with_context(|| format!("inspecting replication slot {slot}"))?
        .get::<_, bool>(0);
    drop(client);
    Ok(exists)
}

async fn drop_replication_slot(pg: &PostgresCdcConfig, slot: &str) -> Result<()> {
    // Shared, properly-escaped builder — the single source for every PG
    // connection the engine opens (M9). The prior hand-built `format!` here
    // was the exact slot-drop/resync hazard M9 closes.
    let client = ventstream_sources::postgres::connect_client(pg, "drop replication slot")
        .await
        .with_context(|| format!("connecting to {} to drop slot {slot}", pg.host))?;
    let res = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| format!("dropping replication slot {slot}"));
    drop(client);
    match res {
        Ok(0) => {
            info!(slot = %slot, "no existing replication slot to drop");
            Ok(())
        }
        Ok(_) => {
            info!(slot = %slot, "dropped existing replication slot");
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Read an optional u64 milliseconds env var, returning a [`Duration`].
fn opt_duration_ms(key: &str, default: Duration) -> Result<Duration> {
    match opt(key)? {
        Some(s) => {
            let ms: u64 = lenient_int(key, &s)?;
            Ok(Duration::from_millis(ms))
        }
        None => Ok(default),
    }
}

/// Initialise the global tracing subscriber.
///
/// Reads two env vars:
///
/// - `RUST_LOG` (standard) — filter directives, e.g.
///   `info,ventstream_sources::neo4j=debug,ventstream::dispatcher=debug`
/// - `VS_LOG_FORMAT` — `pretty` (default, human-readable) or `json`
///   (one record per line, machine-parseable for Datadog / Loki /
///   CloudWatch). Case-insensitive.
///
/// JSON mode surfaces every structured field (the `key=value` parts of
/// every `info!` / `debug!` call) as a top-level JSON property, so
/// log-aggregator queries like `metric:"neo4j.tail.recomposed" AND
/// recomposed:>1000` work directly without grep gymnastics.
/// Stderr-only subscriber for the MCP server: stdout is reserved for
/// protocol frames.
fn install_stderr_tracing() {
    use tracing_subscriber::prelude::*;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_writer(std::io::stderr),
        )
        .init();
}

fn install_tracing(
    telemetry_layer: Option<ventstream_telemetry::TelemetryTraceLayer>,
    log_format: Option<ConfigLogFormat>,
) {
    use tracing_subscriber::prelude::*;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let want_json = match log_format {
        Some(ConfigLogFormat::Json) => true,
        Some(ConfigLogFormat::Pretty) => false,
        None => std::env::var("VS_LOG_FORMAT")
            .map(|v| v.eq_ignore_ascii_case("json"))
            .unwrap_or(false),
    };
    // Layered registry so the telemetry trace layer is registered ALONGSIDE
    // the fmt layer (M14). `Option<Layer>` is itself a no-op `Layer` when None,
    // so this is uniform whether telemetry is configured or not.
    let registry = tracing_subscriber::registry().with(filter);
    if want_json {
        registry
            .with(tracing_subscriber::fmt::layer().json().with_target(true))
            .with(telemetry_layer)
            .init();
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .with(telemetry_layer)
            .init();
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_arguments_parse_repeated_targets_and_bounded_numbers() {
        let argv = vec![
            "ventstream".to_owned(),
            "--check-redis-drift".to_owned(),
            "--redis-target".to_owned(),
            "orders".to_owned(),
            "--redis-target".to_owned(),
            "customers".to_owned(),
            "--redis-drift-scan-limit".to_owned(),
            "25000".to_owned(),
        ];
        assert_eq!(
            repeated_argument_values(&argv, "--redis-target").expect("targets"),
            ["orders", "customers"]
        );
        assert_eq!(
            optional_usize_argument(&argv, "--redis-drift-scan-limit", 100_000)
                .expect("scan limit"),
            25_000
        );
    }

    #[test]
    fn maintenance_arguments_reject_missing_and_repeated_scalar_values() {
        let missing = vec!["ventstream".to_owned(), "--redis-target".to_owned()];
        assert!(repeated_argument_values(&missing, "--redis-target").is_err());

        let repeated = vec![
            "ventstream".to_owned(),
            "--redis-drift-scan-limit".to_owned(),
            "10".to_owned(),
            "--redis-drift-scan-limit".to_owned(),
            "20".to_owned(),
        ];
        assert!(optional_usize_argument(&repeated, "--redis-drift-scan-limit", 100).is_err());
    }

    struct EnvOverride {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvOverride {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvOverride {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.take() {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }

    #[test]
    fn redis_runtime_loader_resolves_sentinel_and_cluster_topologies() {
        let _sentinel_a = EnvOverride::set(
            "VS_TEST_REDIS_SENTINEL_A",
            "redis://sentinel-a.internal:26379",
        );
        let _sentinel_b = EnvOverride::set(
            "VS_TEST_REDIS_SENTINEL_B",
            "redis://sentinel-b.internal:26379",
        );
        let _sentinel_password =
            EnvOverride::set("VS_TEST_REDIS_SENTINEL_PASSWORD", "sentinel-secret");
        let _writer_id = EnvOverride::set("VS_TEST_REDIS_WRITER_ID", "revision-b");
        let _previous_writer = EnvOverride::set("VS_TEST_REDIS_PREVIOUS_WRITER_ID", "revision-a");
        let file = EngineFileConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_TEST_MONGO_URI
    database: shop
    collections: [orders]
sink:
  kind: redis
  redis:
    topology:
      mode: sentinel
      service_name: orders-primary
      endpoints:
        - env:VS_TEST_REDIS_SENTINEL_A
        - env:VS_TEST_REDIS_SENTINEL_B
      data_node_tls: false
      sentinel_auth:
        mode: password
        password_ref: env:VS_TEST_REDIS_SENTINEL_PASSWORD
    keyspace:
      prefix: ventstream:shop
    acknowledgement:
      mode: aof
      replicas: 1
      timeout_ms: 2000
    writer:
      id_ref: env:VS_TEST_REDIS_WRITER_ID
      lease_ms: 15000
      takeover_from_ref: env:VS_TEST_REDIS_PREVIOUS_WRITER_ID
"#,
        )
        .expect("valid Sentinel engine config");
        let sentinel = load_redis_sink_config(Some(&file)).expect("load Sentinel topology");
        match &sentinel.topology {
            RedisTopology::Sentinel(topology) => {
                assert_eq!(topology.service_name, "orders-primary");
                assert_eq!(
                    topology.endpoints,
                    [
                        "redis://sentinel-a.internal:26379",
                        "redis://sentinel-b.internal:26379"
                    ]
                );
                assert_eq!(topology.password.as_deref(), Some("sentinel-secret"));
                assert!(!topology.data_node_tls);
            }
            other => panic!("expected Sentinel topology, got {other:?}"),
        }
        assert_eq!(sentinel.writer_id, "revision-b");
        assert_eq!(sentinel.writer_lease, Duration::from_secs(15));
        assert_eq!(sentinel.writer_takeover_from.as_deref(), Some("revision-a"));
        assert!(matches!(
            sentinel.acknowledgement,
            RedisAcknowledgement::Aof {
                local: true,
                replicas: 1,
                timeout
            } if timeout == Duration::from_secs(2)
        ));

        let _mode = EnvOverride::set("VS_REDIS_SINK_TOPOLOGY", "cluster");
        let _nodes = EnvOverride::set(
            "VS_REDIS_SINK_CLUSTER_URLS",
            "redis://cluster-a.internal:6379, redis://cluster-b.internal:6379",
        );
        let _prefix = EnvOverride::set("VS_REDIS_SINK_KEY_PREFIX", "ventstream:shop");
        let cluster = load_redis_sink_config(None).expect("load Cluster topology");
        match cluster.topology {
            RedisTopology::Cluster { endpoints } => assert_eq!(
                endpoints,
                [
                    "redis://cluster-a.internal:6379",
                    "redis://cluster-b.internal:6379"
                ]
            ),
            other => panic!("expected Cluster topology, got {other:?}"),
        }
    }

    #[test]
    fn redis_runtime_loader_resolves_aof_acknowledgement_from_env() {
        let _mode = EnvOverride::set("VS_REDIS_SINK_ACK_MODE", "aof");
        let _local = EnvOverride::set("VS_REDIS_SINK_ACK_LOCAL_AOF", "false");
        let _replicas = EnvOverride::set("VS_REDIS_SINK_ACK_REPLICAS", "2");
        let _timeout = EnvOverride::set("VS_REDIS_SINK_ACK_TIMEOUT_MS", "2500");
        let acknowledgement =
            load_redis_acknowledgement_from_env().expect("load AOF acknowledgement");
        assert!(matches!(
            acknowledgement,
            RedisAcknowledgement::Aof {
                local: false,
                replicas: 2,
                timeout
            } if timeout == Duration::from_millis(2500)
        ));
    }

    #[test]
    fn redis_runtime_loader_preserves_mounted_credentials_for_rotation() {
        let _endpoint =
            EnvOverride::set("VS_TEST_REDIS_ROTATION_URL", "redis://redis.internal:6379");
        let _username = EnvOverride::set("VS_TEST_REDIS_ROTATION_USERNAME", "ventstream");
        let file = EngineFileConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_TEST_MONGO_URI
    database: shop
    collections: [orders]
sink:
  kind: redis
  redis:
    endpoint_ref: env:VS_TEST_REDIS_ROTATION_URL
    auth:
      mode: acl
      username_ref: env:VS_TEST_REDIS_ROTATION_USERNAME
      password_ref: file:/run/secrets/redis-password
    keyspace:
      prefix: ventstream:shop
"#,
        )
        .expect("valid mounted-credential config");

        let redis = load_redis_sink_config(Some(&file)).expect("load Redis config");
        assert_eq!(redis.username.as_deref(), Some("ventstream"));
        assert!(redis.username_file.is_none());
        assert_eq!(
            redis.password_file.as_deref(),
            Some(Path::new("/run/secrets/redis-password"))
        );
        assert!(redis.password.is_none());
    }

    #[test]
    fn mysql_dispatcher_is_serialized_for_document_ordering() {
        let config = DispatcherConfig {
            max_parallel_bulks: 8,
            ..DispatcherConfig::default()
        };

        let effective = mysql_dispatcher_config(config);

        assert_eq!(effective.max_parallel_bulks, 1);
    }

    fn projection_target_config() -> EngineFileConfig {
        EngineFileConfig::from_yaml_str(
            r#"
schema_version: 1
roles: [cdc]
source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    user_ref: env:VS_PG_USER
    password_ref: env:VS_PG_PASSWORD
    database_ref: env:VS_PG_DATABASE
    publication_ref: env:VS_PG_PUBLICATION
    slot_ref: env:VS_PG_SLOT
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing:
      strategy: by_projection_target
specs:
  joins: joins.yaml
"#,
        )
        .expect("valid engine config")
    }

    fn joins_from_yaml(input: &str) -> Vec<JoinDefinition> {
        serde_yaml::from_str::<JoinsFile>(input)
            .expect("valid joins YAML")
            .joins
    }

    #[test]
    fn mysql_non_primary_related_join_key_requires_full_row_images() {
        let joins = joins_from_yaml(
            r#"
joins:
  - name: orders
    primary: { table: shop.orders, pk: id }
    related:
      - id: items
        table: shop.order_items
        pk: id
        join_on: { from: id, to: order_id }
        embed_as: items
        cardinality: many
"#,
        );
        assert!(mysql_joins_require_full_row_image(&joins));
    }

    #[test]
    fn mysql_related_primary_key_join_does_not_require_full_row_images() {
        let joins = joins_from_yaml(
            r#"
joins:
  - name: orders
    primary: { table: shop.orders, pk: id }
    related:
      - id: customer
        table: shop.customers
        pk: id
        join_on: { from: customer_id, to: id }
        embed_as: customer
        cardinality: one
"#,
        );
        assert!(!mysql_joins_require_full_row_image(&joins));
    }

    #[test]
    fn projection_target_routing_requires_target_on_every_join() {
        let config = projection_target_config();
        let joins = joins_from_yaml(
            r#"
joins:
  - name: orders
    primary: { table: shop.orders, pk: id }
  - name: customers
    primary: { table: shop.customers, pk: id }
    target: { index: customer-search }
"#,
        );

        let error = validate_projection_target_indexes(Some(&config), &joins)
            .expect_err("missing projection target must fail startup");
        assert!(error
            .to_string()
            .contains("missing target.index for: orders"));
    }

    #[test]
    fn projection_target_routing_accepts_complete_targets() {
        let config = projection_target_config();
        let joins = joins_from_yaml(
            r#"
joins:
  - name: orders
    primary: { table: shop.orders, pk: id }
    target: { index: order-search }
  - name: customers
    primary: { table: shop.customers, pk: id }
    target: { index: customer-search }
"#,
        );

        validate_projection_target_indexes(Some(&config), &joins)
            .expect("complete projection targets should pass startup validation");
    }

    fn redis_runtime(
        routing: RedisKeyRouting,
        ownership: RedisKeyspaceOwnership,
    ) -> SinkRuntimeConfig {
        SinkRuntimeConfig::Redis(Box::new(
            RedisConfig::new(
                "redis",
                "redis://127.0.0.1:6379",
                "ventstream:test",
                routing,
            )
            .with_keyspace_ownership(ownership),
        ))
    }

    #[test]
    fn shared_redis_keyspace_rejects_drain_before_state_removal() {
        let sink = redis_runtime(
            RedisKeyRouting::ByOutputRelation,
            RedisKeyspaceOwnership::Shared,
        );
        let targets =
            routed_redis_drain_targets(&sink, vec!["orders".to_owned()], Vec::new()).unwrap();

        let error = sink
            .validate_redis_drain(true, &targets)
            .expect_err("shared keyspace must reject drain");

        assert!(error.to_string().contains("ownership=exclusive"));
        assert!(error
            .to_string()
            .contains("no local cursor state was removed"));
    }

    #[test]
    fn exclusive_fixed_redis_keyspace_allows_finite_replayable_drain() {
        let sink = redis_runtime(
            RedisKeyRouting::Fixed("orders".to_owned()),
            RedisKeyspaceOwnership::Exclusive,
        );
        let targets = routed_redis_drain_targets(&sink, Vec::new(), Vec::new()).unwrap();

        assert_eq!(targets, vec!["orders"]);
        sink.validate_redis_drain(true, &targets)
            .expect("exclusive replayable keyspace should drain");
    }

    #[test]
    fn redis_drain_requires_bootstrap_and_complete_projection_targets() {
        let sink = redis_runtime(
            RedisKeyRouting::ByProjectionTarget,
            RedisKeyspaceOwnership::Exclusive,
        );
        let error = routed_redis_drain_targets(
            &sink,
            vec!["orders".to_owned()],
            vec![Some("orders-view".to_owned()), None],
        )
        .expect_err("missing projection target must reject drain");
        assert!(error.to_string().contains("every projection"));

        let targets = routed_redis_drain_targets(
            &sink,
            vec!["orders".to_owned()],
            vec![
                Some("orders-view".to_owned()),
                Some("orders-view".to_owned()),
            ],
        )
        .unwrap();
        assert_eq!(targets, vec!["orders-view"]);
        assert!(sink.validate_redis_drain(false, &targets).is_err());
    }

    fn one_shot_health_server(status: u16) -> (String, std::thread::JoinHandle<String>) {
        use std::io::{BufRead as _, Write as _};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind health server");
        let address = listener.local_addr().expect("health server address");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept health request");
            let mut reader =
                std::io::BufReader::new(stream.try_clone().expect("clone health stream"));
            let mut request_line = String::new();
            reader
                .read_line(&mut request_line)
                .expect("read health request");
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).expect("read health header");
                if header == "\r\n" || header.is_empty() {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("write health response");
            request_line
        });
        (address.to_string(), handle)
    }

    #[test]
    fn healthcheck_accepts_successful_http_status() {
        let (address, server) = one_shot_health_server(204);
        run_healthcheck(&address, "/healthz").expect("successful healthcheck");
        assert_eq!(
            server.join().expect("health server join"),
            "GET /healthz HTTP/1.1\r\n"
        );
    }

    #[test]
    fn healthcheck_rejects_unsuccessful_http_status() {
        let (address, server) = one_shot_health_server(503);
        let error = run_healthcheck(&address, "/readyz").expect_err("failed healthcheck");
        assert!(error.to_string().contains("503 Test"));
        assert_eq!(
            server.join().expect("health server join"),
            "GET /readyz HTTP/1.1\r\n"
        );
    }

    #[tokio::test]
    async fn fleet_supervisor_stdin_ignores_unknown_input_then_accepts_shutdown() {
        use tokio::io::{AsyncWriteExt, BufReader};

        let (reader, mut writer) = tokio::io::duplex(128);
        let waiter = tokio::spawn(wait_for_supervisor_shutdown(BufReader::new(reader)));
        writer.write_all(b"unknown\nshutdown\n").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("supervisor command timeout")
            .expect("supervisor waiter join");
    }

    #[tokio::test]
    async fn fleet_supervisor_stdin_eof_stops_orphaned_engine() {
        let (reader, writer) = tokio::io::duplex(16);
        drop(writer);
        tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_supervisor_shutdown(BufReader::new(reader)),
        )
        .await
        .expect("supervisor EOF timeout");
    }

    #[test]
    fn lenient_int_accepts_plain_underscore_and_scientific() {
        assert_eq!(parse_lenient_int("1024").unwrap(), 1024);
        assert_eq!(parse_lenient_int("  42 ").unwrap(), 42);
        assert_eq!(parse_lenient_int("536_870_912").unwrap(), 536_870_912);
        assert_eq!(parse_lenient_int("-7").unwrap(), -7);
        // Scientific / float forms the control-plane UI emits — the H11 bug.
        assert_eq!(parse_lenient_int("5.36e+08").unwrap(), 536_000_000);
        assert_eq!(parse_lenient_int("5e8").unwrap(), 500_000_000);
        assert_eq!(parse_lenient_int("1024.0").unwrap(), 1024);
        // 512 MiB rendered in scientific notation (2^29, exact in f64).
        assert_eq!(parse_lenient_int("5.36870912e+08").unwrap(), 536_870_912);
    }

    #[test]
    fn lenient_int_rejects_non_integers() {
        assert!(parse_lenient_int("1.5").is_err());
        assert!(parse_lenient_int("abc").is_err());
        assert!(parse_lenient_int("").is_err());
        assert!(parse_lenient_int("   ").is_err());
        assert!(parse_lenient_int("inf").is_err());
        // Beyond f64's exact-integer range (2^53).
        assert!(parse_lenient_int("1e20").is_err());
    }

    #[test]
    fn lenient_int_range_checks_into_target_type() {
        // A VS_WS_JS_MAX_BYTES value (512 MiB) in scientific notation, the
        // shape that crashed a bare parse::<i64>().
        let v: i64 = lenient_int("VS_WS_JS_MAX_BYTES", "5.36870912e+08").expect("i64");
        assert_eq!(v, 536_870_912);
        // A port rendered in scientific notation.
        let p: u16 = lenient_int("VS_PG_PORT", "5.432e+03").expect("u16");
        assert_eq!(p, 5432);
        // Out of range for the target type → error, not panic, not silent wrap.
        assert!(lenient_int::<u16>("K", "70000").is_err());
        assert!(lenient_int::<usize>("K", "-1").is_err());
    }

    #[test]
    fn realtime_provider_parser_accepts_documented_names_and_aliases() {
        assert_eq!(
            parse_realtime_provider("nats_core").unwrap(),
            RealtimeBrokerProvider::NatsCore
        );
        assert_eq!(
            parse_realtime_provider("jetstream").unwrap(),
            RealtimeBrokerProvider::NatsJetstream
        );
        assert_eq!(
            parse_realtime_provider("redis_streams").unwrap(),
            RealtimeBrokerProvider::RedisStreams
        );
        assert!(parse_realtime_provider("kafka").is_err());
    }

    #[test]
    fn join_checkpoint_requires_matching_state_and_source_progress() {
        let expected = "postgres://db#slot=orders";
        assert!(join_checkpoint_recoverable(Some(expected), expected, true));
        assert!(!join_checkpoint_recoverable(None, expected, true));
        assert!(!join_checkpoint_recoverable(
            Some(expected),
            expected,
            false
        ));
        assert!(!join_checkpoint_recoverable(
            Some("postgres://db#slot=other"),
            expected,
            true
        ));
    }

    #[test]
    fn neo4j_tls_policy_controls_the_uri_scheme() {
        let strict = DatabaseTlsConfig::default();
        assert_eq!(
            apply_neo4j_tls_mode("bolt://db.example.com:7687", Some(&strict)).unwrap(),
            "bolt+s://db.example.com:7687"
        );
        assert!(apply_neo4j_tls_mode("neo4j+ssc://db.example.com", Some(&strict)).is_err());

        let disabled = DatabaseTlsConfig {
            mode: DatabaseTlsMode::Disabled,
            ca_file: None,
        };
        assert_eq!(
            apply_neo4j_tls_mode("neo4j+s://db.example.com", Some(&disabled)).unwrap(),
            "neo4j://db.example.com"
        );
    }

    #[test]
    fn aws_rds_trust_resolves_to_the_packaged_bundle() {
        let tls: FileTlsConfig =
            serde_yaml::from_str("mode: verify_full\ntrust:\n  provider: aws_rds\n")
                .expect("parse AWS RDS trust configuration");
        let resolved = database_tls_or_env(
            Some(&tls),
            "UNUSED_TLS_MODE",
            "UNUSED_TLS_CA_FILE",
            Some("UNUSED_TLS_TRUST_PROVIDER"),
        )
        .expect("resolve AWS RDS trust configuration")
        .expect("TLS configuration");
        let path = resolved.ca_file.expect("materialized provider bundle");
        let bundle = std::fs::read_to_string(path).expect("read materialized provider bundle");
        assert!(bundle.starts_with("-----BEGIN CERTIFICATE-----"));
        assert_eq!(bundle.matches("-----BEGIN CERTIFICATE-----").count(), 108);
    }

    /// Stub backend for [`run_cdc_loop`]: each iteration cancels its
    /// child token (the engine's internal-failure signature) and yields
    /// the scripted outcome.
    struct ScriptedBackend {
        outcomes: std::collections::VecDeque<std::result::Result<EngineIterationOutcome, String>>,
        iterations: u32,
    }

    #[async_trait::async_trait]
    impl CdcBackend for ScriptedBackend {
        async fn validate_drain(&self) -> Result<()> {
            Ok(())
        }
        async fn drain_local(&self) -> Result<()> {
            Ok(())
        }
        async fn reconcile_orphans(&self) -> Result<()> {
            Ok(())
        }
        fn prepare_bootstrap(&mut self) -> Result<()> {
            Ok(())
        }
        async fn run_iteration(
            &mut self,
            inner: ShutdownToken,
            _outer: ShutdownToken,
        ) -> Result<EngineIterationOutcome> {
            self.iterations += 1;
            match self.outcomes.pop_front().expect("scripted outcome") {
                Ok(outcome) => Ok(outcome),
                Err(message) => {
                    inner.cancel();
                    Err(anyhow!(message))
                }
            }
        }
    }

    const REPRO_1045: &str = "mysql operation failed: Server error: `ERROR 28000 (1045): \
         Access denied for user 'ventstream'@'10.2.14.7' (using password: YES)`";

    #[tokio::test(start_paused = true)]
    async fn credential_iteration_failures_exhaust_the_budget_and_exit() {
        let backend = ScriptedBackend {
            outcomes: (0..5).map(|_| Err(REPRO_1045.to_owned())).collect(),
            iterations: 0,
        };
        let error = run_cdc_loop(backend, ShutdownToken::new())
            .await
            .expect_err("terminal");
        let text = error.to_string();
        assert!(text.starts_with("credential error persisted after 5 attempts"));
        assert!(text.contains("supervisor can restart with fresh credentials"));
    }

    #[tokio::test(start_paused = true)]
    async fn transient_iteration_failures_retry_with_backoff_until_shutdown() {
        let backend = ScriptedBackend {
            outcomes: vec![
                Err("mysql connection failed: Connection refused (os error 61)".to_owned()),
                Err("mysql connection failed: connection timed out".to_owned()),
                Ok(EngineIterationOutcome::Shutdown),
            ]
            .into(),
            iterations: 0,
        };
        // Transient errors must keep retrying (bounded backoff, no budget)
        // and reach the scripted clean shutdown.
        assert!(run_cdc_loop(backend, ShutdownToken::new()).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn crash_fast_source_errors_are_terminal_on_first_iteration() {
        let message =
            ventstream_sources::credential::exhausted_message(&"ERROR 28000 (1045): Access denied");
        let backend = ScriptedBackend {
            outcomes: vec![Err(message.clone())].into(),
            iterations: 0,
        };
        let error = run_cdc_loop(backend, ShutdownToken::new())
            .await
            .expect_err("terminal");
        assert_eq!(error.to_string(), message);
    }
}
