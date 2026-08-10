//! `ventstream mcp` — read-only MCP server speaking JSON-RPC 2.0.
//!
//! Exposes sink-materialized documents to MCP clients through the
//! `ventstream_sinks::read` path, so read keys and index names are
//! byte-identical to what the writer produced. The default transport is
//! stdio (stdout carries only protocol frames; logging goes to stderr);
//! `--listen` adds a stateless Streamable HTTP endpoint instead.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::warn;
use ventstream_config::{EngineConfig as EngineFileConfig, McpRuntimeConfig, ValueRef};
use ventstream_core::ShutdownToken;
use ventstream_joins::{Cardinality, JoinDefinition};
use ventstream_sinks::read::{SinkReader, SinkReaderConfig};
use ventstream_sinks::{MeilisearchIndexRouting, RedisKeyRouting};

use crate::{
    load_engine_config_from_env, load_engine_config_from_path, load_joins_yaml, load_sink_config,
    optional_usize_argument, repeated_argument_values, resolve_value_ref, SinkRuntimeConfig,
};

const DEFAULT_MAX_RESULTS: usize = 50;
const HARD_MAX_RESULTS: usize = 500;
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
const RESOURCE_URI_PREFIX: &str = "vs://targets/";
/// The writer-side sugar template for projection-target index routing.
const PROJECTION_TARGET_TEMPLATE: &str = "${header:ventstream.target.index}";
const OUTPUT_RELATION_TEMPLATE: &str = "${header:ventstream.cdc.relation}";
const BY_OUTPUT_RELATION_HINT: &str =
    "by_output_relation routing: pass explicit targets via --target <name> (repeated)";

/// Serve MCP until stdin EOF (stdio) or process termination (HTTP).
pub(crate) async fn run(argv: &[String]) -> Result<()> {
    // `generate-token` needs no config; handled before any loading.
    if argv.get(2).map(String::as_str) == Some("generate-token") {
        return generate_token(argv.iter().any(|arg| arg == "--hash"));
    }
    let options = McpOptions::parse(argv)?;
    let registry = build_registry(&options).await?;
    let keys = resolve_access_keys(&options, &registry)?;
    match options.http {
        // The argv path has no supervisor; the token only fires on failure.
        Some(http) => http::serve(registry, keys, http, ShutdownToken::new()).await,
        None => {
            let scope = stdio_scope(&options, &keys)?;
            serve(&registry, &scope).await
        }
    }
}

/// Entry point for the fleet-managed `mcp` role. The supervisor spawns
/// the engine with no argv: listen/auth/scoping come from `runtime.mcp`
/// in the engine config, readiness is served on the shared health
/// listener, and shutdown arrives through the caller's token (signal or
/// supervisor stdin).
pub(crate) async fn run_role(
    engine_config: Option<EngineFileConfig>,
    shutdown: ShutdownToken,
) -> Result<()> {
    let config = engine_config
        .ok_or_else(|| anyhow!("the mcp role requires an engine config (VS_ENGINE_CONFIG)"))?;
    let runtime = config
        .runtime
        .mcp
        .clone()
        .ok_or_else(|| anyhow!("the mcp role requires runtime.mcp"))?;
    let (options, http_options) = McpOptions::from_runtime(&runtime)?;

    // Health listener first: the fleet supervisor polls /readyz while the
    // registry and sink connections come up.
    let readiness = ventstream_core::ReadinessSignal::new();
    let mut health_handle = None;
    if let Some(listen) = crate::resolve_health_listen(Some(&config)) {
        let gate = crate::health::ReadinessGate::for_mcp(readiness.clone());
        // /metrics is best-effort, matching the other roles.
        let prometheus = match ventstream_telemetry::install_prometheus() {
            Ok(handle) => Some(handle),
            Err(err) => {
                warn!(error = %err, "Prometheus recorder unavailable; /metrics disabled");
                None
            }
        };
        let health_shutdown = shutdown.clone();
        health_handle = Some(tokio::spawn(async move {
            if let Err(err) = crate::health::run(listen, prometheus, gate, health_shutdown).await {
                tracing::error!(error = %err, "health server stopped; continuing without it");
            }
        }));
    }

    let registry = build_registry_from(&options, vec![Some(config)]).await?;
    let keys = resolve_access_keys(&options, &registry)?;
    // Ready = registry built. Degraded sinks stay listed and answer with
    // clean tool errors, so they must not hold readiness down.
    readiness.mark_ready();
    let result = http::serve(registry, keys, http_options, shutdown.clone()).await;
    shutdown.cancel();
    if let Some(handle) = health_handle {
        let _ = handle.await;
    }
    result
}

#[derive(Debug)]
struct McpOptions {
    configs: Vec<String>,
    allow_targets: Vec<String>,
    cli_targets: Vec<String>,
    max_results: usize,
    auth_token_ref: Option<String>,
    keys_ref: Option<String>,
    stdio_key: Option<String>,
    http: Option<http::HttpOptions>,
}

impl McpOptions {
    fn parse(argv: &[String]) -> Result<Self> {
        let max_results = optional_usize_argument(argv, "--max-results", DEFAULT_MAX_RESULTS)?;
        if max_results == 0 || max_results > HARD_MAX_RESULTS {
            return Err(anyhow!("--max-results must be 1 to {HARD_MAX_RESULTS}"));
        }
        let auth_token_ref = single_argument_value(argv, "--auth-token-ref")?;
        let keys_ref = single_argument_value(argv, "--keys-ref")?;
        if auth_token_ref.is_some() && keys_ref.is_some() {
            return Err(anyhow!(
                "--auth-token-ref and --keys-ref are mutually exclusive"
            ));
        }
        let http = http::HttpOptions::parse(argv)?;
        if let Some(http) = &http {
            validate_http_exposure(&http.listen, auth_token_ref.is_some() || keys_ref.is_some())?;
        }
        Ok(Self {
            configs: repeated_argument_values(argv, "--config")?,
            allow_targets: repeated_argument_values(argv, "--allow-target")?,
            cli_targets: repeated_argument_values(argv, "--target")?,
            max_results,
            auth_token_ref,
            keys_ref,
            stdio_key: single_argument_value(argv, "--stdio-key")?,
            http,
        })
    }
}

impl McpOptions {
    /// Build options from the engine config's `runtime.mcp` block (the
    /// fleet role path; no argv). Config validation already enforced the
    /// exposure rule, re-checked here for defense in depth.
    fn from_runtime(runtime: &McpRuntimeConfig) -> Result<(Self, http::HttpOptions)> {
        let listen: SocketAddr = runtime
            .listen
            .parse()
            .with_context(|| format!("runtime.mcp.listen `{}` is invalid", runtime.listen))?;
        let has_auth = runtime.auth_token_ref.is_some() || runtime.keys_ref.is_some();
        validate_http_exposure(&listen, has_auth)?;
        let options = Self {
            configs: Vec::new(),
            allow_targets: runtime.allow_targets.clone(),
            cli_targets: runtime.targets.clone(),
            max_results: DEFAULT_MAX_RESULTS,
            auth_token_ref: runtime.auth_token_ref.clone(),
            keys_ref: runtime.keys_ref.clone(),
            stdio_key: None,
            http: None,
        };
        let http_options = http::HttpOptions {
            listen,
            allow_origins: Vec::new(),
        };
        Ok((options, http_options))
    }
}

/// A non-loopback bind without bearer auth would expose every sink read
/// to the network — refuse to start.
fn validate_http_exposure(listen: &SocketAddr, has_auth: bool) -> Result<()> {
    if !listen.ip().is_loopback() && !has_auth {
        return Err(anyhow!(
            "refusing to serve MCP on non-loopback {listen} without --auth-token-ref or --keys-ref"
        ));
    }
    Ok(())
}

fn single_argument_value(argv: &[String], flag: &str) -> Result<Option<String>> {
    let mut values = repeated_argument_values(argv, flag)?;
    if values.len() > 1 {
        return Err(anyhow!("{flag} may only be supplied once"));
    }
    Ok(values.pop())
}

// -------------------------------------------------------------------------
// Access keys
// -------------------------------------------------------------------------

/// Targets a key may read. Applied after the server-level
/// `--allow-target` filter.
#[derive(Clone, Debug)]
enum KeyScope {
    All,
    Named(BTreeSet<String>),
}

impl KeyScope {
    fn allows(&self, target: &str) -> bool {
        match self {
            Self::All => true,
            Self::Named(targets) => targets.contains(target),
        }
    }
}

/// Full access, used when no keys are configured.
const OPEN_SCOPE: KeyScope = KeyScope::All;

/// One bearer credential. Only the token's sha256 digest is retained;
/// resolved plaintext is dropped at startup.
#[derive(Debug)]
struct AccessKey {
    name: String,
    digest: [u8; 32],
    scope: KeyScope,
}

/// YAML shape of the `--keys-ref` document.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysFile {
    keys: Vec<KeyEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEntry {
    name: String,
    token_ref: Option<String>,
    token_hash: Option<String>,
    targets: KeyTargets,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum KeyTargets {
    Keyword(String),
    List(Vec<String>),
}

/// Empty result means no auth is configured (dev loopback / stdio).
fn resolve_access_keys(options: &McpOptions, registry: &Registry) -> Result<Vec<AccessKey>> {
    if let Some(reference) = &options.auth_token_ref {
        let token = resolve_named_ref(reference, "--auth-token-ref")?;
        return Ok(vec![AccessKey {
            name: "default".to_owned(),
            digest: token_digest(&token),
            scope: KeyScope::All,
        }]);
    }
    let Some(reference) = &options.keys_ref else {
        return Ok(Vec::new());
    };
    let text = resolve_named_ref(reference, "--keys-ref")?;
    let known: Vec<&str> = registry
        .targets
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    parse_keys_file(&text, &known)
}

fn resolve_named_ref(reference: &str, flag: &str) -> Result<String> {
    let reference = ValueRef::parse(reference).map_err(|err| anyhow!("invalid {flag}: {err}"))?;
    resolve_value_ref(&reference)
}

/// Parse and validate the keys YAML against the registry's target names.
fn parse_keys_file(text: &str, known_targets: &[&str]) -> Result<Vec<AccessKey>> {
    let parsed: KeysFile = serde_yaml::from_str(text).context("parsing keys YAML")?;
    if parsed.keys.is_empty() {
        return Err(anyhow!("keys file must define at least one key"));
    }
    let mut names = BTreeSet::new();
    let mut keys = Vec::with_capacity(parsed.keys.len());
    for entry in parsed.keys {
        if entry.name.trim().is_empty() {
            return Err(anyhow!("key names must be non-empty"));
        }
        if !names.insert(entry.name.clone()) {
            return Err(anyhow!("duplicate key name `{}`", entry.name));
        }
        let digest = match (&entry.token_ref, &entry.token_hash) {
            (Some(reference), None) => {
                // Plaintext resolves once and is dropped here.
                token_digest(&resolve_named_ref(reference, "key token_ref")?)
            }
            (None, Some(hash)) => parse_token_hash(hash)
                .with_context(|| format!("key `{}` token_hash", entry.name))?,
            _ => {
                return Err(anyhow!(
                    "key `{}` must set exactly one of token_ref or token_hash",
                    entry.name
                ))
            }
        };
        let scope = key_scope(&entry.name, entry.targets, known_targets)?;
        keys.push(AccessKey {
            name: entry.name,
            digest,
            scope,
        });
    }
    Ok(keys)
}

fn key_scope(name: &str, targets: KeyTargets, known_targets: &[&str]) -> Result<KeyScope> {
    let list = match targets {
        KeyTargets::Keyword(keyword) if keyword == "all" => return Ok(KeyScope::All),
        KeyTargets::Keyword(other) => {
            return Err(anyhow!(
                "key `{name}` targets must be `all` or a list, got `{other}`"
            ))
        }
        KeyTargets::List(list) => list,
    };
    if list.is_empty() {
        return Err(anyhow!("key `{name}` targets list must be non-empty"));
    }
    for target in &list {
        if !known_targets.contains(&target.as_str()) {
            return Err(anyhow!("key `{name}` names unknown target `{target}`"));
        }
    }
    Ok(KeyScope::Named(list.into_iter().collect()))
}

fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Parse `sha256:<64 hex chars>` into a digest.
fn parse_token_hash(value: &str) -> Result<[u8; 32]> {
    let hex = value
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("token_hash must start with `sha256:`"))?;
    if hex.len() != 64 {
        return Err(anyhow!("token_hash must be 64 hex characters"));
    }
    let mut digest = [0u8; 32];
    for (byte, pair) in digest.iter_mut().zip(hex.as_bytes().chunks(2)) {
        let pair = std::str::from_utf8(pair).context("token_hash is not ASCII")?;
        *byte = u8::from_str_radix(pair, 16).context("token_hash is not hex")?;
    }
    Ok(digest)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Match a presented token against every key: the token is hashed once,
/// digests compare in constant time, and the loop never exits early.
fn verify_token<'a>(keys: &'a [AccessKey], token: &str) -> Option<&'a AccessKey> {
    let digest = token_digest(token);
    let mut matched: Option<&AccessKey> = None;
    for key in keys {
        let equal = constant_time_eq(&digest, &key.digest);
        if equal && matched.is_none() {
            matched = Some(key);
        }
    }
    matched
}

/// Length + byte fold without early return, so timing does not leak
/// the mismatch position. (`subtle` is not a workspace dependency.)
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for index in 0..a.len().max(b.len()) {
        let x = a.get(index).copied().unwrap_or(0);
        let y = b.get(index).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

/// stdio carries no bearer token: `--stdio-key` selects a key's scope;
/// without it a configured keys file still grants full access (warned).
fn stdio_scope(options: &McpOptions, keys: &[AccessKey]) -> Result<KeyScope> {
    if keys.is_empty() {
        return Ok(KeyScope::All);
    }
    match &options.stdio_key {
        Some(name) => keys
            .iter()
            .find(|key| key.name == *name)
            .map(|key| key.scope.clone())
            .ok_or_else(|| anyhow!("--stdio-key `{name}` is not defined in the keys file")),
        None => {
            warn!("keys file configured but no --stdio-key; stdio has full access");
            Ok(KeyScope::All)
        }
    }
}

/// `vsk_` + 43 base62 chars ≈ 256 bits from the OS rng.
fn new_token() -> String {
    use rand::Rng as _;
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::rngs::OsRng;
    let suffix: String = (0..43)
        .map(|_| {
            char::from(
                *ALPHABET
                    .get(rng.gen_range(0..ALPHABET.len()))
                    .unwrap_or(&b'0'),
            )
        })
        .collect();
    format!("vsk_{suffix}")
}

fn generate_token(with_hash: bool) -> Result<()> {
    let token = new_token();
    println!("{token}");
    if with_hash {
        println!("sha256:{}", hex_digest(&token_digest(&token)));
    }
    Ok(())
}

// -------------------------------------------------------------------------
// Target registry
// -------------------------------------------------------------------------

/// Read access used by the dispatcher — trait so tests inject a stub.
#[async_trait::async_trait]
trait TargetReader: Send + Sync {
    async fn get_document(&self, target: &str, doc_id: &str) -> Result<Option<Value>, String>;
    async fn search(
        &self,
        target: &str,
        query: &str,
        limit: usize,
        sort: Option<(String, bool)>,
    ) -> Result<Vec<Value>, String>;
    async fn scan(&self, target: &str, pattern: &str, limit: usize) -> Result<Vec<String>, String>;
}

struct SinkReaderAdapter(SinkReader);

#[async_trait::async_trait]
impl TargetReader for SinkReaderAdapter {
    async fn get_document(&self, target: &str, doc_id: &str) -> Result<Option<Value>, String> {
        self.0
            .get_document(target, doc_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn search(
        &self,
        target: &str,
        query: &str,
        limit: usize,
        sort: Option<(String, bool)>,
    ) -> Result<Vec<Value>, String> {
        self.0
            .search(target, query, limit, sort)
            .await
            .map_err(|err| err.to_string())
    }

    async fn scan(&self, target: &str, pattern: &str, limit: usize) -> Result<Vec<String>, String> {
        self.0
            .scan(target, pattern, limit)
            .await
            .map_err(|err| err.to_string())
    }
}

/// A degraded sink keeps its targets listed but rejects reads cleanly.
enum ReaderSlot {
    Ready(Arc<dyn TargetReader>),
    Degraded(String),
}

struct TargetEntry {
    name: String,
    kind: &'static str,
    shape: Value,
    /// Table string used in the canonical doc id for this target.
    ///
    /// Invariant: the joins engine stamps `ventstream.doc.id` as
    /// `doc_id(def.primary.table, components)` — it routes each primary
    /// event by `def.primary.table` and passes that same string into
    /// `doc_id_value`; the SQL-denormalize paths use `pd.primary_table`
    /// (also `def.primary.table`). `effective_name()` is never the doc-id
    /// table. `get_entity` must build ids from `primary.table` only.
    doc_table: Option<String>,
    reader: usize,
}

struct Registry {
    targets: Vec<TargetEntry>,
    readers: Vec<ReaderSlot>,
    max_results: usize,
}

impl Registry {
    /// Targets visible to one key's scope.
    fn visible<'a>(&'a self, scope: &'a KeyScope) -> impl Iterator<Item = &'a TargetEntry> {
        self.targets
            .iter()
            .filter(move |entry| scope.allows(&entry.name))
    }

    /// Out-of-scope targets take the same path as nonexistent ones, so
    /// scoped keys cannot probe for target existence.
    fn target<'a>(&'a self, scope: &'a KeyScope, name: &str) -> Result<&'a TargetEntry, String> {
        self.visible(scope)
            .find(|entry| entry.name == name)
            .ok_or_else(|| format!("unknown target `{name}`"))
    }

    fn reader(&self, entry: &TargetEntry) -> Result<&Arc<dyn TargetReader>, String> {
        match self.readers.get(entry.reader) {
            Some(ReaderSlot::Ready(reader)) => Ok(reader),
            Some(ReaderSlot::Degraded(error)) => Err(format!(
                "target `{}` is degraded — sink connection failed: {error}",
                entry.name
            )),
            None => Err(format!("target `{}` has no reader", entry.name)),
        }
    }

    fn clamp_limit(&self, requested: Option<u64>) -> usize {
        let requested =
            usize::try_from(requested.unwrap_or(self.max_results as u64)).unwrap_or(usize::MAX);
        requested.clamp(1, self.max_results)
    }
}

async fn build_registry(options: &McpOptions) -> Result<Registry> {
    let engine_configs = if options.configs.is_empty() {
        vec![load_engine_config_from_env()?]
    } else {
        options
            .configs
            .iter()
            .map(|path| load_engine_config_from_path(path).map(Some))
            .collect::<Result<Vec<_>>>()?
    };
    build_registry_from(options, engine_configs).await
}

/// Build the registry from already-loaded engine configs (shared by the
/// argv subcommand and the fleet role path).
async fn build_registry_from(
    options: &McpOptions,
    engine_configs: Vec<Option<EngineFileConfig>>,
) -> Result<Registry> {
    let mut targets: Vec<TargetEntry> = Vec::new();
    let mut reader_configs: Vec<SinkReaderConfig> = Vec::new();
    for engine_config in &engine_configs {
        let sink = load_sink_config(engine_config.as_ref())?;
        let (joins, _) = load_joins_yaml(None, engine_config.as_ref())?;
        let names = sink_targets(&sink, &joins, &options.cli_targets)?;
        let reader_index = reader_configs.len();
        for name in names {
            let join = joins
                .iter()
                .find(|definition| definition.target_index() == Some(name.as_str()));
            targets.push(TargetEntry {
                shape: document_shape(join),
                doc_table: join.map(|definition| definition.primary.table.clone()),
                name,
                kind: sink.kind(),
                reader: reader_index,
            });
        }
        reader_configs.push(reader_config(sink));
    }

    check_collisions(&targets)?;
    if !options.allow_targets.is_empty() {
        targets.retain(|entry| options.allow_targets.contains(&entry.name));
        if targets.is_empty() {
            return Err(anyhow!("no targets remain after --allow-target filtering"));
        }
    }
    let readers = connect_readers(reader_configs, &targets).await;
    Ok(Registry {
        targets,
        readers,
        max_results: options.max_results,
    })
}

fn reader_config(sink: SinkRuntimeConfig) -> SinkReaderConfig {
    match sink {
        SinkRuntimeConfig::Redis(config) => SinkReaderConfig::Redis(config),
        SinkRuntimeConfig::OpenSearch(config) => SinkReaderConfig::OpenSearch(config),
        SinkRuntimeConfig::Meilisearch(config) => SinkReaderConfig::Meilisearch(config),
    }
}

/// Eager connect, one reader per sink config. A failed connect degrades
/// that config's targets instead of failing the whole server.
async fn connect_readers(
    configs: Vec<SinkReaderConfig>,
    targets: &[TargetEntry],
) -> Vec<ReaderSlot> {
    let mut readers = Vec::with_capacity(configs.len());
    for (index, config) in configs.into_iter().enumerate() {
        if !targets.iter().any(|entry| entry.reader == index) {
            readers.push(ReaderSlot::Degraded("no targets use this sink".to_owned()));
            continue;
        }
        match SinkReader::connect(config).await {
            Ok(reader) => readers.push(ReaderSlot::Ready(Arc::new(SinkReaderAdapter(reader)))),
            Err(error) => {
                warn!(%error, "sink connection failed; its targets are degraded");
                readers.push(ReaderSlot::Degraded(error.to_string()));
            }
        }
    }
    readers
}

fn check_collisions(targets: &[TargetEntry]) -> Result<()> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for entry in targets {
        *counts.entry(entry.name.as_str()).or_default() += 1;
    }
    let collisions: Vec<&str> = counts
        .into_iter()
        .filter_map(|(name, count)| (count > 1).then_some(name))
        .collect();
    if collisions.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "duplicate target names across configs: {}",
        collisions.join(", ")
    ))
}

/// Enumerate the readable target names for one sink config.
fn sink_targets(
    sink: &SinkRuntimeConfig,
    joins: &[JoinDefinition],
    cli_targets: &[String],
) -> Result<Vec<String>> {
    match sink {
        SinkRuntimeConfig::Redis(config) => match &config.key_routing {
            RedisKeyRouting::Fixed(target) => Ok(vec![target.clone()]),
            RedisKeyRouting::Views(views) => {
                Ok(views.iter().map(|view| view.name.clone()).collect())
            }
            RedisKeyRouting::ByProjectionTarget => projection_targets(joins),
            RedisKeyRouting::ByOutputRelation => explicit_targets(cli_targets),
        },
        SinkRuntimeConfig::OpenSearch(config) => {
            let template = config.index_template.as_str();
            if template == PROJECTION_TARGET_TEMPLATE {
                return projection_targets(joins);
            }
            if template == OUTPUT_RELATION_TEMPLATE {
                return explicit_targets(cli_targets);
            }
            if !template.contains('$') && !template.contains('%') {
                return Ok(vec![template.to_owned()]);
            }
            Err(anyhow!(
                "index not derivable from template `{template}`; template routing unsupported for reads"
            ))
        }
        SinkRuntimeConfig::Meilisearch(config) => match &config.index_routing {
            MeilisearchIndexRouting::Fixed(index) => Ok(vec![index.clone()]),
            MeilisearchIndexRouting::ByProjectionTarget => projection_targets(joins),
            MeilisearchIndexRouting::ByOutputRelation => explicit_targets(cli_targets),
        },
    }
}

/// Mirror of `validate_projection_target_indexes`: every join must carry
/// `target.index`.
fn projection_targets(joins: &[JoinDefinition]) -> Result<Vec<String>> {
    if joins.is_empty() {
        return Err(anyhow!(
            "by_projection_target routing requires join definitions with target.index; none were loaded"
        ));
    }
    let missing: Vec<&str> = joins
        .iter()
        .filter(|definition| definition.target_index().is_none())
        .map(JoinDefinition::effective_name)
        .collect();
    if !missing.is_empty() {
        return Err(anyhow!(
            "by_projection_target routing requires target.index on every join definition (missing for: {})",
            missing.join(", ")
        ));
    }
    Ok(joins
        .iter()
        .filter_map(|definition| definition.target_index().map(str::to_owned))
        .collect())
}

fn explicit_targets(cli_targets: &[String]) -> Result<Vec<String>> {
    if cli_targets.is_empty() {
        return Err(anyhow!(BY_OUTPUT_RELATION_HINT));
    }
    Ok(cli_targets.to_vec())
}

/// JSON description of the composed-document shape for one target.
fn document_shape(join: Option<&JoinDefinition>) -> Value {
    let Some(definition) = join else {
        return json!({"description": "raw event document"});
    };
    let related: Vec<Value> = definition
        .related
        .iter()
        .map(|rel| {
            json!({
                "embed_as": rel.embed_as,
                "table": rel.table,
                "shape": match rel.cardinality {
                    Cardinality::One => "object",
                    Cardinality::Many => "array",
                },
                "fields": if rel.select.is_empty() {
                    json!("all columns")
                } else {
                    json!(rel.select)
                },
            })
        })
        .collect();
    json!({
        "primary": {
            "table": definition.primary.table,
            "pk": definition.primary.pk.columns(),
        },
        "related": related,
    })
}

// -------------------------------------------------------------------------
// JSON-RPC protocol
// -------------------------------------------------------------------------

async fn serve(registry: &Registry, scope: &KeyScope) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await.context("reading stdin")? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(line) {
            Ok(request) => dispatch(registry, scope, &request).await,
            Err(_) => Some(error_response(Value::Null, -32700, "parse error")),
        };
        if let Some(response) = response {
            let mut frame = serde_json::to_vec(&response).context("encoding response")?;
            frame.push(b'\n');
            stdout.write_all(&frame).await.context("writing stdout")?;
            stdout.flush().await.context("flushing stdout")?;
        }
    }
    Ok(())
}

/// Handle one request under one key scope. `None` means no response
/// (notifications).
async fn dispatch(registry: &Registry, scope: &KeyScope, request: &Value) -> Option<Value> {
    let id = request.get("id").cloned();
    let Some(method) = request.get("method").and_then(Value::as_str) else {
        return id.map(|id| error_response(id, -32600, "missing method"));
    };
    if method.starts_with("notifications/") {
        return None;
    }
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    // Requests without an id are notifications; never answer them.
    let id = id?;
    let response = match method {
        "initialize" => result_response(id, initialize_result(&params)),
        "ping" => result_response(id, json!({})),
        "tools/list" => result_response(id, json!({"tools": tool_definitions()})),
        "tools/call" => tools_call(registry, scope, id, &params).await,
        "resources/list" => result_response(id, resources_list(registry, scope)),
        "resources/read" => resources_read(registry, scope, id, &params),
        _ => error_response(id, -32601, &format!("method not found: {method}")),
    };
    Some(response)
}

fn initialize_result(params: &Value) -> Value {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    json!({
        "protocolVersion": protocol_version,
        "capabilities": {"tools": {}, "resources": {}},
        "serverInfo": {"name": "ventstream", "version": env!("CARGO_PKG_VERSION")},
    })
}

fn result_response(id: Value, result: Value) -> Value {
    let mut response = serde_json::Map::new();
    response.insert("jsonrpc".to_owned(), json!("2.0"));
    response.insert("id".to_owned(), id);
    response.insert("result".to_owned(), result);
    Value::Object(response)
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    let mut response = serde_json::Map::new();
    response.insert("jsonrpc".to_owned(), json!("2.0"));
    response.insert("id".to_owned(), id);
    response.insert(
        "error".to_owned(),
        json!({"code": code, "message": message}),
    );
    Value::Object(response)
}

fn tool_definitions() -> Value {
    let limit_schema = json!({"type": "integer", "minimum": 1});
    json!([
        {
            "name": "list_targets",
            "description": "List every readable target: name, sink kind, document shape.",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false},
        },
        {
            "name": "get_entity",
            "description": "Fetch one materialized document by target and primary key.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {"type": "string"},
                    "pk": {
                        "description": "Primary key. Composite keys pass an array of scalar components in pk-column order. For targets without a joins spec, pass the full canonical doc id string (e.g. `public.orders:[\"5\"]`) as the pk.",
                        "anyOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": ["string", "number", "boolean"]}},
                        ],
                    },
                },
                "required": ["target", "pk"],
                "additionalProperties": false,
            },
        },
        {
            "name": "search",
            "description": "Search one target. OpenSearch/Elasticsearch targets accept the FULL Lucene query_string syntax — field matches (status:shipped), boolean operators (AND/OR/NOT), and ranges with date math (total_cents:>5000, updated_at:[now-15m TO *]) — so filtered questions like 'recently updated orders' are answerable directly. Meilisearch targets treat the query as plain full-text keywords. Optional `sort` orders results before `limit` truncates; without it results come back in index order.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {"type": "string"},
                    "query": {
                        "type": "string",
                        "description": "OpenSearch/Elasticsearch: full Lucene query_string, e.g. `status:shipped AND total_cents:>5000` or `updated_at:[now-15m TO *]` (date math supported). Meilisearch: plain full-text keywords.",
                    },
                    "limit": limit_schema,
                    "sort": {
                        "type": "string",
                        "description": "`field`, `field:asc`, or `field:desc` (bare field = ascending). OpenSearch sorts any field (unknown fields are tolerated). Meilisearch requires the attribute to be declared sortable in the index settings.",
                    },
                },
                "required": ["target", "query"],
                "additionalProperties": false,
            },
        },
        {
            "name": "scan",
            "description": "Scan doc ids matching a glob pattern (Redis targets only).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {"type": "string"},
                    "pattern": {"type": "string"},
                    "limit": limit_schema,
                },
                "required": ["target", "pattern"],
                "additionalProperties": false,
            },
        },
    ])
}

async fn tools_call(registry: &Registry, scope: &KeyScope, id: Value, params: &Value) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error_response(id, -32602, "tools/call requires a tool name");
    };
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
    match call_tool(registry, scope, name, &arguments).await {
        Ok(payload) => {
            let text =
                serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
            result_response(id, json!({"content": [{"type": "text", "text": text}]}))
        }
        Err(ToolError::UnknownTool) => error_response(id, -32602, &format!("unknown tool: {name}")),
        Err(ToolError::Failed(message)) => result_response(
            id,
            json!({"content": [{"type": "text", "text": message}], "isError": true}),
        ),
    }
}

#[derive(Debug)]
enum ToolError {
    UnknownTool,
    Failed(String),
}

impl From<String> for ToolError {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

async fn call_tool(
    registry: &Registry,
    scope: &KeyScope,
    name: &str,
    args: &Value,
) -> Result<Value, ToolError> {
    match name {
        "list_targets" => Ok(list_targets(registry, scope)),
        "get_entity" => get_entity(registry, scope, args).await,
        "search" => search(registry, scope, args).await,
        "scan" => scan(registry, scope, args).await,
        _ => Err(ToolError::UnknownTool),
    }
}

fn list_targets(registry: &Registry, scope: &KeyScope) -> Value {
    let targets: Vec<Value> = registry
        .visible(scope)
        .map(|entry| {
            let degraded = matches!(
                registry.readers.get(entry.reader),
                Some(ReaderSlot::Degraded(_))
            );
            json!({
                "name": entry.name,
                "sink": entry.kind,
                "shape": entry.shape,
                "degraded": degraded,
            })
        })
        .collect();
    json!({"targets": targets})
}

fn required_str<'a>(args: &'a Value, field: &str) -> Result<&'a str, ToolError> {
    args.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::Failed(format!("`{field}` must be a string")))
}

async fn get_entity(
    registry: &Registry,
    scope: &KeyScope,
    args: &Value,
) -> Result<Value, ToolError> {
    let target = required_str(args, "target")?;
    let pk = args
        .get("pk")
        .ok_or_else(|| ToolError::Failed("`pk` is required".to_owned()))?;
    let entry = registry.target(scope, target)?;
    let doc_id = entity_doc_id(entry, pk)?;
    let reader = registry.reader(entry)?;
    match reader.get_document(&entry.name, &doc_id).await? {
        Some(document) => Ok(json!({"found": true, "doc_id": doc_id, "document": document})),
        None => Ok(json!({
            "found": false,
            "doc_id": doc_id,
            "message": format!("no document `{doc_id}` in target `{}`", entry.name),
        })),
    }
}

/// Build the canonical doc id for a pk. Joined targets stamp
/// `primary.table` (see [`TargetEntry::doc_table`]); targets without a
/// join definition accept a full doc id string verbatim, or fall back to
/// the target name as the table for array pks.
fn entity_doc_id(entry: &TargetEntry, pk: &Value) -> Result<String, ToolError> {
    if entry.doc_table.is_none() {
        if let Value::String(raw) = pk {
            return Ok(raw.clone());
        }
    }
    let components = pk_components(pk).map_err(ToolError::Failed)?;
    let table = entry.doc_table.as_deref().unwrap_or(entry.name.as_str());
    Ok(ventstream_core::doc_id::doc_id(table, &components))
}

/// Normalize pk input exactly like `ventstream_core::doc_id::component_text`:
/// strings pass through, numbers and bools use their JSON scalar text.
fn pk_components(pk: &Value) -> Result<Vec<String>, String> {
    let scalars: &[Value] = match pk {
        Value::Array(items) => items,
        Value::String(_) | Value::Number(_) | Value::Bool(_) => std::slice::from_ref(pk),
        _ => return Err("pk must be a string or an array of scalars".to_owned()),
    };
    if scalars.is_empty() {
        return Err("pk must not be empty".to_owned());
    }
    scalars
        .iter()
        .map(|value| match value {
            Value::Array(_) | Value::Object(_) | Value::Null => {
                Err("pk components must be scalars".to_owned())
            }
            other => Ok(ventstream_core::doc_id::component_text(other)),
        })
        .collect()
}

async fn search(registry: &Registry, scope: &KeyScope, args: &Value) -> Result<Value, ToolError> {
    let target = required_str(args, "target")?;
    let query = required_str(args, "query")?;
    let sort = args
        .get("sort")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| "`sort` must be a string".to_owned())
                .and_then(parse_sort)
        })
        .transpose()
        .map_err(ToolError::Failed)?;
    let entry = registry.target(scope, target)?;
    let limit = registry.clamp_limit(args.get("limit").and_then(Value::as_u64));
    let reader = registry.reader(entry)?;
    let hits = reader.search(&entry.name, query, limit, sort).await?;
    Ok(json!({"count": hits.len(), "hits": hits}))
}

/// Parse `field`, `field:asc`, or `field:desc` into `(field, descending)`.
fn parse_sort(sort: &str) -> Result<(String, bool), String> {
    let (field, descending) = match sort.rsplit_once(':') {
        None => (sort, false),
        Some((field, "asc")) => (field, false),
        Some((field, "desc")) => (field, true),
        Some(_) => {
            return Err(format!(
                "invalid sort `{sort}`: expected `field`, `field:asc`, or `field:desc`"
            ))
        }
    };
    if field.trim().is_empty() || field.trim() != field {
        return Err(format!(
            "invalid sort `{sort}`: field must be non-empty with no surrounding whitespace"
        ));
    }
    Ok((field.to_owned(), descending))
}

async fn scan(registry: &Registry, scope: &KeyScope, args: &Value) -> Result<Value, ToolError> {
    let target = required_str(args, "target")?;
    let pattern = required_str(args, "pattern")?;
    let entry = registry.target(scope, target)?;
    let limit = registry.clamp_limit(args.get("limit").and_then(Value::as_u64));
    let reader = registry.reader(entry)?;
    let doc_ids = reader.scan(&entry.name, pattern, limit).await?;
    Ok(json!({"count": doc_ids.len(), "doc_ids": doc_ids}))
}

fn resources_list(registry: &Registry, scope: &KeyScope) -> Value {
    let resources: Vec<Value> = registry
        .visible(scope)
        .map(|entry| {
            json!({
                "uri": format!("{RESOURCE_URI_PREFIX}{}", entry.name),
                "name": entry.name,
                "mimeType": "application/json",
            })
        })
        .collect();
    json!({"resources": resources})
}

fn resources_read(registry: &Registry, scope: &KeyScope, id: Value, params: &Value) -> Value {
    let Some(uri) = params.get("uri").and_then(Value::as_str) else {
        return error_response(id, -32602, "resources/read requires a uri");
    };
    let Some(name) = uri.strip_prefix(RESOURCE_URI_PREFIX) else {
        return error_response(id, -32602, &format!("unknown resource uri: {uri}"));
    };
    match registry.target(scope, name) {
        Ok(entry) => {
            let text = serde_json::to_string_pretty(&entry.shape)
                .unwrap_or_else(|_| entry.shape.to_string());
            result_response(
                id,
                json!({"contents": [{
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": text,
                }]}),
            )
        }
        Err(message) => error_response(id, -32602, &message),
    }
}

// -------------------------------------------------------------------------
// Streamable HTTP transport (stateless)
// -------------------------------------------------------------------------

mod http {
    //! Stateless MCP Streamable HTTP: one JSON-RPC message per POST, no
    //! session ids, no server-initiated SSE streams. Dispatch is shared
    //! with the stdio transport. Concurrency is safe: `dispatch` takes
    //! `&Registry`, and the Redis reader serializes commands behind its
    //! own connection `Mutex` (held per command / scan call, never across
    //! another lock — no deadlock path, only serialization).

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result};
    use axum::body::Bytes;
    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{header, HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use axum::Router;
    use serde_json::{json, Value};
    use tokio::net::TcpListener;
    use tracing::info;

    use super::{
        dispatch, error_response, single_argument_value, verify_token, AccessKey, Registry,
        OPEN_SCOPE,
    };
    use crate::repeated_argument_values;

    const MAX_BODY_BYTES: usize = 1024 * 1024;
    const DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

    #[derive(Debug)]
    pub(super) struct HttpOptions {
        pub(super) listen: SocketAddr,
        pub(super) allow_origins: Vec<String>,
    }

    impl HttpOptions {
        /// Exposure validation (auth required off-loopback) happens in
        /// `McpOptions::parse`, which sees the auth flags.
        pub(super) fn parse(argv: &[String]) -> Result<Option<Self>> {
            let Some(listen) = single_argument_value(argv, "--listen")? else {
                return Ok(None);
            };
            let listen: SocketAddr = listen
                .parse()
                .with_context(|| format!("--listen address `{listen}` is invalid"))?;
            Ok(Some(Self {
                listen,
                allow_origins: repeated_argument_values(argv, "--allow-origin")?,
            }))
        }
    }

    struct HttpState {
        registry: Registry,
        /// Empty = no auth required (loopback dev).
        keys: Vec<AccessKey>,
        allow_origins: Vec<String>,
    }

    pub(super) async fn serve(
        registry: Registry,
        keys: Vec<AccessKey>,
        options: HttpOptions,
        shutdown: ventstream_core::ShutdownToken,
    ) -> Result<()> {
        let listener = TcpListener::bind(options.listen)
            .await
            .with_context(|| format!("binding MCP http listener on {}", options.listen))?;
        info!(listen = %options.listen, "mcp http server listening");
        let app = router(Arc::new(HttpState {
            registry,
            keys,
            allow_origins: options.allow_origins,
        }));
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .context("serving MCP http")
    }

    fn router(state: Arc<HttpState>) -> Router {
        Router::new()
            .route("/mcp", post(mcp_post).get(mcp_get))
            .route("/healthz", get(healthz))
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
            .with_state(state)
    }

    /// Unauthenticated liveness for load balancers.
    async fn healthz(State(state): State<Arc<HttpState>>) -> Response {
        json_response(
            StatusCode::OK,
            &json!({"status": "ok", "targets": state.registry.targets.len()}),
        )
    }

    /// No server-initiated SSE streams are offered; stateless POST only.
    async fn mcp_get() -> Response {
        (StatusCode::METHOD_NOT_ALLOWED, "POST one JSON-RPC message").into_response()
    }

    async fn mcp_post(
        State(state): State<Arc<HttpState>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let started = Instant::now();
        let labels = request_labels(&body);
        let (response, key) = mcp_post_inner(&state, &headers, &body).await;
        // The bearer token is never logged; only the matched key name.
        info!(
            status = response.status().as_u16(),
            method = %labels.method,
            tool = %labels.tool,
            key = %key.unwrap_or("-"),
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "mcp http request"
        );
        response
    }

    async fn mcp_post_inner<'a>(
        state: &'a HttpState,
        headers: &HeaderMap,
        body: &Bytes,
    ) -> (Response, Option<&'a str>) {
        let key = match authenticate(state, headers) {
            Ok(key) => key,
            Err(rejection) => return (*rejection, None),
        };
        let key_name = key.map(|key| key.name.as_str());
        let scope = key.map_or(&OPEN_SCOPE, |key| &key.scope);
        if let Some(rejection) = check_origin(state, headers) {
            return (rejection, key_name);
        }
        let request: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(_) => {
                return (
                    json_response(
                        StatusCode::BAD_REQUEST,
                        &error_response(Value::Null, -32700, "parse error"),
                    ),
                    key_name,
                )
            }
        };
        // JSON-RPC batching was removed from recent MCP revisions.
        if request.is_array() {
            return (
                (
                    StatusCode::BAD_REQUEST,
                    "JSON-RPC batch arrays are not supported; send one message per request",
                )
                    .into_response(),
                key_name,
            );
        }
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let response = match tokio::time::timeout(
            DISPATCH_TIMEOUT,
            dispatch(&state.registry, scope, &request),
        )
        .await
        {
            Ok(Some(response)) => json_response(StatusCode::OK, &response),
            // Notification: accepted, nothing to return.
            Ok(None) => StatusCode::ACCEPTED.into_response(),
            Err(_) => json_response(
                StatusCode::OK,
                &error_response(id, -32603, "request timed out"),
            ),
        };
        (response, key_name)
    }

    /// `Ok(None)` = auth not configured; `Ok(Some(key))` = matched key.
    /// The rejection is boxed to keep the `Err` variant small.
    fn authenticate<'a>(
        state: &'a HttpState,
        headers: &HeaderMap,
    ) -> Result<Option<&'a AccessKey>, Box<Response>> {
        if state.keys.is_empty() {
            return Ok(None);
        }
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        match presented.and_then(|token| verify_token(&state.keys, token)) {
            Some(key) => Ok(Some(key)),
            None => Err(Box::new(
                (
                    StatusCode::UNAUTHORIZED,
                    [(header::WWW_AUTHENTICATE, "Bearer")],
                    "missing or invalid bearer token",
                )
                    .into_response(),
            )),
        }
    }

    /// MCP-spec DNS-rebinding protection: a browser-sent non-localhost
    /// Origin is rejected unless explicitly allow-listed.
    fn check_origin(state: &HttpState, headers: &HeaderMap) -> Option<Response> {
        let origin = headers
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())?;
        if origin_allowed(origin, &state.allow_origins) {
            return None;
        }
        Some((StatusCode::FORBIDDEN, "origin not allowed").into_response())
    }

    fn origin_allowed(origin: &str, allow_origins: &[String]) -> bool {
        if allow_origins.iter().any(|allowed| allowed == origin) {
            return true;
        }
        let Some((_, host_port)) = origin.split_once("://") else {
            return false;
        };
        ["localhost", "127.0.0.1", "[::1]"].iter().any(|host| {
            host_port == *host
                || host_port
                    .strip_prefix(host)
                    .is_some_and(|rest| rest.starts_with(':'))
        })
    }

    struct RequestLabels {
        method: String,
        tool: String,
    }

    fn request_labels(body: &Bytes) -> RequestLabels {
        let parsed: Option<Value> = serde_json::from_slice(body).ok();
        let method = parsed
            .as_ref()
            .and_then(|value| value.get("method"))
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_owned();
        let tool = parsed
            .as_ref()
            .filter(|_| method == "tools/call")
            .and_then(|value| value.pointer("/params/name"))
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_owned();
        RequestLabels { method, tool }
    }

    fn json_response(status: StatusCode, body: &Value) -> Response {
        (
            status,
            [(header::CONTENT_TYPE, "application/json")],
            body.to_string(),
        )
            .into_response()
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::panic,
        clippy::indexing_slicing
    )]
    pub(super) mod tests {
        use super::super::KeyScope;
        use super::*;

        fn state(auth_token: Option<&str>, allow_origins: &[&str]) -> Arc<HttpState> {
            // Two keys, so the 401 cases prove a token matching NO key
            // is rejected, not just a single-key mismatch.
            let keys = auth_token
                .map(|token| {
                    vec![
                        AccessKey {
                            name: "default".to_owned(),
                            digest: super::super::token_digest(token),
                            scope: KeyScope::All,
                        },
                        AccessKey {
                            name: "ops".to_owned(),
                            digest: super::super::token_digest("other-token"),
                            scope: KeyScope::All,
                        },
                    ]
                })
                .unwrap_or_default();
            Arc::new(HttpState {
                registry: crate::mcp::tests::stub_registry(),
                keys,
                allow_origins: allow_origins.iter().map(|s| (*s).to_owned()).collect(),
            })
        }

        async fn spawn(state: Arc<HttpState>) -> String {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("addr");
            tokio::spawn(async move {
                axum::serve(listener, router(state)).await.expect("serve");
            });
            format!("http://{addr}")
        }

        #[tokio::test]
        async fn initialize_round_trips_and_healthz_reports_targets() {
            let base = spawn(state(None, &[])).await;
            let client = reqwest::Client::new();
            let response = client
                .post(format!("{base}/mcp"))
                .body(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
                .send()
                .await
                .expect("post");
            assert_eq!(response.status(), 200);
            let body: Value = response.json().await.expect("json");
            assert_eq!(body["result"]["serverInfo"]["name"], "ventstream");

            let health: Value = client
                .get(format!("{base}/healthz"))
                .send()
                .await
                .expect("get")
                .json()
                .await
                .expect("json");
            assert_eq!(health, json!({"status": "ok", "targets": 2}));
        }

        #[tokio::test]
        async fn notifications_return_202_and_batches_are_rejected() {
            let base = spawn(state(None, &[])).await;
            let client = reqwest::Client::new();
            let response = client
                .post(format!("{base}/mcp"))
                .body(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                .send()
                .await
                .expect("post");
            assert_eq!(response.status(), 202);
            assert!(response.text().await.expect("body").is_empty());

            let batch = client
                .post(format!("{base}/mcp"))
                .body("[]")
                .send()
                .await
                .expect("post");
            assert_eq!(batch.status(), 400);
        }

        #[tokio::test]
        async fn missing_or_wrong_bearer_token_is_401() {
            let base = spawn(state(Some("secret-token"), &[])).await;
            let client = reqwest::Client::new();
            let init = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let unauthorized = client
                .post(format!("{base}/mcp"))
                .body(init)
                .send()
                .await
                .expect("post");
            assert_eq!(unauthorized.status(), 401);

            let wrong = client
                .post(format!("{base}/mcp"))
                .bearer_auth("wrong")
                .body(init)
                .send()
                .await
                .expect("post");
            assert_eq!(wrong.status(), 401);

            let ok = client
                .post(format!("{base}/mcp"))
                .bearer_auth("secret-token")
                .body(init)
                .send()
                .await
                .expect("post");
            assert_eq!(ok.status(), 200);
        }

        #[tokio::test]
        async fn non_localhost_origin_is_403_unless_allow_listed() {
            let base = spawn(state(None, &["https://agents.example.com"])).await;
            let client = reqwest::Client::new();
            let init = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
            let rejected = client
                .post(format!("{base}/mcp"))
                .header("Origin", "https://evil.example.com")
                .body(init)
                .send()
                .await
                .expect("post");
            assert_eq!(rejected.status(), 403);

            for origin in ["https://agents.example.com", "http://localhost:3000"] {
                let allowed = client
                    .post(format!("{base}/mcp"))
                    .header("Origin", origin)
                    .body(init)
                    .send()
                    .await
                    .expect("post");
                assert_eq!(allowed.status(), 200, "origin {origin}");
            }
        }

        #[tokio::test]
        async fn oversized_bodies_and_get_are_rejected() {
            let base = spawn(state(None, &[])).await;
            let client = reqwest::Client::new();
            let oversized = client
                .post(format!("{base}/mcp"))
                .body("x".repeat(MAX_BODY_BYTES + 1))
                .send()
                .await
                .expect("post");
            assert_eq!(oversized.status(), 413);

            let get = client.get(format!("{base}/mcp")).send().await.expect("get");
            assert_eq!(get.status(), 405);
        }

        #[test]
        fn non_loopback_listen_without_auth_is_refused() {
            let public: SocketAddr = "0.0.0.0:8790".parse().expect("addr");
            let loopback: SocketAddr = "127.0.0.1:8790".parse().expect("addr");
            let error = super::super::validate_http_exposure(&public, false).expect_err("refusal");
            assert!(error.to_string().contains("--keys-ref"));
            assert!(super::super::validate_http_exposure(&loopback, false).is_ok());
            assert!(super::super::validate_http_exposure(&public, true).is_ok());
        }
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

    struct StubReader;

    #[async_trait::async_trait]
    impl TargetReader for StubReader {
        async fn get_document(&self, _target: &str, doc_id: &str) -> Result<Option<Value>, String> {
            if doc_id == r#"public.orders:["5"]"# {
                Ok(Some(json!({"id": 5, "status": "open"})))
            } else {
                Ok(None)
            }
        }

        async fn search(
            &self,
            _target: &str,
            _query: &str,
            limit: usize,
            sort: Option<(String, bool)>,
        ) -> Result<Vec<Value>, String> {
            let sort = sort.map(|(field, descending)| json!([field, descending]));
            Ok(vec![json!({"limit": limit, "sort": sort})])
        }

        async fn scan(
            &self,
            _target: &str,
            _pattern: &str,
            _limit: usize,
        ) -> Result<Vec<String>, String> {
            Err("stub does not scan".to_owned())
        }
    }

    fn joins_fixture() -> Vec<JoinDefinition> {
        let yaml = r#"
- name: orders-view
  primary:
    table: public.orders
    pk: id
  related:
    - id: customer
      table: public.customers
      pk: id
      join_on: { from: customer_id, to: id }
      embed_as: customer
      select: [name, email]
  target:
    index: orders-view
- primary:
    table: public.invoices
    pk: id
  target:
    index: invoices
"#;
        serde_yaml::from_str(yaml).expect("joins fixture")
    }

    pub(super) fn stub_registry() -> Registry {
        let joins = joins_fixture();
        let targets = joins
            .iter()
            .map(|join| TargetEntry {
                name: join.target_index().expect("target index").to_owned(),
                kind: "opensearch",
                shape: document_shape(Some(join)),
                doc_table: Some(join.primary.table.clone()),
                reader: 0,
            })
            .collect();
        Registry {
            targets,
            readers: vec![ReaderSlot::Ready(Arc::new(StubReader))],
            max_results: 25,
        }
    }

    #[tokio::test]
    async fn initialize_echoes_protocol_version_and_lists_tools() {
        let registry = stub_registry();
        let init = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2024-11-05"}}),
        )
        .await
        .expect("response");
        assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(init["result"]["serverInfo"]["name"], "ventstream");

        let tools = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
        )
        .await
        .expect("response");
        let names: Vec<&str> = tools["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .map(|tool| tool["name"].as_str().expect("name"))
            .collect();
        assert_eq!(names, ["list_targets", "get_entity", "search", "scan"]);
    }

    #[tokio::test]
    async fn notifications_and_unknown_methods_follow_jsonrpc() {
        let registry = stub_registry();
        assert!(dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
        .await
        .is_none());
        let unknown = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 3, "method": "no/such"}),
        )
        .await
        .expect("response");
        assert_eq!(unknown["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn get_entity_builds_doc_id_from_primary_table_and_finds_document() {
        let registry = stub_registry();
        let response = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                    "params": {"name": "get_entity",
                               "arguments": {"target": "orders-view", "pk": 5}}}),
        )
        .await
        .expect("response");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text");
        let payload: Value = serde_json::from_str(text).expect("payload");
        // Doc-id table is `primary.table`, never the join name.
        assert_eq!(payload["doc_id"], r#"public.orders:["5"]"#);
        assert_eq!(payload["found"], true);
        assert_eq!(payload["document"]["status"], "open");
    }

    #[test]
    fn integer_and_string_pks_normalize_identically() {
        let entry = stub_registry().targets.remove(0);
        let from_int = entity_doc_id(&entry, &json!(5)).expect("doc id");
        let from_text = entity_doc_id(&entry, &json!("5")).expect("doc id");
        let from_array = entity_doc_id(&entry, &json!([5])).expect("doc id");
        assert_eq!(from_int, from_text);
        assert_eq!(from_int, from_array);
        assert_eq!(from_int, r#"public.orders:["5"]"#);
        assert!(entity_doc_id(&entry, &json!([[5]])).is_err());
    }

    #[test]
    fn projection_targets_enumerate_join_fixture_and_require_index() {
        let joins = joins_fixture();
        assert_eq!(
            projection_targets(&joins).expect("targets"),
            ["orders-view", "invoices"]
        );
        let mut incomplete = joins;
        incomplete[1].target.index = None;
        let error = projection_targets(&incomplete).expect_err("missing index");
        assert!(error.to_string().contains("public.invoices"));
        assert!(projection_targets(&[]).is_err());
    }

    #[test]
    fn duplicate_target_names_are_a_startup_error() {
        let entry = |name: &str, reader: usize| TargetEntry {
            name: name.to_owned(),
            kind: "redis",
            shape: document_shape(None),
            doc_table: None,
            reader,
        };
        let error = check_collisions(&[
            entry("orders", 0),
            entry("orders", 1),
            entry("customers", 0),
        ])
        .expect_err("collision");
        assert!(error.to_string().contains("orders"));
        assert!(check_collisions(&[entry("orders", 0), entry("customers", 1)]).is_ok());
    }

    #[tokio::test]
    async fn search_limit_is_clamped_and_scan_errors_are_tool_errors() {
        let registry = stub_registry();
        let response = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                    "params": {"name": "search",
                               "arguments": {"target": "orders-view", "query": "x",
                                              "limit": 9999}}}),
        )
        .await
        .expect("response");
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text");
        let payload: Value = serde_json::from_str(text).expect("payload");
        assert_eq!(payload["hits"][0]["limit"], 25);

        let scan = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 6, "method": "tools/call",
                    "params": {"name": "scan",
                               "arguments": {"target": "orders-view", "pattern": "*"}}}),
        )
        .await
        .expect("response");
        assert_eq!(scan["result"]["isError"], true);
    }

    #[tokio::test]
    async fn resources_list_and_read_expose_target_shapes() {
        let registry = stub_registry();
        let list = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 7, "method": "resources/list"}),
        )
        .await
        .expect("response");
        assert_eq!(
            list["result"]["resources"][0]["uri"],
            "vs://targets/orders-view"
        );
        let read = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 8, "method": "resources/read",
                    "params": {"uri": "vs://targets/orders-view"}}),
        )
        .await
        .expect("response");
        let text = read["result"]["contents"][0]["text"]
            .as_str()
            .expect("text");
        let shape: Value = serde_json::from_str(text).expect("shape");
        assert_eq!(shape["primary"]["table"], "public.orders");
        assert_eq!(shape["related"][0]["shape"], "object");
    }

    const HASH_OF_X: &str =
        "sha256:2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";

    #[test]
    fn keys_file_validation_errors() {
        let known = ["orders-view", "invoices"];
        // Both token fields set.
        let both = format!(
            "keys:\n  - name: a\n    token_ref: env:T\n    token_hash: {HASH_OF_X}\n    targets: all\n"
        );
        let error = parse_keys_file(&both, &known).expect_err("both fields");
        assert!(error.to_string().contains("exactly one"));
        // Neither token field set.
        assert!(parse_keys_file("keys:\n  - name: a\n    targets: all\n", &known).is_err());
        // Unknown target.
        let unknown =
            format!("keys:\n  - name: a\n    token_hash: {HASH_OF_X}\n    targets: [nope]\n");
        let error = parse_keys_file(&unknown, &known).expect_err("unknown target");
        assert!(error.to_string().contains("unknown target `nope`"));
        // Duplicate names.
        let duplicate = format!(
            "keys:\n  - name: a\n    token_hash: {HASH_OF_X}\n    targets: all\n  \
             - name: a\n    token_hash: {HASH_OF_X}\n    targets: all\n"
        );
        let error = parse_keys_file(&duplicate, &known).expect_err("duplicate");
        assert!(error.to_string().contains("duplicate key name"));
        // Empty keys list and empty targets list.
        assert!(parse_keys_file("keys: []\n", &known).is_err());
        let empty_targets =
            format!("keys:\n  - name: a\n    token_hash: {HASH_OF_X}\n    targets: []\n");
        assert!(parse_keys_file(&empty_targets, &known).is_err());
        // Valid file parses with the declared scopes.
        let valid = format!(
            "keys:\n  - name: support-bot\n    token_hash: {HASH_OF_X}\n    targets: [orders-view]\n  \
             - name: ops\n    token_hash: {HASH_OF_X}\n    targets: all\n"
        );
        let keys = parse_keys_file(&valid, &known).expect("valid keys");
        assert_eq!(keys.len(), 2);
        assert!(keys[0].scope.allows("orders-view"));
        assert!(!keys[0].scope.allows("invoices"));
        assert!(keys[1].scope.allows("invoices"));
    }

    #[test]
    fn auth_token_ref_and_keys_ref_are_mutually_exclusive() {
        let argv: Vec<String> = [
            "ventstream",
            "mcp",
            "--auth-token-ref",
            "env:A",
            "--keys-ref",
            "env:B",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        let error = McpOptions::parse(&argv).expect_err("exclusive");
        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[tokio::test]
    async fn scoped_key_filters_lists_and_masks_out_of_scope_targets() {
        let registry = stub_registry();
        let scope = KeyScope::Named(["orders-view".to_owned()].into_iter().collect());
        let list = dispatch(
            &registry,
            &scope,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "list_targets", "arguments": {}}}),
        )
        .await
        .expect("response");
        let text = list["result"]["content"][0]["text"].as_str().expect("text");
        let payload: Value = serde_json::from_str(text).expect("payload");
        let names: Vec<&str> = payload["targets"]
            .as_array()
            .expect("targets")
            .iter()
            .map(|target| target["name"].as_str().expect("name"))
            .collect();
        assert_eq!(names, ["orders-view"]);

        let resources = dispatch(
            &registry,
            &scope,
            &json!({"jsonrpc": "2.0", "id": 2, "method": "resources/list"}),
        )
        .await
        .expect("response");
        assert_eq!(
            resources["result"]["resources"]
                .as_array()
                .expect("r")
                .len(),
            1
        );

        // Out-of-scope error is byte-identical to the truly-unknown error.
        let entity_error = |target: &str| {
            let registry = stub_registry();
            let scope = scope.clone();
            let target = target.to_owned();
            async move {
                let response = dispatch(
                    &registry,
                    &scope,
                    &json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                            "params": {"name": "get_entity",
                                       "arguments": {"target": target, "pk": "1"}}}),
                )
                .await
                .expect("response");
                assert_eq!(response["result"]["isError"], true);
                response["result"]["content"][0]["text"]
                    .as_str()
                    .expect("text")
                    .to_owned()
            }
        };
        let out_of_scope = entity_error("invoices").await;
        let truly_unknown = entity_error("missing").await;
        assert_eq!(out_of_scope, "unknown target `invoices`");
        assert_eq!(truly_unknown, "unknown target `missing`");
        assert_eq!(
            out_of_scope.replace("invoices", "missing"),
            truly_unknown,
            "out-of-scope and unknown targets must share one error shape"
        );
    }

    #[test]
    fn token_hash_round_trips_through_generate_and_verify() {
        let token = new_token();
        let hash = format!("sha256:{}", hex_digest(&token_digest(&token)));
        let keys = vec![AccessKey {
            name: "gen".to_owned(),
            digest: parse_token_hash(&hash).expect("hash"),
            scope: KeyScope::All,
        }];
        assert_eq!(
            verify_token(&keys, &token).map(|key| key.name.as_str()),
            Some("gen")
        );
        assert!(verify_token(&keys, "vsk_wrong").is_none());
        assert!(parse_token_hash("sha256:zz").is_err());
        assert!(parse_token_hash("md5:00").is_err());
    }

    #[test]
    fn generated_tokens_have_prefix_length_and_charset() {
        let token = new_token();
        assert!(token.starts_with("vsk_"));
        assert_eq!(token.len(), 47);
        assert!(token
            .strip_prefix("vsk_")
            .expect("prefix")
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric()));
        assert_ne!(new_token(), token);
    }

    #[test]
    fn token_compare_does_not_early_return_on_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"a"));
    }

    #[test]
    fn stdio_scope_selects_key_or_defaults_to_full_access() {
        let options = |stdio_key: Option<&str>| McpOptions {
            configs: Vec::new(),
            allow_targets: Vec::new(),
            cli_targets: Vec::new(),
            max_results: 50,
            auth_token_ref: None,
            keys_ref: None,
            stdio_key: stdio_key.map(str::to_owned),
            http: None,
        };
        let keys = vec![AccessKey {
            name: "support-bot".to_owned(),
            digest: [0; 32],
            scope: KeyScope::Named(["orders-view".to_owned()].into_iter().collect()),
        }];
        assert!(matches!(
            stdio_scope(&options(None), &[]).expect("open"),
            KeyScope::All
        ));
        let scoped = stdio_scope(&options(Some("support-bot")), &keys).expect("scoped");
        assert!(scoped.allows("orders-view"));
        assert!(!scoped.allows("invoices"));
        assert!(stdio_scope(&options(Some("nope")), &keys).is_err());
        assert!(matches!(
            stdio_scope(&options(None), &keys).expect("warned full access"),
            KeyScope::All
        ));
    }

    #[tokio::test]
    async fn mcp_role_config_converts_builds_registry_and_answers_initialize() {
        // Endpoint ref resolves from the environment like a fleet deploy.
        std::env::set_var("VS_TEST_MCP_ROLE_OS_ENDPOINT", "http://127.0.0.1:19200");
        let config = EngineFileConfig::from_yaml_str(
            "schema_version: 1\nroles: [mcp]\nsink:\n  kind: opensearch\n  opensearch:\n    endpoint_ref: env:VS_TEST_MCP_ROLE_OS_ENDPOINT\n    index_routing:\n      strategy: fixed\n      name: orders\nruntime:\n  mcp:\n    listen: 127.0.0.1:0\n    allow_targets: [orders]\n",
        )
        .expect("engine config");
        let runtime = config.runtime.mcp.clone().expect("runtime.mcp");
        let (options, http_options) = McpOptions::from_runtime(&runtime).expect("options");
        assert!(http_options.listen.ip().is_loopback());
        assert_eq!(options.allow_targets, ["orders"]);

        let registry = build_registry_from(&options, vec![Some(config)])
            .await
            .expect("registry");
        let names: Vec<&str> = registry
            .targets
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert_eq!(names, ["orders"]);
        assert!(resolve_access_keys(&options, &registry)
            .expect("keys")
            .is_empty());

        let init = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
        .await
        .expect("response");
        assert_eq!(init["result"]["serverInfo"]["name"], "ventstream");
    }

    #[test]
    fn non_loopback_runtime_mcp_without_auth_is_refused_at_conversion() {
        let runtime = McpRuntimeConfig {
            listen: "0.0.0.0:8790".to_owned(),
            auth_token_ref: None,
            keys_ref: None,
            allow_targets: Vec::new(),
            targets: Vec::new(),
        };
        assert!(McpOptions::from_runtime(&runtime).is_err());
    }

    #[test]
    fn sort_strings_parse_with_asc_default_and_reject_garbage() {
        assert_eq!(
            parse_sort("updated_at").expect("bare"),
            ("updated_at".to_owned(), false)
        );
        assert_eq!(
            parse_sort("updated_at:asc").expect("asc"),
            ("updated_at".to_owned(), false)
        );
        assert_eq!(
            parse_sort("updated_at:desc").expect("desc"),
            ("updated_at".to_owned(), true)
        );
        assert!(parse_sort("updated_at:sideways").is_err());
        assert!(parse_sort(":desc").is_err());
        assert!(parse_sort(" spaced :asc").is_err());
    }

    #[tokio::test]
    async fn search_tool_forwards_sort_and_rejects_bad_sort() {
        let registry = stub_registry();
        let call = |sort: Value| {
            let registry = stub_registry();
            async move {
                dispatch(
                    &registry,
                    &OPEN_SCOPE,
                    &json!({"jsonrpc": "2.0", "id": 9, "method": "tools/call",
                            "params": {"name": "search",
                                       "arguments": {"target": "orders-view", "query": "x",
                                                      "sort": sort}}}),
                )
                .await
                .expect("response")
            }
        };
        let ok = call(json!("updated_at:desc")).await;
        let text = ok["result"]["content"][0]["text"].as_str().expect("text");
        let payload: Value = serde_json::from_str(text).expect("payload");
        assert_eq!(payload["hits"][0]["sort"], json!(["updated_at", true]));

        let bad = call(json!("updated_at:sideways")).await;
        assert_eq!(bad["result"]["isError"], true);
        drop(registry);
    }

    #[tokio::test]
    async fn search_tool_schema_advertises_lucene_query_string_and_sort() {
        let registry = stub_registry();
        let tools = dispatch(
            &registry,
            &OPEN_SCOPE,
            &json!({"jsonrpc": "2.0", "id": 10, "method": "tools/list"}),
        )
        .await
        .expect("response");
        let text = tools["result"].to_string();
        // Agents discover capability from these strings — keep them present.
        assert!(text.contains("Lucene"));
        assert!(text.contains("query_string"));
        assert!(text.contains("now-15m"));
        assert!(text.contains("sortable"));
        assert!(text.contains("canonical doc id"));
    }
}
