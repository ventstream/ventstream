use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use redis::ErrorKind;
#[cfg(test)]
use redis::ServerErrorKind;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::debug;
use ventstream_core::{
    Event, FailedItem, ShutdownToken, Sink, SinkBatch, SinkError, SinkFailureGuard, SinkHealth,
    SinkHealthSnapshot,
};

use super::adaptive::{RedisAdaptiveController, RedisOperationLimits};
use super::capability::{
    read_view_schema_metadata, replace_view_schema_metadata, validate_document_capability,
    validate_or_initialize_view_schema, validate_view_capabilities,
};
use super::config::{
    RedisAcknowledgement, RedisConfig, RedisDocumentFormat, RedisKeyRouting,
    RedisKeyspaceOwnership, RedisTopology,
};
#[cfg(test)]
use super::config::{RedisContract, RedisTlsConfig};
use super::contract::{duration_millis, redis_operation_ttl, view_redis_operation};
use super::error::{
    classify_error, is_authentication_failure, is_capacity_pressure, record_topology_event,
    RedisFailure,
};
use super::keyspace::{
    clear_pattern_len, direct_clear_patterns, encoded_key_len, is_delete_event, is_truncate_event,
    key_from_parts, key_parts, manifest_key, owner_key, staging_key, staging_key_from_parts,
    staging_key_len, truncate_target, uses_json_cache_staging, validate_target_segment,
    version_key, version_key_len, view_clear_patterns, DOC_ID_HEADER,
};
#[cfg(test)]
use super::keyspace::{
    writer_fence_key, writer_lineage_key, PROJECTION_TARGET_HEADER, RELATION_HEADER,
    TARGET_CLEAR_HEADER,
};
use super::ownership::{
    acquire_writer_fence, renew_writer_fence, start_writer_heartbeat, WriterFence, WriterFenceMap,
};
use super::script::RedisScript;
#[cfg(test)]
use super::topology::validate_cluster_slot_owner;
use super::topology::{
    build_connector_with_revision, cluster_slot_retry_error, connect_raw, credential_revision,
    RedisConnection, RedisCredentialRevision,
};
use super::view::{CompiledViews, ViewEstimateLimits, ViewMaterialization};
use crate::opensearch::retry::BackoffSchedule;

const MAX_PIPELINE_COMMANDS: usize = 1_000;
const TARGET_LANE_COUNT: usize = 64;
const CONNECTION_POOL_SIZE: usize = 8;
const ORDERED_DISPATCH_CONCURRENCY: usize = 1;
const CREDENTIAL_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const LSN_HEADER: &str = "ventstream.cdc.lsn";
const TX_ID_HEADER: &str = "ventstream.cdc.tx_id";
const SOURCE_VERSION_HEADER: &str = "ventstream.cdc.source_version";

/// Redis materialization sink.
pub struct RedisSink {
    config: RedisConfig,
    views: Option<CompiledViews>,
    enforce_view_schema: bool,
    connections: Vec<Mutex<RedisConnectionState>>,
    target_lanes: Vec<Mutex<()>>,
    writer_fences: WriterFenceMap,
    lifetime: Arc<()>,
    adaptive: RedisAdaptiveController,
    delivery_health: SinkHealth,
}

struct RedisConnectionState {
    connection: Option<RedisConnection>,
    credential_revision: Option<RedisCredentialRevision>,
    next_credential_check: Instant,
}

struct ConnectedRedis {
    connection: RedisConnection,
    credential_revision: RedisCredentialRevision,
}

struct PreparedBatch {
    operations: Vec<PreparedOperation>,
    rejected: Vec<FailedItem>,
}

enum PreparedOperation {
    Pipeline(PreparedPipeline),
    ViewPipeline(PreparedViewPipeline),
    ClearTarget(PreparedClearTarget),
}

#[derive(Clone)]
struct PreparedPipeline {
    command_count: usize,
    commands: Vec<PreparedCommand>,
    upserts: usize,
    deletes: usize,
    encoded_bytes: usize,
}

#[derive(Clone)]
struct PreparedCommand {
    event_offset: usize,
    kind: PreparedCommandKind,
    target: String,
    key: String,
    version_key: String,
    source_version: Option<String>,
    staging_key: Option<String>,
    payload: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreparedCommandKind {
    Upsert,
    Delete,
}

struct PreparedClearTarget {
    target: String,
    patterns: Vec<String>,
}

struct PreparedViewPipeline {
    commands: Vec<PreparedViewCommand>,
    upserts: usize,
    removals: usize,
    encoded_bytes: usize,
}

struct PreparedViewCommand {
    target: String,
    source_id: String,
    source_version: Option<String>,
    manifest_key: String,
    desired: Option<PreparedViewEntry>,
}

struct PreparedViewEntry {
    key: String,
    owner_key: String,
    staging_key: Option<String>,
    payload: Bytes,
}

struct PreparedPipelineBuilder {
    command_count: usize,
    commands: Vec<PreparedCommand>,
    upserts: usize,
    deletes: usize,
    encoded_bytes: usize,
}

struct PreparedViewPipelineBuilder {
    commands: Vec<PreparedViewCommand>,
    upserts: usize,
    removals: usize,
    encoded_bytes: usize,
}

impl PreparedPipelineBuilder {
    fn new() -> Self {
        Self {
            command_count: 0,
            commands: Vec::new(),
            upserts: 0,
            deletes: 0,
            encoded_bytes: 0,
        }
    }

    fn flush_into(&mut self, operations: &mut Vec<PreparedOperation>) {
        if self.command_count == 0 {
            return;
        }
        operations.push(PreparedOperation::Pipeline(PreparedPipeline {
            command_count: std::mem::take(&mut self.command_count),
            commands: std::mem::take(&mut self.commands),
            upserts: std::mem::take(&mut self.upserts),
            deletes: std::mem::take(&mut self.deletes),
            encoded_bytes: std::mem::take(&mut self.encoded_bytes),
        }));
    }
}

impl PreparedViewPipelineBuilder {
    fn new() -> Self {
        Self {
            commands: Vec::new(),
            upserts: 0,
            removals: 0,
            encoded_bytes: 0,
        }
    }

    fn flush_into(&mut self, operations: &mut Vec<PreparedOperation>) {
        if self.commands.is_empty() {
            return;
        }
        operations.push(PreparedOperation::ViewPipeline(PreparedViewPipeline {
            commands: std::mem::take(&mut self.commands),
            upserts: std::mem::take(&mut self.upserts),
            removals: std::mem::take(&mut self.removals),
            encoded_bytes: std::mem::take(&mut self.encoded_bytes),
        }));
    }
}

impl RedisSink {
    /// Check live Redis connectivity and required capabilities without starting a data path.
    pub async fn diagnose(config: RedisConfig) -> Result<super::RedisDiagnosticReport, SinkError> {
        super::diagnostic::diagnose(&config).await
    }

    /// Inspect Redis keyspace consistency without claiming a writer or changing data.
    pub async fn inspect_drift(
        config: RedisConfig,
        targets: &[String],
        scan_limit: usize,
    ) -> Result<super::RedisDriftReport, SinkError> {
        super::drift::inspect(&config, targets, scan_limit).await
    }

    /// Validate configuration, connect, and verify the Redis endpoint.
    pub async fn connect(config: RedisConfig) -> Result<Self, SinkError> {
        Self::connect_with_shutdown(config, &ShutdownToken::new()).await
    }

    /// Connect while allowing the owning engine iteration to stop promptly.
    pub async fn connect_with_shutdown(
        config: RedisConfig,
        shutdown: &ShutdownToken,
    ) -> Result<Self, SinkError> {
        Self::connect_internal(config, shutdown, true).await
    }

    async fn connect_internal(
        config: RedisConfig,
        shutdown: &ShutdownToken,
        enforce_view_schema: bool,
    ) -> Result<Self, SinkError> {
        config.validate().map_err(SinkError::Blocked)?;
        let views = match &config.key_routing {
            RedisKeyRouting::Views(views) => {
                Some(CompiledViews::compile(views).map_err(SinkError::Blocked)?)
            }
            _ => None,
        };
        let delivery_health = config.delivery_health.clone().unwrap_or_default();
        let adaptive = RedisAdaptiveController::new(
            config.max_batch_bytes,
            MAX_PIPELINE_COMMANDS,
            ORDERED_DISPATCH_CONCURRENCY,
        );
        let mut schedule = BackoffSchedule::new(config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;

        loop {
            let result = tokio::select! {
                () = shutdown.cancelled() => {
                    return Err(SinkError::Connection(
                        "Redis sink startup cancelled".to_owned(),
                    ));
                }
                result = connect_once(&config, enforce_view_schema) => result,
            };
            match result {
                Ok(connected) => {
                    transient_failure.take();
                    ventstream_telemetry::mark_sink_available();
                    let ConnectedRedis {
                        connection,
                        credential_revision,
                    } = connected;
                    let mut initial_connection = Some(connection);
                    let sink = Self {
                        config,
                        views,
                        enforce_view_schema,
                        connections: (0..CONNECTION_POOL_SIZE)
                            .map(|index| {
                                Mutex::new(RedisConnectionState {
                                    connection: if index == 0 {
                                        initial_connection.take()
                                    } else {
                                        None
                                    },
                                    credential_revision: if index == 0 {
                                        Some(credential_revision)
                                    } else {
                                        None
                                    },
                                    next_credential_check: Instant::now()
                                        + CREDENTIAL_REFRESH_INTERVAL,
                                })
                            })
                            .collect(),
                        target_lanes: (0..TARGET_LANE_COUNT).map(|_| Mutex::new(())).collect(),
                        writer_fences: Arc::new(parking_lot::Mutex::new(HashMap::new())),
                        lifetime: Arc::new(()),
                        adaptive,
                        delivery_health,
                    };
                    start_writer_heartbeat(
                        sink.config.clone(),
                        Arc::clone(&sink.writer_fences),
                        sink.delivery_health.clone(),
                        shutdown.clone(),
                        Arc::downgrade(&sink.lifetime),
                    );
                    return Ok(sink);
                }
                Err(SinkError::Connection(reason)) => {
                    if transient_failure.is_none() {
                        transient_failure =
                            Some(delivery_health.begin_transient_failure(reason.clone()));
                        ventstream_telemetry::mark_sink_unavailable();
                    }
                    let Some(delay) = schedule.next() else {
                        return Err(SinkError::Connection(reason));
                    };
                    ventstream_telemetry::bump_sink_retries(1);
                    debug!(
                        sink_id = %config.id,
                        attempt = schedule.attempts_so_far(),
                        ?delay,
                        error = %reason,
                        "Redis connection failed; retrying"
                    );
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            return Err(SinkError::Connection(
                                "Redis sink startup cancelled".to_owned(),
                            ));
                        }
                        () = tokio::time::sleep(jittered_delay(delay)) => {}
                    }
                }
                Err(SinkError::Blocked(reason)) => {
                    delivery_health.mark_blocked(reason.clone());
                    ventstream_telemetry::mark_sink_unavailable();
                    return Err(SinkError::Blocked(reason));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn prepare(&self, batch: &SinkBatch) -> Result<PreparedBatch, SinkError> {
        let limits = self.adaptive.operation_limits();
        match &self.views {
            Some(views) => prepare_view_batch_with_limits(&self.config, views, batch, limits),
            None => prepare_batch_with_limits(&self.config, batch, limits),
        }
    }

    /// Clear a finite set of targets before an exclusive drain/rebuild.
    pub async fn reset_owned_targets(
        config: RedisConfig,
        targets: &[String],
    ) -> Result<(), SinkError> {
        config.validate().map_err(SinkError::Blocked)?;
        if config.keyspace_ownership != RedisKeyspaceOwnership::Exclusive {
            return Err(SinkError::Blocked(
                "Redis target reset requires sink.redis.keyspace.ownership=exclusive".to_owned(),
            ));
        }
        if targets.is_empty() {
            return Err(SinkError::Blocked(
                "Redis target reset requires at least one statically known target".to_owned(),
            ));
        }
        if targets.len() > 10_000 {
            return Err(SinkError::Blocked(
                "Redis target reset is limited to 10000 targets".to_owned(),
            ));
        }

        let view_routing = matches!(config.key_routing, RedisKeyRouting::Views(_));
        let mut unique = BTreeSet::new();
        for target in targets {
            validate_target_segment(target).map_err(SinkError::Blocked)?;
            unique.insert(target.clone());
        }

        let shutdown = ShutdownToken::new();
        let sink = Self::connect_internal(config, &shutdown, !view_routing).await?;
        if view_routing {
            let mut state = sink
                .connections
                .first()
                .ok_or_else(|| SinkError::Connection("Redis pool is empty".to_owned()))?
                .lock()
                .await;
            let connection = state.connection.as_mut().ok_or_else(|| {
                SinkError::Connection("Redis initial connection is unavailable".to_owned())
            })?;
            if let Some(previous) = read_view_schema_metadata(&sink.config, connection).await? {
                for target in previous.targets {
                    validate_target_segment(&target).map_err(|error| {
                        SinkError::Blocked(format!(
                            "stored Redis view schema contains an invalid target: {error}"
                        ))
                    })?;
                    unique.insert(target);
                }
            }
        }
        if unique.len() > 10_000 {
            return Err(SinkError::Blocked(
                "Redis target reset is limited to 10000 current and previous targets".to_owned(),
            ));
        }

        let mut prepared = Vec::with_capacity(unique.len());
        for target in unique {
            let pattern_len = clear_pattern_len(&sink.config.key_prefix, &target);
            if pattern_len > sink.config.max_key_bytes {
                return Err(SinkError::Blocked(format!(
                    "Redis reset target pattern is {pattern_len} bytes; max_key_bytes={}",
                    sink.config.max_key_bytes
                )));
            }
            prepared.push(PreparedOperation::ClearTarget(PreparedClearTarget {
                target: target.clone(),
                patterns: match &sink.config.key_routing {
                    RedisKeyRouting::Views(_) => view_clear_patterns(&sink.config, &target)?,
                    _ => direct_clear_patterns(&sink.config.key_prefix, &target, pattern_len),
                },
            }));
        }

        for operation in &prepared {
            sink.execute_with_retry(operation).await?;
        }
        if view_routing {
            let mut state = sink
                .connections
                .first()
                .ok_or_else(|| SinkError::Connection("Redis pool is empty".to_owned()))?
                .lock()
                .await;
            let connection = state.connection.as_mut().ok_or_else(|| {
                SinkError::Connection("Redis initial connection is unavailable".to_owned())
            })?;
            replace_view_schema_metadata(&sink.config, connection).await?;
        }
        Ok(())
    }

    async fn execute_with_retry(
        &self,
        operation: &PreparedOperation,
    ) -> Result<Vec<FailedItem>, SinkError> {
        let lane_indexes = operation_lane_indexes(operation, self.target_lanes.len());
        let mut lane_guards = Vec::with_capacity(lane_indexes.len());
        for index in &lane_indexes {
            let lane = self.target_lanes.get(*index).ok_or_else(|| {
                SinkError::Connection("Redis target lane is unavailable".to_owned())
            })?;
            lane_guards.push(lane.lock().await);
        }
        let pool_index = lane_indexes.first().copied().unwrap_or(0) % self.connections.len();
        let mut schedule = BackoffSchedule::new(self.config.retry);
        let mut transient_failure: Option<SinkFailureGuard> = None;
        let mut state = self
            .connections
            .get(pool_index)
            .ok_or_else(|| SinkError::Connection("Redis pool lane is unavailable".to_owned()))?
            .lock()
            .await;
        let mut reconnect_before_write = state.connection.is_none();
        let retry_weight = operation.retry_weight();

        loop {
            if self.config.has_rotating_credentials()
                && Instant::now() >= state.next_credential_check
            {
                state.next_credential_check = Instant::now() + CREDENTIAL_REFRESH_INTERVAL;
                match credential_revision(&self.config).await {
                    Ok(revision) if state.credential_revision != Some(revision) => {
                        state.connection = None;
                        reconnect_before_write = true;
                        ventstream_telemetry::record_redis_credential_reload(true);
                    }
                    Ok(_) => {}
                    Err(_) => {
                        state.connection = None;
                        reconnect_before_write = true;
                        ventstream_telemetry::record_redis_credential_reload(false);
                    }
                }
            }
            if reconnect_before_write {
                match connect_once(&self.config, self.enforce_view_schema).await {
                    Ok(replacement) => {
                        state.connection = Some(replacement.connection);
                        state.credential_revision = Some(replacement.credential_revision);
                    }
                    Err(SinkError::Connection(reason)) => {
                        self.adaptive.on_transient_failure(false);
                        if transient_failure.is_none() {
                            transient_failure =
                                Some(self.delivery_health.begin_transient_failure(reason.clone()));
                            ventstream_telemetry::mark_sink_unavailable();
                        }
                        let Some(delay) = schedule.next() else {
                            return Err(SinkError::Connection(reason));
                        };
                        ventstream_telemetry::bump_sink_retries(
                            u64::try_from(retry_weight).unwrap_or(u64::MAX),
                        );
                        debug!(
                            sink_id = %self.config.id,
                            attempt = schedule.attempts_so_far(),
                            ?delay,
                            error = %reason,
                            "Redis reconnection failed; retrying"
                        );
                        tokio::time::sleep(jittered_delay(delay)).await;
                        continue;
                    }
                    Err(SinkError::Blocked(reason)) => {
                        self.delivery_health.mark_blocked(reason.clone());
                        ventstream_telemetry::mark_sink_unavailable();
                        return Err(SinkError::Blocked(reason));
                    }
                    Err(error) => return Err(error),
                }
            }

            let started = Instant::now();
            let result = match state.connection.as_mut() {
                Some(connection) => self.execute_once(operation, connection).await,
                None => Err(redis::RedisError::from((
                    ErrorKind::Io,
                    "Redis pooled connection is unavailable",
                ))),
            };

            match result {
                Ok(outcome) => {
                    self.adaptive.on_success(started.elapsed());
                    transient_failure.take();
                    if matches!(self.delivery_health.snapshot(), SinkHealthSnapshot::Healthy) {
                        ventstream_telemetry::mark_sink_available();
                    }
                    match (operation, outcome) {
                        (
                            PreparedOperation::Pipeline(prepared),
                            RedisOperationOutcome::Pipeline {
                                acknowledgement,
                                rejected,
                                stale_skipped,
                            },
                        ) => {
                            let rejected_offsets = rejected
                                .iter()
                                .map(|item| item.offset)
                                .collect::<std::collections::BTreeSet<_>>();
                            let rejected_upserts = prepared
                                .commands
                                .iter()
                                .filter(|command| {
                                    command.kind == PreparedCommandKind::Upsert
                                        && rejected_offsets.contains(&command.event_offset)
                                })
                                .count();
                            let rejected_deletes = prepared
                                .commands
                                .iter()
                                .filter(|command| {
                                    command.kind == PreparedCommandKind::Delete
                                        && rejected_offsets.contains(&command.event_offset)
                                })
                                .count();
                            ventstream_telemetry::record_redis_pipeline(
                                u64::try_from(prepared.upserts.saturating_sub(rejected_upserts))
                                    .unwrap_or(u64::MAX),
                                u64::try_from(prepared.deletes.saturating_sub(rejected_deletes))
                                    .unwrap_or(u64::MAX),
                                u64::try_from(prepared.encoded_bytes).unwrap_or(u64::MAX),
                                started.elapsed(),
                                acknowledgement
                                    .replica_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                                acknowledgement
                                    .local_aof_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                            );
                            ventstream_telemetry::bump_redis_stale_writes_skipped(
                                u64::try_from(stale_skipped).unwrap_or(u64::MAX),
                            );
                            return Ok(rejected);
                        }
                        (
                            PreparedOperation::ClearTarget(clear),
                            RedisOperationOutcome::ClearTarget {
                                removed,
                                acknowledgement,
                            },
                        ) => {
                            ventstream_telemetry::record_redis_keyspace_clear(
                                u64::try_from(removed).unwrap_or(u64::MAX),
                                started.elapsed(),
                                acknowledgement
                                    .replica_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                                acknowledgement
                                    .local_aof_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                            );
                            debug!(
                                sink_id = %self.config.id,
                                target = %clear.target,
                                removed,
                                "Redis target cleared after truncate"
                            );
                        }
                        (
                            PreparedOperation::ViewPipeline(prepared),
                            RedisOperationOutcome::ViewPipeline {
                                acknowledgement,
                                stale_skipped,
                            },
                        ) => {
                            ventstream_telemetry::record_redis_pipeline(
                                u64::try_from(prepared.upserts).unwrap_or(u64::MAX),
                                u64::try_from(prepared.removals).unwrap_or(u64::MAX),
                                u64::try_from(prepared.encoded_bytes).unwrap_or(u64::MAX),
                                started.elapsed(),
                                acknowledgement
                                    .replica_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                                acknowledgement
                                    .local_aof_acks
                                    .and_then(|acks| u64::try_from(acks).ok()),
                            );
                            ventstream_telemetry::bump_redis_stale_writes_skipped(
                                u64::try_from(stale_skipped).unwrap_or(u64::MAX),
                            );
                            return Ok(Vec::new());
                        }
                        _ => {
                            return Err(SinkError::Blocked(
                                "Redis prepared operation returned an invalid outcome".to_owned(),
                            ));
                        }
                    }
                    return Ok(Vec::new());
                }
                Err(err)
                    if self.config.has_rotating_credentials()
                        && is_authentication_failure(&err) =>
                {
                    self.adaptive.on_transient_failure(false);
                    operation.record_failure();
                    if transient_failure.is_none() {
                        transient_failure = Some(self.delivery_health.begin_transient_failure(
                            "Redis authentication failed while using reloadable credentials",
                        ));
                        ventstream_telemetry::mark_sink_unavailable();
                    }
                    let Some(delay) = schedule.next() else {
                        return Err(SinkError::Connection(
                            "Redis authentication failed while using reloadable credentials"
                                .to_owned(),
                        ));
                    };
                    ventstream_telemetry::bump_sink_retries(
                        u64::try_from(retry_weight).unwrap_or(u64::MAX),
                    );
                    reconnect_before_write = true;
                    state.connection = None;
                    tokio::time::sleep(jittered_delay(delay)).await;
                }
                Err(err) => {
                    record_topology_event(&err, &self.config.topology);
                    match classify_error(&err, &self.config.topology) {
                        RedisFailure::Transient { reason, reconnect } => {
                            self.adaptive
                                .on_transient_failure(is_capacity_pressure(&err));
                            operation.record_failure();
                            if transient_failure.is_none() {
                                transient_failure = Some(
                                    self.delivery_health.begin_transient_failure(reason.clone()),
                                );
                                ventstream_telemetry::mark_sink_unavailable();
                            }
                            let Some(delay) = schedule.next() else {
                                return Err(SinkError::Connection(reason));
                            };
                            ventstream_telemetry::bump_sink_retries(
                                u64::try_from(retry_weight).unwrap_or(u64::MAX),
                            );
                            debug!(
                                sink_id = %self.config.id,
                                attempt = schedule.attempts_so_far(),
                                ?delay,
                                error = %err,
                                "Redis pipeline failed; retrying"
                            );
                            reconnect_before_write = reconnect;
                            tokio::time::sleep(jittered_delay(delay)).await;
                        }
                        RedisFailure::Blocked(reason) => {
                            operation.record_failure();
                            self.delivery_health.mark_blocked(reason.clone());
                            ventstream_telemetry::mark_sink_unavailable();
                            return Err(SinkError::Blocked(reason));
                        }
                    }
                }
            }
        }
    }

    async fn execute_once(
        &self,
        operation: &PreparedOperation,
        connection: &mut RedisConnection,
    ) -> redis::RedisResult<RedisOperationOutcome> {
        match operation {
            PreparedOperation::Pipeline(prepared) => {
                if matches!(self.config.topology, RedisTopology::Cluster { .. }) {
                    let mut rejected = Vec::new();
                    let mut stale_skipped = 0usize;
                    let mut acknowledgement = RedisAcknowledgementOutcome::default();
                    for target_pipeline in split_pipeline_by_target(prepared) {
                        let target = target_pipeline
                            .commands
                            .first()
                            .map(|command| command.target.as_str())
                            .ok_or_else(|| {
                                redis::RedisError::from((
                                    ErrorKind::Client,
                                    "Redis Cluster target pipeline was empty",
                                ))
                            })?;
                        let fence = self.ensure_writer_fence(connection, target).await?;
                        let execution = execute_fenced_pipeline(
                            connection,
                            &self.config,
                            &target_pipeline,
                            &[(target.to_owned(), fence.clone())],
                        )
                        .await?;
                        rejected.extend(execution.rejected);
                        stale_skipped = stale_skipped.saturating_add(execution.stale_skipped);
                        let observed = self.acknowledge(connection, &fence.current_key).await?;
                        acknowledgement = minimum_acknowledgements(acknowledgement, observed);
                    }
                    return Ok(RedisOperationOutcome::Pipeline {
                        acknowledgement,
                        rejected,
                        stale_skipped,
                    });
                }
                let mut fences = Vec::new();
                for command in &prepared.commands {
                    if fences
                        .iter()
                        .any(|(target, _): &(String, WriterFence)| target == &command.target)
                    {
                        continue;
                    }
                    let fence = self
                        .ensure_writer_fence(connection, &command.target)
                        .await?;
                    fences.push((command.target.clone(), fence));
                }
                let execution =
                    execute_fenced_pipeline(connection, &self.config, prepared, &fences).await?;
                let wait_key = fences
                    .first()
                    .map(|(_, fence)| fence.current_key.as_str())
                    .ok_or_else(|| {
                        redis::RedisError::from((
                            ErrorKind::Client,
                            "Redis pipeline had no writer fence",
                        ))
                    })?;
                let acknowledgement = self.acknowledge(connection, wait_key).await?;
                Ok(RedisOperationOutcome::Pipeline {
                    acknowledgement,
                    rejected: execution.rejected,
                    stale_skipped: execution.stale_skipped,
                })
            }
            PreparedOperation::ViewPipeline(prepared) => {
                let mut fences = Vec::new();
                for command in &prepared.commands {
                    if fences
                        .iter()
                        .any(|(target, _): &(String, WriterFence)| target == &command.target)
                    {
                        continue;
                    }
                    let fence = self
                        .ensure_writer_fence(connection, &command.target)
                        .await?;
                    fences.push((command.target.clone(), fence));
                }
                let stale_skipped =
                    execute_fenced_view_pipeline(connection, &self.config, prepared, &fences)
                        .await?;
                let wait_key = fences
                    .first()
                    .map(|(_, fence)| fence.current_key.as_str())
                    .ok_or_else(|| {
                        redis::RedisError::from((
                            ErrorKind::Client,
                            "Redis view pipeline had no writer fence",
                        ))
                    })?;
                let acknowledgement = self.acknowledge(connection, wait_key).await?;
                Ok(RedisOperationOutcome::ViewPipeline {
                    acknowledgement,
                    stale_skipped,
                })
            }
            PreparedOperation::ClearTarget(clear) => {
                let fence = self.ensure_writer_fence(connection, &clear.target).await?;
                let removed = clear_target_keys(connection, &clear.patterns, &fence).await?;
                let acknowledgement = self.acknowledge(connection, &fence.current_key).await?;
                Ok(RedisOperationOutcome::ClearTarget {
                    removed,
                    acknowledgement,
                })
            }
        }
    }

    async fn ensure_writer_fence(
        &self,
        connection: &mut RedisConnection,
        target: &str,
    ) -> redis::RedisResult<WriterFence> {
        let existing = { self.writer_fences.lock().get(target).cloned() };
        if let Some(fence) = existing {
            renew_writer_fence(
                connection,
                &fence,
                duration_millis(self.config.writer_lease),
            )
            .await?;
            return Ok(fence);
        }
        let fence = acquire_writer_fence(connection, &self.config, target).await?;
        let leased_targets = {
            let mut fences = self.writer_fences.lock();
            fences.insert(target.to_owned(), fence.clone());
            fences.len()
        };
        ventstream_telemetry::record_redis_writer_leased_targets(leased_targets);
        self.acknowledge(connection, &fence.current_key).await?;
        Ok(fence)
    }

    async fn acknowledge(
        &self,
        connection: &mut RedisConnection,
        key: &str,
    ) -> redis::RedisResult<RedisAcknowledgementOutcome> {
        let started = Instant::now();
        let mode = match self.config.acknowledgement {
            RedisAcknowledgement::Primary => "primary",
            RedisAcknowledgement::Replicated { .. } => "replicated",
            RedisAcknowledgement::Aof { .. } => "aof",
        };
        let result = self.acknowledge_inner(connection, key).await;
        ventstream_telemetry::record_redis_acknowledgement(mode, started.elapsed(), result.is_ok());
        result
    }

    async fn acknowledge_inner(
        &self,
        connection: &mut RedisConnection,
        key: &str,
    ) -> redis::RedisResult<RedisAcknowledgementOutcome> {
        match self.config.acknowledgement {
            RedisAcknowledgement::Primary => Ok(RedisAcknowledgementOutcome::default()),
            RedisAcknowledgement::Replicated { replicas, timeout } => {
                let mut command = redis::cmd("WAIT");
                command.arg(replicas).arg(duration_millis(timeout));
                let acknowledged = connection.query_on_primary::<usize>(command, key).await?;
                if acknowledged < replicas {
                    return Err(redis::RedisError::from((
                        ErrorKind::Io,
                        "Redis replica acknowledgement target was not met",
                        format!("required {replicas}, acknowledged {acknowledged}"),
                    )));
                }
                Ok(RedisAcknowledgementOutcome {
                    local_aof_acks: None,
                    replica_acks: Some(acknowledged),
                })
            }
            RedisAcknowledgement::Aof {
                local,
                replicas,
                timeout,
            } => {
                let required_local = usize::from(local);
                let mut command = redis::cmd("WAITAOF");
                command
                    .arg(required_local)
                    .arg(replicas)
                    .arg(duration_millis(timeout));
                let (local_acks, replica_acks) = connection
                    .query_on_primary::<(usize, usize)>(command, key)
                    .await?;
                if local_acks < required_local || replica_acks < replicas {
                    return Err(redis::RedisError::from((
                        ErrorKind::Io,
                        "Redis AOF acknowledgement target was not met",
                        format!(
                            "required local {required_local} and replicas {replicas}, acknowledged local {local_acks} and replicas {replica_acks}"
                        ),
                    )));
                }
                Ok(RedisAcknowledgementOutcome {
                    local_aof_acks: Some(local_acks),
                    replica_acks: Some(replica_acks),
                })
            }
        }
    }
}

enum RedisOperationOutcome {
    Pipeline {
        acknowledgement: RedisAcknowledgementOutcome,
        rejected: Vec<FailedItem>,
        stale_skipped: usize,
    },
    ViewPipeline {
        acknowledgement: RedisAcknowledgementOutcome,
        stale_skipped: usize,
    },
    ClearTarget {
        removed: usize,
        acknowledgement: RedisAcknowledgementOutcome,
    },
}

struct RedisPipelineExecution {
    rejected: Vec<FailedItem>,
    stale_skipped: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct RedisAcknowledgementOutcome {
    local_aof_acks: Option<usize>,
    replica_acks: Option<usize>,
}

fn minimum_acknowledgement(current: Option<usize>, next: Option<usize>) -> Option<usize> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.min(next)),
        (None, Some(next)) => Some(next),
        (current, None) => current,
    }
}

fn minimum_acknowledgements(
    current: RedisAcknowledgementOutcome,
    next: RedisAcknowledgementOutcome,
) -> RedisAcknowledgementOutcome {
    RedisAcknowledgementOutcome {
        local_aof_acks: minimum_acknowledgement(current.local_aof_acks, next.local_aof_acks),
        replica_acks: minimum_acknowledgement(current.replica_acks, next.replica_acks),
    }
}

fn split_pipeline_by_target(prepared: &PreparedPipeline) -> Vec<PreparedPipeline> {
    let mut by_target = BTreeMap::<String, Vec<PreparedCommand>>::new();
    for command in &prepared.commands {
        by_target
            .entry(command.target.clone())
            .or_default()
            .push(command.clone());
    }
    by_target
        .into_values()
        .map(|commands| {
            let upserts = commands
                .iter()
                .filter(|command| command.kind == PreparedCommandKind::Upsert)
                .count();
            let deletes = commands.len().saturating_sub(upserts);
            PreparedPipeline {
                command_count: commands.len(),
                commands,
                upserts,
                deletes,
                encoded_bytes: prepared.encoded_bytes,
            }
        })
        .collect()
}

fn operation_lane_indexes(operation: &PreparedOperation, lane_count: usize) -> Vec<usize> {
    let targets = match operation {
        PreparedOperation::Pipeline(prepared) => prepared
            .commands
            .iter()
            .map(|command| command.target.as_str())
            .collect::<BTreeSet<_>>(),
        PreparedOperation::ViewPipeline(prepared) => prepared
            .commands
            .iter()
            .map(|command| command.target.as_str())
            .collect::<BTreeSet<_>>(),
        PreparedOperation::ClearTarget(clear) => BTreeSet::from([clear.target.as_str()]),
    };
    let mut indexes = targets
        .into_iter()
        .map(|target| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            target.hash(&mut hasher);
            usize::try_from(hasher.finish()).unwrap_or(usize::MAX) % lane_count.max(1)
        })
        .collect::<Vec<_>>();
    indexes.sort_unstable();
    indexes.dedup();
    if indexes.is_empty() {
        indexes.push(0);
    }
    indexes
}

impl PreparedOperation {
    fn retry_weight(&self) -> usize {
        match self {
            Self::Pipeline(prepared) => prepared.command_count,
            Self::ViewPipeline(prepared) => prepared.commands.len(),
            Self::ClearTarget(_) => 1,
        }
    }

    fn record_failure(&self) {
        match self {
            Self::Pipeline(_) | Self::ViewPipeline(_) => {
                ventstream_telemetry::bump_redis_pipeline_failures();
            }
            Self::ClearTarget(_) => ventstream_telemetry::bump_redis_keyspace_clear_failures(),
        }
    }
}

async fn connect_once(
    config: &RedisConfig,
    enforce_view_schema: bool,
) -> Result<ConnectedRedis, SinkError> {
    let (connector, credential_revision) = build_connector_with_revision(config).await?;
    let mut connection = connect_raw(&connector, config).await?;
    validate_document_capability(config, &mut connection).await?;
    validate_view_capabilities(config, &mut connection).await?;
    if enforce_view_schema {
        validate_or_initialize_view_schema(config, &mut connection).await?;
    }
    Ok(ConnectedRedis {
        connection,
        credential_revision,
    })
}

#[cfg(test)]
fn prepare_batch(config: &RedisConfig, batch: &SinkBatch) -> Result<PreparedBatch, SinkError> {
    prepare_batch_with_limits(
        config,
        batch,
        RedisOperationLimits {
            max_batch_bytes: config.max_batch_bytes,
            max_commands: MAX_PIPELINE_COMMANDS,
        },
    )
}

fn prepare_batch_with_limits(
    config: &RedisConfig,
    batch: &SinkBatch,
    limits: RedisOperationLimits,
) -> Result<PreparedBatch, SinkError> {
    let mut builder = PreparedPipelineBuilder::new();
    let mut operations = Vec::new();
    let mut rejected = Vec::new();

    for (offset, event) in batch.events().iter().enumerate() {
        if is_truncate_event(event) {
            if config.keyspace_ownership != RedisKeyspaceOwnership::Exclusive {
                return Err(SinkError::Blocked(
                    "Redis target-wide truncate requires sink.redis.keyspace.ownership=exclusive"
                        .to_owned(),
                ));
            }
            builder.flush_into(&mut operations);
            let target = truncate_target(config, event)?;
            let pattern_len = clear_pattern_len(&config.key_prefix, target);
            if pattern_len > config.max_key_bytes {
                return Err(SinkError::Blocked(format!(
                    "Redis truncate target pattern is {pattern_len} bytes; max_key_bytes={}",
                    config.max_key_bytes
                )));
            }
            operations.push(PreparedOperation::ClearTarget(PreparedClearTarget {
                target: target.to_owned(),
                patterns: direct_clear_patterns(&config.key_prefix, target, pattern_len),
            }));
            continue;
        }

        let delete = is_delete_event(event);
        if !delete {
            if event.payload.as_slice().len() > config.max_value_bytes {
                rejected.push(FailedItem {
                    offset,
                    error: format!(
                        "Redis value is {} bytes; max_value_bytes={}",
                        event.payload.as_slice().len(),
                        config.max_value_bytes
                    ),
                });
                continue;
            }
            if config.document_format == RedisDocumentFormat::Json {
                if let Err(err) = validate_json_payload(event.payload.as_slice()) {
                    rejected.push(FailedItem {
                        offset,
                        error: format!("RedisJSON payload is not valid JSON: {err}"),
                    });
                    continue;
                }
            }
        }

        let (target, doc_id) = match key_parts(config, event) {
            Ok(parts) => parts,
            Err(error) => {
                rejected.push(FailedItem { offset, error });
                continue;
            }
        };
        let source_version = match durable_source_version(event) {
            Ok(version) => version,
            Err(error) => {
                rejected.push(FailedItem { offset, error });
                continue;
            }
        };
        let key_len = encoded_key_len(&config.key_prefix, target, doc_id);
        if key_len > config.max_key_bytes {
            rejected.push(FailedItem {
                offset,
                error: format!(
                    "Redis key is {} bytes; max_key_bytes={}",
                    key_len, config.max_key_bytes
                ),
            });
            continue;
        }
        let key = key_from_parts(&config.key_prefix, target, doc_id, key_len);
        let version_key = match version_key(config, target, doc_id) {
            Ok(key) => key,
            Err(error) => {
                rejected.push(FailedItem {
                    offset,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let staging_key = if !delete && uses_json_cache_staging(config) {
            let staging_key_len = staging_key_len(&config.key_prefix, target, doc_id);
            if staging_key_len > config.max_key_bytes {
                rejected.push(FailedItem {
                    offset,
                    error: format!(
                        "Redis staging key is {staging_key_len} bytes; max_key_bytes={}",
                        config.max_key_bytes
                    ),
                });
                continue;
            }
            Some(staging_key_from_parts(
                &config.key_prefix,
                target,
                doc_id,
                staging_key_len,
            ))
        } else {
            None
        };
        let payload_bytes = if delete {
            0
        } else {
            event.payload.as_slice().len()
        };
        let event_bytes = key
            .len()
            .saturating_add(version_key.len())
            .saturating_add(source_version.as_ref().map_or(0, String::len))
            .saturating_add(staging_key.as_ref().map_or(0, std::string::String::len))
            .saturating_add(payload_bytes)
            .saturating_add(96);
        if event_bytes > config.max_batch_bytes {
            rejected.push(FailedItem {
                offset,
                error: format!(
                    "Redis command is approximately {event_bytes} bytes; max_batch_bytes={}",
                    config.max_batch_bytes
                ),
            });
            continue;
        }
        if builder.command_count > 0
            && (builder.encoded_bytes.saturating_add(event_bytes) > limits.max_batch_bytes
                || builder.command_count >= limits.max_commands)
        {
            builder.flush_into(&mut operations);
        }

        if delete {
            builder.deletes = builder.deletes.saturating_add(1);
            builder.commands.push(PreparedCommand {
                event_offset: offset,
                kind: PreparedCommandKind::Delete,
                target: target.to_owned(),
                key,
                version_key,
                source_version,
                staging_key,
                payload: Bytes::new(),
            });
        } else {
            builder.upserts = builder.upserts.saturating_add(1);
            builder.commands.push(PreparedCommand {
                event_offset: offset,
                kind: PreparedCommandKind::Upsert,
                target: target.to_owned(),
                key,
                version_key,
                source_version,
                staging_key,
                payload: event.payload.as_bytes(),
            });
        }
        builder.command_count = builder.command_count.saturating_add(1);
        builder.encoded_bytes = builder.encoded_bytes.saturating_add(event_bytes);
    }

    builder.flush_into(&mut operations);

    Ok(PreparedBatch {
        operations,
        rejected,
    })
}

fn prepare_view_batch_with_limits(
    config: &RedisConfig,
    views: &CompiledViews,
    batch: &SinkBatch,
    limits: RedisOperationLimits,
) -> Result<PreparedBatch, SinkError> {
    let mut builder = PreparedViewPipelineBuilder::new();
    let mut operations = Vec::new();
    let mut prepared_bytes = 0usize;

    for event in batch.events() {
        let matching = views.matching(event).collect::<Vec<_>>();
        if is_truncate_event(event) {
            if matching.is_empty() {
                continue;
            }
            if config.keyspace_ownership != RedisKeyspaceOwnership::Exclusive {
                return Err(SinkError::Blocked(
                    "Redis view truncate requires sink.redis.keyspace.ownership=exclusive"
                        .to_owned(),
                ));
            }
            builder.flush_into(&mut operations);
            for view in matching {
                let patterns = view_clear_patterns(config, view.name())?;
                let clear_bytes = patterns
                    .iter()
                    .map(String::len)
                    .fold(192usize, usize::saturating_add);
                prepared_bytes = prepared_bytes.saturating_add(clear_bytes);
                if prepared_bytes > config.max_batch_bytes {
                    return Err(SinkError::Blocked(format!(
                        "Redis prepared view operations exceed max_batch_bytes={}",
                        config.max_batch_bytes
                    )));
                }
                operations.push(PreparedOperation::ClearTarget(PreparedClearTarget {
                    target: view.name().to_owned(),
                    patterns,
                }));
            }
            continue;
        }
        if matching.is_empty() {
            continue;
        }

        let source_id = match event.headers.get(DOC_ID_HEADER) {
            Some(source_id)
                if !source_id.is_empty() && !source_id.chars().any(char::is_control) =>
            {
                source_id
            }
            _ => {
                return Err(SinkError::Blocked(format!(
                    "event {} has an empty, missing, or control-character `{DOC_ID_HEADER}`; Redis views cannot advance without a stable source identity",
                    event.id
                )));
            }
        };
        let source_version = durable_source_version(event).map_err(SinkError::Blocked)?;
        let delete = is_delete_event(event);
        if !delete && event.payload.len() > config.max_value_bytes {
            return Err(SinkError::Blocked(format!(
                "Redis view source value is {} bytes; max_value_bytes={}; delivery cannot advance while an older materialization may remain visible",
                event.payload.len(),
                config.max_value_bytes
            )));
        }

        let requires_json = !delete
            && (config.document_format == RedisDocumentFormat::Json
                || matching.iter().any(|view| view.requires_json()));
        let document = if requires_json {
            match serde_json::from_slice::<serde_json::Value>(event.payload.as_slice()) {
                Ok(document) => Some(document),
                Err(error) => {
                    return Err(SinkError::Blocked(format!(
                        "Redis view payload is not valid JSON: {error}; delivery cannot advance while an older materialization may remain visible"
                    )));
                }
            }
        } else {
            None
        };

        let mut event_commands = Vec::with_capacity(matching.len());
        let mut event_bytes = 0usize;
        let mut event_upserts = 0usize;
        let mut event_removals = 0usize;

        for view in matching {
            let manifest_key = manifest_key(config, view.name(), source_id)?;
            let desired = if delete {
                None
            } else {
                let remaining_event_bytes = config.max_batch_bytes.saturating_sub(event_bytes);
                match view
                    .materialize(
                        event,
                        document.as_ref(),
                        config.max_key_bytes,
                        config.max_value_bytes.min(remaining_event_bytes),
                    )
                    .map_err(SinkError::Blocked)?
                {
                    ViewMaterialization::Remove => None,
                    ViewMaterialization::Upsert { key_id, payload } => {
                        if payload.len() > config.max_value_bytes {
                            return Err(SinkError::Blocked(format!(
                                "Redis view {} value is {} bytes; max_value_bytes={}",
                                view.name(),
                                payload.len(),
                                config.max_value_bytes
                            )));
                        }
                        let key_len = encoded_key_len(&config.key_prefix, view.name(), &key_id);
                        if key_len > config.max_key_bytes {
                            return Err(SinkError::Blocked(format!(
                                "Redis view {} key is {key_len} bytes; max_key_bytes={}",
                                view.name(),
                                config.max_key_bytes
                            )));
                        }
                        let key = key_from_parts(&config.key_prefix, view.name(), &key_id, key_len);
                        let owner_key = owner_key(config, view.name(), &key_id)?;
                        let staging_key = if uses_json_cache_staging(config) {
                            Some(staging_key(config, view.name(), &key_id)?)
                        } else {
                            None
                        };
                        Some(PreparedViewEntry {
                            key,
                            owner_key,
                            staging_key,
                            payload,
                        })
                    }
                }
            };

            let command_bytes = manifest_key
                .len()
                .saturating_add(source_id.len())
                .saturating_add(source_version.as_ref().map_or(0, String::len))
                .saturating_add(desired.as_ref().map_or(0, |entry| {
                    entry
                        .key
                        .len()
                        .saturating_add(entry.owner_key.len())
                        .saturating_add(
                            entry
                                .staging_key
                                .as_ref()
                                .map_or(0, std::string::String::len),
                        )
                        .saturating_add(entry.payload.len())
                }))
                .saturating_add(192);
            let next_event_bytes = event_bytes.saturating_add(command_bytes);
            if next_event_bytes > config.max_batch_bytes {
                return Err(SinkError::Blocked(format!(
                    "Redis view commands for one source event exceed max_batch_bytes={}",
                    config.max_batch_bytes
                )));
            }
            event_bytes = next_event_bytes;
            if desired.is_some() {
                event_upserts = event_upserts.saturating_add(1);
            } else {
                event_removals = event_removals.saturating_add(1);
            }
            event_commands.push(PreparedViewCommand {
                target: view.name().to_owned(),
                source_id: source_id.to_owned(),
                source_version: source_version.clone(),
                manifest_key,
                desired,
            });
        }

        if event_bytes > config.max_batch_bytes {
            return Err(SinkError::Blocked(format!(
                "Redis view commands for one source event are approximately {event_bytes} bytes; max_batch_bytes={}",
                config.max_batch_bytes
            )));
        }
        prepared_bytes = prepared_bytes.saturating_add(event_bytes);
        if prepared_bytes > config.max_batch_bytes {
            return Err(SinkError::Blocked(format!(
                "Redis prepared view operations exceed max_batch_bytes={}",
                config.max_batch_bytes
            )));
        }
        if !builder.commands.is_empty()
            && (builder.encoded_bytes.saturating_add(event_bytes) > limits.max_batch_bytes
                || builder.commands.len().saturating_add(event_commands.len())
                    > limits.max_commands)
        {
            builder.flush_into(&mut operations);
        }
        builder.commands.extend(event_commands);
        builder.upserts = builder.upserts.saturating_add(event_upserts);
        builder.removals = builder.removals.saturating_add(event_removals);
        builder.encoded_bytes = builder.encoded_bytes.saturating_add(event_bytes);
    }

    builder.flush_into(&mut operations);
    Ok(PreparedBatch {
        operations,
        rejected: Vec::new(),
    })
}

fn validate_json_payload(payload: &[u8]) -> serde_json::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    serde::de::IgnoredAny::deserialize(&mut deserializer)?;
    deserializer.end()
}

async fn verify_writer_fence(
    connection: &mut RedisConnection,
    fence: &WriterFence,
) -> redis::RedisResult<()> {
    const SOURCE: &str = "if redis.call('GET', KEYS[1]) ~= ARGV[1] then \
                 return redis.error_reply('VENTSTREAM_FENCED') \
             end \
             return 1";
    static SCRIPT: LazyLock<RedisScript> = LazyLock::new(|| RedisScript::new(SOURCE));
    SCRIPT
        .prepare_invoke()
        .key(&fence.current_key)
        .arg(&fence.value)
        .invoke::<usize>(connection, &fence.current_key)
        .await?;
    Ok(())
}

async fn clear_target_keys(
    connection: &mut RedisConnection,
    patterns: &[String],
    fence: &WriterFence,
) -> redis::RedisResult<usize> {
    const SCAN_COUNT: usize = 1_000;
    const MAX_UNLINK_KEYS: usize = 1_000;

    verify_writer_fence(connection, fence).await?;
    let initial_cluster_owner = connection
        .stable_cluster_slot_owner(&fence.current_key)
        .await?;
    let mut total_removed = 0usize;
    for pattern in patterns {
        loop {
            let mut cursor = 0u64;
            let mut pass_removed = 0usize;
            loop {
                let mut command = redis::cmd("SCAN");
                command
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(SCAN_COUNT);
                let (next_cursor, keys) = connection
                    .query_on_primary::<(u64, Vec<String>)>(command, &fence.current_key)
                    .await?;
                for chunk in keys.chunks(MAX_UNLINK_KEYS) {
                    const SOURCE: &str = "if redis.call('GET', KEYS[1]) ~= ARGV[1] then \
                                 return redis.error_reply('VENTSTREAM_FENCED') \
                             end \
                             return redis.call('UNLINK', unpack(KEYS, 2))";
                    static SCRIPT: LazyLock<RedisScript> =
                        LazyLock::new(|| RedisScript::new(SOURCE));
                    let mut invocation = SCRIPT.prepare_invoke();
                    invocation
                        .key(&fence.current_key)
                        .key(chunk)
                        .arg(&fence.value);
                    let removed = invocation
                        .invoke::<usize>(connection, &fence.current_key)
                        .await?;
                    pass_removed = pass_removed.saturating_add(removed);
                    total_removed = total_removed.saturating_add(removed);
                }
                cursor = next_cursor;
                if cursor == 0 {
                    break;
                }
            }
            if pass_removed == 0 {
                break;
            }
        }
    }
    verify_writer_fence(connection, fence).await?;
    let final_cluster_owner = connection
        .stable_cluster_slot_owner(&fence.current_key)
        .await?;
    if initial_cluster_owner != final_cluster_owner {
        return Err(cluster_slot_retry_error(
            "Redis Cluster slot ownership changed during target cleanup",
        ));
    }
    Ok(total_removed)
}

async fn execute_fenced_pipeline(
    connection: &mut RedisConnection,
    config: &RedisConfig,
    prepared: &PreparedPipeline,
    fences: &[(String, WriterFence)],
) -> redis::RedisResult<RedisPipelineExecution> {
    const STRING_SCRIPT: &str = "
        local function stored_is_newer(stored, incoming)
            if string.len(stored) ~= string.len(incoming) then
                return string.len(stored) > string.len(incoming)
            end
            return stored > incoming
        end

        local fence_count = tonumber(ARGV[1])
        local command_count = tonumber(ARGV[2])
        for index = 1, fence_count do
            if redis.call('GET', KEYS[index]) ~= ARGV[index + 2] then
                return redis.error_reply('VENTSTREAM_FENCED')
            end
        end

        local results = {}
        local argument = fence_count + 3
        for _ = 1, command_count do
            local operation = ARGV[argument]
            local payload = ARGV[argument + 1]
            local ttl = ARGV[argument + 2]
            local key_index = tonumber(ARGV[argument + 3])
            local version_key_index = tonumber(ARGV[argument + 5])
            local incoming_version = ARGV[argument + 6]
            argument = argument + 7

            local stored_version = redis.call('GET', KEYS[version_key_index])
            if stored_version and not string.match(stored_version, '^%d+$') then
                return redis.error_reply('VENTSTREAM_INVALID_STORED_VERSION')
            end
            local stale = stored_version and (
                incoming_version == '' or stored_is_newer(stored_version, incoming_version)
            )
            if stale then
                results[#results + 1] = 'STALE'
            else
                if operation == 'D' then
                    redis.call('DEL', KEYS[key_index])
                elseif operation == 'S' then
                    redis.call('SET', KEYS[key_index], payload)
                elseif operation == 'SP' then
                    redis.call('SET', KEYS[key_index], payload, 'PX', ttl)
                else
                    return redis.error_reply('VENTSTREAM_INVALID_OPERATION')
                end
                if incoming_version ~= '' then
                    if tonumber(ttl) > 0 then
                        redis.call('SET', KEYS[version_key_index], incoming_version, 'PX', ttl)
                    else
                        redis.call('SET', KEYS[version_key_index], incoming_version)
                    end
                end
                results[#results + 1] = 'OK'
            end
        end
        return results
    ";
    const JSON_SCRIPT: &str = "
        local function stored_is_newer(stored, incoming)
            if string.len(stored) ~= string.len(incoming) then
                return string.len(stored) > string.len(incoming)
            end
            return stored > incoming
        end

        local fence_count = tonumber(ARGV[1])
        local command_count = tonumber(ARGV[2])
        for index = 1, fence_count do
            if redis.call('GET', KEYS[index]) ~= ARGV[index + 2] then
                return redis.error_reply('VENTSTREAM_FENCED')
            end
        end

        local results = {}
        local argument = fence_count + 3
        for _ = 1, command_count do
            local operation = ARGV[argument]
            local payload = ARGV[argument + 1]
            local ttl = ARGV[argument + 2]
            local key_index = tonumber(ARGV[argument + 3])
            local staging_key_index = tonumber(ARGV[argument + 4])
            local version_key_index = tonumber(ARGV[argument + 5])
            local incoming_version = ARGV[argument + 6]
            argument = argument + 7

            local stored_version = redis.call('GET', KEYS[version_key_index])
            if stored_version and not string.match(stored_version, '^%d+$') then
                return redis.error_reply('VENTSTREAM_INVALID_STORED_VERSION')
            end
            local stale = stored_version and (
                incoming_version == '' or stored_is_newer(stored_version, incoming_version)
            )
            if stale then
                results[#results + 1] = 'STALE'
            else
                local result
                if operation == 'D' then
                    result = redis.pcall('DEL', KEYS[key_index])
                elseif operation == 'J' then
                    result = redis.pcall('JSON.SET', KEYS[key_index], '$', payload)
                elseif operation == 'JP' then
                    if staging_key_index == 0 then
                        return redis.error_reply('VENTSTREAM_INVALID_OPERATION')
                    end
                    local current_type = redis.call('TYPE', KEYS[key_index]).ok
                    if current_type ~= 'none' and current_type ~= 'ReJSON-RL' then
                        result = { err = 'WRONGTYPE RedisJSON cache target has a non-JSON value' }
                    else
                        result = redis.pcall('DEL', KEYS[staging_key_index])
                    end
                    if not (type(result) == 'table' and result.err) then
                        result = redis.pcall('JSON.SET', KEYS[staging_key_index], '$', payload)
                    end
                    if not (type(result) == 'table' and result.err) then
                        result = redis.pcall('PEXPIRE', KEYS[staging_key_index], ttl)
                    end
                    if not (type(result) == 'table' and result.err) then
                        result = redis.pcall('RENAME', KEYS[staging_key_index], KEYS[key_index])
                    end
                else
                    return redis.error_reply('VENTSTREAM_INVALID_OPERATION')
                end

                if type(result) == 'table' and result.err then
                    if staging_key_index ~= 0 then
                        redis.pcall('DEL', KEYS[staging_key_index])
                    end
                    local lower_error = string.lower(result.err)
                    if string.sub(result.err, 1, 9) == 'WRONGTYPE'
                        or string.find(lower_error, 'wrong redis type', 1, true) then
                        results[#results + 1] = 'ERR\t' .. result.err
                    else
                        return redis.error_reply(result.err)
                    end
                else
                    if incoming_version ~= '' then
                        if tonumber(ttl) > 0 then
                            redis.call('SET', KEYS[version_key_index], incoming_version, 'PX', ttl)
                        else
                            redis.call('SET', KEYS[version_key_index], incoming_version)
                        end
                    end
                    results[#results + 1] = 'OK'
                end
            end
        end
        return results
    ";

    static STRING_PIPELINE: LazyLock<RedisScript> =
        LazyLock::new(|| RedisScript::new(STRING_SCRIPT));
    static JSON_PIPELINE: LazyLock<RedisScript> = LazyLock::new(|| RedisScript::new(JSON_SCRIPT));
    let script = match config.document_format {
        RedisDocumentFormat::String => &*STRING_PIPELINE,
        RedisDocumentFormat::Json => &*JSON_PIPELINE,
    };
    let routing_key = fences
        .first()
        .map(|(_, fence)| fence.current_key.as_str())
        .ok_or_else(|| {
            redis::RedisError::from((
                ErrorKind::Client,
                "Redis fenced pipeline had no writer fence",
            ))
        })?;
    let mut invocation = script.prepare_invoke();
    for (_, fence) in fences {
        invocation.key(&fence.current_key);
    }
    let mut key_indexes = Vec::with_capacity(prepared.commands.len());
    let mut next_key_index = fences.len().saturating_add(1);
    for prepared_command in &prepared.commands {
        invocation.key(&prepared_command.key);
        let key_index = next_key_index;
        next_key_index = next_key_index.saturating_add(1);
        let staging_key_index = if let Some(staging_key) = &prepared_command.staging_key {
            invocation.key(staging_key);
            let index = next_key_index;
            next_key_index = next_key_index.saturating_add(1);
            index
        } else {
            0
        };
        invocation.key(&prepared_command.version_key);
        let version_key_index = next_key_index;
        next_key_index = next_key_index.saturating_add(1);
        key_indexes.push((key_index, staging_key_index, version_key_index));
    }
    invocation.arg(fences.len()).arg(prepared.command_count);
    for (_, fence) in fences {
        invocation.arg(&fence.value);
    }
    for (prepared_command, (key_index, staging_key_index, version_key_index)) in
        prepared.commands.iter().zip(key_indexes)
    {
        invocation
            .arg(redis_operation(config, prepared_command.kind))
            .arg(prepared_command.payload.as_ref())
            .arg(redis_operation_ttl(config))
            .arg(key_index)
            .arg(staging_key_index)
            .arg(version_key_index)
            .arg(prepared_command.source_version.as_deref().unwrap_or(""));
    }

    let results = invocation
        .invoke::<Vec<String>>(connection, routing_key)
        .await?;
    if results.len() != prepared.command_count {
        return Err(redis::RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "Redis fenced batch response count did not match command count",
        )));
    }

    let mut rejected = Vec::new();
    let mut stale_skipped = 0usize;
    for (index, result) in results.iter().enumerate() {
        if result == "OK" {
            continue;
        }
        if result == "STALE" {
            stale_skipped = stale_skipped.saturating_add(1);
            continue;
        }
        let Some(message) = result.strip_prefix("ERR\t") else {
            return Err(redis::RedisError::from((
                ErrorKind::UnexpectedReturnType,
                "Redis fenced batch returned an invalid command result",
            )));
        };
        let prepared_command = prepared.commands.get(index).ok_or_else(|| {
            redis::RedisError::from((
                ErrorKind::UnexpectedReturnType,
                "Redis fenced batch result index was invalid",
            ))
        })?;
        if is_event_level_script_error(message) {
            rejected.push(FailedItem {
                offset: prepared_command.event_offset,
                error: format!("Redis rejected the event: {message}"),
            });
        } else {
            return Err(redis_script_error(message));
        }
    }
    Ok(RedisPipelineExecution {
        rejected,
        stale_skipped,
    })
}

async fn execute_fenced_view_pipeline(
    connection: &mut RedisConnection,
    config: &RedisConfig,
    prepared: &PreparedViewPipeline,
    fences: &[(String, WriterFence)],
) -> redis::RedisResult<usize> {
    const SCRIPT: &str = "
        local function stored_is_newer(stored, incoming)
            if string.len(stored) ~= string.len(incoming) then
                return string.len(stored) > string.len(incoming)
            end
            return stored > incoming
        end

        local fence_count = tonumber(ARGV[1])
        local command_count = tonumber(ARGV[2])
        local operation = ARGV[3]
        local ttl = ARGV[4]
        for index = 1, fence_count do
            if redis.call('GET', KEYS[index]) ~= ARGV[index + 4] then
                return redis.error_reply('VENTSTREAM_FENCED')
            end
        end

        local argument = fence_count + 5
        local manifest_loaded = {}
        local manifest_keys = {}
        local manifest_owners = {}
        local manifest_versions = {}
        local virtual_owners = {}
        local virtual_presence = {}
        local accepted = {}
        local stale_count = 0
        for command_index = 1, command_count do
            local source_id = ARGV[argument]
            local incoming_version = ARGV[argument + 1]
            local manifest_index = tonumber(ARGV[argument + 2])
            local key_index = tonumber(ARGV[argument + 3])
            local owner_index = tonumber(ARGV[argument + 4])
            argument = argument + 7

            local manifest_key = KEYS[manifest_index]
            local old_key
            local old_owner
            local stored_version
            if manifest_loaded[manifest_key] then
                old_key = manifest_keys[manifest_key]
                old_owner = manifest_owners[manifest_key]
                stored_version = manifest_versions[manifest_key]
            else
                old_key = redis.call('HGET', manifest_key, 'key')
                old_owner = redis.call('HGET', manifest_key, 'owner')
                stored_version = redis.call('HGET', manifest_key, 'version')
                manifest_loaded[manifest_key] = true
            end
            if stored_version and not string.match(stored_version, '^%d+$') then
                return redis.error_reply('VENTSTREAM_INVALID_STORED_VERSION')
            end
            local stale = stored_version and (
                incoming_version == '' or stored_is_newer(stored_version, incoming_version)
            )
            if stale then
                accepted[command_index] = false
                stale_count = stale_count + 1
            else
                accepted[command_index] = true
                if (old_key and not old_owner) or (old_owner and not old_key) then
                    return redis.error_reply('VENTSTREAM_VIEW_MANIFEST_INCOMPLETE')
                end
                if old_key then
                    local old_present = virtual_presence[old_key]
                    if old_present == nil then
                        old_present = redis.call('EXISTS', old_key) == 1
                    end
                    local old_owner_value = virtual_owners[old_owner]
                    if old_owner_value == nil then
                        old_owner_value = redis.call('GET', old_owner)
                    end
                    if old_present and old_owner_value ~= source_id then
                        return redis.error_reply('VENTSTREAM_VIEW_OWNERSHIP_MISMATCH')
                    end
                    virtual_presence[old_key] = false
                    virtual_owners[old_owner] = false
                end

                if key_index ~= 0 then
                    local key = KEYS[key_index]
                    local owner_key = KEYS[owner_index]
                    local present = virtual_presence[key]
                    if present == nil then
                        present = redis.call('EXISTS', key) == 1
                    end
                    local owner = virtual_owners[owner_key]
                    if owner == nil then
                        owner = redis.call('GET', owner_key)
                    end
                    if present and not owner then
                        return redis.error_reply('VENTSTREAM_VIEW_OWNERSHIP_MISSING')
                    end
                    if owner and owner ~= source_id and present then
                        return redis.error_reply('VENTSTREAM_VIEW_KEY_COLLISION')
                    end
                    if (operation == 'J' or operation == 'JP')
                        and virtual_presence[key] == nil then
                        local current_type = redis.call('TYPE', key).ok
                        if current_type ~= 'none' and current_type ~= 'ReJSON-RL' then
                            return redis.error_reply('VENTSTREAM_VIEW_WRONGTYPE')
                        end
                    end
                    virtual_presence[key] = true
                    virtual_owners[owner_key] = source_id
                    manifest_keys[manifest_key] = key
                    manifest_owners[manifest_key] = owner_key
                else
                    manifest_keys[manifest_key] = false
                    manifest_owners[manifest_key] = false
                end
                manifest_versions[manifest_key] = incoming_version ~= '' and incoming_version or false
            end
        end

        argument = fence_count + 5
        for command_index = 1, command_count do
            local source_id = ARGV[argument]
            local incoming_version = ARGV[argument + 1]
            local manifest_index = tonumber(ARGV[argument + 2])
            local key_index = tonumber(ARGV[argument + 3])
            local owner_index = tonumber(ARGV[argument + 4])
            local staging_index = tonumber(ARGV[argument + 5])
            local payload = ARGV[argument + 6]
            argument = argument + 7

            if accepted[command_index] then
                local old_key = redis.call('HGET', KEYS[manifest_index], 'key')
                local old_owner = redis.call('HGET', KEYS[manifest_index], 'owner')
                if old_key and redis.call('GET', old_owner) == source_id then
                    redis.call('DEL', old_key, old_owner)
                end
                redis.call('DEL', KEYS[manifest_index])

                if key_index ~= 0 then
                    if operation == 'S' then
                        redis.call('SET', KEYS[key_index], payload)
                        redis.call('SET', KEYS[owner_index], source_id)
                    elseif operation == 'SP' then
                        redis.call('SET', KEYS[key_index], payload, 'PX', ttl)
                        redis.call('SET', KEYS[owner_index], source_id, 'PX', ttl)
                    elseif operation == 'J' then
                        redis.call('JSON.SET', KEYS[key_index], '$', payload)
                        redis.call('SET', KEYS[owner_index], source_id)
                    elseif operation == 'JP' then
                        if staging_index == 0 then
                            return redis.error_reply('VENTSTREAM_INVALID_OPERATION')
                        end
                        redis.call('DEL', KEYS[staging_index])
                        redis.call('JSON.SET', KEYS[staging_index], '$', payload)
                        redis.call('PEXPIRE', KEYS[staging_index], ttl)
                        redis.call('RENAME', KEYS[staging_index], KEYS[key_index])
                        redis.call('SET', KEYS[owner_index], source_id, 'PX', ttl)
                    else
                        return redis.error_reply('VENTSTREAM_INVALID_OPERATION')
                    end
                    redis.call(
                        'HSET',
                        KEYS[manifest_index],
                        'key',
                        KEYS[key_index],
                        'owner',
                        KEYS[owner_index]
                    )
                end
                if incoming_version ~= '' then
                    redis.call('HSET', KEYS[manifest_index], 'version', incoming_version)
                end
                if (operation == 'SP' or operation == 'JP')
                    and redis.call('EXISTS', KEYS[manifest_index]) == 1 then
                    redis.call('PEXPIRE', KEYS[manifest_index], ttl)
                end
            end
        end
        return {command_count - stale_count, stale_count}
    ";

    struct KeyIndexes {
        manifest: usize,
        key: usize,
        owner: usize,
        staging: usize,
    }

    static VIEW_PIPELINE: LazyLock<RedisScript> = LazyLock::new(|| RedisScript::new(SCRIPT));
    let routing_key = fences
        .first()
        .map(|(_, fence)| fence.current_key.as_str())
        .ok_or_else(|| {
            redis::RedisError::from((
                ErrorKind::Client,
                "Redis fenced view pipeline had no writer fence",
            ))
        })?;
    let mut invocation = VIEW_PIPELINE.prepare_invoke();
    for (_, fence) in fences {
        invocation.key(&fence.current_key);
    }

    let mut indexes = Vec::with_capacity(prepared.commands.len());
    let mut next_key_index = fences.len().saturating_add(1);
    for prepared_command in &prepared.commands {
        invocation.key(&prepared_command.manifest_key);
        let manifest = next_key_index;
        next_key_index = next_key_index.saturating_add(1);
        let (key, owner, staging) = match &prepared_command.desired {
            Some(entry) => {
                invocation.key(&entry.key);
                let key = next_key_index;
                next_key_index = next_key_index.saturating_add(1);
                invocation.key(&entry.owner_key);
                let owner = next_key_index;
                next_key_index = next_key_index.saturating_add(1);
                let staging = match &entry.staging_key {
                    Some(staging_key) => {
                        invocation.key(staging_key);
                        let staging = next_key_index;
                        next_key_index = next_key_index.saturating_add(1);
                        staging
                    }
                    None => 0,
                };
                (key, owner, staging)
            }
            None => (0, 0, 0),
        };
        indexes.push(KeyIndexes {
            manifest,
            key,
            owner,
            staging,
        });
    }

    invocation
        .arg(fences.len())
        .arg(prepared.commands.len())
        .arg(view_redis_operation(config))
        .arg(redis_operation_ttl(config));
    for (_, fence) in fences {
        invocation.arg(&fence.value);
    }
    for (prepared_command, indexes) in prepared.commands.iter().zip(indexes) {
        invocation
            .arg(&prepared_command.source_id)
            .arg(prepared_command.source_version.as_deref().unwrap_or(""))
            .arg(indexes.manifest)
            .arg(indexes.key)
            .arg(indexes.owner)
            .arg(indexes.staging)
            .arg(
                prepared_command
                    .desired
                    .as_ref()
                    .map_or(&[][..], |entry| entry.payload.as_ref()),
            );
    }

    let (written, stale_skipped) = invocation
        .invoke::<(usize, usize)>(connection, routing_key)
        .await?;
    if written.saturating_add(stale_skipped) != prepared.commands.len() {
        return Err(redis::RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "Redis view batch acknowledgement did not match command count",
        )));
    }
    Ok(stale_skipped)
}

fn redis_operation(config: &RedisConfig, kind: PreparedCommandKind) -> &'static str {
    if kind == PreparedCommandKind::Delete {
        return "D";
    }
    view_redis_operation(config)
}

fn durable_source_version(event: &Event) -> Result<Option<String>, String> {
    let Some((header, raw)) = [LSN_HEADER, TX_ID_HEADER, SOURCE_VERSION_HEADER]
        .into_iter()
        .find_map(|header| event.headers.get(header).map(|value| (header, value)))
    else {
        return Ok(None);
    };
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "event {} has an invalid `{header}`; Redis source ordering requires an unsigned decimal integer",
            event.id
        ));
    }
    let canonical = raw.trim_start_matches('0');
    if canonical.is_empty() {
        return Ok(None);
    }
    Ok(Some(canonical.to_owned()))
}

fn is_event_level_script_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    message.starts_with("WRONGTYPE") || lower.contains("wrong redis type")
}

fn redis_script_error(message: &str) -> redis::RedisError {
    let (code, details) = message
        .split_once(' ')
        .map_or((message, None), |(code, details)| (code, Some(details)));
    redis::make_extension_error(code.to_owned(), details.map(str::to_owned))
}

#[async_trait]
impl Sink for RedisSink {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "redis"
    }

    fn estimate_event_bytes(&self, event: &Event) -> usize {
        if let Some(views) = &self.views {
            return views.estimate_event_bytes(
                event,
                &ViewEstimateLimits {
                    key_prefix: &self.config.key_prefix,
                    max_key_bytes: self.config.max_key_bytes,
                    max_value_bytes: self.config.max_value_bytes,
                    max_batch_bytes: self.config.max_batch_bytes,
                    staging_keys: uses_json_cache_staging(&self.config),
                },
            );
        }
        let payload_bytes = if is_delete_event(event) {
            0
        } else {
            event.payload.as_slice().len()
        };
        payload_bytes
            .saturating_add(key_parts(&self.config, event).map_or(
                self.config.max_key_bytes,
                |(target, doc_id)| {
                    let key_bytes = encoded_key_len(&self.config.key_prefix, target, doc_id);
                    let ordering_bytes = version_key_len(&self.config.key_prefix, target, doc_id)
                        .saturating_add(
                            [LSN_HEADER, TX_ID_HEADER, SOURCE_VERSION_HEADER]
                                .into_iter()
                                .find_map(|header| event.headers.get(header))
                                .map_or(0, str::len),
                        );
                    if uses_json_cache_staging(&self.config) && !is_delete_event(event) {
                        key_bytes
                            .saturating_add(ordering_bytes)
                            .saturating_add(staging_key_len(
                                &self.config.key_prefix,
                                target,
                                doc_id,
                            ))
                    } else {
                        key_bytes.saturating_add(ordering_bytes)
                    }
                },
            ))
            .saturating_add(96)
    }

    fn max_request_bytes(&self) -> Option<usize> {
        Some(self.config.max_batch_bytes)
    }

    fn recommended_concurrency(&self, configured_ceiling: usize) -> usize {
        self.adaptive.desired_concurrency(
            configured_ceiling
                .min(self.connections.len())
                .min(ORDERED_DISPATCH_CONCURRENCY),
        )
    }

    async fn write(&self, batch: SinkBatch) -> Result<(), SinkError> {
        let PreparedBatch {
            operations,
            mut rejected,
        } = self.prepare(&batch)?;
        for operation in &operations {
            rejected.extend(self.execute_with_retry(operation).await?);
        }
        if rejected.is_empty() {
            return Ok(());
        }
        rejected.sort_by_key(|item| item.offset);
        rejected.dedup_by_key(|item| item.offset);
        let message = rejected
            .first()
            .map(|item| item.error.clone())
            .unwrap_or_else(|| "Redis rejected an event".to_owned());
        Err(SinkError::Rejected {
            batch_size: batch.len(),
            rejected_count: rejected.len(),
            message,
            failed_items: Some(rejected),
        })
    }
}

fn jittered_delay(delay: Duration) -> Duration {
    delay.mul_f64(rand::thread_rng().gen_range(0.5..=1.0))
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]
mod tests {
    use super::*;
    use crate::RedisSentinelTopology;
    use std::path::PathBuf;
    use ventstream_core::{ContentType, Headers, Payload, SourceUri, Subject};

    fn event(subject: &str, relation: &str, doc_id: &str, payload: &str) -> Event {
        let headers = Headers::empty()
            .with_header(RELATION_HEADER.to_owned(), relation.to_owned())
            .with_header(DOC_ID_HEADER.to_owned(), doc_id.to_owned());
        Event::builder(
            SourceUri::new("test://redis").expect("source"),
            Subject::new(subject).expect("subject"),
        )
        .content_type(ContentType::Json)
        .headers(headers)
        .payload(Payload::from_vec(payload.as_bytes().to_vec()))
        .build()
    }

    fn truncate_event(relation: &str) -> Event {
        let headers = Headers::empty().with_header(RELATION_HEADER.to_owned(), relation.to_owned());
        Event::builder(
            SourceUri::new("test://redis").expect("source"),
            Subject::new(format!("postgres.{relation}.truncate")).expect("subject"),
        )
        .content_type(ContentType::Json)
        .headers(headers)
        .payload(Payload::from_vec(
            br#"{"cascade":false,"restart_identity":false}"#.to_vec(),
        ))
        .build()
    }

    fn prepared_pipelines(prepared: &PreparedBatch) -> Vec<&PreparedPipeline> {
        prepared
            .operations
            .iter()
            .filter_map(|operation| match operation {
                PreparedOperation::Pipeline(pipeline) => Some(pipeline),
                PreparedOperation::ViewPipeline(_) => None,
                PreparedOperation::ClearTarget(_) => None,
            })
            .collect()
    }

    fn config() -> RedisConfig {
        RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive)
    }

    #[test]
    fn key_is_stable_and_cluster_slot_is_target_scoped() {
        let sink_event = event(
            "postgres.public.orders.update",
            "public.orders",
            r#"public.orders:["order:1"]"#,
            r#"{"id":"order:1"}"#,
        );
        let config = config();
        let (target, doc_id) = key_parts(&config, &sink_event).expect("key parts");
        let key_len = encoded_key_len(&config.key_prefix, target, doc_id);
        let key = key_from_parts(&config.key_prefix, target, doc_id, key_len);
        assert_eq!(
            key,
            "ventstream:test:{public.orders}:public.orders%3A%5B%22order%3A1%22%5D"
        );
    }

    #[test]
    fn writer_fence_is_target_scoped_and_outside_the_data_key_pattern() {
        let fence = writer_fence_key("ventstream:test", "orders:*");
        let lineage = writer_lineage_key("ventstream:test", "orders:*");
        assert_eq!(
            fence,
            "ventstream:test:__ventstream:writer:{orders%3A%2A}:current"
        );
        assert_eq!(
            lineage,
            "ventstream:test:__ventstream:writer:{orders%3A%2A}:lineage"
        );
        assert!(fence.contains("{orders%3A%2A}"));
        assert!(lineage.contains("{orders%3A%2A}"));
        assert!(!fence.starts_with("ventstream:test:{orders%3A%2A}:"));
        assert!(!lineage.starts_with("ventstream:test:{orders%3A%2A}:"));
    }

    #[test]
    fn malformed_key_headers_reject_only_the_affected_event() {
        let headers = Headers::empty().with_header(RELATION_HEADER.to_owned(), "orders".to_owned());
        let missing_id = Event::builder(
            SourceUri::new("test://redis").expect("source"),
            Subject::new("postgres.public.orders.update").expect("subject"),
        )
        .headers(headers)
        .payload(Payload::from_vec(br#"{"id":1}"#.to_vec()))
        .build();
        let empty_id = event("postgres.public.orders.update", "orders", "", r#"{"id":2}"#);
        let invalid_target = event(
            "postgres.public.orders.update",
            "orders\narchive",
            "orders:3",
            r#"{"id":3}"#,
        );
        let valid = event(
            "postgres.public.orders.update",
            "orders",
            "orders:4",
            r#"{"id":4}"#,
        );

        let prepared = prepare_batch(
            &config(),
            &SinkBatch::new(vec![missing_id, empty_id, invalid_target, valid]),
        )
        .expect("prepare");
        assert_eq!(prepared.rejected.len(), 3);
        assert_eq!(prepared.rejected[0].offset, 0);
        assert!(prepared.rejected[0].error.contains(DOC_ID_HEADER));
        assert_eq!(prepared.rejected[1].offset, 1);
        assert_eq!(prepared.rejected[2].offset, 2);
        assert_eq!(prepared_pipelines(&prepared)[0].command_count, 1);
    }

    #[test]
    fn json_validation_returns_exact_failed_offset() {
        let config = config().with_document_format(RedisDocumentFormat::Json);
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:1",
                r#"{"id":1}"#,
            ),
            event("postgres.public.orders.insert", "orders", "orders:2", "{"),
        ]);
        let prepared = prepare_batch(&config, &batch).expect("prepare");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 1);
        assert_eq!(
            pipelines.first().expect("prepared pipeline").command_count,
            1
        );
        assert_eq!(prepared.rejected.len(), 1);
        assert_eq!(prepared.rejected.first().expect("rejection").offset, 1);
    }

    #[test]
    fn redis_json_validation_accepts_scalars_and_rejects_trailing_documents() {
        let config = config().with_document_format(RedisDocumentFormat::Json);
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:1",
                "true",
            ),
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:2",
                r#"{"id":2} {"id":3}"#,
            ),
        ]);

        let prepared = prepare_batch(&config, &batch).expect("prepare");
        assert_eq!(prepared_pipelines(&prepared)[0].command_count, 1);
        assert_eq!(prepared.rejected.len(), 1);
        assert_eq!(prepared.rejected[0].offset, 1);
    }

    #[test]
    fn redis_json_delete_does_not_depend_on_the_tombstone_payload() {
        let mut config = config().with_document_format(RedisDocumentFormat::Json);
        config.max_value_bytes = 1;
        let batch = SinkBatch::new(vec![event(
            "postgres.public.orders.delete",
            "orders",
            "orders:1",
            "not-json-and-larger-than-the-value-limit",
        )]);

        let prepared = prepare_batch(&config, &batch).expect("prepare delete");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 1);
        assert_eq!(pipelines[0].deletes, 1);
        assert!(prepared.rejected.is_empty());
    }

    #[test]
    fn endpoint_credentials_are_rejected_before_connect() {
        let config = RedisConfig::new(
            "redis",
            "rediss://user:secret@redis.example.com:6379/",
            "ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn debug_output_redacts_resolved_passwords() {
        let mut config = config().with_auth(
            Some("ventstream".to_owned()),
            Some("do-not-log-this-secret".to_owned()),
        );
        config.topology = RedisTopology::Standalone {
            endpoint: "redis://url-user:url-secret@127.0.0.1:6379".to_owned(),
        };
        let diagnostic = format!("{config:?}");
        assert!(diagnostic.contains("ventstream"));
        assert!(!diagnostic.contains("do-not-log-this-secret"));
        assert!(!diagnostic.contains("url-user"));
        assert!(!diagnostic.contains("url-secret"));
    }

    #[test]
    fn unsafe_key_segments_are_rejected_before_connect() {
        let prefix = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            " ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        );
        assert!(prefix.validate().is_err());

        let target = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::Fixed("orders\n".to_owned()),
        );
        assert!(target.validate().is_err());
    }

    #[tokio::test]
    async fn insecure_tls_urls_are_rejected_before_connect() {
        let result = RedisSink::connect(RedisConfig::new(
            "redis",
            "rediss://127.0.0.1:6379/#insecure",
            "ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        ))
        .await;
        assert!(matches!(
            result,
            Err(SinkError::Blocked(reason)) if reason.contains("#insecure")
        ));
    }

    #[tokio::test]
    async fn startup_retry_stops_when_the_owner_is_cancelled() {
        let shutdown = ShutdownToken::new();
        let cancellation = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            cancellation.cancel();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            RedisSink::connect_with_shutdown(
                RedisConfig::new(
                    "redis",
                    "redis://127.0.0.1:1",
                    "ventstream:test",
                    RedisKeyRouting::ByOutputRelation,
                ),
                &shutdown,
            ),
        )
        .await
        .expect("startup cancellation timed out");

        assert!(matches!(
            result,
            Err(SinkError::Connection(reason)) if reason.contains("cancelled")
        ));
    }

    #[test]
    fn mutual_tls_files_must_be_complete_and_use_rediss() {
        let mut missing_key = config();
        missing_key.topology = RedisTopology::Standalone {
            endpoint: "rediss://redis.example.com:6379".to_owned(),
        };
        missing_key.tls.client_cert_file = Some(PathBuf::from("/run/secrets/client.crt"));
        assert!(missing_key.validate().is_err());

        let mut cleartext = config();
        cleartext.tls.ca_file = Some(PathBuf::from("/run/secrets/ca.crt"));
        assert!(cleartext.validate().is_err());
    }

    #[test]
    fn millisecond_commands_reject_sub_millisecond_durations() {
        let cache = config().with_contract(RedisContract::Cache {
            ttl: Duration::from_nanos(1),
        });
        assert!(cache.validate().is_err());

        let replicated = config().with_acknowledgement(RedisAcknowledgement::Replicated {
            replicas: 1,
            timeout: Duration::from_nanos(1),
        });
        assert!(replicated.validate().is_err());

        let aof = config().with_acknowledgement(RedisAcknowledgement::Aof {
            local: true,
            replicas: 0,
            timeout: Duration::from_nanos(1),
        });
        assert!(aof.validate().is_err());

        let no_aof_target = config().with_acknowledgement(RedisAcknowledgement::Aof {
            local: false,
            replicas: 0,
            timeout: Duration::from_secs(1),
        });
        assert!(no_aof_target.validate().is_err());

        let oversized_cache = config().with_contract(RedisContract::Cache {
            ttl: Duration::from_millis(u64::MAX),
        });
        assert!(oversized_cache.validate().is_err());

        let oversized_ack = config().with_acknowledgement(RedisAcknowledgement::Replicated {
            replicas: 1,
            timeout: Duration::from_millis(u64::MAX),
        });
        assert!(oversized_ack.validate().is_err());
    }

    #[test]
    fn oversized_keys_are_exact_item_rejections() {
        let mut config = config();
        config.max_key_bytes = 64;
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:small",
                r#"{"id":"small"}"#,
            ),
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:this-id-is-too-large-for-the-configured-key-bound",
                r#"{"id":"large"}"#,
            ),
        ]);
        let prepared = prepare_batch(&config, &batch).expect("prepare");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 1);
        assert_eq!(
            pipelines.first().expect("prepared pipeline").command_count,
            1
        );
        assert_eq!(prepared.rejected.len(), 1);
        assert_eq!(prepared.rejected.first().expect("rejection").offset, 1);
    }

    #[test]
    fn oversized_batches_are_split_without_reordering() {
        let mut config = config();
        config.max_batch_bytes = 256;
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:1",
                r#"{"id":1}"#,
            ),
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:2",
                r#"{"id":2}"#,
            ),
        ]);
        let prepared = prepare_batch(&config, &batch).expect("prepare");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 2);
        assert!(pipelines.iter().all(|pipeline| pipeline.command_count == 1));
        assert!(prepared.rejected.is_empty());
    }

    #[test]
    fn adaptive_batch_limit_splits_below_the_operator_ceiling() {
        let config = config();
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:1",
                r#"{"id":1}"#,
            ),
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:2",
                r#"{"id":2}"#,
            ),
        ]);
        let prepared = prepare_batch_with_limits(
            &config,
            &batch,
            RedisOperationLimits {
                max_batch_bytes: 256,
                max_commands: MAX_PIPELINE_COMMANDS,
            },
        )
        .expect("prepare with adaptive limit");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 2);
        assert_eq!(
            pipelines[0].commands[0].key,
            "ventstream:test:{orders}:orders%3A1"
        );
        assert_eq!(
            pipelines[1].commands[0].key,
            "ventstream:test:{orders}:orders%3A2"
        );
    }

    #[test]
    fn command_count_is_bounded_per_lua_invocation() {
        let events = (0..=MAX_PIPELINE_COMMANDS)
            .map(|index| {
                event(
                    "postgres.public.orders.insert",
                    "orders",
                    &format!("orders:{index}"),
                    "{}",
                )
            })
            .collect();

        let prepared = prepare_batch(&config(), &SinkBatch::new(events)).expect("prepare");
        let pipelines = prepared_pipelines(&prepared);
        assert_eq!(pipelines.len(), 2);
        assert_eq!(pipelines[0].command_count, MAX_PIPELINE_COMMANDS);
        assert_eq!(pipelines[1].command_count, 1);
    }

    #[test]
    fn truncate_is_ordered_between_point_writes_without_a_document_id() {
        let batch = SinkBatch::new(vec![
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:before",
                r#"{"id":"before"}"#,
            ),
            truncate_event("orders"),
            event(
                "postgres.public.orders.insert",
                "orders",
                "orders:after",
                r#"{"id":"after"}"#,
            ),
        ]);
        let prepared = prepare_batch(&config(), &batch).expect("prepare");
        assert_eq!(prepared.operations.len(), 3);
        assert!(matches!(
            prepared.operations.first(),
            Some(PreparedOperation::Pipeline(pipeline))
                if pipeline.command_count == 1 && pipeline.upserts == 1
        ));
        assert!(matches!(
            prepared.operations.get(1),
            Some(PreparedOperation::ClearTarget(clear))
                if clear.target == "orders"
                    && clear.patterns
                        == [
                            "ventstream:test:{orders}:*",
                            "ventstream:test:__ventstream:stage:{orders}:*",
                            "ventstream:test:__ventstream:version:{orders}:*",
                        ]
        ));
        assert!(matches!(
            prepared.operations.get(2),
            Some(PreparedOperation::Pipeline(pipeline))
                if pipeline.command_count == 1 && pipeline.upserts == 1
        ));
    }

    #[test]
    fn truncate_rejects_ambiguous_routing_before_execution() {
        let batch = SinkBatch::new(vec![truncate_event("orders")]);
        let projection = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::ByProjectionTarget,
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive);
        assert!(matches!(
            prepare_batch(&projection, &batch),
            Err(SinkError::Blocked(reason)) if reason.contains("by_projection_target")
        ));

        let fixed = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::Fixed("orders".to_owned()),
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive);
        assert!(matches!(
            prepare_batch(&fixed, &batch),
            Err(SinkError::Blocked(reason)) if reason.contains("fixed")
        ));
    }

    #[test]
    fn projection_clear_requires_an_explicit_scoped_target() {
        let mut clear = truncate_event("orders");
        clear.headers = clear
            .headers
            .clone()
            .with_header(TARGET_CLEAR_HEADER.to_owned(), "true".to_owned())
            .with_header(
                PROJECTION_TARGET_HEADER.to_owned(),
                "orders:*?[archive]".to_owned(),
            );
        let projection = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::ByProjectionTarget,
        )
        .with_keyspace_ownership(RedisKeyspaceOwnership::Exclusive);
        let prepared =
            prepare_batch(&projection, &SinkBatch::new(vec![clear])).expect("projection clear");
        assert!(matches!(
            prepared.operations.first(),
            Some(PreparedOperation::ClearTarget(clear))
                if clear.target == "orders:*?[archive]"
                    && clear.patterns
                        == [
                            "ventstream:test:{orders%3A%2A%3F%5Barchive%5D}:*",
                            "ventstream:test:__ventstream:stage:{orders%3A%2A%3F%5Barchive%5D}:*",
                            "ventstream:test:__ventstream:version:{orders%3A%2A%3F%5Barchive%5D}:*",
                        ]
        ));
    }

    #[test]
    fn shared_keyspace_rejects_target_wide_truncate() {
        let shared = RedisConfig::new(
            "redis",
            "redis://127.0.0.1:6379",
            "ventstream:test",
            RedisKeyRouting::ByOutputRelation,
        );
        assert!(matches!(
            prepare_batch(&shared, &SinkBatch::new(vec![truncate_event("orders")])),
            Err(SinkError::Blocked(reason)) if reason.contains("ownership=exclusive")
        ));
    }

    #[test]
    fn retryable_server_pressure_is_transient() {
        for code in ["OOM", "MISCONF", "BUSY", "NOREPLICAS"] {
            let error = redis::make_extension_error(code.to_owned(), Some("pressure".to_owned()));
            assert!(matches!(
                classify_error(&error, &config().topology),
                RedisFailure::Transient {
                    reconnect: false,
                    ..
                }
            ));
        }
        for code in ["OOM", "BUSY", "NOREPLICAS"] {
            let error = redis::make_extension_error(code.to_owned(), Some("pressure".to_owned()));
            assert!(is_capacity_pressure(&error));
        }
        let persistence =
            redis::make_extension_error("MISCONF".to_owned(), Some("persistence".to_owned()));
        assert!(!is_capacity_pressure(&persistence));
    }

    #[test]
    fn superseded_writer_is_transient_without_reconnecting() {
        let error = redis::make_extension_error(
            "VENTSTREAM_FENCED".to_owned(),
            Some("writer generation no longer owns target".to_owned()),
        );
        assert!(matches!(
            classify_error(&error, &config().topology),
            RedisFailure::Transient {
                reconnect: false,
                reason,
            } if reason.contains("fenced")
        ));
    }

    #[test]
    fn authorization_and_unknown_server_errors_are_blocking() {
        let no_permission = redis::RedisError::from((
            ErrorKind::Server(ServerErrorKind::NoPerm),
            "permission denied",
        ));
        assert!(matches!(
            classify_error(&no_permission, &config().topology),
            RedisFailure::Blocked(_)
        ));

        let wrong_type = redis::make_extension_error(
            "WRONGTYPE".to_owned(),
            Some("wrong value type".to_owned()),
        );
        assert!(matches!(
            classify_error(&wrong_type, &config().topology),
            RedisFailure::Blocked(_)
        ));
    }

    #[test]
    fn cluster_redirects_block_without_cluster_topology() {
        let moved =
            redis::RedisError::from((ErrorKind::Server(ServerErrorKind::Moved), "key moved"));
        assert!(matches!(
            classify_error(&moved, &config().topology),
            RedisFailure::Blocked(reason) if reason.contains("Redis Cluster")
        ));
    }

    #[test]
    fn cluster_redirects_remain_retryable_after_native_redirect_attempts() {
        let moved =
            redis::RedisError::from((ErrorKind::Server(ServerErrorKind::Moved), "key moved"));
        let topology = RedisTopology::Cluster {
            endpoints: vec!["redis://127.0.0.1:7000".to_owned()],
        };
        assert!(matches!(
            classify_error(&moved, &topology),
            RedisFailure::Transient {
                reconnect: false,
                ..
            }
        ));
    }

    #[test]
    fn cluster_cleanup_requires_stable_local_slot_ownership() {
        let stable = "node-a 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460\n\
                      node-b 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922";
        validate_cluster_slot_owner(stable, "node-a", 4096).expect("stable slot owner");
        assert!(validate_cluster_slot_owner(stable, "node-a", 8192).is_err());

        let migrating = "node-a 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460 [4096->-node-b]\n\
                         node-b 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922 [4096-<-node-a]";
        let error = validate_cluster_slot_owner(migrating, "node-a", 4096)
            .expect_err("migrating slot must backpressure cleanup");
        assert_eq!(error.kind(), ErrorKind::Server(ServerErrorKind::TryAgain));
    }

    #[test]
    fn readonly_failover_requires_a_fresh_connection() {
        let readonly = redis::RedisError::from((
            ErrorKind::Server(ServerErrorKind::ReadOnly),
            "replica is read-only",
        ));
        assert!(matches!(
            classify_error(&readonly, &config().topology),
            RedisFailure::Transient {
                reconnect: true,
                ..
            }
        ));
    }

    #[test]
    fn sentinel_election_gaps_trigger_rediscovery() {
        let topology = RedisTopology::Sentinel(RedisSentinelTopology {
            endpoints: vec!["redis://127.0.0.1:26379".to_owned()],
            service_name: "mymaster".to_owned(),
            data_node_tls: false,
            username: None,
            password: None,
            username_file: None,
            password_file: None,
            tls: RedisTlsConfig::default(),
        });
        for (label, error) in [
            (
                "missing master",
                redis::RedisError::from((
                    ErrorKind::MasterNameNotFoundBySentinel,
                    "master is being elected",
                )),
            ),
            (
                "master down",
                redis::RedisError::from((
                    ErrorKind::Server(ServerErrorKind::MasterDown),
                    "master is down",
                )),
            ),
        ] {
            let classified = classify_error(&error, &topology);
            assert!(
                matches!(
                    classified,
                    RedisFailure::Transient {
                        reconnect: true,
                        ..
                    }
                ),
                "{label} was classified as {classified:?}"
            );
        }
    }

    #[test]
    fn dropped_connections_require_a_fresh_connection() {
        let dropped = redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(matches!(
            classify_error(&dropped, &config().topology),
            RedisFailure::Transient {
                reconnect: true,
                ..
            }
        ));
    }
}
