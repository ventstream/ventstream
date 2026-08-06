use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

use parking_lot::Mutex as SyncMutex;
use rand::Rng;
use tracing::{debug, info};
use ventstream_core::{ShutdownToken, SinkFailureGuard, SinkHealth, SinkHealthSnapshot};

use super::config::RedisConfig;
use super::contract::duration_millis;
use super::error::{classify_error, RedisFailure};
use super::keyspace::{writer_fence_key, writer_lineage_key};
use super::script::RedisScript;
use super::topology::{
    build_connector_with_revision, connect_raw, credential_revision, RedisConnection,
    RedisCredentialRevision,
};

pub(super) type WriterFenceMap = Arc<SyncMutex<HashMap<String, WriterFence>>>;

#[derive(Clone)]
pub(super) struct WriterFence {
    pub(super) current_key: String,
    pub(super) lineage_key: String,
    pub(super) value: String,
}

pub(super) async fn acquire_writer_fence(
    connection: &mut RedisConnection,
    config: &RedisConfig,
    target: &str,
) -> redis::RedisResult<WriterFence> {
    const SOURCE: &str = "
        local current = redis.call('GET', KEYS[1])
        if not current then
            redis.call('SET', KEYS[2], ARGV[1])
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            return 1
        end
        local lineage = redis.call('GET', KEYS[2])
        if lineage and lineage ~= current then
            return redis.error_reply('VENTSTREAM_FENCED')
        end
        if ARGV[3] ~= '' then
            local owner = string.match(current, '^v1|([^|]+)|')
            if owner == ARGV[3] then
                redis.call('SET', KEYS[2], ARGV[1])
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                return 2
            end
        end
        return redis.error_reply('VENTSTREAM_WRITER_OWNED')
    ";
    static SCRIPT: LazyLock<RedisScript> = LazyLock::new(|| RedisScript::new(SOURCE));

    let current_key = writer_fence_key(&config.key_prefix, target);
    let lineage_key = writer_lineage_key(&config.key_prefix, target);
    if current_key.len() > config.max_key_bytes || lineage_key.len() > config.max_key_bytes {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "Redis writer metadata key exceeds max_key_bytes",
        )));
    }
    let token = format!("{:032x}", rand::thread_rng().gen::<u128>());
    let value = format!("v1|{}|{token}", config.writer_id);
    let result = SCRIPT
        .prepare_invoke()
        .key(&current_key)
        .key(&lineage_key)
        .arg(&value)
        .arg(duration_millis(config.writer_lease))
        .arg(config.writer_takeover_from.as_deref().unwrap_or_default())
        .invoke::<usize>(connection, &current_key)
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let outcome = if error.to_string().contains("VENTSTREAM_WRITER_OWNED") {
                "conflict"
            } else {
                "failure"
            };
            ventstream_telemetry::bump_redis_writer_lease_acquisition(outcome);
            return Err(error);
        }
    };
    if result == 2 {
        ventstream_telemetry::bump_redis_writer_lease_acquisition("handoff");
        info!(
            sink_id = %config.id,
            target,
            writer_id = %config.writer_id,
            previous_writer_id = config.writer_takeover_from.as_deref().unwrap_or_default(),
            "Redis writer handoff completed"
        );
    } else {
        ventstream_telemetry::bump_redis_writer_lease_acquisition("acquired");
    }
    Ok(WriterFence {
        current_key,
        lineage_key,
        value,
    })
}

pub(super) async fn renew_writer_fence(
    connection: &mut RedisConnection,
    fence: &WriterFence,
    lease_ms: u64,
) -> redis::RedisResult<()> {
    const SOURCE: &str = "
        local current = redis.call('GET', KEYS[1])
        local lineage = redis.call('GET', KEYS[2])
        if current then
            if current ~= ARGV[1] or (lineage and lineage ~= ARGV[1]) then
                return redis.error_reply('VENTSTREAM_FENCED')
            end
            if not lineage then
                redis.call('SET', KEYS[2], ARGV[1])
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return 1
        end
        if lineage ~= ARGV[1] then
            return redis.error_reply('VENTSTREAM_FENCED')
        end
        redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
        return 2
    ";
    static SCRIPT: LazyLock<RedisScript> = LazyLock::new(|| RedisScript::new(SOURCE));
    let result = SCRIPT
        .prepare_invoke()
        .key(&fence.current_key)
        .key(&fence.lineage_key)
        .arg(&fence.value)
        .arg(lease_ms)
        .invoke::<usize>(connection, &fence.current_key)
        .await;
    if result.is_err() {
        ventstream_telemetry::bump_redis_writer_lease_renewal_failure();
    }
    if result? == 2 {
        ventstream_telemetry::bump_redis_writer_lease_acquisition("recovered");
    }
    Ok(())
}

pub(super) fn start_writer_heartbeat(
    config: RedisConfig,
    fences: WriterFenceMap,
    delivery_health: SinkHealth,
    shutdown: ShutdownToken,
    lifetime: Weak<()>,
) {
    tokio::spawn(async move {
        let interval = config.writer_lease / 3;
        let lease_ms = duration_millis(config.writer_lease);
        let mut connection: Option<RedisConnection> = None;
        let mut active_revision: Option<RedisCredentialRevision> = None;
        let mut transient_failure: Option<SinkFailureGuard> = None;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            if lifetime.upgrade().is_none() {
                return;
            }
            let mut snapshot = fences.lock().values().cloned().collect::<Vec<_>>();
            snapshot.sort_by(|left, right| left.current_key.cmp(&right.current_key));
            if snapshot.is_empty() {
                continue;
            }
            if config.has_rotating_credentials() && connection.is_some() {
                match credential_revision(&config).await {
                    Ok(revision) if active_revision != Some(revision) => {
                        connection = None;
                        active_revision = None;
                        ventstream_telemetry::record_redis_credential_reload(true);
                    }
                    Ok(_) => {}
                    Err(_) => {
                        connection = None;
                        active_revision = None;
                        ventstream_telemetry::record_redis_credential_reload(false);
                    }
                }
            }
            if connection.is_none() {
                match build_connector_with_revision(&config).await {
                    Ok((connector, revision)) => match connect_raw(&connector, &config).await {
                        Ok(connected) => {
                            connection = Some(connected);
                            active_revision = Some(revision);
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            if transient_failure.is_none() {
                                transient_failure =
                                    Some(delivery_health.begin_transient_failure(reason.clone()));
                            }
                            ventstream_telemetry::mark_sink_unavailable();
                            ventstream_telemetry::bump_sink_retries(1);
                            debug!(sink_id = %config.id, error = %reason, "Redis writer heartbeat connection failed");
                            continue;
                        }
                    },
                    Err(error) => {
                        let reason = error.to_string();
                        if transient_failure.is_none() {
                            transient_failure =
                                Some(delivery_health.begin_transient_failure(reason.clone()));
                        }
                        ventstream_telemetry::mark_sink_unavailable();
                        ventstream_telemetry::bump_sink_retries(1);
                        debug!(sink_id = %config.id, error = %reason, "Redis writer heartbeat connection failed");
                        continue;
                    }
                }
            }

            let Some(active) = connection.as_mut() else {
                continue;
            };
            let mut healthy = true;
            let mut reconnect_required = false;
            for fence in &snapshot {
                if let Err(error) = renew_writer_fence(active, fence, lease_ms).await {
                    let failure = classify_error(&error, &config.topology);
                    let (reason, reconnect) = match failure {
                        RedisFailure::Transient { reason, reconnect } => (reason, reconnect),
                        RedisFailure::Blocked(reason) => (reason, false),
                    };
                    if transient_failure.is_none() {
                        transient_failure =
                            Some(delivery_health.begin_transient_failure(reason.clone()));
                    }
                    ventstream_telemetry::mark_sink_unavailable();
                    ventstream_telemetry::bump_sink_retries(1);
                    debug!(sink_id = %config.id, error = %reason, "Redis writer heartbeat failed");
                    healthy = false;
                    if reconnect {
                        reconnect_required = true;
                        break;
                    }
                }
            }
            if reconnect_required {
                connection = None;
            }
            if healthy {
                transient_failure.take();
                if matches!(delivery_health.snapshot(), SinkHealthSnapshot::Healthy) {
                    ventstream_telemetry::mark_sink_available();
                }
            }
        }
    });
}
