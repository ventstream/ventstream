use std::collections::{BTreeMap, BTreeSet};

use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::debug;
use ventstream_core::SinkError;

use super::config::{
    RedisAcknowledgement, RedisConfig, RedisContract, RedisDocumentFormat, RedisKeyRouting,
    RedisKeyspaceOwnership,
};
use super::contract::{duration_millis, redis_operation_ttl, view_redis_operation};
use super::error::map_connect_error;
use super::keyspace::validate_target_segment;
use super::script::RedisScript;
use super::topology::RedisConnection;

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct RedisViewSchemaMetadata {
    pub(super) version: u8,
    pub(super) digest: String,
    pub(super) targets: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Compatibility of configured declarative views with stored Redis metadata.
pub enum RedisViewSchemaStatus {
    /// The sink does not use declarative view routing.
    NotApplicable,
    /// No view schema has been materialized for this key prefix yet.
    Missing,
    /// The configured views match the stored schema.
    Compatible,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RedisAcknowledgementObservation {
    pub(super) local_aof_acks: Option<u64>,
    pub(super) replica_acks: Option<u64>,
}

pub(super) async fn validate_operation_capabilities(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<RedisAcknowledgementObservation, SinkError> {
    const SCRIPT: &str = "
        local failures = {}
        local function attempt(command, ...)
            local result = redis.pcall(command, ...)
            if type(result) == 'table' and result.err then
                failures[#failures + 1] = command .. ': ' .. result.err
            end
        end

        attempt('SET', KEYS[1], '{}')
        attempt('GET', KEYS[1])
        attempt('EXISTS', KEYS[1])
        attempt('TYPE', KEYS[1])
        attempt('PEXPIRE', KEYS[1], ARGV[2])
        if ARGV[1] == 'J' then attempt('JSON.SET', KEYS[2], '$', '{}') end
        if ARGV[1] == 'JP' then
            attempt('JSON.SET', KEYS[2], '$', '{}')
            attempt('PEXPIRE', KEYS[2], ARGV[2])
        end
        attempt('DEL', unpack(KEYS))
        if #failures > 0 then
            return redis.error_reply(
                'VENTSTREAM_SINK_CAPABILITY ' .. table.concat(failures, '; ')
            )
        end
        return 1
    ";
    let nonce = format!("{:032x}", rand::thread_rng().gen::<u128>());
    let base = format!("{}:__ventstream:capability:{{{nonce}}}", config.key_prefix);
    let keys = [format!("{base}:value"), format!("{base}:json")];
    let mode = match (config.document_format, config.contract) {
        (RedisDocumentFormat::String, RedisContract::MaterializedView) => "S",
        (RedisDocumentFormat::String, RedisContract::Cache { .. }) => "P",
        (RedisDocumentFormat::Json, RedisContract::MaterializedView) => "J",
        (RedisDocumentFormat::Json, RedisContract::Cache { .. }) => "JP",
    };
    let script = RedisScript::new(SCRIPT);
    let mut invocation = script.prepare_invoke();
    invocation
        .key(&keys)
        .arg(mode)
        .arg(redis_operation_ttl(config).max(1));
    let result = invocation
        .invoke::<usize>(connection, &keys[0])
        .await
        .map_err(|error| map_connect_error(&error, config))?;
    if result != 1 {
        return Err(SinkError::Blocked(
            "Redis operation capability probe returned an invalid acknowledgement".to_owned(),
        ));
    }

    if config.keyspace_ownership == RedisKeyspaceOwnership::Exclusive {
        redis::cmd("SCAN")
            .arg(0)
            .arg("MATCH")
            .arg(format!(
                "{}:__ventstream:capability:never-match",
                config.key_prefix
            ))
            .arg("COUNT")
            .arg(1)
            .query_async::<(u64, Vec<String>)>(&mut *connection)
            .await
            .map_err(|error| map_connect_error(&error, config))?;
        redis::cmd("UNLINK")
            .arg(&keys)
            .query_async::<usize>(&mut *connection)
            .await
            .map_err(|error| map_connect_error(&error, config))?;
    }

    match config.acknowledgement {
        RedisAcknowledgement::Primary => Ok(RedisAcknowledgementObservation::default()),
        RedisAcknowledgement::Replicated { replicas, timeout } => {
            let acknowledgements = redis::cmd("WAIT")
                .arg(replicas)
                .arg(duration_millis(timeout))
                .query_async::<u64>(&mut *connection)
                .await
                .map_err(|error| map_connect_error(&error, config))?;
            if acknowledgements < u64::try_from(replicas).unwrap_or(u64::MAX) {
                return Err(SinkError::Connection(format!(
                    "Redis diagnostic required {replicas} replica acknowledgements but received {acknowledgements}"
                )));
            }
            Ok(RedisAcknowledgementObservation {
                local_aof_acks: None,
                replica_acks: Some(acknowledgements),
            })
        }
        RedisAcknowledgement::Aof {
            local,
            replicas,
            timeout,
        } => {
            let required_local = u64::from(local);
            let (local_acks, replica_acks) = redis::cmd("WAITAOF")
                .arg(required_local)
                .arg(replicas)
                .arg(duration_millis(timeout))
                .query_async::<(u64, u64)>(&mut *connection)
                .await
                .map_err(|error| map_connect_error(&error, config))?;
            if local_acks < required_local
                || replica_acks < u64::try_from(replicas).unwrap_or(u64::MAX)
            {
                return Err(SinkError::Connection(format!(
                    "Redis diagnostic required local AOF {required_local} and replica AOF {replicas} acknowledgements but received local {local_acks} and replicas {replica_acks}"
                )));
            }
            Ok(RedisAcknowledgementObservation {
                local_aof_acks: Some(local_acks),
                replica_acks: Some(replica_acks),
            })
        }
    }
}

pub(super) async fn inspect_view_schema(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<RedisViewSchemaStatus, SinkError> {
    let Some(expected) = expected_view_schema_metadata(config)? else {
        return Ok(RedisViewSchemaStatus::NotApplicable);
    };
    let Some(current) = read_view_schema_metadata(config, connection).await? else {
        return Ok(RedisViewSchemaStatus::Missing);
    };
    if current.digest != expected.digest || current.targets != expected.targets {
        return Err(SinkError::Blocked(
            "Redis view definition does not match the schema stored for this key prefix; drain and rebootstrap before deploying this revision"
                .to_owned(),
        ));
    }
    Ok(RedisViewSchemaStatus::Compatible)
}

pub(super) async fn validate_document_capability(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<(), SinkError> {
    if config.document_format != RedisDocumentFormat::Json {
        return Ok(());
    }
    match redis::cmd("COMMAND")
        .arg("INFO")
        .arg("JSON.SET")
        .query_async::<redis::Value>(connection)
        .await
    {
        Ok(redis::Value::Array(entries))
            if entries.is_empty()
                || entries
                    .iter()
                    .all(|entry| matches!(entry, redis::Value::Nil)) =>
        {
            Err(SinkError::Blocked(
                "RedisJSON format requires a Redis endpoint with JSON.SET".to_owned(),
            ))
        }
        Ok(_) => Ok(()),
        Err(error) if error.code() == Some("NOPERM") => {
            debug!("Redis ACL does not allow COMMAND INFO; RedisJSON capability will be verified on first write");
            Ok(())
        }
        Err(error) => Err(map_connect_error(&error, config)),
    }
}

pub(super) async fn validate_view_capabilities(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<(), SinkError> {
    if !matches!(config.key_routing, RedisKeyRouting::Views(_)) {
        return Ok(());
    }

    redis::cmd("SCAN")
        .arg(0)
        .arg("MATCH")
        .arg(format!(
            "{}:__ventstream:capability:never-match",
            config.key_prefix
        ))
        .arg("COUNT")
        .arg(1)
        .query_async::<(u64, Vec<String>)>(&mut *connection)
        .await
        .map_err(|error| map_connect_error(&error, config))?;

    const SCRIPT: &str = "
        local failures = {}
        local function attempt(command, ...)
            local result = redis.pcall(command, ...)
            if type(result) == 'table' and result.err then
                failures[#failures + 1] = command .. ': ' .. result.err
            end
        end

        attempt('SET', KEYS[1], '{}')
        attempt('GET', KEYS[1])
        attempt('EXISTS', KEYS[1])
        attempt('TYPE', KEYS[1])
        attempt('HSET', KEYS[2], 'key', KEYS[1], 'owner', KEYS[3])
        attempt('HGET', KEYS[2], 'key')
        attempt('SET', KEYS[3], 'source')
        attempt('DEL', KEYS[3])
        attempt('SET', KEYS[3], 'source')

        if ARGV[1] == 'SP' then
            attempt('PEXPIRE', KEYS[1], ARGV[2])
            attempt('PEXPIRE', KEYS[2], ARGV[2])
            attempt('PEXPIRE', KEYS[3], ARGV[2])
        elseif ARGV[1] == 'J' then
            attempt('JSON.SET', KEYS[4], '$', '{}')
        elseif ARGV[1] == 'JP' then
            attempt('JSON.SET', KEYS[5], '$', '{}')
            attempt('PEXPIRE', KEYS[5], ARGV[2])
            attempt('RENAME', KEYS[5], KEYS[4])
            attempt('PEXPIRE', KEYS[2], ARGV[2])
            attempt('PEXPIRE', KEYS[3], ARGV[2])
        end

        attempt('DEL', unpack(KEYS))
        attempt('UNLINK', unpack(KEYS))
        if #failures > 0 then
            return redis.error_reply(
                'VENTSTREAM_VIEW_CAPABILITY ' .. table.concat(failures, '; ')
            )
        end
        return 1
    ";

    let nonce = format!("{:032x}", rand::thread_rng().gen::<u128>());
    let base = format!("{}:__ventstream:capability:{{{nonce}}}", config.key_prefix);
    let keys = [
        format!("{base}:value"),
        format!("{base}:manifest"),
        format!("{base}:owner"),
        format!("{base}:json"),
        format!("{base}:stage"),
    ];
    let script = RedisScript::new(SCRIPT);
    let mut invocation = script.prepare_invoke();
    invocation
        .key(&keys)
        .arg(view_redis_operation(config))
        .arg(redis_operation_ttl(config).max(1));
    let result = invocation
        .invoke::<usize>(connection, &keys[0])
        .await
        .map_err(|error| map_connect_error(&error, config))?;
    if result != 1 {
        return Err(SinkError::Blocked(
            "Redis view capability probe returned an invalid acknowledgement".to_owned(),
        ));
    }
    Ok(())
}

fn expected_view_schema_metadata(
    config: &RedisConfig,
) -> Result<Option<RedisViewSchemaMetadata>, SinkError> {
    let RedisKeyRouting::Views(views) = &config.key_routing else {
        return Ok(None);
    };
    let views_by_name = views
        .iter()
        .map(|view| (view.name.as_str(), view))
        .collect::<BTreeMap<_, _>>();
    let contract = match config.contract {
        RedisContract::MaterializedView => {
            serde_json::json!({"mode": "materialized_view"})
        }
        RedisContract::Cache { ttl } => {
            serde_json::json!({"mode": "cache", "ttl_ms": duration_millis(ttl)})
        }
    };
    let canonical = serde_json::json!({
        "version": 1,
        "document_format": match config.document_format {
            RedisDocumentFormat::String => "string",
            RedisDocumentFormat::Json => "json",
        },
        "contract": contract,
        "views": views_by_name,
    });
    let encoded = serde_json::to_vec(&canonical).map_err(|error| {
        SinkError::Blocked(format!("unable to fingerprint Redis view schema: {error}"))
    })?;
    let hash = Sha256::digest(encoded);
    let mut digest = String::with_capacity(71);
    digest.push_str("sha256:");
    for byte in hash {
        use std::fmt::Write as _;
        write!(digest, "{byte:02x}").map_err(|error| {
            SinkError::Blocked(format!(
                "unable to format Redis view schema digest: {error}"
            ))
        })?;
    }
    Ok(Some(RedisViewSchemaMetadata {
        version: 1,
        digest,
        targets: views_by_name
            .keys()
            .map(|name| (*name).to_owned())
            .collect(),
    }))
}

fn view_schema_metadata_key(config: &RedisConfig) -> Result<String, SinkError> {
    let key = format!("{}:__ventstream:view-schema:current", config.key_prefix);
    if key.len() > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis view schema key is {} bytes; max_key_bytes={}",
            key.len(),
            config.max_key_bytes
        )));
    }
    Ok(key)
}

pub(super) async fn validate_or_initialize_view_schema(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<(), SinkError> {
    let Some(expected) = expected_view_schema_metadata(config)? else {
        return Ok(());
    };
    let key = view_schema_metadata_key(config)?;
    let expected = serde_json::to_string(&expected).map_err(|error| {
        SinkError::Blocked(format!(
            "unable to encode Redis view schema metadata: {error}"
        ))
    })?;
    const SCRIPT: &str = "local current = redis.call('GET', KEYS[1]) \
             if current and current ~= ARGV[1] then \
                 return redis.error_reply('VENTSTREAM_VIEW_SCHEMA_MISMATCH') \
             end \
             if not current then redis.call('SET', KEYS[1], ARGV[1]) end \
             return 1";
    let script = RedisScript::new(SCRIPT);
    let mut invocation = script.prepare_invoke();
    invocation.key(&key).arg(expected);
    let result = invocation
        .invoke::<usize>(connection, &key)
        .await
        .map_err(|error| {
            if error.to_string().contains("VENTSTREAM_VIEW_SCHEMA_MISMATCH") {
                SinkError::Blocked(
                    "Redis view definition changed for this key prefix; run an exclusive drain/rebootstrap before starting the new revision"
                        .to_owned(),
                )
            } else {
                map_connect_error(&error, config)
            }
        })?;
    if result != 1 {
        return Err(SinkError::Blocked(
            "Redis view schema check returned an invalid acknowledgement".to_owned(),
        ));
    }
    Ok(())
}

pub(super) async fn read_view_schema_metadata(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<Option<RedisViewSchemaMetadata>, SinkError> {
    let key = view_schema_metadata_key(config)?;
    let Some(value) = redis::cmd("GET")
        .arg(key)
        .query_async::<Option<String>>(&mut *connection)
        .await
        .map_err(|error| map_connect_error(&error, config))?
    else {
        return Ok(None);
    };
    let metadata = serde_json::from_str::<RedisViewSchemaMetadata>(&value).map_err(|error| {
        SinkError::Blocked(format!(
            "stored Redis view schema metadata is invalid: {error}"
        ))
    })?;
    if metadata.version != 1
        || !metadata.digest.starts_with("sha256:")
        || metadata.digest.len() != 71
        || !metadata.digest[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || metadata.targets.is_empty()
        || metadata.targets.len() > 32
    {
        return Err(SinkError::Blocked(
            "stored Redis view schema metadata is invalid".to_owned(),
        ));
    }
    let unique_targets = metadata.targets.iter().collect::<BTreeSet<_>>();
    if unique_targets.len() != metadata.targets.len()
        || metadata
            .targets
            .iter()
            .any(|target| validate_target_segment(target).is_err())
    {
        return Err(SinkError::Blocked(
            "stored Redis view schema target inventory is invalid".to_owned(),
        ));
    }
    Ok(Some(metadata))
}

pub(super) async fn replace_view_schema_metadata(
    config: &RedisConfig,
    connection: &mut RedisConnection,
) -> Result<(), SinkError> {
    let Some(expected) = expected_view_schema_metadata(config)? else {
        return Ok(());
    };
    let key = view_schema_metadata_key(config)?;
    let expected = serde_json::to_string(&expected).map_err(|error| {
        SinkError::Blocked(format!(
            "unable to encode Redis view schema metadata: {error}"
        ))
    })?;
    redis::cmd("SET")
        .arg(key)
        .arg(expected)
        .query_async::<String>(&mut *connection)
        .await
        .map_err(|error| map_connect_error(&error, config))?;
    Ok(())
}
