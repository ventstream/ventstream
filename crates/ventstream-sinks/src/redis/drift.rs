use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use ventstream_core::SinkError;

use super::config::{RedisConfig, RedisKeyRouting};
use super::error::map_connect_error;
use super::keyspace::{manifest_key, target_patterns, writer_fence_key};
use super::topology::{build_connector, connect_raw, RedisConnection};

const MAX_SCAN_LIMIT: usize = 1_000_000;
const SCAN_COUNT: usize = 1_000;
const READ_BATCH: usize = 256;

#[derive(Debug, Clone, Serialize)]
/// Bounded, read-only consistency report for Redis materializations.
pub struct RedisDriftReport {
    /// Maximum keys examined in each target and key class.
    pub scan_limit_per_key_class: usize,
    /// Whether every requested scan reached cursor zero.
    pub complete: bool,
    /// Whether the completed structural scan found no anomalies.
    pub consistent: bool,
    /// Whether authoritative source comparison is still needed to detect missing rows.
    pub source_comparison_required: bool,
    /// Per-target consistency results.
    pub targets: Vec<RedisTargetDrift>,
}

#[derive(Debug, Clone, Serialize)]
/// Structural consistency counts for one Redis routing target.
pub struct RedisTargetDrift {
    /// Decoded routing target from configuration.
    pub target: String,
    /// Whether every key-class scan for this target completed.
    pub scan_complete: bool,
    /// Whether the target currently has a writer-generation key.
    pub writer_fence_present: bool,
    /// Materialized data keys observed.
    pub data_keys: usize,
    /// Durable source-ordering records observed for direct materializations.
    pub version_keys: usize,
    /// View manifest keys observed.
    pub manifest_keys: usize,
    /// View ownership keys observed.
    pub owner_keys: usize,
    /// Temporary RedisJSON staging keys observed.
    pub staging_keys: usize,
    /// Manifests missing their data-key or owner-key field.
    pub incomplete_manifests: usize,
    /// Complete manifests that reference a missing materialized value.
    pub missing_values: usize,
    /// Complete manifests that reference a missing owner record.
    pub missing_owners: usize,
    /// Owner records whose source identity does not match the manifest.
    pub owner_mismatches: usize,
    /// Owner records with no matching manifest.
    pub orphan_owners: usize,
    /// View values with no matching owner record.
    pub unowned_values: usize,
    /// Whether this target needs an authoritative source rebootstrap.
    pub requires_rebootstrap: bool,
}

impl RedisTargetDrift {
    fn anomaly_count(&self) -> usize {
        self.staging_keys
            .saturating_add(self.incomplete_manifests)
            .saturating_add(self.missing_values)
            .saturating_add(self.missing_owners)
            .saturating_add(self.owner_mismatches)
            .saturating_add(self.orphan_owners)
            .saturating_add(self.unowned_values)
    }
}

pub(super) async fn inspect(
    config: &RedisConfig,
    targets: &[String],
    scan_limit: usize,
) -> Result<RedisDriftReport, SinkError> {
    if targets.is_empty() {
        return Err(SinkError::Blocked(
            "Redis drift inspection requires at least one target".to_owned(),
        ));
    }
    if scan_limit == 0 || scan_limit > MAX_SCAN_LIMIT {
        return Err(SinkError::Blocked(format!(
            "Redis drift scan limit must be between 1 and {MAX_SCAN_LIMIT}"
        )));
    }
    config.validate().map_err(SinkError::Blocked)?;
    let connector = build_connector(config).await?;
    let mut connection = connect_raw(&connector, config).await?;
    let view_routing = matches!(config.key_routing, RedisKeyRouting::Views(_));
    let unique_targets = targets.iter().cloned().collect::<BTreeSet<_>>();
    let mut reports = Vec::with_capacity(unique_targets.len());
    for target in &unique_targets {
        reports
            .push(inspect_target(config, &mut connection, target, scan_limit, view_routing).await?);
    }
    let complete = reports.iter().all(|report| report.scan_complete);
    let consistent = complete && reports.iter().all(|report| report.anomaly_count() == 0);
    Ok(RedisDriftReport {
        scan_limit_per_key_class: scan_limit,
        complete,
        consistent,
        source_comparison_required: true,
        targets: reports,
    })
}

async fn inspect_target(
    config: &RedisConfig,
    connection: &mut RedisConnection,
    target: &str,
    scan_limit: usize,
    view_routing: bool,
) -> Result<RedisTargetDrift, SinkError> {
    let patterns = target_patterns(config, target)?;
    let routing_key = writer_fence_key(&config.key_prefix, target);
    let (data, data_complete) =
        scan_keys(config, connection, &patterns.data, &routing_key, scan_limit).await?;
    let (staging, staging_complete) = scan_keys(
        config,
        connection,
        &patterns.staging,
        &routing_key,
        scan_limit,
    )
    .await?;
    let (versions, versions_complete) = scan_keys(
        config,
        connection,
        &patterns.versions,
        &routing_key,
        scan_limit,
    )
    .await?;
    let writer_fence_present = exists(config, connection, &routing_key, &routing_key).await?;

    if !view_routing {
        return Ok(RedisTargetDrift {
            target: target.to_owned(),
            scan_complete: data_complete && staging_complete && versions_complete,
            writer_fence_present,
            data_keys: data.len(),
            version_keys: versions.len(),
            manifest_keys: 0,
            owner_keys: 0,
            staging_keys: staging.len(),
            incomplete_manifests: 0,
            missing_values: 0,
            missing_owners: 0,
            owner_mismatches: 0,
            orphan_owners: 0,
            unowned_values: 0,
            requires_rebootstrap: !staging.is_empty(),
        });
    }

    let (owners, owners_complete) = scan_keys(
        config,
        connection,
        &patterns.owners,
        &routing_key,
        scan_limit,
    )
    .await?;
    let (manifests, manifests_complete) = scan_keys(
        config,
        connection,
        &patterns.manifests,
        &routing_key,
        scan_limit,
    )
    .await?;
    let scan_complete = data_complete
        && staging_complete
        && versions_complete
        && owners_complete
        && manifests_complete;

    let manifest_records = read_manifest_records(config, connection, &manifests).await?;
    let owner_values = read_string_values(config, connection, &owners).await?;
    let data_set = data.iter().cloned().collect::<BTreeSet<_>>();
    let owner_set = owners.iter().cloned().collect::<BTreeSet<_>>();
    let manifest_set = manifests.iter().cloned().collect::<BTreeSet<_>>();
    let data_prefix = patterns.data.strip_suffix('*').unwrap_or(&patterns.data);
    let owner_prefix = patterns
        .owners
        .strip_suffix('*')
        .unwrap_or(&patterns.owners);

    let mut incomplete_manifests = 0usize;
    let mut missing_values = 0usize;
    let mut missing_owners = 0usize;
    let mut owner_mismatches = 0usize;
    for (manifest, fields) in &manifest_records {
        let (data_key, owner_key) = match (&fields.data_key, &fields.owner_key) {
            (Some(data_key), Some(owner_key)) => (data_key, owner_key),
            (None, None)
                if fields.source_version.as_ref().is_some_and(|version| {
                    !version.is_empty() && version.bytes().all(|byte| byte.is_ascii_digit())
                }) =>
            {
                continue;
            }
            _ => {
                incomplete_manifests = incomplete_manifests.saturating_add(1);
                continue;
            }
        };
        if scan_complete && !data_set.contains(data_key) {
            missing_values = missing_values.saturating_add(1);
        }
        let Some(source_id) = owner_values.get(owner_key).and_then(|value| value.as_ref()) else {
            missing_owners = missing_owners.saturating_add(1);
            continue;
        };
        let expected_manifest = manifest_key(config, target, source_id)?;
        if &expected_manifest != manifest {
            owner_mismatches = owner_mismatches.saturating_add(1);
        }
    }

    let mut orphan_owners = 0usize;
    for (owner, source_id) in &owner_values {
        let Some(source_id) = source_id else {
            orphan_owners = orphan_owners.saturating_add(1);
            continue;
        };
        let expected_manifest = manifest_key(config, target, source_id)?;
        let manifest_matches = manifest_records
            .get(&expected_manifest)
            .and_then(|record| record.owner_key.as_ref())
            == Some(owner);
        if scan_complete && (!manifest_set.contains(&expected_manifest) || !manifest_matches) {
            orphan_owners = orphan_owners.saturating_add(1);
        }
    }

    let unowned_values = if scan_complete {
        data.iter()
            .filter(|data_key| {
                data_key
                    .strip_prefix(data_prefix)
                    .is_some_and(|suffix| !owner_set.contains(&format!("{owner_prefix}{suffix}")))
            })
            .count()
    } else {
        0
    };
    let anomaly_count = staging
        .len()
        .saturating_add(incomplete_manifests)
        .saturating_add(missing_values)
        .saturating_add(missing_owners)
        .saturating_add(owner_mismatches)
        .saturating_add(orphan_owners)
        .saturating_add(unowned_values);

    Ok(RedisTargetDrift {
        target: target.to_owned(),
        scan_complete,
        writer_fence_present,
        data_keys: data.len(),
        version_keys: versions.len(),
        manifest_keys: manifests.len(),
        owner_keys: owners.len(),
        staging_keys: staging.len(),
        incomplete_manifests,
        missing_values,
        missing_owners,
        owner_mismatches,
        orphan_owners,
        unowned_values,
        requires_rebootstrap: anomaly_count > 0,
    })
}

struct ManifestRecord {
    data_key: Option<String>,
    owner_key: Option<String>,
    source_version: Option<String>,
}

async fn scan_keys(
    config: &RedisConfig,
    connection: &mut RedisConnection,
    pattern: &str,
    routing_key: &str,
    limit: usize,
) -> Result<(Vec<String>, bool), SinkError> {
    let mut keys = BTreeSet::new();
    let mut cursor = 0u64;
    loop {
        let mut command = redis::cmd("SCAN");
        command
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(SCAN_COUNT);
        let (next, page) = connection
            .query_on_primary::<(u64, Vec<String>)>(command, routing_key)
            .await
            .map_err(|error| map_connect_error(&error, config))?;
        let page_len = page.len();
        for (index, key) in page.into_iter().enumerate() {
            keys.insert(key);
            if keys.len() >= limit {
                let complete = next == 0 && index.saturating_add(1) == page_len;
                return Ok((keys.into_iter().collect(), complete));
            }
        }
        cursor = next;
        if cursor == 0 {
            return Ok((keys.into_iter().collect(), true));
        }
    }
}

async fn exists(
    config: &RedisConfig,
    connection: &mut RedisConnection,
    key: &str,
    routing_key: &str,
) -> Result<bool, SinkError> {
    let mut command = redis::cmd("EXISTS");
    command.arg(key);
    connection
        .query_on_primary(command, routing_key)
        .await
        .map_err(|error| map_connect_error(&error, config))
}

async fn read_manifest_records(
    config: &RedisConfig,
    connection: &mut RedisConnection,
    keys: &[String],
) -> Result<BTreeMap<String, ManifestRecord>, SinkError> {
    let mut records = BTreeMap::new();
    for keys in keys.chunks(READ_BATCH) {
        let mut pipeline = redis::pipe();
        for key in keys {
            pipeline
                .cmd("HMGET")
                .arg(key)
                .arg("key")
                .arg("owner")
                .arg("version");
        }
        let rows = pipeline
            .query_async::<Vec<Vec<Option<String>>>>(&mut *connection)
            .await
            .map_err(|error| map_connect_error(&error, config))?;
        if rows.len() != keys.len() {
            return Err(SinkError::Blocked(
                "Redis manifest inspection returned an invalid response count".to_owned(),
            ));
        }
        for (key, fields) in keys.iter().zip(rows) {
            records.insert(
                key.clone(),
                ManifestRecord {
                    data_key: fields.first().cloned().flatten(),
                    owner_key: fields.get(1).cloned().flatten(),
                    source_version: fields.get(2).cloned().flatten(),
                },
            );
        }
    }
    Ok(records)
}

async fn read_string_values(
    config: &RedisConfig,
    connection: &mut RedisConnection,
    keys: &[String],
) -> Result<BTreeMap<String, Option<String>>, SinkError> {
    let mut values = BTreeMap::new();
    for keys in keys.chunks(READ_BATCH) {
        let Some(routing_key) = keys.first() else {
            continue;
        };
        let mut command = redis::cmd("MGET");
        command.arg(keys);
        let batch = connection
            .query_on_primary::<Vec<Option<String>>>(command, routing_key)
            .await
            .map_err(|error| map_connect_error(&error, config))?;
        if batch.len() != keys.len() {
            return Err(SinkError::Blocked(
                "Redis owner inspection returned an invalid response count".to_owned(),
            ));
        }
        values.extend(keys.iter().cloned().zip(batch));
    }
    Ok(values)
}
