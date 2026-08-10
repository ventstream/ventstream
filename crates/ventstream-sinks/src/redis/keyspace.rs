use std::borrow::Cow;

use ventstream_core::{Event, SinkError};

use super::config::{RedisConfig, RedisContract, RedisDocumentFormat, RedisKeyRouting};

pub(super) const DOC_ID_HEADER: &str = "ventstream.doc.id";
pub(super) const RELATION_HEADER: &str = "ventstream.cdc.relation";
pub(crate) const PROJECTION_TARGET_HEADER: &str = "ventstream.target.index";
pub(super) const TARGET_CLEAR_HEADER: &str = "ventstream.target.clear";

pub(super) fn key_parts<'a>(
    config: &'a RedisConfig,
    event: &'a Event,
) -> Result<(&'a str, &'a str), String> {
    let target = match &config.key_routing {
        RedisKeyRouting::ByOutputRelation => {
            event.headers.get(RELATION_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{RELATION_HEADER}` for Redis key routing",
                    event.id
                )
            })?
        }
        RedisKeyRouting::ByProjectionTarget => {
            event.headers.get(PROJECTION_TARGET_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{PROJECTION_TARGET_HEADER}` for Redis key routing",
                    event.id
                )
            })?
        }
        RedisKeyRouting::Fixed(target) => target,
        RedisKeyRouting::Views(_) => {
            return Err("Redis views routing must use the view materializer".to_owned());
        }
    };
    validate_target_segment(target)?;
    let doc_id = event.headers.get(DOC_ID_HEADER).ok_or_else(|| {
        format!(
            "event {} has no `{DOC_ID_HEADER}`; Redis materialization requires a stable document id",
            event.id
        )
    })?;
    if doc_id.is_empty() || doc_id.chars().any(char::is_control) {
        return Err(format!(
            "event {} has an empty or control-character `{DOC_ID_HEADER}`",
            event.id
        ));
    }
    Ok((target, doc_id))
}

pub(super) fn truncate_target<'a>(
    config: &'a RedisConfig,
    event: &'a Event,
) -> Result<&'a str, SinkError> {
    match &config.key_routing {
        RedisKeyRouting::ByOutputRelation => {
            event.headers.get(RELATION_HEADER).ok_or_else(|| {
                SinkError::Blocked(format!(
                    "truncate event {} has no `{RELATION_HEADER}` for Redis key routing",
                    event.id
                ))
            })
        }
        RedisKeyRouting::ByProjectionTarget => {
            if event.headers.get(TARGET_CLEAR_HEADER) != Some("true") {
                return Err(SinkError::Blocked(
                    "raw truncate events cannot safely clear by_projection_target Redis keys; route the source through SQL or memory projection handling"
                        .to_owned(),
                ));
            }
            event
                .headers
                .get(PROJECTION_TARGET_HEADER)
                .ok_or_else(|| {
                    SinkError::Blocked(format!(
                        "target-clear event {} has no `{PROJECTION_TARGET_HEADER}` for Redis key routing",
                        event.id
                    ))
                })
        }
        RedisKeyRouting::Fixed(_) => Err(SinkError::Blocked(
            "raw truncate events cannot safely clear a fixed Redis target because multiple source relations may share it; use by_output_relation or a join projection"
                .to_owned(),
        )),
        RedisKeyRouting::Views(_) => Err(SinkError::Blocked(
            "Redis views routing must use target-scoped view truncate handling".to_owned(),
        )),
    }
}

pub(crate) fn encoded_key_len(prefix: &str, target: &str, doc_id: &str) -> usize {
    prefix
        .len()
        .saturating_add(4)
        .saturating_add(encoded_segment_len(target))
        .saturating_add(encoded_segment_len(doc_id))
}

pub(super) fn staging_key_len(prefix: &str, target: &str, doc_id: &str) -> usize {
    prefix
        .len()
        .saturating_add(":__ventstream:stage:{".len())
        .saturating_add(encoded_segment_len(target))
        .saturating_add(2)
        .saturating_add(encoded_segment_len(doc_id))
}

pub(super) fn version_key_len(prefix: &str, target: &str, doc_id: &str) -> usize {
    internal_key_len(prefix, "version", target, doc_id)
}

pub(super) fn version_key(
    config: &RedisConfig,
    target: &str,
    doc_id: &str,
) -> Result<String, SinkError> {
    let key_len = version_key_len(&config.key_prefix, target, doc_id);
    if key_len > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis source-version key is {key_len} bytes; max_key_bytes={}",
            config.max_key_bytes
        )));
    }
    Ok(internal_key_from_parts(
        &config.key_prefix,
        "version",
        target,
        doc_id,
        key_len,
    ))
}

fn internal_key_len(prefix: &str, kind: &str, target: &str, id: &str) -> usize {
    prefix
        .len()
        .saturating_add(":__ventstream:".len())
        .saturating_add(kind.len())
        .saturating_add(2)
        .saturating_add(encoded_segment_len(target))
        .saturating_add(2)
        .saturating_add(encoded_segment_len(id))
}

fn internal_key_from_parts(
    prefix: &str,
    kind: &str,
    target: &str,
    id: &str,
    key_len: usize,
) -> String {
    let target = encode_key_segment(target);
    let id = encode_key_segment(id);
    let mut key = String::with_capacity(key_len);
    key.push_str(prefix);
    key.push_str(":__ventstream:");
    key.push_str(kind);
    key.push_str(":{");
    key.push_str(&target);
    key.push_str("}:");
    key.push_str(&id);
    key
}

pub(super) fn manifest_key(
    config: &RedisConfig,
    target: &str,
    source_id: &str,
) -> Result<String, SinkError> {
    let key_len = internal_key_len(&config.key_prefix, "manifest", target, source_id);
    if key_len > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis view manifest key is {key_len} bytes; max_key_bytes={}",
            config.max_key_bytes
        )));
    }
    Ok(internal_key_from_parts(
        &config.key_prefix,
        "manifest",
        target,
        source_id,
        key_len,
    ))
}

pub(super) fn owner_key(
    config: &RedisConfig,
    target: &str,
    key_id: &str,
) -> Result<String, SinkError> {
    let key_len = internal_key_len(&config.key_prefix, "owner", target, key_id);
    if key_len > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis view owner key is {key_len} bytes; max_key_bytes={}",
            config.max_key_bytes
        )));
    }
    Ok(internal_key_from_parts(
        &config.key_prefix,
        "owner",
        target,
        key_id,
        key_len,
    ))
}

pub(super) fn staging_key(
    config: &RedisConfig,
    target: &str,
    key_id: &str,
) -> Result<String, SinkError> {
    let key_len = staging_key_len(&config.key_prefix, target, key_id);
    if key_len > config.max_key_bytes {
        return Err(SinkError::Blocked(format!(
            "Redis view staging key is {key_len} bytes; max_key_bytes={}",
            config.max_key_bytes
        )));
    }
    Ok(staging_key_from_parts(
        &config.key_prefix,
        target,
        key_id,
        key_len,
    ))
}

pub(super) fn direct_clear_patterns(
    prefix: &str,
    target: &str,
    data_pattern_len: usize,
) -> Vec<String> {
    vec![
        clear_pattern(prefix, target, data_pattern_len),
        internal_clear_pattern(prefix, "stage", target),
        internal_clear_pattern(prefix, "version", target),
    ]
}

pub(super) fn view_clear_patterns(
    config: &RedisConfig,
    target: &str,
) -> Result<Vec<String>, SinkError> {
    let patterns = vec![
        clear_pattern(
            &config.key_prefix,
            target,
            clear_pattern_len(&config.key_prefix, target),
        ),
        internal_clear_pattern(&config.key_prefix, "stage", target),
        internal_clear_pattern(&config.key_prefix, "owner", target),
        internal_clear_pattern(&config.key_prefix, "manifest", target),
    ];
    if let Some(pattern) = patterns
        .iter()
        .find(|pattern| pattern.len() > config.max_key_bytes)
    {
        return Err(SinkError::Blocked(format!(
            "Redis view clear pattern is {} bytes; max_key_bytes={}",
            pattern.len(),
            config.max_key_bytes
        )));
    }
    Ok(patterns)
}

pub(super) struct RedisTargetPatterns {
    pub(super) data: String,
    pub(super) staging: String,
    pub(super) versions: String,
    pub(super) owners: String,
    pub(super) manifests: String,
}

pub(super) fn target_patterns(
    config: &RedisConfig,
    target: &str,
) -> Result<RedisTargetPatterns, SinkError> {
    validate_target_segment(target).map_err(SinkError::Blocked)?;
    let patterns = RedisTargetPatterns {
        data: clear_pattern(
            &config.key_prefix,
            target,
            clear_pattern_len(&config.key_prefix, target),
        ),
        staging: internal_clear_pattern(&config.key_prefix, "stage", target),
        versions: internal_clear_pattern(&config.key_prefix, "version", target),
        owners: internal_clear_pattern(&config.key_prefix, "owner", target),
        manifests: internal_clear_pattern(&config.key_prefix, "manifest", target),
    };
    for pattern in [
        &patterns.data,
        &patterns.staging,
        &patterns.versions,
        &patterns.owners,
        &patterns.manifests,
    ] {
        if pattern.len() > config.max_key_bytes {
            return Err(SinkError::Blocked(format!(
                "Redis target inspection pattern is {} bytes; max_key_bytes={}",
                pattern.len(),
                config.max_key_bytes
            )));
        }
    }
    Ok(patterns)
}

fn internal_clear_pattern(prefix: &str, kind: &str, target: &str) -> String {
    let target = encode_key_segment(target);
    format!("{prefix}:__ventstream:{kind}:{{{target}}}:*")
}

pub(super) fn uses_json_cache_staging(config: &RedisConfig) -> bool {
    config.document_format == RedisDocumentFormat::Json
        && matches!(config.contract, RedisContract::Cache { .. })
}

pub(crate) fn validate_target_segment(target: &str) -> Result<(), String> {
    if target.trim().is_empty()
        || target.trim() != target
        || target.len() > 1024
        || target.chars().any(char::is_control)
    {
        return Err(
            "Redis target must be trimmed, 1 to 1024 bytes, and contain no control characters"
                .to_owned(),
        );
    }
    Ok(())
}

pub(super) fn clear_pattern_len(prefix: &str, target: &str) -> usize {
    prefix
        .len()
        .saturating_add(5)
        .saturating_add(encoded_segment_len(target))
}

fn clear_pattern(prefix: &str, target: &str, pattern_len: usize) -> String {
    let target = encode_key_segment(target);
    let mut pattern = String::with_capacity(pattern_len);
    pattern.push_str(prefix);
    pattern.push_str(":{");
    pattern.push_str(&target);
    pattern.push_str("}:*");
    pattern
}

fn encoded_segment_len(input: &str) -> usize {
    input.bytes().fold(0usize, |length, byte| {
        length.saturating_add(
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                1
            } else {
                3
            },
        )
    })
}

pub(crate) fn key_from_parts(prefix: &str, target: &str, doc_id: &str, key_len: usize) -> String {
    let target = encode_key_segment(target);
    let doc_id = encode_key_segment(doc_id);
    let mut key = String::with_capacity(key_len);
    key.push_str(prefix);
    key.push_str(":{");
    key.push_str(&target);
    key.push_str("}:");
    key.push_str(&doc_id);
    key
}

pub(super) fn staging_key_from_parts(
    prefix: &str,
    target: &str,
    doc_id: &str,
    key_len: usize,
) -> String {
    let target = encode_key_segment(target);
    let doc_id = encode_key_segment(doc_id);
    let mut key = String::with_capacity(key_len);
    key.push_str(prefix);
    key.push_str(":__ventstream:stage:{");
    key.push_str(&target);
    key.push_str("}:");
    key.push_str(&doc_id);
    key
}

pub(super) fn writer_fence_key(prefix: &str, target: &str) -> String {
    let target = encode_key_segment(target);
    let mut key =
        String::with_capacity(prefix.len().saturating_add(target.len()).saturating_add(30));
    key.push_str(prefix);
    key.push_str(":__ventstream:writer:{");
    key.push_str(&target);
    key.push_str("}:current");
    key
}

pub(super) fn writer_lineage_key(prefix: &str, target: &str) -> String {
    let target = encode_key_segment(target);
    let mut key =
        String::with_capacity(prefix.len().saturating_add(target.len()).saturating_add(30));
    key.push_str(prefix);
    key.push_str(":__ventstream:writer:{");
    key.push_str(&target);
    key.push_str("}:lineage");
    key
}

pub(crate) fn encode_key_segment(input: &str) -> Cow<'_, str> {
    if input
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Cow::Borrowed(input);
    }
    let mut output = String::with_capacity(input.len().saturating_mul(3));
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            output.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(output, "%{byte:02X}");
        }
    }
    Cow::Owned(output)
}

pub(super) fn is_delete_event(event: &Event) -> bool {
    event.subject.as_str().rsplit('.').next() == Some("delete")
}

pub(super) fn is_truncate_event(event: &Event) -> bool {
    event.subject.as_str().rsplit('.').next() == Some("truncate")
}
