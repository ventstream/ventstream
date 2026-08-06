use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{self, Write};

use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use ventstream_core::Event;

use super::config::{
    RedisView, RedisViewConditionOperator, RedisViewFilterMode, RedisViewMissingBehavior,
    RedisViewValue,
};

const DOC_ID_HEADER: &str = "ventstream.doc.id";
const NAMESPACE_HEADER: &str = "ventstream.cdc.namespace";
const RELATION_HEADER: &str = "ventstream.cdc.relation";
const PROJECTION_TARGET_HEADER: &str = "ventstream.target.index";

const MAX_VIEWS: usize = 32;
const MAX_VIEW_NAME_BYTES: usize = 256;
const MAX_SELECTOR_BYTES: usize = 1_024;
const MAX_TEMPLATE_BYTES: usize = 4_096;
const MAX_TEMPLATE_PARTS: usize = 64;
const MAX_CONDITIONS: usize = 32;
const MAX_CONDITION_VALUES: usize = 64;
const MAX_POINTER_BYTES: usize = 2_048;
const MAX_VALUE_FIELDS: usize = 128;
const MAX_VALUE_FIELD_NAME_BYTES: usize = 256;

pub(super) fn validate_views(views: &[RedisView]) -> Result<(), String> {
    CompiledViews::compile(views).map(|_| ())
}

pub(super) struct CompiledViews {
    views: Vec<CompiledView>,
}

pub(super) struct ViewEstimateLimits<'a> {
    pub(super) key_prefix: &'a str,
    pub(super) max_key_bytes: usize,
    pub(super) max_value_bytes: usize,
    pub(super) max_batch_bytes: usize,
    pub(super) staging_keys: bool,
}

impl CompiledViews {
    pub(super) fn compile(views: &[RedisView]) -> Result<Self, String> {
        if views.is_empty() || views.len() > MAX_VIEWS {
            return Err(format!(
                "Redis views routing requires 1 to {MAX_VIEWS} views"
            ));
        }

        let mut names = HashSet::with_capacity(views.len());
        let mut compiled = Vec::with_capacity(views.len());
        for view in views {
            validate_view_name(&view.name)?;
            if !names.insert(view.name.as_str()) {
                return Err(format!("Redis view names must be unique: {}", view.name));
            }
            compiled.push(CompiledView::compile(view)?);
        }
        Ok(Self { views: compiled })
    }

    pub(super) fn matching<'a>(
        &'a self,
        event: &'a Event,
    ) -> impl Iterator<Item = &'a CompiledView> {
        self.views
            .iter()
            .filter(move |view| view.source_matches(event))
    }

    pub(super) fn estimate_event_bytes(
        &self,
        event: &Event,
        limits: &ViewEstimateLimits<'_>,
    ) -> usize {
        let source_id_bytes = event
            .headers
            .get(DOC_ID_HEADER)
            .map_or(limits.max_key_bytes, |source_id| {
                source_id.len().min(limits.max_key_bytes)
            });
        let key_count = if limits.staging_keys { 4usize } else { 3usize };
        let mut total = 0usize;
        let mut matched = false;
        for view in self.matching(event) {
            matched = true;
            let payload_bytes =
                view.estimated_payload_bytes(event.payload.len(), limits.max_value_bytes);
            total = total
                .saturating_add(view.estimated_key_bytes(
                    event,
                    limits.key_prefix,
                    source_id_bytes,
                    limits.max_key_bytes,
                    key_count,
                ))
                .saturating_add(source_id_bytes)
                .saturating_add(payload_bytes)
                .saturating_add(192)
                .min(limits.max_batch_bytes);
        }
        if matched {
            total.max(1)
        } else {
            1
        }
    }
}

pub(super) struct CompiledView {
    name: String,
    namespace: Option<String>,
    source: CompiledViewSource,
    key: CompiledTemplate,
    on_missing: RedisViewMissingBehavior,
    filter: Option<CompiledFilter>,
    value: RedisViewValue,
    requires_json: bool,
}

enum CompiledViewSource {
    Relation(String),
    ProjectionTarget(String),
}

struct CompiledFilter {
    mode: RedisViewFilterMode,
    conditions: Vec<super::config::RedisViewCondition>,
}

enum TemplatePart {
    Literal(String),
    DocumentId,
    Header(String),
    JsonPointer(String),
}

struct CompiledTemplate {
    parts: Vec<TemplatePart>,
}

#[derive(Debug)]
pub(super) enum ViewMaterialization {
    Remove,
    Upsert { key_id: String, payload: Bytes },
}

impl CompiledView {
    fn compile(view: &RedisView) -> Result<Self, String> {
        let source_count = usize::from(view.source.relation.is_some())
            + usize::from(view.source.projection_target.is_some());
        if source_count != 1 {
            return Err(format!(
                "Redis view {} source must define exactly one of relation or projection_target",
                view.name
            ));
        }
        validate_optional_text(
            &view.name,
            "source.namespace",
            view.source.namespace.as_deref(),
            MAX_SELECTOR_BYTES,
        )?;
        validate_optional_text(
            &view.name,
            "source.relation",
            view.source.relation.as_deref(),
            MAX_SELECTOR_BYTES,
        )?;
        validate_optional_text(
            &view.name,
            "source.projection_target",
            view.source.projection_target.as_deref(),
            MAX_SELECTOR_BYTES,
        )?;
        let source = match (
            view.source.relation.as_ref(),
            view.source.projection_target.as_ref(),
        ) {
            (Some(relation), None) => CompiledViewSource::Relation(relation.clone()),
            (None, Some(target)) => CompiledViewSource::ProjectionTarget(target.clone()),
            _ => unreachable!("source count validated"),
        };

        let key = CompiledTemplate::compile(&view.name, &view.key.template)?;
        let filter = view
            .filter
            .as_ref()
            .map(|filter| CompiledFilter::compile(&view.name, filter))
            .transpose()?;
        validate_view_value(&view.name, &view.value)?;
        let requires_json = key.requires_json()
            || filter.is_some()
            || !matches!(view.value, RedisViewValue::Document);

        Ok(Self {
            name: view.name.clone(),
            namespace: view.source.namespace.clone(),
            source,
            key,
            on_missing: view.key.on_missing,
            filter,
            value: view.value.clone(),
            requires_json,
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) const fn requires_json(&self) -> bool {
        self.requires_json
    }

    fn source_matches(&self, event: &Event) -> bool {
        if self
            .namespace
            .as_deref()
            .is_some_and(|namespace| event.headers.get(NAMESPACE_HEADER) != Some(namespace))
        {
            return false;
        }
        match &self.source {
            CompiledViewSource::Relation(relation) => {
                event.headers.get(RELATION_HEADER) == Some(relation)
            }
            CompiledViewSource::ProjectionTarget(target) => {
                event.headers.get(PROJECTION_TARGET_HEADER) == Some(target)
            }
        }
    }

    pub(super) fn materialize(
        &self,
        event: &Event,
        document: Option<&Value>,
        max_key_bytes: usize,
        max_payload_bytes: usize,
    ) -> Result<ViewMaterialization, String> {
        if self
            .filter
            .as_ref()
            .is_some_and(|filter| !filter.matches(document))
        {
            return Ok(ViewMaterialization::Remove);
        }

        let document_id = event.headers.get(DOC_ID_HEADER).ok_or_else(|| {
            format!(
                "event {} has no `{DOC_ID_HEADER}`; Redis views require a stable document id",
                event.id
            )
        })?;
        let key_id = match self
            .key
            .render(event, document_id, document, max_key_bytes)?
        {
            Some(key) => key,
            None if self.on_missing == RedisViewMissingBehavior::Skip => {
                return Ok(ViewMaterialization::Remove);
            }
            None => {
                return Err(format!(
                    "Redis view {} key template could not resolve every placeholder",
                    self.name
                ));
            }
        };
        if key_id.is_empty() {
            return Err(format!(
                "Redis view {} key template rendered an empty key",
                self.name
            ));
        }

        let payload = match &self.value {
            RedisViewValue::Document => {
                if event.payload.len() > max_payload_bytes {
                    return Err(format!(
                        "Redis view {} value exceeds the remaining per-event batch budget of {max_payload_bytes} bytes",
                        self.name
                    ));
                }
                event.payload.as_bytes()
            }
            RedisViewValue::Pointer(path) => {
                let selected = document
                    .and_then(|document| document.pointer(path))
                    .ok_or_else(|| {
                        format!(
                            "Redis view {} value pointer `{path}` did not resolve",
                            self.name
                        )
                    })?;
                serialize_bounded(selected, max_payload_bytes, &self.name)?
            }
            RedisViewValue::Fields(fields) => {
                let document = document
                    .ok_or_else(|| format!("Redis view {} requires a JSON document", self.name))?;
                let mut output = BTreeMap::new();
                for (name, path) in fields {
                    let selected = document.pointer(path).ok_or_else(|| {
                        format!(
                            "Redis view {} value field `{name}` pointer `{path}` did not resolve",
                            self.name
                        )
                    })?;
                    output.insert(name.as_str(), selected);
                }
                serialize_bounded(&output, max_payload_bytes, &self.name)?
            }
        };
        Ok(ViewMaterialization::Upsert { key_id, payload })
    }

    fn estimated_payload_bytes(&self, source_bytes: usize, max_value_bytes: usize) -> usize {
        let estimate = match &self.value {
            RedisViewValue::Document | RedisViewValue::Pointer(_) => source_bytes,
            RedisViewValue::Fields(fields) => fields.iter().fold(2usize, |total, (name, _)| {
                total
                    .saturating_add(source_bytes)
                    .saturating_add(name.len().saturating_mul(2))
                    .saturating_add(4)
            }),
        };
        estimate.min(max_value_bytes)
    }

    fn estimated_key_bytes(
        &self,
        event: &Event,
        key_prefix: &str,
        source_id_bytes: usize,
        max_key_bytes: usize,
        key_count: usize,
    ) -> usize {
        let key_id_bytes = self
            .key
            .estimated_rendered_bytes(event, source_id_bytes)
            .min(max_key_bytes);
        let fixed_bytes = key_prefix
            .len()
            .saturating_add(self.name.len().saturating_mul(3))
            .saturating_add(32);
        let data_key_bytes = fixed_bytes
            .saturating_add(key_id_bytes.saturating_mul(3))
            .min(max_key_bytes);
        let manifest_key_bytes = fixed_bytes
            .saturating_add(source_id_bytes.saturating_mul(3))
            .min(max_key_bytes);

        manifest_key_bytes
            .saturating_add(data_key_bytes.saturating_mul(key_count.saturating_sub(1)))
    }
}

impl CompiledTemplate {
    fn compile(view_name: &str, template: &str) -> Result<Self, String> {
        if template.is_empty()
            || template.len() > MAX_TEMPLATE_BYTES
            || template.chars().any(char::is_control)
        {
            return Err(format!(
                "Redis view {view_name} key template must be 1 to {MAX_TEMPLATE_BYTES} bytes with no control characters"
            ));
        }

        let mut parts = Vec::new();
        let mut remaining = template;
        let mut has_identity_placeholder = false;
        while let Some(start) = remaining.find("${") {
            if start > 0 {
                parts.push(TemplatePart::Literal(remaining[..start].to_owned()));
            }
            let expression = &remaining[start + 2..];
            let Some(end) = expression.find('}') else {
                return Err(format!(
                    "Redis view {view_name} key template has an unterminated placeholder"
                ));
            };
            let token = &expression[..end];
            let part = if token == "doc_id" {
                has_identity_placeholder = true;
                TemplatePart::DocumentId
            } else if let Some(header) = token.strip_prefix("header:") {
                validate_template_reference(view_name, "header", header)?;
                TemplatePart::Header(header.to_owned())
            } else if let Some(pointer) = token.strip_prefix("json:") {
                validate_json_pointer(view_name, "key template", pointer)?;
                has_identity_placeholder = true;
                TemplatePart::JsonPointer(pointer.to_owned())
            } else {
                return Err(format!(
                    "Redis view {view_name} key template contains unsupported placeholder `${{{token}}}`"
                ));
            };
            parts.push(part);
            remaining = &expression[end + 1..];
            if parts.len() > MAX_TEMPLATE_PARTS {
                return Err(format!(
                    "Redis view {view_name} key template exceeds {MAX_TEMPLATE_PARTS} parts"
                ));
            }
        }
        if !remaining.is_empty() {
            parts.push(TemplatePart::Literal(remaining.to_owned()));
        }
        if !has_identity_placeholder {
            return Err(format!(
                "Redis view {view_name} key template must include `${{doc_id}}` or a `${{json:/pointer}}` placeholder"
            ));
        }
        Ok(Self { parts })
    }

    fn requires_json(&self) -> bool {
        self.parts
            .iter()
            .any(|part| matches!(part, TemplatePart::JsonPointer(_)))
    }

    fn estimated_rendered_bytes(&self, event: &Event, document_id_bytes: usize) -> usize {
        self.parts.iter().fold(0usize, |total, part| {
            total.saturating_add(match part {
                TemplatePart::Literal(value) => value.len(),
                TemplatePart::DocumentId => document_id_bytes,
                TemplatePart::Header(name) => event.headers.get(name).map_or(0, str::len),
                TemplatePart::JsonPointer(_) => event.payload.len(),
            })
        })
    }

    fn render(
        &self,
        event: &Event,
        document_id: &str,
        document: Option<&Value>,
        max_bytes: usize,
    ) -> Result<Option<String>, String> {
        let mut output = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(value) => push_bounded(&mut output, value, max_bytes)?,
                TemplatePart::DocumentId => push_bounded(&mut output, document_id, max_bytes)?,
                TemplatePart::Header(name) => {
                    let Some(value) = event.headers.get(name) else {
                        return Ok(None);
                    };
                    push_bounded(&mut output, value, max_bytes)?;
                }
                TemplatePart::JsonPointer(path) => {
                    let Some(value) = document.and_then(|document| document.pointer(path)) else {
                        return Ok(None);
                    };
                    let Some(value) = scalar_text(value) else {
                        return Err(format!(
                            "Redis view key pointer `{path}` must resolve to a string, number, or boolean"
                        ));
                    };
                    push_bounded(&mut output, &value, max_bytes)?;
                }
            }
        }
        Ok(Some(output))
    }
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.limit)
        {
            self.exceeded = true;
            return Err(io::Error::other("Redis view value limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_bounded<T: Serialize>(
    value: &T,
    max_bytes: usize,
    view_name: &str,
) -> Result<Bytes, String> {
    let mut writer = BoundedWriter::new(max_bytes);
    let result = serde_json::to_writer(&mut writer, value);
    if writer.exceeded {
        return Err(format!(
            "Redis view {view_name} value exceeds the remaining per-event batch budget of {max_bytes} bytes"
        ));
    }
    result.map_err(|error| format!("unable to encode Redis view value: {error}"))?;
    Ok(Bytes::from(writer.bytes))
}

fn push_bounded(output: &mut String, value: &str, max_bytes: usize) -> Result<(), String> {
    if output
        .len()
        .checked_add(value.len())
        .is_none_or(|length| length > max_bytes)
    {
        return Err(format!(
            "Redis view key template exceeds the configured {max_bytes}-byte key limit"
        ));
    }
    output.push_str(value);
    Ok(())
}

impl CompiledFilter {
    fn compile(view_name: &str, filter: &super::config::RedisViewFilter) -> Result<Self, String> {
        if filter.conditions.is_empty() || filter.conditions.len() > MAX_CONDITIONS {
            return Err(format!(
                "Redis view {view_name} filter requires 1 to {MAX_CONDITIONS} conditions"
            ));
        }
        for condition in &filter.conditions {
            validate_json_pointer(view_name, "filter", &condition.path)?;
            match &condition.operator {
                RedisViewConditionOperator::Equals(value)
                | RedisViewConditionOperator::NotEquals(value) => {
                    validate_condition_value(view_name, value)?;
                }
                RedisViewConditionOperator::In(values)
                | RedisViewConditionOperator::NotIn(values) => {
                    if values.is_empty() || values.len() > MAX_CONDITION_VALUES {
                        return Err(format!(
                            "Redis view {view_name} filter membership requires 1 to {MAX_CONDITION_VALUES} values"
                        ));
                    }
                    for value in values {
                        validate_condition_value(view_name, value)?;
                    }
                }
                RedisViewConditionOperator::Exists | RedisViewConditionOperator::NotExists => {}
            }
        }
        Ok(Self {
            mode: filter.mode,
            conditions: filter.conditions.clone(),
        })
    }

    fn matches(&self, document: Option<&Value>) -> bool {
        let matches = |condition: &super::config::RedisViewCondition| {
            let selected = document.and_then(|document| document.pointer(&condition.path));
            match &condition.operator {
                RedisViewConditionOperator::Equals(value) => selected == Some(value),
                RedisViewConditionOperator::NotEquals(value) => selected != Some(value),
                RedisViewConditionOperator::In(values) => {
                    selected.is_some_and(|selected| values.contains(selected))
                }
                RedisViewConditionOperator::NotIn(values) => {
                    selected.is_none_or(|selected| !values.contains(selected))
                }
                RedisViewConditionOperator::Exists => selected.is_some(),
                RedisViewConditionOperator::NotExists => selected.is_none(),
            }
        };
        match self.mode {
            RedisViewFilterMode::All => self.conditions.iter().all(matches),
            RedisViewFilterMode::Any => self.conditions.iter().any(matches),
        }
    }
}

fn validate_view_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty()
        || name.trim() != name
        || name.len() > MAX_VIEW_NAME_BYTES
        || name.starts_with("__ventstream")
        || name.chars().any(char::is_control)
    {
        return Err(format!(
            "Redis view name must be trimmed, 1 to {MAX_VIEW_NAME_BYTES} bytes, contain no control characters, and not use the reserved `__ventstream` prefix"
        ));
    }
    Ok(())
}

fn validate_optional_text(
    view_name: &str,
    field: &str,
    value: Option<&str>,
    max_bytes: usize,
) -> Result<(), String> {
    if value.is_some_and(|value| {
        value.trim().is_empty()
            || value.trim() != value
            || value.len() > max_bytes
            || value.chars().any(char::is_control)
    }) {
        return Err(format!(
            "Redis view {view_name} {field} must be trimmed, 1 to {max_bytes} bytes, and contain no control characters"
        ));
    }
    Ok(())
}

fn validate_template_reference(view_name: &str, kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_POINTER_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "Redis view {view_name} {kind} reference must be 1 to {MAX_POINTER_BYTES} bytes with no control characters"
        ));
    }
    Ok(())
}

fn validate_json_pointer(view_name: &str, field: &str, pointer: &str) -> Result<(), String> {
    if pointer.len() > MAX_POINTER_BYTES
        || (!pointer.is_empty() && !pointer.starts_with('/'))
        || pointer.chars().any(char::is_control)
    {
        return Err(format!(
            "Redis view {view_name} {field} must use an RFC 6901 JSON pointer up to {MAX_POINTER_BYTES} bytes"
        ));
    }
    let mut bytes = pointer.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
            return Err(format!(
                "Redis view {view_name} {field} contains an invalid JSON pointer escape"
            ));
        }
    }
    Ok(())
}

fn validate_condition_value(view_name: &str, value: &Value) -> Result<(), String> {
    if matches!(value, Value::Array(_) | Value::Object(_)) {
        return Err(format!(
            "Redis view {view_name} filter values must be JSON scalars"
        ));
    }
    Ok(())
}

fn validate_view_value(view_name: &str, value: &RedisViewValue) -> Result<(), String> {
    match value {
        RedisViewValue::Document => Ok(()),
        RedisViewValue::Pointer(path) => validate_json_pointer(view_name, "value pointer", path),
        RedisViewValue::Fields(fields) => {
            if fields.is_empty() || fields.len() > MAX_VALUE_FIELDS {
                return Err(format!(
                    "Redis view {view_name} value fields must contain 1 to {MAX_VALUE_FIELDS} entries"
                ));
            }
            let mut names = BTreeSet::new();
            for (name, path) in fields {
                if name.trim().is_empty()
                    || name.trim() != name
                    || name.len() > MAX_VALUE_FIELD_NAME_BYTES
                    || name.chars().any(char::is_control)
                {
                    return Err(format!(
                        "Redis view {view_name} value field names must be trimmed, 1 to {MAX_VALUE_FIELD_NAME_BYTES} bytes, and contain no control characters"
                    ));
                }
                if !names.insert(name) {
                    return Err(format!(
                        "Redis view {view_name} value field names must be unique"
                    ));
                }
                validate_json_pointer(view_name, "value field pointer", path)?;
            }
            Ok(())
        }
    }
}

fn scalar_text(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(value) => Some(Cow::Borrowed(value)),
        Value::Number(value) => Some(Cow::Owned(value.to_string())),
        Value::Bool(value) => Some(Cow::Owned(value.to_string())),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;

    use ventstream_core::{Headers, Payload, SourceUri, Subject};

    use super::*;
    use crate::redis::config::{
        RedisViewCondition, RedisViewFilter, RedisViewKey, RedisViewSource,
    };

    fn view() -> RedisView {
        RedisView {
            name: "open_order_by_id".to_owned(),
            source: RedisViewSource {
                namespace: Some("public".to_owned()),
                relation: Some("orders".to_owned()),
                projection_target: None,
            },
            key: RedisViewKey {
                template: "order:${json:/id}".to_owned(),
                on_missing: RedisViewMissingBehavior::Block,
            },
            filter: Some(RedisViewFilter {
                mode: RedisViewFilterMode::All,
                conditions: vec![RedisViewCondition {
                    path: "/status".to_owned(),
                    operator: RedisViewConditionOperator::In(vec![
                        Value::String("pending".to_owned()),
                        Value::String("processing".to_owned()),
                    ]),
                }],
            }),
            value: RedisViewValue::Fields(BTreeMap::from([
                ("id".to_owned(), "/id".to_owned()),
                ("status".to_owned(), "/status".to_owned()),
            ])),
        }
    }

    fn event(payload: &str) -> Event {
        Event::builder(
            SourceUri::new("postgres://shop.public.orders").expect("source"),
            Subject::new("postgres.public.orders.upsert").expect("subject"),
        )
        .payload(Payload::from_vec(payload.as_bytes().to_vec()))
        .headers(
            Headers::empty()
                .with_header(
                    DOC_ID_HEADER.to_owned(),
                    "public.orders:[\"ord-1\"]".to_owned(),
                )
                .with_header(NAMESPACE_HEADER.to_owned(), "public".to_owned())
                .with_header(RELATION_HEADER.to_owned(), "orders".to_owned()),
        )
        .build()
    }

    #[test]
    fn view_filters_renders_and_projects_fields() {
        let compiled = CompiledViews::compile(&[view()]).expect("compile view");
        let event = event(r#"{"id":"ord-1","status":"pending","ignored":true}"#);
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");
        let selected = compiled.matching(&event).next().expect("matching view");
        let materialization = selected
            .materialize(&event, Some(&document), 16 * 1024, 8 * 1024 * 1024)
            .expect("materialize");
        let upsert = match materialization {
            ViewMaterialization::Upsert { key_id, payload } => Some((key_id, payload)),
            ViewMaterialization::Remove => None,
        };
        assert!(upsert.is_some());
        let Some((key_id, payload)) = upsert else {
            return;
        };
        assert_eq!(key_id, "order:ord-1");
        assert_eq!(
            serde_json::from_slice::<Value>(&payload).expect("projected JSON"),
            serde_json::json!({"id":"ord-1","status":"pending"})
        );
    }

    #[test]
    fn filter_transition_removes_the_previous_view_entry() {
        let compiled = CompiledViews::compile(&[view()]).expect("compile view");
        let event = event(r#"{"id":"ord-1","status":"complete"}"#);
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");
        let selected = compiled.matching(&event).next().expect("matching view");
        assert!(matches!(
            selected
                .materialize(&event, Some(&document), 16 * 1024, 8 * 1024 * 1024)
                .expect("materialize"),
            ViewMaterialization::Remove
        ));
    }

    #[test]
    fn invalid_or_ambiguous_views_are_rejected() {
        let mut invalid = view();
        invalid.source.projection_target = Some("orders".to_owned());
        assert!(CompiledViews::compile(&[invalid]).is_err());

        let mut constant = view();
        constant.key.template = "orders".to_owned();
        assert!(CompiledViews::compile(&[constant]).is_err());

        let duplicate = view();
        assert!(CompiledViews::compile(&[duplicate.clone(), duplicate]).is_err());
    }

    #[test]
    fn missing_key_fields_follow_the_explicit_policy() {
        let mut configured = view();
        configured.filter = None;
        configured.key.on_missing = RedisViewMissingBehavior::Skip;
        let compiled = CompiledViews::compile(&[configured]).expect("compile view");
        let event = event(r#"{"status":"pending"}"#);
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");
        assert!(matches!(
            compiled
                .matching(&event)
                .next()
                .expect("matching view")
                .materialize(&event, Some(&document), 16 * 1024, 8 * 1024 * 1024)
                .expect("skip missing key"),
            ViewMaterialization::Remove
        ));
    }

    #[test]
    fn filter_operators_have_explicit_missing_value_semantics() {
        let document = serde_json::json!({
            "id": "ord-1",
            "status": "pending",
            "tier": "gold",
            "region": "eu"
        });
        let filter = CompiledFilter::compile(
            "orders",
            &RedisViewFilter {
                mode: RedisViewFilterMode::All,
                conditions: vec![
                    RedisViewCondition {
                        path: "/status".to_owned(),
                        operator: RedisViewConditionOperator::Equals(serde_json::json!("pending")),
                    },
                    RedisViewCondition {
                        path: "/status".to_owned(),
                        operator: RedisViewConditionOperator::NotEquals(serde_json::json!(
                            "failed"
                        )),
                    },
                    RedisViewCondition {
                        path: "/tier".to_owned(),
                        operator: RedisViewConditionOperator::In(vec![
                            serde_json::json!("gold"),
                            serde_json::json!("platinum"),
                        ]),
                    },
                    RedisViewCondition {
                        path: "/region".to_owned(),
                        operator: RedisViewConditionOperator::NotIn(vec![serde_json::json!(
                            "blocked"
                        )]),
                    },
                    RedisViewCondition {
                        path: "/id".to_owned(),
                        operator: RedisViewConditionOperator::Exists,
                    },
                    RedisViewCondition {
                        path: "/deleted_at".to_owned(),
                        operator: RedisViewConditionOperator::NotExists,
                    },
                ],
            },
        )
        .expect("compile filter");
        assert!(filter.matches(Some(&document)));

        let any = CompiledFilter::compile(
            "orders",
            &RedisViewFilter {
                mode: RedisViewFilterMode::Any,
                conditions: vec![
                    RedisViewCondition {
                        path: "/status".to_owned(),
                        operator: RedisViewConditionOperator::Equals(serde_json::json!("failed")),
                    },
                    RedisViewCondition {
                        path: "/deleted_at".to_owned(),
                        operator: RedisViewConditionOperator::Exists,
                    },
                ],
            },
        )
        .expect("compile any filter");
        assert!(!any.matches(Some(&document)));
    }

    #[test]
    fn templates_support_headers_document_ids_and_escaped_json_pointers() {
        let mut configured = view();
        configured.filter = None;
        configured.key.template =
            "tenant:${header:tenant}:lookup:${json:/a~1b/~0key}:${doc_id}".to_owned();
        configured.value = RedisViewValue::Pointer("/a~1b".to_owned());
        let compiled = CompiledViews::compile(&[configured]).expect("compile view");
        let mut event = event(r#"{"a/b":{"~key":7}}"#);
        event.headers = event
            .headers
            .clone()
            .with_header("tenant".to_owned(), "shop".to_owned());
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");
        let materialization = compiled
            .matching(&event)
            .next()
            .expect("matching view")
            .materialize(&event, Some(&document), 16 * 1024, 8 * 1024 * 1024)
            .expect("materialize");
        let upsert = match materialization {
            ViewMaterialization::Upsert { key_id, payload } => Some((key_id, payload)),
            ViewMaterialization::Remove => None,
        };
        assert!(upsert.is_some());
        let Some((key_id, payload)) = upsert else {
            return;
        };
        assert_eq!(key_id, "tenant:shop:lookup:7:public.orders:[\"ord-1\"]");
        assert_eq!(
            serde_json::from_slice::<Value>(&payload).expect("pointer value"),
            serde_json::json!({"~key": 7})
        );
    }

    #[test]
    fn projection_sources_and_template_complexity_are_bounded() {
        let mut projection = view();
        projection.source.namespace = None;
        projection.source.relation = None;
        projection.source.projection_target = Some("orders_projection".to_owned());
        let compiled = CompiledViews::compile(&[projection]).expect("projection view");
        let mut projected = event(r#"{"id":"ord-1","status":"pending"}"#);
        projected.headers = projected.headers.clone().with_header(
            PROJECTION_TARGET_HEADER.to_owned(),
            "orders_projection".to_owned(),
        );
        assert_eq!(compiled.matching(&projected).count(), 1);

        let mut oversized = view();
        oversized.key.template = (0..33)
            .map(|index| format!("part-{index}:${{doc_id}}"))
            .collect::<String>();
        assert!(CompiledViews::compile(&[oversized]).is_err());
    }

    #[test]
    fn key_rendering_stops_before_large_placeholders_amplify_memory() {
        let mut configured = view();
        configured.filter = None;
        configured.key.template = "${json:/large}:${json:/large}".to_owned();
        configured.value = RedisViewValue::Document;
        let compiled = CompiledViews::compile(&[configured]).expect("compile view");
        let event = event(&serde_json::json!({"large": "x".repeat(1024)}).to_string());
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");

        let error = compiled
            .matching(&event)
            .next()
            .expect("matching view")
            .materialize(&event, Some(&document), 128, 8 * 1024 * 1024)
            .expect_err("expanded key must be bounded while rendering");

        assert!(error.contains("128-byte key limit"));
    }

    #[test]
    fn field_projection_stops_before_duplicate_subtrees_exceed_the_value_budget() {
        let mut configured = view();
        configured.filter = None;
        configured.key.template = "order:${doc_id}".to_owned();
        configured.value = RedisViewValue::Fields(BTreeMap::from([
            ("first".to_owned(), "/large".to_owned()),
            ("second".to_owned(), "/large".to_owned()),
        ]));
        let compiled = CompiledViews::compile(&[configured]).expect("compile view");
        let event = event(&serde_json::json!({"large": "x".repeat(96)}).to_string());
        let document: Value = serde_json::from_slice(event.payload.as_slice()).expect("JSON");

        let error = compiled
            .matching(&event)
            .next()
            .expect("matching view")
            .materialize(&event, Some(&document), 16 * 1024, 128)
            .expect_err("duplicated fields must respect the output budget");

        assert!(error.contains("128 bytes"));
    }

    #[test]
    fn bounded_serialization_does_not_preallocate_the_value_limit() {
        let writer = BoundedWriter::new(8 * 1024 * 1024);
        assert!(writer.bytes.capacity() <= 1024);
    }

    #[test]
    fn view_estimate_accounts_for_projection_fanout() {
        let compiled = CompiledViews::compile(&[view()]).expect("compile view");
        let event = event(r#"{"id":"ord-1","status":"pending"}"#);
        let estimate = compiled.estimate_event_bytes(
            &event,
            &ViewEstimateLimits {
                key_prefix: "ventstream:test",
                max_key_bytes: 16 * 1024,
                max_value_bytes: 8 * 1024 * 1024,
                max_batch_bytes: 16 * 1024 * 1024,
                staging_keys: false,
            },
        );

        assert!(estimate > event.payload.len().saturating_mul(2));
        assert!(estimate < 8 * 1024);
    }

    #[test]
    fn ordinary_multi_view_batches_fit_the_dispatch_budget() {
        let mut second = view();
        second.name = "order_by_customer".to_owned();
        second.key.template = "customer:${json:/customer_id}:order:${json:/id}".to_owned();
        second.filter = None;
        second.value = RedisViewValue::Document;
        let compiled = CompiledViews::compile(&[view(), second]).expect("compile views");
        let event = event(r#"{"id":"ord-1","customer_id":"customer-1","status":"pending"}"#);
        let estimate = compiled.estimate_event_bytes(
            &event,
            &ViewEstimateLimits {
                key_prefix: "ventstream:test",
                max_key_bytes: 16 * 1024,
                max_value_bytes: 8 * 1024 * 1024,
                max_batch_bytes: 16 * 1024 * 1024,
                staging_keys: false,
            },
        );

        assert!(estimate.saturating_mul(1_000) < 16 * 1024 * 1024);
    }
}
