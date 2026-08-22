//! Event → SurrealDB statement translation: record-id derivation, table
//! routing, and grouping a dispatcher batch into ordered statement runs.
//!
//! Everything on the data path rides RPC bind variables — no document
//! value is ever interpolated into SurrealQL text.

use serde_json::Value;
use ventstream_core::{Event, FailedItem};

use super::config::{SurrealDbConfig, SurrealTableRouting};

pub(super) const DOC_ID_HEADER: &str = "ventstream.doc.id";
pub(super) const RELATION_HEADER: &str = "ventstream.cdc.relation";
pub(super) const PROJECTION_TARGET_HEADER: &str = "ventstream.target.index";
/// Canonical doc id retained on the document for reverse lookup and
/// debugging. The record id itself is derived from it, but string
/// filtering on this field needs no id parsing.
pub(crate) const CANONICAL_ID_FIELD: &str = "_vs_id";
/// A source column named `id` collides with SurrealDB's record-id field
/// and is preserved under this name instead.
pub(crate) const SOURCE_ID_FIELD: &str = "source_id";

/// The record-id component passed to `type::record($tb, $id)`.
///
/// Canonical doc ids are `table:["pk",…]`; the JSON key array maps onto
/// SurrealDB's native array record ids, so composite keys round-trip
/// without encoding. Opaque suffixes (Neo4j elementIds, denormalize
/// ids) become single-element arrays.
///
/// The component is ALWAYS an array, never a bare string: inside a
/// query's param path, `type::record` coerces strings by PARSING them
/// as record-id literals — `"products:4:uuid:7"` collapses to id `4`,
/// silently merging every document whose id shares that prefix. Array
/// components are taken verbatim.
pub(crate) fn record_id_component(doc_id: &str) -> Value {
    if let Some((_, keys)) = doc_id.split_once(':') {
        if let Ok(parsed) = serde_json::from_str::<Value>(keys) {
            if parsed.is_array() {
                return parsed;
            }
        }
        return serde_json::json!([keys]);
    }
    serde_json::json!([doc_id])
}

/// Routing-aware record id. Fixed routing funnels every relation into
/// one table, so the table qualifier the canonical id carries is the
/// only thing keeping `users:["1"]` and `orders:["1"]` apart — the id
/// must stay fully qualified there or the relations silently merge
/// (and delete each other). Relation-per-table routing re-supplies the
/// qualifier through the table itself, so the bare key array is both
/// safe and natural.
pub(crate) fn routed_record_id(config: &SurrealDbConfig, doc_id: &str) -> Value {
    if matches!(config.table_routing, SurrealTableRouting::Fixed(_)) {
        return serde_json::json!([doc_id]);
    }
    record_id_component(doc_id)
}

/// One ordered RPC call: consecutive same-table, same-operation events.
#[derive(Debug)]
pub(super) struct Run {
    /// Routed table name including the prefix, raw (no encoding).
    pub table: String,
    /// Lane override: edge runs execute in their FROM-relation's lane
    /// so they sequence after that relation's document runs — a
    /// document delete auto-purges attached edges, so an edge RELATE
    /// racing it in a parallel lane could lose a fresh edge. `None`
    /// lanes by `table`.
    pub lane: Option<String>,
    /// Operation and its payload.
    pub kind: RunKind,
    /// Offsets into the original dispatcher batch, in order.
    pub offsets: Vec<usize>,
    /// Approximate encoded body bytes.
    pub bytes: usize,
}

impl Run {
    /// The lane this run executes in.
    pub(super) fn lane_key(&self) -> &str {
        self.lane.as_deref().unwrap_or(&self.table)
    }
}

#[derive(Debug)]
pub(super) enum RunKind {
    /// Full-replace upserts: pre-shaped `{"rid": …, "doc": …}` objects,
    /// built once by move so no later stage deep-copies documents.
    Upsert(Vec<Value>),
    /// Deletes by record id component.
    Delete(Vec<Value>),
    /// Remove every record in the table.
    Truncate,
    /// Idempotent edge upserts: per item, delete the deterministic edge
    /// record then RELATE the endpoints — two FOR statements per run,
    /// because RELATE's uniqueness check cannot see a delete issued in
    /// the same statement. Items are `{"eid": …, "a": …, "b": …}` key
    /// arrays; endpoint tables ride the run, not the items.
    EdgeApply {
        /// `in` endpoint table (routed, prefixed).
        a_table: String,
        /// `out` endpoint table (routed, prefixed).
        b_table: String,
        items: Vec<Value>,
    },
}

/// Output of batch translation: ordered runs plus client-side rejects
/// (unparseable payloads, missing headers) with exact batch offsets.
pub(super) struct Translated {
    pub runs: Vec<Run>,
    pub rejects: Vec<FailedItem>,
}

/// One edge operation accumulated during translation, replayed in
/// event order per spec after the document runs.
enum EdgeOp {
    /// `{eid, a, b}` item for an EdgeApply run.
    Apply(Value),
    /// Delete the deterministic edge record (row deleted, FK went
    /// NULL, or key relocation left a stale edge behind).
    Delete(Value),
    /// The FROM relation was truncated: clear the edge table.
    Truncate,
}

/// Per-spec translation context: routed names resolved once per batch
/// plus the ops accumulated in event order.
struct EdgeCtx<'a> {
    spec: &'a super::config::SurrealEdgeSpec,
    routed_from: String,
    routed_to: String,
    edge_table: String,
    ops: Vec<(usize, EdgeOp)>,
}

/// Extract and canonicalize one row's FK component values from the
/// materialized document. Returns `None` when any component is NULL or
/// absent — relationally, no edge. A source column literally named
/// `id` was preserved under `source_id` by `build_document`.
fn fk_components(document: &Value, fk_columns: &[String]) -> Option<Vec<String>> {
    let map = document.as_object()?;
    let mut out = Vec::with_capacity(fk_columns.len());
    for column in fk_columns {
        let field = if column == "id" { SOURCE_ID_FIELD } else { column };
        match map.get(field) {
            None | Some(Value::Null) => return None,
            Some(value) => out.push(ventstream_core::doc_id::component_text(value)),
        }
    }
    Some(out)
}

/// Translate a dispatcher batch. Order is preserved: runs execute
/// sequentially, and a new run starts whenever the table, the operation
/// class, or a batching limit changes. Edge runs append after the
/// document runs (same relative order per spec) and execute in the
/// FROM-relation's lane — see `Run::lane`.
pub(super) fn translate_batch(config: &SurrealDbConfig, events: &[Event]) -> Translated {
    let mut runs: Vec<Run> = Vec::new();
    let mut rejects: Vec<FailedItem> = Vec::new();
    let mut edge_ctxs: Vec<EdgeCtx<'_>> = config
        .graph_edges
        .iter()
        .map(|spec| EdgeCtx {
            spec,
            routed_from: format!("{}{}", config.table_prefix, spec.from_table),
            routed_to: format!("{}{}", config.table_prefix, spec.to_table),
            edge_table: format!("{}{}", config.table_prefix, spec.name),
            ops: Vec::new(),
        })
        .collect();

    for (offset, event) in events.iter().enumerate() {
        let subject = event.subject.as_str();
        let is_delete = subject.ends_with(".delete");
        let is_truncate = subject.ends_with(".truncate");

        let table = match resolve_table(config, event) {
            Ok(table) => table,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };

        if is_truncate {
            for ctx in edge_ctxs.iter_mut().filter(|ctx| ctx.routed_from == table) {
                ctx.ops.push((offset, EdgeOp::Truncate));
            }
            runs.push(Run {
                table,
                lane: None,
                kind: RunKind::Truncate,
                offsets: vec![offset],
                bytes: 0,
            });
            continue;
        }

        let doc_id = match required_doc_id(event) {
            Ok(id) => id,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };
        let rid = routed_record_id(config, doc_id);

        if is_delete {
            for ctx in edge_ctxs.iter_mut().filter(|ctx| ctx.routed_from == table) {
                ctx.ops.push((offset, EdgeOp::Delete(rid.clone())));
            }
            let bytes = doc_id.len() + 8;
            match runs.last_mut() {
                Some(run)
                    if run.table == table
                        && matches!(run.kind, RunKind::Delete(_))
                        && run.offsets.len() < config.batching.max_docs
                        && run.bytes + bytes <= config.batching.max_bytes =>
                {
                    if let RunKind::Delete(ids) = &mut run.kind {
                        ids.push(rid);
                    }
                    run.offsets.push(offset);
                    run.bytes += bytes;
                }
                _ => runs.push(Run {
                    table,
                    lane: None,
                    kind: RunKind::Delete(vec![rid]),
                    offsets: vec![offset],
                    bytes,
                }),
            }
            continue;
        }

        let lookup_paths: Vec<&str> = config
            .lookup_fields
            .iter()
            .filter(|entry| entry.table == table)
            .map(|entry| entry.field.as_str())
            .collect();
        // Build and validate the replacement document BEFORE any run is
        // enqueued: a relocation's old-id delete must never execute on
        // behalf of an event that is then rejected — that would destroy
        // the old record while its replacement rides to the DLQ.
        let document = match build_document(event, doc_id, &lookup_paths) {
            Ok(doc) => doc,
            Err(error) => {
                rejects.push(FailedItem { offset, error });
                continue;
            }
        };
        // Conservative size estimate: payload plus id and framing. The
        // exact-serialization check this replaces allocated the whole
        // document per event; the sink still enforces its precise
        // protocol limit at request time.
        let bytes = event.payload.as_slice().len() + doc_id.len() + 192;
        if bytes > config.batching.max_bytes {
            rejects.push(FailedItem {
                offset,
                error: format!(
                    "document is ~{bytes} bytes, exceeding max_bytes={}",
                    config.batching.max_bytes
                ),
            });
            continue;
        }
        // A key-changing update relocated the doc: enqueue a delete for
        // the previous record first (runs execute in order) so no stale
        // copy stays behind. A malformed old id is a corrupt header —
        // reject the event rather than guess at a destructive write.
        if let Some(old_id) = event.headers.get("ventstream.doc.old_id") {
            if old_id.is_empty() || old_id.chars().any(char::is_control) {
                rejects.push(FailedItem {
                    offset,
                    error: format!(
                        "event {} has an empty or control-character `ventstream.doc.old_id`",
                        event.id
                    ),
                });
                continue;
            }
            for ctx in edge_ctxs.iter_mut().filter(|ctx| ctx.routed_from == table) {
                ctx.ops
                    .push((offset, EdgeOp::Delete(routed_record_id(config, old_id))));
            }
            runs.push(Run {
                table: table.clone(),
                lane: None,
                kind: RunKind::Delete(vec![routed_record_id(config, old_id)]),
                offsets: vec![offset],
                bytes: old_id.len() + 8,
            });
        }
        for ctx in edge_ctxs.iter_mut().filter(|ctx| ctx.routed_from == table) {
            match fk_components(&document, &ctx.spec.fk_columns) {
                Some(components) => {
                    let fk: Value = serde_json::json!(components);
                    let (a_key, b_key) = if ctx.spec.reversed {
                        (fk, rid.clone())
                    } else {
                        (rid.clone(), fk)
                    };
                    ctx.ops.push((
                        offset,
                        EdgeOp::Apply(
                            serde_json::json!({"eid": rid.clone(), "a": a_key, "b": b_key}),
                        ),
                    ));
                }
                // NULL/absent FK is relationally "no edge": remove any
                // edge a previous value materialized.
                None => ctx.ops.push((offset, EdgeOp::Delete(rid.clone()))),
            }
        }
        let pair = serde_json::json!({"rid": rid, "doc": document});
        match runs.last_mut() {
            Some(run)
                if run.table == table
                    && matches!(run.kind, RunKind::Upsert(_))
                    && run.offsets.len() < config.batching.max_docs
                    && run.bytes + bytes <= config.batching.max_bytes =>
            {
                if let RunKind::Upsert(pairs) = &mut run.kind {
                    pairs.push(pair);
                }
                run.offsets.push(offset);
                run.bytes += bytes;
            }
            _ => runs.push(Run {
                table,
                lane: None,
                kind: RunKind::Upsert(vec![pair]),
                offsets: vec![offset],
                bytes,
            }),
        }
    }

    for ctx in edge_ctxs {
        if ctx.ops.is_empty() {
            continue;
        }
        let (routed_from, edge_table) = (&ctx.routed_from, &ctx.edge_table);
        let (a_table, b_table) = if ctx.spec.reversed {
            (ctx.routed_to.clone(), ctx.routed_from.clone())
        } else {
            (ctx.routed_from.clone(), ctx.routed_to.clone())
        };
        for (offset, op) in ctx.ops {
            // Rough per-item estimate mirroring the document path: key
            // arrays only, no payloads.
            let bytes = 96;
            match op {
                EdgeOp::Truncate => runs.push(Run {
                    table: edge_table.clone(),
                    lane: Some(routed_from.clone()),
                    kind: RunKind::Truncate,
                    offsets: vec![offset],
                    bytes: 0,
                }),
                EdgeOp::Delete(eid) => match runs.last_mut() {
                    Some(run)
                        if run.table == *edge_table
                            && matches!(run.kind, RunKind::Delete(_))
                            && run.offsets.len() < config.batching.max_docs
                            && run.bytes + bytes <= config.batching.max_bytes =>
                    {
                        if let RunKind::Delete(ids) = &mut run.kind {
                            ids.push(eid);
                        }
                        run.offsets.push(offset);
                        run.bytes += bytes;
                    }
                    _ => runs.push(Run {
                        table: edge_table.clone(),
                        lane: Some(routed_from.clone()),
                        kind: RunKind::Delete(vec![eid]),
                        offsets: vec![offset],
                        bytes,
                    }),
                },
                EdgeOp::Apply(item) => match runs.last_mut() {
                    Some(run)
                        if run.table == *edge_table
                            && matches!(run.kind, RunKind::EdgeApply { .. })
                            && run.offsets.len() < config.batching.max_docs
                            && run.bytes + bytes <= config.batching.max_bytes =>
                    {
                        if let RunKind::EdgeApply { items, .. } = &mut run.kind {
                            items.push(item);
                        }
                        run.offsets.push(offset);
                        run.bytes += bytes;
                    }
                    _ => runs.push(Run {
                        table: edge_table.clone(),
                        lane: Some(routed_from.clone()),
                        kind: RunKind::EdgeApply {
                            a_table: a_table.clone(),
                            b_table: b_table.clone(),
                            items: vec![item],
                        },
                        offsets: vec![offset],
                        bytes,
                    }),
                },
            }
        }
    }

    Translated { runs, rejects }
}

/// Resolve one event's routed table, prefix applied, no encoding —
/// statements address tables via `type::table()` bind values.
pub(crate) fn resolve_table(config: &SurrealDbConfig, event: &Event) -> Result<String, String> {
    let target = match &config.table_routing {
        SurrealTableRouting::ByOutputRelation => {
            event.headers.get(RELATION_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{RELATION_HEADER}` for table routing",
                    event.id
                )
            })?
        }
        SurrealTableRouting::ByProjectionTarget => {
            event.headers.get(PROJECTION_TARGET_HEADER).ok_or_else(|| {
                format!(
                    "event {} has no `{PROJECTION_TARGET_HEADER}` for table routing",
                    event.id
                )
            })?
        }
        SurrealTableRouting::Fixed(table) => table,
    };
    if target.is_empty() {
        return Err(format!("event {} routed to an empty table name", event.id));
    }
    Ok(format!("{}{}", config.table_prefix, target))
}

fn required_doc_id(event: &Event) -> Result<&str, String> {
    let doc_id = event.headers.get(DOC_ID_HEADER).ok_or_else(|| {
        format!(
            "event {} has no `{DOC_ID_HEADER}`; SurrealDB materialization requires a stable document id",
            event.id
        )
    })?;
    if doc_id.is_empty() || doc_id.chars().any(char::is_control) {
        return Err(format!(
            "event {} has an empty or control-character `{DOC_ID_HEADER}`",
            event.id
        ));
    }
    Ok(doc_id)
}

fn build_document(event: &Event, doc_id: &str, lookup_paths: &[&str]) -> Result<Value, String> {
    let mut parsed: Value = serde_json::from_slice(event.payload.as_slice())
        .map_err(|err| format!("event {} payload is not valid JSON: {err}", event.id))?;
    // Flat CDC updates carry the `{"new":…, "old":…}` transition envelope;
    // the document to materialize is the `new` row.
    if event.subject.as_str().ends_with(".update") {
        if let Some(row) = parsed
            .get_mut("new")
            .filter(|row| row.is_object())
            .map(Value::take)
        {
            parsed = row;
        }
    }
    let Value::Object(mut map) = parsed else {
        return Err(format!(
            "event {} payload is not a JSON object; SurrealDB documents must be objects",
            event.id
        ));
    };
    // `id` is SurrealDB's record-id field: CONTENT carrying a mismatched
    // id is rejected by the server. Preserve the source column under
    // `source_id` instead of mutating or dropping it.
    if let Some(source_id) = map.remove("id") {
        map.entry(SOURCE_ID_FIELD.to_owned()).or_insert(source_id);
    }
    map.insert(
        CANONICAL_ID_FIELD.to_owned(),
        Value::String(doc_id.to_owned()),
    );
    for path in lookup_paths {
        let mut values = Vec::new();
        let mut segments = path.split('.');
        if let Some(root) = segments.next().and_then(|head| map.get(head)) {
            let rest: Vec<&str> = segments.collect();
            collect_path_strings(root, &rest, &mut values);
        }
        map.insert(
            super::config::lookup_field_name(path),
            Value::Array(values.into_iter().map(Value::String).collect()),
        );
    }
    Ok(Value::Object(map))
}

/// Collect the string-canonical forms of every scalar reachable via
/// the remaining path `segments`, descending through arrays — the
/// write-side twin of the read predicate's flatten/map, so stored
/// values compare equal to the WAL's canonical text forms. Borrows the
/// tree; no document copies.
fn collect_path_strings(value: &Value, segments: &[&str], out: &mut Vec<String>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_path_strings(item, segments, out);
            }
        }
        Value::Object(map) => {
            if let Some((head, rest)) = segments.split_first() {
                if let Some(next) = map.get(*head) {
                    collect_path_strings(next, rest, out);
                }
            }
        }
        other if segments.is_empty() => match other {
            Value::String(text) => out.push(text.clone()),
            Value::Number(number) => out.push(number.to_string()),
            Value::Bool(flag) => out.push(flag.to_string()),
            _ => {}
        },
        _ => {}
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

    use ventstream_core::event::{ContentType, Headers, Payload, SourceUri, Subject};

    fn edge_config() -> SurrealDbConfig {
        let mut config = SurrealDbConfig::new("s", "http://x", "vs", "app", "u", "p");
        config.graph_edges = vec![super::super::config::SurrealEdgeSpec {
            name: "wrote".into(),
            from_table: "public.articles".into(),
            fk_columns: vec!["author_id".into()],
            to_table: "public.people".into(),
            reversed: true,
        }];
        config
    }

    fn event(table: &str, op: &str, doc_id: &str, payload: &str) -> Event {
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new(format!("postgres.{table}.{op}")).expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(payload.as_bytes().to_vec()))
            .content_type(ContentType::Json)
            .headers(
                Headers::empty()
                    .with_header("ventstream.doc.id".into(), doc_id.into())
                    .with_header("ventstream.cdc.relation".into(), table.into()),
            )
            .build()
    }

    #[test]
    fn fk_insert_emits_document_then_edge_apply_in_from_lane() {
        let config = edge_config();
        let events = [event(
            "public.articles",
            "insert",
            r#"public.articles:["9"]"#,
            r#"{"id": 9, "title": "CDC", "author_id": 7}"#,
        )];
        let out = translate_batch(&config, &events);
        assert!(out.rejects.is_empty());
        assert_eq!(out.runs.len(), 2);
        assert!(matches!(out.runs[0].kind, RunKind::Upsert(_)));
        let RunKind::EdgeApply { a_table, b_table, items } = &out.runs[1].kind else {
            panic!("expected EdgeApply, got {:?}", out.runs[1].kind);
        };
        // reversed: person -> wrote -> article
        assert_eq!(a_table, "public.people");
        assert_eq!(b_table, "public.articles");
        assert_eq!(out.runs[1].table, "wrote");
        assert_eq!(out.runs[1].lane_key(), "public.articles");
        assert_eq!(
            items[0],
            serde_json::json!({"eid": ["9"], "a": ["7"], "b": ["9"]})
        );
    }

    #[test]
    fn null_fk_emits_edge_delete_not_apply() {
        let config = edge_config();
        let events = [event(
            "public.articles",
            "update",
            r#"public.articles:["9"]"#,
            r#"{"new": {"id": 9, "title": "CDC", "author_id": null}, "old": null}"#,
        )];
        let out = translate_batch(&config, &events);
        assert_eq!(out.runs.len(), 2);
        let RunKind::Delete(eids) = &out.runs[1].kind else {
            panic!("expected edge Delete, got {:?}", out.runs[1].kind);
        };
        assert_eq!(out.runs[1].table, "wrote");
        assert_eq!(eids[0], serde_json::json!(["9"]));
    }

    #[test]
    fn row_delete_also_deletes_edge() {
        let config = edge_config();
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new("postgres.public.articles.delete").expect("subject");
        let events = [Event::builder(source, subject)
            .payload(Payload::from_vec(Vec::new()))
            .headers(
                Headers::empty()
                    .with_header("ventstream.doc.id".into(), r#"public.articles:["9"]"#.into())
                    .with_header("ventstream.cdc.relation".into(), "public.articles".into()),
            )
            .build()];
        let out = translate_batch(&config, &events);
        assert_eq!(out.runs.len(), 2);
        assert!(matches!(out.runs[0].kind, RunKind::Delete(_)));
        let RunKind::Delete(eids) = &out.runs[1].kind else {
            panic!("expected edge Delete");
        };
        assert_eq!(out.runs[1].table, "wrote");
        assert_eq!(out.runs[1].lane_key(), "public.articles");
        assert_eq!(eids[0], serde_json::json!(["9"]));
    }

    #[test]
    fn edge_items_merge_and_doc_runs_stay_contiguous() {
        // Two inserts must yield ONE upsert run and ONE edge run —
        // interleaving edge runs into the document stream would break
        // doc-run merging and double the round-trips.
        let config = edge_config();
        let events = [
            event(
                "public.articles",
                "insert",
                r#"public.articles:["1"]"#,
                r#"{"id": 1, "author_id": 7}"#,
            ),
            event(
                "public.articles",
                "insert",
                r#"public.articles:["2"]"#,
                r#"{"id": 2, "author_id": 8}"#,
            ),
        ];
        let out = translate_batch(&config, &events);
        assert_eq!(out.runs.len(), 2);
        let RunKind::Upsert(pairs) = &out.runs[0].kind else {
            panic!("expected merged upsert run");
        };
        assert_eq!(pairs.len(), 2);
        let RunKind::EdgeApply { items, .. } = &out.runs[1].kind else {
            panic!("expected merged edge run");
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn truncate_of_from_table_truncates_edge_table() {
        let config = edge_config();
        let source = SourceUri::new("postgres://test").expect("uri");
        let subject = Subject::new("postgres.public.articles.truncate").expect("subject");
        let events = [Event::builder(source, subject)
            .payload(Payload::from_vec(Vec::new()))
            .headers(
                Headers::empty()
                    .with_header("ventstream.cdc.relation".into(), "public.articles".into()),
            )
            .build()];
        let out = translate_batch(&config, &events);
        assert_eq!(out.runs.len(), 2);
        assert!(matches!(out.runs[0].kind, RunKind::Truncate));
        assert!(matches!(out.runs[1].kind, RunKind::Truncate));
        assert_eq!(out.runs[1].table, "wrote");
    }

    #[test]
    fn unrelated_tables_and_composite_fks_route_correctly() {
        let mut config = edge_config();
        config.graph_edges[0].fk_columns = vec!["org_id".into(), "team_id".into()];
        config.graph_edges[0].reversed = false;
        let events = [
            event(
                "public.articles",
                "insert",
                r#"public.articles:["1"]"#,
                r#"{"id": 1, "org_id": 5, "team_id": "blue"}"#,
            ),
            event(
                "public.people",
                "insert",
                r#"public.people:["7"]"#,
                r#"{"id": 7, "name": "Ada"}"#,
            ),
        ];
        let out = translate_batch(&config, &events);
        // articles upsert, people upsert, one edge run — people emits none.
        assert_eq!(out.runs.len(), 3);
        let RunKind::EdgeApply { a_table, b_table, items } = &out.runs[2].kind else {
            panic!("expected edge run last");
        };
        assert_eq!(a_table, "public.articles");
        assert_eq!(b_table, "public.people");
        assert_eq!(
            items[0],
            serde_json::json!({"eid": ["1"], "a": ["1"], "b": ["5", "blue"]})
        );
    }

    #[test]
    fn canonical_ids_become_native_array_record_ids() {
        assert_eq!(
            record_id_component(r#"public.orders:["299"]"#),
            serde_json::json!(["299"])
        );
        assert_eq!(
            record_id_component(r#"t:["a","b"]"#),
            serde_json::json!(["a", "b"])
        );
    }

    #[test]
    fn fixed_routing_keeps_ids_fully_qualified() {
        // One shared table: `users:["1"]` and `orders:["1"]` must stay
        // distinct records or the relations silently merge.
        let mut config = SurrealDbConfig::new("s", "http://x", "vs", "app", "u", "p");
        config.table_routing = SurrealTableRouting::Fixed("everything".into());
        assert_eq!(
            routed_record_id(&config, r#"users:["1"]"#),
            serde_json::json!([r#"users:["1"]"#])
        );
        assert_ne!(
            routed_record_id(&config, r#"users:["1"]"#),
            routed_record_id(&config, r#"orders:["1"]"#)
        );
        // Relation-per-table routing keeps the natural bare key.
        config.table_routing = SurrealTableRouting::ByOutputRelation;
        assert_eq!(
            routed_record_id(&config, r#"users:["1"]"#),
            serde_json::json!(["1"])
        );
    }

    #[test]
    fn opaque_ids_become_single_element_arrays_never_strings() {
        // Neo4j-style: table prefix + elementId suffix keeps the suffix.
        assert_eq!(
            record_id_component("products:4:e0b0-af..:17"),
            serde_json::json!(["4:e0b0-af..:17"])
        );
        // No separator: the whole id is the single element.
        assert_eq!(record_id_component("plain"), serde_json::json!(["plain"]));
        // Bare strings are forbidden — param-path type::record would
        // parse them as record literals and merge documents.
        assert!(!record_id_component("products:4").is_string());
        assert_eq!(record_id_component("products:4"), serde_json::json!(["4"]));
    }
}
