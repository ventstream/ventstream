//! SQL-side denormalization for the MySQL source — the bounded-memory
//! alternative to the in-memory join engine.
//!
//! Mirrors [`crate::sql_denormalize`] (Postgres) but in the MySQL dialect:
//! the join is pushed into MySQL via `JSON_OBJECT`/`JSON_ARRAYAGG`, so RSS is
//! O(chunk) instead of O(dataset).
//!
//! - **Bootstrap.** Keyset-chunked `SELECT <pk>, (<doc>) FROM <primary>` where
//!   `<doc>` is a `JSON_OBJECT` of the primary columns plus one embedded
//!   sub-`SELECT` per related entry. Docs stream straight to the sink.
//! - **Tail.** Each binlog CDC event resolves the affected primary key(s) and
//!   re-runs the compose SQL for just those keys. No resident join state.
//!
//! MySQL specifics vs Postgres: no `to_jsonb(row)` (columns are enumerated
//! from `information_schema`); `JSON_ARRAYAGG` doesn't honor `ORDER BY`, so
//! `sort_by` is applied client-side per doc; PKs of changed rows come from the
//! `ventstream.doc.id` header (full-image sources emit `{}` on delete), which
//! is the equivalent of Postgres' REPLICA IDENTITY PK. 1:many child deletes
//! resolve to their parent via the shared sink reverse-lookup.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Pool, Row, Value as MyValue};
use serde_json::Value;
use tracing::{debug, info, warn};
use ventstream_core::{
    ContentType, Event, EventReceiver, EventSender, Headers, Payload, ShutdownToken, SourceUri,
    Subject,
};
use ventstream_joins::{Cardinality, JoinDefinition, RelatedDefinition};
use ventstream_sinks::opensearch::{index_template, OpenSearchConfig, OsReverseLookup};
use ventstream_sources::mysql::MySqlCdcConfig;

/// Greedy-drain ceiling for the tail loop (matches the PG denormalizer).
const TAIL_DRAIN_LIMIT: usize = 1024;
const ACK_BARRIER_HEADER: &str = "ventstream.internal.ack_barrier";
/// Keep each dynamic `IN` statement small enough for predictable parse and
/// execution memory while still amortizing the correlated projection work.
const DEFAULT_RECOMPOSE_KEY_CHUNK: usize = 256;
/// Bounded by the mysql_async pool; enough to overlap query latency without
/// overwhelming the source database with one connection per affected row.
const DEFAULT_RECOMPOSE_QUERY_CONCURRENCY: usize = 4;
/// Retry quickly for short database interruptions, then cap the delay so a
/// retained tail batch continues making progress without a tight error loop.
const TAIL_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const TAIL_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailRetry {
    failures: u32,
    next_backoff: Duration,
}

impl Default for TailRetry {
    fn default() -> Self {
        Self {
            failures: 0,
            next_backoff: TAIL_RETRY_INITIAL_BACKOFF,
        }
    }
}

impl TailRetry {
    fn record_failure(&mut self) -> (u32, Duration) {
        self.failures = self.failures.saturating_add(1);
        let backoff = self.next_backoff;
        self.next_backoff = self
            .next_backoff
            .checked_mul(2)
            .unwrap_or(TAIL_RETRY_MAX_BACKOFF)
            .min(TAIL_RETRY_MAX_BACKOFF);
        (self.failures, backoff)
    }

    fn record_success(&mut self) -> Option<u32> {
        if self.failures == 0 {
            return None;
        }
        let recovered_after = self.failures;
        *self = Self::default();
        Some(recovered_after)
    }
}

fn esc(ident: &str) -> String {
    ident.replace('`', "``")
}

fn relation_of(table: &str) -> &str {
    table.rsplit('.').next().unwrap_or(table)
}

fn namespace_of(table: &str) -> &str {
    table.split('.').next().unwrap_or("")
}

/// `JSON_OBJECT('c', `alias`.`c`, …)` over the given columns.
fn json_object_expr(alias: &str, cols: &[String]) -> String {
    let pairs = cols
        .iter()
        .map(|c| format!("'{}', `{alias}`.`{}`", c.replace('\'', "''"), esc(c)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("JSON_OBJECT({pairs})")
}

/// `r.to = p.from [AND …]` join condition for one related entry.
fn join_condition(rel_alias: &str, related: &RelatedDefinition) -> String {
    related
        .join_on
        .from
        .columns()
        .iter()
        .zip(related.join_on.to.columns())
        .map(|(from, to)| format!("`{rel_alias}`.`{}` = `p`.`{}`", esc(to), esc(from)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// One join definition prepared for SQL execution.
struct PreparedDef {
    /// `ns.rel` of the primary table (the namespace is the MySQL database).
    primary_table: String,
    database: String,
    /// Primary PK column names.
    pk_cols: Vec<String>,
    /// The `JSON_OBJECT(...)` doc expression.
    doc: String,
    /// True if any related entry has `sort_by` (→ client-side array sort).
    needs_sort: bool,
    def: JoinDefinition,
}

impl PreparedDef {
    /// `SELECT CAST(p.`pk` AS CHAR) AS __pkN, (<doc>) AS doc FROM `db`.`rel` p`
    fn select_head(&self) -> String {
        let pk_sel = self
            .pk_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("CAST(`p`.`{}` AS CHAR) AS __pk{i}", esc(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {pk_sel}, ({doc}) AS doc FROM `{db}`.`{rel}` p",
            doc = self.doc,
            db = esc(&self.database),
            rel = esc(relation_of(&self.primary_table)),
        )
    }
}

/// Build one set-based recompose query for a bounded key chunk. Composite
/// primary keys use MySQL row constructors instead of degrading to one query
/// per key.
fn build_recompose_query(pd: &PreparedDef, keys: &[Vec<String>]) -> Option<(String, Vec<String>)> {
    let key_width = pd.pk_cols.len();
    if key_width == 0 {
        return None;
    }
    let valid: Vec<&Vec<String>> = keys.iter().filter(|key| key.len() == key_width).collect();
    if valid.is_empty() {
        return None;
    }
    let params: Vec<String> = valid.iter().flat_map(|key| key.iter().cloned()).collect();
    if key_width == 1 {
        let placeholders = vec!["?"; valid.len()].join(", ");
        let column = pd.pk_cols.first()?;
        return Some((
            format!(
                "{head} WHERE `p`.`{column}` IN ({placeholders})",
                head = pd.select_head(),
                column = esc(column),
            ),
            params,
        ));
    }

    let columns = pd
        .pk_cols
        .iter()
        .map(|column| format!("`p`.`{}`", esc(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let tuple = format!("({})", vec!["?"; key_width].join(", "));
    let placeholders = vec![tuple; valid.len()].join(", ");
    Some((
        format!(
            "{head} WHERE ({columns}) IN ({placeholders})",
            head = pd.select_head(),
        ),
        params,
    ))
}

/// Bounded-memory MySQL denormalizer. Owns a pool; bootstraps + recomposes by
/// re-querying MySQL rather than holding join state.
pub struct MySqlDenormalizer {
    pool: Pool,
    defs: Vec<PreparedDef>,
    chunk_size: u64,
    recompose_key_chunk: usize,
    recompose_query_concurrency: usize,
    sink_lookup: Option<OsReverseLookup>,
}

impl MySqlDenormalizer {
    /// Connect and prepare definitions (enumerates columns to build the doc SQL).
    pub async fn connect(
        config: &MySqlCdcConfig,
        joins: Vec<JoinDefinition>,
        chunk_size: u64,
    ) -> Result<Self> {
        let pool = Pool::new(config.opts());
        let database = config.database.clone();
        let mut defs = Vec::with_capacity(joins.len());
        for def in joins {
            let doc = build_doc_expr(&pool, &database, &def).await?;
            let needs_sort = def.related.iter().any(|r| r.sort_by.is_some());
            defs.push(PreparedDef {
                primary_table: def.primary.table.clone(),
                database: database.clone(),
                pk_cols: def.primary.pk.columns().to_vec(),
                doc,
                needs_sort,
                def,
            });
        }
        Ok(Self {
            pool,
            defs,
            chunk_size: chunk_size.max(1),
            recompose_key_chunk: DEFAULT_RECOMPOSE_KEY_CHUNK,
            recompose_query_concurrency: DEFAULT_RECOMPOSE_QUERY_CONCURRENCY,
            sink_lookup: None,
        })
    }

    pub async fn require_full_binlog_row_image(&self) -> Result<()> {
        let mut conn = self.conn().await?;
        let mode: Option<String> = conn
            .query_first("SELECT @@GLOBAL.binlog_row_image")
            .await
            .context("reading MySQL binlog_row_image")?;
        match mode.as_deref() {
            Some(mode) if mode.eq_ignore_ascii_case("FULL") => Ok(()),
            Some(mode) => anyhow::bail!(
                "MySQL SQL joins require binlog_row_image=FULL when a related join key is not \
                 contained in the related primary key; server reports {mode}"
            ),
            None => anyhow::bail!("MySQL did not report @@GLOBAL.binlog_row_image"),
        }
    }

    /// Override the bounded tail-recomposition limits.
    #[must_use]
    pub fn with_recompose_limits(mut self, key_chunk: usize, concurrency: usize) -> Self {
        self.recompose_key_chunk = key_chunk.max(1);
        self.recompose_query_concurrency = concurrency.max(1);
        self
    }

    /// Enable the sink reverse-lookup fallback for 1:many child deletes.
    #[must_use]
    pub fn with_reverse_lookup(mut self, os: OpenSearchConfig) -> Self {
        match OsReverseLookup::new(os) {
            Ok(lookup) => self.sink_lookup = Some(lookup),
            Err(err) => {
                warn!(error = %err, "mysql sql-denormalize: reverse-lookup disabled (1:many child deletes won't propagate)")
            }
        }
        self
    }

    async fn conn(&self) -> Result<mysql_async::Conn> {
        self.pool
            .get_conn()
            .await
            .context("mysql sql-denormalize: get connection")
    }

    /// Bootstrap every definition: keyset-chunked SQL join → composed docs.
    async fn bootstrap(&self, sender: &EventSender, shutdown: &ShutdownToken) -> Result<()> {
        for pd in &self.defs {
            let started = std::time::Instant::now();
            let mut emitted: u64 = 0;
            let mut last: Option<Vec<String>> = None;
            let order = pd
                .pk_cols
                .iter()
                .map(|c| format!("`p`.`{}`", esc(c)))
                .collect::<Vec<_>>()
                .join(", ");
            loop {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                let (where_clause, params) = match &last {
                    None => (String::new(), Vec::new()),
                    Some(vals) => {
                        let lhs = pd
                            .pk_cols
                            .iter()
                            .map(|c| format!("`p`.`{}`", esc(c)))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let ph = vec!["?"; vals.len()].join(", ");
                        (format!("WHERE ({lhs}) > ({ph}) "), vals.clone())
                    }
                };
                let sql = format!(
                    "{head} {where_clause}ORDER BY {order} LIMIT {limit}",
                    head = pd.select_head(),
                    limit = self.chunk_size,
                );
                let mut conn = self.conn().await?;
                let rows: Vec<Row> = conn
                    .exec(&sql, mysql_params(&params))
                    .await
                    .with_context(|| format!("bootstrap select for {}", pd.primary_table))?;
                drop(conn);
                if rows.is_empty() {
                    break;
                }
                let n = rows.len();
                let mut page_last: Option<Vec<String>> = None;
                for row in &rows {
                    let (pk_text, doc) = decode_doc_row(pd, row)?;
                    let event = build_doc_event(
                        &pd.primary_table,
                        pd.def.target_index(),
                        &pk_text,
                        &doc,
                        false,
                    )?;
                    sender.send(event, shutdown).await.map_err(|err| {
                        anyhow::anyhow!(
                            "publishing MySQL bootstrap document for {}: {err}",
                            pd.primary_table
                        )
                    })?;
                    ventstream_telemetry::bump_events_emitted(1);
                    emitted += 1;
                    page_last = Some(pk_text);
                }
                if (n as u64) < self.chunk_size {
                    break;
                }
                match page_last {
                    Some(p) => last = Some(p),
                    None => break,
                }
            }
            info!(
                primary = %pd.primary_table,
                output = %pd.def.effective_name(),
                rows = emitted,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "mysql sql-denormalize bootstrap: spec complete"
            );
        }
        Ok(())
    }

    /// Recompose a set of primary keys and emit docs. Returns the PK-text of
    /// every primary that still existed (was upserted).
    async fn recompose(
        &self,
        pd: &PreparedDef,
        pks: &[Vec<String>],
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<Vec<Vec<String>>> {
        let mut emitted: Vec<Vec<String>> = Vec::new();
        if pks.is_empty() {
            return Ok(emitted);
        }
        let queries: Vec<(String, Vec<String>)> = pks
            .chunks(self.recompose_key_chunk)
            .filter_map(|keys| build_recompose_query(pd, keys))
            .collect();
        let query_futures = queries.into_iter().map(|(sql, params)| async move {
            let started = std::time::Instant::now();
            let mut conn = self.conn().await?;
            let rows: Vec<Row> = conn
                .exec(&sql, mysql_params(&params))
                .await
                .with_context(|| format!("recompose for {}", pd.primary_table))?;
            debug!(
                primary = %pd.primary_table,
                keys = params.len() / pd.pk_cols.len().max(1),
                rows = rows.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                metric = "mysql.sql_denormalize.recompose_query",
                "mysql sql-denormalize recompose query complete"
            );
            Ok::<Vec<Row>, anyhow::Error>(rows)
        });
        let mut row_sets =
            stream::iter(query_futures).buffer_unordered(self.recompose_query_concurrency);
        while let Some(rows) = row_sets.try_next().await? {
            for row in &rows {
                let (pk_text, doc) = decode_doc_row(pd, row)?;
                let event = build_doc_event(
                    &pd.primary_table,
                    pd.def.target_index(),
                    &pk_text,
                    &doc,
                    false,
                )?;
                sender.send(event, shutdown).await.map_err(|err| {
                    anyhow::anyhow!(
                        "publishing MySQL recomposed document for {}: {err}",
                        pd.primary_table
                    )
                })?;
                ventstream_telemetry::bump_events_emitted(1);
                emitted.push(pk_text);
            }
        }
        Ok(emitted)
    }

    /// Handle a drained batch of tail CDC events: resolve affected primaries
    /// (primary changes, related FK changes, child-delete reverse-lookups),
    /// recompose them, and tombstone primaries that no longer exist.
    async fn handle_tail_batch(
        &self,
        events: &[Event],
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        // Parse each event: relation, op, available row image,
        // and the changed row's PK from the doc-id header.
        let parsed: Vec<ParsedEvent> = events.iter().map(parse_event).collect();

        for pd in &self.defs {
            let mut affected: HashSet<Vec<String>> = HashSet::new();
            let mut deletes: HashSet<Vec<String>> = HashSet::new();
            // Child-delete reverse-lookup, keyed by the embedded PK fields →
            // the deleted children's PK tuples (single- or multi-column).
            let mut lookup_buckets: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
            let mut many1_buckets: HashMap<usize, Vec<Vec<String>>> = HashMap::new();

            for ev in &parsed {
                // Primary-table event → the row's own PK (from the doc id).
                if relation_of(&pd.primary_table) == ev.relation {
                    if let Some(pk) = &ev.doc_id_pk {
                        if ev.op == "delete" {
                            deletes.insert(pk.clone());
                        } else {
                            affected.insert(pk.clone());
                        }
                    }
                    if ev.op == "update" {
                        if let Some(old_pk) = ev
                            .old_row
                            .as_ref()
                            .and_then(|row| extract_cols(row, pd.def.primary.pk.columns()))
                        {
                            if ev.doc_id_pk.as_ref() != Some(&old_pk) {
                                deletes.insert(old_pk);
                            }
                        }
                    }
                }
                // Related-table events → reverse-resolve to affected primaries.
                for (rel_idx, related) in pd.def.related.iter().enumerate() {
                    if relation_of(&related.table) != ev.relation {
                        continue;
                    }
                    let from = related.join_on.from.columns();
                    let to = related.join_on.to.columns();
                    // Try to read the join `to` value: from the full-image row
                    // (insert/update) or, for a delete, from the doc-id PK when
                    // the join targets the related PK.
                    let to_vals = match &ev.row {
                        Some(row) => extract_cols(row, to),
                        None if related.pk.columns() == to => ev.doc_id_pk.clone(),
                        None => None,
                    };
                    match &to_vals {
                        Some(vals) => {
                            if from == pd.def.primary.pk.columns() {
                                affected.insert(vals.clone()); // 1:many — `to` is the parent PK
                            } else {
                                many1_buckets.entry(rel_idx).or_default().push(vals.clone());
                            }
                        }
                        None => {
                            // 1:many child delete with no join key in the event:
                            // resolve the parent via the sink reverse-lookup, by
                            // the deleted child's PK (single- or multi-column).
                            if ev.op == "delete" {
                                if let (Some(child_pk), Some((fields, pk_cols))) = (
                                    &ev.doc_id_pk,
                                    reverse_lookup_fields(pd.def.primary.pk.columns(), related),
                                ) {
                                    if child_pk.len() == pk_cols.len() {
                                        lookup_buckets
                                            .entry(fields)
                                            .or_default()
                                            .push(child_pk.clone());
                                    }
                                }
                            }
                        }
                    }
                    if ev.op == "update" {
                        let old_to_vals = ev.old_row.as_ref().and_then(|row| extract_cols(row, to));
                        if let Some(old_vals) = old_to_vals {
                            if to_vals.as_ref() != Some(&old_vals) {
                                if from == pd.def.primary.pk.columns() {
                                    affected.insert(old_vals);
                                } else {
                                    many1_buckets.entry(rel_idx).or_default().push(old_vals);
                                }
                            }
                        }
                    }
                }
            }

            for (rel_idx, mut tuples) in many1_buckets {
                let Some(related) = pd.def.related.get(rel_idx) else {
                    continue;
                };
                tuples.sort();
                tuples.dedup();
                for pk in self
                    .lookup_primary_by_batch(pd, related.join_on.from.columns(), &tuples)
                    .await?
                {
                    affected.insert(pk);
                }
            }
            for (fields, mut tuples) in lookup_buckets {
                tuples.sort();
                tuples.dedup();
                for pk in self.reverse_lookup_parents(pd, &fields, &tuples).await? {
                    affected.insert(pk);
                }
            }

            for pk in &deletes {
                affected.insert(pk.clone());
            }
            let emitted = if affected.is_empty() {
                Vec::new()
            } else {
                let pks: Vec<Vec<String>> = affected.into_iter().collect();
                debug!(
                    primary = %pd.primary_table,
                    affected = pks.len(),
                    deletes = deletes.len(),
                    batch = events.len(),
                    "mysql sql-denormalize batched tail recompose"
                );
                self.recompose(pd, &pks, sender, shutdown).await?
            };

            // Tombstone deleted keys MySQL no longer has (not re-created this batch).
            let live: HashSet<&Vec<String>> = emitted.iter().collect();
            for pk in &deletes {
                if live.contains(pk) {
                    continue;
                }
                let event = build_doc_event(
                    &pd.primary_table,
                    pd.def.target_index(),
                    pk,
                    &Value::Null,
                    true,
                )?;
                sender.send(event, shutdown).await.map_err(|err| {
                    anyhow::anyhow!("publishing MySQL tombstone for {}: {err}", pd.primary_table)
                })?;
                ventstream_telemetry::bump_events_emitted(1);
            }
        }
        Ok(())
    }

    /// Preserve source acknowledgement barriers as strict ordering boundaries.
    /// Every change segment is fully recomposed before its following barrier is
    /// forwarded, so the dispatcher cannot confirm a MySQL binlog position
    /// while derived documents from that position are still outstanding.
    async fn handle_ordered_tail_batch(
        &self,
        events: &[Event],
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<()> {
        let mut segment_start = 0;
        for (index, event) in events.iter().enumerate() {
            if event.headers.get(ACK_BARRIER_HEADER) != Some("true") {
                continue;
            }
            let segment = events
                .get(segment_start..index)
                .ok_or_else(|| anyhow::anyhow!("invalid MySQL tail barrier boundary"))?;
            self.handle_tail_batch(segment, sender, shutdown).await?;
            sender.send(event.clone(), shutdown).await.map_err(|err| {
                anyhow::anyhow!("forwarding MySQL acknowledgement barrier: {err}")
            })?;
            segment_start = index + 1;
        }
        let trailing = events
            .get(segment_start..)
            .ok_or_else(|| anyhow::anyhow!("invalid MySQL trailing tail boundary"))?;
        self.handle_tail_batch(trailing, sender, shutdown).await
    }

    /// `SELECT pk FROM primary WHERE from_col IN (?,…)` → primaries embedding a
    /// changed many:1 related row. Single-column `from` only; composite falls
    /// back to per-tuple.
    async fn lookup_primary_by_batch(
        &self,
        pd: &PreparedDef,
        from_cols: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<Vec<String>>> {
        if tuples.is_empty() {
            return Ok(Vec::new());
        }
        let n_pk = pd.pk_cols.len();
        let pk_sel = pd
            .pk_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("CAST(`p`.`{}` AS CHAR) AS __pk{i}", esc(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut out = Vec::new();
        let chunks: Vec<(String, Vec<String>)> = match from_cols {
            [col] => {
                let vals: Vec<String> = tuples.iter().filter_map(|t| t.first().cloned()).collect();
                let ph = vec!["?"; vals.len()].join(", ");
                vec![(
                    format!(
                        "SELECT {pk_sel} FROM `{db}`.`{rel}` p WHERE `p`.`{col}` IN ({ph})",
                        db = esc(&pd.database),
                        rel = esc(relation_of(&pd.primary_table)),
                        col = esc(col),
                    ),
                    vals,
                )]
            }
            _ => tuples
                .iter()
                .map(|t| {
                    let cond = from_cols
                        .iter()
                        .map(|c| format!("`p`.`{}` = ?", esc(c)))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    (
                        format!(
                            "SELECT {pk_sel} FROM `{db}`.`{rel}` p WHERE {cond}",
                            db = esc(&pd.database),
                            rel = esc(relation_of(&pd.primary_table)),
                        ),
                        t.clone(),
                    )
                })
                .collect(),
        };
        for (sql, params) in chunks {
            let mut conn = self.conn().await?;
            let rows: Vec<Row> = conn.exec(&sql, mysql_params(&params)).await?;
            drop(conn);
            for row in &rows {
                out.push((0..n_pk).map(|i| row_text(row, i)).collect());
            }
        }
        Ok(out)
    }

    /// Resolve parents of deleted 1:many children via the sink reverse-lookup.
    /// `fields` is one embedded keyword field per child-PK column; `tuples` are
    /// the deleted children's PK component vectors (aligned with `fields`).
    /// Single-column uses the batched `terms` query; composite uses the
    /// per-column `bool/must` variant.
    async fn reverse_lookup_parents(
        &self,
        pd: &PreparedDef,
        fields: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<Vec<String>>> {
        let Some(lookup) = self.sink_lookup.as_ref() else {
            return Ok(Vec::new());
        };
        if tuples.is_empty() {
            return Ok(Vec::new());
        }
        let Some(index) = self.resolve_sink_index(pd, lookup) else {
            return Ok(Vec::new());
        };
        let result = match fields {
            [field] => {
                let values: Vec<String> =
                    tuples.iter().filter_map(|t| t.first().cloned()).collect();
                lookup.doc_ids_by_terms(&index, field, &values).await
            }
            _ => lookup.doc_ids_by_term_tuples(&index, fields, tuples).await,
        };
        let ids = match result {
            Ok(ids) => ids,
            Err(err) => {
                warn!(primary = %pd.primary_table, error = %err, "mysql sql-denormalize: sink reverse-lookup failed");
                return Ok(Vec::new());
            }
        };
        Ok(ids
            .iter()
            .filter_map(|id| primary_pk_from_doc_id(id, &pd.primary_table))
            .collect())
    }

    fn resolve_sink_index(&self, pd: &PreparedDef, lookup: &OsReverseLookup) -> Option<String> {
        let probe = build_doc_event(
            &pd.primary_table,
            pd.def.target_index(),
            &["__vs_probe__".to_owned()],
            &Value::Null,
            false,
        )
        .ok()?;
        index_template::render(lookup.index_template(), &probe, chrono::Utc::now()).ok()
    }

    /// Run: bootstrap, then drain the tail receiver until shutdown.
    pub async fn run(
        self,
        mut receiver: EventReceiver,
        sender: EventSender,
        shutdown: ShutdownToken,
    ) {
        info!(
            defs = self.defs.len(),
            "mysql sql-denormalize: starting bootstrap"
        );
        if let Err(err) = self.bootstrap(&sender, &shutdown).await {
            warn!(error = %err, "mysql sql-denormalize bootstrap failed");
            ventstream_telemetry::record_error("mysql SQL denormalizer bootstrap failed");
            shutdown.cancel();
            return;
        }
        info!("mysql sql-denormalize: bootstrap done, entering tail");
        let mut retry = TailRetry::default();
        'tail: loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                batch = receiver.recv_batch(TAIL_DRAIN_LIMIT) => {
                    if batch.is_empty() {
                        break;
                    }

                    // Keep this exact drained batch until it succeeds. Receiving
                    // a later batch after an error would allow the MySQL source
                    // cursor to advance past unapplied changes.
                    loop {
                        if shutdown.is_cancelled() {
                            break 'tail;
                        }

                        match self
                            .handle_ordered_tail_batch(&batch, &sender, &shutdown)
                            .await
                        {
                            Ok(()) => {
                                if let Some(attempts) = retry.record_success() {
                                    ventstream_telemetry::clear_error();
                                    ventstream_telemetry::set_phase(
                                        ventstream_telemetry::LifecyclePhase::Tailing,
                                    );
                                    info!(
                                        attempts,
                                        batch = batch.len(),
                                        "mysql sql-denormalize tail recompose recovered"
                                    );
                                }
                                break;
                            }
                            Err(err) => {
                                let (attempt, backoff) = retry.record_failure();
                                warn!(
                                    error = %err,
                                    attempt,
                                    batch = batch.len(),
                                    backoff_ms = backoff.as_millis() as u64,
                                    "mysql sql-denormalize tail recompose failed; retaining batch for retry"
                                );
                                ventstream_telemetry::record_error(format!(
                                    "mysql SQL denormalizer tail recompose failed (attempt {attempt}): {err:#}"
                                ));
                                tokio::select! {
                                    biased;
                                    () = shutdown.cancelled() => break 'tail,
                                    () = tokio::time::sleep(backoff) => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        drop(sender);
    }
}

/// Build the `JSON_OBJECT(...)` doc expression for a definition, enumerating
/// the primary's columns from `information_schema`.
async fn build_doc_expr(pool: &Pool, database: &str, def: &JoinDefinition) -> Result<String> {
    let primary_cols = columns_of(pool, database, relation_of(&def.primary.table)).await?;
    let mut pairs: Vec<String> = primary_cols
        .iter()
        .map(|c| format!("'{}', `p`.`{}`", c.replace('\'', "''"), esc(c)))
        .collect();
    for (i, related) in def.related.iter().enumerate() {
        let alias = format!("r{i}");
        let rel_name = relation_of(&related.table);
        let rel_db = namespace_of(&related.table);
        let rel_db = if rel_db.is_empty() { database } else { rel_db };
        let cols = if related.select.is_empty() {
            columns_of(pool, rel_db, rel_name).await?
        } else {
            related.select.clone()
        };
        let proj = json_object_expr(&alias, &cols);
        let cond = join_condition(&alias, related);
        let table = format!("`{}`.`{}`", esc(rel_db), esc(rel_name));
        let sub = match related.cardinality {
            Cardinality::One => format!("(SELECT {proj} FROM {table} `{alias}` WHERE {cond} LIMIT 1)"),
            // JSON_ARRAYAGG ignores ORDER BY; sort_by is applied client-side.
            Cardinality::Many => format!(
                "COALESCE((SELECT JSON_ARRAYAGG({proj}) FROM {table} `{alias}` WHERE {cond}), JSON_ARRAY())"
            ),
        };
        let embed = related.embed_as.replace('\'', "''");
        pairs.push(format!("'{embed}', {sub}"));
    }
    Ok(format!("JSON_OBJECT({})", pairs.join(", ")))
}

/// Column names of a table in ordinal order.
async fn columns_of(pool: &Pool, database: &str, table: &str) -> Result<Vec<String>> {
    let mut conn = pool.get_conn().await.context("columns_of: connection")?;
    let cols: Vec<String> = conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .with_context(|| format!("enumerating columns of {database}.{table}"))?;
    if cols.is_empty() {
        anyhow::bail!("no columns found for {database}.{table} (does it exist?)");
    }
    Ok(cols)
}

struct ParsedEvent {
    relation: String,
    op: String,
    row: Option<Value>,
    old_row: Option<Value>,
    doc_id_pk: Option<Vec<String>>,
}

fn parse_event(event: &Event) -> ParsedEvent {
    let relation = event
        .headers
        .get("ventstream.cdc.relation")
        .map(str::to_owned)
        .unwrap_or_default();
    let op = event
        .subject
        .as_str()
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_owned();
    let raw = serde_json::from_slice::<Value>(event.payload.as_slice()).ok();
    let nonempty_object =
        |value: &Value| value.as_object().is_some_and(|object| !object.is_empty());
    let (row, old_row) = match (op.as_str(), raw) {
        ("update", Some(raw)) if raw.get("new").is_some() => (
            raw.get("new")
                .filter(|value| nonempty_object(value))
                .cloned(),
            raw.get("old")
                .filter(|value| nonempty_object(value))
                .cloned(),
        ),
        ("delete", Some(raw)) if raw.get("old").is_some() => (
            raw.get("old")
                .filter(|value| nonempty_object(value))
                .cloned(),
            None,
        ),
        (_, Some(raw)) if nonempty_object(&raw) => (Some(raw), None),
        _ => (None, None),
    };
    let doc_id_pk = event
        .headers
        .get("ventstream.doc.id")
        .and_then(pk_from_doc_id_str);
    ParsedEvent {
        relation,
        op,
        row,
        old_row,
        doc_id_pk,
    }
}

/// Decode a `(pk_text…, doc)` row, applying client-side `sort_by` if needed.
fn decode_doc_row(pd: &PreparedDef, row: &Row) -> Result<(Vec<String>, Value)> {
    let n_pk = pd.pk_cols.len();
    let pk_text: Vec<String> = (0..n_pk).map(|i| row_text(row, i)).collect();
    let doc_raw: Option<String> = row.get(n_pk);
    let mut doc: Value = match doc_raw {
        Some(s) => serde_json::from_str(&s).unwrap_or(Value::Null),
        None => Value::Null,
    };
    if pd.needs_sort {
        apply_sorts(&mut doc, &pd.def);
    }
    Ok((pk_text, doc))
}

/// Sort each `cardinality: many` embedded array by its `sort_by` column
/// (JSON_ARRAYAGG doesn't guarantee order).
fn apply_sorts(doc: &mut Value, def: &JoinDefinition) {
    let Some(obj) = doc.as_object_mut() else {
        return;
    };
    for related in &def.related {
        let Some(sort_col) = related.sort_by.as_ref() else {
            continue;
        };
        if let Some(Value::Array(arr)) = obj.get_mut(&related.embed_as) {
            arr.sort_by(|a, b| {
                json_cmp(
                    a.get(sort_col).unwrap_or(&Value::Null),
                    b.get(sort_col).unwrap_or(&Value::Null),
                )
            });
        }
    }
}

fn json_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

/// A MySQL value rendered as its text form (for PK components).
fn row_text(row: &Row, idx: usize) -> String {
    match row.as_ref(idx) {
        Some(MyValue::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(MyValue::Int(i)) => i.to_string(),
        Some(MyValue::UInt(u)) => u.to_string(),
        Some(MyValue::Float(f)) => f.to_string(),
        Some(MyValue::Double(d)) => d.to_string(),
        _ => String::new(),
    }
}

fn mysql_params(vals: &[String]) -> Params {
    Params::Positional(vals.iter().map(|v| MyValue::from(v.clone())).collect())
}

fn extract_cols(row: &Value, cols: &[String]) -> Option<Vec<String>> {
    let obj = row.as_object()?;
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        let v = obj.get(c)?;
        out.push(match v {
            Value::String(s) => s.clone(),
            Value::Null => return None,
            other => other.to_string(),
        });
    }
    Some(out)
}

/// 1:many embed whose child PK is in the doc → the embedded field per PK
/// column to reverse-look-up on, plus the child PK columns. Single- and
/// multi-column PKs both supported (the sink ANDs the columns per tuple).
/// Mirrors the PG path.
fn reverse_lookup_fields(
    primary_pk: &[String],
    related: &RelatedDefinition,
) -> Option<(Vec<String>, Vec<String>)> {
    if related.join_on.from.columns() != primary_pk {
        return None;
    }
    let pk_cols = related.pk.columns();
    if pk_cols.is_empty() {
        return None;
    }
    // Every PK column must be embedded (else its keyword field doesn't exist).
    if !related.select.is_empty()
        && !pk_cols
            .iter()
            .all(|c| related.select.iter().any(|s| s == c))
    {
        return None;
    }
    // Base fields (no `.keyword`): the sink matches both base and `.keyword`,
    // so numeric PKs (mapped as `long`) and string PKs both resolve.
    let fields = pk_cols
        .iter()
        .map(|c| format!("{}.{}", related.embed_as, c))
        .collect();
    Some((fields, pk_cols.to_vec()))
}

fn pk_from_doc_id_str(doc_id: &str) -> Option<Vec<String>> {
    let (_table, suffix) = doc_id.split_once(':')?;
    let parsed: Vec<Value> = serde_json::from_str(suffix).ok()?;
    if parsed.is_empty() {
        return None;
    }
    Some(
        parsed
            .iter()
            .map(ventstream_core::doc_id::component_text)
            .collect(),
    )
}

fn primary_pk_from_doc_id(doc_id: &str, primary_table: &str) -> Option<Vec<String>> {
    let rest = doc_id.strip_prefix(&format!("{primary_table}:"))?;
    let parsed: Vec<Value> = serde_json::from_str(rest).ok()?;
    if parsed.is_empty() {
        return None;
    }
    Some(
        parsed
            .iter()
            .map(ventstream_core::doc_id::component_text)
            .collect(),
    )
}

/// Build a composed-doc event (subject `mysql.{ns}.{rel}.{op}`, doc-id header).
fn build_doc_event(
    primary_table: &str,
    target_index: Option<&str>,
    pk_text: &[String],
    doc: &Value,
    delete: bool,
) -> Result<Event> {
    let ns = namespace_of(primary_table);
    let rel = relation_of(primary_table);
    let doc_id = ventstream_core::doc_id::doc_id(primary_table, pk_text);
    let op = if delete { "delete" } else { "upsert" };
    let subject = Subject::new(format!("mysql.{ns}.{rel}.{op}"))
        .map_err(|e| anyhow::anyhow!("invalid subject: {e}"))?;
    let source = SourceUri::new(format!("mysql-sqljoin://{primary_table}"))
        .map_err(|e| anyhow::anyhow!("invalid source uri: {e}"))?;
    let payload = if delete {
        Payload::from_vec(b"{}".to_vec())
    } else {
        Payload::from_vec(serde_json::to_vec(doc).unwrap_or_else(|_| b"{}".to_vec()))
    };
    let mut headers = HashMap::new();
    headers.insert("ventstream.cdc.relation".to_owned(), rel.to_owned());
    headers.insert("ventstream.cdc.namespace".to_owned(), ns.to_owned());
    headers.insert("ventstream.doc.id".to_owned(), doc_id);
    if let Some(target_index) = target_index {
        headers.insert(
            "ventstream.target.index".to_owned(),
            target_index.to_owned(),
        );
    }
    if delete {
        headers.insert("ventstream.cdc.op".to_owned(), "delete".to_owned());
    }
    Ok(Event::builder(source, subject)
        .payload(payload)
        .content_type(ContentType::Json)
        .headers(Headers::from_map(headers))
        .build())
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
    use serde_json::json;

    fn sample_def() -> JoinDefinition {
        serde_json::from_value(json!({
            "name": "orders",
            "primary": { "table": "shop.orders", "pk": "id" },
            "related": [
                { "id": "customer", "table": "shop.customers", "pk": "id",
                  "join_on": { "from": "customer_id", "to": "id" },
                  "embed_as": "customer", "cardinality": "one", "select": ["id", "name"] },
                { "id": "items", "table": "shop.order_items", "pk": "id",
                  "join_on": { "from": "id", "to": "order_id" },
                  "embed_as": "items", "cardinality": "many", "sort_by": "id" }
            ]
        }))
        .expect("deserialize")
    }

    #[test]
    fn json_object_and_join_condition() {
        assert_eq!(
            json_object_expr("r0", &["id".to_owned(), "name".to_owned()]),
            "JSON_OBJECT('id', `r0`.`id`, 'name', `r0`.`name`)"
        );
        let def = sample_def();
        assert_eq!(
            join_condition("r0", &def.related[0]),
            "`r0`.`id` = `p`.`customer_id`"
        );
    }

    #[test]
    fn recompose_query_batches_single_column_keys() {
        let def = sample_def();
        let pd = PreparedDef {
            primary_table: def.primary.table.clone(),
            database: "shop".to_owned(),
            pk_cols: vec!["id".to_owned()],
            doc: "JSON_OBJECT('id', `p`.`id`)".to_owned(),
            needs_sort: false,
            def,
        };
        let (sql, params) =
            build_recompose_query(&pd, &[vec!["1".to_owned()], vec!["2".to_owned()]])
                .expect("query");
        assert!(sql.contains("WHERE `p`.`id` IN (?, ?)"));
        assert_eq!(params, vec!["1", "2"]);
    }

    #[test]
    fn recompose_query_batches_composite_keys_with_row_constructors() {
        let mut def = sample_def();
        def.primary.pk = serde_json::from_value(json!(["tenant_id", "id"])).expect("composite key");
        let pd = PreparedDef {
            primary_table: def.primary.table.clone(),
            database: "shop".to_owned(),
            pk_cols: vec!["tenant_id".to_owned(), "id".to_owned()],
            doc: "JSON_OBJECT('id', `p`.`id`)".to_owned(),
            needs_sort: false,
            def,
        };
        let (sql, params) = build_recompose_query(
            &pd,
            &[
                vec!["a".to_owned(), "1".to_owned()],
                vec!["b".to_owned(), "2".to_owned()],
            ],
        )
        .expect("query");
        assert!(sql.contains("WHERE (`p`.`tenant_id`, `p`.`id`) IN ((?, ?), (?, ?))"));
        assert_eq!(params, vec!["a", "1", "b", "2"]);
    }

    #[test]
    fn tail_retry_backoff_grows_and_caps_deterministically() {
        let mut retry = TailRetry::default();
        let expected = [
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_millis(1_600),
            Duration::from_millis(3_200),
            Duration::from_millis(6_400),
            Duration::from_millis(12_800),
            Duration::from_millis(25_600),
            Duration::from_secs(30),
            Duration::from_secs(30),
        ];

        for (index, expected_backoff) in expected.into_iter().enumerate() {
            let (attempt, backoff) = retry.record_failure();
            assert_eq!(attempt, index as u32 + 1);
            assert_eq!(backoff, expected_backoff);
        }
    }

    #[test]
    fn tail_retry_success_reports_recovery_and_resets_state() {
        let mut retry = TailRetry::default();
        assert_eq!(retry.record_success(), None);

        assert_eq!(retry.record_failure(), (1, Duration::from_millis(100)));
        assert_eq!(retry.record_failure(), (2, Duration::from_millis(200)));
        assert_eq!(retry.record_success(), Some(2));
        assert_eq!(retry, TailRetry::default());
        assert_eq!(retry.record_failure(), (1, Duration::from_millis(100)));
    }

    #[tokio::test]
    async fn acknowledgement_barrier_is_forwarded_unchanged() {
        let denormalizer = MySqlDenormalizer {
            pool: Pool::new(mysql_async::Opts::default()),
            defs: Vec::new(),
            chunk_size: 1,
            recompose_key_chunk: 1,
            recompose_query_concurrency: 1,
            sink_lookup: None,
        };
        let source = SourceUri::new("mysql-cdc://test").expect("source");
        let subject = Subject::new("ventstream.internal.ack_barrier").expect("subject");
        let mut headers = HashMap::new();
        headers.insert(ACK_BARRIER_HEADER.to_owned(), "true".to_owned());
        headers.insert("ventstream.cdc.ack_seq".to_owned(), "9".to_owned());
        let barrier = Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();
        let (sender, mut receiver) = ventstream_core::bus::channel(4);
        let shutdown = ShutdownToken::new();

        denormalizer
            .handle_ordered_tail_batch(std::slice::from_ref(&barrier), &sender, &shutdown)
            .await
            .expect("forward barrier");

        assert_eq!(receiver.recv().await, Some(barrier));
    }

    #[test]
    fn doc_id_pk_roundtrip() {
        let id = ventstream_core::doc_id::doc_id("shop.orders", &["1".to_owned()]);
        assert_eq!(pk_from_doc_id_str(&id), Some(vec!["1".to_owned()]));
        assert_eq!(
            primary_pk_from_doc_id(&id, "shop.orders"),
            Some(vec!["1".to_owned()])
        );
        assert_eq!(primary_pk_from_doc_id(&id, "shop.other"), None);
    }

    #[test]
    fn parse_delete_uses_before_image_when_present() {
        let source = SourceUri::new("mysql://shop/order_items").expect("source");
        let subject = Subject::new("mysql.shop.order_items.delete").expect("subject");
        let mut headers = HashMap::new();
        headers.insert(
            "ventstream.cdc.relation".to_owned(),
            "order_items".to_owned(),
        );
        headers.insert(
            "ventstream.doc.id".to_owned(),
            ventstream_core::doc_id::doc_id("shop.order_items", &["item-1".to_owned()]),
        );
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(
                serde_json::to_vec(&json!({"id": "item-1", "order_id": "order-7"}))
                    .expect("payload"),
            ))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();

        let parsed = parse_event(&event);
        assert_eq!(parsed.op, "delete");
        assert_eq!(
            parsed
                .row
                .as_ref()
                .and_then(|row| extract_cols(row, &["order_id".to_owned()])),
            Some(vec!["order-7".to_owned()])
        );
        assert_eq!(parsed.doc_id_pk, Some(vec!["item-1".to_owned()]));
        assert!(parsed.old_row.is_none());
    }

    #[test]
    fn parse_empty_delete_keeps_doc_id_fallback() {
        let source = SourceUri::new("mysql://shop/order_items").expect("source");
        let subject = Subject::new("mysql.shop.order_items.delete").expect("subject");
        let mut headers = HashMap::new();
        headers.insert(
            "ventstream.cdc.relation".to_owned(),
            "order_items".to_owned(),
        );
        headers.insert(
            "ventstream.doc.id".to_owned(),
            ventstream_core::doc_id::doc_id("shop.order_items", &["item-1".to_owned()]),
        );
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();

        let parsed = parse_event(&event);
        assert!(parsed.row.is_none());
        assert!(parsed.old_row.is_none());
        assert_eq!(parsed.doc_id_pk, Some(vec!["item-1".to_owned()]));
    }

    #[test]
    fn parse_update_preserves_old_and_new_join_keys() {
        let source = SourceUri::new("mysql://shop/order_items").expect("source");
        let subject = Subject::new("mysql.shop.order_items.update").expect("subject");
        let mut headers = HashMap::new();
        headers.insert(
            "ventstream.cdc.relation".to_owned(),
            "order_items".to_owned(),
        );
        let event = Event::builder(source, subject)
            .payload(Payload::from_vec(
                serde_json::to_vec(&json!({
                    "new": {"id": "item-1", "order_id": "order-2"},
                    "old": {"id": "item-1", "order_id": "order-1"}
                }))
                .expect("payload"),
            ))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(headers))
            .build();

        let parsed = parse_event(&event);
        assert_eq!(
            parsed
                .row
                .as_ref()
                .and_then(|row| extract_cols(row, &["order_id".to_owned()])),
            Some(vec!["order-2".to_owned()])
        );
        assert_eq!(
            parsed
                .old_row
                .as_ref()
                .and_then(|row| extract_cols(row, &["order_id".to_owned()])),
            Some(vec!["order-1".to_owned()])
        );
    }

    #[test]
    fn build_doc_event_stamps_and_renders_projection_target_for_upserts_and_deletes() {
        for delete in [false, true] {
            let event = build_doc_event(
                "shop.orders",
                Some("tenant-orders"),
                &["5".to_owned()],
                &json!({ "id": "5" }),
                delete,
            )
            .expect("event");

            assert_eq!(
                event.headers.get("ventstream.target.index"),
                Some("tenant-orders")
            );
            assert_eq!(
                index_template::render(
                    "${header:ventstream.target.index}",
                    &event,
                    chrono::Utc::now()
                )
                .expect("projection target routing should render"),
                "tenant-orders"
            );
        }
    }

    #[test]
    fn apply_sorts_orders_many_array() {
        let def = sample_def();
        let mut doc = json!({
            "id": 1,
            "items": [{"id": 3}, {"id": 1}, {"id": 2}]
        });
        apply_sorts(&mut doc, &def);
        let ids: Vec<i64> = doc["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn reverse_lookup_fields_one_to_many_single_and_composite() {
        let def = sample_def();
        // items: from=id (==primary pk) → 1:many, pk embedded → one field
        assert_eq!(
            reverse_lookup_fields(def.primary.pk.columns(), &def.related[1]),
            Some((vec!["items.id".to_owned()], vec!["id".to_owned()]))
        );
        // customer: from=customer_id (!=primary pk) → many:1 → None
        assert_eq!(
            reverse_lookup_fields(def.primary.pk.columns(), &def.related[0]),
            None
        );
    }

    #[test]
    fn reverse_lookup_fields_supports_composite_child_pk() {
        // 1:many embed with a composite child PK [region, item_id].
        let def: JoinDefinition = serde_json::from_value(json!({
            "name": "orders",
            "primary": { "table": "shop.orders", "pk": "id" },
            "related": [
                { "id": "items", "table": "shop.order_items", "pk": ["region", "item_id"],
                  "join_on": { "from": "id", "to": "order_id" },
                  "embed_as": "items", "cardinality": "many" }
            ]
        }))
        .expect("deserialize");
        assert_eq!(
            reverse_lookup_fields(def.primary.pk.columns(), &def.related[0]),
            Some((
                vec!["items.region".to_owned(), "items.item_id".to_owned()],
                vec!["region".to_owned(), "item_id".to_owned()]
            ))
        );
    }
}
