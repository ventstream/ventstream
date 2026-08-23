//! SQL-side denormalisation for the Postgres source — the bounded-memory
//! alternative to the in-memory join engine.
//!
//! The in-memory [`ventstream_joins::JoinEngine`] holds every related row +
//! reverse index resident, so RSS scales with the joined dataset. This path
//! keeps the SAME `joins:` spec but pushes the join into Postgres:
//!
//! - **Bootstrap.** For each join definition, run a keyset-chunked
//!   `SELECT … to_jsonb(p) || jsonb_build_object('<embed>', <related>) … AS doc
//!   FROM <primary> p WHERE <pk> > $cursor ORDER BY <pk> LIMIT <chunk>`. PG
//!   composes each doc; we stream them straight to the sink. Memory is
//!   O(chunk), not O(dataset).
//! - **Tail.** For each CDC row event, compute the affected primary key(s)
//!   and re-run the same compose SQL for just those keys (`WHERE pk = ANY`).
//!   A change to a child row recomposes its parent; a change to a parent
//!   recomposes itself. No resident state — each recompose re-queries PG
//!   (which is what keeps it bounded), trading RAM for indexed queries.
//!
//! This mirrors the Neo4j denormalize model (query-per-primary, bounded
//! memory) for Postgres. Requires indexes on the FK columns to be fast.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::Client;
use tracing::{debug, info, warn};
use ventstream_core::{
    ContentType, Event, EventReceiver, EventSender, Headers, Payload, ShutdownToken, SourceUri,
    Subject,
};
use ventstream_joins::config::{BackfillConfig, StateConfig, TargetConfig};
use ventstream_joins::{Cardinality, JoinDefinition, PkSpec, PrimaryRef, RelatedDefinition};
#[cfg(test)]
use ventstream_sinks::opensearch::index_template;
use ventstream_sinks::ReverseLookup;
use ventstream_sources::postgres::{connect_client, type_change_epoch, PostgresCdcConfig};

/// Greedy-drain ceiling for the tail loop. `recv_batch` returns one event
/// immediately when idle (low latency) and opportunistically drains up to this
/// many already-queued events under load — so child deletes that arrive
/// together collapse into a single batched reverse-lookup query per field.
const TAIL_DRAIN_LIMIT: usize = 1024;
const JOIN_BARRIER_HEADER: &str = "ventstream.internal.join_barrier";
const JOIN_SEQUENCE_HEADER: &str = "ventstream.internal.join_seq";
const TAIL_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const TAIL_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct SqlDenormalizeDurability {
    transform_progress: Arc<AtomicU64>,
    source_progress: Arc<AtomicU64>,
}

impl SqlDenormalizeDurability {
    pub fn new(transform_progress: Arc<AtomicU64>, source_progress: Arc<AtomicU64>) -> Self {
        Self {
            transform_progress,
            source_progress,
        }
    }

    async fn commit(
        &self,
        sender: &EventSender,
        shutdown: &ShutdownToken,
        next_sequence: &mut u64,
        source_watermark: u64,
    ) -> Result<bool> {
        let sequence = *next_sequence;
        *next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("SQL denormalizer durability sequence exhausted"))?;

        sender
            .send(durability_barrier(sequence)?, shutdown)
            .await
            .map_err(|err| {
                anyhow::anyhow!("emitting SQL denormalizer durability barrier: {err}")
            })?;

        let mut poll = tokio::time::interval(Duration::from_millis(5));
        loop {
            if self.transform_progress.load(Ordering::Acquire) >= sequence {
                break;
            }
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return Ok(false),
                _ = poll.tick() => {}
            }
        }

        if source_watermark > 0 {
            self.source_progress
                .fetch_max(source_watermark, Ordering::Release);
        }
        Ok(true)
    }
}

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
        let failures = self.failures;
        *self = Self::default();
        Some(failures)
    }
}

/// Render `"ns"."rel"` from a `ns.rel` table string (quotes each segment).
fn quote_qualified(table: &str) -> String {
    match table.split_once('.') {
        Some((ns, rel)) => format!("{}.{}", quote_ident(ns), quote_ident(rel)),
        None => quote_ident(table),
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Resolve column types without reading table data. `format_type` returns a
/// SQL-ready type spelling (including quoting and modifiers), which is used by
/// the indexed text-to-native casts in recomposition queries.
async fn load_column_types(
    client: &Client,
    table: &str,
    columns: &[String],
) -> Result<HashMap<String, String>> {
    if columns.is_empty() {
        return Ok(HashMap::new());
    }
    let relation = quote_qualified(table);
    let requested = columns.to_vec();
    let rows = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = to_regclass($1) \
               AND a.attname = ANY($2::text[]) \
               AND a.attnum > 0 \
               AND NOT a.attisdropped",
            &[&relation, &requested],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect())
}

/// Relation segment of a `ns.rel` table (what `ventstream.cdc.relation`
/// carries) — used to route events to the right index and to match
/// incoming tail events against a definition's tables.
fn relation_of(table: &str) -> &str {
    table.rsplit('.').next().unwrap_or(table)
}

fn namespace_of(table: &str) -> &str {
    table.split('.').next().unwrap_or("")
}

/// The projection of a related row: the whole row, or a built object of
/// the selected columns.
fn related_projection(alias: &str, select: &[String]) -> String {
    if select.is_empty() {
        format!("to_jsonb({alias})")
    } else {
        let pairs = select
            .iter()
            .map(|c| format!("'{}', {alias}.{}", c.replace('\'', "''"), quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("jsonb_build_object({pairs})")
    }
}

/// The `WHERE r.to = p.from [AND …]` join condition for one related entry.
fn join_condition(rel_alias: &str, related: &RelatedDefinition) -> String {
    related
        .join_on
        .from
        .columns()
        .iter()
        .zip(related.join_on.to.columns())
        .map(|(from, to)| format!("{rel_alias}.{} = p.{}", quote_ident(to), quote_ident(from)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Build the `doc` expression: `to_jsonb(p) || jsonb_build_object(...)` per
/// related entry (object for `one`, COALESCE'd array for `many`).
fn doc_expr(def: &JoinDefinition) -> String {
    let mut expr = String::from("to_jsonb(p)");
    for (i, related) in def.related.iter().enumerate() {
        let alias = format!("r{i}");
        let table = quote_qualified(&related.table);
        let cond = join_condition(&alias, related);
        let proj = related_projection(&alias, &related.select);
        let embed = related.embed_as.replace('\'', "''");
        let sub = match related.cardinality {
            Cardinality::One => {
                format!("(SELECT {proj} FROM {table} {alias} WHERE {cond} LIMIT 1)")
            }
            Cardinality::Many => {
                let order = related
                    .sort_by
                    .as_ref()
                    .map(|s| format!(" ORDER BY {alias}.{}", quote_ident(s)))
                    .unwrap_or_default();
                format!(
                    "COALESCE((SELECT jsonb_agg({proj}{order}) FROM {table} {alias} WHERE {cond}), '[]'::jsonb)"
                )
            }
        };
        expr.push_str(&format!(" || jsonb_build_object('{embed}', {sub})"));
    }
    expr
}

/// One join definition prepared for SQL execution.
struct PreparedDef {
    /// `ns.rel` of the primary table.
    primary_table: String,
    /// Quoted primary PK columns (e.g. `p."id"`).
    pk_cols: Vec<String>,
    /// Native PG types of the PK columns (for the keyset cast).
    pk_types: Vec<String>,
    /// The `doc` SQL expression.
    doc: String,
    /// SELECT of PK columns as text (for cursor + doc id).
    pk_text_select: String,
    /// Native PG type of each primary-side `join_on.from` column, so the
    /// foreign-change reverse lookup can compare in the column's type and
    /// hit the FK index instead of seq-scanning via `::text`.
    from_types: HashMap<String, String>,
    /// `type_change_epoch` of the primary table when the types above were
    /// loaded; a bump means the source saw a column retype and the cached
    /// spellings may build wrong casts.
    type_epoch: u64,
    def: JoinDefinition,
}

impl PreparedDef {
    /// `SELECT <pk::text>…, (<doc>)::text AS doc FROM <primary> p` — text
    /// so the bytes go straight into the payload, no parse/reserialize.
    fn select_head(&self) -> String {
        format!(
            "SELECT {pk}, ({doc})::text AS doc FROM {table} p",
            pk = self.pk_text_select,
            doc = self.doc,
            table = quote_qualified(&self.primary_table),
        )
    }
}

/// Bounded-memory SQL denormaliser. Owns a query connection; bootstraps and
/// recomposes by re-querying Postgres rather than holding join state.
pub struct SqlDenormalizer {
    client: Client,
    /// Source config retained so the tail loop can rebuild `client` after a
    /// connection loss. A `tokio_postgres::Client` never recovers once its
    /// connection task ends, so retrying queries on it stalls the pipeline
    /// until process restart; redialing also re-resolves DNS, surviving a
    /// database failover to a new address.
    source: PostgresCdcConfig,
    defs: Vec<PreparedDef>,
    chunk_size: i64,
    emit_target_clears: bool,
    /// When set, 1:many child deletes whose WAL lacks the parent FK (Postgres
    /// `REPLICA IDENTITY DEFAULT` logs only the child PK) are resolved by
    /// querying the sink for the parent doc that embeds the deleted child PK,
    /// then recomposing it. None disables the fallback (the delete is skipped,
    /// matching the pre-fix behaviour — operators who don't want sink reads can
    /// instead set `REPLICA IDENTITY FULL`/`USING INDEX` on the child table).
    /// Holds a pooled HTTP client so a stream of child deletes reuses
    /// connections rather than handshaking per lookup.
    sink_lookup: Option<ReverseLookup>,
    /// When set, the tail loop reports sustained batch-retry stalls so the
    /// readiness gate can flip `/readyz` instead of the process looking
    /// healthy while making no progress.
    pipeline_health: Option<ventstream_core::SinkHealth>,
}

impl SqlDenormalizer {
    /// Connect and prepare the definitions (fetches PK types).
    ///
    /// When no explicit join definitions are supplied, every primary-keyed
    /// table in the publication becomes a direct projection. This keeps SQL
    /// mode useful for plain table replication while retaining the same
    /// bounded-memory bootstrap and full-row recomposition semantics used by
    /// joined projections.
    pub async fn connect(
        source: &PostgresCdcConfig,
        publication: &str,
        mut joins: Vec<JoinDefinition>,
        chunk_size: i64,
    ) -> Result<Self> {
        let client = connect_client(source, "sql-denormalize connect")
            .await
            .context("sql-denormalize connect")?;

        if joins.is_empty() {
            joins = discover_direct_projections(&client, publication).await?;
            if joins.is_empty() {
                anyhow::bail!(
                    "publication '{publication}' has no primary-keyed tables to materialize"
                );
            }
            info!(
                publication,
                projections = joins.len(),
                "sql-denormalize: discovered direct publication projections"
            );
        }

        let mut defs = Vec::with_capacity(joins.len());
        for def in joins {
            let primary_table = def.primary.table.clone();
            let pk_cols: Vec<String> = def
                .primary
                .pk
                .columns()
                .iter()
                .map(|c| format!("p.{}", quote_ident(c)))
                .collect();
            let pk_text_select = def
                .primary
                .pk
                .columns()
                .iter()
                .enumerate()
                .map(|(i, c)| format!("p.{}::text AS __pk{i}", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", ");
            // Read native types from the catalog rather than `pg_typeof()` on
            // a sample row. The latter silently fell back to `text` for an
            // empty table, preparing invalid predicates such as
            // `bigint_pk = ANY($1::text[])` before the table's first insert.
            let pk_type_map = load_column_types(&client, &primary_table, def.primary.pk.columns())
                .await
                .with_context(|| format!("fetching pk types for {primary_table}"))?;
            let pk_types: Vec<String> = def
                .primary
                .pk
                .columns()
                .iter()
                .map(|column| {
                    pk_type_map
                        .get(column)
                        .cloned()
                        .unwrap_or_else(|| "text".to_owned())
                })
                .collect();
            // Types of the primary-side `from` columns (for indexed reverse
            // lookups on foreign-side changes).
            let from_cols: Vec<String> = {
                let mut seen = std::collections::BTreeSet::new();
                for r in &def.related {
                    for c in r.join_on.from.columns() {
                        seen.insert(c.clone());
                    }
                }
                seen.into_iter().collect()
            };
            let from_types = load_column_types(&client, &primary_table, &from_cols)
                .await
                .with_context(|| format!("fetching join-column types for {primary_table}"))?;
            let type_epoch = type_change_epoch(&primary_table);
            defs.push(PreparedDef {
                primary_table,
                pk_cols,
                pk_types,
                doc: doc_expr(&def),
                pk_text_select,
                from_types,
                type_epoch,
                def,
            });
        }
        Ok(Self {
            client,
            source: source.clone(),
            defs,
            chunk_size,
            emit_target_clears: false,
            sink_lookup: None,
            pipeline_health: None,
        })
    }

    /// Report tail-loop stalls on this health channel.
    #[must_use]
    pub fn with_pipeline_health(mut self, health: ventstream_core::SinkHealth) -> Self {
        self.pipeline_health = Some(health);
        self
    }

    /// Emit target-scoped clear events for sinks that support them.
    #[must_use]
    pub const fn with_target_clears(mut self) -> Self {
        self.emit_target_clears = true;
        self
    }

    /// Enable the sink reverse-index fallback for 1:many child deletes.
    ///
    /// Pass the same `OpenSearchConfig` the dispatcher writes to. On a child
    /// delete whose parent FK isn't in the WAL, the denormalizer issues a
    /// `terms` query against the embedded child-PK keyword field to find the
    /// affected parent doc(s) and recomposes them. Bounded memory is preserved
    /// — there's no resident FK cache, just an on-demand query, and the HTTP
    /// client is pooled across lookups.
    #[must_use]
    pub fn with_reverse_lookup(mut self, lookup: ReverseLookup) -> Self {
        self.sink_lookup = Some(lookup);
        self
    }

    /// Bootstrap every definition: keyset-chunked SQL join → composed docs.
    async fn bootstrap(&self, sender: &EventSender, shutdown: &ShutdownToken) -> Result<()> {
        for pd in &self.defs {
            let started = std::time::Instant::now();
            let emitted = self.emit_all(pd, None, sender, shutdown).await?;
            info!(
                primary = %pd.primary_table,
                output = %pd.def.effective_name(),
                rows = emitted,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "sql-denormalize bootstrap: spec complete"
            );
        }
        Ok(())
    }

    async fn emit_all(
        &self,
        pd: &PreparedDef,
        lsn: Option<&str>,
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<u64> {
        let mut emitted = 0u64;
        let mut rows = self.fetch_chunk(pd, None).await?;
        loop {
            if shutdown.is_cancelled() || rows.is_empty() {
                return Ok(emitted);
            }
            let n_pk = pd.def.primary.pk.columns().len();
            let full_page = rows.len() as i64 >= self.chunk_size;
            let Some(last_row) = rows.last() else {
                return Ok(emitted);
            };
            let page_last: Vec<String> = (0..n_pk).map(|i| last_row.get::<_, String>(i)).collect();

            // Prefetch: the next chunk's query overlaps this chunk's emit
            // (the tokio_postgres driver task progresses IO independently).
            let fetch_next = async {
                if full_page {
                    Some(self.fetch_chunk(pd, Some(&page_last)).await)
                } else {
                    None
                }
            };
            let emit_chunk = async {
                let mut sent = 0u64;
                for row in &rows {
                    let pk_text: Vec<String> = (0..n_pk).map(|i| row.get::<_, String>(i)).collect();
                    let doc: String = row.get(n_pk);
                    let event = build_doc_event(
                        &pd.primary_table,
                        pd.def.target_index(),
                        &pk_text,
                        Some(&doc),
                        false,
                        lsn,
                    )?;
                    sender
                        .send(event, shutdown)
                        .await
                        .map_err(|err| anyhow::anyhow!("emitting full projection row: {err}"))?;
                    ventstream_telemetry::bump_events_emitted(1);
                    sent = sent.saturating_add(1);
                }
                Ok::<u64, anyhow::Error>(sent)
            };
            let (emit_result, next) = tokio::join!(emit_chunk, fetch_next);
            emitted = emitted.saturating_add(emit_result?);
            match next {
                Some(fetched) => rows = fetched?,
                None => return Ok(emitted),
            }
        }
    }

    /// One keyset-paginated projection chunk after `last`, PK order. The
    /// doc column arrives as text and moves straight into the payload.
    async fn fetch_chunk(
        &self,
        pd: &PreparedDef,
        last: Option<&[String]>,
    ) -> Result<Vec<tokio_postgres::Row>> {
        let order = pd.pk_cols.join(", ");
        let (where_clause, params): (String, Vec<String>) = match last {
            None => (String::new(), Vec::new()),
            Some(vals) => {
                let lhs = pd.pk_cols.join(", ");
                let rhs = pd
                    .pk_types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| format!("${}::text::{}", i + 1, t))
                    .collect::<Vec<_>>()
                    .join(", ");
                (format!("WHERE ({lhs}) > ({rhs}) "), vals.to_vec())
            }
        };
        let sql = format!(
            "{head} {where_clause}ORDER BY {order} LIMIT {limit}",
            head = pd.select_head(),
            limit = self.chunk_size,
        );
        let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        self.client
            .query(&sql, &bind)
            .await
            .with_context(|| format!("full projection select for {}", pd.primary_table))
    }

    /// Recompose a set of primary keys for one definition and emit docs.
    ///
    /// Returns the PK-text of every primary that actually existed in Postgres
    /// (i.e. an upsert was emitted). Keys that no longer exist produce no row,
    /// so they're absent from the result — the caller uses this to decide
    /// which deleted keys still need a tombstone (Postgres truth, not arrival
    /// order, settles within-batch delete↔reinsert races).
    async fn recompose(
        &self,
        pd: &PreparedDef,
        pks: &[Vec<String>],
        lsn: Option<&str>,
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<Vec<Vec<String>>> {
        let mut emitted: Vec<Vec<String>> = Vec::new();
        if pks.is_empty() {
            return Ok(emitted);
        }
        let n_pk = pd.def.primary.pk.columns().len();
        // Single-column PK: WHERE pk = ANY($1::text[]::type[]) — one query
        // for the whole batch. Composite PK: one query per key (rare path).
        if n_pk == 1 {
            let flat: Vec<String> = pks.iter().filter_map(|p| p.first().cloned()).collect();
            let ty = pd.pk_types.first().map_or("text", |t| t.as_str());
            let sql = format!(
                "{head} WHERE {pk} = ANY($1::text[]::{ty}[])",
                head = pd.select_head(),
                pk = pd.pk_cols.first().cloned().unwrap_or_default(),
            );
            let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                vec![&flat as &(dyn tokio_postgres::types::ToSql + Sync)];
            let rows = self
                .client
                .query(&sql, &bind)
                .await
                .with_context(|| format!("recompose for {}", pd.primary_table))?;
            for row in &rows {
                let pk_text: Vec<String> = (0..n_pk).map(|i| row.get::<_, String>(i)).collect();
                let doc: String = row.get(n_pk);
                let event = build_doc_event(
                    &pd.primary_table,
                    pd.def.target_index(),
                    &pk_text,
                    Some(&doc),
                    false,
                    lsn,
                )?;
                sender
                    .send(event, shutdown)
                    .await
                    .map_err(|err| anyhow::anyhow!("emitting recomposed projection row: {err}"))?;
                ventstream_telemetry::bump_events_emitted(1);
                emitted.push(pk_text);
            }
        } else {
            for pk in pks {
                let lhs = pd.pk_cols.join(", ");
                let rhs = pd
                    .pk_types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| format!("${}::text::{}", i + 1, t))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!("{head} WHERE ({lhs}) = ({rhs})", head = pd.select_head());
                let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = pk
                    .iter()
                    .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                let rows = self.client.query(&sql, &bind).await?;
                for row in &rows {
                    let pk_text: Vec<String> = (0..n_pk).map(|i| row.get::<_, String>(i)).collect();
                    let doc: String = row.get(n_pk);
                    let event = build_doc_event(
                        &pd.primary_table,
                        pd.def.target_index(),
                        &pk_text,
                        Some(&doc),
                        false,
                        lsn,
                    )?;
                    sender.send(event, shutdown).await.map_err(|err| {
                        anyhow::anyhow!("emitting recomposed projection row: {err}")
                    })?;
                    ventstream_telemetry::bump_events_emitted(1);
                    emitted.push(pk_text);
                }
            }
        }
        Ok(emitted)
    }

    /// Handle a drained batch of tail CDC events.
    ///
    /// Mirrors the per-event resolution but with two batch wins: (a) all child
    /// deletes whose parent FK is missing (`REPLICA IDENTITY DEFAULT`) are
    /// resolved with ONE sink reverse-lookup `terms` query per embedded field,
    /// and (b) each affected primary is recomposed once even if several events
    /// in the batch touched it.
    ///
    /// Recomposed docs and tombstones are stamped with the batch's high-water
    /// LSN. CDC events arrive in WAL order and the recompose re-queries the
    /// *current* PG state, so a recomposed doc already reflects every commit up
    /// to that LSN; the source slot advances past the whole batch once the sink
    /// confirms it. (Same batch-watermark stamping the Neo4j tail handler uses.)
    async fn handle_tail_batch(
        &self,
        events: &[Event],
        sender: &EventSender,
        shutdown: &ShutdownToken,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let batch_lsn = max_lsn(events);
        let batch_lsn = batch_lsn.as_deref();

        // Parse each event once: (relation, op, effective row, update
        // pre-image). Insert payloads are the flat row; updates are a
        // `{new, old}` envelope; deletes are `{old}` — `effective_row`
        // picks the post-image for insert/update and the pre-image for
        // delete. The update pre-image is kept separately: when the
        // primary key itself changed it names the OLD identity, whose
        // projection must be tombstoned or it stays behind forever.
        let parsed: Vec<(String, String, Value, Option<Value>)> = events
            .iter()
            .map(|event| {
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
                let raw: Value =
                    serde_json::from_slice(event.payload.as_slice()).unwrap_or(Value::Null);
                let row = effective_row(&raw, &op);
                let pre_image = (op == "update").then(|| raw.get("old").cloned()).flatten();
                (relation, op, row, pre_image)
            })
            .collect();

        for pd in &self.defs {
            let primary_truncated = parsed.iter().any(|(relation, op, _, _)| {
                op == "truncate" && relation_of(&pd.primary_table) == relation
            });
            let related_truncated = parsed.iter().any(|(relation, op, _, _)| {
                op == "truncate"
                    && pd
                        .def
                        .related
                        .iter()
                        .any(|related| relation_of(&related.table) == relation)
            });
            if primary_truncated && self.emit_target_clears {
                let clear =
                    build_target_clear_event(&pd.primary_table, pd.def.target_index(), batch_lsn)?;
                sender
                    .send(clear, shutdown)
                    .await
                    .map_err(|err| anyhow::anyhow!("emitting projection clear: {err}"))?;
                ventstream_telemetry::bump_events_emitted(1);
                let rows = self.emit_all(pd, batch_lsn, sender, shutdown).await?;
                debug!(
                    primary = %pd.primary_table,
                    rows,
                    "sql-denormalize primary truncate cleared and rebuilt projection"
                );
                continue;
            }
            if related_truncated {
                let rows = self.emit_all(pd, batch_lsn, sender, shutdown).await?;
                debug!(
                    primary = %pd.primary_table,
                    rows,
                    "sql-denormalize related truncate recomposed projection"
                );
            }

            let mut affected: HashSet<Vec<String>> = HashSet::new();
            let mut deletes: HashSet<Vec<String>> = HashSet::new();
            // Child deletes missing the parent FK: bucket the child PK values
            // per embedded keyword field for one batched reverse-lookup each.
            // Child-delete reverse-lookup, keyed by the embedded PK fields →
            // the deleted children's PK tuples (single- or multi-column).
            let mut lookup_buckets: HashMap<Vec<String>, Vec<Vec<String>>> = HashMap::new();
            // many:1 parent changes: bucket the changed `to` tuples per related
            // def index, for one coalesced PG lookup each.
            let mut many1_buckets: HashMap<usize, Vec<Vec<String>>> = HashMap::new();

            for (relation, op, row, pre_image) in &parsed {
                // Primary-table event → the row's own PK.
                if relation_of(&pd.primary_table) == relation.as_str() {
                    if op == "delete" {
                        if let Some(pk) = extract_cols(row, pd.def.primary.pk.columns()) {
                            deletes.insert(pk);
                        }
                        continue;
                    }
                    if let Some(pk) = extract_cols(row, pd.def.primary.pk.columns()) {
                        // A key-changing update relocated the projection:
                        // the pre-image names the old identity, which must
                        // be tombstoned (verified against Postgres truth
                        // below like any other delete).
                        if let Some(old_pk) = pre_image
                            .as_ref()
                            .and_then(|old| extract_cols(old, pd.def.primary.pk.columns()))
                        {
                            if old_pk != pk {
                                deletes.insert(old_pk);
                            }
                        }
                        affected.insert(pk);
                    }
                }

                // Related-table events → reverse-resolve to affected primaries.
                for (rel_idx, related) in pd.def.related.iter().enumerate() {
                    if relation_of(&related.table) != relation.as_str() {
                        continue;
                    }
                    let from = related.join_on.from.columns();
                    let to = related.join_on.to.columns();
                    // An UPDATE that moves a child to a different parent has
                    // to recompose BOTH parents: the new one gains the row,
                    // the old one must lose it. Computed OUTSIDE the match on
                    // the post-image, because `SET fk = NULL` yields `None`
                    // there — the old parent still has to be rebuilt, and
                    // nesting this inside the `Some` arm silently skipped
                    // exactly that case.
                    let old_vals = (op == "update")
                        .then(|| pre_image.as_ref().and_then(|old| extract_cols(old, to)))
                        .flatten();
                    match extract_cols(row, to) {
                        Some(to_vals) => {
                            let old_vals = old_vals.filter(|old| *old != to_vals);
                            if from == pd.def.primary.pk.columns() {
                                // FK-on-related (1:many): the related row's `to`
                                // value *is* the primary PK — no extra query.
                                if let Some(old_vals) = old_vals {
                                    affected.insert(old_vals);
                                }
                                affected.insert(to_vals);
                            } else {
                                // FK-on-primary (many:1): defer to one coalesced
                                // lookup per related def after the event loop, so
                                // a burst of distinct parent changes collapses
                                // into one indexed `= ANY($1)` query instead of
                                // one round-trip per event.
                                let bucket = many1_buckets.entry(rel_idx).or_default();
                                if let Some(old_vals) = old_vals {
                                    bucket.push(old_vals);
                                }
                                bucket.push(to_vals);
                            }
                        }
                        None => {
                            // The post-image has no join key — either the FK
                            // was set to NULL (update) or this is a delete.
                            // A nulled FK still orphans the previous parent,
                            // so recompose it from the pre-image.
                            if let Some(old_vals) = old_vals {
                                if from == pd.def.primary.pk.columns() {
                                    affected.insert(old_vals);
                                } else {
                                    many1_buckets.entry(rel_idx).or_default().push(old_vals);
                                }
                            }
                            // Join key absent. For a 1:many child DELETE this is
                            // the `REPLICA IDENTITY DEFAULT` case (WAL pre-image
                            // carries only the child PK). Collect the child PK
                            // tuple (single- or multi-column) for a batched sink
                            // reverse-lookup below.
                            if op == "delete" {
                                if let Some((fields, child_pk_cols)) =
                                    reverse_lookup_fields(pd.def.primary.pk.columns(), related)
                                {
                                    if let Some(child_pk) = extract_cols(row, &child_pk_cols) {
                                        lookup_buckets.entry(fields).or_default().push(child_pk);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // One coalesced many:1 PG lookup per related def → affected parents.
            for (rel_idx, mut tuples) in many1_buckets {
                let Some(related) = pd.def.related.get(rel_idx) else {
                    continue;
                };
                let from = related.join_on.from.columns();
                tuples.sort();
                tuples.dedup();
                for pk in self.lookup_primary_by_batch(pd, from, &tuples).await? {
                    affected.insert(pk);
                }
            }

            // One batched reverse-lookup per embedded field set → affected parents.
            for (fields, mut tuples) in lookup_buckets {
                tuples.sort();
                tuples.dedup();
                for pk in self
                    .reverse_lookup_parents_batched(pd, &fields, &tuples)
                    .await?
                {
                    affected.insert(pk);
                }
            }

            // Recompose every affected primary AND every deleted one. A deleted
            // key that no longer exists in Postgres produces no row (no upsert);
            // a key deleted-then-reinserted in the same batch still exists, so
            // it's upserted — Postgres truth, not within-batch arrival order,
            // decides. `recompose` returns exactly the keys it upserted.
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
                    "sql-denormalize batched tail recompose"
                );
                self.recompose(pd, &pks, batch_lsn, sender, shutdown)
                    .await?
            };

            // Tombstone only the deleted keys Postgres no longer has (i.e. not
            // re-created within this batch). Skipping the ones recompose just
            // upserted avoids a same-version delete/upsert race on the sink.
            let live: HashSet<&Vec<String>> = emitted.iter().collect();
            for pk in &deletes {
                if live.contains(pk) {
                    continue;
                }
                let event = build_doc_event(
                    &pd.primary_table,
                    pd.def.target_index(),
                    pk,
                    None,
                    true,
                    batch_lsn,
                )?;
                sender
                    .send(event, shutdown)
                    .await
                    .map_err(|err| anyhow::anyhow!("emitting projection tombstone: {err}"))?;
                ventstream_telemetry::bump_events_emitted(1);
            }
        }
        Ok(())
    }

    /// `SELECT pk FROM primary WHERE from_cols = $vals` — resolves which
    /// primaries embed a changed many:1 related row.
    async fn lookup_primary_by(
        &self,
        pd: &PreparedDef,
        from_cols: &[String],
        vals: &[String],
    ) -> Result<Vec<Vec<String>>> {
        let n_pk = pd.def.primary.pk.columns().len();
        let pk_sel = pd.pk_text_select.clone();
        // Compare in the column's native type (`$n::text::<type>`) so the FK
        // index is used. Falls back to a `::text` comparison only if the
        // type wasn't resolved (which would seq-scan — see from_types).
        let where_text = from_cols
            .iter()
            .enumerate()
            .map(|(i, c)| match pd.from_types.get(c) {
                Some(ty) => format!("p.{} = ${}::text::{}", quote_ident(c), i + 1, ty),
                None => format!("p.{}::text = ${}", quote_ident(c), i + 1),
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT {pk_sel} FROM {table} p WHERE {where_text}",
            table = quote_qualified(&pd.primary_table),
        );
        let bind: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vals
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = self.client.query(&sql, &bind).await?;
        Ok(rows
            .iter()
            .map(|row| (0..n_pk).map(|i| row.get::<_, String>(i)).collect())
            .collect())
    }

    /// Coalesced many:1 reverse lookup: resolve every primary referencing any
    /// of `tuples` in ONE query.
    ///
    /// Single-column `from` (the common case, e.g. `customer_id`) uses
    /// `WHERE from_col = ANY($1::text[]::<type>[])` so the FK index is still
    /// used. Composite `from` falls back to the per-tuple path (rare; keeps
    /// correctness without a complex row-IN construction).
    async fn lookup_primary_by_batch(
        &self,
        pd: &PreparedDef,
        from_cols: &[String],
        tuples: &[Vec<String>],
    ) -> Result<Vec<Vec<String>>> {
        if tuples.is_empty() {
            return Ok(Vec::new());
        }
        let n_pk = pd.def.primary.pk.columns().len();
        let pk_sel = pd.pk_text_select.clone();

        match from_cols {
            [col] => {
                let vals: Vec<String> = tuples.iter().filter_map(|t| t.first().cloned()).collect();
                let where_text = match pd.from_types.get(col) {
                    Some(ty) => format!("p.{} = ANY($1::text[]::{}[])", quote_ident(col), ty),
                    None => format!("p.{}::text = ANY($1::text[])", quote_ident(col)),
                };
                let sql = format!(
                    "SELECT {pk_sel} FROM {table} p WHERE {where_text}",
                    table = quote_qualified(&pd.primary_table),
                );
                let rows = self.client.query(&sql, &[&vals]).await?;
                Ok(rows
                    .iter()
                    .map(|row| (0..n_pk).map(|i| row.get::<_, String>(i)).collect())
                    .collect())
            }
            _ => {
                // Composite FK: per-tuple fallback.
                let mut out = Vec::new();
                for t in tuples {
                    out.extend(self.lookup_primary_by(pd, from_cols, t).await?);
                }
                Ok(out)
            }
        }
    }

    /// Resolve the parent primary key(s) for a whole batch of deleted 1:many
    /// children. `fields` is one embedded keyword field per child-PK column;
    /// `tuples` are the deleted children's PK component vectors (aligned with
    /// `fields`). Single-column uses the batched `terms` query; composite uses
    /// the per-column `bool/must` variant.
    ///
    /// Returns the affected primary keys (empty when the fallback is disabled,
    /// `tuples` is empty, the index can't be resolved, or the sink query fails
    /// — all of which degrade to "don't recompose," never to a hard error).
    async fn reverse_lookup_parents_batched(
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
            warn!(
                primary = %pd.primary_table,
                "reverse-lookup skipped: could not render sink index for child delete"
            );
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
                warn!(
                    primary = %pd.primary_table,
                    error = %err,
                    "sink reverse-lookup failed; child deletes may not propagate this batch"
                );
                return Ok(Vec::new());
            }
        };
        let pks: Vec<Vec<String>> = ids
            .iter()
            .filter_map(|id| primary_pk_from_doc_id(id, &pd.primary_table))
            .collect();
        debug!(
            primary = %pd.primary_table,
            children = tuples.len(),
            parents = pks.len(),
            "sql-denormalize: batched reverse-lookup resolved parents for 1:many child deletes"
        );
        Ok(pks)
    }

    /// Render the sink index name for this def's docs, so the reverse-lookup
    /// queries the same index the dispatcher writes. Uses a probe event
    /// carrying this primary's relation/namespace headers — enough for the
    /// literal and `${header:...}` templates that denormalize upserts use.
    fn resolve_sink_index(&self, pd: &PreparedDef, lookup: &ReverseLookup) -> Option<String> {
        let probe = build_doc_event(
            &pd.primary_table,
            pd.def.target_index(),
            &["__vs_probe__".to_owned()],
            None,
            false,
            None,
        )
        .ok()?;
        lookup.resolve_index_for(&probe)
    }

    /// Run: bootstrap, then drain the tail receiver until shutdown.
    /// Reload catalog types for any def whose primary table saw a column
    /// retype since the types were loaded (signalled via the source's
    /// type-change epoch). On reload failure the stale epoch is kept so the
    /// next batch retries; the old spellings keep flowing meanwhile.
    async fn refresh_stale_types(&mut self) {
        for pd in &mut self.defs {
            let epoch = type_change_epoch(&pd.primary_table);
            if epoch == pd.type_epoch {
                continue;
            }
            let pk_columns = pd.def.primary.pk.columns();
            let pk_type_map = match load_column_types(&self.client, &pd.primary_table, pk_columns)
                .await
            {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        table = %pd.primary_table,
                        error = %err,
                        "sql-denormalize: pk type reload after type change failed; retrying next batch"
                    );
                    continue;
                }
            };
            let from_cols: Vec<String> = {
                let mut seen = std::collections::BTreeSet::new();
                for r in &pd.def.related {
                    for c in r.join_on.from.columns() {
                        seen.insert(c.clone());
                    }
                }
                seen.into_iter().collect()
            };
            let from_types = match load_column_types(&self.client, &pd.primary_table, &from_cols)
                .await
            {
                Ok(map) => map,
                Err(err) => {
                    warn!(
                        table = %pd.primary_table,
                        error = %err,
                        "sql-denormalize: join-column type reload after type change failed; retrying next batch"
                    );
                    continue;
                }
            };
            pd.pk_types = pk_columns
                .iter()
                .map(|column| {
                    pk_type_map
                        .get(column)
                        .cloned()
                        .unwrap_or_else(|| "text".to_owned())
                })
                .collect();
            pd.from_types = from_types;
            pd.type_epoch = epoch;
            info!(
                table = %pd.primary_table,
                epoch,
                "sql-denormalize: reloaded column types after source type change"
            );
        }
    }

    /// Rebuild the query connection if the current one's connection task has
    /// ended. `tokio_postgres::Client` is unusable forever after that — every
    /// query fails instantly — so without this the tail retry loop would spin
    /// on a dead handle until the process is restarted (observed in production
    /// as a multi-hour stall that a 1s restart healed). `PreparedDef` holds
    /// only SQL text and catalog type maps, so nothing else needs rebuilding.
    /// Redial failures are logged and left for the next retry pass; each
    /// attempt dials fresh (re-resolving DNS), so a database that comes back
    /// on a new address is picked up too.
    async fn reconnect_if_dead(&mut self) {
        if !self.client.is_closed() {
            return;
        }
        match connect_client(&self.source, "sql-denormalize reconnect").await {
            Ok(client) => {
                self.client = client;
                info!("sql-denormalize: rebuilt postgres query connection after loss");
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "sql-denormalize: postgres reconnect failed; retrying with next backoff"
                );
            }
        }
    }

    pub async fn run(
        mut self,
        mut receiver: EventReceiver,
        sender: EventSender,
        shutdown: ShutdownToken,
        durability: SqlDenormalizeDurability,
    ) {
        info!(
            defs = self.defs.len(),
            "sql-denormalize: starting bootstrap"
        );
        if let Err(err) = self.bootstrap(&sender, &shutdown).await {
            warn!(error = %err, "sql-denormalize bootstrap failed");
            ventstream_telemetry::record_error("postgres SQL denormalizer bootstrap failed");
            shutdown.cancel();
            return;
        }
        let mut next_sequence = 1u64;
        match durability
            .commit(&sender, &shutdown, &mut next_sequence, 0)
            .await
        {
            Ok(true) => {}
            Ok(false) => return,
            Err(err) => {
                warn!(error = %err, "sql-denormalize bootstrap durability boundary failed");
                ventstream_telemetry::record_error(
                    "postgres SQL denormalizer bootstrap durability boundary failed",
                );
                shutdown.cancel();
                return;
            }
        }
        info!("sql-denormalize: bootstrap done, entering tail");
        let mut retry = TailRetry::default();
        // Held while a batch is failing; dropped on the first success so the
        // readiness gate sees the stall's true duration.
        let mut stall_guard: Option<ventstream_core::SinkFailureGuard> = None;
        'tail: loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                // Greedy drain: one event when idle, up to TAIL_DRAIN_LIMIT
                // under load. Batching collapses co-arriving child deletes into
                // a single reverse-lookup query per field and one recompose per
                // affected primary.
                batch = receiver.recv_batch(TAIL_DRAIN_LIMIT) => {
                    if batch.is_empty() {
                        break; // all senders dropped
                    }

                    self.refresh_stale_types().await;
                    let source_watermark = max_lsn_u64(&batch);
                    loop {
                        if shutdown.is_cancelled() {
                            break 'tail;
                        }

                        let result = async {
                            self.handle_tail_batch(&batch, &sender, &shutdown).await?;
                            durability
                                .commit(
                                    &sender,
                                    &shutdown,
                                    &mut next_sequence,
                                    source_watermark,
                                )
                                .await
                        }
                        .await;

                        match result {
                            Ok(true) => {
                                stall_guard = None;
                                if let Some(attempts) = retry.record_success() {
                                    ventstream_telemetry::clear_error();
                                    ventstream_telemetry::set_phase(
                                        ventstream_telemetry::LifecyclePhase::Tailing,
                                    );
                                    info!(
                                        attempts,
                                        batch = batch.len(),
                                        "postgres SQL denormalizer tail recompose recovered"
                                    );
                                }
                                break;
                            }
                            Ok(false) => break 'tail,
                            Err(err) => {
                                let (attempt, backoff) = retry.record_failure();
                                if stall_guard.is_none() {
                                    if let Some(health) = &self.pipeline_health {
                                        stall_guard =
                                            Some(health.begin_transient_failure(format!("{err:#}")));
                                    }
                                }
                                warn!(
                                    error = %err,
                                    attempt,
                                    batch = batch.len(),
                                    backoff_ms = backoff.as_millis() as u64,
                                    "sql-denormalize tail recompose failed; retaining batch for retry"
                                );
                                ventstream_telemetry::record_error(format!(
                                    "postgres SQL denormalizer tail recompose failed (attempt {attempt}): {err:#}"
                                ));
                                tokio::select! {
                                    biased;
                                    () = shutdown.cancelled() => break 'tail,
                                    () = tokio::time::sleep(backoff) => {}
                                }
                                self.reconnect_if_dead().await;
                            }
                        }
                    }
                }
            }
        }
        // Drop sender → dispatcher drains.
        drop(sender);
    }
}

fn durability_barrier(sequence: u64) -> Result<Event> {
    let source = SourceUri::new("internal://sql-denormalize-durability")
        .map_err(|err| anyhow::anyhow!("invalid durability barrier source: {err}"))?;
    let subject = Subject::new("ventstream.internal.sql_denormalize.barrier")
        .map_err(|err| anyhow::anyhow!("invalid durability barrier subject: {err}"))?;
    let mut headers = HashMap::new();
    headers.insert(JOIN_BARRIER_HEADER.to_owned(), "true".to_owned());
    headers.insert(JOIN_SEQUENCE_HEADER.to_owned(), sequence.to_string());
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
        .content_type(ContentType::Json)
        .headers(Headers::from_map(headers))
        .build())
}

/// Build one no-join projection for a publication table. The sink routing
/// policy still owns the destination index: fixed routing sends every
/// projection to one index, while by-output-relation uses the table name.
fn direct_projection(namespace: &str, name: &str, primary_key: Vec<String>) -> JoinDefinition {
    let table = format!("{namespace}.{name}");
    JoinDefinition {
        name: Some(table.clone()),
        primary: PrimaryRef {
            table,
            pk: PkSpec(primary_key),
        },
        related: Vec::new(),
        target: TargetConfig::default(),
        state: StateConfig::default(),
        backfill: BackfillConfig::default(),
    }
}

/// Discover primary-keyed tables from the configured publication. Tables
/// without a primary key cannot support deterministic upserts/deletes, so they
/// are skipped with an operator-visible warning.
pub(crate) async fn discover_direct_projections(
    client: &Client,
    publication: &str,
) -> Result<Vec<JoinDefinition>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename \
             FROM pg_publication_tables \
             WHERE pubname = $1 \
             ORDER BY schemaname, tablename",
            &[&publication],
        )
        .await
        .with_context(|| format!("listing tables in publication {publication}"))?;

    let mut projections = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.get(0);
        let name: String = row.get(1);
        let pk_rows = client
            .query(
                "SELECT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                   ON kcu.constraint_schema = tc.constraint_schema \
                  AND kcu.constraint_name = tc.constraint_name \
                  AND kcu.table_schema = tc.table_schema \
                  AND kcu.table_name = tc.table_name \
                 WHERE tc.constraint_type = 'PRIMARY KEY' \
                   AND tc.table_schema = $1 \
                   AND tc.table_name = $2 \
                 ORDER BY kcu.ordinal_position",
                &[&namespace, &name],
            )
            .await
            .with_context(|| format!("listing primary key for {namespace}.{name}"))?;
        let primary_key = pk_rows
            .into_iter()
            .map(|pk_row| pk_row.get(0))
            .collect::<Vec<String>>();
        if primary_key.is_empty() {
            warn!(
                table = %format!("{namespace}.{name}"),
                publication,
                "sql-denormalize: skipping publication table without a primary key"
            );
            continue;
        }
        projections.push(direct_projection(&namespace, &name, primary_key));
    }
    Ok(projections)
}

/// Highest `ventstream.cdc.lsn` across a batch, returned as the original
/// header string (LSNs are decimal `u64` strings, parsed numerically to
/// compare). `None` if no event in the batch carries an LSN (e.g. a pure
/// bootstrap flush). Stamped on every recomposed doc + tombstone in the batch.
fn max_lsn(events: &[Event]) -> Option<String> {
    let mut best: Option<(u64, String)> = None;
    for event in events {
        let Some(raw) = event.headers.get("ventstream.cdc.lsn") else {
            continue;
        };
        let Ok(v) = raw.parse::<u64>() else {
            continue;
        };
        let take = match &best {
            None => true,
            Some((b, _)) => v > *b,
        };
        if take {
            best = Some((v, raw.to_owned()));
        }
    }
    best.map(|(_, s)| s)
}

fn max_lsn_u64(events: &[Event]) -> u64 {
    max_lsn(events)
        .and_then(|lsn| lsn.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Select the row whose columns identify the affected primary, by op.
/// Insert payloads are the flat row; update payloads are a `{new, old}`
/// envelope (use the post-image `new`); delete payloads are `{old}` (use
/// the pre-image). Getting this wrong silently drops updates — the post-
/// image columns never get read — so it's guarded by a unit test.
fn effective_row(raw: &Value, op: &str) -> Value {
    match op {
        "update" => raw.get("new").cloned().unwrap_or(Value::Null),
        "delete" => raw.get("old").cloned().unwrap_or(Value::Null),
        _ => raw.clone(),
    }
}

/// Extract the given columns from a row JSON as text values (in order).
/// Returns None if any column is absent.
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

/// If `related` is a 1:many embed whose child PK is present in the composed
/// doc, return the embedded field per PK column to reverse-look-up on and the
/// child PK columns to read from the delete row. Single- and multi-column PKs
/// are both supported — the sink ANDs the columns per tuple. Returns `None`:
///
/// - **Not 1:many** (`join_on.from != primary pk`): a many:1 delete logs the
///   related PK that the join points at, so the normal path already resolves it.
/// - **Child PK not (fully) embedded**: a non-empty `select` that omits any PK
///   column means that keyword field doesn't exist in the doc.
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
    if !related.select.is_empty()
        && !pk_cols
            .iter()
            .all(|c| related.select.iter().any(|s| s == c))
    {
        return None;
    }
    // Base fields (no `.keyword`): the sink matches both base and `.keyword`, so
    // numeric child PKs (mapped as `long`) and string PKs both resolve.
    let fields = pk_cols
        .iter()
        .map(|c| format!("{}.{}", related.embed_as, c))
        .collect();
    Some((fields, pk_cols.to_vec()))
}

/// Decode a canonical denormalized doc id (`{primary_table}:["c1",...]`) back
/// into the primary key text components, so a sink reverse-lookup hit can be
/// fed straight into [`SqlDenormalizer::recompose`]. Returns `None` if the id
/// doesn't carry this primary's prefix or the suffix isn't a JSON array.
fn primary_pk_from_doc_id(doc_id: &str, primary_table: &str) -> Option<Vec<String>> {
    let prefix = format!("{primary_table}:");
    let rest = doc_id.strip_prefix(&prefix)?;
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

/// Build a composed-doc event: payload = doc, headers carry the output
/// relation + deterministic doc id (`{primary_table}:{pk}`). `delete=true`
/// emits a tombstone (null payload, delete op) for a removed primary.
fn build_doc_event(
    primary_table: &str,
    target_index: Option<&str>,
    pk_text: &[String],
    doc_json: Option<&str>,
    delete: bool,
    lsn: Option<&str>,
) -> Result<Event> {
    let ns = namespace_of(primary_table);
    let rel = relation_of(primary_table);
    // Shared canonical encoder — same `{primary_table}:["c1",...]` form the
    // in-memory join engine and the reconcile parser use, so all three agree
    // byte-for-byte and the two modes are interchangeable on one index. PK
    // values are already text here; the engine text-normalizes its typed
    // values to match (see `ventstream_core::doc_id`).
    let doc_id = ventstream_core::doc_id::doc_id(primary_table, pk_text);
    let op = if delete { "delete" } else { "upsert" };
    let subject = Subject::new(format!("postgres.{ns}.{rel}.{op}"))
        .map_err(|e| anyhow::anyhow!("invalid subject: {e}"))?;
    let source = SourceUri::new(format!("postgres-sqljoin://{primary_table}"))
        .map_err(|e| anyhow::anyhow!("invalid source uri: {e}"))?;
    let payload = match doc_json {
        Some(doc) if !delete => Payload::from_vec(doc.as_bytes().to_vec()),
        _ => Payload::from_vec(b"{}".to_vec()),
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
    // Carry the triggering WAL LSN so the dispatcher's watermark advances
    // the replication slot once this recomposed doc is durably written.
    // Bootstrap docs have no LSN (pre-slot data) and must not advance it.
    if let Some(lsn) = lsn {
        headers.insert("ventstream.cdc.lsn".to_owned(), lsn.to_owned());
    }
    Ok(Event::builder(source, subject)
        .payload(payload)
        .content_type(ContentType::Json)
        .headers(Headers::from_map(headers))
        .build())
}

fn build_target_clear_event(
    primary_table: &str,
    target_index: Option<&str>,
    lsn: Option<&str>,
) -> Result<Event> {
    let ns = namespace_of(primary_table);
    let rel = relation_of(primary_table);
    let subject = Subject::new(format!("postgres.{ns}.{rel}.truncate"))
        .map_err(|err| anyhow::anyhow!("invalid subject: {err}"))?;
    let source = SourceUri::new(format!("postgres-sqljoin://{primary_table}"))
        .map_err(|err| anyhow::anyhow!("invalid source uri: {err}"))?;
    let mut headers = HashMap::new();
    headers.insert("ventstream.cdc.relation".to_owned(), rel.to_owned());
    headers.insert("ventstream.cdc.namespace".to_owned(), ns.to_owned());
    headers.insert("ventstream.target.clear".to_owned(), "true".to_owned());
    if let Some(target_index) = target_index {
        headers.insert(
            "ventstream.target.index".to_owned(),
            target_index.to_owned(),
        );
    }
    if let Some(lsn) = lsn {
        headers.insert("ventstream.cdc.lsn".to_owned(), lsn.to_owned());
    }
    Ok(Event::builder(source, subject)
        .payload(Payload::from_vec(b"{}".to_vec()))
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

    /// A 4-table-ish join def built via the real Deserialize path.
    fn sample_def() -> JoinDefinition {
        serde_json::from_value(serde_json::json!({
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
        .expect("deserialize join def")
    }

    #[test]
    fn qualified_and_relation_helpers() {
        assert_eq!(quote_qualified("shop.orders"), "\"shop\".\"orders\"");
        assert_eq!(quote_qualified("noschema"), "\"noschema\"");
        assert_eq!(relation_of("shop.orders"), "orders");
        assert_eq!(namespace_of("shop.orders"), "shop");
    }

    #[test]
    fn direct_projection_materializes_the_complete_primary_row() {
        let def = direct_projection(
            "public",
            "orders",
            vec!["tenant_id".to_owned(), "id".to_owned()],
        );

        assert_eq!(def.effective_name(), "public.orders");
        assert_eq!(def.primary.table, "public.orders");
        assert_eq!(
            def.primary.pk.columns(),
            &["tenant_id".to_owned(), "id".to_owned()]
        );
        assert!(def.related.is_empty());
        assert_eq!(doc_expr(&def), "to_jsonb(p)");
        assert_eq!(def.target_index(), None);
    }

    #[test]
    fn doc_expr_builds_one_and_many_with_projection_and_sort() {
        let e = doc_expr(&sample_def());
        // cardinality: one → scalar subquery, LIMIT 1, projected columns
        assert!(e.contains("jsonb_build_object('customer'"), "{e}");
        assert!(e.contains("LIMIT 1"), "{e}");
        assert!(
            e.contains("jsonb_build_object('id', r0.\"id\", 'name', r0.\"name\")"),
            "{e}"
        );
        // join condition is r.to = p.from
        assert!(e.contains("r0.\"id\" = p.\"customer_id\""), "{e}");
        // cardinality: many → jsonb_agg + COALESCE + ORDER BY sort_by
        assert!(e.contains("jsonb_build_object('items'"), "{e}");
        assert!(
            e.contains("jsonb_agg(to_jsonb(r1) ORDER BY r1.\"id\")"),
            "{e}"
        );
        assert!(e.contains("COALESCE("), "{e}");
        assert!(e.contains("r1.\"order_id\" = p.\"id\""), "{e}");
        // whole-row embed (no select) uses to_jsonb
        assert!(related_projection("r1", &[]).contains("to_jsonb(r1)"));
    }

    #[test]
    fn effective_row_picks_post_image_for_update_pre_image_for_delete() {
        let ins = serde_json::json!({ "id": "1", "v": "a" });
        let upd =
            serde_json::json!({ "new": { "id": "1", "v": "b" }, "old": { "id": "1", "v": "a" } });
        let del = serde_json::json!({ "old": { "id": "1", "v": "a" } });
        // insert: flat row used as-is
        assert_eq!(effective_row(&ins, "insert")["v"], serde_json::json!("a"));
        // update: the NEW (post-image) row — regression guard for the bug
        // where the flat top-level had no columns and updates silently dropped
        assert_eq!(effective_row(&upd, "update")["v"], serde_json::json!("b"));
        // delete: the OLD (pre-image) row, so we can find the affected primary
        assert_eq!(effective_row(&del, "delete")["v"], serde_json::json!("a"));
    }

    #[test]
    fn extract_cols_handles_strings_numbers_missing_null() {
        let row = serde_json::json!({ "id": "5", "customer_id": 7, "status": "paid" });
        assert_eq!(extract_cols(&row, &["id".into()]), Some(vec!["5".into()]));
        // composite, mixed string + number (number stringifies)
        assert_eq!(
            extract_cols(&row, &["id".into(), "customer_id".into()]),
            Some(vec!["5".into(), "7".into()])
        );
        // absent column → None
        assert_eq!(extract_cols(&row, &["nope".into()]), None);
        // null column → None (can't key on it)
        assert_eq!(
            extract_cols(&serde_json::json!({ "id": null }), &["id".into()]),
            None
        );
    }

    #[test]
    fn reverse_lookup_field_for_one_to_many_whole_row_embed() {
        let def = sample_def();
        // `items` is the 1:many embed (from=id == primary pk, whole-row embed).
        let items = def
            .related
            .iter()
            .find(|r| r.embed_as == "items")
            .expect("items related");
        let got = reverse_lookup_fields(def.primary.pk.columns(), items);
        // Base field — the sink matches both it and `.keyword`, so numeric PKs
        // (mapped as `long`, no `.keyword`) resolve too.
        assert_eq!(
            got,
            Some((vec!["items.id".to_owned()], vec!["id".to_owned()]))
        );
    }

    #[test]
    fn reverse_lookup_field_none_for_many_to_one() {
        let def = sample_def();
        // `customer` is many:1 (from=customer_id != primary pk) — the normal
        // delete path resolves it, so no sink fallback.
        let customer = def
            .related
            .iter()
            .find(|r| r.embed_as == "customer")
            .expect("customer related");
        assert_eq!(
            reverse_lookup_fields(def.primary.pk.columns(), customer),
            None
        );
    }

    #[test]
    fn reverse_lookup_field_none_when_select_omits_child_pk() {
        // 1:many but the projection omits the child PK, so the embedded doc has
        // no keyword to match — fall back to replica-identity config instead.
        let related: RelatedDefinition = serde_json::from_value(serde_json::json!({
            "id": "items", "table": "shop.order_items", "pk": "id",
            "join_on": { "from": "id", "to": "order_id" },
            "embed_as": "items", "cardinality": "many", "select": ["sku", "qty"]
        }))
        .expect("related");
        assert_eq!(reverse_lookup_fields(&["id".to_owned()], &related), None);
    }

    #[test]
    fn reverse_lookup_fields_supports_composite_child_pk() {
        // Composite child PK → one embedded field per column (the sink ANDs them
        // per tuple). Previously unsupported (returned None).
        let related: RelatedDefinition = serde_json::from_value(serde_json::json!({
            "id": "lines", "table": "shop.order_lines", "pk": ["order_id", "line_no"],
            "join_on": { "from": "id", "to": "order_id" },
            "embed_as": "lines", "cardinality": "many"
        }))
        .expect("related");
        assert_eq!(
            reverse_lookup_fields(&["id".to_owned()], &related),
            Some((
                vec!["lines.order_id".to_owned(), "lines.line_no".to_owned()],
                vec!["order_id".to_owned(), "line_no".to_owned()]
            ))
        );
    }

    fn lsn_event(lsn: Option<&str>) -> Event {
        let mut h = std::collections::HashMap::new();
        if let Some(l) = lsn {
            h.insert("ventstream.cdc.lsn".to_owned(), l.to_owned());
        }
        let source = SourceUri::new("postgres-sqljoin://shop.orders").expect("uri");
        let subject = Subject::new("postgres.shop.orders.upsert").expect("subject");
        Event::builder(source, subject)
            .payload(Payload::from_vec(b"{}".to_vec()))
            .content_type(ContentType::Json)
            .headers(Headers::from_map(h))
            .build()
    }

    #[test]
    fn max_lsn_picks_numeric_max_and_skips_missing() {
        // Numeric (not lexicographic) max, original string preserved.
        let events = vec![
            lsn_event(Some("90")),
            lsn_event(Some("1000")), // lexically "1000" < "90"; numerically larger
            lsn_event(None),         // no header — ignored
            lsn_event(Some("250")),
        ];
        assert_eq!(max_lsn(&events).as_deref(), Some("1000"));
        // No LSN anywhere (bootstrap-only flush) → None.
        assert_eq!(max_lsn(&[lsn_event(None)]), None);
        assert_eq!(max_lsn(&[]), None);
    }

    #[tokio::test]
    async fn durability_boundary_holds_source_progress_until_sink_ack() {
        let transform_progress = Arc::new(AtomicU64::new(0));
        let source_progress = Arc::new(AtomicU64::new(0));
        let durability = SqlDenormalizeDurability::new(
            Arc::clone(&transform_progress),
            Arc::clone(&source_progress),
        );
        let bus = ventstream_core::EventBus::new(4);
        let (sender, mut receiver) = bus.split();
        let shutdown = ShutdownToken::new();
        let task_shutdown = shutdown.clone();

        let task = tokio::spawn(async move {
            let mut sequence = 1;
            durability
                .commit(&sender, &task_shutdown, &mut sequence, 4242)
                .await
                .expect("durability boundary")
        });

        let barrier = receiver.recv().await.expect("barrier event");
        assert_eq!(barrier.headers.get(JOIN_BARRIER_HEADER), Some("true"));
        assert_eq!(barrier.headers.get(JOIN_SEQUENCE_HEADER), Some("1"));
        assert_eq!(source_progress.load(Ordering::Acquire), 0);

        transform_progress.store(1, Ordering::Release);
        assert!(task.await.expect("durability task"));
        assert_eq!(source_progress.load(Ordering::Acquire), 4242);
    }

    #[test]
    fn tail_retry_backoff_grows_caps_and_resets() {
        let mut retry = TailRetry::default();
        assert_eq!(retry.record_failure(), (1, TAIL_RETRY_INITIAL_BACKOFF));
        let mut last = Duration::ZERO;
        for _ in 0..32 {
            last = retry.record_failure().1;
        }
        assert_eq!(last, TAIL_RETRY_MAX_BACKOFF);
        assert_eq!(retry.record_success(), Some(33));
        assert_eq!(retry, TailRetry::default());
    }

    #[test]
    fn primary_pk_from_doc_id_round_trips_single_and_composite() {
        // Single — built by the shared producer encoder.
        let id = ventstream_core::doc_id::doc_id("shop.orders", &["base-7".to_owned()]);
        assert_eq!(
            primary_pk_from_doc_id(&id, "shop.orders"),
            Some(vec!["base-7".to_owned()])
        );
        // Composite.
        let id2 =
            ventstream_core::doc_id::doc_id("t.show_buyer", &["a".to_owned(), "b".to_owned()]);
        assert_eq!(
            primary_pk_from_doc_id(&id2, "t.show_buyer"),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
    }

    #[test]
    fn primary_pk_from_doc_id_rejects_wrong_prefix_and_bad_suffix() {
        // Different primary table sharing one index → not ours.
        assert_eq!(
            primary_pk_from_doc_id("shop.customers:[\"c1\"]", "shop.orders"),
            None
        );
        // Suffix isn't a JSON array.
        assert_eq!(
            primary_pk_from_doc_id("shop.orders:garbage", "shop.orders"),
            None
        );
    }

    #[test]
    fn build_doc_event_doc_id_relation_lsn_and_tombstone() {
        let ev = build_doc_event(
            "shop.orders",
            Some("tenant-orders"),
            &["5".into()],
            Some(r#"{"id":"5"}"#),
            false,
            Some("4242"),
        )
        .expect("event");
        assert_eq!(
            ev.headers.get("ventstream.doc.id"),
            Some("shop.orders:[\"5\"]")
        );
        assert_eq!(ev.headers.get("ventstream.cdc.relation"), Some("orders"));
        assert_eq!(ev.headers.get("ventstream.cdc.namespace"), Some("shop"));
        assert_eq!(ev.headers.get("ventstream.cdc.lsn"), Some("4242"));
        assert_eq!(
            ev.headers.get("ventstream.target.index"),
            Some("tenant-orders")
        );
        assert_eq!(
            index_template::render("${header:ventstream.target.index}", &ev, chrono::Utc::now())
                .expect("projection target routing should render"),
            "tenant-orders"
        );
        assert!(ev.subject.as_str().ends_with(".upsert"));

        // composite key → pipe-joined doc id
        let ev2 = build_doc_event(
            "t.show_buyer",
            None,
            &["a".into(), "b".into()],
            Some("{}"),
            false,
            None,
        )
        .expect("event");
        assert_eq!(
            ev2.headers.get("ventstream.doc.id"),
            Some("t.show_buyer:[\"a\",\"b\"]")
        );
        assert_eq!(ev2.headers.get("ventstream.cdc.lsn"), None);

        // delete → tombstone op header + delete subject
        let ev3 = build_doc_event(
            "shop.orders",
            Some("tenant-orders"),
            &["5".into()],
            None,
            true,
            None,
        )
        .expect("event");
        assert_eq!(ev3.headers.get("ventstream.cdc.op"), Some("delete"));
        assert_eq!(
            ev3.headers.get("ventstream.target.index"),
            Some("tenant-orders")
        );
        assert!(ev3.subject.as_str().ends_with(".delete"));
    }

    #[test]
    fn target_clear_event_is_scoped_and_checkpointed() {
        let event = build_target_clear_event("shop.orders", Some("tenant-orders"), Some("4242"))
            .expect("target clear");
        assert!(event.subject.as_str().ends_with(".truncate"));
        assert_eq!(event.headers.get("ventstream.target.clear"), Some("true"));
        assert_eq!(
            event.headers.get("ventstream.target.index"),
            Some("tenant-orders")
        );
        assert_eq!(event.headers.get("ventstream.cdc.relation"), Some("orders"));
        assert_eq!(event.headers.get("ventstream.cdc.lsn"), Some("4242"));
        assert_eq!(event.headers.get("ventstream.doc.id"), None);
    }
}
