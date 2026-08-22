//! The [`ventstream_core::Source`] implementation for MongoDB CDC.
//!
//! ### Architecture
//!
//! - Transport: the official `mongodb` async driver over one `Client`
//!   (reused for the snapshot scan and the live change stream).
//! - Cursor: the opaque **resume token** carried on every change event,
//!   persisted via [`super::cursor::CursorFile`] (Mongo has no server-side
//!   ack for change streams — same model as the Neo4j source).
//! - Bootstrap: runs once per state dir, gated on the cursor file's
//!   presence. We open the change stream first (capturing its resume
//!   token), scan the collections, emit a `snapshot-complete` sentinel,
//!   then persist the captured token — so the tail resumes exactly at the
//!   snapshot's consistency point. Rows mutated during the scan are
//!   re-emitted by the tail and de-duplicated by the deterministic
//!   `ventstream.doc.id` at the (idempotent) sink.
//! - Tail: a change-stream loop. Backpressure flows naturally — when the
//!   bus is full, `ctx.sender.send` awaits.
//!
//! ### Crash-safety
//!
//! The resume token is persisted **after** each event is published to the
//! bus. With a sink-progress watermark wired (the default in the engine),
//! tokens are persisted only after the sink durably writes the events
//! published before them, so a crash between token flush and sink write
//! cannot skip events; without one (legacy), the newest token flushes on
//! the timer and the sink's idempotent `doc.id` upserts absorb re-emits. A bootstrap is bracketed by a `mark_incomplete` sentinel: a
//! crash mid-snapshot re-bootstraps rather than resuming from a token whose
//! snapshot never reached the sink. An expired/invalid token (oplog rotated
//! past it) is detected, the cursor wiped, and the source re-bootstraps —
//! avoiding a crash loop.
//!
//! ### Shutdown
//!
//! `tokio::select!` against `ctx.shutdown.cancelled()` at the tail's await
//! point, plus an `is_cancelled` check in the bootstrap scan loop.

use async_trait::async_trait;
use futures_util::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::{ClientOptions, FullDocumentType, Tls, TlsOptions};
use mongodb::{Client, Database};
use serde_json::Value;
use tracing::{error, info, warn};
use ventstream_core::{Source, SourceContext, SourceError};

use super::bson::{bson_to_json, document_to_json};
use super::config::{FullDocument, MongoCdcConfig};
use super::cursor::CursorFile;
use super::event_mapper::{self, Op};
use crate::credential::immediate_message;
use crate::error::MongoCdcError;
use crate::tls::DatabaseTlsMode;

/// MongoDB CDC source.
pub struct MongoCdcSource {
    config: MongoCdcConfig,
    /// Sink-confirmed ack-sequence watermark. When `Some`, tail resume
    /// tokens are only persisted once the sink has durably written the
    /// events published before them — a crash between token flush and
    /// sink write can then never skip events on restart. When `None`,
    /// legacy behavior: flush the newest token on the timer.
    sink_progress: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl MongoCdcSource {
    /// Construct a source from its configuration.
    pub fn new(config: MongoCdcConfig) -> Self {
        Self {
            config,
            sink_progress: None,
        }
    }

    /// Attach the shared sink-progress watermark (see the field doc).
    #[must_use]
    pub fn with_sink_progress(
        mut self,
        progress: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        self.sink_progress = Some(progress);
        self
    }

    async fn connect(&self) -> Result<Client, MongoCdcError> {
        let options = self.client_options().await?;
        Client::with_options(options)
            .map_err(|e| MongoCdcError::Connection(format!("connecting to mongodb: {e}")))
    }

    async fn client_options(&self) -> Result<ClientOptions, MongoCdcError> {
        let mut options = ClientOptions::parse(&self.config.uri)
            .await
            .map_err(|e| MongoCdcError::Connection(format!("parse mongodb URI: {e}")))?;
        if let Some(tls) = &self.config.tls {
            options.tls = Some(match tls.mode {
                DatabaseTlsMode::VerifyFull => {
                    let options = match &tls.ca_file {
                        Some(path) => TlsOptions::builder()
                            .allow_invalid_certificates(false)
                            .ca_file_path(path.clone())
                            .build(),
                        None => TlsOptions::builder()
                            .allow_invalid_certificates(false)
                            .build(),
                    };
                    Tls::Enabled(options)
                }
                DatabaseTlsMode::Disabled => Tls::Disabled,
            });
        }
        Ok(options)
    }

    async fn run_inner(&self, ctx: SourceContext) -> Result<(), MongoCdcError> {
        let client = self.connect().await?;
        let db = client.database(&self.config.database);
        let cursor_file = CursorFile::new(&self.config.state_dir)?;

        // Resolve the resume point. A prior bootstrap left unconfirmed
        // (sentinel present) means the snapshot may never have reached the
        // sink — discard any token and re-bootstrap.
        let persisted = if cursor_file.is_incomplete() {
            warn!(
                source = %self.config.id,
                "prior bootstrap was not sink-confirmed; discarding token and re-bootstrapping"
            );
            cursor_file.delete()?;
            None
        } else {
            cursor_file.read()?
        };
        let resume_token = match &persisted {
            Some(s) => Some(token_from_string(s)?),
            None => None,
        };

        // Open the change stream — from the resume point, or "now" on a cold
        // start. Done before the scan so nothing is missed in the gap.
        if matches!(self.config.full_document, FullDocument::Default) {
            // Raw 1:1 mode writes whole documents; delta-only change
            // events have nothing to write, so every update would be
            // silently skipped ("no fullDocument" warn per event).
            // Refuse up front rather than run a pipeline that drops
            // every update by construction.
            return Err(MongoCdcError::Operation(
                "VS_MONGO_FULL_DOCUMENT=default carries deltas only, and raw document \
                 sync has nothing to write on updates — every update would be dropped. \
                 Use updateLookup (the default) instead"
                    .to_owned(),
            ));
        }
        // `full_document` is only set when we want post-images (update-lookup);
        // the builder takes the value directly, so we chain conditionally.
        let mut watch = db.watch().resume_after(resume_token.clone());
        if matches!(self.config.full_document, FullDocument::UpdateLookup) {
            watch = watch.full_document(FullDocumentType::UpdateLookup);
        }
        let mut stream = match watch.await {
            Ok(s) => s,
            Err(e) if persisted.is_some() && looks_like_resume_failure(&e) => {
                warn!(source = %self.config.id, error = %e,
                    "resume token rejected by mongodb; wiping cursor to re-bootstrap");
                cursor_file.delete()?;
                return Err(MongoCdcError::Operation(format!(
                    "resume token rejected (will re-bootstrap on restart): {e}"
                )));
            }
            Err(e) if is_credential_error(&e) => {
                let message = immediate_message(&e);
                error!(source = %self.config.id, error = %e, "{message}");
                return Err(MongoCdcError::Connection(message));
            }
            Err(e) => {
                return Err(MongoCdcError::Connection(format!(
                    "opening change stream: {e}"
                )))
            }
        };

        // Cold start + bootstrap enabled → snapshot, bracketed by the
        // incomplete sentinel.
        //
        // NOTE: unlike the PG/Neo4j sources we do NOT emit a
        // `snapshot-complete` sentinel. That event exists only to tell the
        // join engine to flush its in-memory state; Phase 1 (raw 1:1) has
        // no join engine to consume it, so it would flow straight to the
        // sink and — under a bare `${header:...relation}` index template —
        // render a reserved `_sentinel` index name that poisons the bulk
        // batch. Re-introduce it (gated on join mode) when joins land.
        if persisted.is_none() && self.config.bootstrap {
            let start_token = stream.resume_token();
            cursor_file.mark_incomplete()?;
            info!(source = %self.config.id, database = %self.config.database, "bootstrap: scanning collections");
            if !self.bootstrap(&db, &ctx).await? {
                // Cancelled mid-scan — leave the sentinel so we re-bootstrap.
                return Ok(());
            }
            // Barrier-gate the snapshot before trusting it: persist the
            // stream-start token and clear the sentinel only once the
            // sink confirms a barrier published AFTER every bootstrap
            // event. Without this, a crash between publish and sink
            // flush would resume past documents that never reached the
            // sink — unrecoverably, since the token predates them.
            if let Some(progress) = &self.sink_progress {
                if !self
                    .publish_ack_barrier(&ctx, BOOTSTRAP_BARRIER_SEQ)
                    .await?
                {
                    return Ok(()); // shutdown; sentinel stays
                }
                if !wait_for_sink_progress(progress, BOOTSTRAP_BARRIER_SEQ, &ctx).await {
                    return Ok(()); // shutdown; sentinel stays
                }
            }
            if let Some(tok) = &start_token {
                cursor_file.write(&token_to_string(tok)?)?;
            }
            cursor_file.clear_incomplete()?;
            info!(source = %self.config.id, "bootstrap complete");
        } else {
            info!(source = %self.config.id, "resuming from persisted token (no bootstrap)");
        }

        self.tail(&mut stream, &ctx, &cursor_file).await
    }

    /// Scan every in-scope collection, emitting one insert event per
    /// document. Returns `false` if shutdown fired mid-scan.
    async fn bootstrap(&self, db: &Database, ctx: &SourceContext) -> Result<bool, MongoCdcError> {
        // First authenticated server operation of the bootstrap: a
        // credential rejection here is terminal, not a scan hiccup.
        let names = db.list_collection_names().await.map_err(|e| {
            if is_credential_error(&e) {
                let message = immediate_message(&e);
                error!(source = %self.config.id, error = %e, "{message}");
                MongoCdcError::Connection(message)
            } else {
                MongoCdcError::Operation(format!("listing collections: {e}"))
            }
        })?;
        for name in names {
            // Skip internal collections and anything out of scope.
            if name.starts_with("system.") || !self.config.collection_allowed(&name) {
                continue;
            }
            let coll = db.collection::<Document>(&name);
            let chunk = u32::try_from(self.config.bootstrap_chunk_size).unwrap_or(1000);
            let mut scan = coll
                .find(doc! {})
                .batch_size(chunk)
                .await
                .map_err(|e| MongoCdcError::Operation(format!("scanning {name}: {e}")))?;
            while let Some(next) = scan.next().await {
                if ctx.shutdown.is_cancelled() {
                    return Ok(false);
                }
                let document =
                    next.map_err(|e| MongoCdcError::Operation(format!("scan cursor {name}: {e}")))?;
                let Some(id) = document.get("_id").map(bson_to_json) else {
                    warn!(collection = %name, "bootstrap doc missing _id; skipping");
                    continue;
                };
                let event = event_mapper::snapshot_insert(
                    &self.config,
                    &name,
                    &id,
                    document_to_json(&document),
                )?;
                if !self.publish(ctx, event).await? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Consume the change stream until shutdown, the stream ends, or a fatal
    /// error. The resume token is held in memory and flushed to disk on a
    /// timer (`token_flush_interval`) — plus on shutdown and stream end —
    /// rather than per-event, so a slow durable filesystem can't cap tail
    /// throughput on the fsync. At-least-once + idempotent doc-ids make the
    /// batching safe (a crash re-tails at most one flush window).
    async fn tail(
        &self,
        stream: &mut mongodb::change_stream::ChangeStream<ChangeStreamEvent<Document>>,
        ctx: &SourceContext,
        cursor_file: &CursorFile,
    ) -> Result<(), MongoCdcError> {
        info!(
            source = %self.config.id,
            token_flush_ms = self.config.token_flush_interval.as_millis() as u64,
            "tailing change stream"
        );
        // Ungated: newest token, flushed on the timer. Gated: FIFO of
        // (ack_seq, token) — a token is only eligible for persistence
        // once `sink_progress` proves every event published before it
        // is sink-durable.
        let mut pending: Option<String> = None;
        let mut pending_gated: std::collections::VecDeque<(u64, String)> =
            std::collections::VecDeque::new();
        let mut next_ack_seq: u64 = BOOTSTRAP_BARRIER_SEQ + 1;
        // Highest ack_seq actually published this session; a skipped
        // event's token becomes durable once the sink confirms this —
        // 0 (nothing published yet) is immediately durable on every
        // path, including resume-without-bootstrap where the watermark
        // starts at 0.
        let mut last_published_seq: u64 = 0;
        let mut flush = tokio::time::interval(self.config.token_flush_interval);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    self.flush_confirmed(cursor_file, &mut pending, &mut pending_gated)?;
                    return Ok(());
                }
                _ = flush.tick() => {
                    self.flush_confirmed(cursor_file, &mut pending, &mut pending_gated)?;
                }
                next = stream.next() => {
                    match next {
                        None => {
                            warn!(source = %self.config.id, "change stream ended; stopping iteration");
                            self.flush_confirmed(cursor_file, &mut pending, &mut pending_gated)?;
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            if is_credential_error(&e) {
                                let message = immediate_message(&e);
                                error!(source = %self.config.id, error = %e, "{message}");
                                return Err(MongoCdcError::Connection(message));
                            }
                            if looks_like_resume_failure(&e) {
                                metrics::counter!("vs_mongo_rebootstraps_total").increment(1);
                                warn!(source = %self.config.id, error = %e,
                                    "change stream resume failure; wiping cursor to re-bootstrap. \
                                     If this repeats, the oplog rotates past the token before \
                                     bootstrap finishes — grow the oplog \
                                     (replSetResizeOplog) or reduce bootstrap time");
                                cursor_file.delete()?;
                            }
                            return Err(MongoCdcError::Operation(format!("change stream: {e}")));
                        }
                        Some(Ok(event)) => {
                            let token = event.id.clone();
                            let seq = next_ack_seq;
                            match self.handle_event(ctx, event, seq).await? {
                                Some(false) => {
                                    // shutdown during publish — flush what's confirmed.
                                    self.flush_confirmed(
                                        cursor_file,
                                        &mut pending,
                                        &mut pending_gated,
                                    )?;
                                    return Ok(());
                                }
                                Some(true) => {
                                    // Published: this token is durable only
                                    // once the sink confirms `seq`.
                                    next_ack_seq += 1;
                                    last_published_seq = seq;
                                    let text = token_to_string(&token)?;
                                    if self.sink_progress.is_some() {
                                        pending_gated.push_back((seq, text));
                                    } else {
                                        pending = Some(text);
                                    }
                                }
                                None => {
                                    // Filtered/skipped: nothing new needs
                                    // sink durability — the token is safe
                                    // once everything published before it
                                    // is confirmed.
                                    let text = token_to_string(&token)?;
                                    if self.sink_progress.is_some() {
                                        pending_gated.push_back((last_published_seq, text));
                                    } else {
                                        pending = Some(text);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Map one change event to a bus event and publish it. Returns
    /// `Ok(Some(true))` if published, `Ok(Some(false))` if shutdown fired,
    /// `Ok(None)` if the event was filtered/skipped (token still advances).
    async fn handle_event(
        &self,
        ctx: &SourceContext,
        event: ChangeStreamEvent<Document>,
        ack_seq: u64,
    ) -> Result<Option<bool>, MongoCdcError> {
        let Some(op) = map_op(&event.operation_type) else {
            // Lifecycle op (drop/rename/invalidate/...) — skip, advance token.
            return Ok(None);
        };
        let Some(collection) = event.ns.as_ref().and_then(|ns| ns.coll.clone()) else {
            return Ok(None);
        };
        if !self.config.collection_allowed(&collection) {
            return Ok(None);
        }
        let Some(id) = event
            .document_key
            .as_ref()
            .and_then(|k| k.get("_id"))
            .map(bson_to_json)
        else {
            warn!(collection = %collection, "change event missing documentKey._id; skipping");
            return Ok(None);
        };

        let full_doc: Option<Value> = if op.is_delete() {
            None
        } else {
            match event.full_document.as_ref() {
                Some(doc) => Some(document_to_json(doc)),
                None => {
                    warn!(collection = %collection,
                        "upsert change has no fullDocument (enable fullDocument=updateLookup); skipping");
                    return Ok(None);
                }
            }
        };

        let bus_event = event_mapper::change_event(&self.config, &collection, op, &id, full_doc)?;
        // The ack sequence rides the same header the dispatcher's
        // watermark reads for Kafka — sink progress becomes "highest
        // contiguous ack_seq durably written", which is what gates
        // resume-token persistence.
        let bus_event = ventstream_core::Event {
            headers: bus_event
                .headers
                .with_header("ventstream.cdc.ack_seq".to_owned(), ack_seq.to_string()),
            ..bus_event
        };
        Ok(Some(self.publish(ctx, bus_event).await?))
    }

    /// Persist the newest resume token that is safe to trust. Ungated:
    /// the newest token seen. Gated: the newest token whose ack
    /// sequence the sink has confirmed — tokens beyond the watermark
    /// stay queued, so a crash between flush and sink write can never
    /// resume past unpersisted events.
    fn flush_confirmed(
        &self,
        cursor_file: &CursorFile,
        pending: &mut Option<String>,
        pending_gated: &mut std::collections::VecDeque<(u64, String)>,
    ) -> Result<(), MongoCdcError> {
        let Some(progress) = &self.sink_progress else {
            return flush_token(cursor_file, pending);
        };
        let confirmed = progress.load(std::sync::atomic::Ordering::Relaxed);
        let mut latest: Option<String> = None;
        while pending_gated
            .front()
            .is_some_and(|(seq, _)| *seq <= confirmed)
        {
            if let Some((_, token)) = pending_gated.pop_front() {
                latest = Some(token);
            }
        }
        if let Some(token) = latest {
            cursor_file.write(&token)?;
        }
        Ok(())
    }

    /// Publish the bootstrap ack barrier: an event carrying only the
    /// barrier + ack-sequence headers. The dispatcher recognizes it,
    /// never forwards it to the sink, and advances the watermark to its
    /// sequence once every batch before it is durable.
    async fn publish_ack_barrier(
        &self,
        ctx: &SourceContext,
        ack_sequence: u64,
    ) -> Result<bool, MongoCdcError> {
        let source =
            ventstream_core::event::SourceUri::new(format!("mongodb-cdc://{}", self.config.id))
                .map_err(|e| MongoCdcError::Internal(format!("ack barrier source: {e}")))?;
        let subject = ventstream_core::event::Subject::new("ventstream.internal.ack_barrier")
            .map_err(|e| MongoCdcError::Internal(format!("ack barrier subject: {e}")))?;
        let mut headers = std::collections::HashMap::new();
        headers.insert(
            "ventstream.internal.ack_barrier".to_owned(),
            "true".to_owned(),
        );
        headers.insert(
            "ventstream.cdc.ack_seq".to_owned(),
            ack_sequence.to_string(),
        );
        let event = ventstream_core::Event::builder(source, subject)
            .payload(ventstream_core::event::Payload::from_vec(b"{}".to_vec()))
            .content_type(ventstream_core::event::ContentType::Json)
            .headers(ventstream_core::event::Headers::from_map(headers))
            .build();
        self.publish(ctx, event).await
    }

    /// Publish to the bus. Returns `false` if shutdown cancelled the send.
    async fn publish(
        &self,
        ctx: &SourceContext,
        event: ventstream_core::Event,
    ) -> Result<bool, MongoCdcError> {
        match ctx.sender.send(event, &ctx.shutdown).await {
            Ok(()) => Ok(true),
            Err(ventstream_core::BackpressureError::Cancelled) => Ok(false),
            Err(e) => Err(MongoCdcError::Internal(format!("bus publish failed: {e}"))),
        }
    }
}

#[async_trait]
impl Source for MongoCdcSource {
    fn id(&self) -> &str {
        &self.config.id
    }

    fn kind(&self) -> &'static str {
        "mongodb_cdc"
    }

    async fn run(&self, ctx: SourceContext) -> Result<(), SourceError> {
        self.run_inner(ctx)
            .await
            .map_err(|e| SourceError::Internal(e.to_string()))
    }
}

/// Map a driver `OperationType` to our op, or `None` for ops the raw source
/// doesn't act on (drop / rename / invalidate / ...).
fn map_op(op: &OperationType) -> Option<Op> {
    match op {
        OperationType::Insert => Some(Op::Insert),
        OperationType::Update | OperationType::Replace => Some(Op::Update),
        OperationType::Delete => Some(Op::Delete),
        _ => None,
    }
}

/// Write the pending resume token to disk (atomic) and clear it. No-op when
/// there's nothing pending.
/// Ack sequence reserved for the bootstrap barrier (tail sequences
/// start at `BOOTSTRAP_BARRIER_SEQ + 1`, so the watermark reaching
/// this value means exactly "the bootstrap is sink-durable").
const BOOTSTRAP_BARRIER_SEQ: u64 = 1;

/// Poll the shared watermark until it reaches `expected` or shutdown.
async fn wait_for_sink_progress(
    progress: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    expected: u64,
    ctx: &SourceContext,
) -> bool {
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if progress.load(std::sync::atomic::Ordering::Acquire) >= expected {
            return true;
        }
        tokio::select! {
            biased;
            () = ctx.shutdown.cancelled() => return false,
            _ = poll.tick() => {}
        }
    }
}

fn flush_token(
    cursor_file: &CursorFile,
    pending: &mut Option<String>,
) -> Result<(), MongoCdcError> {
    if let Some(tok) = pending.take() {
        cursor_file.write(&tok)?;
    }
    Ok(())
}

/// Serialize a resume token to the persisted string form.
fn token_to_string(token: &ResumeToken) -> Result<String, MongoCdcError> {
    serde_json::to_string(token)
        .map_err(|e| MongoCdcError::Internal(format!("serializing resume token: {e}")))
}

/// Parse a persisted resume-token string back into a `ResumeToken`.
fn token_from_string(s: &str) -> Result<ResumeToken, MongoCdcError> {
    serde_json::from_str(s)
        .map_err(|e| MongoCdcError::Internal(format!("parsing resume token: {e}")))
}

/// Heuristic: does this driver error indicate the resume token is no longer
/// usable (oplog rotated past it / change-stream history lost)? When true,
/// the source wipes the cursor and re-bootstraps instead of crash-looping.
fn looks_like_resume_failure(e: &mongodb::error::Error) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    msg.contains("resume")
        || msg.contains("changestreamhistorylost")
        || msg.contains("history lost")
        || msg.contains("oplog")
}

/// Mongo auth failures that cannot heal in-process: server error code 18
/// (AuthenticationFailed), surfaced by the driver either as a command
/// error carrying code 18 or as its authentication error kind.
fn is_credential_error(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Authentication { .. } => true,
        mongodb::error::ErrorKind::Command(command) => command.code == 18,
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::tls::{DatabaseTlsConfig, DatabaseTlsMode};
    use std::path::PathBuf;

    #[tokio::test]
    async fn strict_policy_overrides_unsafe_uri_tls_options() {
        let mut config = MongoCdcConfig::new(
            "mongo",
            "mongodb://localhost:27017/?tls=false",
            "app",
            "/tmp/mongo",
        );
        config.tls = Some(DatabaseTlsConfig {
            mode: DatabaseTlsMode::VerifyFull,
            ca_file: Some(PathBuf::from("/run/secrets/mongo-ca.pem")),
        });
        let source = MongoCdcSource::new(config);
        let options = source.client_options().await.expect("parse options");
        let tls = match options.tls.as_ref() {
            Some(Tls::Enabled(tls)) => Some(tls),
            _ => None,
        };
        assert!(tls.is_some(), "strict policy must enable TLS");
        if let Some(tls) = tls {
            assert_eq!(
                tls.ca_file_path.as_deref(),
                Some(std::path::Path::new("/run/secrets/mongo-ca.pem"))
            );
            assert_eq!(tls.allow_invalid_certificates, Some(false));
        }
    }

    #[tokio::test]
    async fn disabled_policy_overrides_tls_uri() {
        let mut config = MongoCdcConfig::new(
            "mongo",
            "mongodb://localhost:27017/?tls=true",
            "app",
            "/tmp/mongo",
        );
        config.tls = Some(DatabaseTlsConfig {
            mode: DatabaseTlsMode::Disabled,
            ca_file: None,
        });
        let source = MongoCdcSource::new(config);
        let options = source.client_options().await.expect("parse options");
        assert_eq!(options.tls, Some(Tls::Disabled));
    }

    fn live_config(ca_file: Option<PathBuf>) -> MongoCdcConfig {
        let uri = std::env::var("VENTSTREAM_TEST_MONGODB_URI")
            .expect("VENTSTREAM_TEST_MONGODB_URI is required");
        let database = std::env::var("VENTSTREAM_TEST_MONGODB_DATABASE")
            .unwrap_or_else(|_| "admin".to_owned());
        let mut config = MongoCdcConfig::new("mongo-tls-test", uri, database, "/tmp/mongo-tls");
        config.tls = Some(DatabaseTlsConfig {
            mode: DatabaseTlsMode::VerifyFull,
            ca_file,
        });
        config
    }

    #[tokio::test]
    #[ignore = "requires VENTSTREAM_TEST_MONGODB_URI"]
    async fn strict_tls_connects_to_a_trusted_mongodb_server() {
        let source = MongoCdcSource::new(live_config(None));
        let client = source.connect().await.expect("build MongoDB client");
        client
            .database(&source.config.database)
            .run_command(doc! { "ping": 1 })
            .await
            .expect("strict MongoDB TLS connection");
    }

    #[tokio::test]
    #[ignore = "requires VENTSTREAM_TEST_MONGODB_URI and VENTSTREAM_TEST_WRONG_CA_FILE"]
    async fn strict_tls_rejects_an_untrusted_mongodb_ca() {
        let wrong_ca = PathBuf::from(
            std::env::var("VENTSTREAM_TEST_WRONG_CA_FILE")
                .expect("VENTSTREAM_TEST_WRONG_CA_FILE is required"),
        );
        let source = MongoCdcSource::new(live_config(Some(wrong_ca)));
        let mut options = source
            .client_options()
            .await
            .expect("parse MongoDB options");
        options.server_selection_timeout = Some(std::time::Duration::from_secs(5));
        let client = Client::with_options(options).expect("build MongoDB client");
        let result = client
            .database(&source.config.database)
            .run_command(doc! { "ping": 1 })
            .await;
        assert!(result.is_err(), "an unrelated CA must be rejected");
    }

    #[test]
    fn command_code_18_is_credential_and_other_codes_are_not() {
        let auth_failed: mongodb::error::CommandError = serde_json::from_value(serde_json::json!({
            "code": 18,
            "codeName": "AuthenticationFailed",
            "errmsg": "Authentication failed.",
        }))
        .expect("command error");
        let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Command(auth_failed));
        assert!(is_credential_error(&error));

        let cursor_gone: mongodb::error::CommandError = serde_json::from_value(serde_json::json!({
            "code": 286,
            "codeName": "ChangeStreamHistoryLost",
            "errmsg": "Resume of change stream was not possible",
        }))
        .expect("command error");
        let error = mongodb::error::Error::from(mongodb::error::ErrorKind::Command(cursor_gone));
        assert!(!is_credential_error(&error));

        let io = mongodb::error::Error::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        assert!(!is_credential_error(&io));
    }
}
