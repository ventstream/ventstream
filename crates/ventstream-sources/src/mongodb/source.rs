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
//! bus (the legacy at-least-once mode; the sink upserts by `doc.id`, so a
//! crash between publish and sink-write only causes a harmless re-emit on
//! restart). A bootstrap is bracketed by a `mark_incomplete` sentinel: a
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
use mongodb::options::FullDocumentType;
use mongodb::{Client, Database};
use serde_json::Value;
use tracing::{info, warn};
use ventstream_core::{Source, SourceContext, SourceError};

use super::bson::{bson_to_json, document_to_json};
use super::config::{FullDocument, MongoCdcConfig};
use super::cursor::CursorFile;
use super::event_mapper::{self, Op};
use crate::error::MongoCdcError;

/// MongoDB CDC source.
pub struct MongoCdcSource {
    config: MongoCdcConfig,
}

impl MongoCdcSource {
    /// Construct a source from its configuration.
    pub fn new(config: MongoCdcConfig) -> Self {
        Self { config }
    }

    async fn connect(&self) -> Result<Client, MongoCdcError> {
        Client::with_uri_str(&self.config.uri)
            .await
            .map_err(|e| MongoCdcError::Connection(format!("connecting to mongodb: {e}")))
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
        let names = db
            .list_collection_names()
            .await
            .map_err(|e| MongoCdcError::Operation(format!("listing collections: {e}")))?;
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
        let mut pending: Option<String> = None;
        let mut flush = tokio::time::interval(self.config.token_flush_interval);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = ctx.shutdown.cancelled() => {
                    flush_token(cursor_file, &mut pending)?;
                    return Ok(());
                }
                _ = flush.tick() => {
                    flush_token(cursor_file, &mut pending)?;
                }
                next = stream.next() => {
                    match next {
                        None => {
                            warn!(source = %self.config.id, "change stream ended; stopping iteration");
                            flush_token(cursor_file, &mut pending)?;
                            return Ok(());
                        }
                        Some(Err(e)) => {
                            if looks_like_resume_failure(&e) {
                                warn!(source = %self.config.id, error = %e,
                                    "change stream resume failure; wiping cursor to re-bootstrap");
                                cursor_file.delete()?;
                            }
                            return Err(MongoCdcError::Operation(format!("change stream: {e}")));
                        }
                        Some(Ok(event)) => {
                            let token = event.id.clone();
                            if self.handle_event(ctx, event).await?.is_some_and(|sent| !sent) {
                                // shutdown during publish — flush what we have.
                                flush_token(cursor_file, &mut pending)?;
                                return Ok(());
                            }
                            // Advance the in-memory token; the timer flushes it.
                            pending = Some(token_to_string(&token)?);
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
        Ok(Some(self.publish(ctx, bus_event).await?))
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
