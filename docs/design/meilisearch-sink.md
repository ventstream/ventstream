# Meilisearch Sink — Design (preliminary, 2026-08-08)

Status: draft for review. Grounded in the sink contracts in `ventstream-core`
(sink.rs, error.rs), the OpenSearch sink (bisection/retry/error-classification
templates), and the Redis sink (capability probe, adaptive controller, key
encoding), plus the Meilisearch v1.x HTTP API.

## Why this sink is different from the two we have

Meilisearch indexing is **asynchronous**: every document write returns a
`taskUid` immediately and the actual indexing happens later. Task failure is
atomic ("no changes were made"), but per-document errors surface only on the
task object, without offsets. Two of our core contracts collide with that:

1. `Sink::write` returning `Ok(())` means "batch durable." A naive
   implementation that returns after the HTTP 202 would acknowledge data that
   Meilisearch can still reject.
2. The DLQ path requires exact, unique, in-range item offsets
   (dispatcher.rs:629-656 fails closed on anything else). Meilisearch cannot
   give us per-item offsets at all.

The design accepts both constraints instead of fighting them.

## Decisions

### D1. Delivery = enqueue + task-completion poll

`write()` sends the batch, then polls `GET /tasks/{uid}` (shared
`BackoffSchedule` from `opensearch/retry.rs`, jittered, cancel-safe via
`tokio::select!` on the shutdown token) until a terminal status:

- `succeeded` → `Ok(())`
- `failed` with a **transient** error code (see D5) → `SinkError::Connection`
  (engine fails closed, retries the batch; idempotent by D3)
- `failed` with a **document** error → bisection (D2)
- `canceled` → `SinkError::Connection` (someone canceled server-side; retry)
- poll deadline exceeded → `SinkError::Timeout`

Meilisearch also supports task webhooks; not worth an inbound HTTP listener
in the agent for v1. Revisit if poll traffic ever matters.

### D2. Poison isolation by bisection, not by offsets

Since Meilisearch reports "task failed: invalid document" without saying
which one, we reuse the OpenSearch sink's range-splitting machinery
(sink.rs:221-286 pattern): on a document-class task failure, split the batch
and re-send halves recursively. Ranges that succeed are never re-sent
(Meilisearch upserts are idempotent). A single-document range that still
fails is a confirmed poison document → collect its original-batch offset and
error → return `SinkError::Rejected { failed_items: Some([...]) }` with
sorted, deduped offsets (mirroring redis/sink.rs:2221-2244).

Cost: O(k·log n) task round-trips for k poison docs in a batch of n —
acceptable because poison documents are rare and batches are bounded. Until
bisection lands (v1.0 can ship without it), document-class failures map to
`SinkError::Blocked` — the house rule: unknown ⇒ block, never DLQ-poison.

### D3. Primary key encoding: base64url, original ID kept as a field

Meilisearch primary keys allow only `[A-Za-z0-9_-]`, max 511 bytes. Our
canonical doc IDs (`orders:["123"]`) are illegal. Percent-encoding (the Redis
`encode_key_segment` approach) is also illegal here (`%`).

- Primary key := `base64url_nopad(canonical_doc_id)` — injective, reversible,
  charset-legal. 511-byte cap accommodates ~383-byte canonical IDs; longer
  IDs (pathological composite keys) fall back to
  `h-<hex(sha256(canonical))>` (still injective in practice, flagged in
  telemetry).
- The document also carries `"_vs_id": "<canonical doc id>"` so the index
  stays human-debuggable and filterable, and `"_vs_version"` (source
  watermark from `ventstream.cdc.lsn`/`tx_id`/`source_version` headers) for
  audit/debug.
- Delete targeting encodes the same way — deletes require
  `ventstream.doc.id` and fail closed without it (mirror bulk.rs:186-193).

### D4. Ordering: no external versioning ⇒ serialize batches

Meilisearch has no `external_gte` equivalent and no compare-and-set, so
parallel in-flight batches could reorder writes to the same document.
`recommended_concurrency()` returns 1 (the Redis sink precedent,
sink.rs:2213-2219). Batch-level throughput is preserved by batching, not
parallelism; Meilisearch's own server-side task batching (consecutive
compatible tasks merge, order preserved) works in our favor here. Search
workloads do not need more.

### D5. Error classification (the conservative house split)

HTTP layer: 429/408/5xx and connect/timeout → `Connection`; 401/403 →
`Blocked` (auth); other 4xx → `Blocked`. Task layer, by Meilisearch error
code: `index_not_found` (if auto-create disabled), `invalid_document_*`,
`missing_document_id`, `max_fields_limit_exceeded` → document class (D2);
`no_space_left_on_device`, `task_queue_full` (and any `internal`) →
transient/capacity; anything unrecognized → `Blocked`. Delete of a
nonexistent document succeeds in Meilisearch → naturally idempotent, no
special-casing needed (unlike OpenSearch's 404-tolerance logic).

### D6. Startup capability probe (Redis-sink pattern)

`build_sink` is async precisely so this can run before the pipeline starts:

1. `GET /version` — reachability + minimum version gate.
2. Key validity: `GET /keys` self-inspection or probe-by-doing — must have
   documents.add/delete, tasks.get, indexes.create/get on the target scope.
3. Per target index: create-or-validate. Existing index with a **different
   primaryKey** → `Blocked` ("drain and rebootstrap" language, mirroring the
   Redis view-schema mismatch). Missing + auto-create enabled → create with
   our primary key field.
4. Optional managed settings: if the config declares `filterable_attributes`
   / `sortable_attributes` / etc., apply-and-verify them (settings updates
   are tasks too — poll to terminal).
5. Nonce roundtrip: upsert + delete a `__ventstream:capability:<nonce>`
   document, poll both tasks to `succeeded` — proves the whole write path
   including task processing, not just connectivity.

### D7. Backpressure: task-latency EWMA drives the adaptive controller

The enqueue→terminal latency of our own tasks is a direct, free signal of
server pressure (queue depth grows ⇒ latency grows). Reuse the
`RedisAdaptiveController` shape: EWMA of task completion latency; sustained
growth or `task_queue_full`-class failures count as capacity pressure
(halve batch bytes/docs toward floors); steady successes recover toward
ceilings. `max_request_bytes` honors Meilisearch's payload cap (default
100 MB, configurable server-side; our default ceiling 16 MiB, floor 64 KiB).

### D8. Index routing

Same routing options as the Redis keyspace: `by_table` (default —
`{prefix}{relation}` per table, e.g. `vs_orders`) or `fixed` (single index).
Target resolution reuses the `ventstream.cdc.relation` /
`ventstream.target.index` header logic (keyspace.rs:12-52). Index UIDs share
Meilisearch's `[A-Za-z0-9_-]` charset, so the prefix/table encoding reuses
D3's encoder. Truncate events (`.truncate` subject) → `DELETE
/indexes/{uid}/documents` (delete-all), task-polled like everything else.

## Config sketch

```yaml
sink:
  kind: meilisearch
  meilisearch:
    endpoint_ref: env:VS_MEILI_ENDPOINT
    api_key_ref: env:VS_MEILI_API_KEY        # a key, never the master key, docs say so
    index_routing: by_table                   # or fixed
    index_prefix: "vs_"
    fixed_index: null
    auto_create_indexes: true
    primary_key_field: "_vs_pk"
    batching:
      max_docs: 2000
      max_bytes: 16777216
    task:
      poll_initial_ms: 50
      poll_max_ms: 2000
      deadline_ms: 120000
    settings:                                 # optional managed settings, per index
      filterable_attributes: []
      sortable_attributes: []
    tls: {}                                   # shared TLS policy block
```

`SinkConfig` gains `meilisearch: Option<MeilisearchSinkConfig>` +
`SinkKind::Meilisearch` + the validate arm (ventstream-config lib.rs:887-930
pattern, deny_unknown_fields, exactly-one-sub-block rule).

## Module plan

```
crates/ventstream-sinks/src/meilisearch/
  mod.rs        # exports
  config.rs     # MeilisearchSinkConfig + validate
  sink.rs       # Sink impl, write loop, task polling, bisection
  documents.rs  # payload building, pk encoding, delete batching, headers
  error.rs      # HTTP + task error classification
  capability.rs # startup probe (D6)
```

Plus: `build_sink` arm + `SinkRuntimeConfig::Meilisearch` (main.rs:818-836,
2810-2893), `load_meilisearch_config` (+env) into `load_sink_config`
(main.rs:3525-3555), `vs_meili_*` telemetry block in ventstream-telemetry
(task latency histogram sample, enqueue counts, bisection count, task
failures by class), engine-config-contract.md update, and the fleet-side
managed schema mirror (remember the contract-test path/operation counts in
control-api when the fleet side lands).

Housekeeping while we're in there: promote `jittered_delay` (currently
duplicated in opensearch/sink.rs:555 and redis/sink.rs:2247) into a shared
module instead of making a third copy.

## Testing

- `wiremock` suite modeled on the OpenSearch sink tests (sink.rs:899-1808):
  task lifecycle happy path, task failure classes, bisection isolating 1 and
  2 poison docs, pk encoding edge cases (unicode, 511-byte boundary, hash
  fallback), delete-without-doc-id fails closed, capability probe failures
  (bad key scope, primaryKey mismatch), payload-cap bisection, cancel-safety
  mid-poll.
- Acceptance: real Meilisearch container in the demo compose; the money
  demo is `DELETE FROM products WHERE id=...` → document gone from search
  in <1s (delete propagation is the community's #1 documented pain).

## Live validation (2026-08-09)

Verified end-to-end against Meilisearch v1.52 and Postgres 16 logical
replication (single-table joins projection):

- Snapshot bootstrap materialized the table; primary keys arrived as
  base64url with `_vs_id` preserved for debugging.
- Live INSERT and UPDATE streamed through; typo-tolerant search returned
  the updated document.
- **A row DELETE propagated to the search index in 0.22s** — the
  documented community pain (polling can't see deletions) demonstrably
  solved.
- Startup probe correctly failed fast on a missing API key (HTTP 401
  surfaced as Blocked before the pipeline started).

Operational notes from the run:
- Postgres pipelines require a `specs.joins` projection (even
  single-table, `related: []`) because raw postgres events carry no
  `ventstream.doc.id`; Mongo/Neo4j/Kafka/MySQL stamp ids at the source.
  Without joins, every event DLQs with an exact per-item reason (the
  contract behaved as designed). Quickstart docs must include the
  single-table joins snippet; a source-side auto-projection is a
  possible future DX improvement.
- Memory-mode joins need `VS_JOINS_STATE_DIR` (durable state guard).

## Effort estimate

3–5 focused days for v1 (everything except D2 bisection), +1–2 days for
bisection and the fleet managed-config mirror. The OpenSearch and Redis
sinks provide direct templates for ~70% of the code.

## Open questions

1. Ship v1 without bisection (document failures → Blocked) and add it in
   v1.1, or hold for bisection? Leaning: ship without — blocked-on-poison is
   safe, observable, and rare.
2. Managed settings drift: if an operator changes filterableAttributes by
   hand, do we re-assert on reconnect (Redis view-schema precedent says
   verify + Blocked on mismatch) or leave user settings alone unless
   declared? Leaning: only manage what the config declares.
3. Typesense: same async-task-free API? (It's synchronous — a Typesense
   sink after this one is mostly documents.rs + error.rs swaps; keep the
   module boundaries clean for that.)
