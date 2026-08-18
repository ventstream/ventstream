# SurrealDB sink — design notes

## Shape

Ordered statement runs over the HTTP RPC protocol (`POST /rpc`, method
`query`). SurrealDB commits synchronously, so a successful response is the
delivery confirmation — there is no task-polling layer (contrast:
Meilisearch). Within a batch, runs partition into per-table lanes:
strictly ordered inside each routed table (the only ordering that
matters — every run for a record lives in its table's lane), concurrent
across tables. Multi-table pipelines writing over the internet pay ~one
round-trip per flush instead of one per table. Per lane, a retryable
failure at run K restarts from K; replaying committed runs is idempotent
by record id. Dispatcher-level concurrency stays 1 (no per-document
external-version guard yet — the OpenSearch-style LSN versioning is the
designed path to cross-batch parallelism).

## Identity

The canonical doc id `table:["pk",…]` maps onto SurrealDB's native array
record ids via `type::record($tb, $id_array)` — composite keys round-trip
with no encoding. Ids that don't parse as a key array (e.g. Neo4j
elementIds containing `:`) fall back to the whole string as the id part.
The canonical id is also stamped on the document as `_vs_id` for reverse
lookups and debugging. A source column named `id` collides with
SurrealDB's record-id field (CONTENT with a mismatched id is rejected
server-side) and is preserved as `source_id`.

## Injection posture

Every data value rides RPC bind variables. The only identifier positions
SurrealQL cannot parameterize — DDL names and reverse-lookup field paths
— pass a `[A-Za-z0-9_.-]` charset gate and are ⟨⟩-escaped.

## Error classes

- Transport / 5xx / 408 / 429 → retry with backoff.
- Statement errors matching conflict/pressure text (`transaction
  conflict`, `failed to commit`, …) → transient: SurrealDB's optimistic
  concurrency makes concurrent-writer collisions expected; replays
  converge. This is the load-bearing transient case.
- 401/403, other 4xx, and unrecognized statement errors → blocked (never
  DLQ'd by guesswork; schemafull field rejections surface with the exact
  server reason).
- Client-side translation rejects (missing doc id, non-object payload) →
  per-item `Rejected { failed_items }` → DLQ with exact offsets.

## Reverse lookup

`SELECT VALUE _vs_id FROM type::table($tb) WHERE
array::map(array::flatten([<path>]), |$v| type::string($v)) CONTAINSANY
$values` — the flatten/map normalization makes one predicate serve scalar
join fields and 1:many embedded paths (`items.item_id`), comparing the
pgoutput/binlog canonical text forms. Tuple probes AND per-field
containment inside `array::any($tuples, …)`; multi-valued paths make that
an over-approximation, which is safe because recomposition is idempotent.
No index configuration is required (contrast: Meilisearch filterable
attributes).

## Vector indexes

Declared `vector_indexes` are ensured at startup with idempotent
`DEFINE INDEX IF NOT EXISTS … HNSW DIMENSION n DIST …` DDL. Embedding
arrays flow through documents as ordinary JSON; declaring the index makes
them KNN-searchable (`embedding <|K,EF|> $vec`).

## Auth

Every request carries a bearer token from `/signin`, tried at database →
namespace → root scope (narrowest wins). Basic auth is deliberately not
used: Surreal Cloud rejects it for non-root users, and tokens expire
(1h default) — one transparent re-signin retries a 401. The production
posture is a database-scoped user (`DEFINE USER … ON DATABASE … ROLES
OWNER`); root is never required.

## Startup

`auto_create_database` (default OFF) optionally ensures namespace +
database with `DEFINE … IF NOT EXISTS` for dev instances — SurrealDB 3.x
does not auto-create them, and creating them needs elevated credentials,
so production provisions once and runs scoped. A missing scope fails the
probe with the exact provisioning DDL in the message.

## Reverse-lookup scaling

Empirical planner truth (SurrealDB 3.2.4, verified by EXPLAIN): neither
`CONTAINS`/`CONTAINSANY` nor value-IN-array is ever index-served, and
`=` on an indexed array field is whole-array equality — so the designed
"indexed join field" fix is impossible as designed. What ships instead:
declared `lookup_fields` materialize string-canonical join values onto
each document (`_vs_lx_*`), turning the per-row flatten/map closure into
a flat pre-decode filter — measured 5.3x cheaper (153ms → 29ms at 30k
docs). Still O(N), but the cliff moves 5x out, and the field becomes
index-ready the day the planner learns CONTAINSANY. The full O(log N)
escalation, if ever needed, is a writer-maintained inverted side-table
addressed by record-id range scans.

## Batch execution

Within a lane, runs that are all upserts with pairwise-disjoint id sets
(every bootstrap batch; most tail batches) execute concurrently — order
between them is vacuously irrelevant, for any source, with no snapshot
detection. Overlapping ids, deletes, or truncates fall back to strict
sequence. Deletes are set-oriented (`DELETE array::map($rids, …)`), one
statement instead of an interpreted per-id loop. Request-body gzip was
tested and is NOT accepted by SurrealDB's HTTP API (400) — closed.

## Version pin

SurrealDB 3.x only: 3.0 renamed `type::thing` to `type::record`, and the
integration suite runs against the official 3.x image.
