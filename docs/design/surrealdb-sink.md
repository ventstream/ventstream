# SurrealDB sink — design notes

## Shape

Ordered statement runs over the HTTP RPC protocol (`POST /rpc`, method
`query`). SurrealDB commits synchronously, so a successful response is the
delivery confirmation — there is no task-polling layer (contrast:
Meilisearch). Runs execute sequentially at concurrency 1; on a retryable
failure at run K execution restarts from K, and replaying committed runs
is idempotent by record id.

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

## Version pin

SurrealDB 3.x only: 3.0 renamed `type::thing` to `type::record`, and the
integration suite runs against the official 3.x image.
