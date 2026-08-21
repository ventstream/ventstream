# 50M-document bootstrap benchmark — 2026-08-20

Snapshot-bootstrap of 50,000,000 pre-seeded rows per source into OpenSearch,
engine at 2 vCPU / 1 GiB (`run-bootstrap.sh`, throughput profile, 64-byte
payload per row). Every run verified for the exact document count.

| source | seed_s | bootstrap_s | throughput (ev/s) | cpu mean/p95/peak % | rss peak MiB | verified |
|---|---|---|---|---|---|---|
| postgres | 88.6 | 390.4 | 128,059 | 36.2 / 44.3 / 57.9 | 253 | 50,000,000 |
| mysql | 117.3 | 537.8 | 92,973 | 25.1 / 30.0 / 32.1 | 109 | 50,000,000 |
| mongodb | 186.7 | 374.9 | 133,383 | 42.6 / 54.2 / 59.3 | 237 | 50,000,000 |
| kafka | 114.6 | 441.0 | 113,381 | 43.2 / 50.2 / 138.0 | 376 | 50,000,000 |
| neo4j | reused | 707.0¹ | 70,721¹ (~126,000 clean) | 33.4 / 65.0 / 73.6 | 347 | 50,000,000 |

¹ The neo4j wall clock includes ~5 minutes of engine retry while the Neo4j
server recovered from an OOM it suffered under the pre-fix quadratic scan
(see below); the healthy delivery window sustained ~126k events/s.

Host: Docker Desktop on Apple Silicon, 7 CPUs / 7.7 GiB shared by source DB,
OpenSearch (2 CPU / 2.3 GiB), and engine. CPU columns are percent of the
engine's 2-vCPU allocation. Payload 64 B/row (disk-bounded on this host);
the published tail benchmarks use 256 B.

## Findings fixed during this run

Neo4j bootstrap was quadratic at scale: both the plain node/relationship
scans (`bootstrap.rs`) and the denormalize key enumeration
(`denormalize.rs`) keyset-paginated over `elementId()`, which has no index
and no native ordering — every page re-ran a full label scan plus a top-K
sort. Flat on the ~500k-node graphs it was validated on; ~644 docs/s at
50M (a projected 21+ hours). Both paths now stream a single Bolt query
(lazy fetch batches, bounded memory): measured ~126k docs/s at 50M, a
~196x improvement. The old sort pressure was also what OOM-killed the 3 GiB
Neo4j server container mid-run; the streamed scan finished inside the same
limit without incident.

## SurrealDB sink — 2026-08-21

Same harness, same seeds, `VS_BENCH_SINK=surrealdb` (SurrealDB v3, RocksDB
backend, 2 vCPU / 2.3 GiB unless noted). Every run verified for the exact
record count.

| source | seed_s | bootstrap_s | throughput (docs/s) | cpu mean/p95/peak % | rss peak MiB | verified |
|---|---|---|---|---|---|---|
| postgres | 77.7 | 1832.8 | 27,281 | 10.0 / 16.2 / 27.6 | 104 | 50,000,000 |
| mysql | 110.6 | 1855.8 | 26,942 | 9.5 / 15.4 / 98.1 | 108 | 50,000,000 |
| mongodb | 179.0 | 2144.9 | 23,311 | 9.2 / 13.9 / 184.4 | 113 | 50,000,000 |
| kafka | 115.8 | 2302.9 | 21,712 | 9.3 / 12.9 / 37.7 | 200 | 50,000,000 |
| neo4j | 1757.1 | 2203.4 | 22,692 | 12.7 / 17.3 / 35.9 | 146 | 50,000,000 |
| postgres @ surreal 4 vCPU | 82.5 | 1140.1 | 43,854 | 13.9 / 19.8 / 36.8 | 107 | 50,000,000 |

The pace here belongs to SurrealDB's ingest, not the engine: the SurrealDB
server pinned its full CPU allocation for the whole window while the engine
idled near 10% of its own. Doubling the database to 4 vCPU lifted
throughput 61% with no engine change — the sink scales with the hardware
you give the database.

## Meilisearch sink — 2026-08-21

Same harness, `VS_BENCH_SINK=meilisearch` (Meilisearch v1.12, 2 vCPU /
2.3 GiB). Runs use the engine's bulk-batching knobs
(`VS_MEILI_MAX_BATCH_DOCS=250000`, `VS_MEILI_MAX_BATCH_BYTES=100MB`,
`VS_MEILI_TASK_DEADLINE_MS=1800000`) and an index with
`searchableAttributes` restricted to the real payload field — the
production-representative setup, since indexing synthetic unique ids as
search terms benchmarks the wrong thing.

| source | seed_s | bootstrap_s | throughput (docs/s) | cpu mean/p95/peak % | rss peak MiB | verified |
|---|---|---|---|---|---|---|
| postgres | 82.4 | 1159.6 | 43,118 | 28.7 / 90.9 / 114.1 | 997 | 50,000,000 |
| mysql | 117.3 | 1208.0 | 41,392 | 31.1 / 90.5 / 123.9 | 1033 | 50,000,000 |
| mongodb | 201.0 | 1223.7 | 40,858 | 26.5 / 82.3 / 136.4 | 937 | 50,000,000 |
| kafka | 117.5 | 1294.6 | 38,622 | 28.3 / 81.7 / 105.6 | 1034 | 50,000,000 |
| neo4j | reused | 1446.8 | 34,558 | 29.2 / 76.8 / 103.1 | 1031 | 50,000,000 |

With 250k-doc bulk payloads in flight the engine runs at its 1 GiB cgroup
ceiling; the memory controller held every run inside the limit with no
OOM. For 1 GiB pods that want more headroom, `VS_MEILI_MAX_BATCH_DOCS=100000`
trades a little throughput for a much lower RSS peak.

## Elasticsearch sink — 2026-08-21

Same harness, `VS_BENCH_SINK=elasticsearch` (Elasticsearch 8.15.2,
2 vCPU / 2.3 GiB, single node, 1 GiB heap). The engine's bulk writer is
shared with OpenSearch, so Elasticsearch is a peer sink with no separate
code path — these runs confirm the parity at scale.

| source | seed_s | bootstrap_s | throughput (docs/s) | cpu mean/p95/peak % | rss peak MiB | verified |
|---|---|---|---|---|---|---|
| postgres | 75.0 | 444.2 | 112,572 | 34.9 / 49.4 / 58.6 | 248 | 50,000,000 |
| mysql | 108.6 | 555.2 | 90,065 | 24.6 / 31.5 / 33.4 | 111 | 50,000,000 |
| mongodb | 191.1 | 400.7 | 124,773 | 39.6 / 55.7 / 72.6 | 236 | 50,000,000 |
| kafka | 117.1 | 463.9 | 107,778 | 41.2 / 48.9 / 56.8 | 375 | 50,000,000 |
| neo4j | 1772.5 | 540.4 | 92,516 | 51.4 / 66.5 / 85.8 | 342 | 50,000,000 |

Numbers track the OpenSearch campaign closely, as the shared writer
predicts: 90–125k docs/s across every source, engine RSS well under half
the 1 GiB cap.
