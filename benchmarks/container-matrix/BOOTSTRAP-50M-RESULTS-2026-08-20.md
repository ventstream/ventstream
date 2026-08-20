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
