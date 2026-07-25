# General sustainability benchmark results - 2026-07-21

## Scope

The final adaptive-memory engine was exercised on Docker Desktop with seven
available CPUs and 8 GiB of VM memory. Every engine case used two vCPUs. CDC
cases had a 512 MiB hard memory limit and 1 KiB payloads. Realtime cases had a
1 GiB hard memory limit and 256-byte payloads because connection fan-out is
bounded independently from the CDC memory controller.

OpenSearch 2.17.1 used two vCPUs, a 1 GiB JVM heap, no replicas, and disabled
refresh during ingestion. A run passed only when all expected OpenSearch
documents or WebSocket deliveries were present. Realtime validation also
required zero gaps and zero duplicates.

## Direct CDC

| Source | Profile | Records | Throughput | CPU mean / peak | Cgroup peak | RSS HWM | Verified |
|---|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL SQL | throughput | 1,000,000 | 29,551/s | 40% / 121% | 195.0 MiB | 256.5 MiB | 1,000,000 |
| MySQL SQL | throughput | 500,000 | 6,435/s | 121% / 138% | 11.1 MiB | 28.2 MiB | 500,000 |
| MongoDB | balanced | 1,000,000 | 56,815/s | 48% / 69% | 109.8 MiB | 248.5 MiB | 1,000,000 |
| Kafka | throughput | 2,000,000 | 62,950/s | 42% / 56% | 256.6 MiB | 406.4 MiB | 2,000,000 |
| Neo4j | maximum | 100,000 | 3,237/s | 7% / 12% | 34.9 MiB | 59.8 MiB | 100,000 |

Kafka is the final regression result after adding the one-second stable
recovery interval. All other cases ran the same controller implementation
before that recovery-only tuning; pressure escalation and event admission are
unchanged.

## Complex CDC

| Workload | Source changes | Sink writes | Throughput | Engine CPU mean / peak | Cgroup peak | RSS HWM |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL, four tables x three joins | 1,000,000 | 1,000,000 | 28,533/s | 49% / 60% | 164.6 MiB | 218.5 MiB |
| PostgreSQL related-row fan-out | 10,000 | 1,000,000 | 26,463/s | 32% / 47% | 232.4 MiB | 349.6 MiB |
| MySQL, four tables x three joins | 200,000 | 200,000 | 6,136/s | 114% / 139% | 13.3 MiB | 26.8 MiB |
| MySQL related-row fan-out | 10,000 | 200,000 | 43,535/s | 40% / 67% | 205.8 MiB | 242.6 MiB |
| MongoDB, four collections | 1,000,000 | 1,000,000 | 57,251/s | 47% / 78% | 216.8 MiB | 381.5 MiB |
| MongoDB `update_lookup` | 1,000,000 | 1,000,000 | 34,956/s | 30% / 39% | 84.3 MiB | 381.5 MiB |
| Neo4j, four two-hop projections | 100,000 | 20,000 | 595 docs/s | 6% / 8% | 26.5 MiB | 39.2 MiB |
| Neo4j second-hop fan-out | 20,000 | 20,000 | 3,956/s | 13% / 23% | 63.9 MiB | 86.6 MiB |

All four relational indexes contained the expected three joined objects.
MongoDB routed all four collections and verified every replacement document.
Neo4j verified all four independent two-hop document shapes after both initial
materialization and second-hop updates.

## Realtime at 200 clients

Each case published 20,000 events and verified exactly 4,000,000 deliveries.

| Protocol | Provider | Deliveries/s | CPU mean / peak | Cgroup peak | RSS HWM | Gaps / duplicates |
|---|---|---:|---:|---:|---:|---:|
| Raw WebSocket | NATS Core | 392,613 | 84% / 193% | 628.4 MiB | 493.5 MiB | 0 / 0 |
| Raw WebSocket | NATS JetStream | 281,205 | 176% / 205% | 90.0 MiB | 99.6 MiB | 0 / 0 |
| GraphQL WebSocket | NATS JetStream | 199,698 | 185% / 211% | 95.0 MiB | 119.6 MiB | 0 / 0 |
| Raw WebSocket | Redis Streams | 366,477 | 170% / 211% | 265.9 MiB | 114.2 MiB | 0 / 0 |
| GraphQL WebSocket | Redis Streams | 238,315 | 187% / 211% | 326.6 MiB | 342.0 MiB | 0 / 0 |

## Adaptive-controller finding

The first Kafka pass completed correctly but produced 37 pressure entries in
35 seconds as short memory dips repeatedly relaxed controls. The controller
now escalates immediately but requires memory to remain below a recovery
threshold for `recovery_ms` (1,000 ms by default) before relaxing one level.

The identical two-million-event regression changed as follows:

| Metric | Before | Stable recovery | Change |
|---|---:|---:|---:|
| Throughput | 56,453/s | 62,950/s | +11.5% |
| Cgroup peak | 296.4 MiB | 256.6 MiB | -13.4% |
| RSS HWM | 429.1 MiB | 406.4 MiB | -5.3% |
| Pressure log entries | 37 | 8 | -78.4% |

## Verdict

The engine sustained the tested high-volume matrix without an OOM kill,
restart, sink-count mismatch, realtime gap, or duplicate. The runs cover 4.6
million direct CDC records, 4.44 million complex sink writes, and 20 million
realtime deliveries. CDC memory remained bounded under 512 MiB and released
substantially after queues drained.

This establishes bounded high-load behavior on the local container stack; it
is not a substitute for a 24-72 hour production soak. Long-duration leak
detection, checkpoint/restart recovery, managed-service network latency, data
skew, large documents, and OpenSearch throttling still require environment-
specific qualification before assigning production capacity limits.

Raw CSVs and one-second samples are under
`target/benchmarks/container-matrix/adaptive-general-*-20260721` and
`target/benchmarks/container-matrix/adaptive-complex-*-20260721`.
