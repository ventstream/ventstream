# Container performance matrix

This directory contains the fixtures used to measure VentStream's CDC and
realtime hot paths inside Linux containers. The benchmark intentionally runs
one source at a time because a local OpenSearch instance and multiple database
JVMs otherwise compete for Docker Desktop's memory and invalidate comparisons.

The engine container is limited to 2 vCPUs and 1 GiB. Each accepted result
records:

- elapsed time and verified end-to-end throughput;
- mean, p95, and peak engine CPU;
- peak engine cgroup working-set memory;
- process `VmRSS` and `VmHWM` from the engine PID namespace;
- final OpenSearch document count or realtime delivery count.

Source SQL databases use bounded-memory SQL denormalization. OpenSearch is
configured with no replicas and refresh disabled during ingestion; the harness
uses engine counters to detect completion and performs one final refresh for
the correctness check.

Results are machine-specific. Always record Docker's CPU and memory allocation
alongside the generated CSV before comparing runs from different hosts.

The latest all-source adaptive-controller run is documented in
`GENERAL-SUSTAINABILITY-RESULTS-2026-07-21.md`.

The release allocator uses 500 ms dirty-page and 1 second muzzy-page decay.
The source runner applies those settings by default and accepts
`VS_BENCH_ALLOCATOR_CONF` for controlled comparisons. Use
`VS_BENCH_MEMORY_CONTROLLER_ENABLED=false` only for an explicit baseline; the
production default is enabled whenever a finite cgroup limit is visible.

## Build and run

Build the release image from the current working tree:

```sh
docker build -f infra/docker/engine.Dockerfile -t ventstream-engine:bench .
```

Run all CDC sources (PostgreSQL and MySQL use SQL mode):

```sh
VS_BENCH_RUN_ID=sources benchmarks/container-matrix/run-sources.sh all
```

Run all realtime providers and protocols:

```sh
VS_BENCH_RUN_ID=realtime benchmarks/container-matrix/run-realtime.sh all
```

Run the complex projection matrix:

```sh
VS_BENCH_RUN_ID=complex \
VS_BENCH_RELATION_FANOUT=1 \
VS_BENCH_MONGODB_UPDATES=1 \
VS_BENCH_NEO4J_FANOUT=1 \
benchmarks/container-matrix/run-complex.sh all
```

The complex runner accepts `postgres`, `mysql`, `mongodb`, `neo4j`, or `all`.
PostgreSQL and MySQL each materialize four primary tables into four indexes
from one engine. Every output performs three indexed relationships. MongoDB
tails four collections and routes each to its own index. The Neo4j workload
runs four independent Cypher projections in one engine, each with exactly two
relationship hops.

`VS_BENCH_RELATION_FANOUT=1` adds a related-dimension update after the SQL
primary load. `VS_BENCH_MONGODB_UPDATES=1` updates every document in all four
collections with `full_document: update_lookup`. `VS_BENCH_NEO4J_FANOUT=1`
updates every second-hop node after the graph load. These phases measure
update and reverse-fan-out costs separately.

The source command also accepts `postgres`, `mysql`, `mongodb`, `kafka`, or
`neo4j`. The realtime command accepts `nats`, `nats_raw`, `nats_graphql`,
`redis`, `redis_raw`, or `redis_graphql`.

Source record counts, engine CPU/memory limits, payload size, and realtime
event counts can be overridden with the `VS_BENCH_*` variables declared at
the top of each runner. Generated CSVs, logs, and raw samples are written to
`target/benchmarks/container-matrix/<run-id>`.

Use `VS_BENCH_REALTIME_CLIENTS='200'` to isolate one supported concurrency
tier (`1`, `50`, or `200`) during capacity analysis.

Use `VS_BENCH_PROFILES='throughput'` to run a sustained pass for one selected
source profile after the three-profile tuning matrix.

## Tuned profiles

The source runner compares three bounded profiles. The balanced profile uses
an 8,192-event bus and eight OpenSearch requests; throughput uses 32,768 and
16; maximum uses 65,536 and 32. Larger profiles also increase dispatch batch
size, source chunk size, and source concurrency.

Realtime defaults are provider-specific because a single capacity was either
wasteful or lossy under burst load:

- raw NATS Core mailbox: 65,536;
- durable raw WebSocket mailbox: 1,024;
- GraphQL NATS broadcast capacity: 1,024;
- GraphQL Redis broadcast capacity: 1,024;
- raw Redis read batch: 1,000;
- GraphQL Redis read batch: 100.

Correctness is mandatory. A realtime run fails on any gap or duplicate, and a
CDC run fails unless OpenSearch contains the exact expected document count.
The complex runner also verifies nested joined fields after primary loads and
after relation fan-out. Source and sink containers use Docker cgroup sampling;
only the engine uses a PID-namespace sidecar for process RSS measurements.
