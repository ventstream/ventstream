# VentStream

<p align="center">
  <img src="docs-site/images/ventstream-logo.svg" alt="VentStream" width="440">
</p>

<p align="center">
  <img src="docs-site/images/ventstream-architecture.svg" alt="VentStream architecture — one Rust binary with a change-streaming pipeline and a real-time socket-delivery pipeline" width="860">
</p>

**VentStream is a data-streaming engine — one Rust binary that streams
your data wherever it needs to go, in real time.** Two pipelines off one
event core, run together or apart:

1. **Change streaming → any target.** Capture changes from PostgreSQL, Neo4j,
   MongoDB, MySQL/MariaDB, or Kafka/Redpanda and stream them continuously into a
   downstream target — idempotent, bounded, crash-safe. The **sink is an
   interface, not a constraint**: OpenSearch is implemented and tested
   today; any other target is a connector away. Projections are declared
   in YAML and can run standalone or under the optional Fleet control plane.
2. **Real-time socket delivery → clients.** Publishers emit events to
   NATS; VentStream pushes them to subscribed clients over a native
   WebSocket protocol **and** GraphQL subscriptions (`graphql-transport-ws`,
   Apollo-compatible). Typed subscriptions are authored in GraphQL SDL.

Either way the job is the same — **move data, as it changes, to where it's
needed.** Select the pipelines per process with `VS_ROLES=cdc,ws,graphql`.

**Implemented today** (the engine is source/sink-pluggable — this is the
matrix that's built and tested; more backends are planned)
- **Sources:** PostgreSQL logical replication, Neo4j 5.17+ Enterprise CDC,
  MongoDB change streams, MySQL/MariaDB row binlog, and Kafka/Redpanda
  Debezium or raw topics
- **Targets (sinks):** OpenSearch / Elasticsearch — *one* sink behind a
  pluggable interface, not the point of the engine
- **Real-time:** native WebSocket + GraphQL subscriptions over NATS or Redis Streams

**Docs:** the full docs live in `docs-site/` (Mintlify) — concepts, guides,
deploy, and reference. Read them locally with no account or hosting:

```bash
npm i -g mint        # one-time
cd docs-site
mint dev             # → http://localhost:3000 (hot-reloads on edits)
```

## Try it locally

The self-contained demo brings up its Postgres and Neo4j sources plus the sink
with seeded data in one command — see `demo/stack/` (copy-paste runbook).

```bash
cd demo/stack && docker compose up -d
```

## Deploy

Choose a deployment mode in the
[Kubernetes guide](docs-site/deploy/kubernetes.mdx):

- **Standalone native binary:** install a checksum-verified Linux or macOS
  release with `ventstream-installer.sh`, provide `ventstream.yaml` plus local
  environment-backed secrets, and run without Fleet. See the
  [native binary guide](docs-site/deploy/native-binary.mdx).
- **Standalone CDC:** deploy the open-core image with canonical
  `ventstream.yaml`, workload-local Secrets, and a persistent StatefulSet. The
  current `ventstream-agent` chart predates Fleet and is deprecated for new
  installations; use the maintained standalone manifest in the guide.
- **Standalone realtime:** use `infra/helm/ventstream-gateway` for replicated
  native WebSocket and GraphQL roles.
- **Fleet-managed:** use the separately distributed VentStream Fleet control
  plane and supervisor, then administer enrolled engines with `ventstreamctl`.
  The CLI executable is available without GitHub authentication from the
  [public VentStream releases repository](https://github.com/ventstream/ventstream-releases).

Tagged releases publish the multi-architecture engine image at
`ghcr.io/ventstream/ventstream:<version>` and the standalone realtime
chart at `oci://ghcr.io/ventstream/charts/ventstream-gateway`. The
workflow does not publish a floating `latest` tag. Install a chart release with:

```bash
helm upgrade --install realtime \
  oci://ghcr.io/ventstream/charts/ventstream-gateway \
  --version <version>
```

Production values should set `image.digest=sha256:...`; the digest takes
precedence over `image.tag`. Release image digests, per-platform SPDX SBOMs,
vulnerability reports, and checksums are attached to the corresponding GitHub
release. See [`docs/releasing.md`](docs/releasing.md) for publication and
verification policy.

### Fleet-managed mode

VentStream Fleet is distributed separately from this open-core repository. Its
`ventstream-fleet-agent` supervisor runs as PID 1, owns the outbound mTLS
control stream and durable management state, and starts this open-core binary
as a child. Pause, resume, and drain are process lifecycle operations performed
by that supervisor; the engine does not receive Fleet credentials or connect
directly to the Fleet API.

The legacy `VS_CONTROL_PLANE_URL` and `VS_CONTROL_PLANE_KEY` telemetry channel
is not the Fleet protocol. The managed supervisor removes those variables from
the child environment.

When the supervisor sets `VS_FLEET_APPLIED_CONFIG_PATH`, the engine reads the
Fleet-staged non-secret configuration envelope before starting any role. The
envelope must pass its SHA-256 digest check and must not contain top-level
`secrets`. In schema version 1, the engine can consume these control-plane
managed inline specs from `document.specs`, with legacy env file paths as
fallbacks:

- `joins_yaml` replaces `VS_JOINS_YAML` for Postgres/MySQL join projections.
- `neo4j_denormalize_yaml` replaces `VS_NEO4J_DENORMALIZE_YAML`.
- `graphql_schema` replaces `VS_GRAPHQL_SCHEMA`.
- `graphql_subscriptions_yaml` replaces `VS_GRAPHQL_SUBSCRIPTIONS`.
- `graphql_manifest_yaml` replaces `VS_GRAPHQL_MANIFEST`.

Connection strings, passwords, API keys, and certificates remain local env/Secret
configuration. Fleet config stores topology and spec content, not secret values.

The numbered CDC manifests under `infra/k8s/` and the deprecated
`ventstream-agent` chart document the retired telemetry integration and are kept
only as migration references. Do not use them as the current Fleet deployment
path.

## Workspace layout

```
ventstream/
├── crates/
│   ├── ventstream-core/        Core event type, bus, shutdown, traits, errors
│   ├── ventstream-protocol/    Wire schema: event envelope + subject grammar (WS/GraphQL contract)
│   ├── ventstream-config/      Pipeline/spec YAML parsing & validation
│   ├── ventstream-sources/     Source adapters — Postgres, Neo4j, MongoDB, MySQL, Kafka
│   ├── ventstream-joins/       Stateful in-memory joins / denormalization (redb-persisted)
│   ├── ventstream-sinks/       Sink adapters — OpenSearch / Elasticsearch today
│   ├── ventstream-jetstream/   Shared JetStream consumer lifecycle (naming, RAII handle, reaper)
│   ├── ventstream-realtime/    Provider-neutral broker, session, capability, and cursor contract
│   ├── ventstream-redis/       Redis Streams shared tailer, replay, retention, and cursor adapter
│   ├── ventstream-ws/          Native WebSocket delivery gateway
│   ├── ventstream-graphql/     GraphQL subscription gateway (graphql-transport-ws, Apollo)
│   ├── ventstream-telemetry/   Outbound heartbeats to the control plane + tracing
│   └── ventstream/             Binary — CLI, role wiring (cdc/ws/graphql), lifecycle
├── packages/                   TypeScript SDK, client, and example apps
├── demo/                       Runnable demos — stack (CDC), realtime (WS/GraphQL), webapp
├── examples/                   Example specs / projects
├── infra/                      Deploy — helm/ charts, docker/ images, k8s/ manifests
├── docs-site/                  Published docs (Mintlify)
├── docs/                       Internal notes (testing, debugging, pre-prod checklist)
└── .github/workflows/          CI
```

## Engineering bar

- `unsafe_code = "forbid"` across the workspace
- Clippy `pedantic` + `nursery` enabled, with `unwrap` / `panic` / `todo` denied
- No memory leaks: every spawned task is owned and cancellable via `tokio_util::sync::CancellationToken`
- No race conditions: message-passing via channels is the primary concurrency model; shared mutable state requires explicit justification
- Release profile uses `lto = "fat"` + `panic = "abort"` for tightest binary

## Configuration

VentStream supports legacy environment-variable configuration and a canonical
non-secret `ventstream.yaml` via `VS_ENGINE_CONFIG`. Required variables are bare
names; optional ones list their default in parentheses.
The complete, authoritative list lives in
[`docs-site/reference/engine-env.mdx`](docs-site/reference/engine-env.mdx) —
this section is the common subset. Without `VS_ENGINE_CONFIG`, pick roles with
`VS_ROLES` (`cdc`,`ws`,`graphql`) and the CDC source with `VS_CDC_SOURCE`.

Minimal canonical config:

```yaml
schema_version: 1
roles: [cdc]
source: { kind: postgres }
sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    index_routing: { strategy: by_output_relation }
specs:
  joins: ./joins.yaml
```

### CDC source

`VS_CDC_SOURCE` selects `postgres`, `neo4j`, `mongodb`, `mysql` (`mariadb`
alias), or `kafka` (`redpanda` alias). The complete variable reference and
connector-specific guides cover all five source families; the common examples
below show Postgres and Neo4j.

**Postgres** (`VS_CDC_SOURCE=postgres`)
- `VS_PG_HOST`, `VS_PG_PORT` (5432), `VS_PG_USER`, `VS_PG_PASSWORD`, `VS_PG_DATABASE` — connection.
- `VS_PG_TLS_MODE=verify_full`, with optional `VS_PG_TLS_CA_FILE`, enforces certificate and hostname verification on every Postgres connection.
- `VS_PG_PUBLICATION`, `VS_PG_SLOT` — logical-replication publication + slot to consume.
- `VS_PG_BOOTSTRAP_MODE` — `snapshot` to seed state from existing rows on cold start.
- `VS_PG_BOOTSTRAP_CHUNK_SIZE` (10000) — rows per keyset-paginated chunk during bootstrap.
- `VS_JOINS_YAML` — projection spec; `VS_PG_AUTO_RESYNC_ON_YAML_CHANGE` — re-bootstrap when it changes.

**Neo4j** (`VS_CDC_SOURCE=neo4j`)
- `VS_NEO4J_URI` (e.g. `neo4j+s://host:7687`), `VS_NEO4J_USER`, `VS_NEO4J_PASSWORD`, `VS_NEO4J_DATABASE` (`neo4j`).
- `VS_NEO4J_TLS_MODE=verify_full`, with optional `VS_NEO4J_TLS_CA_FILE`, selects strict encrypted Bolt.
- `VS_NEO4J_DENORMALIZE_YAML` — denormalize spec; `VS_NEO4J_STATE_DIR` — cursor directory.
- `VS_NEO4J_BOOTSTRAP_BATCH_SIZE` (2000), `VS_NEO4J_POLL_INTERVAL_MS` (500), `VS_NEO4J_HOT_NODE_THRESHOLD` (100).
- `VS_NEO4J_RECOMPOSE_CHUNK` (128), `VS_NEO4J_RECOMPOSE_CONCURRENCY` (8) — live multi-hop recompose tuning.

### Sink
- `VS_OS_ENDPOINT` — OpenSearch URL.
- `VS_OS_TLS_MODE=verify_full`, with optional `VS_OS_TLS_CA_FILE`, enforces HTTPS certificate and hostname verification.
- `VS_INDEX_TEMPLATE` — destination index pattern; supports `${header:…}` and `%Y/%m/%d`.

### Joins
- `VS_JOINS_YAML` — path to the joins manifest.
- `VS_JOINS_STATE_DIR` — directory for redb-backed join state. Required when
  PostgreSQL or MySQL uses memory-mode joins; mount it on durable storage.

### Performance knobs (defaults validated under 50k-mutation bursts; 100k+ state rows)
| Var | Default | Purpose |
|---|---|---|
| `VS_PERSIST_BATCH_OPS` | `5000` | redb commit threshold. Smaller = faster crash-durability, more fsyncs. |
| `VS_LSN_FLUSH_MS` | `200` | How often the CDC source pushes acked-LSN back to Postgres. |
| `VS_JOIN_IDLE_FLUSH_MS` | `1000` | How often the join engine flushes pending persistence during idle. |
| `VS_BUS_CAPACITY` | `1024` | In-memory channel size between source → join → dispatcher. Larger absorbs bursts at memory cost. |
| `VS_DISPATCH_MAX_EVENTS` | `2000` | Events per sink batch. |
| `VS_DISPATCH_MAX_BATCH_BYTES` | `4194304` | Bytes per sink batch (4 MiB). Keep well below the sink's request-size limit. |
| `VS_DISPATCH_FLUSH_MS` | `500` | How long a non-empty batch may wait before being flushed. |
| `VS_DISPATCH_PARALLEL_BULKS` | `4` | Max sink bulk writes in flight at once. 1 = serial (legacy). 4 ≈ 2.2× resync throughput on a single-node OS; diminishing returns past 8. |

### WebSocket gateway (role: `ws`)
- `VS_WS_LISTEN` (`0.0.0.0:4040`) — bind address; serves `/ws`. (Health lives on the shared `VS_HEALTH_LISTEN` port, not here.)
- `VS_REALTIME_PROVIDER` — `nats_core`, `nats_jetstream`, or `redis_streams` for the enabled realtime roles.
- `VS_REDIS_URL` — Redis connection URL when the provider is `redis_streams`; use `rediss://` in production.
- `VS_WS_NATS_URL` (`nats://127.0.0.1:4222`) — NATS connection.
- `VS_WS_SUBJECTS` (`vs.t.>`) — comma-separated subject filters the gateway accepts.
- `VS_WS_MAILBOX` (`256`) — per-connection outbound mailbox depth. Larger absorbs bursts; smaller surfaces slow-consumer eviction faster.
- `VS_WS_PING_INTERVAL_MS` (`10000`) — server pings the client every tick; keeps TCP/NAT alive.
- `VS_WS_PONG_TIMEOUT_MS` (`30000`) — max wait for a client pong before the connection is reaped.

#### JetStream durable mode (per-connection consumer)
Opt in with `VS_WS_JETSTREAM=1`. Off by default; without it the gateway runs in Core mode (single shared bus subscription, in-process dispatch — much higher density, no replay).

- `VS_WS_JS_STREAM` (`ventstream`) — JetStream stream name.
- `VS_WS_JS_STORAGE` (`file`) — `file` (durable) or `memory` (faster; cleared on NATS restart).
- `VS_WS_JS_MAX_AGE_SECS` (`600`), `VS_WS_JS_MAX_BYTES` (`536870912`, 512 MiB) — self-bounding live-buffer limits (`discard: old`).
- `VS_WS_JS_POD_ID` (auto-ULID) — stable identifier for this pod. Set in production so a restarted pod can find and reap its previous consumers.
- `VS_WS_JS_INACTIVE_THRESHOLD_MS` (`300000`) — JetStream auto-deletes consumers inactive this long. Pull-loop long-polling keeps the consumer "active" during normal operation; this only fires when the pump dies (kill -9, OOM).
- `VS_WS_JS_REAPER_INTERVAL_MS` (`60000`) — how often the in-pod reaper sweeps for orphaned consumers.

### GraphQL gateway (role: `graphql`)
- `VS_GRAPHQL_LISTEN` (`0.0.0.0:4041`) — serves `/graphql`, `/graphql/ws`. (Health lives on the shared `VS_HEALTH_LISTEN` port, not here.)
- `VS_GRAPHQL_NATS_URL` (`nats://127.0.0.1:4222`), `VS_GRAPHQL_STREAM` (`ventstream`) — must consume the stream the `ws` role bootstraps.
- `VS_GRAPHQL_POD_ID` (auto-ULID), `VS_GRAPHQL_INACTIVE_THRESHOLD_MS` (`300000`), `VS_GRAPHQL_REAPER_INTERVAL_MS` (`60000`) — per-subscription consumer lifecycle.
- `VS_GRAPHQL_SCHEMA` — typed subscriptions as **GraphQL SDL** (`@vsSubscribe` / `@source`). The recommended way.
- `VS_GRAPHQL_SUBSCRIPTIONS` — the same model as a YAML manifest (legacy alternative to `VS_GRAPHQL_SCHEMA`).
- `VS_GRAPHQL_MANIFEST` — discoverable-subjects manifest backing the `availableSubjects` query.
- `VS_GRAPHQL_PLAYGROUND` (`1` to enable) — serve the in-browser GraphiQL at `/graphiql` (leave off in prod).

### Operations
- `VS_HEALTH_LISTEN` (`0.0.0.0:4043`) — the single, always-on health server shared by every role; serves `/healthz` (process liveness) + `/readyz`. For `ws` / `graphql`, readiness stays `503` until every enabled gateway has initialized its dependencies and bound its listener; it also returns `503` when WS reaches its capacity threshold. This is the canonical k8s probe target regardless of which roles run. A bind failure is logged and does not take the pipeline down.
- `VS_DLQ_PATH` — JSONL file for dead-lettered events.
- `VS_ADMIN_LISTEN` — bind address for the *optional* admin HTTP server (`/admin/resync`, plus `/admin/healthz`); off unless set. Distinct from the always-on health server above.
- `VS_ROLES` (`cdc`) — comma-separated roles for this binary: any of `cdc`, `graphql`, `ws`. A single binary can run multiple roles in one process.

## Local development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## License

Apache-2.0. See `LICENSE`.
