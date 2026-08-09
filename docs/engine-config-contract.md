# VentStream Engine Config Contract

This document separates the implemented `ventstream.yaml` contract from future
configuration goals. It exists to keep Fleet-managed configuration honest: a
field should not be documented as managed unless the engine actually consumes it.

## Current model

An engine process still runs one CDC source and one sink. Horizontal scale and
multiple tenants/pipelines are handled by running multiple engine agents, not by
placing multiple CDC sources in one engine process.

The engine can run these roles:

- `cdc`: source CDC stream to sink.
- `ws`: native WebSocket fan-out over NATS Core, NATS JetStream, or Redis Streams.
- `graphql`: GraphQL subscriptions over NATS JetStream or Redis Streams.

`VS_ENGINE_CONFIG` points at `ventstream.yaml`. When absent, the engine keeps the
legacy `VS_*` environment-variable behavior.

## Implemented `ventstream.yaml` fields

```yaml
schema_version: 1
roles: [cdc]

source:
  kind: postgres
  postgres:
    host_ref: env:VS_PG_HOST
    port: 5432
    user: ventstream
    password_ref: env:VS_PG_PASSWORD
    database: shop
    publication: ventstream_shop
    slot: ventstream_shop_slot
    bootstrap:
      mode: snapshot
      chunk_size: 10000

sink:
  kind: opensearch
  opensearch:
    endpoint_ref: env:VS_OS_ENDPOINT
    auth:
      mode: api_key
      api_key_ref: env:VS_OS_API_KEY
    index_routing:
      strategy: by_projection_target

specs:
  joins: projections/orders.yaml

runtime:
  health_listen: 0.0.0.0:4043
  bus_capacity: 2048
  dlq_path: /var/lib/ventstream/dlq.jsonl
  dispatch:
    max_events: 1000
    max_batch_bytes: 1048576
    flush_ms: 250
    parallel_bulks: 2
  memory:
    enabled: true
    max_event_bytes: 33554432
    sample_ms: 100
    recovery_ms: 1000
    target_percent: 65
    high_percent: 75
    critical_percent: 85
    hysteresis_percent: 5
  joins:
    state_dir: /var/lib/ventstream/joins
    persist_batch_ops: 5000
    idle_flush_ms: 1000
    lsn_flush_ms: 200
  tenant: tenant_a
```

The engine now consumes:

- `roles`
- `source.kind`
- `source.postgres.*`
- `source.neo4j.*`
- `source.mongodb.*`
- `source.mysql.*`
- `source.kafka.*`
- `sink.opensearch.endpoint_ref`
- `sink.opensearch.auth`
- `sink.opensearch.index_routing` with `by_output_relation`,
  `by_projection_target`, `fixed`, or `template` strategy
- `sink.opensearch.reconcile_allow_full_purge`
- `sink.opensearch.insecure_tls`
- `sink.meilisearch.endpoint_ref`
- `sink.meilisearch.api_key_ref`
- `sink.meilisearch.index_routing` with `by_output_relation`,
  `by_projection_target`, or `fixed` mode
- `sink.meilisearch.index_prefix`, `primary_key_field`,
  `auto_create_indexes`, `max_batch_docs`, `max_batch_bytes`,
  `task_deadline_ms`, `request_timeout_ms`, `settings`, `tls`,
  `insecure_tls`
- `specs.joins`
- `specs.neo4j_denormalize`
- `specs.graphql_schema`
- `specs.graphql_subscriptions`
- `specs.graphql_manifest`
- `runtime.health_listen`
- `runtime.bus_capacity`
- `runtime.dlq_path`
- `runtime.dispatch.*`
- `runtime.memory.*`
- `runtime.joins.*`
- `runtime.realtime.*`
- `runtime.ws.*`
- `runtime.graphql.*`
- `runtime.admin.*`

`by_projection_target` is available for PostgreSQL and MySQL join projections.
Every definition in `specs.joins` must set a non-empty `target.index`; startup
validation rejects incomplete projection routing before CDC begins.
- `runtime.tenant`
- `runtime.log_format`

## Adaptive memory controller

CDC event-count limits do not bound memory when row/document sizes vary. The
adaptive controller therefore charges a conservative byte estimate before an
event enters the engine bus. That charge follows zero-copy event clones through
transform output, dispatcher batches, sink retries, and DLQ handling, and is
released only after the last clone is dropped.

In a Linux container, `enabled: true` detects a finite cgroup v2 or v1 memory
limit automatically and assigns 30% of it to retained events. The remainder is
headroom for the runtime, connector clients, join state, and temporary request
bodies. On bare metal or macOS, set `budget_bytes` explicitly to opt in because
there is no container limit to derive safely.

```yaml
runtime:
  memory:
    enabled: true
    # Optional in a memory-limited container; required to opt in on bare metal.
    budget_bytes: 268435456
    # Must not exceed one quarter of budget_bytes when both are explicit.
    max_event_bytes: 33554432
    sample_ms: 100
    # Escalation is immediate; recovery requires this long below the threshold.
    recovery_ms: 1000
    target_percent: 65
    high_percent: 75
    critical_percent: 85
    hysteresis_percent: 5
```

At the target, high, and critical thresholds the controller progressively
reduces source admission, sink batch bytes, and sink concurrency. Escalation is
immediate. Recovery uses both hysteresis and a continuous recovery interval to
avoid oscillation during burst-and-drain workloads. Joined pipelines reserve transformation
headroom so a full input queue cannot deadlock while producing a larger output
document. A single event above `max_event_bytes` fails explicitly rather than
risking a process OOM. Normal source checkpoint rules still apply: blocked
admission backpressures the source before its durable cursor can advance.

Equivalent legacy environment variables are:

- `VS_MEMORY_CONTROLLER_ENABLED`
- `VS_MEMORY_BUDGET_BYTES`
- `VS_MEMORY_MAX_EVENT_BYTES`
- `VS_MEMORY_SAMPLE_MS`
- `VS_MEMORY_RECOVERY_MS`
- `VS_MEMORY_TARGET_PERCENT`
- `VS_MEMORY_HIGH_PERCENT`
- `VS_MEMORY_CRITICAL_PERCENT`
- `VS_MEMORY_HYSTERESIS_PERCENT`

This controller governs CDC event flow. Realtime WebSocket/GraphQL connection
mailboxes and broadcast capacities remain separately bounded by their role
configuration.

The release container also configures jemalloc with a background thread,
500 ms dirty-page decay, and 1 second muzzy-page decay. Keep equivalent or
faster release behavior in memory-constrained custom images; a long allocator
decay can retain already-freed bulk buffers past the cgroup OOM window even
after admission has been reduced.

## Postgres source settings

```yaml
source:
  kind: postgres
  postgres:
    host: postgres
    host_ref: env:VS_PG_HOST
    port: 5432
    user: ventstream
    user_ref: env:VS_PG_USER
    password_ref: env:VS_PG_PASSWORD
    database: shop
    database_ref: env:VS_PG_DATABASE
    publication: ventstream_shop
    publication_ref: env:VS_PG_PUBLICATION
    slot: ventstream_orders_slot
    slot_ref: env:VS_PG_SLOT
    bootstrap:
      mode: snapshot # snapshot | none
      chunk_size: 10000
    denormalize_mode: memory # memory | sql
    sink_reverse_lookup: true
```

Direct values and `*_ref` are mutually exclusive. `password` is intentionally not
accepted; use `password_ref`.

When `denormalize_mode` is `memory` and `specs.joins` resolves to one or more
join definitions, `runtime.joins.state_dir` (or `VS_JOINS_STATE_DIR`) is
required and must be mounted on durable storage. The engine refuses to start
without it so a restart cannot resume the source beyond join state that only
existed in memory. SQL denormalization does not use this state store.

## Neo4j source settings

```yaml
source:
  kind: neo4j
  neo4j:
    uri_ref: env:VS_NEO4J_URI
    user: neo4j
    user_ref: env:VS_NEO4J_USER
    password_ref: env:VS_NEO4J_PASSWORD
    database: neo4j
    database_ref: env:VS_NEO4J_DATABASE
    namespace: neo4j
    state_dir: /var/lib/ventstream/neo4j-state
    poll_interval_ms: 500
    idle_advance_after_polls: 20
    label_tables:
      Product: products
    reltype_tables:
      SUPPLIED_BY: supplied_by
    label_filter: [Product]
    reltype_filter: [SUPPLIED_BY]
    label_priority: [Product, CatalogItem]
    bootstrap:
      mode: snapshot # snapshot | none
      chunk_size: 2000
    recompose_chunk: 128
    recompose_concurrency: 8
    projection_fan_out: true
    hot_node_threshold: 100
    trust_cert_file: /etc/ventstream/neo4j-ca.pem
```

`password` is intentionally not accepted; use `password_ref`.

## MongoDB source settings

```yaml
source:
  kind: mongodb
  mongodb:
    uri_ref: env:VS_MONGO_URI
    database: shop
    database_ref: env:VS_MONGO_DATABASE
    namespace: commerce
    state_dir: /var/lib/ventstream/mongo-state
    collections: [orders, customers]
    full_document: update_lookup # update_lookup | default
    bootstrap:
      mode: snapshot # snapshot | none
      chunk_size: 1000
    token_flush_ms: 1000
```

MongoDB connection strings often contain credentials, so `uri` is intentionally
not accepted; use `uri_ref`.

## MySQL source settings

```yaml
source:
  kind: mysql
  mysql:
    host: mysql
    host_ref: env:VS_MYSQL_HOST
    port: 3306
    user: repl
    user_ref: env:VS_MYSQL_USER
    password_ref: env:VS_MYSQL_PASSWORD
    database: shop
    database_ref: env:VS_MYSQL_DATABASE
    namespace: shop
    server_id: 4000000000
    state_dir: /var/lib/ventstream/mysql-state
    tables: [orders, customers]
    bootstrap:
      mode: snapshot # snapshot | none
      chunk_size: 1000
    pos_flush_ms: 1000
    denormalize_mode: memory # memory | sql
    sink_reverse_lookup: true
```

`password` is intentionally not accepted; use `password_ref`.

## Kafka/Redpanda source settings

```yaml
source:
  kind: kafka
  kafka:
    brokers: localhost:9092
    brokers_ref: env:VS_KAFKA_BROKERS
    topics: [orders]
    group_id: orders-consumer
    group_id_ref: env:VS_KAFKA_GROUP_ID
    namespace: shop
    unwrap: debezium # debezium | raw
    auto_offset_reset: earliest
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: ventstream
    sasl_username_ref: env:VS_KAFKA_SASL_USERNAME
    sasl_password_ref: env:VS_KAFKA_SASL_PASSWORD
    ssl_ca_location: /etc/ventstream/kafka-ca.pem
    raw_key_field: id
    commit_ms: 1000
```

`sasl_password` is intentionally not accepted; use `sasl_password_ref`.

## Projection specs

Relational projections are currently parsed from a file under `specs.joins`.
The file shape remains:

```yaml
joins:
  - name: orders
    primary:
      table: shop.orders
      pk: order_id
    target:
      index: orders_v1
    related: []
```

Product language can call these "projections", but the current engine file key
is still `joins`. A future schema can add `specs.projections` as an alias after
we decide the public naming.

Neo4j denormalization remains separate because its runtime model is different:

```yaml
denormalize:
  - primary_label: Product
    output_table: products_denormalized
    fan_out_max_hops: 2
    cypher: |
      RETURN elementId(p) AS primaryEid, { id: p.id } AS doc
```

## Security rules

Fleet-managed config must not contain secret values. Use refs:

- `env:VS_PG_PASSWORD`
- `env:VS_NEO4J_PASSWORD`
- `env:VS_MONGO_URI`
- `env:VS_MYSQL_PASSWORD`
- `env:VS_KAFKA_SASL_PASSWORD`
- `env:VS_OS_API_KEY`
- `env:VS_REDIS_URL`

The control plane validates managed configs before selection/apply. Agents still
resolve the referenced values locally from the customer deployment environment.

## Realtime runtime settings

Broker selection and Redis connection settings can be shared by both gateway
roles. `url_ref` is required in managed configuration so credentials do not
enter the control plane:

```yaml
runtime:
  tenant: tenant_a
  realtime:
    provider: redis_streams # nats_core | nats_jetstream | redis_streams
    redis_streams:
      url_ref: env:VS_REDIS_URL
      key_prefix: ventstream
      read_batch: 256
      block_timeout_ms: 5000
      broadcast_capacity: 2048
      max_tenant_hubs: 1024
      max_length: 1000000
      connect_timeout_ms: 5000
      response_timeout_ms: 5000
  ws:
    listen: 0.0.0.0:4040
    mailbox: 256
    ping_interval_ms: 10000
    pong_timeout_ms: 30000
    max_connections: 10000
  graphql:
    listen: 0.0.0.0:4041
    broadcast_capacity: 1024
    playground: false
```

The existing role-local NATS/JetStream shape remains supported:

```yaml
runtime:
  tenant: tenant_a
  ws:
    listen: 0.0.0.0:4040
    nats_url: nats://nats:4222
    subjects: [vs.t.tenant_a.>]
    mailbox: 256
    ping_interval_ms: 10000
    pong_timeout_ms: 30000
    max_connections: 10000
    jetstream:
      stream: ventstream
      pod_id: pod-0
      inactive_threshold_ms: 300000
      reaper_interval_ms: 60000
      replicas: 3
      storage: file # file | memory
      max_age_secs: 600
      max_bytes: 536870912
      max_msgs: -1
  graphql:
    listen: 0.0.0.0:4041
    nats_url: nats://nats:4222
    stream: ventstream
    pod_id: pod-0
    inactive_threshold_ms: 300000
    reaper_interval_ms: 60000
    broadcast_capacity: 1024
    playground: false
  admin:
    listen: 127.0.0.1:4042
    token_ref: env:VS_ADMIN_TOKEN
```

## Still env-driven

These are process/supervisor controls rather than pipeline config:

- `VS_ENGINE_CONFIG`, which points the process at this YAML file.
- `VS_FLEET_APPLIED_CONFIG_PATH`, `VS_FLEET_SUPERVISED`, and
  `VS_FLEET_FORCE_BOOTSTRAP`, which are set by the fleet supervisor.
- `VS_CONTROL_PLANE_URL`, `VS_CONTROL_PLANE_KEY`, and telemetry export knobs,
  which are still owned by the telemetry crate.
- `VS_AGENT_NAME`, used as the default runtime identity/source id when the
  deployment does not provide one through Fleet metadata.
- `VS_SINK`, retained as a legacy sink selector while the only implemented sink
  remains OpenSearch/Elasticsearch-compatible.

## Next migration order

1. Decide public naming for relational `joins` versus `projections`.
2. Add generated JSON Schema for IDE validation and Fleet UI forms.
3. Move telemetry config out of env-only startup if we want it in
   `ventstream.yaml`.
