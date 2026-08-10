# Data MCP server — design (2026-08-10)

Status: specced, not scheduled. Grounded in a full codebase analysis of the
sink encodings, config surface, and packaging options.

## What it is

A stateless read gateway exposing VentStream-materialized sink data to AI
agents over the Model Context Protocol. Agents get live operational context
(the replicas are updated within milliseconds by the pipeline) without any
source-database or control-plane credentials.

Data-plane component: it reads sinks directly. The control plane is never in
the query path (architecture invariant #1). Optionally Fleet-managed later as
an `mcp` role — config delivered by Fleet, traffic still direct.

## Tool surface (v1)

- `list_targets()` — targets with their document shapes, derived from
  pipeline config + joins spec (`JoinDefinition.related[]` gives embed_as,
  cardinality, selected fields — enough to describe each document to the
  agent without touching any database).
- `get_entity(table, pk | pks[])` — O(1) lookup via deterministic doc ids:
  - Redis: `key_from_parts(prefix, target, doc_id)` → GET / JSON.GET by
    document format.
  - OpenSearch/Elasticsearch: `_id` IS the canonical doc id verbatim →
    `GET /{index}/_doc/{id}`.
  - Meilisearch: fetch by the `_vs_id` field (the raw canonical id is stored
    on every document), avoiding pk re-encoding on the read path.
- `search(target, query, limit)` — passthrough with caps: OS `_search`,
  Meilisearch `/search`; Redis targets support `scan(target, pattern)`
  instead.
- MCP resources: `vs://targets/{name}` document-shape descriptions.

Guardrails: read-only sink credentials, per-key target allowlists, result
caps, rate limits. Freshness surfaced from the health endpoint so agents can
distinguish "current" from "pipeline degraded".

## Implementation decision: Rust subcommand (`ventstream mcp`)

A TypeScript `packages/mcp` was evaluated and rejected as the primary
implementation. The decisive argument: it would be the **seventh**
implementation of the deterministic-id/keyspace encodings — and
`doc_id.rs`'s own module header is a post-mortem of what happened last time
those encodings drifted (three in-binary copies disagreed on integer PKs and
silently orphaned documents). A cross-language, cross-repo, independently
versioned reimplementation whose failure mode is silently wrong answers to
an AI agent is the same bug with a bigger blast radius.

The Rust subcommand instead **links against the source of truth**:
`ventstream_core::doc_id`, `redis::keyspace::key_from_parts`,
`meilisearch::documents::encode_primary_key`, `opensearch::index_template::render`
(several need `pub(super)` → `pub(crate)`/`pub` visibility widening — small,
safe). Config parsing (`load_sink_config`, `load_joins_yaml`,
`PipelineEnv::load`), Redis cluster/TLS/auth, and the check-mode subcommand
pattern (`--check-redis-sink`, main.rs:303) are all reused for free.

Distribution tension: `npx @ventstream/mcp` is the ecosystem-standard MCP
install. Resolution: if npx ergonomics prove necessary, ship a **thin
launcher** package that spawns/proxies the engine binary's stdio — never an
independent reader. No duplicated encodings, npm-friendly onboarding.

## Target enumeration rules

`list_targets()` derivability by routing mode:
- `fixed` → the one target. `views` → view names. `by_projection_target` →
  `joins[].target.index` (already enforced present for every join by
  `validate_projection_target_indexes`).
- `by_output_relation` → **not derivable from config** (matches the
  `--check-redis-drift` precedent, which demands explicit `--redis-target`).
  v1: explicit target list in MCP config for this mode; optionally a bounded
  sink-side discovery (`SCAN {prefix}:{*}` / `_cat/indices`) later.

## Ops MCP (separate, later)

A client-side wrapper over the control-plane REST API (the CLI's auth):
`list_pipelines`, `agent_status`, `validate_configuration` (dry-run as an
agent tool), mutating actions gated behind confirmation. Half-day build,
independent of the data MCP.

## Effort and sequencing

~2–3 weeks for the Rust v1 (MCP protocol dependency + dep-policy review is
the long pole; tools themselves are days). Slotted after: soak close-out +
v0.2.21 EKS deploy (Aug 15), the content cadence, and npm publish. The
thin-launcher package depends on npm publish being unblocked anyway.

Not in v1: field-level masking (pipeline feature, composes automatically
when it lands), MCP push/subscriptions (client ecosystem can't receive
them yet), multi-tenant key management (single shared config first).
