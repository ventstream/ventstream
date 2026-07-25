# Pre-production checklist

This is the release gate for VentStream engine and Fleet deployments. It is not
a feature roadmap. A successful local demo proves integration; it does not prove
that an environment is ready to carry customer production traffic.

Last reviewed: 2026-07-12.

Current assessment: **internal alpha**. The core engine, Fleet control plane,
managed supervisor, CLI, authentication, and Kubernetes packaging work end to
end. The release blockers below still require environment-specific implementation
and recorded evidence.

## P0 release blockers

| Gate | Required evidence |
| --- | --- |
| Release images | Reproducible multi-architecture engine and Fleet images are published by CI with immutable digests, SBOMs, provenance, signatures, and an agreed vulnerability threshold. The engine release workflow and Rust dependency policy are implemented. A successful protected-tag run and the corresponding Fleet release evidence still must be retained before this gate closes. |
| Realtime authorization | Native WebSocket and GraphQL subscription clients use validated, expiring credentials; tenant and subject claims are enforced before subscription; publish and subscribe permissions are distinct. The current presence-only token check is not sufficient for untrusted exposure. |
| Observability | Prometheus scrapes `/metrics`; alerts cover missing heartbeat, growing DLQ, cursor lag, agent disconnects, failed operations, SMTP delivery failures, certificate renewal, and Fleet database saturation. Dashboards and log retention are owned by the target platform. |
| Pilot parity and soak | Each intended source/sink combination runs beside the system it replaces. Record document parity, bootstrap duration, mutation-to-sink latency, memory, cursor age, retries, and DLQ behavior under real mutation pressure for the agreed soak period. |
| Failure and recovery | Prove process kill, node drain, source outage, sink outage, control-plane outage, certificate renewal/revocation, backup restore, and rebootstrap. Record RPO/RTO and the decision for PVC snapshot versus rebootstrap recovery. |
| Secrets and network | Connector credentials, SMTP credentials, signing material, and CA private keys come from an approved secret manager. Network policy restricts source, sink, NATS, Fleet API/gateway, SMTP, DNS, and metrics paths to the minimum required flows. |
| Independent security review | Review tenant isolation, authorization matrices, mTLS boundaries, token/session handling, dependency/image/IaC scans, and incident procedures. No unresolved critical or high finding is accepted for release. |

## Engine deployment checks

- Pin the engine image by digest and promote the same digest between
  environments.
- Use canonical `ventstream.yaml`; keep credential values in environment-backed
  Secrets, not in the file or Fleet configuration revision.
- Run every CDC workload as exactly one active replica with a unique source
  cursor identity: replication slot, MySQL server ID, Kafka consumer group, or
  connector state directory as applicable.
- Persist cursor, join, DLQ, and reconciliation state. Verify ownership and free
  space for the non-root container user.
- Use SQL denormalization mode for production SQL-source workloads when bounded
  memory is required, and create indexes for every join/FK lookup first.
- Treat `/healthz` as liveness and `/readyz` as traffic readiness. Bootstrap-aware
  CDC readiness is still a hardening item; do not infer bootstrap completion from
  a live process alone.
- Configure a PodDisruptionBudget for replicated realtime gateways. A PDB cannot
  make a singleton CDC process highly available.
- Do not expose native WebSocket or GraphQL listeners to untrusted clients until
  the realtime authorization gate is closed.

## Local source resilience suites

The real source and OpenSearch integration suites are intentionally excluded
from hosted CI. Run one connector locally after changing it, and run the complete
sequential matrix before a release candidate:

```bash
./scripts/test-sources.sh mongodb
./scripts/test-sources.sh all
```

The selector supports `postgres`, `neo4j`, `mongodb`, `mysql`, and `kafka`.
Each suite owns its Docker containers and drives the real engine binary. Keep the
dated command result with release evidence.

## Fleet control-plane checks

- Use external PostgreSQL with a schema-owning migrator role, a separate DDL-free
  runtime role, encrypted connections, backups, and point-in-time recovery.
- Configure HTTPS ingress, gateway server TLS, the agent client CA, issuing CA,
  enrollment signing key, first-party JWT keyring, independent throttle and mail
  HMAC keys, and SMTP or enterprise OIDC.
- Scope every control worker to explicit organization UUIDs. The placeholder
  organization in the production example is bootstrap-only and performs no
  tenant maintenance.
- Keep API, worker, gateway, migrator, supervisor, and engine revisions within
  their tested compatibility window.
- Run the documented two-phase password-reset rollout when upgrading a release
  that predates credential generation.
- Back up Fleet PostgreSQL before schema upgrades; verify the migration Job before
  rolling API, worker, or gateway pods.
- Enroll each managed deployment with a short-lived single-use grant and retain
  the workload PVC across restarts. The current chart requires the consumed token
  Secret object to remain while enrollment mode is enabled.
- Do not scale automatic enrollment beyond one replica. Replicated managed
  realtime needs pre-provisioned Fleet-compatible identity or the standalone
  gateway chart until per-replica enrollment exists.
- Prove organization isolation with at least two authenticated CLI profiles and
  independently enrolled deployments before release.

## Shipped foundations

- Five CDC source families: PostgreSQL, Neo4j, MongoDB, MySQL/MariaDB, and
  Kafka/Redpanda, with OpenSearch/Elasticsearch as the implemented sink.
- Native WebSocket and GraphQL subscription roles over NATS Core, NATS
  JetStream, or Redis Streams.
- Canonical non-secret engine configuration with server-side Fleet validation,
  immutable revisions, selection, apply, and rollback.
- Built-in verified accounts, rotating refresh sessions, logout/revocation,
  password recovery, signing-key rotation, and optional enterprise OIDC.
- Organization-scoped API/CLI administration, one-time enrollment, mTLS agent
  streams, desired-state operations, receipts, audit records, and health views.
- Engine and Fleet Dockerfiles, a production Fleet umbrella chart, a managed
  agent chart, a standalone realtime chart, and a documented standalone CDC
  StatefulSet baseline.
- Prometheus engine metrics, structured JSON logs, persisted cursors/join state,
  idempotent sink writes, DLQ handling, and tested pause/resume/drain/reconcile/
  rebootstrap paths.
- Cgroup-aware byte admission is enabled for CDC pods; alerts cover sustained
  `vs_memory_pressure_state >= 2`, admission throttling, oversized events, and
  container OOM kills. Pod limits leave non-event headroom and are validated by
  a constrained-memory load test before promotion.
- A locked Rust dependency graph with RustSec, license, source, and wildcard
  policy enforced by pinned tooling in pull requests and daily CI.

## Release evidence record

For each release candidate, retain:

1. Source commit and immutable image digests.
2. Unit, integration, dependency-policy, Helm lint/template, OpenAPI
   compatibility, and full managed engine E2E results.
3. Configuration revisions and redacted deployment values used for the test.
4. Parity, load, soak, chaos, backup/restore, and security reports.
5. Known limitations, rollback steps, incident contacts, and explicit release
   approval.

The engine artifact names, promotion rules, verification commands, and release
recovery procedure are defined in `docs/releasing.md`.

The detailed Fleet-specific gate is maintained in the control-plane repository at
`docs/08-production-readiness.md`.
