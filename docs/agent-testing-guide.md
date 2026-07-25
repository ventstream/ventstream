# Managed Agent Testing Guide

This guide covers the current VentStream Fleet architecture. The control plane is
the Rust workspace in the sibling `ventstream-control-plane` repository. Operators
use `ventstreamctl`; there is no required web console, API-key agent registration,
or direct engine administration endpoint.

## Architecture under test

```text
operator -> ventstreamctl -> control API -> Fleet PostgreSQL
                                      |
managed supervisor -> outbound mTLS -> agent gateway
        |
        +-> VentStream engine child -> source / OpenSearch / NATS
```

The control plane never receives source records, sink documents, or realtime
events. It stores identity, desired state, immutable configuration, durable
operations, status, and audit metadata.

## Prerequisites

- `ventstream` and `ventstream-control-plane` cloned next to one another
- Docker Compose v2
- Rust toolchain pinned by each repository
- `curl`, `jq`, `openssl`, and Python 3

```text
workspace/
├── ventstream/
└── ventstream-control-plane/
```

## Complete local smoke

The maintained smoke starts isolated Postgres and OpenSearch fixtures, the full
Fleet control plane, and a real managed engine. It creates and verifies a built-in
account through Mailpit, creates Fleet resources, enrolls the supervisor, applies
configuration, resumes the engine, and waits for a projected document.

```bash
cd ventstream
./demo/fleet/local.sh reset
./demo/fleet/local.sh start
./demo/fleet/local.sh status
./demo/fleet/local.sh change
./demo/fleet/local.sh pause
./demo/fleet/local.sh resume
```

`change` succeeds only after a live Postgres update reaches OpenSearch. `pause`
and `resume` wait for terminal operation success through the real gateway and
supervisor hooks.

Inspect failures with:

```bash
./demo/fleet/local.sh logs
./demo/fleet/local.sh ctl operations list --pipeline orders-cdc
```

Clean up only the isolated demo projects and state:

```bash
./demo/fleet/local.sh reset
```

## Manual CLI contract

The local smoke automates this sequence:

1. `auth signup`, receive Mailpit verification, and `auth verify-email`.
2. `orgs create` with an initial environment.
3. `pipelines create` and `agents create`.
4. `pipelines configurations create`, `validate`, and `select`.
5. `agents enroll-token create` and one-time mTLS enrollment.
6. Start the supervisor with persisted identity and management state.
7. `pipelines configurations apply --wait`.
8. `pipelines resume --wait`.
9. Inspect `agents status` and `operations list`.

Use the public
[`docs-site/fleet/cli.mdx`](../docs-site/fleet/cli.mdx) and
[`docs-site/deploy/kubernetes-managed-engine.mdx`](../docs-site/deploy/kubernetes-managed-engine.mdx)
for complete commands and security boundaries.

## Required lifecycle checks

### Pause

```bash
ventstreamctl pipelines pause orders-cdc \
  --reason "test maintenance" --wait
```

Confirm the operation succeeds, the supervisor stops the child, cursor/PVC state
remains, and `agents status` reports the desired paused state.

### Resume

```bash
ventstreamctl pipelines resume orders-cdc --wait
```

Confirm the engine resumes from its retained source cursor and a new source change
reaches the sink without a full bootstrap.

### Drain

```bash
ventstreamctl pipelines drain orders-cdc \
  --reason "test retirement" --wait
```

Confirm connector-specific drain cleanup completes and desired state remains
drained across a supervisor restart.

### Reconcile and rebootstrap

```bash
ventstreamctl pipelines reconcile orders-cdc \
  --reason "test parity" --wait

ventstreamctl pipelines rebootstrap orders-cdc \
  --reason "destructive test" --confirm --wait
```

Run rebootstrap only against disposable fixtures. Verify the operation receipt,
attempt history, cursor reset, bootstrap, and final sink parity.

## Configuration checks

1. Create and apply revision A.
2. Create revision B with a deterministic projection change.
3. Validate and select B.
4. Apply B with `--wait` and confirm the agent reports its revision and digest.
5. Roll back to A and confirm the prior immutable bundle is restored.
6. Attempt to submit inline `secrets` or plaintext password fields and confirm
   control-plane/engine validation rejects them.
7. Change the selected revision concurrently and confirm stale `If-Match`
   requests fail instead of overwriting desired state.

## Identity and restart checks

- Verify the enrollment grant is single-use and expires after its configured TTL.
- Restart a supervisor with its identity and management PVC retained; it must not
  require another enrollment redemption.
- Remove trusted desired state before a managed first boot; CDC must fail closed.
- Interrupt the control plane while the engine is running; the engine must keep
  its cached running state and reconnect later.
- Revoke deployment identity through `agents identity revoke`; reconnect must fail
  until a new enrollment is performed.
- Verify private keys, grants, access tokens, connector passwords, and email tokens
  never appear in logs.

## Tenant isolation checks

Create two organizations with separate profiles and pipelines. For every list,
read, lifecycle, configuration, operation, and enrollment endpoint:

- Same-tenant authorized access succeeds.
- Foreign organization, environment, pipeline, deployment, and operation IDs are
  denied without revealing resource existence.
- Organization A cannot observe Organization B IDs through cursors, audit events,
  operation attempts, or status responses.

The control-plane PostgreSQL integration suite provides the lower-level forced-RLS
coverage; this smoke verifies the public API and CLI boundary.

## Kubernetes smoke

The control-plane repository contains a disposable cluster test:

```bash
cd ventstream-control-plane
FLEET_SMOKE_BUILD_IMAGES=1 \
FLEET_SMOKE_ENGINE_MODE=real \
./infra/k8s/fleet-smoke.sh
```

For kind, also set `FLEET_SMOKE_LOAD_KIND_IMAGES=1` and the cluster name when it is
not `kind`. The smoke installs two organizations, exercises cross-tenant denial,
enrolls managed agents, applies configuration, verifies lifecycle operations, and
proves snapshot plus live Postgres-to-OpenSearch-compatible delivery.

## Exit criteria

A release candidate is not accepted from screenshots or queued operations alone.
Record:

- Terminal operation results and delivery attempts
- Agent status before and after each lifecycle transition
- Source mutation and sink evidence
- Restart and control-plane-outage behavior
- Cross-tenant denial evidence
- Image digests, configuration revision/digest, and test timestamps
