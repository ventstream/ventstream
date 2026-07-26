# VentStream demos

The repository contains two independently runnable demonstrations of the
engine's primary capabilities.

## CDC and denormalized projections

[`stack/`](./stack/) runs Postgres and Neo4j sources, two VentStream CDC
agents, and OpenSearch. It bootstraps denormalized order and product indexes,
then demonstrates row updates, relationship fan-out, multi-hop recomposition,
and deletes.

Follow the tested [docs quickstart](../docs-site/quickstart.mdx) from top to
bottom.

## Real-time subscriptions

[`realtime/`](./realtime/) runs NATS JetStream and one engine with the `ws`
and `graphql` roles. It supports the native WebSocket protocol, generic Apollo
subscriptions, typed GraphQL subscriptions from SDL, and the GraphiQL
playground.

Follow the copy-paste [real-time runbook](./realtime/README.md).

## Optional visual assets

- [`webapp/`](./webapp/) is the React/Vite live subscription dashboard.
- [`presentation/`](./presentation/) is the browser-based presentation deck.

The canonical ports, environment variables, event envelope, and subject
grammar live in the two runbooks above. Do not use older standalone commands
from presentations as deployment instructions.
