# VentStream real-time demo — copy-paste runbook

Publish an event to NATS and watch it arrive at a live subscriber in
real time — over **both** transports VentStream speaks:

- **Raw WebSocket** — the native JSON protocol on `ws://localhost:4040/ws`
  (via `@ventstream/client`)
- **Apollo Client** — `graphql-transport-ws` on `ws://localhost:4041/graphql/ws`
  (the standard Apollo subscription link)

```
publisher (@ventstream/sdk) ──► NATS (JetStream) ──► engine ┬─ ws role      ──► raw-WS client
   vs.t.acme.orders.order.status_changed.*         └─ graphql role ──► Apollo client
```

One engine binary runs both gateway roles (`VS_ROLES=ws,graphql`).

## 0. Prerequisites

- Docker + Docker Compose v2
- Node 18+ (to run the example publisher/subscriber scripts)
- Free ports: `4040` (ws), `4041` (graphql), `4222`/`8222` (NATS)

All commands run from the repo root unless noted.

## 1. Start the stack

```bash
cd demo/realtime
docker compose up -d --build
```

Allow 5–10 minutes for the first source build; subsequent builds are cached.
This starts NATS (JetStream) and the engine in `ws,graphql` mode. The
`ws` role bootstraps the JetStream stream `vsws`; the `graphql` role
consumes from it. Both roles share one health endpoint (the gateways
don't serve `/healthz` on their traffic ports). Confirm the engine is up:

```bash
curl -s -o /dev/null -w 'health %{http_code}\n' http://localhost:4043/healthz   # 200
# or watch the container's health status:
docker compose ps   # engine → "healthy"
```

## 2. One-time: build the SDK + client, install the example deps

The publisher/subscriber SDKs are TypeScript; build them once (their
`dist/` is gitignored), then install the example workspace:

```bash
# from the repo root
( cd packages/sdk    && npm install && npm run build )
( cd packages/client && npm install && npm run build )
( cd packages/example && npm install )
```

## 3. Demo A — raw WebSocket transport

`run-demo.mjs` opens a native-protocol WS client, subscribes, publishes
3 events via the SDK, and prints what it receives:

```bash
node packages/example/run-demo.mjs
```

Expected:

```
[client] connected and ready
[publisher] sent id=… subject=vs.t.acme.orders.order.status_changed.order_1
[client] event on vs.t.acme.orders.order.status_changed.order_1: {"id":…,"data":{"from":"pending","to":"confirmed",…}}
…
[demo] received 3 events end-to-end ✓
```

## 4. Demo B — Apollo Client transport

`apollo-demo.mjs` uses Apollo Client's `GraphQLWsLink` (the exact wiring
a frontend would use), runs a discovery query, subscribes, publishes 3
events, and verifies they arrive:

```bash
node packages/example/apollo-demo.mjs
```

Expected:

```
— Querying availableSubjects —
  • orders.order.status_changed.*  (Fired when an order's status transitions.)
  …
— Subscribing to orders.order.status_changed.* —
— Publishing 3 events via @ventstream/sdk —
  ← vs.t.acme.orders.order.status_changed.order_1
    entity=order_1  data={"from":"pending","to":"confirmed",…}
  …
[demo] received 3 events via Apollo Client ✓
```

## 4b. Demo C — GraphiQL playground (zero code)

The gateway serves an in-browser **GraphiQL** at
`http://localhost:4041/graphiql` (enabled here via `VS_GRAPHQL_PLAYGROUND=1`)
with the subscription endpoint and connection params pre-wired — nothing
to configure. The typed fields come from **`subscriptions.graphql`** (an
SDL file with `@vsSubscribe` / `@source` directives, mounted via
`VS_GRAPHQL_SCHEMA`):

```graphql
type Subscription {
  orderStatusChanged(orderId: ID!): OrderStatusChange!
    @vsSubscribe(subject: "orderStatusChanged.{orderId}")
}
```

1. Open `http://localhost:4041/graphiql` and run (▶):
   ```graphql
   subscription { orderStatusChanged(orderId:"order_1"){ id status changedAt } }
   ```
2. Publish an event (the `id` must be a valid ULID):
   ```bash
   docker compose -f demo/realtime/docker-compose.yml exec -T nats-box \
     nats pub vs.t.acme.orderStatusChanged.order_1 \
     '{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event":"orderStatusChanged","tenant":"acme","entity_id":"order_1","occurred_at":"2026-01-01T00:00:00Z","received_at":"2026-01-01T00:00:00Z","schema_version":2,"data":{"status":"confirmed"}}'
   ```
   GraphiQL shows `{ id:"order_1", status:"confirmed", changedAt:"…" }`.
   (Browse the **Docs** panel to see `orderStatusChanged`, `orderEvents`,
   and the generic `events(subject:)`.)

## 5. Optional — the live dashboard (visual)

A Vite + Apollo React dashboard is in `demo/webapp`. Point it at this
stack's GraphQL gateway (port 4041) and run it:

```bash
cd demo/webapp && npm install
VITE_VS_HTTP=http://127.0.0.1:4041/graphql \
VITE_VS_WS=ws://127.0.0.1:4041/graphql/ws \
VITE_VS_TENANT=acme VITE_VS_TOKEN=demo \
  npm run dev
```

Open the printed URL, then re-run `apollo-demo.mjs` (or
`node packages/example/publish-examples.mjs`) and watch events stream
into the UI.

## 6. Verify / inspect

```bash
# JetStream stream + message count
curl -s http://localhost:8222/jsz | python3 -m json.tool | grep -E 'streams|messages' | head

# publish a one-off event via the nats-box CLI helper (the nats server
# image has no client). `id` must be a valid 26-char ULID.
docker compose -f demo/realtime/docker-compose.yml exec -T nats-box \
  nats pub 'vs.t.acme.orderStatusChanged.ad_hoc' \
  '{"id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","event":"orderStatusChanged","tenant":"acme","entity_id":"ad_hoc","occurred_at":"2026-01-01T00:00:00Z","received_at":"2026-01-01T00:00:00Z","schema_version":2,"data":{"from":"pending","to":"paid"}}'
```

(a subscriber from step 3/4 must be connected first — delivery is
`New`, so there's no replay of past events)

## 7. Teardown

```bash
cd demo/realtime

# Stop + remove containers and the network, but KEEP volumes
# (fast re-run; nothing to persist here anyway):
docker compose down --remove-orphans

# Full reset — also remove volumes:
docker compose down -v --remove-orphans
```

`--remove-orphans` clears any leftover containers from earlier runs so the
next `up` is clean.

## Notes

- **Core vs JetStream.** This demo enables JetStream (`VS_WS_JETSTREAM=1`)
  because the GraphQL role consumes from a stream. The native WS role can
  also run in lighter core mode (no stream, no replay) by leaving
  `VS_WS_JETSTREAM` unset.
- **Subjects.** Events use `vs.t.<tenant>.<event>.<id>` (id last);
  clients subscribe with the tenant-relative pattern (the gateway anchors
  `vs.t.<tenant>.` for them). See the [real-time docs](/concepts/real-time-subscriptions).
- **Auth is permissive in v1** — any non-empty token is accepted and the
  tenant is trusted from the connection init. JWT validation is planned.
