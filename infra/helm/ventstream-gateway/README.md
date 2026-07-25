# ventstream-gateway

The VentStream **real-time gateways** — the stateless `ws` (native
WebSocket) and `graphql` (graphql-transport-ws) fan-out roles. Unlike a
single-active CDC StatefulSet, this is a `Deployment` you scale horizontally
or behind an HPA.

```bash
helm install gw ./infra/helm/ventstream-gateway -n ventstream \
  --set nats.url=nats://ventstream-nats.ventstream.svc:4222
```

A `Service` exposes the enabled roles' ports (ws `4040`, graphql `4041`).
Each pod's name is its consumer `pod_id` (downward API), so replicas reap
only their own JetStream consumers.

## Values

| Key | Default | Purpose |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/ventstream/ventstream` / appVersion | engine image |
| `image.digest` | empty | immutable image digest; takes precedence over the tag |
| `roles` | `ws,graphql` | which gateway roles run (`ws`, `graphql`, or both) |
| `replicas` | `2` | pod count (ignored when `autoscaling.enabled`) |
| `nats.url` | `nats://ventstream-nats.ventstream.svc:4222` | the bus (JetStream-enabled) |
| `ws.subjects` | `vs.t.>` | bus subscription; shard per-tenant to scale |
| `ws.mailbox` | `256` | per-connection outbound queue depth |
| `ws.maxConns` | `5000` | per-pod connection cap (OOM backstop); `0` = unlimited. Upgrade past it → `503 + Retry-After`; after gateway startup, `/readyz` flips at 90%. Size to the memory limit (`~(limit − base) / 165 KiB-per-conn`). |
| `ws.jetstream.enabled` | `true` | durable per-conn consumers; **required for the graphql role** |
| `ws.jetstream.stream` / `.storage` | `ventstream` / `file` | stream name; `file` or `memory` |
| `ws.jetstream.maxAgeSecs` / `.maxBytes` | `600` / `512Mi` | self-bounding live-buffer limits |
| `graphql.stream` | `ventstream` | stream to consume (must match `ws.jetstream.stream`) |
| `graphql.schema.inline` / `.existingConfigMap` | unset | typed-subscriptions **SDL** → `VS_GRAPHQL_SCHEMA` |
| `graphql.playground` | `false` | serve GraphiQL at `/graphiql` (leave off in prod) |
| `service.type` / `.wsPort` / `.graphqlPort` | `ClusterIP` / `4040` / `4041` | Service shape |
| `autoscaling.enabled` | `false` | HPA (`min/maxReplicas`). Defaults to **memory** (`targetMemoryUtilizationPercentage: 70`) — the right signal for the RAM-bound WS gateway; jemalloc keeps RSS honest. Set `targetCPUUtilizationPercentage` to also scale on CPU; set either to `null` to disable that metric. Needs metrics-server. |
| `resources` | 250m / 256Mi req, 1Gi mem limit | container resources |

## Notes

- **Core vs JetStream.** With `ws.jetstream.enabled: false` the `ws` role
  runs core mode — zero consumers, highest fan-out throughput,
  at-most-once, no replay. Enable JetStream for per-connection durable
  cursors (and whenever the `graphql` role runs).
- **Split roles to scale independently:** one release `--set roles=ws`,
  another `--set roles=graphql` — point both at the same `nats.url` /
  stream.
- **Typed subscriptions** are authored in GraphQL SDL (`@vsSubscribe` /
  `@source`); see [Real-time subscriptions](/concepts/real-time-subscriptions).
