// End-to-end test of the Apollo-Client-compatible GraphQL gateway.
//
// Setup:
//   - VentStream engine running in `ws,graphql` mode (the ws role
//     bootstraps the JetStream stream the graphql role consumes from)
//   - NATS with JetStream enabled
//   - Subject manifest at packages/example/vs-subjects.yaml
//
// What this script does:
//   1. Opens an Apollo Client pointing at the gateway via
//      `GraphQLWsLink` (the standard Apollo subscription transport).
//   2. Runs an introspection-style query for `availableSubjects` to
//      prove HTTP queries work and the manifest is loaded.
//   3. Starts a subscription on a pattern.
//   4. Publishes three events via @ventstream/sdk.
//   5. Verifies all three arrive at the Apollo subscription callback.

import { ApolloClient, InMemoryCache, HttpLink, gql, split } from "@apollo/client/core/index.js";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions/index.js";
import { getMainDefinition } from "@apollo/client/utilities/index.js";
import { createClient } from "graphql-ws";
import WebSocket from "ws";

import { VentStream } from "@ventstream/sdk";

const HTTP_URL = process.env.VS_GRAPHQL_HTTP || "http://127.0.0.1:4041/graphql";
const WS_URL = process.env.VS_GRAPHQL_WS || "ws://127.0.0.1:4041/graphql/ws";
const NATS_URL = process.env.VS_NATS_URL || "nats://127.0.0.1:4222";

// Apollo Client setup — exactly what a developer would write.
// HTTP queries authenticate via headers; the WS link uses
// `connection_init` (see `connectionParams` below).
const httpLink = new HttpLink({
  uri: HTTP_URL,
  fetch,
  headers: {
    Authorization: "Bearer demo",
    "X-VS-Tenant": "acme",
  },
});

const wsLink = new GraphQLWsLink(
  createClient({
    url: WS_URL,
    webSocketImpl: WebSocket,
    connectionParams: () => ({
      authToken: "demo",
      tenant: "acme",
    }),
  }),
);

const link = split(
  ({ query }) => {
    const def = getMainDefinition(query);
    return def.kind === "OperationDefinition" && def.operation === "subscription";
  },
  wsLink,
  httpLink,
);

const apollo = new ApolloClient({ link, cache: new InMemoryCache() });

// 1. Discovery query — proves HTTP transport + manifest work.
// Note: async-graphql exposes fields in camelCase per GraphQL
// convention. Apollo's `graphql-codegen` would produce typed
// hooks from these names automatically.
console.log("\n— Querying availableSubjects —");
const discovery = await apollo.query({
  query: gql`
    query Discovery {
      availableSubjects {
        pattern
        description
        exampleEventType
      }
    }
  `,
});
for (const s of discovery.data.availableSubjects) {
  console.log(`  • ${s.pattern}  (${s.description ?? "no description"})`);
}

// 2. Subscription — proves WS transport + JetStream consumer path.
console.log("\n— Subscribing to orders.order.status_changed.* —");
const received = [];
const done = new Promise((resolve) => {
  let n = 0;
  const obs = apollo.subscribe({
    query: gql`
      subscription OnOrderStatus($pattern: String!) {
        events(subject: $pattern) {
          id
          event
          subject
          occurredAt
          entityId
          actor {
            kind
            id
          }
          data
        }
      }
    `,
    variables: { pattern: "orders.order.status_changed.*" },
  });
  obs.subscribe({
    next({ data }) {
      const ev = data?.events;
      if (!ev) return;
      received.push(ev);
      console.log(
        `  ← ${ev.subject}\n    entity_id=${ev.entityId}  data=${JSON.stringify(ev.data)}`,
      );
      n += 1;
      if (n >= 3) resolve();
    },
    error(err) {
      console.error("subscription error:", err);
    },
  });
});

// Give the subscription's WS upgrade + connection_init + consumer
// create a beat before publishing.
await new Promise((r) => setTimeout(r, 600));

// 3. Publish three events through the standard SDK.
console.log("\n— Publishing 3 events via @ventstream/sdk —");
const vs = new VentStream({ servers: NATS_URL, name: "apollo-demo" });
await vs.connect();
for (let i = 1; i <= 3; i++) {
  const { id, subject } = await vs.publish({
    tenant: "acme",
    domain: "orders",
    action: "status_changed",
    entity: { kind: "order", id: `order_${i}` },
    actor: { kind: "user", id: "user_456" },
    data: { from: "pending", to: "confirmed", attempt: i },
  });
  console.log(`  → id=${id} subject=${subject}`);
}

await done;
await vs.close();
await apollo.stop();
console.log(`\n[demo] received ${received.length} events via Apollo Client ✓`);
process.exit(0);
