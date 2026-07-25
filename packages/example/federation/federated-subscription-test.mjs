// FEDERATED subscription test — the v1.5 closing-the-loop test.
//
// Flow:
//   client → Cosmo Router (ws://:4044/graphql)
//     → router routes subscription op to VentStream
//     → VentStream creates JetStream consumer for the args-resolved subject
//   publisher → NATS → VentStream → router → client
//
// The subscription operation asks for `Order { id status total_cents
// customer_email }`. VentStream only owns `id` (entity_ref kind);
// the router fetches `status`/`total_cents`/`customer_email` from
// the orders subgraph via `_entities` after the event arrives.
//
// This proves Federation Flavor B: VentStream is a subscription-
// owning subgraph and the router stitches the rest from the entity-
// owning subgraph at delivery time.

import { ApolloClient, InMemoryCache, HttpLink, gql, split } from "@apollo/client/core/index.js";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions/index.js";
import { getMainDefinition } from "@apollo/client/utilities/index.js";
import { createClient } from "graphql-ws";
import WebSocket from "ws";

import { VentStream } from "@ventstream/sdk";

const HTTP_URL = "http://localhost:4044/graphql";
const WS_URL = "ws://localhost:4044/graphql";
const NATS_URL = "nats://127.0.0.1:4222";

const httpLink = new HttpLink({
  uri: HTTP_URL,
  fetch,
  headers: { Authorization: "Bearer demo", "X-VS-Tenant": "acme" },
});

const wsLink = new GraphQLWsLink(
  createClient({
    url: WS_URL,
    webSocketImpl: WebSocket,
    connectionParams: () => ({ authToken: "demo", tenant: "acme" }),
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

// 1. Confirm composed schema reachability via the router.
console.log("— Federation supergraph fields (via Cosmo Router) —");
const probe = await apollo.query({
  query: gql`
    {
      health {
        status
        tenant
      }
      orders {
        id
        status
      }
    }
  `,
});
console.log("  health:", JSON.stringify(probe.data.health));
console.log("  orders subgraph reachable:", probe.data.orders.length, "orders");

// 2. Federated subscription — the router routes to VentStream and stitches.
console.log(
  "\n— Subscribing to orderUpdated(orderId: \"o-42\") with FEDERATED fields —",
);
const received = [];
const done = new Promise((resolve) => {
  const obs = apollo.subscribe({
    query: gql`
      subscription OnOrder($id: ID!) {
        orderUpdated(orderId: $id) {
          id
          status
          total_cents
          customer_email
        }
      }
    `,
    variables: { id: "o-42" },
  });
  obs.subscribe({
    next({ data }) {
      const o = data?.orderUpdated;
      if (!o) return;
      received.push(o);
      console.log(
        `  ← FEDERATED order: id=${o.id} status=${o.status} total_cents=${o.total_cents} email=${o.customer_email}`,
      );
      if (received.length >= 1) resolve();
    },
    error(err) {
      console.error("subscription error:", err);
    },
  });
});

// Give the WS handshake + consumer create a beat.
await new Promise((r) => setTimeout(r, 800));

// 3. Publish via @ventstream/sdk. VentStream receives the event,
//    emits an entity-ref { id: "o-42" }, router fetches the rest
//    from the orders subgraph and stitches into a single payload.
console.log("\n— Publishing one event for o-42 —");
const vs = new VentStream({ servers: NATS_URL, name: "fed-demo" });
await vs.connect();
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "updated",
  entity: { kind: "order", id: "o-42" },
  actor: { kind: "user", id: "u1" },
  data: { reason: "addr-fix" },
});
console.log("  → published");

await done;
await vs.close();
await apollo.stop();
console.log(
  `\n[demo] received ${received.length} federated event(s) with stitched Order fields ✓`,
);
process.exit(received.length >= 1 ? 0 : 1);
