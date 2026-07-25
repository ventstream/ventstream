// End-to-end demo of Model 2 + Federation Flavor B:
//
//   - Engine running with VS_GRAPHQL_SUBSCRIPTIONS=vs-subscriptions.yaml
//   - Apollo Client subscribes to the typed `orderStatusChanged`
//     field declared in the manifest (NOT the generic events()).
//   - @ventstream/sdk publishes events.
//   - Apollo receives properly-typed payloads — Apollo's codegen
//     would produce a hook that returns
//     `{ orderId: string; from: string; to: string; changedAt: string }`.
//
// Plus: introspect the schema to confirm the `Order @key(fields:"id")`
// federation directive is in the published SDL.

import { ApolloClient, InMemoryCache, HttpLink, gql, split } from "@apollo/client/core/index.js";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions/index.js";
import { getMainDefinition } from "@apollo/client/utilities/index.js";
import { createClient } from "graphql-ws";
import WebSocket from "ws";

import { VentStream } from "@ventstream/sdk";

const HTTP_URL = "http://127.0.0.1:4041/graphql";
const WS_URL = "ws://127.0.0.1:4041/graphql/ws";
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

// 1. Federation SDL — proves we're a valid federated subgraph.
console.log("— Federation _service { sdl } excerpt —");
const sdl = await apollo.query({
  query: gql`
    query Service {
      _service {
        sdl
      }
    }
  `,
});
const sdlText = sdl.data._service.sdl;
const orderLine = sdlText.split("\n").find((l) => l.includes("Order"));
const subscriptionTypeBlock = sdlText.substring(
  sdlText.indexOf("type SubscriptionRoot"),
);
console.log("  Order entity:    ", orderLine?.trim());
console.log("  Subscription:\n" + (subscriptionTypeBlock.substring(0, 280).split("\n").map((l) => "    " + l).join("\n") || "<none>"));

// 2. Typed subscription — proves Model 2 works with Apollo.
console.log("\n— Subscribing to orderStatusChanged(orderId: \"o-42\") —");
const received = [];
const done = new Promise((resolve) => {
  let n = 0;
  const obs = apollo.subscribe({
    query: gql`
      subscription OnStatus($id: ID!) {
        orderStatusChanged(orderId: $id) {
          orderId
          from
          to
          changedAt
        }
      }
    `,
    variables: { id: "o-42" },
  });
  obs.subscribe({
    next({ data }) {
      const ev = data?.orderStatusChanged;
      if (!ev) return;
      received.push(ev);
      console.log(
        `  ← orderId=${ev.orderId} from=${ev.from} to=${ev.to} changedAt=${ev.changedAt}`,
      );
      n += 1;
      if (n >= 2) resolve();
    },
    error(err) {
      console.error("subscription error:", err);
    },
  });
});

await new Promise((r) => setTimeout(r, 600));

// 3. Publish events.
console.log("\n— Publishing 2 status changes for o-42 —");
const vs = new VentStream({ servers: NATS_URL, name: "typed-demo" });
await vs.connect();
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "status_changed",
  entity: { kind: "order", id: "o-42" },
  actor: { kind: "user", id: "u1" },
  data: { from: "pending", to: "confirmed" },
});
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "status_changed",
  entity: { kind: "order", id: "o-42" },
  actor: { kind: "user", id: "u1" },
  data: { from: "confirmed", to: "shipped" },
});
// Also publish one for a DIFFERENT order — should NOT be delivered
// because the subscription's subject template is anchored to orderId=o-42.
await vs.publish({
  tenant: "acme",
  domain: "orders",
  action: "status_changed",
  entity: { kind: "order", id: "o-99" },
  actor: { kind: "user", id: "u1" },
  data: { from: "x", to: "y" },
});
console.log("  → 3 publishes (2 for o-42, 1 for o-99 that should be filtered out)");

await done;
await vs.close();
await apollo.stop();
console.log(
  `\n[demo] received ${received.length} typed events (expected: 2 for o-42 only) ✓`,
);
process.exit(received.length === 2 ? 0 : 1);
