// Minimal mock subgraph that owns the `Order` entity. Used for
// federation composition tests against VentStream's GraphQL
// gateway. The router will call this subgraph's `_entities`
// resolver to flesh out an Order when VentStream emits just the
// `{ id }` entity reference from a subscription.

import { ApolloServer } from "@apollo/server";
import { startStandaloneServer } from "@apollo/server/standalone";
import { buildSubgraphSchema } from "@apollo/subgraph";
import gql from "graphql-tag";

const typeDefs = gql`
  type Order @key(fields: "id") {
    id: ID!
    status: String!
    total_cents: Int!
    customer_email: String!
  }

  type Query {
    order(id: ID!): Order
    orders: [Order!]!
  }
`;

const FIXTURES = new Map([
  ["o-42", { id: "o-42", status: "confirmed", total_cents: 12_995, customer_email: "alice@example.com" }],
  ["o-99", { id: "o-99", status: "shipped",   total_cents: 25_000, customer_email: "bob@example.com" }],
]);

const resolvers = {
  Query: {
    order: (_, { id }) => FIXTURES.get(id) ?? null,
    orders: () => Array.from(FIXTURES.values()),
  },
  Order: {
    __resolveReference: ({ id }) => FIXTURES.get(id) ?? null,
  },
};

const server = new ApolloServer({
  schema: buildSubgraphSchema({ typeDefs, resolvers }),
});

const port = Number(process.env.PORT || 4042);
const { url } = await startStandaloneServer(server, {
  listen: { port },
});
console.log(`orders subgraph at ${url}`);
