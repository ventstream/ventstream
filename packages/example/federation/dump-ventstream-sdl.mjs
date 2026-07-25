// Render VentStream's full subgraph SDL — including the Subscription
// type — from regular GraphQL introspection.
//
// Why: async-graphql's `_service { sdl }` output strips subscription
// types (legacy federation v1 behavior). For federation v2.4+ where
// subscriptions are supported in composition, we need the full
// schema. We reconstruct it from regular `__schema { ... }`
// introspection and stamp the federation `@link` extension at the
// top so rover / wgc recognize this as a federated subgraph.

import { getIntrospectionQuery, buildClientSchema, printSchema } from "graphql";
import { writeFileSync } from "node:fs";

const url = process.env.VS_GRAPHQL_HTTP || "http://127.0.0.1:4041/graphql";
const out = process.argv[2] || "ventstream.graphql";

// Auth header is required for any tenant-scoped fields, but
// introspection is open.
const res = await fetch(url, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query: getIntrospectionQuery() }),
});
const body = await res.json();
if (body.errors) {
  console.error(JSON.stringify(body.errors, null, 2));
  process.exit(1);
}

const schema = buildClientSchema(body.data);
let sdl = printSchema(schema);

// Stamp the federation link extension. Order matters — `extend schema`
// has to come before any `type` declarations in some composers. We
// prepend it.
const federationLink = `extend schema @link(
  url: "https://specs.apollo.dev/federation/v2.5"
  import: ["@key", "@shareable", "@external", "@provides", "@requires"]
)`;
sdl = federationLink + "\n\n" + sdl;

// Re-stamp the @key directive on the Order entity. printSchema
// doesn't preserve federation directives because they're not in
// the standard introspection result; we add it back so composition
// recognizes Order as an entity owned by both subgraphs.
sdl = sdl.replace(/^type Order \{/m, 'type Order @key(fields: "id") {');

writeFileSync(out, sdl);
console.log(`wrote ${out} (${sdl.length} bytes)`);
