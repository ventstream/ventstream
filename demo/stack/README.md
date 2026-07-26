# VentStream demo — copy-paste runbook

A self-contained stack that streams changes from **Postgres** and
**Neo4j** into **OpenSearch** as denormalized documents, in real time.
Everything runs in Docker. No control plane, no cloud, no credentials to
provision — copy the commands top to bottom.

The data is a small **e-commerce** dataset (orders + product catalog) so
the scenarios are easy to follow.

```
Postgres  shop.orders + customers + order_items  ─┐
                                                   ├─►  OpenSearch
Neo4j     Product → Category / Supplier → Region ─┘     (orders, products)
```

| Source | Target | What one document looks like |
|--------|--------|------------------------------|
| Postgres `shop.orders` | OS index `orders` | order + embedded customer (1:1) + embedded line items (1:many) |
| Neo4j `Product` | OS index `products` | product + category + supplier→region (2 hops) + tags |

---

## 0. Prerequisites

- Docker + Docker Compose v2 (`docker compose version`)
- ~4 GB free RAM for the containers
- Ports free: `5544` (pg), `7474`/`7687` (neo4j), `9200` (opensearch), `5601` (dashboards, optional)

All commands run from this directory:

```bash
cd ventstream/demo/stack
```

---

## Preflight

Reset this demo's containers and volumes so the counts in this runbook are
deterministic:

```bash
docker compose --profile dashboards down -v --remove-orphans
```

Port `9200` must also be unused before the demo starts:

```bash
docker ps --filter publish=9200 --format 'table {{.Names}}\t{{.Ports}}'
lsof -nP -iTCP:9200 -sTCP:LISTEN
```

Both checks should return no listener. Docker Desktop can publish `9200` even
when a local OpenSearch process already owns `localhost:9200`; in that case,
the verification requests below reach the wrong cluster.

---

## 1. Start the sources + target

`--wait` blocks until all three pass their healthchecks (works the same
on macOS and Linux — no `watch` needed):

```bash
docker compose up -d --wait postgres neo4j opensearch
```

Check status any time with `docker compose ps`.

> First boot pulls images (Postgres, Neo4j Enterprise, OpenSearch) — give
> it a minute. Postgres seeds itself from `seed/postgres.sql` on first
> boot (200 orders, 5 customers, line items, and the `ventstream_shop`
> publication).

---

## 2. Seed Neo4j + enable CDC

Postgres is already seeded. Neo4j needs two one-time commands — enable CDC
on the database, then load the catalog graph:

```bash
# Enable CDC (DIFF enrichment) on the default database.
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'DIFF';"

# Load the product catalog (2,000 products, 4 categories, 20 suppliers, 3 regions).
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  < seed/neo4j.cypher
```

Verify the seed:

```bash
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "MATCH (p:Product) RETURN count(p) AS products;"
# products = 2000
```

---

## 3. Start the engines (with verbose logs)

The engines build from source on first run via cargo-chef — **the first
build takes a few minutes**, subsequent runs are cached and start in
seconds.

```bash
docker compose up -d --build engine-orders engine-products
```

Watch them bootstrap and tail. The demo enables debug logging so source
progress, projection work, and sink batches are visible:

```bash
# Postgres → OpenSearch
docker compose logs -f engine-orders
```

```bash
# Neo4j → OpenSearch  (in another terminal)
docker compose logs -f engine-products
```

You'll see each engine: connect → snapshot-bootstrap the existing rows →
switch to tailing CDC. Look for `phase=tailing` (or "tailing changes").

---

## 4. Verify the initial load reached OpenSearch

```bash
# Orders: should be 200
curl -s 'http://localhost:9200/orders/_count' | jq .count

# Products: should be 2000
curl -s 'http://localhost:9200/products/_count' | jq .count
```

Inspect one denormalized order — note the embedded `customer` and `items`:

```bash
curl -s 'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0001%22%5D' | jq '._source'
```

Inspect one denormalized product (Neo4j source) — note the embedded
`category`, `supplier` → `region` (2 hops), and `tags`:

```bash
curl -s 'http://localhost:9200/products/_search' -H 'content-type: application/json' -d '
  {"size":1,"query":{"term":{"id.keyword":"prod-2"}}}' | jq '.hits.hits[0]._source'
```

> The Postgres doc `_id` is the fully-qualified table name plus the PK
> as a JSON array: `shop.orders:["ord-0001"]` (URL-encoded above).
> The Neo4j doc `_id` is `products_denormalized:<elementId>` — search by
> a field instead of guessing the elementId.

---

## 5. Watch changes propagate (the demo)

Keep `docker compose logs -f engine-orders` (and `engine-products`)
visible while you run these. Each change should appear in the engine log
within ~1s and update OpenSearch.

### 5a. Postgres — update a row (1-hop)

```bash
# Change an order's status. The `orders` document updates.
docker compose exec -T postgres psql -U ventstream -d shop -c \
  "UPDATE shop.orders SET status='shipped', total=999.99 WHERE order_id='ord-0001';"

curl -s 'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0001%22%5D' \
  | jq '._source | {status, total}'
# → { "status": "shipped", "total": 999.99 }
```

### 5b. Postgres — update an embedded parent (1:1 cascade)

```bash
# Rename a customer. EVERY order for cust-002 recomposes with the new name.
# (ord-0001 belongs to cust-002.)
docker compose exec -T postgres psql -U ventstream -d shop -c \
  "UPDATE shop.customers SET tier='platinum', name='Alan T. (VIP)' WHERE customer_id='cust-002';"

curl -s 'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0001%22%5D' \
  | jq '._source.customer'
# → name + tier reflect the change
```

### 5c. Postgres — add a line item (1:many cascade)

```bash
docker compose exec -T postgres psql -U ventstream -d shop -c \
  "INSERT INTO shop.order_items (item_id, order_id, sku, qty, price)
   VALUES ('item-0001-new', 'ord-0001', 'SKU-9999', 5, 49.99);"

curl -s 'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0001%22%5D' \
  | jq '._source.items'
# → array now includes SKU-9999
```

### 5d. Postgres — delete an order

```bash
# Delete the order's line items first (FK constraint), then the order.
docker compose exec -T postgres psql -U ventstream -d shop -c \
  "DELETE FROM shop.order_items WHERE order_id='ord-0001';
   DELETE FROM shop.orders WHERE order_id='ord-0001';"

# The CDC delete event removes the document immediately (a GET by _id is
# real-time; _count lags ~1s behind on OpenSearch's refresh interval).
curl -s -o /dev/null -w '%{http_code}\n' \
  'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0001%22%5D'
# → 404  (document removed)
```

### 5e. Neo4j — rename a hot shared node (bounded fan-out)

`Category` is a low-cardinality shared lookup (4 categories, ~500
products each). A **property change** on the node — renaming it —
correctly cascades to every product in that category. Hot-endpoint
detection only filters *relationship* churn on shared nodes (which would
explode the fan-out); a genuine rename of the embedded value still
propagates. Watch `engine-products` logs recompose ~500 docs.

```bash
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "MATCH (c:Category {id:'cat-electronics'}) SET c.name='Electronics & Gadgets';"

# Confirm all ~500 electronics products carry the new name.
# (Use the .keyword subfield for an exact match — a plain `match` query
#  would also hit the old "Electronics" via the shared token.)
curl -s 'http://localhost:9200/products/_search' -H 'content-type: application/json' -d '
  {"size":0,
   "query":{"term":{"category.id.keyword":"cat-electronics"}},
   "aggs":{"names":{"terms":{"field":"category.name.keyword"}}}}' \
  | jq '.aggregations.names.buckets'
# → [ { "key": "Electronics & Gadgets", "doc_count": 500 } ]
```

### 5f. Neo4j — multi-hop cascade (Supplier → Region)

```bash
# Move supplier 1 to Asia Pacific. Every product of sup-1 recomposes 2 hops out.
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "MATCH (s:Supplier {id:'sup-1'})-[r:LOCATED_IN]->() DELETE r
   WITH s MATCH (reg:Region {id:'reg-apac'}) CREATE (s)-[:LOCATED_IN]->(reg);"

curl -s 'http://localhost:9200/products/_search' -H 'content-type: application/json' -d '
  {"size":1,"query":{"term":{"supplier.id.keyword":"sup-1"}}}' \
  | jq '.hits.hits[0]._source.supplier'
# → region.name = "Asia Pacific"
# (term on .keyword is exact; a `match` on "sup-1" would also hit sup-10, sup-11, …)
```

### 5g. Neo4j — delete a product

```bash
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "MATCH (p:Product {id:'prod-1'}) DETACH DELETE p;"

curl -s 'http://localhost:9200/products/_count' | jq .count
# → 1999
```

### 5h. Stream it continuously (optional)

To *see* the pipeline move, fire updates fast and watch the engine react.
We stream the statements into a **single** psql connection (a per-command
`docker exec` would cap the rate at the container-exec overhead) and pace
server-side with `pg_sleep`:

```bash
# terminal 1 — one update roughly every 100 ms over a single connection
while true; do
  printf "UPDATE shop.orders SET status=(ARRAY['pending','paid','shipped','delivered'])[1+floor(random()*4)], total=round((random()*1000)::numeric,2) WHERE order_id='ord-0002';\nSELECT pg_sleep(0.1);\n"
done | docker compose exec -T postgres psql -U ventstream -d shop -q
```

```bash
# terminal 2 — watch the engine: flush → ack → cursor advance
docker compose logs -f engine-orders
```

Or watch the document change in lockstep:

```bash
while true; do
  curl -s 'http://localhost:9200/orders/_doc/shop.orders:%5B%22ord-0002%22%5D' \
    | jq -c '._source | {status, total}'
  sleep 0.2
done
```

Ctrl-C to stop. Tune the pace with `pg_sleep(0.1)` — `0.02` for ~20 ms,
or drop it for max throughput. Each update recomputes only that one document;
the dispatcher may combine nearby updates into one sink batch. Swap the `WHERE` to
`order_id='ord-'||lpad((1+floor(random()*200))::int::text,4,'0')` to
spread updates across all 200 orders.

---

## 6. Optional — inspection UIs

### OpenSearch Dashboards

```bash
docker compose --profile dashboards up -d dashboards
```

Open **http://localhost:5601** → *Dev Tools* and run:

```
GET orders/_search
GET products/_search
```

Or create index patterns `orders*` / `products*` under *Discover* to
browse documents visually.

### Neo4j Browser

Always on at **http://localhost:7474** (user `neo4j`, password
`ventstream`). Explore the source graph:

```cypher
MATCH (p:Product)-[:IN_CATEGORY]->(c:Category) RETURN p, c LIMIT 25
```

---

## 7. Data-flow verification cheatsheet

```bash
# Source counts
docker compose exec -T postgres psql -U ventstream -d shop -tc \
  "SELECT count(*) FROM shop.orders;"
docker compose exec -T neo4j cypher-shell -u neo4j -p ventstream \
  "MATCH (p:Product) RETURN count(p);"

# Target counts (should match, minus anything you deleted)
curl -s localhost:9200/orders/_count   | jq .count
curl -s localhost:9200/products/_count | jq .count

# Is the Postgres replication slot active?
docker compose exec -T postgres psql -U ventstream -d shop -c \
  "SELECT slot_name, active FROM pg_replication_slots;"

# Any dead-lettered events? This should print nothing.
docker compose logs engine-orders engine-products \
  | grep 'metric="dlq.write"' || true
```

---

## 8. Reset / teardown

```bash
# Keep your data — stop + remove containers/networks but KEEP volumes.
# Fast re-run; engines resume from cursor/state, sources keep data:
docker compose --profile dashboards down --remove-orphans

# Full reset — also drop Postgres data, Neo4j store, and engine state:
docker compose --profile dashboards down -v --remove-orphans
```

> Omit `-v` to keep your volumes (the next `up` resumes); add `-v` for a
> fresh slate that re-seeds from scratch. `--remove-orphans` clears any
> leftover containers from old runs so a re-run doesn't hit name/port
> clashes.

---

## Notes

- **No managed control plane here.** These engines run standalone. A
  standalone process cannot be attached to VentStream Cloud in place; deploy
  a managed agent when you need centralized configuration and lifecycle
  operations.
- **Security disabled for the demo.** OpenSearch runs with the security
  plugin off and Neo4j uses a trivial password. Never copy these settings
  to a real deployment.
- **Specs** live in `specs/orders.yaml` (Postgres joins) and
  `specs/products.yaml` (Neo4j projection). Edit them and the engines
  re-sync on restart (`VS_PG_AUTO_RESYNC_ON_YAML_CHANGE=true`).
