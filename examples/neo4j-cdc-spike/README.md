# neo4j-cdc-spike

Standalone validation that VentStream can pull change data capture from a real
Neo4j 5.x Enterprise instance. **Not part of the main workspace** — kept here as
a reference implementation while the production source plugin is being built.

## What it proves

- `neo4rs` over Bolt can call `db.cdc.current()` and `db.cdc.query(cursor)`.
- Cold start: capture cursor → paginate every node and relationship → emit
  synthetic insert events → persist cursor → start tail. No event loss across
  the snapshot/tail handoff.
- Warm start: skip bootstrap, resume from the persisted cursor.
- Idle cursor advance prevents transaction-log aging.
- Multi-event transactions (e.g. `DETACH DELETE`) preserve `txId` + `seq`
  ordering.

## Prerequisites

- Docker. Neo4j 5.x **Enterprise** (CDC is not available on Community or on
  Aura Free/Pro).
- The container in this repo's demo setup:

  ```bash
  docker run -d --name vs-neo4j \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
    -e NEO4J_AUTH=neo4j/ventstream-spike \
    -e NEO4J_dbms_security_procedures_unrestricted='db.cdc.*' \
    neo4j:5.26-enterprise
  ```

- Enable CDC on the default database:

  ```bash
  docker exec vs-neo4j cypher-shell -u neo4j -p ventstream-spike -d system \
    "ALTER DATABASE neo4j SET OPTION txLogEnrichment 'FULL';"
  ```

## Run

```bash
cd examples/neo4j-cdc-spike
cargo run --release
```

The spike runs for 30 seconds and exits. While it polls, make writes via
`cypher-shell` in another terminal to see events arrive.

State is kept in `./cursor.txt` (gitignored). Delete it to force a fresh
bootstrap.

## Known limitations of the spike (fix in the real source)

- Temporal types (`DateTime`, `Date`, `Time`, `Duration`) fall through to
  `Debug` formatting instead of ISO-8601 strings. The production source must
  convert them properly.
- Pagination uses `SKIP / LIMIT`, which is O(skip) per page on Neo4j. Real
  source will keyset-paginate by `elementId(n) > $last`.
- No selectors — the spike scans everything. The real source will accept a
  selectors YAML so users can filter by label / reltype / properties.
- No backpressure handoff to a downstream sink — the spike prints events.
- Single-database — no support for Neo4j multi-DB or causal clusters yet.
- Hard-coded connection. Real source will load from `VS_NEO4J_*` env vars.

## What this unblocks

The source-plugin work in `crates/ventstream-sources/src/neo4j/` can now lift
the polling loop, cursor management, and bootstrap pagination from this spike
mostly verbatim. The remaining unknowns are the integration points (Source
trait wiring, snapshot-complete sentinel, dispatcher backpressure) rather than
"does Neo4j CDC work from Rust at all."
