# Complex projection benchmark results - 2026-07-20

## Workloads

The engine was limited to 2 vCPUs and 1 GiB RAM. PostgreSQL, MySQL, and Neo4j
were each limited to 2 vCPUs. OpenSearch used 2 vCPUs, a 1 GiB JVM heap, no
replicas, and refresh disabled during ingestion. Documents carry a 256-byte
payload. Elapsed time covers source mutation through acknowledged sink writes;
the final refresh and correctness query are outside the timed region.

The relational fixture has four primary streams in one engine:

| Primary | Output index | Relationships |
|---|---|---|
| orders | `vsbench-complex-orders` | customer, product, region |
| shipments | `vsbench-complex-shipments` | customer, warehouse, carrier |
| invoices | `vsbench-complex-invoices` | customer, account, currency |
| support_cases | `vsbench-complex-support-cases` | customer, agent, support queue |

All 12 joins use indexed keys and SQL denormalization. The fan-out phase changes
10,000 customer rows in bounded 500-row transactions and verifies the updated
nested customer in every dependent document.

The MongoDB fixture tails orders, shipments, invoices, and support_cases from
one change stream and routes them to four indexes. The second phase updates all
documents with `full_document: update_lookup` and verifies the replacement
document in every index.

The Neo4j fixture runs four specs in one engine: Customer to Order to Product,
Seller to Listing to Category, Shipment to Carrier to Region, and SupportCase
to Agent to Team. Each Cypher projection has two hops and its own output index.
The initial graph has five CDC elements per final document: three nodes and two
relationships. The fan-out phase changes every second-hop node and verifies the
new nested value in OpenSearch.

## Final results

| Workload | Source changes | Sink writes | Elapsed | Throughput | Engine CPU mean / peak | Engine RSS HWM |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL, 4 primaries x 3 joins | 1,000,000 | 1,000,000 | 31.501 s | **31,745/s** | 42% / 51% | 159 MiB |
| PostgreSQL related-row fan-out | 10,000 | 1,000,000 | 23.658 s | **42,269/s** | 26% / 35% | 320 MiB |
| MySQL, 4 primaries x 3 joins | 200,000 | 200,000 | 26.725 s | **7,484/s** | 123% / 142% | 21 MiB |
| MySQL related-row fan-out | 10,000 | 200,000 | 4.708 s | **42,481/s** | 59% / 101% | 249 MiB |
| MongoDB, 4 collections | 1,000,000 | 1,000,000 | 10.544 s | **94,841/s** | 54% / 59% | 248 MiB |
| MongoDB `update_lookup`, 4 collections | 1,000,000 | 1,000,000 | 25.920 s | **38,580/s** | 24% / 38% | 248 MiB |
| Neo4j, 4 two-hop specs, throughput profile | 100,000 | 20,000 | 34.441 s | **581 docs/s** | 5% / 8% | 58 MiB |
| Neo4j second-hop fan-out, maximum profile | 20,000 | 20,000 | 3.995 s | **5,006/s** | 6% / 11% | 103 MiB |

Every final count was exact. PostgreSQL and MySQL produced four correctly
routed indexes with all three nested relationships. MongoDB produced four
correctly routed indexes and applied all replacement documents. Neo4j produced
all four two-hop document shapes. Final engine logs contained no warning or
error.

## Tuning conclusions

- PostgreSQL `maximum` reached 31,745/s. The smaller `throughput` profile
  reached 30,651/s at the same 400,000-document comparison scale, so it is the
  lower-memory operational choice when the last 3.5% is not material.
- MySQL `maximum` is the clear choice here: 7,253/s versus 6,294/s balanced at
  the 100,000-document comparison scale. MySQL consumed nearly two source CPUs,
  while the engine remained bounded and below 142% CPU.
- MongoDB `maximum` sustained 94,841 inserts/s and 38,580 update lookups/s. The
  engine process stayed below 249 MiB RSS. MongoDB reached about 948 MiB of its
  2 GiB container during insertion, so source sizing matters more than engine
  sizing for this fixture.
- Neo4j `throughput` produced new two-hop documents slightly faster than
  `maximum` at equal 20,000-document scale: 581/s versus 561/s. `maximum` was
  better for second-hop fan-out: 5,006/s versus 4,163/s.
- Neo4j primary materialization is database/query bound. The engine averaged
  only 5% CPU while Neo4j averaged about 176% CPU. Larger engine queues do not
  solve that boundary; graph shape, indexes, Neo4j sizing, and partitioning do.
- Reverse fan-out is faster than initial materialization because the SQL and
  Neo4j paths coalesce affected IDs and recompose them in bounded batches.

These are local container results, not universal production guarantees. Cloud
sizing must rerun the same fixtures against representative managed databases,
network latency, mappings, payloads, skew, and relationship fan-out ratios.

Raw CSVs and samples:

- `target/benchmarks/container-matrix/complex-calibration`
- `target/benchmarks/container-matrix/complex-sustained-postgres`
- `target/benchmarks/container-matrix/complex-sustained-mysql`
- `target/benchmarks/container-matrix/complex-sustained-neo4j`
- `target/benchmarks/container-matrix/complex-final-postgres`
- `target/benchmarks/container-matrix/complex-final-mysql`
- `target/benchmarks/container-matrix/complex-mongodb-calibration`
- `target/benchmarks/container-matrix/complex-mongodb-sustained`
- `target/benchmarks/container-matrix/complex-final-neo4j`
- `target/benchmarks/container-matrix/complex-final-neo4j-throughput`
