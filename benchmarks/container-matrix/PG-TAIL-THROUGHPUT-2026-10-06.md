# Postgres tail throughput regression (#202) — 2026-10-06

Found during the 2026-10-05 verification sweep: the SQL-denormalize tail
benchmark (`run-sources.sh postgres`, 1,000,000 rows, 1 KiB payload, engine
2 vCPU / 512 MiB, throughput profile, OpenSearch sink) ran at roughly half
of its July rate while every other source was within 7% of baseline.

All numbers below are same-machine, back-to-back runs (a laptop whose
absolute throughput drifted up to 2x over the night — only compare within a
table). Every run verified 1,000,000 documents.

## Bisect

| Build | Throughput | Engine cgroup peak |
|---|---|---|
| v0.1.18 (Jul 30) | 37,879/s | 195 MiB |
| v0.1.19 … v0.1.43, main | 8,300–17,700/s | 64–76 MiB |

v0.1.19 is the first slow release; it contains one substantive commit
(`315e7fc`, the Redis sink), which changed two things on the Postgres path:

1. the source started buffering each whole transaction until COMMIT (with a
   disk spool past 8 MiB) and decoding every payload twice;
2. the SQL denormalizer started **blocking after every tail batch** until
   the sink confirmed that batch durable, before recomposing the next one.

## Isolating the two

The benchmark loads its rows as ONE ~1 GB transaction. A variant commits
every 1,000 rows (≈1 MiB each, never spilling) separates spill cost from the
rest:

| Build | One 1M-row transaction | 1,000-row transactions |
|---|---|---|
| main (303de13) | 15,720/s | 27,198/s |
| + stage A: decode once, buffered spool, configurable budget | 15,515/s | 27,650/s |
| + stage B: asynchronous durability barriers | **20,187/s** | **49,520/s** |
| v0.1.18 | 30,963/s | 52,815/s |

Stage A alone was worth ~7%: the spool was never the main cost. Stage B
(pipelining recomposition against sink confirmation, bounded at 8 in-flight
barriers, same checkpoint invariant) recovered the ordinary-transaction
case to within 6% of v0.1.18 and lifted engine CPU from 35% back to 64% —
the engine had been idle, waiting on the sink, half of the time.

## What remains

With one giant transaction, main+A+B is still ~35% below v0.1.18. That is
the receive-whole-transaction-then-replay shape: the downstream pipeline is
idle while the transaction streams in, then busy while it is replayed,
where v0.1.18 overlapped the two. The exactly-one-commit-LSN stamp that the
buffering serves needs only ONE record held back, not the whole
transaction; streaming with a one-record hold-back is the follow-up.
