# Local production soak

`run-long-lived.sh` is the production soak entry point. It gives PostgreSQL,
MySQL, MongoDB, Kafka, Neo4j, NATS GraphQL, Redis GraphQL, NATS raw sockets,
and Redis raw sockets an equal share of the requested duration. Each engine,
source, sink, and broker remains alive for its entire multi-hour block. This
avoids turning Docker container creation into the workload under test and fits
an 8 GiB Docker Desktop VM by running one topology at a time.

CDC batches reuse stable IDs, bounding source and OpenSearch storage while
still generating real changes. The harness injects alternating source/sink or
broker outages and periodically restarts the engine against its persisted
checkpoint. A batch passes only after exact delivery and destination counts
are verified. Realtime rounds require exact delivery with zero gaps and zero
duplicates. Failed blocks are retained and the next source still runs.

`run-soak.sh` remains available as a short matrix stress tool, but it is not a
valid 24-hour runtime soak because it recreates the topology for every phase.

Start a detached 24-hour run from the repository root. `screen` owns the
process independently of the launching shell, while `caffeinate` prevents the
Mac from sleeping during the run:

```sh
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR="$PWD/target/soak/$RUN_ID"
mkdir -p "$RUN_DIR"
screen -dmS ventstream-soak /bin/zsh -lc \
  "cd '$PWD' && exec caffeinate -disu env VS_SOAK_RUN_ID='$RUN_ID' \
  benchmarks/soak/run-long-lived.sh >'$RUN_DIR/supervisor.log' 2>&1"
```

Inspect the active run:

```sh
benchmarks/soak/status-soak.sh
```

Stop it gracefully using the PID reported by the status command:

```sh
kill -TERM "$(cat target/soak/latest)/soak.pid"
```

Results are written under `target/soak/<run-id>`. `events.jsonl` is the phase
and injection timeline, `resources.tsv` contains 30-second Docker samples,
and `phases/` contains complete logs. The runner stops before starting another
phase if host free space falls below 12 GiB.
