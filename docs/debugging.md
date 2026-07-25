# Debugging VentStream

Practical reference for what to enable and what to grep when something
isn't behaving. All the knobs here are runtime env vars — no rebuild
required.

## Log format

| Env var | Values | Default | What |
|---|---|---|---|
| `RUST_LOG` | `tracing-subscriber` filter directives | `info` | Controls which log targets / levels emit |
| `VS_LOG_FORMAT` | `pretty` / `json` | `pretty` | Human-readable terminal output vs structured JSON for Datadog / Loki / CloudWatch |

JSON mode surfaces every structured field (the `key=value` parts of
every log call) as a top-level JSON property. Queries like
`metric:"neo4j.tail.recomposed" AND recomposed:>1000` work directly.

## Useful filter recipes

These are paste-friendly `RUST_LOG` strings.

### "Just tell me what's happening" (default)
```
RUST_LOG=info
```

### "Why are events slow to land?"
```
RUST_LOG=info,ventstream_sources::neo4j::source=debug,ventstream::dispatcher=debug
```
Surfaces: every `db.cdc.query` call's row count + elapsed; every bulk
flush's batch size + bytes + sink-ack latency; every fan-out cypher's
per-spec elapsed.

### "Is the cursor advancing?"
```
RUST_LOG=info,ventstream_sources::neo4j::source=debug,ventstream_sources::mysql::source=debug,ventstream::dispatcher=debug
```
Look for:
- `metric="neo4j.poll.cypher"` — every poll's row count + elapsed
- `"neo4j cursor drain"` — sink_progress vs pending front (whether the
  gate is releasing cursors)
- `"neo4j tail heartbeat (30s window)"` — running idle_polls count
- For MySQL, pair dispatcher `bulk.ack` entries with the cursor file in
  `VS_MYSQL_STATE_DIR`. The file advances only through the contiguous
  sink-confirmed acknowledgement-barrier prefix; internal barriers are never
  sent to the configured sink.

### "Is the bus full / source backpressured?"
```
RUST_LOG=info,ventstream_core::bus=debug
```
Look for:
- `metric="bus.backpressure"` — bus full at send time (source had to await)
- `metric="bus.backpressure.cleared"` — how long the wait was (`waited_ms`)

If you see frequent `backpressure` events with `waited_ms > 100`, the
dispatcher / sink is too slow for the source's emit rate. Tune
`VS_DISPATCH_PARALLEL_BULKS` higher, or look at the sink's bulk
latency.

### "Why is a specific Cypher slow?"
```
RUST_LOG=info,ventstream_sources::neo4j::denormalize=debug
```
Look for:
- `metric="neo4j.denormalize.fan_out_cypher"` — per-spec per-event
  cypher elapsed (`cypher_elapsed_ms`, `eid_count`)
- `metric="denormalize.tail.recomposed"` — actual rows recomposed +
  end-to-end elapsed per event

### "Did this event get dropped?"
```
RUST_LOG=info,ventstream_sources::neo4j::denormalize=debug,ventstream::dispatcher=debug
```
Look for:
- `"skipping event with no elementId"` (rare — defensive)
- DLQ logs from dispatcher (`"sink rejected some events"`)
- Per-event `"handling event"` breadcrumb with tx_id

### "Sink writes look slow"
```
RUST_LOG=info,ventstream::dispatcher=debug,ventstream_sinks::opensearch=debug
```
Look for `metric="dispatcher.bulk.ack"` — the `elapsed_ms` is wall-clock
from the dispatcher's perspective (bulk request → ack). If this is high,
the OS cluster is slow; if it's low but events still don't land,
investigate the sink-side.

### "Why is the PG replication slot not advancing?" (PG only)
```
RUST_LOG=info,ventstream_sources::postgres=debug
```
Look for:
- `metric="pg.replication.lsn_advance"` — every time the slot moves
  forward. Fields: `advance_to`, `last_acked`, `wal_high_water`. If
  this stops firing, the source is stuck.
- `metric="pg.replication.txn_commit"` — bumps `wal_high_water`. If
  commits flow but lsn_advance doesn't, the sink-progress watermark
  is the bottleneck.
- `metric="pg.replication.keepalive"` — Postgres still reachable
  (every ~10s by default). Absence = connection dropped.

### "Did this PG row get emitted?" (PG only)
```
RUST_LOG=info,ventstream_sources::postgres=debug,ventstream::dispatcher=debug
```
Look for:
- `metric="pg.replication.xlog_data"` with `events_in_payload>0` —
  the WAL message produced N events
- `metric="dispatcher.bulk.ack"` with corresponding LSN values via
  the `max_watermark` field — confirms the events made it through

### "Why is a realtime subscription connected but not delivering?"
```
RUST_LOG=info,ventstream_ws=debug,ventstream_graphql=debug,ventstream_jetstream=debug,ventstream_redis=debug
```
Look for:
- `consumer pump: pull stream error; re-establishing` — a per-connection
  JetStream pull failed and is recovering in place.
- `Redis Streams tail read failed; retrying` — the shared tenant tailer lost
  its blocking read and is reconnecting with bounded backoff.
- `Redis Stream retention failed` — gateway retention could not run. Alert if
  publishers do not apply `MAXLEN`, because the stream can continue growing.
- `pull stream unrecoverable` — retries were exhausted. Native WebSocket
  closes with an error; GraphQL terminates every affected operation with an
  error. Neither path is allowed to leave an active operation silently parked.
- `JetStream acknowledgement failed` — the event remains eligible for
  redelivery; clients must deduplicate by event id.
- `subscription lagged past the broadcast buffer` / `slow consumer` — the
  client could not drain its bounded queue and must reconnect from its cursor.
- `resume cursor expired` / `resume cursor is ahead` — replay was rejected
  explicitly instead of falling back to live-only delivery.

For a short, per-event investigation, add the two hot paths at `trace`:
```
RUST_LOG=info,ventstream_ws::jetstream=trace,ventstream_ws::connection=trace,ventstream_graphql::conn_source=trace
```
`trace` includes event id and provider cursor but never the event payload or
authentication token. Do not leave per-event tracing enabled for sustained
high-volume production traffic.

### CDC delivery Prometheus signals

| Metric | Meaning |
|---|---|
| `vs_events_emitted_total` | Source events admitted to the internal bus |
| `vs_events_received_total` | Events accepted by the dispatcher for sink delivery |
| `vs_events_delivered_total` | Events successfully acknowledged by the sink |
| `vs_events_failed_total` | Events rejected or failed during sink delivery |
| `vs_sink_retries_total` | Event-level sink retry attempts |
| `vs_bus_depth` / `vs_bus_capacity` | Current internal queue occupancy and capacity |
| `vs_backpressure_events_total` | Source sends that encountered a full queue |
| `vs_memory_budget_bytes` / `vs_memory_reserved_bytes` | Event-memory ceiling and bytes retained by live events |
| `vs_memory_pressure_state` | Adaptive state: 0 normal, 1 constrained, 2 high, 3 critical |
| `vs_memory_cgroup_current_bytes` / `vs_memory_cgroup_limit_bytes` | Container working set and hard limit observed by the controller |
| `vs_memory_process_rss_bytes` | Engine process resident set size |
| `vs_memory_throttle_total` | Event admissions delayed by the byte budget |
| `vs_memory_oversized_events_total` | Events rejected above the individual-event ceiling |
| `vs_bulk_write_p50_ms` / `vs_bulk_write_p95_ms` | Percentiles over the latest 256 acknowledged bulk writes |
| `vs_last_input_at_unixtime_ms` | Last event presented to the sink, as Unix milliseconds |
| `vs_last_output_at_unixtime_ms` | Last successful sink acknowledgement, as Unix milliseconds |

Compare received and delivered rates with queue depth, retries, and sink p95.
An idle source can legitimately report zero rates; do not alert on zero traffic
alone.

### Realtime Prometheus signals

| Metric | Meaning |
|---|---|
| `vs_realtime_connections_active{transport}` | Admitted live connections |
| `vs_realtime_connections_total{transport,result}` | Accepted connections |
| `vs_realtime_operations_active{transport="graphql"}` | Active GraphQL subscription operations |
| `vs_realtime_broker_messages_total{transport,provider}` | Broker deliveries consumed by connection sessions |
| `vs_realtime_events_enqueued_total{transport,provider}` | Valid events admitted to the connection/operation queue |
| `vs_realtime_events_emitted_total{transport="graphql",provider}` | Events emitted to matching GraphQL operations |
| `vs_realtime_events_dropped_total{transport,provider,reason}` | Malformed or invalid broker events deliberately rejected |
| `vs_realtime_frames_written_total{transport="ws"}` | Native event frames written to the socket sink |
| `vs_realtime_pull_restarts_total{transport,provider,reason}` | Pull flows re-established after broker interruption |
| `vs_realtime_terminal_failures_total{transport,provider,reason}` | Pumps that could not recover or failed unexpectedly |
| `vs_realtime_ack_failures_total{transport,provider}` | Provider acceptance or acknowledgement failures |
| `vs_realtime_slow_consumers_total{transport,provider}` | Connections/operations terminated for bounded-buffer lag |
| `vs_realtime_resume_attempts_total{transport,provider,result}` | Accepted, invalid, expired, or ahead resume attempts |
| `vs_realtime_broker_sessions_total{provider,resumed}` | Redis broker sessions opened, split by live-only or resumed |
| `vs_realtime_broker_ingress_total{provider}` | Entries received by shared provider tailers |
| `vs_realtime_broker_replay_events_total{provider}` | Retained events replayed into reconnecting sessions |
| `vs_realtime_broker_restarts_total{provider,reason}` | Shared provider tailer read failures and retries |
| `vs_realtime_broker_terminal_failures_total{provider,reason}` | Shared provider recovery thresholds exhausted |
| `vs_realtime_broker_entries_dropped_total{provider,reason}` | Provider entries rejected before gateway fan-out |
| `vs_realtime_broker_retention_runs_total{provider,result}` | Redis retention backstop outcomes |

## Useful metric tags

Every important log line carries a `metric=...` field. Grep these to
isolate a single signal:

**Shared infrastructure (any source):**

| Metric | Where | What |
|---|---|---|
| `dispatcher.bulk.start` | dispatcher | Bulk write about to begin |
| `dispatcher.bulk.ack` | dispatcher | Bulk acknowledged with elapsed_ms |
| `bus.backpressure` | bus | Source had to await capacity |
| `bus.backpressure.cleared` | bus | Backpressure window ended (with waited_ms) |
| `memory.admission.blocked` | memory controller | Event admission is waiting for byte capacity |
| `memory.pressure.transition` | memory controller | Cgroup pressure state changed with current/limit bytes |

**Neo4j source:**

| Metric | What |
|---|---|
| `neo4j.poll.cypher` | One per `db.cdc.query` call (rows + elapsed_ms) |
| `neo4j.tail.summary` | 30s rolling heartbeat stats |
| `neo4j.tail.recomposed` | Per-event fan-out output |
| `neo4j.tail.deleted` | Per-primary delete |
| `neo4j.denormalize.fan_out_cypher` | Per-spec per-event cypher time |

**Postgres source:**

| Metric | What |
|---|---|
| `pg.replication.xlog_data` | One per WAL message (events_in_payload + payload_bytes) |
| `pg.replication.txn_begin` | Transaction begin (xid + final_lsn) |
| `pg.replication.txn_commit` | Transaction commit (commit_lsn + end_lsn) |
| `pg.replication.keepalive` | Server keepalive ping |
| `pg.replication.lsn_advance` | Replication slot advanced (advance_to + last_acked) |
| `pg.replication.logical_message_ignored` | Non-XLogData logical replication message |
| `pg.fetcher.query` | Sync-on-miss fetcher SQL (sql + param_count) |

## Sensitive-data policy

The DEBUG logs are deliberately scoped to **schema and operational
fields only**. The following are NEVER logged:

- **Row contents** from CDC events (PG WAL payloads, Neo4j event JSON
  values). The dispatcher logs batches by size and byte count, never
  by content.
- **Fetcher query parameter values** (PK lookup values, FK values).
  Only `param_count` is logged. The SQL template is logged because
  it contains only column names + `$N` placeholders.
- **Connection passwords** — config logs only host, port, database,
  slot.
- **Logical replication message content** — only the prefix (which is
  schema-like) and byte count are logged.

If you spot something logged that looks like it could be sensitive,
flag it — these logs may end up in shared aggregators.

## Common scenarios

### "Cascade event seems to hang"

1. Filter: `RUST_LOG=info,ventstream_sources::neo4j::denormalize=debug`
2. Trigger the cascade
3. Look for `metric="neo4j.denormalize.fan_out_cypher"` — that tells
   you whether the cypher itself is slow (large `cypher_elapsed_ms`)
   or the recompose loop afterwards
4. If cypher is fast but recompose is slow, the bottleneck is the bus
   / dispatcher / sink. Switch to `RUST_LOG=info,ventstream_core::bus=debug`
   and look for backpressure

### "Cursor file isn't advancing"

1. Filter: `RUST_LOG=info,ventstream_sources::neo4j::source=debug`
2. Watch `"neo4j cursor drain"` lines:
   - `sink_progress=0` always → dispatcher isn't writing the watermark.
     Check the events have `ventstream.cdc.tx_id` header.
   - `sink_progress < pending_front_tx` → sink is behind. Investigate
     bulk write latency.
   - `cursor_advanced=true` periodically → it's working. Cursor file
     write is gated correctly.

### "Memory grows unboundedly"

The most likely culprits are:
- DLQ file growing (`VS_DLQ_PATH` size on disk)
- Pending cursors queue (debug log `"neo4j cursor drain"` shows
  `pending_after`)

If `pending_after` keeps climbing across heartbeats, the sink is too
slow for the source's emit rate. Same diagnosis path as backpressure.

## Performance probing

For one-off latency investigations:

```bash
# Tail the binary log and extract per-event elapsed times
grep "denormalize.tail.recomposed" /tmp/vs.log | \
  awk -F'elapsed_ms=' '{print $2}' | awk '{print $1}' | sort -n | \
  awk 'BEGIN{c=0} {a[c++]=$1} END{print "p50="a[int(c*0.5)]" p95="a[int(c*0.95)]" max="a[c-1]}'
```

The same one-liner can be wrapped in a load-test script that ends with a
distribution summary derived from these logs.

## What still isn't there

- **OpenTelemetry spans**: not wired through yet. The `tracing` crate
  is in use but spans aren't emitted at every hop. Possible follow-up
  if a request-tracing story is needed beyond logs.
- **dd-trace**: in workspace deps, but not initialised in the binary.
  Adding it is ~1 day of work; the structured logs are usable today.
