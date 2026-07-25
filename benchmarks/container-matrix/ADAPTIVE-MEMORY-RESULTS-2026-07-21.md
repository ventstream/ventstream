# Adaptive Memory Controller Results — 2026-07-21

## Workload

- MongoDB CDC to OpenSearch, exact end-to-end document verification.
- 100,000 inserts across the maximum profile.
- 4 KiB document payloads.
- Engine: 2 vCPU, 65,536-event bus, 10,000-event/32 MiB batches, and a
  configured ceiling of 32 parallel bulks.
- Production jemalloc settings:
  `background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:1000`.

## Constrained-memory result

The engine completed under a 256 MiB hard cgroup limit:

| Result | Value |
|---|---:|
| Documents verified | 100,000 / 100,000 |
| Elapsed | 5.819 s |
| Throughput | 17,185.08 events/s |
| CPU mean / p95 / peak | 38.09% / 51.06% / 51.06% |
| Cgroup working-set peak | 138.90 MiB |
| Process RSS peak | 179.66 MiB |
| Process HWM | 226.89 MiB |
| OOM killed | No |

The controller crossed `Normal -> Constrained -> High`, reduced admitted event
bytes and sink work, then recovered while preserving exact output parity.

## 1 GiB controller comparison

Both runs used the same image, allocator settings, source data, and maximum
profile. Only `VS_MEMORY_CONTROLLER_ENABLED` changed.

| Controller | Throughput | CPU mean | Cgroup peak | RSS peak | HWM |
|---|---:|---:|---:|---:|---:|
| Enabled | 19,976.03 events/s | 38.28% | 358.40 MiB | 490.27 MiB | 533.54 MiB |
| Disabled | 19,527.44 events/s | 44.95% | 525.10 MiB | 658.71 MiB | 695.01 MiB |

For this burst workload, control reduced cgroup peak by 31.7%, RSS peak by
25.6%, and HWM by 23.2%. Throughput was 2.3% higher rather than lower because
bounded in-flight work reduced allocation and scheduling pressure.

## Failure-driven tuning

Two deliberately aggressive candidates were rejected before the final result:

1. A 45%-of-cgroup event budget with 70/82/92 pressure thresholds and 10-second
   allocator decay reached 83% cgroup use and was OOM-killed.
2. A 30% budget with 65/75/85 thresholds still OOM-killed when the allocator
   retained burst allocations for 10 seconds; process HWM reached 263.78 MiB.

The accepted policy combines a 30% automatic event budget, 65/75/85 pressure
thresholds, 100 ms sampling, stronger batch/concurrency reductions, and short
background allocator decay. Admission control alone cannot protect a tight
cgroup if the allocator retains already-freed bulk buffers past the OOM window.

Raw samples are under:

- `target/benchmarks/container-matrix/adaptive-mongo-256m-v3`
- `target/benchmarks/container-matrix/adaptive-mongo-1g-on`
- `target/benchmarks/container-matrix/adaptive-mongo-1g-off`
