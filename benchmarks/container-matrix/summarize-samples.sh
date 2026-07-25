#!/usr/bin/env bash
set -euo pipefail

stats=${1:?docker stats file is required}
rss=${2:?process memory file is required}

awk '
function mib(value, unit) {
  if (unit == "B") return value / 1048576
  if (unit == "KiB" || unit == "kB") return value / 1024
  if (unit == "MiB") return value
  if (unit == "GiB") return value * 1024
  return value
}
{
  cpu=$1; gsub(/%/, "", cpu)
  memory=$2
  unit=memory; gsub(/[0-9.]/, "", unit)
  gsub(/[A-Za-z]/, "", memory)
  memory=mib(memory+0, unit)
  if (cpu != "" && cpu + 0 >= 0) {
    cpus[++n]=cpu+0; sum+=cpu; if (cpu > cpu_max) cpu_max=cpu
  }
  if (memory > mem_max) mem_max=memory
}
END {
  if (n == 0) { printf "0,0,0,0"; exit }
  for (i=1; i<=n; i++) for (j=i+1; j<=n; j++) if (cpus[j] < cpus[i]) {
    t=cpus[i]; cpus[i]=cpus[j]; cpus[j]=t
  }
  p95_index=int(n*0.95); if (p95_index < n*0.95) p95_index++
  p95=cpus[p95_index]
  printf "%.2f,%.2f,%.2f,%.2f", sum/n, p95, cpu_max, mem_max
}' "$stats"

awk 'BEGIN { rss=0; hwm=0 } { if ($2>rss) rss=$2; if ($3>hwm) hwm=$3 } END {
  printf ",%.2f,%.2f\n", rss/1024, hwm/1024
}' "$rss"
