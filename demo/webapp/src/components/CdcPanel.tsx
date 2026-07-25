import { useState } from 'react';
import { useCdc, type SyncState } from './useCdc';

const BURST_RATES = [1, 5, 10, 25, 50];
const BURST_DURATIONS = [10, 30, 60, 120];

const BURST_TABLES: { value: string; label: string }[] = [
  { value: 'mixed',                   label: 'mixed (all tables · all ops)' },
  { value: 'shows.update',            label: 'shows · status flips' },
  { value: 'shows.insert',            label: 'shows · INSERT (count climbs)' },
  { value: 'shows.delete',            label: 'shows · DELETE (count drops)' },
  { value: 'routes.rename',           label: 'routes · rename (fanout)' },
  { value: 'show_venue.rename',       label: 'show_venue · rename (fanout)' },
  { value: 'show_buyer.rename',       label: 'show_buyer · rename' },
  { value: 'holds.insert',            label: 'holds · INSERT' },
  { value: 'holds.delete',            label: 'holds · DELETE' },
  { value: 'radius_clauses.insert',   label: 'radius_clauses · INSERT' },
  { value: 'radius_clauses.delete',   label: 'radius_clauses · DELETE' },
];

export function CdcPanel() {
  const {
    status, error,
    renameRoute, insertHolds, startBurst, stopBurst,
    syncState, resetOs, fullResync,
  } = useCdc();
  const [burstRate, setBurstRate] = useState<number>(5);
  const [burstDuration, setBurstDuration] = useState<number>(30);
  const [burstTable, setBurstTable] = useState<string>('mixed');

  const burstElapsed =
    status.burstRunning && status.burstStartedAt
      ? Math.max(0, Math.floor(Date.now() / 1000 - status.burstStartedAt))
      : 0;
  const syncing = syncState.kind === 'syncing';
  const resetting = syncState.kind === 'resetting';

  return (
    <div className="bg-panel border border-border rounded-lg overflow-hidden">
      <div className="px-5 py-3 border-b border-border flex items-center justify-between gap-4 flex-wrap">
        <div>
          <div className="text-[10px] uppercase tracking-wider text-muted font-medium">
            CDC pipeline
          </div>
          <div className="text-sm text-fg font-semibold mt-0.5">
            Postgres → VentStream → OpenSearch
          </div>
        </div>
        <WriteRate rate={status.osWritesPerSec} />
        <SyncBadge inSync={status.inSync} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2">
        {/* Left: state counters */}
        <div className="p-5 border-r border-border space-y-3">
          <div className="text-[10px] uppercase tracking-wider text-muted font-medium">Per-table state</div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-muted text-[10px] uppercase tracking-wider">
                <th className="text-left font-medium py-1.5">Table</th>
                <th className="text-right font-medium py-1.5">Postgres</th>
                <th className="text-right font-medium py-1.5">OpenSearch</th>
                <th className="w-4"></th>
              </tr>
            </thead>
            <tbody className="font-mono tabular-nums">
              <TableRow label="shows"          pg={status.pg.shows}          os={status.os.shows}          primary />
              <TableRow label="routes"         pg={status.pg.routes}         os={status.os.routes}         approx />
              <TableRow label="show_venue"     pg={status.pg.show_venue}     os={status.os.show_venue}     approx />
              <TableRow label="show_buyer"     pg={status.pg.show_buyer}     os={status.os.show_buyer}     approx />
              <TableRow label="holds"          pg={status.pg.holds}          os={status.os.holds} />
              <TableRow label="radius_clauses" pg={status.pg.radius_clauses} os={status.os.radius_clauses} />
            </tbody>
          </table>
          <div className="pt-2 mt-2 border-t border-border/50 text-xs text-muted">
            mutations this session: <span className="font-mono text-fg">{status.mutationsTotal}</span>
          </div>
        </div>

        {/* Right: trigger buttons */}
        <div className="p-5 space-y-4">
          <div className="text-[10px] uppercase tracking-wider text-muted font-medium">Trigger</div>

          <div className="space-y-2">
            <button
              onClick={renameRoute}
              disabled={status.burstRunning}
              className="w-full px-4 py-2.5 rounded-md font-medium text-sm bg-accent/10 border border-accent/40 text-accent hover:bg-accent/20 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-left flex items-center gap-3"
            >
              <span className="font-mono text-base">⌁</span>
              <div className="flex-1">
                <div>Rename a route</div>
                <div className="text-[11px] text-muted font-normal">1 update → fans out to ~20 shows</div>
              </div>
            </button>
            <button
              onClick={() => insertHolds(50)}
              disabled={status.burstRunning}
              className="w-full px-4 py-2.5 rounded-md font-medium text-sm bg-accent/10 border border-accent/40 text-accent hover:bg-accent/20 disabled:opacity-40 disabled:cursor-not-allowed transition-colors text-left flex items-center gap-3"
            >
              <span className="font-mono text-base">+</span>
              <div className="flex-1">
                <div>Insert 50 random holds</div>
                <div className="text-[11px] text-muted font-normal">touches 50 shows, each gets a new embedded hold</div>
              </div>
            </button>
          </div>

          <div className="pt-2 border-t border-border/50 space-y-2">
            <div className="text-[10px] uppercase tracking-wider text-muted font-medium">Continuous burst</div>
            <div className="flex flex-wrap items-end gap-3">
              <label className="flex flex-col gap-1">
                <span className="text-[10px] text-muted">Table</span>
                <select
                  value={burstTable}
                  onChange={(e) => setBurstTable(e.target.value)}
                  disabled={status.burstRunning}
                  className="bg-bg border border-border rounded px-2 py-1.5 text-xs font-mono w-56 disabled:opacity-50"
                >
                  {BURST_TABLES.map((t) => (
                    <option key={t.value} value={t.value}>{t.label}</option>
                  ))}
                </select>
                {burstTable === 'shows.delete' && (
                  <span className="text-[10px] text-muted font-mono">
                    deletable: <span className="text-fg">{status.burstDeletableShows.toLocaleString()}</span>
                  </span>
                )}
              </label>
              <label className="flex flex-col gap-1">
                <span className="text-[10px] text-muted">Rate</span>
                <select
                  value={burstRate}
                  onChange={(e) => setBurstRate(parseFloat(e.target.value))}
                  disabled={status.burstRunning}
                  className="bg-bg border border-border rounded px-2 py-1.5 text-xs font-mono w-24 disabled:opacity-50"
                >
                  {BURST_RATES.map((r) => (
                    <option key={r} value={r}>{r} /sec</option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1">
                <span className="text-[10px] text-muted">Duration</span>
                <select
                  value={burstDuration}
                  onChange={(e) => setBurstDuration(parseInt(e.target.value, 10))}
                  disabled={status.burstRunning}
                  className="bg-bg border border-border rounded px-2 py-1.5 text-xs font-mono w-24 disabled:opacity-50"
                >
                  {BURST_DURATIONS.map((d) => (
                    <option key={d} value={d}>{d}s</option>
                  ))}
                </select>
              </label>
              {!status.burstRunning ? (
                <button
                  onClick={() => startBurst(burstRate, burstDuration, burstTable)}
                  className="ml-auto px-4 py-2 rounded-md font-medium text-sm bg-accent2/15 border border-accent2/50 text-accent2 hover:bg-accent2/25 transition-colors flex items-center gap-2"
                >
                  <span>▶</span><span>Start burst</span>
                </button>
              ) : (
                <button
                  onClick={stopBurst}
                  className="ml-auto px-4 py-2 rounded-md font-medium text-sm bg-bad/15 border border-bad/50 text-bad hover:bg-bad/25 transition-colors flex items-center gap-2"
                >
                  <span>■</span><span>Stop burst</span>
                </button>
              )}
            </div>
            {status.burstRunning && (
              <div className="text-xs text-accent2 flex items-center gap-2">
                <span className="inline-block w-2 h-2 bg-accent2 rounded-full animate-pulse" />
                bursting · {burstTable} · {status.burstRate}/sec · {burstElapsed}s
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="border-t border-border p-5 space-y-3 bg-bg/30">
        <div className="text-[10px] uppercase tracking-wider text-muted font-medium">Bulk operations</div>
        <div className="flex flex-wrap items-center gap-3">
          <button
            onClick={resetOs}
            disabled={resetting || syncing || status.burstRunning}
            className="px-4 py-2 rounded-md font-medium text-sm bg-bad/10 border border-bad/40 text-bad hover:bg-bad/20 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
            title="Delete the OpenSearch index. Doc count will drop to 0 — gives the next full-sync something to do."
          >
            <span>⊘</span>
            <span>{resetting ? 'Resetting…' : 'Reset OpenSearch'}</span>
          </button>
          <button
            onClick={fullResync}
            disabled={syncing || resetting}
            className="px-4 py-2 rounded-md font-medium text-sm bg-accent/15 border border-accent/50 text-accent hover:bg-accent/25 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
            title="Re-emit every row from every configured table through the engine. Idempotent — OS docs are upserted by deterministic ID."
          >
            <span>↻</span>
            <span>{syncing ? 'Syncing…' : 'Full sync from Postgres'}</span>
          </button>
          <SyncStateLine state={syncState} osDocs={status.os.docs} />
        </div>
      </div>

      <LastActionBar action={status.lastAction} ts={status.lastActionTs} error={error} />
    </div>
  );
}

function WriteRate({ rate }: { rate: number }) {
  const active = rate > 0.05;
  return (
    <div className="flex items-center gap-2 text-xs">
      <div className="flex flex-col items-end">
        <span className="text-[9px] uppercase tracking-wider text-muted leading-tight">OS writes</span>
        <span className={`font-mono tabular-nums font-semibold leading-tight ${active ? 'text-accent2' : 'text-muted'}`}>
          {rate.toFixed(1)} <span className="text-[10px] text-muted font-normal">/ sec</span>
        </span>
      </div>
      {active && <span className="inline-block w-2 h-2 bg-accent2 rounded-full animate-pulse" />}
    </div>
  );
}

function LastActionBar({ action, ts, error }: { action: string; ts: number; error: string | null }) {
  // Highlight when an action came in within the last 1.5 seconds, so the
  // audience can clearly see "something just happened" during a burst.
  const recent = ts > 0 && Date.now() - ts < 1500;
  return (
    <div
      className={`border-t border-border px-5 py-3 text-xs transition-colors ${
        recent ? 'bg-accent/5' : ''
      }`}
    >
      <span className="text-[10px] uppercase tracking-wider text-muted mr-3">Last action</span>
      <span
        className={`font-mono ${
          recent ? 'text-accent font-semibold' : action ? 'text-fg' : 'text-muted'
        }`}
      >
        {action || '— idle'}
      </span>
      {error && <span className="ml-3 text-bad">· {error}</span>}
    </div>
  );
}

function SyncStateLine({ state, osDocs }: { state: SyncState; osDocs: number }) {
  if (state.kind === 'idle') return null;
  if (state.kind === 'resetting') {
    return <span className="text-xs text-bad">wiping OpenSearch index…</span>;
  }
  if (state.kind === 'syncing') {
    const elapsed = ((Date.now() - state.startedAt) / 1000).toFixed(1);
    return (
      <span className="text-xs text-accent flex items-center gap-2">
        <span className="inline-block w-2 h-2 bg-accent rounded-full animate-pulse" />
        syncing · OS docs so far: <span className="font-mono">{osDocs.toLocaleString()}</span> · {elapsed}s
      </span>
    );
  }
  if (state.kind === 'done') {
    return (
      <span className="text-xs text-good">
        ✓ resynced {state.result.stats.total.toLocaleString()} rows in {(state.result.stats.elapsed_ms / 1000).toFixed(1)}s
        <span className="text-muted ml-1">
          ({state.result.tables_processed.length} tables)
        </span>
      </span>
    );
  }
  return <span className="text-xs text-bad">error: {state.message}</span>;
}

function SyncBadge({ inSync }: { inSync: boolean }) {
  return inSync ? (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-2 h-2 rounded-full bg-good pulse-live" />
      <span className="text-good font-medium">PG = OS exact</span>
    </div>
  ) : (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-2 h-2 rounded-full bg-accent2" />
      <span className="text-accent2 font-medium">catching up…</span>
    </div>
  );
}

function TableRow({
  label,
  pg,
  os,
  primary = false,
  approx = false,
}: {
  label: string;
  pg: number;
  os: number;
  primary?: boolean;
  /** OS count for this table comes from a cardinality (HyperLogLog) aggregation
   *  which is ~99% accurate. Tolerate small drift before painting it red. */
  approx?: boolean;
}) {
  const exact = pg === os && pg > 0;
  // Cardinality tolerance: within 1%, consider it a match.
  const tolerance = approx ? Math.max(1, Math.ceil(pg * 0.01)) : 0;
  const close = !exact && pg > 0 && Math.abs(pg - os) <= tolerance;
  const drift = !exact && !close && pg > 0;
  return (
    <tr className={`border-b border-border/40 last:border-b-0 ${primary ? 'bg-bg/40' : ''}`}>
      <td className={`py-1.5 ${primary ? 'text-fg font-semibold' : 'text-muted'}`}>
        {label}
        {approx && <span className="ml-1 text-[9px] text-muted/60 font-normal" title="OS count via cardinality aggregation (~99% accurate)">·approx</span>}
      </td>
      <td className="py-1.5 text-right text-accent">{pg.toLocaleString()}</td>
      <td className={`py-1.5 text-right ${exact || close ? 'text-accent2' : drift ? 'text-bad' : 'text-muted'}`}>
        {os.toLocaleString()}
      </td>
      <td className="py-1.5 pl-2 text-right">
        {exact ? (
          <span className="text-good" title="exact match">✓</span>
        ) : close ? (
          <span className="text-good" title={`within tolerance · ${Math.abs(pg - os)} off`}>≈</span>
        ) : drift ? (
          <span className="text-bad" title={`drift: ${pg - os}`}>⊖</span>
        ) : (
          <span className="text-muted">·</span>
        )}
      </td>
    </tr>
  );
}
