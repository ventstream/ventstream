import { useCallback, useEffect, useRef, useState } from 'react';

interface TableCounts {
  shows: number;
  routes: number;
  show_venue: number;
  show_buyer: number;
  holds: number;
  radius_clauses: number;
}

interface OsCounts extends TableCounts {
  docs: number; // top-level doc count, alias for shows
}

export interface CdcStatus {
  pg: TableCounts;
  os: OsCounts;
  burstRunning: boolean;
  burstRate: number;
  burstStartedAt: number | null;
  mutationsTotal: number;
  burstDeletableShows: number;
  lastAction: string;
  lastActionTs: number;
  /** Rolling OS write rate (upserts per second) over a 5-second window. */
  osWritesPerSec: number;
  inSync: boolean;
}

export interface ResyncResult {
  tables_processed: string[];
  stats: { total: number; elapsed_ms: number };
}

export type SyncState =
  | { kind: 'idle' }
  | { kind: 'resetting' }
  | { kind: 'syncing'; startedAt: number }
  | { kind: 'done'; result: ResyncResult; finishedAt: number }
  | { kind: 'error'; message: string };

const EMPTY_PG: TableCounts = {
  shows: 0, routes: 0, show_venue: 0, show_buyer: 0, holds: 0, radius_clauses: 0,
};

const EMPTY_OS: OsCounts = {
  docs: 0, shows: 0, routes: 0, show_venue: 0, show_buyer: 0, holds: 0, radius_clauses: 0,
};

// The engine's OS sink writes to a static index name now (no date
// suffix) — see VS_INDEX_TEMPLATE='events-${header:ventstream.cdc.relation}'
// in the demo run script. One index per CDC table, upserted by doc_id;
// avoids the date-rotation-vs-upsert split that would happen at UTC
// midnight if the template included %Y-%m-%d.
const OS_INDEX = 'events-shows';

/** Aggregation query that pulls every per-table count we want in one shot. */
const OS_AGG_BODY = JSON.stringify({
  size: 0,
  track_total_hits: true,
  aggs: {
    routes:    { cardinality: { field: 'route.route_id.keyword',           precision_threshold: 40000 } },
    show_venue:{ cardinality: { field: 'venue.show_venue_id.keyword',      precision_threshold: 40000 } },
    show_buyer:{ cardinality: { field: 'buyer.show_id.keyword',            precision_threshold: 40000 } },
    holds:     { value_count: { field: 'holds.hold_id.keyword' } },
    radius:    { value_count: { field: 'radius_clauses.radius_clause_id.keyword' } },
  },
});

export function useCdc() {
  const [status, setStatus] = useState<CdcStatus>({
    pg: EMPTY_PG,
    os: EMPTY_OS,
    burstRunning: false,
    burstRate: 0,
    burstStartedAt: null,
    mutationsTotal: 0,
    burstDeletableShows: 0,
    lastAction: '',
    lastActionTs: 0,
    osWritesPerSec: 0,
    inSync: false,
  });
  const [error, setError] = useState<string | null>(null);
  const [syncState, setSyncState] = useState<SyncState>({ kind: 'idle' });
  // Track which 'syncing' run has already been declared visually done
  // so the HTTP response (when it eventually returns) doesn't clobber
  // the displayed timer with the actual longer engine-side elapsed.
  const declaredDoneFor = useRef<number | null>(null);
  /** Rolling window of (timestamp, index_total) samples for OS write rate. */
  const osStatsBuf = useRef<{ ts: number; total: number }[]>([]);
  /** Previous lastAction string — used to detect changes for the timestamp. */
  const prevLastAction = useRef<string>('');

  const poll = useCallback(async () => {
    try {
      const [pgRes, osAggRes, osStatsRes] = await Promise.all([
        fetch('/publisher/cdc/status', { cache: 'no-store' }),
        fetch(`/os/${OS_INDEX}/_search?size=0`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: OS_AGG_BODY,
          cache: 'no-store',
        }),
        fetch(`/os/${OS_INDEX}/_stats/indexing?level=indices`, { cache: 'no-store' }),
      ]);
      if (!pgRes.ok) throw new Error(`pg status HTTP ${pgRes.status}`);
      const pgBody = await pgRes.json();
      const pg: TableCounts = pgBody.pg ?? EMPTY_PG;

      let os: OsCounts = EMPTY_OS;
      if (osAggRes.ok) {
        const aggBody = await osAggRes.json();
        const docs: number = aggBody.hits?.total?.value ?? 0;
        const a = aggBody.aggregations ?? {};
        os = {
          docs,
          shows: docs,
          routes: a.routes?.value ?? 0,
          show_venue: a.show_venue?.value ?? 0,
          show_buyer: a.show_buyer?.value ?? 0,
          holds: a.holds?.value ?? 0,
          radius_clauses: a.radius?.value ?? 0,
        };
      }

      // OS indexing rate. /_stats returns indexing.index_total — a
      // monotonic counter of all index ops. We diff samples over a
      // 5-second rolling window to get ops/sec.
      let osWritesPerSec = 0;
      if (osStatsRes.ok) {
        const statsBody = await osStatsRes.json();
        const indexStats =
          statsBody.indices?.[OS_INDEX]?.total?.indexing ??
          Object.values(statsBody.indices ?? {})[0] as any;
        const total: number = indexStats?.index_total ?? indexStats?.total?.indexing?.index_total ?? 0;
        const now = Date.now();
        osStatsBuf.current.push({ ts: now, total });
        const cutoff = now - 5000;
        osStatsBuf.current = osStatsBuf.current.filter((s) => s.ts >= cutoff);
        if (osStatsBuf.current.length >= 2) {
          const first = osStatsBuf.current[0];
          const last = osStatsBuf.current[osStatsBuf.current.length - 1];
          const dt = (last.ts - first.ts) / 1000;
          if (dt > 0) osWritesPerSec = Math.max(0, (last.total - first.total) / dt);
        }
      }

      const lastAction = pgBody.last_action ?? '';
      const actionChanged = lastAction && lastAction !== prevLastAction.current;
      prevLastAction.current = lastAction;

      setStatus((prev) => ({
        pg,
        os,
        burstRunning: !!pgBody.burst?.running,
        burstRate: pgBody.burst?.rate ?? 0,
        burstStartedAt: pgBody.burst?.started_at ?? null,
        mutationsTotal: pgBody.mutations_total ?? 0,
        burstDeletableShows: pgBody.burst_deletable_shows ?? 0,
        lastAction,
        lastActionTs: actionChanged ? Date.now() : prev.lastActionTs,
        osWritesPerSec,
        inSync: pg.shows === os.docs,
      }));
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    poll();
    const id = setInterval(poll, 1000);
    return () => clearInterval(id);
  }, [poll]);

  // Stop the sync timer as soon as OS catches up to PG — the engine
  // may still be finalizing fanouts on /admin/resync, but visually
  // the sync is done from the user's perspective. The HTTP response
  // (when it finally arrives) overwrites with full stats.
  useEffect(() => {
    if (syncState.kind !== 'syncing') return;
    if (declaredDoneFor.current === syncState.startedAt) return;
    const elapsedMs = Date.now() - syncState.startedAt;
    // Wait at least 1.2s so the climb is visible. Then check sync.
    if (elapsedMs < 1200) return;
    if (status.pg.shows > 0 && status.os.docs === status.pg.shows) {
      declaredDoneFor.current = syncState.startedAt;
      setSyncState({
        kind: 'done',
        result: {
          tables_processed: ['(all)'],
          stats: { total: status.pg.shows, elapsed_ms: elapsedMs },
        },
        finishedAt: Date.now(),
      });
    }
  }, [status.os.docs, status.pg.shows, syncState]);

  const renameRoute = useCallback(async () => {
    setError(null);
    try {
      const r = await fetch('/publisher/cdc/route_rename', { method: 'POST' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      poll();
    } catch (e) {
      setError((e as Error).message);
    }
  }, [poll]);

  const insertHolds = useCallback(async (n: number) => {
    setError(null);
    try {
      const r = await fetch('/publisher/cdc/insert_holds', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ n }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      poll();
    } catch (e) {
      setError((e as Error).message);
    }
  }, [poll]);

  const startBurst = useCallback(async (rate: number, duration: number, table: string = 'mixed') => {
    setError(null);
    try {
      const r = await fetch('/publisher/cdc/burst_start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rate, duration, table }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      poll();
    } catch (e) {
      setError((e as Error).message);
    }
  }, [poll]);

  const stopBurst = useCallback(async () => {
    setError(null);
    try {
      const r = await fetch('/publisher/cdc/burst_stop', { method: 'POST' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      poll();
    } catch (e) {
      setError((e as Error).message);
    }
  }, [poll]);

  /** Wipe the OS index. Watch the doc counter drop to 0. */
  const resetOs = useCallback(async () => {
    setSyncState({ kind: 'resetting' });
    try {
      const r = await fetch(`/os/${OS_INDEX}`, { method: 'DELETE' });
      // 200 (deleted) and 404 (didn't exist yet) are both fine.
      if (!r.ok && r.status !== 404) {
        const txt = await r.text();
        throw new Error(`HTTP ${r.status} · ${txt.slice(0, 200)}`);
      }
      setSyncState({ kind: 'idle' });
      poll();
    } catch (e) {
      setSyncState({ kind: 'error', message: (e as Error).message });
    }
  }, [poll]);

  /** Drive a full re-sync via the engine's admin endpoint. */
  const fullResync = useCallback(async () => {
    const started = Date.now();
    declaredDoneFor.current = null;
    setSyncState({ kind: 'syncing', startedAt: started });
    try {
      const r = await fetch('/admin/resync', { method: 'POST' });
      if (!r.ok) {
        const txt = await r.text();
        throw new Error(`HTTP ${r.status} · ${txt.slice(0, 200)}`);
      }
      // If we already optimistically declared done (OS caught up
      // first), keep the user-facing timer and just refresh status.
      // Otherwise commit the HTTP-derived stats.
      if (declaredDoneFor.current !== started) {
        const body: ResyncResult = await r.json();
        setSyncState({ kind: 'done', result: body, finishedAt: Date.now() });
      } else {
        // Drain the body so the connection cleans up.
        await r.json().catch(() => undefined);
      }
      poll();
    } catch (e) {
      // Suppress errors that arrive after we already showed done.
      if (declaredDoneFor.current === started) return;
      setSyncState({ kind: 'error', message: (e as Error).message });
    }
  }, [poll]);

  return {
    status,
    error,
    syncState,
    renameRoute,
    insertHolds,
    startBurst,
    stopBurst,
    resetOs,
    fullResync,
  };
}
