import { useCallback, useEffect, useState } from 'react';

export interface PublisherStatus {
  running: boolean;
  rate: number;
  duration: number;
  startedAt: number | null;
  orderId: string | null;
}

const INITIAL: PublisherStatus = {
  running: false,
  rate: 0,
  duration: 0,
  startedAt: null,
  orderId: null,
};

/** Talk to the publisher HTTP control plane via Vite proxy. */
export function usePublisherControl() {
  const [status, setStatus] = useState<PublisherStatus>(INITIAL);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const r = await fetch('/publisher/status', { cache: 'no-store' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = await r.json();
      setStatus({
        running: !!j.running,
        rate: j.rate ?? 0,
        duration: j.duration ?? 0,
        startedAt: j.started_at ?? null,
        orderId: j.order_id ?? null,
      });
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 1000);
    return () => clearInterval(id);
  }, [refresh]);

  const start = useCallback(
    async (rate: number, duration: number, orderId: string) => {
      setError(null);
      try {
        const r = await fetch('/publisher/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ rate, duration, orderId }),
        });
        if (!r.ok) {
          const txt = await r.text();
          throw new Error(txt || `HTTP ${r.status}`);
        }
        await refresh();
      } catch (e) {
        setError((e as Error).message);
      }
    },
    [refresh],
  );

  const stop = useCallback(async () => {
    setError(null);
    try {
      const r = await fetch('/publisher/stop', { method: 'POST' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    }
  }, [refresh]);

  return { status, error, start, stop };
}
