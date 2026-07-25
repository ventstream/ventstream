import { useEffect, useRef, useState } from 'react';

/**
 * Polls NATS JetStream monitoring (proxied through Vite at /nats/jsz)
 * to surface a "publisher side" view in the same window as the
 * subscription receiver.
 *
 * We capture the stream's message count at mount time as a baseline so
 * the displayed `sessionPublished` is what's flowed THIS demo session,
 * not the lifetime stream total.
 */
export function usePublisher(streamName: string) {
  const [lifetimePublished, setLifetimePublished] = useState<number | null>(null);
  const [sessionPublished, setSessionPublished] = useState(0);
  const [publishedPerSec, setPublishedPerSec] = useState(0);
  const baseline = useRef<number | null>(null);
  const lastSeen = useRef<number | null>(null);
  const lastSeenAt = useRef<number | null>(null);
  const rateBuf = useRef<{ ts: number; delta: number }[]>([]);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const res = await fetch('/nats/jsz?streams=true', { cache: 'no-store' });
        if (!res.ok) return;
        const json = await res.json();
        const streams =
          json.account_details?.[0]?.stream_detail ??
          json.streams ??
          [];
        const target = streams.find((s: any) => {
          const name = s.config?.name ?? s.name;
          return name === streamName;
        });
        if (!target) return;
        const total: number = target.state?.messages ?? target.messages ?? 0;
        if (cancelled) return;
        if (baseline.current == null) baseline.current = total;
        setLifetimePublished(total);
        setSessionPublished(Math.max(0, total - (baseline.current ?? 0)));

        // Rate window: track deltas across polls
        const now = Date.now();
        if (lastSeen.current != null && lastSeenAt.current != null) {
          const delta = total - lastSeen.current;
          if (delta > 0) {
            rateBuf.current.push({ ts: now, delta });
          }
        }
        lastSeen.current = total;
        lastSeenAt.current = now;

        // Trim to last 5s and compute rate
        const cutoff = now - 5_000;
        rateBuf.current = rateBuf.current.filter((e) => e.ts >= cutoff);
        const recent = rateBuf.current.reduce((sum, e) => sum + e.delta, 0);
        setPublishedPerSec(recent / 5);
      } catch {
        // Monitoring may not be enabled or NATS may be down — just hold the last good value.
      }
    }
    poll();
    const id = setInterval(poll, 500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [streamName]);

  return { lifetimePublished, sessionPublished, publishedPerSec };
}
