import { useEffect, useRef } from 'react';
import type { Event } from './useSubscription';

interface Props {
  events: Event[];
}

export function EventLog({ events }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    ref.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, [events]);

  return (
    <div className="bg-panel border border-border rounded-lg overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 border-b border-border">
        <div className="text-[10px] uppercase tracking-wider text-muted font-medium">
          Event Log
        </div>
        <div className="text-xs text-muted">
          showing latest {events.length}
        </div>
      </div>
      <div ref={ref} className="max-h-[420px] overflow-y-auto">
        {events.length === 0 ? (
          <div className="px-5 py-12 text-center text-muted text-sm">
            Waiting for events… publish to the NATS subject this subscription is bound to.
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead className="bg-bg/40 text-muted">
              <tr>
                <th className="text-left font-medium px-5 py-2.5 uppercase tracking-wider text-[10px]">Time</th>
                <th className="text-left font-medium px-5 py-2.5 uppercase tracking-wider text-[10px]">Order</th>
                <th className="text-left font-medium px-5 py-2.5 uppercase tracking-wider text-[10px]">From</th>
                <th className="text-left font-medium px-5 py-2.5 uppercase tracking-wider text-[10px]">→ To</th>
                <th className="text-left font-medium px-5 py-2.5 uppercase tracking-wider text-[10px]">Changed at</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e, i) => (
                <tr
                  key={`${e.ts}-${i}`}
                  className={`event-row border-t border-border/60 ${i === 0 ? 'bg-accent/5' : ''}`}
                >
                  <td className="px-5 py-2.5 font-mono text-muted">{formatTime(e.ts)}</td>
                  <td className="px-5 py-2.5 font-mono">{e.orderId}</td>
                  <td className="px-5 py-2.5">
                    <Pill text={e.from} />
                  </td>
                  <td className="px-5 py-2.5">
                    <Pill text={e.to} accent />
                  </td>
                  <td className="px-5 py-2.5 font-mono text-muted">{formatTime(Date.parse(e.changedAt))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function Pill({ text, accent = false }: { text: string; accent?: boolean }) {
  return (
    <span
      className={`inline-block px-2 py-0.5 rounded-full font-mono text-[11px] ${
        accent
          ? 'bg-accent/15 text-accent border border-accent/30'
          : 'bg-border/40 text-muted border border-border'
      }`}
    >
      {text}
    </span>
  );
}

function formatTime(ts: number): string {
  const d = new Date(ts);
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  const s = String(d.getSeconds()).padStart(2, '0');
  const ms = String(d.getMilliseconds()).padStart(3, '0');
  return `${h}:${m}:${s}.${ms}`;
}
