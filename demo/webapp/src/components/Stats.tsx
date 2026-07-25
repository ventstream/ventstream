interface Props {
  className?: string;
  total: number;
  eventsPerSec: number;
  lastLatencyMs: number | null;
  publishedSession: number;
  publishedPerSec: number;
  active: boolean;
}

export function Stats({
  className = '',
  total,
  eventsPerSec,
  lastLatencyMs,
  publishedSession,
  publishedPerSec,
  active,
}: Props) {
  return (
    <div className={`bg-panel border border-border rounded-lg p-5 ${className}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted font-medium">
        Publisher &harr; Subscriber
      </div>

      <div className="mt-3 grid grid-cols-2 gap-4">
        <Side
          label="Publisher"
          accent="accent-2"
          stats={[
            { label: 'Published (this session)', value: publishedSession.toLocaleString() },
            { label: 'Publish rate', value: `${publishedPerSec.toFixed(1)} / sec` },
          ]}
        />
        <Side
          label="Subscriber"
          accent="accent"
          stats={[
            { label: 'Received', value: total.toLocaleString() },
            { label: 'Receive rate', value: `${eventsPerSec.toFixed(1)} / sec` },
            { label: 'Last latency', value: lastLatencyMs == null ? '—' : `${lastLatencyMs.toFixed(0)} ms` },
          ]}
        />
      </div>

      <div className="mt-4 text-xs text-muted">
        Subscription:&nbsp;
        <span className={`font-mono ${active ? 'text-accent' : 'text-bad'}`}>
          {active ? 'orderStatusChanged' : 'paused'}
        </span>
      </div>
    </div>
  );
}

function Side({
  label,
  accent,
  stats,
}: {
  label: string;
  accent: 'accent' | 'accent-2';
  stats: { label: string; value: string }[];
}) {
  const color = accent === 'accent' ? 'text-accent' : 'text-accent2';
  const border = accent === 'accent' ? 'border-l-accent' : 'border-l-accent2';
  return (
    <div className={`pl-3 border-l-2 ${border}`}>
      <div className={`text-[10px] uppercase tracking-wider font-semibold ${color}`}>{label}</div>
      <div className="mt-2 space-y-1.5">
        {stats.map((s) => (
          <div key={s.label} className="flex items-baseline justify-between gap-3">
            <span className="text-[11px] text-muted">{s.label}</span>
            <span className={`font-mono tabular-nums font-semibold ${color}`}>{s.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
