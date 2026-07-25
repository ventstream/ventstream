import { useEffect, useState } from 'react';

interface Props {
  active: boolean;
  /** Timestamp of the most recent event; used to trigger flow animation. */
  pulseTrigger: number | null;
}

export function PipelineDiagram({ active, pulseTrigger }: Props) {
  const [animKey, setAnimKey] = useState(0);

  useEffect(() => {
    if (pulseTrigger == null) return;
    setAnimKey((k) => k + 1);
  }, [pulseTrigger]);

  return (
    <div className="bg-panel border border-border rounded-lg p-6">
      <div className="text-[10px] uppercase tracking-wider text-muted font-medium mb-4">
        Pipeline · NATS → Gateway → Apollo
      </div>
      <div className="flex items-center justify-between gap-4">
        <Node label="Publisher" sub="NATS publish" />
        <Lane key={`p1-${animKey}`} run={active} />
        <Node label="JetStream" sub="vsws stream" highlight />
        <Lane key={`p2-${animKey}`} run={active} />
        <Node label="VentStream" sub="graphql role" highlight />
        <Lane key={`p3-${animKey}`} run={active} />
        <Node label="Apollo Client" sub="this browser" accent />
      </div>
      <div className="mt-4 text-[11px] text-muted text-center">
        {active
          ? 'Each event you see in the log below flowed through every hop above in real time.'
          : 'Subscription paused — events continue to be retained in JetStream until you resume.'}
      </div>
    </div>
  );
}

function Node({
  label,
  sub,
  highlight = false,
  accent = false,
}: {
  label: string;
  sub: string;
  highlight?: boolean;
  accent?: boolean;
}) {
  const ring = accent
    ? 'border-accent2 shadow-[0_0_0_3px_rgba(249,115,22,0.15)]'
    : highlight
    ? 'border-accent shadow-[0_0_0_3px_rgba(94,234,212,0.10)]'
    : 'border-border';
  return (
    <div className={`flex-shrink-0 px-4 py-3 rounded-lg border ${ring} bg-bg text-center min-w-[112px]`}>
      <div className={`text-sm font-semibold ${accent ? 'text-accent2' : highlight ? 'text-accent' : 'text-fg'}`}>
        {label}
      </div>
      <div className="text-[10px] text-muted mt-0.5">{sub}</div>
    </div>
  );
}

function Lane({ run }: { run: boolean }) {
  return (
    <div className="flex-1 relative h-px bg-border overflow-hidden">
      {run && (
        <div className="absolute inset-y-0 -top-[3px] left-0 w-2 h-2 rounded-full bg-accent flow-dot" />
      )}
    </div>
  );
}
