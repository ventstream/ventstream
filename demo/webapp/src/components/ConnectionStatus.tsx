import { useEffect, useState } from 'react';
import { wsState, subscribeWs, type WsState } from '../apollo';

interface Props {
  className?: string;
}

const COPY: Record<WsState['status'], { label: string; color: string; dot: string }> = {
  connecting: { label: 'Connecting…', color: 'text-accent2', dot: 'bg-accent2' },
  connected: { label: 'Live', color: 'text-good', dot: 'bg-good pulse-live' },
  closed: { label: 'Disconnected', color: 'text-bad', dot: 'bg-bad' },
  error: { label: 'Error', color: 'text-bad', dot: 'bg-bad' },
};

export function ConnectionStatus({ className = '' }: Props) {
  const [s, setS] = useState<WsState>({ ...wsState });
  useEffect(() => subscribeWs(() => setS({ ...wsState })), []);
  const c = COPY[s.status];
  return (
    <div className={`bg-panel border border-border rounded-lg p-5 ${className}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted font-medium">WebSocket</div>
      <div className="mt-3 flex items-center gap-3">
        <div className={`w-3 h-3 rounded-full ${c.dot}`} />
        <span className={`text-2xl font-semibold ${c.color}`}>{c.label}</span>
      </div>
      <div className="mt-3 text-xs text-muted">
        Reconnect attempts: <span className="font-mono text-fg">{s.attempts}</span>
      </div>
    </div>
  );
}
