import { useState } from 'react';
import { usePublisherControl } from './usePublisherControl';

interface Props {
  orderId: string;
  onOrderIdChange: (s: string) => void;
  active: boolean;
  onToggle: () => void;
}

const RATE_PRESETS = [0.5, 1, 2, 5, 10, 25];
const DURATION_PRESETS = [30, 60, 120, 300, 600];

export function Controls({ orderId, onOrderIdChange, active, onToggle }: Props) {
  const { status, error, start, stop } = usePublisherControl();
  const [rate, setRate] = useState<number>(2);
  const [duration, setDuration] = useState<number>(60);

  const runningElapsed =
    status.running && status.startedAt
      ? Math.max(0, Math.floor(Date.now() / 1000 - status.startedAt))
      : 0;

  return (
    <div className="bg-panel border border-border rounded-lg p-5 space-y-4">
      <div className="text-[10px] uppercase tracking-wider text-muted font-medium">Controls</div>

      {/* Subscription side */}
      <div className="flex flex-wrap items-end gap-4">
        <label className="flex flex-col gap-1.5">
          <span className="text-xs text-muted">Subscribe to orderId</span>
          <input
            type="text"
            value={orderId}
            onChange={(e) => onOrderIdChange(e.target.value)}
            className="bg-bg border border-border rounded-md px-3 py-2 text-sm font-mono w-64 focus:outline-none focus:border-accent"
          />
        </label>
        <button
          onClick={onToggle}
          className={`px-4 py-2 rounded-md font-medium text-sm transition-colors ${
            active
              ? 'bg-bad/10 border border-bad/40 text-bad hover:bg-bad/20'
              : 'bg-accent/10 border border-accent/40 text-accent hover:bg-accent/20'
          }`}
        >
          {active ? 'Unsubscribe' : 'Subscribe'}
        </button>
      </div>

      <div className="border-t border-border" />

      {/* Publisher side */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-muted font-medium mb-3">
          Publisher
        </div>
        <div className="flex flex-wrap items-end gap-4">
          <label className="flex flex-col gap-1.5">
            <span className="text-xs text-muted">Rate (events / sec)</span>
            <select
              value={rate}
              onChange={(e) => setRate(parseFloat(e.target.value))}
              disabled={status.running}
              className="bg-bg border border-border rounded-md px-3 py-2 text-sm font-mono w-40 disabled:opacity-50 focus:outline-none focus:border-accent2"
            >
              {RATE_PRESETS.map((r) => (
                <option key={r} value={r}>
                  {r} / sec
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1.5">
            <span className="text-xs text-muted">Duration</span>
            <select
              value={duration}
              onChange={(e) => setDuration(parseInt(e.target.value, 10))}
              disabled={status.running}
              className="bg-bg border border-border rounded-md px-3 py-2 text-sm font-mono w-40 disabled:opacity-50 focus:outline-none focus:border-accent2"
            >
              {DURATION_PRESETS.map((d) => (
                <option key={d} value={d}>
                  {d >= 60 ? `${d / 60} min` : `${d} s`}
                </option>
              ))}
            </select>
          </label>

          {!status.running ? (
            <button
              onClick={() => start(rate, duration, orderId)}
              className="px-5 py-2 rounded-md font-medium text-sm bg-accent2/15 border border-accent2/50 text-accent2 hover:bg-accent2/25 transition-colors flex items-center gap-2"
            >
              <span>▶</span>
              <span>Start publishing</span>
            </button>
          ) : (
            <button
              onClick={() => stop()}
              className="px-5 py-2 rounded-md font-medium text-sm bg-bad/15 border border-bad/50 text-bad hover:bg-bad/25 transition-colors flex items-center gap-2"
            >
              <span>■</span>
              <span>Stop publishing</span>
            </button>
          )}

          <div className="ml-auto text-xs">
            {status.running ? (
              <span className="text-accent2">
                <span className="inline-block w-2 h-2 bg-accent2 rounded-full animate-pulse mr-2 align-middle" />
                publishing &middot; {status.rate} / sec &middot; {runningElapsed}s / {status.duration}s
              </span>
            ) : (
              <span className="text-muted">publisher idle</span>
            )}
          </div>
        </div>
        {error && <p className="mt-2 text-xs text-bad">control-plane error: {error}</p>}
      </div>
    </div>
  );
}
