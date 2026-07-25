import { useState } from 'react';
import { Header } from './components/Header';
import { ConnectionStatus } from './components/ConnectionStatus';
import { PipelineDiagram } from './components/PipelineDiagram';
import { Stats } from './components/Stats';
import { EventLog } from './components/EventLog';
import { Controls } from './components/Controls';
import { useSubscription } from './components/useSubscription';
import { usePublisher } from './components/usePublisher';
import { CdcPanel } from './components/CdcPanel';

export default function App() {
  const [orderId, setOrderId] = useState('ORD_TEST_1');
  const [active, setActive] = useState(true);
  const { events, eventsPerSec, total, lastLatencyMs } = useSubscription(orderId, active);
  const { sessionPublished, publishedPerSec } = usePublisher('vsws');

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <main className="flex-1 max-w-[1400px] w-full mx-auto px-6 py-6 space-y-6">
        <CdcPanel />

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <ConnectionStatus className="lg:col-span-1" />
          <Stats
            className="lg:col-span-2"
            total={total}
            eventsPerSec={eventsPerSec}
            lastLatencyMs={lastLatencyMs}
            publishedSession={sessionPublished}
            publishedPerSec={publishedPerSec}
            active={active}
          />
        </div>

        <PipelineDiagram active={active} pulseTrigger={events[0]?.ts ?? null} />

        <Controls
          orderId={orderId}
          onOrderIdChange={setOrderId}
          active={active}
          onToggle={() => setActive((a) => !a)}
        />

        <EventLog events={events} />
      </main>
      <footer className="border-t border-border px-6 py-3 text-xs text-muted flex justify-between">
        <span>VentStream Demo · graphql-transport-ws · Apollo Client</span>
        <span className="font-mono">{import.meta.env.VITE_VS_WS ?? 'ws://127.0.0.1:8092/graphql/ws'}</span>
      </footer>
    </div>
  );
}
