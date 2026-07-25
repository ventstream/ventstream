import { gql, useSubscription as useApolloSubscription } from '@apollo/client';
import { useEffect, useRef, useState } from 'react';

const ORDER_STATUS_CHANGED = gql`
  subscription OrderStatusChanged($orderId: ID!) {
    orderStatusChanged(orderId: $orderId) {
      orderId
      from
      to
      changedAt
    }
  }
`;

export interface Event {
  ts: number;
  orderId: string;
  from: string;
  to: string;
  changedAt: string;
}

const MAX_EVENTS = 100;

export function useSubscription(orderId: string, active: boolean) {
  const [events, setEvents] = useState<Event[]>([]);
  const [total, setTotal] = useState(0);
  const [eventsPerSec, setEventsPerSec] = useState(0);
  const [lastLatencyMs, setLastLatencyMs] = useState<number | null>(null);

  // Rolling 5-second buffer of receipt timestamps. Don't name this
  // `window` — that shadows the browser global and some bundlers
  // mishandle the closure.
  const recvTimes = useRef<number[]>([]);

  const { data } = useApolloSubscription(ORDER_STATUS_CHANGED, {
    variables: { orderId },
    skip: !active || !orderId,
  });

  useEffect(() => {
    const payload = data?.orderStatusChanged;
    if (!payload) return;
    const now = Date.now();
    const ev: Event = {
      ts: now,
      orderId: payload.orderId,
      from: payload.from,
      to: payload.to,
      changedAt: payload.changedAt,
    };
    setEvents((prev) => [ev, ...prev].slice(0, MAX_EVENTS));
    setTotal((t) => t + 1);
    const lat = now - Date.parse(payload.changedAt);
    if (Number.isFinite(lat)) setLastLatencyMs(lat);
    recvTimes.current.push(now);
  }, [data]);

  // Rate counter — recompute every 250 ms for a snappier display
  useEffect(() => {
    const id = setInterval(() => {
      const now = Date.now();
      const cutoff = now - 5_000;
      recvTimes.current = recvTimes.current.filter((t) => t >= cutoff);
      setEventsPerSec(recvTimes.current.length / 5);
    }, 250);
    return () => clearInterval(id);
  }, []);

  // Reset when subscription is toggled off
  useEffect(() => {
    if (!active) {
      setEventsPerSec(0);
      recvTimes.current = [];
    }
  }, [active]);

  return { events, total, eventsPerSec, lastLatencyMs };
}
