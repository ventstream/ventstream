import type { Event } from "../src/envelope.js";

type ExpectedEvent = {
  id: string;
  event: string;
  tenant: string;
  entity_id: string;
  actor?: { kind: string; id: string };
  occurred_at: string;
  received_at: string;
  schema_version: number;
  data: unknown;
  metadata: {
    trace_id?: string;
    correlation_id?: string;
    causation_id?: string;
  };
};

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends
  (<T>() => T extends B ? 1 : 2) ? true : false;
type Assert<T extends true> = T;

type ProtocolV2EventMatches = Assert<Equal<Event, ExpectedEvent>>;

export type { ProtocolV2EventMatches };
