// Envelope types — duplicated from @ventstream/sdk so the client
// package has no Node-only dependencies. When the envelope schema
// evolves, update both packages in lockstep.

export const SCHEMA_VERSION = 2;

export interface Entity {
  kind: string;
  id: string;
}

export interface Actor {
  kind: string;
  id: string;
}

export interface Metadata {
  trace_id?: string;
  correlation_id?: string;
  causation_id?: string;
}

export interface Event {
  id: string;
  event: string;
  tenant: string;
  entity_id: string;
  actor?: Actor;
  occurred_at: string;
  received_at: string;
  schema_version: number;
  data: unknown;
  metadata: Metadata;
}
