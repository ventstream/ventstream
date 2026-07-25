// Subject grammar — must match `crates/ventstream-protocol/src/subject.rs`.

const STRICT = /^[a-z0-9_-]+$/;
const ENTITY_ID = /^[A-Za-z0-9_-]+$/;
const MAX_STRICT = 64;
const MAX_ENTITY_ID = 128;

export class ProtocolError extends Error {
  constructor(
    public readonly field: string,
    message: string,
  ) {
    super(`${field}: ${message}`);
    this.name = "ProtocolError";
  }
}

function checkStrict(field: string, value: string): void {
  if (value.length === 0) {
    throw new ProtocolError(field, "must be non-empty");
  }
  if (value.length > MAX_STRICT) {
    throw new ProtocolError(field, `max ${MAX_STRICT} chars`);
  }
  if (!STRICT.test(value)) {
    throw new ProtocolError(field, "only [a-z0-9_-] allowed");
  }
}

function checkEntityId(field: string, value: string): void {
  if (value.length === 0) {
    throw new ProtocolError(field, "must be non-empty");
  }
  if (value.length > MAX_ENTITY_ID) {
    throw new ProtocolError(field, `max ${MAX_ENTITY_ID} chars`);
  }
  if (!ENTITY_ID.test(value)) {
    throw new ProtocolError(field, "only [A-Za-z0-9_-] allowed");
  }
}

/**
 * Build the canonical subject for a published event.
 *
 * Format: `vs.t.{tenant}.{event}.{entity_id}`.
 *
 * Throws `ProtocolError` if any component violates the grammar.
 */
export function buildSubject(args: {
  tenant: string;
  domain: string;
  entityKind: string;
  entityId: string;
  action: string;
}): string {
  checkStrict("tenant", args.tenant);
  checkStrict("domain", args.domain);
  checkStrict("entity.kind", args.entityKind);
  checkEntityId("entity.id", args.entityId);
  checkStrict("action", args.action);
  const event = buildEventType({
    domain: args.domain,
    entityKind: args.entityKind,
    action: args.action,
  });
  return `vs.t.${args.tenant}.${event}.${args.entityId}`;
}

/**
 * Build the envelope `event` field: `{domain}.{entityKind}.{action}`.
 */
export function buildEventType(args: {
  domain: string;
  entityKind: string;
  action: string;
}): string {
  checkStrict("domain", args.domain);
  checkStrict("entity.kind", args.entityKind);
  checkStrict("action", args.action);
  return `${args.domain}.${args.entityKind}.${args.action}`;
}
