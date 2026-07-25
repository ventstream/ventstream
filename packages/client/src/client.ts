// The browser/Node WebSocket client.
//
// Wraps the gateway protocol with three layers of value over a raw
// WebSocket:
//
//   1. Subscription bookkeeping — install / replay-on-reconnect / remove
//   2. Auto-reconnect with jittered exponential backoff
//   3. Typed event dispatch — your handler sees a strongly-typed `Event`
//
// The client is intentionally event-emitter shaped (callbacks) rather
// than RxJS / async-iterator. Plain callbacks are the smallest API
// surface that works in both browser and Node without polyfills.

import WebSocket from "isomorphic-ws";

import type { Event } from "./envelope.js";
import type { ClientMessage, ServerMessage } from "./protocol.js";

export type EventHandler = (
  event: Event,
  ctx: { subject: string; cursor?: string; seq?: number },
) => void;

export interface Subscription {
  readonly id: string;
  readonly pattern: string;
  unsubscribe(): void;
}

export interface VentStreamClientConfig {
  /** WebSocket URL of the engine (e.g. `ws://localhost:4040/ws`). */
  url: string;
  /** Tenant scope for this connection. */
  tenant: string;
  /** Auth token sent in the Hello frame. */
  token: string;
  /** Last successfully processed provider-neutral cursor. */
  resumeCursor?: string;
  /** Reconnect on disconnect (default: true). */
  reconnect?: boolean;
  /** Initial reconnect delay (ms, default 500). Backs off up to 30s. */
  reconnectDelayMs?: number;
  /** Called on every fatal close (after all reconnect attempts). */
  onClose?: (reason: string) => void;
  /** Called on every error frame from the server. */
  onError?: (err: { code: string; message: string }) => void;
}

interface RegisteredSubscription {
  id: string;
  pattern: string;
  handler: EventHandler;
}

/**
 * The VentStream WebSocket client.
 *
 * ```ts
 * const client = new VentStreamClient({
 *   url: "ws://localhost:4040/ws",
 *   tenant: "acme",
 *   token: "demo",
 * });
 * await client.connect();
 *
 * const sub = client.subscribe("orders.order.status_changed.*", (event) => {
 *   console.log(event.entity_id, event.data);
 * });
 *
 * // later:
 * sub.unsubscribe();
 * await client.close();
 * ```
 */
export class VentStreamClient {
  private readonly cfg: Required<
    Pick<VentStreamClientConfig, "url" | "tenant" | "token">
  > &
    VentStreamClientConfig;
  private ws: WebSocket | undefined;
  private readyResolve: (() => void) | undefined;
  private readyPromise: Promise<void> | undefined;
  private readonly subs = new Map<string, RegisteredSubscription>();
  private closing = false;
  private subCounter = 0;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(cfg: VentStreamClientConfig) {
    this.cfg = {
      reconnect: true,
      reconnectDelayMs: 500,
      ...cfg,
    };
  }

  /** Open the WS connection and complete the Hello handshake. */
  async connect(): Promise<void> {
    if (this.readyPromise) return this.readyPromise;
    this.readyPromise = new Promise((resolve) => {
      this.readyResolve = resolve;
    });
    this.openSocket();
    return this.readyPromise;
  }

  /** Close the connection. Subsequent calls to `subscribe` will throw. */
  async close(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.ws?.close();
  }

  /**
   * Install a subscription. The handler is invoked for every event
   * matching the pattern (in unanchored form — the server prepends
   * the tenant from this connection's scope).
   */
  subscribe(pattern: string, handler: EventHandler): Subscription {
    const id = this.nextSubId();
    this.subs.set(id, { id, pattern, handler });
    this.sendIfOpen({ type: "subscribe", id, pattern });
    return {
      id,
      pattern,
      unsubscribe: () => {
        if (this.subs.delete(id)) {
          this.sendIfOpen({ type: "unsubscribe", id });
        }
      },
    };
  }

  // --- internals -----------------------------------------------------

  private openSocket(): void {
    const ws = new WebSocket(this.cfg.url);
    this.ws = ws;

    ws.addEventListener("open", () => {
      const hello: ClientMessage = {
        type: "hello",
        tenant: this.cfg.tenant,
        token: this.cfg.token,
        ...(this.cfg.resumeCursor !== undefined
          ? { resume_from_cursor: this.cfg.resumeCursor }
          : {}),
      };
      ws.send(JSON.stringify(hello));
    });

    ws.addEventListener("message", (evt: { data: unknown }) => {
      const raw = typeof evt.data === "string" ? evt.data : String(evt.data);
      let msg: ServerMessage;
      try {
        msg = JSON.parse(raw) as ServerMessage;
      } catch {
        return;
      }
      this.handleServerMessage(msg);
    });

    ws.addEventListener("close", () => {
      this.handleClose();
    });

    ws.addEventListener("error", () => {
      // The `close` listener will fire next; let the reconnect path
      // handle it. We don't surface raw socket errors — they tend to
      // be implementation noise.
    });
  }

  private handleServerMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case "ready": {
        this.reconnectAttempt = 0;
        // Replay existing subscriptions on (re)connect.
        for (const sub of this.subs.values()) {
          this.sendIfOpen({
            type: "subscribe",
            id: sub.id,
            pattern: sub.pattern,
          });
        }
        if (this.readyResolve) {
          this.readyResolve();
          this.readyResolve = undefined;
        }
        return;
      }
      case "event": {
        // Deliver to every locally-registered subscription whose
        // server-assigned id matches. The server already filtered by
        // pattern; we just dispatch.
        for (const matchedId of msg.matched) {
          const sub = this.subs.get(matchedId);
          if (sub) {
            try {
              sub.handler(msg.event, {
                subject: msg.subject,
                ...(msg.cursor !== undefined ? { cursor: msg.cursor } : {}),
                ...(msg.seq !== undefined ? { seq: msg.seq } : {}),
              });
            } catch {
              // Handler threw — don't take down the dispatch loop.
            }
          }
        }
        return;
      }
      case "error": {
        if (this.cfg.onError) {
          this.cfg.onError({ code: msg.code, message: msg.message });
        }
        return;
      }
      case "subscribed":
      case "unsubscribed":
      case "pong":
        // Acks — nothing to do at this layer. Observability layer can
        // be added if/when needed.
        return;
    }
  }

  private handleClose(): void {
    this.ws = undefined;
    if (this.closing) {
      this.cfg.onClose?.("closed by caller");
      return;
    }
    if (!this.cfg.reconnect) {
      this.cfg.onClose?.("connection closed (reconnect disabled)");
      return;
    }
    // Re-arm reconnect with jittered exponential backoff. Capped at 30s.
    const base = this.cfg.reconnectDelayMs ?? 500;
    const exp = Math.min(base * 2 ** this.reconnectAttempt, 30_000);
    const jitter = exp * (0.7 + Math.random() * 0.6);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.openSocket();
    }, jitter);
  }

  private sendIfOpen(msg: ClientMessage): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  private nextSubId(): string {
    this.subCounter += 1;
    return `c${this.subCounter}`;
  }
}
