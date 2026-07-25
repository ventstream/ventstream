import { connect as connectNats, StringCodec } from "nats";
import WebSocket from "ws";

const transport = process.env.VS_BENCH_TRANSPORT || "raw";
const provider = process.env.VS_BENCH_PROVIDER || "nats_core";
const url = process.env.VS_BENCH_WS_URL || "ws://127.0.0.1:4040/ws";
const natsUrl = process.env.VS_BENCH_NATS_URL || "nats://127.0.0.1:4222";
const redisUrl = process.env.VS_BENCH_REDIS_URL || "redis://127.0.0.1:6379";
const clients = Number.parseInt(process.env.VS_BENCH_CLIENTS || "1", 10);
const events = Number.parseInt(process.env.VS_BENCH_EVENTS || "10000", 10);
const payloadBytes = Number.parseInt(process.env.VS_BENCH_PAYLOAD_BYTES || "256", 10);
const timeoutMs = Number.parseInt(process.env.VS_BENCH_TIMEOUT_MS || "180000", 10);
const tenant = "acme";
const payload = "x".repeat(payloadBytes);
const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

if (!Number.isSafeInteger(clients) || clients <= 0 || !Number.isSafeInteger(events) || events <= 0) {
  throw new Error("clients and events must be positive safe integers");
}

function eventId(sequence) {
  let value = BigInt(sequence);
  let encoded = "";
  while (value > 0n) {
    encoded = alphabet[Number(value % 32n)] + encoded;
    value /= 32n;
  }
  return (`0${encoded.padStart(25, "0")}`).slice(-26);
}

function envelope(sequence) {
  return {
    id: eventId(sequence),
    event: "benchmark.event",
    tenant,
    entity_id: String(sequence),
    occurred_at: "2026-07-20T00:00:00.000Z",
    received_at: "2026-07-20T00:00:00.000Z",
    schema_version: 2,
    data: { sequence: String(sequence), payload },
    metadata: {},
  };
}

class BenchmarkConnection {
  constructor(index) {
    this.index = index;
    this.socket = undefined;
    this.received = 0;
    this.lastSequence = 0;
    this.gaps = 0;
    this.duplicates = 0;
    this.resolveDone = undefined;
    this.rejectDone = undefined;
    this.done = new Promise((resolve, reject) => {
      this.resolveDone = resolve;
      this.rejectDone = reject;
    });
  }

  async connect() {
    const protocols = transport === "graphql" ? ["graphql-transport-ws"] : undefined;
    this.socket = new WebSocket(url, protocols);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`connection ${this.index} timed out`)), 15000);
      this.socket.once("error", reject);
      this.socket.once("open", () => {
        if (transport === "graphql") {
          this.socket.send(JSON.stringify({
            type: "connection_init",
            payload: { tenant, authToken: "benchmark" },
          }));
        } else {
          this.socket.send(JSON.stringify({ type: "hello", tenant, token: "benchmark" }));
        }
      });
      this.socket.on("message", (raw) => {
        let message;
        try { message = JSON.parse(String(raw)); } catch { return; }
        const ready = transport === "graphql"
          ? message.type === "connection_ack"
          : message.type === "ready";
        if (ready) {
          if (transport === "graphql") {
            this.socket.send(JSON.stringify({
              id: `bench-${this.index}`,
              type: "subscribe",
              payload: {
                query: "subscription { benchmarkEvents { id sequence payload } }",
              },
            }));
          } else {
            this.socket.send(JSON.stringify({
              type: "subscribe",
              id: `bench-${this.index}`,
              pattern: "benchmark.event.>",
            }));
          }
          clearTimeout(timer);
          resolve();
          return;
        }
        this.onMessage(message);
      });
      this.socket.on("error", (error) => this.rejectDone(error));
      this.socket.on("close", (code, reason) => {
        if (this.received < events) {
          this.rejectDone(new Error(`connection ${this.index} closed ${code}: ${String(reason)}`));
        }
      });
    });
  }

  onMessage(message) {
    let sequence;
    if (transport === "graphql" && message.type === "next") {
      sequence = message.payload?.data?.benchmarkEvents?.sequence;
    } else if (transport === "raw" && message.type === "event") {
      sequence = message.event?.data?.sequence;
    } else if (message.type === "error") {
      this.rejectDone(new Error(`connection ${this.index}: ${JSON.stringify(message)}`));
      return;
    } else {
      return;
    }
    const current = Number.parseInt(String(sequence), 10);
    if (!Number.isSafeInteger(current)) {
      this.rejectDone(new Error(
        `connection ${this.index} received invalid sequence ${sequence}: ${JSON.stringify(message)}`,
      ));
      return;
    }
    if (current <= this.lastSequence) this.duplicates += 1;
    else if (this.lastSequence > 0 && current !== this.lastSequence + 1) this.gaps += current - this.lastSequence - 1;
    this.lastSequence = Math.max(this.lastSequence, current);
    this.received += 1;
    if (this.received === events) this.resolveDone();
  }

  close() {
    if (this.socket?.readyState === WebSocket.OPEN) this.socket.close(1000, "benchmark complete");
  }
}

async function publishNats() {
  const nc = await connectNats({ servers: natsUrl, name: "ventstream-realtime-benchmark" });
  const codec = StringCodec();
  for (let i = 1; i <= events; i += 1) {
    nc.publish(`vs.t.${tenant}.benchmark.event.${i}`, codec.encode(JSON.stringify(envelope(i))));
    if (i % 10000 === 0) await nc.flush();
  }
  await nc.flush();
  await nc.close();
}

async function publishRedis() {
  const { createClient: createRedisClient } = await import("../sdk/node_modules/redis/dist/index.js");
  const redis = createRedisClient({ url: redisUrl });
  await redis.connect();
  const key = `ventstream:{${tenant}}:events`;
  for (let start = 1; start <= events; start += 1000) {
    const multi = redis.multi();
    const end = Math.min(events, start + 999);
    for (let i = start; i <= end; i += 1) {
      multi.xAdd(key, "*", {
        subject: `vs.t.${tenant}.benchmark.event.${i}`,
        event: JSON.stringify(envelope(i)),
      });
    }
    await multi.exec();
  }
  await redis.quit();
}

const connections = Array.from({ length: clients }, (_, index) => new BenchmarkConnection(index));
for (let offset = 0; offset < connections.length; offset += 25) {
  await Promise.all(connections.slice(offset, offset + 25).map((connection) => connection.connect()));
}
// GraphQL consumers and Redis tenant tailers are asynchronous after the
// protocol-level acknowledgement; let them install broker subscriptions.
await new Promise((resolve) => setTimeout(resolve, 1000));

const started = process.hrtime.bigint();
if (provider === "redis_streams") await publishRedis();
else await publishNats();

await Promise.race([
  Promise.all(connections.map((connection) => connection.done)),
  new Promise((_, reject) => setTimeout(() => reject(new Error("delivery timeout")), timeoutMs)),
]);
const elapsedSeconds = Number(process.hrtime.bigint() - started) / 1e9;
const received = connections.reduce((sum, connection) => sum + connection.received, 0);
const gaps = connections.reduce((sum, connection) => sum + connection.gaps, 0);
const duplicates = connections.reduce((sum, connection) => sum + connection.duplicates, 0);
for (const connection of connections) connection.close();

console.log(JSON.stringify({
  transport,
  provider,
  clients,
  events,
  received,
  elapsed_seconds: elapsedSeconds,
  published_eps: events / elapsedSeconds,
  deliveries_eps: received / elapsedSeconds,
  gaps,
  duplicates,
}));
process.exit(0);
