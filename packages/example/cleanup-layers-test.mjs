// Integration test for the three-layer JetStream consumer cleanup
// hierarchy. Run against an engine in JetStream mode with short
// inactive-threshold and reaper-interval, e.g.:
//
//   VS_WS_JETSTREAM=1
//   VS_WS_JS_POD_ID=podA
//   VS_WS_JS_INACTIVE_THRESHOLD_MS=2000
//   VS_WS_JS_REAPER_INTERVAL_MS=1000
//
// What we assert (per layer):
//
//   Layer 1 (RAII drop guard, ~ms):
//     graceful WS close → consumer gone within ~500ms
//   Layer 2 (inactive_threshold, ~5min in prod / 2s in this test):
//     create a consumer with no client pulling from it (simulating
//     a kill -9'd pod whose drop guard never ran) → NATS deletes
//     after the threshold elapses.
//   Layer 3 (reaper, ~60s in prod / 1s here):
//     create a consumer with this pod's owned-marker prefix but no
//     active connection → reaper sweeps it within reaper_interval.

import { connect as natsConnect } from "nats";
import { VentStreamClient } from "@ventstream/client";

const NATS_URL = process.env.VS_NATS_URL || "nats://127.0.0.1:4222";
const WS_URL = process.env.VS_WS_URL || "ws://127.0.0.1:4040/ws";
const STREAM = process.env.VS_WS_JS_STREAM || "ventstream";
const POD_ID = process.env.VS_WS_JS_POD_ID || "podA";
const INACTIVE_MS = Number(process.env.VS_WS_JS_INACTIVE_THRESHOLD_MS || 2000);
const REAPER_MS = Number(process.env.VS_WS_JS_REAPER_INTERVAL_MS || 1000);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
const record = (name, ok, detail) => {
  results.push({ name, ok, detail });
  console.log(`${ok ? "✅" : "❌"} ${name}${detail ? "  — " + detail : ""}`);
};

const nc = await natsConnect({ servers: NATS_URL });
const jsm = await nc.jetstreamManager();

async function listConsumers() {
  const names = [];
  const lister = jsm.consumers.list(STREAM);
  for await (const c of lister) names.push(c.name);
  return names;
}

async function countOurs() {
  const all = await listConsumers();
  return all.filter((n) => n.includes(`-p-${POD_ID}-c-`)).length;
}

// ---------------------------------------------------------------
// Layer 1: graceful WS close → drop guard deletes consumer
// ---------------------------------------------------------------
async function layer1() {
  const name = "L1: graceful close → consumer deleted in <500ms";
  const before = await countOurs();
  const c = new VentStreamClient({
    url: WS_URL,
    tenant: "acme",
    token: "demo",
  });
  await c.connect();
  // Subscribe so the consumer is fully set up.
  c.subscribe("orders.>", () => {});
  await sleep(200);
  const peak = await countOurs();
  const created = peak === before + 1;
  await c.close();
  await sleep(500);
  const after = await countOurs();
  const cleaned = after === before;
  record(
    name,
    created && cleaned,
    `before=${before} peak=${peak} after=${after}`,
  );
}

// ---------------------------------------------------------------
// Layer 2: inactive_threshold deletes a consumer with no puller
// ---------------------------------------------------------------
async function layer2() {
  const name = `L2: inactive_threshold (~${INACTIVE_MS}ms)`;
  const before = await countOurs();
  // Create a consumer that nobody will pull from. Name it so the
  // reaper would also delete it — but we want to test that NATS
  // itself deletes it via inactive_threshold *before* the reaper
  // runs. To isolate Layer 2 from Layer 3, use a name that does
  // NOT include the pod marker (so the reaper skips it).
  const consumerName = `vs-t-acme-isolated-test-${Date.now()}`;
  await jsm.consumers.add(STREAM, {
    durable_name: consumerName,
    filter_subject: "vs.t.acme.>",
    deliver_policy: "new",
    ack_policy: "explicit",
    inactive_threshold: INACTIVE_MS * 1_000_000, // ns
  });
  // Verify it exists right after creation.
  const justCreated = (await listConsumers()).includes(consumerName);
  // Wait past inactive_threshold + slack.
  await sleep(INACTIVE_MS + 1000);
  const gone = !(await listConsumers()).includes(consumerName);
  record(
    name,
    justCreated && gone,
    `created=${justCreated} deleted_after_threshold=${gone}`,
  );
}

// ---------------------------------------------------------------
// Layer 3: reaper sweeps pod-owned consumer with no active conn
// ---------------------------------------------------------------
async function layer3() {
  const name = `L3: reaper sweeps orphan (~${REAPER_MS}ms)`;
  // Manually create a consumer that LOOKS like it belongs to this
  // pod (so the reaper considers it) but with a fake connection_id
  // (so the in-process registry never knows about it). Set a long
  // inactive_threshold to ensure NATS doesn't delete it first.
  const fakeConnId = "01HFAKE0FAKE0FAKE0FAKE0FAKE";
  const orphanName = `vs-t-acme-p-${POD_ID}-c-${fakeConnId}`;
  await jsm.consumers.add(STREAM, {
    durable_name: orphanName,
    filter_subject: "vs.t.acme.>",
    deliver_policy: "new",
    ack_policy: "explicit",
    inactive_threshold: 60 * 60 * 1_000_000_000, // 1 hour
  });
  const justCreated = (await listConsumers()).includes(orphanName);
  // Wait two reaper intervals (with slack).
  await sleep(REAPER_MS * 2 + 500);
  const gone = !(await listConsumers()).includes(orphanName);
  record(
    name,
    justCreated && gone,
    `created=${justCreated} reaped=${gone}`,
  );
  // Defensive cleanup if the reaper missed it.
  if (!gone) {
    try {
      await jsm.consumers.delete(STREAM, orphanName);
    } catch {}
  }
}

await layer1();
await layer2();
await layer3();

await nc.drain();

const failures = results.filter((r) => !r.ok);
console.log(
  `\n${results.length - failures.length}/${results.length} cleanup layers verified`,
);
process.exit(failures.length === 0 ? 0 : 1);
