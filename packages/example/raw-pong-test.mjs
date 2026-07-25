// Raw-TCP/WebSocket test for the engine's pong-timeout behavior.
//
// Why raw: the `ws` library and every browser WebSocket implementation
// auto-respond to Ping frames with Pong (RFC 6455 §5.5.2 makes that
// the standard behavior). To exercise the server's pong-timeout path
// we need a client that *refuses* to send pongs. This is the only way
// to verify that an idle network-partitioned client gets cleaned up
// by the engine without waiting for OS-level TCP keepalive.
//
// What we do:
//   1. Open a TCP socket to the gateway.
//   2. Send the WebSocket HTTP/1.1 upgrade handshake.
//   3. Read frames; if we see a Ping, log it but do NOT reply.
//   4. Send a masked Hello text frame so the engine starts the
//      heartbeat.
//   5. Wait. The engine's pong_timeout deadline should fire and the
//      server should send a Close frame (and/or TCP FIN).
//   6. Assert this happens within (pong_timeout + ping_interval + slack).
//
// Run alongside an engine started with:
//   VS_WS_PING_INTERVAL_MS=200 VS_WS_PONG_TIMEOUT_MS=800 ./ventstream

import { createConnection } from "node:net";
import { randomBytes, createHash } from "node:crypto";

const HOST = process.env.VS_WS_HOST || "127.0.0.1";
const PORT = Number(process.env.VS_WS_PORT || 4040);
const PONG_TIMEOUT_MS = Number(process.env.VS_WS_PONG_TIMEOUT_MS || 800);
const PING_INTERVAL_MS = Number(process.env.VS_WS_PING_INTERVAL_MS || 200);
// Allow up to (timeout + interval + 500ms slack) for the close to land.
const DEADLINE_MS = PONG_TIMEOUT_MS + PING_INTERVAL_MS + 500;

// WebSocket frame opcodes (RFC 6455 §5.2).
const OP_TEXT = 0x1;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/** Build the Sec-WebSocket-Accept value from the client's key. */
function acceptValue(key) {
  return createHash("sha1").update(key + WS_GUID).digest("base64");
}

/**
 * Encode a masked text frame (RFC 6455 §5.3 — masking is mandatory
 * for client→server frames). Returns a Buffer ready to write to TCP.
 */
function maskedTextFrame(payloadStr) {
  const payload = Buffer.from(payloadStr, "utf8");
  const len = payload.length;

  // Header byte 0: FIN=1, RSV=000, opcode=text(1).
  const b0 = 0x80 | OP_TEXT;

  let header;
  if (len < 126) {
    header = Buffer.from([b0, 0x80 | len]); // MASK=1, len.
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = b0;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = b0;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  const mask = randomBytes(4);
  const masked = Buffer.alloc(len);
  for (let i = 0; i < len; i++) {
    masked[i] = payload[i] ^ mask[i % 4];
  }
  return Buffer.concat([header, mask, masked]);
}

/**
 * Decode one frame from the buffer. Returns {opcode, payload, consumed}
 * or null if a full frame isn't yet available.
 *
 * Server→client frames have MASK=0 (RFC 6455 §5.1), so we don't
 * unmask. We also don't support fragmentation here — every frame the
 * engine sends fits in one chunk.
 */
function tryDecodeFrame(buf) {
  if (buf.length < 2) return null;
  const b0 = buf[0];
  const b1 = buf[1];
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let len = b1 & 0x7f;
  let offset = 2;
  if (len === 126) {
    if (buf.length < offset + 2) return null;
    len = buf.readUInt16BE(offset);
    offset += 2;
  } else if (len === 127) {
    if (buf.length < offset + 8) return null;
    const big = buf.readBigUInt64BE(offset);
    len = Number(big);
    offset += 8;
  }
  if (masked) offset += 4; // skip mask key (won't happen from server)
  if (buf.length < offset + len) return null;
  const payload = buf.subarray(offset, offset + len);
  return { opcode, payload, consumed: offset + len };
}

function performHandshake(socket) {
  const key = randomBytes(16).toString("base64");
  const req =
    `GET /ws HTTP/1.1\r\n` +
    `Host: ${HOST}:${PORT}\r\n` +
    `Upgrade: websocket\r\n` +
    `Connection: Upgrade\r\n` +
    `Sec-WebSocket-Key: ${key}\r\n` +
    `Sec-WebSocket-Version: 13\r\n\r\n`;
  socket.write(req);
  return { key, expected: acceptValue(key) };
}

function main() {
  return new Promise((resolve) => {
    const socket = createConnection({ host: HOST, port: PORT });
    let buffer = Buffer.alloc(0);
    let handshakeDone = false;
    const events = {
      pingsSeen: 0,
      closeFrame: false,
      tcpFin: false,
      readyAcked: false,
      handshakeOk: false,
    };
    const start = Date.now();

    const finish = (ok, detail) => {
      const elapsed = Date.now() - start;
      console.log(
        `${ok ? "✅" : "❌"} raw pong-timeout test ` +
          `(elapsed=${elapsed}ms, pings=${events.pingsSeen}, ` +
          `closeFrame=${events.closeFrame}, fin=${events.tcpFin})` +
          (detail ? `  — ${detail}` : ""),
      );
      try {
        socket.destroy();
      } catch {}
      resolve(ok);
    };

    let handshakeKey;
    socket.on("connect", () => {
      handshakeKey = performHandshake(socket);
    });

    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      // Phase 1: parse HTTP response.
      if (!handshakeDone) {
        const sep = buffer.indexOf("\r\n\r\n");
        if (sep < 0) return;
        const headerText = buffer.subarray(0, sep).toString("utf8");
        buffer = buffer.subarray(sep + 4);
        const lines = headerText.split(/\r\n/);
        const status = lines[0];
        const accept = lines
          .map((l) => l.split(/:\s*/))
          .find((p) => p[0]?.toLowerCase() === "sec-websocket-accept");
        if (!status?.includes(" 101 ")) {
          return finish(false, `handshake non-101: ${status}`);
        }
        if (!accept || accept[1] !== handshakeKey.expected) {
          return finish(false, `handshake bad accept: ${accept?.[1]}`);
        }
        events.handshakeOk = true;
        handshakeDone = true;
        // Send Hello as a masked text frame.
        const hello = JSON.stringify({
          type: "hello",
          tenant: "acme",
          token: "demo",
        });
        socket.write(maskedTextFrame(hello));
      }

      // Phase 2: decode WS frames. We deliberately DO NOT respond to
      // pings — that's the whole point of the test.
      while (true) {
        const frame = tryDecodeFrame(buffer);
        if (!frame) break;
        buffer = buffer.subarray(frame.consumed);
        switch (frame.opcode) {
          case OP_TEXT: {
            const text = frame.payload.toString("utf8");
            try {
              const msg = JSON.parse(text);
              if (msg.type === "ready") events.readyAcked = true;
            } catch {
              /* ignore */
            }
            break;
          }
          case OP_PING:
            events.pingsSeen += 1;
            // Intentionally drop. RFC says we SHOULD reply with
            // Pong; we don't, to exercise the server timeout.
            break;
          case OP_PONG:
            break;
          case OP_CLOSE:
            events.closeFrame = true;
            // Don't reply — let TCP teardown happen.
            break;
          default:
            break;
        }
      }
    });

    socket.on("end", () => {
      events.tcpFin = true;
      const ok =
        events.handshakeOk &&
        events.readyAcked &&
        events.pingsSeen >= 1 &&
        (events.closeFrame || events.tcpFin);
      finish(ok, ok ? null : "missing one of: handshake, ready, ping, close");
    });

    socket.on("error", (err) => {
      finish(false, `socket error: ${err.message}`);
    });

    setTimeout(() => {
      if (!events.closeFrame && !events.tcpFin) {
        finish(
          false,
          `no close within ${DEADLINE_MS}ms (pong_timeout=${PONG_TIMEOUT_MS}ms, ping_interval=${PING_INTERVAL_MS}ms)`,
        );
      }
    }, DEADLINE_MS);
  });
}

const ok = await main();
process.exit(ok ? 0 : 1);
