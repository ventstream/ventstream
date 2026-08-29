#!/usr/bin/env python3
"""Reverse proxy that adds random per-request latency in front of OpenSearch.

Sink bulks issued in parallel only reorder if a later bulk can *finish* before
an earlier one. On a local single-node OpenSearch with sub-millisecond RTT the
requests arrive in send order and are applied in send order, so the hazard
never shows. Real deployments have network jitter, multi-node clusters and
uneven bulk sizes; this proxy stands in for that by holding each request for a
random 0..JITTER_MS before forwarding it, so bulks in flight overtake each
other exactly as they can in production.

    jitter-proxy.py <listen-port> <upstream-port> <jitter-ms>
"""
import http.client
import random
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LISTEN, UPSTREAM, JITTER_MS = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
TRACE = sys.argv[4] if len(sys.argv) > 4 else None
HOP = {"connection", "keep-alive", "transfer-encoding", "content-length", "host"}
ROUND = re.compile(rb'"round":(\d+)')
trace_lock = threading.Lock()


def trace(received, forwarded, body):
    """One line per bulk: receive time, forward time, and the `round` values
    the bulk carries — enough to count how often a later bulk was forwarded
    ahead of an earlier one (an overtake) after the fact."""
    if not TRACE:
        return
    found = ROUND.findall(body)
    rounds = sorted({int(m) for m in found})
    with trace_lock, open(TRACE, "a", encoding="utf-8") as out:
        out.write(f"{received:.6f} {forwarded:.6f} {rounds[0] if rounds else -1} {rounds[-1] if rounds else -1} {len(found)}\n")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def _forward(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        # Only writes carry the ordering hazard; delaying reads too would
        # just slow the harness's own polling.
        if body:
            received = time.time()
            time.sleep(random.uniform(0, JITTER_MS) / 1000.0)
            if b"_bulk" in self.path.encode() or self.path.endswith("/_bulk"):
                trace(received, time.time(), body)
        upstream = http.client.HTTPConnection("127.0.0.1", UPSTREAM, timeout=60)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        upstream.request(self.command, self.path, body=body, headers=headers)
        response = upstream.getresponse()
        payload = response.read()
        self.send_response(response.status)
        for key, value in response.getheaders():
            if key.lower() not in HOP:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        upstream.close()

    do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _forward


server = ThreadingHTTPServer(("127.0.0.1", LISTEN), Handler)
server.daemon_threads = True
threading.Thread(target=server.serve_forever, daemon=True).start()
print(f"jitter proxy :{LISTEN} -> :{UPSTREAM} (0..{JITTER_MS} ms per write)", flush=True)
try:
    while True:
        time.sleep(3600)
except KeyboardInterrupt:
    pass
