"""Tiny stand-in for Capo's `POST /amc/webhook` listener.

The real listener (Task #10) is built on FastAPI per §5.2. Until that exists,
this stub is enough to exercise the harness's HMAC-verification path and the
shape of the 204 / 401 / 400 responses. It deliberately implements ONLY the
behaviors S-4 needs to probe:

- HMAC verification before body parse → 401 on mismatch.
- Missing `X-AMC-Delivery-Id` → 400 (this is the harness's *expected* behavior;
  see findings doc Section 6 for the open question against real AMC).
- In-memory dedupe LRU keyed on `X-AMC-Delivery-Id` covering up to 15 min.
- 204 No Content on accept / dedupe.

This stub is NOT a substitute for the real listener. It exists so the harness
itself can be unit-tested and so a contributor can run an end-to-end smoke run
on a laptop with no AMC and no Capo.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any


class DedupeLRU:
    """Tiny LRU keyed by delivery-id with a TTL (default 15 min).

    Spec reference §5.2: dedupe must cover ≥15 minutes of AMC retries.
    """

    def __init__(self, ttl_seconds: int = 15 * 60, max_entries: int = 10_000) -> None:
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._items: OrderedDict[str, float] = OrderedDict()

    def seen(self, delivery_id: str) -> bool:
        now = time.monotonic()
        self._expire(now)
        if delivery_id in self._items:
            self._items.move_to_end(delivery_id)
            return True
        self._items[delivery_id] = now
        if len(self._items) > self.max_entries:
            self._items.popitem(last=False)
        return False

    def _expire(self, now: float) -> None:
        cutoff = now - self.ttl
        while self._items:
            k, t = next(iter(self._items.items()))
            if t < cutoff:
                self._items.popitem(last=False)
            else:
                break


def make_handler(secret: bytes, dedupe: DedupeLRU) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _empty(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        # NOTE: do NOT log message bodies — spec §5.2 forbids logging before
        # signature verification. The default BaseHTTPRequestHandler log
        # writes only the request line, which is acceptable.

        def do_POST(self) -> None:  # noqa: N802 — stdlib signature
            if self.path != "/amc/webhook":
                self._empty(404)
                return

            length = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(length) if length else b""

            signature_header = self.headers.get("X-AMC-Signature", "")
            if not signature_header.startswith("sha256="):
                # Missing/malformed signature → 401, no parse, no log of body.
                self._json(401, {"error": {"code": "BAD_SIGNATURE",
                                           "message": "X-AMC-Signature missing or malformed"}})
                return

            provided = signature_header.removeprefix("sha256=")
            expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(provided, expected):
                self._json(401, {"error": {"code": "BAD_SIGNATURE",
                                           "message": "X-AMC-Signature does not match body HMAC"}})
                return

            delivery_id = self.headers.get("X-AMC-Delivery-Id")
            if not delivery_id:
                # Open question per findings doc: real AMC behavior TBD. Stub
                # rejects to make the failure observable in tests.
                self._json(400, {"error": {"code": "VALIDATION_ERROR",
                                           "message": "X-AMC-Delivery-Id is required"}})
                return

            # Dedupe check. Spec §5.2 — duplicates within window return 204
            # and are NOT enqueued.
            already_seen = dedupe.seen(delivery_id)
            # In the real listener we'd enqueue here when not already_seen.
            self._empty(204)
            return

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser(description="S-4 stub Capo listener")
    ap.add_argument("--secret", required=True, help="AMC_WEBHOOK_SECRET")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--dedupe-ttl-seconds", type=int, default=15 * 60)
    args = ap.parse_args()

    dedupe = DedupeLRU(ttl_seconds=args.dedupe_ttl_seconds)
    handler = make_handler(args.secret.encode("utf-8"), dedupe)
    server = HTTPServer((args.host, args.port), handler)
    print(f"[stub] listening on http://{args.host}:{args.port}/amc/webhook (dedupe TTL {args.dedupe_ttl_seconds}s)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[stub] shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":  # pragma: no cover
    main()
