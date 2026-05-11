"""HMAC-SHA256 signer matching the AMC → Capo webhook contract.

Spec reference: capo-SPEC.md §5.2, §7.4.

The AMC contract is:

    X-AMC-Signature: sha256=<lowercase hex of HMAC-SHA256(secret, raw_body)>
    X-AMC-Delivery-Id: <uuid v4>

Capo must compute the HMAC over the *raw* request body (bytes) and compare with
``hmac.compare_digest`` BEFORE parsing the body. This module produces signatures
that match that contract so the harness can drive both happy-path and
tampered-signature scenarios deterministically.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SignedRequest:
    """A fully-signed AMC-shaped request ready to POST."""

    body: bytes
    headers: dict[str, str]

    @property
    def delivery_id(self) -> str:
        return self.headers["X-AMC-Delivery-Id"]


def sign_body(secret: str, body: bytes) -> str:
    """Return the ``sha256=<hex>`` header value for ``body`` using ``secret``."""
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def make_envelope(
    text: str = "hi",
    channel_id: str = "+15551234567",
    sender_id: str = "+15557654321",
    message_id: str | None = None,
    ts: str = "2026-05-10T12:34:56Z",
    approval_id: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a canonical AMCInboundEnvelope. Spec §7.4."""
    env: dict[str, Any] = {
        "id": message_id or f"msg-{uuid.uuid4()}",
        "channel_id": channel_id,
        "sender_id": sender_id,
        "text": text,
        "ts": ts,
        "approval_id": approval_id,
    }
    env.update(extra)
    return env


def sign_request(
    secret: str,
    envelope: dict[str, Any],
    delivery_id: str | None = None,
    tamper_signature: bool = False,
    omit_delivery_id: bool = False,
) -> SignedRequest:
    """Build a signed POST /amc/webhook request.

    Parameters
    ----------
    secret:
        Shared HMAC secret (``AMC_WEBHOOK_SECRET`` in production).
    envelope:
        The dict that becomes the JSON body. Use :func:`make_envelope`.
    delivery_id:
        Override the ``X-AMC-Delivery-Id``. Defaults to a fresh UUIDv4 — pass an
        explicit value to drive the duplicate-delivery scenario.
    tamper_signature:
        When ``True``, return a signature that does NOT match the body (drives
        the 401 mismatch scenario). Implementation flips one nibble of the
        valid signature so the rest of the header shape stays intact.
    omit_delivery_id:
        When ``True``, do not include the ``X-AMC-Delivery-Id`` header at all
        (drives the missing-header scenario).
    """
    body = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = sign_body(secret, body)
    if tamper_signature:
        # Flip the last hex character so length + prefix stay valid but the
        # digest does not match. compare_digest will still run in constant time.
        last = signature[-1]
        flipped = "0" if last != "0" else "1"
        signature = signature[:-1] + flipped

    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "X-AMC-Signature": signature,
    }
    if not omit_delivery_id:
        headers["X-AMC-Delivery-Id"] = delivery_id or str(uuid.uuid4())

    return SignedRequest(body=body, headers=headers)
