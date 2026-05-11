"""S-4 smoke-test harness driver.

Posts signed AMC-shaped webhooks at a Capo listener (or local stub) and
optionally drives the AMC REST round-trip for ``mark_read`` idempotency and
error-code probing.

This is intentionally a small script rather than a pytest module so it can be
run standalone from a checkpoint-gate manual demo or from CI, and so it can be
re-purposed against a *real* AMC instance without dragging in the Capo test
framework.

Spec references:
- §5.2 — AMC Webhook Listener + Dispatcher (acceptance criteria)
- §7.4 — POST /amc/webhook endpoint contract
- §7.5 — AMC REST contract (send, mark_read, error codes)
- §9.0 — Spike S-4 deliverable
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
import uuid
from typing import Any

try:
    import httpx
except ImportError:  # pragma: no cover - dev hint
    sys.stderr.write(
        "[harness] httpx not installed. Run `pip install httpx` (preferred) or "
        "`uv pip install httpx` before invoking the harness.\n"
    )
    raise

from scenarios import Expectation, Scenario, select
from signer import make_envelope, sign_request


# ---------------------------------------------------------------------------
# Result reporting
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ScenarioResult:
    scenario: Scenario
    status: str  # "pass" | "fail" | "skip"
    observed: dict[str, Any]
    error: str | None = None
    elapsed_ms: float = 0.0

    def line(self) -> str:
        symbol = {"pass": "[PASS]", "fail": "[FAIL]", "skip": "[SKIP]"}[self.status]
        tail = f" — {self.error}" if self.error else ""
        return f"{symbol} {self.scenario.name} ({self.elapsed_ms:.0f}ms){tail}"


# ---------------------------------------------------------------------------
# Listener-facing scenarios
# ---------------------------------------------------------------------------


def _post_signed(
    client: httpx.Client,
    listener_url: str,
    secret: str,
    envelope: dict[str, Any],
    *,
    delivery_id: str | None = None,
    tamper_signature: bool = False,
    omit_delivery_id: bool = False,
) -> tuple[httpx.Response, float]:
    req = sign_request(
        secret=secret,
        envelope=envelope,
        delivery_id=delivery_id,
        tamper_signature=tamper_signature,
        omit_delivery_id=omit_delivery_id,
    )
    t0 = time.perf_counter()
    resp = client.post(listener_url, content=req.body, headers=req.headers)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return resp, elapsed_ms


def run_listener_scenario(
    scenario: Scenario,
    listener_url: str,
    secret: str,
    *,
    client: httpx.Client,
) -> ScenarioResult:
    env = make_envelope(text=f"S-4:{scenario.name}")
    observed: dict[str, Any] = {}

    if scenario.expects is Expectation.LISTENER_204:
        resp, ms = _post_signed(client, listener_url, secret, env)
        observed.update(status=resp.status_code, elapsed_ms=ms)
        sla = scenario.params.get("sla_p99_seconds", 1.0) * 1000
        ok = resp.status_code == 204 and ms <= sla
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else f"expected 204 within {sla:.0f}ms, got {resp.status_code} in {ms:.0f}ms",
            elapsed_ms=ms,
        )

    if scenario.expects is Expectation.LISTENER_401:
        resp, ms = _post_signed(client, listener_url, secret, env, tamper_signature=True)
        observed.update(status=resp.status_code, body=_safe_json(resp))
        ok = resp.status_code == 401
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else f"expected 401 BAD_SIGNATURE, got {resp.status_code}",
            elapsed_ms=ms,
        )

    if scenario.expects is Expectation.LISTENER_400:
        resp, ms = _post_signed(client, listener_url, secret, env, omit_delivery_id=True)
        observed.update(status=resp.status_code, body=_safe_json(resp))
        # Open question: spec does not pin a code here. Record observed and pass
        # iff we got a 4xx (clear client error). The findings doc owns the
        # canonical answer once probed against real AMC.
        ok = 400 <= resp.status_code < 500
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else f"expected 4xx for missing delivery-id, got {resp.status_code}",
            elapsed_ms=ms,
        )

    if scenario.expects is Expectation.LISTENER_204_DEDUPED:
        delivery_id = str(uuid.uuid4())
        resp1, ms1 = _post_signed(client, listener_url, secret, env, delivery_id=delivery_id)
        time.sleep(scenario.params.get("interval_seconds", 1))
        resp2, ms2 = _post_signed(client, listener_url, secret, env, delivery_id=delivery_id)
        observed.update(first=resp1.status_code, second=resp2.status_code)
        ok = resp1.status_code == 204 and resp2.status_code == 204
        # NOTE: We cannot prove "not enqueued" from the harness without an
        # internal Capo probe. The findings doc records this as a partial-
        # verification item — the integration test in Phase 1 (Task #10)
        # adds the internal-state assertion via a test hook.
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else f"expected 204+204, got {resp1.status_code}+{resp2.status_code}",
            elapsed_ms=ms1 + ms2,
        )

    if scenario.expects is Expectation.LISTENER_204_REENQUEUED:
        # Window is 15 min default. Harness will not actually sleep 15 minutes
        # by default — skip unless --include-slow was set. The driver in main()
        # handles the gating; here we just signal skip to keep this function
        # deterministic.
        return ScenarioResult(
            scenario=scenario,
            status="skip",
            observed={"reason": "long-duration scenario; pass --include-slow"},
            elapsed_ms=0,
        )

    return ScenarioResult(
        scenario=scenario,
        status="skip",
        observed={"reason": "not a listener scenario"},
        elapsed_ms=0,
    )


# ---------------------------------------------------------------------------
# AMC REST scenarios (mark_read, error codes)
# ---------------------------------------------------------------------------


def run_amc_rest_scenario(
    scenario: Scenario,
    amc_base: str,
    amc_token: str,
    *,
    client: httpx.Client,
) -> ScenarioResult:
    headers = {
        "Authorization": f"Bearer {amc_token}",
        "X-Agent-ID": "capo",
        "Content-Type": "application/json",
    }

    if scenario.expects is Expectation.AMC_MARK_READ_IDEMPOTENT:
        # Caller must seed a message_id that exists on the AMC instance, or
        # use AMC's unread-sweep endpoint to pull a real id. For a real-run
        # spike, the operator supplies --seed-message-id.
        message_id = scenario.params.get("seed_message_id") or f"smoke-{uuid.uuid4()}"

        t0 = time.perf_counter()
        r1 = client.post(
            f"{amc_base}/messages/mark_read",
            headers={**headers, "Idempotency-Key": str(uuid.uuid4())},
            json={"message_ids": [message_id]},
        )
        r2 = client.post(
            f"{amc_base}/messages/mark_read",
            headers={**headers, "Idempotency-Key": str(uuid.uuid4())},
            json={"message_ids": [message_id]},
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000

        observed = {
            "first": {"status": r1.status_code, "body": _safe_json(r1)},
            "second": {"status": r2.status_code, "body": _safe_json(r2)},
        }
        first_marked = (
            r1.status_code == 200
            and message_id in (r1.json().get("marked_read") or [])
        )
        second_already = (
            r2.status_code == 200
            and message_id in (r2.json().get("already_read") or [])
        )
        ok = first_marked and second_already
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else "first call must mark; second must report already_read",
            elapsed_ms=elapsed_ms,
        )

    if scenario.expects is Expectation.AMC_ERROR_CODE:
        # Generic error-code probe. The AMC instance is expected to expose a
        # fault-injection mechanism (header or test endpoint) — the harness
        # exposes the code we want and the operator wires the probe to AMC's
        # actual mechanism in the findings doc.
        code = scenario.params["code"]
        # Convention: send to /messages/send with the requested code as a
        # X-Test-Force-Error header. The findings doc records the actual
        # mechanism observed against the real AMC instance.
        r = client.post(
            f"{amc_base}/messages/send",
            headers={**headers, "Idempotency-Key": str(uuid.uuid4()), "X-Test-Force-Error": code},
            json={"channel_id": "+15551234567", "text": "smoke", "reply_to_message_id": None, "approval": None},
        )
        observed = {"status": r.status_code, "body": _safe_json(r)}
        body = _safe_json(r) or {}
        err = (body.get("error") or {}) if isinstance(body, dict) else {}
        seen_code = err.get("code")
        retry_after = err.get("retry_after_seconds")

        ok = seen_code == code
        if scenario.params.get("expect_retry_after") and retry_after is None:
            ok = False
        return ScenarioResult(
            scenario=scenario,
            status="pass" if ok else "fail",
            observed=observed,
            error=None if ok else f"expected code={code!r}, observed={seen_code!r}",
            elapsed_ms=0,
        )

    return ScenarioResult(
        scenario=scenario,
        status="skip",
        observed={"reason": "not an AMC REST scenario"},
        elapsed_ms=0,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return resp.text[:200]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="S-4 AMC webhook smoke harness")
    ap.add_argument("--listener", default="http://127.0.0.1:8090/amc/webhook",
                    help="Capo listener URL (default: local dev).")
    ap.add_argument("--secret", default=None,
                    help="AMC_WEBHOOK_SECRET. Required for listener scenarios.")
    ap.add_argument("--amc-base", default=None,
                    help="AMC base URL (e.g. http://127.0.0.1:9000). Required for "
                         "REST scenarios.")
    ap.add_argument("--amc-token", default=None,
                    help="AMC bearer token. Required for REST scenarios.")
    ap.add_argument("--scenarios", default="all",
                    help='Comma-separated scenario names, or "all".')
    ap.add_argument("--include-slow", action="store_true",
                    help="Include long-duration scenarios (e.g. dedupe-window).")
    ap.add_argument("--json", action="store_true",
                    help="Emit machine-readable JSON report.")
    args = ap.parse_args(argv)

    scenarios = select(args.scenarios)
    results: list[ScenarioResult] = []

    with httpx.Client(timeout=10.0) as client:
        for sc in scenarios:
            if sc.expects in {
                Expectation.LISTENER_204,
                Expectation.LISTENER_401,
                Expectation.LISTENER_400,
                Expectation.LISTENER_204_DEDUPED,
                Expectation.LISTENER_204_REENQUEUED,
            }:
                if not args.secret:
                    results.append(ScenarioResult(sc, "skip", {"reason": "no --secret"}))
                    continue
                if sc.expects is Expectation.LISTENER_204_REENQUEUED and not args.include_slow:
                    results.append(ScenarioResult(sc, "skip", {"reason": "needs --include-slow"}))
                    continue
                results.append(run_listener_scenario(sc, args.listener, args.secret, client=client))
            else:
                if not (args.amc_base and args.amc_token):
                    results.append(ScenarioResult(sc, "skip", {"reason": "no --amc-base / --amc-token"}))
                    continue
                results.append(run_amc_rest_scenario(sc, args.amc_base, args.amc_token, client=client))

    failed = [r for r in results if r.status == "fail"]
    if args.json:
        print(json.dumps(
            [
                {
                    "name": r.scenario.name,
                    "status": r.status,
                    "elapsed_ms": r.elapsed_ms,
                    "error": r.error,
                    "observed": r.observed,
                }
                for r in results
            ],
            indent=2, default=str,
        ))
    else:
        for r in results:
            print(r.line())
        print(f"\n{len(results)} scenarios — "
              f"{sum(1 for r in results if r.status == 'pass')} pass, "
              f"{len(failed)} fail, "
              f"{sum(1 for r in results if r.status == 'skip')} skip")

    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
