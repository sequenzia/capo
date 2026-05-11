"""Declarative scenario table for the S-4 smoke harness.

Each scenario is a pure description of the request(s) to send and the expected
listener / AMC behavior. The harness module is the driver that maps these onto
real HTTP calls. Keeping the scenarios declarative means later phases (Phase 1
integration tests, §10.2 restart-resilience tests) can re-import this table
without depending on the CLI shape of ``harness.py``.

Spec references: capo-SPEC.md §5.2 (listener), §7.4 (envelope), §7.5 (REST
contract), §9.0 (spike).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Expectation(str, Enum):
    """High-level outcome a scenario asserts on."""

    LISTENER_204 = "listener_204"               # signed + fresh delivery-id
    LISTENER_204_DEDUPED = "listener_204_dedup"  # duplicate delivery-id, no enqueue
    LISTENER_204_REENQUEUED = "listener_204_reenq"  # post-window duplicate
    LISTENER_401 = "listener_401"               # signature mismatch
    LISTENER_400 = "listener_400"               # missing X-AMC-Delivery-Id
    AMC_MARK_READ_OK = "amc_mark_read_ok"        # first call: id in marked_read
    AMC_MARK_READ_IDEMPOTENT = "amc_mark_read_idem"  # second call: id in already_read
    AMC_ERROR_CODE = "amc_error_code"            # outbound REST surfaces a known code


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    expects: Expectation
    # Free-form metadata consumed by the driver; kept open so scenarios can
    # express things like "reuse the previous delivery-id" or "wait N seconds".
    params: dict[str, Any] = field(default_factory=dict)
    # Marks scenarios that REQUIRE a live AMC instance (i.e. cannot run against
    # the local stub listener). Used by the harness to skip + warn in dev runs.
    requires_real_amc: bool = False
    # Marks scenarios that REQUIRE the Capo listener (Task #10). Cannot run
    # purely against the stub when validating the *real* dedupe LRU.
    requires_capo_listener: bool = False


# ---------------------------------------------------------------------------
# Listener-side scenarios (POST /amc/webhook)
# ---------------------------------------------------------------------------

SCENARIOS: list[Scenario] = [
    Scenario(
        name="signed_envelope_accepted",
        description=(
            "Happy path: well-formed envelope + valid signature + fresh "
            "delivery-id. Listener must respond 204 within the §5.2 fast-ACK "
            "SLA (< 1s P99)."
        ),
        expects=Expectation.LISTENER_204,
        params={"sla_p99_seconds": 1.0},
    ),
    Scenario(
        name="tampered_signature_rejected",
        description=(
            "Single-nibble tamper of X-AMC-Signature. Listener must respond "
            "401 with the BAD_SIGNATURE error shape from §7.4 and MUST NOT "
            "parse the body or log message content."
        ),
        expects=Expectation.LISTENER_401,
        params={"error_code": "BAD_SIGNATURE"},
    ),
    Scenario(
        name="missing_delivery_id_rejected",
        description=(
            "Signed body but no X-AMC-Delivery-Id header. Spec does not name "
            "an explicit response code for this case; the harness asserts "
            "the documented behavior captured in the findings doc (currently "
            "expected 400 VALIDATION_ERROR — to be confirmed against real AMC)."
        ),
        expects=Expectation.LISTENER_400,
        params={"open_question": "spec-clarification-needed"},
    ),
    Scenario(
        name="duplicate_delivery_id_deduped",
        description=(
            "Send the same delivery-id twice within the dedupe window (default "
            "15 min per §5.2). Both calls return 204 but only the first is "
            "enqueued to the per-channel worker."
        ),
        expects=Expectation.LISTENER_204_DEDUPED,
        params={"delivery_id_reuse": True, "interval_seconds": 1},
        requires_capo_listener=True,
    ),
    Scenario(
        name="duplicate_after_window",
        description=(
            "Re-send the same delivery-id AFTER the dedupe window. Behavior "
            "is implementation-defined: documented in the findings doc as an "
            "open question. The harness records observed behavior rather than "
            "asserting a specific outcome."
        ),
        expects=Expectation.LISTENER_204_REENQUEUED,
        params={"delivery_id_reuse": True, "interval_seconds": 16 * 60},
        requires_capo_listener=True,
    ),

    # ---------------------------------------------------------------------
    # Outbound REST scenarios (POST /messages/mark_read, etc.)
    # ---------------------------------------------------------------------
    Scenario(
        name="mark_read_twice_same_id",
        description=(
            "Call POST /messages/mark_read twice with the same message_id. "
            "Per §7.5, the first call returns the id in marked_read and the "
            "second returns the id in already_read. No 4xx on the second call."
        ),
        expects=Expectation.AMC_MARK_READ_IDEMPOTENT,
        requires_real_amc=True,
    ),

    # ---------------------------------------------------------------------
    # Error-code surface (one probe per documented code in §5.2 + §7.5)
    # ---------------------------------------------------------------------
    Scenario(
        name="error_code_rate_limited",
        description="Force RATE_LIMITED on outbound send; verify retry_after_seconds present.",
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "RATE_LIMITED", "expect_retry_after": True},
        requires_real_amc=True,
    ),
    Scenario(
        name="error_code_platform_auth",
        description="Bad bearer token → PLATFORM_AUTH; must NOT auto-retry.",
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "PLATFORM_AUTH", "retryable": False},
        requires_real_amc=True,
    ),
    Scenario(
        name="error_code_channel_not_found",
        description="Send to a nonexistent channel_id → CHANNEL_NOT_FOUND.",
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "CHANNEL_NOT_FOUND", "retryable": False},
        requires_real_amc=True,
    ),
    Scenario(
        name="error_code_attachment_too_large",
        description="Send with an oversized attachment → ATTACHMENT_TOO_LARGE.",
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "ATTACHMENT_TOO_LARGE", "retryable": False},
        requires_real_amc=True,
    ),
    Scenario(
        name="error_code_validation_error",
        description="Send with a missing required field → VALIDATION_ERROR.",
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "VALIDATION_ERROR", "retryable": False},
        requires_real_amc=True,
    ),
    Scenario(
        name="error_code_internal_error",
        description=(
            "Force an INTERNAL_ERROR (typically requires AMC test harness "
            "support — fault-injection endpoint). Treat as transient: retryable."
        ),
        expects=Expectation.AMC_ERROR_CODE,
        params={"code": "INTERNAL_ERROR", "retryable": True},
        requires_real_amc=True,
    ),
]


def by_name(name: str) -> Scenario:
    for s in SCENARIOS:
        if s.name == name:
            return s
    raise KeyError(f"Unknown scenario: {name!r}. Known: {[s.name for s in SCENARIOS]}")


def select(names: str | list[str]) -> list[Scenario]:
    """Resolve ``"all"`` / comma-separated / list inputs to a list of scenarios."""
    if names == "all" or names == ["all"]:
        return list(SCENARIOS)
    if isinstance(names, str):
        names = [n.strip() for n in names.split(",") if n.strip()]
    return [by_name(n) for n in names]
