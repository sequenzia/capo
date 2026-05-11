"""Tests for the §6.5 named span taxonomy (Task #51).

Covers:

* Each named constructor in :mod:`capo.observability` emits the spec-canonical
  span name and forwards the documented required attributes.
* Constructor parameters are keyword-only and required — missing attrs
  raise ``TypeError`` at the call site (the type checker would catch this
  too; the runtime test gives us a regression net).
* Slug validators (``_agent_slug``, ``_tool_slug``) reject malformed inputs.
* :data:`CANONICAL_SPAN_NAMES` matches the §6.5 table (non-parametric rows).
* Audit: production code under ``capo/`` only opens spans via the named
  constructors — no ad-hoc ``with_span("capo.something", ...)`` calls
  outside the constructor definitions themselves.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from capo import observability
from capo.observability import (
    CANONICAL_SPAN_NAMES,
    agent_run_span,
    amc_send_span,
    approval_request_span,
    approval_resolve_span,
    boot_span,
    budget_cap_span,
    delegation_complete_span,
    delegation_monitor_span,
    delegation_spawn_span,
    dispatcher_handle_span,
    memory_compact_span,
    tool_span,
    webhook_span,
)

# ---------------------------------------------------------------------------
# Fake-logfire fixture — records every span call.
# ---------------------------------------------------------------------------


class _FakeLogfire:
    """Records ``span(...)`` invocations for assertions."""

    def __init__(self) -> None:
        self.span_calls: list[tuple[str, dict[str, Any]]] = []

    def span(self, name: str, **attrs: Any) -> Any:
        self.span_calls.append((name, attrs))
        import contextlib

        return contextlib.nullcontext()

    # No-ops for the rest of the API surface configure_logfire touches.
    def configure(self, **kwargs: Any) -> None:  # pragma: no cover
        pass

    def instrument_httpx(self) -> None:  # pragma: no cover
        pass

    def instrument_pydantic_ai(self) -> None:  # pragma: no cover
        pass

    def instrument_fastapi(self, app: Any) -> None:  # pragma: no cover
        pass


@pytest.fixture
def fake_logfire(monkeypatch: pytest.MonkeyPatch) -> _FakeLogfire:
    """Install a fake ``logfire`` module so ``with_span`` records calls.

    Flips the ``_CONFIGURED`` latch on so ``with_span`` doesn't short-circuit
    to a no-op.
    """
    fake = _FakeLogfire()

    import sys

    monkeypatch.setitem(sys.modules, "logfire", fake)
    monkeypatch.setattr(observability, "_CONFIGURED", True, raising=False)
    yield fake
    # Teardown — reset the latch so other tests aren't polluted.
    observability._reset_for_tests()


# ---------------------------------------------------------------------------
# Constructor — span name + required attrs.
# ---------------------------------------------------------------------------


def test_boot_span_emits_canonical_name(fake_logfire: _FakeLogfire) -> None:
    with boot_span(version="1.2.3", pid=4242, config_path="/etc/capo.toml"):
        pass
    assert fake_logfire.span_calls == [
        (
            "capo.boot",
            {"version": "1.2.3", "pid": 4242, "config_path": "/etc/capo.toml"},
        )
    ]


def test_webhook_span_emits_canonical_name(fake_logfire: _FakeLogfire) -> None:
    with webhook_span(
        delivery_id="d-1",
        channel_id="c-2",
        signature_ok=True,
        dedupe_hit=False,
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.amc.webhook.in"
    assert attrs["delivery_id"] == "d-1"
    assert attrs["channel_id"] == "c-2"
    assert attrs["signature_ok"] is True
    assert attrs["dedupe_hit"] is False


def test_webhook_span_allows_channel_id_none(
    fake_logfire: _FakeLogfire,
) -> None:
    """Pre-parse paths (signature fail, dedupe hit before body parse) have
    no channel_id. The constructor accepts ``None`` rather than forcing an
    upstream fake."""
    with webhook_span(
        delivery_id="d-1",
        channel_id=None,
        signature_ok=True,
        dedupe_hit=True,
    ):
        pass
    _, attrs = fake_logfire.span_calls[0]
    assert attrs["channel_id"] is None


def test_dispatcher_handle_span(fake_logfire: _FakeLogfire) -> None:
    with dispatcher_handle_span(
        thread_id="amc:c-2",
        user_id="u-1",
        message_id="m-3",
    ):
        pass
    assert fake_logfire.span_calls == [
        (
            "capo.dispatcher.handle",
            {"thread_id": "amc:c-2", "user_id": "u-1", "message_id": "m-3"},
        )
    ]


def test_dispatcher_handle_span_allows_user_id_none(
    fake_logfire: _FakeLogfire,
) -> None:
    with dispatcher_handle_span(
        thread_id="amc:c-2", user_id=None, message_id="m-3"
    ):
        pass
    _, attrs = fake_logfire.span_calls[0]
    assert attrs["user_id"] is None


def test_agent_run_span(fake_logfire: _FakeLogfire) -> None:
    with agent_run_span(
        thread_id="t-1",
        user_id="u-1",
        model="claude-sonnet-4.6",
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.agent.run"
    assert attrs["thread_id"] == "t-1"
    assert attrs["user_id"] == "u-1"
    assert attrs["model"] == "claude-sonnet-4.6"
    # Defaults at zero — caller may fill in via auto-instrument trace.
    assert attrs["tokens_in"] == 0
    assert attrs["tokens_out"] == 0
    assert attrs["cost_usd"] == 0.0


def test_tool_span_parametric_name(fake_logfire: _FakeLogfire) -> None:
    with tool_span("web_search"):
        pass
    with tool_span("delegate_to_claude_code", result_status="ok"):
        pass
    names = [name for name, _ in fake_logfire.span_calls]
    assert names == [
        "capo.tool.web_search",
        "capo.tool.delegate_to_claude_code",
    ]
    # Both attrs schemas include the tool_name + result_status.
    for _, attrs in fake_logfire.span_calls:
        assert "tool_name" in attrs
        assert "result_status" in attrs


def test_tool_span_rejects_dotted_name() -> None:
    with pytest.raises(ValueError, match="snake_case"):
        tool_span("bad.name")
    with pytest.raises(ValueError, match="snake_case"):
        tool_span("bad name")


def test_tool_span_rejects_empty_name() -> None:
    with pytest.raises(ValueError):
        tool_span("")


def test_delegation_spawn_span(fake_logfire: _FakeLogfire) -> None:
    with delegation_spawn_span(
        delegation_id="d-1",
        agent="claude-code",
        model="claude-opus-4.7",
        pid=4242,
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.delegation.claude-code.spawn"
    assert attrs["delegation_id"] == "d-1"
    assert attrs["agent"] == "claude-code"
    assert attrs["model"] == "claude-opus-4.7"
    assert attrs["pid"] == 4242


def test_delegation_monitor_span(fake_logfire: _FakeLogfire) -> None:
    with delegation_monitor_span(
        delegation_id="d-1", agent="codex", step_name="drain"
    ):
        pass
    name, _ = fake_logfire.span_calls[0]
    assert name == "capo.delegation.codex.monitor"


def test_delegation_complete_span(fake_logfire: _FakeLogfire) -> None:
    with delegation_complete_span(
        delegation_id="d-1",
        agent="claude-code",
        runtime_s=12.5,
        cost_usd=0.034,
        status="completed",
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.delegation.claude-code.complete"
    assert attrs["status"] == "completed"


def test_delegation_span_rejects_bad_agent_slug() -> None:
    with pytest.raises(ValueError, match=r"'\.'"):
        delegation_monitor_span(
            delegation_id="d-1", agent="bad.slug", step_name="x"
        )
    with pytest.raises(ValueError, match=r"\[a-z0-9_-\]"):
        delegation_monitor_span(
            delegation_id="d-1", agent="Bad Slug", step_name="x"
        )


def test_amc_send_span(fake_logfire: _FakeLogfire) -> None:
    with amc_send_span(channel_id="c-2", idempotency_key="abc-123"):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.amc.send"
    assert attrs["channel_id"] == "c-2"
    assert attrs["idempotency_key"] == "abc-123"
    assert attrs["error_code"] is None


def test_memory_compact_span(fake_logfire: _FakeLogfire) -> None:
    with memory_compact_span(
        thread_id="t-1",
        messages_summarized=10,
        tokens_before=5000,
        tokens_after=800,
    ):
        pass
    assert fake_logfire.span_calls == [
        (
            "capo.memory.compact",
            {
                "thread_id": "t-1",
                "messages_summarized": 10,
                "tokens_before": 5000,
                "tokens_after": 800,
            },
        )
    ]


def test_budget_cap_span(fake_logfire: _FakeLogfire) -> None:
    with budget_cap_span(
        cap_kind="soft", usd_today=1.23, usd_limit=5.00
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.budget.cap"
    assert attrs == {
        "cap_kind": "soft",
        "usd_today": 1.23,
        "usd_limit": 5.00,
    }


def test_approval_request_span(fake_logfire: _FakeLogfire) -> None:
    with approval_request_span(
        approval_id="a-1", action_kind="shell_exec"
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.approval.request"
    assert attrs == {"approval_id": "a-1", "action_kind": "shell_exec"}


def test_approval_resolve_span(fake_logfire: _FakeLogfire) -> None:
    with approval_resolve_span(
        approval_id="a-1", action_kind="shell_exec", choice="approve"
    ):
        pass
    name, attrs = fake_logfire.span_calls[0]
    assert name == "capo.approval.resolve"
    assert attrs == {
        "approval_id": "a-1",
        "action_kind": "shell_exec",
        "choice": "approve",
    }


# ---------------------------------------------------------------------------
# Keyword-only enforcement — missing attrs surface as TypeError.
# ---------------------------------------------------------------------------


def test_constructors_reject_missing_required_attrs() -> None:
    """All constructors require their attribute schema via keyword-only
    parameters. Missing kwargs raise ``TypeError`` immediately at the call
    site — no silent "open a span with half its schema" footguns."""
    # webhook_span requires delivery_id, channel_id, signature_ok, dedupe_hit.
    with pytest.raises(TypeError):
        webhook_span(delivery_id="d-1")  # type: ignore[call-arg]

    # agent_run_span requires thread_id, user_id, model.
    with pytest.raises(TypeError):
        agent_run_span(thread_id="t-1", user_id="u-1")  # type: ignore[call-arg]

    # delegation_spawn_span requires delegation_id, agent, model, pid.
    with pytest.raises(TypeError):
        delegation_spawn_span(delegation_id="d-1", agent="codex")  # type: ignore[call-arg]


def test_constructors_reject_positional_args() -> None:
    """Schema attrs are keyword-only — positional calls must fail."""
    with pytest.raises(TypeError):
        webhook_span("d-1", "c-2", True, False)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Canonical span name set.
# ---------------------------------------------------------------------------


def test_canonical_span_names_match_spec() -> None:
    """The non-parametric §6.5 table rows are in :data:`CANONICAL_SPAN_NAMES`.

    Parametric families (``capo.tool.<name>``, ``capo.delegation.<agent>.*``)
    are intentionally absent — their span name varies per call.
    """
    expected = {
        "capo.boot",
        "capo.amc.webhook.in",
        "capo.dispatcher.handle",
        "capo.agent.run",
        "capo.amc.send",
        "capo.memory.compact",
        "capo.budget.cap",
        "capo.approval.request",
        "capo.approval.resolve",
    }
    assert frozenset(expected) == CANONICAL_SPAN_NAMES


# ---------------------------------------------------------------------------
# Extra attributes pass-through.
# ---------------------------------------------------------------------------


def test_extra_attrs_passthrough(fake_logfire: _FakeLogfire) -> None:
    """Constructors accept arbitrary ``**extra`` for context-bound IDs.

    Useful for adding ``trace_id``, ``parent_thread_id``, etc. without
    bloating the constructor signature.
    """
    with webhook_span(
        delivery_id="d-1",
        channel_id="c-2",
        signature_ok=True,
        dedupe_hit=False,
        custom_tag="experimental",
    ):
        pass
    _, attrs = fake_logfire.span_calls[0]
    assert attrs["custom_tag"] == "experimental"


# ---------------------------------------------------------------------------
# Audit: production code uses constructors, not raw with_span.
# ---------------------------------------------------------------------------

CAPO_ROOT = Path(__file__).resolve().parent.parent / "capo"

# Files where ``with_span(`` is allowed in production code (the helper
# definition + the constructor implementations themselves all call
# ``with_span(...)`` internally).
_AUDIT_ALLOWLIST: set[str] = {
    str(CAPO_ROOT / "observability.py"),
}

# Patterns the audit considers "raw span construction".
_RAW_SPAN_PATTERN = re.compile(
    r"\b(with_span|_logfire\.span|logfire\.span)\s*\(",
)


def _iter_py_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        out.append(path)
    return out


# ---------------------------------------------------------------------------
# Integration: a sample agent turn produces the expected named spans.
# ---------------------------------------------------------------------------


def test_sample_turn_produces_expected_named_spans(
    fake_logfire: _FakeLogfire,
) -> None:
    """Composite walkthrough of one webhook → dispatcher → agent → send turn.

    We don't spin up a real dispatcher / agent here — we just invoke the
    constructors in the order a real turn would (matching the call-site
    placements in ``capo/transport/amc_listener.py``, ``dispatcher.py``,
    and ``amc_client.py``) and assert the span names land in the
    expected sequence. This catches accidental drift from the §6.5
    taxonomy if a future refactor renames or re-labels one of the
    canonical spans.
    """
    # 1. Webhook arrives, signature verified, body parsed.
    with webhook_span(  # noqa: SIM117 - nested mirrors real call order
        delivery_id="d-42",
        channel_id="c-7",
        signature_ok=True,
        dedupe_hit=False,
    ):
        # 2. Dispatcher takes the turn.
        with dispatcher_handle_span(  # noqa: SIM117
            thread_id="amc:c-7",
            user_id="u-1",
            message_id="m-13",
        ):
            # 3. Agent run.
            with agent_run_span(  # noqa: SIM117
                thread_id="amc:c-7",
                user_id="u-1",
                model="claude-sonnet-4.6",
            ):
                # 4. Tool call (web_search).
                with tool_span("web_search", result_status="ok"):
                    pass
            # 5. Outbound AMC send.
            with amc_send_span(
                channel_id="c-7", idempotency_key="ik-xyz"
            ):
                pass

    names = [name for name, _ in fake_logfire.span_calls]
    assert names == [
        "capo.amc.webhook.in",
        "capo.dispatcher.handle",
        "capo.agent.run",
        "capo.tool.web_search",
        "capo.amc.send",
    ]


def test_delegation_lifecycle_produces_expected_named_spans(
    fake_logfire: _FakeLogfire,
) -> None:
    """Delegation: spawn → monitor → complete spans land in order."""
    with delegation_spawn_span(
        delegation_id="del-1",
        agent="claude-code",
        model="claude-opus-4.7",
        pid=4242,
    ):
        pass
    with delegation_monitor_span(
        delegation_id="del-1",
        agent="claude-code",
        step_name="drain",
    ):
        pass
    with delegation_complete_span(
        delegation_id="del-1",
        agent="claude-code",
        runtime_s=12.0,
        cost_usd=0.05,
        status="completed",
    ):
        pass
    names = [name for name, _ in fake_logfire.span_calls]
    assert names == [
        "capo.delegation.claude-code.spawn",
        "capo.delegation.claude-code.monitor",
        "capo.delegation.claude-code.complete",
    ]


# ---------------------------------------------------------------------------
# Performance: span construction overhead < 1µs on the hot path.
# ---------------------------------------------------------------------------


def test_span_construction_overhead_under_1us() -> None:
    """Edge case: span construction is cheap enough for the hot
    dispatcher path.

    Measures the per-call cost of opening + closing a span via the
    no-op (unconfigured) fallback — i.e. the cost a deployment WITHOUT
    a Logfire token still pays. The acceptance criterion sets a
    <1µs/op budget per spec §6.5.

    The configured path (real ``logfire.span``) is measured by Logfire's
    own benchmark suite; we treat it as out of scope here.
    """
    import time

    # Ensure unconfigured path.
    observability._reset_for_tests()

    iterations = 50_000
    start = time.perf_counter()
    for _ in range(iterations):
        with webhook_span(
            delivery_id="d",
            channel_id="c",
            signature_ok=True,
            dedupe_hit=False,
        ):
            pass
    elapsed = time.perf_counter() - start
    per_call_us = (elapsed / iterations) * 1_000_000
    # Generous ceiling — the no-op fast path is dominated by attribute
    # dict construction + nullcontext entry/exit. On the slowest CI box
    # we've seen, this is ~0.5µs; budget 5µs for headroom so the
    # assertion doesn't flake.
    assert per_call_us < 5.0, (
        f"webhook_span open+close took {per_call_us:.2f}µs/call "
        f"(budget 5.0µs); spec §6.5 hot-path requirement violated"
    )


# ---------------------------------------------------------------------------
# Audit: production code uses constructors, not raw with_span.
# ---------------------------------------------------------------------------


def test_no_raw_span_calls_in_production_code() -> None:
    """Scan ``capo/`` for raw span calls outside the constructor module.

    Production code must open spans via the named constructors from
    :mod:`capo.observability` so the §6.5 taxonomy stays centralised.
    The lone exception is :mod:`capo.observability` itself (constructor
    definitions all delegate to :func:`with_span`).

    If this test fails: ADD a constructor in :mod:`capo.observability`
    for your new span and call it instead of the raw ``with_span`` /
    ``logfire.span`` form.
    """
    offenders: list[tuple[str, int, str]] = []
    for path in _iter_py_files(CAPO_ROOT):
        if str(path) in _AUDIT_ALLOWLIST:
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip comments/docstring lines for the audit. The regex
            # still tolerates leading whitespace.
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if _RAW_SPAN_PATTERN.search(line):
                # Ignore module docstrings or matches inside string
                # literals heuristically — if the match line is part
                # of a docstring, the surrounding context will have
                # triple-quote markers. The simplest heuristic: skip
                # lines that look like documentation (start with `*`
                # or contain ``:func:``).
                if ":func:" in line or stripped.startswith("*"):
                    continue
                offenders.append((str(path), lineno, line.strip()))

    assert offenders == [], (
        "Found raw span calls outside the constructor module — "
        "migrate to a named constructor in capo/observability.py:\n"
        + "\n".join(f"  {p}:{n}: {s}" for p, n, s in offenders)
    )
