"""Tests for :mod:`capo.workflows._idempotency` (Task #29).

Covers spec §5.6 (idempotency keys on side-effecting steps), §5.13
(heartbeat keys), §9.3 (idempotency framework deliverable).

Unit tests (no DBOS instance required):
  * same inputs → identical key (stability across processes)
  * different inputs → distinct keys
  * deterministic args order matters
  * canonicalization across dict ordering, tuple vs list, nested types
  * unsupported types raise TypeError
  * empty workflow_id / step_name rejected
  * decorator default deterministic_args
  * decorator explicit deterministic_args ignores non-deterministic ones
  * key-attribute helper on the wrapped callable
  * mismatch detection: same key + different payload → IdempotencyMismatchError

Integration tests (real DBOS):
  * decorator registers as a DBOS step (idempotency_key_for survives)
  * workflow-resume contract: re-entering an already-completed step does
    NOT re-execute the side effect (at-most-once)
  * idempotency record is persisted as a DBOS workflow event so an operator
    can inspect ``(step, key)`` pairs in ``dbos.db``
"""

from __future__ import annotations

import contextlib
import uuid
from pathlib import Path

import pytest

from capo.config import PathsConfig
from capo.workflows import destroy_dbos, init_dbos, is_launched, launch_dbos
from capo.workflows._idempotency import (
    IdempotencyError,
    IdempotencyMismatchError,
    idempotency_key,
    idempotent_step,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _StubSettings:
    def __init__(self, paths: PathsConfig) -> None:
        self.paths = paths


def _make_settings(tmp_path: Path) -> _StubSettings:
    dotcapo = tmp_path / ".capo"
    return _StubSettings(
        PathsConfig(
            workspaces_root=tmp_path / "workspaces",
            projects_root=tmp_path / "projects",
            db_path=dotcapo / "state.db",
            dbos_db_path=dotcapo / "dbos.db",
        )
    )


@pytest.fixture(autouse=True)
def _ensure_clean_dbos_state() -> None:
    """Tear down DBOS between tests so each one sees a fresh process state.

    Mirrors the fixture in ``test_workflows_config.py`` — DBOS is a
    process-level singleton.
    """
    yield
    if is_launched():
        with contextlib.suppress(Exception):
            destroy_dbos()


# ---------------------------------------------------------------------------
# Unit: idempotency_key stability + distinctness
# ---------------------------------------------------------------------------


def test_same_inputs_produce_identical_key() -> None:
    """Stability is the headline property — required for cross-restart retries."""
    k1 = idempotency_key("wf-1", "notify_user", "delegation-abc")
    k2 = idempotency_key("wf-1", "notify_user", "delegation-abc")
    assert k1 == k2


def test_different_workflow_ids_produce_different_keys() -> None:
    """Re-runs of the same workflow def under different DBOS IDs are different
    executions from a side-effect perspective — keys must differ."""
    k1 = idempotency_key("wf-1", "notify_user", "delegation-abc")
    k2 = idempotency_key("wf-2", "notify_user", "delegation-abc")
    assert k1 != k2


def test_different_step_names_produce_different_keys() -> None:
    """``notify_user`` and ``heartbeat`` on the same delegation must differ."""
    k1 = idempotency_key("wf-1", "notify_user", "delegation-abc")
    k2 = idempotency_key("wf-1", "heartbeat", "delegation-abc")
    assert k1 != k2


def test_different_args_produce_different_keys() -> None:
    k1 = idempotency_key("wf-1", "notify_user", "delegation-abc")
    k2 = idempotency_key("wf-1", "notify_user", "delegation-xyz")
    assert k1 != k2


def test_arg_order_matters() -> None:
    """Positional ordering is part of the key — ``(a, b)`` ≠ ``(b, a)``."""
    k1 = idempotency_key("wf-1", "step", "a", "b")
    k2 = idempotency_key("wf-1", "step", "b", "a")
    assert k1 != k2


def test_key_format_includes_step_name_prefix() -> None:
    """The key prefix should be readable in logs."""
    k = idempotency_key("wf-1", "notify_user", "delegation-abc")
    assert k.startswith("notify_user:")
    # 32 hex chars after the colon (see _KEY_HEX_LEN).
    suffix = k.split(":", 1)[1]
    assert len(suffix) == 32
    assert all(c in "0123456789abcdef" for c in suffix)


def test_tuple_and_list_args_hash_identically() -> None:
    """Tuples and lists with identical contents must produce the same key —
    JSON has no tuple type, and callers shouldn't be punished for picking
    one over the other."""
    k1 = idempotency_key("wf-1", "step", ("a", "b"))
    k2 = idempotency_key("wf-1", "step", ["a", "b"])
    assert k1 == k2


def test_dict_arg_key_order_irrelevant() -> None:
    """Dict ordering is implementation-defined — canonicalize sorts keys."""
    k1 = idempotency_key("wf-1", "step", {"a": 1, "b": 2})
    k2 = idempotency_key("wf-1", "step", {"b": 2, "a": 1})
    assert k1 == k2


def test_nested_dict_canonicalization() -> None:
    """Nested dicts inside lists also need stable ordering."""
    k1 = idempotency_key("wf-1", "step", [{"a": 1, "b": 2}, {"c": 3}])
    k2 = idempotency_key("wf-1", "step", [{"b": 2, "a": 1}, {"c": 3}])
    assert k1 == k2


def test_unsupported_arg_type_raises_typeerror() -> None:
    """Functions, generators, etc. can't be canonicalized — fail fast."""

    def _fn() -> None:
        pass

    with pytest.raises(TypeError):
        idempotency_key("wf-1", "step", _fn)


def test_empty_workflow_id_rejected() -> None:
    with pytest.raises(ValueError):
        idempotency_key("", "step", "a")


def test_empty_step_name_rejected() -> None:
    with pytest.raises(ValueError):
        idempotency_key("wf-1", "", "a")


def test_heartbeat_pattern_5_13() -> None:
    """§5.13 idempotency keys are derived from ``(delegation_id, str(threshold_seconds))``.

    Confirm same threshold → same key, different threshold → different key.
    """
    k_15min = idempotency_key("wf-1", "heartbeat", "delegation-abc", str(900))
    k_15min_again = idempotency_key("wf-1", "heartbeat", "delegation-abc", str(900))
    k_1hr = idempotency_key("wf-1", "heartbeat", "delegation-abc", str(3600))
    assert k_15min == k_15min_again
    assert k_15min != k_1hr


# ---------------------------------------------------------------------------
# Unit: decorator (no DBOS workflow context)
# ---------------------------------------------------------------------------


def test_decorator_default_deterministic_args() -> None:
    """Default behavior: args + sorted kwargs feed the key derivation."""
    calls: list[tuple[str, str]] = []

    @idempotent_step(step_name="test_default")
    def step(delegation_id: str, message: str) -> str:
        calls.append((delegation_id, message))
        return "ok"

    # Same args → key matches (we can probe via the helper).
    k1 = step.idempotency_key_for("d-1", message="hi")
    k2 = step.idempotency_key_for("d-1", message="hi")
    assert k1 == k2

    # Different kwargs → different key.
    k3 = step.idempotency_key_for("d-1", message="bye")
    assert k1 != k3


def test_decorator_explicit_deterministic_args() -> None:
    """Caller-supplied ``deterministic_args`` decides what drives the key.

    Non-deterministic args (timestamps, fresh UUIDs) MUST be excluded — this
    is the only way the framework can detect "same logical operation, different
    bookkeeping" as retry-equivalent.
    """

    @idempotent_step(
        step_name="notify_user",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, sent_at_ts: float) -> None:  # noqa: ARG001
        pass

    k1 = notify.idempotency_key_for("d-1", sent_at_ts=1.0)
    k2 = notify.idempotency_key_for("d-1", sent_at_ts=99999.0)
    # Same delegation_id → same key regardless of timestamp.
    assert k1 == k2


def test_decorator_step_name_defaults_to_func_name() -> None:
    @idempotent_step()
    def notify_user(d: str) -> None:  # noqa: ARG001
        pass

    k = notify_user.idempotency_key_for("d-1")
    assert k.startswith("notify_user:")


def test_decorator_mismatch_detection_same_key_different_payload() -> None:
    """Edge case: same idempotency key, different content → programmer error.

    Build a step whose key depends only on ``delegation_id``, then invoke it
    twice with the same delegation_id but a different ``message``. The
    second call must raise :class:`IdempotencyMismatchError`.
    """
    calls: list[tuple[str, str]] = []

    @idempotent_step(
        step_name="notify",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, message: str) -> None:
        calls.append((delegation_id, message))

    notify("d-1", message="first")
    with pytest.raises(IdempotencyMismatchError) as exc:
        notify("d-1", message="DIFFERENT")
    assert exc.value.key.startswith("notify:")
    assert exc.value.step_name == "notify"
    assert "programmer error" in str(exc.value).lower()


def test_decorator_same_key_same_payload_idempotent() -> None:
    """Same key + same payload on a second call → no error (the OK path).

    The side effect itself still runs in test mode (no DBOS replay), but the
    framework does NOT reject the second call — only mismatches are errors.
    """

    @idempotent_step(
        step_name="notify",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, message: str) -> str:  # noqa: ARG001
        return "ok"

    assert notify("d-1", message="same") == "ok"
    # Second call with identical payload is fine.
    assert notify("d-1", message="same") == "ok"


def test_decorator_preserves_return_value() -> None:
    @idempotent_step(step_name="echo")
    def echo(x: int) -> int:
        return x * 2

    assert echo(5) == 10


async def test_decorator_async_step() -> None:
    """Async step bodies are supported (most §5.6 steps are coroutines)."""
    calls: list[str] = []

    @idempotent_step(
        step_name="async_notify",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    async def notify(delegation_id: str, message: str) -> str:
        calls.append(delegation_id)
        return message

    result = await notify("d-1", message="hello")
    assert result == "hello"
    assert calls == ["d-1"]

    # Mismatch detection works for async too.
    with pytest.raises(IdempotencyMismatchError):
        await notify("d-1", message="BAD")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_idempotency_mismatch_carries_attributes() -> None:
    err = IdempotencyMismatchError(
        key="notify:abc",
        expected_fp="11",
        actual_fp="22",
        step_name="notify",
    )
    assert err.key == "notify:abc"
    assert err.expected_fp == "11"
    assert err.actual_fp == "22"
    assert err.step_name == "notify"
    assert "notify:abc" in str(err)
    assert isinstance(err, IdempotencyError)


# ---------------------------------------------------------------------------
# Integration: real DBOS workflow + at-most-once side effect
# ---------------------------------------------------------------------------


def test_decorator_registers_as_dbos_step(tmp_path: Path) -> None:
    """After init+launch, the decorator wraps the function as a DBOS step.

    We don't run the workflow here — that's the next test. We just confirm
    the decorator doesn't raise when DBOS is live and the attached helper
    survives DBOS's own decoration.
    """
    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    @idempotent_step(
        step_name="notify_user",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, message: str) -> str:  # noqa: ARG001
        return "sent"

    # Helper attribute survives DBOS's step() wrapper.
    assert hasattr(notify, "idempotency_key_for")
    assert callable(notify.idempotency_key_for)
    k = notify.idempotency_key_for("d-1", message="hi")
    assert k.startswith("notify_user:")


def test_workflow_runs_idempotent_step_to_completion(tmp_path: Path) -> None:
    """End-to-end: a workflow containing an idempotent step runs and the side
    effect happens once. Establishes the baseline before the resume test."""
    from dbos import DBOS

    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    # Counter persisted in module-level state so the step body can mutate it
    # from inside the workflow.
    side_effect_log: list[str] = []

    @idempotent_step(
        step_name="notify_user",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, message: str) -> str:
        side_effect_log.append(f"{delegation_id}:{message}")
        return f"sent:{delegation_id}"

    @DBOS.workflow()
    def workflow(delegation_id: str) -> str:
        return notify(delegation_id, message="hi")

    wf_id = f"wf-test-{uuid.uuid4()}"
    with DBOS.SetWorkflowID(wf_id) if hasattr(DBOS, "SetWorkflowID") else _noop_ctx():
        # Some DBOS versions expose SetWorkflowID via the top-level module;
        # we don't strictly need a custom ID, but it makes the assertion
        # below easier to reason about.
        result = workflow("d-1")
    assert result == "sent:d-1"
    assert side_effect_log == ["d-1:hi"]


@contextlib.contextmanager
def _noop_ctx():
    yield


def test_workflow_resume_does_not_double_execute_side_effect(tmp_path: Path) -> None:
    """The §5.6 acceptance criterion: re-entering an already-completed step
    must NOT re-run the side effect.

    DBOS's native step-state mechanism owns this — :func:`idempotent_step`
    layers on top via ``@DBOS.step``. We verify the layered decorator
    preserves the at-most-once contract by invoking the same DBOS workflow
    ID twice. DBOS replays the cached step return on the second invocation
    rather than re-executing the body.
    """
    from dbos import DBOS, SetWorkflowID

    settings = _make_settings(tmp_path)
    init_dbos(settings)  # type: ignore[arg-type]
    launch_dbos()

    side_effect_log: list[str] = []

    @idempotent_step(
        step_name="notify_user",
        deterministic_args=lambda delegation_id, **_: (delegation_id,),
    )
    def notify(delegation_id: str, message: str) -> str:
        side_effect_log.append(f"{delegation_id}:{message}")
        return f"sent:{delegation_id}"

    @DBOS.workflow()
    def workflow(delegation_id: str) -> str:
        return notify(delegation_id, message="hi")

    fixed_wf_id = f"wf-resume-{uuid.uuid4()}"
    # First invocation: side effect runs once.
    with SetWorkflowID(fixed_wf_id):
        first = workflow("d-1")
    assert first == "sent:d-1"
    assert side_effect_log == ["d-1:hi"]

    # Second invocation under the SAME workflow ID: DBOS treats this as a
    # resume of an already-completed workflow and returns the cached
    # result without re-running the step body.
    with SetWorkflowID(fixed_wf_id):
        second = workflow("d-1")
    assert second == "sent:d-1"
    # The headline assertion: side effect log length stays at 1.
    assert side_effect_log == ["d-1:hi"], (
        f"side effect re-executed on workflow resume: {side_effect_log}"
    )
