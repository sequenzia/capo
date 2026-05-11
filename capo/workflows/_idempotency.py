"""Step idempotency framework for DBOS workflows (spec §5.6, §5.13, §9.3).

DBOS provides at-least-once step semantics: if Capo crashes mid-step, the step
is re-executed on recovery (spike S-2 §6 — observed and accepted). For any
``@DBOS.step`` whose body has external side effects — subprocess spawn,
``amc.send``, or any DB insert that is NOT already wrapped by
:func:`capo.memory.store.begin_immediate_with_retry` — the side effect MUST
therefore be idempotent or the receiver MUST dedupe.

This module gives Capo two pieces of the puzzle:

1. :func:`idempotency_key` — a pure helper that derives a stable string key
   from ``(workflow_id, step_name, *deterministic_args)``. Stability across
   processes, restarts, and Python releases is the contract; the format is
   internal but the key is suitable to use as e.g. the AMC ``Idempotency-Key``
   header (§7.5) or any other dedupe token the receiver checks.

2. :func:`idempotent_step` — a decorator that composes with
   :meth:`dbos.DBOS.step` and:

   * computes the key for each call (using the workflow's ``DBOS.workflow_id``
     plus the step name and a caller-supplied "deterministic args" tuple);

   * **persists the key plus a payload fingerprint** in the workflow's step
     state via :meth:`dbos.DBOS.set_event` so the operator can audit
     ``(workflow, step, idem_key, payload_fp)`` after the fact;

   * **rejects** a second call with the same key but a different payload
     fingerprint as a programmer error (:class:`IdempotencyMismatchError`).

DBOS's native step-state mechanism handles the *resume-skip* behavior: a
``@DBOS.step`` that has already returned for this ``(workflow_id, function_id)``
pair is replayed from the checkpoint rather than re-executed. Our wrapper sits
*around* the DBOS step, so on resume the cached step return is replayed
verbatim — the side effect runs at-most-once across restarts.

Design notes
------------

* **Deterministic args are explicit.** Callers pick which arguments contribute
  to the key, because some real-world step inputs are non-deterministic across
  retries (e.g. a freshly-generated timestamp). We deliberately do NOT hash
  ``*args, **kwargs`` of the wrapped function — that would silently produce
  different keys when irrelevant state changes between attempts.

* **Payload fingerprint != key.** The key is derived only from the
  ``deterministic_args``. The *fingerprint* is a hash of the full call (key +
  args + kwargs) that we use to detect "same key, different payload" as a
  programmer bug. If two callers reuse the same key with different content,
  one of them is wrong — surface it loudly.

* **Persistence via workflow events.** DBOS exposes
  :meth:`DBOS.set_event` for arbitrary K/V state on the current workflow.
  We namespace under ``capo.idem.<step_name>`` so multiple idempotent steps
  in one workflow don't collide. The event value is a small JSON object
  ``{"key", "fp"}`` so an operator inspecting the DBOS system DB can see what
  key was emitted for which step. The step-level skip-on-replay behavior is
  provided by :meth:`DBOS.step` itself; we layer the key persistence on top.

* **Test-friendly.** The decorator falls back to a "best-effort" mode when
  called outside a DBOS workflow context (``DBOS.workflow_id`` raises). This
  lets unit tests exercise the key derivation and mismatch detection without
  spinning up a DBOS instance. The integration test (``test_idempotency.py``)
  runs the decorator inside a real workflow + restart cycle to verify the
  at-most-once side-effect contract.

* **Source-of-truth for callers.** Producers of side-effecting steps —
  ``notify_user``, ``amc.send`` heartbeat (§5.13), subprocess-spawn step
  (§5.6 restart contract) — should use :func:`idempotent_step` instead of
  hand-rolling key derivation. Centralizing the algorithm here means a
  future change (e.g. switching hash function or normalization rules)
  affects every site uniformly.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from functools import wraps
from typing import Any, ParamSpec, TypeVar

logger = logging.getLogger("capo.workflows.idempotency")

# ---------------------------------------------------------------------------
# Public errors
# ---------------------------------------------------------------------------


class IdempotencyError(RuntimeError):
    """Base class for idempotency-framework errors.

    Specific subclasses signal programmer errors that should fail loudly in
    test and in production — silently masking these would let a duplicate
    side effect slip through.
    """


class IdempotencyMismatchError(IdempotencyError):
    """Same idempotency key reused with a *different* payload fingerprint.

    Raised when :func:`idempotent_step` sees a second invocation that derives
    the same key (same workflow + step + deterministic args) but whose full
    call payload (args, kwargs) hashes to a different fingerprint than the
    one previously persisted. This means a caller is either:

    * passing the same ``deterministic_args`` for semantically different
      operations (probably a bug in how they pick deterministic args), or

    * mutating one of the args after key derivation but before the step runs
      (which would break at-most-once semantics on retry).

    Attributes
    ----------
    key:
        The idempotency key that collided.
    expected_fp:
        The payload fingerprint persisted on the first call.
    actual_fp:
        The payload fingerprint from the new call.
    step_name:
        The wrapped step's logical name.
    """

    def __init__(
        self,
        *,
        key: str,
        expected_fp: str,
        actual_fp: str,
        step_name: str,
    ) -> None:
        self.key = key
        self.expected_fp = expected_fp
        self.actual_fp = actual_fp
        self.step_name = step_name
        super().__init__(
            f"idempotency key collision for step '{step_name}': "
            f"key={key!r} already persisted with fingerprint {expected_fp!r}, "
            f"but new call has fingerprint {actual_fp!r}. "
            "Same key + different payload is a programmer error — pick "
            "deterministic_args that uniquely identify this side effect."
        )


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

#: Length of the truncated hex digest used for keys. SHA-256 produces a 64-char
#: hex digest; 32 chars (128 bits) is more than enough for collision resistance
#: in our key space (single workflow × ~tens of steps × small arg cardinality)
#: and keeps the key readable in logs.
_KEY_HEX_LEN: int = 32

#: Length of the payload fingerprint hex digest. Shorter than the key — its
#: only job is to detect "same key, different payload" so we don't need full
#: cryptographic collision resistance, just enough to make accidental
#: collisions vanishingly unlikely.
_FP_HEX_LEN: int = 16

#: Prefix used when persisting key/fingerprint pairs as DBOS workflow events.
#: Namespaces our entries so they don't collide with caller-set events.
_EVENT_KEY_PREFIX: str = "capo.idem"


def _canonicalize(value: Any) -> Any:
    """Convert ``value`` to a JSON-serializable form with a stable shape.

    We accept whatever Python types callers pass as ``deterministic_args``
    (strings, ints, bools, None, dicts, lists, tuples) and normalize:

    * ``tuple`` → ``list`` (JSON has no tuple type; tuples and lists with the
      same contents must hash identically).
    * ``dict`` keys sorted (so dict ordering doesn't change the digest).
    * Anything else with a sensible ``__str__`` falls through to ``str(value)``
      so callers can pass small value-objects (e.g. :class:`pathlib.Path`,
      :class:`uuid.UUID`) without crashing — at the cost of "best-effort"
      stability for those types. Callers wanting strict stability should
      pre-stringify with a deterministic repr.

    Raises
    ------
    TypeError
        If ``value`` is a type we can't safely canonicalize (e.g. a function,
        a generator, a live socket). Better to fail at key-derivation time
        than to silently produce non-reproducible keys.
    """
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list | tuple):
        return [_canonicalize(v) for v in value]
    if isinstance(value, dict):
        # ``sort_keys`` on json.dumps would handle this for the top-level
        # dict, but nested dicts inside lists need recursive normalization
        # before the JSON layer sees them — otherwise nested key ordering
        # is implementation-defined.
        try:
            items = sorted(value.items(), key=lambda kv: str(kv[0]))
        except TypeError as exc:
            raise TypeError(
                f"cannot canonicalize dict with unorderable keys: {value!r}"
            ) from exc
        return {str(k): _canonicalize(v) for k, v in items}
    # Last resort: small value-objects with a stable __str__ (Path, UUID,
    # Decimal, datetime). We document this as best-effort rather than promise
    # cryptographic stability for arbitrary types.
    if hasattr(value, "__class__") and value.__class__.__module__ != "builtins":
        return f"{value.__class__.__module__}.{value.__class__.__qualname__}:{value!s}"
    raise TypeError(
        f"cannot canonicalize value of type {type(value).__name__!r}: "
        "supported types are None, bool, int, float, str, list/tuple, dict, "
        "and small value-objects with a stable __str__."
    )


def _stable_digest(*parts: Any, length: int) -> str:
    """SHA-256 over the canonicalized parts, truncated to ``length`` hex chars.

    We feed each part through :func:`_canonicalize` then serialize the whole
    sequence as JSON with ``sort_keys=True`` so the byte sequence fed to the
    hash function is deterministic across Python versions and runs.

    Using ``separators=(',', ':')`` avoids whitespace variability and shaves
    a few bytes per digest; the JSON shape is internal so we don't owe
    callers human-readable output.
    """
    payload = [_canonicalize(p) for p in parts]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:length]


def idempotency_key(
    workflow_id: str,
    step_name: str,
    *deterministic_args: Any,
) -> str:
    """Derive a stable idempotency key for a workflow step invocation.

    The key is a SHA-256 digest (truncated to 128 bits / 32 hex chars,
    prefixed with the step name for human readability in logs) computed over
    ``(workflow_id, step_name, *deterministic_args)``. Stability properties:

    * Same inputs in any process at any time → same key.
    * Different inputs → different key (with overwhelming probability).
    * Order of ``deterministic_args`` matters — ``(a, b)`` ≠ ``(b, a)``.

    Format: ``"<step_name>:<32 hex chars>"``. The step-name prefix is a
    convenience for grep-ing logs; the cryptographic property comes from the
    SHA-256 portion.

    Parameters
    ----------
    workflow_id:
        The DBOS workflow ID. Typically obtained from
        :attr:`dbos.DBOS.workflow_id` inside a workflow body. Required so
        keys are scoped to a single workflow execution — re-runs of the same
        workflow definition with different DBOS-assigned IDs produce
        different keys (which is what we want: those ARE different
        executions from a side-effect perspective).
    step_name:
        Logical name of the step. Use a short, stable identifier
        (e.g. ``"notify_user"``, ``"amc_send"``). Embedded in the key prefix
        for log readability.
    deterministic_args:
        Caller-selected values that uniquely identify *this particular
        invocation's side effect*. Examples: the delegation_id for a
        ``notify_user`` send; the ``(delegation_id, threshold_seconds)`` pair
        for a heartbeat (§5.13). **Do NOT include** non-deterministic values
        (timestamps generated at call time, fresh UUIDs, random nonces) —
        those would prevent the key from matching across retries.

    Returns
    -------
    str
        The stable idempotency key.

    Raises
    ------
    TypeError
        If any ``deterministic_args`` value isn't safely canonicalizable
        (see :func:`_canonicalize`).
    ValueError
        If ``workflow_id`` or ``step_name`` is empty.

    Examples
    --------
    >>> idempotency_key("wf-123", "notify_user", "delegation-abc")
    'notify_user:8a3f...'  # truncated
    """
    if not workflow_id:
        raise ValueError("workflow_id must be non-empty")
    if not step_name:
        raise ValueError("step_name must be non-empty")
    digest = _stable_digest(workflow_id, step_name, list(deterministic_args), length=_KEY_HEX_LEN)
    return f"{step_name}:{digest}"


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------


P = ParamSpec("P")
R = TypeVar("R")

#: A function that, given the wrapped step's positional and keyword arguments,
#: returns the tuple of values that should drive key derivation. Callers pass
#: this to :func:`idempotent_step` so the framework knows which args are
#: "deterministic" and which (timestamps, fresh UUIDs) should be ignored.
DeterministicArgsFn = Callable[..., Sequence[Any]]


@dataclass(frozen=True)
class _PersistedRecord:
    """In-DBOS-event representation of an idempotency record.

    Serialized as a small dict (``{"key", "fp"}``) so an operator inspecting
    ``dbos.db`` events can see what was emitted, and so deserialization is
    schema-stable.
    """

    key: str
    fp: str

    def to_dict(self) -> dict[str, str]:
        return {"key": self.key, "fp": self.fp}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> _PersistedRecord:
        return cls(key=str(data["key"]), fp=str(data["fp"]))


def _payload_fingerprint(key: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Fingerprint the full call payload for the mismatch check.

    Includes the key itself so two different keys with the same args still
    produce different fingerprints (useful for namespacing diagnostics).
    """
    return _stable_digest(key, list(args), kwargs, length=_FP_HEX_LEN)


def _current_workflow_id() -> str | None:
    """Return ``DBOS.workflow_id`` if we're inside a workflow body, else None.

    DBOS raises (or returns ``None``, depending on version) when called
    outside a workflow context. We catch broadly so the decorator's
    test-mode fallback works regardless of the failure shape.
    """
    try:
        from dbos import DBOS  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover - dbos is a hard dep
        return None
    try:
        wf_id = DBOS.workflow_id
    except Exception:
        return None
    return wf_id if isinstance(wf_id, str) and wf_id else None


def _persist_or_check(
    *,
    step_name: str,
    key: str,
    fp: str,
) -> None:
    """Persist (key, fp) on the current workflow, or raise on mismatch.

    Uses :meth:`dbos.DBOS.set_event` for write and :meth:`dbos.DBOS.get_event`
    (against the current workflow id) for read-back. ``get_event`` blocks
    until the key exists OR the timeout elapses — we use ``timeout_seconds=0``
    so a missing key returns immediately (None) and we can take the "first
    sighting" branch.

    No-op when called outside a DBOS workflow (test mode): the in-memory
    fallback in :func:`idempotent_step` handles mismatch detection so unit
    tests don't need a running DBOS instance.
    """
    try:
        from dbos import DBOS  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover
        return

    event_key = f"{_EVENT_KEY_PREFIX}.{step_name}.{key}"
    wf_id = _current_workflow_id()
    if wf_id is None:
        # Outside a workflow — caller is in test mode. Skip DBOS persistence;
        # the in-memory cache in idempotent_step handles mismatch detection.
        return

    # Probe for an existing entry with timeout=0 so we don't block. DBOS's
    # get_event returns None when the key has never been set.
    try:
        existing = DBOS.get_event(wf_id, event_key, timeout_seconds=0)
    except Exception:
        # Some DBOS versions raise on missing rather than returning None.
        # Treat that as "first sighting".
        existing = None

    if existing is None:
        try:
            DBOS.set_event(event_key, _PersistedRecord(key=key, fp=fp).to_dict())
        except Exception as exc:
            logger.warning(
                "failed to persist idempotency record (best-effort) "
                "key=%s step=%s err=%s",
                key,
                step_name,
                exc,
                extra={
                    "event": "capo.workflows.idempotency.persist_failed",
                    "idempotency_key": key,
                    "step": step_name,
                },
            )
        return

    # Found a prior record — verify the payload fingerprint matches.
    try:
        record = _PersistedRecord.from_dict(existing)
    except (KeyError, TypeError, ValueError) as exc:
        # Persisted shape is unexpected — surface as a programmer error,
        # don't silently overwrite.
        raise IdempotencyError(
            f"corrupt idempotency record for key {key!r}: {existing!r} ({exc})"
        ) from exc

    if record.fp != fp:
        raise IdempotencyMismatchError(
            key=key,
            expected_fp=record.fp,
            actual_fp=fp,
            step_name=step_name,
        )


# In-process mismatch cache for the test-mode (no-DBOS) path. We deliberately
# scope this *per-decorator-instance* so two independently-decorated functions
# don't share state, and so test isolation is straightforward: re-decorating
# in a new test gives a fresh cache.
def _make_in_process_cache() -> dict[str, str]:
    """Factory so each decorated step gets its own dict (no cross-test bleed)."""
    return {}


def idempotent_step(
    *,
    step_name: str | None = None,
    deterministic_args: DeterministicArgsFn | None = None,
    dbos_step_kwargs: dict[str, Any] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator: wrap a step body with idempotency-key generation + persistence.

    The decorated callable is registered as a ``@DBOS.step`` (with optional
    keyword arguments forwarded via ``dbos_step_kwargs``). On each call,
    *before* the step body runs, the framework:

    1. Computes the idempotency key from
       ``(DBOS.workflow_id, step_name, *deterministic_args(args, kwargs))``.
    2. Persists ``{key, payload_fingerprint}`` as a DBOS workflow event under
       ``capo.idem.<step_name>.<key>``. This makes the key audit-trailable in
       ``dbos.db`` and lets a later step (or operator) verify that the side
       effect was emitted exactly once.
    3. If a record with the same key already exists with a *different*
       payload fingerprint, raises :class:`IdempotencyMismatchError`.

    The step body itself is unmodified — it receives the original args and
    kwargs and is free to use the key (we attach it to ``DBOS.span`` via a
    log line for tracing). On workflow re-entry, DBOS's native step-state
    mechanism replays the cached step return value without re-invoking the
    body, which means the side effect runs at most once across crashes.

    Parameters
    ----------
    step_name:
        Logical name embedded in the key prefix and DBOS event namespace.
        Defaults to ``func.__name__`` when not supplied. Keep it stable —
        renaming will invalidate the key for in-flight workflows.
    deterministic_args:
        Function taking the wrapped step's ``(*args, **kwargs)`` and returning
        the sequence of values to feed :func:`idempotency_key`. If ``None``,
        the framework hashes ``args + tuple(sorted(kwargs.items()))`` directly
        — a safe default for steps whose every argument is deterministic, but
        callers with non-deterministic inputs (timestamps, fresh UUIDs) MUST
        supply this explicitly.
    dbos_step_kwargs:
        Keyword arguments forwarded to :meth:`dbos.DBOS.step`. Useful for
        enabling DBOS-level retries or naming the step in DBOS's UI.

    Returns
    -------
    A decorator that, applied to a function, returns the registered DBOS step.

    Raises
    ------
    IdempotencyMismatchError
        When the wrapped step is called a second time with the same key but
        different args / kwargs (programmer error).

    Examples
    --------
    >>> @idempotent_step(
    ...     step_name="notify_user",
    ...     deterministic_args=lambda delegation_id, **_: (delegation_id,),
    ... )
    ... def notify_user(delegation_id: str, message: str) -> None:
    ...     amc.send(...)
    """
    step_kwargs = dict(dbos_step_kwargs or {})

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = step_name or func.__name__
        is_coro = asyncio.iscoroutinefunction(func) or inspect.iscoroutinefunction(func)
        in_proc_cache: dict[str, str] = _make_in_process_cache()

        def _derive_key_and_fp(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str, str]:
            det_args: Sequence[Any]
            if deterministic_args is None:
                # Default: hash positional args + sorted kwargs.
                det_args = (*args, *tuple(sorted(kwargs.items())))
            else:
                det_args = tuple(deterministic_args(*args, **kwargs))
            wf_id = _current_workflow_id()
            # Outside a workflow (test mode), use a stable sentinel so the
            # key derivation is still meaningful for the unit-test
            # "same inputs → same key" assertion. The sentinel is constant
            # across the process so repeated calls match.
            wf_id = wf_id or "__capo_idem_no_workflow__"
            key = idempotency_key(wf_id, name, *det_args)
            fp = _payload_fingerprint(key, args, kwargs)
            return key, fp

        def _check_in_process(key: str, fp: str) -> None:
            """In-memory mismatch detection for the test-mode (no-DBOS) path."""
            prior = in_proc_cache.get(key)
            if prior is None:
                in_proc_cache[key] = fp
                return
            if prior != fp:
                raise IdempotencyMismatchError(
                    key=key, expected_fp=prior, actual_fp=fp, step_name=name
                )

        if is_coro:

            @wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key, fp = _derive_key_and_fp(args, kwargs)
                _persist_or_check(step_name=name, key=key, fp=fp)
                _check_in_process(key, fp)
                logger.debug(
                    "idempotent step start key=%s step=%s",
                    key,
                    name,
                    extra={
                        "event": "capo.workflows.idempotency.start",
                        "idempotency_key": key,
                        "step": name,
                    },
                )
                return await func(*args, **kwargs)  # type: ignore[no-any-return,misc]

            wrapped: Callable[P, R] = async_wrapper  # type: ignore[assignment]
        else:

            @wraps(func)
            def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                key, fp = _derive_key_and_fp(args, kwargs)
                _persist_or_check(step_name=name, key=key, fp=fp)
                _check_in_process(key, fp)
                logger.debug(
                    "idempotent step start key=%s step=%s",
                    key,
                    name,
                    extra={
                        "event": "capo.workflows.idempotency.start",
                        "idempotency_key": key,
                        "step": name,
                    },
                )
                return func(*args, **kwargs)

            wrapped = sync_wrapper

        # Expose the wrapped function's idempotency-key derivation as an
        # attribute so callers can compute the key without invoking the step
        # (useful when an external receiver — e.g. AMC — needs the key as a
        # header value).
        def _key_for(*args: Any, **kwargs: Any) -> str:
            key, _fp = _derive_key_and_fp(args, kwargs)
            return key

        wrapped.idempotency_key_for = _key_for  # type: ignore[attr-defined]
        wrapped.__capo_idem_step_name__ = name  # type: ignore[attr-defined]
        wrapped.__capo_idem_cache__ = in_proc_cache  # type: ignore[attr-defined]

        # Register the wrapped callable as a DBOS step so DBOS's native
        # step-state mechanism handles resume-skip-on-success. We register
        # AFTER our wrapper so the wrapper's mismatch check + persistence run
        # under the step's checkpoint.
        try:
            from dbos import DBOS  # type: ignore[import-not-found]
        except ImportError:  # pragma: no cover - dbos is a hard dep
            return wrapped

        step_decorated = DBOS.step(**step_kwargs)(wrapped)  # type: ignore[arg-type]
        # Preserve our attached attributes on the DBOS-decorated callable.
        step_decorated.idempotency_key_for = wrapped.idempotency_key_for  # type: ignore[attr-defined]
        step_decorated.__capo_idem_step_name__ = name  # type: ignore[attr-defined]
        step_decorated.__capo_idem_cache__ = in_proc_cache  # type: ignore[attr-defined]
        return step_decorated  # type: ignore[no-any-return]

    return decorator


# Type alias for the awaitable signature variant — exported so callers can
# annotate handlers that take an idempotent-step callable.
IdempotentStep = Callable[P, R] | Callable[P, Awaitable[R]]


__all__ = [
    "IdempotencyError",
    "IdempotencyMismatchError",
    "IdempotentStep",
    "idempotency_key",
    "idempotent_step",
]
