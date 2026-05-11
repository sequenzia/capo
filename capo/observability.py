"""Capo observability wiring (spec §6.5 — Logfire primary backend).

Phase 5 / Task #50. This module owns the Logfire boot-time configuration
and exposes a small surface used throughout the codebase:

* :func:`configure_logfire` — call once at boot from :func:`capo.main.amain`.
  Idempotent: safe to call multiple times within a single process. Catches
  the underlying Logfire SDK's exceptions structurally and logs them, but
  never raises — Logfire is a *degradable* observability dependency, not a
  hard-fail boot prerequisite (spec §6.5 implicit, Task #50 acceptance
  criterion: "Token present but invalid → Logfire logs error, app continues
  running").
* :func:`with_span` — thin sugar over :func:`logfire.span` that tolerates
  Logfire being unconfigured (no-op context manager) so call sites in
  rendering-determinism paths (§6.5 / §5.6) don't need their own guard.
* :func:`is_configured` — process-level flag flipped on by a successful
  :func:`configure_logfire`. Used by other helpers
  (e.g. ``capo.tools._subprocess._log_error``) that already have their own
  detection but may migrate to the shared flag in follow-up cleanups.
* :class:`LogfireMissingError` — typed error raised at boot when the
  ``logfire`` library is not importable (acceptance criterion: "Logfire
  library missing → typed error at boot").

Auto-instrumentation
--------------------

When :func:`configure_logfire` succeeds, it also calls Logfire's
auto-instrument hooks for the three libraries Capo uses in the request
path:

* :mod:`fastapi` — :func:`logfire.instrument_fastapi` (requires the live
  app instance; passed in via ``fastapi_app`` keyword).
* :mod:`httpx` — :func:`logfire.instrument_httpx` (process-wide patch).
* :mod:`pydantic_ai` — :func:`logfire.instrument_pydantic_ai`
  (process-wide patch — picks up every ``Agent`` instance constructed in
  the same process).

Each auto-instrument call is wrapped in its own ``try``/``except`` so a
single missing optional dependency (e.g. ``opentelemetry-instrumentation-
fastapi`` not installed) doesn't take the whole observability boot down.

LOGFIRE_IGNORE_NO_CONFIG
------------------------

Logfire emits a ``LogfireNotConfiguredWarning`` on the first ``span`` or
``info``/``warn``/``error`` call when ``configure()`` hasn't been run.
For local dev where the user doesn't have a Logfire account yet, we want
to fall back to a silent no-op instead. The spec contract:

  If ``observability.token`` is unset AND ``LOGFIRE_TOKEN`` is unset
  AND ``LOGFIRE_IGNORE_NO_CONFIG=1`` is in the process environment,
  :func:`configure_logfire` skips configuration silently and returns
  ``None``. :func:`is_configured` keeps returning ``False``.

This is the only "skip silently" branch — every other branch either
configures or logs the failure.

Span taxonomy
-------------

The span names from spec §6.5 (e.g. ``capo.boot``, ``capo.agent.run``)
are NOT enforced by this module; that's deferred to Task #51. This
module's :func:`with_span` is the canonical call site for any future
taxonomy-validating decorator, so wiring it in early keeps the migration
to Task #51 strictly additive.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from capo.config import Settings

logger = logging.getLogger("capo.observability")

# ---------------------------------------------------------------------------
# Module-level state.
# ---------------------------------------------------------------------------

#: Process-level flag flipped on by a successful :func:`configure_logfire`.
#: Use :func:`is_configured` to read it (tests can override the helper).
_CONFIGURED: bool = False

#: Lock around :data:`_CONFIGURED` so concurrent boot paths (tests in
#: pytest-asyncio for instance) don't double-configure.
_CONFIGURE_LOCK = threading.Lock()


class LogfireMissingError(RuntimeError):
    """Raised by :func:`configure_logfire` when the ``logfire`` package
    cannot be imported.

    This is a *boot-time* hard error (acceptance criterion: "Logfire
    library missing → typed error at boot"). The :mod:`capo.main` entry
    point translates this into a single-line stderr message + exit code,
    consistent with the existing ``DBOSInitError`` and ``ConfigError``
    error reporting style.
    """


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def is_configured() -> bool:
    """Return True iff :func:`configure_logfire` completed successfully
    this process. False otherwise (skipped, errored, or not yet called).
    """
    return _CONFIGURED


def configure_logfire(settings: Settings) -> Any:  # noqa: ANN401
    """Configure Logfire from validated :class:`Settings` at boot.

    Parameters
    ----------
    settings:
        Validated Capo :class:`capo.config.Settings`. Reads:

        * ``settings.observability.logfire_enabled`` — master toggle. When
          False, this function is a no-op (returns ``None`` without
          touching Logfire at all).
        * ``settings.observability.token`` — optional explicit
          :class:`pydantic.SecretStr`. When set, supersedes the env var.
        * ``settings.observability.service_name`` — service tag (default
          ``"capo"``).
        * ``settings.observability.environment`` — environment tag
          (default ``"dev"``, set to ``"prod"`` in production
          ``config.toml``).

    Returns
    -------
    The active ``logfire`` module on success; ``None`` if configuration
    was skipped (either ``logfire_enabled=False`` or the silent-skip
    ``LOGFIRE_IGNORE_NO_CONFIG`` branch).

    Raises
    ------
    LogfireMissingError
        When the ``logfire`` package cannot be imported. This is the only
        condition under which this function raises.

    Idempotency
    -----------
    Safe to call multiple times. On second-and-later calls when already
    configured, this is a no-op that returns the cached ``logfire``
    module. (Logfire's own ``configure()`` is also idempotent, but
    short-circuiting here avoids repeating the instrumentation calls and
    re-emitting the same SDK warnings.)
    """
    global _CONFIGURED

    with _CONFIGURE_LOCK:
        # Master toggle: caller asked us to skip entirely.
        if not settings.observability.logfire_enabled:
            logger.info(
                "logfire disabled via observability.logfire_enabled=false; "
                "skipping configuration",
                extra={"event": "capo.observability.disabled"},
            )
            return None

        # Idempotency: already configured this process → return cached module.
        if _CONFIGURED:
            try:
                import logfire  # noqa: PLC0415
            except ImportError as exc:  # pragma: no cover - shouldn't happen
                raise LogfireMissingError(
                    "logfire package is not importable after prior "
                    "successful configuration"
                ) from exc
            return logfire

        # Resolve token: explicit > env var > None.
        token = _resolve_token(settings)

        # Silent-skip branch: dev mode without a Logfire account.
        if token is None and os.environ.get("LOGFIRE_IGNORE_NO_CONFIG") == "1":
            logger.info(
                "logfire token absent and LOGFIRE_IGNORE_NO_CONFIG=1; "
                "skipping configuration silently",
                extra={"event": "capo.observability.skipped_no_token"},
            )
            return None

        # Import logfire — typed error at boot if missing.
        try:
            import logfire  # noqa: PLC0415
        except ImportError as exc:
            raise LogfireMissingError(
                "logfire package is not installed; install with "
                "`uv sync` (logfire is a runtime dependency per spec §6.5)"
            ) from exc

        # Configure. Wrapped so a bad token / network failure / SDK bug
        # never aborts boot — observability degrades, app continues.
        try:
            logfire.configure(
                token=token,
                service_name=settings.observability.service_name,
                environment=settings.observability.environment,
                send_to_logfire=token is not None,
            )
        except Exception as exc:  # noqa: BLE001 - intentional broad catch
            # Acceptance criterion: configure_logfire exceptions are
            # caught + logged structurally; app continues.
            logger.exception(
                "logfire configure() failed; observability is degraded",
                extra={
                    "event": "capo.observability.configure_failed",
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
            )
            return None

        # Auto-instrument. Each library wrapped in its own try/except so a
        # single failure doesn't cascade — e.g. pydantic_ai not installed
        # in a slimmed-down image shouldn't kill httpx + fastapi tracing.
        _instrument_libraries(logfire, settings)

        _CONFIGURED = True
        logger.info(
            "logfire configured service=%s environment=%s token=%s",
            settings.observability.service_name,
            settings.observability.environment,
            "present" if token is not None else "absent",
            extra={
                "event": "capo.observability.configured",
                "service_name": settings.observability.service_name,
                "environment": settings.observability.environment,
                "has_token": token is not None,
            },
        )
        return logfire


def with_span(name: str, **attrs: Any) -> Any:  # noqa: ANN401
    """Return a span context manager for ``name`` with ``attrs``.

    Thin wrapper over :func:`logfire.span` that:

    * Tolerates Logfire being unconfigured (returns a no-op context
      manager) — call sites in deterministic paths shouldn't need their
      own ``if is_configured(): ...`` guards.
    * Tolerates Logfire being missing (same no-op fallback) — keeps the
      ``capo.observability`` import safe even in slimmed-down test envs
      where ``logfire`` isn't pinned.

    Usage::

        from capo.observability import with_span

        with with_span("capo.amc.send", channel_id=cid, idempotency_key=k):
            await amc_client.send(cid, text, idempotency_key=k)

    Task #51 will layer on a taxonomy-enforcing decorator; this helper is
    the canonical call site for that future enforcement.
    """
    # If we haven't configured Logfire, every span call would emit a
    # LogfireNotConfiguredWarning and pollute logs/test output. Fast-path
    # to a no-op CM instead.
    if not _CONFIGURED:
        return contextlib.nullcontext()

    try:
        import logfire  # noqa: PLC0415
    except ImportError:  # pragma: no cover - configured implies importable
        return contextlib.nullcontext()

    try:
        return logfire.span(name, **attrs)
    except Exception as exc:  # noqa: BLE001 - never block on observability
        logger.warning(
            "logfire.span(%r) failed; emitting no-op span: %s",
            name,
            exc,
            extra={
                "event": "capo.observability.span_failed",
                "span_name": name,
                "error_type": exc.__class__.__name__,
            },
        )
        return contextlib.nullcontext()


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _resolve_token(settings: Settings) -> str | None:
    """Resolve the Logfire write token from settings/env.

    Precedence:
      1. ``settings.observability.token`` (SecretStr) if set.
      2. ``LOGFIRE_TOKEN`` from ``os.environ`` if non-empty.
      3. ``None``.
    """
    explicit = settings.observability.token
    if explicit is not None:
        value = explicit.get_secret_value()
        if value:
            return value

    env_token = os.environ.get("LOGFIRE_TOKEN")
    if env_token:
        return env_token

    return None


def _instrument_libraries(logfire: Any, settings: Settings) -> None:  # noqa: ANN401, ARG001
    """Apply auto-instrumentation patches (FastAPI, httpx, Pydantic AI).

    Each patch is wrapped in its own ``try``/``except`` so a single
    failure (typically a missing optional ``opentelemetry-instrumentation-
    *`` extra) doesn't take down the others. We log each failure
    structurally so an operator can see which library tracing is
    degraded.

    FastAPI auto-instrumentation requires the live :class:`fastapi.FastAPI`
    app instance, which Capo doesn't construct until *after*
    :func:`configure_logfire` runs in :func:`capo.main.amain`. We
    therefore expose a separate :func:`instrument_fastapi_app` helper
    that the boot path calls once the app is built. The httpx and
    pydantic_ai patches don't need any handle and are applied here.
    """
    # httpx: process-wide patch. Picks up every AsyncClient/Client.
    try:
        logfire.instrument_httpx()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "logfire.instrument_httpx() failed: %s", exc,
            extra={
                "event": "capo.observability.instrument_httpx_failed",
                "error_type": exc.__class__.__name__,
            },
        )

    # pydantic_ai: process-wide patch. Picks up every Agent instance.
    try:
        logfire.instrument_pydantic_ai()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "logfire.instrument_pydantic_ai() failed: %s", exc,
            extra={
                "event": "capo.observability.instrument_pydantic_ai_failed",
                "error_type": exc.__class__.__name__,
            },
        )


def instrument_fastapi_app(app: Any) -> None:  # noqa: ANN401
    """Apply Logfire's FastAPI auto-instrument to a live app instance.

    Called from :func:`capo.main.amain` AFTER :func:`build_app` returns
    and AFTER :func:`configure_logfire` has been invoked. No-op when
    Logfire is unconfigured or the library is missing — keeps the boot
    path tolerant of partial observability setups.
    """
    if not _CONFIGURED:
        return
    try:
        import logfire  # noqa: PLC0415
    except ImportError:  # pragma: no cover
        return
    try:
        logfire.instrument_fastapi(app)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "logfire.instrument_fastapi(app) failed: %s", exc,
            extra={
                "event": "capo.observability.instrument_fastapi_failed",
                "error_type": exc.__class__.__name__,
            },
        )


def _reset_for_tests() -> None:
    """Reset the module-level configured flag.

    Tests that exercise :func:`configure_logfire` more than once in the
    same process need to clear the idempotency latch between cases. Not
    part of the public API; intentionally underscore-prefixed.
    """
    global _CONFIGURED
    with _CONFIGURE_LOCK:
        _CONFIGURED = False


# ---------------------------------------------------------------------------
# Task #51 — named span constructors (spec §6.5 taxonomy).
# ---------------------------------------------------------------------------
#
# The spec §6.5 table is the *single source of truth* for the Capo span
# taxonomy. To prevent ad-hoc drift (call sites picking names by hand,
# typos, missing required attributes), every production span in ``capo/``
# MUST be opened via one of the named constructors below. Each constructor:
#
# * pins the span name to the spec verbatim (or to the documented
#   ``capo.tool.<name>`` / ``capo.delegation.<agent>.<phase>`` pattern);
# * declares its required attributes as keyword-only parameters so the
#   type checker + Pydantic AI's strict-kwargs enforcement flag missing
#   fields at the call site;
# * accepts arbitrary additional ``extra`` attributes via ``**extra`` —
#   useful for context-bound IDs (trace IDs, parent thread IDs, etc.)
#   without bloating the constructor signature.
#
# Constructors return whatever :func:`with_span` returns — a context
# manager (either a real ``logfire.span`` or a ``contextlib.nullcontext``).
#
# Why constructors rather than decorators?
#
# Most call sites wrap an ``await`` inside a ``with`` block, sometimes
# with branching (e.g. ``send_failed_terminally`` flag flips, exception
# remapping). A decorator would force restructuring those into helper
# functions; the cost outweighs the benefit. Constructors keep the
# existing code shape while still concentrating the taxonomy in one file.
#
# The :mod:`tests.test_span_taxonomy_audit` test scans ``capo/`` for raw
# ``with_span(`` calls and rejects any that aren't on this module's own
# ``with_span`` definition — i.e. production code may only acquire spans
# via these constructors.


def _agent_slug(agent: str) -> str:
    """Validate the ``<agent>`` slug used in delegation span names.

    Spec §6.5 hard-codes ``claude_code`` and ``codex`` as the two
    sub-agents. We accept arbitrary lowercase ``[a-z0-9_]`` slugs so
    future Phase 6+ agents (e.g. ``gemini``) can plug in without a
    constructor edit, but reject anything containing a dot (which would
    collide with the span name hierarchy) or empty / non-ASCII content.
    """
    if not agent or not isinstance(agent, str):
        raise ValueError("agent slug must be a non-empty string")
    if "." in agent:
        raise ValueError(
            f"agent slug must not contain '.' (got {agent!r}); "
            "it is concatenated into the span name"
        )
    if not all(c.isascii() and (c.isalnum() or c in ("_", "-")) for c in agent):
        raise ValueError(
            f"agent slug must match [a-z0-9_-] (got {agent!r})"
        )
    return agent


def _tool_slug(tool_name: str) -> str:
    """Validate the agent-facing tool name used in ``capo.tool.<name>``.

    Pydantic AI tool names are snake_case (the convention enforced by
    ``@agent.tool``); we lock the constructor to that shape so the span
    name stays predictable for Logfire queries.
    """
    if not tool_name or not isinstance(tool_name, str):
        raise ValueError("tool_name must be a non-empty string")
    if "." in tool_name or " " in tool_name:
        raise ValueError(
            f"tool_name must be snake_case (got {tool_name!r}); "
            "it is concatenated into the span name"
        )
    return tool_name


def boot_span(
    *,
    version: str,
    pid: int,
    config_path: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.boot`` — process boot path (spec §6.5)."""
    return with_span(
        "capo.boot",
        version=version,
        pid=pid,
        config_path=config_path,
        **extra,
    )


def webhook_span(
    *,
    delivery_id: str,
    channel_id: str | None,
    signature_ok: bool,
    dedupe_hit: bool,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.amc.webhook.in`` — inbound webhook handler (spec §6.5).

    ``channel_id`` may be ``None`` when the envelope failed to parse
    (the channel isn't known until after parsing succeeds). All other
    required attributes have well-defined values at every call site.
    """
    return with_span(
        "capo.amc.webhook.in",
        delivery_id=delivery_id,
        channel_id=channel_id,
        signature_ok=signature_ok,
        dedupe_hit=dedupe_hit,
        **extra,
    )


def dispatcher_handle_span(
    *,
    thread_id: str,
    user_id: str | None,
    message_id: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.dispatcher.handle`` — per-channel worker turn (spec §6.5).

    ``user_id`` may be ``None`` at the very top of the worker turn,
    before the §5.11 sender resolution gate runs.
    """
    return with_span(
        "capo.dispatcher.handle",
        thread_id=thread_id,
        user_id=user_id,
        message_id=message_id,
        **extra,
    )


def agent_run_span(
    *,
    thread_id: str,
    user_id: str,
    model: str,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_usd: float = 0.0,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.agent.run`` — Pydantic AI agent run (spec §6.5).

    Token counts and cost are opened at zero and may be updated by the
    caller on the live span (via the returned context manager's
    ``set_attribute`` method when logfire is configured). The
    no-op fallback context manager silently ignores those updates.
    """
    return with_span(
        "capo.agent.run",
        thread_id=thread_id,
        user_id=user_id,
        model=model,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cost_usd=cost_usd,
        **extra,
    )


def tool_span(
    tool_name: str,
    *,
    result_status: str = "pending",
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.tool.<tool_name>`` — tool execution (spec §6.5).

    ``tool_name`` is the agent-facing tool name as registered with
    Pydantic AI (snake_case). Examples: ``web_search``, ``fetch_url``,
    ``shell_exec``, ``delegate_to_claude_code``, ``delegate_to_codex``,
    ``session_new``, ``kill_delegation``.

    The full span name becomes ``capo.tool.<tool_name>`` so a Logfire
    operator can filter by tool family with a single prefix predicate.

    ``result_status`` opens as ``"pending"`` (the call-site is expected
    to update it before exit — typically to ``"ok"``, ``"error"``, or a
    tool-specific code like ``"approval_required"``). The fallback no-op
    span ignores the value.
    """
    slug = _tool_slug(tool_name)
    return with_span(
        f"capo.tool.{slug}",
        tool_name=tool_name,
        result_status=result_status,
        **extra,
    )


def delegation_spawn_span(
    *,
    delegation_id: str,
    agent: str,
    model: str | None,
    pid: int | None,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.delegation.<agent>.spawn`` — subprocess spawn step (§6.5).

    ``pid`` may be ``None`` when the span is opened *before* the spawn
    actually returns a live process handle (e.g. the constructor is
    called as a context manager around the whole spawn attempt, and the
    PID is filled in on the live span via ``set_attribute`` once known).
    """
    slug = _agent_slug(agent)
    return with_span(
        f"capo.delegation.{slug}.spawn",
        delegation_id=delegation_id,
        agent=agent,
        model=model,
        pid=pid,
        **extra,
    )


def delegation_monitor_span(
    *,
    delegation_id: str,
    agent: str,
    step_name: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.delegation.<agent>.monitor`` — DBOS workflow body (§6.5).

    Wrapped around the body of :func:`capo.workflows.delegation.monitor_delegation`
    (and any retry attempts on workflow re-entry). ``step_name`` is the
    DBOS step the workflow is currently executing (or ``"workflow"`` if
    the span covers the entire workflow body).
    """
    slug = _agent_slug(agent)
    return with_span(
        f"capo.delegation.{slug}.monitor",
        delegation_id=delegation_id,
        agent=agent,
        step_name=step_name,
        **extra,
    )


def delegation_complete_span(
    *,
    delegation_id: str,
    agent: str,
    runtime_s: float,
    cost_usd: float,
    status: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.delegation.<agent>.complete`` — completion step (§6.5).

    ``status`` ∈ {``completed``, ``failed``, ``killed``} per the
    returncode classification in :mod:`capo.workflows.delegation`.
    """
    slug = _agent_slug(agent)
    return with_span(
        f"capo.delegation.{slug}.complete",
        delegation_id=delegation_id,
        agent=agent,
        runtime_s=runtime_s,
        cost_usd=cost_usd,
        status=status,
        **extra,
    )


def amc_send_span(
    *,
    channel_id: str,
    idempotency_key: str,
    error_code: str | None = None,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.amc.send`` — outbound AMC REST (spec §6.5).

    ``error_code`` opens as ``None`` and is set by the caller on the
    live span (when configured) if an :class:`AMCError` surfaces. The
    no-op fallback silently ignores updates.
    """
    return with_span(
        "capo.amc.send",
        channel_id=channel_id,
        idempotency_key=idempotency_key,
        error_code=error_code,
        **extra,
    )


def memory_compact_span(
    *,
    thread_id: str,
    messages_summarized: int,
    tokens_before: int,
    tokens_after: int,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.memory.compact`` — compaction event (spec §6.5).

    Phase 5 wiring (Task #56 owns memory compaction). The constructor
    is provided eagerly so the compaction implementer doesn't need to
    extend this module.
    """
    return with_span(
        "capo.memory.compact",
        thread_id=thread_id,
        messages_summarized=messages_summarized,
        tokens_before=tokens_before,
        tokens_after=tokens_after,
        **extra,
    )


def budget_cap_span(
    *,
    cap_kind: str,
    usd_today: float,
    usd_limit: float,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.budget.cap`` — soft/hard cap hit (spec §6.5).

    ``cap_kind`` ∈ {``soft``, ``hard``, ``override``} per Task #49's
    budget vocabulary.
    """
    return with_span(
        "capo.budget.cap",
        cap_kind=cap_kind,
        usd_today=usd_today,
        usd_limit=usd_limit,
        **extra,
    )


def approval_request_span(
    *,
    approval_id: str,
    action_kind: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.approval.request`` — approval requested (spec §6.5).

    ``action_kind`` describes what the user is being asked to approve
    (e.g. ``"shell_exec"``, ``"delegate_claude_code"``). The span
    covers the full request→decision lifecycle inside the DBOS
    workflow body (the workflow does not split request and resolve
    into separate spans — :func:`approval_resolve_span` is opened at
    the dispatcher's ``/approve``/``/deny`` shortcut handler).
    """
    return with_span(
        "capo.approval.request",
        approval_id=approval_id,
        action_kind=action_kind,
        **extra,
    )


def approval_resolve_span(
    *,
    approval_id: str,
    action_kind: str,
    choice: str,
    **extra: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Open ``capo.approval.resolve`` — approval resolved (spec §6.5).

    ``choice`` ∈ {``approve``, ``deny``, ``timeout``} per the §5.8
    approval semantics. Opened at the dispatcher's slash-command
    handler when a user replies with ``/approve`` or ``/deny``, and at
    the DBOS workflow's timeout branch.
    """
    return with_span(
        "capo.approval.resolve",
        approval_id=approval_id,
        action_kind=action_kind,
        choice=choice,
        **extra,
    )


#: Set of canonical span names produced by this module (the verbatim
#: §6.5 table plus the parametric ``capo.tool.*`` and
#: ``capo.delegation.<agent>.*`` families). The audit test imports this
#: to confirm constructors map 1:1 to spec rows.
CANONICAL_SPAN_NAMES: frozenset[str] = frozenset(
    {
        "capo.boot",
        "capo.amc.webhook.in",
        "capo.dispatcher.handle",
        "capo.agent.run",
        # capo.tool.<name> — parametric.
        # capo.delegation.<agent>.{spawn,monitor,complete} — parametric.
        "capo.amc.send",
        "capo.memory.compact",
        "capo.budget.cap",
        "capo.approval.request",
        "capo.approval.resolve",
    }
)


__all__ = [
    "CANONICAL_SPAN_NAMES",
    "LogfireMissingError",
    "agent_run_span",
    "amc_send_span",
    "approval_request_span",
    "approval_resolve_span",
    "boot_span",
    "budget_cap_span",
    "configure_logfire",
    "delegation_complete_span",
    "delegation_monitor_span",
    "delegation_spawn_span",
    "dispatcher_handle_span",
    "instrument_fastapi_app",
    "is_configured",
    "memory_compact_span",
    "tool_span",
    "webhook_span",
    "with_span",
]
