"""Pre-agent-turn budget hook — soft + hard daily cost cap enforcement.

Spec §5.9 / §6.5 / Task #49.

The :func:`check_budget` function is called by the dispatcher BEFORE every
``agent.run`` loop on an inbound envelope. It compares today's running USD
total (Task #48's :func:`capo.costs.daily_total_usd`) against the per-user
daily caps configured under ``settings.budget``:

* ``settings.budget.soft_daily_usd`` — when crossed, the user receives a
  one-line heads-up *prepended* to the agent reply, but the turn proceeds.
* ``settings.budget.hard_daily_usd`` — when crossed, the dispatcher
  **refuses** the turn and replies with a friendly error message that
  names the cap and suggests the ``/override`` slash command.

A ``/override`` sentinel armed by Task #52 (per-thread, single-turn) lets
the user bypass exactly one hard-blocked turn. The sentinel is consumed
**only when a hard block would have fired** — it does NOT consume on a
clean turn or on a soft-warn-only turn (so the user can still queue an
override pre-emptively and it survives until actually needed).

The hook is **fail-open** on internal errors: if the cost accountant
raises or the settings access fails, :func:`check_budget` returns
``status="ok"`` and logs a warning. Cost caps are observability +
guardrails, not security — a database hiccup must never wedge the user.

Naming note
-----------

The task description (Task #49) references ``settings.cost.daily_soft_cap_usd``
/ ``settings.cost.daily_hard_cap_usd``. The project's existing
:class:`capo.config.BudgetConfig` already defines the same caps under
``settings.budget.soft_daily_usd`` / ``hard_daily_usd`` — added in the
Phase 1 config loader (Task #4). We re-use the existing namespace rather
than introducing a parallel ``[cost]`` block; the dispatcher integration
reads via ``settings.budget`` and this module exposes the resolved values
on :class:`BudgetCheckResult`. Defaults (soft=5.00, hard=20.00 USD) only
apply in the rare path where the settings access itself fails — the
TOML loader requires the caps to be present in ``config.toml``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger("capo.budget")

#: Fallback caps used when :func:`check_budget` cannot read ``settings.budget``
#: (e.g. a test passes an object without the attribute). Matches the Task #49
#: acceptance criteria defaults — soft=$5, hard=$20.
DEFAULT_SOFT_CAP_USD: Decimal = Decimal("5.00")
DEFAULT_HARD_CAP_USD: Decimal = Decimal("20.00")

#: Type alias for the four possible outcomes of :func:`check_budget`.
BudgetStatus = Literal["ok", "soft_warn", "hard_block", "overridden"]


@dataclass(frozen=True)
class BudgetCheckResult:
    """Result of a single :func:`check_budget` call.

    Attributes
    ----------
    status:
        ``"ok"`` — under both caps, proceed normally.
        ``"soft_warn"`` — at/over soft cap but under hard cap; proceed
        but prepend a heads-up to the agent reply.
        ``"hard_block"`` — at/over hard cap and no override armed;
        refuse the turn and reply with :attr:`message`.
        ``"overridden"`` — would have been ``hard_block`` but the
        caller's ``override_armed`` flag was set; proceed. The caller
        is responsible for *consuming* the override sentinel (see
        :meth:`capo.transport.dispatcher.ChannelWorker.consume_cost_override`).
    daily_usd:
        Today's UTC-calendar-day USD total for the user. ``Decimal(0)``
        when no spend recorded yet today, or on a fail-open path.
    soft_cap_usd, hard_cap_usd:
        The resolved caps used for this comparison (Decimal).
    message:
        User-facing message for ``soft_warn`` / ``hard_block`` /
        ``overridden`` outcomes; ``None`` for ``ok``.
    """

    status: BudgetStatus
    daily_usd: Decimal
    soft_cap_usd: Decimal
    hard_cap_usd: Decimal
    message: str | None


# ---------------------------------------------------------------------------
# User-facing messages — module-level constants so tests pin stable wording.
# ---------------------------------------------------------------------------


def format_soft_warn_message(daily_usd: Decimal, soft_cap_usd: Decimal) -> str:
    """One-line heads-up prepended to the agent reply when soft cap crossed."""
    return (
        f"Heads up: you've used ${daily_usd:.2f} today "
        f"(soft cap ${soft_cap_usd:.2f})."
    )


def format_hard_block_message(daily_usd: Decimal, hard_cap_usd: Decimal) -> str:
    """Friendly hard-block reply. Names the cap + suggests ``/override``.

    Wording is locked to the Task #49 acceptance criteria: the user must
    be able to (a) see the cap they hit, (b) see today's running total,
    and (c) know that ``/override`` will unblock the next turn.
    """
    return (
        f"Daily cost cap reached: ${daily_usd:.2f} of ${hard_cap_usd:.2f}. "
        "Reply with /override to bypass the cap for the next turn."
    )


def format_overridden_message(daily_usd: Decimal, hard_cap_usd: Decimal) -> str:
    """One-line note for the overridden path; prepended to the agent reply.

    Tells the user they're past the hard cap but their override is being
    consumed for this turn. Helps avoid surprise "I keep getting charged"
    follow-ups.
    """
    return (
        f"Override consumed: spend ${daily_usd:.2f} is past the "
        f"${hard_cap_usd:.2f} hard cap. Next turn re-enforces the cap."
    )


# ---------------------------------------------------------------------------
# Cap resolution
# ---------------------------------------------------------------------------


def _resolve_caps(settings: Any) -> tuple[Decimal, Decimal]:
    """Pull (soft, hard) caps from ``settings.budget`` as Decimals.

    Falls back to :data:`DEFAULT_SOFT_CAP_USD` / :data:`DEFAULT_HARD_CAP_USD`
    when the attribute access fails (defensive — the real config loader
    requires these fields, but tests sometimes pass shimmed settings).

    The TOML loader stores the caps as ``float``; we convert via ``str()``
    so we don't get binary-float noise in the comparison.
    """
    try:
        budget = settings.budget
        soft = Decimal(str(budget.soft_daily_usd))
        hard = Decimal(str(budget.hard_daily_usd))
        return soft, hard
    except (AttributeError, TypeError, ValueError) as exc:
        logger.warning(
            "budget hook: settings.budget unavailable, using defaults "
            "(soft=$%s, hard=$%s): %r",
            DEFAULT_SOFT_CAP_USD,
            DEFAULT_HARD_CAP_USD,
            exc,
            extra={"event": "capo.budget.settings.unavailable"},
        )
        return DEFAULT_SOFT_CAP_USD, DEFAULT_HARD_CAP_USD


def _utc_today_iso() -> str:
    """Today's UTC calendar day in ``"YYYY-MM-DD"`` form."""
    return datetime.now(UTC).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Public: check_budget
# ---------------------------------------------------------------------------


async def check_budget(
    user_id: str,
    parent_thread_id: str,
    db_path: Path | str,
    settings: Any,
    *,
    override_armed: bool = False,
    day_iso: str | None = None,
) -> BudgetCheckResult:
    """Pre-agent budget gate (§5.9 acceptance criteria for Task #49).

    Reads the user's daily USD total from Task #48's cost accountant,
    compares against the soft/hard caps from ``settings.budget``, and
    returns a typed :class:`BudgetCheckResult` the dispatcher uses to
    decide whether to enter the agent loop, prepend a warning, or refuse
    the turn.

    Override semantics
    ------------------

    When ``override_armed=True``:

    * Hard-block would have fired → ``status="overridden"`` (the caller
      must consume the sentinel via
      :meth:`ChannelWorker.consume_cost_override`).
    * Soft-warn would have fired → ``status="soft_warn"`` unchanged. The
      override is NOT consumed — users may pre-arm ``/override`` ahead
      of time and it survives until a hard block actually fires.
    * ``ok`` → unchanged, override NOT consumed.

    Fail-open
    ---------

    Any exception inside :func:`capo.costs.daily_total_usd` (DB missing,
    schema not migrated, query error) is caught and surfaces as
    ``status="ok"`` with a logged warning. Cost caps are observability —
    a hiccup must not wedge user-facing turns.

    Args:
        user_id: Internal user id resolved by the dispatcher's
            :func:`capo.transport.user_resolver.resolve_user_id`.
        parent_thread_id: The §5.7 ``"amc:<channel_id>"`` thread id.
            Currently unused for cap evaluation (per-user-per-day is
            the V1 cap shape) but accepted on the signature so future
            per-thread caps can land without changing the dispatcher.
        db_path: Filesystem path to ``state.db``. Typically
            ``settings.paths.db_path``.
        settings: The validated :class:`capo.config.Settings` instance
            (or a test shim). Only ``settings.budget.soft_daily_usd`` /
            ``hard_daily_usd`` are read.
        override_armed: ``True`` when the per-thread override sentinel
            is currently set (caller checked via
            :meth:`ChannelWorker.is_cost_override_armed`). See module
            docstring for consumption semantics.
        day_iso: Optional override for the UTC calendar day. Defaults
            to ``datetime.now(UTC).strftime("%Y-%m-%d")``. Test-only;
            production callers should let the default fire.

    Returns:
        A frozen :class:`BudgetCheckResult` carrying the status, the
        resolved caps (Decimal), today's spend (Decimal), and the
        user-facing message for non-``ok`` outcomes.
    """
    # ``parent_thread_id`` is intentionally accepted but unused in V1 —
    # documented in the docstring. The reference keeps it live for ruff.
    _ = parent_thread_id

    soft_cap, hard_cap = _resolve_caps(settings)
    today = day_iso or _utc_today_iso()

    # ---- Read today's spend (fail-open) ---------------------------------
    daily_usd: Decimal
    try:
        # Lazy import keeps ``capo.budget`` importable in environments
        # without the cost accountant migration applied (early-boot tests).
        from capo.costs import daily_total_usd

        result = await daily_total_usd(db_path, user_id, today)
        # ``daily_total_usd`` already returns a Decimal — defensive coerce
        # below in case a future test passes a plain int/float.
        daily_usd = result if isinstance(result, Decimal) else Decimal(str(result))
    except Exception as exc:  # noqa: BLE001 - intentional fail-open
        logger.warning(
            "budget hook: daily_total_usd failed for user_id=%r — failing open "
            "(allowing turn): %r",
            user_id,
            exc,
            extra={
                "event": "capo.budget.daily_total_usd.failed",
                "user_id": user_id,
                "error": repr(exc),
            },
        )
        return BudgetCheckResult(
            status="ok",
            daily_usd=Decimal("0.000000"),
            soft_cap_usd=soft_cap,
            hard_cap_usd=hard_cap,
            message=None,
        )

    # ---- Classify --------------------------------------------------------
    if daily_usd >= hard_cap:
        if override_armed:
            return BudgetCheckResult(
                status="overridden",
                daily_usd=daily_usd,
                soft_cap_usd=soft_cap,
                hard_cap_usd=hard_cap,
                message=format_overridden_message(daily_usd, hard_cap),
            )
        return BudgetCheckResult(
            status="hard_block",
            daily_usd=daily_usd,
            soft_cap_usd=soft_cap,
            hard_cap_usd=hard_cap,
            message=format_hard_block_message(daily_usd, hard_cap),
        )

    if daily_usd >= soft_cap:
        # Soft-cap-only crossing: override is NOT consumed (per Task #49
        # documented choice — overrides only matter when a hard block
        # would have fired). Returning ``soft_warn`` regardless of
        # ``override_armed``.
        return BudgetCheckResult(
            status="soft_warn",
            daily_usd=daily_usd,
            soft_cap_usd=soft_cap,
            hard_cap_usd=hard_cap,
            message=format_soft_warn_message(daily_usd, soft_cap),
        )

    return BudgetCheckResult(
        status="ok",
        daily_usd=daily_usd,
        soft_cap_usd=soft_cap,
        hard_cap_usd=hard_cap,
        message=None,
    )


__all__ = [
    "DEFAULT_HARD_CAP_USD",
    "DEFAULT_SOFT_CAP_USD",
    "BudgetCheckResult",
    "BudgetStatus",
    "check_budget",
    "format_hard_block_message",
    "format_overridden_message",
    "format_soft_warn_message",
]
