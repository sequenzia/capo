"""Per-channel asyncio dispatcher (spec §5.2, §5.11, §9.1, §15.3).

The dispatcher owns the back half of Capo's inbound transport. The webhook
handler (Task #10) verifies HMAC + dedupes + ACKs ``204`` in < 1s, then hands
the envelope to :meth:`Dispatcher.enqueue`. From there the dispatcher owns
the full turn lifecycle:

1. Route the envelope to a per-``channel_id`` :class:`ChannelWorker`, lazily
   creating one on first contact. Different channels run in parallel — a
   slow turn on one channel does not block another channel.
2. Inside the worker, in strict arrival order:

   a. Resolve ``sender_id`` → internal ``user_id`` via
      :func:`capo.transport.user_resolver.resolve_user_id`. Unknown senders
      are rejected (logged with the canonical structured record) and the
      message is still marked-read so AMC stops redelivering it.
   b. Materialize the conversation history for ``(user_id, thread_id)``
      and the active session_id via :mod:`capo.memory.conversation`.
   c. Run the agent: ``await agent.run(text, message_history=history,
      deps=CapoDeps(...))``.
   d. Persist ``result.new_messages()`` to the per-thread history.
   e. ``amc.send`` the agent output back to the channel, then
      ``amc.mark_read`` the inbound message — both via the typed
      :class:`capo.transport.amc_client.AMCClient`.

3. Errors during any step are caught at the worker boundary so a single
   bad turn never kills the dispatcher. Specific AMC error codes get the
   §5.2 / §7.5 treatment:

   * ``RATE_LIMITED`` — :class:`AMCClient` already sleeps ``Retry-After``
     internally; the dispatcher does NOT retry on top. Documented decision
     to keep the retry policy in one place.
   * ``PLATFORM_AUTH`` — log a Logfire-friendly alert and do NOT retry.
     A fallback reply attempt is made; if AMC is unreachable that attempt
     itself fails and we move on. We never crash the dispatcher.
   * ``CHANNEL_NOT_FOUND`` — log + do NOT retry + do NOT attempt a
     fallback reply (the channel is gone).

Queue-depth guard: each :class:`ChannelWorker` owns an
:class:`asyncio.Queue` bounded by ``settings.concurrency.queue_depth_max``
(default 100, see §5.12 healthz + §15.3). Overflow is *dropped with a
logged warning* in Phase 1 — Task #10 may later promote this to a 429
surface back to the caller; the API entry point is
:meth:`Dispatcher.enqueue` which already returns ``True``/``False``
indicating whether the envelope was accepted.

Logging
-------

This module uses stdlib :mod:`logging` under the
``capo.transport.dispatcher`` logger. Logfire instrumentation will tee the
same logger automatically (spec §6.5); no Logfire dependency is required.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from capo.budget import (
    BudgetCheckResult,
    check_budget,
)
from capo.costs import record_run_usage
from capo.deps import CapoDeps
from capo.memory.compaction import (
    SummarizerProtocol,
    compact_thread_async,
    should_compact,
)
from capo.memory.conversation import (
    append_messages,
    get_or_create_active_session,
    load_history,
    thread_id_for_amc,
)
from capo.observability import agent_run_span
from capo.tools.basic import ApprovalRequired
from capo.transport.amc_client import (
    AMCError,
    AMCInboundEnvelope,
    ChannelNotFound,
    PlatformAuth,
)
from capo.transport.slash import (
    DELEGATION_ID_RE,
    SlashCommand,
    parse_slash_command,
)
from capo.transport.user_resolver import (
    format_unknown_sender_log_record,
    resolve_user_id,
)
from capo.workflows.approval import APPROVAL_TOPIC

if TYPE_CHECKING:  # pragma: no cover - typing only
    import httpx

    from capo.config import Settings
    from capo.transport.amc_client import AMCClient

logger = logging.getLogger("capo.transport.dispatcher")

#: User-facing fallback message when AMC auth is broken (§5.2 error table).
PLATFORM_AUTH_REPLY: str = (
    "Capo can't reach AMC — check your bearer token"
)

#: User-facing fallback message when the channel has vanished (§5.2).
CHANNEL_NOT_FOUND_REPLY: str = "I can't reach that channel anymore"

#: User-facing fallback message for any other internal error (§5.2 edge case
#: "Worker crash mid-turn"). Kept generic — the structured log carries the
#: actual exception detail.
INTERNAL_ERROR_REPLY: str = (
    "Something went wrong handling your message. Capo will retry."
)


def format_approval_required_reply(exc: ApprovalRequired) -> str:
    """Render the Phase-2 stub reply for an :class:`ApprovalRequired` exception.

    The full approval workflow (§5.8) lands in Phase 4. Phase 2 catches the
    exception at the dispatcher boundary and replies with a fixed stub that
    surfaces the original command + reason so the user sees *why* the agent
    bailed. Wording is locked: tests assert this exact format so a future
    Phase-4 rewrite stays intentional rather than accidental.
    """
    return (
        f"That action ({exc.command}) needs approval. "
        f"Reason: {exc.reason}. Full approval flow lands in Phase 4."
    )


# Type alias: a zero-arg callable returning a fresh sqlite3.Connection. The
# dispatcher calls this once per envelope so workers don't share a connection
# across asyncio tasks (sqlite3 connections are not thread-safe by default).
StoreConnFactory = Callable[[], sqlite3.Connection]

# Type alias: a zero-arg callable returning a fresh CapoDeps for this turn.
# Optional; defaults to a thin shim that builds one from Settings + a shared
# httpx client. Tests inject a fake to avoid touching httpx at all.
DepsFactory = Callable[[str], CapoDeps]


# ===== BEGIN Task #40: Approval inbound routing (§5.8 / §5.9 / §7.5) =========
# This region intercepts ``/approve [<id>]`` and ``/deny [<id>]`` BEFORE the
# agent loop runs. Approval replies are pre-agent control messages — they
# resolve a pending DBOS workflow via ``DBOS.send_async`` on the
# ``approval_decision`` topic and never enter the LLM-driven agent. The
# slash-command parser itself lives in :mod:`capo.transport.slash` (Task
# #41); this module wires the resulting :class:`SlashCommand` into the DBOS
# approval workflow.
# =============================================================================


#: Friendly reply strings — kept module-level so the test suite asserts on
#: stable wording. Wording is intentional and reviewed; future edits should
#: update the dispatcher tests in lock-step.
APPROVAL_REPLY_NO_PENDING: str = (
    "No pending approval found in this thread."
)
APPROVAL_REPLY_UNKNOWN_ID: str = (
    "No approval with that id was found."
)
APPROVAL_REPLY_ALREADY_RESOLVED: str = (
    "That approval is already resolved."
)
APPROVAL_REPLY_STILL_CREATING: str = (
    "Approval is still being created, try again in a moment."
)


def _select_approval_row(
    conn: sqlite3.Connection,
    *,
    approval_id: str | None,
    parent_thread_id: str,
) -> dict[str, Any] | None:
    """Look up the relevant ``approvals`` row.

    If ``approval_id`` is provided, return that row (regardless of status —
    the caller decides whether ``already resolved`` applies). If ``None``,
    return the **most recent pending** approval in ``parent_thread_id`` per
    §5.9 acceptance criteria (``ORDER BY requested_at DESC LIMIT 1``).
    """
    conn.row_factory = sqlite3.Row
    if approval_id is not None:
        row = conn.execute(
            "SELECT approval_id, status, workflow_id, parent_thread_id "
            "FROM approvals WHERE approval_id = ?",
            (approval_id,),
        ).fetchone()
        return dict(row) if row else None
    row = conn.execute(
        "SELECT approval_id, status, workflow_id, parent_thread_id "
        "FROM approvals WHERE parent_thread_id = ? AND status = 'pending' "
        "ORDER BY requested_at DESC LIMIT 1",
        (parent_thread_id,),
    ).fetchone()
    return dict(row) if row else None


async def _send_approval_decision(
    workflow_id: str,
    payload: dict[str, Any],
) -> None:
    """Send the approval decision via :meth:`dbos.DBOS.send_async`.

    Uses the ``approval_decision`` topic (imported as :data:`APPROVAL_TOPIC`).
    Falls back to the sync ``DBOS.send`` via ``asyncio.to_thread`` for older
    DBOS versions that did not expose an async send — mirrors the fallback
    in :func:`capo.workflows.approval.force_resolve_approval`.
    """
    from dbos import DBOS  # type: ignore[import-not-found]

    try:
        await DBOS.send_async(workflow_id, payload, APPROVAL_TOPIC)
    except Exception:  # noqa: BLE001 - intentional fallback
        await asyncio.to_thread(
            DBOS.send, workflow_id, payload, APPROVAL_TOPIC
        )


def format_approval_recorded_reply(approval_id: str) -> str:
    """Render the AMC acknowledgement for a recorded approval decision.

    Kept as a function (rather than a constant) so the approval_id is
    embedded in a predictable way and the test suite can assert against
    one canonical format.
    """
    return f"approval recorded: {approval_id}"


# ===== END Task #40 ==========================================================


# ===== BEGIN Task #52: Session-control slash commands (§5.10) ================
# Pre-agent handlers for ``/new``, ``/status``, ``/clear``, ``/kill <id>``,
# ``/override``. These commands are intercepted by the dispatcher BEFORE the
# agent loop runs (same pre-agent shape as the Task #40 approval shortcuts).
# Each command consumes zero LLM tokens. The dispatcher writes back an AMC
# reply describing the side effect.
#
# Naming convention for reply strings: module-level constants so the test
# suite asserts on stable wording. Wording is intentional and reviewed —
# updates to the strings here must update the corresponding dispatcher
# tests in lock-step.
# =============================================================================


#: Reply text emitted by ``/new`` after rolling the conversation thread to
#: a fresh session row.
SESSION_REPLY_NEW: str = "Started new session"

#: Reply text emitted by ``/clear`` after deleting per-thread conversation
#: message rows.
SESSION_REPLY_CLEARED: str = "Conversation cleared"

#: Reply text emitted by ``/clear`` when the thread already had no
#: messages. Distinct from :data:`SESSION_REPLY_CLEARED` so the user can
#: tell the difference if they care; identical phrasing trailer keeps it
#: friendly.
SESSION_REPLY_CLEARED_EMPTY: str = "Conversation cleared (nothing to clear)"

#: Reply text emitted by ``/override`` after setting the per-thread
#: bypass-cost-cap sentinel. Task #49 reads the sentinel before the cost
#: cap check on the next agent turn.
SESSION_REPLY_OVERRIDE_ARMED: str = (
    "Cost cap override armed for the next agent turn"
)

#: Reply text emitted by ``/kill`` when no delegation id was supplied.
SESSION_REPLY_KILL_MISSING_ID: str = (
    "missing delegation id (usage: /kill <delegation_id>)"
)

#: Reply text emitted by ``/kill <bad_id>`` when the token is not a valid
#: 32-lowercase-hex shape OR the id is shaped but no row exists.
SESSION_REPLY_KILL_UNKNOWN: str = "unknown delegation"

#: Reply template for unknown slash verbs (e.g. ``/foo``). The dispatcher
#: formats with the user-supplied verb so the user sees their typo.
SESSION_REPLY_UNKNOWN_VERB: str = (
    "Unknown command: /{verb}. "
    "Supported: /new /status /clear /kill /override /approve /deny"
)


def format_status_reply(
    *,
    model: str,
    active_delegations: int,
    daily_cost_usd: float | None,
    conversation_message_count: int | None,
) -> str:
    """Render the ``/status`` AMC reply.

    The format is a compact multi-line summary the user can read on
    mobile. Kept deterministic (no timestamps, no env reads) so the test
    suite can assert against a fixed string.

    Parameters
    ----------
    model
        The current orchestrator model id (``settings.models.default``).
    active_delegations
        Count of ``delegations`` rows with ``status='running'`` for the
        current user.
    daily_cost_usd
        Today's USD total (UTC date) for the current user, summed across
        all models, from Task #48's cost accountant. ``None`` when the
        cost accountant is unavailable (legacy environment, or the
        ``costs`` table is missing) — the reply renders ``unavailable``
        so operators can spot the gap.
    conversation_message_count
        Number of rows in ``conversation_history`` for the active session.
        ``None`` when no active session exists yet — reply renders 0.
    """
    cost_line = (
        f"${daily_cost_usd:.4f}" if daily_cost_usd is not None else "unavailable"
    )
    msgs_line = (
        str(conversation_message_count)
        if conversation_message_count is not None
        else "0"
    )
    return (
        "Status:\n"
        f"  model: {model}\n"
        f"  active delegations: {active_delegations}\n"
        f"  conversation messages: {msgs_line}\n"
        f"  today's cost: {cost_line}"
    )


def format_kill_reply(*, delegation_id: str, result: dict[str, Any]) -> str:
    """Render the ``/kill`` AMC reply from :func:`kill_delegation` output.

    :func:`capo.tools.delegations.kill_delegation` returns a dict whose
    ``status`` is either ``"killed"`` or ``"already_terminal"``.
    """
    status = str(result.get("status") or "unknown")
    if status == "killed":
        cascaded = result.get("cascaded_approvals")
        if isinstance(cascaded, int) and cascaded > 0:
            return (
                f"killed delegation {delegation_id} "
                f"(cancelled {cascaded} tied approval"
                f"{'s' if cascaded != 1 else ''})"
            )
        return f"killed delegation {delegation_id}"
    if status == "already_terminal":
        prior = str(result.get("prior_status") or "terminal")
        return (
            f"delegation {delegation_id} already terminal "
            f"(status={prior})"
        )
    return f"kill_delegation({delegation_id}) → {status}"


def format_unknown_command_reply(verb: str) -> str:
    """Render the friendly reply for an unrecognized slash verb."""
    return SESSION_REPLY_UNKNOWN_VERB.format(verb=verb)


# Matches a leading ``/<verb>`` token on the first line of the inbound
# text. The dispatcher uses this to detect the "user typed a slash
# command but the parser didn't recognize the verb" case so we can emit
# a friendly "Unknown command: /<verb>" reply instead of asking the
# agent to handle it. Mirrors the parser's verb regex but tolerates a
# wider trailing-character set (we extract for display, not for
# recognition).
_UNKNOWN_VERB_RE = __import__("re").compile(r"^\s*/([A-Za-z][A-Za-z0-9_-]*)")


def _extract_unknown_slash_verb(text: object) -> str | None:
    """Return the verb of a leading slash command, or ``None`` if not slash-shaped.

    Only fires when:

    * ``text`` is a string.
    * Its first non-empty line starts with ``/<verb>`` where ``<verb>``
      is a letter followed by letters/digits/underscore/dash.
    * ``<verb>`` (lowercased) is NOT in
      :data:`capo.transport.slash.RECOGNIZED_VERBS`. (If it IS recognized
      the parser already handled it — we wouldn't be here.)

    Returns the verb (without the leading ``/``, in lowercase) for the
    dispatcher to embed in the friendly error. Returns ``None`` when
    the text doesn't look like a slash command at all (natural-language
    inputs flow through to the agent).
    """
    if not isinstance(text, str):
        return None
    first_line = text.splitlines()[0] if text else ""
    match = _UNKNOWN_VERB_RE.match(first_line)
    if match is None:
        return None
    from capo.transport.slash import RECOGNIZED_VERBS

    verb = match.group(1).lower()
    if verb in RECOGNIZED_VERBS:
        return None
    return verb


# ---- Task #52 / Task #53 shared helpers (refactored) -----------------------
#
# The synchronous DB helpers + ``read_daily_cost_usd`` were refactored into
# :mod:`capo.tools.session` (Task #53) so both the slash-command path
# (this module) and the natural-language tool path
# (``session_new`` / ``session_status`` / ``session_clear``) share one
# implementation. The private aliases below preserve the in-module name
# (``_select_active_session_id`` etc.) so the inline handler bodies further
# down — and the dispatcher test suite — keep working unchanged.
from capo.tools.session import (  # noqa: E402 - keep import grouped with consumers
    delete_thread_history as _delete_thread_history,
)
from capo.tools.session import (  # noqa: E402
    end_active_session as _end_active_session,
)
from capo.tools.session import (  # noqa: E402
    read_daily_cost_usd as _read_daily_cost_usd,
)
from capo.tools.session import (  # noqa: E402
    select_active_delegation_count as _select_active_delegation_count,
)
from capo.tools.session import (  # noqa: E402
    select_active_session_id as _select_active_session_id,
)
from capo.tools.session import (  # noqa: E402
    select_conversation_count as _select_conversation_count,
)

# ===== END Task #52 ==========================================================


class ChannelWorker:
    """One asyncio worker dedicated to a single ``channel_id``.

    Owns:

    * A bounded :class:`asyncio.Queue` (``maxsize = queue_depth_max``).
    * A long-running :class:`asyncio.Task` running :meth:`_run`.

    Envelopes enqueued via :meth:`put_nowait` are processed in strict FIFO
    order; the worker never advances to the next envelope until the current
    turn either succeeds, fails-and-falls-back, or is dropped (unknown
    sender). All exceptions are caught inside :meth:`_run` so the task
    stays alive across faulty turns.
    """

    def __init__(
        self,
        channel_id: str,
        *,
        settings: Settings,
        agent: Any,
        amc_client: AMCClient,
        store_conn_factory: StoreConnFactory,
        deps_factory: DepsFactory,
        loop: asyncio.AbstractEventLoop | None = None,
        compaction_summarizer: SummarizerProtocol | None = None,
    ) -> None:
        self.channel_id = channel_id
        self._settings = settings
        self._agent = agent
        self._amc = amc_client
        self._conn_factory = store_conn_factory
        self._deps_factory = deps_factory
        self._loop = loop
        # Task #54 (§5.7 hybrid compaction): optional cheap-model agent the
        # dispatcher invokes after every turn that exceeds threshold. Tests
        # inject a stub; production injection lives in :class:`Dispatcher`
        # (built lazily from ``settings.models.router``). When ``None``,
        # compaction is silently skipped — the dispatcher MUST stay alive
        # even if no summarizer was wired.
        self._compaction_summarizer: SummarizerProtocol | None = (
            compaction_summarizer
        )

        self._queue: asyncio.Queue[AMCInboundEnvelope] = asyncio.Queue(
            maxsize=settings.concurrency.queue_depth_max
        )
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

        # Task #52 / §5.10 ``/override``: per-thread "next turn bypasses
        # the hard cost cap" sentinel. The set is keyed by ``thread_id``
        # (the §5.7 ``amc:<channel_id>`` form) so a worker dedicated to
        # ``channel_id=X`` only ever has at most one entry. Stored as a
        # set so a hypothetical future cross-thread worker can reuse the
        # same code. Task #49 reads this via :meth:`consume_cost_override`
        # before the cost cap check on the next agent turn — once
        # consumed it auto-clears (single-turn override per §5.10).
        self._cost_overrides: set[str] = set()

    # ---- lifecycle --------------------------------------------------------

    def start(self) -> None:
        """Spawn the worker task. Idempotent."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._run(), name=f"capo.dispatcher.worker[{self.channel_id}]"
        )

    async def stop(self) -> None:
        """Cancel the worker task and wait for it to unwind.

        Drops any envelopes still in the queue — Phase 1 invariant. AMC will
        redeliver unacked messages on next boot via the boot-time unread
        sweep (§5.2), so unprocessed envelopes are not lost.
        """
        self._stopping = True
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    # ---- enqueue ----------------------------------------------------------

    def queue_depth(self) -> int:
        """Current number of envelopes pending in this worker's queue."""
        return self._queue.qsize()

    def put_nowait(self, envelope: AMCInboundEnvelope) -> bool:
        """Try to enqueue ``envelope`` without awaiting.

        Returns ``True`` on success, ``False`` when the queue is at capacity
        (the envelope is dropped and a structured warning is logged — see
        module docstring for the documented policy).
        """
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            logger.warning(
                "dispatcher dropped envelope: queue full for channel_id=%s "
                "(depth=%d, cap=%d, message_id=%s)",
                self.channel_id,
                self._queue.qsize(),
                self._queue.maxsize,
                envelope.id,
                extra={
                    "event": "capo.dispatcher.envelope.dropped.queue_full",
                    "channel_id": self.channel_id,
                    "queue_depth": self._queue.qsize(),
                    "queue_cap": self._queue.maxsize,
                    "message_id": envelope.id,
                },
            )
            return False
        return True

    # ---- worker loop ------------------------------------------------------

    async def _run(self) -> None:
        """Main per-channel processing loop. Never exits except via cancel."""
        while True:
            envelope = await self._queue.get()
            try:
                await self._handle_envelope(envelope)
            except asyncio.CancelledError:
                # Re-raise so the task actually unwinds on stop().
                raise
            except BaseException:
                # Final defensive net — _handle_envelope already swallows
                # turn-level errors, but if something inside it raises (e.g.
                # a logging bug), we still must NOT let the task die.
                logger.exception(
                    "dispatcher worker recovered from unexpected error "
                    "in channel_id=%s (message_id=%s)",
                    self.channel_id,
                    envelope.id,
                )
            finally:
                self._queue.task_done()

    async def _handle_envelope(self, envelope: AMCInboundEnvelope) -> None:
        """Run one full turn for ``envelope``. Never re-raises.

        Implements the §5.2 worker pipeline plus the §5.11 user-resolution
        gate. All exceptions are caught and logged — a single bad turn must
        never crash the dispatcher.
        """
        # ---- 1. Resolve sender -> user_id (§5.11) -----------------------
        user_id = resolve_user_id(envelope.sender_id, self._settings)
        if user_id is None:
            record = format_unknown_sender_log_record(envelope.sender_id)
            logger.warning(
                "dispatcher rejected envelope: unknown sender (channel_id=%s, "
                "message_id=%s)",
                self.channel_id,
                envelope.id,
                extra={
                    **record,
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                },
            )
            # Defense in depth: still mark_read so AMC stops redelivering.
            await self._safe_mark_read(envelope.id)
            return

        thread_id = thread_id_for_amc(envelope.channel_id)
        reply_text: str | None = None
        send_failed_terminally = False

        # ---- 1.5 Pre-agent slash-command interception (§5.8 / §5.10) ----
        # Slash commands are pre-agent control messages: zero-token
        # shortcuts that consume no LLM time. The dispatcher parses the
        # text via :func:`parse_slash_command`; on a match we route to
        # the right handler and reply via AMC. The agent NEVER sees a
        # recognized slash command.
        #
        # Recognized verbs split into two groups:
        #
        # * Approval shortcuts (``/approve``, ``/deny``) — Task #40 owns
        #   the state machine: pending-row lookup, DBOS.send_async,
        #   workflow_id race, idempotent re-acks.
        # * Session-control (``/new``, ``/status``, ``/clear``,
        #   ``/kill <id>``, ``/override``) — Task #52 owns these.
        #
        # Unknown slash verbs (e.g. ``/foo``) return ``None`` from the
        # parser; we additionally intercept the leading-slash case here
        # so the user gets a friendly "Unknown command: /foo" reply
        # instead of the agent attempting to answer it.
        parsed: SlashCommand | None = parse_slash_command(envelope.text)
        if parsed is not None:
            slash_reply = await self._dispatch_slash_command(
                parsed,
                envelope=envelope,
                user_id=user_id,
                parent_thread_id=thread_id,
            )
            if slash_reply is not None:
                await self._send_reply_and_mark_read(envelope, slash_reply)
                return
        else:
            # Unknown slash verb — friendly error. We only intercept here
            # when the text DEFINITELY looks like a slash command but
            # the parser refused it (unrecognized verb). Natural-language
            # input that starts with text (no leading slash) flows
            # through to the agent as normal.
            unknown_verb = _extract_unknown_slash_verb(envelope.text)
            if unknown_verb is not None:
                await self._send_reply_and_mark_read(
                    envelope, format_unknown_command_reply(unknown_verb)
                )
                return

        # ---- 1.6 Pre-agent budget hook (Task #49 / §5.9 / §6.5) ---------
        # Compares today's per-user USD total against ``settings.budget``
        # soft + hard caps BEFORE entering the agent loop. Three outcomes
        # matter to the dispatcher:
        #
        # * ``hard_block`` (no override armed) → friendly refusal reply,
        #   mark_read, return. The agent is NEVER invoked.
        # * ``overridden`` (hard cap crossed, but ``/override`` armed) →
        #   consume the override sentinel, prepend the "override consumed"
        #   note to the agent reply, proceed.
        # * ``soft_warn`` → prepend the soft-cap heads-up to the agent
        #   reply, proceed normally.
        # * ``ok`` → no-op, proceed normally.
        #
        # The check is fail-open inside :func:`capo.budget.check_budget`
        # (errors return ``ok``) — a cost-accountant hiccup must not
        # wedge the user-facing turn.
        override_armed = self.is_cost_override_armed(thread_id)
        budget_result: BudgetCheckResult = await check_budget(
            user_id=user_id,
            parent_thread_id=thread_id,
            db_path=self._settings.paths.db_path,
            settings=self._settings,
            override_armed=override_armed,
        )

        if budget_result.status == "hard_block":
            # Refuse the turn. No agent invocation, no override consumed.
            logger.warning(
                "dispatcher hard cap reached: channel_id=%s message_id=%s "
                "user_id=%s daily_usd=%s hard_cap_usd=%s",
                self.channel_id,
                envelope.id,
                user_id,
                budget_result.daily_usd,
                budget_result.hard_cap_usd,
                extra={
                    "event": "capo.dispatcher.budget.hard_block",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "user_id": user_id,
                    "daily_usd": str(budget_result.daily_usd),
                    "hard_cap_usd": str(budget_result.hard_cap_usd),
                },
            )
            await self._send_reply_and_mark_read(
                envelope, budget_result.message or ""
            )
            return

        budget_prefix: str | None = None
        if budget_result.status == "overridden":
            # Atomically consume the per-thread override sentinel. The
            # next turn will re-enforce the cap. We consume BEFORE the
            # agent.run path so that even if the agent itself errors
            # mid-turn the override is still spent — matches §5.10
            # "next agent turn" wording (one-shot).
            self.consume_cost_override(thread_id)
            budget_prefix = budget_result.message
            logger.info(
                "dispatcher cost cap overridden: channel_id=%s message_id=%s "
                "user_id=%s daily_usd=%s hard_cap_usd=%s",
                self.channel_id,
                envelope.id,
                user_id,
                budget_result.daily_usd,
                budget_result.hard_cap_usd,
                extra={
                    "event": "capo.dispatcher.budget.overridden",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "user_id": user_id,
                    "daily_usd": str(budget_result.daily_usd),
                    "hard_cap_usd": str(budget_result.hard_cap_usd),
                },
            )
        elif budget_result.status == "soft_warn":
            budget_prefix = budget_result.message
            logger.info(
                "dispatcher soft cap crossed: channel_id=%s message_id=%s "
                "user_id=%s daily_usd=%s soft_cap_usd=%s",
                self.channel_id,
                envelope.id,
                user_id,
                budget_result.daily_usd,
                budget_result.soft_cap_usd,
                extra={
                    "event": "capo.dispatcher.budget.soft_warn",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "user_id": user_id,
                    "daily_usd": str(budget_result.daily_usd),
                    "soft_cap_usd": str(budget_result.soft_cap_usd),
                },
            )

        # ---- 2. Agent turn (history, run, persist) ----------------------
        #
        # All DB work for a single turn happens inside one ``asyncio.to_thread``
        # block per phase so the sqlite3 connection is used by exactly one
        # thread at a time. Connections are opened and closed within the
        # same thread to keep the sqlite3 thread invariant simple even under
        # cancellation. ``agent.run`` itself runs on the asyncio main thread
        # between the two DB phases — the connection has been closed by
        # then.
        try:
            def _prepare() -> tuple[str, list[Any]]:
                conn = self._conn_factory()
                try:
                    sid = get_or_create_active_session(conn, user_id, thread_id)
                    hist = load_history(conn, user_id, thread_id, sid)
                    return sid, hist
                finally:
                    conn.close()

            session_id, history = await asyncio.to_thread(_prepare)

            deps = self._deps_factory(user_id)
            # The default ``_default_deps_factory`` takes only ``user_id``
            # by signature, but Task #53 NL session tools require
            # ``deps.thread_id`` to be set so their SQL scopes correctly.
            # ``CapoDeps`` is a non-frozen dataclass; setting the field
            # post-construction is a harmless, no-op for tools that
            # ignore it. Tests that supply a custom ``deps_factory``
            # populating thread_id themselves will be overwritten here
            # with the canonical envelope-derived value — that's the
            # correct behaviour.
            # ``CapoDeps`` is a non-frozen dataclass — ``thread_id`` is
            # always settable. ``contextlib.suppress(AttributeError)`` is
            # a defensive guard in case a future test supplies a frozen
            # variant.
            with contextlib.suppress(AttributeError):  # pragma: no cover - defensive
                deps.thread_id = thread_id
            # Spec §6.5: ``capo.agent.run`` — Pydantic AI agent run.
            # ``model`` is read from ``self._agent.model`` when available;
            # token / cost attributes open at zero and are filled in by
            # the cost-accountant block below via the auto-instrument
            # pydantic_ai spans (Logfire merges them on trace). The Capo
            # span here exists for taxonomy alignment with §6.5 dashboards.
            _agent_model = (
                getattr(self._agent, "model_name", None)
                or getattr(self._agent, "model", None)
                or "unknown"
            )
            with agent_run_span(
                thread_id=thread_id,
                user_id=user_id,
                model=str(_agent_model),
                message_id=envelope.id,
            ):
                result = await self._agent.run(
                    envelope.text,
                    message_history=history,
                    deps=deps,
                )
            # Persist the new messages BEFORE attempting the outbound
            # send so the conversation memory survives even if AMC is
            # unreachable. (The user re-asking later will see the
            # previous turn in history.)
            new_messages = list(result.new_messages())
            if new_messages:
                def _persist(msgs: list[Any]) -> None:
                    conn = self._conn_factory()
                    try:
                        append_messages(
                            conn, user_id, thread_id, session_id, msgs
                        )
                    finally:
                        conn.close()

                await asyncio.to_thread(_persist, new_messages)

                # ---- Cost accountant (Task #48 / §5.9 / §6.5) ---------
                # Tally LLM API costs from each ModelResponse in the turn
                # and persist to ``costs``. Failures are logged + swallowed
                # inside ``record_run_usage`` — the dispatcher MUST NOT
                # crash on a malformed usage event or a SQLite hiccup.
                try:
                    await record_run_usage(
                        self._settings.paths.db_path,
                        new_messages,
                        user_id=user_id,
                        parent_thread_id=thread_id,
                    )
                except Exception as cost_exc:  # noqa: BLE001 - defensive
                    logger.warning(
                        "dispatcher cost accountant failed for "
                        "channel_id=%s message_id=%s user_id=%s: %r",
                        self.channel_id,
                        envelope.id,
                        user_id,
                        cost_exc,
                        extra={
                            "event": "capo.dispatcher.costs.failed",
                            "channel_id": self.channel_id,
                            "message_id": envelope.id,
                            "user_id": user_id,
                            "error": repr(cost_exc),
                        },
                    )

                # ---- Lazy compaction (Task #54 / §5.7 / §6.5) ----------
                # After every agent turn we check whether the per-thread
                # history has grown past either configured threshold and,
                # if so, replace the oldest messages with a single summary.
                # Fully fail-open: any exception is swallowed inside
                # ``_maybe_compact`` and never crashes the turn — a failed
                # compaction is operationally fine, the user's reply still
                # goes out below.
                await self._maybe_compact(
                    user_id=user_id,
                    thread_id=thread_id,
                    session_id=session_id,
                    envelope_id=envelope.id,
                )
            reply_text = str(result.output)
        except ApprovalRequired as exc:
            # A Phase-2 tool refused to auto-run and asked for user
            # approval (spec §5.8). Phase 4 will route this through a
            # real approval prompt; Phase 2 just surfaces a stub reply
            # so the user knows *why* the agent stopped.
            logger.info(
                "dispatcher tool requires approval: channel_id=%s "
                "message_id=%s user_id=%s command=%r reason=%r",
                self.channel_id,
                envelope.id,
                user_id,
                exc.command,
                exc.reason,
                extra={
                    "event": "capo.dispatcher.tool.approval_required",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "user_id": user_id,
                    "approval_command": exc.command,
                    "approval_reason": exc.reason,
                },
            )
            reply_text = format_approval_required_reply(exc)
        except Exception as exc:
            # Agent / memory failure. Log full traceback and fall through to
            # the generic error reply path.
            logger.error(
                "dispatcher turn failed for channel_id=%s message_id=%s "
                "user_id=%s: %s\n%s",
                self.channel_id,
                envelope.id,
                user_id,
                exc,
                traceback.format_exc(),
                extra={
                    "event": "capo.dispatcher.turn.failed",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "user_id": user_id,
                    "error": repr(exc),
                },
            )
            reply_text = INTERNAL_ERROR_REPLY

        # ---- 2.5 Apply budget prefix (soft_warn / overridden) -----------
        # Prepend the one-line budget heads-up (if any) to whatever
        # ``reply_text`` carries — agent output, ApprovalRequired stub,
        # or the generic internal-error string. The prefix is added
        # AFTER the agent-turn block so a soft-warn user STILL sees an
        # error reply when the agent itself crashes; the dispatcher's
        # job is to ensure the user always sees the warning whenever
        # the turn was actually attempted.
        if budget_prefix is not None and reply_text is not None:
            reply_text = f"{budget_prefix}\n\n{reply_text}"

        # ---- 3. amc.send (with §5.2 / §7.5 error treatment) -------------
        if reply_text is not None:
            try:
                await self._amc.send(
                    envelope.channel_id,
                    reply_text,
                    reply_to_message_id=envelope.id,
                )
            except PlatformAuth as exc:
                # §5.2: log alert, do NOT retry. Attempt a fallback reply
                # via AMC; if AMC itself is unreachable (the auth failure
                # already proves it might be) we just give up gracefully —
                # the dispatcher MUST stay alive.
                logger.error(
                    "dispatcher AMC PLATFORM_AUTH for channel_id=%s "
                    "message_id=%s: %s",
                    self.channel_id,
                    envelope.id,
                    exc.message,
                    extra={
                        "event": "capo.dispatcher.amc.platform_auth",
                        "channel_id": self.channel_id,
                        "message_id": envelope.id,
                        "amc_code": exc.code,
                    },
                )
                send_failed_terminally = True
                await self._try_fallback_reply(
                    envelope, PLATFORM_AUTH_REPLY
                )
            except ChannelNotFound as exc:
                # §5.2: log, do NOT retry, do NOT fallback (channel gone).
                logger.warning(
                    "dispatcher AMC CHANNEL_NOT_FOUND for channel_id=%s "
                    "message_id=%s: %s",
                    self.channel_id,
                    envelope.id,
                    exc.message,
                    extra={
                        "event": "capo.dispatcher.amc.channel_not_found",
                        "channel_id": self.channel_id,
                        "message_id": envelope.id,
                        "amc_code": exc.code,
                    },
                )
                send_failed_terminally = True
            except AMCError as exc:
                # All other AMC errors (RATE_LIMITED is already retried
                # inside AMCClient; if it surfaces here it means the
                # 30s deadline was busted — log and move on).
                logger.error(
                    "dispatcher AMC send failed for channel_id=%s "
                    "message_id=%s code=%s: %s",
                    self.channel_id,
                    envelope.id,
                    exc.code,
                    exc.message,
                    extra={
                        "event": "capo.dispatcher.amc.send_failed",
                        "channel_id": self.channel_id,
                        "message_id": envelope.id,
                        "amc_code": exc.code,
                    },
                )
                send_failed_terminally = True
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "dispatcher unexpected error during amc.send for "
                    "channel_id=%s message_id=%s: %s",
                    self.channel_id,
                    envelope.id,
                    exc,
                )
                send_failed_terminally = True

        # ---- 4. amc.mark_read (always attempted; idempotent) ------------
        # Even on PLATFORM_AUTH / CHANNEL_NOT_FOUND we still try mark_read.
        # AMC's contract is that mark_read is idempotent on message_id, so
        # double-marks are safe. If mark_read itself fails we log + move on;
        # the boot-time unread sweep (§5.2) will re-deliver on next start.
        await self._safe_mark_read(envelope.id)
        # ``send_failed_terminally`` is intentionally unused after this point
        # — it's tracked so future Task-12 / Task-30 hooks can decorate the
        # turn span. Phase 1 doesn't need it past the log line.
        _ = send_failed_terminally

    # ---- Task #40: approval inbound routing -----------------------------

    async def _try_handle_approval_command(
        self,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        parent_thread_id: str,
    ) -> str | None:
        """Intercept ``/approve``/``/deny`` and route to the DBOS workflow.

        Returns the AMC reply text on a successfully-handled approval reply
        (the caller sends it + mark_reads + returns). Returns ``None`` when
        the message is NOT an approval slash command — the caller then
        proceeds to the regular agent turn.

        Behaviour table:

        ``/approve <id>`` / ``/deny <id>`` with known pending row →
            ``DBOS.send_async`` decision + ``approval recorded: <id>``.
        ``/approve`` (no id) → look up most-recent pending in this thread.
            If none → ``"No pending approval ..."``.
        Known id but already resolved → ``"That approval is already
            resolved."`` (idempotent: repeated ``/approve <id>`` does NOT
            re-send a decision).
        Unknown id → ``"No approval with that id was found."``.
        Row exists but ``workflow_id IS NULL`` (race: row inserted, workflow
            id not yet persisted) → wait briefly + retry once. If still
            ``NULL`` → ``"Approval is still being created, try again ..."``.

        Note on V1 cross-user authz: any sender in the same
        ``parent_thread_id`` may resolve a pending approval — this is
        documented in §5.8 and explicit in this implementation. The
        ``requester_user_id`` is NOT enforced against ``user_id``.
        """
        parsed: SlashCommand | None = parse_slash_command(envelope.text)
        if parsed is None:
            return None
        verb = parsed.verb
        approval_id = parsed.approval_id
        reason = parsed.reason

        # Structured log so operators can grep these events.
        logger.info(
            "dispatcher approval command intercepted: channel_id=%s "
            "message_id=%s user_id=%s verb=%s approval_id=%r",
            self.channel_id,
            envelope.id,
            user_id,
            verb,
            approval_id,
            extra={
                "event": "capo.dispatcher.approval.intercepted",
                "channel_id": self.channel_id,
                "message_id": envelope.id,
                "user_id": user_id,
                "approval_verb": verb,
                "approval_id": approval_id,
            },
        )

        def _lookup(approval_id_arg: str | None) -> dict[str, Any] | None:
            conn = self._conn_factory()
            try:
                return _select_approval_row(
                    conn,
                    approval_id=approval_id_arg,
                    parent_thread_id=parent_thread_id,
                )
            finally:
                conn.close()

        row = await asyncio.to_thread(_lookup, approval_id)

        # ---- No row found ------------------------------------------------
        if row is None:
            if approval_id is None:
                return APPROVAL_REPLY_NO_PENDING
            return APPROVAL_REPLY_UNKNOWN_ID

        resolved_approval_id = str(row["approval_id"])
        status = str(row.get("status") or "")
        workflow_id = row.get("workflow_id")

        # ---- Already terminal -> idempotent ack ("already resolved") -----
        # Repeated ``/approve <id>`` for an id whose row is already terminal
        # MUST NOT re-fire ``DBOS.send`` (it'd pollute the dbos.db
        # notifications table with a now-useless message). Reply with the
        # canonical "already resolved" string.
        if status and status != "pending":
            return APPROVAL_REPLY_ALREADY_RESOLVED

        # ---- workflow_id NULL race: brief wait + single retry -----------
        # The §5.8 INSERT step is wrapped in @idempotent_step; the
        # workflow_id column is populated in the same INSERT, but the
        # @DBOS.workflow() decorator captures ``DBOS.workflow_id`` only
        # after the body starts. There is a small window between the row
        # being externally visible and ``workflow_id`` being persisted.
        # Wait briefly + re-read once before failing.
        if not isinstance(workflow_id, str) or not workflow_id:
            await asyncio.sleep(0.5)
            row = await asyncio.to_thread(_lookup, resolved_approval_id)
            if row is None:
                return APPROVAL_REPLY_UNKNOWN_ID
            status = str(row.get("status") or "")
            if status and status != "pending":
                return APPROVAL_REPLY_ALREADY_RESOLVED
            workflow_id = row.get("workflow_id")
            if not isinstance(workflow_id, str) or not workflow_id:
                return APPROVAL_REPLY_STILL_CREATING

        # ---- Send the decision via DBOS ---------------------------------
        payload: dict[str, Any] = {
            "status": verb,
            "resolved_by": user_id,
            "reason": reason,
        }
        try:
            await _send_approval_decision(workflow_id, payload)
        except Exception as exc:  # noqa: BLE001 - intentional broad catch
            # If the workflow is not found in DBOS (already completed /
            # orphaned), surface the friendly "already resolved" reply
            # rather than the internal error reply. We can't reliably
            # discriminate the DBOS exception type without coupling to
            # dbos internals, so we re-check the row status as a tiebreaker
            # — if it flipped to terminal between our SELECT and the send,
            # treat as "already resolved" (idempotent). Otherwise log +
            # generic error.
            recheck = await asyncio.to_thread(_lookup, resolved_approval_id)
            if (
                recheck is not None
                and str(recheck.get("status") or "") not in ("", "pending")
            ):
                return APPROVAL_REPLY_ALREADY_RESOLVED
            logger.error(
                "dispatcher DBOS.send_async failed for approval_id=%s "
                "(channel_id=%s message_id=%s): %r",
                resolved_approval_id,
                self.channel_id,
                envelope.id,
                exc,
                extra={
                    "event": "capo.dispatcher.approval.send_failed",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "approval_id": resolved_approval_id,
                },
            )
            # Treat workflow-not-found / lookup failures as "already
            # resolved" so the user sees a friendly reply rather than the
            # generic internal-error string.
            return APPROVAL_REPLY_ALREADY_RESOLVED

        return format_approval_recorded_reply(resolved_approval_id)

    # ---- Task #52: session-control slash command dispatch ---------------

    async def _dispatch_slash_command(
        self,
        parsed: SlashCommand,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        parent_thread_id: str,
    ) -> str | None:
        """Route a parsed :class:`SlashCommand` to the right handler.

        Returns the AMC reply text for the caller to send + mark_read.
        Approval shortcuts (``/approve`` / ``/deny``) delegate to the
        Task #40 handler. Session-control verbs (``/new``, ``/status``,
        ``/clear``, ``/kill``, ``/override``) are handled inline below.

        Returns ``None`` only as a fall-through guard for "verb in
        :data:`SlashVerb` Literal but no dispatch handler" — should be
        unreachable, but staying defensive keeps the failure mode
        "natural-language pass-through to the agent" rather than crash.
        """
        verb = parsed.verb

        logger.info(
            "dispatcher slash command intercepted: channel_id=%s "
            "message_id=%s user_id=%s verb=%s",
            self.channel_id,
            envelope.id,
            user_id,
            verb,
            extra={
                "event": "capo.dispatcher.slash.intercepted",
                "channel_id": self.channel_id,
                "message_id": envelope.id,
                "user_id": user_id,
                "slash_verb": verb,
            },
        )

        if verb in ("approve", "deny"):
            # Delegate to the Task #40 handler. The legacy entry point
            # re-parses internally; we accept the small redundancy in
            # exchange for keeping the well-tested handler untouched.
            return await self._try_handle_approval_command(
                envelope=envelope,
                user_id=user_id,
                parent_thread_id=parent_thread_id,
            )

        if verb == "new":
            return await self._handle_new_command(
                envelope=envelope,
                user_id=user_id,
                thread_id=parent_thread_id,
            )
        if verb == "status":
            return await self._handle_status_command(
                envelope=envelope,
                user_id=user_id,
                thread_id=parent_thread_id,
            )
        if verb == "clear":
            return await self._handle_clear_command(
                envelope=envelope,
                user_id=user_id,
                thread_id=parent_thread_id,
            )
        if verb == "kill":
            return await self._handle_kill_command(
                parsed,
                envelope=envelope,
                user_id=user_id,
                thread_id=parent_thread_id,
            )
        if verb == "override":
            return await self._handle_override_command(
                envelope=envelope,
                user_id=user_id,
                thread_id=parent_thread_id,
            )

        # Defensive: unreachable for any verb in ``SlashVerb``.
        return None

    async def _handle_new_command(
        self,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        thread_id: str,
    ) -> str:
        """``/new``: end the current session so a fresh one opens on next turn.

        Conversation history on disk is preserved per §5.10 ("older
        history retained on disk") — we only flip the active session
        row's ``ended_at`` column. The next agent turn will call
        :func:`get_or_create_active_session` and observe no active row,
        which creates a fresh ``session_id``.
        """
        def _do() -> None:
            conn = self._conn_factory()
            try:
                _end_active_session(conn, user_id, thread_id)
            finally:
                conn.close()

        await asyncio.to_thread(_do)
        return SESSION_REPLY_NEW

    async def _handle_status_command(
        self,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        thread_id: str,
    ) -> str:
        """``/status``: emit a thread/session summary reply.

        Reads:

        * Active running delegations count from ``delegations`` for
          ``user_id``.
        * Conversation message count from ``conversation_history`` for
          the active session (``None`` if no active session).
        * Today's UTC USD cost via :func:`_read_daily_cost_usd` (Task
          #48 wraps it; falls back to ``None`` if unavailable).
        * Current orchestrator model from ``settings.models.default``.
        """
        def _read() -> tuple[int, int | None]:
            conn = self._conn_factory()
            try:
                active = _select_active_delegation_count(conn, user_id)
                sid = _select_active_session_id(conn, user_id, thread_id)
                if sid is None:
                    msg_count: int | None = None
                else:
                    msg_count = _select_conversation_count(
                        conn, user_id, thread_id, sid
                    )
                return active, msg_count
            finally:
                conn.close()

        active_delegations, conv_count = await asyncio.to_thread(_read)
        daily_cost = await _read_daily_cost_usd(self._settings, user_id)

        return format_status_reply(
            model=self._settings.models.default,
            active_delegations=active_delegations,
            daily_cost_usd=daily_cost,
            conversation_message_count=conv_count,
        )

    async def _handle_clear_command(
        self,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        thread_id: str,
    ) -> str:
        """``/clear``: delete per-thread conversation messages + end session.

        Per §5.10: same as ``/new`` but also clears the message rows
        for the thread. Returns the canonical reply
        :data:`SESSION_REPLY_CLEARED` when rows existed,
        :data:`SESSION_REPLY_CLEARED_EMPTY` when there were no messages
        to delete (friendly distinction; either way the thread starts
        empty after this).
        """
        def _do() -> int:
            conn = self._conn_factory()
            try:
                return _delete_thread_history(conn, user_id, thread_id)
            finally:
                conn.close()

        deleted = await asyncio.to_thread(_do)
        if deleted > 0:
            return SESSION_REPLY_CLEARED
        return SESSION_REPLY_CLEARED_EMPTY

    async def _handle_kill_command(
        self,
        parsed: SlashCommand,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        thread_id: str,
    ) -> str:
        """``/kill <delegation_id>``: route to :func:`kill_delegation`.

        Edge cases per task spec:

        * No id → ``SESSION_REPLY_KILL_MISSING_ID``.
        * Mis-shaped (non-32-hex) id → ``SESSION_REPLY_KILL_UNKNOWN``.
        * Unknown id (DB miss) → ``SESSION_REPLY_KILL_UNKNOWN``.
        * Kill route raises :class:`ApprovalRejected` (non-owner, denied) →
          surface the friendly approval-rejected description.
        * Successful kill → :func:`format_kill_reply`.
        """
        delegation_id = parsed.delegation_id
        if delegation_id is None:
            return SESSION_REPLY_KILL_MISSING_ID
        if not DELEGATION_ID_RE.match(delegation_id):
            return SESSION_REPLY_KILL_UNKNOWN

        # Lazy import: keeps Phase-1 callers off the delegations module.
        from pydantic_ai import RunContext
        from pydantic_ai.models.test import TestModel
        from pydantic_ai.usage import RunUsage

        from capo.tools.delegations import (
            DelegationNotFound,
            kill_delegation,
        )

        deps = self._deps_factory(user_id)
        # The dispatcher tool path constructs a minimal RunContext —
        # ``kill_delegation`` only reads ``ctx.deps`` (no model/usage
        # introspection). The ``TestModel`` + ``RunUsage`` placeholders
        # satisfy Pydantic AI's required fields without spinning a real
        # LLM client. Mirrors the agent-tool invocation contract.
        ctx: RunContext[CapoDeps] = RunContext(
            deps=deps,
            model=TestModel(),
            usage=RunUsage(),
        )
        try:
            result = await kill_delegation(
                ctx,
                delegation_id,
                reason="user requested via /kill",
            )
        except DelegationNotFound:
            return SESSION_REPLY_KILL_UNKNOWN
        except ApprovalRequired as exc:
            # Non-owner kill and approval infra unavailable. Surface
            # the friendly approval-required reply rather than crashing.
            return format_approval_required_reply(exc)
        except Exception as exc:  # noqa: BLE001 - last-line defence
            # ``ApprovalRejected`` (non-owner denied/expired) lives in
            # ``capo.tools.basic``; we don't want a hard import here for
            # backward compat. Treat any other exception as a friendly
            # surfaced error rather than letting the dispatcher's
            # internal-error path catch it (which would have implied
            # "Capo will retry" — wrong for a user-initiated kill).
            logger.warning(
                "dispatcher /kill failed for delegation_id=%s: %r",
                delegation_id,
                exc,
                extra={
                    "event": "capo.dispatcher.kill.failed",
                    "delegation_id": delegation_id,
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                },
            )
            return f"kill_delegation({delegation_id}) refused: {exc}"

        if not isinstance(result, dict):
            return f"kill_delegation({delegation_id}) → {result!r}"
        return format_kill_reply(
            delegation_id=delegation_id, result=result
        )

    async def _handle_override_command(
        self,
        *,
        envelope: AMCInboundEnvelope,
        user_id: str,
        thread_id: str,
    ) -> str:
        """``/override``: arm the per-thread "bypass cost cap" sentinel.

        Sets a flag in :attr:`_cost_overrides`. Task #49's hard cost cap
        check consults :meth:`consume_cost_override` BEFORE rejecting a
        turn; consumption auto-clears the sentinel so each ``/override``
        applies to exactly one subsequent agent turn (per §5.10
        "temporarily bypasses ... for the next agent turn").
        """
        self._cost_overrides.add(thread_id)
        logger.info(
            "dispatcher /override armed for thread_id=%s user_id=%s",
            thread_id,
            user_id,
            extra={
                "event": "capo.dispatcher.override.armed",
                "thread_id": thread_id,
                "user_id": user_id,
                "channel_id": self.channel_id,
                "message_id": envelope.id,
            },
        )
        return SESSION_REPLY_OVERRIDE_ARMED

    # ---- Task #52 public API: cost override sentinel --------------------

    def is_cost_override_armed(self, thread_id: str) -> bool:
        """Return ``True`` if a ``/override`` is pending for ``thread_id``."""
        return thread_id in self._cost_overrides

    def consume_cost_override(self, thread_id: str) -> bool:
        """Atomic test-and-clear of the override sentinel.

        Task #49's cost-cap pre-check calls this once per agent turn:
        on ``True``, the cap check is skipped for this turn AND the
        sentinel is cleared (so the next turn enforces the cap again).
        """
        if thread_id in self._cost_overrides:
            self._cost_overrides.discard(thread_id)
            return True
        return False

    async def _send_reply_and_mark_read(
        self, envelope: AMCInboundEnvelope, reply_text: str
    ) -> None:
        """Send a single reply + mark_read for a pre-agent control path.

        Mirrors the §5.2 / §7.5 error treatment applied to agent-turn
        replies: ``PLATFORM_AUTH`` / ``CHANNEL_NOT_FOUND`` / other AMC
        errors are caught and logged. ``mark_read`` is always attempted.
        """
        try:
            await self._amc.send(
                envelope.channel_id,
                reply_text,
                reply_to_message_id=envelope.id,
            )
        except PlatformAuth as exc:
            logger.error(
                "dispatcher AMC PLATFORM_AUTH on approval ack for "
                "channel_id=%s message_id=%s: %s",
                self.channel_id,
                envelope.id,
                exc.message,
                extra={
                    "event": "capo.dispatcher.approval.amc.platform_auth",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "amc_code": exc.code,
                },
            )
            await self._try_fallback_reply(envelope, PLATFORM_AUTH_REPLY)
        except ChannelNotFound as exc:
            logger.warning(
                "dispatcher AMC CHANNEL_NOT_FOUND on approval ack for "
                "channel_id=%s message_id=%s: %s",
                self.channel_id,
                envelope.id,
                exc.message,
                extra={
                    "event": "capo.dispatcher.approval.amc.channel_not_found",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "amc_code": exc.code,
                },
            )
        except AMCError as exc:
            logger.error(
                "dispatcher AMC send failed on approval ack for "
                "channel_id=%s message_id=%s code=%s: %s",
                self.channel_id,
                envelope.id,
                exc.code,
                exc.message,
                extra={
                    "event": "capo.dispatcher.approval.amc.send_failed",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                    "amc_code": exc.code,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "dispatcher unexpected error on approval ack for "
                "channel_id=%s message_id=%s: %s",
                self.channel_id,
                envelope.id,
                exc,
            )
        await self._safe_mark_read(envelope.id)

    async def _try_fallback_reply(
        self, envelope: AMCInboundEnvelope, text: str
    ) -> None:
        """Best-effort fallback reply send. Swallows any error."""
        try:
            await self._amc.send(
                envelope.channel_id, text, reply_to_message_id=envelope.id
            )
        except Exception as exc:  # noqa: BLE001 - intentional swallow
            logger.warning(
                "dispatcher fallback reply send failed for channel_id=%s "
                "message_id=%s: %r",
                self.channel_id,
                envelope.id,
                exc,
                extra={
                    "event": "capo.dispatcher.amc.fallback_failed",
                    "channel_id": self.channel_id,
                    "message_id": envelope.id,
                },
            )

    async def _safe_mark_read(self, message_id: str) -> None:
        """Idempotent mark_read; logs and continues on any failure."""
        try:
            await self._amc.mark_read([message_id])
        except Exception as exc:  # noqa: BLE001 - intentional swallow
            logger.warning(
                "dispatcher mark_read failed for channel_id=%s message_id=%s: %r",
                self.channel_id,
                message_id,
                exc,
                extra={
                    "event": "capo.dispatcher.amc.mark_read_failed",
                    "channel_id": self.channel_id,
                    "message_id": message_id,
                },
            )

    async def _maybe_compact(
        self,
        *,
        user_id: str,
        thread_id: str,
        session_id: str,
        envelope_id: str,
    ) -> None:
        """Conditionally compact ``(user_id, thread_id, session_id)``.

        Triggered at the tail of every successful agent turn (after the
        new ``ModelMessage`` rows have been persisted). Idempotent and
        fail-open per Task #54 / §5.7:

        - If ``settings.compaction.enabled`` is ``False`` → no-op.
        - If no ``compaction_summarizer`` was injected → no-op (the
          dispatcher is wired without one in unit tests; production
          injection lives in :class:`Dispatcher`).
        - If the history is below threshold → no-op.
        - If the summarizer raises → log + no-op (history untouched).

        The check itself is cheap (a single ``COUNT(*)`` via the
        existing connection-factory pattern + ``should_compact``), and
        the LLM call only runs when the threshold is actually exceeded.
        """
        if not self._settings.compaction.enabled:
            return
        if self._compaction_summarizer is None:
            return
        try:
            # Load the current history snapshot to feed ``should_compact``.
            # We re-use the same connection-factory pattern as the rest of
            # the dispatcher so SQLite stays single-threaded.
            def _load() -> list[Any]:
                conn = self._conn_factory()
                try:
                    return load_history(conn, user_id, thread_id, session_id)
                finally:
                    conn.close()

            history = await asyncio.to_thread(_load)
            if not should_compact(history, self._settings):
                return

            result = await compact_thread_async(
                thread_id=thread_id,
                user_id=user_id,
                session_id=session_id,
                db_path=self._settings.paths.db_path,
                summarizer=self._compaction_summarizer,
                settings=self._settings,
            )
            if result.compacted:
                logger.info(
                    "dispatcher compacted thread channel_id=%s message_id=%s "
                    "user_id=%s messages_summarized=%d tokens_before=%d "
                    "tokens_after=%d",
                    self.channel_id,
                    envelope_id,
                    user_id,
                    result.messages_summarized,
                    result.tokens_before,
                    result.tokens_after,
                    extra={
                        "event": "capo.dispatcher.compaction.applied",
                        "channel_id": self.channel_id,
                        "message_id": envelope_id,
                        "user_id": user_id,
                        "messages_summarized": result.messages_summarized,
                        "tokens_before": result.tokens_before,
                        "tokens_after": result.tokens_after,
                    },
                )
        except Exception as exc:  # noqa: BLE001 - fail-open
            logger.warning(
                "dispatcher compaction failed channel_id=%s message_id=%s "
                "user_id=%s: %r",
                self.channel_id,
                envelope_id,
                user_id,
                exc,
                extra={
                    "event": "capo.dispatcher.compaction.failed",
                    "channel_id": self.channel_id,
                    "message_id": envelope_id,
                    "user_id": user_id,
                    "error": repr(exc),
                },
            )


class Dispatcher:
    """The per-process per-channel envelope dispatcher (spec §5.2).

    The webhook handler (Task #10) and the boot-time unread sweep (Task #18)
    both call :meth:`enqueue` to hand a verified, deduped envelope to the
    dispatcher. The dispatcher owns:

    * The ``channel_id -> ChannelWorker`` map.
    * Lazy worker creation on first envelope per channel.
    * The :meth:`start` / :meth:`stop` lifecycle hooks called from
      :mod:`capo.main`.

    Parameters
    ----------
    settings:
        Validated :class:`capo.config.Settings`. Read for
        ``concurrency.queue_depth_max`` and via :func:`resolve_user_id`.
    agent:
        Constructed Pydantic AI agent (see :func:`capo.agent.build_agent`).
        The dispatcher calls ``agent.run(text, message_history=..., deps=...)``.
    amc_client:
        Constructed :class:`capo.transport.amc_client.AMCClient`. The
        dispatcher calls ``send`` and ``mark_read`` on every turn.
    store_conn_factory:
        Zero-arg callable returning a fresh :class:`sqlite3.Connection` for
        a single turn. Connections are not shared across turns — the worker
        opens one per envelope and closes it in ``finally`` to keep the
        sqlite3 thread invariant simple. Production wires
        :func:`capo.memory.store.open_connection`.
    deps_factory:
        Optional callable ``(user_id) -> CapoDeps`` for constructing the
        per-turn agent deps. Defaults to a shim that builds a fresh
        :class:`CapoDeps` from ``settings`` + the shared
        :class:`httpx.AsyncClient` passed via ``http_client``. Tests pass
        a fake to avoid httpx entirely.
    http_client:
        Optional shared :class:`httpx.AsyncClient` used by the default
        :class:`CapoDeps` factory. Ignored when ``deps_factory`` is
        explicitly provided. The dispatcher does NOT own the client's
        lifecycle (Task #10 / :mod:`capo.main` does).
    """

    def __init__(
        self,
        settings: Settings,
        agent: Any,
        amc_client: AMCClient,
        store_conn_factory: StoreConnFactory,
        *,
        deps_factory: DepsFactory | None = None,
        http_client: httpx.AsyncClient | None = None,
        compaction_summarizer: SummarizerProtocol | None = None,
    ) -> None:
        self._settings = settings
        self._agent = agent
        self._amc = amc_client
        self._conn_factory = store_conn_factory
        self._workers: dict[str, ChannelWorker] = {}
        self._stopped = False
        self._lock = asyncio.Lock()
        # Task #54 (§5.7): optional cheap-model agent used by every
        # :class:`ChannelWorker` for hybrid compaction. ``None`` disables
        # compaction at the worker level. Production main wires this from
        # ``settings.models.router``; tests usually leave it unset.
        self._compaction_summarizer: SummarizerProtocol | None = (
            compaction_summarizer
        )

        if deps_factory is not None:
            self._deps_factory: DepsFactory = deps_factory
        else:
            if http_client is None:
                raise ValueError(
                    "Dispatcher requires either deps_factory or http_client"
                )
            shared_http = http_client

            def _default_deps_factory(user_id: str) -> CapoDeps:
                return CapoDeps.from_settings(
                    settings, shared_http, user_id=user_id
                )

            self._deps_factory = _default_deps_factory

    # ---- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """No-op in Phase 1; workers are spawned lazily on first envelope.

        Kept as an explicit lifecycle hook so :mod:`capo.main` can call
        ``await dispatcher.start()`` symmetrically with ``stop()``.
        """
        self._stopped = False

    async def stop(self) -> None:
        """Cancel every channel worker and drain in-flight tasks."""
        self._stopped = True
        # Snapshot under the lock so concurrent enqueue() can't observe a
        # half-cleared map.
        async with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        await asyncio.gather(
            *(w.stop() for w in workers), return_exceptions=True
        )

    # ---- enqueue ----------------------------------------------------------

    async def enqueue(self, envelope: AMCInboundEnvelope) -> bool:
        """Route ``envelope`` to its per-channel worker.

        Returns ``True`` when the envelope is accepted into the queue,
        ``False`` when the worker queue is full and the envelope was dropped
        with a logged warning (Phase 1 default policy; see module docstring).

        Idempotent w.r.t. worker creation: the first call for a new
        ``channel_id`` lazily creates and starts a worker; subsequent calls
        reuse it.
        """
        if self._stopped:
            logger.warning(
                "dispatcher refused envelope: dispatcher is stopped "
                "(channel_id=%s, message_id=%s)",
                envelope.channel_id,
                envelope.id,
            )
            return False

        worker = await self._get_or_create_worker(envelope.channel_id)
        return worker.put_nowait(envelope)

    async def _get_or_create_worker(self, channel_id: str) -> ChannelWorker:
        worker = self._workers.get(channel_id)
        if worker is not None:
            return worker
        async with self._lock:
            worker = self._workers.get(channel_id)
            if worker is None:
                worker = ChannelWorker(
                    channel_id,
                    settings=self._settings,
                    agent=self._agent,
                    amc_client=self._amc,
                    store_conn_factory=self._conn_factory,
                    deps_factory=self._deps_factory,
                    compaction_summarizer=self._compaction_summarizer,
                )
                worker.start()
                self._workers[channel_id] = worker
        return worker

    # ---- introspection (used by /healthz Task #34) ------------------------

    def worker_count(self) -> int:
        """Number of live :class:`ChannelWorker` instances."""
        return len(self._workers)

    def queue_depths(self) -> dict[str, int]:
        """Map of ``channel_id -> current queue depth``. Snapshot only."""
        return {cid: w.queue_depth() for cid, w in self._workers.items()}


__all__ = [
    "APPROVAL_REPLY_ALREADY_RESOLVED",
    "APPROVAL_REPLY_NO_PENDING",
    "APPROVAL_REPLY_STILL_CREATING",
    "APPROVAL_REPLY_UNKNOWN_ID",
    "CHANNEL_NOT_FOUND_REPLY",
    "ChannelWorker",
    "Dispatcher",
    "INTERNAL_ERROR_REPLY",
    "PLATFORM_AUTH_REPLY",
    "SESSION_REPLY_CLEARED",
    "SESSION_REPLY_CLEARED_EMPTY",
    "SESSION_REPLY_KILL_MISSING_ID",
    "SESSION_REPLY_KILL_UNKNOWN",
    "SESSION_REPLY_NEW",
    "SESSION_REPLY_OVERRIDE_ARMED",
    "SESSION_REPLY_UNKNOWN_VERB",
    "format_approval_recorded_reply",
    "format_approval_required_reply",
    "format_kill_reply",
    "format_status_reply",
    "format_unknown_command_reply",
]
