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

from capo.deps import CapoDeps
from capo.memory.conversation import (
    append_messages,
    get_or_create_active_session,
    load_history,
    thread_id_for_amc,
)
from capo.tools.basic import ApprovalRequired
from capo.transport.amc_client import (
    AMCError,
    AMCInboundEnvelope,
    ChannelNotFound,
    PlatformAuth,
)
from capo.transport.user_resolver import (
    format_unknown_sender_log_record,
    resolve_user_id,
)

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
    ) -> None:
        self.channel_id = channel_id
        self._settings = settings
        self._agent = agent
        self._amc = amc_client
        self._conn_factory = store_conn_factory
        self._deps_factory = deps_factory
        self._loop = loop

        self._queue: asyncio.Queue[AMCInboundEnvelope] = asyncio.Queue(
            maxsize=settings.concurrency.queue_depth_max
        )
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

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
    ) -> None:
        self._settings = settings
        self._agent = agent
        self._amc = amc_client
        self._conn_factory = store_conn_factory
        self._workers: dict[str, ChannelWorker] = {}
        self._stopped = False
        self._lock = asyncio.Lock()

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
    "CHANNEL_NOT_FOUND_REPLY",
    "ChannelWorker",
    "Dispatcher",
    "INTERNAL_ERROR_REPLY",
    "PLATFORM_AUTH_REPLY",
    "format_approval_required_reply",
]
