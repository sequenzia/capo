"""Hybrid per-thread conversation memory compaction (spec §5.7, §5.11, §15.3).

When a thread's :func:`capo.memory.conversation.load_history` grows past a
threshold (message count OR estimated token count — whichever fires first),
the oldest ``len(messages) - keep_recent_messages`` rows are replaced with a
single LLM-generated summary message. The most recent
``settings.compaction.keep_recent_messages`` rows are preserved verbatim so
the agent retains short-term context.

This module implements the Phase-5 "lazy compaction" hook (Task #54). It is
called by the dispatcher (:mod:`capo.transport.dispatcher`) at the end of
every agent turn after the new messages have been persisted. The check is
cheap (a single ``COUNT(*)`` + size estimate) — work only happens when the
threshold is actually exceeded. Idempotent: a thread already at-or-below
``keep_recent_messages`` is a no-op.

Atomicity
---------

The replacement is wrapped in a single
:func:`capo.memory.store.begin_immediate_with_retry` block:

  1. ``SELECT`` the oldest N rows under the lock to lock the snapshot we
     summarized against — if a concurrent writer slipped a new message in
     between the LLM call and the DELETE, we abort (the new tail message
     would otherwise be incorrectly summarized).
  2. ``DELETE`` the oldest N rows.
  3. ``INSERT`` a single summary row at ``message_index = old_oldest_index``.

The remaining "kept" rows keep their original ``message_index`` values, so
ordering is preserved and ``load_history`` returns
``[summary, kept_1, kept_2, ...]``.

Failure modes
-------------

Compaction is **fail-open**: any exception (LLM error, DB hiccup, snapshot
mismatch) is logged at WARNING level and the original transcript is left
intact. The dispatcher never crashes on a compaction failure — the user
still sees the agent's reply.

Spec references
---------------

- §5.7: Conversation Memory (per-thread, per-user) — compaction trigger,
  preserve-handles invariant, summary-as-single-system-message rendering.
- §5.11 (Task description): ``threshold_messages``, ``threshold_tokens``,
  ``keep_recent_messages``, summary preamble format.
- §6.5: ``capo.memory.compact`` Logfire span (constructor lives in
  :mod:`capo.observability`).
- §15.3: ``[compaction]`` config block.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from capo.memory.store import begin_immediate_with_retry, open_connection
from capo.observability import memory_compact_span

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from pydantic_ai.messages import ModelMessage

    from capo.config import Settings

logger = logging.getLogger("capo.memory.compaction")


__all__ = [
    "CompactionResult",
    "SummarizerProtocol",
    "compact_thread",
    "compact_thread_async",
    "estimate_tokens",
    "format_summary_preamble",
    "should_compact",
]


# Task-description compatibility alias. The public entry point is
# :func:`compact_thread_async` (LLM summarization is inherently async via
# Pydantic AI); ``compact_thread`` is the synonymous spelling used in
# Task #54's acceptance criteria. Both names dispatch to the same coroutine.
def compact_thread(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
    """Alias for :func:`compact_thread_async` (Task #54 naming).

    Returns the same awaitable; usage::

        result = await compact_thread(
            thread_id=..., user_id=..., session_id=..., db_path=...,
            summarizer=..., settings=...,
        )
    """
    return compact_thread_async(*args, **kwargs)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompactionResult:
    """Outcome of a :func:`compact_thread` call.

    ``compacted`` is the load-bearing field: ``True`` when rows were
    actually replaced, ``False`` when the call was a no-op (threshold not
    exceeded, empty thread, LLM failure, snapshot drift, etc.). Callers
    should NEVER raise on ``compacted=False`` — fail-open is the contract.
    """

    compacted: bool
    messages_summarized: int = 0
    messages_kept: int = 0
    tokens_before: int = 0
    tokens_after: int = 0
    reason: str = ""


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


# Rough heuristic: ~4 chars per token in English. Same heuristic the spec
# applies for `/status` reporting; cheap and good enough for threshold
# detection (we're choosing between "compact now" vs "skip", not billing).
_CHARS_PER_TOKEN = 4


def estimate_tokens(messages: Sequence[ModelMessage]) -> int:
    """Estimate total tokens across ``messages`` using the §5.7 heuristic.

    Serializes each message via :class:`ModelMessagesTypeAdapter` (the same
    on-disk shape used by :mod:`capo.memory.conversation`) and divides the
    byte count by 4. This intentionally over-counts JSON syntax overhead
    so the threshold trips slightly earlier — preferable to under-counting
    and letting the context window blow up.

    Returns 0 for an empty sequence.
    """
    if not messages:
        return 0
    # Lazy import — keeps pydantic_ai off the import path for callers that
    # only need :func:`should_compact` with already-known counts.
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    raw = ModelMessagesTypeAdapter.dump_json(list(messages))
    return max(1, len(raw) // _CHARS_PER_TOKEN)


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------


def should_compact(
    messages: Sequence[ModelMessage],
    settings: Settings,
) -> bool:
    """Return whether ``messages`` exceed either configured threshold.

    Returns ``True`` when:

    - ``compaction.enabled`` is ``True``, AND
    - the count exceeds ``keep_recent_messages`` (no point compacting if
      everything would be preserved verbatim anyway), AND
    - EITHER ``len(messages) > threshold_messages`` OR
      ``estimate_tokens(messages) > threshold_tokens``.

    Returns ``False`` (no-op) otherwise.
    """
    cfg = settings.compaction
    if not cfg.enabled:
        return False
    n = len(messages)
    if n == 0:
        return False
    # If everything would be preserved, there's nothing to summarize.
    if n <= cfg.keep_recent_messages:
        return False
    if n > cfg.threshold_messages:
        return True
    return cfg.threshold_tokens > 0 and estimate_tokens(messages) > cfg.threshold_tokens


# ---------------------------------------------------------------------------
# Summary message construction
# ---------------------------------------------------------------------------


def format_summary_preamble(
    *,
    oldest_index: int,
    newest_index: int,
    oldest_ts: str | None,
    newest_ts: str | None,
) -> str:
    """Render the canonical preamble for a compaction summary message.

    Wording is locked: tests assert this exact prefix shape so a future
    rewrite stays intentional. Example output::

        [Compacted: messages 1-12 covering 2026-05-11T00:01Z to
        2026-05-11T03:42Z]

    1-based indices are user-visible. If timestamps are unknown (the row
    wasn't carrying one), the timestamp clause is omitted.
    """
    range_clause = f"messages {oldest_index + 1}-{newest_index + 1}"
    if oldest_ts and newest_ts:
        return f"[Compacted: {range_clause} covering {oldest_ts} to {newest_ts}]"
    return f"[Compacted: {range_clause}]"


def _build_summary_message(
    *,
    oldest_index: int,
    newest_index: int,
    oldest_ts: str | None,
    newest_ts: str | None,
    summary_text: str,
) -> ModelMessage:
    """Construct the single ``ModelRequest`` carrying the summary.

    The summary is wrapped as a ``ModelRequest(parts=[SystemPromptPart(...)])``
    so it loads into the next ``agent.run`` via ``message_history=...`` exactly
    like any other history entry. The preamble is concatenated with the
    LLM-generated summary so the agent sees::

        [Compacted: messages 1-12 ...]: <summary text>
    """
    from pydantic_ai.messages import ModelRequest, SystemPromptPart

    preamble = format_summary_preamble(
        oldest_index=oldest_index,
        newest_index=newest_index,
        oldest_ts=oldest_ts,
        newest_ts=newest_ts,
    )
    content = f"{preamble}: {summary_text.strip()}"
    return ModelRequest(parts=[SystemPromptPart(content=content)])  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Summarizer interface
# ---------------------------------------------------------------------------


class SummarizerProtocol(Protocol):
    """Minimal interface a "cheap-model" agent must satisfy.

    Pydantic AI's :class:`Agent` already satisfies this — its ``run`` is an
    async method returning an object with an ``output`` attribute. Tests
    inject a stub that returns canned text.
    """

    async def run(self, prompt: str) -> Any: ...  # noqa: ANN401


_SUMMARY_INSTRUCTIONS = (
    "You are a concise conversation summarizer. Given a transcript of "
    "messages between a user, an assistant, and tool calls, produce a "
    "single short paragraph capturing: (1) what the user asked about, "
    "(2) key facts or decisions established, and (3) any in-flight "
    "delegation handles (cc-<id>, codex-<id>) that should be preserved. "
    "Be factual and brief — this summary REPLACES the messages in the "
    "agent's context window, so do not editorialize."
)


async def _invoke_summarizer(
    summarizer: SummarizerProtocol,
    messages: Sequence[ModelMessage],
) -> str:
    """Render ``messages`` as a flat string and run the summarizer agent.

    The prompt is intentionally simple — the summarizer is a cheap model
    (typically ``config.models.router``) and does not need tool access.
    """
    # Lazy import — keeps pydantic_ai off the import path for callers that
    # only need :func:`should_compact`.
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    raw = ModelMessagesTypeAdapter.dump_json(list(messages)).decode("utf-8")
    prompt = f"{_SUMMARY_INSTRUCTIONS}\n\n---\n{raw}\n---"
    result = await summarizer.run(prompt)
    text = getattr(result, "output", None)
    if text is None:
        # Defensive: very old Pydantic AI returned ``.data`` — handle both.
        text = getattr(result, "data", None)
    if text is None:
        raise RuntimeError("summarizer returned no output")
    return str(text)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _fetch_history_rows(
    conn: sqlite3.Connection,
    user_id: str,
    thread_id: str,
    session_id: str,
) -> list[tuple[int, str, str | None]]:
    """Return ``(message_index, model_message_json, ts)`` rows for the session."""
    cursor = conn.execute(
        "SELECT message_index, model_message_json, ts "
        "FROM conversation_history "
        "WHERE user_id = ? AND thread_id = ? AND session_id = ? "
        "ORDER BY message_index ASC",
        (user_id, thread_id, session_id),
    )
    return [(int(idx), str(payload), ts) for idx, payload, ts in cursor.fetchall()]


def _decode_messages(payloads: Sequence[str]) -> list[ModelMessage]:
    """Decode a sequence of per-row JSON payloads to ``ModelMessage`` objects."""
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    out: list[ModelMessage] = []
    for payload in payloads:
        out.extend(ModelMessagesTypeAdapter.validate_json(payload))
    return out


def _encode_message(message: ModelMessage) -> str:
    """Encode a single message to the on-disk per-row JSON shape (length-1 array)."""
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    return ModelMessagesTypeAdapter.dump_json([message]).decode("utf-8")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def compact_thread_async(
    *,
    thread_id: str,
    user_id: str,
    session_id: str,
    db_path: Path,
    summarizer: SummarizerProtocol,
    settings: Settings,
) -> CompactionResult:
    """Compact ``(user_id, thread_id, session_id)`` end-to-end. Returns the outcome.

    Fail-open: any exception is logged at WARNING and a ``compacted=False``
    result is returned. The dispatcher relies on this contract — a compaction
    failure must never crash a turn.

    Atomicity contract (§5.7, §7.3):

    1. **Snapshot** the full history under no lock (cheap read).
    2. **Summarize** via ``summarizer.run(...)`` (async LLM call).
    3. **Commit** the replacement under
       :func:`begin_immediate_with_retry`:
       - re-SELECT to detect concurrent writers (snapshot drift); if the
         row count or oldest-index changed, abort the compaction.
       - DELETE oldest rows + INSERT summary row at the original oldest
         message_index.

    The keep-recent rows keep their existing ``message_index`` values, so
    a subsequent :func:`load_history` returns
    ``[summary, kept_1, kept_2, ...]`` in their natural order.
    """
    import asyncio

    cfg = settings.compaction

    # Phase 1: snapshot the existing rows (no DB lock — read-only).
    def _snapshot() -> tuple[
        list[tuple[int, str, str | None]],
        list[ModelMessage],
    ]:
        conn = open_connection(db_path)
        try:
            rows = _fetch_history_rows(conn, user_id, thread_id, session_id)
            if not rows:
                return rows, []
            decoded = _decode_messages([payload for _, payload, _ in rows])
            return rows, decoded
        finally:
            conn.close()

    try:
        rows, history = await asyncio.to_thread(_snapshot)
    except Exception as exc:  # noqa: BLE001 - fail-open
        logger.warning(
            "compaction snapshot failed thread_id=%s user_id=%s: %r",
            thread_id,
            user_id,
            exc,
            extra={
                "event": "capo.memory.compact.snapshot_failed",
                "thread_id": thread_id,
                "user_id": user_id,
                "error": repr(exc),
            },
        )
        return CompactionResult(compacted=False, reason="snapshot_failed")

    # Edge case: empty thread or already-recent — no-op (idempotency).
    if not should_compact(history, settings):
        return CompactionResult(compacted=False, reason="not_needed")

    keep_count = cfg.keep_recent_messages
    summarize_count = len(history) - keep_count
    if summarize_count <= 0:  # pragma: no cover - guarded by should_compact
        return CompactionResult(compacted=False, reason="nothing_to_summarize")

    to_summarize_msgs = history[:summarize_count]
    to_summarize_rows = rows[:summarize_count]
    oldest_index = to_summarize_rows[0][0]
    newest_index = to_summarize_rows[-1][0]
    oldest_ts = to_summarize_rows[0][2]
    newest_ts = to_summarize_rows[-1][2]
    tokens_before = estimate_tokens(history)

    # Phase 2: invoke the LLM. Fail-open on any error.
    try:
        summary_text = await _invoke_summarizer(summarizer, to_summarize_msgs)
    except Exception as exc:  # noqa: BLE001 - fail-open
        logger.warning(
            "compaction summarizer failed thread_id=%s user_id=%s "
            "messages=%d: %r",
            thread_id,
            user_id,
            summarize_count,
            exc,
            extra={
                "event": "capo.memory.compact.summarizer_failed",
                "thread_id": thread_id,
                "user_id": user_id,
                "messages_summarized": summarize_count,
                "error": repr(exc),
            },
        )
        return CompactionResult(compacted=False, reason="summarizer_failed")

    summary_msg = _build_summary_message(
        oldest_index=oldest_index,
        newest_index=newest_index,
        oldest_ts=oldest_ts,
        newest_ts=newest_ts,
        summary_text=summary_text,
    )
    summary_payload = _encode_message(summary_msg)

    # Phase 3: commit the replacement atomically. Detect snapshot drift —
    # if another writer slipped a row in between phases 1 and 3, abort.
    expected_indices = {idx for idx, _, _ in to_summarize_rows}

    def _commit() -> tuple[bool, int]:
        conn = open_connection(db_path)
        try:
            with begin_immediate_with_retry(
                conn,
                operation=f"compact_thread:{thread_id}",
            ):
                # Re-read the targeted rows under the lock. If any of the
                # expected indices is missing, another writer (or another
                # compaction) raced us — abort.
                current = conn.execute(
                    "SELECT message_index FROM conversation_history "
                    "WHERE user_id = ? AND thread_id = ? AND session_id = ? "
                    "AND message_index <= ? "
                    "ORDER BY message_index ASC",
                    (user_id, thread_id, session_id, newest_index),
                ).fetchall()
                current_indices = {int(row[0]) for row in current}
                if current_indices != expected_indices:
                    return False, 0

                # DELETE the summarized rows.
                conn.execute(
                    "DELETE FROM conversation_history "
                    "WHERE user_id = ? AND thread_id = ? AND session_id = ? "
                    "AND message_index >= ? AND message_index <= ?",
                    (
                        user_id,
                        thread_id,
                        session_id,
                        oldest_index,
                        newest_index,
                    ),
                )
                # INSERT the single summary row at oldest_index. The
                # remaining "kept" rows keep their existing indices.
                conn.execute(
                    "INSERT INTO conversation_history "
                    "(user_id, thread_id, message_index, session_id, "
                    "model_message_json) VALUES (?, ?, ?, ?, ?)",
                    (
                        user_id,
                        thread_id,
                        oldest_index,
                        session_id,
                        summary_payload,
                    ),
                )

                # Update sessions.compacted_to_message_index so /status and
                # downstream callers can observe the high-water mark. The
                # column was provisioned in migration 001 for this purpose.
                with contextlib.suppress(sqlite3.OperationalError):
                    conn.execute(
                        "UPDATE sessions "
                        "SET compacted_to_message_index = ? "
                        "WHERE session_id = ? AND user_id = ? "
                        "AND thread_id = ?",
                        (newest_index, session_id, user_id, thread_id),
                    )
            return True, summarize_count
        finally:
            conn.close()

    try:
        committed, n = await asyncio.to_thread(_commit)
    except Exception as exc:  # noqa: BLE001 - fail-open
        logger.warning(
            "compaction commit failed thread_id=%s user_id=%s: %r",
            thread_id,
            user_id,
            exc,
            extra={
                "event": "capo.memory.compact.commit_failed",
                "thread_id": thread_id,
                "user_id": user_id,
                "error": repr(exc),
            },
        )
        return CompactionResult(compacted=False, reason="commit_failed")

    if not committed:
        logger.info(
            "compaction aborted due to snapshot drift thread_id=%s user_id=%s",
            thread_id,
            user_id,
            extra={
                "event": "capo.memory.compact.drift",
                "thread_id": thread_id,
                "user_id": user_id,
            },
        )
        return CompactionResult(compacted=False, reason="snapshot_drift")

    tokens_after = tokens_before - estimate_tokens(to_summarize_msgs) + estimate_tokens(
        [summary_msg]
    )

    # §6.5: capo.memory.compact span (fail-open on any logfire error).
    with contextlib.suppress(Exception), memory_compact_span(
        thread_id=thread_id,
        messages_summarized=n,
        tokens_before=tokens_before,
        tokens_after=tokens_after,
        user_id=user_id,
        session_id=session_id,
    ):
        pass

    # Note: the span is opened-and-immediately-closed because the
    # compaction work is already done at this point. Logfire records the
    # event with its attributes regardless of body duration. We do this
    # AFTER the commit to ensure the span doesn't appear for failed/
    # drifted attempts.

    logger.info(
        "compaction succeeded thread_id=%s user_id=%s messages_summarized=%d "
        "tokens_before=%d tokens_after=%d",
        thread_id,
        user_id,
        n,
        tokens_before,
        tokens_after,
        extra={
            "event": "capo.memory.compact.succeeded",
            "thread_id": thread_id,
            "user_id": user_id,
            "messages_summarized": n,
            "tokens_before": tokens_before,
            "tokens_after": tokens_after,
        },
    )
    return CompactionResult(
        compacted=True,
        messages_summarized=n,
        messages_kept=keep_count,
        tokens_before=tokens_before,
        tokens_after=tokens_after,
        reason="ok",
    )
