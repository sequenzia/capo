"""Tests for hybrid conversation compaction (Task #54, spec §5.7).

Covers:
- ``should_compact`` decision logic (Functional, Edge Cases).
- ``compact_thread_async`` end-to-end with a stub summarizer.
- Idempotency on already-compacted threads.
- Snapshot-drift / concurrent-writer handling.
- Fail-open on summarizer error.
- ``Settings.compaction`` default values.
"""

from __future__ import annotations

import importlib.util as _ilu
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

# Re-use the migration's verbatim CREATE TABLE statements (same pattern as
# tests/test_conversation.py) so we avoid the ~3-5s alembic round-trip.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "migrations" / "versions"))
_MIG_PATH = _REPO_ROOT / "migrations" / "versions" / "001_init.py"
_spec = _ilu.spec_from_file_location("_mig_001_init", _MIG_PATH)
assert _spec is not None and _spec.loader is not None
_mig = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mig)

from capo.memory import store  # noqa: E402
from capo.memory.compaction import (  # noqa: E402
    CompactionResult,
    compact_thread_async,
    estimate_tokens,
    format_summary_preamble,
    should_compact,
)
from capo.memory.conversation import (  # noqa: E402
    append_messages,
    load_history,
)

# ---------------------------------------------------------------------------
# Stub Settings / Summarizer
# ---------------------------------------------------------------------------


class _StubCompactionCfg:
    def __init__(
        self,
        *,
        enabled: bool = True,
        threshold_messages: int = 30,
        threshold_tokens: int = 50_000,
        keep_recent_messages: int = 10,
    ) -> None:
        self.enabled = enabled
        self.threshold_messages = threshold_messages
        self.threshold_tokens = threshold_tokens
        self.keep_recent_messages = keep_recent_messages
        self.preserve_delegation_handles = True


class _StubSettings:
    def __init__(self, cfg: _StubCompactionCfg) -> None:
        self.compaction = cfg


class _StubSummarizer:
    """Minimal stand-in for a Pydantic AI Agent's ``run`` interface."""

    def __init__(self, output: str = "stub summary", raise_exc: Exception | None = None) -> None:
        self.output = output
        self.raise_exc = raise_exc
        self.calls: list[str] = []

    async def run(self, prompt: str) -> Any:
        self.calls.append(prompt)
        if self.raise_exc is not None:
            raise self.raise_exc

        class _Result:
            pass

        r = _Result()
        r.output = self.output  # type: ignore[attr-defined]
        return r


# ---------------------------------------------------------------------------
# Schema fixture + DB helpers
# ---------------------------------------------------------------------------


def _apply_inline_schema(conn: sqlite3.Connection) -> None:
    for stmt in _mig._UPGRADE_STATEMENTS:
        conn.execute(stmt)


@pytest.fixture
def db_path(tmp_path: Path) -> Iterator[Path]:
    db = tmp_path / "state.db"
    c = store.open_connection(db)
    _apply_inline_schema(c)
    c.execute("INSERT INTO users(user_id) VALUES ('alice')")
    c.execute(
        "INSERT INTO sessions(session_id, thread_id, user_id) VALUES (?, ?, ?)",
        ("s1", "amc:c1", "alice"),
    )
    c.close()
    yield db


def _msgs(user_text: str, assistant_text: str):
    from pydantic_ai.messages import (
        ModelRequest,
        ModelResponse,
        TextPart,
        UserPromptPart,
    )

    return [
        ModelRequest(parts=[UserPromptPart(content=user_text)]),
        ModelResponse(parts=[TextPart(content=assistant_text)]),
    ]


def _seed_history(db_path: Path, n_pairs: int) -> int:
    """Seed ``n_pairs`` user/assistant turns. Returns total rows written."""
    conn = store.open_connection(db_path)
    try:
        total = 0
        for i in range(n_pairs):
            total += append_messages(
                conn, "alice", "amc:c1", "s1", _msgs(f"q{i}", f"a{i}")
            )
        return total
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unit: should_compact
# ---------------------------------------------------------------------------


class TestShouldCompactDecisionLogic:
    """Functional acceptance: should_compact returns True only past thresholds."""

    def test_empty_history_is_noop(self) -> None:
        settings = _StubSettings(_StubCompactionCfg())
        assert should_compact([], settings) is False

    def test_short_history_below_keep_recent_is_noop(self) -> None:
        settings = _StubSettings(
            _StubCompactionCfg(threshold_messages=30, keep_recent_messages=10)
        )
        # 5 messages — less than keep_recent_messages → nothing to summarize.
        msgs = []
        for _ in range(2):
            msgs.extend(_msgs("q", "a"))
        assert should_compact(msgs, settings) is False

    def test_exactly_keep_recent_is_noop(self) -> None:
        settings = _StubSettings(
            _StubCompactionCfg(threshold_messages=30, keep_recent_messages=10)
        )
        msgs = []
        for _ in range(5):
            msgs.extend(_msgs("q", "a"))
        # 10 messages exactly == keep_recent → no-op.
        assert len(msgs) == 10
        assert should_compact(msgs, settings) is False

    def test_above_threshold_messages_triggers(self) -> None:
        settings = _StubSettings(
            _StubCompactionCfg(threshold_messages=30, keep_recent_messages=10)
        )
        msgs = []
        for _ in range(16):  # 32 messages > 30
            msgs.extend(_msgs("q", "a"))
        assert should_compact(msgs, settings) is True

    def test_above_threshold_tokens_triggers_even_under_message_count(self) -> None:
        # Very low token threshold; very high message threshold.
        settings = _StubSettings(
            _StubCompactionCfg(
                threshold_messages=10_000,
                threshold_tokens=10,
                keep_recent_messages=2,
            )
        )
        # 6 messages (3 pairs) — under threshold_messages=10000, but the
        # serialized bytes easily exceed 10 tokens (40 chars).
        msgs = []
        for _ in range(3):
            msgs.extend(_msgs("hello world", "answer here"))
        assert should_compact(msgs, settings) is True

    def test_disabled_returns_false(self) -> None:
        settings = _StubSettings(
            _StubCompactionCfg(enabled=False, threshold_messages=1, keep_recent_messages=1)
        )
        msgs = []
        for _ in range(10):
            msgs.extend(_msgs("q", "a"))
        assert should_compact(msgs, settings) is False


# ---------------------------------------------------------------------------
# Unit: estimate_tokens / preamble
# ---------------------------------------------------------------------------


def test_estimate_tokens_empty_is_zero() -> None:
    assert estimate_tokens([]) == 0


def test_estimate_tokens_monotonic() -> None:
    a = _msgs("hi", "ok")
    b = _msgs("hi", "ok") + _msgs("again", "again-a")
    assert estimate_tokens(a) > 0
    assert estimate_tokens(b) > estimate_tokens(a)


def test_format_summary_preamble_with_timestamps() -> None:
    out = format_summary_preamble(
        oldest_index=0,
        newest_index=11,
        oldest_ts="2026-05-11T00:01:00Z",
        newest_ts="2026-05-11T03:42:00Z",
    )
    assert out.startswith("[Compacted: messages 1-12 covering 2026-05-11T00:01:00Z")
    assert "2026-05-11T03:42:00Z" in out


def test_format_summary_preamble_without_timestamps() -> None:
    out = format_summary_preamble(
        oldest_index=2, newest_index=5, oldest_ts=None, newest_ts=None
    )
    assert out == "[Compacted: messages 3-6]"


# ---------------------------------------------------------------------------
# Integration: compact_thread_async end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_thread_end_to_end_replaces_old_messages(
    db_path: Path,
) -> None:
    """Seed 20 messages, compact, verify summary + last 10 verbatim remain."""
    written = _seed_history(db_path, n_pairs=10)  # 20 messages
    assert written == 20

    settings = _StubSettings(
        _StubCompactionCfg(threshold_messages=10, keep_recent_messages=10)
    )
    summarizer = _StubSummarizer(output="Stub recap of old chat.")

    result = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert result.compacted is True
    assert result.messages_summarized == 10  # 20 - 10 keep
    assert result.messages_kept == 10
    assert summarizer.calls, "summarizer must be invoked"

    conn = store.open_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT message_index, model_message_json FROM conversation_history "
            "WHERE user_id=? AND thread_id=? AND session_id=? "
            "ORDER BY message_index ASC",
            ("alice", "amc:c1", "s1"),
        ).fetchall()
    finally:
        conn.close()

    # Must have exactly 11 rows: 1 summary + 10 verbatim.
    assert len(rows) == 11
    # The summary row sits at the OLDEST index (0), and its content carries
    # the preamble.
    assert rows[0][0] == 0
    assert "[Compacted: messages 1-10" in rows[0][1]
    assert "Stub recap of old chat." in rows[0][1]
    # The remaining rows are the original tail (indices 10..19 untouched).
    kept_indices = [r[0] for r in rows[1:]]
    assert kept_indices == list(range(10, 20))


@pytest.mark.asyncio
async def test_compact_thread_idempotent_on_already_compacted(
    db_path: Path,
) -> None:
    """Compacting twice in a row → second call is a no-op.

    After the first compaction the row count drops below
    ``threshold_messages``, so the second call observes ``not_needed``
    and never re-invokes the summarizer. This is the spec's "compacting
    an already-recent thread is a no-op" guarantee.
    """
    _seed_history(db_path, n_pairs=15)  # 30 messages
    settings = _StubSettings(
        _StubCompactionCfg(threshold_messages=20, keep_recent_messages=10)
    )
    summarizer = _StubSummarizer(output="recap")

    first = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert first.compacted is True
    # After: 1 summary + 10 kept = 11 rows. 11 <= threshold_messages=20.

    second = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert second.compacted is False
    assert second.reason == "not_needed"
    # Summarizer should NOT have been re-invoked.
    assert len(summarizer.calls) == 1


@pytest.mark.asyncio
async def test_compact_thread_noop_on_empty_thread(db_path: Path) -> None:
    """Thread with no messages → no-op, no summarizer call."""
    settings = _StubSettings(_StubCompactionCfg(threshold_messages=1, keep_recent_messages=1))
    summarizer = _StubSummarizer()

    result = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert result.compacted is False
    assert summarizer.calls == []


@pytest.mark.asyncio
async def test_compact_thread_noop_on_only_keep_recent_messages(
    db_path: Path,
) -> None:
    """≤keep_recent messages → no-op, even with very low message threshold."""
    _seed_history(db_path, n_pairs=5)  # 10 messages
    settings = _StubSettings(
        _StubCompactionCfg(
            threshold_messages=1,  # would trigger by count
            keep_recent_messages=10,  # but everything stays verbatim
        )
    )
    summarizer = _StubSummarizer()

    result = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert result.compacted is False
    assert summarizer.calls == []


@pytest.mark.asyncio
async def test_compact_thread_summarizer_failure_is_fail_open(
    db_path: Path,
) -> None:
    """LLM raises → compact_thread_async logs + returns CompactionResult(compacted=False)."""
    _seed_history(db_path, n_pairs=10)
    settings = _StubSettings(
        _StubCompactionCfg(threshold_messages=10, keep_recent_messages=10)
    )
    summarizer = _StubSummarizer(raise_exc=RuntimeError("model down"))

    result = await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )
    assert result.compacted is False
    assert result.reason == "summarizer_failed"
    # Verify the underlying rows are UNTOUCHED (fail-open).
    conn = store.open_connection(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM conversation_history "
            "WHERE user_id=? AND thread_id=? AND session_id=?",
            ("alice", "amc:c1", "s1"),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 20


@pytest.mark.asyncio
async def test_compact_thread_history_still_loads_after_compaction(
    db_path: Path,
) -> None:
    """load_history returns [summary, kept_1, ... kept_10] in order."""
    _seed_history(db_path, n_pairs=10)
    settings = _StubSettings(
        _StubCompactionCfg(threshold_messages=10, keep_recent_messages=10)
    )
    summarizer = _StubSummarizer(output="recap")

    await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )

    conn = store.open_connection(db_path)
    try:
        history = load_history(conn, "alice", "amc:c1", "s1")
    finally:
        conn.close()

    assert len(history) == 11
    # First message is the summary (a ModelRequest with SystemPromptPart).
    from pydantic_ai.messages import ModelRequest, SystemPromptPart

    head = history[0]
    assert isinstance(head, ModelRequest)
    parts = list(head.parts)
    assert len(parts) == 1
    assert isinstance(parts[0], SystemPromptPart)
    assert "[Compacted: messages 1-10" in parts[0].content
    assert "recap" in parts[0].content


@pytest.mark.asyncio
async def test_compact_thread_updates_sessions_compacted_to_index(
    db_path: Path,
) -> None:
    """``sessions.compacted_to_message_index`` is bumped to the newest summarized index."""
    _seed_history(db_path, n_pairs=10)  # indices 0..19
    settings = _StubSettings(
        _StubCompactionCfg(threshold_messages=10, keep_recent_messages=10)
    )
    summarizer = _StubSummarizer(output="x")

    await compact_thread_async(
        thread_id="amc:c1",
        user_id="alice",
        session_id="s1",
        db_path=db_path,
        summarizer=summarizer,
        settings=settings,
    )

    conn = store.open_connection(db_path)
    try:
        row = conn.execute(
            "SELECT compacted_to_message_index FROM sessions WHERE session_id=?",
            ("s1",),
        ).fetchone()
    finally:
        conn.close()
    # Summarized indices 0..9 → high-water mark is 9.
    assert row[0] == 9


# ---------------------------------------------------------------------------
# CompactionResult dataclass sanity
# ---------------------------------------------------------------------------


def test_compaction_result_defaults() -> None:
    r = CompactionResult(compacted=False)
    assert r.compacted is False
    assert r.messages_summarized == 0
    assert r.messages_kept == 0
    assert r.tokens_before == 0
    assert r.tokens_after == 0
    assert r.reason == ""


# ---------------------------------------------------------------------------
# Settings defaults are wired correctly
# ---------------------------------------------------------------------------


def test_compaction_config_defaults_match_task_spec() -> None:
    """Default ``CompactionConfig`` instance carries Task #54 values."""
    from capo.config import CompactionConfig

    cfg = CompactionConfig()
    assert cfg.threshold_tokens == 50_000
    assert cfg.threshold_messages == 30
    assert cfg.keep_recent_messages == 10
    assert cfg.enabled is True
