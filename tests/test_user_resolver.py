"""Unit tests for :mod:`capo.transport.user_resolver` and the eager
``[users]`` empty-table boot check on :class:`capo.config.UsersConfig`.

Covers Task #17 acceptance criteria (spec §5.11, §9.1, §15.3):

* Each mapped sender resolves to the configured ``user_id``.
* Two senders pointing at the same ``user_id`` both resolve there
  (intended: shared conversation history).
* Two **users** claiming the same sender is rejected at lookup time.
* Unknown sender → ``None``; the dispatcher rejects + warns (Task #11).
* Empty ``[users]`` table at :meth:`Settings.load` → :class:`ConfigError`
  with the key path ``users`` and a "at least one user is required"
  message.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from capo.config import ConfigError, Settings
from capo.transport.user_resolver import (
    UNKNOWN_SENDER,
    format_unknown_sender_log_record,
    resolve_user_id,
)
from tests.test_config import _VALID_ENV, _VALID_TOML, _write_config

# Re-use the canonical valid TOML / env / config-writer from test_config so we
# exercise the real Settings.load path (not a hand-rolled stub).


# ---------------------------------------------------------------------------
# Pure resolver behaviour.
# ---------------------------------------------------------------------------


def test_resolve_known_sender_returns_user_id(tmp_path: Path) -> None:
    """Every sender listed under ``[users.<id>].amc_senders`` resolves."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    # Baseline config maps both an SMS and a Discord sender to "owner".
    assert resolve_user_id("+15551234567", s) == "owner"
    assert resolve_user_id("discord:user:123", s) == "owner"


def test_resolve_two_senders_one_user_id_both_route_same(tmp_path: Path) -> None:
    """Two senders mapped to the same user_id both route to that user.

    Intentional per §5.11: a single human reachable via SMS *and* Discord
    sees one shared conversation history.
    """
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    assert resolve_user_id("+15551234567", s) == resolve_user_id(
        "discord:user:123", s
    )


def test_resolve_unknown_sender_returns_none(tmp_path: Path) -> None:
    """Unmapped senders return ``None`` (= :data:`UNKNOWN_SENDER`).

    The dispatcher (Task #11) is expected to log a warning and drop the
    envelope when this happens — no domain row is written.
    """
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    assert resolve_user_id("+19998887777", s) is None
    assert resolve_user_id("discord:user:999", s) is UNKNOWN_SENDER
    # Empty sender id is treated as unknown — guards against malformed envelopes.
    assert resolve_user_id("", s) is None


def test_resolve_multiple_users_each_maps_to_own_user_id(tmp_path: Path) -> None:
    """Distinct ``[users.X]`` blocks each resolve to their own user_id."""
    toml = _VALID_TOML + """
[users.guest]
amc_senders = ["+15559998888"]
display_name = "Guest"
"""
    cfg = _write_config(tmp_path, toml)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    assert resolve_user_id("+15551234567", s) == "owner"
    assert resolve_user_id("discord:user:123", s) == "owner"
    assert resolve_user_id("+15559998888", s) == "guest"
    # Cross-user senders do not leak across users.
    assert resolve_user_id("+15559998888", s) != "owner"


def test_resolve_duplicate_sender_across_users_rejected_at_lookup(
    tmp_path: Path,
) -> None:
    """Two **users** claiming the same sender raises on lookup.

    This is enforced by :attr:`UsersConfig.sender_to_user_id`; we surface it
    here so a future dispatcher caller gets a hard failure rather than a
    silent "last user wins" bug.
    """
    toml = _VALID_TOML + """
[users.guest]
amc_senders = ["+15551234567"]
display_name = "Guest"
"""
    cfg = _write_config(tmp_path, toml)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    with pytest.raises(ValueError, match="claimed by both"):
        resolve_user_id("+15551234567", s)


# ---------------------------------------------------------------------------
# Eager empty-[users] boot check (Settings.load).
# ---------------------------------------------------------------------------


def test_empty_users_table_rejected_at_load(tmp_path: Path) -> None:
    """``[users]`` table present but empty → ConfigError at load time."""
    # Strip the [users.owner] block, leaving no user entries at all.
    toml = _VALID_TOML.replace(
        '[users.owner]\n'
        'amc_senders = ["+15551234567", "discord:user:123"]\n'
        'display_name = "Owner"\n',
        "",
    )
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    msg = str(exc.value)
    assert "users" in msg
    assert "at least one user is required" in msg


def test_users_table_missing_entirely_rejected_at_load(tmp_path: Path) -> None:
    """Same failure mode if the ``[users]`` block is omitted entirely."""
    toml = _VALID_TOML.replace(
        '[users.owner]\n'
        'amc_senders = ["+15551234567", "discord:user:123"]\n'
        'display_name = "Owner"\n',
        "",
    )
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError, match="at least one user is required"):
        Settings.load(cfg, env_override=dict(_VALID_ENV))


def test_v1_ships_with_exactly_one_mapping(tmp_path: Path) -> None:
    """V1 invariant per §5.11: the canonical config has exactly one user."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert len(s.users.entries) == 1
    # And the literal TOML key is the user_id written to every domain table
    # (per the §15.3 note).
    assert list(s.users.entries) == ["owner"]


# ---------------------------------------------------------------------------
# Unknown-sender log record helper.
# ---------------------------------------------------------------------------


def test_format_unknown_sender_log_record_minimal() -> None:
    """Record carries event name + sender_id at minimum."""
    rec = format_unknown_sender_log_record("+15551112222")
    assert rec == {
        "event": "amc.envelope.rejected.unknown_sender",
        "sender_id": "+15551112222",
    }


def test_format_unknown_sender_log_record_with_delivery_id() -> None:
    """Record carries delivery_id when provided (correlates with AMC retries)."""
    rec = format_unknown_sender_log_record(
        "+15551112222", delivery_id="01HXYZ-deadbeef"
    )
    assert rec["delivery_id"] == "01HXYZ-deadbeef"
    assert rec["sender_id"] == "+15551112222"
    assert rec["event"] == "amc.envelope.rejected.unknown_sender"
