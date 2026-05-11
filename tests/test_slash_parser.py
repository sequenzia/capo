"""Unit tests for the ``/approve`` / ``/deny`` slash command parser (Task #41).

Covers every acceptance criterion from the task spec plus exhaustive edge
cases. The parser is pure, so these tests are simple input → output
assertions — no fixtures, no mocking. Aim is ~100% line coverage of
``capo/transport/slash.py``.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from capo.transport.slash import (
    APPROVAL_ID_RE,
    DELEGATION_ID_RE,
    RECOGNIZED_VERBS,
    SlashCommand,
    parse_slash_command,
)

# ---------------------------------------------------------------------------
# Test fixtures (plain constants — no pytest fixtures needed)
# ---------------------------------------------------------------------------

#: A real-shaped uuid4 hex (32 lowercase hex chars).
VALID_ID = uuid.uuid4().hex
OTHER_VALID_ID = uuid.uuid4().hex


# ---------------------------------------------------------------------------
# SlashCommand value-object
# ---------------------------------------------------------------------------


class TestSlashCommandModel:
    """The frozen value-object itself: shape, immutability, strict fields."""

    def test_construct_minimal(self) -> None:
        cmd = SlashCommand(verb="approve")
        assert cmd.verb == "approve"
        assert cmd.approval_id is None
        assert cmd.reason is None

    def test_construct_full(self) -> None:
        cmd = SlashCommand(verb="deny", approval_id=VALID_ID, reason="bad diff")
        assert cmd.verb == "deny"
        assert cmd.approval_id == VALID_ID
        assert cmd.reason == "bad diff"

    def test_frozen(self) -> None:
        cmd = SlashCommand(verb="approve")
        with pytest.raises(ValidationError):
            cmd.verb = "deny"  # type: ignore[misc]

    def test_extra_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            SlashCommand(verb="approve", junk="x")  # type: ignore[call-arg]

    def test_unknown_verb_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SlashCommand(verb="nope")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Functional: recognized forms
# ---------------------------------------------------------------------------


class TestRecognizedForms:
    """Every form listed in the acceptance criteria."""

    def test_approve_no_args(self) -> None:
        cmd = parse_slash_command("/approve")
        assert cmd == SlashCommand(verb="approve", approval_id=None, reason=None)

    def test_approve_with_id(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID}")
        assert cmd == SlashCommand(verb="approve", approval_id=VALID_ID, reason=None)

    def test_approve_with_id_and_reason(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID} ship it")
        assert cmd == SlashCommand(verb="approve", approval_id=VALID_ID, reason="ship it")

    def test_deny_no_args(self) -> None:
        cmd = parse_slash_command("/deny")
        assert cmd == SlashCommand(verb="deny", approval_id=None, reason=None)

    def test_deny_with_id(self) -> None:
        cmd = parse_slash_command(f"/deny {VALID_ID}")
        assert cmd == SlashCommand(verb="deny", approval_id=VALID_ID, reason=None)

    def test_deny_with_id_and_reason(self) -> None:
        cmd = parse_slash_command(f"/deny {VALID_ID} too risky")
        assert cmd == SlashCommand(verb="deny", approval_id=VALID_ID, reason="too risky")


# ---------------------------------------------------------------------------
# Functional: case insensitivity
# ---------------------------------------------------------------------------


class TestCaseInsensitivity:
    def test_uppercase_approve(self) -> None:
        cmd = parse_slash_command(f"/APPROVE {VALID_ID}")
        assert cmd is not None
        assert cmd.verb == "approve"
        assert cmd.approval_id == VALID_ID

    def test_mixed_case_deny(self) -> None:
        cmd = parse_slash_command(f"/DeNy {VALID_ID}")
        assert cmd is not None
        assert cmd.verb == "deny"

    def test_uppercase_approve_no_args(self) -> None:
        cmd = parse_slash_command("/APPROVE")
        assert cmd is not None
        assert cmd.verb == "approve"

    def test_uppercase_id_not_recognized(self) -> None:
        # APPROVAL_ID_RE is lowercase-only; an uppercase hex token is
        # treated as a reason, not an id.
        upper_id = VALID_ID.upper()
        cmd = parse_slash_command(f"/approve {upper_id}")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason == upper_id


# ---------------------------------------------------------------------------
# Functional: whitespace canonicalization
# ---------------------------------------------------------------------------


class TestWhitespace:
    def test_leading_and_trailing_whitespace_stripped(self) -> None:
        cmd = parse_slash_command(f"   /approve {VALID_ID}   ")
        assert cmd == SlashCommand(verb="approve", approval_id=VALID_ID, reason=None)

    def test_internal_whitespace_in_reason_collapsed(self) -> None:
        cmd = parse_slash_command(f"/approve   {VALID_ID}   reason with   spaces")
        assert cmd is not None
        assert cmd.reason == "reason with spaces"

    def test_tabs_collapsed_to_single_space(self) -> None:
        cmd = parse_slash_command(f"/approve\t{VALID_ID}\tlooks\t\tgood")
        assert cmd is not None
        assert cmd.approval_id == VALID_ID
        assert cmd.reason == "looks good"

    def test_reason_only_whitespace_becomes_none(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID}    \t  ")
        assert cmd is not None
        assert cmd.reason is None


# ---------------------------------------------------------------------------
# Functional: approval_id shape validation
# ---------------------------------------------------------------------------


class TestApprovalIdValidation:
    def test_uuid4_hex_accepted(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID}")
        assert cmd is not None
        assert cmd.approval_id == VALID_ID

    def test_dashed_uuid_treated_as_reason(self) -> None:
        # Standard UUID with dashes does NOT match the canonical hex form.
        dashed = str(uuid.uuid4())
        cmd = parse_slash_command(f"/approve {dashed}")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason == dashed

    def test_short_hex_treated_as_reason(self) -> None:
        cmd = parse_slash_command("/approve deadbeef explanation")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason == "deadbeef explanation"

    def test_long_hex_treated_as_reason(self) -> None:
        # 33 chars — one too many.
        too_long = "a" * 33
        cmd = parse_slash_command(f"/approve {too_long}")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason == too_long

    def test_non_hex_token_treated_as_reason(self) -> None:
        # 32 chars but contains non-hex.
        not_hex = "z" * 32
        cmd = parse_slash_command(f"/approve {not_hex} suffix")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason == f"{not_hex} suffix"

    def test_id_then_reason(self) -> None:
        cmd = parse_slash_command(f"/deny {VALID_ID} not ready")
        assert cmd is not None
        assert cmd.approval_id == VALID_ID
        assert cmd.reason == "not ready"


# ---------------------------------------------------------------------------
# Non-matches: return None
# ---------------------------------------------------------------------------


class TestNonMatches:
    def test_empty_string(self) -> None:
        assert parse_slash_command("") is None

    def test_whitespace_only(self) -> None:
        assert parse_slash_command("   \t  ") is None

    def test_no_leading_slash(self) -> None:
        assert parse_slash_command(f"approve {VALID_ID}") is None

    def test_natural_language(self) -> None:
        assert parse_slash_command("please approve this") is None

    def test_slash_alone(self) -> None:
        assert parse_slash_command("/") is None

    def test_unknown_verb(self) -> None:
        # Truly unknown verb — ``/kill`` is now §5.10 (Task #52). Use a
        # verb that is NOT in ``RECOGNIZED_VERBS``.
        assert parse_slash_command(f"/frobnicate {VALID_ID}") is None

    def test_unknown_verb_no_args(self) -> None:
        # ``/status`` is now §5.10 (Task #52). Use a truly unknown verb.
        assert parse_slash_command("/whatever") is None

    def test_none_input(self) -> None:
        # Defensive: non-str input returns None rather than raising.
        assert parse_slash_command(None) is None  # type: ignore[arg-type]

    def test_non_string_input(self) -> None:
        assert parse_slash_command(123) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Edge: verb prefix collisions
# ---------------------------------------------------------------------------


class TestVerbBoundary:
    """Verbs that *start with* ``approve``/``deny`` but aren't them."""

    def test_approveall_not_recognized(self) -> None:
        assert parse_slash_command(f"/approveall {VALID_ID}") is None

    def test_approver_not_recognized(self) -> None:
        assert parse_slash_command(f"/approver {VALID_ID}") is None

    def test_denial_not_recognized(self) -> None:
        assert parse_slash_command(f"/denial {VALID_ID}") is None

    def test_approve_followed_by_punctuation_not_recognized(self) -> None:
        # ``/approve!`` is not a valid invocation — the verb is not
        # followed by whitespace.
        assert parse_slash_command("/approve!") is None


# ---------------------------------------------------------------------------
# Edge: multi-line messages
# ---------------------------------------------------------------------------


class TestMultiLine:
    def test_only_first_line_parsed(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID}\nsecond line\n/deny {OTHER_VALID_ID}")
        assert cmd is not None
        assert cmd.verb == "approve"
        assert cmd.approval_id == VALID_ID
        assert cmd.reason is None

    def test_multiline_reason_dropped_after_first_line(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID} reason line one\nreason line two")
        assert cmd is not None
        assert cmd.reason == "reason line one"

    def test_crlf_newline(self) -> None:
        cmd = parse_slash_command(f"/approve {VALID_ID}\r\nignored")
        assert cmd is not None
        assert cmd.verb == "approve"
        assert cmd.approval_id == VALID_ID

    def test_cr_only_newline(self) -> None:
        cmd = parse_slash_command(f"/deny {VALID_ID}\rignored")
        assert cmd is not None
        assert cmd.verb == "deny"
        assert cmd.approval_id == VALID_ID

    def test_leading_blank_line_means_no_command(self) -> None:
        # First line is empty → no slash → no match.
        assert parse_slash_command(f"\n/approve {VALID_ID}") is None


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Task #52: §5.10 session-control verbs (/new, /status, /clear, /kill, /override)
# ---------------------------------------------------------------------------


class TestNewVerb:
    def test_new_no_args(self) -> None:
        cmd = parse_slash_command("/new")
        assert cmd == SlashCommand(verb="new")

    def test_new_case_insensitive(self) -> None:
        cmd = parse_slash_command("/NeW")
        assert cmd is not None
        assert cmd.verb == "new"

    def test_new_with_trailing_whitespace(self) -> None:
        cmd = parse_slash_command("  /new   ")
        assert cmd == SlashCommand(verb="new")

    def test_new_extra_tokens_ignored(self) -> None:
        # ``/new`` accepts no arguments. Any trailing tokens are tolerated
        # and silently ignored (zero-token shortcut per §5.10).
        cmd = parse_slash_command("/new please")
        assert cmd == SlashCommand(verb="new")

    def test_newer_not_recognized(self) -> None:
        # Verb-boundary regex must reject ``/newer``.
        assert parse_slash_command("/newer") is None


class TestStatusVerb:
    def test_status_no_args(self) -> None:
        cmd = parse_slash_command("/status")
        assert cmd == SlashCommand(verb="status")

    def test_status_case_insensitive(self) -> None:
        cmd = parse_slash_command("/STATUS")
        assert cmd is not None
        assert cmd.verb == "status"

    def test_status_extra_tokens_ignored(self) -> None:
        cmd = parse_slash_command("/status now")
        assert cmd == SlashCommand(verb="status")


class TestClearVerb:
    def test_clear_no_args(self) -> None:
        cmd = parse_slash_command("/clear")
        assert cmd == SlashCommand(verb="clear")

    def test_clear_case_insensitive(self) -> None:
        cmd = parse_slash_command("/CLEAR")
        assert cmd is not None
        assert cmd.verb == "clear"


class TestKillVerb:
    def test_kill_with_valid_hex_id(self) -> None:
        cmd = parse_slash_command(f"/kill {VALID_ID}")
        assert cmd == SlashCommand(verb="kill", delegation_id=VALID_ID)

    def test_kill_case_insensitive(self) -> None:
        cmd = parse_slash_command(f"/KILL {VALID_ID}")
        assert cmd is not None
        assert cmd.verb == "kill"
        assert cmd.delegation_id == VALID_ID

    def test_kill_no_id(self) -> None:
        # Edge case from task: ``/kill`` with no id → parser returns the
        # command with ``delegation_id=None``. The dispatcher emits the
        # friendly "missing delegation id" error.
        cmd = parse_slash_command("/kill")
        assert cmd == SlashCommand(verb="kill", delegation_id=None)

    def test_kill_with_garbage_token(self) -> None:
        # The parser surfaces non-hex tokens verbatim — the dispatcher
        # validates against ``DELEGATION_ID_RE`` and emits "unknown
        # delegation".
        cmd = parse_slash_command("/kill not-a-valid-id")
        assert cmd == SlashCommand(
            verb="kill", delegation_id="not-a-valid-id"
        )

    def test_kill_extra_args_ignored(self) -> None:
        # ``/kill <id> extra args`` keeps only the id; the extra args are
        # tolerated and discarded (no per-kill reason in §5.10).
        cmd = parse_slash_command(f"/kill {VALID_ID} something extra")
        assert cmd is not None
        assert cmd.verb == "kill"
        assert cmd.delegation_id == VALID_ID
        assert cmd.reason is None


class TestOverrideVerb:
    def test_override_no_args(self) -> None:
        cmd = parse_slash_command("/override")
        assert cmd == SlashCommand(verb="override")

    def test_override_case_insensitive(self) -> None:
        cmd = parse_slash_command("/OvErRiDe")
        assert cmd is not None
        assert cmd.verb == "override"


class TestSessionControlCommandFields:
    """Fields not relevant to the session-control verbs are always None."""

    @pytest.mark.parametrize(
        "verb_text,expected_verb",
        [
            ("/new", "new"),
            ("/status", "status"),
            ("/clear", "clear"),
            ("/override", "override"),
        ],
    )
    def test_no_id_or_reason(self, verb_text: str, expected_verb: str) -> None:
        cmd = parse_slash_command(verb_text)
        assert cmd is not None
        assert cmd.verb == expected_verb
        assert cmd.approval_id is None
        assert cmd.delegation_id is None
        assert cmd.reason is None

    def test_kill_carries_only_delegation_id(self) -> None:
        cmd = parse_slash_command(f"/kill {VALID_ID}")
        assert cmd is not None
        assert cmd.approval_id is None
        assert cmd.reason is None
        assert cmd.delegation_id == VALID_ID


class TestModuleSurface:
    def test_recognized_verbs_set(self) -> None:
        # Full §5.10 set after Task #52.
        assert (
            frozenset(
                {
                    "approve",
                    "deny",
                    "new",
                    "status",
                    "clear",
                    "kill",
                    "override",
                }
            )
            == RECOGNIZED_VERBS
        )

    def test_approval_id_re_accepts_uuid4_hex(self) -> None:
        for _ in range(10):
            assert APPROVAL_ID_RE.match(uuid.uuid4().hex)

    def test_approval_id_re_rejects_uppercase(self) -> None:
        assert APPROVAL_ID_RE.match(VALID_ID.upper()) is None

    def test_approval_id_re_rejects_dashed(self) -> None:
        assert APPROVAL_ID_RE.match(str(uuid.uuid4())) is None

    def test_delegation_id_re_accepts_uuid4_hex(self) -> None:
        for _ in range(10):
            assert DELEGATION_ID_RE.match(uuid.uuid4().hex)

    def test_delegation_id_re_rejects_uppercase(self) -> None:
        assert DELEGATION_ID_RE.match(VALID_ID.upper()) is None

    def test_delegation_id_re_rejects_dashed(self) -> None:
        assert DELEGATION_ID_RE.match(str(uuid.uuid4())) is None
