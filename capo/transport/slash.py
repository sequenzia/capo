"""Slash command parser for pre-agent control commands (spec §5.8, §5.10).

Per §5.8 "Inbound Contract Addendum" and §5.10 acceptance criteria, the
dispatcher must pre-parse inbound text for slash commands BEFORE entering
the agent loop. This module provides the parser for the full §5.10 set:

* ``/approve``, ``/deny`` — approval shortcuts (Task #41).
* ``/new`` — start a fresh session (Task #52).
* ``/status`` — thread status snapshot (Task #52).
* ``/clear`` — clear per-thread conversation memory (Task #52).
* ``/kill <delegation_id>`` — kill a delegation (Task #52).
* ``/override`` — bypass hard cost cap for the next agent turn (Task #52).

Design constraints
------------------

* **Pure function, no side effects.** The parser is a single
  :func:`parse_slash_command` entry point + a frozen Pydantic value-object.
  Easy to unit-test, trivial to compose into the dispatcher without
  import-time coupling.
* **Conservative recognition.** Only verbs that are *exactly* in
  :data:`RECOGNIZED_VERBS` (case-insensitively) match. ``/approveall``,
  ``/approver``, ``/den`` etc. return ``None`` — they will fall through to
  the agent loop.
* **Hex-id validation.** Approval IDs and delegation IDs are both minted
  as ``uuid.uuid4().hex`` (32 lowercase hex chars). A token that does NOT
  match ``^[a-f0-9]{32}$`` is NOT recognized as an ID. For ``/approve`` /
  ``/deny`` such a token is treated as part of the reason; for ``/kill``
  the parser still captures the raw token (the dispatcher decides what to
  do — typically a friendly "unknown delegation" error).
* **Reason canonicalization.** Internal whitespace runs are collapsed to a
  single space, leading/trailing whitespace is stripped. Multi-line input
  truncates at the first newline (only the first line is parsed).

The parser returns :class:`SlashCommand` on a match and ``None`` for
everything else (non-slash input, unknown verb, blank string). The
dispatcher uses ``None`` as "fall through to the agent."
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Verbs recognized by this parser. The full §5.10 pre-agent set:
#: approval shortcuts (``approve``/``deny`` — Task #41) plus the
#: session-control commands shipped in Task #52 (``new``/``status``/
#: ``clear``/``kill``/``override``).
RECOGNIZED_VERBS: frozenset[str] = frozenset(
    {"approve", "deny", "new", "status", "clear", "kill", "override"}
)

#: Regex for the canonical 32-lowercase-hex id shape used by BOTH approval
#: ids (``uuid.uuid4().hex`` per Task #39) AND delegation ids
#: (``uuid.uuid4().hex`` per Task #35 / Task #45). The parser uses this to
#: decide whether the first token after ``/approve`` / ``/deny`` is an id
#: or part of the reason. ``/kill`` uses :data:`DELEGATION_ID_RE` (the same
#: pattern, exposed under a separate name for caller clarity).
APPROVAL_ID_RE: re.Pattern[str] = re.compile(r"^[a-f0-9]{32}$")

#: Regex for the canonical delegation-id shape. Identical to
#: :data:`APPROVAL_ID_RE` — both are ``uuid.uuid4().hex``. Exposed under a
#: separate name so call-sites in the dispatcher (Task #52) can read
#: clearly. The dispatcher validates the user-supplied token against this
#: pattern before issuing a DB lookup.
DELEGATION_ID_RE: re.Pattern[str] = re.compile(r"^[a-f0-9]{32}$")

# Internal regex: matches the leading ``/<verb>`` token. Anchored at start
# of string after lstrip. Verb is captured case-insensitively; we lowercase
# before comparing against ``RECOGNIZED_VERBS``.
#
# The trailing ``(?:\s|$)`` boundary is critical — it rejects ``/approveall``
# and ``/approver`` (the verb must be followed by whitespace or end-of-line).
_LEAD_VERB_RE: re.Pattern[str] = re.compile(
    r"^/([A-Za-z]+)(?:\s|$)",
)

# Internal regex: collapses runs of whitespace (spaces, tabs, …) to a single
# space inside the reason. We've already truncated at the first newline, so
# ``\s`` is safe — it cannot eat a real newline boundary.
_WHITESPACE_RUN_RE: re.Pattern[str] = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Value-object
# ---------------------------------------------------------------------------


#: The verb Literal for :class:`SlashCommand`. Defined module-level so
#: downstream callers can ``from capo.transport.slash import SlashVerb``
#: when narrowing.
SlashVerb = Literal[
    "approve", "deny", "new", "status", "clear", "kill", "override"
]


class SlashCommand(BaseModel):
    """Parsed pre-agent slash command (§5.8 / §5.10).

    Frozen Pydantic value-object per Capo's project pattern (see
    execution_context.md "Code Style"): immutable, ``extra="forbid"`` so
    accidental field-name typos surface as validation errors instead of
    silent attribute creation.

    Attributes
    ----------
    verb
        Recognized command verb, normalized to lowercase. One of:
        ``"approve"``, ``"deny"``, ``"new"``, ``"status"``, ``"clear"``,
        ``"kill"``, ``"override"``. Other verbs never produce a
        :class:`SlashCommand` — :func:`parse_slash_command` returns
        ``None`` for unknown verbs.
    approval_id
        For ``approve`` / ``deny`` only. The approval id when the first
        token after the verb matches :data:`APPROVAL_ID_RE`, else
        ``None``. When ``None`` for ``approve`` / ``deny``, the dispatcher
        falls back to "most recent pending approval in the thread"
        (§5.8 Inbound Contract Addendum). Always ``None`` for non-approval
        verbs.
    delegation_id
        For ``kill`` only. Raw token passed after ``/kill``. ``None`` when
        the user omitted the argument (dispatcher replies "missing
        delegation id"). Note: this field is NOT validated for shape — the
        dispatcher inspects the token against :data:`DELEGATION_ID_RE` and
        emits the appropriate friendly error. Always ``None`` for other
        verbs.
    reason
        Free-form reason text (canonicalized whitespace) or ``None`` if no
        reason was supplied. Whitespace runs inside the reason are
        collapsed to a single space; leading/trailing whitespace is
        stripped. Used by ``approve`` / ``deny`` (operator commentary on
        the decision). For other verbs the field is always ``None`` —
        ``/new``, ``/status``, ``/clear``, ``/override`` accept no
        arguments at all and ``/kill`` accepts only an id.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    verb: SlashVerb
    approval_id: str | None = None
    delegation_id: str | None = None
    reason: str | None = None


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_slash_command(message_text: str) -> SlashCommand | None:
    """Parse ``message_text`` into a :class:`SlashCommand`, or ``None``.

    Recognized forms (case-insensitive on the verb)::

        /approve
        /approve <id>
        /approve <id> <reason text>
        /deny
        /deny <id>
        /deny <id> <reason text>
        /new
        /status
        /clear
        /kill <delegation_id>
        /override

    Returns ``None`` for any input that does not start with one of the
    recognized verbs. The dispatcher treats ``None`` as "fall through to
    the agent loop" — natural-language input is never mis-classified as a
    slash command.

    Parameters
    ----------
    message_text
        Raw inbound message text (typically ``AMCInboundEnvelope.text``).
        ``None`` and non-``str`` inputs are tolerated and treated as
        non-matches; callers that pass typed strings get the same answer.

    Returns
    -------
    SlashCommand | None
        A frozen value-object on match, ``None`` otherwise.

    Behavior notes
    --------------
    * **First line only.** Multi-line input is truncated at the first
      newline before parsing. ``"/approve <id>\\nmore"`` parses as
      ``/approve <id>`` with the second line discarded — this matches the
      §5.8 expectation that the slash command is a single-line shortcut.
    * **Leading/trailing whitespace** on the first line is stripped before
      the verb check, so ``"  /approve  "`` matches.
    * **Verb boundary.** The verb must be followed by whitespace or
      end-of-line. ``/approveall`` and ``/approver`` are NOT matches.
    * **Approval-id shape.** A token following the verb is treated as an
      id only when it matches ``^[a-f0-9]{32}$``. Anything else — uppercase
      hex, dashed UUID, plain English — is treated as the first word of
      the reason and ``approval_id`` is left ``None``.
    * **Reason canonicalization.** Internal whitespace runs collapse to a
      single space; leading/trailing whitespace is stripped. An empty or
      whitespace-only reason becomes ``None``.
    """

    # Tolerate non-str inputs so the dispatcher can call this with whatever
    # AMC handed it (defensive — the live envelope type is ``str``, but
    # narrowing here keeps the failure mode "no match" instead of
    # "TypeError in the inbound hot path").
    if not isinstance(message_text, str):
        return None

    # First-line-only rule. Truncate at the first newline (LF, CR, or CRLF
    # — splitlines handles all three) BEFORE trimming, so we never accept
    # multi-line bodies that "happen to start with" a slash command on
    # line 1 but have additional commands on line 2.
    first_line = message_text.splitlines()[0] if message_text else ""

    # Trim leading/trailing whitespace on the first line. The verb regex
    # is anchored at start-of-string, so we must remove the leading run
    # before running it.
    trimmed = first_line.strip()
    if not trimmed or not trimmed.startswith("/"):
        return None

    match = _LEAD_VERB_RE.match(trimmed)
    if match is None:
        return None

    verb_raw = match.group(1).lower()
    if verb_raw not in RECOGNIZED_VERBS:
        return None

    # Everything after the matched ``/<verb>`` (including the trailing
    # whitespace consumed by the boundary). ``.end()`` points one past the
    # boundary char — safe even if the verb hit end-of-line (then the
    # remainder is just an empty string).
    remainder = trimmed[match.end() :]

    # Dispatch on verb. Each branch is responsible for shaping the
    # remainder into the right fields on :class:`SlashCommand` and
    # returning a fully-constructed value-object.
    if verb_raw in ("approve", "deny"):
        approval_id, reason = _split_id_and_reason(remainder)
        verb_aod: Literal["approve", "deny"] = (
            "approve" if verb_raw == "approve" else "deny"
        )
        return SlashCommand(
            verb=verb_aod, approval_id=approval_id, reason=reason
        )

    if verb_raw == "kill":
        # ``/kill`` accepts a single positional ``delegation_id`` token.
        # The dispatcher decides what to do with mis-shaped or missing
        # tokens — the parser just surfaces them verbatim.
        delegation_id = _first_token_or_none(remainder)
        return SlashCommand(verb="kill", delegation_id=delegation_id)

    # ``/new``, ``/status``, ``/clear``, ``/override`` accept no
    # arguments. Per §5.10 they're zero-token shortcuts. Any extra tokens
    # after the verb are tolerated and ignored (e.g. trailing whitespace
    # / accidental "/status please") — the user clearly intended the
    # command. This matches the "case-insensitive verbs, simple usage"
    # spirit of §5.10.
    verb_simple: Literal["new", "status", "clear", "override"]
    if verb_raw == "new":
        verb_simple = "new"
    elif verb_raw == "status":
        verb_simple = "status"
    elif verb_raw == "clear":
        verb_simple = "clear"
    else:
        # ``override`` is the only remaining recognized verb.
        verb_simple = "override"
    return SlashCommand(verb=verb_simple)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _split_id_and_reason(remainder: str) -> tuple[str | None, str | None]:
    """Split the post-verb remainder into ``(approval_id, reason)``.

    Behavior:

    * Empty/whitespace-only remainder → ``(None, None)``.
    * First whitespace-delimited token matches :data:`APPROVAL_ID_RE` →
      that token is the id; the rest (canonicalized) is the reason, or
      ``None`` if empty.
    * Otherwise → ``(None, canonicalized_remainder)`` — the operator
      meant the whole tail as a reason. The dispatcher's "most recent
      pending approval in thread" fallback will pick up from here.

    Implementation note: ``str.split(maxsplit=1)`` separates the first
    token from the tail in O(n) without allocating intermediate lists for
    every word. The single-pass design keeps the parser linear in input
    size — important for the inbound hot path.
    """
    stripped = remainder.strip()
    if not stripped:
        return None, None

    parts = stripped.split(maxsplit=1)
    first_token = parts[0]
    tail = parts[1] if len(parts) == 2 else ""

    if APPROVAL_ID_RE.match(first_token):
        canonical_reason = _canonicalize_reason(tail)
        return first_token, canonical_reason

    # First token isn't an id → treat the whole remainder as the reason.
    canonical_reason = _canonicalize_reason(stripped)
    return None, canonical_reason


def _canonicalize_reason(text: str) -> str | None:
    """Strip and collapse internal whitespace; return ``None`` if empty."""
    if not text:
        return None
    collapsed = _WHITESPACE_RUN_RE.sub(" ", text).strip()
    return collapsed or None


def _first_token_or_none(remainder: str) -> str | None:
    """Return the first whitespace-delimited token of ``remainder`` or ``None``.

    Used by the ``/kill <delegation_id>`` branch — we surface the raw
    user-supplied token verbatim (no shape validation here). The
    dispatcher applies :data:`DELEGATION_ID_RE` to decide between
    "missing delegation id" (``None``) and "unknown delegation"
    (a non-hex / unknown id).
    """
    stripped = remainder.strip()
    if not stripped:
        return None
    return stripped.split(maxsplit=1)[0]


__all__ = [
    "APPROVAL_ID_RE",
    "DELEGATION_ID_RE",
    "RECOGNIZED_VERBS",
    "SlashCommand",
    "SlashVerb",
    "parse_slash_command",
]
