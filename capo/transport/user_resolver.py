"""AMC sender → internal ``user_id`` resolver (spec §5.11, §15.3).

The dispatcher (Task #11) calls :func:`resolve_user_id` for every inbound
envelope BEFORE invoking the agent. The resolved ``user_id`` is what gets
written to every domain table (``conversation_history``, ``delegations``,
``sessions``, ``approvals``, ``daily_costs``), satisfying the §5.11
acceptance criterion that records are partitioned by ``user_id``.

Design notes
------------

* This module is a pure-function helper — no logging side-effects, no I/O.
  The dispatcher owns the "log a warning and drop the envelope" policy for
  unknown senders (see :func:`format_unknown_sender_log_record` for the
  canonical log message format the dispatcher should emit).
* The mapping itself is built once at Settings load and exposed as
  :attr:`capo.config.Settings.sender_to_user_id` (a flat ``dict[str, str]``).
  Adding more users is a config edit + restart (V1 invariant per §5.11).
* Two senders mapping to the same ``user_id`` is intended (shared inbox /
  cross-platform identity); both route to the same conversation history.
* The empty-``[users]`` edge case is rejected at Settings load via
  :class:`capo.config.UsersConfig`, so callers can assume the mapping is
  non-empty.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from capo.config import Settings

#: Sentinel returned by :func:`resolve_user_id` when no mapping exists. The
#: dispatcher checks for ``None`` and rejects the envelope with a logged
#: warning; this constant is exported only so call-sites can ``is`` compare
#: when the sentinel needs to be passed through additional layers.
UNKNOWN_SENDER: Final[None] = None


def resolve_user_id(sender_id: str, settings: Settings) -> str | None:
    """Resolve an AMC envelope ``sender_id`` to its internal ``user_id``.

    Parameters
    ----------
    sender_id:
        The AMC envelope ``sender_id`` field — e.g. ``"+15551234567"`` for an
        iMessage envelope or ``"discord:user:123"`` for a Discord envelope.
        Compared verbatim against the values in each
        ``[users.<user_id>].amc_senders`` list.
    settings:
        Loaded :class:`capo.config.Settings`. The lookup uses
        :attr:`Settings.sender_to_user_id`, which is the canonical
        sender → ``user_id`` mapping built from the ``[users]`` table.

    Returns
    -------
    str | None
        The matching ``user_id`` (the literal TOML key after ``[users.``)
        if ``sender_id`` is mapped, otherwise ``None`` (= :data:`UNKNOWN_SENDER`).
        The dispatcher is expected to log a warning and drop the envelope
        when ``None`` is returned (spec §5.11: "Unknown senders are rejected
        at the dispatcher").

    Notes
    -----
    Pure function — no logging, no side-effects. This keeps the helper
    trivially unit-testable and lets the dispatcher own the rejection
    policy (warning level, log fields, metric increment, etc.).
    """
    if not sender_id:
        return None
    mapping = settings.sender_to_user_id
    return mapping.get(sender_id)


def format_unknown_sender_log_record(
    sender_id: str, *, delivery_id: str | None = None
) -> dict[str, str]:
    """Return a structured log record for an unknown-sender rejection.

    Centralizes the field names so the dispatcher (Task #11) and any future
    metrics emitter use the same keys. The dispatcher is expected to log
    this at ``WARNING`` level under the ``capo.transport.dispatcher`` logger
    and drop the envelope without writing any domain row (spec §5.11).
    """
    record: dict[str, str] = {
        "event": "amc.envelope.rejected.unknown_sender",
        "sender_id": sender_id,
    }
    if delivery_id is not None:
        record["delivery_id"] = delivery_id
    return record
