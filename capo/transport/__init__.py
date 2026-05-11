"""Capo transport subsystem: AMC listener, dispatcher, REST client, and
sender→user_id resolution.

Currently landed:

- :func:`user_resolver.resolve_user_id` — pure function mapping AMC envelope
  sender ids to internal ``user_id`` values from
  :class:`capo.config.UsersConfig`. Used by the dispatcher (Task #11) to
  partition every domain row by ``user_id`` per spec §5.11.
- :class:`amc_client.AMCClient` — async REST wrapper for the three Capo-facing
  AMC endpoints (``GET /messages/unread``, ``POST /messages/send``,
  ``POST /messages/mark_read``) per spec §5.2 + §7.5. Owns the typed error
  hierarchy and the §7.5 retry policy (``RATE_LIMITED`` + transient 5xx
  honor ``Retry-After``; ``PLATFORM_AUTH`` / ``CHANNEL_NOT_FOUND`` /
  ``ATTACHMENT_TOO_LARGE`` / ``VALIDATION_ERROR`` raise on first contact).

- :class:`dispatcher.Dispatcher` + :class:`dispatcher.ChannelWorker` —
  per-``channel_id`` asyncio queue worker that runs the §5.2 turn pipeline
  (resolve user, agent.run, amc.send, amc.mark_read) and handles the §7.5
  error contract (PLATFORM_AUTH, CHANNEL_NOT_FOUND, etc.).

Pending tasks (intentionally NOT re-exported here yet to keep import-time
surface minimal):

- ``amc_listener`` — FastAPI webhook receiver (Task #10).
"""

from capo.transport.amc_client import (
    AMCClient,
    AMCError,
    AMCInboundEnvelope,
    AMCInternalError,
    AttachmentTooLarge,
    ChannelNotFound,
    MarkReadResult,
    PlatformAuth,
    RateLimited,
    SendResult,
    ValidationError,
)
from capo.transport.dispatcher import ChannelWorker, Dispatcher
from capo.transport.user_resolver import (
    UNKNOWN_SENDER,
    resolve_user_id,
)

__all__ = [
    "AMCClient",
    "AMCError",
    "AMCInboundEnvelope",
    "AMCInternalError",
    "AttachmentTooLarge",
    "ChannelNotFound",
    "ChannelWorker",
    "Dispatcher",
    "MarkReadResult",
    "PlatformAuth",
    "RateLimited",
    "SendResult",
    "UNKNOWN_SENDER",
    "ValidationError",
    "resolve_user_id",
]
