"""Basic (non-coding) agent tools: ``web_search``, ``fetch_url``, ``shell_exec``.

These are the Phase-1 tools required by spec §5.1 / §9.1, plus the Phase-2
``shell_exec`` tool (spec §6.2, task #25) which lands an allowlist-enforced
subprocess runner. The actual approval workflow that satisfies
``ApprovalRequired`` lands in Phase 4 (§5.8) — Phase 2 just raises.

Design contract
---------------

* **Both tools take ``ctx: RunContext[CapoDeps]`` first.** Pydantic AI
  injects ``ctx.deps`` automatically from the ``deps=`` kwarg of
  ``agent.run``.
* **Errors are returned, not raised.** Per spec §5.1 acceptance criteria
  for the agent loop — tool exceptions must not crash the loop. Recoverable
  conditions (non-2xx, body cap exceeded, missing provider) return a
  structured :class:`~capo.deps.FetchError` or raise :class:`ModelRetry`
  to give the LLM a chance to fix its input. Only programmer errors and
  totally unexpected exceptions bubble up — and even those are caught +
  logged by the registration wrapper so the agent loop survives.
* **SOUL anchoring.** Tool docstrings MUST NOT contain SOUL content; SOUL
  is anchored in :class:`pydantic_ai.Agent`'s ``instructions=`` only
  (spec §5.1). Tests assert this anchoring by scanning rendered tool
  descriptions.
"""

from __future__ import annotations

import inspect
import logging
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict
from pydantic_ai import ModelRetry, RunContext

from capo.deps import CapoDeps, FetchError, SearchResult
from capo.tools._approval import (
    REQUEST_TYPE_SHELL_EXEC,
    ApprovalRejected,
    ApprovalRequired,
    ApprovalUnavailableError,
    new_approval_id,
    request_tool_approval,
    utc_iso_now,
)

if TYPE_CHECKING:  # pragma: no cover - imported only for type hints
    from pydantic_ai import Agent

    from capo.workflows.approval import ApprovalDecision

logger = logging.getLogger("capo.tools.basic")

# ---------------------------------------------------------------------------
# Constants — keep behavior reviewable from this one place.
# ---------------------------------------------------------------------------

#: Hard cap on response body bytes ``fetch_url`` will buffer. 1 MiB keeps the
#: agent context window safe and prevents accidental DoS via huge HTML
#: pages. Larger fetches return a structured ``body_too_large`` error.
FETCH_URL_MAX_BYTES: int = 1024 * 1024  # 1 MiB

#: Per-request timeout for ``fetch_url`` in seconds. Independent from the
#: AMC client deadline — agent tool calls should fail fast.
FETCH_URL_TIMEOUT_S: float = 15.0

#: Default timeout for ``shell_exec`` in seconds. Allowlisted commands are
#: expected to be quick (e.g. ``git status``, ``ls``); anything longer is a
#: smell and should fail fast rather than block the agent loop.
SHELL_EXEC_TIMEOUT_S: float = 30.0

#: Hard cap on captured stdout/stderr from ``shell_exec`` (per stream).
#: 64 KiB each keeps the agent context window safe; over-cap output is
#: truncated with a clear marker rather than dropped silently.
SHELL_EXEC_OUTPUT_MAX_BYTES: int = 64 * 1024  # 64 KiB

#: Shell metacharacters that must not appear in any ``shell_exec`` command.
#: Presence of any of these (even inside what looks like a quoted arg) is
#: treated as an attempt to escape the argv contract; we route to approval.
#: We intentionally match the raw command string AND the tokenized argv so
#: tricks like ``git log '; rm -rf /'`` are caught — the tokens would look
#: benign but the raw string would still contain ``;``.
_SHELL_METACHARACTERS: tuple[str, ...] = (
    ";",
    "&&",
    "||",
    "|",
    "`",
    "$(",
    ">",
    "<",
)


# ---------------------------------------------------------------------------
# shell_exec — types + exception.
# ---------------------------------------------------------------------------


class ShellResult(BaseModel):
    """Structured result returned by :func:`shell_exec`.

    ``stdout`` / ``stderr`` are decoded UTF-8 with ``errors="replace"`` and
    are each capped at :data:`SHELL_EXEC_OUTPUT_MAX_BYTES`. When a stream
    is truncated, a trailing marker is appended so the agent can see that
    output was elided (rather than silently losing context).
    """

    model_config = ConfigDict(frozen=True)

    exit_code: int
    """Process exit code. ``-1`` indicates an internal timeout kill."""

    stdout: str
    """Captured stdout (UTF-8 ``replace``-decoded, byte-capped)."""

    stderr: str
    """Captured stderr (UTF-8 ``replace``-decoded, byte-capped)."""

    runtime_seconds: float
    """Wall-clock runtime in seconds."""


# ``ApprovalRequired``, ``ApprovalRejected`` and ``ApprovalUnavailableError``
# now live in :mod:`capo.tools._approval` so the delegation tools
# (Task #43) can share them. They are re-exported here at module level
# for backwards compatibility with callers / tests that imported them
# from ``capo.tools.basic``.


# ---------------------------------------------------------------------------
# Tool implementations.
#
# These are module-level functions (NOT methods) so they can be unit-tested
# in isolation without spinning up an Agent. ``register_basic_tools`` below
# wires them onto an Agent via ``@agent.tool``.
# ---------------------------------------------------------------------------


async def web_search(
    ctx: RunContext[CapoDeps], query: str
) -> list[SearchResult]:
    """Search the public web and return up to a handful of result snippets.

    Args:
        query: A non-empty natural-language search query.

    Returns:
        A list of ``SearchResult`` items (possibly empty if the provider
        returns no hits). The list is the canonical agent-visible result;
        backend-specific metadata is intentionally stripped.

    Raises:
        ModelRetry: If ``query`` is empty/whitespace, telling the LLM to
            retry with a real query. If no provider is configured, the
            same exception fires with a clear "no provider configured"
            message so the agent surfaces it to the user instead of
            silently returning ``[]``.
    """
    if not query or not query.strip():
        raise ModelRetry("web_search: query is required (non-empty string)")

    client = ctx.deps.web_search_client
    if client is None:
        raise ModelRetry(
            "web_search: no web-search provider configured "
            "(deps.web_search_client is None)"
        )

    result = client.search(query.strip())
    if inspect.isawaitable(result):
        result = await result
    return list(result)


async def fetch_url(
    ctx: RunContext[CapoDeps], url: str
) -> str | FetchError:
    """Fetch the body of ``url`` over HTTPS/HTTP and return it as text.

    Caps the response body at :data:`FETCH_URL_MAX_BYTES` and the request
    at :data:`FETCH_URL_TIMEOUT_S` seconds. Non-2xx responses and
    over-cap bodies return a structured :class:`FetchError` so the agent
    can reason about them in-band instead of treating them as exceptions.

    Args:
        url: An absolute http(s) URL to fetch.

    Returns:
        The response body as a UTF-8 string (decoded with ``replace``
        errors so binary noise can't crash the tool), OR a ``FetchError``
        describing why the fetch failed (non-2xx, body cap, etc.).

    Raises:
        ModelRetry: If ``url`` is empty / not a string. Real network
            failures (DNS, TLS, refused) are caught and returned as a
            ``FetchError`` with ``error="http_error"`` and ``status=None``
            so the agent loop survives transient outages.
    """
    if not url or not isinstance(url, str) or not url.strip():
        raise ModelRetry("fetch_url: url is required (absolute http(s) URL)")

    client = ctx.deps.http_client
    try:
        # ``stream=True`` lets us enforce the size cap without materializing
        # the entire body when the server is hostile / huge.
        async with client.stream(
            "GET",
            url,
            timeout=FETCH_URL_TIMEOUT_S,
            follow_redirects=True,
        ) as response:
            if not (200 <= response.status_code < 300):
                # Read body up to the cap for diagnostics, but don't return
                # it as success content.
                await response.aread()
                return FetchError(
                    error="http_error",
                    status=response.status_code,
                    url=url,
                    detail=f"non-2xx response: {response.status_code}",
                )

            buf = bytearray()
            async for chunk in response.aiter_bytes():
                buf.extend(chunk)
                if len(buf) > FETCH_URL_MAX_BYTES:
                    return FetchError(
                        error="body_too_large",
                        status=response.status_code,
                        url=url,
                        detail=(
                            f"response body exceeded {FETCH_URL_MAX_BYTES} bytes"
                        ),
                    )
        return bytes(buf).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - intentional broad catch
        # Network errors (DNS, TLS, timeouts, refused) all surface here.
        # We log them so operators can see them, and return a structured
        # error so the agent loop doesn't crash.
        logger.warning(
            "fetch_url failed: url=%s err=%s",
            url,
            exc.__class__.__name__,
        )
        return FetchError(
            error="http_error",
            status=None,
            url=url,
            detail=f"{exc.__class__.__name__}: {exc}",
        )


# ---------------------------------------------------------------------------
# shell_exec — allowlist-enforced subprocess runner (spec §6.2 / §5.8).
# ---------------------------------------------------------------------------


def _truncate_stream(data: str, max_bytes: int) -> str:
    """Return ``data`` capped at ``max_bytes`` UTF-8 bytes with a marker.

    The cap is applied on the encoded byte length (not character count) so
    the reported size matches what the spec promises. A short trailing
    marker is appended on truncation so the agent doesn't silently lose
    context.
    """
    encoded = data.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return data
    truncated = encoded[:max_bytes].decode("utf-8", errors="replace")
    return (
        f"{truncated}\n[... output truncated at {max_bytes} bytes ...]"
    )


def _resolve_cwd(
    command: str,
    cwd: str | None,
    *,
    projects_root: Path | None,
    workspaces_root: Path | None,
) -> Path:
    """Resolve ``cwd`` to an absolute path within the allowed roots.

    Raises:
        ApprovalRequired: If the resolved cwd is not within
            ``projects_root`` or ``workspaces_root``.
        ValueError: If neither root is configured (programmer error — the
            dispatcher must hand a populated ``CapoDeps`` to tools).
    """
    if projects_root is None and workspaces_root is None:
        raise ValueError(
            "shell_exec: neither projects_root nor workspaces_root is "
            "configured on CapoDeps; refusing to run."
        )

    if cwd is None:
        # Default to projects_root (per task spec). If it isn't configured,
        # fall back to workspaces_root so the tool stays usable.
        target = projects_root if projects_root is not None else workspaces_root
        assert target is not None  # narrowed by the guard above.
        return target.resolve()

    resolved = Path(cwd).expanduser().resolve()
    roots: list[Path] = []
    if projects_root is not None:
        roots.append(projects_root.resolve())
    if workspaces_root is not None:
        roots.append(workspaces_root.resolve())

    for root in roots:
        if resolved == root or resolved.is_relative_to(root):
            return resolved

    raise ApprovalRequired(
        command,
        f"cwd {str(resolved)!r} is outside projects_root/workspaces_root",
    )


@dataclass(frozen=True)
class _ShellPrecheck:
    """Internal result of the synchronous shell_exec pre-check phase.

    ``raw`` and ``argv`` are the validated original command + tokens.
    ``resolved_cwd`` is the path the command would run in. ``allowlisted``
    is ``True`` when the binary is in ``settings.shell.allowlist``; ``False``
    means the caller MUST route through the approval workflow before
    executing.
    """

    raw: str
    argv: list[str]
    resolved_cwd: Path
    allowlisted: bool


def _shell_exec_precheck(
    ctx: RunContext[CapoDeps],
    command: str,
    cwd: str | None,
) -> _ShellPrecheck:
    """Run the synchronous structural checks for ``shell_exec``.

    Raises :class:`ApprovalRequired` for hard rejections that the approval
    workflow itself would never accept: shell metacharacters, tokenization
    errors, ``sudo``, cwd outside ``projects_root`` / ``workspaces_root``.

    Returns ``_ShellPrecheck`` with ``allowlisted=False`` when the binary
    is NOT in the configured allowlist — caller decides whether to invoke
    the approval workflow or fall back to ``ApprovalRequired``.
    """
    # --- Empty-command guard (clear ValueError per task spec).
    if not command or not command.strip():
        raise ValueError("shell_exec: command is required (non-empty string)")

    raw = command.strip()

    # --- Metacharacter scan on the RAW command string. We check the raw
    # string (not just the tokenized argv) so escape attempts inside what
    # looks like a quoted arg — e.g. ``git log '; rm -rf /'`` — still trip
    # the guard. shlex.split would happily turn that into two clean tokens.
    for meta in _SHELL_METACHARACTERS:
        if meta in raw:
            raise ApprovalRequired(
                raw,
                f"command contains forbidden shell metacharacter {meta!r}",
            )

    # --- Tokenize.
    try:
        argv = shlex.split(raw)
    except ValueError as exc:
        # Unbalanced quotes etc. — treat as non-approvable; the caller can
        # retry with a fixed string.
        raise ApprovalRequired(
            raw, f"could not tokenize command: {exc}"
        ) from exc

    if not argv:
        # ``shlex.split('   ')`` returns ``[]``; mirrors the empty-command
        # guard above for paranoia.
        raise ValueError("shell_exec: command is required (non-empty string)")

    # --- Per-token metachar guard (belt + braces — catches anything the
    # raw-string scan missed for quoted variants like ``"foo;bar"``).
    for tok in argv:
        for meta in _SHELL_METACHARACTERS:
            if meta in tok:
                raise ApprovalRequired(
                    raw,
                    f"argument {tok!r} contains forbidden shell "
                    f"metacharacter {meta!r}",
                )

    # --- sudo guard (special-cased because it's an allowlist-bypass
    # vector even when the binary itself isn't ``sudo`` — e.g. ``git`` is
    # allowlisted but ``sudo git`` is not).
    if argv[0] == "sudo" or "sudo" in argv:
        raise ApprovalRequired(raw, "sudo is not permitted")

    # --- cwd scoping (path-scope is also a structural check; even if a
    # human approves a non-allowlisted binary they can't approve running
    # outside the configured roots).
    resolved_cwd = _resolve_cwd(
        raw,
        cwd,
        projects_root=ctx.deps.projects_root,
        workspaces_root=ctx.deps.workspaces_root,
    )

    # --- Allowlist check (soft — caller routes through approval workflow
    # if not allowlisted).
    allowlist = set(ctx.deps.settings.shell.allowlist)
    allowlisted = argv[0] in allowlist
    return _ShellPrecheck(
        raw=raw, argv=argv, resolved_cwd=resolved_cwd, allowlisted=allowlisted
    )


def _execute_command(raw: str, argv: list[str], resolved_cwd: Path) -> ShellResult:
    """Execute the already-validated argv and return a :class:`ShellResult`.

    Shared between the allowlisted path and the post-approval path —
    ``shell=False``, byte-capped stdout/stderr, internal-timeout returns
    ``exit_code=-1`` (not an exception) so the agent loop stays alive.
    """
    start = time.monotonic()
    try:
        completed = subprocess.run(  # noqa: S603 - argv form, allowlisted binary
            argv,
            shell=False,
            capture_output=True,
            timeout=SHELL_EXEC_TIMEOUT_S,
            cwd=str(resolved_cwd),
            text=True,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = time.monotonic() - start
        partial_stdout = exc.stdout if isinstance(exc.stdout, str) else (
            exc.stdout.decode("utf-8", errors="replace")
            if isinstance(exc.stdout, (bytes, bytearray))
            else ""
        )
        partial_stderr = exc.stderr if isinstance(exc.stderr, str) else (
            exc.stderr.decode("utf-8", errors="replace")
            if isinstance(exc.stderr, (bytes, bytearray))
            else ""
        )
        timeout_marker = (
            f"\n[shell_exec: command timed out after "
            f"{SHELL_EXEC_TIMEOUT_S}s and was killed]"
        )
        logger.warning(
            "shell_exec timeout: cmd=%r cwd=%s elapsed=%.2fs",
            raw,
            str(resolved_cwd),
            elapsed,
        )
        return ShellResult(
            exit_code=-1,
            stdout=_truncate_stream(partial_stdout, SHELL_EXEC_OUTPUT_MAX_BYTES),
            stderr=_truncate_stream(
                (partial_stderr or "") + timeout_marker,
                SHELL_EXEC_OUTPUT_MAX_BYTES,
            ),
            runtime_seconds=elapsed,
        )

    elapsed = time.monotonic() - start
    return ShellResult(
        exit_code=completed.returncode,
        stdout=_truncate_stream(
            completed.stdout or "", SHELL_EXEC_OUTPUT_MAX_BYTES
        ),
        stderr=_truncate_stream(
            completed.stderr or "", SHELL_EXEC_OUTPUT_MAX_BYTES
        ),
        runtime_seconds=elapsed,
    )


async def _request_shell_approval(
    ctx: RunContext[CapoDeps],
    pre: _ShellPrecheck,
    cwd: str | None,
) -> ApprovalDecision:
    """Invoke the §5.8 approval workflow for a non-allowlisted command.

    Thin wrapper around :func:`capo.tools._approval.request_tool_approval`
    that assembles the shell-specific ``request_payload``. Raises
    :class:`ApprovalUnavailableError` (caught by :func:`shell_exec`)
    when the approval workflow can't be reached — missing ``state.db``
    path, DBOS not launched, or import failure.
    """
    # Caller-supplied deterministic inputs — generated BEFORE the call so a
    # workflow replay observes the same approval_id + requested_at.
    approval_id = new_approval_id()
    requested_at = utc_iso_now()
    request_payload: dict[str, Any] = {
        "command": pre.raw,
        "args": list(pre.argv),
        "cwd": cwd if cwd is not None else str(pre.resolved_cwd),
    }
    return await request_tool_approval(
        ctx.deps,
        request_type=REQUEST_TYPE_SHELL_EXEC,
        request_payload=request_payload,
        approval_id=approval_id,
        requested_at=requested_at,
        log_subject=f"shell_exec {pre.raw!r}",
    )


async def shell_exec(
    ctx: RunContext[CapoDeps],
    command: str,
    cwd: str | None = None,
) -> ShellResult:
    """Execute a shell command, gating non-allowlisted commands behind §5.8.

    Behavior (spec §6.2, §5.8, §5.9, §7.5; Task #42):

    * Empty / whitespace command → :class:`ValueError`.
    * Shell metacharacters in the raw command string (``;``, ``&&``,
      ``||``, ``|``, backticks, ``$(``, ``>``, ``<``), tokenization
      errors, ``sudo``, and cwd outside ``projects_root`` ∪
      ``workspaces_root`` → :class:`ApprovalRequired` (these are
      structural argv-contract violations, not human-reviewable policy
      decisions).
    * If the binary IS in ``settings.shell.allowlist``: the command runs
      directly with ``shell=False``; ``cwd=None`` defaults to
      ``projects_root``; output is byte-capped per stream; internal
      timeouts return ``exit_code=-1`` (in-band, not an exception).
    * If the binary is NOT allowlisted: the §5.8 approval workflow is
      invoked. ``approval_id`` is freshly generated via
      :func:`uuid.uuid4.hex`; ``requested_at`` is the caller's UTC ISO
      timestamp (``%Y-%m-%dT%H:%M:%SZ``); ``request_type='shell_exec'``;
      ``request_payload={"command": cmd, "args": argv, "cwd": cwd}``;
      ``requester_user_id`` and ``parent_thread_id`` populated from
      :class:`CapoDeps`.
      * On ``decision.status == "approved"``: the original command
        executes via the same code path as allowlisted commands.
      * On ``decision.status in {"denied", "expired", "cancelled"}``:
        :class:`ApprovalRejected` is raised carrying the
        :class:`~capo.workflows.approval.ApprovalDecision`.
    * If the approval-workflow infrastructure is genuinely unavailable
      (DBOS not launched, approval module not importable, ``state.db``
      not configured): falls back to :class:`ApprovalRequired` for
      backwards compatibility with unit tests and degraded environments.

    Args:
        command: The full command line (e.g. ``"git status"``). Tokenized
            via :func:`shlex.split`.
        cwd: Optional working directory. Must resolve to a path within
            ``projects_root`` or ``workspaces_root``. ``None`` defaults
            to ``projects_root``.

    Returns:
        :class:`ShellResult` with the exit code, captured streams, and
        wall-clock runtime.

    Raises:
        ValueError: If ``command`` is empty / whitespace-only.
        ApprovalRequired: Structural rejections OR approval-workflow
            unavailable.
        ApprovalRejected: Approval workflow ran and returned
            ``denied``/``expired``/``cancelled``.
    """
    pre = _shell_exec_precheck(ctx, command, cwd)

    if pre.allowlisted:
        return _execute_command(pre.raw, pre.argv, pre.resolved_cwd)

    # Non-allowlisted — route through the approval workflow.
    try:
        decision = await _request_shell_approval(ctx, pre, cwd)
    except ApprovalUnavailableError as exc:
        # Fall back to the prior ``ApprovalRequired`` behavior so callers in
        # non-DBOS environments (unit tests, smoke runs) still get a
        # well-typed signal instead of an opaque RuntimeError.
        allowlist = sorted(set(ctx.deps.settings.shell.allowlist))
        raise ApprovalRequired(
            pre.raw,
            (
                f"binary {pre.argv[0]!r} is not in the shell allowlist "
                f"({allowlist}) and approval workflow is unavailable "
                f"({exc.reason})"
            ),
        ) from exc

    if decision.status == "approved":
        logger.info(
            "shell_exec: approval %s approved by %s — executing %r",
            decision.approval_id,
            decision.resolved_by,
            pre.raw,
            extra={
                "event": "capo.tools.basic.shell_exec.approved",
                "approval_id": decision.approval_id,
                "resolved_by": decision.resolved_by,
            },
        )
        return _execute_command(pre.raw, pre.argv, pre.resolved_cwd)

    # denied / expired / cancelled — typed error for the agent loop.
    logger.info(
        "shell_exec: approval %s resolved %s by %s (reason=%r)",
        decision.approval_id,
        decision.status,
        decision.resolved_by,
        decision.reason,
        extra={
            "event": "capo.tools.basic.shell_exec.rejected",
            "approval_id": decision.approval_id,
            "status": decision.status,
            "resolved_by": decision.resolved_by,
        },
    )
    raise ApprovalRejected(decision)


# ---------------------------------------------------------------------------
# Registration helper — called from build_agent.
# ---------------------------------------------------------------------------


def register_basic_tools(agent: Agent[CapoDeps, Any]) -> None:
    """Register :func:`web_search` and :func:`fetch_url` onto ``agent``.

    Kept as a free function (not a method on ``Agent``) so:

    * Phase-1 ``build_agent`` can call it unconditionally.
    * Tests can build a real :class:`pydantic_ai.Agent`, call this helper,
      and then introspect ``agent.toolsets`` / tool descriptions to verify
      registration and SOUL-anchoring.

    The decorators preserve the underlying functions, so callers can still
    invoke ``web_search`` / ``fetch_url`` directly in unit tests by
    constructing a ``RunContext`` manually.
    """
    agent.tool(web_search)
    agent.tool(fetch_url)


__all__ = [
    "FETCH_URL_MAX_BYTES",
    "FETCH_URL_TIMEOUT_S",
    "SHELL_EXEC_OUTPUT_MAX_BYTES",
    "SHELL_EXEC_TIMEOUT_S",
    "ApprovalRejected",
    "ApprovalRequired",
    "ShellResult",
    "fetch_url",
    "register_basic_tools",
    "shell_exec",
    "web_search",
]
