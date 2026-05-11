"""Capo runtime configuration (Pydantic Settings).

Loads and validates ``config.toml`` (per spec §15.3) and ``.env`` (secrets only,
per spec §6.2). Validation runs at boot and fails fast with a clear,
single-line error message — never a stack trace — when misconfigured.

Design notes:

* TOML is parsed with the stdlib ``tomllib`` (Python 3.11+); we add no new
  third-party deps.
* Secrets (``AMC_WEBHOOK_SECRET``, ``AMC_BEARER_TOKEN``, and the various model
  API keys) are typed as :class:`pydantic.SecretStr` so they are redacted in
  ``repr``/``str`` and from logger output by default.
* Unknown TOML keys are *warned* (via :mod:`logging`) rather than silently
  dropped — surfacing typos without breaking forward-compat.
* Errors include the offending key path (``amc.listen_host``, ``soul.dir``,
  etc.) so misconfigs are debuggable from the error alone.

The public entry point is :meth:`Settings.load`. ``capo.main`` calls it on
boot and exits 2 with the error's message on any :class:`ConfigError` or
:class:`FileNotFoundError`.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import tomllib
from pathlib import Path
from typing import Any

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("capo.config")

# Loopback hosts that do not require TLS for the AMC listener. Anything else
# triggers the TLS-required validator (see :class:`AmcConfig`).
_LOOPBACK_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "::1", "localhost"})


class ConfigError(Exception):
    """Raised when configuration loading or validation fails.

    The message is intended to be printed verbatim to stderr by the
    entry-point; it always includes either an offending key path or the
    file path that could not be loaded.
    """


# ---------------------------------------------------------------------------
# Sub-models — one per table in §15.3.
# ---------------------------------------------------------------------------


class _StrictModel(BaseModel):
    """Base for sub-models: forbid extras so unknown keys are detected.

    Unknown keys are not silently dropped — we strip them at the loader layer
    after emitting a ``logging.warning`` so the user sees their typo.
    """

    model_config = {"extra": "forbid"}


class ModelsSubagentsConfig(_StrictModel):
    """``[models.subagents]`` — model IDs used when spawning subagents."""

    claude_code: str
    codex: str


class ModelsConfig(_StrictModel):
    """``[models]`` — Pydantic AI model IDs for the router / default / heavy
    paths plus the subagent table."""

    default: str
    router: str
    heavy: str
    subagents: ModelsSubagentsConfig


class PathsConfig(_StrictModel):
    """``[paths]`` — on-disk locations. ``~`` is expanded; paths are not
    required to exist at validation time (they may be created at boot)."""

    workspaces_root: Path
    projects_root: Path
    db_path: Path
    dbos_db_path: Path

    @field_validator("workspaces_root", "projects_root", "db_path", "dbos_db_path")
    @classmethod
    def _expand(cls, v: Path) -> Path:
        return Path(str(v)).expanduser()


class SoulConfig(_StrictModel):
    """``[soul]`` — soul prompt selection. ``dir`` may be relative; resolved
    against the config file's parent directory by :meth:`Settings.load`."""

    active: str
    dir: Path

    @field_validator("dir")
    @classmethod
    def _expand(cls, v: Path) -> Path:
        return Path(str(v)).expanduser()


class AmcConfig(_StrictModel):
    """``[amc]`` — AMC transport. ``listen_host`` off-loopback requires
    ``tls_cert`` and ``tls_key`` (per spec §6.2 — TLS becomes mandatory if the
    listener ever binds off-loopback)."""

    base_url: str
    agent_id: str
    listen_host: str = "127.0.0.1"
    listen_port: int = Field(default=8090, ge=1, le=65535)
    max_boot_wait_seconds: int = Field(default=60, ge=0)
    tls_cert: Path | None = None
    tls_key: Path | None = None

    @field_validator("listen_host")
    @classmethod
    def _host_non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("amc.listen_host must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def _tls_required_off_loopback(self) -> AmcConfig:
        if _is_loopback(self.listen_host):
            return self
        if self.tls_cert is None or self.tls_key is None:
            raise ValueError(
                f"amc.listen_host={self.listen_host!r} is not loopback; "
                "amc.tls_cert and amc.tls_key are required when binding "
                "off-loopback (spec §6.2)."
            )
        return self


class AgentBinaryConfig(_StrictModel):
    """Per-agent block under ``[agents.<name>]``. Extra keys are agent-specific
    (e.g. ``default_permission_mode``, ``worktree_base_branch``,
    ``default_sandbox``); we accept any string-valued options here."""

    model_config = {"extra": "allow"}

    binary: str


class AgentsConfig(_StrictModel):
    """``[agents.claude_code]`` and ``[agents.codex]`` blocks."""

    claude_code: AgentBinaryConfig
    codex: AgentBinaryConfig


class BudgetConfig(_StrictModel):
    """``[budget]`` — soft / hard daily caps in USD."""

    soft_daily_usd: float = Field(ge=0)
    hard_daily_usd: float = Field(ge=0)
    notify_channel: str

    @model_validator(mode="after")
    def _hard_gte_soft(self) -> BudgetConfig:
        if self.hard_daily_usd < self.soft_daily_usd:
            raise ValueError(
                "budget.hard_daily_usd must be >= budget.soft_daily_usd "
                f"(got hard={self.hard_daily_usd}, soft={self.soft_daily_usd})"
            )
        return self


class ShellConfig(_StrictModel):
    """``[shell]`` — allowlist for the ``shell_exec`` tool (spec §6.2)."""

    allowlist: list[str]

    @field_validator("allowlist")
    @classmethod
    def _nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("shell.allowlist must contain at least one entry")
        for cmd in v:
            if not cmd or any(c.isspace() for c in cmd):
                raise ValueError(
                    f"shell.allowlist entries must be non-empty single tokens "
                    f"(got {cmd!r})"
                )
        return v


class ConcurrencyConfig(_StrictModel):
    """``[concurrency]`` — caps on parallel delegations and per-channel queue depth.

    ``queue_depth_max`` bounds each per-``channel_id`` dispatcher worker's
    asyncio queue (spec §5.2, §5.12, §15.3). Default 100 per channel — keeps
    a single channel's burst from exhausting memory while still tolerating
    AMC's ~13-minute retry window on a temporary outage.
    """

    max_delegations: int = Field(ge=1)
    queue_depth_max: int = Field(default=100, ge=1)


class RetentionConfig(_StrictModel):
    """``[retention]`` — how long to keep delegation output rows.

    Task #55 (§8.1, §8.4): a nightly maintenance job prunes
    ``delegation_output`` rows older than ``delegation_output_days`` whose
    parent delegation is in a terminal state. ``run_hour_local`` controls
    the wake time (24h clock, local TZ); ``vacuum_after_prune`` is an
    opt-in ``VACUUM`` after each successful prune.
    """

    delegation_output_days: int = Field(default=14, ge=0)
    run_hour_local: int = Field(default=3, ge=0, le=23)
    vacuum_after_prune: bool = False


class CompactionConfig(_StrictModel):
    """``[compaction]`` — conversation compaction thresholds (spec §5.7, §15.3).

    Hybrid compaction (Task #54): when a thread's history grows past either
    threshold (``threshold_messages`` or ``threshold_tokens``, whichever fires
    first), the oldest messages are replaced with a single LLM-generated
    summary while the last ``keep_recent_messages`` are preserved verbatim.

    Backward compatibility: the original spec §15.3 schema only had
    ``threshold_tokens`` (default 100,000) + ``preserve_delegation_handles``.
    Task #54 introduces ``threshold_messages``, ``keep_recent_messages``, and
    ``enabled`` with defaults so existing TOML configs remain valid.
    """

    threshold_tokens: int = Field(default=50_000, ge=0)
    threshold_messages: int = Field(default=30, ge=1)
    keep_recent_messages: int = Field(default=10, ge=1)
    enabled: bool = True
    preserve_delegation_handles: bool = True


class ApprovalConfig(_StrictModel):
    """``[approval]`` — approval-required action timeout (§5.8)."""

    timeout_seconds: int = Field(ge=1)


class HeartbeatConfig(_StrictModel):
    """``[heartbeat]`` — milestone notification intervals (seconds)."""

    intervals_seconds: list[int]

    @field_validator("intervals_seconds")
    @classmethod
    def _positive_sorted(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("heartbeat.intervals_seconds must be non-empty")
        if any(i <= 0 for i in v):
            raise ValueError("heartbeat.intervals_seconds entries must be > 0")
        return v


class UserEntry(_StrictModel):
    """One ``[users.<user_id>]`` block. The TOML key is the literal
    ``user_id`` (per the inline note in §15.3)."""

    amc_senders: list[str]
    display_name: str

    @field_validator("amc_senders")
    @classmethod
    def _nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("users.<id>.amc_senders must be non-empty")
        return v


class UsersConfig(BaseModel):
    """``[users]`` — arbitrary user_id → entry mapping. Also exposes a flat
    ``sender_to_user_id`` derived map for the dispatcher (spec §5.1).

    Unlike the other sub-models we keep ``extra="allow"`` because the TOML
    schema is ``[users.<user_id>]`` where ``<user_id>`` is user-defined. A
    model_validator parses each unknown attribute into a :class:`UserEntry`.
    """

    model_config = {"extra": "allow"}

    @model_validator(mode="after")
    def _parse_user_entries(self) -> UsersConfig:
        """Coerce each extra dict into a typed :class:`UserEntry`.

        Also enforces the V1 invariant (spec §5.11): at least one
        ``[users.<user_id>]`` block must be present. An empty ``[users]``
        table (or no table at all) is a boot-time misconfig.
        """
        extras = self.__pydantic_extra__ or {}
        if not extras:
            # Empty [users] table — fail fast with the offending key path so
            # the loader's single-line formatter shows ``users: at least one
            # user is required`` rather than a silent runtime symptom where
            # every envelope is rejected as "unknown sender".
            raise ValueError(
                "at least one user is required; add a [users.<user_id>] "
                "block to config.toml (spec §5.11, §15.3)"
            )
        parsed: dict[str, UserEntry] = {}
        for user_id, raw in extras.items():
            if isinstance(raw, UserEntry):
                parsed[user_id] = raw
                continue
            if not isinstance(raw, dict):
                raise ValueError(
                    f"users.{user_id}: expected a table, got {type(raw).__name__}"
                )
            try:
                parsed[user_id] = UserEntry(**raw)
            except ValidationError as exc:
                # Re-raise with the offending user_id prepended to each error.
                msgs = [
                    f"users.{user_id}." + ".".join(str(p) for p in e["loc"])
                    + f": {e['msg']}"
                    for e in exc.errors()
                ]
                raise ValueError("; ".join(msgs)) from exc
        # Stash parsed entries back into extras so they survive future access.
        self.__pydantic_extra__ = parsed
        return self

    @property
    def entries(self) -> dict[str, UserEntry]:
        """All user blocks keyed by ``user_id``."""
        return dict(self.__pydantic_extra__ or {})

    @property
    def sender_to_user_id(self) -> dict[str, str]:
        """Flat AMC-sender → user_id map. Raises if two users claim the same
        sender (which would otherwise silently shadow earlier users)."""
        flat: dict[str, str] = {}
        for user_id, entry in self.entries.items():
            for sender in entry.amc_senders:
                if sender in flat and flat[sender] != user_id:
                    raise ValueError(
                        f"users: AMC sender {sender!r} is claimed by both "
                        f"{flat[sender]!r} and {user_id!r}"
                    )
                flat[sender] = user_id
        return flat


class ObservabilityConfig(_StrictModel):
    """``[observability]`` — Logfire toggles (spec §6.5).

    Phase 5 (Task #50) extends this block with Logfire wiring fields. All
    fields are optional with sensible defaults so existing config files
    that only set ``logfire_enabled = true`` continue to load unchanged.

    Token precedence (resolved at ``configure_logfire`` time, not here):
      1. ``observability.token`` from TOML/.env (this field, when set).
      2. ``LOGFIRE_TOKEN`` from the process environment.
      3. None — Logfire is skipped silently if ``LOGFIRE_IGNORE_NO_CONFIG=1``
         is also set, otherwise Logfire's own credentials-file lookup runs.
    """

    logfire_enabled: bool = True
    token: SecretStr | None = None
    service_name: str = "capo"
    environment: str = "dev"


class BootConfig(_StrictModel):
    """``[boot]`` — boot-time pre-flight knobs (spec §5.4, §7.5, §12.1).

    Task #62: gate the binary version pre-checks. ``skip_binary_precheck``
    is intentionally off-by-default; flipping it true is the documented
    escape hatch for smoke tests / CI runs that don't ship ``claude`` or
    ``codex`` binaries on the executor. When skipped the boot path logs an
    informational event and proceeds (no version assertion).
    """

    skip_binary_precheck: bool = False


# ---------------------------------------------------------------------------
# Top-level Settings model.
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Capo top-level settings.

    Composed from a TOML file (config tables) plus a ``.env`` file (secrets
    only). Use :meth:`Settings.load` — directly instantiating this class is
    intended for tests that already have a dict in hand.
    """

    # TOML-sourced tables.
    models: ModelsConfig
    paths: PathsConfig
    soul: SoulConfig
    amc: AmcConfig
    agents: AgentsConfig
    budget: BudgetConfig
    shell: ShellConfig
    concurrency: ConcurrencyConfig
    retention: RetentionConfig
    compaction: CompactionConfig
    approval: ApprovalConfig
    heartbeat: HeartbeatConfig
    users: UsersConfig
    observability: ObservabilityConfig
    # Task #62: ``[boot]`` is optional in config.toml — defaults give the
    # production behavior (precheck enabled) so existing config files load
    # unchanged. Set ``boot.skip_binary_precheck = true`` for smoke/CI runs
    # without the claude/codex CLIs.
    boot: BootConfig = Field(default_factory=BootConfig)

    # .env-sourced secrets. Names match the environment variables verbatim.
    amc_webhook_secret: SecretStr = Field(alias="AMC_WEBHOOK_SECRET")
    amc_bearer_token: SecretStr = Field(alias="AMC_BEARER_TOKEN")
    anthropic_api_key: SecretStr | None = Field(default=None, alias="ANTHROPIC_API_KEY")
    openai_api_key: SecretStr | None = Field(default=None, alias="OPENAI_API_KEY")

    # We intentionally do NOT use ``pydantic-settings``' built-in env source
    # for the TOML-sourced tables — there are real environment variables that
    # collide with table names (e.g. ``SHELL`` collides with the ``shell``
    # field). Instead, ``Settings.load`` reads ``.env`` + ``os.environ``
    # explicitly and passes both the TOML tables and the secret aliases via
    # ``__init__`` kwargs. The custom ``settings_customise_sources`` below
    # disables the env / dotenv / secrets sources so they cannot inject
    # surprise values.
    model_config = SettingsConfigDict(
        env_file=None,
        env_file_encoding="utf-8",
        extra="forbid",  # Top-level extras at the Settings layer are a bug.
        populate_by_name=True,
        case_sensitive=False,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        """Use only the init source. See note on :attr:`model_config`."""
        return (init_settings,)


    # ---- redaction guarantees -------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover - trivial wrapper
        # SecretStr already renders as ``SecretStr('**********')``; we just
        # make the default repr explicit so future fields can't bypass it.
        return super().__repr__()

    # ---- helpers --------------------------------------------------------------

    @property
    def sender_to_user_id(self) -> dict[str, str]:
        """Convenience pass-through to :attr:`UsersConfig.sender_to_user_id`."""
        return self.users.sender_to_user_id

    # ---- loader ---------------------------------------------------------------

    @classmethod
    def load(
        cls,
        config_path: Path,
        *,
        env_file: Path | None = None,
        env_override: dict[str, str] | None = None,
    ) -> Settings:
        """Load and validate Settings from ``config_path`` + environment.

        Parameters
        ----------
        config_path:
            Path to ``config.toml``. Must exist and be readable.
        env_file:
            Optional explicit ``.env`` path. If provided, its keys are loaded
            into a local mapping (NOT into ``os.environ``) and overlaid on top
            of ``env_override``. If ``None``, only ``os.environ`` is used.
        env_override:
            Optional explicit env mapping for tests. When provided, it
            *replaces* ``os.environ`` for the purpose of secret loading.

        Raises
        ------
        ConfigError
            On any validation failure, missing config file, missing required
            secret, or unreadable soul file. The message includes the
            offending key path or file path.
        """
        config_path = Path(config_path).expanduser()
        if not config_path.is_file():
            raise ConfigError(
                f"config.toml not found at {config_path}. "
                "Pass --config <path> or set CAPO_CONFIG."
            )

        try:
            with config_path.open("rb") as fh:
                raw = tomllib.load(fh)
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(
                f"config.toml at {config_path} is not valid TOML: {exc}"
            ) from exc
        except OSError as exc:
            raise ConfigError(
                f"failed to read config.toml at {config_path}: {exc}"
            ) from exc

        # Resolve [soul].dir relative to the config file directory if not
        # absolute. Done before validation so the soul-file existence check
        # has the final path to test.
        config_dir = config_path.parent
        soul_block = raw.get("soul")
        if isinstance(soul_block, dict):
            soul_dir_value = soul_block.get("dir")
            if isinstance(soul_dir_value, str):
                soul_path = Path(soul_dir_value).expanduser()
                if not soul_path.is_absolute():
                    soul_block["dir"] = str((config_dir / soul_path).resolve())

        # Detect and warn on unknown top-level keys before passing to Pydantic.
        cls._warn_unknown_keys(raw)

        # Compose env mapping: base os.environ (or override), then .env overlay.
        env_map: dict[str, str] = (
            dict(os.environ) if env_override is None else dict(env_override)
        )
        if env_file is not None:
            env_map = {**env_map, **_parse_env_file(env_file)}

        # Pull out the secrets explicitly so we can give better errors than
        # pydantic-settings does by default.
        required_secret = "AMC_WEBHOOK_SECRET"
        if not env_map.get(required_secret):
            raise ConfigError(
                f"{required_secret} is required in .env. "
                "Create .env beside config.toml or export it; never commit it. "
                "(spec §6.2)"
            )
        if not env_map.get("AMC_BEARER_TOKEN"):
            raise ConfigError(
                "AMC_BEARER_TOKEN is required in .env. "
                "Create .env beside config.toml or export it; never commit it. "
                "(spec §6.2)"
            )

        # Build the kwargs Pydantic Settings expects. We pass TOML tables as
        # direct fields and the secrets via their aliases.
        kwargs: dict[str, Any] = {
            "models": raw.get("models", {}),
            "paths": raw.get("paths", {}),
            "soul": raw.get("soul", {}),
            "amc": raw.get("amc", {}),
            "agents": raw.get("agents", {}),
            "budget": raw.get("budget", {}),
            "shell": raw.get("shell", {}),
            "concurrency": raw.get("concurrency", {}),
            "retention": raw.get("retention", {}),
            "compaction": raw.get("compaction", {}),
            "approval": raw.get("approval", {}),
            "heartbeat": raw.get("heartbeat", {}),
            "users": raw.get("users", {}),
            "observability": raw.get("observability", {}),
            "boot": raw.get("boot", {}),
            "AMC_WEBHOOK_SECRET": env_map[required_secret],
            "AMC_BEARER_TOKEN": env_map["AMC_BEARER_TOKEN"],
        }
        if env_map.get("ANTHROPIC_API_KEY"):
            kwargs["ANTHROPIC_API_KEY"] = env_map["ANTHROPIC_API_KEY"]
        if env_map.get("OPENAI_API_KEY"):
            kwargs["OPENAI_API_KEY"] = env_map["OPENAI_API_KEY"]

        try:
            settings = cls(**kwargs)
        except ValidationError as exc:
            raise ConfigError(_format_validation_error(exc)) from exc

        # Post-validation: soul file must resolve and be readable.
        soul_file = settings.soul.dir / f"{settings.soul.active}.md"
        if not soul_file.is_file():
            raise ConfigError(
                f"soul.active={settings.soul.active!r} but soul file not "
                f"found at {soul_file}. Create the file or correct "
                "soul.active / soul.dir."
            )

        return settings

    # ---- validators on raw TOML ----------------------------------------------

    @staticmethod
    def _warn_unknown_keys(raw: dict[str, Any]) -> None:
        """Emit ``logging.warning`` for any top-level TOML key we don't know.

        Pydantic's ``extra="forbid"`` would *reject* these; per the
        acceptance criteria we instead *warn* so the user sees their typo
        without breaking forward-compatible additions on the rollout side.
        """
        known = {
            "models",
            "paths",
            "soul",
            "amc",
            "agents",
            "budget",
            "shell",
            "concurrency",
            "retention",
            "compaction",
            "approval",
            "heartbeat",
            "users",
            "observability",
            "boot",
        }
        for key in raw:
            if key not in known:
                logger.warning(
                    "config.toml: unknown top-level key %r — ignored "
                    "(typo or forward-compat field?)",
                    key,
                )
                # Remove it so Settings(extra="forbid") doesn't choke.
        # Mutate the dict in place by stripping unknowns.
        for key in list(raw.keys()):
            if key not in known:
                raw.pop(key)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _is_loopback(host: str) -> bool:
    """Return True if ``host`` is a loopback name or address."""
    h = host.strip().lower()
    if h in _LOOPBACK_HOSTS:
        return True
    try:
        addr = ipaddress.ip_address(h)
    except ValueError:
        return False
    return addr.is_loopback


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a minimal ``.env`` (``KEY=VALUE`` per line, ``#`` comments,
    optional surrounding quotes).

    We use a tiny in-house parser rather than ``pydantic-settings``' built-in
    so we can keep the loader pure (no ``os.environ`` mutation) and emit
    ConfigError with the precise file path on failure.
    """
    if not path.is_file():
        raise ConfigError(f".env file not found at {path}")
    out: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"failed to read .env at {path}: {exc}") from exc

    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigError(
                f".env at {path}:{lineno}: missing '=' in line {raw_line!r}"
            )
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        # Strip optional surrounding quotes.
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {'"', "'"}:
            val = val[1:-1]
        if not key:
            raise ConfigError(
                f".env at {path}:{lineno}: empty key in line {raw_line!r}"
            )
        out[key] = val
    return out


def _format_validation_error(exc: ValidationError) -> str:
    """Render a Pydantic ``ValidationError`` as a single-line message that
    always names the offending key path (e.g. ``amc.listen_host``)."""
    parts: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ()))
        msg = err.get("msg", "invalid value")
        parts.append(f"{loc or '<root>'}: {msg}")
    return "config validation failed: " + "; ".join(parts)
