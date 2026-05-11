"""Unit tests for :mod:`capo.config` (Task 6).

Covers acceptance criteria from spec §6.2, §9.1, §15.3:

* Valid TOML + .env loads cleanly.
* Each table validates with valid + invalid inputs.
* Secrets are SecretStr — never appear in repr/str.
* ``[users]`` parses to a sender→user_id map.
* Off-loopback listener bind requires TLS.
* Missing soul file raises a clear error citing the path checked.
* Missing AMC_WEBHOOK_SECRET / AMC_BEARER_TOKEN raise ConfigError.
* Missing config.toml: clean error, no stack trace.
* Unknown TOML keys logged as warnings; not silently dropped.
* All validation errors include the offending key path.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from capo.config import ConfigError, Settings

# ---------------------------------------------------------------------------
# Helpers — build a valid baseline config and selectively mutate it.
# ---------------------------------------------------------------------------


_VALID_TOML = """\
[models]
default = "anthropic:claude-sonnet-4-6"
router  = "anthropic:claude-haiku-4-5"
heavy   = "anthropic:claude-opus-4-7"

[models.subagents]
claude_code = "anthropic:claude-sonnet-4-6"
codex       = "openai:gpt-5"

[paths]
workspaces_root = "~/.capo/workspaces"
projects_root   = "~/code"
db_path         = "~/.capo/state.db"
dbos_db_path    = "~/.capo/dbos.db"

[soul]
active = "default"
dir    = "souls"

[amc]
base_url              = "http://127.0.0.1:8080"
agent_id              = "capo"
listen_host           = "127.0.0.1"
listen_port           = 8090
max_boot_wait_seconds = 60

[agents.claude_code]
binary = "claude"
default_permission_mode = "acceptEdits"
worktree_base_branch = "main"

[agents.codex]
binary = "codex"
default_sandbox = "modal"

[budget]
soft_daily_usd = 25
hard_daily_usd = 75
notify_channel = "amc:default"

[shell]
allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc","find","du","df","uname"]

[concurrency]
max_delegations = 3

[retention]
delegation_output_days = 7

[compaction]
threshold_tokens = 100000
preserve_delegation_handles = true

[approval]
timeout_seconds = 1800

[heartbeat]
intervals_seconds = [900, 3600, 14400]

[users.owner]
amc_senders = ["+15551234567", "discord:user:123"]
display_name = "Owner"

[observability]
logfire_enabled = true
"""


_VALID_ENV = {
    "AMC_WEBHOOK_SECRET": "shhh-webhook",
    "AMC_BEARER_TOKEN": "shhh-bearer",
    "ANTHROPIC_API_KEY": "sk-ant-shhh",
    "OPENAI_API_KEY": "sk-openai-shhh",
}


def _write_config(
    tmp_path: Path,
    toml: str = _VALID_TOML,
    *,
    soul_active: str = "default",
) -> Path:
    """Write a config.toml + matching soul file, return the config path."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(toml)
    souls = tmp_path / "souls"
    souls.mkdir(exist_ok=True)
    (souls / f"{soul_active}.md").write_text("# soul: " + soul_active)
    return cfg


# ---------------------------------------------------------------------------
# Happy path.
# ---------------------------------------------------------------------------


def test_valid_config_and_env_loads(tmp_path: Path) -> None:
    """Baseline: valid TOML + valid env → Settings loads."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    assert s.models.default == "anthropic:claude-sonnet-4-6"
    assert s.models.subagents.claude_code == "anthropic:claude-sonnet-4-6"
    assert s.amc.listen_host == "127.0.0.1"
    assert s.amc.listen_port == 8090
    assert s.budget.hard_daily_usd >= s.budget.soft_daily_usd
    assert s.concurrency.max_delegations == 3
    assert s.observability.logfire_enabled is True
    assert "git" in s.shell.allowlist


def test_users_parses_sender_to_user_id_mapping(tmp_path: Path) -> None:
    """``[users]`` block flattens to sender → user_id."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    mapping = s.sender_to_user_id
    assert mapping["+15551234567"] == "owner"
    assert mapping["discord:user:123"] == "owner"
    assert s.users.entries["owner"].display_name == "Owner"


def test_users_duplicate_sender_across_users_errors(tmp_path: Path) -> None:
    """Same AMC sender claimed by two users is rejected."""
    toml = _VALID_TOML + """
[users.guest]
amc_senders = ["+15551234567"]
display_name = "Guest"
"""
    cfg = _write_config(tmp_path, toml)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    with pytest.raises(ValueError, match="claimed by both"):
        _ = s.sender_to_user_id


# ---------------------------------------------------------------------------
# Secrets — SecretStr redaction.
# ---------------------------------------------------------------------------


def test_secrets_never_appear_in_repr_or_str(tmp_path: Path) -> None:
    """SecretStr fields are redacted in repr / str output."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    rendered = repr(s) + " | " + str(s)
    assert "shhh-webhook" not in rendered
    assert "shhh-bearer" not in rendered
    assert "sk-ant-shhh" not in rendered
    assert "sk-openai-shhh" not in rendered

    # And the field-level repr is the redacted SecretStr placeholder.
    assert "shhh-webhook" not in repr(s.amc_webhook_secret)
    # Sanity: the actual secret is still retrievable via get_secret_value().
    assert s.amc_webhook_secret.get_secret_value() == "shhh-webhook"


def test_secrets_not_in_logged_output(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Logging the Settings object does not leak secret values."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    log = logging.getLogger("capo.test")
    with caplog.at_level(logging.INFO, logger="capo.test"):
        log.info("settings=%r", s)
        log.info("settings_str=%s", s)
    text = caplog.text
    for secret in _VALID_ENV.values():
        assert secret not in text, f"leaked {secret!r} in log output"


# ---------------------------------------------------------------------------
# .env loading + missing required secrets.
# ---------------------------------------------------------------------------


def test_missing_amc_webhook_secret_raises(tmp_path: Path) -> None:
    """Missing AMC_WEBHOOK_SECRET → ConfigError naming the env var."""
    cfg = _write_config(tmp_path)
    env = dict(_VALID_ENV)
    del env["AMC_WEBHOOK_SECRET"]
    with pytest.raises(ConfigError, match="AMC_WEBHOOK_SECRET is required"):
        Settings.load(cfg, env_override=env)


def test_missing_amc_bearer_token_raises(tmp_path: Path) -> None:
    """Missing AMC_BEARER_TOKEN → ConfigError naming the env var."""
    cfg = _write_config(tmp_path)
    env = dict(_VALID_ENV)
    del env["AMC_BEARER_TOKEN"]
    with pytest.raises(ConfigError, match="AMC_BEARER_TOKEN is required"):
        Settings.load(cfg, env_override=env)


def test_env_file_loaded_when_provided(tmp_path: Path) -> None:
    """``.env`` overlay works when ``env_file=`` is passed explicitly."""
    cfg = _write_config(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "AMC_WEBHOOK_SECRET=from-file\n"
        "AMC_BEARER_TOKEN='from-file-bearer'\n"
        "# comment line ignored\n"
        "\n"
    )
    s = Settings.load(cfg, env_file=env_path, env_override={})
    assert s.amc_webhook_secret.get_secret_value() == "from-file"
    assert s.amc_bearer_token.get_secret_value() == "from-file-bearer"


def test_env_file_malformed_line_errors(tmp_path: Path) -> None:
    """A line in .env without ``=`` raises ConfigError with the path."""
    cfg = _write_config(tmp_path)
    env_path = tmp_path / ".env"
    env_path.write_text("AMC_WEBHOOK_SECRET=ok\nNOT_AN_ASSIGNMENT\n")
    with pytest.raises(ConfigError, match=r"\.env at .*: missing '='"):
        Settings.load(cfg, env_file=env_path, env_override={})


# ---------------------------------------------------------------------------
# Missing config.toml + invalid TOML.
# ---------------------------------------------------------------------------


def test_missing_config_file_clean_error(tmp_path: Path) -> None:
    """Missing config.toml → ConfigError (clean error, not a stack trace)."""
    missing = tmp_path / "does-not-exist.toml"
    with pytest.raises(ConfigError, match="not found"):
        Settings.load(missing, env_override=dict(_VALID_ENV))


def test_invalid_toml_clean_error(tmp_path: Path) -> None:
    """Corrupt TOML → ConfigError with the file path."""
    cfg = tmp_path / "config.toml"
    cfg.write_text("[models\nthis is not toml")
    with pytest.raises(ConfigError, match="not valid TOML"):
        Settings.load(cfg, env_override=dict(_VALID_ENV))


# ---------------------------------------------------------------------------
# Listener bind + TLS enforcement.
# ---------------------------------------------------------------------------


def test_loopback_listener_does_not_require_tls(tmp_path: Path) -> None:
    """Loopback bind is fine without TLS — current spec §6.2 default."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert s.amc.tls_cert is None
    assert s.amc.tls_key is None


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0", "192.168.1.10", "10.0.0.1", "example.com"],
)
def test_off_loopback_without_tls_rejected(tmp_path: Path, host: str) -> None:
    """Off-loopback bind without TLS → ConfigError citing the bind addr."""
    toml = _VALID_TOML.replace(
        'listen_host           = "127.0.0.1"',
        f'listen_host           = "{host}"',
    )
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert "amc" in str(exc.value)
    assert host in str(exc.value)


def test_off_loopback_with_tls_accepted(tmp_path: Path) -> None:
    """Off-loopback bind with tls_cert + tls_key → loads."""
    toml = _VALID_TOML.replace(
        'max_boot_wait_seconds = 60',
        'max_boot_wait_seconds = 60\ntls_cert = "/tmp/cert.pem"\ntls_key = "/tmp/key.pem"',
    ).replace(
        'listen_host           = "127.0.0.1"',
        'listen_host           = "0.0.0.0"',
    )
    cfg = _write_config(tmp_path, toml)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert str(s.amc.tls_cert) == "/tmp/cert.pem"
    assert str(s.amc.tls_key) == "/tmp/key.pem"


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_loopback_aliases_all_recognized(tmp_path: Path, host: str) -> None:
    """All three loopback spellings skip the TLS requirement."""
    toml = _VALID_TOML.replace(
        'listen_host           = "127.0.0.1"',
        f'listen_host           = "{host}"',
    )
    cfg = _write_config(tmp_path, toml)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert s.amc.listen_host == host


# ---------------------------------------------------------------------------
# Soul file resolution.
# ---------------------------------------------------------------------------


def test_missing_soul_file_clean_error(tmp_path: Path) -> None:
    """Soul file missing → ConfigError citing the path checked."""
    cfg = _write_config(tmp_path, soul_active="default")
    # Remove the soul file we just created.
    soul_file = tmp_path / "souls" / "default.md"
    soul_file.unlink()
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    msg = str(exc.value)
    assert "soul" in msg
    assert str(soul_file) in msg


def test_soul_file_path_resolves_relative_to_config(tmp_path: Path) -> None:
    """``soul.dir`` is resolved relative to the config file directory."""
    cfg = _write_config(tmp_path)
    s = Settings.load(cfg, env_override=dict(_VALID_ENV))
    # The validator should have rewritten soul.dir to absolute.
    assert s.soul.dir.is_absolute()
    assert s.soul.dir == (tmp_path / "souls").resolve()


def test_soul_active_pointing_to_missing_variant_errors(tmp_path: Path) -> None:
    """``soul.active=concise`` without ``souls/concise.md`` → ConfigError."""
    toml = _VALID_TOML.replace('active = "default"', 'active = "concise"')
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError, match="concise"):
        Settings.load(cfg, env_override=dict(_VALID_ENV))


# ---------------------------------------------------------------------------
# Per-table validation: valid + invalid pairs.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "search,replace,key_path",
    [
        # models: missing required key surfaces as ``models.subagents.codex``
        (
            'codex       = "openai:gpt-5"',
            "",
            "models.subagents.codex",
        ),
        # amc: invalid port surfaces as ``amc.listen_port``
        (
            "listen_port           = 8090",
            "listen_port           = 70000",
            "amc.listen_port",
        ),
        # budget: hard < soft surfaces as ``budget`` (model_validator path)
        (
            "hard_daily_usd = 75",
            "hard_daily_usd = 10",
            "budget",
        ),
        # shell: empty allowlist
        (
            'allowlist = ["git","ls","rg","cat","pwd","which","head","tail","wc",'
            '"find","du","df","uname"]',
            "allowlist = []",
            "shell.allowlist",
        ),
        # concurrency: zero max_delegations
        (
            "max_delegations = 3",
            "max_delegations = 0",
            "concurrency.max_delegations",
        ),
        # retention: negative days
        (
            "delegation_output_days = 7",
            "delegation_output_days = -1",
            "retention.delegation_output_days",
        ),
        # approval: zero timeout
        (
            "timeout_seconds = 1800",
            "timeout_seconds = 0",
            "approval.timeout_seconds",
        ),
        # heartbeat: negative interval
        (
            "intervals_seconds = [900, 3600, 14400]",
            "intervals_seconds = [-1, 3600]",
            "heartbeat.intervals_seconds",
        ),
    ],
)
def test_invalid_table_inputs_name_key_path(
    tmp_path: Path, search: str, replace: str, key_path: str
) -> None:
    """Invalid input in each table includes the offending key path in the error."""
    toml = _VALID_TOML.replace(search, replace)
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert key_path in str(exc.value), (
        f"expected key_path {key_path!r} in error, got: {exc.value}"
    )


def test_validation_error_includes_offending_key_path_explicit(tmp_path: Path) -> None:
    """Spot-check: validation error names ``amc.listen_port`` for a bad port."""
    toml = _VALID_TOML.replace(
        "listen_port           = 8090",
        "listen_port           = -1",
    )
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    assert "amc.listen_port" in str(exc.value)


# ---------------------------------------------------------------------------
# Unknown keys: warn, do not silently drop.
# ---------------------------------------------------------------------------


def test_unknown_toml_key_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Unknown top-level TOML key produces a warning but does not fail."""
    toml = _VALID_TOML + "\n[future_feature]\nflag = true\n"
    cfg = _write_config(tmp_path, toml)

    with caplog.at_level(logging.WARNING, logger="capo.config"):
        s = Settings.load(cfg, env_override=dict(_VALID_ENV))

    assert s.amc.listen_port == 8090
    assert any(
        "future_feature" in rec.message and rec.levelno == logging.WARNING
        for rec in caplog.records
    )


def test_unknown_nested_key_rejected_with_path(tmp_path: Path) -> None:
    """Unknown keys *inside* a known table are rejected with the key path."""
    toml = _VALID_TOML.replace(
        "[concurrency]\nmax_delegations = 3",
        "[concurrency]\nmax_delegations = 3\nunknown_field = 1",
    )
    cfg = _write_config(tmp_path, toml)
    with pytest.raises(ConfigError) as exc:
        Settings.load(cfg, env_override=dict(_VALID_ENV))
    msg = str(exc.value)
    assert "concurrency" in msg
    assert "unknown_field" in msg
