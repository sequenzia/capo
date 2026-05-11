"""Tests for ``capo.observability`` (Task #50 — Logfire wiring).

Covers the §6.5 / Task #50 acceptance criteria:

* ``configure_logfire`` is idempotent.
* Token sourced from ``settings.observability.token`` or ``LOGFIRE_TOKEN``.
* Service name + environment threaded into ``logfire.configure(...)``.
* Auto-instrument FastAPI / httpx / pydantic_ai calls fire.
* ``LOGFIRE_IGNORE_NO_CONFIG=1`` + no token → silent skip.
* ``logfire.configure`` raising → caught + logged, app continues.
* ``logfire`` package missing → :class:`LogfireMissingError` raised.
* ``with_span`` works through both configured and unconfigured paths.
* ``is_configured`` reflects the latch correctly.
"""

from __future__ import annotations

import contextlib
import sys
from typing import Any

import pytest

from capo import observability
from capo.config import ObservabilityConfig
from capo.observability import (
    LogfireMissingError,
    configure_logfire,
    instrument_fastapi_app,
    is_configured,
    with_span,
)

# ---------------------------------------------------------------------------
# Helpers — minimal Settings stand-in.
# ---------------------------------------------------------------------------


class _SettingsStub:
    """Minimal Settings stand-in for tests.

    ``configure_logfire`` only touches ``settings.observability.*``, so a
    tiny attribute-bag avoids the overhead of building the full
    :class:`capo.config.Settings` per test (which would require valid
    TOML, .env, soul file, etc.).
    """

    def __init__(self, observability_cfg: ObservabilityConfig) -> None:
        self.observability = observability_cfg


def _make_settings(
    *,
    enabled: bool = True,
    token: str | None = None,
    service_name: str = "capo",
    environment: str = "dev",
) -> _SettingsStub:
    return _SettingsStub(
        ObservabilityConfig(
            logfire_enabled=enabled,
            token=token,  # type: ignore[arg-type]  # SecretStr coerce
            service_name=service_name,
            environment=environment,
        )
    )


class _FakeLogfire:
    """In-memory ``logfire`` stand-in used to assert call patterns."""

    def __init__(
        self,
        *,
        configure_raises: BaseException | None = None,
        span_raises: BaseException | None = None,
        instrument_httpx_raises: BaseException | None = None,
        instrument_fastapi_raises: BaseException | None = None,
        instrument_pydantic_ai_raises: BaseException | None = None,
    ) -> None:
        self.configure_calls: list[dict[str, Any]] = []
        self.instrument_fastapi_calls: list[Any] = []
        self.instrument_httpx_count = 0
        self.instrument_pydantic_ai_count = 0
        self.span_calls: list[tuple[str, dict[str, Any]]] = []
        self._configure_raises = configure_raises
        self._span_raises = span_raises
        self._instrument_httpx_raises = instrument_httpx_raises
        self._instrument_fastapi_raises = instrument_fastapi_raises
        self._instrument_pydantic_ai_raises = instrument_pydantic_ai_raises

    def configure(self, **kwargs: Any) -> None:
        self.configure_calls.append(kwargs)
        if self._configure_raises is not None:
            raise self._configure_raises

    def instrument_fastapi(self, app: Any) -> None:
        if self._instrument_fastapi_raises is not None:
            raise self._instrument_fastapi_raises
        self.instrument_fastapi_calls.append(app)

    def instrument_httpx(self) -> None:
        if self._instrument_httpx_raises is not None:
            raise self._instrument_httpx_raises
        self.instrument_httpx_count += 1

    def instrument_pydantic_ai(self) -> None:
        if self._instrument_pydantic_ai_raises is not None:
            raise self._instrument_pydantic_ai_raises
        self.instrument_pydantic_ai_count += 1

    def span(self, name: str, **attrs: Any) -> contextlib.AbstractContextManager[Any]:
        self.span_calls.append((name, attrs))
        if self._span_raises is not None:
            raise self._span_raises
        return contextlib.nullcontext(self)


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_observability() -> None:
    """Reset the module-level configured latch between tests."""
    observability._reset_for_tests()
    yield
    observability._reset_for_tests()


@pytest.fixture
def fake_logfire(monkeypatch: pytest.MonkeyPatch) -> _FakeLogfire:
    """Install a :class:`_FakeLogfire` as the importable ``logfire`` module."""
    fake = _FakeLogfire()
    # Make `import logfire` inside configure_logfire resolve to this fake.
    monkeypatch.setitem(sys.modules, "logfire", fake)  # type: ignore[arg-type]
    return fake


# ---------------------------------------------------------------------------
# Functional: idempotency.
# ---------------------------------------------------------------------------


def test_configure_logfire_is_idempotent(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling configure_logfire twice should configure once."""
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok-xyz")
    settings = _make_settings()

    first = configure_logfire(settings)
    second = configure_logfire(settings)

    assert first is fake_logfire
    assert second is fake_logfire
    assert len(fake_logfire.configure_calls) == 1
    # Auto-instrument should have fired exactly once.
    assert fake_logfire.instrument_httpx_count == 1
    assert fake_logfire.instrument_pydantic_ai_count == 1
    assert is_configured() is True


# ---------------------------------------------------------------------------
# Functional: token sourcing.
# ---------------------------------------------------------------------------


def test_token_from_settings_wins_over_env(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "env-token")
    settings = _make_settings(token="explicit-token")

    configure_logfire(settings)

    assert fake_logfire.configure_calls[0]["token"] == "explicit-token"


def test_token_from_env_var(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "env-token")
    monkeypatch.delenv("LOGFIRE_IGNORE_NO_CONFIG", raising=False)
    settings = _make_settings(token=None)

    configure_logfire(settings)

    assert fake_logfire.configure_calls[0]["token"] == "env-token"


def test_service_name_and_environment_threaded(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok-1")
    settings = _make_settings(service_name="capo", environment="prod")

    configure_logfire(settings)

    call = fake_logfire.configure_calls[0]
    assert call["service_name"] == "capo"
    assert call["environment"] == "prod"
    assert call["send_to_logfire"] is True


# ---------------------------------------------------------------------------
# Functional: auto-instrument.
# ---------------------------------------------------------------------------


def test_auto_instrument_httpx_and_pydantic_ai(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    settings = _make_settings()

    configure_logfire(settings)

    assert fake_logfire.instrument_httpx_count == 1
    assert fake_logfire.instrument_pydantic_ai_count == 1


def test_instrument_fastapi_app_calls_logfire(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    configure_logfire(_make_settings())

    app = object()
    instrument_fastapi_app(app)

    assert fake_logfire.instrument_fastapi_calls == [app]


def test_instrument_fastapi_app_noop_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When configure_logfire was skipped, instrument_fastapi_app is a no-op."""
    fake = _FakeLogfire()
    monkeypatch.setitem(sys.modules, "logfire", fake)  # type: ignore[arg-type]

    app = object()
    instrument_fastapi_app(app)  # never configured

    assert fake.instrument_fastapi_calls == []


def test_auto_instrument_individual_failure_tolerated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One instrument_* failing doesn't take the others down."""
    fake = _FakeLogfire(
        instrument_httpx_raises=RuntimeError("httpx ext missing"),
    )
    monkeypatch.setitem(sys.modules, "logfire", fake)  # type: ignore[arg-type]
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")

    result = configure_logfire(_make_settings())

    # configure_logfire still returns the module + sets configured=True.
    assert result is fake
    assert is_configured() is True
    # pydantic_ai still got patched even though httpx failed.
    assert fake.instrument_pydantic_ai_count == 1


# ---------------------------------------------------------------------------
# Edge cases: disable / silent skip.
# ---------------------------------------------------------------------------


def test_disabled_via_settings_returns_none(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    settings = _make_settings(enabled=False)

    result = configure_logfire(settings)

    assert result is None
    assert is_configured() is False
    assert fake_logfire.configure_calls == []


def test_silent_skip_when_no_token_and_ignore_no_config(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    monkeypatch.setenv("LOGFIRE_IGNORE_NO_CONFIG", "1")
    settings = _make_settings(token=None)

    result = configure_logfire(settings)

    assert result is None
    assert is_configured() is False
    assert fake_logfire.configure_calls == []


def test_no_token_no_ignore_still_configures(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without LOGFIRE_IGNORE_NO_CONFIG, an absent token still calls
    logfire.configure() — Logfire's own credential-file lookup runs and
    we let it decide. send_to_logfire flips off so the SDK doesn't try
    to push to the cloud without a token."""
    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    monkeypatch.delenv("LOGFIRE_IGNORE_NO_CONFIG", raising=False)
    settings = _make_settings(token=None)

    configure_logfire(settings)

    assert len(fake_logfire.configure_calls) == 1
    call = fake_logfire.configure_calls[0]
    assert call["token"] is None
    assert call["send_to_logfire"] is False


# ---------------------------------------------------------------------------
# Edge case: invalid token (configure raises) → caught + logged.
# ---------------------------------------------------------------------------


def test_configure_exception_is_caught_app_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """logfire.configure raising must NOT propagate out of configure_logfire."""
    fake = _FakeLogfire(
        configure_raises=ValueError("invalid token format"),
    )
    monkeypatch.setitem(sys.modules, "logfire", fake)  # type: ignore[arg-type]
    monkeypatch.setenv("LOGFIRE_TOKEN", "bad-token")

    with caplog.at_level("ERROR", logger="capo.observability"):
        result = configure_logfire(_make_settings())

    assert result is None
    assert is_configured() is False
    # The exception was logged structurally.
    matching = [
        r for r in caplog.records
        if r.name == "capo.observability"
        and "configure() failed" in r.getMessage()
    ]
    assert len(matching) == 1


# ---------------------------------------------------------------------------
# Edge case: logfire library missing → typed error at boot.
# ---------------------------------------------------------------------------


def test_logfire_library_missing_raises_typed_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force ``import logfire`` to fail; configure_logfire raises typed error."""
    # Remove logfire from sys.modules (if cached) and intercept the import.
    monkeypatch.delitem(sys.modules, "logfire", raising=False)

    real_import = (
        __builtins__["__import__"]  # type: ignore[index]
        if isinstance(__builtins__, dict)
        else __builtins__.__import__
    )

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "logfire":
            raise ImportError("No module named 'logfire'")
        return real_import(name, *args, **kwargs)

    # Patch the builtin __import__ for the duration of the test.
    if isinstance(__builtins__, dict):
        monkeypatch.setitem(__builtins__, "__import__", fake_import)
    else:
        monkeypatch.setattr(__builtins__, "__import__", fake_import)

    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")

    with pytest.raises(LogfireMissingError) as exc_info:
        configure_logfire(_make_settings())

    assert "logfire" in str(exc_info.value).lower()
    assert is_configured() is False


# ---------------------------------------------------------------------------
# Functional: with_span.
# ---------------------------------------------------------------------------


def test_with_span_emits_via_logfire_when_configured(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    configure_logfire(_make_settings())

    with with_span("capo.test.span", foo="bar", n=42) as span:
        assert span is fake_logfire  # nullcontext returned fake here

    assert fake_logfire.span_calls == [
        ("capo.test.span", {"foo": "bar", "n": 42}),
    ]


def test_with_span_noop_when_unconfigured() -> None:
    """When Logfire is not configured, with_span returns a no-op CM."""
    with with_span("capo.test.span", foo="bar"):
        pass  # nothing should raise; nullcontext returns None


def test_with_span_tolerates_span_construction_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If logfire.span(...) raises, with_span falls back to a no-op CM."""
    fake = _FakeLogfire(span_raises=RuntimeError("span failure"))
    monkeypatch.setitem(sys.modules, "logfire", fake)  # type: ignore[arg-type]
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    configure_logfire(_make_settings())

    # Should not raise.
    with with_span("capo.test.span"):
        pass


# ---------------------------------------------------------------------------
# is_configured reflects state.
# ---------------------------------------------------------------------------


def test_is_configured_starts_false_and_flips_on_success(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGFIRE_TOKEN", "tok")
    assert is_configured() is False
    configure_logfire(_make_settings())
    assert is_configured() is True


def test_is_configured_stays_false_on_disabled(
    fake_logfire: _FakeLogfire,
) -> None:
    configure_logfire(_make_settings(enabled=False))
    assert is_configured() is False


def test_is_configured_stays_false_on_silent_skip(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    monkeypatch.setenv("LOGFIRE_IGNORE_NO_CONFIG", "1")
    configure_logfire(_make_settings(token=None))
    assert is_configured() is False


# ---------------------------------------------------------------------------
# Integration: configure with a real ObservabilityConfig instance.
# ---------------------------------------------------------------------------


def test_real_observability_config_roundtrip(
    fake_logfire: _FakeLogfire,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the real :class:`ObservabilityConfig` to confirm field shape."""
    from pydantic import SecretStr

    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    cfg = ObservabilityConfig(
        logfire_enabled=True,
        token=SecretStr("from-toml-token"),
        service_name="capo",
        environment="prod",
    )
    settings = _SettingsStub(cfg)

    configure_logfire(settings)

    call = fake_logfire.configure_calls[0]
    assert call["token"] == "from-toml-token"
    assert call["service_name"] == "capo"
    assert call["environment"] == "prod"
