# Getting Started

This guide takes you from a clean checkout to a running Capo process. Capo is a single long-lived Python 3.12 service that receives AMC webhooks, routes each turn through a Pydantic AI agent, and delegates heavy work to Claude Code and Codex subprocesses. You install and run everything through [`uv`](https://docs.astral.sh/uv/).

The fastest path to confidence is the [smoke test](#smoke-test): `uv run capo --config ./config.toml --no-serve` validates your settings and builds the agent without binding any sockets.

## Prerequisites

Capo validates the external CLI binaries at boot, so install these before your first real run.

| Tool | Minimum version | Purpose |
|---|---|---|
| **Python** | 3.12+ | Runtime (`pyproject` pins `requires-python = ">=3.12"`). |
| **`uv`** (Astral) | latest | Dependency management and the `capo` entry point. |
| **`claude`** (Claude Code CLI) | 2.1.138 | The primary delegation target; version is enforced at boot. |
| **`codex`** (Codex CLI) | 0.130.0 | Secondary delegation target; version is enforced at boot. |
| **`litestream`** | 0.5.x+ | SQLite replication. Optional for a first local run. |

!!! note "Boot-time binary precheck"
    Before DBOS initializes, Capo runs a version precheck against `claude` and `codex` (each with a 5-second timeout). A missing, unparseable, or out-of-date binary raises `BinaryPrecheckError` and exits with code 2.

    For CI and smoke runs where the delegation CLIs aren't installed, you can skip the precheck entirely:

    ``` toml
    [boot]
    skip_binary_precheck = true
    ```

    Use this only for boot validation — a production run that delegates work still needs the real binaries.

## Install

Sync the locked dependency set from the repo root:

``` bash
uv sync
```

!!! note "uv manages the virtual environment"
    `uv sync` creates and populates a project-local `.venv` from `pyproject.toml` and the lockfile. You don't need to activate it manually — prefix commands with `uv run` (for example `uv run capo --version`) and `uv` resolves the right interpreter and dependencies automatically.

Verify the install with the version flag, which needs no config and no external binaries:

``` bash
uv run capo --version
```

This prints `capo <version>` and exits 0.

## Configure

To actually run, Capo needs a `config.toml` plus a `.env` (secrets) **beside it** in the same directory. The `.env` file is auto-discovered at `<config_dir>/.env`.

!!! warning "No example config is checked into the repo"
    There is intentionally no `config.toml` committed to the repository. The canonical template lives in `internal/ops/RUNBOOK.md` §2.3. Copy it, then fill in the required secrets. See the [Configuration](configuration.md) page for the full schema.

Two secrets in `.env` are **hard-required** — Capo will not boot without them:

| Variable | Purpose |
|---|---|
| `AMC_WEBHOOK_SECRET` | Validates inbound AMC webhook signatures. |
| `AMC_BEARER_TOKEN` | Authenticates outbound AMC REST sends. |

In addition, provide a provider key for whichever models you use:

- `ANTHROPIC_API_KEY` — for Anthropic / Claude models
- `OPENAI_API_KEY` — for OpenAI models

Set one or both depending on your configured models.

!!! info "Settings are immutable at runtime"
    Capo loads settings once at boot and never hot-reloads them. **A config change means a restart.** There is no SIGHUP reload and no live config endpoint.

For the complete option reference — model selection, channel limits, compaction, retention, observability — see the [Configuration](configuration.md) page.

## Apply database migrations

Capo uses two SQLite files: `state.db` (your Alembic-managed application domain) and `dbos.db` (managed entirely by DBOS). Apply the `state.db` schema with Alembic:

``` bash
uv run alembic upgrade head
```

This runs the migration chain in order:

```
001_init → 002_approvals → 003_approvals_request_types → 004_costs
```

!!! warning "Run Alembic from the repo root — never against `dbos.db`"
    `alembic.ini` resolves paths relative to the repository root, so you must run `uv run alembic upgrade head` from the repo root or migrations will target the wrong location.

    **Never** point Alembic at `dbos.db`. DBOS owns and manages its own schema; running Alembic against it corrupts the workflow store. Alembic touches `state.db` only.

## Smoke test

The `--no-serve` flag is the canonical boot smoke test. It loads and validates your settings, runs the binary precheck, and builds the Pydantic AI agent — then exits **without** starting the webhook listener or binding any sockets:

``` bash
uv run capo --config ./config.toml --no-serve
```

**Success looks like:** the process exits with code **0** and no error printed to stderr. That confirms your `config.toml`, `.env` secrets, binary versions, and agent build are all valid.

If a binary version is too old or missing, this is also where you'll see a one-line `BinaryPrecheckError` and exit code 2 — fix the binary (or set `skip_binary_precheck` for CI) and re-run.

## Run Capo

Once the smoke test passes, start the full process. This binds the FastAPI/uvicorn AMC webhook listener and runs the dispatcher, cold-boot resume sweep, and unread sweep:

``` bash
uv run capo --config ./config.toml
```

The process is long-lived and stays in the foreground. It shuts down cleanly on `SIGINT` / `SIGTERM` (Ctrl-C). See [Architecture](architecture.md) for what happens to an inbound message once the listener is up.

## Run the tests

Run the full suite (configured with `asyncio_mode = "auto"` and `-ra --strict-markers`):

``` bash
uv run pytest -q
```

Lint the package with Ruff (line length 100, target `py312`):

``` bash
uv run ruff check capo/
```

!!! note "Heavier checkpoint tests"
    Phase-checkpoint tests (`tests/test_phaseN_checkpoint.py`) launch a real DBOS instance against a temporary SQLite database, exercising the full durable-workflow path. They're slower than the rest of the suite but verify end-to-end behavior.

## CLI reference

`capo` exposes exactly three flags (from `capo/main.py`):

| Flag | Type | Behavior |
|---|---|---|
| `--config PATH` | optional value | Path to `config.toml`. Resolution precedence: `--config` > `$CAPO_CONFIG` > `./config.toml`. If `--config` is given but the file is missing, Capo exits **2** with a clear error. |
| `--no-serve` | flag | Load settings and build the agent, then exit **0** without starting the webhook listener. For smoke tests / CI. |
| `--version` | flag | Print `capo <version>` and exit 0. Works with no config present. |

## Boot errors

Every fatal boot failure follows the same idiom: a single-line message to **stderr** (no stack trace) and exit code **2**. The typed errors you may encounter are:

- `ConfigError` — invalid or missing config / settings
- `BinaryPrecheckError` — `claude` or `codex` missing, unparseable, or below the minimum version
- `AgentBuildError` — the Pydantic AI agent failed to build
- `DBOSInitError` — DBOS failed to initialize its workflow store
- `LogfireMissingError` — required observability wiring is unavailable

When a boot command exits non-zero, read the single stderr line — it names the offending path or binary directly.

## Next steps

- **[Configuration](configuration.md)** — the full `config.toml` schema, the `.env` secret reference, and the RUNBOOK template location.
- **[Architecture](architecture.md)** — how the webhook listener, per-channel queues, agent loop, and DBOS workflows fit together.
- **[Operations](operations.md)** — Litestream replication, paired restore, launchd supervision, and the production runbook.
