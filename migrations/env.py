"""Alembic environment for Capo ``state.db`` migrations.

Resolves the target SQLite URL with this precedence (highest first):

1. ``-x dburl=...`` passed to the alembic CLI (``context.get_x_argument``).
2. ``CAPO_STATE_DB`` environment variable. May be a bare filesystem path
   (e.g. ``/tmp/state.db``) or a full SQLAlchemy URL
   (``sqlite:////tmp/state.db``).
3. ``sqlalchemy.url`` from ``alembic.ini``.

Foreign-key enforcement is enabled per-connection via ``PRAGMA foreign_keys=ON``
so the migration's FK definitions are honored at apply time (SQLite defaults
to FKs *off*).

This module is intentionally tiny and dependency-light: it does NOT import
``capo.config`` so it can run in CI or against a fresh checkout without any
``config.toml`` present.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, event, pool
from sqlalchemy.engine import Engine

# Alembic Config object — gives us access to ``alembic.ini`` values.
config = context.config

# Configure Python logging from the alembic.ini ``[loggers]`` section.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Capo manages schema via raw SQL in ``versions/001_init.py``; no SQLAlchemy
# metadata is required for autogenerate.
target_metadata = None


def _resolve_db_url() -> str:
    """Resolve the SQLite URL using the documented precedence chain."""
    x_args = context.get_x_argument(as_dictionary=True)
    if "dburl" in x_args and x_args["dburl"]:
        return x_args["dburl"]

    env_value = os.environ.get("CAPO_STATE_DB")
    if env_value:
        # Accept either a full URL or a bare path. Bare paths become
        # ``sqlite:///<abs-path>`` (three slashes for relative, four for abs).
        if "://" in env_value:
            return env_value
        if env_value.startswith("/"):
            return f"sqlite:///{env_value}"
        return f"sqlite:///{env_value}"

    ini_url = config.get_main_option("sqlalchemy.url")
    if not ini_url:
        raise RuntimeError(
            "Alembic could not resolve a database URL. Set CAPO_STATE_DB, "
            "pass -x dburl=sqlite:///path, or set sqlalchemy.url in "
            "alembic.ini."
        )
    return ini_url


def _enable_sqlite_foreign_keys(engine: Engine) -> None:
    """Attach a ``connect`` listener that turns on FK enforcement.

    SQLite defaults to ``PRAGMA foreign_keys = OFF`` on every new connection;
    we must opt in or the FK constraints declared in the migration are silently
    ignored. This matches the §7.3 pragma list applied by
    ``capo.memory.store.open_connection``.
    """

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, _connection_record):  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys = ON")
        finally:
            cursor.close()


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting to a DB."""
    url = _resolve_db_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Open a real connection and run the migrations."""
    url = _resolve_db_url()

    # Override the alembic.ini URL with the resolved one so
    # ``engine_from_config`` picks it up.
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = url

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    _enable_sqlite_foreign_keys(connectable)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
