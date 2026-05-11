"""Smoke test: can DBOS 2.21.0 launch against a SQLite system database?"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from dbos import DBOS, DBOSConfig


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="s2-smoke-"))
    db_path = workdir / "dbos.db"
    config: DBOSConfig = {
        "name": "s2-smoke",
        "system_database_url": f"sqlite:///{db_path}",
        "run_admin_server": False,
    }
    DBOS(config=config)

    @DBOS.workflow()
    def hello(name: str) -> str:
        return greet(name)

    @DBOS.step()
    def greet(name: str) -> str:
        return f"hi {name}"

    DBOS.launch()
    try:
        result = hello("world")
        print(f"OK result={result!r} db={db_path} exists={db_path.exists()} size={db_path.stat().st_size if db_path.exists() else 0}")
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
