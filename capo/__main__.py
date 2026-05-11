"""Allow ``python -m capo`` to invoke the CLI entry point."""

from __future__ import annotations

from capo.main import main

if __name__ == "__main__":
    raise SystemExit(main())
