"""capo — a routing agent that delegates to Claude Code and Codex via AMC.

This is the top-level package. The runtime entry point lives in ``capo.main``;
``__main__.py`` allows ``python -m capo`` to invoke it.

See ``internal/specs/capo-SPEC.md`` and ``internal/blueprints/capo-blueprint.md``
for the authoritative design.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
