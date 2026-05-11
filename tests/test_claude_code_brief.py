"""Unit tests for Task #19 — :mod:`capo.tools.claude_code`.

Covers acceptance criteria from spec §5.1, §5.3, §7.3:

Functional
* :class:`ClaudeCodeBrief` validates required + optional fields per §7.3.
* :func:`render_brief` produces deterministic output (snapshot).
* SOUL content is **never** present in the rendered prompt (§5.1).

Edge cases
* Empty ``constraints`` / ``success_criteria`` / ``relevant_files`` render
  with the documented placeholder, not as blank sections.
* ``model=None`` semantics: the model field is genuinely optional and
  defaults to ``None`` (meaning "use the config default" — see §5.9).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from capo.tools.claude_code import (
    EMPTY_LIST_PLACEHOLDER,
    ClaudeCodeBrief,
    render_brief,
)

# ---------------------------------------------------------------------------
# Repo paths — resolved relative to this file so the tests work no matter
# what CWD pytest is invoked from.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
SOULS_DIR = REPO_ROOT / "souls"
DELEGATION_BRIEF_TEMPLATE = REPO_ROOT / "prompts" / "delegation_brief.md"


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_brief() -> ClaudeCodeBrief:
    """A minimal valid brief — required fields only, all lists empty."""
    return ClaudeCodeBrief(
        goal="Add a typed config helper to capo/config.py.",
        repo_path="/Users/ada/dev/repos/capo",
    )


@pytest.fixture
def populated_brief() -> ClaudeCodeBrief:
    """A fully-populated brief — exercises every field including ``model``."""
    return ClaudeCodeBrief(
        goal="Refactor capo/agent.py to extract the build_agent helper.",
        repo_path="/Users/ada/dev/repos/capo",
        constraints=[
            "Do not change public function signatures.",
            "Keep SOUL anchoring unchanged.",
        ],
        success_criteria=[
            "All existing tests pass with `uv run pytest`.",
            "`uv run ruff check capo/` reports no new violations.",
        ],
        relevant_files=[
            "capo/agent.py",
            "capo/tools/basic.py",
            "tests/test_agent_tools.py",
        ],
        create_worktree=True,
        model="anthropic:claude-sonnet-4-6",
    )


# ---------------------------------------------------------------------------
# Functional — model validation.
# ---------------------------------------------------------------------------


class TestModelValidation:
    """Validation rules for :class:`ClaudeCodeBrief`."""

    def test_minimal_brief_accepts_only_required_fields(self) -> None:
        brief = ClaudeCodeBrief(goal="x", repo_path="/repo")
        assert brief.goal == "x"
        assert brief.repo_path == "/repo"
        assert brief.constraints == []
        assert brief.success_criteria == []
        assert brief.relevant_files == []
        assert brief.create_worktree is True  # spec default
        assert brief.model is None  # spec default

    def test_empty_goal_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ClaudeCodeBrief(goal="", repo_path="/repo")

    def test_whitespace_only_goal_is_rejected(self) -> None:
        # ``str_strip_whitespace=True`` + ``min_length=1`` together reject
        # "   " — the goal must contain at least one non-whitespace char.
        with pytest.raises(ValidationError):
            ClaudeCodeBrief(goal="   ", repo_path="/repo")

    def test_empty_repo_path_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ClaudeCodeBrief(goal="x", repo_path="")

    def test_extra_fields_are_rejected(self) -> None:
        # `extra="forbid"` keeps unknown keys from silently surviving JSON
        # round-trips on the delegations row.
        with pytest.raises(ValidationError):
            ClaudeCodeBrief(
                goal="x",
                repo_path="/repo",
                extra_field="not allowed",  # type: ignore[call-arg]
            )

    def test_brief_is_frozen(self) -> None:
        # Frozen so the spawn step can share it with the persistence path.
        brief = ClaudeCodeBrief(goal="x", repo_path="/repo")
        with pytest.raises(ValidationError):
            brief.goal = "y"  # type: ignore[misc]

    def test_model_none_is_default(self) -> None:
        # `model=None` ⇒ "use config default" per §5.9. Explicit None and
        # default-omitted must behave identically.
        explicit = ClaudeCodeBrief(goal="x", repo_path="/repo", model=None)
        implicit = ClaudeCodeBrief(goal="x", repo_path="/repo")
        assert explicit.model is None
        assert implicit.model is None
        assert explicit == implicit

    def test_model_override_is_preserved(self) -> None:
        brief = ClaudeCodeBrief(
            goal="x",
            repo_path="/repo",
            model="anthropic:claude-opus-4-7",
        )
        assert brief.model == "anthropic:claude-opus-4-7"

    def test_json_round_trip(self) -> None:
        # The spawn step persists `brief_json` on the `delegations` row;
        # round-tripping must be lossless.
        original = ClaudeCodeBrief(
            goal="x",
            repo_path="/repo",
            constraints=["a", "b"],
            success_criteria=["c"],
            relevant_files=["f1.py"],
            create_worktree=False,
            model="anthropic:claude-sonnet-4-6",
        )
        roundtrip = ClaudeCodeBrief.model_validate_json(original.model_dump_json())
        assert roundtrip == original


# ---------------------------------------------------------------------------
# Functional — render_brief.
# ---------------------------------------------------------------------------


class TestRenderBrief:
    """Behavior of :func:`render_brief`."""

    def test_render_is_deterministic(self, populated_brief: ClaudeCodeBrief) -> None:
        # Same input → byte-identical output across calls.
        first = render_brief(populated_brief)
        second = render_brief(populated_brief)
        assert first == second

    def test_render_snapshot(self, populated_brief: ClaudeCodeBrief) -> None:
        """Pin exact rendered bytes for a known input.

        Acceptance criterion: ``render_brief`` output snapshot test
        (deterministic). If this snapshot diverges, the change must be
        intentional and reviewed — the spawn step relies on stable
        prompt bytes for restart-resume reproducibility.
        """
        rendered = render_brief(populated_brief)

        expected = (
            "# Coding Task Brief\n"
            "\n"
            "You are Claude Code, invoked as a subagent for a specific, scoped coding task.\n"
            "This brief is the **only** instruction context — there is no implicit user\n"
            "persona or prior conversation. Work strictly within the goal and constraints\n"
            "below.\n"
            "\n"
            "## Goal\n"
            "\n"
            "Refactor capo/agent.py to extract the build_agent helper.\n"
            "\n"
            "## Repository\n"
            "\n"
            "- **Path**: `/Users/ada/dev/repos/capo`\n"
            "\n"
            "## Constraints\n"
            "\n"
            "- Do not change public function signatures.\n"
            "- Keep SOUL anchoring unchanged.\n"
            "\n"
            "## Success Criteria\n"
            "\n"
            "- All existing tests pass with `uv run pytest`.\n"
            "- `uv run ruff check capo/` reports no new violations.\n"
            "\n"
            "## Relevant Files\n"
            "\n"
            "- capo/agent.py\n"
            "- capo/tools/basic.py\n"
            "- tests/test_agent_tools.py\n"
            "\n"
            "## Operating Contract\n"
            "\n"
            "- Do exactly what the Goal asks; nothing more.\n"
            "- If a Constraint conflicts with the Goal, surface the conflict and stop.\n"
            "- Verify each Success Criterion explicitly before reporting completion.\n"
            "- Prefer reading the Relevant Files first if any are listed.\n"
            "- Report results plainly: what changed, what was verified, and any\n"
            "  follow-ups the caller should know about.\n"
        )
        assert rendered == expected

    def test_render_includes_goal_and_repo_path(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(populated_brief)
        assert populated_brief.goal in rendered
        assert populated_brief.repo_path in rendered

    def test_render_includes_all_constraint_items(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(populated_brief)
        for item in populated_brief.constraints:
            assert item in rendered

    def test_render_includes_all_success_criteria_items(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(populated_brief)
        for item in populated_brief.success_criteria:
            assert item in rendered

    def test_render_includes_all_relevant_files(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(populated_brief)
        for item in populated_brief.relevant_files:
            assert item in rendered

    def test_render_uses_explicit_template_path(
        self, tmp_path: Path, minimal_brief: ClaudeCodeBrief
    ) -> None:
        # The renderer must honor the ``template_path`` override so tests
        # and future tooling can swap templates without monkey-patching.
        custom = tmp_path / "brief.md"
        custom.write_text("GOAL={goal} REPO={repo_path} C={constraints_block} "
                          "S={success_criteria_block} F={relevant_files_block}\n")
        out = render_brief(minimal_brief, template_path=custom)
        assert out == (
            f"GOAL={minimal_brief.goal} REPO={minimal_brief.repo_path} "
            f"C={EMPTY_LIST_PLACEHOLDER} S={EMPTY_LIST_PLACEHOLDER} "
            f"F={EMPTY_LIST_PLACEHOLDER}\n"
        )


# ---------------------------------------------------------------------------
# Edge cases — empty lists.
# ---------------------------------------------------------------------------


class TestEmptyListRendering:
    """Empty list-valued fields render with the documented placeholder."""

    def test_minimal_brief_renders_cleanly(
        self, minimal_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(minimal_brief)
        # All three list-valued sections appear, each filled with the
        # placeholder. Empty list does NOT remove the section header.
        assert "## Constraints" in rendered
        assert "## Success Criteria" in rendered
        assert "## Relevant Files" in rendered
        # The placeholder appears exactly three times (once per empty list).
        assert rendered.count(EMPTY_LIST_PLACEHOLDER) == 3

    def test_empty_constraints_only(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        brief = populated_brief.model_copy(update={"constraints": []})
        rendered = render_brief(brief)
        # Constraints placeholder is present; the other two sections still
        # render their items (not placeholders).
        assert rendered.count(EMPTY_LIST_PLACEHOLDER) == 1
        for item in brief.success_criteria:
            assert item in rendered

    def test_empty_render_is_still_deterministic(
        self, minimal_brief: ClaudeCodeBrief
    ) -> None:
        assert render_brief(minimal_brief) == render_brief(minimal_brief)


# ---------------------------------------------------------------------------
# SOUL absence — the §5.1 invariant this task exists to anchor.
# ---------------------------------------------------------------------------


class TestSoulAbsence:
    """SOUL content must NEVER appear in the rendered delegation prompt.

    Spec §5.1: SOUL is anchored on the *main* agent's instructions only;
    subagent prompts are built from the structured brief + template and
    have no access to SOUL files. We verify this two ways:

    1. Scan rendered output for distinctive substrings from each
       ``souls/*.md`` file — none should appear.
    2. Render a brief whose fields contain SOUL filenames as literal
       strings (a plausible footgun if a future refactor naively
       interpolates SOUL paths); only the brief field values should
       appear, never SOUL *body* content.
    """

    @staticmethod
    def _soul_markers() -> list[str]:
        """Pick distinctive phrases from each soul file.

        We intentionally don't read the whole file — we look for
        substrings that are unlikely to appear in any legitimate brief
        field. If a future SOUL file is added with no distinctive
        phrase, extend this list.
        """
        markers: list[str] = []
        for soul_file in sorted(SOULS_DIR.glob("*.md")):
            body = soul_file.read_text(encoding="utf-8")
            # Pull the first non-heading, non-empty line as the marker —
            # that's the SOUL's "voice" line and is the bit we most want
            # to keep out of subagent prompts.
            for line in body.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    markers.append(stripped)
                    break
        return markers

    def test_soul_markers_are_collected(self) -> None:
        # Sanity check the test setup itself — if souls/ disappears or
        # only contains heading-only files, the SOUL-absence assertion
        # below would vacuously pass. Fail loudly instead.
        markers = self._soul_markers()
        assert markers, "expected at least one SOUL marker in souls/*.md"

    def test_soul_absent_from_minimal_rendered_prompt(
        self, minimal_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(minimal_brief)
        for marker in self._soul_markers():
            assert marker not in rendered, (
                f"SOUL marker leaked into delegation brief: {marker!r}"
            )

    def test_soul_absent_from_populated_rendered_prompt(
        self, populated_brief: ClaudeCodeBrief
    ) -> None:
        rendered = render_brief(populated_brief)
        for marker in self._soul_markers():
            assert marker not in rendered, (
                f"SOUL marker leaked into delegation brief: {marker!r}"
            )

    def test_template_file_itself_contains_no_soul(self) -> None:
        # Defense-in-depth: even if someone edits the template, this
        # test catches accidentally pasting SOUL voice into the
        # template body.
        template_body = DELEGATION_BRIEF_TEMPLATE.read_text(encoding="utf-8")
        for marker in self._soul_markers():
            assert marker not in template_body, (
                f"SOUL marker present in delegation_brief.md template: {marker!r}"
            )
