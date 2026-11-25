"""Shared git-tree introspection for dit workflows.

Hosts the canonical ``git_info`` helper used by the workflows and by
:mod:`dit.snapshot`.
"""

from __future__ import annotations

import subprocess
from collections.abc import Sequence


def git_info(repo_dir: str) -> tuple[str, bool]:
    """Return ``(short_sha, dirty)`` for the git checkout at ``repo_dir``.

    ``dirty`` reflects tracked-file modifications + deletions only
    (``git status --porcelain --untracked-files=no``); untracked files do
    not count as dirty, matching the snapshot capture boundary in
    ``scripts/snapshot.sh`` (``git add -u``). Centralised here so the
    workflows and :func:`dit.snapshot.resolve_pipeline_commit` agree on what
    "dirty" means.
    """
    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    porcelain = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    return short, bool(porcelain)


def show_file(repo_dir: str, ref: str, path: str) -> str | None:
    """The content of ``path`` at ``ref`` in the checkout at ``repo_dir``, or
    ``None`` when git cannot give it: no such path at that ref, no such ref,
    or no repository there.

    Read with ``git show <ref>:<path>``: no worktree is created and the
    checkout's working tree is not touched. For reading one file of a
    binding's ref before anything is run for it.
    """
    result = subprocess.run(
        ["git", "-C", repo_dir, "show", f"{ref}:{path}"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout if result.returncode == 0 else None


def first_file(repo_dir: str, ref: str, paths: Sequence[str]) -> tuple[str, str] | None:
    """``(path, content)`` of the first of ``paths`` that exists at ``ref``,
    or ``None``. For a file whose place moved between a repository's versions."""
    found = ((path, show_file(repo_dir, ref, path)) for path in paths)
    return next(((path, content) for path, content in found if content is not None), None)
