"""Shared git-tree introspection for dit workflows.

Hosts the canonical ``git_info`` helper used by the workflows and by
:mod:`dit.snapshot`.
"""

from __future__ import annotations

import subprocess


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
