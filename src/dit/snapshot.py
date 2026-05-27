"""Resolve the committed git ref a dit run records as its ``pipeline_commit``.

Under the no-dirty-tree policy (``docs/no-dirty-tree-pivot.md``), every dit
run executes a committed, pushed git ref. A dirty pipeline checkout is
auto-snapshotted to ``refs/dit-snapshots/<pipeline>/<sha>`` via
``scripts/snapshot.sh`` (deterministic orphan commit; see M-pivot-1) and that
ref's commit is recorded.

Two entry points create snapshots:

* ``make dit-cloud`` (shell) calls ``scripts/snapshot.sh`` directly, before
  the Cloud Build submit -- the laptop has the git-push credentials. The
  resolved commit is threaded into the build as the ``DIT_PIPELINE_COMMIT``
  env var so the workflow records it without re-snapshotting.
* Local ``dit run --runner=dataflow`` calls :func:`resolve_pipeline_commit`,
  which shells out to ``scripts/snapshot.sh`` from the laptop.

When ``DIT_PIPELINE_COMMIT`` is set, resolution is a no-op: the snapshot has
already happened and we just record the value.

Auto-snapshot requires an editable dit install (``pip install -e``) so the
``scripts/`` directory is locatable. This is not a real limitation: only an
editable pipeline install can be dirty in the first place. The ``-ref`` and
snapshot install modes point at an already-committed ref (clean), so they
never reach the snapshot path.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from pathlib import Path

from dit.git_info import git_info

logger = logging.getLogger(__name__)

ENV_PIPELINE_COMMIT = "DIT_PIPELINE_COMMIT"


def _dit_root() -> Path:
    # src/dit/snapshot.py -> repo root is parents[2] for an editable install.
    return Path(__file__).resolve().parents[2]


def snapshot_script() -> Path:
    return _dit_root() / "scripts" / "snapshot.sh"


def create_snapshot(repo_dir: str, pipeline: str) -> str:
    """Snapshot the dirty working tree of ``repo_dir`` and return the ref.

    Shells out to ``scripts/snapshot.sh``, which builds a deterministic
    orphan snapshot commit, pushes it to ``origin`` under
    ``refs/dit-snapshots/<pipeline>/<sha>``, and prints the ref to stdout.
    Raises if the script isn't locatable (non-editable dit install -- which
    can't have a dirty pipeline tree to snapshot anyway).
    """
    script = snapshot_script()
    if not script.exists():
        raise RuntimeError(
            f"snapshot script not found at {script}; auto-snapshot needs an "
            "editable dit install (pip install -e). For a non-editable install, "
            "run against an already-committed pipeline ref."
        )
    return subprocess.run(
        [str(script), pipeline, repo_dir],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


_SNAPSHOT_MSG_PREFIX = "dit snapshot of "
_FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")


def snapshot_parent(commit: str, repo_dir: str) -> str | None:
    """Return the parent SHA a snapshot commit was based on, or None.

    A dit snapshot commit's subject is ``dit snapshot of <40-char-sha>`` (set
    by ``scripts/snapshot.sh``). For such a commit, return ``<sha>``; for any
    other commit (a real branch/main commit, or one we can't read), return
    None. Recorded in ``dit_runs.pipeline_commit_parent`` so a snapshot run's
    reproduce context survives even if the snapshot ref is later deleted.
    """
    try:
        subject = subprocess.run(
            ["git", "log", "-1", "--format=%s", commit],
            cwd=repo_dir, check=True, capture_output=True, text=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    if not subject.startswith(_SNAPSHOT_MSG_PREFIX):
        return None
    candidate = subject[len(_SNAPSHOT_MSG_PREFIX):].strip()
    # snapshot.sh records the full 40-char HEAD sha. Validate the shape so a
    # malformed / hand-edited message can't write junk into the
    # pipeline_commit_parent column.
    if _FULL_SHA_RE.fullmatch(candidate):
        return candidate
    return None


def resolve_pipeline_commit(
    repo_dir: str,
    pipeline: str,
    *,
    runner: str,
    require_clean: bool = False,
) -> tuple[str, bool]:
    """Return ``(pipeline_commit, unreviewed)`` for this run.

    * ``DIT_PIPELINE_COMMIT`` set -> ``(value, True)``. Cloud path: the
      snapshot already happened on the laptop; just record it. Treated as
      unreviewed (a snapshot or ad-hoc ref, by construction).
    * clean tree -> ``(HEAD short sha, False)``. The reviewed/main path.
    * dirty + ``runner != "dataflow"`` -> ``(HEAD short sha, True)`` with NO
      snapshot. The docker runner executes the pipeline image locally against
      the mounted working tree; no remote workers means a pushed snapshot
      adds nothing. Recorded as unreviewed for provenance.
    * dirty + ``runner == "dataflow"`` + ``require_clean`` -> ``SystemExit``.
    * dirty + ``runner == "dataflow"`` -> auto-snapshot + push, return
      ``(snapshot short sha, True)``.
    """
    override = os.environ.get(ENV_PIPELINE_COMMIT, "").strip()
    if override:
        logger.info("pipeline_commit from %s: %s", ENV_PIPELINE_COMMIT, override)
        return override, True

    short, dirty = git_info(repo_dir)
    if not dirty:
        return short, False

    if runner != "dataflow":
        logger.warning(
            "working tree at %s is dirty; runner=%s executes the working tree "
            "directly (no snapshot needed). Recording as an unreviewed run.",
            repo_dir, runner,
        )
        return short, True

    if require_clean:
        raise SystemExit(
            "error: working tree is dirty and --require-clean was set. Commit + "
            "push your changes, or drop --require-clean to auto-snapshot the tree "
            "to refs/dit-snapshots/<pipeline>/<sha> and run against that."
        )

    ref = create_snapshot(repo_dir, pipeline)
    snapshot_short = subprocess.run(
        ["git", "rev-parse", "--short", ref],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()
    logger.info("auto-snapshot: pipeline_commit=%s (ref=%s)", snapshot_short, ref)
    return snapshot_short, True
