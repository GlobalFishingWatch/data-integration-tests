"""Auto-build the Dataflow worker image so unreviewed code actually runs.

A ``--runner=dataflow`` run loads pipeline code from two places: the submitter
constructs the pipeline from the installed/snapshotted source, but the
**workers** execute transforms from the container named by ``--worker-image``,
which defaults to a published registry image. So if a run executes unreviewed
code (a snapshot / dirty tree / unmerged branch) against the default worker
image, the workers silently run the *old published* code and the run is
meaningless. The snapshot mechanism (M-pivot-1/2) fixes submitter-side
reproducibility but cannot fix this -- the worker image is a separate artifact
on a separate (container-registry) channel.

:func:`ensure_worker_image` closes the gap: when a run is unreviewed and the
caller left ``--worker-image`` at the default, it builds a content-addressable
worker image from the pipeline source via a kaniko Cloud Build
(``docker/worker-image/cloudbuild.yaml``) and returns it. The build is
idempotent -- the tag is ``dit-<pipeline_commit>``, so an unchanged tree
resolves to the same tag and an existing image is reused (no rebuild).

One mechanism covers both entry points: the workflow calls this from ``main()``,
so it runs whether the workflow was launched by ``make dit-cloud`` (inside
ditbox -- a nested Cloud Build, kept fast by the shared kaniko cache) or
locally (``dit run --runner=dataflow``, submitting from the laptop).
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# dit images live under gcr.io/world-fishing-827/dit/* (docs/conventions.md).
IMAGE_NAMESPACE = "gcr.io/world-fishing-827/dit"


def _dit_root() -> Path:
    # src/dit/worker_image.py -> repo root is parents[2] for an editable install.
    return Path(__file__).resolve().parents[2]


def worker_image_tag(pipeline: str, commit: str) -> str:
    """Content-addressable worker-image tag for a pipeline + commit."""
    return f"{IMAGE_NAMESPACE}/{pipeline}:dit-{commit}"


def _image_exists(tag: str) -> bool:
    """True iff ``tag`` already resolves in the registry (gcr.io)."""
    try:
        proc = subprocess.run(
            ["gcloud", "container", "images", "describe", tag],
            capture_output=True, text=True,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "gcloud not found on PATH; worker-image auto-build needs the Cloud "
            "SDK. Install it, or pass an explicit --worker-image so dit skips "
            "the build."
        ) from e
    return proc.returncode == 0


def _export_commit_tree(repo_dir: str, commit: str, dest: str) -> None:
    """Materialise ``commit``'s tracked tree into ``dest`` (``git archive``).

    ``git archive <commit> | tar -x -C dest`` exports exactly the commit's tree
    -- no working-tree state, no untracked files, no ``.git``.
    ``--end-of-options`` guards against a commit-ish that starts with ``-``.
    """
    try:
        archive = subprocess.run(
            ["git", "archive", "--format=tar", "--end-of-options", commit],
            cwd=repo_dir, check=True, capture_output=True,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "git not found on PATH; worker-image auto-build needs git to export "
            "the commit tree."
        ) from e
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="replace") if e.stderr else ""
        raise RuntimeError(
            f"`git archive {commit}` in {repo_dir} failed (is the commit present "
            f"there?): {stderr}"
        ) from e
    try:
        subprocess.run(["tar", "-x", "-C", dest], input=archive.stdout, check=True)
    except FileNotFoundError as e:
        raise RuntimeError(
            "tar not found on PATH; worker-image auto-build needs tar to unpack "
            "the commit tree."
        ) from e


def _build_and_push(repo_dir: str, tag: str, commit: str) -> None:
    """Submit the kaniko Cloud Build that builds the pipeline's prod target.

    The build context is ``commit``'s tree (materialised via ``git archive``),
    NOT ``repo_dir``'s working tree. This is load-bearing for a ``--REF`` run:
    the submitter installs ``<commit>`` (``git+file://...@<ref>``), but
    ``repo_dir`` may hold a *different* checkout -- a dirty tree, or a branch
    other than the ref. Building from the working tree would put the workers on
    different code than the submitter, which is exactly the submitter-vs-worker
    mismatch this function exists to close. ``git archive`` also excludes
    untracked files and ``.git`` from the context (the .gcloudignore hardening
    against shipping rogue .env/sa.json still applies on top).
    """
    config = _dit_root() / "docker" / "worker-image" / "cloudbuild.yaml"
    if not config.exists():
        raise RuntimeError(
            f"worker-image build config not found at {config}; auto-build needs "
            "an editable dit install (pip install -e)."
        )
    ignore_file = _dit_root() / ".gcloudignore"
    with tempfile.TemporaryDirectory(prefix="dit-worker-ctx-") as ctx:
        _export_commit_tree(repo_dir, commit, ctx)
        cmd = ["gcloud", "builds", "submit", f"--config={config}"]
        if ignore_file.exists():
            cmd.append(f"--ignore-file={ignore_file}")
        cmd += [f"--substitutions=_IMAGE={tag}", ctx]
        logger.info(
            "building worker image %s from %s@%s (kaniko Cloud Build)",
            tag, repo_dir, commit,
        )
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError as e:
            raise RuntimeError(
                "gcloud not found on PATH; worker-image auto-build needs the Cloud "
                "SDK. Install it, or pass an explicit --worker-image so dit skips "
                "the build."
            ) from e


def ensure_worker_image(
    *,
    pipeline: str,
    repo_dir: str,
    commit: str,
    runner: str,
    unreviewed: bool,
    worker_image: str,
    default_worker_image: str,
) -> str:
    """Return the worker image the run should use, building one if needed.

    Builds (and returns) a content-addressable image only when **all** of:

    * ``runner == "dataflow"`` -- the docker runner builds from the mounted
      source, so there's no submitter/worker split to bridge;
    * ``worker_image == default_worker_image`` -- an explicit ``--worker-image``
      is always respected;
    * ``unreviewed`` -- reviewed/main code is assumed already present in the
      default published image; only unreviewed code (snapshot / dirty /
      unmerged) is missing from it.

    Otherwise returns ``worker_image`` unchanged. Idempotent: if the
    ``dit-<commit>`` tag already exists, the build is skipped.
    """
    if runner != "dataflow":
        return worker_image
    if worker_image != default_worker_image:
        logger.info("worker image overridden (%s); not auto-building", worker_image)
        return worker_image
    if not unreviewed:
        return worker_image

    tag = worker_image_tag(pipeline, commit)
    if _image_exists(tag):
        logger.info("worker image %s already present; skipping build", tag)
        return tag

    logger.warning(
        "run executes unreviewed code (%s) against the default worker image; "
        "auto-building a worker image so the workers actually run it.", commit,
    )
    _build_and_push(repo_dir, tag, commit)
    return tag
