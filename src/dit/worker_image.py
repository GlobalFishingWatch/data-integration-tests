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
    proc = subprocess.run(
        ["gcloud", "container", "images", "describe", tag],
        capture_output=True, text=True,
    )
    return proc.returncode == 0


def _build_and_push(repo_dir: str, tag: str) -> None:
    """Submit the kaniko Cloud Build that builds the pipeline's prod target."""
    config = _dit_root() / "docker" / "worker-image" / "cloudbuild.yaml"
    if not config.exists():
        raise RuntimeError(
            f"worker-image build config not found at {config}; auto-build needs "
            "an editable dit install (pip install -e)."
        )
    logger.info("building worker image %s from %s (kaniko Cloud Build)", tag, repo_dir)
    subprocess.run(
        [
            "gcloud", "builds", "submit",
            f"--config={config}",
            f"--substitutions=_IMAGE={tag}",
            repo_dir,
        ],
        check=True,
    )


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
    _build_and_push(repo_dir, tag)
    return tag
