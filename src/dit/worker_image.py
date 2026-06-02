"""Auto-build a pipeline image for both Dataflow workers and the docker runner.

Two consumers need a registry-pullable image that matches the pipeline code
this dit run actually executes:

* **Dataflow workers** (Beam consumers: pipe-gaps, port-visits). The submitter
  installs pipeline code from the snapshot, but Beam workers execute transforms
  from the container named by ``--sdk_container_image``. If a run executes
  unreviewed code (snapshot / dirty / unmerged) against the default published
  worker image, the workers silently run the *old published* code -- the
  submitter-vs-worker split. M-pivot-4 closed that gap by auto-building a
  worker image from the pipeline source.
* **The docker runner** (pipe-events). The runner ``docker run``s a published
  image directly. The "default" image is a *local* compose tag
  (``gfw/pipe-events``) that does NOT resolve in cloud (ditbox / Cloud Build)
  -- nothing pushed it there. In cloud mode the docker runner needs a real
  registry path to pull from, and that's the same auto-build artefact.

Both consumers want the same artefact: a content-addressable, kaniko-built
image at ``gcr.io/world-fishing-827/dit/<pipeline>:dit-<pipeline_commit>``.
:func:`ensure_pipeline_image` is the single entry point producing it. The
build is idempotent -- the tag is ``dit-<pipeline_commit>``, so an unchanged
tree resolves to the same tag and an existing image is reused (no rebuild).

The trigger differs slightly between the two consumers:

* **Dataflow worker** (``runner="dataflow"``): build only when the run is
  unreviewed (reviewed code is assumed already in the default published
  worker image). An explicit ``--worker-image`` is always respected.
* **Docker runner** (``runner="docker"``): build when ``need_registry_image``
  is set (the caller has decided "the local default is unreachable from this
  runtime" -- typically cloud mode where the compose-local tag won't pull).
  For pipe-events there is no upstream published image at all, so the
  registry artefact is the only way to run from a non-local context.

The kaniko Cloud Build (``docker/worker-image/cloudbuild.yaml``) targets the
pipeline's Dockerfile ``prod`` stage by default; both Beam pipelines and
pipe-events expose a ``prod`` target that bakes the runtime the consumer
needs. The build cache lives at
``gcr.io/world-fishing-827/github.com/globalfishingwatch/kaniko-cache`` (an
existing wf827 path the ditbox + worker-image builds share -- preserve, do
not regress).
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


def pipeline_image_tag(pipeline: str, commit: str) -> str:
    """Content-addressable pipeline-image tag for a pipeline + commit.

    Used for BOTH the Dataflow worker-image consumer and the docker-runner
    consumer (same artefact, different consumer; see module docstring).
    """
    return f"{IMAGE_NAMESPACE}/{pipeline}:dit-{commit}"


# Backwards-compatible alias for the prior name. The function never produced
# anything Dataflow-specific -- it produced "the pipeline's image" -- but the
# original name was coined when only Beam-worker consumption existed.
worker_image_tag = pipeline_image_tag


def _image_exists(tag: str) -> bool:
    """True iff ``tag`` already resolves in the registry (gcr.io)."""
    try:
        proc = subprocess.run(
            ["gcloud", "container", "images", "describe", tag],
            capture_output=True, text=True,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "gcloud not found on PATH; pipeline-image auto-build needs the "
            "Cloud SDK. Install it, or pass an explicit image override so dit "
            "skips the build."
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
            "git not found on PATH; pipeline-image auto-build needs git to "
            "export the commit tree."
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
            "tar not found on PATH; pipeline-image auto-build needs tar to "
            "unpack the commit tree."
        ) from e


def _build_and_push(repo_dir: str, tag: str, commit: str) -> None:
    """Submit the kaniko Cloud Build that builds the pipeline's prod target.

    The build context is ``commit``'s tree (materialised via ``git archive``),
    NOT ``repo_dir``'s working tree. This is load-bearing for a ``--REF`` run:
    the submitter installs ``<commit>`` (``git+file://...@<ref>``), but
    ``repo_dir`` may hold a *different* checkout -- a dirty tree, or a branch
    other than the ref. Building from the working tree would put the consumer
    (Beam workers or the docker runner) on different code than the submitter
    -- exactly the mismatch this function exists to close. ``git archive``
    also excludes untracked files and ``.git`` from the context (the
    .gcloudignore hardening against shipping rogue .env/sa.json still applies
    on top).
    """
    config = _dit_root() / "docker" / "worker-image" / "cloudbuild.yaml"
    if not config.exists():
        raise RuntimeError(
            f"pipeline-image build config not found at {config}; auto-build "
            "needs an editable dit install (pip install -e)."
        )
    ignore_file = _dit_root() / ".gcloudignore"
    with tempfile.TemporaryDirectory(prefix="dit-pipeline-ctx-") as ctx:
        _export_commit_tree(repo_dir, commit, ctx)
        cmd = ["gcloud", "builds", "submit", f"--config={config}"]
        if ignore_file.exists():
            cmd.append(f"--ignore-file={ignore_file}")
        cmd += [f"--substitutions=_IMAGE={tag}", ctx]
        logger.info(
            "building pipeline image %s from %s@%s (kaniko Cloud Build)",
            tag, repo_dir, commit,
        )
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError as e:
            raise RuntimeError(
                "gcloud not found on PATH; pipeline-image auto-build needs the "
                "Cloud SDK. Install it, or pass an explicit image override so "
                "dit skips the build."
            ) from e


def ensure_pipeline_image(
    *,
    pipeline: str,
    repo_dir: str,
    commit: str,
    runner: str,
    unreviewed: bool,
    worker_image: str,
    default_worker_image: str,
    need_registry_image: bool = False,
) -> str:
    """Return the pipeline image the run should use, building one if needed.

    Serves both consumers (see module docstring):

    * **Dataflow** (``runner == "dataflow"``): builds when ALL of (a)
      ``worker_image == default_worker_image`` and (b) ``unreviewed``. An
      explicit ``--worker-image`` is always respected; reviewed code is
      assumed already published in the default worker image.
    * **Docker runner** (``runner == "docker"``): builds when ``worker_image
      == default_worker_image`` AND ``need_registry_image`` is True. The
      ``unreviewed`` flag does NOT gate the docker-runner path: the default
      tag is a *local* compose identifier (e.g. ``gfw/pipe-events``) that does
      not resolve in any cloud context whether the code is reviewed or not, so
      a registry artefact is needed unconditionally when the caller signals
      "this runtime can't reach the local default". Laptop callers leave
      ``need_registry_image=False`` and keep using the compose
      ``build_from_source`` path; cloud callers (e.g. inside ditbox / Cloud
      Build) set ``need_registry_image=True`` so the runner has something it
      can ``docker run`` against.

    Returns the (possibly newly-built) image tag. Idempotent: if the
    ``dit-<commit>`` tag already exists in the registry, the build is skipped.
    """
    if runner == "dataflow":
        if worker_image != default_worker_image:
            logger.info(
                "worker image overridden (%s); not auto-building", worker_image,
            )
            return worker_image
        if not unreviewed:
            return worker_image
        trigger_reason = (
            f"run executes unreviewed code ({commit}) against the default "
            "worker image"
        )
    elif runner == "docker":
        if not need_registry_image:
            # Laptop docker-runner path: the local compose tag is fine; the
            # runner builds from source via docker compose.
            return worker_image
        if worker_image != default_worker_image:
            logger.info(
                "docker image overridden (%s); not auto-building", worker_image,
            )
            return worker_image
        trigger_reason = (
            f"docker-runner cloud mode: local default {worker_image!r} won't "
            "resolve in this runtime; auto-building a registry image"
        )
    else:
        # Unknown runner: opt out (be conservative; never surprise-build).
        return worker_image

    tag = pipeline_image_tag(pipeline, commit)
    if _image_exists(tag):
        logger.info("pipeline image %s already present; skipping build", tag)
        return tag

    logger.warning(
        "%s; building a content-addressable image so the consumer actually "
        "runs this code.", trigger_reason,
    )
    _build_and_push(repo_dir, tag, commit)
    return tag


# Backwards-compatible alias for the prior name. The function never produced
# anything Dataflow-specific -- it produced "the pipeline's image" -- but the
# original name was coined when only Beam-worker consumption existed. The new
# name (``ensure_pipeline_image``) is preferred; the alias keeps any external
# importer functional during the transition.
ensure_worker_image = ensure_pipeline_image
