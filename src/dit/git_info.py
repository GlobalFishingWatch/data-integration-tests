"""Shared git-tree introspection for dit workflows.

Today this module just hosts the submitter/worker-image mismatch warning.
The per-workflow ``_git_info()`` helpers stay duplicated for now (per the
"duplicate until 3" rule); promote them here when a third workflow lands.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


def warn_if_worker_image_misses_dirty_tree(
    *,
    dirty_fn: Callable[[], bool],
    repo_dir: str,
    runner: str,
    worker_image: str,
    default_worker_image: str,
) -> None:
    """Log a prominent warning when the submitter is running a dirty tree but
    Dataflow workers will pull the default registry image (which won't
    contain those changes).

    Triggered iff all of:

    * ``runner == "dataflow"`` (the docker runner builds the image from
      source, so there is no submitter/worker split).
    * ``worker_image == default_worker_image`` (the caller did not override).
    * ``dirty_fn()`` returns True (submitter has uncommitted changes).

    ``dirty_fn`` is invoked lazily, after the cheap early-returns, so callers
    that pass ``--suffix`` (or otherwise avoid touching git) are not forced
    into a ``git status`` shell-out they don't need.

    The check is intentionally lenient (warning, not block) because legitimate
    cases exist -- dirty test harness code, docs, workflow-side tweaks, etc.
    -- and false positives would block productive work. The signal here is
    a near-zero-cost "are you sure?" for the common footgun where someone
    iterates on pipeline code, runs the workflow, and is surprised that
    their changes did not take effect because workers cannot see them.
    """
    if runner != "dataflow":
        return
    if worker_image != default_worker_image:
        return
    if not dirty_fn():
        return

    banner = "!" * 80
    logger.warning(banner)
    logger.warning(
        "WARNING: submitter tree at %s is dirty, but --worker-image is the "
        "default published image (%s). Dataflow workers pull only from the "
        "registry -- they will NOT execute your local changes. To actually "
        "test the dirty changes, build + push a worker image from your tree "
        "(e.g. to gcr.io/world-fishing-827/dit/<pipeline>:<tag>) and pass "
        "--worker-image=<that-image>.",
        repo_dir, worker_image,
    )
    logger.warning(banner)
