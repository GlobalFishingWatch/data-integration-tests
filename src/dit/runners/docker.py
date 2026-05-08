"""Docker-based pipeline runner.

Invokes a published pipeline image with the given CLI args. Each call gets a
unique compose project name when ``build_from_source`` is set, so concurrent
invocations under ``--parallel`` do not race on creating the default network.
(Without uniquification, three parallel ``docker compose run`` calls all try
to create the same ``<project>_default`` network and the daemon errors with
"network with name X already exists" for the laggers.)
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import uuid

logger = logging.getLogger(__name__)

_BUILD_LOCK = threading.Lock()
_BUILT_PROJECTS: set[str] = set()


def _ensure_built(project_name: str) -> None:
    """Build the dev image once per (project_name) per process. Thread-safe."""
    with _BUILD_LOCK:
        if project_name in _BUILT_PROJECTS:
            return
        logger.info("docker: building dev image for project %s ...", project_name)
        subprocess.run(
            ["docker", "compose", "-p", project_name, "build", "dev"],
            check=True,
        )
        _BUILT_PROJECTS.add(project_name)


def run(
    image_tag: str,
    args: list[str],
    *,
    env: dict | None = None,
    project_name: str | None = None,
    build_from_source: bool = False,
    entrypoint: str | None = None,
) -> int:
    """Invoke a pipeline via docker.

    By default runs ``docker run --rm <image_tag> <args...>`` against a
    published image. When ``build_from_source=True`` falls back to
    ``docker compose run`` against the local dev service -- intended for
    pipelines whose images are not yet published (pipe-gaps' workflow needs
    this today).

    A unique compose project name (``-p <name>-<uuid>``) is used per call so
    parallel invocations do not race on docker network creation.

    ``entrypoint`` overrides the image's default ENTRYPOINT (passes through
    as ``--entrypoint`` to docker / docker compose run). Pipe-gaps' dev
    image has no default ``pipe-gaps`` entrypoint baked in, so its workflow
    sets ``entrypoint="pipe-gaps"``.

    Returns the docker subprocess exit code.
    """
    base = project_name or "dit-runner"
    unique_project = f"{base}-{uuid.uuid4().hex[:8]}"

    if build_from_source:
        _ensure_built(unique_project)
        cmd = ["docker", "compose", "-p", unique_project, "run", "--rm"]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(["dev", *args])
    else:
        cmd = ["docker", "run", "--rm", "--name", unique_project]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend([image_tag, *args])

    proc_env = None
    if env is not None:
        proc_env = {**os.environ, **env}

    logger.info("docker: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False, env=proc_env)
    return result.returncode
