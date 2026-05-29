"""Docker-based pipeline runner.

Invokes a published pipeline image with the given CLI args. Each call gets a
unique compose project name when ``build_from_source`` is set, so concurrent
invocations under ``--parallel`` do not race on creating the default network.
(Without uniquification, three parallel ``docker compose run`` calls all try
to create the same ``<project>_default`` network and the daemon errors with
"network with name X already exists" for the laggers.)

Each ``build_from_source`` call also tears down its own project network in a
``finally`` after the container exits -- ``docker compose run --rm`` removes
containers but leaves the ``<project>_default`` bridge network behind, and the
default address pool (172.16-172.31, /24 each) exhausts after a few dozen
unique-named runs.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import uuid
from collections.abc import Sequence

logger = logging.getLogger(__name__)

_BUILD_LOCK = threading.Lock()
_BUILT_PROJECTS: set[str] = set()


def _ensure_built(project_name: str, *, service: str = "dev") -> None:
    """Build the compose ``service`` image once per (project_name) per process.

    Thread-safe. ``service`` defaults to ``"dev"`` (the Beam consumers'
    compose service); pipe-events passes ``"pipeline"``.
    """
    with _BUILD_LOCK:
        if project_name in _BUILT_PROJECTS:
            return
        logger.info("docker: building %s image for project %s ...", service, project_name)
        subprocess.run(
            ["docker", "compose", "-p", project_name, "build", service],
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
    volumes: Sequence[str] = (),
    service: str = "dev",
) -> int:
    """Invoke a pipeline via docker.

    By default runs ``docker run --rm <image_tag> <args...>`` against a
    published image. When ``build_from_source=True`` falls back to
    ``docker compose run`` against the local compose ``service`` -- intended
    for pipelines whose images are not yet published (pipe-gaps' workflow
    needs this today).

    A unique compose project name (``-p <name>-<uuid>``) is used per call so
    parallel invocations do not race on docker network creation.

    ``entrypoint`` overrides the image's default ENTRYPOINT (passes through
    as ``--entrypoint`` to docker / docker compose run). Pipe-gaps' dev
    image has no default ``pipe-gaps`` entrypoint baked in, so its workflow
    sets ``entrypoint="pipe-gaps"``; pipe-events' image bakes ``scripts/run``
    as ENTRYPOINT and overrides it to ``"pipe"`` (the Python CLI).

    ``volumes`` is a sequence of ``-v`` mount specs (``<src>:<dst>`` or a
    named-volume ``<vol>:<dst>``), threaded through to BOTH the ``docker run``
    and ``docker compose run`` paths. pipe-events authenticates to GCP via a
    docker **named volume** ``gcp`` mounted at ``/root/.config`` (created with
    ``docker volume create gcp`` + ``gcloud auth login``), so its workflow
    passes ``volumes=["gcp:/root/.config"]``. The default empty tuple keeps
    existing callers (pipe-gaps, port-visits) byte-identical -- no ``-v`` flags
    are emitted unless a mount is requested.

    ``service`` is the compose service name used on the ``build_from_source``
    path (``docker compose run <service>``). Defaults to ``"dev"`` so existing
    Beam consumers are unaffected; pipe-events' compose service is named
    ``"pipeline"``.

    Returns the docker subprocess exit code.
    """
    base = project_name or "dit-runner"
    unique_project = f"{base}-{uuid.uuid4().hex[:8]}"

    volume_flags: list[str] = []
    for vol in volumes:
        volume_flags.extend(["-v", vol])

    if build_from_source:
        _ensure_built(unique_project, service=service)
        cmd = ["docker", "compose", "-p", unique_project, "run", "--rm"]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(volume_flags)
        cmd.extend([service, *args])
    else:
        cmd = ["docker", "run", "--rm", "--name", unique_project]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(volume_flags)
        cmd.extend([image_tag, *args])

    proc_env = None
    if env is not None:
        proc_env = {**os.environ, **env}

    logger.info("docker: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, check=False, env=proc_env)
        return result.returncode
    finally:
        if build_from_source:
            _teardown_compose_network(unique_project)


def _teardown_compose_network(project_name: str) -> None:
    """Remove the ``<project>_default`` bridge network left behind by
    ``docker compose run --rm``. Uses ``docker network rm`` directly (rather
    than ``docker compose -p <name> down``) so cleanup doesn't depend on a
    compose file being present in the CWD. External volumes (e.g. the ``gcp``
    auth volume) are not touched -- they live outside the project namespace.

    Idempotent: silently no-ops if the network is gone, in use, or never
    existed (e.g., when the run failed before container start).
    """
    network = f"{project_name}_default"
    result = subprocess.run(
        ["docker", "network", "rm", network],
        check=False,
        capture_output=True,
        text=True,
    )
    logger.debug(
        "docker network rm %s -> rc=%d %s",
        network, result.returncode, (result.stderr or "").strip(),
    )
