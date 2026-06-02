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


_CLOUD_MODE_ENV = "DIT_CLOUD_MODE"
_LAPTOP_AUTH_PREFIX = "/root/.config"

# Quota project for BigQuery (and other GCP) API calls in cloud mode. Cloud
# Build's metadata server issues tokens whose default quota_project_id is the
# build-host project (e.g. 1034185025654, a Google-managed Cloud Build pool
# project), not the build SA's home project. Without this override, BQ calls
# get 403'd with "API has not been used in project <build-host>" because the
# API isn't enabled in that consumer project (and isn't ours to enable).
# We pin to ``world-fishing-827`` -- where the build SA lives and where dit's
# Beam workflows already write outputs (per [[prod-infra-boundary]], all dit
# writes stay in wf827 namespaces; the quota project must match).
_CLOUD_MODE_QUOTA_PROJECT = "world-fishing-827"


def _is_laptop_auth_mount(volume_spec: str) -> bool:
    """True iff ``volume_spec`` mounts onto the laptop-mode ADC location.

    Laptop mode mounts the ``gcp`` named volume at ``/root/.config`` (or any
    subdirectory of it). In cloud mode those mounts are dropped because the
    named volume does not exist in Cloud Build; the inner container reaches
    ADC via the metadata server (over ``--network=host``) instead.

    A docker ``-v`` spec is ``<source>:<target>[:<mode>]``. We classify by
    ``target`` (the second colon-separated field): if it equals
    ``/root/.config`` or starts with ``/root/.config/`` the mount is treated
    as laptop-mode auth.
    """
    parts = volume_spec.split(":")
    if len(parts) < 2:
        return False
    target = parts[1]
    return target == _LAPTOP_AUTH_PREFIX or target.startswith(_LAPTOP_AUTH_PREFIX + "/")


def _apply_cloud_mode(volumes: Sequence[str]) -> list[str]:
    """Return the docker flags to insert before the image/service positional.

    Triggered solely by the ``DIT_CLOUD_MODE`` env var (any non-empty value).
    When set ("we are running inside ditbox / Cloud Build"):

    * ``--network=host`` is added so the inner container shares the build
      VM's network namespace and can reach Cloud Build's metadata server at
      ``169.254.169.254`` -- google-auth's ADC discovery chain finds the
      metadata server and obtains a fresh OAuth token bound to the build SA,
      same mechanism prod uses via GKE Workload Identity.
    * ``-e GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827`` is added so the
      inner container's BQ (and other GCP) API calls send
      ``X-Goog-User-Project: world-fishing-827`` -- without this, the
      metadata-server-issued token defaults the quota project to the build
      host (e.g. ``1034185025654``, a Cloud Build-managed pool project)
      where BQ API isn't enabled, and calls 403 with "API has not been used
      in project <build-host>".
    * any caller-supplied volume targeting ``/root/.config`` (or below) is
      dropped -- the laptop-mode ``gcp:/root/.config`` named volume doesn't
      exist in Cloud Build and would mount as an empty anonymous volume,
      shadowing whatever the base image has there (typically nothing
      load-bearing, but cleaner to drop than to leave an empty mount).
      Dropped specs are logged at INFO so the override is visible.

    When the env var is unset (laptop), returns the laptop-mode ``-v`` flags
    unchanged: behaviour is byte-identical to pre-cloud-mode callers.

    Earlier the cloud path bind-mounted a short-lived ADC JSON file at the
    standard ADC location. That approach was abandoned after live testing:
    the older ``google-auth`` baked into pipe-events' Python 3.8 image tries
    to refresh ``authorized_user`` credentials before the first API call,
    ignoring the pre-issued ``token`` field, and a refresh with placeholder
    OAuth client material fails with ``invalid_client``. Metadata-server
    access via ``--network=host`` sidesteps the issue entirely -- the
    container never holds long-lived material, just like prod.

    Pure function over the env var + ``volumes``; safe to unit-test directly.
    """
    cloud_mode = bool(os.environ.get(_CLOUD_MODE_ENV))

    if not cloud_mode:
        flags: list[str] = []
        for vol in volumes:
            flags.extend(["-v", vol])
        return flags

    out: list[str] = [
        "--network=host",
        "-e", f"GOOGLE_CLOUD_QUOTA_PROJECT={_CLOUD_MODE_QUOTA_PROJECT}",
    ]
    for vol in volumes:
        if _is_laptop_auth_mount(vol):
            logger.info(
                "docker: cloud mode active (%s set); dropping laptop-mode mount %r",
                _CLOUD_MODE_ENV, vol,
            )
            continue
        out.extend(["-v", vol])
    logger.info(
        "docker: cloud mode active; adding --network=host so the inner "
        "container can reach Cloud Build's metadata server for ADC.",
    )
    return out


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

    **Cloud mode (env-triggered, no parameter).** When the ``DIT_CLOUD_MODE``
    env var is set (any non-empty value), the runner adds ``--network=host``
    to the docker invocation so the inner container can reach Cloud Build's
    metadata server for ADC -- same mechanism prod uses via GKE Workload
    Identity, no on-disk credential material. Laptop-mode mounts targeting
    ``/root/.config`` (or below) are also dropped (the ``gcp`` named volume
    does not exist in Cloud Build). The workflow's
    ``volumes=["gcp:/root/.config"]`` argument is therefore left unchanged;
    the same workflow code runs identically on laptop, prod (Workload
    Identity), and the ditbox cloud path (this mode). The trigger is
    intentionally an env var rather than a parameter so workflows stay
    unaware of the execution context.

    Returns the docker subprocess exit code.
    """
    base = project_name or "dit-runner"
    unique_project = f"{base}-{uuid.uuid4().hex[:8]}"

    cloud_flags = _apply_cloud_mode(volumes)

    if build_from_source:
        _ensure_built(unique_project, service=service)
        cmd = ["docker", "compose", "-p", unique_project, "run", "--rm"]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(cloud_flags)
        cmd.extend([service, *args])
    else:
        cmd = ["docker", "run", "--rm", "--name", unique_project]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(cloud_flags)
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
