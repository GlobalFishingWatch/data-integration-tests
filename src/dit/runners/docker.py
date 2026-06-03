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

# The docker network Cloud Build attaches build-step containers to. A sidecar
# "fake" metadata server lives on this network and returns OAuth tokens for
# the user-configured ``serviceAccount:`` (``automated-testing@`` for dit) --
# distinct from the build VM's real metadata server, which returns the
# Google-managed ``cloudbuild-untrusted@argo-prod-*`` identity (the docker
# daemon host). Sibling containers launched via ``docker run`` are attached
# to the daemon's default network, NOT ``cloudbuild``; ``--network=cloudbuild``
# explicitly re-attaches them so they see the same fake metadata server the
# build step does. Reference: cloud-build-local's open-source metadata.go +
# earthly/earthly#1628.
_CLOUDBUILD_NETWORK = "cloudbuild"


def _is_laptop_auth_mount(volume_spec: str) -> bool:
    """True iff ``volume_spec`` mounts onto the laptop-mode ADC location.

    Laptop mode mounts the ``gcp`` named volume at ``/root/.config`` (or any
    subdirectory of it). In cloud mode those mounts are dropped because the
    named volume does not exist in Cloud Build; the inner container reaches
    ADC via the fake metadata server (over ``--network=cloudbuild``) instead.

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

    * ``--network=cloudbuild`` is added so the inner container attaches to
      the ``cloudbuild`` docker network Cloud Build creates per build, where
      a fake metadata server returns OAuth tokens bound to the build SA
      (``automated-testing@``). google-auth's ADC discovery chain finds the
      fake metadata server at ``metadata.google.internal`` and obtains a
      fresh token; same mechanism prod uses via GKE Workload Identity, just
      a different metadata-server provider.
    * any caller-supplied volume targeting ``/root/.config`` (or below) is
      dropped -- the laptop-mode ``gcp:/root/.config`` named volume doesn't
      exist in Cloud Build and would mount as an empty anonymous volume,
      shadowing whatever the base image has there (typically nothing
      load-bearing, but cleaner to drop than to leave an empty mount).
      Dropped specs are logged at INFO so the override is visible.

    When the env var is unset (laptop), returns the laptop-mode ``-v`` flags
    unchanged: behaviour is byte-identical to pre-cloud-mode callers.

    **History (two prior designs falsified by live evidence).** First, an
    ADC-file bind-mount approach was tried; older ``google-auth`` in
    pipe-events' Python 3.8 image refreshes ``authorized_user`` credentials
    before the first API call (ignoring the pre-issued ``token`` field), and
    refresh against placeholder OAuth client material fails with
    ``invalid_client``. Second, ``--network=host`` was tried; that attaches
    the inner container to the docker daemon's host network namespace, NOT
    the build step's, so the metadata server returns the Google-managed
    ``cloudbuild-untrusted@argo-prod-*`` identity instead of the build SA --
    causing ``USER_PROJECT_DENIED`` failures even when explicit quota-project
    overrides are applied (the caller identity is wrong, not the quota
    project). ``--network=cloudbuild`` is the documented sibling-container
    pattern that resolves both: the inner container sees the same fake
    metadata server the build step does, no credential material on disk,
    no IAM grants to Google-managed accounts.

    Pure function over the env var + ``volumes``; safe to unit-test directly.
    """
    cloud_mode = bool(os.environ.get(_CLOUD_MODE_ENV))

    if not cloud_mode:
        flags: list[str] = []
        for vol in volumes:
            flags.extend(["-v", vol])
        return flags

    out: list[str] = [f"--network={_CLOUDBUILD_NETWORK}"]
    for vol in volumes:
        if _is_laptop_auth_mount(vol):
            logger.info(
                "docker: cloud mode active (%s set); dropping laptop-mode mount %r",
                _CLOUD_MODE_ENV, vol,
            )
            continue
        out.extend(["-v", vol])
    logger.info(
        "docker: cloud mode active; adding --network=%s so the inner "
        "container can reach Cloud Build's fake metadata server for ADC.",
        _CLOUDBUILD_NETWORK,
    )
    return out


def run(
    image_tag: str,
    args: list[str],
    *,
    env: dict | None = None,
    container_env: dict | None = None,
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

    ``env`` sets env vars on the HOST subprocess (the ``docker`` / ``docker
    compose`` process itself). It does NOT set env vars inside the inner
    container; for that, see ``container_env``.

    ``container_env`` injects ``-e KEY=VALUE`` flags into the docker /
    docker compose invocation so the named env vars are visible inside the
    inner container. Concretely needed when a workflow's CLI relies on env-
    var-driven defaults that the ``--<flag>`` arg surface doesn't reach.
    For example, pipe-segment v5.0.x's Beam ``WriteToBigQuery`` constructs
    its own ``google-cloud-bigquery`` client whose default-project resolution
    walks ``GOOGLE_CLOUD_PROJECT`` env -> ADC project metadata; the Beam
    pipeline option ``--project=...`` is read earlier in the pipeline
    construction and isn't forwarded to this internal client. Setting
    ``container_env={"GOOGLE_CLOUD_PROJECT": "world-fishing-827"}`` closes
    that gap. ``examples/example_segment.sh`` does the same via ``-e``
    inline on the docker compose command, so this parameter is just lifting
    that documented escape hatch into the harness. The default ``None``
    means no ``-e`` flags are emitted, byte-identical to existing callers
    (pipe-gaps, port-visits, pipe-events).

    **Cloud mode (env-triggered, no parameter).** When the ``DIT_CLOUD_MODE``
    env var is set (any non-empty value), the runner adds
    ``--network=cloudbuild`` to the docker invocation so the inner container
    attaches to Cloud Build's per-build docker network where a fake metadata
    server returns OAuth tokens for the build SA (``automated-testing@``).
    google-auth's ADC discovery finds the fake metadata server at
    ``metadata.google.internal`` -- same mechanism prod uses via GKE
    Workload Identity, just a different metadata-server provider, and no
    on-disk credential material. Laptop-mode mounts targeting
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

    # ``-e KEY=VALUE`` flags emitted between ``run --rm`` and the
    # image/service positional. Ordering matters: docker rejects ``-e``
    # after the positional. Sorted for deterministic output (helps tests +
    # human log scanning).
    container_env_flags: list[str] = []
    if container_env:
        for key in sorted(container_env):
            container_env_flags.extend(["-e", f"{key}={container_env[key]}"])

    if build_from_source:
        _ensure_built(unique_project, service=service)
        cmd = ["docker", "compose", "-p", unique_project, "run", "--rm"]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(container_env_flags)
        cmd.extend(cloud_flags)
        cmd.extend([service, *args])
    else:
        cmd = ["docker", "run", "--rm", "--name", unique_project]
        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])
        cmd.extend(container_env_flags)
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
