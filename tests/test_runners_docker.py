"""Tests for ``dit.runners.docker.run`` volume + service extensions (Commit A).

The subprocess + network-teardown collaborators are monkeypatched so no real
docker calls happen; assertions are on the constructed command vector.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dit.runners import docker as dit_docker


def _captured_cmd(mock_run: MagicMock) -> list[str]:
    """Return the command vector from the FIRST positional arg of the docker
    invocation (skip any ``docker compose ... build`` call captured first)."""
    for call in mock_run.call_args_list:
        cmd = call.args[0]
        # The teardown call is `docker network rm ...`; the build call is
        # `docker compose ... build <svc>`. The run is `docker run ...` or
        # `docker compose ... run ...`.
        if "run" in cmd and "build" not in cmd:
            return cmd
    raise AssertionError(f"no run command captured in {mock_run.call_args_list}")


# --------------------------------------------------------------------------
# docker run (published image) path
# --------------------------------------------------------------------------

def test_run_published_no_volumes_emits_no_v_flag():
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        rc = dit_docker.run("gfw/pipe-events", ["incremental_events", "-start", "2012-01-01"])
    assert rc == 0
    cmd = _captured_cmd(mock_run)
    assert "-v" not in cmd
    assert cmd[:3] == ["docker", "run", "--rm"]
    assert "gfw/pipe-events" in cmd


def test_run_published_threads_volumes():
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run(
            "gfw/pipe-events",
            ["incremental_events"],
            entrypoint="pipe",
            volumes=["gcp:/root/.config"],
        )
    cmd = _captured_cmd(mock_run)
    assert "-v" in cmd
    assert "gcp:/root/.config" in cmd
    # the -v immediately precedes its spec
    assert cmd[cmd.index("-v") + 1] == "gcp:/root/.config"
    # entrypoint threaded through
    assert "--entrypoint" in cmd
    assert cmd[cmd.index("--entrypoint") + 1] == "pipe"


def test_run_published_multiple_volumes():
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run(
            "img",
            ["op"],
            volumes=["gcp:/root/.config", "data:/opt/data"],
        )
    cmd = _captured_cmd(mock_run)
    assert cmd.count("-v") == 2
    assert "gcp:/root/.config" in cmd
    assert "data:/opt/data" in cmd


def test_run_published_volume_precedes_image():
    # The -v flags must come BEFORE the image name (docker run flags precede
    # the image positional, everything after is the container's argv).
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run("gfw/pipe-events", ["op"], volumes=["gcp:/root/.config"])
    cmd = _captured_cmd(mock_run)
    assert cmd.index("gcp:/root/.config") < cmd.index("gfw/pipe-events")


# --------------------------------------------------------------------------
# docker compose run (build_from_source) path
# --------------------------------------------------------------------------

def test_build_from_source_default_service_is_dev():
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run("img", ["op"], build_from_source=True)
    cmd = _captured_cmd(mock_run)
    assert cmd[:5] == ["docker", "compose", "-p", cmd[3], "run"]
    assert "dev" in cmd


def test_build_from_source_threads_volumes_and_service():
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run(
            "gfw/pipe-events",
            ["incremental_events"],
            entrypoint="pipe",
            volumes=["gcp:/root/.config"],
            service="pipeline",
            build_from_source=True,
        )
    cmd = _captured_cmd(mock_run)
    assert "-v" in cmd
    assert "gcp:/root/.config" in cmd
    assert "pipeline" in cmd
    assert "dev" not in cmd
    # the compose service positional follows the flags + comes before args
    assert cmd.index("pipeline") < cmd.index("incremental_events")


def test_build_from_source_builds_named_service():
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run("img", ["op"], service="pipeline", build_from_source=True)
    # the build call (first) must target the named service
    build_calls = [c.args[0] for c in mock_run.call_args_list if "build" in c.args[0]]
    assert build_calls, "expected a `docker compose ... build` call"
    assert "pipeline" in build_calls[0]
