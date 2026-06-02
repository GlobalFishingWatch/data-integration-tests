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


# --------------------------------------------------------------------------
# Cloud mode (DIT_CLOUD_MODE env-triggered) — --network=host for metadata-server ADC
# --------------------------------------------------------------------------


def _captured_v_specs(cmd: list[str]) -> list[str]:
    """Return the value of every ``-v <spec>`` pair in ``cmd``."""
    return [cmd[i + 1] for i, tok in enumerate(cmd) if tok == "-v"]


# ----- helper (pure function) ---------------------------------------------

def test_apply_cloud_mode_unset_passes_volumes_through(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_MODE", raising=False)
    assert dit_docker._apply_cloud_mode(["gcp:/root/.config"]) == [
        "-v", "gcp:/root/.config",
    ]


def test_apply_cloud_mode_unset_empty_volumes(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_MODE", raising=False)
    assert dit_docker._apply_cloud_mode(()) == []


_QUOTA_PROJECT_FLAGS = ["-e", "GOOGLE_CLOUD_QUOTA_PROJECT=world-fishing-827"]


def test_apply_cloud_mode_set_adds_host_network_and_quota_project(monkeypatch):
    """Cloud mode on + no volumes: --network=host added AND quota-project env
    var set (so the BQ client sends X-Goog-User-Project=world-fishing-827 --
    without this the metadata-server token defaults to the build-host project
    where BQ API isn't enabled, and calls 403)."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    flags = dit_docker._apply_cloud_mode(())
    assert flags == ["--network=host", *_QUOTA_PROJECT_FLAGS]


def test_apply_cloud_mode_set_drops_laptop_mount(monkeypatch):
    """Cloud mode on + laptop mount: laptop mount dropped; the cloud-mode
    flags (--network=host + quota-project env) are added."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    flags = dit_docker._apply_cloud_mode(["gcp:/root/.config"])
    assert flags == ["--network=host", *_QUOTA_PROJECT_FLAGS]


def test_apply_cloud_mode_drops_laptop_subdir_mount(monkeypatch):
    """A mount targeting /root/.config/gcloud (or below) is laptop-auth too."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    flags = dit_docker._apply_cloud_mode(["gcp:/root/.config/gcloud"])
    assert flags == ["--network=host", *_QUOTA_PROJECT_FLAGS]


def test_apply_cloud_mode_keeps_unrelated_volumes(monkeypatch):
    """Cloud mode preserves unrelated volumes; only laptop-mode auth mounts drop."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    flags = dit_docker._apply_cloud_mode([
        "gcp:/root/.config",
        "data:/opt/data",
        "/host/path:/container/path",
    ])
    assert flags == [
        "--network=host",
        *_QUOTA_PROJECT_FLAGS,
        "-v", "data:/opt/data",
        "-v", "/host/path:/container/path",
    ]


def test_apply_cloud_mode_network_host_precedes_volumes(monkeypatch):
    """--network=host comes first so it lands before the image positional and
    is unambiguously a docker flag, not a -v target."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    flags = dit_docker._apply_cloud_mode(["data:/opt/data"])
    assert flags[0] == "--network=host"


def test_apply_cloud_mode_drop_is_logged(monkeypatch, caplog):
    """When cloud mode drops a laptop mount, the dropped spec is logged."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    with caplog.at_level("INFO", logger="dit.runners.docker"):
        dit_docker._apply_cloud_mode(["gcp:/root/.config"])
    msgs = " | ".join(r.message for r in caplog.records)
    assert "dropping laptop-mode mount" in msgs
    assert "gcp:/root/.config" in msgs


def test_apply_cloud_mode_logs_host_network_activation(monkeypatch, caplog):
    """Cloud mode activation is logged so the override is visible."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    with caplog.at_level("INFO", logger="dit.runners.docker"):
        dit_docker._apply_cloud_mode(())
    msgs = " | ".join(r.message for r in caplog.records)
    assert "--network=host" in msgs


# ----- end-to-end through run() — docker run path -------------------------

def test_run_published_cloud_mode_adds_host_network(monkeypatch):
    """Cloud mode on + docker run path: cmd contains --network=host."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run("gfw/pipe-events", ["incremental_events"])
    cmd = _captured_cmd(mock_run)
    assert "--network=host" in cmd


def test_run_published_cloud_mode_drops_laptop_keeps_network_host(monkeypatch):
    """When the workflow passes the laptop named-volume AND cloud mode is on,
    the laptop mount is dropped and --network=host is added."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run(
            "gfw/pipe-events",
            ["incremental_events"],
            entrypoint="pipe",
            volumes=["gcp:/root/.config"],
        )
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert "gcp:/root/.config" not in specs
    assert "--network=host" in cmd


def test_run_published_cloud_mode_unset_byte_identical(monkeypatch):
    """With DIT_CLOUD_MODE unset, behaviour is unchanged from today."""
    monkeypatch.delenv("DIT_CLOUD_MODE", raising=False)
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run(
            "gfw/pipe-events",
            ["incremental_events"],
            entrypoint="pipe",
            volumes=["gcp:/root/.config"],
        )
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert specs == ["gcp:/root/.config"]
    assert "--network=host" not in cmd


def test_run_published_cloud_mode_drop_logged(monkeypatch, caplog):
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)),
        caplog.at_level("INFO", logger="dit.runners.docker"),
    ):
        dit_docker.run("gfw/pipe-events", ["op"], volumes=["gcp:/root/.config"])
    msgs = " | ".join(r.message for r in caplog.records)
    assert "dropping laptop-mode mount" in msgs


# ----- end-to-end through run() — docker compose run path -----------------

def test_build_from_source_cloud_mode_adds_host_network(monkeypatch):
    """Cloud mode on + build_from_source path: cmd contains --network=host."""
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run("img", ["op"], build_from_source=True)
    cmd = _captured_cmd(mock_run)
    assert "--network=host" in cmd


def test_build_from_source_cloud_mode_drops_laptop_keeps_network_host(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_MODE", "1")
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run(
            "img",
            ["op"],
            entrypoint="pipe",
            volumes=["gcp:/root/.config"],
            service="pipeline",
            build_from_source=True,
        )
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert "gcp:/root/.config" not in specs
    assert "--network=host" in cmd
    # service positional still threaded correctly
    assert "pipeline" in cmd
    assert cmd.index("pipeline") < cmd.index("op")


def test_build_from_source_cloud_mode_unset_byte_identical(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_MODE", raising=False)
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run(
            "img",
            ["op"],
            volumes=["gcp:/root/.config"],
            service="pipeline",
            build_from_source=True,
        )
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert specs == ["gcp:/root/.config"]
    assert "--network=host" not in cmd
