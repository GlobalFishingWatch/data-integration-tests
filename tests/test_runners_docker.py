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
# Cloud-auth mode (DIT_CLOUD_AUTH_ADC env-triggered) — Commit A
# --------------------------------------------------------------------------

_ADC_TARGET = "/root/.config/gcloud/application_default_credentials.json"


def _captured_v_specs(cmd: list[str]) -> list[str]:
    """Return the value of every ``-v <spec>`` pair in ``cmd``."""
    return [cmd[i + 1] for i, tok in enumerate(cmd) if tok == "-v"]


# ----- helper (pure function) ---------------------------------------------

def test_apply_cloud_auth_mode_unset_passes_through(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_AUTH_ADC", raising=False)
    assert dit_docker._apply_cloud_auth_mode(["gcp:/root/.config"]) == [
        "-v", "gcp:/root/.config",
    ]


def test_apply_cloud_auth_mode_unset_empty_volumes(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_AUTH_ADC", raising=False)
    assert dit_docker._apply_cloud_auth_mode(()) == []


def test_apply_cloud_auth_mode_set_appends_ro_mount(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    flags = dit_docker._apply_cloud_auth_mode(())
    assert flags == ["-v", f"/workspace/dit-adc.json:{_ADC_TARGET}:ro"]


def test_apply_cloud_auth_mode_drops_laptop_mount(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    flags = dit_docker._apply_cloud_auth_mode(["gcp:/root/.config"])
    # laptop mount dropped; only the ADC mount survives
    assert flags == ["-v", f"/workspace/dit-adc.json:{_ADC_TARGET}:ro"]


def test_apply_cloud_auth_mode_drops_laptop_subdir_mount(monkeypatch):
    """A mount targeting /root/.config/gcloud (or below) is laptop-auth too."""
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    flags = dit_docker._apply_cloud_auth_mode(["gcp:/root/.config/gcloud"])
    assert flags == ["-v", f"/workspace/dit-adc.json:{_ADC_TARGET}:ro"]


def test_apply_cloud_auth_mode_keeps_unrelated_volumes(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    flags = dit_docker._apply_cloud_auth_mode([
        "gcp:/root/.config",
        "data:/opt/data",
        "/host/path:/container/path",
    ])
    # the two unrelated mounts are kept; the laptop mount is replaced
    assert flags == [
        "-v", "data:/opt/data",
        "-v", "/host/path:/container/path",
        "-v", f"/workspace/dit-adc.json:{_ADC_TARGET}:ro",
    ]


def test_apply_cloud_auth_mode_drop_is_logged(monkeypatch, caplog):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    with caplog.at_level("INFO", logger="dit.runners.docker"):
        dit_docker._apply_cloud_auth_mode(["gcp:/root/.config"])
    # the dropped spec is named in the log
    msgs = " | ".join(r.message for r in caplog.records)
    assert "dropping laptop-mode mount" in msgs
    assert "gcp:/root/.config" in msgs


def test_apply_cloud_auth_mode_logs_mount_not_token(monkeypatch, caplog):
    """Log must mention the source file path, not anything resembling token
    contents (the path is logged at INFO; nothing else from the file is)."""
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    with caplog.at_level("INFO", logger="dit.runners.docker"):
        dit_docker._apply_cloud_auth_mode(())
    msgs = " | ".join(r.message for r in caplog.records)
    assert "/workspace/dit-adc.json" in msgs
    assert _ADC_TARGET in msgs


# ----- end-to-end through run() — docker run path -------------------------

def test_run_published_cloud_auth_adds_mount(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    with patch.object(dit_docker.subprocess, "run",
                      return_value=MagicMock(returncode=0)) as mock_run:
        dit_docker.run("gfw/pipe-events", ["incremental_events"])
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert f"/workspace/dit-adc.json:{_ADC_TARGET}:ro" in specs


def test_run_published_cloud_auth_drops_laptop_and_adds_adc(monkeypatch):
    """When the workflow passes the laptop named-volume AND cloud-auth is on,
    the laptop mount is dropped and the cloud-auth bind-mount is added."""
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
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
    assert f"/workspace/dit-adc.json:{_ADC_TARGET}:ro" in specs


def test_run_published_cloud_auth_unset_byte_identical(monkeypatch):
    """With DIT_CLOUD_AUTH_ADC unset, behaviour is unchanged from today."""
    monkeypatch.delenv("DIT_CLOUD_AUTH_ADC", raising=False)
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
    # no ADC bind-mount anywhere
    assert all(_ADC_TARGET not in s for s in specs)


def test_run_published_cloud_auth_drop_logged(monkeypatch, caplog):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)),
        caplog.at_level("INFO", logger="dit.runners.docker"),
    ):
        dit_docker.run("gfw/pipe-events", ["op"], volumes=["gcp:/root/.config"])
    msgs = " | ".join(r.message for r in caplog.records)
    assert "dropping laptop-mode mount" in msgs


# ----- end-to-end through run() — docker compose run path -----------------

def test_build_from_source_cloud_auth_adds_mount(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
    dit_docker._BUILT_PROJECTS.clear()
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)) as mock_run,
        patch.object(dit_docker, "_teardown_compose_network"),
    ):
        dit_docker.run("img", ["op"], build_from_source=True)
    cmd = _captured_cmd(mock_run)
    specs = _captured_v_specs(cmd)
    assert f"/workspace/dit-adc.json:{_ADC_TARGET}:ro" in specs


def test_build_from_source_cloud_auth_drops_laptop_and_adds_adc(monkeypatch):
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", "/workspace/dit-adc.json")
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
    assert f"/workspace/dit-adc.json:{_ADC_TARGET}:ro" in specs
    # service positional still threaded correctly
    assert "pipeline" in cmd
    assert cmd.index("pipeline") < cmd.index("op")


def test_build_from_source_cloud_auth_unset_byte_identical(monkeypatch):
    monkeypatch.delenv("DIT_CLOUD_AUTH_ADC", raising=False)
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


# ----- logging hygiene: token contents must not be log-leakable -----------

def test_run_cloud_auth_log_records_mount_path_not_file_contents(monkeypatch, caplog, tmp_path):
    """A realistic source path containing token-shaped text proves the runner
    logs paths only -- the path is logged, but nothing reads or surfaces
    the file's contents."""
    adc = tmp_path / "dit-adc.json"
    adc.write_text('{"token": "ya29.SENSITIVE_TOKEN_SHOULD_NOT_LEAK"}')
    monkeypatch.setenv("DIT_CLOUD_AUTH_ADC", str(adc))
    with (
        patch.object(dit_docker.subprocess, "run",
                     return_value=MagicMock(returncode=0)),
        caplog.at_level("INFO", logger="dit.runners.docker"),
    ):
        dit_docker.run("gfw/pipe-events", ["op"])
    msgs = " | ".join(r.message for r in caplog.records)
    assert str(adc) in msgs
    assert "ya29." not in msgs
    assert "SENSITIVE_TOKEN" not in msgs
