"""Unit tests for ``dit.worker_image.ensure_pipeline_image``.

The gcloud calls (image existence check + kaniko build submit) are mocked;
these cover the trigger matrix and idempotency, not a real build. The trigger
matrix differs by consumer:

* Dataflow worker (``runner="dataflow"``): build only when the run is
  unreviewed AND the worker image is left at the default.
* Docker runner (``runner="docker"``): build only when
  ``need_registry_image=True`` (cloud-mode signal) AND the image is left at
  the default; ``unreviewed`` does NOT gate this path -- the default local
  compose tag never resolves in cloud regardless of review state.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from dit import worker_image

DEFAULT = "us-central1-docker.pkg.dev/gfw-int-infrastructure/core/pipe-gaps:v0.9.6"


def _kwargs(**overrides):
    base = dict(
        pipeline="pipe-gaps",
        repo_dir="/repo",
        commit="abc1234",
        runner="dataflow",
        unreviewed=True,
        worker_image=DEFAULT,
        default_worker_image=DEFAULT,
    )
    base.update(overrides)
    return base


# --------------------------------------------------------------------------
# Tag helpers
# --------------------------------------------------------------------------

def test_tag_is_content_addressable() -> None:
    assert (
        worker_image.pipeline_image_tag("pipe-gaps", "abc1234")
        == "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"
    )


def test_tag_alias_preserves_old_name() -> None:
    """Back-compat alias: ``worker_image_tag`` is the old name, still works."""
    assert worker_image.worker_image_tag is worker_image.pipeline_image_tag
    assert (
        worker_image.worker_image_tag("pipe-events", "deadbee")
        == "gcr.io/world-fishing-827/dit/pipe-events:dit-deadbee"
    )


def test_ensure_alias_preserves_old_name() -> None:
    """``ensure_worker_image`` remains importable for any out-of-tree callers."""
    assert worker_image.ensure_worker_image is worker_image.ensure_pipeline_image


# --------------------------------------------------------------------------
# Dataflow trigger matrix (unchanged)
# --------------------------------------------------------------------------

def test_dataflow_docker_runner_default_never_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default docker-runner path (no need_registry_image): never builds."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: pytest.fail("checked"))
    out = worker_image.ensure_pipeline_image(**_kwargs(runner="docker"))
    assert out == DEFAULT


def test_dataflow_explicit_worker_image_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    custom = "gcr.io/world-fishing-827/dit/pipe-gaps:my-custom"
    out = worker_image.ensure_pipeline_image(**_kwargs(worker_image=custom))
    assert out == custom


def test_dataflow_reviewed_code_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    out = worker_image.ensure_pipeline_image(**_kwargs(unreviewed=False))
    assert out == DEFAULT


def test_dataflow_unreviewed_existing_image_skips_build(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: True)
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("rebuilt"))
    out = worker_image.ensure_pipeline_image(**_kwargs())
    assert out == "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"


def test_dataflow_unreviewed_missing_image_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    built = []
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: built.append((repo, tag, commit)))
    out = worker_image.ensure_pipeline_image(**_kwargs())
    tag = "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"
    assert out == tag
    assert built == [("/repo", tag, "abc1234")]


def test_pipeline_namespacing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    captured = {}
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: captured.update(tag=tag))
    worker_image.ensure_pipeline_image(
        **_kwargs(pipeline="anchorages_pipeline", commit="def5678")
    )
    assert captured["tag"] == "gcr.io/world-fishing-827/dit/anchorages_pipeline:dit-def5678"


# --------------------------------------------------------------------------
# Docker-runner trigger matrix (new -- pipe-events / docker-runner generalisation)
# --------------------------------------------------------------------------

PIPE_EVENTS_LOCAL = "gfw/pipe-events"


def _docker_kwargs(**overrides):
    """Docker-runner shape: local compose tag as both worker_image + default."""
    base = dict(
        pipeline="pipe-events",
        repo_dir="/repo",
        commit="abc1234",
        runner="docker",
        unreviewed=False,
        worker_image=PIPE_EVENTS_LOCAL,
        default_worker_image=PIPE_EVENTS_LOCAL,
        need_registry_image=False,
    )
    base.update(overrides)
    return base


def test_docker_laptop_path_returns_default_no_build(monkeypatch: pytest.MonkeyPatch) -> None:
    """need_registry_image=False (laptop) keeps the local compose tag, no build."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: pytest.fail("checked"))
    out = worker_image.ensure_pipeline_image(**_docker_kwargs())
    assert out == PIPE_EVENTS_LOCAL


def test_docker_cloud_existing_image_skips_build(monkeypatch: pytest.MonkeyPatch) -> None:
    """need_registry_image=True + image already in registry: no build."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: True)
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("rebuilt"))
    out = worker_image.ensure_pipeline_image(**_docker_kwargs(need_registry_image=True))
    assert out == "gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"


def test_docker_cloud_missing_image_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """need_registry_image=True + image missing: build the docker-runner image."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    built = []
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: built.append((repo, tag, commit)))
    out = worker_image.ensure_pipeline_image(**_docker_kwargs(need_registry_image=True))
    tag = "gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"
    assert out == tag
    assert built == [("/repo", tag, "abc1234")]


def test_docker_cloud_reviewed_code_still_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """unreviewed gating does NOT apply to the docker-runner path -- the local
    compose tag never resolves in cloud regardless of review state.
    """
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    built = []
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: built.append((repo, tag, commit)))
    out = worker_image.ensure_pipeline_image(
        **_docker_kwargs(need_registry_image=True, unreviewed=False)
    )
    assert out == "gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"
    assert len(built) == 1


def test_docker_cloud_explicit_override_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit non-default image is always respected, even with the cloud
    signal set -- the caller is in control."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    custom = "gcr.io/world-fishing-827/dit/pipe-events:my-custom"
    out = worker_image.ensure_pipeline_image(
        **_docker_kwargs(need_registry_image=True, worker_image=custom)
    )
    assert out == custom


def test_unknown_runner_never_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Defensive: an unknown runner string opts out of any build."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: pytest.fail("checked"))
    out = worker_image.ensure_pipeline_image(**_kwargs(runner="unknown"))
    assert out == DEFAULT


# --------------------------------------------------------------------------
# Build-and-push internals
# --------------------------------------------------------------------------

def test_build_raises_without_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    # Point _dit_root at a dir lacking docker/worker-image/cloudbuild.yaml.
    monkeypatch.setattr(worker_image, "_dit_root", lambda: tmp_path)
    with pytest.raises(RuntimeError, match="editable dit install"):
        worker_image._build_and_push("/repo", "gcr.io/x/y:z", "abc1234")


def _raise_fnf(*a, **k):
    raise FileNotFoundError("gcloud")


def test_image_exists_clear_error_without_gcloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image.subprocess, "run", _raise_fnf)
    with pytest.raises(RuntimeError, match="gcloud not found"):
        worker_image._image_exists("gcr.io/x/y:z")


def test_build_clear_error_without_gcloud(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    # Real cloudbuild config exists (dit root); the commit-tree export succeeds;
    # gcloud is what's missing.
    (tmp_path / "docker" / "worker-image").mkdir(parents=True)
    (tmp_path / "docker" / "worker-image" / "cloudbuild.yaml").write_text("steps: []\n")
    monkeypatch.setattr(worker_image, "_dit_root", lambda: tmp_path)
    monkeypatch.setattr(worker_image, "_export_commit_tree", lambda repo, commit, dest: None)
    monkeypatch.setattr(worker_image.subprocess, "run", _raise_fnf)
    with pytest.raises(RuntimeError, match="gcloud not found"):
        worker_image._build_and_push("/repo", "gcr.io/x/y:z", "abc1234")


def test_build_uses_commit_tree_not_working_tree(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """_build_and_push exports the commit's tree and hands gcloud THAT context
    dir -- never repo_dir. Fixes the REF + auto-build mismatch where the worker
    image was built from the working tree instead of the installed ref."""
    (tmp_path / "docker" / "worker-image").mkdir(parents=True)
    (tmp_path / "docker" / "worker-image" / "cloudbuild.yaml").write_text("steps: []\n")
    monkeypatch.setattr(worker_image, "_dit_root", lambda: tmp_path)

    exported: dict = {}

    def fake_export(repo_dir, commit, dest):
        exported.update(repo_dir=repo_dir, commit=commit, dest=dest)
        (Path(dest) / "marker").write_text("ok")

    monkeypatch.setattr(worker_image, "_export_commit_tree", fake_export)

    captured: dict = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        # Context must still exist inside the TemporaryDirectory at submit time.
        captured["dest_existed"] = (Path(exported["dest"]) / "marker").exists()
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(worker_image.subprocess, "run", fake_run)

    worker_image._build_and_push("/repo", "gcr.io/x/y:dit-abc1234", "abc1234")

    assert (exported["repo_dir"], exported["commit"]) == ("/repo", "abc1234")
    assert captured["cmd"][-1] == exported["dest"]  # context = materialised tree
    assert captured["cmd"][-1] != "/repo"           # ...not the working tree
    assert captured["dest_existed"] is True


def test_export_commit_tree_exports_committed_not_working_tree(tmp_path) -> None:
    """git archive of <commit> yields the COMMITTED content even when the
    working tree has diverged -- the heart of the worker/submitter fix."""

    def g(*args):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    g("init", "-q", "-b", "main")
    g("config", "user.email", "t@dit.local")
    g("config", "user.name", "t")
    (tmp_path / "code.py").write_text("committed\n")
    g("add", ".")
    g("commit", "-qm", "init")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, capture_output=True, text=True,
    ).stdout.strip()
    # Diverge the working tree AFTER committing.
    (tmp_path / "code.py").write_text("DIRTY-working-tree\n")

    dest = tmp_path / "ctx"
    dest.mkdir()
    worker_image._export_commit_tree(str(tmp_path), commit, str(dest))
    assert (dest / "code.py").read_text() == "committed\n"
