"""Unit tests for ``dit.worker_image.ensure_pipeline_image``.

The gcloud calls (image existence check + kaniko build submit) are mocked;
these cover the trigger matrix and idempotency, not a real build. Trigger is
symmetric across both consumers (Beam workers + dit's docker runner): build
when ALL of (a) ``worker_image == default_worker_image`` (no explicit
override) and (b) ``unreviewed`` is True (published default doesn't have
these changes). Otherwise pass through.
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


def test_no_ensure_worker_image_back_compat_alias() -> None:
    """The prior ``ensure_worker_image`` name is intentionally NOT aliased to
    the renamed function -- the signature also changed (no more ``runner`` /
    ``need_registry_image`` kwargs), so a back-compat alias would silently
    accept the old call shape and crash at the kwarg-binding level instead of
    giving a clear ``ImportError``."""
    assert not hasattr(worker_image, "ensure_worker_image")


# --------------------------------------------------------------------------
# Trigger matrix (symmetric across both consumers)
# --------------------------------------------------------------------------

def test_explicit_worker_image_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit non-default ``worker_image`` is always respected, regardless
    of ``unreviewed`` — the caller is in control."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    custom = "gcr.io/world-fishing-827/dit/pipe-gaps:my-custom"
    out = worker_image.ensure_pipeline_image(**_kwargs(worker_image=custom))
    assert out == custom


def test_reviewed_code_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reviewed code at the pinned default: no build, return canonical."""
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    out = worker_image.ensure_pipeline_image(**_kwargs(unreviewed=False))
    assert out == DEFAULT


def test_unreviewed_existing_image_skips_build(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unreviewed code + default + image already in dit/ registry: no rebuild."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: True)
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("rebuilt"))
    out = worker_image.ensure_pipeline_image(**_kwargs())
    assert out == "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"


def test_unreviewed_missing_image_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unreviewed code + default + image missing: kaniko build under dit/."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    built = []
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: built.append((repo, tag, commit)))
    out = worker_image.ensure_pipeline_image(**_kwargs())
    tag = "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"
    assert out == tag
    assert built == [("/repo", tag, "abc1234")]


def test_pipeline_namespacing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The produced tag is keyed on the pipeline name, not hardcoded."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    captured = {}
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: captured.update(tag=tag))
    worker_image.ensure_pipeline_image(
        **_kwargs(pipeline="anchorages_pipeline", commit="def5678")
    )
    assert captured["tag"] == "gcr.io/world-fishing-827/dit/anchorages_pipeline:dit-def5678"


def test_pipe_events_pipeline_namespacing(monkeypatch: pytest.MonkeyPatch) -> None:
    """pipe-events (the docker-runner consumer) uses the same machinery."""
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    captured = {}
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag, commit: captured.update(tag=tag))
    worker_image.ensure_pipeline_image(
        **_kwargs(pipeline="pipe-events", commit="abc1234")
    )
    assert captured["tag"] == "gcr.io/world-fishing-827/dit/pipe-events:dit-abc1234"


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
