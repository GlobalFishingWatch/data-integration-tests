"""Unit tests for dit.worker_image.ensure_worker_image.

The gcloud calls (image existence check + kaniko build submit) are mocked;
these cover the trigger matrix and idempotency, not a real build.
"""

from __future__ import annotations

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


def test_tag_is_content_addressable() -> None:
    assert (
        worker_image.worker_image_tag("pipe-gaps", "abc1234")
        == "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"
    )


def test_docker_runner_never_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: pytest.fail("checked"))
    out = worker_image.ensure_worker_image(**_kwargs(runner="docker"))
    assert out == DEFAULT


def test_explicit_worker_image_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    custom = "gcr.io/world-fishing-827/dit/pipe-gaps:my-custom"
    out = worker_image.ensure_worker_image(**_kwargs(worker_image=custom))
    assert out == custom


def test_reviewed_code_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("built"))
    out = worker_image.ensure_worker_image(**_kwargs(unreviewed=False))
    assert out == DEFAULT


def test_unreviewed_existing_image_skips_build(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: True)
    monkeypatch.setattr(worker_image, "_build_and_push", lambda *a, **k: pytest.fail("rebuilt"))
    out = worker_image.ensure_worker_image(**_kwargs())
    assert out == "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"


def test_unreviewed_missing_image_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    built = []
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag: built.append((repo, tag)))
    out = worker_image.ensure_worker_image(**_kwargs())
    tag = "gcr.io/world-fishing-827/dit/pipe-gaps:dit-abc1234"
    assert out == tag
    assert built == [("/repo", tag)]


def test_pipeline_namespacing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_image, "_image_exists", lambda t: False)
    captured = {}
    monkeypatch.setattr(worker_image, "_build_and_push", lambda repo, tag: captured.update(tag=tag))
    worker_image.ensure_worker_image(
        **_kwargs(pipeline="anchorages_pipeline", commit="def5678")
    )
    assert captured["tag"] == "gcr.io/world-fishing-827/dit/anchorages_pipeline:dit-def5678"


def test_build_raises_without_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    # Point _dit_root at a dir lacking docker/worker-image/cloudbuild.yaml.
    monkeypatch.setattr(worker_image, "_dit_root", lambda: tmp_path)
    with pytest.raises(RuntimeError, match="editable dit install"):
        worker_image._build_and_push("/repo", "gcr.io/x/y:z")
