import logging

from dit.git_info import warn_if_worker_image_misses_dirty_tree

DEFAULT = "us-central1-docker.pkg.dev/foo/bar:v0.9.6"


def _kwargs(**overrides):
    base = dict(
        dirty=True,
        repo_dir="/workspace",
        runner="dataflow",
        worker_image=DEFAULT,
        default_worker_image=DEFAULT,
    )
    base.update(overrides)
    return base


def test_warns_when_dataflow_default_image_and_dirty(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    warn_if_worker_image_misses_dirty_tree(**_kwargs())
    assert any("submitter tree" in r.message for r in caplog.records)


def test_silent_when_tree_clean(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    warn_if_worker_image_misses_dirty_tree(**_kwargs(dirty=False))
    assert caplog.records == []


def test_silent_when_worker_image_overridden(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    warn_if_worker_image_misses_dirty_tree(
        **_kwargs(worker_image="gcr.io/foo/bar:my-build")
    )
    assert caplog.records == []


def test_silent_for_docker_runner(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    warn_if_worker_image_misses_dirty_tree(**_kwargs(runner="docker"))
    assert caplog.records == []
