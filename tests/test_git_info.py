import logging

from dit.git_info import warn_if_worker_image_misses_dirty_tree

DEFAULT = "us-central1-docker.pkg.dev/foo/bar:v0.9.6"


class _Counter:
    """Stand-in for a dirty-check that records how many times it was invoked."""

    def __init__(self, value: bool) -> None:
        self.value = value
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        return self.value


def _kwargs(dirty_fn, **overrides):
    base = dict(
        dirty_fn=dirty_fn,
        repo_dir="/workspace",
        runner="dataflow",
        worker_image=DEFAULT,
        default_worker_image=DEFAULT,
    )
    base.update(overrides)
    return base


def test_warns_when_dataflow_default_image_and_dirty(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    fn = _Counter(True)
    warn_if_worker_image_misses_dirty_tree(**_kwargs(fn))
    assert any("submitter tree" in r.message for r in caplog.records)
    assert fn.calls == 1


def test_silent_when_tree_clean(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    fn = _Counter(False)
    warn_if_worker_image_misses_dirty_tree(**_kwargs(fn))
    assert caplog.records == []
    assert fn.calls == 1  # dirty status had to be checked


def test_silent_when_worker_image_overridden(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    fn = _Counter(True)
    warn_if_worker_image_misses_dirty_tree(
        **_kwargs(fn, worker_image="gcr.io/foo/bar:my-build")
    )
    assert caplog.records == []
    assert fn.calls == 0  # dirty check skipped entirely


def test_silent_for_docker_runner(caplog):
    caplog.set_level(logging.WARNING, logger="dit.git_info")
    fn = _Counter(True)
    warn_if_worker_image_misses_dirty_tree(**_kwargs(fn, runner="docker"))
    assert caplog.records == []
    assert fn.calls == 0  # dirty check skipped entirely
