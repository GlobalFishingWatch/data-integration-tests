"""Tests for the ``dit run`` SIGTERM cleanup trap (run-cache M6).

Signals themselves aren't raised here -- the handler is called directly, which
is what a real SIGTERM does anyway (Python dispatches to it on the main thread
between bytecodes). ``dit.cache.cancel_run`` is mocked throughout; its own
behaviour is covered in ``tests/test_cache.py``.
"""

from __future__ import annotations

import signal
from unittest.mock import patch

import pytest

from dit import cli, runstate


@pytest.fixture(autouse=True)
def _reset_state():
    """Each test starts with no active run and no in-flight cleanup."""
    runstate.clear_active_run_id()
    cli._cleanup_in_progress = False
    yield
    runstate.clear_active_run_id()
    cli._cleanup_in_progress = False


# --------------------------------------------------------------------------
# dit.runstate
# --------------------------------------------------------------------------

def test_runstate_roundtrip() -> None:
    assert runstate.get_active_run_id() is None
    runstate.set_active_run_id("abc123def456")
    assert runstate.get_active_run_id() == "abc123def456"
    runstate.clear_active_run_id()
    assert runstate.get_active_run_id() is None


def test_resolve_run_context_publishes_run_id() -> None:
    """The load-bearing wiring: the id minted inside the workflow's main()
    must reach the CLI's handler, or SIGTERM has nothing to cancel."""
    from dit import workflow

    # An explicit --suffix takes the git_info branch (no auto-snapshot); with
    # a clean, reviewed tree no worker image gets built either.
    with (
        patch.object(workflow, "git_info", return_value=("c0ffee1", False)),
        patch.object(workflow, "is_unreviewed", return_value=False),
        patch.object(workflow, "snapshot_parent", return_value=None),
        patch.object(workflow, "ensure_pipeline_image", return_value="img:tag"),
        patch.object(workflow, "dit_commit", return_value="ditsha0"),
    ):
        ctx = workflow.resolve_run_context(
            repo_dir="/tmp/whatever",
            pipeline_name="pipe-gaps",
            runner="dataflow",
            require_clean=False,
            worker_image="img:tag",
            default_worker_image="img:tag",
            suffix="explicit-suffix",
            resolve_digest=False,
        )

    assert runstate.get_active_run_id() == ctx.run_id


# --------------------------------------------------------------------------
# _on_sigterm
# --------------------------------------------------------------------------

def test_sigterm_before_run_starts_is_not_an_error() -> None:
    """No run_id means no dit_run_id-labelled job can exist yet -- exit
    cleanly without calling cancel_run (which would raise on an unknown id)."""
    with patch("dit.cache.cancel_run") as cancel:
        with pytest.raises(SystemExit) as exc:
            cli._on_sigterm(signal.SIGTERM, None)
    assert exc.value.code == cli._SIGTERM_EXIT_CODE
    cancel.assert_not_called()


def test_sigterm_cancels_the_active_run() -> None:
    runstate.set_active_run_id("deadbeef1234")
    with patch("dit.cache.cancel_run") as cancel:
        with pytest.raises(SystemExit) as exc:
            cli._on_sigterm(signal.SIGTERM, None)
    cancel.assert_called_once_with("deadbeef1234")
    assert exc.value.code == cli._SIGTERM_EXIT_CODE


def test_sigterm_still_exits_when_cleanup_fails(capsys) -> None:
    """A failing cancel_run must never mask the termination, and must tell the
    operator how to finish the job by hand."""
    runstate.set_active_run_id("deadbeef1234")
    with patch("dit.cache.cancel_run", side_effect=RuntimeError("gcloud exploded")):
        with pytest.raises(SystemExit) as exc:
            cli._on_sigterm(signal.SIGTERM, None)
    assert exc.value.code == cli._SIGTERM_EXIT_CODE
    err = capsys.readouterr().err
    assert "gcloud exploded" in err
    assert "make dit-cancel RUN_ID=deadbeef1234" in err


class _HardExit(Exception):
    """Stands in for ``os._exit``, which never returns in production. Without
    this the mocked call would fall through into the normal cleanup path and
    the test would assert the opposite of what it means to."""


def test_second_sigterm_exits_immediately_without_second_cancel() -> None:
    """Escalation path: a second SIGTERM while cleanup runs bails out via
    os._exit rather than stacking another cancel_run."""
    runstate.set_active_run_id("deadbeef1234")
    cli._cleanup_in_progress = True  # simulate cleanup already underway
    with (
        patch("dit.cache.cancel_run") as cancel,
        patch("os._exit", side_effect=_HardExit) as hard_exit,
    ):
        with pytest.raises(_HardExit):
            cli._on_sigterm(signal.SIGTERM, None)
    hard_exit.assert_called_once_with(cli._SIGTERM_EXIT_CODE)
    cancel.assert_not_called()


# --------------------------------------------------------------------------
# _install_sigterm_handler
# --------------------------------------------------------------------------

def test_install_registers_the_handler(monkeypatch) -> None:
    monkeypatch.delenv(cli._DISABLE_ENV, raising=False)
    with patch("signal.signal") as sig:
        cli._install_sigterm_handler()
    sig.assert_called_once_with(signal.SIGTERM, cli._on_sigterm)


def test_install_respects_the_opt_out(monkeypatch, capsys) -> None:
    monkeypatch.setenv(cli._DISABLE_ENV, "1")
    with patch("signal.signal") as sig:
        cli._install_sigterm_handler()
    sig.assert_not_called()
    # The opt-out must be loud -- a silently-disabled cleanup is how jobs leak.
    assert cli._DISABLE_ENV in capsys.readouterr().err


def test_install_tolerates_non_main_thread(monkeypatch) -> None:
    """signal.signal raises ValueError off the main thread; skip the trap
    rather than crashing the run."""
    monkeypatch.delenv(cli._DISABLE_ENV, raising=False)
    with patch("signal.signal", side_effect=ValueError("not main thread")):
        cli._install_sigterm_handler()  # must not raise
