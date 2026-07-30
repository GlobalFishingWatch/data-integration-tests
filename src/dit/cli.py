"""Top-level CLI entry point for dit.

`dit run <workflow-path> [args...]` loads the Python module at
``<workflow-path>`` and invokes its ``main(argv) -> int`` entry point with
the remaining argv. The CLI is one consumer of the library; workflows can
also be invoked directly from a pytest target.
"""
from __future__ import annotations

import importlib.util
import os
import signal
import sys
from pathlib import Path

import click

from dit import runstate


@click.group()
def main() -> None:
    """Run cross-pipeline integration tests."""


# --------------------------------------------------------------------------
# SIGTERM cleanup (run-cache M6)
# --------------------------------------------------------------------------

#: Conventional shell exit code for "terminated by SIGTERM" (128 + 15).
_SIGTERM_EXIT_CODE = 143

#: Set once the handler starts cleaning up, so a second SIGTERM escalates to
#: an immediate exit instead of starting a second ``cancel_run``.
_cleanup_in_progress = False

#: Opt-out escape hatch. Set to any non-empty value to leave SIGTERM at its
#: default disposition -- useful when debugging a hang and you want the
#: run's Dataflow jobs left alive for inspection.
_DISABLE_ENV = "DIT_NO_CANCEL_ON_SIGTERM"


def _on_sigterm(signum: int, frame: object) -> None:
    """Cancel the active run's Dataflow jobs, then exit.

    **Why this exists.** A dit run submits Dataflow jobs that outlive the
    submitting process: killing ``dit run`` does not stop them. Cloud Build
    sends SIGTERM when a build is cancelled (and a force-push to a
    PR-triggered build cancels the in-flight one), so without this trap every
    cancellation orphans the run's jobs -- they keep burning worker-hours and
    Dataflow quota until someone notices and runs ``make dit-cancel``.

    Best-effort by construction: we are between SIGTERM and Cloud Build's
    follow-up SIGKILL, so cleanup gets whatever grace period the platform
    allows. :func:`dit.cache.cancel_run` is ordered to spend it well -- it
    cancels labelled Dataflow jobs FIRST, before any cache-row lookup, which
    is exactly right here because an interrupted run typically has live jobs
    but no cache row yet (rows are written only when a mode completes).

    Exits via :func:`sys.exit` rather than :func:`os._exit` so the workflow's
    own ``finally`` blocks still run -- git worktree teardown in the
    cross-version workflows, docker compose network removal in the runner.
    Those are cheap and local; the expensive remote cleanup already happened
    here.

    KNOWN LIMITATION -- region. :func:`dit.cache.cancel_run` discovers jobs in
    ``DIT_DATAFLOW_REGION`` (default ``us-central1``). A run that overrides
    the region with ``--dataflow-region`` alone, without also exporting
    ``DIT_DATAFLOW_REGION``, will have its jobs looked for in the wrong region
    and the handler will find nothing to cancel. The message below names the
    manual fallback (which takes an explicit region) for that case. Publishing
    the resolved region alongside the run_id in :mod:`dit.runstate` would
    close this; deferred as it needs a workflow-side change to thread through.
    """
    global _cleanup_in_progress
    if _cleanup_in_progress:
        # A second SIGTERM while cleanup is still running: the operator (or
        # the platform escalating) is insisting. Leave immediately rather
        # than stacking another cancel_run on top of the in-flight one.
        os._exit(_SIGTERM_EXIT_CODE)
    _cleanup_in_progress = True

    run_id = runstate.get_active_run_id()
    if run_id is None:
        # No run_id yet means no dit_run_id-labelled job can exist yet, so
        # there is genuinely nothing to cancel -- not an error.
        click.echo(
            "SIGTERM received before the run started; nothing to clean up.",
            err=True,
        )
        sys.exit(_SIGTERM_EXIT_CODE)

    click.echo(
        f"SIGTERM received -- cancelling run {run_id} (best-effort)...",
        err=True,
    )
    try:
        # Lazy import: keeps the BQ stack out of `dit run`'s startup path.
        from dit.cache import cancel_run

        cancel_run(run_id)
    except ValueError as e:
        # cancel_run raises ValueError for EXACTLY one condition: the run_id
        # matched no cache rows AND no labelled Dataflow jobs. Reaching it here
        # is the expected early-termination case, NOT a failure: the run_id is
        # published before the first job is submitted (digest resolution and
        # source snapshotting happen in between), so a SIGTERM in that window
        # has genuinely nothing to clean up. Reporting it as "cleanup FAILED"
        # would send the operator chasing a non-problem. (Copilot review, #70.)
        #
        # The one way this branch can hide a real leak is the region caveat
        # below: jobs submitted to a region we did not search look identical to
        # no jobs at all, so the message still points at the manual fallback.
        click.echo(
            f"run {run_id}: nothing found to cancel -- no labelled Dataflow "
            f"jobs and no cache rows (expected if SIGTERM arrived before the "
            f"first job was submitted).\n"
            f"If this run used a non-default --dataflow-region, re-check with "
            f"`make dit-cancel RUN_ID={run_id} REGION=<region>`.\n"
            f"({e})",
            err=True,
        )
    except Exception as e:  # noqa: BLE001 -- terminating anyway; never mask the exit
        # Includes cancel_run's RuntimeError (Dataflow job discovery itself
        # failed -- gcloud/auth/region problem), where jobs may well be running.
        click.echo(
            f"cleanup for run {run_id} FAILED: {e}\n"
            f"Run `make dit-cancel RUN_ID={run_id}` manually "
            f"(add REGION=<region> if the run used a non-default "
            f"--dataflow-region).",
            err=True,
        )
    else:
        click.echo(f"run {run_id} cancelled.", err=True)
    sys.exit(_SIGTERM_EXIT_CODE)


def _install_sigterm_handler() -> None:
    """Install :func:`_on_sigterm`, unless opted out via ``DIT_NO_CANCEL_ON_SIGTERM``.

    SIGTERM only -- deliberately not SIGINT. Cloud Build cancellation is the
    case this exists for; Ctrl-C keeps its conventional ``KeyboardInterrupt``
    behaviour so an interactive operator can still interrupt a run and inspect
    its jobs, with ``make dit-cancel`` as the explicit cleanup path.
    """
    if os.environ.get(_DISABLE_ENV):
        click.echo(
            f"{_DISABLE_ENV} set -- SIGTERM will NOT cancel the run's "
            "Dataflow jobs. Clean up with `make dit-cancel RUN_ID=<id>`.",
            err=True,
        )
        return
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except ValueError:
        # signal() only works on the main thread. If dit run is ever driven
        # from a worker thread, skip the trap rather than crash the run.
        pass


def _ensure_workflows_root_on_path(workflow_path: Path) -> None:
    """Put the directory containing the ``workflows/`` tree on ``sys.path``.

    Workflows are loaded via ``spec_from_file_location``, which creates a
    standalone module not bound to any package. Cross-workflow imports like
    ``from workflows.pipe_gaps.mode_equivalence import ...`` only resolve
    if the directory containing ``workflows/`` is discoverable, which it
    isn't by default in cloud runs (the laptop relies on ``PYTHONPATH=.``;
    Cloud Build doesn't set it). Walk up from the workflow file until we
    find a ``workflows/`` ancestor and prepend its parent to ``sys.path``.
    Idempotent: no-op if the entry is already present.
    """
    for parent in workflow_path.resolve().parents:
        if parent.name == "workflows":
            root = str(parent.parent)
            if root not in sys.path:
                sys.path.insert(0, root)
            return


@main.command(
    "run",
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    },
)
@click.argument(
    "workflow_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.argument("workflow_args", nargs=-1, type=click.UNPROCESSED)
def run(workflow_path: Path, workflow_args: tuple[str, ...]) -> None:
    """Load WORKFLOW_PATH and invoke its main() with the remaining args.

    Installs a SIGTERM trap first: a cancelled run (Cloud Build cancellation,
    force-push to a PR-triggered build) tears down its own Dataflow jobs
    instead of orphaning them. See :func:`_on_sigterm`.
    """
    _install_sigterm_handler()
    _ensure_workflows_root_on_path(workflow_path)
    spec = importlib.util.spec_from_file_location(workflow_path.stem, workflow_path)
    if spec is None or spec.loader is None:
        click.echo(f"could not load workflow at {workflow_path}", err=True)
        sys.exit(2)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    workflow_main = getattr(module, "main", None)
    if workflow_main is None:
        click.echo(
            f"workflow {workflow_path} has no main() entry point",
            err=True,
        )
        sys.exit(2)

    sys.exit(workflow_main(list(workflow_args)))


@main.command("cache-cancel")
@click.argument("run_id")
@click.option(
    "--region",
    default=None,
    help="Dataflow region to search for the run's jobs. "
         "Defaults to DIT_DATAFLOW_REGION, then us-central1.",
)
def cache_cancel(run_id: str, region: str | None) -> None:
    """Cancel run RUN_ID: cancel its Dataflow jobs, drop its output tables,
    and mark its dit_runs rows cancelled.

    RUN_ID is the per-run 12-hex id the workflow logs (``run_id=...``) and
    stamps as the ``dit_run_id`` Dataflow label. All sibling modes sharing
    that id are cleaned up together. Idempotent.
    """
    # Lazy import so `dit run` / `dit --help` don't import the BQ-touching
    # module (and its lazy google-cloud-bigquery dependency) unnecessarily.
    from dit.cache import cancel_run

    try:
        cancel_run(run_id, region=region)
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
