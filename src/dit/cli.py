"""Top-level CLI entry point for dit.

`dit run <workflow-path> [args...]` loads the Python module at
``<workflow-path>`` and invokes its ``main(argv) -> int`` entry point with
the remaining argv. The CLI is one consumer of the library; workflows can
also be invoked directly from a pytest target.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import click


@click.group()
def main() -> None:
    """Run cross-pipeline integration tests."""


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
    """Load WORKFLOW_PATH and invoke its main() with the remaining args."""
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
