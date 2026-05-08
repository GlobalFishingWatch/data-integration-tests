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


if __name__ == "__main__":
    main()
