"""``dit run`` must load workflow files that define string-annotated dataclasses.

Regression for 2026-10-22: ``@dataclasses.dataclass`` under ``from __future__ import
annotations`` looks its module up in ``sys.modules``; ``dit run`` executed workflow
files without registering them, so ``cross_version_ais.py``, ``cross_version_fishing.py``
and ``identity_match_key.py`` failed at import with ``'NoneType' object has no attribute
'__dict__'``. No workflow ``main()`` is called here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from dit import cli

REPO_ROOT = Path(__file__).resolve().parents[1]

DATACLASS_WORKFLOW = """\
from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class Pins:
    table: str


def main(argv):
    print("ran with", argv, Pins("t").table)
    return 0
"""


def _spec(path: Path):
    return importlib.util.spec_from_file_location(path.stem, path)


def test_string_annotated_dataclass_workflow_loads(tmp_path: Path) -> None:
    path = tmp_path / "wf_with_dataclass.py"
    path.write_text(DATACLASS_WORKFLOW)

    module = cli._exec_workflow_module(_spec(path))

    assert module.Pins("x").table == "x"


def test_registration_is_temporary(tmp_path: Path) -> None:
    path = tmp_path / "wf_temporary.py"
    path.write_text(DATACLASS_WORKFLOW)

    cli._exec_workflow_module(_spec(path))

    assert "wf_temporary" not in sys.modules


def test_existing_module_with_the_same_name_is_restored(tmp_path: Path, monkeypatch) -> None:
    sentinel = object()
    monkeypatch.setitem(sys.modules, "wf_clash", sentinel)
    path = tmp_path / "wf_clash.py"
    path.write_text(DATACLASS_WORKFLOW)

    cli._exec_workflow_module(_spec(path))

    assert sys.modules["wf_clash"] is sentinel


def test_registration_is_removed_when_the_workflow_fails_to_import(tmp_path: Path) -> None:
    path = tmp_path / "wf_broken.py"
    path.write_text("raise RuntimeError('boom')\n")

    with pytest.raises(RuntimeError, match="boom"):
        cli._exec_workflow_module(_spec(path))

    assert "wf_broken" not in sys.modules


def test_dit_run_invokes_main_of_a_dataclass_workflow(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(cli, "_install_sigterm_handler", lambda: None)
    path = tmp_path / "wf_cli.py"
    path.write_text(DATACLASS_WORKFLOW)

    result = CliRunner().invoke(cli.main, ["run", str(path), "--flag"])

    assert result.exit_code == 0, result.output
    assert "ran with ['--flag'] t" in result.output


@pytest.mark.parametrize(
    "workflow",
    [
        "workflows/port_visits/cross_version_ais.py",
        "workflows/pipe_events/cross_version_fishing.py",
        "workflows/pipe_segment/identity_match_key.py",
    ],
)
def test_real_dataclass_workflows_load(workflow: str) -> None:
    path = REPO_ROOT / workflow
    cli._ensure_workflows_root_on_path(path)

    module = cli._exec_workflow_module(_spec(path))

    assert callable(module.main)
