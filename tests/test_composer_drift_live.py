"""Opt-in: the drift specs against a real composer-dags-production checkout.

Skipped unless a checkout with ``origin/main`` is found, as in
``tests/test_composer.py``: ``$DIT_COMPOSER_DAGS_DIR``, else
``$PROJECTS/composer-dags-production``, else a sibling of this repo. The
checkout is read with ``git show`` only and never fetched, so these tests see
whatever ``origin/main`` it last fetched.

They assert structure and one verdict, never production's values:

* every spec's snapshot and task ids exist, so every spec can be read;
* port visits, the one workflow expected in sync, has no drift.

The second fails when production changes what its port-visits tasks run.
That is the signal this check exists for, not a broken test: read
``dit run workflows/composer_drift.py --workflow port_visits/ais --detail``.
"""

from __future__ import annotations

import pytest

from dit import composer, drift
from workflows import composer_drift as runner


def _live_commit() -> str | None:
    repo = composer.default_composer_dir()
    if not (repo / ".git").exists():
        return None
    try:
        return composer.resolve_ref(repo, composer.DEFAULT_REF)
    except RuntimeError:
        return None


pytestmark = pytest.mark.skipif(
    _live_commit() is None, reason="no composer-dags-production checkout with origin/main",
)


@pytest.fixture(scope="module")
def live() -> tuple[tuple[drift.WorkflowReport, ...], tuple[drift.WorkflowError, ...]]:
    return drift.run_specs(runner.SPECS, composer.default_composer_dir(), _live_commit())


def test_every_specs_snapshot_and_tasks_exist(live) -> None:
    reports, errors = live

    assert [(e.spec.name, e.message) for e in errors] == []
    assert [r.spec.name for r in reports] == [spec.name for spec in runner.SPECS]
    for report in reports:
        assert [s.task.task_id for s in report.steps] == [
            step.task_id for step in report.spec.steps
        ]
        # Every mirrored task is a container task with an image and a command.
        assert all(s.task.image and s.argv.production.positionals for s in report.steps)


def test_port_visits_is_in_sync_with_production(live) -> None:
    reports, _ = live
    (report,) = [r for r in reports if r.spec.name == "port_visits/ais"]

    assert report.spec.in_sync
    assert [f.field for f in report.fields if not f.matches] == []
    assert [s.spec.step for s in report.steps if s.drift] == []
    assert drift.exit_code(reports, ()) == 0
