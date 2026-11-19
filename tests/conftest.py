"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from dit.workflow import MAX_NUM_WORKERS_CEILING_ENV


@pytest.fixture(autouse=True)
def _no_worker_cap_ceiling(monkeypatch):
    """Workflows read DIT_MAX_NUM_WORKERS_CEILING when they parse their
    arguments; clear it so a developer's environment cannot change what the
    tests see. Tests that need it set it themselves."""
    monkeypatch.delenv(MAX_NUM_WORKERS_CEILING_ENV, raising=False)
