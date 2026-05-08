"""Thin subprocess wrapper around the ``table-check`` CLI.

This module is deliberately a shim. New comparison features (tolerances,
output formats, dimensional breakdowns, ignore-columns) belong upstream in
``table_identical_checks``; do not grow them here.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence

logger = logging.getLogger(__name__)


def compare_tables(
    table_a: str,
    table_b: str,
    *,
    keys: Sequence[str],
    view_suffix: str = "",
    ignore_columns: Sequence[str] = (),
    tolerance: dict[str, float] | None = None,
) -> int:
    """Run ``table-check summary`` between two output tables.

    Returns the ``table-check`` exit code (0 on identical, non-zero on diff).

    ``view_suffix`` is appended to both fully-qualified table names before the
    call. SCD-2 consumers (pipe-gaps, pipe-events) pass ``"_last_versions"``;
    truncate-shape consumers (port visits) pass ``""``.

    ``tolerance`` maps column name to absolute tolerance, forwarded as
    ``--tolerance=<col>:<value>`` to ``table-check`` (which natively supports
    per-column overrides via that syntax).

    ``ignore_columns`` is part of the contract but ``table-check summary``
    does not yet support it. Passing a non-empty value raises
    :class:`NotImplementedError`; the feature should be added upstream in
    ``table_identical_checks`` rather than reimplemented here.
    """
    if ignore_columns:
        raise NotImplementedError(
            "ignore_columns is not yet supported by `table-check summary`; "
            "add it upstream in table_identical_checks rather than in dit.compare"
        )

    target_a = f"{table_a}{view_suffix}"
    target_b = f"{table_b}{view_suffix}"
    cmd: list[str] = [
        "table-check",
        "summary",
        f"--table-a={target_a}",
        f"--table-b={target_b}",
        f"--keys={','.join(keys)}",
        "--format=table",
    ]
    if tolerance:
        for column, value in tolerance.items():
            cmd.append(f"--tolerance={column}:{value}")

    logger.info("running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    return result.returncode
