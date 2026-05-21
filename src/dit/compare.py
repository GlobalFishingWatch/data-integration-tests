"""Thin subprocess wrapper around the ``table-check`` CLI.

This module is deliberately a shim. New comparison features (tolerances,
output formats, dimensional breakdowns, ignore-columns) belong upstream in
``table_identical_checks``; do not grow them here.
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path

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
    """Run ``table-check summary`` between two tables; return identity verdict.

    Returns ``0`` if the tables are identical (post-tolerance) and ``1`` if
    any row differs. The exact differing-row count
    (``rows_only_in_a + rows_only_in_b + rows_in_both_with_differences``) is
    logged at INFO; the full per-column delta lives in the JSON file
    ``table-check`` writes via ``--output-json``.

    The 0/1 clamp is deliberate: callers commonly propagate this return value
    into ``sys.exit(main())`` (e.g. ``workflows/port_visits/ais.py``), and
    POSIX exit codes are truncated to 0-255. Returning the raw differing-row
    count would silently exit with status 0 whenever the count is a multiple
    of 256, falsely signalling success on a substantial real diff. Existing
    ``rc == 0`` / ``rc != 0`` callers (cross-version verdict, mode-equivalence
    compare_all) keep working with correct semantics.

    Counts come from ``table-check``'s ``--output-json`` summary, not the
    subprocess exit code -- ``table-check summary`` always exits 0 regardless
    of whether differences were found (it's informational, not assertional).
    The earlier shim returned that exit code and so reported "IDENTICAL" for
    every successful comparison; this implementation reads the real count
    from the JSON output then clamps as described above.

    Falls back to the subprocess exit code only when the JSON output is
    missing or unparseable -- which signals a real subprocess failure
    (auth, malformed query, etc.) rather than the informational success
    case. In that path the return value is the non-zero subprocess rc,
    preserving the "non-zero means something went wrong" contract.

    ``view_suffix`` is appended to both fully-qualified table names before
    the call. SCD-2 consumers (pipe-gaps, pipe-events) pass
    ``"_last_versions"``; truncate-shape consumers (port visits) pass ``""``.

    ``tolerance`` maps column name to absolute tolerance, forwarded as
    ``--tolerance=<col>:<value>`` to ``table-check`` (which natively
    supports per-column overrides via that syntax).

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

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        json_path = Path(f.name)

    try:
        cmd: list[str] = [
            "table-check",
            "summary",
            f"--table-a={target_a}",
            f"--table-b={target_b}",
            f"--keys={','.join(keys)}",
            "--format=table",
            f"--output-json={json_path}",
        ]
        if tolerance:
            for column, value in tolerance.items():
                cmd.append(f"--tolerance={column}:{value}")

        logger.info("running: %s", " ".join(cmd))
        result = subprocess.run(cmd, check=False)

        if not json_path.exists() or json_path.stat().st_size == 0:
            # Subprocess didn't produce a summary -- treat as failure.
            logger.warning("table-check produced no JSON output; falling back to exit code rc=%d",
                           result.returncode)
            return result.returncode or 1

        try:
            data = json.loads(json_path.read_text())
        except json.JSONDecodeError as exc:
            logger.warning("table-check JSON unparseable (%s); falling back to exit code rc=%d",
                           exc, result.returncode)
            return result.returncode or 1

        diff_rows = (
            int(data.get("rows_only_in_a", 0))
            + int(data.get("rows_only_in_b", 0))
            + int(data.get("rows_in_both_with_differences", 0))
        )
        logger.info("table-check verdict for %s vs %s: %d differing row(s)",
                    target_a, target_b, diff_rows)
        # Clamp to 0/1 so callers using this as a sys.exit() code don't hit
        # OS truncation (exit codes are mod-256); see the docstring for the
        # full reasoning.
        return 1 if diff_rows > 0 else 0
    finally:
        json_path.unlink(missing_ok=True)
