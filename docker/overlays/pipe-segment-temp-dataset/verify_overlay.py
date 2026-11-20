"""Self-check for the pipe-segment temp-dataset overlay (run inside the image by build.sh).

Usage: verify_overlay.py --patch <patch file name> [--package-dir DIR] [--static-only]

Static check (needs no pipe-segment and no Beam, so DIT's tests run it too):

* every call to Beam's ``ReadFromBigQuery`` in the ``pipe_segment`` package passes
  ``**temp_dataset_kwargs()``;
* the modules that read BigQuery, and how many reads each has, are exactly those the
  patch covers (``EXPECTED_READS_BY_PATCH``). A read added upstream, in a patched
  module or in a new one, fails the build instead of the Dataflow job;
* no module uses ``ReadAllFromBigQuery``, which the patch does not cover.

Runtime check (inside the image): with the variable unset the helper adds nothing;
set, it returns a ``DatasetReference``; Beam's ``ReadFromBigQuery`` forwards it to the
export source, whose ``_setup_temporary_dataset`` then returns without creating
anything; and every patched module imports the helper.

The pipe-anchorages overlay's first patch missed one of two reads and its first
Dataflow smoke failed on ``bigquery.datasets.create`` (2026-10-22). That is why the
reads are counted here.
"""

from __future__ import annotations

import argparse
import ast
import importlib
import importlib.util
import os
from pathlib import Path

#: BigQuery reads per module (path relative to the ``pipe_segment`` package), for each
#: patch. v5.1.0 added the read of the prior segments (the incremental segmenter).
_READS_V5_0_3 = {
    "segment_identity/read_source.py": 1,   # segments joined with fragments
    "transform/read_fragments.py": 1,       # the fragment history (full-history segmenter)
    "transform/read_messages.py": 1,        # the normalized messages
    "transform/satellite_offsets.py": 1,    # satellite timing offsets (DIT never runs it)
}
EXPECTED_READS_BY_PATCH = {
    "temp_dataset_v5.0.3.patch": _READS_V5_0_3,
    "temp_dataset.patch": {
        **_READS_V5_0_3,
        "transform/read_prior_segments.py": 1,  # the state of the day before the range
    },
}

HELPER = "temp_dataset_kwargs"
READ = "ReadFromBigQuery"
#: Other Beam entry points into a BigQuery query read; the patch covers none of them.
UNCOVERED_READS = ("ReadAllFromBigQuery",)


def _callee(node: ast.expr) -> str | None:
    """The last name of a call target: ``beam.io.ReadFromBigQuery`` -> ``ReadFromBigQuery``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _calls(source: str, name: str) -> list[ast.Call]:
    return [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and _callee(node.func) == name
    ]


def _passes_helper(call: ast.Call) -> bool:
    """Whether ``call`` has a ``**temp_dataset_kwargs()`` argument."""
    return any(
        keyword.arg is None
        and isinstance(keyword.value, ast.Call)
        and _callee(keyword.value.func) == HELPER
        for keyword in call.keywords
    )


def bigquery_reads(source: str) -> tuple[int, int]:
    """``(reads, patched reads)`` in one module's source. Pure."""
    reads = _calls(source, READ)
    return len(reads), sum(map(_passes_helper, reads))


def uncovered_reads(source: str) -> list[str]:
    """The names in ``UNCOVERED_READS`` that ``source`` calls. Pure."""
    return [name for name in UNCOVERED_READS if _calls(source, name)]


def reads_by_module(package_dir: Path) -> dict[str, tuple[int, int]]:
    """``{module path: (reads, patched reads)}`` for every module of the package that
    reads BigQuery through Beam."""
    counts = {
        path.relative_to(package_dir).as_posix(): bigquery_reads(path.read_text())
        for path in sorted(package_dir.rglob("*.py"))
    }
    return {rel: count for rel, count in counts.items() if count[0]}


def check_static(package_dir: Path, patch: str) -> None:
    """Assert the package's BigQuery reads are exactly the patched ones."""
    expected = EXPECTED_READS_BY_PATCH[patch]
    found = reads_by_module(package_dir)
    assert {rel: reads for rel, (reads, _) in found.items()} == expected, (
        f"BigQuery reads per module differ from what {patch} covers.\n"
        f"  found:    { {rel: reads for rel, (reads, _) in found.items()} }\n"
        f"  expected: {expected}\n"
        "A read was added, moved or removed upstream: update the patch and "
        "EXPECTED_READS_BY_PATCH together."
    )
    unpatched = {rel: count for rel, count in found.items() if count[0] != count[1]}
    assert not unpatched, f"reads without **{HELPER}(): {unpatched} (reads, patched)"
    for path in sorted(package_dir.rglob("*.py")):
        names = uncovered_reads(path.read_text())
        rel = path.relative_to(package_dir)
        assert not names, f"{rel} uses {names}, which the patch does not cover"


def _module_name(rel: str) -> str:
    return "pipe_segment." + rel.removesuffix(".py").replace("/", ".")


def check_runtime(patch: str) -> None:
    """Assert the helper and Beam behave as the overlay needs. Imports pipe-segment and Beam."""
    from apache_beam.io.gcp import bigquery as beam_bq
    from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
    from pipe_segment import _dit_overlay

    os.environ.pop(_dit_overlay.ENV, None)
    assert _dit_overlay.temp_dataset_kwargs() == {}, "unset env must add nothing"

    os.environ[_dit_overlay.ENV] = "world-fishing-827.tech_great_expectations"
    kwargs = _dit_overlay.temp_dataset_kwargs()
    ref = kwargs["temp_dataset"]
    assert isinstance(ref, DatasetReference)
    assert (ref.projectId, ref.datasetId) == ("world-fishing-827", "tech_great_expectations")

    read = beam_bq.ReadFromBigQuery(query="SELECT 1", use_standard_sql=True, **kwargs)
    assert read._kwargs["temp_dataset"] is ref, "ReadFromBigQuery must forward temp_dataset"

    source = beam_bq._CustomBigQuerySource(
        query="SELECT 1", use_standard_sql=True, temp_dataset=ref,
        method=beam_bq.ReadFromBigQuery.Method.EXPORT,
        job_name="j", step_name="s", unique_id="u",
    )

    class _NoCreate:
        def get_query_location(self, *a, **k):
            raise AssertionError("must not look up a location to create a dataset")

        def create_temporary_dataset(self, *a, **k):
            raise AssertionError("must not create a temporary dataset")

    source._setup_temporary_dataset(_NoCreate())

    for rel in EXPECTED_READS_BY_PATCH[patch]:
        module = importlib.import_module(_module_name(rel))
        assert getattr(module, HELPER) is _dit_overlay.temp_dataset_kwargs, module.__name__


def _installed_package_dir() -> Path:
    spec = importlib.util.find_spec("pipe_segment")
    if spec is None or not spec.submodule_search_locations:
        raise SystemExit("pipe_segment is not installed; pass --package-dir")
    return Path(next(iter(spec.submodule_search_locations)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--patch", required=True, choices=sorted(EXPECTED_READS_BY_PATCH),
                        help="The patch the tree was built with.")
    parser.add_argument("--package-dir", type=Path, default=None,
                        help="The pipe_segment package directory. Default: the installed one.")
    parser.add_argument("--static-only", action="store_true",
                        help="Count the reads only; import neither pipe-segment nor Beam.")
    args = parser.parse_args(argv)

    package_dir = args.package_dir or _installed_package_dir()
    check_static(package_dir, args.patch)
    if not args.static_only:
        check_runtime(args.patch)
    reads = sum(EXPECTED_READS_BY_PATCH[args.patch].values())
    print(f"overlay self-check OK ({args.patch}: {reads} BigQuery reads, all patched)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
