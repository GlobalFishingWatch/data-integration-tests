"""Cross-version experiment for pipe-anchorages port-visits (AIS).

Pins the source data via BQ snapshots at a fixed timestamp, runs
``workflows/port_visits/ais.py`` once per pipeline-version binding (with each
binding pointed at the snapshotted inputs), and then diffs corresponding output
tables pairwise across bindings. Diff results are reported but do not fail the
run — the *point* of cross-version testing is to surface behaviour change, so
a non-empty diff is information, not error.

Example: validate PIPELINE-1465 by comparing v4.6.4 against the fix branch::

    dit run workflows/port_visits/cross_version_ais.py \\
        --experiment-id pipeline-1465 \\
        --pin-source-at 2026-05-15T10:00:00Z \\
        --binding v464=v4.6.4 \\
        --binding fix=fix/PIPELINE-1465_port_visit_start_location \\
        --modes 1_bf \\
        --runner dataflow --parallel --build-from-source

Steps:

1. Verify every binding's git ref exists in ``$PROJECTS/anchorages_pipeline``.
2. Create snapshot datasets ``dit_exp_<sanitized_exp_id>_{internal,published}``
   (idempotent; default 7-day expiration).
3. ``dit.bq.snapshot_dataset`` the three workflow input tables
   (``messages_positions``, ``segment_info``, ``segs_activity``) from the source
   stem into the snapshot datasets at ``--pin-source-at``.
4. For each binding: ``git worktree add`` a temp dir at the ref, invoke
   ``ais.py`` from that worktree with overridden ``--source-dataset-stem`` and a
   binding-scoped ``--suffix``, then tear down the worktree.
5. For each mode in ``--modes`` and each pair of bindings, compare the
   corresponding ``port_visits_<exp>-<binding>_<mode>`` tables on ``visit_id``.

``--dry-run`` skips the ``ais.py`` invocations and the diff phase but still
performs dataset creation, snapshotting, and worktree setup/teardown — useful
for validating the orchestration without burning Dataflow cost.
"""
from __future__ import annotations

import argparse
import itertools
import logging
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from dit import bq as dit_bq
from dit import compare as dit_compare

logger = logging.getLogger(__name__)


PROJECT = "world-fishing-827"
DEFAULT_DEST_DATASET = os.environ.get("DIT_DEST_DATASET", "tech_great_expectations")
DEFAULT_SOURCE_DATASET_STEM = "pipe_ais_test_202408290000"
DEFAULT_MODES = "1_bf,2_bfd,3_bftruncate"
DEFAULT_PROJECTS_DIR = os.environ.get("PROJECTS") or str(Path(__file__).resolve().parents[2].parent)
DEFAULT_PIPELINE_DIR = os.path.join(DEFAULT_PROJECTS_DIR, "anchorages_pipeline")
DEFAULT_SNAPSHOT_EXPIRATION_DAYS = 7

# Required tables in each half of the source dataset.
SOURCE_TABLES = {
    "_internal": ("messages_positions",),
    "_published": ("segment_info", "segs_activity"),
}

AIS_WORKFLOW = Path(__file__).with_name("ais.py")


# --------------------------------------------------------------------------
# Argparse
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> tuple[argparse.Namespace, list[str]]:
    p = argparse.ArgumentParser(
        description="Cross-version experiment for pipe-anchorages port-visits.",
    )
    p.add_argument("--experiment-id", required=True,
                   help="Slug for the experiment; appears in snapshot dataset name and output-table suffixes.")
    p.add_argument("--pin-source-at", required=True,
                   help="ISO 8601 timestamp for the source-data snapshot (e.g. 2026-05-15T10:00:00Z).")
    p.add_argument("--binding", action="append", required=True, dest="bindings",
                   help="`name=ref` pair, repeatable. Both must be valid git refs in --pipeline-dir.")
    p.add_argument("--modes", default=DEFAULT_MODES,
                   help=f"Comma-separated mode names whose output tables get diffed pairwise. Default {DEFAULT_MODES}.")
    p.add_argument("--pipeline-dir", default=DEFAULT_PIPELINE_DIR,
                   help="anchorages_pipeline checkout used for git worktrees.")
    p.add_argument("--source-dataset-stem", default=DEFAULT_SOURCE_DATASET_STEM,
                   help="Source dataset stem to snapshot from (the workflow's --source-dataset-stem default).")
    p.add_argument("--dest-dataset", default=DEFAULT_DEST_DATASET,
                   help="BQ dataset holding the output tables produced by each binding's ais.py run.")
    p.add_argument("--snapshot-expiration-days", type=int, default=DEFAULT_SNAPSHOT_EXPIRATION_DAYS,
                   help="default_table_expiration for the created snapshot datasets, in days.")
    p.add_argument("--dry-run", action="store_true",
                   help="Skip ais.py invocations and pairwise diffs; still creates datasets / snapshots / worktrees.")
    args, ais_extra_args = p.parse_known_args(argv)
    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    args.pin_source_at = _parse_iso8601(args.pin_source_at)
    return args, ais_extra_args


def _parse_binding(spec: str) -> tuple[str, str]:
    if "=" not in spec:
        raise SystemExit(f"--binding must be name=ref; got: {spec!r}")
    name, ref = spec.split("=", 1)
    if not name or not ref:
        raise SystemExit(f"--binding must be name=ref with both parts non-empty; got: {spec!r}")
    return name, ref


def _parse_iso8601(s: str) -> datetime:
    # Accept both "...Z" and explicit offsets.
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s)
    except ValueError as exc:
        raise SystemExit(f"--pin-source-at: invalid ISO 8601 timestamp {s!r}: {exc}") from exc


# --------------------------------------------------------------------------
# Pre-flight: verify refs
# --------------------------------------------------------------------------

def _verify_refs(pipeline_dir: str, bindings: list[tuple[str, str]]) -> None:
    if not Path(pipeline_dir, ".git").exists():
        raise SystemExit(f"--pipeline-dir {pipeline_dir} is not a git repo.")
    for name, ref in bindings:
        result = subprocess.run(
            ["git", "-C", pipeline_dir, "rev-parse", "--verify", "--quiet", ref + "^{commit}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise SystemExit(
                f"binding {name!r} ref {ref!r} not found in {pipeline_dir}. "
                f"Fetch it first (e.g. `git -C {pipeline_dir} fetch origin {ref}`)."
            )
        logger.info("binding %s: ref %s resolves to %s", name, ref, result.stdout.strip())


# --------------------------------------------------------------------------
# Snapshot datasets
# --------------------------------------------------------------------------

def _sanitize_for_dataset(s: str) -> str:
    # BQ dataset names: letters, digits, underscore only; must start with letter or underscore.
    return s.replace("-", "_")


def _snapshot_stem(experiment_id: str) -> str:
    return f"dit_exp_{_sanitize_for_dataset(experiment_id)}"


def _ensure_dataset(fq_name: str, *, expiration_days: int) -> None:
    from google.cloud import bigquery
    from google.cloud.exceptions import Conflict

    client = bigquery.Client(project=PROJECT)
    dataset = bigquery.Dataset(fq_name)
    dataset.default_table_expiration_ms = expiration_days * 24 * 60 * 60 * 1000
    try:
        client.create_dataset(dataset, exists_ok=True)
    except Conflict:
        pass  # exists_ok=True should swallow this, but defend against version drift
    logger.info("ensured dataset %s (expiration %dd)", fq_name, expiration_days)


def _snapshot_source(args: argparse.Namespace) -> str:
    snap_stem = _snapshot_stem(args.experiment_id)
    for half, tables in SOURCE_TABLES.items():
        src_dataset = f"{PROJECT}.{args.source_dataset_stem}{half}"
        dst_dataset = f"{PROJECT}.{snap_stem}{half}"
        _ensure_dataset(dst_dataset, expiration_days=args.snapshot_expiration_days)
        created = dit_bq.snapshot_dataset(
            src_dataset, dst_dataset,
            tables=list(tables),
            as_of=args.pin_source_at,
            project=PROJECT,
        )
        logger.info("snapshotted %d table(s) from %s into %s (as_of=%s): %s",
                    len(created), src_dataset, dst_dataset, args.pin_source_at.isoformat(),
                    [t.split('.')[-1] for t in created])
    return snap_stem


# --------------------------------------------------------------------------
# Per-binding run via git worktree
# --------------------------------------------------------------------------

def _ais_args_for_binding(extra_args: list[str], *, snap_stem: str, suffix: str) -> list[str]:
    """Strip user-supplied --source-dataset-stem / --suffix / --experiment-id
    so the wrapper's overrides win."""
    drop_kvs = {"--source-dataset-stem", "--suffix", "--experiment-id"}
    out: list[str] = []
    skip_next = False
    for arg in extra_args:
        if skip_next:
            skip_next = False
            continue
        if arg in drop_kvs:
            skip_next = True
            continue
        if any(arg.startswith(k + "=") for k in drop_kvs):
            continue
        out.append(arg)
    out.extend([
        "--source-dataset-stem", snap_stem,
        "--suffix", suffix,
        "--allow-dirty-tree",  # worktree's git status is clean but ais.py's _git_info still triggers; harmless
    ])
    return out


def _run_binding(
    *,
    name: str,
    ref: str,
    snap_stem: str,
    suffix: str,
    pipeline_dir: str,
    ais_extra_args: list[str],
    dry_run: bool,
) -> int:
    worktree_dir = tempfile.mkdtemp(prefix=f"dit-xv-{name}-")
    try:
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "add", "--force", worktree_dir, ref],
            check=True, capture_output=True, text=True,
        )
        logger.info("binding %s: worktree at %s @ %s", name, worktree_dir, ref)

        argv = _ais_args_for_binding(ais_extra_args, snap_stem=snap_stem, suffix=suffix)
        cmd = [sys.executable, str(AIS_WORKFLOW), *argv]
        logger.info("binding %s: invoking %s", name, " ".join(shlex.quote(c) for c in cmd))

        if dry_run:
            logger.info("binding %s: --dry-run set; skipping ais.py invocation", name)
            return 0

        env = {**os.environ}
        result = subprocess.run(cmd, cwd=worktree_dir, env=env, check=False)
        return result.returncode
    finally:
        # `git worktree remove --force` works even if the worktree path was modified.
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "remove", "--force", worktree_dir],
            check=False, capture_output=True,
        )
        # Tolerate residue if git's bookkeeping somehow left files behind.
        shutil.rmtree(worktree_dir, ignore_errors=True)
        logger.info("binding %s: worktree torn down", name)


# --------------------------------------------------------------------------
# Pairwise diffs
# --------------------------------------------------------------------------

def _visits_table(dest_dataset: str, suffix: str, mode: str) -> str:
    return f"{PROJECT}.{dest_dataset}.port_visits_{suffix}_{mode}"


def _diff_pair(
    *, dest_dataset: str, a_suffix: str, b_suffix: str, mode: str,
) -> int:
    return dit_compare.compare_tables(
        _visits_table(dest_dataset, a_suffix, mode),
        _visits_table(dest_dataset, b_suffix, mode),
        keys=["visit_id"],
        view_suffix="",
    )


def _run_diffs(
    *, modes: list[str], suffix_by_binding: dict[str, str], dest_dataset: str,
) -> dict[tuple[str, str, str], int]:
    results: dict[tuple[str, str, str], int] = {}
    bindings = list(suffix_by_binding.keys())
    for mode in modes:
        for a, b in itertools.combinations(bindings, 2):
            rc = _diff_pair(
                dest_dataset=dest_dataset,
                a_suffix=suffix_by_binding[a],
                b_suffix=suffix_by_binding[b],
                mode=mode,
            )
            results[(mode, a, b)] = rc
            verdict = "IDENTICAL" if rc == 0 else f"DIFFERENT (table-check rc={rc})"
            logger.info("diff mode=%s %s vs %s -> %s", mode, a, b, verdict)
    return results


def _summarize(results: dict[tuple[str, str, str], int]) -> str:
    lines = ["", "Cross-version diff summary:"]
    for (mode, a, b), rc in results.items():
        verdict = "IDENTICAL" if rc == 0 else "DIFFERENT"
        lines.append(f"  mode={mode}  {a} vs {b}  -> {verdict}  (rc={rc})")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args, ais_extra_args = parse_args(argv)

    logger.info("experiment_id: %s", args.experiment_id)
    logger.info("pin_source_at: %s", args.pin_source_at.isoformat())
    logger.info("bindings: %s", args.bindings)
    logger.info("modes: %s", args.modes)
    logger.info("pipeline_dir: %s", args.pipeline_dir)
    logger.info("source stem -> snapshot stem: %s -> %s",
                args.source_dataset_stem, _snapshot_stem(args.experiment_id))

    _verify_refs(args.pipeline_dir, args.bindings)
    snap_stem = _snapshot_source(args)

    # Each binding gets a deterministic suffix the diff step can address.
    suffix_by_binding = {
        name: f"{args.experiment_id}-{name}" for name, _ in args.bindings
    }

    for name, ref in args.bindings:
        rc = _run_binding(
            name=name, ref=ref,
            snap_stem=snap_stem,
            suffix=suffix_by_binding[name],
            pipeline_dir=args.pipeline_dir,
            ais_extra_args=ais_extra_args,
            dry_run=args.dry_run,
        )
        if rc != 0:
            raise SystemExit(f"binding {name!r} (ref={ref}) failed with rc={rc}")

    if args.dry_run:
        logger.info("--dry-run set; skipping pairwise diffs.")
        return 0

    results = _run_diffs(
        modes=args.modes,
        suffix_by_binding=suffix_by_binding,
        dest_dataset=args.dest_dataset,
    )
    print(_summarize(results), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
