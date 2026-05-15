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
        --binding before=v4.6.4 \\
        --binding after=fix/PIPELINE-1465_port_visit_start_location \\
        --binding-worker-image after=gcr.io/world-fishing-827/pipe-anchorages-1465-after:latest \\
        --modes 1_bf \\
        --runner dataflow --parallel --build-from-source

Steps:

1. Verify every binding's git ref exists in ``$PROJECTS/anchorages_pipeline``.
2. Create snapshot datasets ``dit_exp_<sanitized_exp_id>_{internal,published}``
   (idempotent; default 7-day expiration).
3. ``dit.bq.snapshot_dataset`` the three workflow input tables
   (``messages_positions``, ``segment_info``, ``segs_activity``) from the source
   stem into the snapshot datasets at ``--pin-source-at``.
4. For each binding (in parallel by default; ``--sequential-bindings`` opts out):
   ``git worktree add`` a temp dir at the ref, invoke ``ais.py`` from that
   worktree with overridden ``--source-dataset-stem``, a binding-scoped
   ``--suffix``, and optionally a per-binding ``--worker-image`` (from
   ``--binding-worker-image NAME=IMAGE``), then tear down the worktree.
   Each subprocess's stdout/stderr is line-prefixed ``[<binding>] `` so
   parallel runs interleave readably.
5. For each mode in ``--modes`` and each pair of bindings, compare the
   corresponding ``port_visits_<exp>-<binding>_<mode>`` tables on ``visit_id``.
   Pairs touching a binding that failed (rc != 0) are SKIPPED, not diffed.

The overall exit code is non-zero iff any binding failed; an individual
binding failure does not abort siblings.

``--dry-run`` skips the ``ais.py`` invocations and the diff phase but still
performs dataset creation, snapshotting, and worktree setup/teardown — useful
for validating the orchestration without burning Dataflow cost.

Note on cross-version semantics: ``ais.py``'s default ``--worker-image`` is a
fixed published path (e.g. ``pipe-anchorages:v4.6.4``). Without
``--binding-worker-image`` overrides, every binding's Dataflow workers run the
SAME published code regardless of the worktree ref — only the submission-side
orchestrator differs. For changes that live in worker code (most pipeline
changes — Beam PTransforms, DoFns), per-binding ``--worker-image`` is
required for the cross-version diff to be meaningful. Manually build + push
each binding's worktree to a registry both you and the Dataflow worker SA can
access (e.g. ``gcr.io/world-fishing-827/...``) and pass it here.
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
import threading
from concurrent.futures import ThreadPoolExecutor
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
    p.add_argument("--sequential-bindings", action="store_true",
                   help="Run bindings serially instead of the default (parallel). Useful when debugging a single binding's logs without interleave.")
    p.add_argument("--binding-worker-image", action="append", default=[],
                   dest="binding_worker_images",
                   help="`name=image` pair, repeatable. Overrides --worker-image (passed to ais.py) for that one binding. "
                        "Lets cross-version actually exercise per-binding worker code, which the default static --worker-image does not. "
                        "Bindings without an override use ais.py's default.")
    args, ais_extra_args = p.parse_known_args(argv)
    args.bindings = [_parse_binding(b) for b in args.bindings]
    args.modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    args.pin_source_at = _parse_iso8601(args.pin_source_at)
    args.binding_worker_images = dict(_parse_binding(b) for b in args.binding_worker_images)
    _binding_names = {n for n, _ in args.bindings}
    _unknown = set(args.binding_worker_images) - _binding_names
    if _unknown:
        raise SystemExit(
            f"--binding-worker-image references unknown binding(s): {sorted(_unknown)}; "
            f"declared bindings: {sorted(_binding_names)}"
        )
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

def _ais_args_for_binding(
    extra_args: list[str],
    *,
    snap_stem: str,
    suffix: str,
    experiment_id: str,
    binding_name: str,
    worker_image: Optional[str] = None,
) -> list[str]:
    """Strip user-supplied overrides for fields the wrapper owns, then
    re-inject the wrapper's values. ``--suffix`` controls table names;
    ``--experiment-id`` + ``--binding-name`` flow into Dataflow job names
    and BQ labels. ``--worker-image`` is dropped from extra_args only when
    a per-binding override is provided -- otherwise ais.py's default wins."""
    drop_kvs = {"--source-dataset-stem", "--suffix", "--experiment-id", "--binding-name"}
    if worker_image is not None:
        drop_kvs = drop_kvs | {"--worker-image"}
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
        "--experiment-id", experiment_id,
        "--binding-name", binding_name,
        "--allow-dirty-tree",  # worktree's git status is clean but ais.py's _git_info still triggers; harmless
    ])
    if worker_image is not None:
        out.extend(["--worker-image", worker_image])
    return out


def _stream_prefixed(stream, prefix: str, sink) -> None:
    """Reader thread: copies lines from ``stream`` to ``sink`` with ``prefix``.
    Python's stdout is GIL-protected at the per-write level, so concurrent
    reader threads on different subprocesses interleave cleanly at line
    granularity."""
    try:
        for line in iter(stream.readline, ""):
            sink.write(f"{prefix}{line}")
            sink.flush()
    finally:
        stream.close()


def _run_binding(
    *,
    name: str,
    ref: str,
    experiment_id: str,
    snap_stem: str,
    suffix: str,
    pipeline_dir: str,
    ais_extra_args: list[str],
    dry_run: bool,
    worker_image: Optional[str] = None,
) -> int:
    worktree_dir = tempfile.mkdtemp(prefix=f"dit-xv-{name}-")
    try:
        subprocess.run(
            ["git", "-C", pipeline_dir, "worktree", "add", "--force", worktree_dir, ref],
            check=True, capture_output=True, text=True,
        )
        logger.info("binding %s: worktree at %s @ %s", name, worktree_dir, ref)

        argv = _ais_args_for_binding(
            ais_extra_args,
            snap_stem=snap_stem, suffix=suffix,
            experiment_id=experiment_id, binding_name=name,
            worker_image=worker_image,
        )
        cmd = [sys.executable, str(AIS_WORKFLOW), *argv]
        logger.info("binding %s: invoking %s", name, " ".join(shlex.quote(c) for c in cmd))
        if worker_image is not None:
            logger.info("binding %s: --worker-image override -> %s", name, worker_image)

        if dry_run:
            logger.info("binding %s: --dry-run set; skipping ais.py invocation", name)
            return 0

        # Stream subprocess output with a [binding-name] prefix so parallel
        # runs interleave readably. stderr is merged into stdout to keep
        # ordering coherent.
        prefix = f"[{name}] "
        env = {**os.environ}
        proc = subprocess.Popen(
            cmd, cwd=worktree_dir, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        reader = threading.Thread(
            target=_stream_prefixed,
            args=(proc.stdout, prefix, sys.stderr),
            daemon=True,
        )
        reader.start()
        rc = proc.wait()
        reader.join()
        return rc
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


_SKIPPED = -1  # sentinel rc for diff pairs we couldn't run


def _run_diffs(
    *,
    modes: list[str],
    suffix_by_binding: dict[str, str],
    dest_dataset: str,
    failed_bindings: set[str],
) -> dict[tuple[str, str, str], int]:
    results: dict[tuple[str, str, str], int] = {}
    bindings = list(suffix_by_binding.keys())
    for mode in modes:
        for a, b in itertools.combinations(bindings, 2):
            if a in failed_bindings or b in failed_bindings:
                results[(mode, a, b)] = _SKIPPED
                failed_side = a if a in failed_bindings else b
                logger.info("diff mode=%s %s vs %s -> SKIPPED (binding %s failed)",
                            mode, a, b, failed_side)
                continue
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
        if rc == _SKIPPED:
            verdict = "SKIPPED"
        elif rc == 0:
            verdict = "IDENTICAL"
        else:
            verdict = "DIFFERENT"
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

    def _invoke(name: str, ref: str) -> tuple[str, int]:
        rc = _run_binding(
            name=name, ref=ref,
            experiment_id=args.experiment_id,
            snap_stem=snap_stem,
            suffix=suffix_by_binding[name],
            pipeline_dir=args.pipeline_dir,
            ais_extra_args=ais_extra_args,
            dry_run=args.dry_run,
            worker_image=args.binding_worker_images.get(name),
        )
        return name, rc

    rc_by_binding: dict[str, int] = {}
    if args.sequential_bindings or len(args.bindings) == 1:
        logger.info("running %d binding(s) sequentially", len(args.bindings))
        for name, ref in args.bindings:
            n, rc = _invoke(name, ref)
            rc_by_binding[n] = rc
    else:
        logger.info("running %d bindings in parallel", len(args.bindings))
        with ThreadPoolExecutor(max_workers=len(args.bindings)) as ex:
            for n, rc in ex.map(lambda nr: _invoke(*nr), args.bindings):
                rc_by_binding[n] = rc

    failed_bindings = {n for n, rc in rc_by_binding.items() if rc != 0}
    for name, rc in rc_by_binding.items():
        if rc != 0:
            logger.error("binding %s failed with rc=%d", name, rc)

    if args.dry_run:
        logger.info("--dry-run set; skipping pairwise diffs.")
        return 1 if failed_bindings else 0

    results = _run_diffs(
        modes=args.modes,
        suffix_by_binding=suffix_by_binding,
        dest_dataset=args.dest_dataset,
        failed_bindings=failed_bindings,
    )
    print(_summarize(results), file=sys.stderr)
    return 1 if failed_bindings else 0


if __name__ == "__main__":
    sys.exit(main())
