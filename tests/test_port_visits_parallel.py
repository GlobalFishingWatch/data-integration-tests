"""Tests for the ``6_parallel`` mode of ``workflows/port_visits/ais.py``.

No cloud call: docker, the run cache, the comparison and the run context are
patched. Covers parsing and refusals, the slice plans of every mode (the
sequential ones unchanged), concurrent submission, failure handling, the
cache key, the judgement against the declaration, and the dry run.
"""
from __future__ import annotations

import dataclasses
import threading
from datetime import date
from unittest.mock import patch

import pytest

from dit import orchestration as dit_orchestration
from dit import workflow as dit_workflow
from dit.workflow import RunContext
from workflows.port_visits import ais as mod
from workflows.port_visits import cross_version_ais as xv
from workflows.port_visits import orchestration as pv_orchestration

_SUFFIX = "exp01_abc1234_aabbcc"
_PARALLEL = ["--steps", mod.STEP_THIN, "--modes", f"{mod.MODE_BF},{mod.MODE_PARALLEL}"]


def _args(*cli: str):
    args = mod.parse_args(list(cli))
    args.run_id = "rid01"
    args.commit_sha = "abc1234"
    args.worker_image_tag = "tag"
    args.launched_by = "tester"
    args.worker_image_digest = "gcr.io/foo/pipe-anchorages@sha256:0011"
    args.run_context = RunContext(
        pipeline_commit=args.commit_sha, unreviewed=False, pipeline_commit_parent=None,
        worker_image=args.worker_image, worker_image_digest=args.worker_image_digest,
        run_id=args.run_id, dit_commit="ditsha0",
    )
    return args


# --- parsing ----------------------------------------------------------------------

def test_the_mode_is_opt_in() -> None:
    assert mod.MODE_PARALLEL in mod.SELECTABLE_MODES
    assert mod.parse_args([]).modes == list(mod.DEFAULT_MODES)
    assert mod.parse_args([]).parallel_slices == ()


def test_default_slices_are_the_12_months_of_the_default_window() -> None:
    slices = mod.parse_args(_PARALLEL).parallel_slices
    assert len(slices) == 12
    assert slices[0] == (date(2020, 1, 1), date(2020, 1, 31))
    assert slices[-1] == (date(2020, 12, 1), date(2020, 12, 31))


def test_day_slices_over_january() -> None:
    # 31 slices at once x 4 workers = 124, above the bound: allowed explicitly.
    args = mod.parse_args([*_PARALLEL, "--slices", "day", "--start", "2020-01-01",
                           "--end", "2020-01-31", "--allow-many-workers"])
    assert len(args.parallel_slices) == 31


# --- the bound on workers across concurrent slices -------------------------------

_JANUARY_BY_DAY = [*_PARALLEL, "--slices", "day", "--start", "2020-01-01", "--end", "2020-01-31"]


def test_more_than_100_workers_across_slices_is_refused_at_parse_time() -> None:
    with pytest.raises(SystemExit) as exc:
        mod.parse_args(_JANUARY_BY_DAY)  # 31 x 4 = 124
    message = str(exc.value)
    assert "31 slice(s) in flight x --max-num-workers 4 = up to 124" in message
    assert "shares its regional Dataflow quota with production" in message
    assert "--allow-many-workers" in message


@pytest.mark.parametrize("extra", [
    ("--max-concurrent-slices", "25"),        # 25 x 4 = 100: at the bound
    ("--max-num-workers", "3"),               # 31 x 3 = 93
    ("--allow-many-workers",),                # 124, allowed
])
def test_the_worker_bound_allows(extra: tuple[str, ...]) -> None:
    assert len(mod.parse_args([*_JANUARY_BY_DAY, *extra]).parallel_slices) == 31


def test_allow_many_workers_without_the_mode_is_refused() -> None:
    with pytest.raises(SystemExit, match="--allow-many-workers needs 6_parallel"):
        mod.parse_args(["--allow-many-workers"])


def test_the_plan_states_the_worker_bound() -> None:
    lines = mod.describe_plan(_args(*_PARALLEL, "--max-concurrent-slices", "5"))
    assert ("workers: 5 slice(s) in flight x --max-num-workers 4 = up to 20 at once "
            "(refused above 100 without --allow-many-workers)") in lines


def test_step_2_is_refused_with_the_reason() -> None:
    with pytest.raises(SystemExit, match="WRITE_TRUNCATE"):
        mod.parse_args(["--modes", f"{mod.MODE_BF},{mod.MODE_PARALLEL}"])  # both steps
    with pytest.raises(SystemExit, match="WRITE_TRUNCATE"):
        mod.parse_args(["--steps", mod.STEP_VISITS, "--thinned-message-table", "p.d.t",
                        "--modes", f"{mod.MODE_BF},{mod.MODE_PARALLEL}"])


def test_needs_1_bf_to_compare_with() -> None:
    with pytest.raises(SystemExit, match="add 1_bf"):
        mod.parse_args(["--steps", mod.STEP_THIN, "--modes", mod.MODE_PARALLEL])


@pytest.mark.parametrize("flags", [["--slices", "month"], ["--max-concurrent-slices", "2"]])
def test_parallel_flags_without_the_mode_are_refused(flags) -> None:
    with pytest.raises(SystemExit, match="need 6_parallel"):
        mod.parse_args(flags)


@pytest.mark.parametrize("spec,match", [
    ("fortnight", "unknown slice spec"),
    ('[["2020-01-01", "2020-06-30"], ["2020-07-02", "2020-12-31"]]', "in no slice"),
    ('[["2020-01-01", "2020-12-31"]]', "same job as 1_bf"),
    ("400d", "same job as 1_bf"),
])
def test_bad_or_useless_slices_are_refused(spec, match) -> None:
    with pytest.raises(SystemExit, match=match):
        mod.parse_args([*_PARALLEL, "--slices", spec])


def test_max_concurrent_slices_must_be_positive() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args([*_PARALLEL, "--max-concurrent-slices", "0"])


def test_cross_version_wrapper_refuses_the_mode() -> None:
    with pytest.raises(SystemExit, match="not supported by the cross-version wrapper"):
        xv.parse_args(["--experiment-id", "exp01", "--binding", "a=v1",
                       "--steps", mod.STEP_THIN, "--modes", f"1_bf,{mod.MODE_PARALLEL}"])


# --- plans: the sequential modes are unchanged ----------------------------------------

def test_sequential_plans_match_the_previous_slicing() -> None:
    start, end = date(2020, 1, 1), date(2020, 12, 31)
    assert mod.mode_slices(mod.MODE_BF, start, end, 3) == ((start, end),)
    assert mod.mode_slices(mod.MODE_BFD, start, end, 3) == (
        (start, date(2020, 12, 28)), (date(2020, 12, 29),) * 2,
        (date(2020, 12, 30),) * 2, (end, end))
    assert mod.mode_slices(mod.MODE_BFTRUNCATE, start, end, 3) == (
        (start, end), (date(2020, 12, 29),) * 2, (date(2020, 12, 30),) * 2, (end, end))
    assert mod.mode_slices(mod.MODE_BFD, start, end, 0) == ((start, end),)


def test_execute_bfd_runs_its_slices_in_order_with_the_old_counters() -> None:
    args = _args("--tail-days", "2")
    calls = []
    with patch.object(mod, "_run_slice", side_effect=lambda a, **k: calls.append(k)):
        mod.execute_bfd(args, _SUFFIX)
    assert [(c["slice_start"], c["slice_end"], c["iteration"], c["total_iterations"])
            for c in calls] == [
        (date(2020, 1, 1), date(2020, 12, 29), 1, 3),
        (date(2020, 12, 30), date(2020, 12, 30), 2, 3),
        (date(2020, 12, 31), date(2020, 12, 31), 3, 3),
    ]


# --- execution -----------------------------------------------------------------------

def test_all_slices_are_submitted_before_any_is_awaited() -> None:
    """Every docker call waits on a barrier for all 12: if the slices ran one
    after the other, the barrier would time out and the test would fail."""
    args = _args(*_PARALLEL)
    barrier = threading.Barrier(12, timeout=10)
    argvs = []

    def docker_run(image, argv, **kwargs):
        argvs.append(argv)
        barrier.wait()
        return 0

    with patch.object(mod.dit_docker, "run", side_effect=docker_run):
        mod.execute_parallel(args, _SUFFIX)
    assert len(argvs) == 12
    assert all(argv[0] == "port-state-transitions" for argv in argvs)


def test_each_slice_has_its_own_job_name_and_slice_labels() -> None:
    args = _args(*_PARALLEL)
    argvs = []
    with patch.object(mod.dit_docker, "run", side_effect=lambda i, argv, **k: argvs.append(argv) or 0):
        mod.execute_parallel(args, _SUFFIX)
    names = {next(t for t in argv if t.startswith("--job_name=")) for argv in argvs}
    assert len(names) == 12
    feb = next(argv for argv in argvs if "--start-date=2020-02-01" in argv)
    assert "--end-date=2020-03-01" in feb  # exclusive end of 2020-02-29
    assert "--labels=dit_slice_start=2020-02-01" in feb
    assert "--labels=dit_slice_end=2020-02-29" in feb
    assert "--labels=dit_iteration=2" in feb and "--labels=dit_total_iterations=12" in feb
    assert "--labels=dit_mode=6_parallel" in feb
    # One table for the whole mode.
    assert {t for argv in argvs for t in argv if t.startswith("--bq-out-")} == {
        f"--bq-out-port-state-transitions={mod._thinned_table(args, _SUFFIX, mod.MODE_PARALLEL)}"}


def test_a_failed_slice_fails_the_mode_after_all_were_awaited() -> None:
    args = _args(*_PARALLEL)
    calls = []

    def docker_run(image, argv, **kwargs):
        calls.append(argv)
        return 1 if "--start-date=2020-03-01" in argv else 0

    with patch.object(mod.dit_docker, "run", side_effect=docker_run):
        with pytest.raises(mod.ParallelSlicesFailed) as exc:
            mod.execute_parallel(args, _SUFFIX)
    assert len(calls) == 12
    assert len(exc.value.failures) == 1 and "slice 3/12" in exc.value.failures[0]


def test_max_concurrent_slices_caps_the_overlap() -> None:
    args = _args(*_PARALLEL, "--max-concurrent-slices", "3")
    lock, running, peak = threading.Lock(), [0], [0]

    def docker_run(image, argv, **kwargs):
        with lock:
            running[0] += 1
            peak[0] = max(peak[0], running[0])
        threading.Event().wait(0.01)
        with lock:
            running[0] -= 1
        return 0

    with patch.object(mod.dit_docker, "run", side_effect=docker_run):
        mod.execute_parallel(args, _SUFFIX)
    assert peak[0] <= 3


# --- cache --------------------------------------------------------------------------

def test_cache_key_holds_the_slices_and_the_mode_never_reads_the_cache() -> None:
    args = _args(*_PARALLEL)
    params = mod.canonical_params_dict(args, mod.MODE_PARALLEL)
    assert len(params["slices"]) == 12 and params["slices"][0] == ["2020-01-01", "2020-01-31"]
    assert "slices" not in mod.canonical_params_dict(args, mod.MODE_BF)
    with (
        patch.object(dit_workflow, "read_cache") as read,
        patch.object(dit_workflow, "write_cache"),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True]),
        patch.object(dit_workflow, "expires_at_for"),
    ):
        mod._run_with_cache(args, mod.MODE_PARALLEL, _SUFFIX, lambda **k: None)
        read.assert_not_called()
        mod._run_with_cache(args, mod.MODE_BF, _SUFFIX, lambda **k: None)
        read.assert_called_once()


def test_a_failed_parallel_run_still_writes_its_row_for_cleanup() -> None:
    """The table may hold the slices that succeeded: the row, status failed,
    lets ``make dit-cancel`` find and drop it. The failure is re-raised."""
    args = _args(*_PARALLEL)

    def fail(**kwargs):
        raise mod.ParallelSlicesFailed(["slice 1/12: boom"], 12)

    with (
        patch.object(dit_workflow, "read_cache"),
        patch.object(dit_workflow, "write_cache") as write,
        patch.object(dit_workflow, "expires_at_for"),
        pytest.raises(mod.ParallelSlicesFailed),
    ):
        mod._run_with_cache(args, mod.MODE_PARALLEL, _SUFFIX, fail)

    (row,) = [call.args[0] for call in write.call_args_list]
    assert row.status == dit_workflow.STATUS_FAILED
    assert row.run_id == "rid01"
    assert row.output_tables == [mod._output_table(args, _SUFFIX, mod.MODE_PARALLEL)]


def test_a_failure_of_a_sequential_mode_writes_no_row() -> None:
    args = _args(*_PARALLEL)

    def fail(**kwargs):
        raise RuntimeError("slice failed")

    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "write_cache") as write,
        pytest.raises(RuntimeError),
    ):
        mod._run_with_cache(args, mod.MODE_BF, _SUFFIX, fail)
    write.assert_not_called()


# --- main: judgement against the declaration -------------------------------------------

def _main(*cli: str, compare_rc: int = 0, fail_slice: bool = False, declaration=None):
    def run_with_cache(execute_fn, **kwargs):
        if fail_slice and kwargs["log_label"] == mod.MODE_PARALLEL:
            raise mod.ParallelSlicesFailed(["slice 1/12: boom"], 12)
        return kwargs["output_fqn"]

    orchestration = pv_orchestration.ORCHESTRATION
    if declaration is not None:
        orchestration = dataclasses.replace(orchestration, steps=(declaration,))
    with (
        patch.object(dit_workflow, "resolve_pipeline_commit", return_value=("abc1234", False)),
        patch.object(dit_workflow, "snapshot_parent", return_value=None),
        patch.object(dit_workflow, "dit_commit", return_value="ditsha0"),
        patch.object(dit_workflow, "resolve_worker_image_to_digest", side_effect=lambda i: i),
        patch.object(mod, "resolve_worker_image_to_digest", side_effect=lambda i: i),
        patch.object(mod.dit_workflow, "run_with_cache", side_effect=run_with_cache),
        patch.object(mod.dit_compare, "compare_tables", return_value=compare_rc) as compare,
        patch.object(mod.pv_orchestration, "ORCHESTRATION", orchestration),
    ):
        rc = mod.main([*_PARALLEL, *cli])
    return rc, compare


def test_allowed_and_identical_passes() -> None:
    rc, compare = _main()
    assert rc == 0
    compare.assert_called_once()
    a, b = compare.call_args.args
    assert a.endswith("_1_bf") and b.endswith("_6_parallel")
    assert compare.call_args.kwargs["keys"] == mod.THIN_COMPARE_KEYS


def test_allowed_and_different_fails() -> None:
    assert _main(compare_rc=1)[0] == 1


def test_a_failed_slice_fails_without_comparing() -> None:
    rc, compare = _main(fail_slice=True)
    assert rc == 1
    compare.assert_not_called()


def _forbidden() -> dit_orchestration.StepOrchestration:
    return dataclasses.replace(
        pv_orchestration.ORCHESTRATION.step(mod.STEP_THIN),
        parallel_slices=dit_orchestration.PARALLEL_FORBIDDEN,
    )


def test_forbidden_and_different_is_as_declared(caplog) -> None:
    caplog.set_level("INFO")
    assert _main(compare_rc=1, declaration=_forbidden())[0] == 0
    assert "as declared" in caplog.text


def test_forbidden_and_failed_is_as_declared_but_logged_as_a_warning(caplog) -> None:
    caplog.set_level("INFO")
    assert _main(fail_slice=True, declaration=_forbidden())[0] == 0
    (record,) = [r for r in caplog.records if "quota or permissions" in r.getMessage()
                 and "6_parallel:" in r.getMessage()]
    assert record.levelname == "WARNING"
    assert "as declared" in record.getMessage()


def test_forbidden_and_identical_warns_the_declaration_may_be_too_strict(caplog) -> None:
    assert _main(declaration=_forbidden())[0] == 0
    assert "may be too strict" in caplog.text


def test_sequential_modes_are_compared_among_themselves_and_parallel_with_1_bf_only() -> None:
    rc, compare = _main("--modes", "1_bf,2_bfd,6_parallel")
    assert rc == 0
    pairs = [tuple(t.rsplit("_", 2)[-2] + "_" + t.rsplit("_", 1)[-1] for t in call.args)
             for call in compare.call_args_list]
    assert sorted(pairs) == [("1_bf", "2_bfd"), ("1_bf", "6_parallel")]


def test_a_sequential_difference_still_fails_next_to_a_passing_parallel_mode() -> None:
    with patch.object(mod, "compare_all", return_value=1):
        rc, _ = _main("--modes", "1_bf,2_bfd,6_parallel")
    assert rc == 1


# --- dry run -----------------------------------------------------------------------------

def test_dry_run_lists_the_plan_and_creates_nothing(caplog) -> None:
    caplog.set_level("INFO")
    with (
        patch.object(mod, "resolve_run_context") as ctx,
        patch.object(mod.dit_docker, "run") as docker,
        patch.object(mod.dit_compare, "compare_tables") as compare,
    ):
        assert mod.main([*_PARALLEL, "--dry-run", "--experiment-id", "exp01"]) == 0
    ctx.assert_not_called(); docker.assert_not_called(); compare.assert_not_called()
    assert "mode 6_parallel: submitted at once, 12 job(s) per step" in caplog.text
    assert "12/12 [2020-12-01, 2020-12-31] dit-anchorages-pipeline-thin-exp01-6-parallel-12-12" in caplog.text
    assert "parallel slices allowed (claimed" in caplog.text


def test_describe_plan_of_the_default_run_has_three_pairs() -> None:
    lines = mod.describe_plan(_args())
    assert sum(line.startswith("compare ") for line in lines) == 3
