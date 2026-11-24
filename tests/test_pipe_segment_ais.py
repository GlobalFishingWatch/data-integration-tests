"""Tests for ``workflows/pipe_segment/ais.py``: mode equivalence of the segment step.

What they pin:

* the unit, a (mode, segmenter) variant: how ``--modes`` and ``--segmenter``
  select variants, in canonical order;
* the plan: which days each job of each mode covers, that the jobs of every
  plan are consecutive (``check_chain``), and how many jobs a run submits;
* the argv of every job of every variant: its date range, the tables it reads
  and writes, ``--incremental`` and its tables for the incremental segmenter
  only, no satellite flag ever, the Dataflow options;
* the refusal of ``incremental`` on an image that does not know the flag,
  before any cloud call;
* the image guard (no auto-build; unreviewed code refused on a default image);
* the pin window, the refusal of output tables that already hold rows, the
  comparison pairs and the exit code, and that ``--dry-run`` creates nothing.

No docker, BigQuery, Dataflow, registry or git call: every boundary is
patched. What pipe-segment does with these argvs is not tested here.
"""

from __future__ import annotations

import contextlib
import itertools
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from dit import bq as dit_bq
from dit import workflow as dit_workflow
from dit.job_names import MAX_JOB_NAME
from dit.workflow import RunContext
from workflows.pipe_segment import ais as mod
from workflows.pipe_segment import identity_match_key as imk

GE = "world-fishing-827.tech_great_expectations"
PIN = f"{GE}.dit_exp_exp_pipe_segment_normalized_messages_20200101_20200111_20261120t101500z"
PINNED = imk._PinnedSources(normalized_messages=PIN)
SUFFIX = "exp_abc1234_f00d00"
#: An overlay image of a later commit, given by flag: it knows --incremental.
NEWER = "gcr.io/world-fishing-827/dit/pipe-segment:v5.1.0-pr230-abc1234-temp-dataset-04b40b54"
#: The overlay of the version the VMS DAG pins: older than --incremental.
V503 = "gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-f4d39fe-temp-dataset-82b7f0a8"
UNVERSIONED = "gcr.io/world-fishing-827/dit/pipe-segment:pr-230-abc1234-temp-dataset-04b40b54"
NEWER_IMAGES = ("--image-tag", NEWER, "--worker-image", NEWER)
V503_IMAGES = ("--image-tag", V503, "--worker-image", V503)

#: The pipe-segment PR #227 test: full history in one job (the reference) and
#: backfill-then-daily; incremental backfill-then-daily, rerun and every-day.
PR227_MODES = (
    "1_bf:full-history,2_bfd:full-history,"
    "2_bfd:incremental,3_bftruncate:incremental,4_daily:incremental"
)

FULL, INC = mod.SEGMENTER_FULL_HISTORY, mod.SEGMENTER_INCREMENTAL
D = date.fromisoformat

#: The four satellite flags of pipe-segment's ``segment`` command. DIT passes none.
SATELLITE_FLAGS = {
    "--in_normalized_sat_offset_messages_table", "--in_norad_to_receiver_table",
    "--in_sat_positions_table", "--out_sat_offsets_table",
}


def _args(*cli: str, window: tuple[str, str] = ("2020-01-01", "2020-01-10")):
    """A parsed namespace over a ten-day window, stamped as ``main`` stamps it."""
    args = mod.parse_args([
        "--experiment-id", "exp", "--start", window[0], "--end", window[1], *cli,
    ])
    args.run_id, args.commit_sha = "run123", "abc1234"
    return args


def _flags(argv: list[str]) -> dict[str, list[str]]:
    """Flag name -> its values, for ``--name=value`` and bare ``--name`` tokens."""
    found: dict[str, list[str]] = {}
    for token in argv[1:]:
        name, _, value = token.partition("=")
        found.setdefault(name, []).append(value)
    return found


def _one(argv: list[str], flag: str) -> str:
    (value,) = _flags(argv)[flag]
    return value


def _labels(argv: list[str]) -> dict[str, str]:
    return dict(pair.split("=") for pair in _flags(argv)["--labels"])


def _variant(label: str) -> mod.Variant:
    mode, _, segmenter = label.partition(":")
    return mod.Variant(mode, segmenter)


def _argvs(args, variant: mod.Variant) -> list[list[str]]:
    start, end = args.date_range
    return [
        mod.segment_argv(args, job, suffix=SUFFIX, pinned=PINNED)
        for job in mod.plan_jobs(variant, start, end, args.tail_days)
    ]


# --------------------------------------------------------------------------
# variants: --modes and --segmenter
# --------------------------------------------------------------------------

def test_default_is_full_history_in_one_job_against_three_incremental_shapes() -> None:
    args = mod.parse_args([])

    # The reference first: full history in one job carries no state between
    # jobs. Then the segmenter the AIS DAG passes, in the three cheap shapes.
    assert [v.label for v in args.variants] == [
        "1_bf:full-history", "1_bf:incremental", "2_bfd:incremental",
        "3_bftruncate:incremental",
    ]
    assert mod.DEFAULT_SEGMENTER == INC and args.segmenter == INC
    assert mod.DEFAULT_MODES == ("1_bf:full-history", "1_bf", "2_bfd", "3_bftruncate")
    # The every-day mode stays opt-in.
    assert mod.SELECTABLE_MODES == ("1_bf", "2_bfd", "3_bftruncate", "4_daily")


def test_the_default_modes_with_full_history_are_the_three_full_history_shapes() -> None:
    """What the version the VMS DAG pins can run: no incremental variant left."""
    args = mod.parse_args(["--segmenter", FULL])

    assert [v.label for v in args.variants] == [
        "1_bf:full-history", "2_bfd:full-history", "3_bftruncate:full-history",
    ]


def test_bare_modes_take_the_default_segmenter() -> None:
    assert [v.label for v in mod.parse_args(["--modes", "1_bf,4_daily"]).variants] == [
        "1_bf:incremental", "4_daily:incremental",
    ]


@pytest.mark.parametrize(("modes", "segmenter", "expected"), [
    # A bare mode takes every segmenter of --segmenter.
    ("1_bf", "full-history", ["1_bf:full-history"]),
    ("1_bf", "incremental", ["1_bf:incremental"]),
    ("1_bf,4_daily", "full-history,incremental",
     ["1_bf:full-history", "1_bf:incremental", "4_daily:full-history", "4_daily:incremental"]),
    # A qualified entry names its own, whatever --segmenter says.
    ("2_bfd:incremental", "full-history", ["2_bfd:incremental"]),
    ("1_bf,2_bfd:incremental", "full-history", ["1_bf:full-history", "2_bfd:incremental"]),
    # Canonical order (by mode, then segmenter), whatever the order typed.
    ("4_daily:incremental,2_bfd:incremental,1_bf", "full-history",
     ["1_bf:full-history", "2_bfd:incremental", "4_daily:incremental"]),
    ("2_bfd", "incremental,full-history", ["2_bfd:full-history", "2_bfd:incremental"]),
    # Duplicates collapse, whitespace is tolerated.
    (" 1_bf , 1_bf:full-history ,1_bf ", "full-history", ["1_bf:full-history"]),
    ("2_bfd : incremental", "full-history", ["2_bfd:incremental"]),
])
def test_modes_entries_are_variants(modes, segmenter, expected) -> None:
    assert [v.label for v in mod.parse_variants(modes, segmenter)] == expected


def test_the_pr227_test_is_five_variants_with_the_full_history_job_first() -> None:
    variants = mod.parse_variants(PR227_MODES, "full-history")

    assert [v.label for v in variants] == [
        "1_bf:full-history",          # A, the reference
        "2_bfd:full-history",         # B
        "2_bfd:incremental",          # C
        "3_bftruncate:incremental",   # E
        "4_daily:incremental",        # D
    ]


@pytest.mark.parametrize(("modes", "segmenter", "message"), [
    ("1_bf,5_typo", "full-history", "unknown mode\\(s\\) 5_typo"),
    ("1_bf:incrementl", "full-history", "unknown mode\\(s\\) 1_bf:incrementl"),
    ("2_bfd:", "full-history", "unknown mode\\(s\\) 2_bfd:"),
    (":incremental", "full-history", "unknown mode\\(s\\) :incremental"),
    ("", "full-history", "--modes must name at least one mode"),
    (" , ", "full-history", "--modes must name at least one mode"),
    ("1_bf", "nope", "unknown segmenter\\(s\\) nope"),
    ("1_bf", "", "--segmenter must name at least one segmenter"),
])
def test_a_bad_selection_fails_at_parse_time(modes, segmenter, message) -> None:
    """A typo that ran nothing, or dropped a variant, would read as a pass."""
    with pytest.raises(SystemExit, match=message):
        mod.parse_args(["--modes", modes, "--segmenter", segmenter])


def test_variant_tags_are_safe_in_table_names_and_distinct() -> None:
    tags = [v.tag for v in mod.ALL_VARIANTS]

    # 4 modes x 2 segmenters x 2 sides.
    assert len(set(tags)) == len(mod.ALL_VARIANTS) == 16
    assert all(re.fullmatch(r"[a-z0-9_]+", tag) for tag in tags)
    assert mod.Variant("2_bfd", INC).tag == "2_bfd_inc"
    assert mod.Variant("1_bf", FULL).tag == "1_bf_full"
    assert mod.Variant("1_bf", FULL, mod.SIDE_BASE).tag == "1_bf_full_base"


# --------------------------------------------------------------------------
# the plan: which days each job covers
# --------------------------------------------------------------------------

def _plan(mode: str, tail_days: int = 3) -> list[tuple[str, str]]:
    return [
        (first.isoformat(), last.isoformat())
        for first, last in mod.plan_slices(mode, D("2020-01-01"), D("2020-01-10"), tail_days)
    ]


def test_bf_is_one_job_over_the_window() -> None:
    assert _plan("1_bf") == [("2020-01-01", "2020-01-10")]


def test_bfd_backfills_up_to_the_tail_then_runs_each_tail_day() -> None:
    assert _plan("2_bfd") == [
        ("2020-01-01", "2020-01-07"),
        ("2020-01-08", "2020-01-08"), ("2020-01-09", "2020-01-09"), ("2020-01-10", "2020-01-10"),
    ]


def test_bftruncate_backfills_everything_then_reruns_the_tail_in_order() -> None:
    assert _plan("3_bftruncate") == [
        ("2020-01-01", "2020-01-10"),
        ("2020-01-08", "2020-01-08"), ("2020-01-09", "2020-01-09"), ("2020-01-10", "2020-01-10"),
    ]


def test_daily_runs_every_day_of_the_window_as_its_own_job_in_order() -> None:
    assert _plan("4_daily") == [(f"2020-01-{d:02d}",) * 2 for d in range(1, 11)]
    # Whatever --tail-days is: it does not apply to this mode.
    assert _plan("4_daily", tail_days=0) == _plan("4_daily")


def test_a_tail_of_zero_leaves_one_job() -> None:
    assert _plan("2_bfd", tail_days=0) == _plan("3_bftruncate", tail_days=0) == _plan("1_bf")


def test_a_tail_of_the_whole_window_makes_bfd_the_every_day_shape() -> None:
    assert _plan("2_bfd", tail_days=9) == _plan("4_daily")


@pytest.mark.parametrize("mode", mod.SELECTABLE_MODES)
@pytest.mark.parametrize("tail_days", range(0, 10))
def test_every_plan_chains(mode, tail_days) -> None:
    """Consecutive days, no gap, and the window fully written at the end:
    what an incremental job needs, checked for every mode and tail."""
    start, end = D("2020-01-01"), D("2020-01-10")
    slices = mod.plan_slices(mode, start, end, tail_days)

    mod.check_chain(slices, start=start, end=end)
    # Independently of check_chain: every job after the first starts at most
    # one day after the last day the job before it wrote.
    for (_, written), (first, _) in itertools.pairwise(slices):
        assert first <= written + timedelta(days=1)
    assert slices[0][0] == start and slices[-1][1] == end
    assert {day for first, last in slices for day in mod._days(first, last)} == set(
        mod._days(start, end))


@pytest.mark.parametrize(("slices", "message"), [
    # A day is skipped: the incremental segmenter would start from a missing state.
    ([("2020-01-01", "2020-01-03"), ("2020-01-05", "2020-01-10")], "gap"),
    # The first job does not start the window.
    ([("2020-01-02", "2020-01-10")], "not on the window's first day"),
    # A rerun that is not followed through to the end leaves stale days.
    ([("2020-01-01", "2020-01-10"), ("2020-01-08", "2020-01-08")],
     "not on the window's last day"),
    # Reruns out of order: after rerunning the 10th, the 9th's rerun leaves it stale.
    ([("2020-01-01", "2020-01-10"), ("2020-01-10", "2020-01-10"), ("2020-01-09", "2020-01-09")],
     "not on the window's last day"),
    ([("2020-01-01", "2020-01-11")], "leaves the window"),
    ([("2019-12-31", "2020-01-10")], "leaves the window"),
    ([("2020-01-05", "2020-01-01")], "before it starts"),
    ([], "not on the window's last day"),
])
def test_check_chain_rejects_plans_that_do_not_chain(slices, message) -> None:
    with pytest.raises(ValueError, match=message):
        mod.check_chain(
            [(D(a), D(b)) for a, b in slices], start=D("2020-01-01"), end=D("2020-01-10"),
        )


def test_jobs_carry_their_place_in_the_variants_run_order() -> None:
    jobs = mod.plan_jobs(_variant("2_bfd:incremental"), D("2020-01-01"), D("2020-01-10"), 3)

    assert [(j.iteration, j.total, j.first) for j in jobs] == [
        (1, 4, True), (2, 4, False), (3, 4, False), (4, 4, False),
    ]
    assert all(j.variant == _variant("2_bfd:incremental") for j in jobs)


@pytest.mark.parametrize(("cli", "jobs"), [
    ((), 10),                                            # the default: 1 + 1 + 4 + 4
    (("--modes", "1_bf"), 1),
    (("--modes", PR227_MODES), 44),                      # 1 + 4 + 4 + 4 + 31
    (("--modes", ",".join(mod.SELECTABLE_MODES), "--segmenter", f"{FULL},{INC}"), 80),
])
def test_job_counts_on_the_default_window(cli, jobs) -> None:
    """January 2020, --tail-days 3: what a run submits to Dataflow."""
    args = mod.parse_args(list(cli))

    assert (args.start, args.end, args.tail_days) == ("2020-01-01", "2020-01-31", 3)
    assert sum(map(len, mod._plans(args).values())) == jobs


@pytest.mark.parametrize(("cli", "message"), [
    (("--tail-days", "-1"), "--tail-days must be between 0 and"),
    # A tail longer than the window would start before --start.
    (("--start", "2020-01-01", "--end", "2020-01-03", "--tail-days", "3"),
     "--tail-days must be between 0 and"),
    (("--start", "2020-01-05", "--end", "2020-01-01"), "must be <= --end"),
    (("--start", "2020-13-01"), "--start: invalid date"),
    (("--end", "yesterday"), "--end: invalid date"),
])
def test_bad_windows_fail_at_parse_time(cli, message) -> None:
    with pytest.raises(SystemExit, match=message):
        mod.parse_args(list(cli))


def test_the_default_window_is_one_month_of_the_cohorts_data_year() -> None:
    """The staging cohort's data is 2020; its name carries the snapshot date."""
    start, end = D(mod.DEFAULT_START), D(mod.DEFAULT_END)

    assert (start, end) == (D("2020-01-01"), D("2020-01-31"))
    assert mod.DEFAULT_TAIL_DAYS == 3
    assert "pipe_ais_test_" in imk.DEFAULT_SRC_NORMALIZED_TABLE


# --------------------------------------------------------------------------
# argv: every job of every variant
# --------------------------------------------------------------------------

@pytest.mark.parametrize("variant", mod.ALL_VARIANTS, ids=lambda v: v.label)
@pytest.mark.parametrize("runner", ["dataflow", "docker"])
def test_every_job_of_every_variant(variant, runner) -> None:
    args = _args("--runner", runner, *NEWER_IMAGES)
    start, end = args.date_range
    jobs = mod.plan_jobs(variant, start, end, args.tail_days)
    tables = {
        table: f"{GE}.{table}_{SUFFIX}_{variant.tag}"
        for table in ("messages_segmented", "segments", "fragments")
    }

    for job, argv in zip(jobs, _argvs(args, variant), strict=True):
        flags = _flags(argv)
        assert argv[0] == "segment"
        # The job's own days, inclusive on both ends as pipe-segment takes them.
        assert _one(argv, "--date_range") == f"{job.start},{job.end}"
        # Reads: the pinned copy, never the moving source.
        assert _one(argv, "--in_normalized_messages_table") == PIN
        # Writes: the variant's own three tables, the same in every job. The
        # fragments table is also the full-history segmenter's history.
        assert _one(argv, "--out_segmented_messages_table") == tables["messages_segmented"]
        assert _one(argv, "--out_segments_table") == tables["segments"]
        assert _one(argv, "--fragments_table") == tables["fragments"]
        assert "--out_fragments_table" not in flags
        if variant.segmenter == INC:
            # The state of the day before: the variant's own stored rows.
            assert flags["--incremental"] == [""]
            assert _one(argv, "--in_segments_table") == tables["segments"]
            assert _one(argv, "--in_fragments_table") == tables["fragments"]
        else:
            assert not {"--incremental", "--in_segments_table", "--in_fragments_table"} & set(
                flags)
        # No satellite timing offsets in DIT, ever.
        assert not SATELLITE_FLAGS & set(flags)
        assert not any("sat" in name for name in flags)
        assert _one(argv, "--project") == "world-fishing-827"
        assert _one(argv, "--runner") == {"dataflow": "dataflow", "docker": "DirectRunner"}[runner]
        labels = _labels(argv)
        assert (labels["dit_mode"], labels["dit_segmenter"]) == (variant.mode, variant.segmenter)
        assert (labels["dit_iteration"], labels["dit_total_iterations"]) == (
            str(job.iteration), str(job.total))
        assert (labels["dit_slice_start"], labels["dit_slice_end"]) == (
            job.start.isoformat(), job.end.isoformat())


def test_no_table_is_shared_between_variants() -> None:
    """A variant reads its own fragments and segments as its history, so two
    variants on one table would read each other's days."""
    args = _args()
    tables = [
        fqn for variant in mod.ALL_VARIANTS
        for fqn in mod._tables(args, SUFFIX, variant).values()
    ]

    assert len(set(tables)) == len(tables) == 48


def test_a_full_history_dataflow_job_argv_is_pinned() -> None:
    args = _args("--modes", "2_bfd")
    argv = _argvs(args, _variant("2_bfd:full-history"))[1]

    assert argv == [
        "segment",
        "--date_range=2020-01-08,2020-01-08",
        f"--in_normalized_messages_table={PIN}",
        f"--out_segmented_messages_table={GE}.messages_segmented_{SUFFIX}_2_bfd_full",
        f"--out_segments_table={GE}.segments_{SUFFIX}_2_bfd_full",
        f"--fragments_table={GE}.fragments_{SUFFIX}_2_bfd_full",
        "--labels=environment=develop",
        "--labels=resource_creator=dit",
        "--labels=step=segment",
        "--labels=experiment_id=exp",
        "--labels=dit_workflow=pipe_segment_ais",
        "--labels=dit_mode=2_bfd",
        "--labels=dit_segmenter=full-history",
        "--labels=dit_side=head",
        "--labels=dit_iteration=2",
        "--labels=dit_total_iterations=4",
        "--labels=dit_slice_start=2020-01-08",
        "--labels=dit_slice_end=2020-01-08",
        "--labels=dit_run_id=run123",
        "--labels=dit_commit_sha=abc1234",
        "--project=world-fishing-827",
        "--runner=dataflow",
        f"--temp_location=gs://{imk.DEFAULT_DATAFLOW_TEMP_BUCKET}/dit/dataflow_temp",
        f"--staging_location=gs://{imk.DEFAULT_DATAFLOW_TEMP_BUCKET}/dit/dataflow_staging",
        "--wait_for_job",
        "--region=us-central1",
        "--max_num_workers=4",
        "--worker_machine_type=e2-standard-4",
        "--disk_size_gb=50",
        "--no_use_public_ips",
        "--network=gfw-internal-network",
        "--subnetwork=regions/us-central1/subnetworks/gfw-internal-us-central1",
        f"--service_account_email={dit_workflow.DEFAULT_DATAFLOW_SA}",
        "--job_name=dit-pipe-segment-segment-exp-2-bfd-full-2-4",
        f"--sdk_container_image={mod.DEFAULT_WORKER_IMAGE}",
    ]


def test_an_incremental_job_adds_exactly_three_flags_after_the_tables() -> None:
    args = _args("--modes", "2_bfd", "--segmenter", f"{FULL},{INC}", *NEWER_IMAGES)
    full = _argvs(args, _variant("2_bfd:full-history"))[1]
    incremental = _argvs(args, _variant("2_bfd:incremental"))[1]

    extra = [
        "--incremental",
        f"--in_segments_table={GE}.segments_{SUFFIX}_2_bfd_inc",
        f"--in_fragments_table={GE}.fragments_{SUFFIX}_2_bfd_inc",
    ]
    assert incremental[6:9] == extra
    # Apart from them, the two segmenters get the same flags.
    assert [t.partition("=")[0] for t in incremental if t not in extra] == [
        t.partition("=")[0] for t in full
    ]


def test_a_full_history_job_has_the_flags_of_the_cross_version_segment_step() -> None:
    """The argv comes from identity_match_key's builder, which mirrors
    production's task; the drift check compares that builder's output."""
    ours = _argvs(_args("--modes", "1_bf"), _variant("1_bf:full-history"))[0]
    theirs = imk._segment_args(
        pinned=PINNED, dest_dataset="tech_great_expectations", suffix="x",
        experiment_id="exp", binding_name="b", date_range=(D("2020-01-01"), D("2020-01-10")),
        runner="dataflow", worker_image="image", ssvid_filter_query=None,
        service_account="sa", max_num_workers=4, worker_sizing=imk.STEP_WORKER_SIZING["segment"],
    )

    assert set(_flags(ours)) == set(_flags(theirs))


def test_the_docker_runner_gets_no_dataflow_option() -> None:
    argv = _argvs(_args("--runner", "docker", "--modes", "1_bf"), _variant("1_bf:full-history"))[0]
    flags = _flags(argv)

    assert _one(argv, "--runner") == "DirectRunner"
    assert not {"--job_name", "--sdk_container_image", "--max_num_workers", "--region",
                "--worker_machine_type", "--no_use_public_ips", "--wait_for_job"} & set(flags)
    # Beam's BigQuery read needs a GCS location on every runner.
    assert "--temp_location" in flags


def test_cap_sizing_filter_and_worker_image_reach_the_job() -> None:
    args = _args(
        "--modes", "1_bf", "--max-num-workers", "9", "--worker-machine-type", "e2-small",
        "--disk-size-gb", "20", "--ssvid-filter-query", '"123","456"',
        "--worker-image", NEWER, "--service-account", "sa@p.iam.gserviceaccount.com",
        "--dest-dataset", "scratch",
    )
    argv = _argvs(args, _variant("1_bf:full-history"))[0]

    assert _one(argv, "--max_num_workers") == "9"
    assert _one(argv, "--worker_machine_type") == "e2-small"
    assert _one(argv, "--disk_size_gb") == "20"
    assert _one(argv, "--ssvid_filter_query") == '"123","456"'
    assert _one(argv, "--sdk_container_image") == NEWER
    assert _one(argv, "--service_account_email") == "sa@p.iam.gserviceaccount.com"
    assert _one(argv, "--out_segments_table") == (
        f"world-fishing-827.scratch.segments_{SUFFIX}_1_bf_full")


def test_setup_file_only_with_build_from_source() -> None:
    plain = _argvs(_args("--modes", "1_bf"), _variant("1_bf:full-history"))[0]
    built = _argvs(_args("--modes", "1_bf", "--build-from-source"),
                   _variant("1_bf:full-history"))[0]

    assert "--setup_file" not in _flags(plain)
    assert _one(built, "--setup_file") == "./setup.py"


def test_every_label_is_one_that_pipe_segment_and_bigquery_accept() -> None:
    """pipe-segment splits each entry on ``=``; BigQuery and Dataflow take
    lowercase letters, digits, ``_`` and ``-``, 63 characters at most."""
    args = _args("--modes", PR227_MODES, *NEWER_IMAGES)
    args.run_id, args.commit_sha = "Run=1", "ABC.def"

    for variant in args.variants:
        for argv in _argvs(args, variant):
            pairs = _flags(argv)["--labels"]
            assert all(pair.count("=") == 1 for pair in pairs)
            for key, value in (pair.split("=") for pair in pairs):
                assert re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", key)
                assert re.fullmatch(r"[a-z0-9_-]{1,63}", value)
            # What `make dit-cancel` and the SIGTERM trap find the jobs by.
            assert _labels(argv)["dit_run_id"] == "run-1"


def test_job_names_are_distinct_and_fit() -> None:
    args = _args("--modes", ",".join(mod.SELECTABLE_MODES), "--segmenter", f"{FULL},{INC}",
                 *NEWER_IMAGES)
    names = [_one(argv, "--job_name") for v in args.variants for argv in _argvs(args, v)]

    assert len(set(names)) == len(names)
    assert all(len(name) <= MAX_JOB_NAME and re.fullmatch(r"[a-z0-9-]+", name) for name in names)
    assert "dit-pipe-segment-segment-exp-4-daily-inc-10-10" in names


def test_a_long_experiment_id_is_shortened_not_the_variant_or_the_counter() -> None:
    args = mod.parse_args([
        "--experiment-id", "a" * 32, "--start", "2020-01-01", "--end", "2020-12-31",
        "--modes", "4_daily:incremental", *NEWER_IMAGES,
    ])
    job = mod._plans(args)[_variant("4_daily:incremental")][-1]

    name = mod._job_name(args.experiment_id, job)
    assert len(name) <= MAX_JOB_NAME
    assert name.endswith("-4-daily-inc-366-366")


# --------------------------------------------------------------------------
# no satellite timing offsets, ever
# --------------------------------------------------------------------------

def test_the_parser_has_no_satellite_option() -> None:
    with pytest.raises(SystemExit) as rejected:
        mod.parse_args(["--include-satellite-offsets"])
    assert rejected.value.code == 2  # argparse: unrecognized arguments

    source = Path(mod.__file__).read_text()
    assert not SATELLITE_FLAGS & set(re.findall(r"--[a-z_]+", source))
    # The statement itself lives with the cross-version workflow and is not restated.
    assert "No satellite timing offsets, ever" in (mod.__doc__ or "")
    assert "No satellite timing offsets in DIT" in (imk.__doc__ or "")


# --------------------------------------------------------------------------
# --incremental needs an image that knows it
# --------------------------------------------------------------------------

@pytest.mark.parametrize(("image", "version"), [
    (NEWER, (5, 1, 0)),
    (mod.DEFAULT_OVERLAY_IMAGE, (5, 1, 0)),
    (V503, (5, 0, 3)),
    ("registry.example.invalid/core/pipe-segment:v5.0.3", (5, 0, 3)),  # a published tag
    ("registry/pipe-segment:v5.10.2", (5, 10, 2)),
    ("registry/pipe-segment:5.1.0-rc1", (5, 1, 0)),
    ("localhost:5000/pipe-segment:v6.0.0@sha256:abc", (6, 0, 0)),
    (UNVERSIONED, None),
    ("gcr.io/world-fishing-827/dit/pipe-segment:dit-abc1234", None),
    ("registry/pipe-segment", None),                          # untagged
    ("registry/pipe-segment@sha256:abc", None),               # digest only
    ("registry/pipe-segment:v5.1", None),                     # not a full version
])
def test_the_pipeline_version_is_read_from_the_start_of_the_image_tag(image, version) -> None:
    assert mod.image_pipeline_version(image) == version


def _support(*cli: str, repo_dir: str = ".") -> None:
    mod.check_incremental_support(mod.parse_args(list(cli)), repo_dir)


@pytest.mark.parametrize("cli", [
    # The default run selects incremental variants; an older image by flag
    # does not know the flag.
    V503_IMAGES,
    (*V503_IMAGES, "--modes", "1_bf:full-history,2_bfd:incremental"),
    # Either image alone is enough to refuse on Dataflow: both run the job's code.
    ("--image-tag", V503),
    ("--worker-image", V503),
    ("--runner", "docker", "--image-tag", V503),
])
def test_incremental_is_refused_on_an_image_older_than_the_flag(cli) -> None:
    with pytest.raises(SystemExit) as refused:
        _support(*cli)

    message = str(refused.value)
    assert "the incremental segmenter (" in message
    assert "needs pipe-segment v5.1.0 or later" in message
    assert f"{V503} holds pipe-segment v5.0.3" in message
    # Why it is a refusal and not a warning: the flag would be ignored silently.
    assert "would not fail" in message and "without testing anything" in message
    # The two ways out: full history only on this image, or a newer image.
    assert "--segmenter full-history" in message
    assert f"{mod.OVERLAY_DIR}/build.sh" in message


def test_the_refusal_names_the_flag_whose_image_is_too_old() -> None:
    with pytest.raises(SystemExit, match=r"and --worker-image \S+ holds pipe-segment v5.0.3"):
        _support("--worker-image", V503)
    with pytest.raises(SystemExit, match=r"and --image-tag \S+ holds pipe-segment v5.0.3"):
        _support("--image-tag", V503)


@pytest.mark.parametrize("cli", [
    # The default: the default image knows the flag.
    (),
    ("--modes", PR227_MODES),
    ("--segmenter", INC, *NEWER_IMAGES),
    # The docker runner has no workers: only the submitter image runs.
    ("--runner", "docker", "--image-tag", NEWER, "--worker-image", V503),
    # Nothing incremental selected: any image will do. The run for the
    # version the VMS DAG pins.
    ("--segmenter", FULL, *V503_IMAGES),
    ("--modes", "1_bf:full-history,4_daily:full-history", *V503_IMAGES),
    # A compare-only run executes no image.
    (*V503_IMAGES, "--skip-pipelines", "--suffix", "s"),
])
def test_incremental_runs_when_every_executed_image_knows_the_flag(cli) -> None:
    _support(*cli)


def test_an_image_whose_tag_names_no_version_is_refused_unless_vouched_for(caplog) -> None:
    images = ("--image-tag", UNVERSIONED, "--worker-image", UNVERSIONED)
    with pytest.raises(SystemExit) as refused:
        _support(*images)

    message = str(refused.value)
    assert "does not start with a pipe-segment version" in message
    assert "--assume-incremental-support" in message

    with caplog.at_level("WARNING", logger=mod.logger.name):
        _support(*images, "--assume-incremental-support")
    assert sum("on your word" in r.getMessage() for r in caplog.records) == 2


def test_vouching_does_not_override_a_tag_that_names_an_older_version() -> None:
    with pytest.raises(SystemExit, match="holds pipe-segment v5.0.3"):
        _support(*V503_IMAGES, "--assume-incremental-support")


def _checkout(tmp_path: Path, *, with_flag: bool) -> str:
    command = tmp_path / mod._SEGMENT_COMMAND_FILE
    command.parent.mkdir(parents=True)
    command.write_text('add("--date_range")\n' + ('add("--incremental")\n' if with_flag else ""))
    return str(tmp_path)


def test_build_from_source_reads_the_flag_from_the_working_tree(tmp_path) -> None:
    cli = ("--runner", "docker", "--build-from-source")

    _support(*cli, repo_dir=_checkout(tmp_path / "new", with_flag=True))
    with pytest.raises(SystemExit, match="declares no --incremental flag"):
        _support(*cli, repo_dir=_checkout(tmp_path / "old", with_flag=False))
    with pytest.raises(SystemExit, match="declares no --incremental flag"):
        _support(*cli, repo_dir=str(tmp_path / "not-a-checkout"))


def test_build_from_source_on_dataflow_still_checks_the_worker_image(tmp_path) -> None:
    repo = _checkout(tmp_path, with_flag=True)

    with pytest.raises(SystemExit, match=r"--worker-image \S+ holds pipe-segment v5.0.3"):
        _support("--build-from-source", "--worker-image", V503, repo_dir=repo)
    _support("--build-from-source", repo_dir=repo)
    _support("--build-from-source", "--worker-image", NEWER, repo_dir=repo)


# --------------------------------------------------------------------------
# the image guard: no auto-build, unreviewed code refused on a default image
# --------------------------------------------------------------------------

def test_the_default_image_is_the_overlay_of_the_version_the_ais_dag_pins() -> None:
    assert mod.DEFAULT_IMAGE_TAG == mod.DEFAULT_WORKER_IMAGE == mod.DEFAULT_OVERLAY_IMAGE
    assert mod.DEFAULT_OVERLAY_IMAGE.startswith("gcr.io/world-fishing-827/dit/pipe-segment:")
    # One definition for both pipe-segment workflows.
    assert imk.DEFAULT_IMAGE_TAG == mod.DEFAULT_OVERLAY_IMAGE
    assert imk.DEFAULT_PIPELINE_VERSION == mod.DEFAULT_PIPELINE_VERSION == "v5.1.0"
    # It knows --incremental, the default segmenter.
    assert mod.image_pipeline_version(mod.DEFAULT_OVERLAY_IMAGE) == (5, 1, 0)
    assert mod.image_pipeline_version(mod.DEFAULT_OVERLAY_IMAGE) >= mod.INCREMENTAL_SINCE


@pytest.mark.parametrize(("cli", "expected"), [
    ((), ("--image-tag", "--worker-image")),
    (("--image-tag", NEWER), ("--worker-image",)),
    (("--worker-image", NEWER), ("--image-tag",)),
    (NEWER_IMAGES, ()),
    (("--build-from-source",), ("--worker-image",)),
    (("--runner", "docker"), ("--image-tag",)),
    (("--runner", "docker", "--build-from-source"), ()),
    (("--skip-pipelines", "--suffix", "s"), ()),
])
def test_default_images_executed(cli, expected) -> None:
    assert mod._default_images_executed(mod.parse_args(list(cli))) == expected


@pytest.mark.parametrize("cli", [(), ("--runner", "docker"), ("--image-tag", NEWER)])
def test_unreviewed_code_on_a_default_image_is_refused(cli) -> None:
    with pytest.raises(SystemExit) as refusal:
        mod.check_images(mod.parse_args(list(cli)), "abc1234", True)

    message = str(refusal.value)
    assert "abc1234" in message and mod.DEFAULT_OVERLAY_IMAGE in message
    assert f"{mod.OVERLAY_DIR}/build.sh" in message
    assert "--runner docker --build-from-source" in message


@pytest.mark.parametrize(("cli", "unreviewed"), [
    (NEWER_IMAGES, True),
    (("--runner", "docker", "--build-from-source"), True),
    (("--skip-pipelines", "--suffix", "s"), True),
    ((), False),
])
def test_other_runs_pass_the_image_guard(cli, unreviewed) -> None:
    mod.check_images(mod.parse_args(list(cli)), "abc1234", unreviewed)


# --------------------------------------------------------------------------
# main(): pin, refusals, run order, comparison, dry run
# --------------------------------------------------------------------------

_CTX = RunContext(
    pipeline_commit="abc1234", unreviewed=False, pipeline_commit_parent=None,
    worker_image="resolved", worker_image_digest="resolved", run_id="run123",
    dit_commit="ditsha0",
)


class _Boundaries:
    """Every boundary of ``main()``, patched. ``calls`` records their order."""

    def __init__(self, *, docker_rc=0, compare_rc=0, nonempty=(), unreviewed=False):
        self.calls: list[str] = []
        self.docker_rc, self.compare_rc = docker_rc, compare_rc
        self.nonempty, self.unreviewed = list(nonempty), unreviewed
        self.docker: list[tuple[str, list[str], dict]] = []
        self.compared: list[tuple[str, str, list[str]]] = []
        self.pins: list[dict] = []
        self.context: dict = {}

    def _resolve(self, **kwargs):
        self.calls.append("resolve_run_context")
        self.context = kwargs
        kwargs["check_images"]("abc1234", self.unreviewed)
        return _CTX.__class__(**{**vars(_CTX), "worker_image": kwargs["worker_image"]})

    def _nonempty(self, fqns, **_):
        self.calls.append("nonempty_tables")
        self.checked = list(fqns)
        return [fqn for fqn in fqns if fqn in self.nonempty]

    def _pin(self, source, **kwargs):
        self.calls.append("pin")
        self.pins.append({"source": source, **kwargs})
        return PIN

    def _docker(self, image, argv, **kwargs):
        self.calls.append("docker")
        self.docker.append((image, argv, kwargs))
        rc = self.docker_rc
        return rc(argv) if callable(rc) else rc

    def _compare(self, a, b, *, keys, view_suffix):
        self.calls.append("compare")
        self.compared.append((a, b, list(keys)))
        rc = self.compare_rc
        return rc(a, b) if callable(rc) else rc

    @contextlib.contextmanager
    def patched(self):
        with (
            patch.object(mod, "resolve_run_context", side_effect=self._resolve),
            patch.object(mod, "resolve_pipeline_commit",
                         side_effect=lambda *a, **k: (
                             self.calls.append("resolve_pipeline_commit"),
                             ("abc1234", self.unreviewed))[1]),
            patch.object(mod, "nonempty_tables", side_effect=self._nonempty),
            patch.object(imk.dit_bq, "pin_source_into_experiment", side_effect=self._pin),
            patch.object(imk.dit_bq, "require_pinned_source", side_effect=lambda fqn, **_: fqn),
            patch.object(mod.dit_docker, "run", side_effect=self._docker),
            patch.object(mod.dit_compare, "compare_tables", side_effect=self._compare),
            patch.object(mod.uuid, "uuid4", return_value=type("U", (), {"hex": "f00d00ffff"})()),
            patch.object(imk, "datetime", wraps=datetime) as clock,
        ):
            clock.now.return_value = datetime(2026, 11, 20, 10, 15, tzinfo=timezone.utc)
            yield self


def _main(*cli: str, **boundaries):
    b = _Boundaries(**boundaries)
    with b.patched():
        try:
            rc = mod.main(["--experiment-id", "exp", "--start", "2020-01-01",
                           "--end", "2020-01-10", *cli])
        except SystemExit as exc:
            rc = exc
    return rc, b


def test_main_pins_the_window_once_and_every_job_reads_the_copy() -> None:
    rc, b = _main("--modes", "1_bf,2_bfd")

    assert rc == 0
    (pin,) = b.pins
    assert pin["source"] == imk.DEFAULT_SRC_NORMALIZED_TABLE
    assert pin["role"] == imk.PIN_ROLE and pin["experiment_id"] == "exp"
    # [start, end] inclusive is the half-open [start, end + 1 day) on `timestamp`:
    # every job reads only its own days of the source, and the jobs cover the window.
    assert pin["window"] == dit_bq.SourceWindow(
        column="timestamp", start=D("2020-01-01"), end=D("2020-01-11"))
    assert pin["dry_run"] is False
    assert len(b.docker) == 5
    assert {_one(argv, "--in_normalized_messages_table") for _, argv, _ in b.docker} == {PIN}


def test_main_runs_each_variants_jobs_in_order_with_the_overlays_variables() -> None:
    rc, b = _main("--modes", "2_bfd:incremental,3_bftruncate:incremental", *NEWER_IMAGES)

    assert rc == 0
    ranges = [(_labels(argv)["dit_mode"], _one(argv, "--date_range")) for _, argv, _ in b.docker]
    assert ranges == [
        ("2_bfd", "2020-01-01,2020-01-07"),
        ("2_bfd", "2020-01-08,2020-01-08"),
        ("2_bfd", "2020-01-09,2020-01-09"),
        ("2_bfd", "2020-01-10,2020-01-10"),
        ("3_bftruncate", "2020-01-01,2020-01-10"),
        ("3_bftruncate", "2020-01-08,2020-01-08"),
        ("3_bftruncate", "2020-01-09,2020-01-09"),
        ("3_bftruncate", "2020-01-10,2020-01-10"),
    ]
    for image, argv, kwargs in b.docker:
        assert image == NEWER and _one(argv, "--sdk_container_image") == NEWER
        assert kwargs["entrypoint"] == "pipe-segment"
        assert kwargs["volumes"] == ["gcp:/root/.config"]
        assert kwargs["build_from_source"] is False
        # The overlay stages its reads in this dataset; Beam's write needs the project.
        assert kwargs["container_env"] == {
            "GOOGLE_CLOUD_PROJECT": "world-fishing-827",
            "DIT_BQ_TEMP_DATASET": GE,
        }
    # The run's tables carry the generated suffix and the run id reaches the labels.
    assert _one(b.docker[0][1], "--out_segments_table") == (
        f"{GE}.segments_exp_abc1234_f00d00_2_bfd_inc")
    assert _labels(b.docker[0][1])["dit_run_id"] == "run123"


def test_main_checks_then_pins_then_runs_then_compares() -> None:
    rc, b = _main("--modes", "1_bf,2_bfd")

    assert rc == 0
    assert b.calls == [
        "resolve_run_context", "nonempty_tables", "pin", *["docker"] * 5, *["compare"] * 3,
    ]
    # No auto-build: the workflow hands the harness its own image check.
    assert b.context["check_images"] is not None
    assert b.context["resolve_digest"] is False
    assert b.context["pipeline_name"] == "pipe-segment"
    assert b.context["default_worker_image"] == mod.DEFAULT_WORKER_IMAGE


def test_the_default_run_compares_three_incremental_shapes_with_full_history() -> None:
    """No flag beyond the window: ten jobs on the default overlay image, one
    of them full history, and each variant compared with it on the three tables."""
    rc, b = _main()

    assert rc == 0
    jobs = [
        (_labels(argv)["dit_mode"], _labels(argv)["dit_segmenter"], "--incremental" in argv)
        for _, argv, _ in b.docker
    ]
    assert jobs == [
        ("1_bf", FULL, False),
        ("1_bf", INC, True),
        *[("2_bfd", INC, True)] * 4,
        *[("3_bftruncate", INC, True)] * 4,
    ]
    assert {image for image, _, _ in b.docker} == {mod.DEFAULT_OVERLAY_IMAGE}
    assert {_one(argv, "--sdk_container_image") for _, argv, _ in b.docker} == {
        mod.DEFAULT_OVERLAY_IMAGE}
    # Three pairs (each variant with the reference) on three tables.
    assert len(b.compared) == 9
    assert all(a.endswith("_1_bf_full") for a, _, _ in b.compared)
    assert [b_.rsplit("f00d00_", 1)[1] for _, b_, _ in b.compared[:3]] == [
        "1_bf_inc", "2_bfd_inc", "3_bftruncate_inc"]


def test_main_refuses_incremental_on_an_older_image_before_any_cloud_call() -> None:
    rc, b = _main(*V503_IMAGES)

    assert isinstance(rc, SystemExit)
    assert "needs pipe-segment v5.1.0 or later" in str(rc)
    assert b.calls == []


def test_main_refuses_unreviewed_code_on_the_default_image_before_pinning() -> None:
    rc, b = _main(unreviewed=True)

    assert isinstance(rc, SystemExit) and f"{mod.OVERLAY_DIR}/build.sh" in str(rc)
    assert b.calls == ["resolve_run_context"]


def test_main_refuses_tables_that_already_hold_rows_before_pinning() -> None:
    """Rows left by another run would be read as this run's history: the
    doubled counts of the pipe-segment PR's own test."""
    held = f"{GE}.fragments_old_2_bfd_inc"
    rc, b = _main("--modes", "1_bf,2_bfd", "--suffix", "old", nonempty=[held])

    assert isinstance(rc, SystemExit)
    assert held in str(rc) and "must start from empty tables" in str(rc)
    assert b.calls == ["resolve_run_context", "nonempty_tables"]
    # Every table of every selected variant was looked at.
    assert sorted(b.checked) == sorted(
        f"{GE}.{table}_old_{tag}"
        for table in imk.SEGMENT_OUTPUT_TABLES for tag in ("1_bf_inc", "2_bfd_inc")
    )


def test_a_failed_job_ends_the_variant_and_the_run() -> None:
    rc, b = _main(
        "--modes", "2_bfd,3_bftruncate",
        docker_rc=lambda argv: 1 if _one(argv, "--date_range").startswith("2020-01-09") else 0,
    )

    assert isinstance(rc, SystemExit)
    assert "2_bfd:incremental job 3/4" in str(rc) and "later jobs were not run" in str(rc)
    # Jobs 1 to 3 of the first variant; nothing after, and no comparison.
    assert b.calls.count("docker") == 3 and "compare" not in b.calls


def test_main_compares_every_pair_on_the_three_tables_discriminating_first() -> None:
    rc, b = _main("--modes", "1_bf:full-history,2_bfd,4_daily", "--compare", "all-pairs",
                  "--suffix", "s", "--skip-pipelines")

    assert rc == 0 and "docker" not in b.calls and "pin" not in b.calls
    pairs = [("1_bf_full", "2_bfd_inc"), ("1_bf_full", "4_daily_inc"),
             ("2_bfd_inc", "4_daily_inc")]
    assert b.compared == [
        (f"{GE}.{table}_s_{a}", f"{GE}.{table}_s_{b_}", keys)
        for table, keys in (
            ("segments", ["seg_id", "frag_id"]),
            ("messages_segmented", ["msgid"]),
            ("fragments", ["frag_id"]),
        )
        for a, b_ in pairs
    ]
    assert [table for table, _ in mod.COMPARED_TABLES] == [
        "segments", "messages_segmented", "fragments"]
    assert mod.COMPARED_TABLES[0][1].startswith("DISCRIMINATING")
    assert mod.COMPARED_TABLES[2][1].startswith("weak")


def test_by_default_each_variant_is_compared_with_the_reference_only() -> None:
    rc, b = _main("--modes", "1_bf:full-history,2_bfd,4_daily",
                  "--suffix", "s", "--skip-pipelines")

    assert rc == 0
    assert [(a, b_) for a, b_, _ in b.compared] == [
        (f"{GE}.{table}_s_1_bf_full", f"{GE}.{table}_s_{other}")
        for table in ("segments", "messages_segmented", "fragments")
        for other in ("2_bfd_inc", "4_daily_inc")
    ]


def test_the_pr227_run_compares_four_pairs_or_ten_with_all_pairs() -> None:
    args = mod.parse_args(["--modes", PR227_MODES, *NEWER_IMAGES])
    assert [(a.label, b.label) for a, b in mod.comparison_pairs(args.variants, args.compare)] == [
        ("1_bf:full-history", other) for other in (
            "2_bfd:full-history", "2_bfd:incremental",
            "3_bftruncate:incremental", "4_daily:incremental")
    ]
    pairs = mod.comparison_pairs(args.variants, mod.COMPARE_ALL_PAIRS)

    assert len(pairs) == 10
    assert [(a.label, b.label) for a, b in pairs[:4]] == [
        ("1_bf:full-history", other) for other in (
            "2_bfd:full-history", "2_bfd:incremental",
            "3_bftruncate:incremental", "4_daily:incremental")
    ]


def test_any_difference_fails_the_run_and_the_summary_names_it(capsys) -> None:
    rc, b = _main(
        "--modes", "1_bf:full-history,2_bfd", "--suffix", "s", "--skip-pipelines",
        compare_rc=lambda a, _b: 1 if ".segments_" in a else 0,
    )

    assert rc == 1
    summary = capsys.readouterr().err
    assert "segments            1_bf:full-history vs 2_bfd:incremental  -> DIFFERENT" in summary
    assert "fragments           1_bf:full-history vs 2_bfd:incremental  -> IDENTICAL" in summary


def test_one_variant_is_run_and_not_compared(caplog) -> None:
    with caplog.at_level("INFO", logger=mod.logger.name):
        rc, b = _main("--modes", "1_bf")

    assert rc == 0 and b.calls.count("docker") == 1 and "compare" not in b.calls
    assert any("no pair to compare" in r.getMessage() for r in caplog.records)


def test_skip_comparisons_runs_the_jobs_only() -> None:
    rc, b = _main("--modes", "1_bf,2_bfd", "--skip-comparisons")

    assert rc == 0 and b.calls.count("docker") == 5 and "compare" not in b.calls


def test_skip_pipelines_needs_the_suffix_of_the_tables_to_compare() -> None:
    with pytest.raises(SystemExit, match="pass its --suffix"):
        mod.parse_args(["--skip-pipelines"])


def test_parallel_runs_every_variant_and_keeps_each_variants_order() -> None:
    rc, b = _main("--modes", "2_bfd,4_daily", "--parallel")

    assert rc == 0
    by_mode: dict[str, list[int]] = {}
    for _, argv, _ in b.docker:
        labels = _labels(argv)
        by_mode.setdefault(labels["dit_mode"], []).append(int(labels["dit_iteration"]))
    assert by_mode == {"2_bfd": [1, 2, 3, 4], "4_daily": list(range(1, 11))}


def test_reuse_pin_reads_an_earlier_copy_and_creates_none() -> None:
    rc, b = _main("--modes", "1_bf", "--reuse-pin", "20261120t101500z")

    assert rc == 0 and "pin" not in b.calls
    assert _one(b.docker[0][1], "--in_normalized_messages_table") == PIN


def test_dry_run_reports_the_plan_and_creates_nothing(caplog) -> None:
    with caplog.at_level("INFO"):
        rc, b = _main("--modes", PR227_MODES, *NEWER_IMAGES, "--dry-run")

    assert rc == 0
    # The commit is read without snapshotting, the tables are only looked at,
    # and the copy is only dry-run. No run context, no job, no comparison.
    assert b.calls == ["resolve_pipeline_commit", "nonempty_tables", "pin"]
    assert b.pins[0]["dry_run"] is True
    log = "\n".join(r.getMessage() for r in caplog.records)
    assert "dry run: nothing is created" in log
    assert "plan: 5 variant(s), 23 segment job(s), 12 comparison(s)" in log
    assert ("4_daily:incremental job 10/10: 2020-01-10 (1d); reads the pinned messages of "
            "that range and the segments and fragments of 2020-01-09 in its own tables") in log
    assert "1_bf:full-history job 1/1: 2020-01-01..2020-01-10 (10d)" in log
    assert "no fragment history (its tables are empty)" in log
    assert "reference: 1_bf:full-history (every variant is compared with it)" in log
    assert "compare segments on seg_id,frag_id, 4 pair(s): DISCRIMINATING" in log


# --------------------------------------------------------------------------
# the base side: --modes entries ending in @base run on --base-image
# --------------------------------------------------------------------------

#: The three-sided test of a pipe-segment change: the base version, the head
#: in the base's mode, and the head in its new mode.
THREE_SIDED = "1_bf:full-history@base,1_bf:full-history,1_bf:incremental"


@pytest.mark.parametrize(("modes", "segmenter", "expected"), [
    (THREE_SIDED, "incremental",
     ["1_bf:full-history@base", "1_bf:full-history", "1_bf:incremental"]),
    # Within a mode the base sorts first, whatever the order typed.
    ("2_bfd,1_bf:full-history,1_bf:full-history@base", "incremental",
     ["1_bf:full-history@base", "1_bf:full-history", "2_bfd:incremental"]),
    # A bare mode at base takes every segmenter of --segmenter, as at head.
    ("1_bf@base", "full-history", ["1_bf:full-history@base"]),
    ("1_bf @ base", "full-history,incremental",
     ["1_bf:full-history@base", "1_bf:incremental@base"]),
])
def test_an_entry_ending_in_base_is_a_variant_of_its_own(modes, segmenter, expected) -> None:
    assert [v.label for v in mod.parse_variants(modes, segmenter)] == expected


@pytest.mark.parametrize("modes", ["1_bf@", "1_bf@bse", "1_bf:full-history@head@base", "@base"])
def test_an_unknown_side_fails_at_parse_time(modes) -> None:
    with pytest.raises(SystemExit, match="unknown mode"):
        mod.parse_variants(modes, "full-history")


def test_a_base_variant_runs_the_base_image_and_the_head_its_own() -> None:
    rc, b = _main("--modes", THREE_SIDED, "--base-image", V503)

    assert rc == 0
    ran = [
        (image, _labels(argv)["dit_side"], _labels(argv)["dit_segmenter"],
         _one(argv, "--sdk_container_image"), "--incremental" in argv)
        for image, argv, _ in b.docker
    ]
    assert ran == [
        (V503, "base", "full-history", V503, False),
        (mod.DEFAULT_IMAGE_TAG, "head", "full-history", mod.DEFAULT_WORKER_IMAGE, False),
        (mod.DEFAULT_IMAGE_TAG, "head", "incremental", mod.DEFAULT_WORKER_IMAGE, True),
    ]
    # One pin; every side reads the same copy.
    assert len(b.pins) == 1
    assert {_one(argv, "--in_normalized_messages_table") for _, argv, _ in b.docker} == {PIN}


def test_base_and_head_full_history_differ_only_in_what_dit_owns() -> None:
    """The base gets the flags the head gets in the same mode: no flag is
    added or removed for the older version."""
    args = _args("--modes", THREE_SIDED, "--base-image", V503)
    base, head, _ = args.variants
    (base_argv,), (head_argv,) = _argvs(args, base), _argvs(args, head)

    differing = {
        name for name in _flags(base_argv) | _flags(head_argv)
        if _flags(base_argv).get(name) != _flags(head_argv).get(name)
    }
    assert differing == {
        "--out_segmented_messages_table", "--out_segments_table", "--fragments_table",
        "--labels", "--job_name", "--sdk_container_image",
    }


def test_the_base_has_its_own_tables_and_is_the_reference() -> None:
    rc, b = _main("--modes", THREE_SIDED, "--base-image", V503, "--compare", "all-pairs",
                  "--suffix", "s", "--skip-pipelines")

    assert rc == 0
    assert [(a, b_) for a, b_, _ in b.compared[:3]] == [
        (f"{GE}.segments_s_1_bf_full_base", f"{GE}.segments_s_1_bf_full"),
        (f"{GE}.segments_s_1_bf_full_base", f"{GE}.segments_s_1_bf_inc"),
        (f"{GE}.segments_s_1_bf_full", f"{GE}.segments_s_1_bf_inc"),
    ]
    assert len(b.compared) == 9


@pytest.mark.parametrize(("cli", "message"), [
    # No default base: the base is whichever version the test is about.
    (("--modes", THREE_SIDED), "run on --base-image, and none is given"),
    # A base image nothing runs on would read as a compared base.
    (("--base-image", V503), "no --modes entry ends in '@base'"),
    # The working tree is the head.
    (("--modes", THREE_SIDED, "--base-image", V503, "--build-from-source",
      "--runner", "docker"), "--build-from-source runs the working tree"),
])
def test_a_base_selection_that_cannot_run_fails_at_parse_time(cli, message) -> None:
    with pytest.raises(SystemExit, match=message):
        mod.parse_args(list(cli))


def test_comparing_tables_of_a_base_run_needs_no_base_image() -> None:
    args = mod.parse_args(["--modes", THREE_SIDED, "--suffix", "s", "--skip-pipelines"])
    assert mod._images_executed(args, mod.SIDE_BASE) == ()


def test_incremental_is_checked_per_side() -> None:
    # A base older than the flag is fine while every @base entry is full history.
    _support("--modes", THREE_SIDED, "--base-image", V503)
    _support("--modes", "1_bf@base,1_bf", "--base-image", NEWER)

    with pytest.raises(SystemExit) as refused:
        _support("--modes", "1_bf:incremental@base,1_bf", "--base-image", V503)
    message = str(refused.value)
    assert "the incremental segmenter (1_bf:incremental@base)" in message
    assert f"--base-image {V503} holds pipe-segment v5.0.3" in message
    assert "name full history in every '@base' entry" in message
    assert "as --base-image" in message


def test_the_head_image_guards_ignore_the_base_image() -> None:
    args = mod.parse_args(["--modes", THREE_SIDED, "--base-image", V503])

    assert mod._default_images_executed(args) == ("--image-tag", "--worker-image")
    # Only base variants selected: no head image runs, so none is at its default.
    only_base = mod.parse_args(["--modes", "1_bf:full-history@base", "--base-image", V503])
    assert mod._default_images_executed(only_base) == ()
    mod.check_images(only_base, "abc1234", unreviewed=True)


def test_the_plan_names_each_variants_image(caplog) -> None:
    with caplog.at_level("INFO"):
        rc, _ = _main("--modes", THREE_SIDED, "--base-image", V503, "--dry-run")

    assert rc == 0
    log = "\n".join(r.getMessage() for r in caplog.records)
    assert f"--base-image: {V503}" in log
    assert f"variant 1_bf:full-history@base: 1 job(s); image {V503}; tables" in log
    assert f"variant 1_bf:full-history: 1 job(s); image {mod.DEFAULT_IMAGE_TAG}; tables" in log
    assert "reference: 1_bf:full-history@base" in log


def test_base_job_names_are_distinct_and_fit() -> None:
    args = _args("--modes", "3_bftruncate:full-history@base,3_bftruncate:full-history",
                 "--base-image", V503)
    names = [_one(argv, "--job_name") for v in args.variants for argv in _argvs(args, v)]

    assert len(set(names)) == len(names) == 8
    assert all(len(name) <= MAX_JOB_NAME for name in names)
    assert names[0] == "dit-pipe-segment-segment-exp-3-bftruncate-full-base-1-4"


def test_compare_must_name_a_known_choice() -> None:
    with pytest.raises(SystemExit):
        mod.parse_args(["--compare", "pairs"])


def test_dry_run_still_refuses_what_a_run_would_refuse() -> None:
    rc, b = _main("--dry-run", unreviewed=True)
    assert isinstance(rc, SystemExit) and "unreviewed" in str(rc)
    assert b.calls == ["resolve_pipeline_commit"]

    rc, b = _main("--dry-run", *V503_IMAGES)
    assert isinstance(rc, SystemExit) and "needs pipe-segment v5.1.0" in str(rc)
    assert b.calls == []


def test_nonempty_tables_reads_metadata_only() -> None:
    from google.cloud.exceptions import NotFound

    class _Client:
        def __init__(self, project):
            self.project = project

        def get_table(self, fqn):
            if fqn.endswith("missing"):
                raise NotFound("no such table")
            return type("T", (), {"num_rows": 7 if fqn.endswith("held") else 0})()

    with patch.object(mod.bigquery, "Client", _Client):
        assert mod.nonempty_tables(["p.d.missing", "p.d.empty", "p.d.held"]) == ["p.d.held"]


def test_the_staging_run_keeps_productions_sizing_and_says_so(caplog) -> None:
    with caplog.at_level("WARNING", logger=dit_workflow.logger.name):
        _main("--modes", "1_bf")
    assert any(r.getMessage().startswith("cost hint") for r in caplog.records)

    assert mod.STEP_WORKER_SIZING == {"segment": imk.STEP_WORKER_SIZING["segment"]}
    assert mod.DEFAULT_MAX_NUM_WORKERS == 4
