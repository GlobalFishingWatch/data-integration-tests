"""Tests for ``--steps`` in ``workflows/port_visits/ais.py``.

Covers step selection end to end without any cloud call: parsing and the
input-table rules, which docker calls a slice makes, the step-aware cache key
(unchanged for the default selection), what the cache wrapper records, and
which table and key the comparison uses.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dit import workflow as dit_workflow
from dit.workflow import RunContext
from workflows.port_visits import ais as mod

_SUFFIX = "exp01_abc1234_aabbcc"


def _args(*cli: str):
    args = mod.parse_args(list(cli))
    # Normally stamped by main() from the resolved run context.
    args.run_id = "rid01"
    args.commit_sha = "abc1234"
    args.worker_image_tag = "tag"
    args.launched_by = "tester"
    args.worker_image_digest = "gcr.io/foo/pipe-anchorages@sha256:0011"
    args.run_context = RunContext(
        pipeline_commit=args.commit_sha,
        unreviewed=False,
        pipeline_commit_parent=None,
        worker_image=args.worker_image,
        worker_image_digest=args.worker_image_digest,
        run_id=args.run_id,
        dit_commit="ditsha0",
    )
    return args


def _subcommands(args) -> list[str]:
    """The pipe-anchorages subcommand of every docker call one slice makes."""
    calls = []
    with patch.object(mod.dit_docker, "run",
                      side_effect=lambda *a, **k: calls.append(a) or 0):
        mod._run_slice(args, mode=mod.MODE_BF, slice_start=date(2020, 1, 1),
                       slice_end=date(2020, 1, 31), suffix=_SUFFIX,
                       iteration=1, total_iterations=1)
    return [argv[0] for _, argv in calls]


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------

def test_steps_default_to_both_in_pipeline_order():
    assert mod.parse_args([]).steps == [mod.STEP_THIN, mod.STEP_VISITS]
    assert mod.parse_args(["--steps", "port_visits,thin_port_messages"]).steps == [
        mod.STEP_THIN, mod.STEP_VISITS,
    ]


def test_an_unknown_step_is_rejected_naming_the_flag():
    with pytest.raises(SystemExit, match="unknown step.*valid: thin_port_messages,port_visits"):
        mod.parse_args(["--steps", "raw_port_events"])


def test_port_visits_alone_needs_a_thinned_input():
    with pytest.raises(SystemExit, match="pass --thinned-message-table"):
        mod.parse_args(["--steps", "port_visits"])


def test_a_thinned_input_needs_port_visits_alone():
    """Before --steps, the table alone skipped step 1. Now the skip is explicit,
    and a table next to a running step 1 would be silently ignored, so it fails."""
    with pytest.raises(SystemExit, match="needs --steps port_visits"):
        mod.parse_args(["--thinned-message-table", "p.d.t"])


# --------------------------------------------------------------------------
# which steps a slice runs
# --------------------------------------------------------------------------

@pytest.mark.parametrize(("cli", "thin_cmd", "visits_cmd"), [
    ((), "thin_port_messages", "port_visits"),
    (("--pipeline-cli", "v5"), "port-state-transitions", "port-visits"),
])
def test_each_selection_runs_exactly_its_steps(cli, thin_cmd, visits_cmd):
    assert _subcommands(_args(*cli)) == [thin_cmd, visits_cmd]
    assert _subcommands(_args(*cli, "--steps", "thin_port_messages")) == [thin_cmd]
    assert _subcommands(_args(
        *cli, "--steps", "port_visits", "--thinned-message-table", "p.d.t",
    )) == [visits_cmd]


# --------------------------------------------------------------------------
# cache key
# --------------------------------------------------------------------------

_ALL_FIELDS = {
    "mode", "start", "end", "submitter_image_digest",
    "source_messages_fqn", "named_anchorages",
    "source_segment_info_fqn", "source_segs_activity_fqn", "thinned_message_table",
}


def test_default_steps_leave_the_cache_key_unchanged():
    """Both steps -> the pre---steps key shape, so existing rows still hit."""
    assert set(mod.canonical_params_dict(_args(), mod.MODE_BF)) == _ALL_FIELDS


def test_thin_port_messages_alone_keys_only_on_what_step_1_reads():
    p = mod.canonical_params_dict(_args("--steps", "thin_port_messages"), mod.MODE_BF)
    assert p["steps"] == [mod.STEP_THIN]
    assert {"source_messages_fqn", "named_anchorages"} <= set(p)
    assert not {"source_segment_info_fqn", "source_segs_activity_fqn",
                "thinned_message_table"} & set(p)


def test_port_visits_alone_keys_only_on_what_step_2_reads():
    p = mod.canonical_params_dict(
        _args("--steps", "port_visits", "--thinned-message-table", "p.d.t"), mod.MODE_BF,
    )
    assert p["steps"] == [mod.STEP_VISITS]
    assert p["thinned_message_table"] == "p.d.t"
    assert not {"source_messages_fqn", "named_anchorages"} & set(p)


# --------------------------------------------------------------------------
# what a run records and compares
# --------------------------------------------------------------------------

def _record(args) -> tuple[str, list[str]]:
    """(returned FQN, recorded output tables) for a cache miss of 1_bf."""
    with (
        patch.object(dit_workflow, "read_cache", return_value=None),
        patch.object(dit_workflow, "verify_tables_exist", return_value=[True, True]),
        patch.object(dit_workflow, "write_cache") as write,
        patch.object(dit_workflow, "expires_at_for",
                     return_value=datetime(2026, 11, 1, tzinfo=timezone.utc)),
    ):
        result = mod._run_with_cache(args, mod.MODE_BF, _SUFFIX, MagicMock())
    return result, write.call_args.args[0].output_tables


def test_thin_port_messages_alone_records_and_compares_the_thinned_table():
    args = _args("--steps", "thin_port_messages")
    result, recorded = _record(args)

    thinned = mod._thinned_table(args, _SUFFIX, mod.MODE_BF)
    assert result == thinned
    assert recorded == [thinned]
    assert mod.COMPARE_KEYS_BY_STEP[mod._last_step(args)] == ("identifier", "timestamp")


def test_both_steps_still_compare_visits_and_record_the_intermediate():
    args = _args()
    result, recorded = _record(args)

    assert result == mod._visits_table(args, _SUFFIX, mod.MODE_BF)
    assert recorded == [result, mod._thinned_table(args, _SUFFIX, mod.MODE_BF)]
    assert mod.COMPARE_KEYS_BY_STEP[mod._last_step(args)] == mod.COMPARE_KEYS


def test_compare_all_compares_on_the_given_keys():
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare:
        mod.compare_all({"1_bf": "p.d.a", "2_bfd": "p.d.b"}, ["1_bf", "2_bfd"],
                        keys=mod.THIN_COMPARE_KEYS)
    assert compare.call_args.kwargs["keys"] == ("identifier", "timestamp")
