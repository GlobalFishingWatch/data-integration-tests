"""Tests for ``--modes`` subset selection (orchestration-evaluation axis 2).

Covers the shared ``dit.workflow`` helpers plus each consumer's wiring: the
three mode-family workflows (pipe-gaps ``mode_equivalence``, port-visits
``ais``, pipe-events ``fishing``) and the ``cross_version_ais`` wrapper that
forwards its selection down to ``ais.py``.

The behaviours worth protecting:
  * a typo'd mode fails loudly (silently running nothing looks like success);
  * selection order never leaks into run/pair order;
  * comparisons only pair modes that actually ran;
  * a single-mode run reports "nothing to compare", not a passed assertion.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from dit.workflow import add_modes_arg, parse_modes
from workflows.pipe_events import fishing as fishing_mod
from workflows.pipe_gaps import mode_equivalence as gaps_mod
from workflows.port_visits import ais as ais_mod
from workflows.port_visits import cross_version_ais as xv_mod

_CHOICES = ("1_bf", "2_bfd", "3_bftruncate")


# --------------------------------------------------------------------------
# dit.workflow.parse_modes
# --------------------------------------------------------------------------

def test_parse_modes_default_is_every_choice() -> None:
    assert parse_modes(",".join(_CHOICES), choices=_CHOICES) == list(_CHOICES)


def test_parse_modes_subset() -> None:
    assert parse_modes("1_bf", choices=_CHOICES) == ["1_bf"]
    assert parse_modes("1_bf,2_bfd", choices=_CHOICES) == ["1_bf", "2_bfd"]


def test_parse_modes_normalises_to_canonical_order() -> None:
    """CLI order must not leak into run order or comparison-pair order --
    otherwise two equivalent invocations produce differently-ordered logs and
    pairs."""
    assert parse_modes("3_bftruncate,1_bf", choices=_CHOICES) == ["1_bf", "3_bftruncate"]
    assert parse_modes("2_bfd,1_bf", choices=_CHOICES) == ["1_bf", "2_bfd"]


def test_parse_modes_tolerates_whitespace_and_dedupes() -> None:
    assert parse_modes(" 1_bf , 2_bfd ,1_bf ", choices=_CHOICES) == ["1_bf", "2_bfd"]


def test_parse_modes_rejects_unknown_mode() -> None:
    """A typo'd mode must fail loudly: silently running nothing and comparing
    nothing is indistinguishable from a clean pass."""
    with pytest.raises(SystemExit) as exc:
        parse_modes("1_bf,4_typo", choices=_CHOICES)
    msg = str(exc.value)
    assert "4_typo" in msg
    assert "1_bf,2_bfd,3_bftruncate" in msg  # names the valid set


def test_parse_modes_rejects_empty_selection() -> None:
    for raw in ("", "   ", ",,"):
        with pytest.raises(SystemExit):
            parse_modes(raw, choices=_CHOICES)


def test_add_modes_arg_defaults_to_all_choices() -> None:
    import argparse

    p = argparse.ArgumentParser()
    add_modes_arg(p, choices=_CHOICES)
    assert p.parse_args([]).modes == "1_bf,2_bfd,3_bftruncate"
    assert p.parse_args(["--modes", "1_bf"]).modes == "1_bf"


# --------------------------------------------------------------------------
# Per-workflow wiring: parse_args validates and canonicalises
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "mod, extra",
    [
        (gaps_mod, []),
        (ais_mod, []),
        (fishing_mod, []),
    ],
    ids=["pipe_gaps", "port_visits", "pipe_events"],
)
def test_workflow_modes_default_is_all_three(mod, extra) -> None:
    args = mod.parse_args(extra)
    assert args.modes == ["1_bf", "2_bfd", "3_bftruncate"]


@pytest.mark.parametrize(
    "mod",
    [gaps_mod, ais_mod, fishing_mod],
    ids=["pipe_gaps", "port_visits", "pipe_events"],
)
def test_workflow_modes_subset_parsed_to_list(mod) -> None:
    assert mod.parse_args(["--modes", "1_bf"]).modes == ["1_bf"]


@pytest.mark.parametrize(
    "mod",
    [gaps_mod, ais_mod, fishing_mod],
    ids=["pipe_gaps", "port_visits", "pipe_events"],
)
def test_workflow_rejects_unknown_mode_before_any_cloud_call(mod) -> None:
    """Validation lives in parse_args precisely so it fires before a snapshot,
    an image build, or a Dataflow submission."""
    with pytest.raises(SystemExit):
        mod.parse_args(["--modes", "nope"])


@pytest.mark.parametrize(
    "mod",
    [gaps_mod, ais_mod, fishing_mod],
    ids=["pipe_gaps", "port_visits", "pipe_events"],
)
def test_workflow_modes_canonicalised(mod) -> None:
    assert mod.parse_args(["--modes", "3_bftruncate,1_bf"]).modes == ["1_bf", "3_bftruncate"]


# --------------------------------------------------------------------------
# pipe-gaps: mutate_recover keeps its own gate
# --------------------------------------------------------------------------

def test_pipe_gaps_mutate_recover_is_not_selectable_via_modes() -> None:
    """MODE_MUTATE_RECOVER needs the restricted-ssvids machinery that --modes
    knows nothing about, so it keeps --enable-pipeline-4 as its single gate.
    Naming it in --modes is an error that points the user at the right flag."""
    assert gaps_mod.MODE_MUTATE_RECOVER not in gaps_mod.SELECTABLE_MODES
    with pytest.raises(SystemExit):
        gaps_mod.parse_args(["--modes", gaps_mod.MODE_MUTATE_RECOVER])


def test_pipe_gaps_enable_pipeline_4_requires_bf() -> None:
    """mutate_recover is compared against 1_bf, and --auto-restrict reads
    1_bf's output table -- so excluding it would break both."""
    with pytest.raises(SystemExit):
        gaps_mod.parse_args([
            "--enable-pipeline-4", "--restricted-ssvids", "1,2",
            "--modes", "2_bfd",
        ])
    # 1_bf present -> accepted.
    args = gaps_mod.parse_args([
        "--enable-pipeline-4", "--restricted-ssvids", "1,2",
        "--modes", "1_bf,2_bfd",
    ])
    assert args.modes == ["1_bf", "2_bfd"]


# --------------------------------------------------------------------------
# Comparison gating: pairs only among modes that ran
# --------------------------------------------------------------------------

def _fqns(*modes: str) -> dict[str, str]:
    return {m: f"proj.ds.tbl_{m}" for m in modes}


def test_port_visits_compare_all_pairs_only_selected_modes() -> None:
    fqns = _fqns("1_bf", "2_bfd", "3_bftruncate")
    with patch.object(ais_mod.dit_compare, "compare_tables", return_value=0) as cmp:
        rc = ais_mod.compare_all(fqns, ["1_bf", "2_bfd"])
    assert rc == 0
    # One pair, not three -- 3_bftruncate never ran.
    assert cmp.call_count == 1
    a, b = cmp.call_args.args[:2]
    assert (a, b) == ("proj.ds.tbl_1_bf", "proj.ds.tbl_2_bfd")


def test_port_visits_compare_all_single_mode_compares_nothing() -> None:
    """The cheap-smoke shape: run one mode, assert nothing. Must NOT invoke
    the comparator at all, and must still exit 0."""
    with patch.object(ais_mod.dit_compare, "compare_tables") as cmp:
        rc = ais_mod.compare_all(_fqns("1_bf"), ["1_bf"])
    assert rc == 0
    cmp.assert_not_called()


def test_port_visits_compare_all_full_set_still_three_pairs() -> None:
    """Default behaviour is unchanged: all three modes -> three pairs."""
    fqns = _fqns("1_bf", "2_bfd", "3_bftruncate")
    with patch.object(ais_mod.dit_compare, "compare_tables", return_value=0) as cmp:
        ais_mod.compare_all(fqns, ["1_bf", "2_bfd", "3_bftruncate"])
    assert cmp.call_count == 3


def test_pipe_events_compare_all_pairs_only_selected_modes() -> None:
    fqns = _fqns("1_bf", "2_bfd", "3_bftruncate")
    with patch.object(fishing_mod.dit_compare, "compare_tables", return_value=0) as cmp:
        rc = fishing_mod.compare_all(fqns, ["1_bf", "3_bftruncate"], label="fishing_events")
    assert rc == 0
    assert cmp.call_count == 1
    a, b = cmp.call_args.args[:2]
    assert (a, b) == ("proj.ds.tbl_1_bf", "proj.ds.tbl_3_bftruncate")


def test_pipe_events_compare_all_single_mode_compares_nothing() -> None:
    with patch.object(fishing_mod.dit_compare, "compare_tables") as cmp:
        rc = fishing_mod.compare_all(_fqns("1_bf"), ["1_bf"], label="fishing_events")
    assert rc == 0
    cmp.assert_not_called()


def test_pipe_events_compare_all_still_propagates_divergence() -> None:
    """Subsetting must not weaken the failure signal for the pairs that DO run."""
    fqns = _fqns("1_bf", "2_bfd")
    with patch.object(fishing_mod.dit_compare, "compare_tables", return_value=7):
        rc = fishing_mod.compare_all(fqns, ["1_bf", "2_bfd"], label="fishing_events")
    assert rc != 0


# --------------------------------------------------------------------------
# cross_version_ais: forwards the selection so bindings RUN only those modes
# --------------------------------------------------------------------------

def _xv_fqns():
    return xv_mod._CrossVersionSnapshotFQNs(
        messages_positions="dst.m",
        segment_info="dst.si",
        segs_activity="dst.sa",
    )


def test_cross_version_forwards_modes_to_ais() -> None:
    """Before ais.py had --modes, a --modes subset here narrowed only the DIFF
    set -- every binding still ran all three modes, so the flag saved nothing
    on the expensive half. It must now reach ais.py."""
    out = xv_mod._ais_args_for_binding(
        [],
        snapshot_fqns=_xv_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=["1_bf"],
    )
    assert "--modes" in out
    assert out[out.index("--modes") + 1] == "1_bf"


def test_cross_version_drops_user_supplied_modes() -> None:
    """The wrapper owns --modes on both halves (run + diff); a user extra could
    otherwise desync them -- bindings running one set while we diff another."""
    out = xv_mod._ais_args_for_binding(
        ["--modes", "2_bfd"],
        snapshot_fqns=_xv_fqns(),
        suffix="exp01-before",
        experiment_id="exp01",
        binding_name="before",
        modes=["1_bf"],
    )
    assert out.count("--modes") == 1
    assert out[out.index("--modes") + 1] == "1_bf"


def test_cross_version_validates_modes_against_ais() -> None:
    """cross_version previously accepted any string, so a typo produced zero
    diff pairs -- which reads exactly like "everything matched"."""
    with pytest.raises(SystemExit):
        xv_mod.parse_args([
            "--experiment-id", "exp01",
            "--pin-source-at", "2026-05-15T10:00:00Z",
            "--binding", "before=v1",
            "--modes", "nope",
        ])


def test_cross_version_mode_set_tracks_ais() -> None:
    """The two must not drift: cross_version imports ais.py's constant."""
    assert xv_mod.AIS_SELECTABLE_MODES is ais_mod.SELECTABLE_MODES
