"""Tests for the per-side adjustments of the pipe-segment cross-version
workflow: ``workflows/pipe_segment/adjustments.py`` and what
``identity_match_key.py`` does with it.

* The declared adjustment: below v5.1.0 the incremental segmenter's three
  flags are removed, decided by the ref's own CLI source first and by its
  version second.
* The base interface passes ``--incremental`` as production's task does.
* ``--binding-adjust``, ``--binding-image`` and ``--allow-same-gpsdio-pin``.
* The preflight: every flag against the ref's CLI source, before any pin.
* The run log, and the tables rules (union of inputs, one-sided outputs,
  equal keys), the latter with synthetic table adjustments only: no
  pipe-segment version changes its tables.

The pipe-segment checkout these tests read is synthetic: a throwaway git
repository with invented command modules that declare the flags DIT passes.
No docker, BigQuery or Dataflow call. One opt-in test at the end reads a real
pipe-segment checkout when one with the tags v5.0.3 and v5.1.0 is found.
"""

from __future__ import annotations

import argparse
import logging
import re
import subprocess
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from dit import adjust, git_info
from dit import bq as dit_bq
from workflows.pipe_segment import adjustments
from workflows.pipe_segment import identity_match_key as mod

INCREMENTAL_FLAGS = ("--incremental", "--in_segments_table", "--in_fragments_table")
OUT = "world-fishing-827.ds"
_GE = "world-fishing-827.tech_great_expectations"

# --------------------------------------------------------------------------
# A synthetic pipe-segment checkout
# --------------------------------------------------------------------------

#: The flags each command declares in the synthetic checkout: those DIT passes
#: that are pipe-segment's own. Beam's options are declared by no command.
DECLARED = {
    "segment": ["--in_normalized_messages_table", "--out_segmented_messages_table",
                "--fragments_table", "--out_segments_table", "--date_range", "--wait_for_job",
                "--ssvid_filter_query", "--bins_per_day"],
    "segment_identity": ["--source_segments", "--source_fragments", "--dest_segment_identity",
                         "--date_range", "--wait_for_job"],
    "segment_vessel_daily": ["--source_segment_identity", "--destination", "--date_range",
                             "--labels", "--project"],
    "segment_info": ["--source_segment_identity", "--source_segment_vessel", "--destination",
                     "--labels", "--project"],
    "vessel_info": ["--source_segment_identity", "--source_segment_vessel", "--destination",
                    "--labels", "--project"],
    "segment_vessel": ["--source_segment_vessel_daily", "--destination", "--labels",
                       "--project"],
}


def _command_module(flags: list[str]) -> str:
    return "def add_to_subparsers(add):\n" + "".join(
        f'    add("{flag}", help="see --not_a_declaration")\n' for flag in flags)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _commit(repo: Path, message: str, *, tag: str | None = None) -> None:
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", message)
    if tag:
        _git(repo, "tag", tag)


def _write_commands(repo: Path, root: str, *, incremental: bool) -> None:
    commands = repo / root / "pipe_segment" / "cli" / "commands"
    commands.mkdir(parents=True, exist_ok=True)
    for step, flags in DECLARED.items():
        extra = list(INCREMENTAL_FLAGS) if incremental and step == "segment" else []
        (commands / f"{step}.py").write_text(_command_module([*flags, *extra]))


@pytest.fixture(scope="module")
def checkout(tmp_path_factory) -> str:
    """A git repository shaped like pipe-segment's, with:

    * tag ``v5.0.0``: the old layout (commands at the root, ``version.py``);
    * tag ``v5.0.3``: the ``src/`` layout and ``pyproject.toml``, no
      incremental segmenter;
    * tag ``v5.1.0``: the three incremental flags declared;
    * branch ``unreleased``: the three flags declared, the version still 5.0.3;
    * branch ``no-cli``: no command module at all.
    """
    repo = tmp_path_factory.mktemp("pipe-segment")
    _git(repo, "init", "-q", "-b", "main")

    _write_commands(repo, ".", incremental=False)
    (repo / "pipe_segment" / "version.py").write_text("__version__ = '5.0.0'\n")
    _commit(repo, "v5.0.0", tag="v5.0.0")

    _git(repo, "rm", "-q", "-r", "pipe_segment")
    _write_commands(repo, "src", incremental=False)
    (repo / "pyproject.toml").write_text('[project]\nname = "pipe-segment"\nversion = "5.0.3"\n')
    _commit(repo, "v5.0.3", tag="v5.0.3")

    _git(repo, "checkout", "-q", "-b", "unreleased")
    _write_commands(repo, "src", incremental=True)
    _commit(repo, "the incremental segmenter, not released yet")

    _git(repo, "checkout", "-q", "main")
    _write_commands(repo, "src", incremental=True)
    (repo / "pyproject.toml").write_text('[project]\nname = "pipe-segment"\nversion = "5.1.0"\n')
    _commit(repo, "v5.1.0", tag="v5.1.0")

    _git(repo, "checkout", "-q", "-b", "no-cli")
    _git(repo, "rm", "-q", "-r", "src")
    _commit(repo, "no command modules")
    _git(repo, "checkout", "-q", "main")
    return str(repo)


def _args(checkout: str | None, *extra: str, runner: str = "dataflow",
          bindings: tuple[str, ...] = ("before=v5.0.3", "after=v5.1.0")) -> argparse.Namespace:
    return mod.parse_args([
        "--experiment-id", "exp",
        *(token for binding in bindings for token in ("--binding", binding)),
        "--date-range", "2020-01-01,2020-01-02",
        "--dest-dataset", "ds",
        "--service-account", "sa@p.iam.gserviceaccount.com",
        "--runner", runner,
        *(["--pipeline-dir", checkout] if checkout else []),
        *extra,
    ])


def _suffixes(args: argparse.Namespace) -> dict[str, str]:
    return {name: f"{args.experiment_id}_{name}" for name, _ in args.bindings}


def _flags(argv: list[str]) -> list[str]:
    return [token.split("=", 1)[0] for token in argv if token.startswith("--")]


def _value(argv: list[str], flag: str) -> str:
    (value,) = [t.split("=", 1)[1] for t in argv if t.startswith(f"{flag}=")]
    return value


def _segment(args: argparse.Namespace, name: str, **kwargs: Any) -> list[str]:
    return mod._step_argvs(
        args, binding_name=name, suffix=f"exp_{name}", worker_image="registry.invalid/p:v0",
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"), **kwargs,
    )["segment"]


# --------------------------------------------------------------------------
# What is read from a ref
# --------------------------------------------------------------------------

def test_show_file_reads_one_file_at_a_ref_and_gives_none_when_it_cannot(checkout) -> None:
    assert 'version = "5.0.3"' in (git_info.show_file(checkout, "v5.0.3", "pyproject.toml") or "")
    assert git_info.show_file(checkout, "v5.0.0", "pyproject.toml") is None      # no such path
    assert git_info.show_file(checkout, "no-such-ref", "pyproject.toml") is None
    assert git_info.show_file("/nonexistent/dir", "v5.0.3", "pyproject.toml") is None


def test_first_file_takes_the_first_path_that_exists_at_the_ref(checkout) -> None:
    paths = adjustments.command_files("segment")

    assert paths == ("src/pipe_segment/cli/commands/segment.py",
                     "pipe_segment/cli/commands/segment.py")
    assert git_info.first_file(checkout, "v5.1.0", paths)[0] == paths[0]
    assert git_info.first_file(checkout, "v5.0.0", paths)[0] == paths[1]
    assert git_info.first_file(checkout, "no-cli", paths) is None


@pytest.mark.parametrize("ref, version", [
    ("v5.0.0", (5, 0, 0)),          # version.py at the root
    ("v5.0.3", (5, 0, 3)),          # pyproject.toml
    ("v5.1.0", (5, 1, 0)),
    ("unreleased", (5, 0, 3)),      # still the release before it
    ("no-such-ref", None),
])
def test_the_version_a_ref_states(checkout, ref, version) -> None:
    assert adjustments.read_version(checkout, ref) == version


@pytest.mark.parametrize("ref, version", [
    ("v5.0.3", (5, 0, 3)), ("5.1.0", (5, 1, 0)), ("v10.2.33", (10, 2, 33)),
    ("HEAD", None), ("main", None), ("v5.1", None), ("v5.0.3-hotfix", None),
    ("release/v5.0.3", None), ("", None),
])
def test_a_ref_names_a_version_only_when_it_is_written_as_one(ref, version) -> None:
    assert adjustments.ref_version(ref) == version


def test_the_flags_each_command_declares_are_read_per_step_in_either_layout(checkout) -> None:
    new = adjustments.read_declared(checkout, "v5.1.0", mod.STEPS)
    old = adjustments.read_declared(checkout, "v5.0.0", mod.STEPS)

    assert list(new) == list(old) == list(mod.STEPS)
    assert new["segment"] == frozenset(DECLARED["segment"]) | frozenset(INCREMENTAL_FLAGS)
    assert old["segment"] == frozenset(DECLARED["segment"])
    assert new["segment_info"] == frozenset(DECLARED["segment_info"])
    # A flag named only in a help string is not a declaration.
    assert "--not_a_declaration" not in new["segment"]
    # A ref without the command modules: nothing read, which is not "declares nothing".
    assert adjustments.read_declared(checkout, "no-cli", mod.STEPS) == {}


def test_binding_info_holds_what_the_ref_says_about_itself(checkout) -> None:
    info = adjustments.binding_info(checkout, "after", "v5.1.0", mod.STEPS)

    assert (info.name, info.ref, info.version) == ("after", "v5.1.0", (5, 1, 0))
    assert info.declares("segment", "--incremental") is True
    assert info.declares("segment_identity", "--incremental") is False
    offline = adjustments.offline_binding_info("before", "v5.0.3")
    assert (offline.version, offline.declared) == ((5, 0, 3), None)
    assert adjustments.offline_binding_info("x", "HEAD").version is None
    assert adjustments.offline_binding_info("x", None).version is None


# --------------------------------------------------------------------------
# The declared adjustment: no incremental segmenter below v5.1.0
# --------------------------------------------------------------------------

def _info(version=None, declared=None) -> adjust.BindingInfo:
    return adjust.BindingInfo("b", version=version, declared=declared)


WITH = {"segment": frozenset({"--date_range", "--incremental"})}
WITHOUT = {"segment": frozenset({"--date_range"})}


@pytest.mark.parametrize("info, lacks", [
    # The ref's own CLI source decides when it was read...
    (_info(declared=WITHOUT), True),
    (_info(declared=WITH), False),
    # ...also against the version: an unreleased head states the release before it.
    (_info(version=(5, 0, 3), declared=WITH), False),
    (_info(version=(5, 1, 0), declared=WITHOUT), True),
    # Otherwise the version decides.
    (_info(version=(5, 0, 3)), True),
    (_info(version=(4, 9, 9)), True),
    (_info(version=(5, 1, 0)), False),
    (_info(version=(6, 0, 0)), False),
    # A source read for other steps only says nothing about segment.
    (_info(version=(5, 0, 3), declared={"segment_info": frozenset()}), True),
    # Nothing known: the binding is taken to be what the base is for.
    (_info(), False),
])
def test_when_a_binding_lacks_the_incremental_segmenter(info, lacks) -> None:
    assert adjustments.lacks_incremental(info) is lacks


def test_the_one_declared_adjustment_removes_the_three_flags_from_segment() -> None:
    (adjustment,) = adjustments.DECLARED

    assert adjustment is adjustments.NO_INCREMENTAL_SEGMENTER
    assert [(edit.step, edit.description) for edit in adjustment.edits] == [
        ("segment", f"remove {flag}") for flag in INCREMENTAL_FLAGS
    ]
    assert adjustments.INCREMENTAL_FLAGS == INCREMENTAL_FLAGS
    assert adjustments.INCREMENTAL_SINCE == (5, 1, 0)
    assert "below v5.1.0" in adjustment.reason and "Beam, which ignores it" in adjustment.reason


def test_every_older_version_with_an_adjustment_names_its_composer_commit() -> None:
    assert list(adjustments.FROZEN_AT) == [(5, 0, 3)]
    assert all(len(commit) == 40 and int(commit, 16) for commit in adjustments.FROZEN_AT.values())
    assert all(version < adjustments.INCREMENTAL_SINCE for version in adjustments.FROZEN_AT)


# --------------------------------------------------------------------------
# The base interface, and each binding's argv
# --------------------------------------------------------------------------

@pytest.mark.parametrize("runner", ["dataflow", "DirectRunner"])
def test_the_base_passes_incremental_with_the_bindings_own_tables(runner) -> None:
    args = _args(None, runner=runner)
    segment = _segment(args, "after")

    assert _flags(segment)[5:8] == list(INCREMENTAL_FLAGS)
    assert "--incremental" in segment                      # bare, as production passes it
    assert _value(segment, "--in_segments_table") == f"{OUT}.segments_exp_after"
    assert _value(segment, "--in_fragments_table") == f"{OUT}.fragments_exp_after"
    assert _value(segment, "--in_segments_table") == _value(segment, "--out_segments_table")
    assert _value(segment, "--in_fragments_table") == _value(segment, "--fragments_table")


def test_with_no_adjustment_each_argv_is_the_builders_token_for_token() -> None:
    """The base interface adds nothing to and takes nothing from what the
    builders emit, apart from filling the pinned source."""
    args = _args(None)
    kwargs = dict(dest_dataset="ds", suffix="exp_after", experiment_id="exp",
                  binding_name="after")
    argvs = mod._step_argvs(
        args, binding_name="after", suffix="exp_after", worker_image="registry.invalid/p:v0",
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
    )

    assert argvs["segment"] == mod._segment_args(
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
        date_range=args.date_range, runner="dataflow", worker_image="registry.invalid/p:v0",
        ssvid_filter_query=None, service_account="sa@p.iam.gserviceaccount.com",
        max_num_workers=50, worker_sizing=args.worker_sizings["segment"],
        extra_flags=mod._incremental_flags(dest_dataset="ds", suffix="exp_after"), **kwargs,
    )
    assert argvs["segment_info"] == mod._segment_info_args(**kwargs)
    assert list(argvs) == list(mod.STEPS)


def test_a_binding_below_v5_1_0_loses_the_three_flags_and_nothing_else() -> None:
    args = _args(None)
    old, new = _segment(args, "before"), _segment(args, "after")

    assert not set(INCREMENTAL_FLAGS) & set(_flags(old))
    assert _flags(old) == [flag for flag in _flags(new) if flag not in INCREMENTAL_FLAGS]
    # No other step differs between the two bindings beyond their own names.
    for name in ("before", "after"):
        argvs = mod._step_argvs(
            args, binding_name=name, suffix=f"exp_{name}", worker_image=None,
            pinned=mod._PinnedSources(normalized_messages="p.d.normalized"))
        assert not any(set(INCREMENTAL_FLAGS) & set(_flags(argvs[step]))
                       for step in mod.STEPS[1:])


def test_the_refs_source_overrules_the_version_for_an_unreleased_head(checkout) -> None:
    args = _args(checkout, bindings=("before=v5.0.3", "after=unreleased"))
    infos = mod._binding_infos(args)

    assert infos["after"].version == (5, 0, 3) == infos["before"].version
    assert set(INCREMENTAL_FLAGS) <= set(_flags(_segment(args, "after", binding=infos["after"])))
    assert not set(INCREMENTAL_FLAGS) & set(
        _flags(_segment(args, "before", binding=infos["before"])))


def test_the_interface_names_the_input_and_every_output_with_its_keys() -> None:
    args = _args(None)
    interface = mod._base_interface(
        args, binding_name="after", suffix="exp_after", worker_image=None)

    assert interface.inputs == (adjust.Input(
        role="normalized_messages", source=mod.DEFAULT_SRC_NORMALIZED_TABLE,
        window=dit_bq.SourceWindow("timestamp", date(2020, 1, 1), date(2020, 1, 3)),
    ),)
    assert [(o.name, o.table, list(o.keys)) for o in interface.outputs] == [
        (table, f"{OUT}.{table}_exp_after", mod.COMPARE_KEYS[table])
        for table in (*mod.SEGMENT_OUTPUT_TABLES, *mod.DOWNSTREAM_OUTPUT_TABLES)
    ]
    assert all(o.meaning for o in interface.outputs)
    assert set(mod.OUTPUT_MEANINGS) == set(mod.COMPARE_KEYS)
    # The pinned source is a placeholder until it is pinned.
    assert "--in_normalized_messages_table={in.normalized_messages}" in interface.steps["segment"]
    assert adjust.unknown_placeholders(interface) == ()

    skipped = mod._base_interface(
        _args(None, "--skip-downstream"), binding_name="after", suffix="s", worker_image=None)
    assert list(skipped.steps) == ["segment"]
    assert [o.name for o in skipped.outputs] == list(mod.SEGMENT_OUTPUT_TABLES)


def test_the_mode_equivalence_workflow_still_builds_its_own_flags() -> None:
    """``ais.py`` shares ``_incremental_flags`` and the builder, not the base
    interface: its full-history job passes none of the three flags."""
    from workflows.pipe_segment import ais

    assert ais.INCREMENTAL_SINCE is adjustments.INCREMENTAL_SINCE
    assert ais.image_pipeline_version is adjustments.image_pipeline_version
    assert mod._incremental_flags(dest_dataset="ds", suffix="s_1_bf_inc") == [
        "--incremental",
        f"{INCREMENTAL_FLAGS[1]}={OUT}.segments_s_1_bf_inc",
        f"{INCREMENTAL_FLAGS[2]}={OUT}.fragments_s_1_bf_inc",
    ]


# --------------------------------------------------------------------------
# --binding-adjust
# --------------------------------------------------------------------------

def test_an_ad_hoc_edit_changes_one_bindings_argv_only() -> None:
    args = _args(None, "--binding-adjust", "after=segment:add:--bins_per_day=8",
                 "--binding-adjust", "after=segment_info:set:--project=other")

    assert _segment(args, "after")[-1] == "--bins_per_day=8"
    assert "--bins_per_day" not in _flags(_segment(args, "before"))
    after = mod._step_argvs(
        args, binding_name="after", suffix="exp_after", worker_image=None,
        pinned=mod._PinnedSources(normalized_messages="p.d.normalized"))
    assert after["segment_info"][-1] == "--project=other"
    assert after["vessel_info"][-1] == "--project=world-fishing-827"


def test_ad_hoc_edits_come_after_the_declared_ones_in_the_order_given() -> None:
    """The older binding loses the flags by the declared adjustment; an ad hoc
    edit can give one back, and a later one can change it again."""
    args = _args(
        None,
        "--binding-adjust", "before=segment:add:--in_segments_table={out.segments}",
        "--binding-adjust", "before=segment:set:--in_segments_table={out.fragments}",
        "--binding-adjust", "after=segment:remove:--incremental",
    )
    before, after = _segment(args, "before"), _segment(args, "after")

    assert before[-1] == f"--in_segments_table={OUT}.fragments_exp_before"
    assert "--incremental" not in before
    assert "--incremental" not in after and "--in_segments_table" in _flags(after)


def test_a_placeholder_names_the_bindings_own_table_or_the_pinned_source() -> None:
    args = _args(None, "--binding-adjust",
                 "after=segment:set:--in_fragments_table={in.normalized_messages}")

    assert _value(_segment(args, "after"), "--in_fragments_table") == "p.d.normalized"


def test_both_bindings_on_full_history_by_three_removals() -> None:
    args = _args(None, *(
        token for flag in INCREMENTAL_FLAGS
        for token in ("--binding-adjust", f"after=segment:remove:{flag}")
    ))

    assert _flags(_segment(args, "after")) == _flags(_segment(args, "before"))


@pytest.mark.parametrize("spec, message", [
    ("nobody=segment:remove:--incremental",
     "--binding-adjust names the binding 'nobody', and the bindings are before, after"),
    ("after=segmnt:remove:--incremental", "no step 'segmnt'; the steps are segment, segment_"),
    ("after=segment:drop:--incremental", "unknown edit 'drop'"),
    ("after=segment:remove", "The form is NAME=<step>:<op>:<flag>"),
    ("after=segment:set:--x", "set needs a value"),
])
def test_a_bad_binding_adjust_is_refused_at_parse_time(spec, message) -> None:
    with pytest.raises(SystemExit, match=message):
        _args(None, "--binding-adjust", spec)


def test_an_ad_hoc_edit_that_finds_nothing_to_change_is_an_error() -> None:
    """The older binding has no ``--incremental`` left to remove."""
    args = _args(None, "--binding-adjust", "before=segment:remove:--incremental")

    with pytest.raises(adjust.AdjustmentError) as stale:
        _segment(args, "before")

    assert "binding before: step segment: cannot remove --incremental" in str(stale.value)
    assert "ad hoc: before=segment:remove:--incremental" in str(stale.value)
    # The other binding is not affected.
    assert "--incremental" in _segment(args, "after")


def test_an_ad_hoc_edit_on_a_skipped_step_is_an_error() -> None:
    args = _args(None, "--skip-downstream",
                 "--binding-adjust", "after=segment_info:set:--project=other")

    with pytest.raises(adjust.AdjustmentError, match="no step 'segment_info'"):
        _segment(args, "after")


# --------------------------------------------------------------------------
# --binding-image and --allow-same-gpsdio-pin
# --------------------------------------------------------------------------

V503_IMAGE = "gcr.io/world-fishing-827/dit/pipe-segment:v5.0.3-abc1234-temp-dataset-04b40b54"


def _run_binding(args: argparse.Namespace, name: str, ref: str, *, resolved: str | None = None,
                 binding: adjust.BindingInfo | None = None):
    """One binding with the harness and the runner patched: ``(asked image,
    calls)`` where each call is ``(image, argv)``."""
    asked: list[str] = []
    calls: list[tuple[str, list[str]]] = []

    def context(**kwargs):
        asked.append(kwargs["worker_image"])
        return SimpleNamespace(worker_image=resolved or kwargs["worker_image"],
                               pipeline_commit="c", unreviewed=False)

    with (
        patch.object(mod.subprocess, "run"),
        patch.object(mod, "_verify_gpsdio_segment", return_value="pin"),
        patch.object(mod, "resolve_run_context", side_effect=context),
        patch.object(mod.dit_docker, "run",
                     side_effect=lambda image, argv, **kw: calls.append((image, argv)) or 0),
    ):
        assert mod._run_binding(
            name=name, ref=ref, args=args, suffix=f"exp_{name}", binding=binding,
            pinned=mod._PinnedSources(normalized_messages="p.d.normalized"),
        ) == 0
    return asked[0], calls


def test_a_binding_image_is_that_bindings_submitter_and_worker_image() -> None:
    args = _args(None, "--binding-image", f"before={V503_IMAGE}")
    asked_before, before = _run_binding(args, "before", "v5.0.3")
    asked_after, after = _run_binding(args, "after", "v5.1.0")

    assert asked_before == V503_IMAGE and asked_after == mod.DEFAULT_IMAGE_TAG
    assert {image for image, _ in before} == {V503_IMAGE}
    assert {image for image, _ in after} == {mod.DEFAULT_IMAGE_TAG}
    assert [_value(argv, "--sdk_container_image") for _, argv in before[:2]] == [V503_IMAGE] * 2
    assert [_value(argv, "--sdk_container_image") for _, argv in after[:2]] == [
        mod.DEFAULT_IMAGE_TAG] * 2
    # Each on the segmenter its pipe-segment has.
    assert "--incremental" not in before[0][1] and "--incremental" in after[0][1]


@pytest.mark.parametrize("extra, message", [
    (["--binding-image", "nobody=img"], "--binding-image names the binding 'nobody'"),
    (["--binding-image", "before"], "--binding-image must be NAME=IMAGE"),
    (["--binding-image", "before="], "--binding-image must be NAME=IMAGE"),
    (["--binding-image", "before=a", "--binding-image", "before=b"],
     "--binding-image is given twice for the binding 'before'"),
])
def test_a_bad_binding_image_is_refused_at_parse_time(extra, message) -> None:
    with pytest.raises(SystemExit, match=message):
        _args(None, *extra)


def test_without_binding_images_every_binding_asks_for_the_image_tag() -> None:
    args = _args(None, "--image-tag", "registry.invalid/p:custom")

    assert args.binding_images == {} and args.binding_adjustments == []
    assert _run_binding(args, "before", "v5.0.3")[0] == "registry.invalid/p:custom"


def _check_versions(caplog, args, versions: dict[str, tuple[int, int, int] | None]):
    """``_check_image_versions`` over bindings of the given stated versions:
    ``(SystemExit or None, the warnings logged)``."""
    refs = dict(args.bindings)
    sides = {
        name: SimpleNamespace(binding=adjust.BindingInfo(name=name, ref=refs[name], version=v))
        for name, v in versions.items()
    }
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        try:
            mod._check_image_versions(args, sides)
            refused = None
        except SystemExit as exc:
            refused = exc
    return refused, [r.getMessage() for r in caplog.records]


def test_a_tagged_ref_on_an_image_of_another_version_is_refused_before_pinning(caplog) -> None:
    """The scenario of the review: ``before=v5.0.3`` without its own image
    would run the default v5.1.0 image with v5.0.3's flags, and the run would
    report v5.1.0 code as v5.0.3."""
    refused, warnings = _check_versions(
        caplog, _args(None), {"before": (5, 0, 3), "after": (5, 1, 0)})

    assert refused is not None and warnings == []
    message = str(refused)
    assert "binding before: the image" in message and "tagged pipe-segment v5.1.0" in message
    assert "ref v5.0.3 states v5.0.3" in message
    assert "--binding-image before=<overlay image of v5.0.3>" in message
    assert "binding after" not in message
    # An image older than the ref is refused as well.
    older = _args(None, "--image-tag", V503_IMAGE, "--binding-image", f"before={V503_IMAGE}")
    refused, _ = _check_versions(caplog, older, {"before": (5, 0, 3), "after": (5, 1, 0)})
    assert refused is not None and "binding after" in str(refused)


@pytest.mark.parametrize("extra, runner, bindings, versions", [
    # Its own image of its own version.
    (("--binding-image", f"before={V503_IMAGE}"), "dataflow",
     ("before=v5.0.3", "after=v5.1.0"), {"before": (5, 0, 3), "after": (5, 1, 0)}),
    # Its own image whose tag names no version.
    (("--binding-image", "before=registry.invalid/p:dit-abc"), "dataflow",
     ("before=v5.0.3", "after=v5.1.0"), {"before": (5, 0, 3), "after": (5, 1, 0)}),
    # A ref whose version cannot be read.
    ((), "dataflow", ("x=HEAD",), {"x": None}),
    # On the docker runner with --build-from-source no image runs.
    (("--build-from-source",), "DirectRunner",
     ("before=v5.0.3", "after=v5.1.0"), {"before": (5, 0, 3), "after": (5, 1, 0)}),
])
def test_image_versions_allowed_without_a_word(caplog, extra, runner, bindings, versions) -> None:
    refused, warnings = _check_versions(
        caplog, _args(None, *extra, runner=runner, bindings=bindings), versions)
    assert refused is None and warnings == []


def test_an_unreleased_head_on_the_next_release_gets_a_warning_only(caplog) -> None:
    """A branch whose code still states v5.0.3 on an image tagged v5.1.0, the
    release right after: taken as an unreleased head (the head of the PR that
    became v5.1.0 states v5.0.3)."""
    args = _args(None, "--binding-image", f"before={V503_IMAGE}",
                 bindings=("before=v5.0.3", "after=pr-227-head"))
    refused, warnings = _check_versions(caplog, args, {"before": (5, 0, 3), "after": (5, 0, 3)})

    assert refused is None
    (warning,) = warnings
    assert "binding after" in warning and "the release after the v5.0.3" in warning
    assert "unreleased head" in warning
    # Two releases ahead, or the ref written as a tag: refused.
    two_ahead = _check_versions(caplog, args, {"before": (5, 0, 3), "after": (4, 9, 0)})[0]
    assert two_ahead is not None and "binding after" in str(two_ahead)


def test_the_preflight_reads_the_versions_from_the_refs_and_refuses(checkout, caplog) -> None:
    """End to end through ``_preflight``, with the versions read from each
    ref's own source: ``before=v5.0.3`` on the default image is refused;
    with its own v5.0.3 image it passes; the ``unreleased`` branch (code
    stating 5.0.3, the incremental flags declared) on the default v5.1.0
    image only warns."""
    def preflight(*extra: str, bindings=("before=v5.0.3", "after=v5.1.0")):
        args = _args(checkout, *extra, bindings=bindings)
        return mod._preflight(args, _suffixes(args))

    with pytest.raises(SystemExit, match="binding before: the image .* tagged pipe-segment v5.1.0"):
        preflight()
    assert set(preflight("--binding-image", f"before={V503_IMAGE}")) == {"before", "after"}

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        preflight("--binding-image", f"before={V503_IMAGE}",
                  bindings=("before=v5.0.3", "head=unreleased"))
    assert any("binding head" in r.getMessage() and "unreleased head" in r.getMessage()
               for r in caplog.records)


def test_the_base_interface_names_its_bare_flags() -> None:
    """Every flag without a value in the base argvs is in ``bare``, so that
    removing one never takes the token after it."""
    args = _args(None)
    interface = mod._base_interface(args, binding_name="before", suffix="s", worker_image="img")
    valueless = {
        token for tokens in interface.steps.values() for token in tokens
        if token.startswith("--") and "=" not in token
    }

    assert valueless == interface.bare == mod.BARE_FLAGS
    assert interface.multi == frozenset()


def test_removing_a_bare_flag_followed_by_a_positional_keeps_the_positional() -> None:
    interface = mod._base_interface(
        _args(None), binding_name="before", suffix="s", worker_image="img")
    tokens = ("segment", "--incremental", "positional", "--x=1")
    remove = adjust.remove("--incremental")

    assert remove.change(tokens, interface.bare, interface.multi) == (
        "segment", "positional", "--x=1")
    # Without the hint the positional would be read as the flag's value.
    assert remove.change(tokens, frozenset(), frozenset()) == ("segment", "--x=1")


def test_the_version_check_runs_in_the_preflight_and_not_in_run_binding(caplog) -> None:
    """The check refuses before pinning; ``_run_binding`` logs nothing about
    versions any more."""
    with caplog.at_level(logging.WARNING, logger=mod.logger.name):
        _run_binding(_args(None), "before", "v5.0.3")
    assert not [r for r in caplog.records if "tagged pipe-segment" in r.getMessage()]
    assert not hasattr(mod, "_warn_on_version_mismatch")


def test_the_same_gpsdio_pin_is_refused_unless_allowed(caplog) -> None:
    bindings = [("before", "v5.0.3"), ("after", "v5.1.0")]
    with patch.object(mod, "_read_gpsdio_pin_at_ref", return_value="gpsdio-segment @ x@v3.0.0"):
        with pytest.raises(SystemExit, match="pass --allow-same-gpsdio-pin"):
            mod._verify_distinct_gpsdio_pins("unused", bindings)
        with caplog.at_level(logging.INFO, logger=mod.logger.name):
            pins = mod._verify_distinct_gpsdio_pins("unused", bindings, allow_same=True)

    assert set(pins) == {"before", "after"}
    assert any("allowed by --allow-same-gpsdio-pin" in r.getMessage() for r in caplog.records)
    assert _args(None).allow_same_gpsdio_pin is False
    assert _args(None, "--allow-same-gpsdio-pin").allow_same_gpsdio_pin is True


# --------------------------------------------------------------------------
# The preflight: before anything is pinned
# --------------------------------------------------------------------------

def _sides(checkout: str, *extra: str, **kwargs: Any) -> dict[str, adjust.Adjusted]:
    args = _args(checkout, *extra, **kwargs)
    return mod._sides(args, _suffixes(args), mod._binding_infos(args))


@pytest.mark.parametrize("runner", ["dataflow", "DirectRunner"])
@pytest.mark.parametrize("extra", [
    (), ("--build-from-source",), ("--ssvid-filter-query", '"1","2"'), ("--skip-downstream",),
    ("--max-num-workers", "7", "--disk-size-gb", "100"),
])
def test_every_flag_dit_passes_is_declared_or_beams(checkout, runner, extra) -> None:
    """Both bindings pass the check as they are: v5.1.0 with the base, v5.0.3
    with the declared adjustment. So the exempt list covers what DIT emits."""
    sides = _sides(checkout, *extra, runner=runner)

    mod._check_flags(sides)
    assert [adjustments.flag_problems(side.interface, side.binding, beam_steps=mod.BEAM_STEPS)
            for side in sides.values()] == [[], []]
    assert [bool(side.applied) for side in sides.values()] == [True, False]


def test_beams_options_are_exempt_on_the_beam_steps_only() -> None:
    assert adjustments.BEAM_OPTIONS == mod.adjustments.drift.DATAFLOW_OWNED_FLAGS | {
        "--project", "--runner", "--region", "--setup_file", "--worker_machine_type",
        "--disk_size_gb", "--no_use_public_ips", "--network",
    }
    # pipe-segment declares --wait_for_job itself, so it is checked.
    assert "--wait_for_job" not in adjustments.BEAM_OPTIONS
    # No Beam option is among the flags a command declares.
    declared = {flag for flags in DECLARED.values() for flag in flags} | set(INCREMENTAL_FLAGS)
    assert adjustments.BEAM_OPTIONS & declared == {"--labels", "--project"}   # the SQL steps'


def test_the_base_on_a_ref_without_the_flag_would_be_refused(checkout) -> None:
    """What the declared adjustment prevents: without it the v5.0.3 binding
    would pass three flags its segment command does not declare."""
    args = _args(checkout)
    with patch.object(adjustments, "DECLARED", ()):
        sides = mod._sides(args, _suffixes(args), mod._binding_infos(args))
        with pytest.raises(SystemExit) as refused:
            mod._check_flags(sides)

    message = str(refused.value)
    for flag in INCREMENTAL_FLAGS:
        assert (f"binding before (ref v5.0.3): step segment passes {flag}, which the ref's "
                f"segment command does not declare") in message
    assert "binding after" not in message
    assert "Beam, which ignores it" in message and "Nothing was pinned or run" in message
    assert "--binding-adjust <binding>=<step>:remove:<flag>" in message


def test_an_ad_hoc_flag_the_ref_does_not_declare_is_refused(checkout) -> None:
    sides = _sides(checkout, "--binding-adjust", "after=segment:add:--no_such_flag=1")

    with pytest.raises(SystemExit, match="binding after .ref v5.1.0.: step segment passes "
                                         "--no_such_flag"):
        mod._check_flags(sides)


def test_a_beam_option_on_a_sql_step_is_refused(checkout) -> None:
    """The SQL commands would ignore it silently too."""
    sides = _sides(checkout, "--binding-adjust", "after=segment_info:add:--runner=dataflow")

    with pytest.raises(SystemExit, match="step segment_info passes --runner"):
        mod._check_flags(sides)
    # The same option is Beam's on a Beam step, where it is already passed.
    assert "--runner" in _flags(list(sides["after"].interface.steps["segment"]))


def test_a_ref_whose_cli_source_cannot_be_read_is_refused(checkout) -> None:
    sides = _sides(checkout, bindings=("before=v5.0.3", "after=no-cli"))

    with pytest.raises(SystemExit) as refused:
        mod._check_flags(sides)

    message = str(refused.value)
    assert "binding after (ref no-cli): step segment: its command module was not found" in message
    assert "src/pipe_segment/cli/commands/segment.py" in message
    assert "binding before" not in message


def _main(checkout: str, *extra: str, **kwargs: Any) -> dict[str, Any]:
    """``main`` on the synthetic checkout with every cloud and docker call
    patched. The preflight's git reads are real."""
    ran: list[dict] = []
    argv = [
        "--experiment-id", "exp",
        *(t for b in kwargs.get("bindings", ("before=v5.0.3", "after=v5.1.0"))
          for t in ("--binding", b)),
        "--date-range", "2020-01-01,2020-01-02", "--dest-dataset", "ds",
        "--pipeline-dir", checkout, "--allow-same-gpsdio-pin",
        # v5.0.3 gets its own image, as the version check requires.
        "--binding-image", f"before={V503_IMAGE}", *extra,
    ]
    with (
        patch.object(mod, "_verify_distinct_gpsdio_pins"),
        patch.object(mod, "_preflight_images"),
        patch.object(mod.dit_bq, "pin_source_into_experiment",
                     side_effect=lambda source, **kw: f"{_GE}.copy_of_{source.split('.')[-1]}"
                     ) as pin,
        patch.object(mod, "_run_binding", side_effect=lambda **kw: ran.append(kw) or 0),
        patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare,
    ):
        try:
            rc: Any = mod.main(argv)
        except SystemExit as exc:
            rc = exc
    return dict(rc=rc, pin=pin, ran=ran, compare=compare)


def test_main_checks_every_binding_before_it_pins_anything(checkout) -> None:
    refused = _main(checkout, "--binding-adjust", "after=segment:add:--no_such_flag")

    assert isinstance(refused["rc"], SystemExit)
    assert "step segment passes --no_such_flag" in str(refused["rc"])
    refused["pin"].assert_not_called()
    assert refused["ran"] == []


@pytest.mark.parametrize("extra, message", [
    (("--binding-adjust", "before=segment:remove:--incremental"),
     "binding before: step segment: cannot remove --incremental"),
    (("--binding-adjust", "after=segment:add:--bins_per_day={out.typo}"),
     r"binding after: \{out.typo\} names no output and no input of this binding"),
])
def test_main_refuses_a_stale_edit_and_a_bad_placeholder_before_pinning(
    checkout, extra, message,
) -> None:
    refused = _main(checkout, *extra)

    assert isinstance(refused["rc"], SystemExit)
    assert re.search(message, str(refused["rc"]))
    assert "Nothing was pinned or run" in str(refused["rc"])
    refused["pin"].assert_not_called()


def test_main_hands_each_binding_what_the_preflight_found(checkout) -> None:
    result = _main(checkout)

    assert result["rc"] == 0
    result["pin"].assert_called_once()                    # one input, pinned once
    before, after = result["ran"]
    copy = f"{_GE}.copy_of_normalized_messages"
    for run in (before, after):
        assert run["pinned"] == mod._PinnedSources(normalized_messages=copy)
        assert run["pinned_inputs"] == {"normalized_messages": copy}
    # The binding info read from each ref's source, not the offline one.
    assert before["binding"].declares("segment", "--incremental") is False
    assert after["binding"].declares("segment", "--incremental") is True
    assert (before["binding"].version, after["binding"].version) == ((5, 0, 3), (5, 1, 0))
    # Eight tables compared between the two.
    assert result["compare"].call_count == 8


def test_a_dry_run_still_runs_the_preflight(checkout) -> None:
    refused = _main(checkout, "--dry-run", "--binding-adjust", "after=segment:add:--nope")
    passed = _main(checkout, "--dry-run")

    assert isinstance(refused["rc"], SystemExit)
    assert passed["rc"] == 0 and passed["pin"].call_args.kwargs["dry_run"] is True
    passed["compare"].assert_not_called()


# --------------------------------------------------------------------------
# The run log
# --------------------------------------------------------------------------

def _logged(caplog, sides) -> list[str]:
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=mod.logger.name):
        mod._log_sides(sides)
    return [r.getMessage() for r in caplog.records]


def test_the_log_names_each_bindings_edits_with_their_reason(checkout, caplog) -> None:
    lines = _logged(caplog, _sides(
        checkout, "--binding-adjust", "after=segment:add:--bins_per_day=8"))

    assert "[before] ref v5.0.3, pipe-segment v5.0.3: 3 edit(s) to the base interface" in lines
    for flag in INCREMENTAL_FLAGS:
        assert (f"[before]   segment: remove {flag} "
                f"({adjustments.NO_INCREMENTAL_SEGMENTER.reason})") in lines
    assert "[after] ref v5.1.0, pipe-segment v5.1.0: 1 edit(s) to the base interface" in lines
    assert ("[after]   segment: add --bins_per_day=8 "
            "(ad hoc: after=segment:add:--bins_per_day=8)") in lines


def test_the_log_prints_the_flag_difference_between_the_sides(checkout, caplog) -> None:
    lines = _logged(caplog, _sides(checkout))

    start = lines.index("flags  segment: before against after")
    assert lines[start + 1:start + 4] == [
        f"flags    only after: {flag}" for flag in INCREMENTAL_FLAGS
    ]
    # The other steps differ in the bindings' own tables, labels and job
    # names only, which is not a difference of interface.
    assert [line for line in lines if line.startswith("flags  ") and "same command" in line] == [
        f"flags  {step}: same command and flags on before and after" for step in mod.STEPS[1:]
    ]
    assert not [line for line in lines if "lineage" in line or "not compared" in line]


def test_two_bindings_of_one_version_log_no_difference(checkout, caplog) -> None:
    lines = _logged(caplog, _sides(checkout, bindings=("a=v5.1.0", "b=v5.1.0")))

    assert "[a] ref v5.1.0, pipe-segment v5.1.0: the base interface, no adjustment" in lines
    assert [line for line in lines if line.startswith("flags  ")] == [
        f"flags  {step}: same command and flags on a and b" for step in mod.STEPS
    ]


def test_every_binding_own_flag_is_one_a_step_passes() -> None:
    """``BINDING_OWN_FLAGS`` is compared by presence only, so it must not grow
    beyond the flags whose values really are a binding's own."""
    args = _args(None)
    passed = {
        flag for argv in mod._step_argvs(
            args, binding_name="after", suffix="exp_after", worker_image="img",
            pinned=mod._PinnedSources(normalized_messages="p.d.normalized")).values()
        for flag in _flags(argv)
    }

    assert mod.BINDING_OWN_FLAGS <= passed
    assert "--in_normalized_messages_table" not in mod.BINDING_OWN_FLAGS
    assert "--date_range" not in mod.BINDING_OWN_FLAGS and "--runner" not in mod.BINDING_OWN_FLAGS


# --------------------------------------------------------------------------
# Tables: synthetic adjustments only (no pipe-segment version changes its tables)
# --------------------------------------------------------------------------

OTHER_SOURCE = adjust.Input(
    role="normalized_v2", source="world-fishing-827.other.normalized_v2",
    window=dit_bq.SourceWindow("timestamp", date(2020, 1, 1), date(2020, 1, 3)),
)


def _only(name: str):
    return lambda binding: binding.name == name


def _declared(*edits: adjust.Edit, binding: str = "after") -> tuple[adjust.Adjustment, ...]:
    return (
        adjustments.NO_INCREMENTAL_SEGMENTER,
        adjust.Adjustment(edits=edits, applies=_only(binding), reason="synthetic table change"),
    )


NEW_INPUT = _declared(
    adjust.replace_inputs(["normalized_messages"], OTHER_SOURCE),
    *adjust.on_step("segment", adjust.set(
        "--in_normalized_messages_table", "{in.normalized_v2}")),
)


def test_the_union_of_the_bindings_inputs_is_pinned_once_each(checkout) -> None:
    with patch.object(adjustments, "DECLARED", NEW_INPUT):
        result = _main(checkout)

    assert result["rc"] == 0
    assert [call.args[0] for call in result["pin"].call_args_list] == [
        mod.DEFAULT_SRC_NORMALIZED_TABLE, OTHER_SOURCE.source]
    # One pin stamp for both, and a role in the name that tells them apart.
    stamps = {call.kwargs["pinned_at"] for call in result["pin"].call_args_list}
    assert len(stamps) == 1
    assert [call.kwargs["role"] for call in result["pin"].call_args_list] == [
        "pipe_segment", "pipe_segment_normalized_v2"]
    before, after = result["ran"]
    assert before["pinned_inputs"] == {
        "normalized_messages": f"{_GE}.copy_of_normalized_messages"}
    assert after["pinned_inputs"] == {"normalized_v2": f"{_GE}.copy_of_normalized_v2"}
    # The binding the base input was taken from has no copy of it.
    assert after["pinned"] == mod._PinnedSources(normalized_messages="")


def test_each_side_reads_its_own_pinned_copy() -> None:
    args = _args(None)
    with patch.object(adjustments, "DECLARED", NEW_INPUT):
        after = _segment(args, "after", pinned_inputs={"normalized_v2": "p.ge.copy_v2"})
        before = _segment(args, "before")

    assert _value(after, "--in_normalized_messages_table") == "p.ge.copy_v2"
    assert _value(before, "--in_normalized_messages_table") == "p.d.normalized"


def test_the_log_says_that_lineage_is_the_operators_claim(checkout, caplog) -> None:
    with patch.object(adjustments, "DECLARED", NEW_INPUT):
        sides = _sides(checkout)
        with caplog.at_level(logging.INFO, logger=mod.logger.name):
            mod._log_sides(sides)

    (note,) = [r for r in caplog.records if r.getMessage().startswith("lineage: ")]
    assert note.levelno == logging.WARNING
    assert f"before reads normalized_messages={mod.DEFAULT_SRC_NORMALIZED_TABLE}" in (
        note.getMessage())
    assert f"after reads normalized_v2={OTHER_SOURCE.source}" in note.getMessage()
    assert "the operator's claim" in note.getMessage()


def test_two_inputs_under_one_pinned_name_are_refused() -> None:
    """The copy's name holds the role and the table name, not the dataset."""
    args = _args(None)
    same_name = adjust.Input("normalized_messages", "world-fishing-827.other.normalized_messages",
                             mod._base_input(args).window)
    with (
        patch.object(mod.dit_bq, "pin_source_into_experiment") as pin,
        pytest.raises(SystemExit, match="two inputs would be pinned under one name"),
    ):
        mod._pin_union(args, (mod._base_input(args), same_name))

    pin.assert_not_called()


def test_an_output_only_one_binding_writes_is_listed_as_not_compared(checkout, capsys) -> None:
    declared = _declared(adjust.remove_output("fragments"))
    with patch.object(adjustments, "DECLARED", declared):
        result = _main(checkout)

    assert result["rc"] == 0
    assert result["compare"].call_count == 7
    assert not [c for c in result["compare"].call_args_list if "fragments_exp" in c.args[0]]
    assert ("  fragments  before vs after  -> NOT COMPARED (only before writes it)"
            in capsys.readouterr().err)


def test_a_renamed_output_is_compared_under_its_new_name(checkout) -> None:
    declared = (
        *_declared(adjust.rename_output("segments", "segs")),
        adjust.Adjustment(edits=(adjust.rename_output("segments", "segs"),),
                          applies=_only("before"), reason="synthetic"),
    )
    args = _args(checkout)
    with (
        patch.object(adjustments, "DECLARED", declared),
        patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare,
    ):
        results = mod._run_diffs(
            args=args, suffix_by_binding=_suffixes(args), failed_bindings=set())

    assert ("segs", "before", "after") in results and len(results) == 8
    (call,) = [c for c in compare.call_args_list if "segments_exp" in c.args[0]]
    assert call.args == (f"{OUT}.segments_exp_before", f"{OUT}.segments_exp_after")
    assert call.kwargs["keys"] == ["seg_id", "frag_id"]


def test_bindings_compared_on_different_keys_are_refused_before_pinning(checkout) -> None:
    declared = _declared(adjust.set_keys("segments", ["seg_id"]))
    with patch.object(adjustments, "DECLARED", declared):
        refused = _main(checkout)

    assert isinstance(refused["rc"], SystemExit)
    message = str(refused["rc"])
    assert "bindings before and after: compared outputs must have the same keys" in message
    assert "segments: seg_id, frag_id against seg_id" in message
    assert "Nothing was pinned or run" in message
    refused["pin"].assert_not_called()


def test_a_failed_binding_skips_its_comparisons() -> None:
    args = _args(None)
    with patch.object(mod.dit_compare, "compare_tables", return_value=0) as compare:
        results = mod._run_diffs(
            args=args, suffix_by_binding=_suffixes(args), failed_bindings={"after"})

    compare.assert_not_called()
    assert set(results.values()) == {mod._SKIPPED} and len(results) == 8
    assert mod._not_compared({
        name: side.interface for name, side in mod._sides(args, _suffixes(args)).items()
    }) == []


# --------------------------------------------------------------------------
# Opt-in: a real pipe-segment checkout
# --------------------------------------------------------------------------

def _real_checkout() -> str | None:
    repo = mod.DEFAULT_PIPELINE_DIR
    tags = ("v5.0.3", "v5.1.0")
    found = all(
        git_info.first_file(repo, tag, adjustments.command_files("segment")) is not None
        for tag in tags
    )
    return repo if found else None


@pytest.mark.skipif(_real_checkout() is None,
                    reason="no pipe-segment checkout with the tags v5.0.3 and v5.1.0")
@pytest.mark.parametrize("runner", ["dataflow", "DirectRunner"])
def test_the_preflight_passes_on_the_real_tags(runner) -> None:
    """Reads the real command modules at the two tags with ``git show``. A
    static read of source: what the CLI does with these flags is not run."""
    sides = _sides(_real_checkout(), "--build-from-source", "--ssvid-filter-query", '"1"',
                   runner=runner)

    mod._check_flags(sides)
    assert (sides["before"].binding.version, sides["after"].binding.version) == (
        (5, 0, 3), (5, 1, 0))
    assert [edit.description for a in sides["before"].applied for edit in a.edits] == [
        f"remove {flag}" for flag in INCREMENTAL_FLAGS]
    assert sides["after"].applied == ()
