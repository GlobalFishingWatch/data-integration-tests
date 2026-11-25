"""Tests for ``dit.adjust``: per-side adjustments of a pipeline's interface.

Everything here is pure and synthetic: the interfaces are made up, and no
pipeline, git, BigQuery or docker call is made. The table edits, the union of
inputs and the pairing of outputs have no real case yet, so these synthetic
interfaces are the only thing that exercises them.
"""

from __future__ import annotations

import dataclasses
from datetime import date

import pytest

from dit import adjust, composer
from dit.adjust import AdjustmentError, BindingInfo, Input, Interface, Output

# A step in both spellings a workflow may emit: ``--name=value`` and
# ``--name value``, a bare flag, a repeated flag, and a flag whose value
# holds ``=``.
SEGMENT = (
    "segment",
    "--date_range=2020-01-01,2020-01-02",
    "--in_messages", "p.d.messages",
    "--out_segments=p.d.segments_x",
    "--incremental",
    "--labels=step=segment",
    "--labels=binding=x",
    "--wait_for_job",
    "--runner=dataflow",
)
IDENTITY = ("segment_identity", "--source_segments=p.d.segments_x", "--project=p")

MESSAGES = Input("messages", "p.src.messages", ("timestamp", date(2020, 1, 1), date(2020, 1, 3)))
SEGMENTS = Output("segments", "p.d.segments_x", ("seg_id", "frag_id"), "one row per segment-day")
IDENTITIES = Output("identities", "p.d.identities_x", ("seg_id",))

BASE = Interface(
    steps={"segment": SEGMENT, "segment_identity": IDENTITY},
    inputs=(MESSAGES,),
    outputs=(SEGMENTS, IDENTITIES),
)

ANY = BindingInfo("any")


def _always(_: BindingInfo) -> bool:
    return True


def _never(_: BindingInfo) -> bool:
    return False


def _adjustment(*edits: adjust.Edit, applies=_always, reason: str = "test") -> adjust.Adjustment:
    return adjust.Adjustment(edits=edits, applies=applies, reason=reason)


def _segment(*edits: adjust.FlagEdit, base: Interface = BASE) -> tuple[str, ...]:
    """The segment step's tokens after ``edits``."""
    adjusted = adjust.apply(base, ANY, declared=[_adjustment(*adjust.on_step("segment", *edits))])
    return adjusted.interface.steps["segment"]


# --------------------------------------------------------------------------
# The interface
# --------------------------------------------------------------------------

def test_an_interface_is_frozen_and_does_not_share_the_callers_containers() -> None:
    steps = {"segment": list(SEGMENT)}
    interface = Interface(steps=steps, inputs=[MESSAGES], outputs=[SEGMENTS])

    steps["segment"].append("--late")
    steps["other"] = ["x"]
    assert dict(interface.steps) == {"segment": SEGMENT}
    assert interface.inputs == (MESSAGES,) and interface.outputs == (SEGMENTS,)
    with pytest.raises(dataclasses.FrozenInstanceError):
        interface.inputs = ()  # type: ignore[misc]
    with pytest.raises(TypeError):
        interface.steps["segment"] = ()  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        MESSAGES.source = "other"  # type: ignore[misc]


def test_steps_keep_their_run_order() -> None:
    assert list(BASE.steps) == ["segment", "segment_identity"]
    assert list(BASE.with_step("segment", ("segment",)).steps) == ["segment", "segment_identity"]


@pytest.mark.parametrize("kwargs, message", [
    (dict(inputs=(MESSAGES, MESSAGES)), "input role given more than once: messages"),
    (dict(outputs=(SEGMENTS, dataclasses.replace(IDENTITIES, name="segments"))),
     "output name given more than once: segments"),
])
def test_an_interface_refuses_a_repeated_role_or_output_name(kwargs, message) -> None:
    with pytest.raises(AdjustmentError, match=message):
        Interface(steps={}, **kwargs)


def test_lookups_name_what_exists_when_they_fail() -> None:
    assert BASE.input("messages") is MESSAGES
    assert BASE.output("segments") is SEGMENTS
    with pytest.raises(AdjustmentError, match="no step 'nope'; the steps are segment, segment_"):
        BASE.step("nope")
    with pytest.raises(AdjustmentError, match="no input 'nope'; the inputs are messages"):
        BASE.input("nope")
    with pytest.raises(AdjustmentError, match="no output 'nope'; the outputs are segments, ident"):
        BASE.output("nope")


# --------------------------------------------------------------------------
# No adjustment: the tokens come back as given
# --------------------------------------------------------------------------

def test_with_no_adjustment_the_interface_is_the_base_itself() -> None:
    assert adjust.apply(BASE, ANY).interface is BASE
    assert adjust.apply(BASE, ANY).applied == ()
    assert adjust.describe(adjust.apply(BASE, ANY)) == []


def test_an_adjustment_that_does_not_apply_leaves_every_token_as_given() -> None:
    declared = [_adjustment(*adjust.on_step("segment", adjust.remove("--incremental")),
                            applies=_never)]
    adjusted = adjust.apply(BASE, ANY, declared=declared)

    assert adjusted.interface is BASE
    assert adjusted.interface.steps["segment"] == SEGMENT
    # The same token objects, in the same order: nothing was parsed and rebuilt.
    assert all(a is b for a, b in zip(adjusted.interface.steps["segment"], SEGMENT, strict=True))


def test_filling_tokens_without_placeholders_changes_nothing() -> None:
    filled = adjust.fill(BASE, adjust.placeholder_values(BASE, {"messages": "p.d.pinned"}))

    assert dict(filled.steps) == dict(BASE.steps)
    assert all(a is b for a, b in zip(filled.steps["segment"], SEGMENT, strict=True))


def test_an_edit_touches_only_the_tokens_of_its_flag_and_step() -> None:
    adjusted = adjust.apply(BASE, ANY, declared=[
        _adjustment(*adjust.on_step("segment", adjust.remove("--incremental"))),
    ]).interface

    assert adjusted.steps["segment"] == tuple(t for t in SEGMENT if t != "--incremental")
    assert adjusted.steps["segment_identity"] is BASE.steps["segment_identity"]
    assert adjusted.inputs == BASE.inputs and adjusted.outputs == BASE.outputs
    # The base is not changed.
    assert BASE.steps["segment"] == SEGMENT


# --------------------------------------------------------------------------
# Flag edits
# --------------------------------------------------------------------------

def test_remove_drops_the_flag_in_either_spelling_with_its_value() -> None:
    assert _segment(adjust.remove("--incremental")) == tuple(
        t for t in SEGMENT if t != "--incremental")
    assert _segment(adjust.remove("--out_segments")) == tuple(
        t for t in SEGMENT if not t.startswith("--out_segments"))
    # ``--name value``: both tokens go.
    assert _segment(adjust.remove("--in_messages")) == tuple(
        t for t in SEGMENT if t not in ("--in_messages", "p.d.messages"))


def test_remove_drops_every_mention_of_a_repeated_flag() -> None:
    assert _segment(adjust.remove("--labels")) == tuple(
        t for t in SEGMENT if not t.startswith("--labels"))


def test_remove_takes_a_multi_value_flag_with_all_its_values() -> None:
    base = Interface(
        steps={"segment": ("segment", "--labels", "a=1", "b=2", "--runner=dataflow")},
        multi=frozenset({"--labels"}),
    )

    assert _segment(adjust.remove("--labels"), base=base) == ("segment", "--runner=dataflow")
    # Without the hint the values read as command words and would be left behind.
    unhinted = dataclasses.replace(base, multi=frozenset())
    assert _segment(adjust.remove("--labels"), base=unhinted) == (
        "segment", "b=2", "--runner=dataflow")


def test_a_bare_flag_followed_by_a_word_needs_the_bare_hint() -> None:
    tokens = ("--verbose", "segment", "--runner=dataflow")
    hinted = Interface(steps={"segment": tokens}, bare=frozenset({"--verbose"}))

    assert _segment(adjust.remove("--verbose"), base=hinted) == ("segment", "--runner=dataflow")


def test_add_appends_the_flag_with_or_without_a_value() -> None:
    assert _segment(adjust.add("--bins_per_day", "8")) == (*SEGMENT, "--bins_per_day=8")
    assert _segment(adjust.add("--dry")) == (*SEGMENT, "--dry")
    # A value may hold ``=`` and ``:``.
    assert _segment(adjust.add("--query", "a=b:c"))[-1] == "--query=a=b:c"


def test_rename_keeps_place_value_and_spelling() -> None:
    renamed = _segment(adjust.rename("--out_segments", "--segments_table"))
    assert renamed == tuple(
        "--segments_table=p.d.segments_x" if t.startswith("--out_segments") else t
        for t in SEGMENT
    )
    spaced = _segment(adjust.rename("--in_messages", "--source"))
    assert spaced[2:4] == ("--source", "p.d.messages")
    assert len(spaced) == len(SEGMENT)
    # Every mention of a repeated flag.
    assert [t for t in _segment(adjust.rename("--labels", "--tags")) if "step=" in t or
            "binding=" in t] == ["--tags=step=segment", "--tags=binding=x"]
    assert _segment(adjust.rename("--incremental", "--inc"))[5] == "--inc"


def test_set_replaces_the_value_and_keeps_the_spelling() -> None:
    assert _segment(adjust.set("--runner", "DirectRunner"))[-1] == "--runner=DirectRunner"
    spaced = _segment(adjust.set("--in_messages", "p.d.other"))
    assert spaced[2:4] == ("--in_messages", "p.d.other") and len(spaced) == len(SEGMENT)
    # A bare flag gets its value in the ``=`` form.
    assert _segment(adjust.set("--incremental", "true"))[5] == "--incremental=true"


def test_command_replaces_the_command_words_where_the_first_stood() -> None:
    assert _segment(adjust.command("segment-v2")) == ("segment-v2", *SEGMENT[1:])
    assert _segment(adjust.command("run", "segment")) == ("run", "segment", *SEGMENT[1:])
    two_words = Interface(steps={"segment": ("pipe", "segment", "--runner=dataflow")})
    assert _segment(adjust.command("segment"), base=two_words) == ("segment", "--runner=dataflow")
    no_words = Interface(steps={"segment": ("--runner=dataflow",)})
    assert _segment(adjust.command("segment"), base=no_words) == ("segment", "--runner=dataflow")


@pytest.mark.parametrize("edit, message", [
    (adjust.remove("--nope"), "--nope is not passed, so there is nothing to change"),
    (adjust.rename("--nope", "--other"), "--nope is not passed"),
    (adjust.set("--nope", "1"), "--nope is not passed"),
    (adjust.add("--incremental"), "--incremental is already passed; use set"),
    (adjust.add("--runner", "x"), "--runner is already passed"),
    (adjust.rename("--runner", "--incremental"), "--incremental is already passed, so --runner"),
    (adjust.set("--runner", "dataflow"), "--runner already has the value 'dataflow'"),
    (adjust.set("--labels", "x"), "--labels is passed 2 times"),
    (adjust.command("segment"), "the command already is segment"),
])
def test_an_edit_that_finds_nothing_to_change_is_an_error(edit, message) -> None:
    """A stale rule must be loud, not a silent no-op."""
    with pytest.raises(AdjustmentError, match=message):
        _segment(edit)


def test_an_edit_on_a_step_the_interface_lacks_is_an_error() -> None:
    declared = [_adjustment(*adjust.on_step("gone", adjust.remove("--x")))]

    with pytest.raises(AdjustmentError, match="step gone: cannot remove --x: no step 'gone'"):
        adjust.apply(BASE, ANY, declared=declared)


def test_the_error_names_the_step_the_edit_and_the_adjustments_reason() -> None:
    declared = [_adjustment(*adjust.on_step("segment", adjust.remove("--nope")),
                            reason="below 9.9.9 there is no --nope")]

    with pytest.raises(AdjustmentError) as stale:
        adjust.apply(BASE, ANY, declared=declared)

    assert str(stale.value) == (
        "step segment: cannot remove --nope: --nope is not passed, so there is nothing to "
        "change. Adjustment: below 9.9.9 there is no --nope"
    )


@pytest.mark.parametrize("build", [
    lambda: adjust.remove("incremental"),
    lambda: adjust.add("--flag=value"),
    lambda: adjust.rename("--old", "new"),
    lambda: adjust.set("-", "1"),
    lambda: adjust.rename("--same", "--same"),
    lambda: adjust.command(),
])
def test_a_malformed_edit_is_refused_when_it_is_built(build) -> None:
    with pytest.raises(AdjustmentError):
        build()


def test_the_edit_named_set_does_not_break_the_module() -> None:
    """``adjust.set`` shadows the builtin inside the module. The functions that
    need a set must still work after it is defined."""
    with pytest.raises(AdjustmentError, match="more than once"):
        Interface(steps={}, inputs=(MESSAGES, MESSAGES))
    assert adjust.lineage_note({"a": BASE, "b": BASE}) is None
    assert adjust.declared_flags('add("--x")') == frozenset({"--x"})


# --------------------------------------------------------------------------
# Table edits (synthetic interfaces only: no real case yet)
# --------------------------------------------------------------------------

POSITIONS = Input("positions", "p.src.positions")
MERGED = Input("merged", "p.src.merged")
TWO_INPUTS = dataclasses.replace(BASE, inputs=(MESSAGES, POSITIONS))


def _edited(*edits: adjust.Edit, base: Interface = BASE) -> Interface:
    return adjust.apply(base, ANY, declared=[_adjustment(*edits)]).interface


def test_add_and_remove_an_input() -> None:
    assert _edited(adjust.add_input(POSITIONS)).inputs == (MESSAGES, POSITIONS)
    assert _edited(adjust.remove_input("messages")).inputs == ()
    assert _edited(adjust.remove_input("messages"), base=TWO_INPUTS).inputs == (POSITIONS,)


def test_two_inputs_become_one() -> None:
    merged = _edited(adjust.replace_inputs(["messages", "positions"], MERGED), base=TWO_INPUTS)

    assert merged.inputs == (MERGED,)
    # The new input takes the place of the first it replaces.
    third = Input("third", "p.src.third")
    wide = dataclasses.replace(BASE, inputs=(third, MESSAGES, POSITIONS))
    assert _edited(adjust.replace_inputs(["messages", "positions"], MERGED), base=wide).inputs == (
        third, MERGED)
    # One input may be replaced by another of the same role: a new source.
    moved = Input("messages", "p.other.messages")
    assert _edited(adjust.replace_inputs(["messages"], moved)).inputs == (moved,)


def test_output_edits() -> None:
    extra = Output("fragments", "p.d.fragments_x", ("frag_id",))

    assert _edited(adjust.add_output(extra)).outputs == (SEGMENTS, IDENTITIES, extra)
    assert _edited(adjust.remove_output("segments")).outputs == (IDENTITIES,)
    renamed = _edited(adjust.rename_output("segments", "segs")).outputs
    assert renamed == (dataclasses.replace(SEGMENTS, name="segs"), IDENTITIES)
    # The table stays the binding's own; only the name sides are paired by changes.
    assert renamed[0].table == SEGMENTS.table
    rekeyed = _edited(adjust.set_keys("identities", ["seg_id", "day"])).outputs
    assert rekeyed == (SEGMENTS, dataclasses.replace(IDENTITIES, keys=("seg_id", "day")))


@pytest.mark.parametrize("edit, message", [
    (adjust.add_input(MESSAGES), "input 'messages' is already read"),
    (adjust.remove_input("positions"), "no input 'positions'"),
    (adjust.replace_inputs(["messages", "positions"], MERGED), "no input 'positions'"),
    (adjust.add_output(SEGMENTS), "output 'segments' is already written"),
    (adjust.remove_output("fragments"), "no output 'fragments'"),
    (adjust.rename_output("fragments", "frags"), "no output 'fragments'"),
    (adjust.rename_output("segments", "identities"), "output 'identities' already exists"),
    (adjust.set_keys("fragments", ["frag_id"]), "no output 'fragments'"),
    (adjust.set_keys("segments", ["seg_id", "frag_id"]), "already has the keys seg_id, frag_id"),
])
def test_a_table_edit_that_finds_nothing_to_change_is_an_error(edit, message) -> None:
    with pytest.raises(AdjustmentError, match=message):
        _edited(edit)


def test_replacing_inputs_by_a_role_another_input_holds_is_an_error() -> None:
    with pytest.raises(AdjustmentError, match="input 'positions' is already read"):
        _edited(adjust.replace_inputs(["messages"], POSITIONS), base=TWO_INPUTS)


def test_malformed_table_edits_are_refused_when_built() -> None:
    with pytest.raises(AdjustmentError, match="at least one input"):
        adjust.replace_inputs([], MERGED)
    with pytest.raises(AdjustmentError, match="at least one compare key"):
        adjust.set_keys("segments", [])


def test_table_edits_do_not_touch_the_tokens() -> None:
    edited = _edited(adjust.add_input(POSITIONS), adjust.rename_output("segments", "segs"))

    assert edited.steps["segment"] is BASE.steps["segment"]
    assert edited.steps["segment_identity"] is BASE.steps["segment_identity"]


# --------------------------------------------------------------------------
# Layers
# --------------------------------------------------------------------------

OLD = BindingInfo("before", ref="v1.0.0", version=(1, 0, 0))
NEW = BindingInfo("after", ref="v2.0.0", version=(2, 0, 0))

BELOW_2 = adjust.Adjustment(
    edits=adjust.on_step("segment", adjust.remove("--incremental")),
    applies=lambda b: b.version is not None and b.version < (2, 0, 0),
    reason="below 2.0.0 there is no --incremental",
)


def test_a_declared_adjustment_applies_to_the_bindings_it_is_for() -> None:
    old = adjust.apply(BASE, OLD, declared=[BELOW_2])
    new = adjust.apply(BASE, NEW, declared=[BELOW_2])

    assert "--incremental" not in old.interface.steps["segment"]
    assert old.applied == (BELOW_2,) and old.binding is OLD
    assert new.interface is BASE and new.applied == ()


def test_an_adjustment_is_itself_a_function_of_the_interface() -> None:
    assert BELOW_2(BASE).steps["segment"] == tuple(t for t in SEGMENT if t != "--incremental")
    assert BELOW_2.edits[0](BASE).steps == BELOW_2(BASE).steps


def test_layers_apply_in_order_and_a_later_layer_wins() -> None:
    """Base, then declared, then ad hoc; each works on the result before it."""
    declared = [
        _adjustment(*adjust.on_step("segment", adjust.add("--bins_per_day", "4")), reason="one"),
        _adjustment(*adjust.on_step("segment", adjust.set("--bins_per_day", "8")), reason="two"),
    ]
    _, ad_hoc = adjust.parse_ad_hoc("any=segment:set:--bins_per_day=16")

    assert adjust.apply(BASE, ANY, declared=declared[:1]).interface.steps["segment"][-1] == (
        "--bins_per_day=4")
    assert adjust.apply(BASE, ANY, declared=declared).interface.steps["segment"][-1] == (
        "--bins_per_day=8")
    final = adjust.apply(BASE, ANY, declared=declared, ad_hoc=[ad_hoc])
    assert final.interface.steps["segment"][-1] == "--bins_per_day=16"
    assert [a.reason for a in final.applied] == [
        "one", "two", "ad hoc: any=segment:set:--bins_per_day=16"]


def test_an_ad_hoc_edit_can_undo_a_declared_one() -> None:
    _, back = adjust.parse_ad_hoc("before=segment:add:--incremental")
    adjusted = adjust.apply(BASE, OLD, declared=[BELOW_2], ad_hoc=[back])

    assert adjusted.interface.steps["segment"] == (
        *(t for t in SEGMENT if t != "--incremental"), "--incremental")
    # Declared before ad hoc, whatever the order of the arguments' edits.
    assert adjusted.applied == (BELOW_2, back)


def test_an_ad_hoc_edit_that_a_declared_one_made_pointless_is_an_error() -> None:
    _, again = adjust.parse_ad_hoc("before=segment:remove:--incremental")

    with pytest.raises(AdjustmentError, match="--incremental is not passed"):
        adjust.apply(BASE, OLD, declared=[BELOW_2], ad_hoc=[again])


def test_describe_lists_every_edit_with_its_step_and_reason() -> None:
    both = adjust.Adjustment(
        edits=(*adjust.on_step("segment", adjust.remove("--incremental"),
                               adjust.add("--bins_per_day", "8")),
               adjust.add_input(POSITIONS)),
        applies=_always, reason="why",
    )

    assert adjust.describe(adjust.apply(BASE, ANY, declared=[both])) == [
        "segment: remove --incremental (why)",
        "segment: add --bins_per_day=8 (why)",
        "add input positions (p.src.positions) (why)",
    ]


def test_an_adjustment_needs_at_least_one_edit() -> None:
    with pytest.raises(AdjustmentError, match="has no edits"):
        adjust.Adjustment(edits=(), applies=_always, reason="empty")


# --------------------------------------------------------------------------
# What a rule may ask about a binding
# --------------------------------------------------------------------------

def test_a_binding_says_whether_its_cli_source_declares_a_flag() -> None:
    read = BindingInfo("b", declared={"segment": frozenset({"--incremental"})})

    assert read.declares("segment", "--incremental") is True
    assert read.declares("segment", "--other") is False
    # Not read: unknown, which is not "no".
    assert read.declares("segment_info", "--incremental") is None
    assert BindingInfo("b").declares("segment", "--incremental") is None


# --------------------------------------------------------------------------
# The ad hoc form
# --------------------------------------------------------------------------

def test_the_ad_hoc_form_applies_to_the_named_binding_only() -> None:
    name, adjustment = adjust.parse_ad_hoc("after=segment:remove:--incremental")

    assert name == "after"
    assert adjustment.applies(NEW) and not adjustment.applies(OLD)
    assert adjustment.reason == "ad hoc: after=segment:remove:--incremental"
    assert adjust.apply(BASE, OLD, ad_hoc=[adjustment]).interface is BASE
    assert "--incremental" not in adjust.apply(
        BASE, NEW, ad_hoc=[adjustment]).interface.steps["segment"]


@pytest.mark.parametrize("spec, tokens", [
    ("b=segment:remove:--incremental", tuple(t for t in SEGMENT if t != "--incremental")),
    ("b=segment:add:--dry", (*SEGMENT, "--dry")),
    ("b=segment:add:--bins_per_day=8", (*SEGMENT, "--bins_per_day=8")),
    # Only the first ``:`` and ``=`` separate: a value may hold both.
    ("b=segment:add:--table=proj:ds.t", (*SEGMENT, "--table=proj:ds.t")),
    ("b=segment:add:--query=a=b", (*SEGMENT, "--query=a=b")),
    ("b=segment:add:--in_segments={out.segments}", (*SEGMENT, "--in_segments={out.segments}")),
    ("b=segment:set:--runner=DirectRunner", (*SEGMENT[:-1], "--runner=DirectRunner")),
    ("b=segment:rename:--runner=--beam_runner", (*SEGMENT[:-1], "--beam_runner=dataflow")),
])
def test_each_ad_hoc_edit(spec: str, tokens: tuple[str, ...]) -> None:
    _, adjustment = adjust.parse_ad_hoc(spec)

    assert adjust.apply(BASE, BindingInfo("b"), ad_hoc=[adjustment]).interface.steps[
        "segment"] == tokens


@pytest.mark.parametrize("spec, message", [
    ("segment:remove:--x", "not a binding name, a step, an edit and a flag"),
    ("b=segment:remove", "not a binding name"),
    ("b=:remove:--x", "not a binding name"),
    ("=segment:remove:--x", "not a binding name"),
    ("b=segment:drop:--x", "unknown edit 'drop'; choose from remove, add, rename, set"),
    ("b=segment:command:other", "unknown edit 'command'"),
    ("b=segment:remove:--x=1", "remove takes a flag name and no value"),
    ("b=segment:set:--x", "set needs a value after ="),
    ("b=segment:rename:--x", "rename needs the new name after ="),
    ("b=segment:rename:--x=y", "'y' is not a flag name"),
    ("b=segment:remove:x", "'x' is not a flag name"),
])
def test_a_malformed_ad_hoc_edit_names_the_form(spec: str, message: str) -> None:
    with pytest.raises(AdjustmentError, match=message) as refused:
        adjust.parse_ad_hoc(spec)

    assert repr(spec) in str(refused.value)
    assert "NAME=<step>:<op>:<flag>[=<value>]" in str(refused.value)
    # A ValueError, so argparse-style callers can catch it as one.
    assert isinstance(refused.value, ValueError)


# --------------------------------------------------------------------------
# Placeholders
# --------------------------------------------------------------------------

def test_placeholders_name_the_sides_own_tables_and_pinned_inputs() -> None:
    edited = _segment(
        adjust.add("--in_segments", "{out.segments}"),
        adjust.set("--in_messages", "{in.messages}"),
    )
    interface = BASE.with_step("segment", edited)

    assert adjust.placeholders(interface) == ("in.messages", "out.segments")
    assert adjust.unknown_placeholders(interface) == ()
    values = adjust.placeholder_values(interface, {"messages": "p.ge.pinned_messages"})
    assert values == {
        "out.segments": "p.d.segments_x", "out.identities": "p.d.identities_x",
        "in.messages": "p.ge.pinned_messages",
    }
    filled = adjust.fill(interface, values).steps["segment"]
    assert filled[2:4] == ("--in_messages", "p.ge.pinned_messages")
    assert filled[-1] == "--in_segments=p.d.segments_x"


def test_a_renamed_output_is_named_by_its_new_name() -> None:
    renamed = _edited(adjust.rename_output("segments", "segs"))

    assert adjust.placeholder_values(renamed, {"messages": "x"})["out.segs"] == "p.d.segments_x"
    assert "out.segments" not in adjust.placeholder_values(renamed, {"messages": "x"})


def test_a_placeholder_that_names_nothing_is_found_before_and_refused_at_filling() -> None:
    interface = BASE.with_step("segment", (*SEGMENT, "--in_segments={out.typo}", "--x={in.gone}"))

    assert adjust.unknown_placeholders(interface) == ("out.typo", "in.gone")
    with pytest.raises(AdjustmentError, match=r"step segment: '--in_segments=\{out.typo\}' names "
                                              r"\{out.typo\}, which is not an output"):
        adjust.fill(interface, adjust.placeholder_values(interface, {"messages": "x"}))


def test_other_braces_are_left_alone() -> None:
    interface = BASE.with_step("segment", ("segment", '--labels={"a": 1}', "--x={other}"))

    assert adjust.placeholders(interface) == ()
    assert adjust.fill(interface, {}).steps["segment"] == interface.steps["segment"]


def test_placeholder_values_need_a_pinned_copy_of_every_input() -> None:
    with pytest.raises(AdjustmentError, match="no pinned copy for the input.s. positions"):
        adjust.placeholder_values(TWO_INPUTS, {"messages": "x"})


# --------------------------------------------------------------------------
# Inputs of several sides
# --------------------------------------------------------------------------

def test_the_union_of_inputs_holds_each_distinct_input_once() -> None:
    other_window = dataclasses.replace(MESSAGES, window=None)
    left = dataclasses.replace(BASE, inputs=(MESSAGES, POSITIONS))
    right = dataclasses.replace(BASE, inputs=(MERGED, MESSAGES))

    assert adjust.union_inputs([BASE, BASE]) == (MESSAGES,)
    assert adjust.union_inputs([left, right]) == (MESSAGES, POSITIONS, MERGED)
    # The same table over another window is another copy.
    assert adjust.union_inputs([BASE, dataclasses.replace(BASE, inputs=(other_window,))]) == (
        MESSAGES, other_window)
    assert adjust.union_inputs([]) == ()


def test_the_lineage_note_is_given_only_when_the_sides_inputs_differ() -> None:
    merged = _edited(adjust.replace_inputs(["messages", "positions"], MERGED), base=TWO_INPUTS)

    assert adjust.lineage_note({"before": TWO_INPUTS, "after": TWO_INPUTS}) is None
    assert adjust.lineage_note({"only": BASE}) is None
    # The order of a side's inputs does not make them differ.
    swapped = dataclasses.replace(TWO_INPUTS, inputs=(POSITIONS, MESSAGES))
    assert adjust.lineage_note({"before": TWO_INPUTS, "after": swapped}) is None

    note = adjust.lineage_note({"before": TWO_INPUTS, "after": merged})
    assert note is not None
    assert "before reads messages=p.src.messages, positions=p.src.positions" in note
    assert "after reads merged=p.src.merged" in note
    assert "the operator's claim" in note and "cannot show that they hold the same data" in note


# --------------------------------------------------------------------------
# Outputs of two sides
# --------------------------------------------------------------------------

def test_outputs_are_paired_by_name_and_one_sided_ones_are_reported() -> None:
    fragments = Output("fragments", "p.d.fragments_y", ("frag_id",))
    right = Interface(steps={}, outputs=(
        dataclasses.replace(IDENTITIES, table="p.d.identities_y"), fragments,
    ))

    pairing = adjust.pair_outputs(BASE, right)

    assert [(a.table, b.table) for a, b in pairing.compared] == [
        ("p.d.identities_x", "p.d.identities_y")]
    assert pairing.only_left == (SEGMENTS,)
    assert pairing.only_right == (fragments,)
    adjust.check_keys(pairing)


def test_a_rename_makes_two_differently_named_outputs_a_pair() -> None:
    right = Interface(steps={}, outputs=(
        Output("segs", "p.d.segs_y", ("seg_id", "frag_id")), IDENTITIES))

    assert adjust.pair_outputs(BASE, right).only_left == (SEGMENTS,)
    renamed = adjust.pair_outputs(_edited(adjust.rename_output("segments", "segs")), right)
    assert [(a.name, a.table, b.table) for a, b in renamed.compared] == [
        ("segs", "p.d.segments_x", "p.d.segs_y"),
        ("identities", "p.d.identities_x", "p.d.identities_x"),
    ]
    assert renamed.only_left == renamed.only_right == ()


def test_a_compared_pair_with_different_keys_is_refused() -> None:
    rekeyed = _edited(adjust.set_keys("segments", ["seg_id"]))

    with pytest.raises(AdjustmentError) as refused:
        adjust.check_keys(adjust.pair_outputs(BASE, rekeyed))

    message = str(refused.value)
    assert "segments: seg_id, frag_id against seg_id" in message
    assert "identities" not in message.split("Give both sides")[0].split("differ for")[1]
    # Key order is not a difference.
    reordered = _edited(adjust.set_keys("segments", ["frag_id", "seg_id"]))
    adjust.check_keys(adjust.pair_outputs(BASE, reordered))
    # An output taken out of the comparison on one side is not checked.
    adjust.check_keys(adjust.pair_outputs(BASE, _edited(adjust.remove_output("segments"))))


# --------------------------------------------------------------------------
# Flag diff between two sides
# --------------------------------------------------------------------------

def _other_binding(interface: Interface) -> Interface:
    """The same interface as another binding would get it: its own tables and labels."""
    return dataclasses.replace(interface, steps={
        step: tuple(t.replace("_x", "_y").replace("binding=x", "binding=y") for t in tokens)
        for step, tokens in interface.steps.items()
    })


OWN = {"--out_segments", "--source_segments", "--labels"}


def test_two_bindings_on_the_same_interface_differ_only_in_their_own_flags() -> None:
    diffs = adjust.diff_steps(BASE, _other_binding(BASE), presence_only=OWN)

    assert [d.step for d in diffs] == ["segment", "segment_identity"]
    assert all(d.identical for d in diffs)
    # Without the hint the binding's own values show as differences.
    by_value = adjust.diff_steps(BASE, _other_binding(BASE))
    assert by_value[0].flags.differing == ("--out_segments", "--labels")
    assert adjust.format_step_diff(diffs[0], "before", "after") == [
        "segment: same command and flags on before and after"]


def test_the_diff_shows_what_an_adjustment_changed() -> None:
    old = adjust.apply(BASE, OLD, declared=[BELOW_2]).interface
    (segment, identity) = adjust.diff_steps(old, _other_binding(BASE), presence_only=OWN)

    assert segment.flags.only_right == ("--incremental",) and segment.flags.only_left == ()
    assert identity.identical
    assert adjust.format_step_diff(segment, "before", "after") == [
        "segment: before against after", "  only after: --incremental"]


def test_a_flag_compared_by_presence_still_shows_when_one_side_lacks_it() -> None:
    without = BASE.with_step("segment", tuple(t for t in SEGMENT if "--labels" not in t))
    (segment, _) = adjust.diff_steps(without, BASE, presence_only=OWN)

    assert segment.flags.only_right == ("--labels",)
    assert adjust.format_step_diff(segment, "a", "b") == ["segment: a against b",
                                                           "  only b: --labels"]


def test_the_diff_reports_values_commands_and_one_sided_steps() -> None:
    right = Interface(steps={
        "segment": ("segment-v2", *SEGMENT[1:-1], "--runner=DirectRunner"),
        "extra": ("extra",),
    })
    segment, identity, extra = adjust.diff_steps(BASE, right)

    assert segment.command_differs and segment.flags.differing == ("--runner",)
    assert adjust.format_step_diff(segment, "a", "b") == [
        "segment: a against b",
        "  command: segment against segment-v2",
        "  values differ: a --runner=dataflow, b --runner=DirectRunner",
    ]
    assert identity.right is None and identity.flags is None and not identity.identical
    assert adjust.format_step_diff(identity, "a", "b") == [
        "segment_identity: only a runs this step"]
    assert adjust.format_step_diff(extra, "a", "b") == ["extra: only b runs this step"]


def test_the_diff_reads_each_side_with_its_own_hints() -> None:
    spaced = Interface(
        steps={"segment": ("segment", "--labels", "step=segment", "binding=x")},
        multi=frozenset({"--labels"}),
    )
    joined = Interface(
        steps={"segment": ("segment", "--labels=step=segment", "--labels=binding=x")})

    (diff,) = adjust.diff_steps(spaced, joined)
    assert diff.identical
    assert isinstance(diff.left, composer.ParsedArgv)


# --------------------------------------------------------------------------
# Preflight: flags against a CLI source
# --------------------------------------------------------------------------

CLI_SOURCE = '''
class Segment(Command):
    HELP = "segment pipeline. Pass --not_a_flag to nothing."

    def add_to_subparsers(cls, subparsers):
        add(
            "--date_range", metavar=' ', type=valid_daterange,
            help="Range of dates. See also --hidden_in_help.")
        add("--wait_for_job", action="store_true")
        add('--single_quoted', default=None)
        self.parser.add_argument('-v', '--verbose', action='store_true')
        add("--with-dash")
'''


def test_declared_flags_are_the_quoted_strings_that_are_one_flag_name() -> None:
    assert adjust.declared_flags(CLI_SOURCE) == frozenset({
        "--date_range", "--wait_for_job", "--single_quoted", "-v", "--verbose", "--with-dash",
    })
    assert adjust.declared_flags("") == frozenset()
    # Mismatched quotes, a value, or prose around the name: not a declaration.
    assert adjust.declared_flags("""add("--a') add("--b=1") add("see --c")""") == frozenset()


def test_undeclared_flags_are_those_neither_declared_nor_exempt() -> None:
    declared = adjust.declared_flags(CLI_SOURCE)
    tokens = ("segment", "--date_range=a,b", "--incremental", "--labels=x=1", "--labels=y=2",
              "--wait_for_job", "--runner=dataflow", "--in_segments_table", "p.d.t")

    assert adjust.undeclared_flags(tokens, declared) == (
        "--incremental", "--labels", "--runner", "--in_segments_table")
    assert adjust.undeclared_flags(tokens, declared, exempt={"--labels", "--runner"}) == (
        "--incremental", "--in_segments_table")
    assert adjust.undeclared_flags(("segment", "--date_range=a,b"), declared) == ()


def test_undeclared_flags_read_the_tokens_with_the_hints_given() -> None:
    tokens = ("segment", "--labels", "a=1", "b=2", "--date_range=a,b")

    assert adjust.undeclared_flags(tokens, {"--date_range"}, multi={"--labels"}) == ("--labels",)
    assert adjust.undeclared_flags(tokens, {"--date_range", "--labels"}) == ()
