"""Per-side adjustments of a pipeline's interface in a cross-version run.

A cross-version workflow runs the same pipeline at several refs (the
*bindings*, e.g. base and head) and compares their outputs. By default every
binding gets the same command line, the same inputs and the same outputs. That
only works while the refs share an interface. This module is how a workflow
states where they do not.

**Interface.** What faces the pipeline, for one binding (:class:`Interface`):

* ``steps``: per step, the command tokens the workflow hands to the runner;
* ``inputs``: the source tables the binding reads, pinned once per run;
* ``outputs``: the tables the binding writes, with their compare keys.

**Adjustment.** A pure function ``Interface -> Interface``, built from edits.
Flag edits change one step's tokens (:func:`remove`, :func:`add`,
:func:`rename`, :func:`set`, :func:`command`, placed on a step with
:func:`on_step`). Table edits change inputs and outputs (:func:`add_input`,
:func:`remove_input`, :func:`replace_inputs`, :func:`rename_output`,
:func:`add_output`, :func:`remove_output`, :func:`set_keys`).

**Layers** (:func:`apply`). The workflow's base interface comes first. It
mirrors what production runs today. Then come the declared adjustments whose
``applies`` holds for the binding (rules that hold for a range of pipeline
versions, kept next to the workflow), then the ad hoc ones of the run
(:func:`parse_ad_hoc`, the ``--binding-adjust`` form). Each layer works on the
result of the one before, so a later layer wins.

**Rules the edits follow.**

* With no adjustment the tokens come back as they were given: the same
  objects in the same order. An edit rewrites only the tokens of the flag it
  names and keeps the spelling it finds (``--name=value`` or ``--name value``).
  Nothing is parsed and written back as a whole.
* An edit that finds nothing to change raises :class:`AdjustmentError`: a
  remove, rename or set of a flag that is not there, an add of one that is, a
  value or command that is already what the edit asks for, a table edit that
  names an input or output the interface does not have. A rule that no longer
  matches the base is stale, and a stale rule must be loud.

**Placeholders.** A token may name one of the binding's own tables:
``{out.<name>}`` is the table of the output ``<name>``, ``{in.<role>}`` the
pinned copy of the input ``<role>``. :func:`fill` replaces them once the
inputs are pinned. An ad hoc edit needs them, since the operator cannot know a
binding's table names: ``after=segment:add:--in_segments_table={out.segments}``.

**What the helpers are for.**

* :func:`union_inputs`: every distinct input of all sides, to pin each once.
* :func:`lineage_note`: when the sides read different inputs, the sentence for
  the run log. DIT cannot show that two source tables hold the same data; that
  they do is the operator's claim.
* :func:`pair_outputs` and :func:`check_keys`: sides are compared by output
  name. An output on one side only is reported, never dropped silently, and a
  compared pair must have the same keys on both sides.
* :func:`diff_steps`: the flag differences between two sides, for the run log.
* :func:`declared_flags` and :func:`undeclared_flags`: the preflight. A
  pipeline that hands unknown flags to Beam ignores a wrong flag silently, so
  every pipeline flag of the adjusted argv is checked against the flags the
  ref's own CLI source declares.

**No real case for the table edits yet.** No pipeline DIT covers changes its
tables between two versions today. The table edits, the union of inputs and
the pairing of outputs are tested with synthetic interfaces only.

Pure: stdlib and :mod:`dit.composer`. No git, BigQuery or docker call; the
workflow reads the CLI source and pins the inputs.
"""
from __future__ import annotations

import dataclasses
import functools
import re
from collections import Counter
from collections.abc import Callable, Collection, Hashable, Iterable, Mapping, Sequence
from types import MappingProxyType

from dit import composer

# NOTE: this module defines an edit named ``set`` (the design's name for it),
# which shadows the builtin from its definition on. Nothing here may call the
# builtin ``set``; use a set display or ``frozenset``.

Tokens = tuple[str, ...]


class AdjustmentError(ValueError):
    """An adjustment that cannot be applied as written, or two sides that
    cannot be compared after it."""


# --------------------------------------------------------------------------
# Interface
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Input:
    """One source table a binding reads.

    ``role`` names the input within an interface and in ``{in.<role>}``.
    ``source`` is the table to pin and ``window`` what of it to copy (a
    ``dit.bq.SourceWindow``; ``None`` for the whole table). The window is
    opaque here and must be hashable.
    """

    role: str
    source: str
    window: Hashable = None


@dataclasses.dataclass(frozen=True)
class Output:
    """One table a binding writes.

    ``name`` is what sides are compared by, and the name in ``{out.<name>}``.
    ``table`` is this binding's own table. ``keys`` are the compare keys, and
    ``meaning`` says in a few words what a row is.
    """

    name: str
    table: str
    keys: tuple[str, ...]
    meaning: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "keys", tuple(self.keys))


def _repeated(names: Iterable[str]) -> list[str]:
    return sorted(name for name, count in Counter(names).items() if count > 1)


@dataclasses.dataclass(frozen=True)
class Interface:
    """What one binding's run hands to the pipeline and takes back from it.

    ``steps`` maps each step to its command tokens, in run order. ``bare`` and
    ``multi`` are the :func:`dit.composer.parse_argv` hints the edits and the
    diff read the tokens with: flags that take no value, and flags that take
    several.
    """

    steps: Mapping[str, Tokens]
    inputs: tuple[Input, ...] = ()
    outputs: tuple[Output, ...] = ()
    bare: frozenset[str] = frozenset()
    multi: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        frozen = {
            "steps": MappingProxyType({s: tuple(tokens) for s, tokens in self.steps.items()}),
            "inputs": tuple(self.inputs),
            "outputs": tuple(self.outputs),
            "bare": frozenset(self.bare),
            "multi": frozenset(self.multi),
        }
        for name, value in frozen.items():
            object.__setattr__(self, name, value)
        for kind, names in (
            ("input role", [i.role for i in self.inputs]),
            ("output name", [o.name for o in self.outputs]),
        ):
            if repeated := _repeated(names):
                raise AdjustmentError(f"{kind} given more than once: {', '.join(repeated)}")

    def step(self, name: str) -> Tokens:
        """The tokens of step ``name``; :class:`AdjustmentError` when absent."""
        if name not in self.steps:
            raise AdjustmentError(
                f"no step {name!r}; the steps are {', '.join(self.steps) or 'none'}"
            )
        return self.steps[name]

    def with_step(self, name: str, tokens: Sequence[str]) -> Interface:
        """This interface with the tokens of step ``name`` replaced."""
        self.step(name)
        return dataclasses.replace(self, steps={**self.steps, name: tuple(tokens)})

    def input(self, role: str) -> Input:
        found = next((i for i in self.inputs if i.role == role), None)
        if found is None:
            raise AdjustmentError(
                f"no input {role!r}; the inputs are "
                f"{', '.join(i.role for i in self.inputs) or 'none'}"
            )
        return found

    def output(self, name: str) -> Output:
        found = next((o for o in self.outputs if o.name == name), None)
        if found is None:
            raise AdjustmentError(
                f"no output {name!r}; the outputs are "
                f"{', '.join(o.name for o in self.outputs) or 'none'}"
            )
        return found


# --------------------------------------------------------------------------
# Edits
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Edit:
    """One change to an interface: a pure function and its description for the
    run log. ``step`` is the step a flag edit changes, ``None`` for a table
    edit."""

    description: str
    change: Callable[[Interface], Interface]
    step: str | None = None

    def __call__(self, interface: Interface) -> Interface:
        return self.change(interface)


#: What a flag edit does to one step's tokens, given the interface's hints.
TokenChange = Callable[[Tokens, frozenset[str], frozenset[str]], Tokens]


@dataclasses.dataclass(frozen=True)
class FlagEdit:
    """A change to one step's tokens, not yet placed on a step (:func:`on_step`)."""

    description: str
    change: TokenChange


def on_step(step: str, *edits: FlagEdit) -> tuple[Edit, ...]:
    """``edits`` as edits of step ``step``, in order."""
    def lift(edit: FlagEdit) -> Edit:
        def change(interface: Interface) -> Interface:
            return interface.with_step(
                step, edit.change(interface.step(step), interface.bare, interface.multi),
            )
        return Edit(edit.description, change, step=step)

    return tuple(map(lift, edits))


def _check_flag(flag: str) -> str:
    if not flag.startswith("-") or "=" in flag or len(flag) < 2:
        raise AdjustmentError(
            f"{flag!r} is not a flag name: write it as the pipeline takes it, with its "
            f"leading dashes and without a value"
        )
    return flag


def _mentions(
    tokens: Tokens, flag: str, bare: frozenset[str], multi: frozenset[str],
) -> tuple[composer.ArgvSpan, ...]:
    return tuple(
        span for span in composer.argv_spans(tokens, bare=bare, multi=multi)
        if span.name == flag
    )


def _require(mentions: tuple[composer.ArgvSpan, ...], flag: str) -> tuple[composer.ArgvSpan, ...]:
    if not mentions:
        raise AdjustmentError(f"{flag} is not passed, so there is nothing to change")
    return mentions


def _splice(tokens: Tokens, replacements: Mapping[int, tuple[int, Tokens]]) -> Tokens:
    """``tokens`` with each ``start -> (stop, new tokens)`` replacement made.
    Every token outside the replaced ranges is kept as it is."""
    pieces: list[str] = []
    i = 0
    while i < len(tokens):
        if i in replacements:
            stop, new = replacements[i]
            pieces.extend(new)
            i = stop
        else:
            pieces.append(tokens[i])
            i += 1
    return tuple(pieces)


def remove(flag: str) -> FlagEdit:
    """Drop every mention of ``flag``, with its values."""
    _check_flag(flag)

    def change(tokens: Tokens, bare: frozenset[str], multi: frozenset[str]) -> Tokens:
        mentions = _require(_mentions(tokens, flag, bare, multi), flag)
        return _splice(tokens, {span.start: (span.stop, ()) for span in mentions})

    return FlagEdit(f"remove {flag}", change)


def add(flag: str, value: str | None = None) -> FlagEdit:
    """Append ``flag``, as ``--flag=value`` or bare. The flag must not be
    passed yet: to change a value, use :func:`set`."""
    _check_flag(flag)
    token = flag if value is None else f"{flag}={value}"

    def change(tokens: Tokens, bare: frozenset[str], multi: frozenset[str]) -> Tokens:
        if _mentions(tokens, flag, bare, multi):
            raise AdjustmentError(
                f"{flag} is already passed; use set to change its value"
            )
        return (*tokens, token)

    return FlagEdit(f"add {token}", change)


def rename(old: str, new: str) -> FlagEdit:
    """Pass ``old``'s values under the name ``new``, in the same place and spelling."""
    _check_flag(old)
    _check_flag(new)
    if old == new:
        raise AdjustmentError(f"rename {old} to itself changes nothing")

    def change(tokens: Tokens, bare: frozenset[str], multi: frozenset[str]) -> Tokens:
        mentions = _require(_mentions(tokens, old, bare, multi), old)
        if _mentions(tokens, new, bare, multi):
            raise AdjustmentError(f"{new} is already passed, so {old} cannot take its name")
        return _splice(tokens, {
            span.start: (span.start + 1, (new + tokens[span.start][len(old):],))
            for span in mentions
        })

    return FlagEdit(f"rename {old} to {new}", change)


def set(flag: str, value: str) -> FlagEdit:  # noqa: A001 (the edit's name in the design)
    """Give ``flag`` the value ``value``. The flag must be passed once."""
    _check_flag(flag)

    def change(tokens: Tokens, bare: frozenset[str], multi: frozenset[str]) -> Tokens:
        mentions = _require(_mentions(tokens, flag, bare, multi), flag)
        if len(mentions) > 1:
            raise AdjustmentError(
                f"{flag} is passed {len(mentions)} times; set changes a flag passed once"
            )
        (span,) = mentions
        if span.values == (value,):
            raise AdjustmentError(f"{flag} already has the value {value!r}")
        spaced = tokens[span.start] == flag and span.stop - span.start > 1
        new = (flag, value) if spaced else (f"{flag}={value}",)
        return _splice(tokens, {span.start: (span.stop, new)})

    return FlagEdit(f"set {flag}={value}", change)


def command(*positionals: str) -> FlagEdit:
    """Replace the step's command words (the tokens that are not flags) by
    ``positionals``, where the first of them stood."""
    if not positionals:
        raise AdjustmentError("command needs at least one word")

    def change(tokens: Tokens, bare: frozenset[str], multi: frozenset[str]) -> Tokens:
        words = tuple(
            span for span in composer.argv_spans(tokens, bare=bare, multi=multi)
            if span.name is None
        )
        if tuple(tokens[span.start] for span in words) == positionals:
            raise AdjustmentError(f"the command already is {' '.join(positionals)}")
        if not words:
            return (*positionals, *tokens)
        first = words[0].start
        return _splice(tokens, {
            span.start: (span.stop, positionals if span.start == first else ())
            for span in words
        })

    return FlagEdit(f"command {' '.join(positionals)}", change)


# -- Table edits -------------------------------------------------------------

def _roles(interface: Interface) -> list[str]:
    return [i.role for i in interface.inputs]


def _names(interface: Interface) -> list[str]:
    return [o.name for o in interface.outputs]


def add_input(new: Input) -> Edit:
    """Read one more source table."""
    def change(interface: Interface) -> Interface:
        if new.role in _roles(interface):
            raise AdjustmentError(f"input {new.role!r} is already read")
        return dataclasses.replace(interface, inputs=(*interface.inputs, new))

    return Edit(f"add input {new.role} ({new.source})", change)


def remove_input(role: str) -> Edit:
    """Stop reading the input ``role``."""
    def change(interface: Interface) -> Interface:
        interface.input(role)
        return dataclasses.replace(
            interface, inputs=tuple(i for i in interface.inputs if i.role != role),
        )

    return Edit(f"remove input {role}", change)


def replace_inputs(old_roles: Sequence[str], new: Input) -> Edit:
    """Read ``new`` in place of the inputs ``old_roles`` (two become one). The
    new input takes the place of the first one it replaces."""
    old = tuple(old_roles)
    if not old:
        raise AdjustmentError("replace_inputs needs at least one input to replace")

    def change(interface: Interface) -> Interface:
        for role in old:
            interface.input(role)
        if new.role in _roles(interface) and new.role not in old:
            raise AdjustmentError(f"input {new.role!r} is already read")
        return dataclasses.replace(interface, inputs=tuple(
            new if i.role == old[0] else i
            for i in interface.inputs if i.role == old[0] or i.role not in old
        ))

    return Edit(f"replace input {', '.join(old)} by {new.role} ({new.source})", change)


def add_output(new: Output) -> Edit:
    """Write one more table."""
    def change(interface: Interface) -> Interface:
        if new.name in _names(interface):
            raise AdjustmentError(f"output {new.name!r} is already written")
        return dataclasses.replace(interface, outputs=(*interface.outputs, new))

    return Edit(f"add output {new.name}", change)


def remove_output(name: str) -> Edit:
    """Stop writing, and so comparing, the output ``name``."""
    def change(interface: Interface) -> Interface:
        interface.output(name)
        return dataclasses.replace(
            interface, outputs=tuple(o for o in interface.outputs if o.name != name),
        )

    return Edit(f"remove output {name}", change)


def rename_output(old: str, new: str) -> Edit:
    """Compare the output ``old`` under the name ``new``. The table stays the
    binding's own; only the name sides are paired by changes."""
    def change(interface: Interface) -> Interface:
        interface.output(old)
        if old == new or new in _names(interface):
            raise AdjustmentError(f"output {new!r} already exists, so {old!r} cannot take its name")
        return dataclasses.replace(interface, outputs=tuple(
            dataclasses.replace(o, name=new) if o.name == old else o
            for o in interface.outputs
        ))

    return Edit(f"rename output {old} to {new}", change)


def set_keys(name: str, keys: Sequence[str]) -> Edit:
    """Compare the output ``name`` on ``keys``."""
    new_keys = tuple(keys)
    if not new_keys:
        raise AdjustmentError(f"output {name!r} needs at least one compare key")

    def change(interface: Interface) -> Interface:
        if interface.output(name).keys == new_keys:
            raise AdjustmentError(f"output {name!r} already has the keys {', '.join(new_keys)}")
        return dataclasses.replace(interface, outputs=tuple(
            dataclasses.replace(o, keys=new_keys) if o.name == name else o
            for o in interface.outputs
        ))

    return Edit(f"set keys of {name} to {', '.join(new_keys)}", change)


# --------------------------------------------------------------------------
# Adjustments and layers
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class BindingInfo:
    """What a rule may know about a binding to decide whether it applies.

    ``version`` is the pipeline version the binding's code carries, when
    known. ``declared`` maps each step to the flags the ref's own CLI source
    declares, when that source was read. An unreleased head still carries the
    version of the release before it, so a rule about a flag should ask
    :meth:`declares` first and fall back to the version.
    """

    name: str
    ref: str | None = None
    version: tuple[int, ...] | None = None
    declared: Mapping[str, frozenset[str]] | None = None

    def declares(self, step: str, flag: str) -> bool | None:
        """Whether the ref's CLI source declares ``flag`` for ``step``; ``None``
        when the source was not read for that step."""
        if self.declared is None or step not in self.declared:
            return None
        return flag in self.declared[step]


@dataclasses.dataclass(frozen=True)
class Adjustment:
    """Edits that hold together: for which bindings, and why.

    ``reason`` is printed with every edit in the run log, so it should say
    what differs in the pipeline, not what the edit does.
    """

    edits: tuple[Edit, ...]
    applies: Callable[[BindingInfo], bool]
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "edits", tuple(self.edits))
        if not self.edits:
            raise AdjustmentError(f"adjustment {self.reason!r} has no edits")

    def __call__(self, interface: Interface) -> Interface:
        def step(current: Interface, edit: Edit) -> Interface:
            try:
                return edit(current)
            except AdjustmentError as exc:
                where = f"step {edit.step}: " if edit.step else ""
                raise AdjustmentError(
                    f"{where}cannot {edit.description}: {exc}. Adjustment: {self.reason}"
                ) from exc

        return functools.reduce(step, self.edits, interface)


@dataclasses.dataclass(frozen=True)
class Adjusted:
    """A binding's interface after its layers, and the adjustments that made
    it, in the order they were applied."""

    binding: BindingInfo
    interface: Interface
    applied: tuple[Adjustment, ...] = ()


def apply(
    base: Interface,
    binding: BindingInfo,
    *,
    declared: Sequence[Adjustment] = (),
    ad_hoc: Sequence[Adjustment] = (),
) -> Adjusted:
    """``base`` with the layers of ``binding``: the ``declared`` adjustments
    that apply to it, then the ``ad_hoc`` ones that do, each in the order
    given. A later adjustment works on the result of the earlier ones.

    With nothing to apply, the result's interface is ``base`` itself.
    """
    applied = tuple(a for a in (*declared, *ad_hoc) if a.applies(binding))
    return Adjusted(
        binding=binding,
        interface=functools.reduce(lambda current, a: a(current), applied, base),
        applied=applied,
    )


def describe(adjusted: Adjusted) -> list[str]:
    """One line per edit applied to the binding, with its reason. Empty when
    the binding runs the base interface."""
    return [
        (f"{edit.step}: " if edit.step else "") + f"{edit.description} ({a.reason})"
        for a in adjusted.applied for edit in a.edits
    ]


# -- The ad hoc form ---------------------------------------------------------

#: The edits ``--binding-adjust`` can ask for. Table edits and ``command`` are
#: declared in code.
AD_HOC_OPS = ("remove", "add", "rename", "set")

AD_HOC_FORM = "NAME=<step>:<op>:<flag>[=<value>]"


@dataclasses.dataclass(frozen=True)
class _Named:
    """``applies`` of an ad hoc adjustment: the binding of that name."""

    name: str

    def __call__(self, binding: BindingInfo) -> bool:
        return binding.name == self.name


def parse_ad_hoc(spec: str) -> tuple[str, Adjustment]:
    """One ``NAME=<step>:<op>:<flag>[=<value>]`` as ``(binding name, adjustment)``.

    * ``remove``: ``after=segment:remove:--incremental``
    * ``add``: ``after=segment:add:--bins_per_day=8``, or bare
      ``after=segment:add:--incremental``
    * ``rename``: ``after=segment:rename:--fragments_table=--out_fragments_table``
    * ``set``: ``after=segment:set:--bins_per_day=8``

    A value may hold ``:`` and ``=``; only the first of each separates.
    ``ValueError`` (an :class:`AdjustmentError`) for any other form.
    """
    def malformed(why: str) -> AdjustmentError:
        return AdjustmentError(f"{spec!r}: {why}. The form is {AD_HOC_FORM}")

    name, separator, rest = spec.partition("=")
    parts = rest.split(":", 2)
    if not (name and separator and len(parts) == 3 and all(parts)):
        raise malformed("not a binding name, a step, an edit and a flag")
    step, op, argument = parts
    if op not in AD_HOC_OPS:
        raise malformed(f"unknown edit {op!r}; choose from {', '.join(AD_HOC_OPS)}")
    flag, has_value, value = argument.partition("=")
    if op == "remove" and has_value:
        raise malformed("remove takes a flag name and no value")
    if op in ("rename", "set") and not value:
        raise malformed(f"{op} needs {'the new name' if op == 'rename' else 'a value'} after =")
    builders: Mapping[str, Callable[[], FlagEdit]] = {
        "remove": lambda: remove(flag),
        "add": lambda: add(flag, value if has_value else None),
        "rename": lambda: rename(flag, value),
        "set": lambda: set(flag, value),
    }
    try:
        edit = builders[op]()
    except AdjustmentError as exc:
        raise malformed(str(exc)) from exc
    return name, Adjustment(
        edits=on_step(step, edit), applies=_Named(name), reason=f"ad hoc: {spec}",
    )


# --------------------------------------------------------------------------
# Placeholders
# --------------------------------------------------------------------------

_PLACEHOLDER = re.compile(r"\{((?:in|out)\.[A-Za-z0-9_]+)\}")


def placeholders(interface: Interface) -> tuple[str, ...]:
    """The placeholder names the interface's tokens use, without repeats."""
    return tuple(dict.fromkeys(
        name
        for tokens in interface.steps.values() for token in tokens
        for name in _PLACEHOLDER.findall(token)
    ))


def unknown_placeholders(interface: Interface) -> tuple[str, ...]:
    """The placeholders that name no output and no input of the interface.
    Checked before anything is pinned, so a mistyped name stops the run early."""
    known = {f"out.{name}" for name in _names(interface)} | {
        f"in.{role}" for role in _roles(interface)
    }
    return tuple(name for name in placeholders(interface) if name not in known)


def placeholder_values(interface: Interface, pinned: Mapping[str, str]) -> dict[str, str]:
    """What each placeholder of the interface stands for: an output's own
    table, and for each input its pinned copy from ``pinned`` (role to table)."""
    missing = [role for role in _roles(interface) if role not in pinned]
    if missing:
        raise AdjustmentError(f"no pinned copy for the input(s) {', '.join(missing)}")
    return {
        **{f"out.{o.name}": o.table for o in interface.outputs},
        **{f"in.{i.role}": pinned[i.role] for i in interface.inputs},
    }


def fill(interface: Interface, values: Mapping[str, str]) -> Interface:
    """The interface with every ``{in.<role>}`` and ``{out.<name>}`` in its
    tokens replaced from ``values``. A token without a placeholder is kept as
    it is. :class:`AdjustmentError` for a placeholder ``values`` does not hold."""
    def fill_token(step: str, token: str) -> str:
        def value(match: re.Match[str]) -> str:
            if match.group(1) not in values:
                raise AdjustmentError(
                    f"step {step}: {token!r} names {{{match.group(1)}}}, which is not an "
                    f"output or a pinned input of this side "
                    f"(known: {', '.join(sorted(values)) or 'none'})"
                )
            return values[match.group(1)]

        return _PLACEHOLDER.sub(value, token) if "{" in token else token

    return dataclasses.replace(interface, steps={
        step: tuple(fill_token(step, token) for token in tokens)
        for step, tokens in interface.steps.items()
    })


# --------------------------------------------------------------------------
# Inputs of several sides
# --------------------------------------------------------------------------

def union_inputs(interfaces: Iterable[Interface]) -> tuple[Input, ...]:
    """Every distinct input of ``interfaces``, in first-seen order: what to
    pin, each once. Two sides that read the same table over the same window in
    the same role share one copy."""
    return tuple(dict.fromkeys(i for interface in interfaces for i in interface.inputs))


def lineage_note(sides: Mapping[str, Interface]) -> str | None:
    """What to log when the sides do not read the same inputs, else ``None``.

    DIT pins what each side names. It cannot show that two different source
    tables hold the same data, so a comparison across them rests on the
    operator's word. The note names every side's tables.
    """
    if len({frozenset(interface.inputs) for interface in sides.values()}) <= 1:
        return None
    def reads(interface: Interface) -> str:
        return ", ".join(f"{i.role}={i.source}" for i in interface.inputs) or "nothing"

    listing = "; ".join(
        f"{name} reads {reads(interface)}" for name, interface in sides.items()
    )
    return (
        f"the sides do not read the same inputs: {listing}. DIT pins each table as named "
        f"and cannot show that they hold the same data: that the sides are comparable is "
        f"the operator's claim."
    )


# --------------------------------------------------------------------------
# Outputs of two sides
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class OutputPairing:
    """The outputs of two sides, paired by name. ``compared`` holds
    ``(left, right)`` pairs in the left side's order; the other two hold the
    outputs one side has and the other lacks."""

    compared: tuple[tuple[Output, Output], ...]
    only_left: tuple[Output, ...] = ()
    only_right: tuple[Output, ...] = ()


def pair_outputs(left: Interface, right: Interface) -> OutputPairing:
    """Pair the outputs of two sides by name."""
    by_name = {o.name: o for o in right.outputs}
    left_names = frozenset(_names(left))
    return OutputPairing(
        compared=tuple((o, by_name[o.name]) for o in left.outputs if o.name in by_name),
        only_left=tuple(o for o in left.outputs if o.name not in by_name),
        only_right=tuple(o for o in right.outputs if o.name not in left_names),
    )


def check_keys(pairing: OutputPairing) -> None:
    """Refuse a pairing in which a compared pair has different compare keys:
    such a comparison would match rows on two different identities.
    :class:`AdjustmentError` naming every such pair. Key order does not count."""
    unequal = [
        f"{left.name}: {', '.join(left.keys)} against {', '.join(right.keys)}"
        for left, right in pairing.compared if frozenset(left.keys) != frozenset(right.keys)
    ]
    if unequal:
        raise AdjustmentError(
            "compared outputs must have the same keys on both sides; they differ for "
            + "; ".join(unequal)
            + ". Give both sides the same keys (set_keys), or take the output out of the "
              "comparison on one side (remove_output)."
        )


# --------------------------------------------------------------------------
# Flag diff between two sides, for the run log
# --------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class StepDiff:
    """One step on two sides. A side that does not run the step has ``None``
    for its argv, and then there is no flag diff."""

    step: str
    left: composer.ParsedArgv | None
    right: composer.ParsedArgv | None
    flags: composer.FlagDiff | None

    @property
    def command_differs(self) -> bool:
        return (
            self.left is not None and self.right is not None
            and self.left.positionals != self.right.positionals
        )

    @property
    def identical(self) -> bool:
        return self.flags is not None and self.flags.identical and not self.command_differs


def _presence(parsed: composer.ParsedArgv, names: Collection[str]) -> composer.ParsedArgv:
    """``parsed`` with each flag in ``names`` reduced to one valueless mention."""
    kept = dict.fromkeys(
        composer.Flag(f.name, None) if f.name in names else f for f in parsed.flags
    )
    return dataclasses.replace(parsed, flags=tuple(kept))


def diff_steps(
    left: Interface, right: Interface, *, presence_only: Collection[str] = (),
) -> tuple[StepDiff, ...]:
    """The steps of two sides compared: command words, and flags by name and
    value (:func:`dit.composer.diff_flags`).

    Flags in ``presence_only`` are compared by presence, not by value: the
    flags whose values differ between any two bindings by design (each
    binding's own tables, labels and job names).
    """
    names = frozenset(presence_only)

    def parsed(interface: Interface, step: str) -> composer.ParsedArgv | None:
        if step not in interface.steps:
            return None
        return _presence(
            composer.parse_argv(interface.steps[step], bare=interface.bare, multi=interface.multi),
            names,
        )

    def diff(step: str) -> StepDiff:
        a, b = parsed(left, step), parsed(right, step)
        return StepDiff(
            step=step, left=a, right=b,
            flags=None if a is None or b is None else composer.diff_flags(a, b),
        )

    return tuple(map(diff, dict.fromkeys((*left.steps, *right.steps))))


def _show(parsed: composer.ParsedArgv, name: str) -> str:
    return " ".join(name if v is None else f"{name}={v}" for v in parsed.values(name))


def format_step_diff(diff: StepDiff, left_name: str, right_name: str) -> list[str]:
    """``diff`` as lines for the run log. One line when the sides agree."""
    if diff.left is None or diff.right is None or diff.flags is None:
        runs = left_name if diff.right is None else right_name
        return [f"{diff.step}: only {runs} runs this step"]
    if diff.identical:
        return [f"{diff.step}: same command and flags on {left_name} and {right_name}"]
    flags = diff.flags
    return [
        f"{diff.step}: {left_name} against {right_name}",
        *([f"  command: {' '.join(diff.left.positionals)} against "
           f"{' '.join(diff.right.positionals)}"] if diff.command_differs else []),
        *(f"  only {left_name}: {_show(diff.left, n)}" for n in flags.only_left),
        *(f"  only {right_name}: {_show(diff.right, n)}" for n in flags.only_right),
        *(f"  values differ: {left_name} {_show(diff.left, n)}, "
          f"{right_name} {_show(diff.right, n)}" for n in flags.differing),
    ]


# --------------------------------------------------------------------------
# Preflight: flags against the ref's CLI source
# --------------------------------------------------------------------------

_FLAG_LITERAL = re.compile(r"""(["'])(--?[A-Za-z][A-Za-z0-9_-]*)\1""")


def declared_flags(source: str) -> frozenset[str]:
    """The flags a CLI source file declares, read statically: every quoted
    string that is exactly one flag name (``"--date_range"``, ``'-v'``).

    This finds argparse and click declarations written as string literals. It
    does not run the file, so it misses a flag whose name is built at run time
    and counts a flag-shaped literal that is not a declaration. A flag named
    only inside a longer help string is not counted.
    """
    return frozenset(match.group(2) for match in _FLAG_LITERAL.finditer(source))


def undeclared_flags(
    tokens: Sequence[str],
    declared: Collection[str],
    *,
    exempt: Collection[str] = (),
    bare: Collection[str] = (),
    multi: Collection[str] = (),
) -> tuple[str, ...]:
    """The flags in ``tokens`` that are neither in ``declared`` nor ``exempt``,
    in argv order and without repeats.

    ``exempt`` is for the flags the pipeline's command does not declare on
    purpose because another parser takes them (Beam's pipeline options).
    """
    parsed = composer.parse_argv(tokens, bare=bare, multi=multi)
    return tuple(n for n in parsed.names() if n not in declared and n not in exempt)
