"""Declared adjustments of pipe-segment's interface, and what DIT reads of a
pipe-segment ref to decide which of them apply.

The pipe-segment workflows build a *base* interface that mirrors what
production runs today (``identity_match_key._base_interface``). A binding
whose pipe-segment differs from that gets the adjustments declared here whose
``applies`` holds for it. See :mod:`dit.adjust` for the layers and the edits.

**One adjustment today: no incremental segmenter below v5.1.0.** pipe-segment
v5.1.0 added three flags to ``segment``: ``--incremental``,
``--in_segments_table`` and ``--in_fragments_table`` (compare
``src/pipe_segment/cli/commands/segment.py`` at tags v5.0.3 and v5.1.0: the
file's only change is the three declarations). The base passes all three,
as production's task passes ``--incremental`` since 2026-11-20. For a binding
whose ``segment`` command does not declare the flag, the three are removed.

The plan words this rule from the other side ("5.1.0 and later: add
``--incremental`` and its tables"). It is the same rule. It is written as a
removal because the base is what production runs today, and the rule is what
gets deleted when no run needs a version below v5.1.0 as a base.

**Why the rule asks the ref's source first, and the version second.** An
unreleased head still carries the version number of the release before it. A
head with the incremental segmenter and ``version = "5.0.3"`` in its
``pyproject.toml`` must keep the flags. So ``lacks_incremental`` asks whether
the ref's ``segment`` command declares ``--incremental``, and only when that
source was not read does it compare the version.

**Removal rule for old layers.** An adjustment is deleted when no run needs
its version as a base. The proposal is to keep production's current and
previous version. Each adjustment for an older version names the composer
commit at which production last ran that version (``FROZEN_AT``); the frozen
drift spec in ``composer_drift.py`` checks base plus adjustment against it.

**What is read from a ref, and how.** With ``git show <ref>:<path>``
(``dit.git_info.show_file``): no worktree, no checkout, nothing executed.

* The flags a command declares: every quoted flag name in the command's
  module (``dit.adjust.declared_flags``).
* The version: ``pyproject.toml``'s ``version`` (v5.0.3 and later), else
  ``__version__`` in the package's ``version.py`` (up to v5.0.2).

Both were checked against the local pipe-segment checkout at tags v5.0.0,
v5.0.2, v5.0.3 and v5.1.0 on 2026-11-24. A static read is a reading of
source, not a run of the CLI.
"""
from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence

from dit import adjust, composer, drift
from dit.git_info import first_file, show_file

#: The first pipe-segment version whose ``segment`` command declares
#: ``--incremental``: the tag v5.1.0 has it, v5.0.3 does not.
INCREMENTAL_SINCE = (5, 1, 0)

#: The three flags v5.1.0 added to ``segment``.
INCREMENTAL_FLAGS = ("--incremental", "--in_segments_table", "--in_fragments_table")

#: The step the flags belong to.
SEGMENT_STEP = "segment"


def lacks_incremental(binding: adjust.BindingInfo) -> bool:
    """Whether the binding's ``segment`` command has no ``--incremental``.

    The ref's own CLI source decides when it was read. Otherwise the version
    does. With neither known the binding is taken to know the flag, as the
    base does; the preflight then needs the source to let the run start.
    """
    declared = binding.declares(SEGMENT_STEP, INCREMENTAL_FLAGS[0])
    if declared is not None:
        return not declared
    return binding.version is not None and binding.version < INCREMENTAL_SINCE


#: Below v5.1.0: remove the incremental segmenter's three flags.
NO_INCREMENTAL_SEGMENTER = adjust.Adjustment(
    edits=adjust.on_step(SEGMENT_STEP, *map(adjust.remove, INCREMENTAL_FLAGS)),
    applies=lacks_incremental,
    reason="pipe-segment below v5.1.0 has no incremental segmenter: its segment command "
           "does not declare the flag, and would hand it to Beam, which ignores it",
)

#: Every declared adjustment, in the order they are applied.
DECLARED: tuple[adjust.Adjustment, ...] = (NO_INCREMENTAL_SEGMENTER,)

#: Per older version an adjustment exists for: the composer-dags-production
#: commit at which production last ran that version. For v5.0.3 it is the
#: first parent of the merge of composer's PR #3145, which moved the AIS v3
#: DAG's segment group to v5.1.0 on 2026-11-20 (found with ``git log
#: --first-parent`` on the AIS v3 snapshot in a local checkout, 2026-11-24).
FROZEN_AT: Mapping[tuple[int, int, int], str] = {
    (5, 0, 3): "cb0c9a3b793050fe0263152f6ff87d165e447b71",
}


# --------------------------------------------------------------------------
# Versions
# --------------------------------------------------------------------------

_VERSION_TAG_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?!\d)")
_VERSION_REF_RE = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")


def _version(match: re.Match[str] | None) -> tuple[int, int, int] | None:
    if match is None:
        return None
    major, minor, patch = map(int, match.groups())
    return major, minor, patch


def version_text(version: Sequence[int]) -> str:
    return "v" + ".".join(map(str, version))


def image_pipeline_version(image: str) -> tuple[int, int, int] | None:
    """The pipe-segment version an image's tag starts with, or ``None``.

    ``…/pipe-segment:v5.1.0-4ddde03-temp-dataset-…`` gives ``(5, 1, 0)``; so
    does a published ``…:v5.1.0``. A tag that starts with anything else
    (``dit-<commit>``, a branch name) names no version.
    """
    return _version(_VERSION_TAG_RE.match(composer.image_version(image) or ""))


def ref_version(ref: str) -> tuple[int, int, int] | None:
    """The version a ref names when it is written as a version tag
    (``v5.0.3``, ``5.0.3``), else ``None``. No git call."""
    return _version(_VERSION_REF_RE.fullmatch(ref))


#: Where a pipe-segment ref states its version, newest layout first.
_VERSION_SOURCES = (
    ("pyproject.toml", re.compile(r'^version\s*=\s*"(\d+)\.(\d+)\.(\d+)', re.MULTILINE)),
    ("src/pipe_segment/version.py",
     re.compile(r"""__version__\s*=\s*["'](\d+)\.(\d+)\.(\d+)""")),
    ("pipe_segment/version.py",
     re.compile(r"""__version__\s*=\s*["'](\d+)\.(\d+)\.(\d+)""")),
)


def read_version(pipeline_dir: str, ref: str) -> tuple[int, int, int] | None:
    """The version the code at ``ref`` states, or ``None`` when no known file
    states one. An unreleased head states the version of the release before it."""
    contents = ((show_file(pipeline_dir, ref, path), pattern) for path, pattern in _VERSION_SOURCES)
    versions = (
        _version(pattern.search(content)) for content, pattern in contents if content is not None
    )
    return next((version for version in versions if version is not None), None)


# --------------------------------------------------------------------------
# The CLI source of a ref
# --------------------------------------------------------------------------

#: Where a pipe-segment ref declares the flags of the command ``{step}``:
#: under ``src/`` since v5.0.3, at the repository root before.
COMMAND_FILES = (
    "src/pipe_segment/cli/commands/{step}.py",
    "pipe_segment/cli/commands/{step}.py",
)


def command_files(step: str) -> tuple[str, ...]:
    return tuple(path.format(step=step) for path in COMMAND_FILES)


def read_declared(
    pipeline_dir: str, ref: str, steps: Sequence[str],
) -> dict[str, frozenset[str]]:
    """Per step, the flags its command declares at ``ref``. A step whose
    command module is not found at the ref is left out."""
    sources = {step: first_file(pipeline_dir, ref, command_files(step)) for step in steps}
    return {
        step: adjust.declared_flags(found[1])
        for step, found in sources.items() if found is not None
    }


def binding_info(
    pipeline_dir: str, name: str, ref: str, steps: Sequence[str],
) -> adjust.BindingInfo:
    """What the rules may know about the binding ``name`` at ``ref``, read
    from the ref's own source in the checkout at ``pipeline_dir``."""
    return adjust.BindingInfo(
        name=name,
        ref=ref,
        version=read_version(pipeline_dir, ref) or ref_version(ref),
        declared=read_declared(pipeline_dir, ref, steps),
    )


def offline_binding_info(name: str, ref: str | None) -> adjust.BindingInfo:
    """The binding without any git call: its version only when the ref is
    written as a version tag, and no CLI source. For building a binding's
    argv where no checkout is at hand (the drift spec, tests)."""
    return adjust.BindingInfo(
        name=name, ref=ref, version=None if ref is None else ref_version(ref),
    )


# --------------------------------------------------------------------------
# Preflight: which flags are pipe-segment's own
# --------------------------------------------------------------------------

#: Flags of the two Beam steps that pipe-segment's commands do not declare and
#: Beam takes: the preflight does not look for them in the command's module.
#:
#: Why they are Beam's. ``pipe-segment``'s entry point parses with
#: ``parse_known_args`` (``cli/main.py``) and hands the flags it does not know
#: to the command, which builds ``PipelineOptions`` from them (``pipeline.py``,
#: ``SegmentPipeline.build``; read at v5.0.3 and v5.1.0). Of the flags DIT
#: emits for ``segment`` and ``segment_identity``, these are the ones the two
#: command modules do not declare at either tag (listed by reading both
#: modules, 2026-11-24):
#:
#: * ``dit.drift.DATAFLOW_OWNED_FLAGS``: job name, labels, service account,
#:   temp and staging locations, subnetwork, worker cap, worker image;
#: * and the eight below: project, runner, region, setup file, machine type,
#:   disk size, private IPs, network.
#:
#: DIT does not check them against Beam: Beam ignores an option it does not
#: know too, and the list is hand-written. ``--wait_for_job`` is not here: both
#: commands declare it.
#:
#: The four SQL steps get no exemption. Their commands declare everything DIT
#: passes them, ``--labels`` and ``--project`` included, and a Beam option
#: passed to one would be ignored silently.
BEAM_OPTIONS = drift.DATAFLOW_OWNED_FLAGS | frozenset({
    "--project",
    "--runner",
    "--region",
    "--setup_file",
    "--worker_machine_type",
    "--disk_size_gb",
    "--no_use_public_ips",
    "--network",
})


def flag_problems(
    interface: adjust.Interface,
    binding: adjust.BindingInfo,
    *,
    beam_steps: Collection[str],
) -> list[str]:
    """What stops the binding's argv from being trusted, one sentence each:
    a step whose command module was not read at the ref, and every pipeline
    flag a step passes that its command does not declare there. Empty when
    every flag is accounted for."""
    def problems(step: str, tokens: Sequence[str]) -> list[str]:
        declared = None if binding.declared is None else binding.declared.get(step)
        if declared is None:
            return [
                f"step {step}: its command module was not found at the ref (looked for "
                f"{' and '.join(command_files(step))}), so its flags cannot be checked"
            ]
        return [
            f"step {step} passes {flag}, which the ref's {step} command does not declare "
            f"({' or '.join(command_files(step))})"
            for flag in adjust.undeclared_flags(
                tokens, declared,
                exempt=BEAM_OPTIONS if step in beam_steps else (),
                bare=interface.bare, multi=interface.multi,
            )
        ]

    return [p for step, tokens in interface.steps.items() for p in problems(step, tokens)]
