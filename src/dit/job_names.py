"""Dataflow job-name builder shared across dit workflows.

All dit-launched Dataflow jobs share the prefix ``dit-<repo>-<step>-...`` so a
single label-filterable selector finds them in the Dataflow UI. This module
centralises the composition + truncation logic so workflows pass semantic
parts (repo, step, experiment_id, mode, ...) rather than building strings
locally.

Dataflow accepts longer names in practice but documents a 63-char cap; we
honour it by truncating ``experiment_id`` from the right when needed, since
the other parts (binding, mode, iteration counter) are load-bearing for
at-a-glance triage of concurrent jobs.
"""

from __future__ import annotations

import re

MAX_JOB_NAME = 63

_UNSAFE_CHARS_RE = re.compile(r"[^a-z0-9-]+")
_REPEATED_HYPHEN_RE = re.compile(r"-+")


def to_safe_for_job_name(s: str) -> str:
    """Coerce to the lowercase / digit / hyphen alphabet Dataflow expects.

    Lowercases, replaces runs of any non ``[a-z0-9-]`` character with a single
    hyphen, collapses repeated hyphens, and strips leading/trailing hyphens.
    Guarantees the output is a non-empty Dataflow-name-segment when the input
    contains at least one alphanumeric character; returns ``""`` otherwise
    (caller's responsibility to handle that degenerate case).
    """
    s = _UNSAFE_CHARS_RE.sub("-", s.lower())
    s = _REPEATED_HYPHEN_RE.sub("-", s)
    return s.strip("-")


def make_job_name(
    *,
    repo: str,
    step: str,
    experiment_id: str,
    mode: str | None = None,
    binding: str | None = None,
    iteration: int | None = None,
    total_iterations: int | None = None,
    max_len: int = MAX_JOB_NAME,
) -> str:
    """Build ``dit-<repo>-<step>-<exp>-<binding?>-<mode?>-<N?>-<M?>``.

    Truncates ``experiment_id`` from the right when the composed name would
    exceed ``max_len``. ``binding``, ``mode``, and the iteration counter are
    preserved because they're load-bearing for triage. Iteration is rendered
    only when both ``iteration`` and ``total_iterations`` are provided.
    """
    fixed = ["dit", to_safe_for_job_name(repo), to_safe_for_job_name(step)]
    tail: list[str] = []
    if binding:
        tail.append(to_safe_for_job_name(binding))
    if mode:
        tail.append(to_safe_for_job_name(mode))
    if iteration is not None and total_iterations is not None:
        tail.append(f"{iteration}-{total_iterations}")

    exp = to_safe_for_job_name(experiment_id)
    candidate = "-".join(fixed + [exp] + tail)
    if len(candidate) <= max_len:
        return candidate

    fixed_and_tail_len = len("-".join(fixed + [""] + tail))
    available = max_len - fixed_and_tail_len
    if available < 1:
        # Slicing the whole candidate would chop the load-bearing tail
        # (binding / mode / iteration counter) and could leave a trailing
        # hyphen, which Dataflow rejects. Surface a clear caller error
        # instead.
        raise ValueError(
            f"cannot fit job name within {max_len} chars: the fixed parts "
            f"(repo/step) plus the tail (binding/mode/iteration) already "
            f"occupy {fixed_and_tail_len - 1} chars, leaving no room for the "
            f"experiment id. Shorten one of them or raise max_len."
        )
    return "-".join(fixed + [exp[:available]] + tail)
