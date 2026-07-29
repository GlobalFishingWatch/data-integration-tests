"""Process-global registry of the active dit run's identity.

**Why this module exists.** ``dit run``'s SIGTERM handler needs the run's
``run_id`` so it can call :func:`dit.cache.cancel_run` and tear down the
Dataflow jobs the run submitted. But the ``run_id`` is minted *inside* the
workflow's ``main()`` (by :func:`dit.workflow.resolve_run_context`), which
the CLI invokes opaquely -- the CLI cannot know it up front, and threading it
back out would mean changing every workflow's entry-point contract. So the
workflow publishes it here the moment it exists, and the handler reads it.

**Deliberately import-light** -- no BigQuery, no ``google-cloud-*``, no
``dit.cache``. ``dit.cli`` imports this eagerly at module scope, and pays
for the BQ stack only inside the handler (the same lazy-import reasoning
that keeps ``dit --help`` fast).

**No lock, on purpose.** The obvious instinct is to guard the global with a
``threading.Lock``; that would be a latent deadlock. Python delivers signals
to the main thread between bytecodes, so a SIGTERM arriving while the main
thread happens to hold a non-reentrant lock inside :func:`set_active_run_id`
would deadlock the handler's :func:`get_active_run_id` against itself.
Assigning and reading a single module-level name is atomic under the GIL,
which is all the safety this needs.

**Single-run scope.** One ``dit run`` process drives one workflow ``main()``,
which calls ``resolve_run_context`` once; the ``--parallel`` paths fan out
threads *within* that one ``run_id``. There is never a second concurrent run
in-process to confuse the global with.
"""
from __future__ import annotations

from typing import Optional

#: The active run's id, or ``None`` before ``resolve_run_context`` has run
#: (or after an explicit :func:`clear_active_run_id`).
_active_run_id: Optional[str] = None


def set_active_run_id(run_id: str) -> None:
    """Publish ``run_id`` as the process's active run.

    Called by :func:`dit.workflow.resolve_run_context` as soon as the id is
    minted -- deliberately *before* any Dataflow job is submitted, so the
    window in which a cancellation could leak an unlabelled job is as small
    as the code allows.
    """
    global _active_run_id
    _active_run_id = run_id


def get_active_run_id() -> Optional[str]:
    """The active run's id, or ``None`` if no run has started yet.

    ``None`` is a meaningful answer for the cleanup path, not an error: no
    ``run_id`` means no ``dit_run_id``-labelled Dataflow job can exist yet,
    so there is genuinely nothing to cancel.
    """
    return _active_run_id


def clear_active_run_id() -> None:
    """Forget the active run. Exists for test isolation; production code has
    no reason to call it (the process exits at the end of a run)."""
    global _active_run_id
    _active_run_id = None
