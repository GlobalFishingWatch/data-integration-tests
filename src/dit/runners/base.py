"""Runner protocol shared by docker and dataflow implementations."""

from __future__ import annotations

from typing import Any, Protocol


class Runner(Protocol):
    """A runner invokes a pipeline and returns its exit code.

    Concrete implementations (``dit.runners.docker``, ``dit.runners.dataflow``)
    differ in how they execute ``args`` -- subprocess vs in-process Beam
    submission -- but agree on the call shape so workflows can swap runners
    without restructuring.
    """

    def run(self, args: list[str], *, env: dict | None = None, **kwargs: Any) -> int: ...
