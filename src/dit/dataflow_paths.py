"""GCS locations for dit's Beam/Dataflow jobs.

Every temp and staging object a dit job writes goes under ``gs://<bucket>/dit/``.
The default bucket (``DIT_DATAFLOW_TEMP_BUCKET``, ``pipe-temp-us-central-ttl7``)
is also production's Dataflow temp bucket, and production writes directly under
``dataflow_temp/`` and ``dataflow_staging/`` (composer-dags
``gfw/common/config/dataflow.py``). The ``dit/`` prefix keeps dit's objects
separable from production's for inspection, cleanup and cost attribution
without needing a different bucket or new permissions.

Import-light on purpose: ``dit.runners.dataflow`` and every workflow use it.
"""

from __future__ import annotations

#: Top-level prefix of every GCS object dit's Dataflow jobs write.
GCS_PREFIX = "dit"


def dataflow_temp_location(bucket: str) -> str:
    """Beam ``--temp_location`` for dit jobs in ``bucket``."""
    return f"gs://{bucket}/{GCS_PREFIX}/dataflow_temp"


def dataflow_staging_location(bucket: str) -> str:
    """Beam ``--staging_location`` for dit jobs in ``bucket``."""
    return f"gs://{bucket}/{GCS_PREFIX}/dataflow_staging"
