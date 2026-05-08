"""Dataflow-based in-process pipeline runner.

Submits a Beam pipeline to Dataflow and blocks until completion. The
submission step (which includes Beam's sdist build via ``python -m build``)
is serialised via :data:`_DATAFLOW_SUBMIT_LOCK`; the long
``wait_until_finish()`` runs outside the lock so multiple jobs execute
concurrently on Dataflow. Without the lock split, parallel submissions race
on Beam's temporary source directory.

The runner accepts a ``pipeline_builder`` callable so it stays
pipeline-agnostic (pipe-gaps, port-visits, pipe-events all bring their own
pipeline factories). Pre-existing temp BQ dataset injection -- the
``_DagFactoryWithTempDataset`` override pattern from the source -- is
applied here when ``bq_temp_dataset`` is set: the runner monkey-patches the
``read_from_bigquery_factory`` of the workflow's DAG factory class to
inject ``temp_dataset=<existing dataset>``. This avoids the
``bigquery.datasets.create`` permission requirement at submission time;
Beam reuses the existing dataset and creates only a temp table inside it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

# Splits Beam submission from waiting so concurrent invocations serialise the
# submit step (which builds an sdist in a shared temp dir) but parallelise the
# long wait_until_finish() on Dataflow. See module docstring.
_DATAFLOW_SUBMIT_LOCK = threading.Lock()


def _parse_args(args: list[str]) -> dict[str, Any]:
    """Translate ``["--key=value", "--flag", "--other", "x"]`` into a dict.

    Accepts the two common forms: ``--key=value`` (single token) and
    ``--key value`` (two tokens). Bare flags (``--foo`` with no value) map to
    ``True``. Unknown tokens are kept as positional and ignored at the
    options-merge level.
    """
    parsed: dict[str, Any] = {}
    i = 0
    while i < len(args):
        tok = args[i]
        if tok.startswith("--"):
            body = tok[2:]
            if "=" in body:
                key, value = body.split("=", 1)
                parsed[key.replace("-", "_")] = value
                i += 1
                continue
            key = body.replace("-", "_")
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                parsed[key] = args[i + 1]
                i += 2
                continue
            parsed[key] = True
            i += 1
            continue
        i += 1
    return parsed


def _wrap_factory_with_temp_dataset(dag_factory_cls: type, bq_temp_dataset: str) -> type:
    """Subclass ``dag_factory_cls`` to inject ``temp_dataset`` into its
    ``read_from_bigquery_factory`` -- the ``_DagFactoryWithTempDataset``
    pattern from ``mode_equivalence.py``.

    Importing apache_beam happens here so callers that don't use the
    dataflow runner don't pay the import cost.
    """
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as bq_clients

    temp_proj, temp_ds = bq_temp_dataset.split(".", 1)
    temp_dataset_ref = bq_clients.DatasetReference(
        projectId=temp_proj, datasetId=temp_ds,
    )

    class _DagFactoryWithTempDataset(dag_factory_cls):  # type: ignore[misc, valid-type]
        @property
        def read_from_bigquery_factory(self):
            def _factory(**kwargs: Any) -> Any:
                kwargs.setdefault("temp_dataset", temp_dataset_ref)
                return beam.io.ReadFromBigQuery(**kwargs)
            return _factory

    return _DagFactoryWithTempDataset


def run(
    args: list[str],
    *,
    image_tag: str | None,
    service_account: str,
    region: str,
    temp_bucket: str,
    subnetwork: str,
    bq_temp_dataset: str,
    env: dict | None = None,
    pipeline_builder: Callable[[Mapping[str, Any]], Any] | None = None,
    dag_factory_cls: type | None = None,
) -> int:
    """Submit a Beam pipeline to Dataflow and wait for completion.

    Parameters mirror the contract in ``docs/plan.md``:

    * ``args`` -- extra Beam pipeline-options as ``--key=value`` tokens. Merged
      under the explicit knobs below; the explicit knobs win.
    * ``image_tag`` -- Dataflow worker harness image tag. Optional because Beam
      wires the worker image differently (typically via ``setup.py`` staging).
    * ``service_account`` / ``region`` / ``temp_bucket`` / ``subnetwork`` /
      ``bq_temp_dataset`` -- Dataflow knobs. **Function parameters, not
      module-level constants** (decision 5 in the plan). ``temp_bucket`` is
      expanded to ``temp_location=gs://<bucket>/dataflow_temp`` and
      ``staging_location=gs://<bucket>/dataflow_staging``.
    * ``pipeline_builder`` -- callable that, given a merged options mapping,
      returns a built ``gfw.common.beam.pipeline.Pipeline``-shaped object
      (must expose ``_pre_hooks``, ``_post_hooks``, ``apply_dag()``, and
      ``.pipeline.run()``). Workflows bring their own pipeline factory; the
      runner stays pipeline-agnostic.
    * ``dag_factory_cls`` -- optional DAG factory class. When ``bq_temp_dataset``
      is set, the runner subclasses it to inject ``temp_dataset`` into
      ``read_from_bigquery_factory`` (the ``_DagFactoryWithTempDataset``
      pattern). The wrapped class is then passed to ``pipeline_builder`` via
      the ``dag_factory_cls`` key in the options mapping. Workflows that
      don't need temp-dataset injection can omit this and ignore the key.

    Returns 0 on ``PipelineState.DONE``, non-zero otherwise.
    """
    if pipeline_builder is None:
        raise ValueError(
            "dataflow.run requires a pipeline_builder; the runner is "
            "pipeline-agnostic and the workflow brings its own Beam pipeline "
            "factory."
        )
    if env is not None:
        # env is reserved for parity with the Runner protocol; the dataflow
        # runner is in-process so there is no subprocess to forward env to.
        # Surface a clear signal rather than silently dropping the dict.
        logger.warning(
            "dataflow.run ignores env=%r (in-process runner has no subprocess)",
            env,
        )

    # Late imports so importers that only use the docker runner don't pay
    # the apache_beam import cost.
    from apache_beam.runners.runner import PipelineState

    parsed = _parse_args(args)
    parsed.setdefault("runner", "DataflowRunner")
    parsed.setdefault("service_account_email", service_account)
    parsed.setdefault("region", region)
    parsed.setdefault("temp_location", f"gs://{temp_bucket}/dataflow_temp")
    parsed.setdefault("staging_location", f"gs://{temp_bucket}/dataflow_staging")
    parsed.setdefault("subnetwork", subnetwork)
    if image_tag:
        parsed.setdefault("sdk_container_image", image_tag)

    effective_factory_cls = dag_factory_cls
    if bq_temp_dataset and dag_factory_cls is not None:
        effective_factory_cls = _wrap_factory_with_temp_dataset(
            dag_factory_cls, bq_temp_dataset,
        )

    options: dict[str, Any] = dict(parsed)
    if effective_factory_cls is not None:
        options["dag_factory_cls"] = effective_factory_cls
    if bq_temp_dataset:
        options["bq_temp_dataset"] = bq_temp_dataset

    pipeline = pipeline_builder(options)

    # Replicates gfw.common.beam.pipeline.Pipeline.run so we can release the
    # lock between submission and waiting. Reaches into _pre_hooks /
    # _post_hooks; acceptable for an integration-test runner.
    with _DATAFLOW_SUBMIT_LOCK:
        for hook in pipeline._pre_hooks:
            hook(pipeline)
        pipeline.apply_dag()
        result = pipeline.pipeline.run()  # Beam's submit -- returns on submission, doesn't wait.

    # Wait outside the lock so concurrent submissions wait in parallel.
    result.wait_until_finish()

    if result.state == PipelineState.DONE:
        for hook in pipeline._post_hooks:
            hook(pipeline)
        return 0

    logger.warning(
        "Dataflow pipeline did not finish successfully (state=%s); skipping post-hooks.",
        result.state,
    )
    return 1
