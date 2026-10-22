"""Self-check for the pipe-anchorages temp-dataset overlay (also used inside the image build).

Asserts: (1) unset env -> no extra kwargs; (2) env set -> a DatasetReference; (3) Beam's
ReadFromBigQuery forwards it to the EXPORT source, whose _setup_temporary_dataset then
returns without creating anything; (4) both patched pipelines import the helper.
"""

import os

import pipe_anchorages.pipelines.port_state_transitions.main as pst_main
import pipe_anchorages.pipelines.port_visits.main as pv_main
from apache_beam.io.gcp import bigquery as beam_bq
from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
from pipe_anchorages import _dit_overlay

os.environ.pop(_dit_overlay.ENV, None)
assert _dit_overlay.temp_dataset_kwargs() == {}, "unset env must add nothing"

os.environ[_dit_overlay.ENV] = "world-fishing-827.tech_great_expectations"
kwargs = _dit_overlay.temp_dataset_kwargs()
ref = kwargs["temp_dataset"]
assert isinstance(ref, DatasetReference)
assert (ref.projectId, ref.datasetId) == ("world-fishing-827", "tech_great_expectations")

read = beam_bq.ReadFromBigQuery(query="SELECT 1", use_standard_sql=True, **kwargs)
assert read._kwargs["temp_dataset"] is ref, "ReadFromBigQuery must forward temp_dataset"

source = beam_bq._CustomBigQuerySource(
    query="SELECT 1", use_standard_sql=True, temp_dataset=ref,
    method=beam_bq.ReadFromBigQuery.Method.EXPORT, job_name="j", step_name="s", unique_id="u",
)


class _NoCreate:
    def get_query_location(self, *a, **k):
        raise AssertionError("must not look up a location to create a dataset")

    def create_temporary_dataset(self, *a, **k):
        raise AssertionError("must not create a temporary dataset")


source._setup_temporary_dataset(_NoCreate())

for module in (pst_main, pv_main):
    assert module.temp_dataset_kwargs is _dit_overlay.temp_dataset_kwargs, module.__name__

print("overlay self-check OK")
