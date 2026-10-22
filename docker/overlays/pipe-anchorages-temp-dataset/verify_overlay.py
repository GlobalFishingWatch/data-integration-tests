"""Self-check for the pipe-anchorages temp-dataset overlay (also used inside the image build).

Asserts: (1) unset env -> no extra kwargs; (2) env set -> a DatasetReference; (3) Beam's
ReadFromBigQuery forwards it to the EXPORT source, whose _setup_temporary_dataset then
returns without creating anything; (4) both patched pipelines import the helper; (5) every
BigQuery read in both pipelines passes it. (5) was added after the first v5 staging smoke
(2026-10-22): the patch covered ReadMessages but not ReadNamedAnchorages, so Beam tried to
create its own temp dataset and the job failed on bigquery.datasets.create.
"""

import inspect
import os
import re

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

# Every read must stage in the given dataset: one patched kwargs dict per from_query call,
# and no other way into Beam's ReadFromBigQuery. A new upstream read fails the build here.
EXPECTED_READS = {pst_main: 2, pv_main: 1}  # ReadMessages + ReadNamedAnchorages; ReadPortStateTransitions
for module, expected in EXPECTED_READS.items():
    src = inspect.getsource(module)
    reads = len(re.findall(r"ReadFromBigQuery\.from_query\(", src))
    kwargs_args = re.findall(r"read_from_bigquery_kwargs=\{[^}]*\}", src)
    patched = [k for k in kwargs_args if "**temp_dataset_kwargs()" in k]
    assert reads == expected, f"{module.__name__}: {reads} from_query reads, expected {expected}"
    assert len(kwargs_args) == reads == len(patched), (
        f"{module.__name__}: {reads} reads, {len(kwargs_args)} kwargs, {len(patched)} patched"
    )
    assert not re.search(r"beam\.io\.ReadFromBigQuery|io\.ReadFromBigQuery\(", src), module.__name__

print("overlay self-check OK")
