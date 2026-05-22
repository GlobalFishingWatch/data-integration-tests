-- Migration: dit_meta.runs (run cache + registry + provenance).
-- See docs/run-cache.md for the design.
--
-- Apply: `bq query --use_legacy_sql=false --project_id=world-fishing-827 \
--             < migrations/001_dit_meta_runs.sql`
--
-- Prerequisite: the `dit_meta` dataset must exist with `dataEditor` for
-- `automated-testing@world-fishing-827.iam.gserviceaccount.com`. One-time
-- terraform via cloud-platform-terraform (or `bq mk dit_meta` + IAM via
-- the console).

CREATE TABLE IF NOT EXISTS `world-fishing-827.dit_meta.runs` (
  run_id              STRING    NOT NULL,
  cache_key           STRING    NOT NULL,

  workflow            STRING    NOT NULL,
  pipeline            STRING    NOT NULL,
  experiment_id       STRING    NOT NULL,
  pipeline_commit     STRING    NOT NULL,
  pipeline_dirty      BOOL      NOT NULL,
  dit_commit          STRING    NOT NULL,
  workflow_file_sha1  STRING    NOT NULL,
  worker_image        STRING    NOT NULL,

  params_json         JSON,

  output_tables       ARRAY<STRING>,
  dataflow_job_ids    ARRAY<STRING>,
  cloud_build_id      STRING,

  started_at          TIMESTAMP NOT NULL,
  finished_at         TIMESTAMP,
  status              STRING    NOT NULL,
  expires_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(started_at)
CLUSTER BY pipeline, cache_key
OPTIONS (
  description = 'dit run cache + registry. See docs/run-cache.md.'
);
