-- Migration: tech_great_expectations.dit_runs (run cache + registry + provenance).
-- See docs/run-cache.md for the design.
--
-- Apply: `bq query --use_legacy_sql=false --project_id=world-fishing-827 \
--             < migrations/001_dit_meta_runs.sql`
--
-- Dataset: lives in the existing `tech_great_expectations` dataset that
-- dit already writes workflow outputs to, so no new dataset creation or
-- IAM grant is required. The `dit_` prefix scopes the table within the
-- shared dataset. If/when we outgrow this and want a dit-owned dataset,
-- the table can move with no API-shape change (just bump TABLE_FQN in
-- src/dit/cache.py).

CREATE TABLE IF NOT EXISTS `world-fishing-827.tech_great_expectations.dit_runs` (
  run_id              STRING    NOT NULL,
  cache_key           STRING    NOT NULL,

  workflow            STRING    NOT NULL,
  pipeline            STRING    NOT NULL,
  experiment_id       STRING    NOT NULL,
  pipeline_commit     STRING    NOT NULL,
  -- pipeline_dirty: legacy "was the tree dirty" flag. Superseded by
  -- unreviewed_code (M-pivot-3); retained NOT NULL for one release while
  -- write_cache dual-writes it (= unreviewed_code), then dropped. See
  -- migrations/002_unreviewed_code.sql + docs/no-dirty-tree-pivot.md.
  pipeline_dirty      BOOL      NOT NULL,
  -- unreviewed_code: approximates "not a reviewed/main commit". As shipped
  -- (M-pivot-3) it's TRUE for snapshot refs / dirty trees / DIT_PIPELINE_COMMIT
  -- runs and FALSE for a clean checkout of ANY branch (the precise
  -- merge-base-with-origin/main refinement is deferred). read_cache no longer
  -- filters on it (snapshots are cacheable); informational only -- strict-
  -- provenance queries should read FALSE as "clean checkout", not "on main".
  unreviewed_code     BOOL,
  -- pipeline_commit_parent: for a snapshot run, the HEAD the dirty tree was
  -- based on (parsed from the snapshot commit message "dit snapshot of <sha>").
  -- NULL for non-snapshot runs.
  pipeline_commit_parent STRING,
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
