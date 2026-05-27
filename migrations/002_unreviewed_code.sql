-- Migration 002: unreviewed_code + pipeline_commit_parent (M-pivot-3).
-- See docs/no-dirty-tree-pivot.md § M-pivot-3.
--
-- Apply: `bq query --use_legacy_sql=false --project_id=world-fishing-827 \
--             < migrations/002_unreviewed_code.sql`
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + a backfill UPDATE guarded on NULL.
-- Safe to re-run. Fresh installs get these columns from 001's CREATE TABLE;
-- this migration converges existing tables.
--
-- Why two columns:
--   * unreviewed_code  -- sharper semantic than pipeline_dirty. The
--     no-dirty-tree pivot makes every run a committed ref, so "dirty" stops
--     meaning anything; "is this code reviewed?" is the real question.
--     read_cache drops its `pipeline_dirty = FALSE` filter so content-
--     addressable snapshot refs become cacheable on repeat runs.
--     NOTE (as shipped, M-pivot-3): the flag is an APPROXIMATION -- TRUE for
--     snapshot refs / dirty trees / DIT_PIPELINE_COMMIT runs, FALSE for a
--     clean checkout of ANY branch. The precise "merged into origin/main"
--     check (git merge-base --is-ancestor) is deferred. Strict-provenance
--     queries must NOT treat unreviewed_code = FALSE as "on main" -- it only
--     means "ran from a clean checkout".
--   * pipeline_commit_parent  -- for snapshot rows, the HEAD the dirty tree was
--     based on. Lets a query reconstruct the reproduce context even if the
--     snapshot ref is later deleted (secret-leak remediation).
--
-- pipeline_dirty is NOT dropped here. write_cache dual-writes it
-- (= unreviewed_code) for one release so older readers keep working; a later
-- migration drops it once nothing reads it.

ALTER TABLE `world-fishing-827.tech_great_expectations.dit_runs`
  ADD COLUMN IF NOT EXISTS unreviewed_code BOOL;

ALTER TABLE `world-fishing-827.tech_great_expectations.dit_runs`
  ADD COLUMN IF NOT EXISTS pipeline_commit_parent STRING;

-- Backfill existing rows: the old pipeline_dirty flag is the best available
-- proxy for unreviewed_code on historical data. pipeline_commit_parent stays
-- NULL for backfilled rows (pre-pivot snapshots didn't record a parent).
UPDATE `world-fishing-827.tech_great_expectations.dit_runs`
  SET unreviewed_code = pipeline_dirty
  WHERE unreviewed_code IS NULL;
