-- Run once for installations created before run selection, snapshot payloads,
-- and the reference data dictionary were added.

ALTER TABLE ${catalog}.${schema}.pair_recon_run_status ADD COLUMNS (
  from_date DATE,
  to_date DATE,
  vintage STRING,
  sample_rate DOUBLE,
  vintage_sample STRING
);

ALTER TABLE ${catalog}.${schema}.pair_recon_progress_snapshot ADD COLUMNS (
  payload STRING
);

UPDATE ${catalog}.${schema}.pair_recon_progress_snapshot
SET payload = detail
WHERE payload IS NULL AND detail IS NOT NULL;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_reference (
  table_name STRING NOT NULL,
  column_name STRING NOT NULL,
  keyword STRING,
  definition STRING NOT NULL,
  updated_at TIMESTAMP_NTZ NOT NULL
) USING DELTA;

-- Run setup/reference.sql after this migration to load the maintained definitions.
