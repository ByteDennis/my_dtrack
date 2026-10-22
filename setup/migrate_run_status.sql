-- Run once when pair_recon_run_status was created before result summaries were added.

ALTER TABLE ${catalog}.${schema}.pair_recon_run_status ADD COLUMNS (
  row_total BIGINT,
  row_matches BIGINT,
  row_differences BIGINT,
  column_total BIGINT,
  column_matches BIGINT,
  column_differences BIGINT,
  runtime_failures BIGINT
);
