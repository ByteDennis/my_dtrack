-- Run once before importing the new three-worksheet setup/pairs.xlsx.
-- Existing config_json and vintage columns remain available for legacy fallback.

ALTER TABLE ${catalog}.${schema}.pair_recon_pair_meta ADD COLUMNS (
  key_columns ARRAY<STRING>,
  vintage STRING,
  left_source STRING,
  left_conn STRING,
  left_table STRING,
  left_date_col STRING,
  left_options_json STRING,
  right_source STRING,
  right_conn STRING,
  right_table STRING,
  right_date_col STRING,
  right_options_json STRING,
  run_defaults_json STRING
);

ALTER TABLE ${catalog}.${schema}.pair_recon_pair_sql ADD COLUMNS (
  left_query_windows_json STRING,
  right_query_windows_json STRING,
  updated_at TIMESTAMP_NTZ
);
