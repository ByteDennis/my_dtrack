CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_pair_meta (
  pair_name STRING NOT NULL,
  enabled BOOLEAN NOT NULL,
  key_columns ARRAY<STRING>,
  vintage STRING NOT NULL,
  left_source STRING NOT NULL,
  left_conn STRING NOT NULL,
  left_table STRING,
  left_date_col STRING,
  left_options_json STRING,
  right_source STRING NOT NULL,
  right_conn STRING,
  right_table STRING,
  right_date_col STRING,
  right_options_json STRING,
  run_defaults_json STRING,
  config_json STRING COMMENT 'Deprecated compatibility snapshot; explicit columns are authoritative',
  latest_run_id STRING,
  latest_status STRING,
  updated_at TIMESTAMP_NTZ
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_column_map (
  pair_name STRING NOT NULL,
  left_column STRING NOT NULL,
  right_column STRING NOT NULL,
  data_type STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_pair_sql (
  pair_name STRING NOT NULL,
  left_sql STRING,
  left_query_windows_json STRING,
  right_sql STRING,
  right_query_windows_json STRING,
  updated_at TIMESTAMP_NTZ
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_run_status (
  run_id STRING NOT NULL,
  pair_name STRING NOT NULL,
  owner STRING NOT NULL,
  state STRING NOT NULL,
  timezone STRING NOT NULL,
  started_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_run_error (
  run_id STRING NOT NULL,
  check_name STRING NOT NULL,
  pair_name STRING NOT NULL,
  vintage STRING,
  column_name STRING,
  partition_value STRING,
  left_value STRING,
  right_value STRING,
  detail STRING,
  payload STRING,
  created_at TIMESTAMP_NTZ
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_progress_snapshot (
  snapshot_ts TIMESTAMP_NTZ NOT NULL,
  run_id STRING NOT NULL,
  pair_name STRING NOT NULL,
  state STRING NOT NULL,
  units_expected BIGINT NOT NULL,
  units_ready BIGINT NOT NULL,
  units_complete BIGINT NOT NULL,
  units_failed BIGINT NOT NULL,
  worker_threads BIGINT NOT NULL,
  detail STRING
) USING DELTA;
