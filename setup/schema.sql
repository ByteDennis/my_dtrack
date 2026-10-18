CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_pair_meta (
  pair_name STRING NOT NULL,
  enabled BOOLEAN NOT NULL,
  config_json STRING NOT NULL,
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
  right_sql STRING
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
