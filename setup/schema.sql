CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.pair_recon_pair_meta (
  pair_name STRING NOT NULL,
  enabled BOOLEAN NOT NULL,
  config_json STRING NOT NULL,
  latest_run_id STRING,
  latest_status STRING,
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
