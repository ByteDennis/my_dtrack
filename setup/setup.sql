-- PairRecon Databricks SQL / Unity Catalog example upserts.
-- Run setup/schema.sql first, then replace catalog, schema, object names, and example values.
-- The first three tables are user-maintained configuration. The last three are normally
-- maintained by PairRecon and are included only as complete, repeatable examples.

-- # catalog.schema.pair_recon_pair_meta
MERGE INTO catalog.schema.pair_recon_pair_meta AS t
USING (
  SELECT
    'orders' AS pair_name,
    true AS enabled,
    array('ORDER_ID') AS key_columns,
    'month' AS vintage,
    'oracle' AS left_source,
    'PCDS' AS left_conn,
    'SALES.LEFT_ORDERS' AS left_table,
    'BUSINESS_DT' AS left_date_col,
    CAST(NULL AS STRING) AS left_options_json,
    'databricks_query' AS right_source,
    CAST(NULL AS STRING) AS right_conn,
    CAST(NULL AS STRING) AS right_table,
    'business_dt' AS right_date_col,
    CAST(NULL AS STRING) AS right_options_json,
    '{"partition_mode":"date","sample":0.1,"checks":["row_count","hash_sum"]}' AS run_defaults_json,
    '{"key":"ORDER_ID","vintage":"month","left":{"source":"oracle","conn_macro":"PCDS","schema":"SALES","table":"LEFT_ORDERS","date_col":"BUSINESS_DT"},"right":{"source":"databricks","kind":"query","date_col":"business_dt"}}' AS config_json,
    CAST(current_timestamp() AS TIMESTAMP_NTZ) AS updated_at
) AS s
ON t.pair_name = s.pair_name
WHEN MATCHED THEN UPDATE SET
  t.enabled = s.enabled,
  t.key_columns = s.key_columns,
  t.vintage = s.vintage,
  t.left_source = s.left_source,
  t.left_conn = s.left_conn,
  t.left_table = s.left_table,
  t.left_date_col = s.left_date_col,
  t.left_options_json = s.left_options_json,
  t.right_source = s.right_source,
  t.right_conn = s.right_conn,
  t.right_table = s.right_table,
  t.right_date_col = s.right_date_col,
  t.right_options_json = s.right_options_json,
  t.run_defaults_json = s.run_defaults_json,
  t.config_json = s.config_json,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
  pair_name, enabled, key_columns, vintage,
  left_source, left_conn, left_table, left_date_col, left_options_json,
  right_source, right_conn, right_table, right_date_col, right_options_json,
  run_defaults_json, config_json, latest_run_id, latest_status, updated_at
) VALUES (
  s.pair_name, s.enabled, s.key_columns, s.vintage,
  s.left_source, s.left_conn, s.left_table, s.left_date_col, s.left_options_json,
  s.right_source, s.right_conn, s.right_table, s.right_date_col, s.right_options_json,
  s.run_defaults_json, s.config_json, NULL, NULL, s.updated_at
);

-- # catalog.schema.pair_recon_column_map
-- This upserts listed mappings; it does not delete older mappings omitted from this example.
MERGE INTO catalog.schema.pair_recon_column_map AS t
USING (
  SELECT 'orders' AS pair_name, 'BUSINESS_DT' AS left_column,
         'business_dt' AS right_column, CAST(NULL AS STRING) AS data_type
  UNION ALL
  SELECT 'orders', 'ORDER_ID', 'order_id', 'numeric'
  UNION ALL
  SELECT 'orders', 'AMOUNT', 'amount', 'numeric'
  UNION ALL
  SELECT 'orders', 'POSTED_AT', 'posted_date',
         '{"type":"date","left_date_type":"date","right_date_type":"date"}'
) AS s
ON t.pair_name = s.pair_name AND t.left_column = s.left_column
WHEN MATCHED THEN UPDATE SET
  t.right_column = s.right_column,
  t.data_type = s.data_type
WHEN NOT MATCHED THEN INSERT (
  pair_name, left_column, right_column, data_type
) VALUES (
  s.pair_name, s.left_column, s.right_column, s.data_type
);

-- # catalog.schema.pair_recon_pair_sql
MERGE INTO catalog.schema.pair_recon_pair_sql AS t
USING (
  SELECT
    'orders' AS pair_name,
    'SELECT * FROM LEFT_ORDERS l WHERE {{query_windows:driver}}' AS left_sql,
    '{"driver":{"expression":"l.BUSINESS_DT","date_type":"date"}}' AS left_query_windows_json,
    'SELECT * FROM catalog.source.orders r WHERE {{query_windows:driver}}' AS right_sql,
    '{"driver":{"expression":"r.business_dt","date_type":"date"}}' AS right_query_windows_json,
    CAST(current_timestamp() AS TIMESTAMP_NTZ) AS updated_at
) AS s
ON t.pair_name = s.pair_name
WHEN MATCHED THEN UPDATE SET
  t.left_sql = s.left_sql,
  t.left_query_windows_json = s.left_query_windows_json,
  t.right_sql = s.right_sql,
  t.right_query_windows_json = s.right_query_windows_json,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
  pair_name, left_sql, left_query_windows_json,
  right_sql, right_query_windows_json, updated_at
) VALUES (
  s.pair_name, s.left_sql, s.left_query_windows_json,
  s.right_sql, s.right_query_windows_json, s.updated_at
);

-- # catalog.schema.pair_recon_run_status
-- Normally written by PairRecon. This example is for repair/testing only.
MERGE INTO catalog.schema.pair_recon_run_status AS t
USING (
  SELECT
    'example-orders-owner-20261021' AS run_id,
    'orders' AS pair_name,
    'example.owner' AS owner,
    'running' AS state,
    'America/New_York' AS timezone,
    CAST('2026-10-21 12:00:00' AS TIMESTAMP_NTZ) AS started_at,
    CAST(current_timestamp() AS TIMESTAMP_NTZ) AS updated_at,
    CAST(NULL AS TIMESTAMP_NTZ) AS ended_at,
    0 AS row_total,
    0 AS row_matches,
    0 AS row_differences,
    0 AS column_total,
    0 AS column_matches,
    0 AS column_differences,
    0 AS runtime_failures
) AS s
ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
  t.pair_name = s.pair_name,
  t.owner = s.owner,
  t.state = s.state,
  t.timezone = s.timezone,
  t.started_at = s.started_at,
  t.updated_at = s.updated_at,
  t.ended_at = s.ended_at,
  t.row_total = s.row_total,
  t.row_matches = s.row_matches,
  t.row_differences = s.row_differences,
  t.column_total = s.column_total,
  t.column_matches = s.column_matches,
  t.column_differences = s.column_differences,
  t.runtime_failures = s.runtime_failures
WHEN NOT MATCHED THEN INSERT (
  run_id, pair_name, owner, state, timezone, started_at, updated_at, ended_at,
  row_total, row_matches, row_differences,
  column_total, column_matches, column_differences, runtime_failures
) VALUES (
  s.run_id, s.pair_name, s.owner, s.state, s.timezone,
  s.started_at, s.updated_at, s.ended_at,
  s.row_total, s.row_matches, s.row_differences,
  s.column_total, s.column_matches, s.column_differences, s.runtime_failures
);

-- # catalog.schema.pair_recon_run_error
-- Normally replaced by PairRecon when a run finishes. This example is for repair/testing only.
MERGE INTO catalog.schema.pair_recon_run_error AS t
USING (
  SELECT
    'example-orders-owner-20261021' AS run_id,
    'hash_sum' AS check_name,
    'orders' AS pair_name,
    '2026-10-01' AS vintage,
    'AMOUNT' AS column_name,
    CAST(NULL AS STRING) AS partition_value,
    '12345' AS left_value,
    '12346' AS right_value,
    'Example mismatch' AS detail,
    '{"verdict":"diff"}' AS payload,
    CAST('2026-10-21 12:05:00' AS TIMESTAMP_NTZ) AS created_at
) AS s
ON t.run_id = s.run_id
 AND t.check_name = s.check_name
 AND t.pair_name = s.pair_name
 AND t.vintage <=> s.vintage
 AND t.column_name <=> s.column_name
 AND t.partition_value <=> s.partition_value
WHEN MATCHED THEN UPDATE SET
  t.left_value = s.left_value,
  t.right_value = s.right_value,
  t.detail = s.detail,
  t.payload = s.payload,
  t.created_at = s.created_at
WHEN NOT MATCHED THEN INSERT (
  run_id, check_name, pair_name, vintage, column_name, partition_value,
  left_value, right_value, detail, payload, created_at
) VALUES (
  s.run_id, s.check_name, s.pair_name, s.vintage, s.column_name, s.partition_value,
  s.left_value, s.right_value, s.detail, s.payload, s.created_at
);

-- # catalog.schema.pair_recon_progress_snapshot
-- PairRecon keeps one current progress row per daily run_id. This example is for repair/testing only.
MERGE INTO catalog.schema.pair_recon_progress_snapshot AS t
USING (
  SELECT
    CAST('2026-10-21 12:03:00' AS TIMESTAMP_NTZ) AS snapshot_ts,
    'example-orders-owner-20261021' AS run_id,
    'orders' AS pair_name,
    'attributes' AS state,
    CAST(30 AS BIGINT) AS units_expected,
    CAST(30 AS BIGINT) AS units_ready,
    CAST(12 AS BIGINT) AS units_complete,
    CAST(0 AS BIGINT) AS units_failed,
    CAST(4 AS BIGINT) AS worker_threads,
    'Example progress snapshot' AS detail
) AS s
ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
  t.snapshot_ts = s.snapshot_ts,
  t.pair_name = s.pair_name,
  t.state = s.state,
  t.units_expected = s.units_expected,
  t.units_ready = s.units_ready,
  t.units_complete = s.units_complete,
  t.units_failed = s.units_failed,
  t.worker_threads = s.worker_threads,
  t.detail = s.detail
WHEN NOT MATCHED THEN INSERT (
  snapshot_ts, run_id, pair_name, state, units_expected, units_ready,
  units_complete, units_failed, worker_threads, detail
) VALUES (
  s.snapshot_ts, s.run_id, s.pair_name, s.state, s.units_expected, s.units_ready,
  s.units_complete, s.units_failed, s.worker_threads, s.detail
);
