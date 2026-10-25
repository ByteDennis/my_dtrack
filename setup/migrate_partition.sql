-- Run once for an existing pair_recon_pair_meta table, then refresh it from pairs.xlsx.
ALTER TABLE ${catalog}.${schema}.pair_recon_pair_meta ADD COLUMNS (
  partition STRING
);

UPDATE ${catalog}.${schema}.pair_recon_pair_meta
SET partition = CASE
  WHEN get_json_object(run_defaults_json, '$.partition_mode') = 'all' THEN 'all'
  WHEN get_json_object(run_defaults_json, '$.partition_grain') = 'month' THEN 'month'
  ELSE 'day'
END
WHERE partition IS NULL;
