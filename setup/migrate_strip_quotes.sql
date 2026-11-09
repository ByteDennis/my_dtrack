-- Deploy the updated Left and Right code before enabling this rule.
MERGE INTO ${catalog}.${schema}.pair_recon_normalization AS t
USING (
  SELECT 'strip_quotes' AS rule_name, 'all' AS source,
         CAST(NULL AS STRING) AS match_value, CAST(NULL AS STRING) AS canonical_value,
         true AS enabled
) AS s
ON t.rule_name = s.rule_name AND t.source = s.source
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
