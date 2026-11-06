-- Deploy the updated Left and Right code before enabling this rule.
MERGE INTO ${catalog}.${schema}.pair_recon_normalization AS t
USING (
  SELECT 'zz_missing_whitespace' AS rule_name, 'all' AS source,
         '@whitespace' AS match_value, '@missing' AS canonical_value,
         true AS enabled
) AS s
ON t.rule_name = s.rule_name AND t.source = s.source
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
