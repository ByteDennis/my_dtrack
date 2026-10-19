-- Run once for a pair_recon_pair_sql table created before vintage selection was added.
ALTER TABLE ${catalog}.${schema}.pair_recon_pair_sql ADD COLUMNS (
  vintage_sample_size INT,
  vintage_values ARRAY<STRING>
);
