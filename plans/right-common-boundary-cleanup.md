# Right Common Boundary Cleanup

## Goal

Keep `dtrack_right` standalone for Databricks while removing local CSV and non-Spark source
implementation from its runtime package.

## Behavior lock

- Left and Right key-family normalization remains identical.
- Left and Right Spark sampling predicates remain identical for fraction and fixed samples.
- Spark statistics and row/column reconciliation behavior remain unchanged.
- The local runner keeps its CSV stand-in and pandas reference statistics outside `dtrack_right`.

## Changes

1. Replace byte-identical common-module tests with normalization/hash/sample behavior parity tests.
2. Move `CsvAnchorTarget` to `dtrack_local` and use Left CSV helpers there.
3. Remove pandas, CSV sampling/statistics, and non-Spark sampling SQL from `dtrack_right`.
4. Enforce a small explicit `dtrack_right/common` inventory in packaging tests.
5. Update operator documentation and run unit, Spark, lint, build, and package-boundary checks.

## Non-goals

- No protocol payload changes.
- No sampling/hash algorithm changes.
- No report or S3/Volume layout changes.
