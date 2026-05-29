# 08 — `platforms/databricks.py`

The Databricks runner is younger and **shorter** than Athena's (~485 lines vs
~2400) because it borrows the Athena SQL templates wholesale and only swaps
the dialect bits that have to change. The bargain you pay: there is **no
`extract_*.sql` intermediate file** — SQL is built and executed in one pass.

This tutorial explains the dialect adaptations, the authentication shape,
the fallback path that calls into the Athena builder, and why the
`/api/extract/run-sql` integration needs a special branch (see tutorial 11).

## 1. Authentication

```python
databricks_connect(catalog_schema=None)
```

Reads two env vars (raises if either is missing):

- `DB_PROFILE` — a profile name in `~/.databrickscfg` used by
  `databricks.sdk.core.Config`.
- `DB_HTTPS` — the HTTP path of the SQL warehouse
  (`/sql/1.0/warehouses/<id>`).

Authentication shape:

1. `Config(profile=…)` resolves OAuth client credentials from `~/.databrickscfg`.
2. `cfg.authenticate()` returns an HTTP `Authorization` header; the bearer
   token is extracted via string-split (databricks-sdk's public API doesn't
   expose the raw token).
3. The token + host + http_path are passed to
   `databricks.sql.connect(server_hostname, http_path, access_token)`.

There's also a vestigial `aws_creds_renew()` call at the top because
Databricks SQL warehouses sit in the same VPC as the Athena workloads on
this user's box, and renewing STS lets the connection reach S3-backed
external tables. It's a no-op on machines without an AWS configuration.

### 1.1 `_resolve_catalog_schema(conn_macro)`

The UI lets the user type either `catalog.schema` or just `schema`. When
they type only a schema, the resolver prepends `DB_DEFAULT_CATALOG` from env.
If that's also missing, the bare schema is forwarded as-is — Databricks SQL
treats it as a USE SCHEMA.

## 2. Discovery

```python
_discover_columns_databricks(cursor, catalog, schema, table)
  → {column_name: data_type}
```

Reads from `<catalog>.information_schema.columns`. Note the catalog must be
explicit — Databricks' Unity Catalog requires it. Result is the dict the
col-stats builder consumes to decide numeric/string/date branches.

## 3. The dialect adaptation matrix

Per the May-2026 dialect parity audit (see `tests/test_hash16_dialect_parity.py`
and the existing memory note), Athena→Databricks needs **six** SQL function
swaps:

| Stat                | Athena (Trino)                  | Databricks (Spark SQL)           |
| ------------------- | ------------------------------- | -------------------------------- |
| n_unique            | `approx_distinct(col)`          | `approx_count_distinct(col, 0.023)` (rsd matches Trino default ~2.3%) |
| hash_sum            | `from_base(SUBSTR(md5(…),1,4), 16)` | `conv(SUBSTR(md5(…),1,4), 16, 10)` |
| md5 wrap            | `to_utf8(col)` before md5       | omitted — Spark `md5` accepts string directly |
| md5 → bytes         | `to_hex(md5(…))`                | omitted — Spark `md5` returns hex string |
| Date parse          | `date_parse(col, '%Y%m%d')`     | `to_date(col, 'yyyyMMdd')`       |
| Timestamp parse     | `from_iso8601_timestamp(col)`   | `to_timestamp(col)`              |

Item-1 is the gotcha: Spark's default `approx_count_distinct(col)` uses
~5% relative error vs Trino's ~2.3%, producing a measured 3% drift on
large-cardinality columns. The Databricks builder always passes `0.023`
explicitly so the two sides agree.

## 4. The portable hash

```python
def _databricks_portable_hash16(value_sql):
    return (
        f"CAST(conv(SUBSTR(LOWER(md5(TRIM({value_sql}))), 1, 4), "
        f"16, 10) AS BIGINT)"
    )
```

Side-by-side with Athena (`_athena_portable_hash16`), this is the cleanest
demonstration of the dialect swap. The semantics are identical: take the
md5 hex string, slice the first 4 characters, parse them as hex into an
integer. The same SUM-of-hashes invariant holds — matching `hash_sum`
columns mean the multisets match across engines.

Tests in `tests/test_hash16_dialect_parity.py` verify byte-level parity
between the two against a fixed corpus.

## 5. The col-stats path

```python
_extract_cols_for_table_databricks(tbl_cfg, outdir, max_workers, db_path, vintage, preset, stats_override)
```

For each column in the table:

1. Look up the column's `data_type` from `_discover_columns_databricks`.
2. Decide numeric vs string vs date via `is_numeric_type` / type
   string-matching.
3. Try the **Athena-style** SQL via `_extract_col_athena_accepts_query()` —
   a tiny adapter that takes the Athena builder's output and rewrites the
   six function calls to their Spark equivalents.
4. If the rewrite path can't handle the column (e.g. unusual type), fall
   back to `_query_databricks_fallback(extract_cfg, col, dtype, full_table, …)`
   which builds a more conservative Spark SQL from scratch.

The `_log_sql(label, sql)` helper writes every emitted SQL to a per-qname
log file when `DTRACK_LOG_DBX_SQL` is set, useful for debugging dialect
issues.

## 6. The row-extraction path

`_build_row_sql_databricks(tbl_cfg)` emits:

```sql
SELECT DATE_TRUNC('DAY', trade_date) AS dt,
       COUNT(*) AS row_count
FROM catalog.schema.trades
WHERE ${user_where} AND ${date_bounds}
GROUP BY DATE_TRUNC('DAY', trade_date)
ORDER BY dt
```

`_extract_rows_for_table_databricks(tbl_cfg, outdir)` runs that block and
writes `<qname>_row.csv` directly — no parsing step.

## 7. Public entry point

```python
extract_databricks(config_path, outdir, types=None, max_workers=None,
                   db_path=None, vintage=None, force=False,
                   from_date=None, to_date=None,
                   preset=None, stats_override=None)
```

Walks `dbx_tables = [t for t in all_tables if (t.source or '').lower() == 'databricks']`.
For each, runs row + col extraction (gated on `types`). Writes a `_timing.csv`
under `outdir` at the end summarizing per-table elapsed seconds.

What it **does not** do:

- Does not write `extract_row.sql` / `extract_col.sql`. That's why the web
  layer's `/api/extract/run-sql` had a bug routing `platform="databricks"`
  to Athena — the file the runner expected didn't exist. The fix routes
  databricks straight to `extract_databricks` and skips the file-existence
  check (tutorial 11 §3).
- Does not accept `on_progress` / `cancel_event`. The web layer tees stdout
  into the SSE stream instead (tutorial 11 §4). Adding native progress
  hooks is on the roadmap; until then, per-table prints arrive in the UI
  log via the stdout-tee.

## 8. Mock mode

`DTRACK_DATABRICKS_MOCK=<dir>` (or `DTRACK_MOCK=<dir>`): `extract_databricks`
delegates to `_extract_mock_tables` (tutorial 06 §9) instead of opening a
connection. Tests in `tests/e2e/test_10_databricks.py` use this path.

## 9. Caveats worth memorizing

- **No SQL file means no `Resume` button.** The Athena/Oracle UI re-uses
  the existing SQL file to skip already-extracted blocks. Databricks
  re-extracts every column on every run. The cache (`platforms/base.cache_get`)
  still works, so re-runs with the same SQL+scope hit the SQLite cache, but
  there's no per-column "skip if CSV has rows" shortcut.
- **No native cancel.** `cancel_event.is_set()` is checked once at the top
  of `extract_databricks`. After it starts, the only way to cancel is to
  let it run.
- **HyperLogLog drift** if the `rsd=0.023` argument is ever dropped. The
  parity tests will catch it but the symptom (3% n_unique drift on big
  columns) is easy to misdiagnose as data drift.

## 10. The fallback shape, side-by-side

```
                          column type known?
                                  │
                                  ▼
                  ┌─── Athena-style template works? ───┐
                  │                                     │
              yes │                                     │ no
                  ▼                                     ▼
   _extract_col_athena(*) → rewrite 6 fns      _query_databricks_fallback(*)
                  │                                     │
                  └─────────────► emit per-row dict ◄──┘
                                          │
                                          ▼
                                  insert into per-qname accumulator
                                          │
                                          ▼
                                  flush to <qname>_col.csv
```

The fallback writes simpler SQL: bare `MIN`/`MAX`/`COUNT`, no `top_10`,
matching the lifeboat shape Athena uses for the degrade path. If a Databricks
column drops to the fallback, `partial='databricks_degraded'` is stamped in
the CSV row — visible in the comparison report.

That's the Databricks story. Next file: `09-platforms-oracle-hadoop.md` —
the gen-then-run runners that look more like Athena but talk to Oracle and
Hive.
