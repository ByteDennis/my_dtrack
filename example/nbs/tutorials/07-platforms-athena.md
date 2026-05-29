# 07 — `platforms/athena.py`

Athena is the **reference implementation** of the gen-then-run extraction
shape. Oracle, Hadoop, and CSV all model their runners on this one. If you
understand the Athena module you can read any of the others in a sitting.

The module is ~2400 lines. The big ones to know:

- `extract_aws(config_path, outdir, types, …)` — the public entry point.
- `_extract_cols_for_table(tbl_cfg, outdir, max_workers, …)` — the per-table
  col-stats SQL builder.
- `_write_combined_sql(aws_tables, outdir, extract_type, …)` — emits
  `extract_row.sql` / `extract_col.sql`.
- `run_sql_file(sql_path, outdir, max_workers, …)` — reads back the SQL file
  and runs each block via pyathena with a thread pool.
- `athena_connect(database, …)`, `aws_creds_renew(…)` — connection layer.
- `AthenaBuilder` — the `PlatformBuilder` subclass.

## 1. Authentication & connection

```python
aws_creds_renew(ttl_minutes=50, retries=3, retry_delay=10)
athena_connect(data_base=None, *, region=None, work_group=None, …)
```

`aws_creds_renew` refreshes STS credentials via the user's configured
`mfa_serial` / `role_arn` (from `.env`). Called once at the start of every
extractor run and once at the start of each new thread because pyathena's
connection objects are not credential-aware after the role expires.

`athena_connect` returns a pyathena cursor configured against `data_base`
(the Athena database/catalog). When `data_base` is None, the connection
uses `AWS_ATHENA_WORKGROUP` and falls back to the workgroup default.

## 2. Dialect helpers

### 2.1 `_athena_date_cast(date_expr, date_dtype, date_format=None)`

Converts a column reference into a `DATE` value for use in `DATE_TRUNC`. The
heuristic:

- `DATE` / `TIMESTAMP` → no cast.
- `VARCHAR` with `'YYYY-MM-DD'` → `from_iso8601_date(col)`.
- `VARCHAR` with `'YYYYMMDD'` → `date_parse(col, '%Y%m%d')`.
- `VARCHAR` with `'DDMONYYYY'` → `date_parse(col, '%d%b%Y')`.
- Numeric `YYYYMMDD` → `date_parse(cast(col as varchar), '%Y%m%d')`.
- Numeric `YYYYMM` → first-of-month derivation.

### 2.2 `_vintage_date_expr_athena(date_expr, vintage, …)`

Wraps a date expression in the Trino `DATE_TRUNC('month', …)` family
(see `VINTAGE_PRESETS['aws']`). For `sample` and `all` vintages, it returns
the raw expression so the GROUP BY reduces to one bucket.

### 2.3 `_format_athena_date_bound(date_str, date_type, is_upper)`

Renders a single date literal in the column's stored shape. `is_upper=True`
makes the bound exclusive-style for `<= upper` — needed when comparing
strings whose ISO ordering matches calendar ordering.

## 3. Discovery & metadata

`_discover_columns_athena(cursor, database, table)` runs `DESCRIBE` against
the table and yields `(column_name, data_type)` pairs. Stored into
`_column_meta` by `discover_aws_columns(config_path, outdir, db_path)`.

The CLI's `dtrack gen-aws` calls this once per table before generating
extract SQL — without it the col builder doesn't know which columns are
numeric, dates, or strings.

## 4. Row-extraction SQL

The row SQL is shape:

```sql
-- @qname=analytics_db.trades
-- @type=row
SELECT date_trunc('day', trade_date) AS dt,
       COUNT(*) AS row_count
FROM analytics_db.trades
WHERE trade_date >= DATE '2026-01-01' AND trade_date <= DATE '2026-04-30'
  AND ${user-supplied WHERE}
GROUP BY date_trunc('day', trade_date)
ORDER BY dt;
```

The `-- @qname=…` and `-- @type=…` markers are how `parse_sql_file` (below)
re-attaches each result block to its `<qname>_row.csv`.

## 5. Column-extraction SQL

The expensive part. For each (table, column) pair the builder emits a block:

```sql
-- @qname=analytics_db.trades
-- @col=price
-- @type=col
WITH base AS (
  SELECT ${vintage_expr} AS dt, ${col_with_sentinels} AS v
  FROM analytics_db.trades
  WHERE ${date_filter} AND ${where_left}
)
SELECT
  dt,
  'price' AS column_name,
  '${col_type}' AS col_type,
  COUNT(*) AS n_total,
  COUNT_IF(v IS NULL) AS n_missing,
  approx_distinct(v) AS n_unique,
  CAST(NULL AS DOUBLE) AS mean,    -- never emitted
  CAST(NULL AS DOUBLE) AS std,
  CAST(MIN(v) AS VARCHAR) AS min_val,
  CAST(MAX(v) AS VARCHAR) AS max_val,
  ${top10_subquery_or_NULL} AS top_10,
  AVG(LENGTH(CAST(v AS VARCHAR))) AS length_avg,
  MAX(LENGTH(CAST(v AS VARCHAR))) AS length_max,
  SUM(${hash16_expr}) AS hash_sum
FROM base
GROUP BY dt
ORDER BY dt;
```

Knobs:

- The preset (Quick / Standard / Detailed) decides which stats are computed
  vs emitted as NULL.
- `_athena_portable_hash16(value_sql)` produces the engine-specific 16-bit
  hash (tutorial 06 §5).
- `apply_null_sentinels` wraps the column reference with the per-platform
  sentinel list.

The `top_10` block is the most expensive. When the Detailed preset asks for
it, the builder emits a correlated sub-query:

```sql
(SELECT ARRAY_AGG(v_kv ORDER BY cnt DESC) FROM (
   SELECT v || '=' || CAST(cnt AS VARCHAR) AS v_kv, cnt
   FROM (SELECT v, COUNT(*) AS cnt FROM base WHERE v IS NOT NULL
         GROUP BY v ORDER BY cnt DESC LIMIT 10)
)) AS top_10
```

`top_10` is a single VARCHAR holding `val=cnt|val=cnt|…` — easier to write
to CSV than a structured array.

## 6. The lifeboat path

`_build_lifeboat_col_sql_athena(col_name, col_q, col_dtype, …)` produces a
**conservative** version of the col block: no CTE chain, no top_10, no
approx_distinct, just `MIN/MAX/COUNT`. When the main SQL fails with
`Query exhausted resources` (per `PLATFORM_RUNNER['athena']['lifeboat_errors']`),
the runner retries with this version.

The lifeboat block is bracketed with markers:

```sql
-- @degrade_sql
${lifeboat_sql}
-- @end_degrade
```

`run_sql_file` knows to skip the lifeboat when the main path succeeds and to
fall back to it on the matching error. The `partial` column of the CSV
gets `'athena_degraded'` (from `PLATFORM_RUNNER['athena']['degraded_label']`)
so the comparison engine knows not to trust the per-column stats from that
row at full fidelity.

## 7. `parse_sql_file(sql_path)`

Returns a list of `block` dicts:

```python
{
  'name': 'analytics_db.trades/price' | 'analytics_db.trades',
  'type': 'col' | 'row',
  'qname': 'analytics_db.trades',
  'sql':   '<main SQL>',
  'lifeboat_sql': '<degraded SQL>' or None,
  'conn_macro': None,   # athena reuses connection per database
}
```

Markers parsed:

- `-- @qname=…` / `-- @col=…` / `-- @type=…`
- `-- @degrade_sql` / `-- @end_degrade`
- `-- @conn_macro=…` (used by Oracle / Hadoop, not Athena)

The parser is intentionally line-based and forgiving — comments inside the
SQL body don't break it.

## 8. `run_sql_file(sql_path, outdir, max_workers, …)`

The runner skeleton:

```python
blocks = parse_sql_file(sql_path)
set_athena_cache(os.path.join(outdir, ".cache.db"))
# Group by qname so per-table CSVs are emitted as the last column finishes.
table_blocks = {qname: [block, …]}

with ThreadPoolExecutor(_max_workers) as pool:
    futures = [pool.submit(_execute_one_block, b) for b in blocks]
    for fut in as_completed(futures):
        result = fut.result()
        on_progress(result)
        merge_into_csv(result)
```

Per-block execution:

1. Check `cancel_event` — if set, skip with `cancelled=True`.
2. `cache_get(sql, scope=qname)`; if hit, return cached rows.
3. Submit via `athena_connect(database=qname.split('.')[0]).cursor().execute(sql)`.
4. On `Query exhausted resources` (or any `lifeboat_errors` substring),
   retry with `lifeboat_sql` and mark `partial='athena_degraded'`.
5. `cache_put(sql, scope=qname, rows)`.
6. Stream `(qname, dt, col, …)` into the per-table accumulator; flush to
   `<qname>_col.csv` when the last block for that qname completes.

The runner accepts an `on_progress(result)` callback — used by the SSE
endpoint in `web/app.py` to stream per-block progress to the browser.

It also accepts a `cancel_event` (`threading.Event`) so the web layer can
short-circuit queued blocks (in-flight queries finish because pyathena
doesn't expose a cheap interrupt, but the worker pool stops dispatching).

## 9. Resume mode

`resume=True` (col mode only): before submitting a block, the runner reads
`<qname>_col.csv` if it exists and **skips any block whose column already
has rows in the CSV**. The preloaded rows are merged with newly-extracted
rows on flush. That makes "Resume" buttons in the web UI cheap to re-click
when only one or two columns failed.

## 10. Cost-aware behavior

Two flags in the constants of this module matter for production:

- `_athena_cache_db_default = ".cache.db"` — every gen-aws run drops this
  file next to the CSVs. Tutorial 06 §1 explains the cache mechanics.
- `set_athena_cache(path)` — public function used by the test suite to point
  the cache at a temp directory.

The `id_card` cache (a small per-table summary written by
`_write_id_card_cache(cache_db_path, qname, n_total, payload)`) holds the
output of `_probe_id_cols_athena` so the column-mapping UI doesn't reprobe
identity columns on every page load.

## 11. Mock mode

`_extract_aws_mock(config_path, outdir, types, db_path, mock_dir)` and
`_extract_aws_mock_with_where(…)` are the bypass for `DTRACK_ATHENA_MOCK`.
They read CSV fixtures from `mock_dir/<qname>_*.csv`, apply the date filter
in Python, and write to `outdir/<qname>_*.csv`. End-to-end tests use this
path so they don't need AWS credentials.

## 12. Quick mental model

```
extract_aws(config, outdir, …)
   │
   ├─ _write_combined_sql(aws_tables, outdir, 'row', …)   ─► extract_row.sql
   ├─ _write_combined_sql(aws_tables, outdir, 'col', …)   ─► extract_col.sql
   │
   └─ run_sql_file(<each>, outdir, workers, …)
         │
         ├─ parse_sql_file       (block list)
         ├─ ThreadPoolExecutor   (one future per block)
         │     ↓
         │  ┌─ cache_get?
         │  │  yes → return cached
         │  │  no  → athena execute → cache_put
         │  └─ on degrade_error → retry with lifeboat_sql
         │
         └─ merge per-block rows into <qname>_*.csv
```

Read this picture before `databricks.py` (next file) — Databricks
deliberately took shortcuts the Athena module didn't.
