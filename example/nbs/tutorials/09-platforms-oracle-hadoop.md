# 09 — Oracle, Hadoop / Hive, and the SAS bridge

This tutorial covers three modules that share a shape but differ in dialect
and connector:

- `dtrack/platforms/oracle.py` (~950 lines) — Python-direct extraction via
  `oracledb`.
- `dtrack/platforms/hadoop.py` (~1200 lines) — Hive via `pyodbc` against an
  ODBC DSN.
- `dtrack/platforms/hadoop_sas.py` (~2450 lines) — SAS code generator for
  the same tables, used when the user prefers to run extraction inside SAS.

All three implement the **gen-then-run** model: a `_write_combined_*_sql`
function emits the per-block SQL into `extract_row.sql` / `extract_col.sql`,
and a `run_*_sql_file` reads it back and dispatches. The shape mirrors
`platforms/athena.py` (tutorial 07); this tutorial focuses on what's
different.

## 1. Oracle (`platforms/oracle.py`)

### 1.1 Connection

`oracle_connect(conn_macro, *, user=None, password=None)` in `db.py`
(not `oracle.py`) is the connection layer. It:

1. Resolves the macro through `resolve_oracle_macro(conn_macro)` which
   merges `MACRO2SVC` (compile-time) with `DTRACK_ORACLE_MACROS` (env-time).
2. Looks up `<MACRO>_USER` / `<MACRO>_PWD` env vars when user/password not
   passed explicitly.
3. Calls `oracledb.connect(user=…, password=…, dsn=…)`.

`oracledb` is the modern driver; the older `cx_Oracle` is not used. Thick
mode is enabled when `ORACLE_LIB_DIR` is set (the SAS server image needs
it because Oracle Instant Client lives in a non-standard path).

### 1.2 Dialect helpers (Oracle-specific)

- `_oracle_portable_hash16(value_sql)` — `STANDARD_HASH(value, 'MD5')` →
  RAW → `TO_NUMBER(SUBSTR(rawtohex(…), 1, 4), 'XXXX')` cast to NUMBER.
- `_vintage_date_expr_oracle(date_col_q, vintage, date_dtype, date_format)`
  — uses `TRUNC(col, 'IW')` / `TRUNC(col, 'MM')` etc. (see
  `VINTAGE_PRESETS['oracle']` in constants).
- `_build_lifeboat_col_sql_oracle` — strips the freq-table CTE chain when
  Oracle hits `ORA-01652` (temp tablespace exhausted) or `ORA-04030` (process
  out of memory).

### 1.3 Per-table col-stats builder

`_build_col_sql_oracle(tbl_cfg, col, dtype, full_table, …)` emits the same
14-stat shape as Athena but in Oracle dialect. Key differences:

- Oracle's `COUNT(DISTINCT col)` is exact, not approximate — there is no
  `approx_distinct`. For very-high-cardinality columns this can be slow
  enough to trigger the lifeboat path.
- String literals use single quotes; identifiers use double quotes.
- `LENGTH` works on both VARCHAR2 and CLOB.

The CTE chain for Detailed-preset stats:

```sql
WITH base AS (SELECT col v FROM tbl WHERE …),
     freq AS (SELECT v, COUNT(*) cnt FROM base WHERE v IS NOT NULL GROUP BY v),
     ranked AS (SELECT v, cnt, DENSE_RANK() OVER (ORDER BY cnt DESC) rnk FROM freq)
SELECT … FROM base, freq, ranked WHERE rnk <= 10
```

`utils_stats.py` carries the legacy Oracle SQL builders. New extractions
use the ones in `oracle.py`; `utils_stats.py` is kept for the SAS code
generator's IN-SAS-PROCSQL fallback.

### 1.4 `run_oracle_sql_file(sql_path, outdir, max_workers, …)`

Same skeleton as `athena.run_sql_file`. Key differences:

- Each block carries `@conn_macro=…` because Oracle connections are
  macro-scoped. The runner opens one connection per macro and pools cursors.
- Default `ORACLE_MAX_WORKERS=2` (vs Athena 8). Each Oracle session is a
  whole DB process; over-parallelism hurts.
- The cache scope is the macro, not the database.

### 1.5 Public entry

`extract_oracle(config_path, outdir, types, …)`:

1. `_write_combined_row_sql_oracle(oracle_tables, outdir, db_path, vintage)`
2. `_write_combined_col_sql_oracle(oracle_tables, outdir, db_path, preset, stats_override)`
3. `run_oracle_sql_file(<each>, outdir, …)`

Mock-mode env: `DTRACK_ORACLE_MOCK` — same shape as Athena's mock fixture
directory.

## 2. Hadoop / Hive (`platforms/hadoop.py`)

### 2.1 Connection

```python
hadoop_connect(dsn: Optional[str] = None, *, autocommit=True)
```

Uses `pyodbc.connect('DSN=<name>', autocommit=True)`. Three knobs:

- Empty `dsn` → reads `HADOOP_DSN` env, defaulting to `Hive_Prod`.
- `SSL_CERT_FILE` is **temporarily unset** around the connect call; the
  Hive ODBC driver on this user's box gets confused if cert verification
  is on for the bootstrap. Restored after `connect` returns.
- DSN-not-found errors get a friendly "the DSN must be defined in `/etc/odbc.ini`
  or `~/.odbc.ini`" message.

### 2.2 The `approx_distinct` decision

Hive's `approx_distinct(col, rsd)` is recent. `_approx_distinct_enabled(tbl_cfg,
config)` reads two booleans (table-level then config-level) and falls back to
exact `COUNT(DISTINCT col)`. `_approx_distinct_error()` provides the
substring-list that triggers a fallback retry (`function 'approx_distinct'
not found`).

### 2.3 Library-vs-direct shape

Hive has two read modes:

- **HiveServer2 direct** (via ODBC): the default. Runs SQL synchronously.
- **LIBNAME-mounted** (when `tbl_cfg.libname.startswith("hdp")`): the table
  is exposed through a SAS LIBNAME and queried via SAS proc-sql passthrough.
  The col-stats SQL is generated by `_build_col_sql_hadoop` but then wrapped
  in SAS by `hadoop_sas.gen_sas`.

`_is_libname_table(tbl_cfg)` and `_libname_mount_for(tbl_cfg)` are the
predicates that route between the two.

### 2.4 Vintage & dates

Hive's `TRUNC` supports the Oracle-compatible truncations, so
`VINTAGE_PRESETS['hadoop']` mirrors `oracle`'s. But Hive's date arithmetic
is less forgiving:

- `_oracle_date_transform(date_col, transform)` applies the user-configured
  date transform (e.g. `TO_DATE(col, 'YYYYMMDD')`) before the truncation.
- `_format_date_bound(date_str, date_type, is_sas_src, is_upper, …)`
  handles the SAS-style `'15JAN2026'd` literal when the underlying source
  is read through a SAS libref.

### 2.5 Runner

`run_hadoop_sql_file(sql_path, outdir, max_workers, …)`:

- Default `HADOOP_MAX_WORKERS=4`.
- Lifeboat trigger list is broad (OOM, GC overhead, Container killed, etc.,
  see `PLATFORM_RUNNER['hadoop']['lifeboat_errors']`) because Hive surfaces
  the same root cause under many error strings.
- Each block carries `@conn_macro=…` (the DSN name). Block-level
  parallelism uses a single shared pyodbc connection per DSN with one
  cursor per worker — pyodbc serializes execute() calls per connection so
  this is fine.

### 2.6 The `HadoopBuilder` legacy path

`HadoopBuilder(PlatformBuilder)` is the polymorphic entry used by
`utils_stats.build_column_sql` when called with `source="hadoop"`. New
code paths call `_build_col_sql_hadoop` directly.

## 3. `platforms/hadoop_sas.py` — the SAS bridge

This module is **the largest** in `platforms/` and the trickiest. It
generates SAS programs (`extract_row.sas`, `extract_col.sas`) that, when run
inside SAS Studio, produce the same `<qname>_row.csv` / `<qname>_col.csv`
as the Python runners. Two reasons to use it:

- The SAS server has direct access to Oracle / Hive that the Python
  environment doesn't.
- The compliance team prefers SAS code over Python for the canonical
  extraction.

### 3.1 Code generation by template

Templates live in `dtrack/platforms/templates/`:

- `meta.sas` — table-of-tables driver.
- `rows_sas.sas`, `rows_oracle.sas`, `rows_hadoop.sas` — per-source row drivers.
- `column_hadoop.sas` — Hive-via-SAS col driver.
- `columns.sas` — Oracle/SAS col driver.
- `template.sas` — the entry skeleton.
- `instruct.md` — human-readable instructions written next to the
  generated programs.

`_render_col_template(tpl_name, rows, rows_placeholder, …)` reads the
template, substitutes `{rows_placeholder}` with the per-table data lines,
and writes the result. Each row line is `"qname","col","col_type","date_filter"`.

### 3.2 The bucket spec

`_compute_bucket_specs(tbl_cfg, db_path)` and `_compute_col_spec(tbl_cfg,
db_path)` build the per-table "what to extract" record. They read
`_row_comparison.matching_dates` so the SAS path agrees with the Python
runners on the bucket set.

### 3.3 Oracle inside SAS

When `tbl_cfg.libname` exists and the LIBNAME points at Oracle, the SAS
program uses `PROC SQL CONNECT TO oracle (USER=&u PASSWORD=&p PATH=&svc)`
and runs the SQL as **passthrough**. The col stats SQL is generated by
`utils_stats.build_column_sql(…, is_oracle=True)`. This is the only place
`utils_stats` is still load-bearing.

### 3.4 Hive inside SAS

`column_hadoop.sas` is the most complex template. It loops over a data step
of (qname, col, col_type) and emits one PROC SQL per column with a CONNECT
TO HADOOP block. The Hive SQL inside is generated by `_build_col_sql_hadoop`
in the **passthrough** mode (no SAS macro variables — the SAS code subs them
in by hand).

### 3.5 The `gen_sas` entry

```python
gen_sas(config_path, outdir, types, env_path, db_path, vintage, …)
```

Walks `sas_tables` (everything in the config that didn't match aws / oracle /
databricks). Writes:

- `extract_row.sas`
- `extract_col.sas`
- `instruct.md` — a markdown file describing what env vars / connections
  the SAS user must define.
- A `.sas` companion script per table for diagnostic runs.

The Python pipeline calls `gen_sas` first (tutorial 05 §4) so the SAS files
are ready by the time the user opens SAS Studio.

## 4. SAS LIBNAME prelude — `platforms/sas_libname.py`

Tiny module (~90 lines). Holds string-builders for SAS LIBNAME statements
(`libname ora oracle path=… user=… pwd=…;`) and credential macros (the
`%let oracle_usr = …` block). Used by `hadoop_sas.gen_sas` and by the
playground SAS endpoint.

## 5. The `/api/playground/sas` engine — `platforms/sas_test.py`

Different concern: this module generates **standalone** SAS scripts the user
can paste into SAS Studio to **test a single connection**. Driven by the
Playground page when the user picks "SAS (gen)" engine.

Public entry: `gen_sas_script(kind, method, …)`.

- `kind`: `"oracle"` or `"hadoop"`.
- `method`: `"libname"` or `"procsql"`.

Returns `{filename, content}` JSON. The filename encodes the connection
details (`<method>_<kind>_<server>.sas`). The content includes credential
placeholders (`%let oracle_usr = …`) that the user fills in before submitting.

The Playground UI shows the rendered script in the result card and a
Download button (`pgDownloadSAS`).

## 6. The CSV runner — `platforms/csv_duckdb.py`

CSV mode is the **odd one out**: in-process via DuckDB. Covered in tutorial
10 alongside the SAS code-gen surface; mentioned here because both
`extract_csv` and `run_csv_sql_file` adopt the same gen-then-run shape, and
the runner is plugged into `/api/extract/run-sql` exactly like Oracle / Hive.

## 7. Quick comparison

| Dimension              | Oracle               | Hive                       | Databricks       | Athena                    |
| ---------------------- | -------------------- | -------------------------- | ---------------- | ------------------------- |
| Connector              | `oracledb`           | `pyodbc` (DSN)             | `databricks-sql` | `pyathena`                |
| Identifier scope       | macro                | DSN                        | catalog.schema   | database                  |
| Workers default        | 2                    | 4                          | 4                | 8                         |
| Writes `extract_*.sql` | yes                  | yes                        | **no**           | yes                       |
| approx_distinct        | exact COUNT(DISTINCT)| approx_distinct (optional) | approx_count_distinct(rsd=0.023) | approx_distinct |
| Lifeboat trigger       | ORA-01652, -04030    | broad OOM list             | Job aborted, OOM | "Query exhausted resources" |

Next file: `10-platforms-csv-sas.md` — the in-process DuckDB engine and the
generated-SAS surface.
