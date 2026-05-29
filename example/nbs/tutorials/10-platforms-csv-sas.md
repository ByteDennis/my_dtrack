# 10 — CSV (DuckDB) and SAS code generation

Two surface areas in one tutorial because they're both **"engineless"** in
their own way:

- `platforms/csv_duckdb.py` runs entirely in-process with **DuckDB** — no
  network, no auth, no cluster. The "engine" is the DuckDB library.
- `platforms/sas.py` + `platforms/sas_libname.py` + `platforms/sas_test.py`
  + `platforms/hadoop_sas.py` **generate** SAS programs the user runs
  outside dtrack. dtrack itself never executes SAS code.

## 1. `platforms/csv_duckdb.py` — in-process CSV reconciliation

### 1.1 Why DuckDB

CSV mode treats a **folder of CSV files** as a database. DuckDB:

- Reads CSVs faster than pandas for >1M rows (vectorized I/O).
- Speaks ANSI-ish SQL so the col-stats builders can use the same shapes
  Athena does.
- Runs **in-process** — no service to set up, no port to open, no
  credentials to thread through.

This makes CSV mode the easiest way to validate dtrack's behavior end-to-end
without setting up a database. Every E2E test that doesn't depend on a real
warehouse uses it.

### 1.2 Connection

```python
duckdb_connect()
  -> a fresh in-memory DuckDB connection
```

`_query_duckdb(sql, conn_macro)` rewrites the SQL's `FROM analytics.trades`
clauses into `FROM read_csv_auto('/path/to/analytics/trades.csv')` via
`_rewrite_csv_from_clauses(sql)` so the runner doesn't need to be path-aware.

### 1.3 SQL builders

`_build_row_sql_csv(tbl_cfg, db_path, vintage)` and `_build_col_sql_csv(…)`
mirror the Athena builders. DuckDB's dialect is close to Trino:

- `date_trunc('month', col)` works.
- `approx_count_distinct(col)` works (no rsd parameter).
- `md5(col)` returns hex; the `hash16` shape uses
  `CAST(SUBSTR(md5(CAST(col AS VARCHAR)), 1, 4) AS INT128)` then mod-cast.

`csv_sql_filename(extract_type)` returns `extract_row.sql` / `extract_col.sql`
just like Athena.

### 1.4 Runner

`run_csv_sql_file(sql_path, outdir, max_workers=4, …)` is the gen-then-run
runner for CSV. Differences:

- The "connection" scope is the folder path, not a database.
- Workers default to 4 (CPU-bound).
- Lifeboat triggers cover DuckDB OOM strings (`Out of Memory`,
  `could not allocate`).
- Cache works the same way — `.cache.db` under the outdir.

### 1.5 Discovery

`discover_csv_columns(csv_path)` does `read_csv_auto(path, sample_size=N)`
and reads `cur.description` to discover types. Written to `_column_meta`
by `_discover_and_write_csv_columns(csv_tables, outdir, db_path)`.

### 1.6 The csv side per pair

CSV is the only source that can sit on **both sides** of a pair. The web UI
detects `hasCsvLeft` / `hasCsvRight` separately (`pairs.js` lines 1210–1212)
and runs two passes — one for the left outdir, one for the right — because
each side wrote its `extract_*.sql` to its own folder.

## 2. SAS code generation

dtrack generates SAS programs for two distinct use cases:

| Use case            | Module                  | Output                            |
| ------------------- | ----------------------- | --------------------------------- |
| Bulk extraction     | `platforms/hadoop_sas.py` | `extract_row.sas`, `extract_col.sas` for `dtrack run-full`  |
| Playground (manual) | `platforms/sas_test.py` | one-off `.sas` script the user submits in SAS Studio |

### 2.1 `platforms/sas.py` — the legacy builder ABC

Tiny module (~37 lines). Defines `class SASBuilder(PlatformBuilder)` which
delegates `stats_sql` / `top10_sql` to `utils_stats.build_column_sql(…,
is_oracle=True)`. Kept for `utils_stats.build_column_sql` polymorphism.
You almost never edit this file.

### 2.2 `platforms/sas_libname.py` — LIBNAME string builders

Pure string-builders. ~90 lines, no I/O, no DB. Heavy comments. Three knobs:

- `oracle_libname(alias, svc, …)` → `libname ORA oracle path=&svc user=&u pwd=&p;`
- `hadoop_libname(alias, server, host, schema, …)` → the SAS-Hadoop OSH
  LIBNAME statement with `jar_path` and `config_path` injected.
- `credential_macros()` → the `%let oracle_usr=…; %let oracle_pwd=…;` block.

`libname_prelude(oracle_aliases, hadoop_profiles)` is the canonical caller —
yields a SAS string that mounts every needed library before any data step.
`libname_clear(aliases)` cleans up at the end.

### 2.3 `platforms/sas_test.py` — `/api/playground/sas` script generator

The Playground page calls `gen_sas_script(kind, method, …)` and the user
gets back a single SAS program. Composition:

1. **Header**: `%let oracle_usr = ;` placeholder block via
   `_oracle_cred_block`.
2. **Connection prelude**: `_sas_hadoop_options(...)` for Hadoop, or an
   Oracle LIBNAME from `sas_libname.oracle_libname`.
3. **Body**: the user-supplied SQL or SAS code, wrapped depending on
   method:
   - `libname`: `data ora.tmp; set ora.t; run;` style, the user's code goes
     inside.
   - `procsql`: `proc sql; connect to oracle/hadoop (...); select * from
     connection to oracle/hadoop (...); quit;`
4. **Cleanup**: `libname_clear` releases the libref.

Sub-helpers:

- `_looks_like_sql_select`, `_looks_like_sas_block` — heuristic body
  detection so the same input can be a SELECT or a full data step.
- `_convert_sql_date_literals_to_sas(sql)` — `DATE '2026-01-15'` →
  `'15JAN2026'd`. Necessary because SAS proc-sql doesn't accept ANSI date
  literals.
- `_qualify_with_libref(sql, libref)` — prepends `LIBREF.` to bare table
  refs so the user can write `select * from trades` and get
  `select * from ORA.trades`.
- `_wrap_proc_sql(sql, outobs)` — wraps a bare SELECT in the `proc sql;`
  + `connect to` boilerplate and applies the `outobs=N` row cap if the
  user typed `LIMIT N` in their body.
- `_extract_row_limit(sql)` — pulls the `LIMIT N` out of the body before
  wrapping, since SAS proc-sql doesn't accept LIMIT.

The Playground page's "SAS (gen)" engine just calls this module and shows
the result. The user submits it in SAS Studio by hand — dtrack never
runs it.

### 2.4 `platforms/hadoop_sas.py` — bulk SAS generator

The other consumer of SAS templates. Tutorial 09 §3 covers this — it's the
SAS-code generator used by `dtrack run-full` for tables whose extraction
should happen inside SAS.

The split between `sas_test.py` and `hadoop_sas.py` is:

- `sas_test.py` produces **one** standalone script for **one** ad-hoc query.
- `hadoop_sas.py` produces **two** programs (`extract_row.sas`,
  `extract_col.sas`) driven by tables-of-tables.

Both modules share the same LIBNAME prelude (`sas_libname.py`) but otherwise
don't import each other.

## 3. SAS templates

Templates live in `dtrack/platforms/templates/`:

```
meta.sas          ← driver: defines the table-of-tables data step
rows_oracle.sas   ← per-row driver for Oracle-source tables
rows_hadoop.sas   ← per-row driver for Hadoop/Hive-source tables (via LIBNAME)
rows_sas.sas      ← per-row driver for native SAS datasets
columns.sas       ← per-column driver for Oracle/native SAS
column_hadoop.sas ← per-column driver for Hive
template.sas      ← the top-level skeleton that includes them
instruct.md       ← user-facing instructions written next to the program
```

`_render_col_template(tpl_name, rows, rows_placeholder, …)` is the renderer.
It loads the template as a plain string and substitutes:

- `{rows_placeholder}` → the per-table data lines (a SAS data-step assignment
  block).
- `{__LIBNAME_PRELUDE__}` → output of `libname_prelude(…)`.
- `{__SQL_CTE_PREFIX__}` → optional CTE pre-amble emitted by
  `_extract_col_oracle` / `_build_col_sql_hadoop`.

No Jinja, no Mako — string substitution is enough for the surface area.

## 4. Mock-mode behavior

CSV and SAS mode behave differently under `DTRACK_MOCK`:

- **CSV**: there's nothing to mock — the data is already in CSV. The mock
  variable is ignored; the runner reads the real folder.
- **SAS**: `_extract_sas_mock(config_path, outdir, types, db_path, mock_dir)`
  replaces the SAS-script generation with a Python-side CSV producer that
  mirrors the SAS output shape. Used by E2E tests that don't have SAS
  installed.

## 5. Practical advice

- For **demos** and tests: use CSV mode. The toolchain in
  `example/generate_mock_data.py` writes fixture CSVs to `example/csv/`.
- For **production**: use the database extractors (Athena, Databricks,
  Oracle, Hive). CSV mode is great for development but loses the SQL push-down
  optimizations a real warehouse provides.
- When **debugging dialect issues**: `DTRACK_LOG_DBX_SQL=1` (Databricks),
  `DTRACK_LOG_SQL=1` (universal) print every emitted SQL to a per-qname
  log file under the outdir.

Next file: `11-web-backend.md` — the FastAPI app that exposes all of this
over HTTP.
