# 01 — Overview

`dtrack` is a data-reconciliation toolkit. The user wants to convince themselves
that **a table on platform A** (Oracle, Hadoop, SAS, Athena, Databricks, a CSV
folder) holds the **same data** as **a table on platform B**. dtrack does that
by extracting per-date row counts and per-column descriptive statistics from
both sides, persisting them in a single SQLite "project db", and then comparing
the two sides offline.

This first tutorial maps the package so the remaining 11 files have a frame
to hang off.

## 1. The big picture

```
   config (dtrack.json) ─────┐
   project.db (SQLite) ──────┼──► CLI (cli.py)  ──┐
                             │                    ├─►  per-platform extract
                             │     web UI (app.py)┘    (athena / databricks /
                             │                          oracle / hadoop / csv
                             │                          / sas)
                             │                                │
                             │                                ▼
                             │                       extract_row.sql
                             │                       extract_col.sql
                             │                       <table>_row.csv
                             │                       <table>_col.csv
                             ▼                                │
                       loader.py  ◄───────── load-row / load-col ◄─┘
                             │
                             ▼
                       compare.py ──► HTML / Excel reports
```

Every arrow in that picture is implemented by exactly one module — the goal of
this series is to explain each module on its own terms, then put them back
together in tutorial 05 (`pipeline.py`).

## 2. Package layout

```
dtrack/
├── __init__.py
├── cli.py            ← argparse dispatch, every `just …` recipe lands here
├── config.py         ← unified config loader + per-pair mutators
├── constants.py      ← STATS_PRESETS, CONNECTION_MACROS, DATA_SOURCES, …
├── db.py             ← SQLite schema + every read/write the rest of the code does
├── pipeline.py       ← orchestrates a full extract → load → compare cycle
├── loader.py         ← bulk-load CSVs produced by the extractors into project.db
├── compare.py        ← row + column comparison engine (offline, all in SQLite)
├── csv_compare.py    ← pandas-based "raw CSV diff" used by /csv_compare
├── stats.py          ← Python fallback stat computations (mock mode, CSV)
├── utils_stats.py    ← legacy SQL builders kept for backwards compat
├── date_utils.py     ← parse/normalize dates + vintage bucketing
├── html_export.py    ← HTML report renderer
├── interact.py       ← terminal prompts used by `cli.py` (skip pair, mapping…)
├── platforms/        ← per-platform connectors and SQL builders
│   ├── base.py       ← caching, qualified_name, dialect helpers
│   ├── athena.py     ← pyathena + Trino dialect
│   ├── databricks.py ← databricks-sql + Spark SQL dialect
│   ├── oracle.py     ← oracledb + Oracle dialect
│   ├── hadoop.py     ← pyodbc (Hive) + Hive dialect
│   ├── csv_duckdb.py ← in-process DuckDB engine over a CSV folder
│   ├── sas.py        ← shared SAS builder bits
│   ├── sas_libname.py← LIBNAME prelude generator
│   ├── sas_test.py   ← /api/playground/sas script generator
│   ├── hadoop_sas.py ← SAS proc-sql passthrough for Oracle + Hive
│   └── templates/    ← *.sas templates used by hadoop_sas
└── web/              ← FastAPI app + Jinja templates + static JS
    ├── app.py        ← all HTTP routes and SSE stream handlers
    ├── templates/    ← jinja2 templates (one per page)
    └── static/       ← page-specific JS, all framework-free
```

## 3. The two extraction shapes

There are **two** kinds of extractor in `dtrack/platforms/`. Tutorials 06–10
explain them in detail; the gist:

| Style                       | Platforms                       | What it does |
| --------------------------- | ------------------------------- | ------------ |
| **gen-then-run**            | Athena, Oracle, Hadoop, CSV     | Step 1 writes `extract_row.sql` / `extract_col.sql` from the config; step 2 reads that file back, parses blocks, dispatches to the platform's connector with a thread pool. The SQL file is the user-readable contract between the two steps. |
| **direct (no SQL file)**    | Databricks, SAS                 | The extractor builds SQL in memory and runs it block-by-block in one pass. There is no intermediate `extract_*.sql` you can inspect or edit. |

The web `/api/extract/run-sql` endpoint dispatches by platform string; if it
ever fails to recognize a platform it falls through to Athena (the historical
default). This is exactly the bug we fixed for Databricks in May 2026.

## 4. Two long-lived state stores

dtrack persists everything in **one** SQLite database (default `project.db`)
plus per-extractor **cache** databases (default `.cache.db` next to the
extracted CSVs):

| Store                              | Purpose                                              |
| ---------------------------------- | ---------------------------------------------------- |
| `project.db`                       | Authoritative state: pairs, row counts, col stats, comparisons, mapping overrides, playground history. Schema lives in `dtrack/db.py::_SCHEMA`. |
| `<outdir>/.cache.db`               | Per-outdir result cache. `cache_get(sql, scope)` short-circuits the engine call when the same SQL+scope produced a result before — invaluable for Athena where every re-run costs money. Cleared by deleting the file. |
| in-memory `_PLAYGROUND_CACHE`      | LRU of the last 10 playground results; backs `/api/playground/download/{key}` streaming. |
| in-memory `_PLAYGROUND_RUNNING`    | Lookup of cursor/conn handles for in-flight playground queries so cancel can interrupt them. |
| in-memory `_active_runs`           | Same idea for `/api/extract/run-sql` — keyed by run_id. |

## 5. CLI vs Web — same backend, different driver

The CLI (`cli.py`) and the web app (`web/app.py`) both call the **same**
extract/load/compare functions in `pipeline.py` and the platform modules.
The differences are:

- **CLI**: stdin/stdout, argparse, blocking. Good for cron, CI, and SAS shops
  that prefer a `dtrack run-full` line in a shell wrapper.
- **Web**: FastAPI, Server-Sent Events for live progress, an in-browser
  playground for ad-hoc SQL, and a UI for editing pairs / column mappings /
  date filters / column include/exclude.

The web app never duplicates business logic — when it accepts a request it
imports the same function the CLI would call.

## 6. Reading order for the rest of this series

If you're new to the codebase, walk the tutorials in order. If you're hunting a
specific change, the table below maps subsystems to tutorials:

| You want to change…                             | Read   |
| ----------------------------------------------- | ------ |
| The SQLite schema or `project.db` migrations    | `02-data-model.md` |
| Connection macros / preset definitions          | `03-config-constants.md` |
| A CLI command's flags or behavior               | `04-cli.md` |
| The `dtrack run-full` orchestration             | `05-pipeline.md` |
| Per-platform caching or dialect helpers         | `06-platforms-base.md` |
| AWS Athena extraction SQL or the runner         | `07-platforms-athena.md` |
| Databricks extraction (or dialect adaptations)  | `08-platforms-databricks.md` |
| Oracle / Hadoop / Hive / SAS-passthrough        | `09-platforms-oracle-hadoop.md` |
| CSV mode (DuckDB) or SAS code generation        | `10-platforms-csv-sas.md` |
| A web route, SSE endpoint, or the playground    | `11-web-backend.md` |
| A page's JS, button wiring, or persistence      | `12-web-frontend.md` |

## 7. Key invariants worth memorizing

These are repeated across the rest of the series:

1. **All numeric column stats are stored as TEXT** in `_col_stats`. Cross-source
   compatibility (Oracle's `NUMBER` vs Spark's `DOUBLE` vs DuckDB's `DECIMAL`)
   beats one decimal point of fidelity. Comparison code parses them at compare
   time via `_safe_int` / `_safe_float`.
2. **`source_table` is the qname** — i.e. `qualified_name(tbl_cfg)`, NOT the
   user-friendly `name`. It's the join key everywhere in `_col_stats`,
   `_row_counts`, `_column_meta`, `_metadata`. Tutorial 06 explains the
   qname rules.
3. **`pair_name` is the pair's stable identifier.** `_row_comparison`,
   `_col_comparison`, `_table_pairs`, `_sample_date` all key on it. The user
   sees it in every URL fragment.
4. **`dt` in `_col_stats` is the vintage bucket label**, not the original
   `date_var`. The mapping from a raw date to a bucket label lives in
   `date_utils.bucket_date(dt_str, vintage)`.
5. **The cache is platform-wide but scope-aware.** `cache_get(sql, scope)`
   keys on `sha256(sql + scope)` where `scope` is typically `qname` — a
   probe SQL for one table never collides with the same probe SQL for a
   different table.

The next tutorial dives into the SQLite schema in detail.
