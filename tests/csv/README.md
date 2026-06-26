# `tests/csv/` — CSV↔CSV reconciliation suite

A `source=csv` pair is powered by **in-process DuckDB**, so the entire
reconciliation pipeline — generate SQL → run → load → compare — executes with
**no external database engine**. That makes CSV↔CSV the one place we can test
the whole flow on any host (Linux/Mac/CI), unlike `tests/api/` and
`tests/e2e/`, which need live Oracle/Athena/Hadoop connections and are
skipped off the Windows dev box.

This suite also **simulates CSV extracts "obtained from" other engines**
(Oracle, Hadoop, Databricks, Athena). Every platform's extractor writes the
*same* canonical output schema (`ROW_CSV_HEADERS` / `COL_CSV_HEADERS`); only
the raw `dt` formatting differs (Athena `2026-01-02`, Hadoop `20260102`,
Oracle `02-JAN-26`). We write those output CSVs directly and confirm the loader
canonicalises them so a pair whose two sides came from *different* engines
still reconciles correctly.

## Layout

```
tests/csv/
  conftest.py              # sandbox (sqlite db + outdirs) and FastAPI client fixtures
  helpers.py               # raw-data + canonical-output CSV writers, engine date rendering
  test_csv_pipeline.py     # full DuckDB gen→run→load→compare (row + col)
  test_engine_outputs.py   # simulate per-engine output CSVs; cross-engine reconcile
  test_api_csv.py          # HTTP: /api/csv_compare/* and /api/generate + /api/extract/run-sql
  js/
    helpers.mjs            # extract a named pure fn from static/*.js and eval it in a vm
    fmt.test.mjs           # row_compare.js formatters (fmtCount/fmtDiff/fmtRange/esc)
    diff_classifier.test.mjs  # col_compare.js _compHasDiff + csv_compare.js _cvEsc
```

## Running the Python tests (pytest)

```bash
# from the repo root, with the project's venv active
python -m pytest tests/csv/ -v
```

Requirements:

- **duckdb** — already a project dependency. `test_csv_pipeline.py` skips itself
  if it's somehow missing.
- **httpx** — needed only by `test_api_csv.py` (Starlette's `TestClient` speaks
  HTTP over httpx). It's a test-only extra; if it isn't installed those tests
  **skip** (they don't fail). To run them:

  ```bash
  pip install httpx
  ```

Run a single file or test:

```bash
python -m pytest tests/csv/test_engine_outputs.py -v
python -m pytest "tests/csv/test_csv_pipeline.py::test_full_pipeline_row_compare" -v
```

> Note: the directory is intentionally **not** a Python package (no
> `__init__.py`) — a `csv` package would shadow the stdlib `csv` module.
> pytest puts the directory on `sys.path`, so the tests import the local
> `helpers` module directly (`from helpers import ...`).

## Running the frontend JS tests (Node, no npm install)

The page scripts in `dtrack/web/static/*.js` are plain script-tag globals (no
ES modules / no build step). `js/helpers.mjs` extracts a single pure function
by name and evaluates just that snippet in a `node:vm` sandbox, so no DOM,
browser, or bundler is needed. Uses Node's built-in test runner — **nothing to
`npm install`**.

```bash
# Node 18+ (tested on Node 24)
node --test tests/csv/js/*.test.mjs
```

(`node --test tests/csv/js/` — passing the bare directory — is flaky across
Node versions; pass the glob as shown.)

What's covered:

- `fmtCount` / `fmtDiff` / `fmtRange` / `esc` — the row-compare display
  formatters.
- `_compHasDiff` + `_DIFF_FNS` — the column-compare **mismatch classifier**
  (the core reconciliation predicate on the frontend), driven by a stubbed
  `_getCompareFields()` to control the active "Compare fields" set.
- `_cvEsc` — the CSV-compare HTML escaper.

## What each test asserts

| File | Focus |
|------|-------|
| `test_csv_pipeline.py` | Real DuckDB extraction emits the canonical `dt,row_count` and 14-field col CSVs; loading + `compare_row_counts` / `compare_column_stats` correctly flag a known row-count and `min`/`n_unique` divergence; identical inputs reconcile clean. |
| `test_engine_outputs.py` | The loader canonicalises Athena/Hadoop/Oracle/Databricks date formats to `YYYY-MM-DD`; a pair split across two engines reconciles when data matches and surfaces a real count/unique drift when it doesn't. |
| `test_api_csv.py` | `/api/csv_compare/inspect` + `/run` over multipart uploads; `/api/generate` writes `extract_row.sql` for a csv pair and `/api/extract/run-sql` (platform=csv) executes it into `{qname}_row.csv`. |
| `js/*.test.mjs` | The pure display + mismatch-classification helpers the compare UI relies on. |
