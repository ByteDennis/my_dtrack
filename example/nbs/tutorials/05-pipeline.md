# 05 — `pipeline.py`

`dtrack/pipeline.py` is the orchestrator behind `dtrack run` (and the
`/api/run` button on the web UI, when wired). It is exactly 220 lines and
imports nothing at module top — every dependency is imported inside the
function that needs it. The shape on purpose: `dtrack list` should not pay
for importing `pyathena`.

## 1. Entry point

```python
def run_pipeline(
    project_db, config_path,
    outdir=None, sas_outdir=None, csv_outdir=None,
    types=None, vintage=None, yes=False,
    html_row=None, html_col=None, title=None, subtitle=None,
    workers=None, force=False,
    skip_extract=False, skip_load=False, skip_compare=False,
    from_date=None, to_date=None,
    preset=None, stats_override=None,
):
```

`types` defaults to `["row", "col"]` — both passes happen by default.

The function is the canonical reference for "what runs in what order" and
"which platform writes where." The four steps are tagged with `# -- Step N --`
comments and correspond 1:1 to the four `--skip-*` flags.

## 2. Step 1 — init

Either creates a fresh project db or reuses the existing one. Nothing in
`run_pipeline` ever drops a table or column; `init` is idempotent because
`init_database` uses `CREATE TABLE IF NOT EXISTS`.

The user normally calls `dtrack init` once at project birth, then
`dtrack run …` for every refresh. This step is a safety net for the case
where the user pipes a fresh path into `--project-db`.

## 3. Classification — table → platform

```python
sas_tables, aws_tables, oracle_tables, databricks_tables = [], [], [], []
for tbl in tables:
    source = (tbl.get("source") or "").lower()
    processed = tbl.get("processed") or ""
    if processed.startswith("$"):
        sas_tables.append(tbl)
    elif source == "aws":          aws_tables.append(tbl)
    elif source == "oracle":       oracle_tables.append(tbl)
    elif source == "databricks":   databricks_tables.append(tbl)
    else:                          sas_tables.append(tbl)
```

Notes:

- `processed.startswith("$")` is the **SAS-shim escape hatch**: any
  upstream-processed dataset (regardless of `source`) routes through the
  SAS generator so that the upstream SAS step is preserved verbatim.
- The `else: sas_tables.append(tbl)` is intentional — `hadoop` and `sas`
  both fall through to `gen_sas` which dispatches further inside
  `hadoop_sas.py` based on `is_sas_table(tbl)`.
- No `csv` branch here because the in-process DuckDB engine is only invoked
  via `/api/extract/run-sql` from the web UI; the CLI pipeline does not
  yet read from raw CSV folders.

## 4. Step 2 — extract

One call per non-empty bucket. The order is deliberate:

1. `gen_sas` (writes `.sas` files, **no execution**).
2. `extract_oracle`.
3. `extract_aws` (Athena).
4. `extract_databricks`.

The SAS step intentionally runs first because the user typically launches
it manually in SAS Studio after the rest finishes; running it before the
Python extractors means the `.sas` files are ready by the time they're
needed.

All four extractors accept the same kwargs (`outdir`, `types`, `max_workers`,
`db_path`, `vintage`, `force`, `from_date`, `to_date`, `preset`, `stats_override`).
That uniformity is what makes the orchestrator trivial.

`force=True` invalidates the per-outdir cache (`.cache.db`). Without it, an
unchanged SQL block reuses a cached result from a previous run.

## 5. Step 3 — load

`_load_data(project_db, config, tables, csv_outdir, sas_outdir, types)`:
walks every table in the config, picks the **right folder** for that
table's source, and feeds the resulting CSV to the loader.

```python
folder = csv_outdir if source in ("aws", "oracle", "databricks") else sas_outdir
```

That single line is why Oracle / AWS / Databricks all write to the
"right-side outdir" and SAS / Hadoop write to the "left-side outdir" in the
web UI. The web app reads the same rule from `web/app.py::_side_outdir`.

For each table:

- **row**: `<folder>/{qname}_row.csv` → `loader.load_row_counts(mode="upsert")`.
  After loading, it prints how many date buckets landed.
- **col**: `<folder>/{qname}_col.csv` → `loader.load_precomputed_col_stats(
  mode="upsert", vintage=...)`. The vintage comes from the table config when
  set, else from `_metadata`, defaulting to `"day"`. This is what fills the
  `dt` column in `_col_stats` with bucket labels instead of raw dates.

`mode="upsert"` means: existing rows for the same composite key
(`source_table, dt`) get **replaced**, new rows get **inserted**. Idempotent.

## 6. Step 4 — compare

`_run_comparisons` calls **the CLI command handlers** directly via a synthetic
`argparse.Namespace`:

```python
from .cli import cmd_compare_row
args = Namespace(project_db=…, config=…, from_date=None, to_date=None, …)
cmd_compare_row(args)
```

This is deliberate (and is the only circular import in the package — but
it's resolved lazily so it works). The point: the orchestrator and the
direct `dtrack compare-row` invocation **share exactly one code path**.

That guarantees:

- HTML output uses the same renderer (`html_export.generate_*`).
- Skip-pair prompts work identically (`--yes` passes through).
- The persisted `_row_comparison` / `_col_comparison` rows are identical to
  what `dtrack compare-row` would have written.

If `vintage` was passed at orchestrator level, it propagates only to the
col compare step — row compare doesn't take a vintage.

## 7. The `--skip-*` flags

Three boolean flags, each gates one of steps 2-4:

| Flag             | Use when…                                                              |
| ---------------- | ---------------------------------------------------------------------- |
| `--skip-extract` | The extractor already ran in a previous shell; you only want re-load. |
| `--skip-load`    | The CSVs are stale but you want to re-run comparisons on what's in db. |
| `--skip-compare` | You're refreshing inputs and will re-compare later by hand.            |

You can chain them: `dtrack run --skip-extract --skip-load` runs comparisons
only, equivalent to invoking `cmd_compare_row` then `cmd_compare_col` by
hand. Most CI shapes use `--skip-extract` because the extractors run in a
nightly upstream job.

## 8. Outdir layout

Default tree (when `--outdir .` is passed):

```
./
├── sas/         ← SAS extract programs + their result CSVs land here
│   ├── extract_row.sas
│   ├── extract_col.sas
│   ├── <qname>_row.csv
│   └── <qname>_col.csv
├── csv/         ← Athena / Oracle / Databricks land here
│   ├── extract_row.sql        (athena, oracle)
│   ├── extract_col.sql
│   ├── <qname>_row.csv
│   ├── <qname>_col.csv
│   └── .cache.db              (per-outdir SQL cache)
└── output/      ← reports
    ├── compare_row.html
    └── compare_col.html
```

`--sas-outdir` and `--csv-outdir` let you override individually. `--html-row`
and `--html-col` let you redirect the reports.

## 9. Concurrency model

`run_pipeline` itself is single-threaded — it walks platforms in series. The
**parallelism lives inside each extractor**, controlled by the platform-specific
worker count (Athena defaults to 8, Databricks 4, Oracle 2, Hive 4). That's
intentional: parallelizing across platforms doesn't help (each one talks to
a different cluster) and would scatter the log output.

The web UI's `/api/extract/run-sql` is per-platform too, but the user can
click "Run AWS" and "Run Databricks" in quick succession — they then run
concurrently in two separate background threads.

## 10. What `run_pipeline` does *not* do

- It does not write configs back. The CLI handler does that via
  `save_unified_config` when the user accepts an interactive change.
- It does not maintain `_table_pairs`. Pair definitions live in
  `dtrack.json` (read at step 4) and in `_table_pairs` (written by the UI
  or `match-columns`).
- It does not handle "did we get partial results?" — that's the lifeboat
  fallback inside each platform's runner.

That clear separation is what makes the pipeline composable: a future `run`
that targets a single pair, or a single platform, or a single bucket, is a
matter of slicing the table list — the rest of the function takes care of
itself.

Next file: `06-platforms-base.md` — the shared helpers every extractor uses.
