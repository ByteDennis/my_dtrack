# 11 — `web/app.py` (the FastAPI backend)

`dtrack/web/app.py` is one ~5700-line FastAPI file: 93 routes, an SSE
streamer, a playground engine, history endpoints, in-memory caches for
cancellation, and Jinja-rendered HTML pages. Despite the size it stays
flat — there is no inner package structure, and the route function below
each `@app.<method>(…)` is the whole handler.

This tutorial groups the routes by concern and explains the shared
machinery: SSE streaming, cancellation registries, the side-outdir rule,
the playground stack, and the export endpoints.

## 1. Page renderers (HTML)

| Route             | Template          | What it shows                                         |
| ----------------- | ----------------- | ----------------------------------------------------- |
| `/`               | _layout fallback  | The dashboard / pairs landing.                        |
| `/pairs`          | `pairs.html`      | Pair list + the **Row Gen** flow (per-pair preview, "Run AWS / Databricks / Oracle / Hadoop / CSV Extraction" buttons). |
| `/load_row`       | `load_row.html`   | Drag/drop CSV uploader → `_row_counts`.               |
| `/row_compare`    | `row_compare.html`| Date overlap, matching/non-matching dates, comparison run + edit. |
| `/col_mapping`    | `col_mapping.html`| Manual column mapping; auto-match button.             |
| `/col_gen_rough`  | redirect          | Legacy → `/col_gen?preset=quick`.                     |
| `/col_gen`        | `col_gen.html`    | Col preset radio, **Run X / Resume X / CSV Run** buttons. |
| `/load_col`       | `load_col.html`   | Drag/drop CSV uploader → `_col_stats`.                |
| `/col_compare`    | `col_compare.html`| Per-pair col-stat diff grid + export buttons.         |
| `/playground`     | `playground.html` | Ad-hoc SQL runner with history (tutorial 12).         |
| `/csv_compare`    | `csv_compare.html`| Raw-CSV pandas diff (no DB involved).                 |
| `/sync`           | `sync.html`       | Read JSON → DB / DB → JSON sync controls.             |

All page renderers are one-liner `return templates.TemplateResponse(request,
"…")`. State lives in the JS (which fetches from `/api/...`) and in the
project DB.

## 2. Status / introspection (`/api/status`, `/api/constants`, `/api/date-types`)

- `/api/status` → `{db_path, config_path, tables_loaded, pairs, last_run, …}`.
  The home page polls this.
- `/api/constants` → returns `DATA_SOURCES`, `CONNECTION_MACROS`, `DATE_COLUMN_TYPES`,
  `HADOOP_LIBNAME_PROFILES`, `HADOOP_PROCSQL_PROFILES`. Every JS page calls
  this once on load to populate dropdowns.
- `/api/date-types` → flat list of `{value, label}` from `DATE_COLUMN_TYPES`.

## 3. The extraction stack

### 3.1 `/api/init` and `/api/refresh-metadata`

`/api/init` runs `init_database`. `/api/refresh-metadata` walks
`_row_counts` / `_col_stats` and refills `_metadata.min_date_loaded` /
`max_date_loaded` so the home page's per-table summary stays accurate.

### 3.2 `/api/extract`

Single-shot extractor that **doesn't stream**. Body: `{platform, type,
from_date, to_date, outdir, max_workers, overwrite_cache}`. Captures stdout
into a `StringIO`, returns `{ok, output}`. Used by the per-page "Generate
files" buttons before the user clicks "Run Extraction".

Dispatch on `platform`:
- `aws` → `extract_aws`
- `oracle` → `extract_oracle`
- `databricks` → `extract_databricks`
- else → `gen_sas` (Hadoop / SAS)

### 3.3 `/api/extract/run-sql` (SSE)

The streaming runner. Body: `{platform, type, outdir, max_workers, resume,
overwrite_cache}`. Returns a `StreamingResponse(media_type='text/event-stream')`.

The function shape is documented in our most recent fix (the databricks
branch). Briefly:

1. Resolve `sql_path = os.path.join(outdir, f"extract_{type}.sql")`. Skip
   the existence check when `platform == "databricks"` (Databricks builds
   SQL in-process).
2. Allocate a `run_id` (uuid hex). Register a `threading.Event` in
   `_active_runs[run_id]` so `/api/extract/cancel/{run_id}` can signal it.
3. Push the first message `{"_run_id": run_id}` onto the progress queue
   so the client gets the id immediately.
4. Run the platform runner in a background thread. The runner accepts
   `on_progress(msg)` callback and a `cancel_event`. Each block completion
   pushes a `progress` dict onto the queue.
5. The async `_stream()` generator polls the queue and yields one SSE event
   per message: `event: run_id|start|progress|log|done` with JSON data.
6. On exception, pushes a `{"_done": True, "ok": False, "error": str(e)}`.
7. On completion, removes the run from `_active_runs`.

Event taxonomy:

| Event       | When                                                                 |
| ----------- | -------------------------------------------------------------------- |
| `run_id`    | First message. Lets the client wire its Cancel button.               |
| `start`     | A worker picked up a block. Carries `running` and optional `eta_sec`. |
| `progress`  | A block finished. Carries `done`, `total`, `name`, `ok`, `rows`, `elapsed`, optional `error`. |
| `log`       | (Databricks only, for now.) A line of stdout from the runner.        |
| `done`      | Final summary. `ok`, `succeeded`, `total`, `failed`, `results`.      |

### 3.4 `/api/extract/cancel/{run_id}`

Sets `_active_runs[run_id].set()`. The runner's worker loop checks
`cancel_event.is_set()` before each block submit and exits early. In-flight
queries finish (pyathena/oracledb/databricks-sql can't be interrupted mid-execute),
but the queue stops draining.

### 3.5 `/api/extract/dry-run`

Returns the SQL that **would** be generated for a given `(platform, type,
pair_names, from_date, to_date)` without writing it. Used by the col-gen
page's "Preview SQL" button.

## 4. The generation step (`/api/generate`)

Writes `extract_row.sas`, `extract_row.sql`, `extract_col.sas`,
`extract_col.sql` from the **current config** + `_table_pairs` hydration.
Body: `{type, from_date, to_date, sas_outdir, aws_outdir, pair_names,
preset, level, stats_override, cells}`.

Notable mechanics:

- **Hydration from the DB**: lines ~792–804 hydrate `col_map` and `col_filter`
  from `_table_pairs` if they're missing in JSON. The DB is the source of
  truth at request time (tutorial 02 §3).
- **Bucket check**: after generation, `_check_pair_buckets(filtered_config,
  pair_names, from_date, to_date)` emits `[BUCKET CHECK]` log lines warning
  if left/right vintage buckets would diverge. col_gen.js highlights these
  green (PASS) / amber (WARNING).
- **Cell-targeted re-extraction**: when `cells = [{pair, col, vintage}, …]`
  is provided (sent from the col-compare drill-down), only those
  (qname, mapped_col, vintage) combinations are emitted. Used to re-extract
  a single suspicious cell without re-running the whole pair.

## 5. Loading endpoints (`/api/load*`)

Multiple endpoints because the load flow has two modes (upload-CSV vs
specify-path) and two targets (row vs col):

| Route                                    | Purpose                                  |
| ---------------------------------------- | ---------------------------------------- |
| `/api/load`                              | Bulk load — runs the orchestrator's load step over a whole folder. |
| `/api/load/row/upload`                   | POST a single CSV file; loaded into `_row_counts`. |
| `/api/load/row/path`                     | POST a path on the server; same effect.  |
| `/api/load/columns/upload`, `…/path`     | Same shape for `_column_meta`.           |
| `/api/load/col-stats/upload`, `…/path`   | Same shape for `_col_stats`.             |
| `/api/scan/csv`, `/api/scan/folder`      | "What CSV files are in this folder?" GETs. |

All loaders delegate to `dtrack.loader.load_*`.

## 6. Comparison endpoints

### Row

| Route                                              | Purpose                          |
| -------------------------------------------------- | -------------------------------- |
| `/api/compare/row`                                 | Run row compare for all pairs.   |
| `/api/compare/row/{pair_name}`                     | GET — per-pair result.           |
| `/api/compare/row/{pair_name}` PUT                 | Save edited matching/excluded dates. |
| `/api/compare/row/{pair_name}/preview` POST        | Re-compute without persisting (drag-to-edit). |
| `/api/compare/row/export/excel/{pair_name}`        | XLSX export of one pair.         |
| `/api/compare/row/export/excel-all`                | XLSX of every pair.              |
| `/api/compare/row/export/csv/{pair_name}`          | Same as XLSX but CSV.            |
| `/api/compare/row/export/count-csv/{pair_name}`    | Per-date row counts CSV.         |
| `/api/compare/row/export/html`                     | HTML report.                     |
| `/api/clear/row`                                   | Wipe `_row_comparison` + `_row_counts` for a list of pairs. |
| `/api/status/row`                                  | Compact per-pair status badges (used by the pairs page sidebar). |

### Col

Same shape: `/api/compare/col`, `/api/compare/col/{pair_name}` (GET/PUT),
`/api/compare/col/export/*` (HTML, log, excel), `/api/status/col`.

The col-compare endpoints also drive **per-cell drill-down** — when the user
clicks one cell in the diff grid, the JS calls `/api/extract/dry-run` with
the cells filter to get the SQL that re-extracts that one cell.

## 7. Pair management

The pairs page reads/writes these:

| Route                                          | Purpose                                          |
| ---------------------------------------------- | ------------------------------------------------ |
| `GET /api/pairs/list`                          | All pairs + summary.                             |
| `POST /api/pairs`                              | Create a new pair.                               |
| `PUT /api/pairs/{name}`                        | Update one pair (vintage, dates, where, …).      |
| `DELETE /api/pairs/{name}`                     | Drop the pair from `_table_pairs` and config.    |
| `POST /api/pairs/skip`                         | Skip flag (the orchestrator skips on next run).  |
| `POST /api/pairs/reload`                       | Re-sync from `dtrack.json` into `_table_pairs`.  |
| `GET/PUT /api/pairs/{name}/annotations`        | Free-text notes per pair (HTML report).          |
| `GET /api/pairs/{name}/columns`                | Discovered columns from `_column_meta` for both sides. |
| `POST /api/pairs/{name}/col_filter/preview`    | Preview include/exclude pattern matches.         |
| `POST /api/pairs/{name}/col_rejected`          | Append to `col_rejected` list.                   |
| `POST /api/pairs/{name}/col_type_overrides`    | Save per-column type pin (left side).            |
| `POST /api/pairs/{name}/col_type_overrides_right` | Same, right side.                            |
| `PUT /api/pairs/{name}/col-mappings`           | Save `col_map`.                                  |
| `GET /api/pairs/{name}/col-mappings/csv`       | Download the current map as CSV.                 |
| `GET /api/pairs/{name}/columns/csv`            | Download the col-stats diff table as CSV.        |

## 8. Playground stack

Covered in detail by the recent diff. The endpoints:

| Route                                       | What it does                                                              |
| ------------------------------------------- | ------------------------------------------------------------------------- |
| `POST /api/playground/run`                  | Runs ad-hoc SQL against `athena/oracle/hadoop/databricks`. Supports `use_cache` for sha256(engine‖conn‖sql) lookups; full results stored in LRU. |
| `GET /api/playground/download/{cache_key}`  | Streams the full cached result as CSV via `StreamingResponse`. Browser never holds full rows. |
| `POST /api/playground/cancel/{run_id}`      | `cursor.cancel()` / `conn.cancel()` for an in-flight query.               |
| `POST /api/playground/cancel_running`       | Cancel all (UI "Stop" button).                                            |
| `POST /api/playground/sas`                  | Returns a SAS test script via `platforms.sas_test.gen_sas_script`.        |
| `GET /api/playground/history`               | Lists runs from `_playground_history` (default limit 50).                 |
| `PUT /api/playground/history/{run_id}`      | Update note.                                                              |
| `DELETE /api/playground/history/{run_id}`   | Delete a row.                                                             |

Two in-memory registries back it:

- `_PLAYGROUND_RUNNING: dict[int, {cursor, conn, cancelled}]` — keyed by
  history_id. Lookups for cancel.
- `_PLAYGROUND_CACHE: OrderedDict[str, dict]` — LRU max 10 entries.
  Stores `columns`, full `rows`, metadata, and `history_id`.

## 9. Sync + global settings

- `/sync` page + `/api/sync/inspect`, `/api/sync/push`, `/api/sync/reload_json`:
  three-way diff between `dtrack.json`, `_table_pairs`, and `_metadata`,
  with explicit push / pull controls. Used to recover when the JSON and
  the DB drift.
- `/api/global_settings` GET/POST: stores user UI preferences (outdir
  paths, max workers per side, overwrite-cache toggle default) in a
  `_global_settings` row inside `_metadata` with `table_name="_settings"`.
  Used by every page to avoid asking the user the same question.

## 10. CSV compare

`/api/csv_compare/inspect` and `/api/csv_compare/run` are a separate stack
that talks to `dtrack.csv_compare` (pandas-based). They don't write to the
project DB — output is a JSON diff payload the JS renders directly.

## 11. Shared machinery

### 11.1 The cancellation pattern

Three in-memory dicts back the three concurrent operations:

- `_active_runs` — `/api/extract/run-sql` runs.
- `_PLAYGROUND_RUNNING` — `/api/playground/run` queries.
- (none for `/api/extract` — single-shot, can't be cancelled).

All three use `threading.Lock`-guarded dicts and string-id lookups. The
cancel endpoints look up the handle and call the engine-specific cancel
(cursor.cancel for pyathena, conn.cancel for oracledb, `event.set()` for
the SSE runner). When the run finishes, it pops itself from the registry
in `finally`.

### 11.2 The side-outdir rule

`_side_outdir(platform)` returns the global setting for that platform's
side: `csv/` for `aws | databricks | csv-right`, `sas/` for `oracle |
hadoop | sas | csv-left`. Used by `/api/extract` and `/api/extract/run-sql`
to default `outdir` when the body doesn't specify one.

The constant `_RIGHT_PLATFORMS = {"aws", "athena", "databricks"}` is the
single point of truth for which platforms write to the right side.

### 11.3 The `_resolve(path)` helper

Resolves a user-supplied path against the project's `outdir` to avoid
asking JS to compute absolute paths.

### 11.4 The `_row_to_strs` helper

Converts a pyathena/oracledb/databricks-sql row tuple to a list of strings
the JSON encoder can handle. The playground uses this to stringify all
fetched rows.

## 12. Tests

The endpoint suite has medium-deep coverage in `tests/test_playground_history.py`,
`tests/test_databricks_catalog_resolve.py`, and the e2e bag under
`tests/e2e/`. The web tests use `fastapi.testclient.TestClient` and run
against a temp project db.

Next file: `12-web-frontend.md` — the JS that drives these routes.
