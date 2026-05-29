# 03 — `config.py` and `constants.py`

These two modules hold every static knob the rest of dtrack reads. `constants.py`
is read-only — the user can edit it once per box and forget it. `config.py` is
the runtime loader, validator, and per-pair mutator for `dtrack.json`.

## 1. `constants.py` — the knob registry

### 1.1 `STATS_PRESETS`

Three named presets that drive **what stats** the col-stats SQL emits. The
preset name flows through `/api/generate?preset=…` into each platform's
`_build_col_sql_*` function which gates whole sub-queries on the boolean.

| Stat name | quick | standard | detailed | Cost characterization                                 |
| --------- | :---: | :------: | :------: | ----------------------------------------------------- |
| `n_total`   | ✓ | ✓ | ✓ | `COUNT(*)` — free.                                    |
| `n_missing` | ✓ | ✓ | ✓ | `COUNT(*) FILTER (WHERE col IS NULL)`.                |
| `min_max`   | ✓ | ✓ | ✓ | quick/standard: scalar `MIN`/`MAX`. detailed: `val=cnt` form that needs the freq table. |
| `length`    | ✓ | ✓ | ✓ | `AVG(LENGTH(col))`, `MAX(LENGTH(col))`. Coarse signal added May 2026. |
| `n_unique`  | ✗ | ✓ | ✓ | `approx_count_distinct` (Athena/Spark) or `COUNT(DISTINCT col)` (Oracle/Hive). |
| `hash_sum`  | ✗ | ✓ | ✓ | `SUM(hash16(col))` — multiset checksum. |
| `top_10`    | ✗ | ✗ | ✓ | Needs a per-column `GROUP BY` with a `WITH` chain. Most expensive. |

`mean` and `std` are deliberately **never** emitted — empirical signal-to-noise
was poor and they double the query cost. Tutorial 06 §4 explains the SQL
shapes.

### 1.2 `PLATFORM_RUNNER`

One row per engine. The shared parallel runner reads:

- `worker_env`, `worker_default` — env var for max workers and the default
  if it's missing. So `dtrack run-full --workers 16` overrides; `ATHENA_MAX_WORKERS=16` in `.env` overrides; otherwise `worker_default` applies.
- `degraded_label` — what gets written into the "partial" column of the result
  CSV when the `@degrade_sql` lifeboat fires.
- `lifeboat_errors` — substring patterns that trigger the lifeboat fallback
  instead of failing the block. Athena needs one ("Query exhausted resources"),
  Hive needs many (TEZ / MR can fail eight different ways for the same OOM).

If you add a new engine, drop a row here and the parallel runner picks it up
without any other code change.

### 1.3 `NULL_SENTINELS`

Per-platform, per-type lists of "values that mean missing". Applied **before**
stats compute, by `platforms/base.apply_null_sentinels(col_ref, col_type,
dialect, sentinels)`. Without this, `MIN`/`n_missing`/`n_unique` diverge between
Oracle (`-999`) and Athena (`NULL`) for reasons that have nothing to do with
data drift.

The categories are:

- `numeric`: literal numbers. Get rendered into `CASE WHEN col IN (-999, -9999, …) THEN NULL ELSE col END`.
- `string`: literal strings. Same shape but quoted.
- `date`: literal date strings. The builder converts to platform-native date
  literals at SQL emit time.

Edit this dict per environment; there's no per-pair override (yet).

### 1.4 `DATA_SOURCES`

The dropdown list rendered everywhere a source picker appears. Adding a row
here is half the work; the other half is teaching `/api/extract/run-sql` how
to dispatch (tutorial 11) and writing a new file under `dtrack/platforms/`.

### 1.5 `DATE_COLUMN_TYPES`, `DATE_TYPE_FORMATS`, `DATE_TYPE_TO_CANONICAL_DTYPE`

Three dicts that together form **the date-type registry**. The comments in
`constants.py:228–238` are the spec — to add a new format you must:

1. Drop a row into `DATE_TYPE_FORMATS` mapping it to a strftime-like label.
2. Drop a row into `DATE_TYPE_TO_CANONICAL_DTYPE` mapping it to one of
   `DATE` / `TIMESTAMP` / `NUMBER` / `VARCHAR2`.
3. Add a strftime pattern to `reformat_date`'s `fmt_map` in `platforms/base.py`.
4. Add a dropdown entry to `DATE_COLUMN_TYPES` above.

All platforms route through `resolve_date_format(date_filter, tbl_cfg)` and
`apply_null_sentinels` — no per-platform edits needed when adding a format.

### 1.6 `CONNECTION_MACROS`

The macro suggestions shown in every "connection" datalist. Keep this short
and authoritative — they're user-visible. Empty `databricks` / `csv` arrays
on purpose: Databricks uses `DB_PROFILE` env var, CSV uses a directory path,
neither is a "macro" in the Oracle sense.

### 1.7 `HADOOP_LIBNAME_PROFILES` / `HADOOP_PROCSQL_PROFILES`

Pre-filled connection parameter sets shown in the SAS playground when the
user picks "Hadoop / Hive". Two dicts because LIBNAME and PROC SQL CONNECT
take subtly different params on this box. The Playground JS toggles which
dict it reads based on the method radio.

### 1.8 `VINTAGE_PRESETS`

Per-platform per-vintage SQL fragment. `{col}` is the substitution slot.
Used by every `_vintage_date_expr_*` helper to turn a raw column reference
into a bucketed expression for `GROUP BY`.

The Athena and Oracle entries diverge intentionally: Oracle uses `TRUNC(col,
'IW')` (ISO week) while Athena uses `DATE_TRUNC('week', col)` (Sunday-based).
Tutorial 07 has the alignment notes.

## 2. `config.py` — the runtime config

### 2.1 What it loads

`load_unified_config(config_path)`:
- Reads the JSON file.
- Calls `validate_unified_config` which checks each pair's `left` / `right`
  blocks via `validate_unified_pair`, the global `date_types` via
  `validate_date_types`, and prints actionable error messages.
- Returns the dict for the caller to use.

There is no schema enforcement *engine* (no JSONSchema, no pydantic) — just
plain Python checks. If you add a new top-level key to dtrack.json, extend
`validate_unified_config` and only then will the loader accept the file.

### 2.2 Per-pair mutators

A long list of small functions whose contract is "load → mutate → save". Each
one takes `config` (the dict) and a `pair_name`, never the path. The web app
calls `save_unified_config(config, config_path)` itself; the CLI calls
`save_and_pause` to give the user a chance to inspect the diff.

Important mutators:

| Function                      | What it changes                              |
| ----------------------------- | -------------------------------------------- |
| `set_pair_col_map`            | Writes `pairs.<name>.col_map`.               |
| `set_pair_where_map`          | Writes `pairs.<name>.where_left/right`.      |
| `add_ignored_rows`            | Appends to `pairs.<name>.excludeDates`.      |
| `add_ignored_columns`         | Appends to `pairs.<name>.col_exclude`.       |
| `set_col_type_override`       | Pins a `col_type` for one column.            |
| `ensure_pair_defaults`        | Idempotent: fills in `vintage="all"`, empty `excludeDates`, etc. Called on every load to migrate old configs forward. |
| `mark_pair_skipped`           | Sets `pairs.<name>._skipped = true` — the orchestrator skips on next run. |

### 2.3 Reading helpers

`get_pair_col_map`, `get_pair_where_map`, `get_ignored_rows`,
`get_ignored_columns`, `get_col_type_overrides` are pure read helpers that
return `{}` / `[]` rather than `None`. The pattern lets callers do
`for col in get_ignored_columns(...)` without `None`-guards.

### 2.4 `get_all_tables_from_unified(config)`

The most-used function in this module. Walks every pair's left and right
blocks and returns a list of resolved `tbl_cfg` dicts ready to feed into a
platform extractor. Each dict has:

- `name`           — pair-side name (`_derive_side_name` resolves a
                      default when left.name is missing).
- `source`         — `"oracle"` / `"aws"` / etc.
- `store`          — DB / catalog / schema / macro.
- `date_var`       — copied from `date_types[qname]`.
- `date_type`      — same.
- `date_format`    — same (optional).
- `_pairs`         — list of pair names this table belongs to (a table can
                      appear in multiple pairs).
- `_selected_cols` — applied by `apply_col_filter` later.

`load_tables_from_config(config)` in `platforms/base.py` is a thin alias that
the runners use; both produce identical output.

### 2.5 `_derive_side_name`

Quietly important. When a `pairs.<name>.left.name` is missing or set to
`"auto"`, this function builds a deterministic default by combining
`store` + `name` shapes. Idempotent and side-effect-free.

## 3. The flow

```
dtrack.json ──load_unified_config──► config dict
                                       │
                                       ├──get_all_tables_from_unified──► [tbl_cfg, …]
                                       │                                    │
                                       │                                    ▼
                                       │                            extractor (platform)
                                       │
                                       └──save_unified_config──► dtrack.json (rewritten)
```

The same `config dict` is the input to `_compute_pair_summary` (when comparing),
to the SQL builders (when generating), and to the SAS template renderer
(when generating .sas).

Next file: `04-cli.md` — the entry points the user types in a terminal.
