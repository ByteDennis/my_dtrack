# 04 — `cli.py`

`dtrack/cli.py` is the argparse dispatcher behind every `just …` recipe and
every command in `commands.txt`. The user types `dtrack <subcommand> <args>`,
`main()` resolves the subparser, loads the env file, then calls a `cmd_*`
handler. The handlers themselves are short — most of the heavy lifting lives
in `pipeline.py` or the per-platform modules.

This tutorial documents each subcommand and the conventions all handlers share.

## 1. Skeleton of `main()`

The whole module is one function with three responsibilities:

1. **Build the argparse tree** with one `add_parser` block per subcommand (lines
   ~1050–1260). Every shared flag (`--config`, `--from-date`, `--to-date`,
   `--preset`, `--workers`) is added by hand to each subparser — there's no
   parent-parser trickery. That deliberate verbosity makes the `--help` output
   for each command self-contained.
2. **Discover and load an env file** (lines ~1278–1305). The search order is:

   - `--env` CLI flag (highest priority);
   - `DTRACK_ENV_FILE` environment variable;
   - `dtrack.conf` / `.env` / `dtrack.env` walking up from the cwd (via
     python-dotenv `find_dotenv`);
   - `~/.dtrack/dtrack.conf`, then `/etc/dtrack/dtrack.conf`.

   The resolved file is loaded with `dotenv_values` and applied with
   `os.environ.setdefault` — anything already in the environment **wins** over
   the file. `DTRACK_ENV_FILE_LOADED` is stamped into the environment for
   downstream debuggers.

3. **Dispatch to a `cmd_*` handler** via the `commands = {...}` dict. The dict
   is the canonical list of subcommands — `_layout.html`'s nav is *not* the
   source of truth.

There is also a `--debug ipdb|attach` flag bound on the root parser that
installs a post-mortem hook (`ipdb`) or listens for a remote debugger
(`debugpy` on port 5678). Useful when the user wants to interactively poke at
a failing run.

## 2. The subcommands

### `init`
`cmd_init(args)` → `init_database(project_db)` + write a minimal
`dtrack.json` if `--config` doesn't exist. Idempotent.

### `load-row`
Bulk-loads `<qname>_row.csv` files from `--csv-dir` into `_row_counts`.
Delegates to `loader.load_row_counts`. Detects date and count columns by header
name heuristics (`detect_date_column`, `detect_count_column`).

### `load-col`
Same shape, target `_col_stats`. The CSV header is `COL_CSV_HEADERS` from
`constants.py`. Updates `_metadata.min_date_loaded` / `max_date_loaded`
as a side effect.

### `load-columns`
Different: loads `_column_meta` from a CSV (per-table schema dump). This is
the file produced by `discover_aws_columns` / `_discover_columns_databricks`
during gen-time and consumed later by the col-stats builders.

### `compare-row`
`cmd_compare_row(args)`:
1. Loads config, hydrates pair definitions from `_table_pairs`.
2. For each pair, calls `compare.compare_row_counts(db_path, left, right,
   from_date, to_date)` which reads `_row_counts` and computes overlapping /
   non-overlapping / excluded date sets.
3. Persists via `save_row_comparison`.
4. With `--html`, calls `html_export.generate_row_count_html` to render the
   report.

Prompts the user to skip a pair when they have catastrophically different
counts unless `-y` is passed.

### `compare-col`
Same shape as `compare-row` but reads `_col_stats`, calls
`compare.compare_column_stats`, persists via `save_col_comparison`, renders
via `html_export.generate_column_stats_html`. Supports `--vintage` to override
the per-pair bucket and `--no-date-filter` to bypass date scoping entirely
(useful when running ad-hoc comparisons on unspanned tables).

### `gen-sas`
`cmd_gen_sas(args)` → `hadoop_sas.gen_sas(config, outdir, types, env_path,
db_path, vintage, …)`. Writes SAS programs (`extract_row.sas`,
`extract_col.sas`) for tables whose source is `sas` / `oracle` / `hadoop`.
Uses the SAS templates under `dtrack/platforms/templates/`. Tutorial 10
covers the template language.

### `gen-aws`
`extract_aws(config, outdir, types, max_workers, db_path, vintage, …)` writes
`extract_row.sql` / `extract_col.sql` for `source: "aws"` tables and runs
them immediately. With `--type col` it pulls the `--preset` and
`--stats-override` knobs (see `_parse_stats_override`).

### `gen-databricks`
`extract_databricks(…)`. Same shape as `gen-aws` but for `source:
"databricks"` tables. Note: **does not write an `extract_*.sql` file** —
SQL is built in-memory and executed directly. Tutorial 08 explains why.

### `gen-oracle`
`extract_oracle(…)`. Same shape as `gen-aws` but for `source: "oracle"` and
SAS-passthrough tables. `--workers` is conservatively low (default 2) because
Oracle sessions are expensive.

### `match-columns`
`cmd_match_columns(args)`:
1. For every pair, reads `_column_meta` for left and right.
2. Calls `compare.match_columns_from_dicts` which fuzzy-matches by name with
   wildcard transforms (`apply_column_rules`).
3. With `--mode replace`, overwrites `col_map` from scratch; with `merge`
   (default), only fills missing entries.

### `list`, `list-pairs`
Read-only inspection. List all loaded tables and their metadata, or list
all pairs and their reconciliation status.

### `run`
`cmd_run(args)` is `dtrack run-full` — the orchestrator. Delegates to
`pipeline.run_pipeline(...)`. Tutorial 05 walks through the inside.

### `serve`
`cmd_serve(args)`:
1. Imports `web.app` (lazy — keeps the CLI fast at startup).
2. Sets `_db_path`, `_config_path` module-level globals (used by every route).
3. Runs `uvicorn.run(app, host, port)`.

### `query`
`cmd_query(args)` runs a single SQL string against `project.db`. With
`--write`, the connection is opened RW; otherwise RO. Output goes to stdout
as TSV. Used by the `just query` recipe.

### `doctor`
`cmd_doctor(args)` is a debugging command. It prints:

- Which env file was loaded;
- Every `DTRACK_*` environment variable currently set;
- The resolved Oracle macros (after `_load_macro_overrides` merges
  `DTRACK_ORACLE_MACROS` into the static `MACRO2SVC`);
- The AWS region/work_group/role variables in effect.

When a user reports "it can't see my macro," ask them to paste the doctor output.

## 3. Conventions all handlers follow

### 3.1 `_require_db(db_path)`
Every handler that touches the project db calls this first. It refuses to
proceed if the file doesn't exist; the user is told to run `dtrack init` first.

### 3.2 `_load_config(config_path)`
Tiny wrapper around `load_unified_config` that prints a friendly error and
exits 1 on failure. Always paired with `_require_db`.

### 3.3 `_parse_stats_override(spec)`
Parses `"n_unique=on,hash_sum=off"` strings from `--stats-override` into the
dict shape that the SQL builders expect. Returns `None` for empty input so
the preset stays authoritative.

### 3.4 Exit codes
- `0` — success.
- `1` — user error (missing db, missing config, invalid arg).
- `2` — runtime error (DB connect failed, query failed). Stack traces are
  shown only when `--debug` was on.

### 3.5 Mock-mode env vars
Three knobs let the handler run end-to-end without network I/O:

- `DTRACK_MOCK=<dir>` — universal: every platform reads CSV fixtures from `<dir>`.
- `DTRACK_ORACLE_MOCK=<dir>` — only oracle mocks (used in test_e2e).
- `DTRACK_ATHENA_MOCK=<dir>` / `DTRACK_DATABRICKS_MOCK=<dir>` — per-engine.

Relative paths are resolved against the env file's directory so tests stay
portable.

## 4. What the CLI *doesn't* do

- It does not own SQL building. The handlers call into per-platform
  `extract_*` / `gen_*` functions which own SQL.
- It does not own comparison logic. `compare.py` is the engine; the CLI
  hands it a config and a db path.
- It does not maintain web state. `serve` is a thin uvicorn entry.

That makes the CLI very easy to extend: add a new subparser, a `cmd_*`
handler that imports from a domain module, and a row in the `commands`
dict at the bottom of `main()`.

Next file: `05-pipeline.md` — the orchestrator behind `dtrack run`.
