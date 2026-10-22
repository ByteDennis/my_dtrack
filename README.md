# pair_recon

`pair_recon` validates one Oracle, Hadoop, or local CSV source against one Databricks or local CSV target per execution. Pair definitions and SQL stay in Unity Catalog. Local defaults are saved in UTF-8 YAML; credentials stay in `.pair_recon.env` and `~/.databrickscfg`.

## Runtime

1. The local launcher reads one enabled pair plus its mappings and optional SQL from Unity Catalog.
2. Left pulls source counts and selected column statistics with bounded worker threads.
3. Artifacts are written to a Unity Catalog Volume under a daily `pair-owner-YYYYMMDD` run directory.
4. The current progress snapshot is updated in place by `run_id` while validation runs.
5. A Databricks job validates a database Right side; a CSV Right side runs locally without starting that job.
6. Pair, run-status, and run-error Delta tables are updated for either path.
7. Windows sends the result through the signed-in Outlook profile when recipients are configured.

Different owners may validate the same pair independently because the owner is part of `run_id`. Re-running the same pair and owner on the same configured-timezone day replaces that day's Volume artifacts, status, errors, and progress snapshot.

## Install

```bash
uv sync
cp .pair_recon.env.example .pair_recon.env
# Fill DBX_* in .pair_recon.env, then:
./setup/setup.sh
```

Windows:

```bat
uv sync --locked
copy .pair_recon.env.example .pair_recon.env
rem Fill DBX_* in .pair_recon.env, then:
setup\setup.cmd
run.cmd
```

Workstation execution is supported only through `uv run`. `run.cmd` and `run.sh` are thin wrappers around `uv run --project ... pair_recon`; PairRecon does not build a standalone executable. `just publish` creates `dist/pair_recon.zip`, a minimal source release derived from the current runtime packages, `pyproject.toml`, `justfile`, environment template, launchers, and workbook. The ZIP omits tests, development tools, GitLab tooling, setup SQL/notebooks, caches, and detailed project documentation; its compact `readme.txt` contains only installation and run steps.

Install Hadoop support with `uv sync --extra hadoop`; its DSN defaults from `HADOOP_DSN`. Use [setup/schema.sql](setup/schema.sql) to create the six Delta tables, then fill [setup/pairs.xlsx](setup/pairs.xlsx). The workbook has three worksheets: `pairs` for compact source/run metadata, `pair_sql` for optional SQL and query-window declarations, and a concise read-only `field_guide`. Rows join by `pair_name`. `install.py` validates the workbook and upserts `pair_recon_pair_meta` plus `pair_recon_pair_sql`; it preserves `latest_run_id`/`latest_status` and does not delete omitted pairs. Each run performs the same comparison before pair selection: changed or new workbook rows are upserted first, so workbook values win without touching remote pairs omitted from the workbook. Set `DBX_PAIR_WORKBOOK` to select the workbook; it defaults to `setup/pairs.xlsx`. Existing installations must run [setup/migrate_pair_metadata.sql](setup/migrate_pair_metadata.sql) once before importing this workbook and [setup/migrate_run_status.sql](setup/migrate_run_status.sql) once before using result-summary status fields. Use `--skip-config` only for code/job-only deployment.

In `pairs`, `left_conn` means an Oracle macro, Hadoop schema, or CSV path. `right_conn` means `catalog.schema` for `databricks_table`, or a path for `databricks_files`/CSV; it stays blank for `databricks_query`. `left_table` and `right_table` are used only where the selected source needs them. `vintage` is explicit. Rare source settings go in the side `*_options_json`; prompt defaults go in `run_defaults_json`. Prefer percentage sampling such as `{"sample":0.1}`; sampled runs also need stable `key_columns`. `run_defaults_json` accepts `run_mode`, `partition_mode`, `checks`, `sample`, `strict`, `audit`, `sync_remote`, `left_workers`, `right_workers`, `vintage`, `from_date`, `to_date` (both `YYYY-MM-DD`), `excluded_partitions`, `vintage_sample_size`, and `vintage_values`; the date-only fields are rejected when `partition_mode` is `all`. Saved answers for the same pair in `.pair_recon.yaml` still win over these defaults. Relative CSV paths resolve from the process working directory. Date types are auto-detected unless explicitly overridden in side options. A numeric `YYYYMM` partition column uses `{"date_type":{"type":"number","format":"month"}}`; this is a partition date type, not `pair_recon_column_map.data_type`.

For Oracle-to-Databricks column mappings, run `uv run --with jupyterlab jupyter lab setup/mapping.ipynb`. The notebook loads profile, warehouse, and pair-table defaults from `.pair_recon.env`; set `PAIR_NAME` in the notebook. It reads the new workbook row directly, queries real Oracle cursor metadata and Databricks `DESCRIBE QUERY`, proposes deterministic matches, and previews them. Review or edit `approved`, then set `APPLY = True` to upsert `pair_recon_column_map`; unmatched and ambiguous columns are never applied automatically. [setup/setup.sql](setup/setup.sql) remains a manual SQL example and repair reference.

`pair_recon_column_map.data_type` accepts `numeric`, `categorical`, a registered type such as `sas_datetime_date`, or a custom JSON type such as `{"type":"date","left_date_type":"date","right_date_type":"date"}`. Mapping rows define the effective `col_map` and type overrides. `config_json` remains in `pair_recon_pair_meta` only as a generated snapshot; the explicit columns are authoritative.

`pair_recon_pair_sql` contains at most one row per pair. A non-blank `left_sql` or `right_sql` replaces that side's table relation; a blank value uses its configured table. SQL remains the base relation so PairRecon can apply outer date guards, sampling, and aggregation. Named `{{query_windows:name}}` markers are supported on Oracle Left and Databricks Right SQL; declarations live beside that SQL in `left_query_windows_json` or `right_query_windows_json`.

Row checks always cover the complete requested range. Attribute sampling normally uses the `sample` fraction in `run_defaults_json` or the run prompt; for example, `0.1` means ten percent by deterministic key hash. Optional `vintage_values` or `vintage_sample_size` defaults may narrow which date buckets receive attribute checks; exact values normalize to the configured `vintage`, and only one vintage selector may be set.

## Environment

Every PairRecon CLI, the installer, and the mapping notebook automatically load only `.pair_recon.env`; a value already present in the process environment wins. The normal UC-driven run needs `ORACLE_USER` plus either `ORACLE_PASSWORD` or the selected connection macro's `<MACRO>_PWD` value when Left is Oracle. `ORACLE_SVC` may map macros to services, for example `pb30:service,pcds:service`; `PAIR_RECON_ORACLE_MACROS` remains a compatibility alias. Add `ORACLE_DSN` only when the macro map does not provide the service. A pair may instead name custom `user_env`, `password_env`, and `dsn_env` variables in `left_options_json`. `PAIR_RECON_OUTPUT_DIR` optionally selects the local artifact root and defaults to `outputs` under the current working directory. Databricks authentication stays in `~/.databrickscfg`.

The earlier `pkg/distributed` branch used S3/AWS/WinSCP paths and many `DTRACK_*` runtime variables. The current UC Volume design does not require them.

## Run

```bash
./run.sh
just run
```

```bat
run.cmd
```

Use `just run --defaults` (or `--default`) to load `.pair_recon.yaml` and start without any prompts; an optional YAML path may follow the flag. Add `--autorun` to catch the specific Right error for unexcluded mismatched partitions, normalize and save those dates, then restart the same daily run ID automatically (up to three retries). Add `--force` to clear the run's Volume/Catalog results and bypass PairRecon row/pull caches. These switches are independent and may be combined, for example `just run --defaults --autorun --force`.

The prompts ask for owner, Databricks profile and table locations, pair, run mode, timezone, partition mode, checks, sample, strict normalization, optional date range and vintage, and confirmation. Date mode also asks for each side's date column and date type; types accept `date`, `string`, `number`, `sas`, or an inline JSON specification such as `{"type":"string","format":"compact"}`. These four effective values are saved as `left_date_col`, `left_date_type`, `right_date_col`, and `right_date_type`, printed with the other settings, and applied to the runtime config. `Excluded partitions` accepts comma-separated `YYYY-MM-DD` values and inclusive ranges such as `2025-04-02->2025-04-05, 2025-04-10`; the normalized list is saved as `excluded_partitions`. Before pair selection, PairRecon validates the configured workbook, compares its `pairs` and `pair_sql` rows with UC, and upserts only changed or new rows; workbook values win. Pair choices are then reloaded from enabled UC rows. `full` mode starts either the configured Databricks job or local CSV Right. `left-only` uses the Left definition, vintage selection, sample, checks, and approved column mapping to publish Left artifacts only. Owner defaults to the `USERNAME` environment variable, falling back to the saved value only when `USERNAME` is unavailable. Checks use a terminal checkbox list: enter comma-separated names or numbers to toggle `[x]` selections, then press Enter to confirm. Press Enter elsewhere to reuse saved defaults. The run ID is generated automatically as `<pair>-<owner>-YYYYMMDD` in the configured timezone; the same ID is overwritten on another run that day.

The exclusion prompt can be prefilled directly in `.pair_recon.yaml` for the saved pair:

```yaml
excluded_partitions: 2025-04-02->2025-04-05, 2025-04-10
```

PairRecon expands ranges inclusively and rewrites the saved setting as normalized dates after a confirmed run. For a numeric `YYYYMM` date type, dates in the same month collapse to the first-day logical partition key and SQL predicates use the physical `YYYYMM` number; excluding any date in April therefore excludes the April partition.

Before execution, PairRecon creates the local output root when needed and atomically writes `outputs/YYYY-MM-DD/<run_id>/request.json` by default. Set `PAIR_RECON_OUTPUT_DIR` to change that root; a relative value is resolved from the current working directory. The same run directory contains `left.log`, DuckDB temporary state, pulled Left CSVs, fixed-key CSVs, and their metadata. Source input files are never moved. The UTF-8 snapshot contains the current user settings, the pair/mapping/SQL rows pulled from Catalog, and the merged effective config; credential fields are omitted. SQL text is retained verbatim in `.txt` audit artifacts, so credentials must never be embedded in SQL. The Right notebook logs at INFO to stdout, so its progress shows live on the Databricks job run page and in the cluster driver logs; when the Right job fails, PairRecon fetches the task's error and Python traceback through the Jobs run-output API and writes them, with the run page URL, into the raised error and `left.log`. On success the notebook returns its outcome counts via `dbutils.notebook.exit`, and they are merged into the result's `right` section without downloading result JSON files locally.

The Volume run directory is single-pair and flat: `_status/`, `row_check/`, `units/`, `col_check/`, `audit/`, `audit_errors/`, `report/`, `plan.json`, `mapping.csv`, and `fixed_keys.json` live directly below `<run_id>/`. There is no intermediate `pairs/<pair>/` directory. `mapping.csv` is the concise operator view; `plan.json` remains the machine-readable Left-to-Right execution contract.

A successful `left-only` run ends as `left_complete`, not `passed`, because no reconciliation verdict exists. It uploads the full-range Left row counts to `row_check/left.json`; sampling affects attribute statistics, not those counts. When column checks are selected, it also uploads the selected vintage/sample/mapping plan to `plan.json`, fixed sample keys when applicable, and shard-batched `left_statistics` units under `units/`; each logical unit still represents one vintage and mapped Left column. `_status/col_check.closed.json` then marks all planned Left statistics ready. In `pair_recon_progress_snapshot`, `units_ready` counts logical Left units while `units_complete` remains the count of completed Right comparisons. Snapshot pairs are excluded from left-only mode because choosing their historical comparison vintage requires Right row counts. Row-check partition objects and `report/row_check.csv` expose the outcome as `result` (`match`, `count_diff`, `left_only`, or `right_only`).

Registered attribute checks are `n_total`, `n_missing`, `n_unique`, `mean`, `std`, `min`, `max`, `length_avg`, `length_max`, and `hash_sum`; `row_count` remains the pair-level check. `partition_mode=all` treats the complete relation as the pair-level `all` partition and does not require a date column.

Detailed differences are stored in `pair_recon_run_error`. For a column-statistic mismatch, `column_name` identifies the Left column, `vintage` identifies the comparison bucket, `check_name` identifies the failed statistic, and `left_value`/`right_value` contain the compared values. `payload` contains the complete result JSON, including the mapped Right column and every calculated match/difference field. Row-count findings use `check_name='row_count'` and `partition_value`; execution failures use `check_name='runtime'` and put the message in `detail`. A successful left-only run produces no findings. `pair_recon_run_status` stores lifecycle state plus `row_total`, `row_matches`, `row_differences`, `column_total`, `column_matches`, `column_differences`, and `runtime_failures`. The Right notebook publishes successful/different summaries itself; the local launcher only forwards failed/cancelled Jobs API details directly, without downloading Volume result JSON. `pair_recon_pair_meta.latest_run_id/latest_status` points to the latest outcome, including `left_complete`.

For a current-view Left source against Right history, set `snapshot: true` in that pair's metadata. Left is counted as a complete snapshot; Right chooses the latest partition in the configured range whose row count matches, then column checks compare the snapshot with that partition.

## Databricks tools

`just dbx` uses the Databricks SDK profile without storing credentials in PairRecon files:

```bash
just dbx login --no-open
just dbx workspace upload src/pair_recon_right pair_recon_right --dry-run
just dbx workspace download pair_recon_right downloaded-right
just dbx workspace ls pair_recon_right -R
just dbx workspace cp source.py archive/source.py --dry-run
just dbx workspace mv old.py new.py
just dbx workspace rm obsolete.py --dry-run
just dbx workspace rm -rf obsolete-folder
just dbx compute status --cluster-id CLUSTER_ID
just dbx warehouse status --warehouse-id WAREHOUSE_ID
just dbx sql --unity main.sales.orders
just dbx sql --unity "SELECT current_user()"
just dbx sql --glue sales.orders --limit 10
just dbx runs --limit 10
```

Set `DBX_PROFILE`, `DBX_CLUSTER_ID`, `DBX_WAREHOUSE_ID`, `DBX_WORKSPACE_PDIR`, `DBX_JOB_NAME`, `DBX_PAIR_TABLE`, `DBX_PAIR_WORKBOOK`, `DBX_STATUS_TABLE`, `DBX_ERROR_TABLE`, and `DBX_RUN_PATH` in `.pair_recon.env` to omit repeated setup and operator options. `DBX_PAIR_WORKBOOK` selects the authoritative local workbook used for the pre-run metadata comparison. The table variables provide the corresponding run-prompt defaults; the progress table defaults to the status table's schema. `DBX_RUN_PATH` supplies the Volume runs path. `dbx sql` reads its profile and warehouse from that environment file: `--unity` accepts a Unity Catalog three-part table or SQL, `--glue` accepts a Hive metastore `database.table` or SQL, and both print five rows by default. Relative Workspace paths resolve below `/Users/<current-user>/`; absolute paths remain supported. Workspace `ls`, `cp`, `mv`, and `rm` follow the same path rule; recursive copy uses `cp -r`, replacement requires `-f`, and mutating operations support `--dry-run`. Because the Workspace API has no server-side rename or copy endpoint, `cp` uses export/import and `mv` deletes the source only after copying succeeds; `mv` is not atomic. Upload and download support files, notebooks, recursive folders, `--dry-run`, and `--overwrite`. `rm` accepts multiple paths, `-r/--recursive`, `-f/--force`, and `--dry-run`. The setup installer and `Sync remote` mirror `src/pair_recon_right` to `DBX_WORKSPACE_PDIR/pair_recon_right`, deleting remote package objects that no longer exist locally only after every upload succeeds. Its job notebook is the sibling `DBX_WORKSPACE_PDIR/pair_recon_right_job`, so it imports the package normally. Set `DBX_PKG_RENAME=pair_recon_right -> pair_recon` to upload the folder as `pair_recon` (notebook `pair_recon_job`); only the notebook's `pair_recon_right` imports are rewritten on upload, because every module inside the package uses relative imports. If the configured Right job name or saved ID no longer exists, PairRecon catches `RESOURCE_DOES_NOT_EXIST`, creates a job named by `DBX_JOB_NAME` on `DBX_CLUSTER_ID`, and saves Databricks' new numeric job ID to `.pair_recon.yaml`. Enter `-` or `clear` at the Right job prompt to discard the saved ID and resolve/create the job from `DBX_JOB_NAME`. Left and Right sources come directly from pair metadata, so the run prompt no longer asks for duplicate input types or CSV paths. The run prompt asks whether to sync the Right package (default `true` for a Databricks Right), whether to retain SQL audit artifacts (default `false`), the Left source-query budget (default `2`), and the Databricks query budget (default `8`). `STATS_WORKERS` controls local per-vintage statistics shards and defaults to `2`. Use `just dbx --debug attach ...` or `just dbx --debug console ...` only when troubleshooting.

Run `just live-right` on a configured work computer to execute the Right date SQL contract through `WorkspaceClient` and `DBX_WAREHOUSE_ID`. It loads `.pair_recon.env`, uses `DBX_PROFILE` and `DBX_AUTH_TYPE`, and performs read-only SQL without creating Unity Catalog objects.

## GitLab tools

PairRecon can create a private GitLab project and dispatch a validation-only snapshot through an installed and authenticated `glab` CLI:

```bash
just gitlab create pair-validation --group GROUP
just gitlab plan --scope both
just gitlab dispatch GROUP/pair-validation --scope both --dry-run
just gitlab dispatch GROUP/pair-validation --scope both
```

Scopes are `left`, `right`, `both`, and `all`. `both` includes the local orchestrator, Left, Right, shared tests, and setup files. The default [pair_recon.ignore](pair_recon.ignore) excludes DBX, GitLab tooling, credentials, generated output, local/live tests, and caches. Selection walks the filesystem independently of `.gitignore`; negation rules are intentionally unsupported. Dispatch tracks its files in `.pair_recon-dispatch.json`, preserves unrelated destination files, creates a normal commit, and pushes without force. Use `GITLAB_HOST` for a self-managed hostname and `just gitlab --debug attach ...` when troubleshooting.

## Verify

```bash
just test
just lint
```

Live Oracle query/export/timing tests are opt-in and are not part of the default suite:

```bash
just live
```

Every recipe defaults to normal mode. Python workflows accept `--debug console` or `--debug attach`; console enters ipdb post-mortem at the original exception frame, while attach waits for VS Code `debugpy` on port 5678. Lint uses `--debug` for verbose diagnostics. `just sas` creates only `left.sas` under `${PAIR_RECON_OUTPUT_DIR:-outputs}/sas`; use `just sas side=right` or `just sas side=both` when needed, or pass `bundle=...` for an explicit location. After SAS writes either sampled CSV, use the normal `run.cmd` or `just run` prompts to select CSV inputs. When Right is CSV, comparison stays local and reuses the existing multithread statistics engine; no extra recipe is needed.

See [guideme.txt](guideme.txt) for the complete local, Databricks, Hadoop, CSV, SAS, progress, Outlook, and troubleshooting flow.
