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

Different owners may validate the same pair independently because the owner is part of `run_id`. Re-running the same pair and owner on the same configured-timezone day resumes that run: compatible row counts, Left units, local pulls, and successful Right results are reused, while failures and differences are retried. Use `--force` when the day's Volume artifacts, status, errors, progress, and local PairRecon caches must be replaced.

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

Install Hadoop support with `uv sync --extra hadoop`; its DSN defaults from `HADOOP_DSN`. Set `DBX_CATALOG_SCHEMA=catalog.schema`, then use [setup/schema.sql](setup/schema.sql) to create the PairRecon Delta tables and run [setup/reference.sql](setup/reference.sql) to seed the data dictionary. Table names are fixed centrally in [constant.py](src/pair_recon/core/metadata/constant.py); operators provide only the catalog and schema. Fill [setup/pairs.xlsx](setup/pairs.xlsx). The workbook has three worksheets: `pairs` for compact source/run metadata, `pair_sql` for optional SQL and query-window declarations, and a concise read-only `field_guide`. Rows join by `pair_name`. `install.py` validates the workbook and upserts `pair_recon_pair_meta` plus `pair_recon_pair_sql`; it preserves `latest_run_id`/`latest_status` and does not delete omitted pairs. Before every pair selection, an existing workbook is compared with Unity Catalog and changed or new rows are upserted, so workbook values win without touching remote pairs omitted from it. `DBX_PAIR_WORKBOOK` selects the workbook and defaults to `setup/pairs.xlsx`; when that file does not exist, the run also falls back to existing Unity Catalog metadata without downloading or creating a workbook. Existing installations must run [setup/migrate_pair_metadata.sql](setup/migrate_pair_metadata.sql), [setup/migrate_run_status.sql](setup/migrate_run_status.sql), [setup/migrate_observability.sql](setup/migrate_observability.sql), and [setup/migrate_cache.sql](setup/migrate_cache.sql) once as applicable, then rerun `setup/schema.sql` and `setup/reference.sql`. Use `--skip-config` only for code/job-only deployment.

In `pairs`, `left_conn` means an Oracle macro, Hadoop schema, or CSV path. `right_conn` means `catalog.schema` for `databricks_table`, or a path for `databricks_files`/CSV; it stays blank for `databricks_query`. `left_table` and `right_table` are used only where the selected source needs them. `vintage` is explicit, and `partition` is `day`, `month`, or `all`. Rare source settings go in the side `*_options_json`; prompt defaults go in `run_defaults_json`. Prefer percentage sampling such as `{"sample":0.1}`; sampled runs also need stable `key_columns`, each written as `NAME[type]` where the type is `numeric`, `categorical`, or a registry name (`string`, `exact`, `date`, `digits`, `date_type`, `sas_datetime_date`); the workbook rejects a key without a type, the type overrides that key's `pair_recon_column_map.data_type`, and the sync writes it there. Keys are configured only in the workbook or `pair_recon_pair_meta`, never at the prompt. `run_defaults_json` accepts `run_mode`, `checks`, `sample`, `audit`, `sync_remote`, `left_workers`, `right_workers`, `vintage`, `from_date`, `to_date` (both `YYYY-MM-DD`), `excluded_partitions`, `excluded_columns` (mapped Left names to leave out of the column check, matched case-insensitively; the prompt `Excluded columns` edits the same list), `sample_rows` (default 50), `vintage_sample_size`, and `vintage_values`; `checks`, `excluded_partitions`, `excluded_columns`, and `vintage_values` accept either JSON arrays or comma-separated strings and are stored as canonical arrays. The date-only fields are rejected when `partition` is `all`. Saved answers for the same pair in `.pair_recon.yaml` still win over these defaults. Relative CSV paths resolve from the process working directory. Date types are auto-detected unless explicitly overridden in side options. An explicit Right `date_type` must fit the physical column: the Right health check rejects `string`, `number`, and `sas` date types on `date`/`timestamp` columns and `date` on text columns (Databricks ANSI mode would otherwise fail with `CAST_INVALID_INPUT`, or parse every value to NULL); the same rule applies to the `right_date_type` of a date `data_type` on a key or compared column. A numeric `YYYYMM` partition column uses `{"date_type":{"type":"number","format":"month"}}`; this is a partition date type, not `pair_recon_column_map.data_type`. Oracle `left_options_json` may tune streaming with `arraysize`, `prefetchrows`, `fetch_batch_size`, `fetch_retries`, and `max_sessions`. A transient Oracle connection loss retries the complete vintage with a new connection and preserves already completed vintages.

For a slow Oracle or Hadoop Left sample, set `left_options_json` once, for example `{"sas_sample_csv":"C:/PairRecon/samples/orders/left_sample.csv"}`. Run `left-only` first. PairRecon completes the ordinary full-source Left row check, deterministically selects the requested vintages, and, if the CSV is absent or stale, writes `left_sample.sql` plus `left_sample.request.yaml` beside it and ends as `awaiting_sample` without starting Right. The SQL uses the source's native Oracle or Hadoop dialect and combines every selected vintage into one result; run it through the existing SAS Oracle/ODBC pass-through connection and export one CSV at the configured path. The query includes `__pair_recon_request_id`, which must remain in the CSV. Then change only `run_mode` to `full`: PairRecon validates the request ID and header, reuses the row-check checkpoint, splits the combined CSV by its date column, and uses it only for Left column statistics. A missing or stale file never falls back to another live Left sample pull. Per-vintage `left.csv` files are disjoint temporary slices, not full copies of the combined file; after a successful completed run and difference-sample publication, PairRecon removes those slices and their cache metadata while retaining the combined SAS CSV, SQL, and request. Failed or interrupted runs retain temporary slices for resume.

For Oracle-to-Databricks column mappings, run `uv run --with jupyterlab jupyter lab setup/mapping.ipynb`. The notebook loads profile, warehouse, and catalog-schema defaults from `.pair_recon.env`; set `PAIR_NAME` in the notebook. It derives the fixed metadata table names, reads the new workbook row directly, queries real Oracle cursor metadata and Databricks `DESCRIBE QUERY`, proposes deterministic matches, and previews them. Review or edit `approved`, then set `APPLY = True` to upsert `pair_recon_column_map`; unmatched and ambiguous columns are never applied automatically. [setup/setup.sql](setup/setup.sql) remains a manual SQL example and repair reference.

`pair_recon_column_map.data_type` accepts `numeric`, `categorical`, a registered normalizer family (`string`, `exact`, `date`, `month`, `digits`, `date_type`, `sas_datetime_date`, `date_string`), or a date object such as `{"type":"date","left_date_type":"date","right_date_type":{"type":"string","format":"compact"}}`. Key families and column types share that registry: a key column takes its sampling normalization from the same `data_type` (`numeric` selects the numeric family) unless `key_families` in a direct config names it, and the partition date key falls back to the pair-level `date_type` values. A `data_type` on the partition date column or a key is accepted; the partition key's Right name is `right_date_col`, and a mapping row that names a different Right column fails the plan. Leave `data_type` empty for the default behaviour. Mapping rows define the effective `col_map` and type overrides. `config_json` remains in `pair_recon_pair_meta` only as a generated snapshot; the explicit columns are authoritative.

`pair_recon_pair_sql` contains at most one row per pair. A non-blank `left_sql` or `right_sql` replaces that side's table relation; a blank value uses its configured table. SQL remains the base relation so PairRecon can apply outer date guards, sampling, and aggregation. Named `{{query_windows:name}}` markers are supported on Oracle Left and Databricks Right SQL; declarations live beside that SQL in `left_query_windows_json` or `right_query_windows_json`.

Row checks always cover the complete requested range. Attribute sampling normally uses the `sample` fraction in `run_defaults_json` or the run prompt; for example, `0.1` means ten percent by deterministic key hash. Optional `vintage_values` or `vintage_sample_size` defaults may narrow which date buckets receive attribute checks; exact values normalize to the configured `vintage`, and only one vintage selector may be set. A random `vintage_sample_size` also skips the buckets that earlier runs of the same pair and vintage already finished, as recorded in `pair_recon_window_history`, so daily runs work through the remaining buckets; the log reports `vintage coverage: checked_before=4/36 selected=4 remaining_after=28`. Once every bucket is covered the run fails with `coverage N/N` until `--recheck` or explicit `vintage_values` is given. A window is recorded as finished only when every mapped column of that window has a durable Right result and all of its partitions matched on row count; windows with runtime failures are never recorded.

## Environment

Every PairRecon CLI, the installer, and the mapping notebook automatically load only `.pair_recon.env`; a value already present in the process environment wins. The normal UC-driven run needs `ORACLE_USER` plus either `ORACLE_PASSWORD` or the selected connection macro's `<MACRO>_PWD` value when Left is Oracle. `ORACLE_SVC` may map macros to services, for example `pb30:service,pcds:service`. Add `ORACLE_DSN` only when the macro map does not provide the service. With LDAP naming set `LDAP_DSN=ldap://host/service,cn=OracleContext,...` instead: PairRecon resolves it through `ldap3` to the TNS descriptor once per source, no service mapping is needed, `{service}` in the URL is filled from the macro map, and an explicit `dsn` or a per-macro `<MACRO>_LDAP_DSN` takes precedence. A pair may instead name custom `user_env`, `password_env`, `dsn_env`, and `ldap_dsn_env` variables in `left_options_json`. `PAIR_RECON_OUTPUT_DIR` optionally selects the local artifact root and defaults to `outputs` under the current working directory. Databricks authentication stays in `~/.databrickscfg`.

The earlier `pkg/distributed` branch used S3/AWS/WinSCP paths and many `DTRACK_*` runtime variables. The current UC Volume design does not require them.

## Run

```bash
./run.sh
just run
```

```bat
run.cmd
```

`just run smoke` (or `pair_recon smoke --defaults .pair_recon.yaml`) is the cheap pre-flight for the saved pair: both health checks, 100 sampled Left rows with the real predicate, a Volume round trip, and a short Right job that writes its state and re-selects those keys; a `sample_mismatch` verdict means key types, date mapping, or sampling columns differ before any full scan is paid for. `just run batch PAIR1 PAIR2 ...` runs pairs sequentially in subprocesses, classifies each by return code (`different` is an executed run, not a failure), and writes `batch_summary.json`. A plan without windows ends as `empty`, `left_only`, or `right_only` after both row counts are saved. Oracle sessions are capped per source (`ORACLE_MAX_SESSIONS`, default 4); `ORA-02391` waits `ORACLE_SESSION_COOLDOWN_SECONDS` (30) up to `ORACLE_SESSION_RETRIES` (3) times. The stage watchdog cancels both sides and keeps the Volume checkpoints when `FETCH_STALL_SECONDS` (1800), `RIGHT_FIRST_RESULT_SECONDS` (14400), `RIGHT_STATE_STALL_SECONDS` (7200), or `VOLUME_READ_FAILURES` (10) is exceeded; set a value to 0 to disable that check. The Right computes window statistics `RIGHT_STATS_COLUMNS` (50) columns at a time: each window is first materialized once as a persisted temp view (`SELECT * ... WHERE partition AND sample predicate`), so the count, every statistics batch, the diagnostic sample, and the row-level slice read that cache instead of re-planning and re-scanning the source relation (a view that takes minutes to analyze pays that only once per window); a window with at most `RIGHT_STATS_DRIVER_ROWS` (2000000) sampled rows is projected by Spark as normalized text, `RIGHT_STATS_DRIVER_CELLS` (5000000) cells per scan so a small window needs one scan, at most `RIGHT_DRIVER_FETCHES` (1) window holding a pandas frame at once (about 150 bytes per cell on the driver, so the defaults peak near 750 MB and suit a 15 GB driver), and reduced in pandas on the driver (the same sha256 hash sum and counts the Left's DuckDB computes), larger windows aggregate in Spark, so wide tables no longer stall Spark's planner; it publishes `fetching` progress (window, columns done) to `_protocol/right.json` after every batch, which is what the state-stall check watches. A state file read while the other side overwrites it (torn JSON) is retried on both sides instead of failing the job.

Use `just run --defaults` to load `.pair_recon.yaml` and start without any prompts; an optional YAML path may follow the flag. By default, PairRecon catches the specific Right error for unexcluded mismatched partitions, normalizes and saves those dates, then resumes the same daily run ID automatically (up to three retries). Add `--earlystop` to report that error immediately without modifying exclusions or rerunning. Add `--force` to clear the run's Volume/Catalog results and bypass PairRecon row/pull caches; automatic recovery remains a full restart when force is enabled. Whatever the flags, a failed or cancelled Right job stops the Left within seconds: the launcher cancels the Oracle, Hadoop, or DuckDB statements still running and does not wait for in-flight pulls to finish. Left column pulls start only after the Right has published its row-check result, so unexcluded mismatches surface before any multi-hour pull. Resuming keeps the Left row counts and rebuilds the plan from them, and a plan without any windows fails immediately instead of starting the Right job. No wait has a fixed deadline any more: the Right Databricks job starts only after the Left row count has been published, the Left waits for the Right row-check result for as long as the job is alive, the Right waits for Left units for as long as the Left heartbeat is fresh, and the launcher follows the job in one-hour polling chunks without a cap. The Left writes its heartbeat during the row count, every waiting loop, and every pull, so the only thing that ends a wait early is a dead peer: a stale Left heartbeat (ten minutes) stops the Right, and a failed or cancelled Right stops the Left. Add `--recheck` to ignore `pair_recon_window_history` when choosing random vintages.

The prompts ask for owner, Databricks profile, one Unity Catalog schema, pair, run mode, timezone, partition mode, checks, sample, optional date range and vintage, and confirmation. Date mode also asks for each side's date column and date type; types accept `date`, `string`, `number`, `sas`, or an inline JSON specification such as `{"type":"string","format":"compact"}`. Normalization is controlled only by enabled rows in `pair_recon_normalization`, never by a prompt or YAML switch. The table has only `rule_name`, `source`, `match_value`, `canonical_value`, and `enabled`: a null `match_value` identifies a supported transform, while a value defines a source-scoped synonym. `source=all` applies across platforms; synonym targets may use `@missing` or `@run_date`. The effective enabled rule list is saved as `normalization_rules` in `.pair_recon.yaml`, the run config, request snapshot, and `config/manifest.json`. The seeded rules are `trim`, `lowercase`, `decimal`, `strip_datetime`, `oracle_today_3000` (`3000-01-01` to `@run_date`), and `oracle_missing_9999` (`9999-12-31` to `@missing`), all enabled by default; rerunning `schema.sql` preserves existing `enabled` choices. `strip_datetime` makes ISO datetimes such as `2025-10-10 00:00:00.000` compare as `2025-10-10`. Mapping `data_type=date` remains available for explicit per-column date parsing. After `Vintage`, `Vintage sample` accepts `all`, a positive count such as `5`, or comma-separated `YYYY-MM-DD` values; the effective choice is saved to YAML and applied to the run config. These effective values are printed with the other settings. `Excluded partitions` accepts comma-separated `YYYY-MM-DD` values and inclusive ranges such as `2025-04-02->2025-04-05, 2025-04-10`; the normalized list is saved as `excluded_partitions`. Before pair selection, PairRecon always validates an existing configured workbook, compares its `pairs` and `pair_sql` rows with UC, and upserts only changed or new rows. Pair choices are always loaded from enabled UC rows. `full` mode starts either the configured Databricks job or local CSV Right. `left-only` uses the Left definition, vintage selection, sample, checks, and approved column mapping to publish Left artifacts only. Owner defaults to the `USERNAME` environment variable, falling back to the saved value only when `USERNAME` is unavailable. Every ordinary terminal question preloads its displayed default into an editable input buffer: press Enter to retain it, edit or delete it directly, or type `undo` to return to the previous question. Checks retain the checkbox interaction: each input redraws `[x]/[ ]`, comma-separated names or numbers toggle selections, and Enter confirms. `Run now` is always the final interactive question, and either answer overwrites the selected YAML defaults before exit or execution. The run ID is generated automatically as `<pair>-<owner>-YYYYMMDD` in the configured timezone; another run that day resumes compatible checkpoints under the same ID unless `--force` is supplied.

The exclusion prompt can be prefilled directly in `.pair_recon.yaml` for the saved pair:

```yaml
excluded_partitions: 2025-04-02->2025-04-05, 2025-04-10
```

PairRecon expands ranges inclusively and rewrites the saved setting as normalized dates after a confirmed run. For a numeric `YYYYMM` date type, dates in the same month collapse to the first-day logical partition key and SQL predicates use the physical `YYYYMM` number; excluding any date in April therefore excludes the April partition.

Before execution, PairRecon creates the local output root when needed and atomically writes `outputs/YYYY-MM-DD/<run_id>/request.json` by default. Set `PAIR_RECON_OUTPUT_DIR` to change that root; a relative value is resolved from the current working directory. The same run directory contains `left.log`, DuckDB temporary state, pulled Left CSVs, fixed-key CSVs, and their metadata. Source input files are never moved. The UTF-8 snapshot contains the current user settings, the pair/mapping/SQL rows pulled from Catalog, and the merged effective config; credential fields are omitted. SQL text is retained verbatim in `.txt` audit artifacts, so credentials must never be embedded in SQL. The Right notebook logs at INFO to stdout, so its progress shows live on the Databricks job run page and in the cluster driver logs, and a daemon thread rewrites the buffered log to `_protocol/right.log` on the Volume every 30 seconds (capped at 20 MB, Volume write failures are retried on the next flush); after the Right job ends PairRecon copies it to `right.log` next to `left.log`. When the Right job fails, PairRecon fetches the task's error and Python traceback through the Jobs run-output API and writes them, with the run page URL, into the raised error and `left.log`. On success the notebook returns its outcome counts via `dbutils.notebook.exit`, and they are merged into the result's `right` section without downloading result JSON files locally.

The Volume run directory is single-pair and flat; there is no intermediate `pairs/<pair>/`
directory. A completed full run has this logical shape (optional and exceptional files are marked):

```text
<run_id>/
├── config/
│   ├── manifest.json                machine-readable execution contract
│   ├── mapping.csv                  concise operator mapping view
│   └── sampled_keys.json            fixed-N sampling only
├── results/
│   ├── row.json                     reconciled partition counts
│   ├── units.csv                    terminal logical-unit snapshot
│   └── report/{index.html,report.xlsx}
├── differences/<vintage>/samples.xlsx  Left keys plus side-by-side differing attributes
├── differences/<vintage>/rows.xlsx     first sample_rows row-level differences of the differing columns
├── audit/
│   ├── summary.txt
│   ├── sql/*.txt
│   └── errors/*.json
└── _protocol/                        internal resumable distributed state
    ├── run.json
    ├── left.json
    ├── right.json
    ├── row/left.json
    ├── windows/<vintage>/{left.json,right.json,sample_keys.json}
    ├── units/<sequence>-<count>.json
    ├── outcomes/{matches,differences,failures}/
    └── tmp/{right_stats,right_samples}/
```

The protocol deliberately does not put every signal into one growing JSON file: concurrent rewrites
would create a single hot object and make crash recovery all-or-nothing. Instead, the two small
state files replace high-frequency marker scans, immutable payloads remain reconstructable, and
successful matches are compacted into bounded result batches. Differences and failures stay
individually addressable so `retry-failed` can delete only the work that must be rerun. Protocol 4
has no legacy marker files: `_protocol/` is private machine state, while `config/`, `results/`,
`differences/`, and `audit/` are the stable operator-facing surface.

A successful `left-only` run ends as `left_complete`, not `passed`, because no reconciliation verdict exists. A full run ends as `passed` or `different` only when every planned unit has a durable Right result; any runtime failure leaves the run `incomplete`, and a plan whose windows or columns were all excluded fails on both sides instead of passing with nothing compared. It uploads full-range Left row counts to `_protocol/row/left.json`; sampling affects attribute statistics, not those counts. Column checks publish the execution contract under `config/`, batched Left statistics under `_protocol/units/`, and completion in `_protocol/left.json`. `results/row.json`, `results/units.csv`, and `results/report/report.xlsx` are the operator-facing results. For a differing vintage, `differences/<vintage>/samples.xlsx` shows the original Left sampled keys followed by only the statistically differing Left attributes, two blank spacer columns, and the corresponding Right attributes. The same vintage also gets `rows.xlsx` (also saved as `rows.xlsx` in the local run directory beside `left.csv`): after comparison the Right streams every sampled row of the differing columns (normalized key, raw values, normalized values) partition by partition into a gzip CSV, and the Left joins them with its local window CSV in DuckDB on the normalized key, so only `sample_rows` rows whose normalized values differ, or that exist on one side only, are written, with a `status` column and red differing cells. Left data never leaves the machine; the Right slice is deleted from the Volume after publishing. Differing cells are red, key headers are bold, and `sample_rows` limits the output per vintage (default 50). A resume removes derived `results/report/*` and `results/units.csv`, then regenerates them at a successful terminal boundary while preserving compatible protocol checkpoints. Only `--force` clears the entire run.

Registered attribute checks are `n_total`, `n_missing`, `n_unique`, `mean`, `std`, `min`, `max`, `length_avg`, `length_max`, and `hash_sum`; `row_count` remains the pair-level check. `partition=all` treats the complete relation as the pair-level `all` partition and does not require a date column.

Detailed differences are stored in `pair_recon_run_error`. For a column-statistic mismatch, `column_name` identifies the Left column, `vintage` identifies the comparison bucket, `check_name` identifies the failed statistic, and `left_value`/`right_value` contain the compared values. `payload` contains the complete result JSON, including the mapped Right column and every calculated match/difference field. Row-count findings use `check_name='row_count'` and `partition_value`; execution failures use `check_name='runtime'` and put the message in `detail`. A successful left-only run produces no findings. `pair_recon_run_status` stores lifecycle and summary counts plus the effective `from_date`, `to_date`, `vintage`, `sample_rate`, and `vintage_sample`. `pair_recon_progress_snapshot.payload` is the complete expandable JSON snapshot, including run, Left, Right, row, unit, and thread state. `pair_recon_reference` documents every status/error/snapshot field and enumerated keyword. The Right notebook publishes successful/different summaries itself; the local launcher only forwards failed/cancelled Jobs API details directly, without downloading Volume result JSON. `pair_recon_pair_meta.latest_run_id/latest_status` points to the latest outcome, including `left_complete`. `pair_recon_window_history` holds one row per finished vintage window (`pair_name`, `vintage`, `window_value`, `run_id`, `result`, partition/column/unit counts, `checked_at`); a run replaces its own rows when it republishes, and the next day's random vintage selection excludes every window listed there for other run IDs. Create it on existing installations by rerunning `setup/schema.sql` (or the `uc_tables` notebook); until it exists, publishing logs `window history not written` and continues.

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

`dbx runs [JOB_ID_OR_NAME]` accepts a Databricks job ID or exact job name; when omitted it uses `right_job_id` from `.pair_recon.yaml`. `dbx runs --run-id RUN_ID` shows one concrete run. The cache inspection surface uses the configured SQL warehouse and `DBX_CATALOG_SCHEMA.pair_recon_cache_index`: `dbx cache [PAIR]` explains the selected Pair (defaulting to `.pair_recon.yaml`), `dbx cache show CACHE_KEY` prints one complete index row, and `dbx cache peek CACHE_KEY [--limit 5]` previews JSON/text payloads or Parquet rows.

Set `DBX_PROFILE`, `DBX_CLUSTER_ID`, `DBX_WAREHOUSE_ID`, `DBX_WORKSPACE_PDIR`, `DBX_JOB_NAME`, `DBX_CATALOG_SCHEMA`, `DBX_PAIR_WORKBOOK`, and `DBX_RUN_PATH` in `.pair_recon.env` to omit repeated setup and operator options. `DBX_CATALOG_SCHEMA` is exactly `catalog.schema`; PairRecon derives every fixed table name from it. `DBX_PAIR_WORKBOOK` selects the local workbook; when it exists, its changes are upserted before reading the remote metadata tables. `DBX_RUN_PATH` supplies the Volume runs path. `dbx runs` reads the saved `right_job_id` from `.pair_recon.yaml` when `--job-id` is omitted; an explicit `--job-id` still wins. `dbx sql` reads its profile and warehouse from that environment file: `--unity` accepts a Unity Catalog three-part table or SQL, `--glue` accepts a Hive metastore `database.table` or SQL, and both print five rows by default. Relative Workspace paths resolve below `/Users/<current-user>/`; absolute paths remain supported. Workspace `ls`, `cp`, `mv`, and `rm` follow the same path rule; recursive copy uses `cp -r`, replacement requires `-f`, and mutating operations support `--dry-run`. Because the Workspace API has no server-side rename or copy endpoint, `cp` uses export/import and `mv` deletes the source only after copying succeeds; `mv` is not atomic. Upload and download support files, notebooks, recursive folders, `--dry-run`, and `--overwrite`. `rm` accepts multiple paths, `-r/--recursive`, `-f/--force`, and `--dry-run`. The setup installer and `Sync remote` mirror `src/pair_recon_right` to `DBX_WORKSPACE_PDIR/pair_recon_right`, deleting remote package objects that no longer exist locally only after every upload succeeds. Its job notebook is the sibling `DBX_WORKSPACE_PDIR/pair_recon_right_job`; its first cell installs the uploaded package folder's `requirements.txt`, its second calls `dbutils.library.restartPython()`, and the remaining cells import the package and run the job. The requirements path is rendered as an absolute `/Workspace/...` path during upload. Add Right-only dependencies such as `openpyxl>=3.1` to [src/pair_recon_right/requirements.txt](src/pair_recon_right/requirements.txt). Set `DBX_PKG_RENAME=pair_recon_right -> pair_recon` to upload the folder as `pair_recon` (notebook `pair_recon_job`); only the notebook's `pair_recon_right` imports are rewritten on upload, because every module inside the package uses relative imports. If the configured Right job name or saved ID no longer exists, PairRecon catches `RESOURCE_DOES_NOT_EXIST`, creates a job named by `DBX_JOB_NAME` on `DBX_CLUSTER_ID`, and saves Databricks' new numeric job ID to `.pair_recon.yaml`. Delete the prefilled Right job reference to resolve or create the job from `DBX_JOB_NAME`. Left and Right sources come directly from pair metadata, so the run prompt no longer asks for duplicate input types or CSV paths. The run prompt asks whether to sync the Right package (default `true` for a Databricks Right), whether to retain SQL audit artifacts (default `false`), the Left source-query budget (default `2`), and the Databricks query budget (default `8`). `STATS_WORKERS` controls local per-vintage statistics shards and defaults to `2`. Use `just dbx --debug attach ...` or `just dbx --debug console ...` only when troubleshooting.

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

Every recipe defaults to normal mode. Python workflows accept `--debug console` or `--debug attach`; console enters ipdb post-mortem at the original exception frame, while attach waits for VS Code `debugpy` on port 5678. Lint uses `--debug` for verbose diagnostics. `just sas` remains available for an explicitly requested standalone SAS bundle. The integrated `sas_sample_csv` flow needs no source switch or CSV-input prompt: the first run emits native SQL beside the configured CSV, and the full run consumes that validated CSV automatically for Left column sampling.

See [guideme.txt](guideme.txt) for the complete local, Databricks, Hadoop, CSV, SAS, progress, Outlook, and troubleshooting flow.
