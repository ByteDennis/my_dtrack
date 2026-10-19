# pair_recon

`pair_recon` validates one Oracle, Hadoop, or local CSV source against one Databricks or local CSV target per execution. Pair definitions and SQL stay in Unity Catalog. Local defaults are saved in UTF-8 YAML; credentials stay in `.pair_recon.env` and `~/.databrickscfg`.

## Runtime

1. The local launcher reads one enabled pair plus its mappings and optional SQL from Unity Catalog.
2. Left pulls source counts and selected column statistics with bounded worker threads.
3. Artifacts are written to a Unity Catalog Volume under an isolated `run_id`.
4. Progress snapshots are appended to a Delta table while validation runs.
5. A Databricks job validates a catalog Right side; a CSV Right side runs locally without starting that job.
6. Pair, run-status, and run-error Delta tables are updated for either path.
7. Windows sends the result through the signed-in Outlook profile when recipients are configured.

Concurrent users may validate the same pair because all artifacts and result rows are isolated by `run_id`. Resume removes failed or different results and preserves matches. Restart replaces the selected run.

## Install

```bash
uv sync
cp .pair_recon.env.example .pair_recon.env
./setup/setup.sh --profile corp --workspace-dir /Workspace/Users/name/pair_recon --cluster-id CLUSTER_ID
```

Windows:

```bat
copy .pair_recon.env.example .pair_recon.env
setup\setup.cmd --profile corp --workspace-dir /Workspace/Users/name/pair_recon --cluster-id CLUSTER_ID
```

Install Hadoop support with `uv sync --extra hadoop`; put its non-secret ODBC DSN name in the pair's `left.dsn` metadata so the run prompt only needs the Hadoop schema. Use [setup/schema.sql](setup/schema.sql) to create the six Delta tables. The column-map and pair-SQL tables are resolved automatically in the same catalog/schema as the configured pair table. Existing installations created before vintage selection must run [setup/migrate_pair_sql_vintages.sql](setup/migrate_pair_sql_vintages.sql) once.

`pair_recon_column_map.data_type` accepts `numeric`, `categorical`, a registered type such as `sas_datetime_date`, or a custom JSON type such as `{"type":"date","left_date_type":"timestamp","right_date_type":"date"}`. Mapping rows replace `col_map`, `col_type_overrides`, and `column_types` from `config_json` for that pair.

`pair_recon_pair_sql` contains at most one row per pair. A non-blank `left_sql` or `right_sql` replaces that side's table relation; a blank value uses its configured table. SQL remains the base relation so PairRecon can apply outer date guards, sampling, and aggregation. Named `{{query_windows:name}}` markers are supported on Oracle Left and Databricks Right SQL; declarations remain under the matching side's `query_windows` object in `config_json`.

Row checks always cover the complete requested range. Attribute checks may use all vintage buckets (both selection fields null), a deterministic random subset (`vintage_sample_size`), or exact buckets (`vintage_values`). Exact values are normalized to the selected day/week/month/quarter/year bucket. Random selection changes with a new run ID but is stable when the same run is resumed. Set only one selection field; unavailable explicit buckets fail before attribute work starts.

## Environment

The normal UC-driven run needs only `ORACLE_USER` and `ORACLE_PASSWORD` in `.pair_recon.env` when Left is Oracle. Add `ORACLE_DSN` only when the pair metadata does not already provide `left.service` or `left.dsn`. A pair may instead name custom `user_env`, `password_env`, and `dsn_env` variables. Databricks authentication stays in `~/.databrickscfg`; Hadoop's DSN, DBX IDs, GitLab host, worker counts, memory, and paths are metadata, prompt defaults, CLI options, or optional tuning—not required environment variables.

The earlier `pkg/distributed` branch used S3/AWS/WinSCP paths and many `DTRACK_*` runtime variables. The current UC Volume design does not require them.

## Run

```bash
./run.sh
just run
```

```bat
run.cmd
```

The prompts ask for owner, Databricks profile and table locations, pair, Left and Right inputs, timezone, partition mode, checks, sample, strict normalization, optional date range and vintage, run ID, and confirmation. Checks use a terminal checkbox list: enter comma-separated names or numbers to toggle `[x]` selections, then press Enter to confirm. Press Enter elsewhere to reuse saved defaults. A generated run ID remains the default so a normal run does not overwrite history.

After confirmation and before execution, PairRecon atomically writes `.pair_recon-work/<run_id>/request.json`. The UTF-8 snapshot contains the current user settings, the pair/mapping/SQL rows pulled from Catalog, and the merged effective config; credential fields are omitted. SQL text is retained verbatim for audit, so credentials must never be embedded in SQL.

Registered attribute checks are `n_total`, `n_missing`, `n_unique`, `mean`, `std`, `min`, `max`, `length_avg`, `length_max`, and `hash_sum`; `row_count` remains the pair-level check. `partition_mode=all` treats the complete relation as the pair-level `all` partition and does not require a date column.

For a current-view Left source against Right history, set `snapshot: true` in that pair's metadata. Left is counted as a complete snapshot; Right chooses the latest partition in the configured range whose row count matches, then column checks compare the snapshot with that partition.

## Databricks tools

`just dbx` uses the Databricks SDK profile without storing credentials in PairRecon files:

```bash
just dbx login --no-open
just dbx workspace upload src/pair_recon_right /Workspace/Users/name/pair_recon_right --dry-run
just dbx workspace download /Workspace/Users/name/pair_recon_right downloaded-right
just dbx compute status --cluster-id CLUSTER_ID
just dbx warehouse status --warehouse-id WAREHOUSE_ID
just dbx sql "SELECT current_user()" --warehouse-id WAREHOUSE_ID
just dbx runs --limit 10
```

Set `PAIR_RECON_DBX_PROFILE`, `PAIR_RECON_DBX_CLUSTER_ID`, and `PAIR_RECON_DBX_WAREHOUSE_ID` in `.pair_recon.env` to omit repeated options. Upload and download support files, notebooks, recursive folders, `--dry-run`, and `--overwrite`. Use `just dbx --debug attach ...` or `just dbx --debug ipdb ...` only when troubleshooting.

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
just build
just publish
```

`just publish` builds a main-program-only portable ZIP for the current operating system and CPU under `dist/`. Run it on Windows x64 to produce the full Windows x64 artifact. The extracted `pair-recon` folder contains only the main executable, its no-Python launcher, and required runtime libraries; DBX/GitLab tools, source documentation, setup SQL, and credentials are excluded. Build and test each target platform on that platform. Windows publishing includes `pyodbc` for the Hadoop input path; non-Windows portable builds currently omit Hadoop/pyodbc. ODBC drivers remain external.

Live Oracle query/export/timing tests are opt-in and are not part of the default suite:

```bash
just live
```

Every recipe defaults to normal mode. Python workflows accept `--debug ipdb` or `--debug attach`; attach waits for VS Code `debugpy` on port 5678. Build and lint use `--debug` for verbose diagnostics. `just sas` creates only `left.sas`; use `just sas side=right` or `just sas side=both` when needed. After SAS writes either sampled CSV, use the normal `run.cmd` or `just run` prompts to select CSV inputs. When Right is CSV, comparison stays local and reuses the existing multithread statistics engine; no extra recipe is needed.

See [guideme.txt](guideme.txt) for the complete local, Databricks, Hadoop, CSV, SAS, progress, Outlook, and troubleshooting flow.
