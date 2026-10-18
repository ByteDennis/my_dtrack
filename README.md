# pair_recon

`pair_recon` validates one Oracle, Hadoop, or local CSV source against one Databricks or local CSV target per execution. Pair definitions and SQL stay in Unity Catalog. Local defaults are saved in UTF-8 YAML; credentials stay in `.pair_recon.env` and `~/.databrickscfg`.

## Runtime

1. The local launcher reads one enabled pair from the Unity Catalog metadata table.
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

Install Hadoop support with `uv sync --extra hadoop`; the `hive` ODBC DSN comes from `HADOOP_DSN`, so the run prompt only needs the Hadoop schema. Use [setup/schema.sql](setup/schema.sql) when the four Delta tables do not already exist.

## Run

```bash
./run.sh
just run
```

```bat
run.cmd
```

The prompts ask for owner, Databricks profile and table locations, pair, Left and Right inputs, timezone, partition mode, checks, sample, strict normalization, optional date range and vintage, run ID, and confirmation. Press Enter to reuse saved defaults. A generated run ID remains the default so a normal run does not overwrite history.

Supported checks are `row_count`, `hash_sum`, and `n_unique`. `partition_mode=all` treats the complete relation as the pair-level `all` partition and does not require a date column.

For a current-view Left source against Right history, set `snapshot: true` in that pair's metadata. Left is counted as a complete snapshot; Right chooses the latest partition in the configured range whose row count matches, then column checks compare the snapshot with that partition.

## Verify

```bash
just test
just lint
just build
```

Live Oracle query/export/timing tests are opt-in and are not part of the default suite:

```bash
just live
```

Every executable Just recipe defaults to normal mode and accepts `--debug`; Python run failures open `ipdb`, while build and lint recipes emit verbose diagnostics. `just sas` creates only `left.sas`; use `just sas side=right` or `just sas side=both` when needed. After SAS writes either sampled CSV, use the normal `run.cmd` or `just run` prompts to select CSV inputs. When Right is CSV, comparison stays local and reuses the existing multithread statistics engine; no extra recipe is needed.

See [guideme.txt](guideme.txt) for the complete local, Databricks, Hadoop, CSV, SAS, progress, Outlook, and troubleshooting flow.
