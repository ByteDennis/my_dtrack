# Remote Left/Right run with a live dashboard

This example turns the former `06_pair_progress.ipynb` walkthrough into two explicit phases:

```text
remote_job/
├── setup/                 one-time Databricks deployment
├── src/                   checked-in launcher, collector notebook, and dashboard definition
├── run.sh                 Linux/macOS entry
└── run.cmd                Windows entry (also works by double-clicking)
```

The local launcher submits the saved Right Databricks job first, then starts the Left work on the
current machine. The two sides coordinate through the same run folder. When Right finishes, the
launcher immediately runs the progress collector. The collector appends a Delta snapshot and
updates automatic blockers; the Views and Dashboard do not need to be rebuilt because their
datasets query those Views directly. A Databricks Alert then emails the run summary by using its
HTML template and query-result table. A scheduled collector also refreshes active runs every 15
minutes.

## 1. One-time setup

Prerequisites:

- `uv sync --extra dbx` has completed.
- The selected Databricks profile can upload Workspace files, create Jobs, use the SQL warehouse,
  create Alerts, create tables/Views in the target schema, and create/publish Lakeview dashboards.
- The SQL warehouse is serverless or pro and the setup identity has permission to run the Alert.
- `--right-runs` is the Databricks-visible path for the same objects that Left writes. For example,
  Left can use `s3://bucket/dtrack/runs` while Right uses an External Volume path that exposes that
  location.
- If Right reads `s3://` directly without an instance profile, pass `--aws-secret-scope`.

Linux/macOS:

```bash
examples/remote_job/setup/setup.sh \
  --config config.json \
  --right-runs /Volumes/main/dtrack/work/dtrack/v1/runs \
  --workspace-dir /Workspace/Shared/dtrack-example \
  --schema main.dtrack \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --profile corp \
  --alert-email you@example.com
```

Windows PowerShell or Command Prompt:

```bat
examples\remote_job\setup\setup.cmd --config config.json --right-runs /Volumes/main/dtrack/work/dtrack/v1/runs --workspace-dir /Workspace/Shared/dtrack-example --schema main.dtrack --warehouse-id YOUR_WAREHOUSE_ID --profile corp
```

Running either setup entry without those options prompts for the missing values. Setup is
idempotent: it uploads the current `src/dtrack_right`, creates/replaces the Delta Views, upserts
active Pair rows, creates or resets the Right, collector, and Alert Jobs, creates or updates the
dashboard, and creates or updates the Databricks Alert. It writes runtime identifiers and Alert
recipients to `.setup.json`. If `--alert-email` is omitted, setup uses the current Databricks
user's workspace email. Repeat the option for multiple workspace users, or set a comma-separated
`DTRACK_ALERT_EMAIL` value.

## 2. Run-level inputs

Run from the repository root. With no arguments, the scripts prompt for the config and Left runs
root:

```bash
examples/remote_job/run.sh
```

Or provide them explicitly:

```bash
examples/remote_job/run.sh \
  --config config.json \
  --run-s3 s3://bucket/dtrack/runs \
  --run-id adhoc-20261016 \
  --pair orders \
  --pair customers
```

Windows users can double-click `run.cmd` for prompts or pass the same options in a terminal. The
launcher uses `pathlib`, UTF-8 JSON, and argument lists rather than shell quoting, so the Python
flow is shared across Windows and Databricks/Linux.

The sequence is:

1. Submit the saved Right job with this run's `run_id`.
2. Run `doctor` locally.
3. For config version 1, run `row-check` then `col-check`; for version 2, run `left-run`.
4. Wait for Right to publish the final report.
5. Run the collector immediately and print the dashboard URL.
6. Run the serialized Alert Job for this `run_id`; Databricks evaluates the template and emails
   the summary table and Dashboard link.

Reusing the same `run_id` resumes completed DTrack work. This simultaneous launcher deliberately
does not expose `--force`: clearing an old run while Right is already starting could let Right read
stale protocol documents. Use the normal `dtrack tools rollback` workflow before relaunching when
saved stages must be cleared. If a local step fails, the launcher cancels the Right run that it
started, asks the collector to snapshot the partial failure, and evaluates the same Alert so the
failure is also visible by email.

The Alert schedule itself is paused. The saved Alert Job evaluates it immediately after each
collector run. A first task writes the requested `run_id` into a singleton Delta context row and a
second Alert task reads only that run. The Job has `max_concurrent_runs: 1`, so overlapping local
launchers cannot cause one run's email to contain another run's data. The Alert uses
`retrigger_seconds: 1`, which sends a notification for every requested evaluation even while the
condition remains triggered. Databricks Alert templates expose a query-result table rather than
arbitrary local file attachments; the email therefore links to the live dashboard and the HTML
report URI instead of attaching HTML/XLSX files.

## Dashboard data path

```text
Left + Right protocol documents
        │
        ▼
progress collector Job
        │ append / merge
        ▼
dtrack_progress_snapshot + dtrack_pair_blocker
        ├───────────────────────────────┐
        ▼                               ▼
v_pair_status + v_pair_daily     Databricks Alert Job
        │ query                         │ template email
        ▼                               ▼
Lakeview dashboard                 workspace users
```

The View definitions are stable setup objects. Normal runs update their backing Delta tables; the
next dashboard query/refresh therefore reads the new snapshot without republishing the dashboard.
