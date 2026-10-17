# Remote Left/Right run with dashboard and Outlook delivery

This example separates one-time Databricks deployment from each reconciliation run:

```text
remote_job/
├── setup/                 one-time Databricks deployment
├── src/                   launcher, Outlook sender, collector notebook, dashboard definition
├── run.cmd                Windows entry (also works by double-clicking)
└── run.sh                 Windows Git Bash entry
```

The local launcher submits the saved Right Databricks job first, then starts Left on the current
machine. Both sides coordinate through the same run folder. When Right finishes, the launcher runs
the collector immediately. The collector appends a Delta snapshot and updates blockers; the Views
and Dashboard read those tables, so normal runs do not rebuild or republish the dashboard. Finally,
the local launcher sends one custom Outlook message with the live Dashboard link and every available
HTML, XLSX, and JSON report attached.

Outlook is the only delivery path. There is no `--mail-mode` switch and no Databricks Alert Job.

## 1. One-time setup

Prerequisites:

- `uv sync --extra dbx` has completed.
- The selected Databricks profile can upload Workspace files, create Jobs, use the SQL warehouse,
  create tables/Views in the target schema, and create/publish Lakeview dashboards.
- `--right-runs` is the Databricks-visible path for the same objects that Left writes. For example,
  Left can use `s3://bucket/dtrack/runs` while Right uses an External Volume path exposing that
  location.
- If Right reads `s3://` directly without an instance profile, pass `--aws-secret-scope`.

Linux/macOS or Windows Git Bash:

```bash
examples/remote_job/setup/setup.sh \
  --config config.json \
  --right-runs /Volumes/main/dtrack/work/dtrack/v1/runs \
  --workspace-dir /Workspace/Shared/dtrack-example \
  --schema main.dtrack \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --profile corp \
  --email-to you@example.com
```

Windows PowerShell or Command Prompt:

```bat
examples\remote_job\setup\setup.cmd --config config.json --right-runs /Volumes/main/dtrack/work/dtrack/v1/runs --workspace-dir /Workspace/Shared/dtrack-example --schema main.dtrack --warehouse-id YOUR_WAREHOUSE_ID --profile corp --email-to you@example.com
```

Running either setup entry without required options prompts for the missing values. Setup is
idempotent: it uploads the current `src/dtrack_right`, creates/replaces the Delta Views, upserts
active Pair rows, creates or resets the queued Right and collector Jobs, and creates or updates the
dashboard. It writes runtime identifiers and Outlook recipients to `.setup.json`. Repeat
`--email-to` for multiple recipients, use a comma/semicolon-separated value, or set
`DTRACK_EMAIL_TO`.

## 2. Run-level inputs

The run launcher requires Windows, an interactive signed-in Classic Outlook profile, and the
`dbx` extra (which installs `pywin32` on Windows). New Outlook does not expose the COM automation
used here. The launcher checks the platform and Python dependency before starting the Databricks
job; Outlook validates the interactive profile when the message is created.

From the repository root, double-click `run.cmd`, invoke it from a terminal, or use `run.sh` in
Windows Git Bash. With no arguments, the scripts prompt for the config and Left runs root:

```bat
examples\remote_job\run.cmd
```

```bash
examples/remote_job/run.sh \
  --config config.json \
  --run-s3 s3://bucket/dtrack/runs \
  --run-id adhoc-20261017 \
  --pair orders \
  --pair customers
```

The command and setup scripts enable UTF-8, while Python uses `pathlib`, UTF-8 JSON, and argument
lists rather than shell-quoted command strings. Databricks Right still runs on Linux; only the local
Outlook delivery boundary is Windows-specific.

The sequence is:

1. Verify that Classic Outlook COM automation is available locally.
2. Submit the saved Right job with this run's `run_id`.
3. Run `doctor` locally.
4. For config version 1, run `row-check` then `col-check`; for version 2, run `left-run`.
5. Wait for Right to publish the final report.
6. Run the collector immediately so the backing Views and Dashboard reflect this run.
7. Read `report/summary.json`, `report/index.html`, and `report/report.xlsx` from the run folder;
   send a custom Outlook HTML email and attach every file that exists.

Reusing the same `run_id` resumes completed DTrack work. This simultaneous launcher deliberately
does not expose `--force`: clearing an old run while Right is already starting could let Right read
stale protocol documents. Use the normal `dtrack tools rollback` workflow before relaunching when
saved stages must be cleared.

If a local or remote step fails, the launcher cancels the Right run it started, asks the collector
to snapshot the partial failure, and sends a `FAILED` Outlook message. Missing report files do not
prevent that failure email; only files already published are attached. Outlook uses the default
interactive profile and account, calls `Save()` before `Send()`, and does not store mail credentials
in `.setup.json`.

## Dashboard and email data path

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
v_pair_status + v_pair_daily      local Outlook sender
        │ query                         │ HTML + attachments
        ▼                               ▼
Lakeview dashboard                 configured recipients
```

The View definitions are stable setup objects. Normal runs update their backing Delta tables; the
next dashboard query or refresh therefore reads the new snapshot without republishing the dashboard.
