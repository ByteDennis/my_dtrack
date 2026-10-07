# dtrack local: the backup plan, everything on this machine, step by step
#
# 0. Once: `uv sync --extra dbx`; .env with DTRACK_DBX_PROFILE (corp), DTRACK_DBX_WAREHOUSE_ID,
#    the Left credentials, and optionally DTRACK_RUN_S3.
# 1. Check:        dtrack local preflight --config c.json
#    Sample keys:  dtrack local key-check --config c.json --pair orders
# 2. Everything:   dtrack local run --config c.json \
#                      [--run-s3 s3://<bucket>/dtrack/runs]
#    See examples/nbs/02_local.ipynb for the current SQL Warehouse workflow.
# 3. Look:         <out>/<run_id>/report/index.html, report.xlsx;
#                  duckdb -readonly <out>/<run_id>/_local/data.duckdb
#
# Interrupted? The same command with --run-id <run_id> resumes; finished work is skipped.
# Sampling always happens before transfer.
# Config fixed? dtrack tools rollback --run-s3 <out> --run-id <run_id> --pair P \
#   [--pair Q|--pair all] --stage col|row
#   [--config c.json] --yes, then `run --run-id <run_id> --pair P` (or notebook 04).

from .cli import main

raise SystemExit(main())
