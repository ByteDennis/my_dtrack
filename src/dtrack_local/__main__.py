# python -m dtrack_local: the backup plan, everything on this machine, step by step
#
# 0. Once: `uv sync --extra dbx`; .env with DTRACK_DBX_PROFILE (corp), DTRACK_DBX_WAREHOUSE_ID,
#    the Left credentials, and optionally DTRACK_RUN_S3.
# 1. Check:        python -m dtrack_local preflight --config c.json
#    Sample keys:  python -m dtrack_local key-check --config c.json --pair orders
# 2. Everything:   python -m dtrack_local run --config c.json \
#                      [--run-s3 s3://<bucket>/dtrack/runs]
#    or step by step in src/dtrack_local/notebooks: 00 setup/preflight, 01 row check,
#    02 col check, 03 report + backup, 04 roll one pair back
# 3. Look:         <out>/<run_id>/report/index.html, report.xlsx;
#                  duckdb -readonly <out>/<run_id>/_local/data.duckdb
#
# Interrupted? The same command with --run-id <run_id> resumes; finished work is skipped.
# Presigned downloads blocked? --disposition inline. Need the full Right window? --no-push-sample.
# Config fixed? dtrack tools rollback --run-s3 <out> --run-id <run_id> --pair P \
#   [--pair Q|--pair all] --stage col|row
#   [--config c.json] --yes, then `run --run-id <run_id> --pair P` (or notebook 04).

from .cli import main

raise SystemExit(main())
