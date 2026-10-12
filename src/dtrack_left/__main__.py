# python -m dtrack_left (same as `dtrack`): the distributed flow, step by step
#
# 0. Once: `uv sync` (+ --extra dbx / ldap / athena as needed); .env with the Left credentials,
#    DTRACK_RUN_S3 (stable runs root), optional DTRACK_RUN_ID, DTRACK_DBX_PROFILE; upload Right:
#      dtrack tools dbx workspace upload src/dtrack_right /Users/<you>/dtrack/dtrack_right --overwrite
# 1. Check the config:        dtrack tools doctor --config c.json
# 2. Pick the run:            export DTRACK_RUN_S3=s3://<bucket>/dtrack/work/runs
#                             export DTRACK_RUN_ID=<new id>  # or let row-check generate one
# 3a. Config version 2:       dtrack left-run --config c.json --pull-workers 1 --stat-workers 1
#                             then start/restart the Right job after Left finishes
# 3b. Config version 1:       start the Right job: notebook dtrack_right/notebook with widgets run_s3=<runs root>,
#                             run_id=<run_id>, gate=auto; report goes to <run>/report/
#                             [aws_secret_scope when run_s3 is s3://]; it may start before step 4
# 4. Stage 1, Left counts:    dtrack row-check --config c.json
# 5. Stage 2, Left units:     dtrack col-check           (waits for _status/row_check.closed.json)
# 6. Follow the Right job:    dtrack tools dbx runs --active | dtrack tools dbx runs --run-id <job run>
# 7. Report:                  read <run-s3>/<run-id>/report/ or publish it with tools blog
#
# Interrupted? Re-run the same step with the same run id; finished work is skipped.
# Config fixed? dtrack tools rollback --pair P [--pair Q|--pair all] --stage col|row
#   then `col`: steps 3, 5 (--pair P), 6, 7;  `row`: steps 3-7 with --pair P on 4 and 5.
#   Several pairs or Stage 1 settings of many pairs changed: use a new run id instead.

from .cli import main

raise SystemExit(main())
