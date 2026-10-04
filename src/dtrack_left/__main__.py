# python -m dtrack_left (same as `dtrack`): the distributed flow, step by step
#
# 0. Once: `uv sync` (+ --extra dbx / ldap / athena as needed); .env with the Left credentials
#    (<macro>_USR/_PWD/_DSN), DTRACK_REPORT_S3, DTRACK_DBX_PROFILE; upload the Right package:
#      dtrack dbx workspace upload src/dtrack_right /Users/<you>/dtrack/dtrack_right --overwrite
# 1. Check the config:        dtrack doctor --config c.json
# 2. Pick the run:            export DTRACK_S3=s3://<bucket>/dtrack/work/runs/<run_id>
#                             (the last folder is the run id; a new id for every fresh run)
# 3. Start the Right job:     notebook dtrack_right/notebook with widgets work=<runs root>,
#                             run_id=<run_id>, report=<report store>, gate=auto
#                             [aws_secret_scope when work is s3://]; it may start before step 4
# 4. Stage 1, Left counts:    dtrack row-check --config c.json
# 5. Stage 2, Left units:     dtrack col-check           (waits for row_check/_CLOSED)
# 6. Follow the Right job:    dtrack dbx runs --active   |   dtrack dbx runs --run-id <job run>
# 7. Report:                  dtrack fetch-report --out ./report [--run-id <run_id>]
#
# Interrupted? Re-run the same step with the same run id; finished work is skipped.
# Config fixed for one pair?  dtrack rollback --run $DTRACK_S3 --pair P --stage col|row [--yes]
#   then `col`: steps 3, 5 (--pair P), 6, 7;  `row`: steps 3-7 with --pair P on 4 and 5.
#   Several pairs or Stage 1 settings of many pairs changed: use a new run id instead.

from .cli import main

raise SystemExit(main())
