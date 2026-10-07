# dtrack_local: the backup plan; both sides pulled to this machine and compared here
#
# API (what the notebooks and `python -m dtrack_local` call)
#   open_run(config, run_id=None, out=".dtrack-local/runs", profile=None, warehouse_id=None,
#            inline=False, pairs=None) -> LocalRun
#   preflight(run)            Left SELECT ... WHERE 1=0, Right DESCRIBE QUERY
#   row_check(run, gate="auto"|"manual"|"ratio:x")   Stage 1: counts, reconcile, gate, manifests
#   col_check(run)            Stage 2: pull, statistics, compare (units skipped when already done)
#   report(run)                                      <run>/report/
#   backup(run, s3_root)                             <run> -> s3_root/<run_id>/ (never _local/)
#   run_all(run, gate=, run_s3=)
#
# Run folder <out>/<run_id>/ (same layout as S3, so `rollback --run-s3 <out> --run-id` works)
#   row_check/  manifest/  _INDEX/  units/  results/  dead/  reports/  report/  audit.txt
#   audit/ detailed events (latest five runs); audit_errors/ best-effort failures; _STATUS.json
#   _local/left/  _local/right/   pulled window CSVs;  _local/data.duckdb   every pulled window
#   _local/logs/errors.log         ERROR/CRITICAL records and unexpected tracebacks; not console
#   (tables left__<pair>, right__<pair>, column dtrack_bucket)
#
# Right side: WarehouseTarget reads through a SQL warehouse (Statement Execution API): counts are
#   one GROUP BY there, JSON_ARRAY windows come back as text, statistics are computed here
#   with the Left side's code. By default result chunks use presigned links; inline=True returns
#   rows through the workspace host (<= 25 MiB per window). Sampling always runs in the warehouse.

from .pipeline import (
    Landing,
    LocalRun,
    WarehouseTarget,
    backup,
    col_check,
    open_run,
    preflight,
    report,
    row_check,
    run_all,
)
from .warehouse import Warehouse

__all__ = [
    "Landing",
    "LocalRun",
    "Warehouse",
    "WarehouseTarget",
    "backup",
    "col_check",
    "open_run",
    "preflight",
    "report",
    "row_check",
    "run_all",
]
