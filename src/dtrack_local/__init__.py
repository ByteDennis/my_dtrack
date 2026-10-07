# dtrack_local: the backup plan; both sides pulled to this machine and compared here
#
# API (what the notebooks and `python -m dtrack_local` call)
#   open_run(config, run_id=None, out=".dtrack-local/runs", profile=None, warehouse_id=None,
#            disposition="external_links"|"inline", push_sample=True, pairs=None) -> LocalRun
#   preflight(run)            Left SELECT ... WHERE 1=0, Right DESCRIBE QUERY
#   row_check(run, gate="auto"|"manual"|"ratio:x")   Stage 1: counts, reconcile, gate, manifests
#   col_check(run)            Stage 2: pull, statistics, compare (units skipped when already done)
#   report(run)                                      <run>/report/
#   backup(run, s3_root, include_data=False)         <run> -> s3_root/<run_id>/ (not _local/)
#   run_all(run, gate=, run_s3=, include_data=)
#
# Run folder <out>/<run_id>/ (same layout as S3, so `rollback --run-s3 <out> --run-id` works)
#   row_check/  manifest/  _INDEX/  units/  results/  dead/  reports/  report/  audit.txt
#   audit/ immutable per-statement events; audit_errors/ best-effort write failures; _STATUS.json
#   _local/left/  _local/right/   pulled window CSVs;  _local/data.duckdb   every pulled window
#   _local/logs/errors.log         ERROR/CRITICAL records and unexpected tracebacks; not console
#   (tables left__<pair>, right__<pair>, column dtrack_bucket)
#
# Right side: WarehouseTarget reads through a SQL warehouse (Statement Execution API): counts are
#   one GROUP BY there, JSON_ARRAY windows come back as text, statistics are computed here
#   with the Left side's code. disposition external_links = presigned chunk downloads (large),
#   inline = rows through the workspace host (<= 25 MiB per window, when downloads are blocked).
#   push_sample defaults on; disable it only when the full Right window must be landed locally.

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
