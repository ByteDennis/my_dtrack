# dtrack_local: the backup plan; both sides pulled to this machine and compared here
#
# API (what the notebooks and `python -m dtrack_local` call)
#   open_run(config, run_id=None, out=".dtrack-local/runs", profile=None, warehouse_id=None,
#            disposition="external_links"|"inline", push_sample=False, pairs=None) -> LocalRun
#   preflight(run)            Left SELECT ... WHERE 1=0, Right DESCRIBE QUERY
#   row_check(run, gate="auto"|"manual"|"ratio:x")   Stage 1: counts, reconcile, gate, manifests
#   col_check(run)            Stage 2: pull, statistics, compare (units skipped when already done)
#   report(run, report_s3=None)                      <run>/report/, optional report store publish
#   backup(run, s3_root, include_data=False)         <run> -> s3_root/<run_id>/ (not _local/)
#   run_all(run, gate=, report_s3=, backup_s3=, include_data=)
#
# Run folder <out>/<run_id>/ (same layout as an S3 run, so `dtrack rollback --run` works on it)
#   row_check/  manifest/  _INDEX/  units/  results/  dead/  reports/  report/  _STATUS.json
#   _local/left/  _local/right/   pulled window CSVs;  _local/data.duckdb   every pulled window
#   (tables left__<pair>, right__<pair>, column dtrack_bucket)
#
# Right side: WarehouseTarget reads through a SQL warehouse (Statement Execution API): counts are
#   one GROUP BY there, windows come back as text (CAST AS STRING), statistics are computed here
#   with the Left side's code. disposition external_links = presigned chunk downloads (large),
#   inline = rows through the workspace host (<= 25 MiB per window, when downloads are blocked).
#   push_sample filters the key-hash sample in the warehouse as well (fewer rows pulled).

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
