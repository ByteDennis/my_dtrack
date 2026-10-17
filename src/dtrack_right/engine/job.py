from __future__ import annotations

import logging
import time
from pathlib import Path

from .backends.store import resolve_run
from .checks.col_check import ColCheck
from .checks.row_check import run_row_check
from .common.model.audit import SqlAudit
from .common.model.protocol import (
    AUDIT_SUMMARY,
    active_unit_ids,
    list_left_documents,
    list_results,
    write_run_status,
)
from .report.build import CONTENT_TYPES, build_report

log = logging.getLogger("dtrack.right")


# >>> the whole Right-side job: watch for left counts, reconcile, drain, report; started by hand, exits by itself <<< #
def run(
    run_s3: str,
    target_factory,
    *,
    run_id: str | None = None,
    out_dir: str | Path = "/tmp/dtrack-report",
    gate: str = "auto",
    poll_interval: float = 2,
    idle_timeout: float = 900,
    heartbeat_timeout: float = 180,
    prefetch_depth: int = 3,
    max_attempts: int = 3,
    wait_timeout: float = 1800,
) -> dict:
    log.info(
        "right job start: run_s3=%s run_id=%s gate=%s",
        run_s3,
        run_id or "<auto>",
        gate,
    )
    started = time.monotonic()
    while True:
        store, resolved = resolve_run(run_s3, run_id)
        if store is not None and list_left_documents(store):
            break
        if time.monotonic() - started > wait_timeout:
            raise TimeoutError(
                "no run with pairs/<pair>/row_check/left.json appeared before wait_timeout"
            )
        time.sleep(poll_interval)
    log.info("run resolved: %s (%s)", resolved, store.root_uri)
    write_run_status(store, {"state": "right_waiting", "role": "right", "run_id": resolved})

    def reconcile_pending() -> bool:
        return run_row_check(store, target_factory, gate=gate) > 0

    reconcile_pending()
    write_run_status(store, {"state": "right_col_check", "role": "right"})
    col_check = ColCheck(
        store,
        target_factory,
        prefetch_depth=prefetch_depth,
        max_attempts=max_attempts,
        idle_timeout=idle_timeout,
        heartbeat_timeout=heartbeat_timeout,
        poll_interval=poll_interval,
        on_idle=reconcile_pending,
    )
    results = col_check.run()
    files = build_report(store, out_dir)
    summary = __import__("json").loads(files["summary.json"].read_text())
    published = []
    for name, path in sorted(files.items()):
        key = f"report/{name}"
        store.put_bytes(
            key,
            path.read_bytes(),
            content_type=CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
        )
        published.append(key)
        log.info("published %s", key)
    audit_written = SqlAudit(store, "right").refresh()
    active = active_unit_ids(store)
    total_results = sum(value["unit_id"] in active for _key, value in list_results(store))
    write_run_status(
        store,
        {
            "state": "complete",
            "role": "right",
            "run_id": resolved,
            "results": total_results,
            "new_results": len(results),
        },
    )
    log.info(
        "right job done: %d new result(s), %d total, report -> %s/report/",
        len(results),
        total_results,
        store.root_uri,
    )
    return {
        "results": len(results),
        "summary": summary,
        "published": published,
        "audit": AUDIT_SUMMARY if audit_written else None,
    }
