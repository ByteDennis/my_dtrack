from __future__ import annotations

import logging
import time
from pathlib import Path

from .backends.store import open_store, resolve_run
from .checks.col_check import ColCheck
from .checks.row_check import run_row_check
from .report.build import build_report, publish_report

log = logging.getLogger("dtrack.right")


# >>> the whole Right-side job: watch for left counts, reconcile, drain, report; started by hand, exits by itself <<< #
def run(
    work: str,
    target_factory,
    *,
    run_id: str | None = None,
    report: str | None = None,
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
        "right job start: work=%s run_id=%s report=%s gate=%s",
        work,
        run_id or "<auto>",
        report,
        gate,
    )
    started = time.monotonic()
    while True:
        store, resolved = resolve_run(work, run_id)
        if store is not None and store.list_keys("row_check/"):
            break
        if time.monotonic() - started > wait_timeout:
            raise TimeoutError("no run with row_check/*.left.json appeared before wait_timeout")
        time.sleep(poll_interval)
    log.info("run resolved: %s (%s)", resolved, store.root_uri)
    store.put_json("_STATUS.json", {"state": "right_waiting", "role": "right", "run_id": resolved})

    def reconcile_pending() -> bool:
        return run_row_check(store, target_factory, gate=gate) > 0

    reconcile_pending()
    store.put_json("_STATUS.json", {"state": "right_col_check", "role": "right"})
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
    if report:
        published = publish_report(open_store(report), summary["run_id"], files, summary)
    store.put_json(
        "_STATUS.json",
        {"state": "complete", "role": "right", "run_id": resolved, "results": len(results)},
    )
    log.info("right job done: %d result(s), report -> %s", len(results), report or out_dir)
    return {"results": len(results), "summary": summary, "published": published}
