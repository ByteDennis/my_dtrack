from __future__ import annotations

import logging
import time
from pathlib import Path

from .consumer import Consumer
from .reporting.build import build_report, publish_report
from .stage1_right import run_stage1_right
from .store import open_store, resolve_run

log = logging.getLogger("dtrack.cloud")


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
    kms_key_id: str | None = None,
) -> dict:
    log.info(
        "cloud job start: work=%s run_id=%s report=%s gate=%s",
        work,
        run_id or "<auto>",
        report,
        gate,
    )
    started = time.monotonic()
    while True:
        store, resolved = resolve_run(work, run_id, kms_key_id=kms_key_id)
        if store is not None and store.list_keys("stage1/"):
            break
        if time.monotonic() - started > wait_timeout:
            raise TimeoutError("no run with stage1/*.left.json appeared before wait_timeout")
        time.sleep(poll_interval)
    log.info("run resolved: %s (%s)", resolved, store.root_uri)
    store.put_json("_STATUS.json", {"state": "cloud_waiting", "role": "right", "run_id": resolved})

    def reconcile_pending() -> bool:
        return run_stage1_right(store, target_factory, gate=gate) > 0

    reconcile_pending()
    store.put_json("_STATUS.json", {"state": "cloud_draining", "role": "right"})
    consumer = Consumer(
        store,
        target_factory,
        prefetch_depth=prefetch_depth,
        max_attempts=max_attempts,
        idle_timeout=idle_timeout,
        heartbeat_timeout=heartbeat_timeout,
        poll_interval=poll_interval,
        on_idle=reconcile_pending,
    )
    results = consumer.run()
    files = build_report(store, out_dir)
    summary = __import__("json").loads(files["summary.json"].read_text())
    published = []
    if report:
        published = publish_report(
            open_store(report, kms_key_id=kms_key_id), summary["run_id"], files, summary
        )
    store.put_json(
        "_STATUS.json",
        {"state": "complete", "role": "right", "run_id": resolved, "results": len(results)},
    )
    log.info("cloud job done: %d result(s), report -> %s", len(results), report or out_dir)
    return {"results": len(results), "summary": summary, "published": published}
