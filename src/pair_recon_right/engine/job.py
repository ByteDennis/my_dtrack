import logging
import tempfile
import time
from pathlib import Path

from .backends.store import resolve_run
from .checks.col_check import DEFAULT_HEARTBEAT_TIMEOUT, ColCheck, require_left_heartbeat
from .checks.row_check import run_row_check
from .common.model.protocol import (
    active_unit_ids,
    list_failures,
    list_left_documents,
    list_plans,
    list_results,
    list_row_results,
    write_run_status,
)
from .report import CONTENT_TYPES, build_report

log = logging.getLogger("pair_recon.right.job")
WAIT_LOG_INTERVAL = 300.0


# >>> blocks until the Left published its row data; only a stale Left heartbeat ends the wait <<< #
def wait_for_left_row_data(run_root, run_id, *, poll_interval, heartbeat_timeout):
    started = time.monotonic()
    last_log = started
    while True:
        store, resolved = resolve_run(run_root, run_id)
        if store is not None and list_left_documents(store):
            return store, resolved
        if store is not None:
            require_left_heartbeat(store, heartbeat_timeout)
        now = time.monotonic()
        if now - last_log >= WAIT_LOG_INTERVAL:
            log.info("waiting for Left row data: run_id=%s elapsed=%.0fs", run_id, now - started)
            last_log = now
        time.sleep(poll_interval)


# >>> a released plan without any unit means nothing was compared; never report that as passed <<< #
def require_plan_units(store) -> None:
    plans = [plan for _key, plan in list_plans(store)]
    if plans and not any(int(plan.get("expected_units") or 0) for plan in plans):
        raise RuntimeError(
            "plan has no units to compare: every window or column was excluded; "
            "fix the date mapping or sampling before rerunning"
        )


def publish_report(store) -> None:
    with tempfile.TemporaryDirectory(prefix="pair-recon-report-") as folder:
        files = build_report(store, folder)
        store.delete_keys(store.list_keys("report/"))
        for name, path in files.items():
            path = Path(path)
            store.put_bytes(
                f"report/{name}",
                path.read_bytes(),
                content_type=CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
            )


def summary(store):
    active = active_unit_ids(store)
    scoped = bool(list_plans(store))
    rows = [value for _, value in list_row_results(store)]
    columns = [
        value for _, value in list_results(store) if not scoped or value.get("unit_id") in active
    ]
    failures = [
        value
        for _, value in list_failures(store)
        if not scoped or value.get("unit_id") in active or value.get("source")
    ]
    return {
        "row_errors": sum(
            partition.get("result") != "match"
            for row in rows
            for partition in (row.get("partitions") or {}).values()
        ),
        "column_errors": sum(row.get("verdict") != "match" for row in columns),
        "failures": len(failures),
    }


def run(
    run_root,
    target_factory,
    *,
    run_id=None,
    gate="auto",
    poll_interval=2,
    heartbeat_timeout=DEFAULT_HEARTBEAT_TIMEOUT,
    prefetch_depth=3,
    right_workers=8,
    max_attempts=3,
    store=None,
):
    resolved = run_id
    if store is None:
        store, resolved = wait_for_left_row_data(
            run_root,
            run_id,
            poll_interval=poll_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
    elif not resolved:
        raise ValueError("run_id is required with an existing store")
    write_run_status(store, {"state": "right_row_check", "role": "right", "run_id": resolved})

    def reconcile():
        return run_row_check(store, target_factory, gate=gate) > 0

    try:
        reconcile()
    except Exception:
        if list_row_results(store):
            try:
                publish_report(store)
            except Exception:
                log.exception("could not publish partial row-check report")
        raise
    if not list_plans(store):
        result = summary(store)
        publish_report(store)
        write_run_status(store, {"state": "complete", "role": "right", **result})
        return result
    require_plan_units(store)
    write_run_status(store, {"state": "right_col_check", "role": "right"})
    compared = ColCheck(
        store,
        target_factory,
        prefetch_depth=prefetch_depth,
        right_workers=right_workers,
        max_attempts=max_attempts,
        heartbeat_timeout=heartbeat_timeout,
        poll_interval=poll_interval,
        on_idle=reconcile,
    ).run()
    result = summary(store)
    active = active_unit_ids(store)
    result["results"] = sum(value["unit_id"] in active for _, value in list_results(store))
    result["new_results"] = len(compared)
    publish_report(store)
    write_run_status(store, {"state": "complete", "role": "right", **result})
    return result
