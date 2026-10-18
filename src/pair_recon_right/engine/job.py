import time

from .backends.store import resolve_run
from .checks.col_check import ColCheck
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


def summary(store):
    rows = [value for _, value in list_row_results(store)]
    columns = [value for _, value in list_results(store)]
    failures = [value for _, value in list_failures(store)]
    return {
        "row_errors": sum(
            partition.get("verdict") != "match"
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
    idle_timeout=900,
    heartbeat_timeout=180,
    prefetch_depth=3,
    max_attempts=3,
    wait_timeout=1800,
):
    started = time.monotonic()
    while True:
        store, resolved = resolve_run(run_root, run_id)
        if store is not None and list_left_documents(store):
            break
        if time.monotonic() - started > wait_timeout:
            raise TimeoutError("Left row data did not arrive before wait_timeout")
        time.sleep(poll_interval)
    write_run_status(store, {"state": "right_row_check", "role": "right", "run_id": resolved})

    def reconcile():
        return run_row_check(store, target_factory, gate=gate) > 0

    reconcile()
    if not list_plans(store):
        result = summary(store)
        write_run_status(store, {"state": "complete", "role": "right", **result})
        return result
    write_run_status(store, {"state": "right_col_check", "role": "right"})
    compared = ColCheck(
        store,
        target_factory,
        prefetch_depth=prefetch_depth,
        max_attempts=max_attempts,
        idle_timeout=idle_timeout,
        heartbeat_timeout=heartbeat_timeout,
        poll_interval=poll_interval,
        on_idle=reconcile,
    ).run()
    result = summary(store)
    active = active_unit_ids(store)
    result["results"] = sum(value["unit_id"] in active for _, value in list_results(store))
    result["new_results"] = len(compared)
    write_run_status(store, {"state": "complete", "role": "right", **result})
    return result
