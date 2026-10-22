import json
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from pair_recon_right.engine.common.model.protocol import (
    get_left_state,
    get_right_state,
    get_run_status,
    left_row_path,
    list_failures,
    list_results,
    list_units,
    plan_path,
    row_result_path,
)

log = logging.getLogger("pair_recon.progress")


def progress_snapshot(store, run_id: str, pair: str, timezone: str, state: str | None = None):
    status = get_run_status(store)
    plan_key = plan_path(pair)
    row_key = row_result_path(pair)
    left_key = left_row_path(pair)
    plan = store.get_json(plan_key) if store.exists(plan_key) else {}
    row = store.get_json(row_key) if store.exists(row_key) else {}
    left = store.get_json(left_key) if store.exists(left_key) else {}
    partitions = row.get("partitions") or {}
    left_counts = left.get("counts") or {}
    plan_ids = {pair: plan.get("plan_id")} if plan else {}
    left_state = get_left_state(store)
    right_state = get_right_state(store)
    units = (
        int(left_state.get("published_units") or 0)
        if left_state.get("plan_ids") == plan_ids
        else len(list_units(store))
    )
    if right_state.get("plan_ids") == plan_ids:
        results = int(right_state.get("matched_units") or 0) + int(
            right_state.get("different_units") or 0
        )
        failures = int(right_state.get("failed_units") or 0)
    else:
        results = len(list_results(store))
        failures = len(list_failures(store))
    threads = sorted(
        item.name for item in threading.enumerate() if item.name.startswith("pair_recon")
    )
    detail = {
        "row_matches": sum(value.get("result") == "match" for value in partitions.values()),
        "row_partitions": len(partitions),
        "left_partitions": len(left_counts),
        "left_rows": sum(int(value) for value in left_counts.values()),
        "threads": threads,
    }
    return {
        "snapshot_ts": datetime.now(ZoneInfo(timezone))
        .replace(tzinfo=None)
        .isoformat(timespec="microseconds"),
        "run_id": run_id,
        "pair_name": pair,
        "state": state or status.get("state") or "starting",
        "units_expected": int(plan.get("expected_units") or 0),
        "units_ready": units,
        "units_complete": results,
        "units_failed": failures,
        "worker_threads": len(threads),
        "detail": json.dumps(detail, ensure_ascii=False, separators=(",", ":")),
    }


class ProgressSampler:
    def __init__(self, store, run_id, pair, timezone, publish, *, interval=10):
        self.store = store
        self.run_id = run_id
        self.pair = pair
        self.timezone = timezone
        self.publish = publish
        self.interval = max(0.01, float(interval))
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, name="pair_recon-progress", daemon=True)

    def capture(self, state=None, *, required=False):
        row = progress_snapshot(self.store, self.run_id, self.pair, self.timezone, state=state)
        try:
            self.publish(row)
        except Exception:
            log.exception("progress snapshot failed")
            if required:
                raise
        return row

    def _run(self):
        while not self.stopped.wait(self.interval):
            self.capture()

    def start(self, *, required=False):
        self.capture(required=required)
        self.thread.start()
        return self

    def stop(self, state=None, *, required=False, capture=True):
        self.stopped.set()
        self.thread.join(timeout=max(1, self.interval * 2))
        return self.capture(state, required=required) if capture else None
