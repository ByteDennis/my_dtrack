import json
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from pair_recon_right.engine.common.model.protocol import (
    get_run_status,
    pair_root,
    plan_path,
    row_result_path,
)

log = logging.getLogger("pair_recon.progress")


def progress_snapshot(store, run_id: str, pair: str, timezone: str, state: str | None = None):
    status = get_run_status(store)
    prefix = pair_root(pair) + "/"
    keys = store.list_keys(prefix)
    plan_key = plan_path(pair)
    row_key = row_result_path(pair)
    plan = store.get_json(plan_key) if plan_key in keys else {}
    row = store.get_json(row_key) if row_key in keys else {}
    partitions = row.get("partitions") or {}
    units = sum("/units/" in key and key.endswith(".json") for key in keys)
    results = sum("/col_check/results/" in key and key.endswith(".json") for key in keys)
    failures = sum("/col_check/failures/" in key and key.endswith(".json") for key in keys)
    threads = sorted(
        item.name for item in threading.enumerate() if item.name.startswith("pair_recon")
    )
    detail = {
        "row_matches": sum(value.get("verdict") == "match" for value in partitions.values()),
        "row_partitions": len(partitions),
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

    def stop(self, state=None, *, required=False):
        self.stopped.set()
        self.thread.join(timeout=max(1, self.interval * 2))
        return self.capture(state, required=required)
