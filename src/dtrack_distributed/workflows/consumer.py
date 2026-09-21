from __future__ import annotations

import csv
import time
import traceback
from collections import defaultdict
from pathlib import Path

from ..domain.comparison import compare_statistics
from ..domain.manifest import unit_id
from ..engine.sampling import sample_csv
from ..engine.statistics import compute_csv_statistics
from .scheduler import DrainScheduler


class Consumer:
    def __init__(
        self,
        store,
        *,
        fetch_right,
        checkpoint=None,
        prefetch_depth: int = 3,
        max_attempts: int = 3,
        idle_timeout: float = 900,
        heartbeat_timeout: float = 180,
        poll_interval: float = 2,
    ):
        self.store = store
        self.fetch_right = fetch_right
        self.checkpoint = checkpoint
        self.scheduler = DrainScheduler(prefetch_depth=prefetch_depth)
        self.max_attempts = max_attempts
        self.idle_timeout = idle_timeout
        self.heartbeat_timeout = heartbeat_timeout
        self.poll_interval = poll_interval
        self.manifests = set()
        self.cursor = None
        self.seen = set(checkpoint.completed() if checkpoint else [])
        self.attempts = defaultdict(int)
        self.results = []

    def poll(self):
        changed = 0
        for key, manifest in self.store.list_json("manifest/"):
            if key == "manifest/_CLOSED" or key in self.manifests:
                continue
            self.scheduler.add_manifest(manifest)
            self.manifests.add(key)
            changed += 1
        for key, pointer in self.store.list_json("_INDEX/", start_after=self.cursor):
            self.cursor = key
            if pointer["unit_id"] in self.seen:
                continue
            self.seen.add(pointer["unit_id"])
            self.scheduler.add_unit(pointer)
            changed += 1
        return changed

    def _failure(self, task, exc):
        uid = task.payload.get("unit_id") or f"right--{task.pair}--{task.window}"
        self.attempts[uid] += 1
        if self.attempts[uid] >= self.max_attempts:
            units = [uid]
            if task.kind == "prefetch":
                units = [
                    unit_id(task.pair, task.window, col)
                    for col in self.scheduler.manifests[task.pair]["columns"]
                ]
            for failed_uid in units:
                self.store.put_dead(
                    failed_uid,
                    {
                        "unit_id": failed_uid,
                        "pair": task.pair,
                        "window": task.window,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                )
            return
        if task.kind == "prefetch":
            self.scheduler.mark_prefetch_failed(task.pair, task.window)
        else:
            self.scheduler.requeue_unit(task.payload)

    def run_task(self, task):
        try:
            if task.kind == "prefetch":
                manifest = self.scheduler.manifests[task.pair]
                self.scheduler.mark_right_ready(
                    task.pair, task.window, self.fetch_right(manifest, task.payload)
                )
                return
            pointer = task.payload
            left = self.store.get_json(pointer["payload_key"])
            right = self.scheduler.right_stats[(task.pair, task.window)][pointer["column"]]
            result = compare_statistics(
                pair=task.pair, window=task.window, column=pointer["column"], left=left, right=right
            )
            result["unit_id"] = pointer["unit_id"]
            self.store.put_result(pointer["unit_id"], result)
            if self.checkpoint:
                self.checkpoint.record(pointer["unit_id"], result)
            self.results.append(result)
        except Exception as exc:  # noqa: BLE001 - a Unit failure is protocol state
            self._failure(task, exc)

    def complete(self):
        if not self.store.exists("manifest/_CLOSED"):
            return False
        expected = int(self.store.get_json("manifest/_CLOSED")["expected_units"])
        return (
            len(self.store.list_keys("results/")) + len(self.store.list_keys("dead/")) >= expected
        )

    def run(self):
        last_progress = time.monotonic()
        while True:
            if self.poll():
                last_progress = time.monotonic()
            task = self.scheduler.next_task()
            if task:
                self.run_task(task)
                last_progress = time.monotonic()
                continue
            if self.complete():
                self.store.put_json("_STATUS.json", {"state": "complete"})
                return self.results
            age = self.store.heartbeat_age()
            if age is not None and age > self.heartbeat_timeout:
                raise RuntimeError(f"producer heartbeat is stale ({age:.1f}s)")
            if time.monotonic() - last_progress > self.idle_timeout:
                raise TimeoutError("consumer idle timeout expired")
            time.sleep(self.poll_interval)

    def write_csv(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fields = sorted({key for row in self.results for key in row})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted(self.results, key=lambda row: row["unit_id"]))


class ConsumerRuntime:
    def __init__(self, target_factory, work_dir: str | Path):
        self.target_factory = target_factory
        self.work_dir = Path(work_dir)

    def fetch_right(self, manifest, window):
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        output = self.work_dir / manifest["pair"] / window["bucket"] / "right.csv"
        path = self.target_factory(manifest["right"]).pull_window(
            window["partitions"], right_columns, output, key_columns=manifest["right_key_cols"]
        )
        plan = manifest["sample"]
        sampled = sample_csv(
            path,
            key_columns=manifest["right_key_cols"],
            mode=plan["mode"],
            rate=plan.get("rate"),
            count=plan.get("count"),
            families=manifest["key_families"],
            key_tuples=window.get("key_tuples"),
        )
        stats = compute_csv_statistics(sampled, right_columns, strict=manifest.get("strict", False))
        return {left: stats[right] for left, right in manifest["col_map"].items()}
