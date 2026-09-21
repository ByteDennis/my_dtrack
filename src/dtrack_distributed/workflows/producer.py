from __future__ import annotations

import itertools
import time
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from ..adapters.sources.factory import source_from_config
from ..domain.manifest import unit_id, validate_manifest
from ..engine.sampling import sample_csv
from ..engine.statistics import compute_csv_statistics


def iter_windows_round_robin(manifests: Iterable[dict[str, Any]]):
    manifests = list(manifests)
    for windows in itertools.zip_longest(*(item.get("windows") or [] for item in manifests)):
        for manifest, window in zip(manifests, windows):
            if window is not None:
                yield manifest, window


class ProducerPipeline:
    def __init__(
        self,
        store,
        *,
        pull_window: Callable,
        compute_stats: Callable,
        pull_workers: int = 2,
        stat_workers: int = 2,
        wip: int = 200,
        backpressure_sleep: float = 1.0,
    ):
        self.store = store
        self.pull_window = pull_window
        self.compute_stats = compute_stats
        self.pull_workers = max(1, pull_workers)
        self.stat_workers = max(1, stat_workers)
        self.wip = max(1, wip)
        self.backpressure_sleep = max(0, backpressure_sleep)

    def run(self, manifests: Iterable[dict[str, Any]]) -> int:
        manifests = list(manifests)
        for manifest in manifests:
            validate_manifest(manifest)
        existing_rows = self.store.list_json("_INDEX/")
        existing = {row["unit_id"] for _key, row in existing_rows}
        sequence = max((row.get("sequence", 0) for _key, row in existing_rows), default=0) + 1
        first_sequence = sequence
        ready = deque()
        plans = iter(iter_windows_round_robin(manifests))
        exhausted = False
        self.store.put_json("_STATUS.json", {"state": "producer_running"})
        self.store.heartbeat()
        with (
            ThreadPoolExecutor(self.pull_workers) as pulls,
            ThreadPoolExecutor(self.stat_workers) as stats,
        ):
            pull_jobs, stat_jobs = {}, {}
            while pull_jobs or stat_jobs or ready or not exhausted:
                while not exhausted and len(pull_jobs) < self.pull_workers:
                    try:
                        manifest, window = next(plans)
                    except StopIteration:
                        exhausted = True
                        break
                    job = pulls.submit(self.pull_window, manifest, window)
                    pull_jobs[job] = (manifest, window)
                while ready and len(stat_jobs) < self.stat_workers:
                    manifest, window, value = ready.popleft()
                    job = stats.submit(self.compute_stats, manifest, window, value)
                    stat_jobs[job] = (manifest, window)
                active = set(pull_jobs) | set(stat_jobs)
                if not active:
                    continue
                complete, _pending = wait(active, return_when=FIRST_COMPLETED)
                for job in complete:
                    if job in pull_jobs:
                        manifest, window = pull_jobs.pop(job)
                        ready.append((manifest, window, job.result()))
                        continue
                    manifest, window = stat_jobs.pop(job)
                    rows = job.result()
                    for column in manifest["columns"]:
                        uid = unit_id(manifest["pair"], window["bucket"], column)
                        if uid in existing:
                            continue
                        while self.store.ready_unclaimed() > self.wip:
                            time.sleep(self.backpressure_sleep)
                        self.store.commit_unit(
                            sequence,
                            uid,
                            rows[column],
                            {
                                "pair": manifest["pair"],
                                "window": window["bucket"],
                                "column": column,
                                "right_column": manifest["col_map"][column],
                            },
                        )
                        sequence += 1
                    self.store.heartbeat()
        published = {
            row["pair"]: row
            for key, row in self.store.list_json("manifest/")
            if key != "manifest/_CLOSED"
        }
        expected = sum(row["expected_units"] for row in published.values())
        indexed = len(self.store.list_keys("_INDEX/"))
        if indexed >= expected:
            self.store.close_manifests(list(published), expected)
        self.store.put_json(
            "_STATUS.json",
            {
                "state": "producer_closed" if indexed >= expected else "producer_partial",
                "indexed_units": indexed,
            },
        )
        return sequence - first_sequence


class ProducerRuntime:
    def __init__(self, work_dir: str | Path):
        self.work_dir = Path(work_dir)

    def pull_window(self, manifest, window):
        output = self.work_dir / manifest["pair"] / window["bucket"] / "left.csv"
        path = source_from_config(manifest["left"]).pull_window(
            window["partitions"], manifest["columns"], output, key_columns=manifest["key_cols"]
        )
        plan = manifest["sample"]
        return sample_csv(
            path,
            key_columns=manifest["key_cols"],
            mode=plan["mode"],
            rate=plan.get("rate"),
            count=plan.get("count"),
            families=manifest["key_families"],
            key_tuples=window.get("key_tuples"),
        )

    @staticmethod
    def compute_stats(manifest, _window, path):
        return compute_csv_statistics(
            path, manifest["columns"], strict=manifest.get("strict", False)
        )
