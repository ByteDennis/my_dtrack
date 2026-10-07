from __future__ import annotations

import itertools
import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import pandas as pd

from ..backends.sources import source_from_config
from ..common.audit import SqlAudit, attach_sql_audit
from ..common.manifest import unit_id, validate_manifest
from ..common.stats import compute_csv_statistics

log = logging.getLogger("dtrack.left.col_check")


# >>> block until the Right side has closed Stage 1; surface its status while waiting <<< #
def wait_for_manifests(store, *, timeout: float = 1800, poll_interval: float = 3, pair_names=None):
    started = time.monotonic()
    last_state = None
    while True:
        if store.exists("row_check/_CLOSED"):
            manifests = [
                value
                for key, value in store.list_json("manifest/")
                if key != "manifest/_CLOSED"
                and not key.endswith(".keys.json")
                and (not pair_names or value["pair"] in pair_names)
            ]
            log.info("row_check/_CLOSED seen: %d manifest(s) released", len(manifests))
            return manifests
        state = store.get_json("_STATUS.json") if store.exists("_STATUS.json") else {}
        if state != last_state:
            log.info("waiting for the Right side: status=%s", state)
            last_state = state
        if store.exists("_PREFLIGHT.json") and not store.get_json("_PREFLIGHT.json").get(
            "ok", True
        ):
            raise RuntimeError(f"Right side preflight failed: {store.get_json('_PREFLIGHT.json')}")
        if time.monotonic() - started > timeout:
            raise TimeoutError("Right side did not close the row check before timeout")
        time.sleep(poll_interval)


# >>> fixed-N sampling: the left side chooses the keys once and shares them through manifest/<pair>.keys.json <<< #
def publish_fixed_keys(store, manifest, work_dir: str | Path) -> dict | None:
    if manifest["sample"]["mode"] != "fixed" or store.exists(
        f"manifest/{manifest['pair']}.keys.json"
    ):
        return None
    source = attach_sql_audit(
        source_from_config(manifest["left"]),
        SqlAudit(store, "left", pair=manifest["pair"]),
    )
    windows = {}
    for window in manifest["windows"]:
        output = Path(work_dir) / manifest["pair"] / window["bucket"] / "keys.csv"
        sampled = source.sample_keys(
            window["partitions"],
            manifest["key_cols"],
            output,
            count=manifest["sample"]["count"],
            key_families=manifest["key_families"],
        )
        rows = pd.read_csv(sampled, dtype=object)[manifest["key_cols"]]
        windows[window["bucket"]] = rows.drop_duplicates().values.tolist()
    document = {"pair": manifest["pair"], "windows": windows}
    store.put_json(f"manifest/{manifest['pair']}.keys.json", document)
    log.info(
        "fixed sample keys %s: %s -> manifest/%s.keys.json",
        manifest["pair"],
        {k: len(v) for k, v in windows.items()},
        manifest["pair"],
    )
    for window in manifest["windows"]:
        window["key_tuples"] = windows[window["bucket"]]
    return document


def iter_windows_round_robin(manifests: Iterable[dict[str, Any]]):
    manifests = list(manifests)
    for windows in itertools.zip_longest(*(item.get("windows") or [] for item in manifests)):
        for manifest, window in zip(manifests, windows):
            if window is not None:
                yield manifest, window


class ColCheckPipeline:
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
        self.store.put_json("_STATUS.json", {"state": "left_col_check"})
        self.store.heartbeat()
        log.info(
            "col_check start: %d manifest(s), %d unit(s) already indexed, next sequence=%d, wip=%d",
            len(manifests),
            len(existing),
            sequence,
            self.wip,
        )
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
                    log.info(
                        "pull %s %s: %d partition(s)",
                        manifest["pair"],
                        window["bucket"],
                        len(window["partitions"]),
                    )
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
                        log.info("pulled %s %s -> stat queue", manifest["pair"], window["bucket"])
                        ready.append((manifest, window, job.result()))
                        continue
                    manifest, window = stat_jobs.pop(job)
                    rows = job.result()
                    first = sequence
                    for column in manifest["columns"]:
                        uid = unit_id(manifest["pair"], window["bucket"], column)
                        if uid in existing:
                            continue
                        while self.store.ready_unclaimed() > self.wip:
                            log.info("backpressure: ready_unclaimed > wip=%d, sleeping", self.wip)
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
                    log.info(
                        "committed %s %s: %d unit(s) _INDEX/%08d..%08d (data before pointer)",
                        manifest["pair"],
                        window["bucket"],
                        sequence - first,
                        first,
                        max(first, sequence - 1),
                    )
                    self.store.heartbeat()
        published = {
            row["pair"]: row
            for key, row in self.store.list_json("manifest/")
            if key != "manifest/_CLOSED" and not key.endswith(".keys.json")
        }
        expected = sum(row["expected_units"] for row in published.values())
        indexed = len(self.store.list_keys("_INDEX/"))
        if indexed >= expected:
            self.store.close_manifests(list(published), expected)
            log.info(
                "manifest/_CLOSED written: pairs=%s expected_units=%d", sorted(published), expected
            )
        else:
            log.warning(
                "col_check partial: %d/%d units indexed, _CLOSED not written", indexed, expected
            )
        self.store.put_json(
            "_STATUS.json",
            {
                "state": "left_col_check_closed"
                if indexed >= expected
                else "left_col_check_partial",
                "indexed_units": indexed,
            },
        )
        SqlAudit(self.store, "left").refresh()
        return sequence - first_sequence


# >>> data rows in a CSV, header excluded <<< #
def _row_count(path: str | Path) -> int:
    with Path(path).open(encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


class LeftRuntime:
    def __init__(self, work_dir: str | Path, store=None):
        self.work_dir = Path(work_dir)
        self.store = store
        self.sources = {}
        self.sources_lock = threading.Lock()

    def source(self, manifest):
        pair = manifest["pair"]
        with self.sources_lock:
            if pair not in self.sources:
                self.sources[pair] = attach_sql_audit(
                    source_from_config(manifest["left"]),
                    SqlAudit(self.store, "left", pair=pair),
                )
            return self.sources[pair]

    def pull_window(self, manifest, window):
        if manifest["sample"]["mode"] == "fixed" and "key_tuples" not in window:
            keys = self.store.get_json(f"manifest/{manifest['pair']}.keys.json")["windows"]
            window["key_tuples"] = keys.get(window["bucket"], [])
        output = self.work_dir / manifest["pair"] / window["bucket"] / "left.csv"
        path = self.source(manifest).pull_window(
            window["partitions"],
            manifest["columns"],
            output,
            key_columns=manifest["key_cols"],
            sample_plan=manifest["sample"],
            key_families=manifest["key_families"],
            side="left",
            key_tuples=window.get("key_tuples"),
        )
        log.info(
            "left %s %s: %d sampled row(s) pulled (%s)",
            manifest["pair"],
            window["bucket"],
            _row_count(path),
            path,
        )
        return path

    @staticmethod
    def compute_stats(manifest, _window, path):
        return compute_csv_statistics(
            path,
            manifest["columns"],
            strict=manifest.get("strict", False),
            col_types=manifest.get("col_type_overrides"),
        )
