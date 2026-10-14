from __future__ import annotations

import csv
import hashlib
import itertools
import json
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
from ..backends.store import replace_local_file
from ..common.audit import SqlAudit, attach_sql_audit
from ..common.duck import use_temp
from ..common.manifest import planned_unit_ids, unit_id_for, validate_manifest, window_id
from ..common.protocol import (
    fixed_keys_document,
    fixed_keys_path,
    get_run_status,
    left_row_check_close_document,
    list_failures,
    list_plans,
    list_results,
    list_units,
    refresh_units_status,
    row_check_is_closed,
    write_run_status,
)
from ..common.stats import compute_csv_statistics

log = logging.getLogger("dtrack.left.col_check")

ORACLE_MAX_PROJECTION_COLUMNS = 1000
DEFAULT_COLUMN_BATCH_SIZE = 500
BACKPRESSURE_LOG_INTERVAL = 30.0


# >>> block until the Right side has closed Stage 1; surface its status while waiting <<< #
def wait_for_manifests(store, *, timeout: float = 1800, poll_interval: float = 3, pair_names=None):
    started = time.monotonic()
    last_state = None
    while True:
        plans = [
            value
            for _key, value in list_plans(store)
            if not pair_names or value["pair"] in pair_names
        ]
        if row_check_is_closed(store):
            log.info("row check closed: %d plan(s) released", len(plans))
            return plans
        left_closed = left_row_check_close_document(store)
        if left_closed:
            expected = set(pair_names or left_closed.get("pairs") or [])
            planned = {plan["pair"] for plan in plans if int(plan.get("version") or 1) == 2}
            if expected and expected <= planned:
                log.info("version 2 Left plans ready: %d plan(s) released", len(plans))
                return plans
        state = get_run_status(store)
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


# >>> fixed-N sampling: the left side chooses the keys once and shares them through the Pair plan folder <<< #
def publish_fixed_keys(store, manifest, work_dir: str | Path) -> dict | None:
    key_path = fixed_keys_path(manifest["pair"])
    if manifest["sample"]["mode"] != "fixed":
        return None
    existing = fixed_keys_document(store, manifest["pair"])
    if existing is not None and int(manifest.get("version") or 1) == 1:
        for window in manifest["windows"]:
            window["key_tuples"] = existing["windows"].get(window["bucket"], [])
        return None
    source = attach_sql_audit(
        source_from_config(manifest["left"]),
        SqlAudit(store, "left", pair=manifest["pair"]),
    )
    windows = dict((existing or {}).get("windows") or {})
    window_ids = dict((existing or {}).get("window_ids") or {})
    changed = existing is None or existing.get("plan_id") != manifest.get("plan_id")
    for window in manifest["windows"]:
        current_window_id = window_id(manifest, window)
        if (
            int(manifest.get("version") or 1) == 2
            and window_ids.get(window["bucket"]) == current_window_id
        ):
            window["key_tuples"] = windows.get(window["bucket"], [])
            continue
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
        window_ids[window["bucket"]] = current_window_id
        window["key_tuples"] = windows[window["bucket"]]
        changed = True
    document = {
        "pair": manifest["pair"],
        "plan_id": manifest.get("plan_id"),
        "windows": {window["bucket"]: windows[window["bucket"]] for window in manifest["windows"]},
        "window_ids": {
            window["bucket"]: window_ids[window["bucket"]] for window in manifest["windows"]
        },
    }
    if not changed:
        return None
    store.put_json(key_path, document)
    log.info(
        "fixed sample keys %s: %s -> %s",
        manifest["pair"],
        {k: len(v) for k, v in windows.items()},
        key_path,
    )
    return document


def iter_windows_round_robin(manifests: Iterable[dict[str, Any]]):
    manifests = list(manifests)
    for windows in itertools.zip_longest(*(item.get("windows") or [] for item in manifests)):
        for manifest, window in zip(manifests, windows):
            if window is not None:
                yield manifest, window


def iter_column_batches(manifests: Iterable[dict[str, Any]]):
    for manifest, window in iter_windows_round_robin(manifests):
        left = manifest["left"]
        if not left.get("query_projection"):
            yield manifest, window
            continue
        prefix = list(dict.fromkeys([left["date_col"], *manifest["key_cols"]]))
        available = ORACLE_MAX_PROJECTION_COLUMNS - len(prefix)
        if available < 1:
            raise ValueError(
                f"{manifest['pair']}: date and key columns exhaust the Oracle projection limit"
            )
        configured = int(left.get("column_batch_size") or DEFAULT_COLUMN_BATCH_SIZE)
        batch_size = min(configured, available)
        columns = manifest["columns"]
        total = max(1, (len(columns) + batch_size - 1) // batch_size)
        for offset in range(0, len(columns), batch_size):
            index = offset // batch_size + 1
            yield (
                {
                    **manifest,
                    "columns": columns[offset : offset + batch_size],
                    "_column_batch": {"index": index, "total": total},
                },
                window,
            )


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
        version2 = any(int(item.get("version") or 1) == 2 for item in manifests)
        pair_names = [manifest["pair"] for manifest in manifests]
        active_ids = set().union(*(planned_unit_ids(manifest) for manifest in manifests))
        existing_rows = list_units(self.store, pair_names)
        existing = {row["unit_id"] for _key, row in existing_rows}
        ready_ids = existing & active_ids
        sequence = max((row.get("sequence", 0) for _key, row in existing_rows), default=0) + 1
        first_sequence = sequence
        ready = deque()
        plans = iter(iter_column_batches(manifests))
        exhausted = False
        write_run_status(self.store, {"state": "left_col_check", "role": "left"})
        self.store.heartbeat()
        log.info(
            "col_check start: %d plan(s), %d unit(s) already ready, next sequence=%d, wip=%d",
            len(manifests),
            len(ready_ids),
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
                    if all(
                        unit_id_for(manifest, window, column) in existing
                        for column in manifest["columns"]
                    ):
                        log.info(
                            "skip %s %s: every unit already indexed",
                            manifest["pair"],
                            window["bucket"],
                        )
                        continue
                    batch = manifest.get("_column_batch")
                    batch_text = f" batch {batch['index']}/{batch['total']}" if batch else ""
                    log.info(
                        "pull %s %s%s: %d partition(s), %d column(s)",
                        manifest["pair"],
                        window["bucket"],
                        batch_text,
                        len(window["partitions"]),
                        len(manifest["columns"]),
                    )
                    job = pulls.submit(self.pull_window, manifest, window)
                    pull_jobs[job] = (manifest, window)
                while ready and len(stat_jobs) < self.stat_workers:
                    manifest, window, value = ready.popleft()
                    batch = manifest.get("_column_batch")
                    batch_text = f" batch {batch['index']}/{batch['total']}" if batch else ""
                    log.info(
                        "statistics %s %s%s: calculating %d column(s)",
                        manifest["pair"],
                        window["bucket"],
                        batch_text,
                        len(manifest["columns"]),
                    )
                    job = stats.submit(self.compute_stats, manifest, window, value)
                    stat_jobs[job] = (manifest, window)
                active = set(pull_jobs) | set(stat_jobs)
                if not active:
                    continue
                complete, _pending = wait(active, return_when=FIRST_COMPLETED)
                for job in complete:
                    if job in pull_jobs:
                        manifest, window = pull_jobs.pop(job)
                        batch = manifest.get("_column_batch")
                        batch_text = f" batch {batch['index']}/{batch['total']}" if batch else ""
                        log.info(
                            "pulled %s %s%s -> stat queue",
                            manifest["pair"],
                            window["bucket"],
                            batch_text,
                        )
                        ready.append((manifest, window, job.result()))
                        continue
                    manifest, window = stat_jobs.pop(job)
                    rows = job.result()
                    first = sequence
                    for column in manifest["columns"]:
                        uid = unit_id_for(manifest, window, column)
                        if uid in existing:
                            continue
                        waiting_started = None
                        last_wait_log = None
                        while True:
                            ready_unclaimed = (
                                self._ready_unclaimed(active_ids)
                                if version2
                                else self.store.ready_unclaimed()
                            )
                            if ready_unclaimed <= self.wip:
                                if waiting_started is not None:
                                    log.info(
                                        "backpressure cleared: ready_unclaimed=%d, wip=%d, "
                                        "waited=%.1fs",
                                        ready_unclaimed,
                                        self.wip,
                                        time.monotonic() - waiting_started,
                                    )
                                break
                            now = time.monotonic()
                            if (
                                waiting_started is None
                                or now - last_wait_log >= BACKPRESSURE_LOG_INTERVAL
                            ):
                                if waiting_started is None:
                                    waiting_started = now
                                last_wait_log = now
                                log.info(
                                    "backpressure: ready_unclaimed=%d, wip=%d, over_limit=%d; "
                                    "waiting for Right (%.1fs)",
                                    ready_unclaimed,
                                    self.wip,
                                    ready_unclaimed - self.wip,
                                    now - waiting_started,
                                )
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
                                "plan_id": manifest.get("plan_id"),
                                "window_id": window_id(manifest, window),
                            },
                        )
                        ready_ids.add(uid)
                        sequence += 1
                    batch = manifest.get("_column_batch")
                    batch_text = f" batch {batch['index']}/{batch['total']}" if batch else ""
                    log.info(
                        "committed %s %s%s: %d unit(s), sequence %08d..%08d, ready %d/%d",
                        manifest["pair"],
                        window["bucket"],
                        batch_text,
                        sequence - first,
                        first,
                        max(first, sequence - 1),
                        len(ready_ids),
                        len(active_ids),
                    )
                    refresh_units_status(self.store)
                    self.store.heartbeat()
        published = {row["pair"]: row for _key, row in list_plans(self.store)}
        published_ids = set().union(*(planned_unit_ids(row) for row in published.values()))
        expected = sum(row["expected_units"] for row in published.values())
        indexed = len(
            {
                row["unit_id"]
                for _key, row in list_units(self.store, list(published))
                if row["unit_id"] in published_ids
            }
        )
        if indexed >= expected:
            self.store.close_manifests(
                list(published),
                expected,
                plan_ids={pair: row.get("plan_id") for pair, row in published.items()},
            )
            log.info("col check closed: pairs=%s expected_units=%d", sorted(published), expected)
        else:
            log.warning(
                "col_check partial: %d/%d units ready, closure not written", indexed, expected
            )
        write_run_status(
            self.store,
            {
                "state": "left_col_check_closed"
                if indexed >= expected
                else "left_col_check_partial",
                "role": "left",
                "indexed_units": indexed,
            },
        )
        refresh_units_status(self.store)
        SqlAudit(self.store, "left").refresh()
        return sequence - first_sequence

    def _ready_unclaimed(self, active_ids: set[str]) -> int:
        units = {
            value["unit_id"]
            for _key, value in list_units(self.store)
            if value["unit_id"] in active_ids
        }
        finished = {
            value["unit_id"]
            for rows in (list_results(self.store), list_failures(self.store))
            for _key, value in rows
            if value["unit_id"] in active_ids
        }
        return len(units - finished)


# >>> data rows in a CSV, header excluded <<< #
def _row_count(path: str | Path) -> int:
    with Path(path).open(encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


class LeftRuntime:
    def __init__(
        self,
        work_dir: str | Path,
        store=None,
        *,
        reuse_pulled: bool = True,
        force: bool = False,
    ):
        self.work_dir = Path(work_dir)
        use_temp(self.work_dir / "duckdb")
        self.store = store
        self.reuse_pulled = reuse_pulled
        self.force = force
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
            keys = fixed_keys_document(self.store, manifest["pair"])["windows"]
            window["key_tuples"] = keys.get(window["bucket"], [])
        batch = manifest.get("_column_batch")
        filename = f"left-{batch['index']:04d}.csv" if batch and batch["total"] > 1 else "left.csv"
        output = self.work_dir / manifest["pair"] / window["bucket"] / filename
        metadata = output.with_name(output.name + ".dtrack.json")
        selected = list(
            dict.fromkeys(
                [manifest["left"]["date_col"], *manifest["key_cols"], *manifest["columns"]]
            )
        )
        cache = {
            "format": "dtrack-left-pull-v1",
            "run_id": manifest["run_id"],
            "pair": manifest["pair"],
            "plan_id": manifest.get("plan_id"),
            "window_id": window_id(manifest, window),
            "columns": selected,
            "key_tuples_sha256": hashlib.sha256(
                json.dumps(
                    window.get("key_tuples"),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest(),
        }
        if self.reuse_pulled and not self.force and output.is_file() and metadata.is_file():
            try:
                saved = json.loads(metadata.read_text(encoding="utf-8"))
                with output.open(encoding="utf-8-sig", newline="") as handle:
                    header = next(csv.reader(handle), [])
                if saved == cache and header == selected:
                    log.info(
                        "reuse left %s %s%s: %s",
                        manifest["pair"],
                        window["bucket"],
                        f" batch {batch['index']}/{batch['total']}" if batch else "",
                        output,
                    )
                    return output
            except (OSError, ValueError, json.JSONDecodeError):
                log.warning("ignoring invalid Left pull cache metadata: %s", metadata)
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
        metadata.parent.mkdir(parents=True, exist_ok=True)
        temporary = metadata.with_name(metadata.name + ".part")
        try:
            temporary.write_text(
                json.dumps(cache, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            replace_local_file(temporary, metadata)
        finally:
            temporary.unlink(missing_ok=True)
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
            column_types=manifest.get("column_types"),
            side="left",
        )
