from __future__ import annotations

import logging
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

from ..common.model.attribute_checks import get_attribute_check
from ..common.model.audit import SqlAudit, attach_sql_audit
from ..common.model.manifest import DEFAULT_STATISTICS, planned_unit_ids, unit_id_for, window_id
from ..common.model.protocol import (
    active_unit_ids,
    col_check_close_document,
    current_plan_ids,
    fixed_keys_document,
    list_failures,
    list_plans,
    list_results,
    refresh_units_status,
    right_ready_path,
    timestamp,
    unit_prefix,
    unit_statistics,
)


@dataclass(frozen=True)
class DrainTask:
    kind: str
    pair: str
    window: str
    payload: dict[str, Any]


def compare_statistics(
    *,
    pair: str,
    window: str,
    column: str,
    left: dict[str, Any],
    right: dict[str, Any],
    statistics: list[str] | None = None,
) -> dict[str, Any]:
    selected = statistics or list(DEFAULT_STATISTICS)
    result: dict[str, Any] = {"pair": pair, "vintage": window, "column": column}
    checks = []
    for name in selected:
        values, match = get_attribute_check(name).compare(left, right)
        result.update(values)
        if match is not None:
            checks.append(match)
    if statistics is None:
        result["top_10_left"] = left.get("top_10") or ""
        result["top_10_right"] = right.get("top_10") or ""
    result["col_type"] = left.get("col_type") or right.get("col_type") or ""
    result["verdict"] = "match" if all(checks) else "diff"
    return result


class DrainScheduler:
    def __init__(self, *, prefetch_depth: int = 3):
        self.prefetch_depth = max(1, prefetch_depth)
        self.manifests = {}
        self.windows = {}
        self.prefetch = deque()
        self.prefetching = set()
        self.right_stats = {}
        self.pending = defaultdict(deque)
        self.ready = deque()
        self.unit_windows = set()

    def add_manifest(self, manifest, completed_ids=()):
        if manifest["pair"] in self.manifests:
            return
        self.manifests[manifest["pair"]] = manifest
        completed_ids = set(completed_ids)
        for window in manifest["windows"]:
            key = (manifest["pair"], window["bucket"])
            self.windows[key] = window
            if not manifest.get("columns") or any(
                unit_id_for(manifest, window, column) not in completed_ids
                for column in manifest["columns"]
            ):
                self.prefetch.append(key)

    def add_unit(self, pointer):
        key = (pointer["pair"], pointer["window"])
        self.unit_windows.add(key)
        (self.ready if key in self.right_stats else self.pending[key]).append(pointer)

    def mark_right_ready(self, pair, window, stats):
        key = (pair, window)
        self.right_stats[key] = stats
        self.prefetching.discard(key)
        while self.pending[key]:
            self.ready.append(self.pending[key].popleft())

    def mark_prefetch_failed(self, pair, window):
        key = (pair, window)
        self.prefetching.discard(key)
        self.prefetch.appendleft(key)

    def next_task(self):
        if self.ready:
            row = self.ready.popleft()
            return DrainTask("compare", row["pair"], row["window"], row)
        while self.prefetch:
            key = next((item for item in self.prefetch if item in self.unit_windows), None)
            speculative = len(self.right_stats) + len(self.prefetching)
            if key is None and speculative >= self.prefetch_depth:
                return None
            key = key or self.prefetch[0]
            self.prefetch.remove(key)
            if key in self.right_stats or key in self.prefetching:
                continue
            self.prefetching.add(key)
            return DrainTask("prefetch", key[0], key[1], self.windows[key])
        return None

    def requeue_unit(self, pointer):
        self.ready.append(pointer)


log = logging.getLogger("pair_recon.right.col_check")


class ColCheck:
    def __init__(
        self,
        store,
        target_factory,
        *,
        prefetch_depth=3,
        max_attempts=3,
        idle_timeout=900,
        heartbeat_timeout=180,
        poll_interval=2,
        on_idle=None,
    ):
        self.store = store
        self.target_factory = target_factory
        self.scheduler = DrainScheduler(prefetch_depth=prefetch_depth)
        self.max_attempts = max_attempts
        self.idle_timeout = idle_timeout
        self.heartbeat_timeout = heartbeat_timeout
        self.poll_interval = poll_interval
        self.on_idle = on_idle
        self.manifests = set()
        self.completed_ids = {value["unit_id"] for _key, value in list_results(store)}
        self.completed_ids |= {value["unit_id"] for _key, value in list_failures(store)}
        self.seen = set(self.completed_ids)
        self.active_ids = set()
        self.attempts = defaultdict(int)
        self.results = []
        self.targets = {}
        self.unit_cursors = {}

    def _target(self, manifest):
        pair = manifest["pair"]
        if pair not in self.targets:
            self.targets[pair] = attach_sql_audit(
                self.target_factory(
                    {
                        **manifest["right"],
                        "partition_mode": manifest.get("partition_mode", "date"),
                    }
                ),
                SqlAudit(self.store, "right", pair=pair),
            )
        return self.targets[pair]

    def poll(self):
        changed = 0
        for key, manifest in list_plans(self.store):
            identity = (key, manifest.get("plan_id"))
            if identity in self.manifests:
                continue
            if manifest["sample"]["mode"] == "fixed":
                keys_document = fixed_keys_document(self.store, manifest["pair"])
                if keys_document is None:
                    continue
                if int(manifest.get("version") or 1) == 2 and (
                    keys_document.get("plan_id") != manifest.get("plan_id")
                    or any(
                        (keys_document.get("window_ids") or {}).get(window["bucket"])
                        != window_id(manifest, window)
                        for window in manifest["windows"]
                    )
                ):
                    continue
                key_tuples = keys_document["windows"]
                for window in manifest["windows"]:
                    window["key_tuples"] = key_tuples.get(window["bucket"], [])
            self.scheduler.add_manifest(manifest, self.completed_ids)
            self.active_ids.update(planned_unit_ids(manifest))
            self.manifests.add(identity)
            log.info(
                "manifest %s loaded: %d windows x %d columns = %d units -> B-queue seeded",
                manifest["pair"],
                len(manifest["windows"]),
                len(manifest["columns"]),
                manifest["expected_units"],
            )
            changed += 1
        batches = [
            (unit_prefix(pair), self.unit_cursors.get(pair))
            for pair in sorted(self.scheduler.manifests)
        ]
        for prefix, cursor in batches:
            for key, pointer in self.store.list_json(prefix, start_after=cursor):
                self.unit_cursors[pointer["pair"]] = key
                if pointer["unit_id"] not in self.active_ids or pointer["unit_id"] in self.seen:
                    continue
                self.seen.add(pointer["unit_id"])
                self.scheduler.add_unit(pointer)
                changed += 1
        if changed:
            log.info("poll: %d new protocol object(s)", changed)
        return changed

    def _failure(self, task, exc):
        uid = task.payload.get("unit_id") or f"right--{task.pair}--{task.window}"
        details = "".join(traceback.format_exception(exc))
        self.attempts[uid] += 1
        if self.attempts[uid] >= self.max_attempts:
            units = [uid]
            if task.kind == "prefetch":
                manifest = self.scheduler.manifests[task.pair]
                window = self.scheduler.windows[(task.pair, task.window)]
                units = [unit_id_for(manifest, window, col) for col in manifest["columns"]]
            for failed_uid in units:
                log.error(
                    "dead %s after %d attempt(s): %s",
                    failed_uid,
                    self.attempts[uid],
                    exc,
                    extra={"pair_recon_traceback": details},
                )
                self.store.put_dead(
                    failed_uid,
                    {
                        "unit_id": failed_uid,
                        "pair": task.pair,
                        "window": task.window,
                        "error": str(exc),
                        "traceback": details,
                    },
                )
            refresh_units_status(self.store)
            return
        log.warning("retry %s attempt %d: %s", uid, self.attempts[uid], exc)
        if task.kind == "prefetch":
            self.scheduler.mark_prefetch_failed(task.pair, task.window)
        else:
            self.scheduler.requeue_unit(task.payload)

    def run_task(self, task):
        try:
            manifest = self.scheduler.manifests[task.pair]
            if task.kind == "prefetch":
                started = time.monotonic()
                log.info(
                    "B fetch_right %s %s: %d partition(s)",
                    task.pair,
                    task.window,
                    len(task.payload["partitions"]),
                )
                stats = self._target(manifest).window_statistics(manifest, task.payload)
                mapped = {left: stats[right] for left, right in manifest["col_map"].items()}
                self.scheduler.mark_right_ready(task.pair, task.window, mapped)
                self.store.put_json(
                    right_ready_path(task.pair, task.window),
                    {
                        "pair": task.pair,
                        "window": task.window,
                        "plan_id": manifest.get("plan_id"),
                        "window_id": window_id(manifest, task.payload),
                        "right_ready_at": timestamp(self.store),
                    },
                )
                refresh_units_status(self.store)
                log.info(
                    "B right stats ready %s %s (%.3fs) -> %d unit(s) promoted to A-queue",
                    task.pair,
                    task.window,
                    time.monotonic() - started,
                    len(self.scheduler.ready),
                )
                return
            pointer = task.payload
            left = unit_statistics(self.store, pointer)
            right = self.scheduler.right_stats[(task.pair, task.window)][pointer["column"]]
            result = compare_statistics(
                pair=task.pair,
                window=task.window,
                column=pointer["column"],
                left=left,
                right=right,
                statistics=manifest.get("statistics"),
            )
            result["unit_id"] = pointer["unit_id"]
            result["right_col"] = pointer.get("right_column") or pointer["column"]
            self.store.put_result(pointer["unit_id"], result)
            refresh_units_status(self.store)
            self.results.append(result)
            log.info(
                "A compare %s %s %s -> %s",
                task.pair,
                task.window,
                pointer["column"],
                result["verdict"].upper(),
            )
        except Exception as exc:
            self._failure(task, exc)

    def complete(self):
        closed = col_check_close_document(self.store)
        if closed is None:
            return False
        plans = [plan for _key, plan in list_plans(self.store)]
        if any(int(plan.get("version") or 1) == 2 for plan in plans) and closed.get(
            "plan_ids"
        ) != current_plan_ids(self.store):
            return False
        active = active_unit_ids(self.store)
        finished = {value["unit_id"] for _key, value in list_results(self.store)}
        finished |= {value["unit_id"] for _key, value in list_failures(self.store)}
        return active <= finished

    def run(self):
        last_progress = time.monotonic()
        while True:
            if self.poll():
                last_progress = time.monotonic()
            task = (
                None if not self.scheduler.ready and self.complete() else self.scheduler.next_task()
            )
            if task:
                self.run_task(task)
                last_progress = time.monotonic()
                continue
            if self.complete():
                log.info(
                    "col_check complete: %d result(s), %d dead",
                    len(self.results),
                    sum(
                        value["unit_id"] in active_unit_ids(self.store)
                        for _key, value in list_failures(self.store)
                    ),
                )
                return self.results
            if self.on_idle and self.on_idle():
                last_progress = time.monotonic()
                continue
            age = self.store.heartbeat_age()
            if age is not None and age > self.heartbeat_timeout:
                log.error("left heartbeat stale: %.1fs > %.1fs", age, self.heartbeat_timeout)
                raise RuntimeError(f"left heartbeat is stale ({age:.1f}s)")
            if time.monotonic() - last_progress > self.idle_timeout:
                raise TimeoutError("right idle timeout expired")
            time.sleep(self.poll_interval)
