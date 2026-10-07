from __future__ import annotations

import logging
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

from ..common.audit import SqlAudit, attach_sql_audit
from ..common.manifest import unit_id


@dataclass(frozen=True)
class DrainTask:
    kind: str
    pair: str
    window: str
    payload: dict[str, Any]


def _number(row: dict[str, Any], name: str) -> int:
    value = row.get(name)
    return 0 if value in (None, "") else int(value)


def _float_match(left: Any, right: Any, tolerance: float = 1e-6) -> bool | None:
    if left in (None, "") or right in (None, ""):
        return None
    try:
        left_number, right_number = float(left), float(right)
    except (TypeError, ValueError):
        return str(left).strip() == str(right).strip()
    return abs(left_number - right_number) <= max(
        1e-9, tolerance * max(abs(left_number), abs(right_number), 1.0)
    )


def compare_statistics(
    *,
    pair: str,
    window: str,
    column: str,
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {"pair": pair, "vintage": window, "column": column}
    for name in ("n_total", "n_missing", "n_unique"):
        left_value, right_value = _number(left, name), _number(right, name)
        result[f"{name}_left"] = left_value
        result[f"{name}_right"] = right_value
        result[f"{name}_diff"] = right_value - left_value
    checks = []
    for name in ("mean", "std", "length_avg", "length_max", "hash_sum"):
        match = _float_match(left.get(name), right.get(name))
        result[f"{name}_left"] = left.get(name)
        result[f"{name}_right"] = right.get(name)
        result[f"{name}_match"] = match
        if match is not None:
            checks.append(match)
    for name in ("mean", "std"):
        try:
            result[f"{name}_diff"] = float(right[name]) - float(left[name])
        except (KeyError, TypeError, ValueError):
            result[f"{name}_diff"] = None
    for name in ("min_val", "max_val"):
        left_value = "" if left.get(name) is None else str(left.get(name)).strip()
        right_value = "" if right.get(name) is None else str(right.get(name)).strip()
        label = "min" if name == "min_val" else "max"
        result[f"{label}_left"] = left_value
        result[f"{label}_right"] = right_value
        same = left_value == right_value
        if not same and "numeric" in (left.get("col_type"), right.get("col_type")):
            same = bool(left_value and right_value and _float_match(left_value, right_value, 0))
        result[f"{label}_match"] = same
        checks.append(same)
    result["top_10_left"] = left.get("top_10") or ""
    result["top_10_right"] = right.get("top_10") or ""
    count_match = all(result[f"{name}_diff"] == 0 for name in ("n_total", "n_missing", "n_unique"))
    result["col_type"] = left.get("col_type") or right.get("col_type") or ""
    result["verdict"] = "match" if count_match and all(checks) else "diff"
    return result


# >>> ready comparisons always outrank speculative Right-side prefetch <<< #
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

    def add_manifest(self, manifest):
        if manifest["pair"] in self.manifests:
            return
        self.manifests[manifest["pair"]] = manifest
        for window in manifest["windows"]:
            key = (manifest["pair"], window["bucket"])
            self.windows[key] = window
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


log = logging.getLogger("dtrack.right.col_check")


# >>> drain loop: A (compare) first, B (fetch own side) otherwise; exits on _CLOSED + all units accounted <<< #
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
        self.cursor = None
        self.seen = {
            key.removeprefix("results/").removesuffix(".json")
            for key in store.list_keys("results/")
        }
        self.seen |= {
            key.removeprefix("dead/").removesuffix(".json") for key in store.list_keys("dead/")
        }
        self.attempts = defaultdict(int)
        self.results = []
        self.targets = {}

    def _target(self, manifest):
        pair = manifest["pair"]
        if pair not in self.targets:
            self.targets[pair] = attach_sql_audit(
                self.target_factory(manifest["right"]),
                SqlAudit(self.store, "right", pair=pair),
            )
        return self.targets[pair]

    def poll(self):
        changed = 0
        for key, manifest in self.store.list_json("manifest/"):
            if key == "manifest/_CLOSED" or key.endswith(".keys.json") or key in self.manifests:
                continue
            if manifest["sample"]["mode"] == "fixed":
                keys_key = f"manifest/{manifest['pair']}.keys.json"
                if not self.store.exists(keys_key):
                    continue
                key_tuples = self.store.get_json(keys_key)["windows"]
                for window in manifest["windows"]:
                    window["key_tuples"] = key_tuples.get(window["bucket"], [])
            self.scheduler.add_manifest(manifest)
            self.manifests.add(key)
            log.info(
                "manifest %s loaded: %d windows x %d columns = %d units -> B-queue seeded",
                manifest["pair"],
                len(manifest["windows"]),
                len(manifest["columns"]),
                manifest["expected_units"],
            )
            changed += 1
        for key, pointer in self.store.list_json("_INDEX/", start_after=self.cursor):
            self.cursor = key
            if pointer["unit_id"] in self.seen:
                continue
            self.seen.add(pointer["unit_id"])
            self.scheduler.add_unit(pointer)
            changed += 1
        if changed:
            log.info("poll: %d new object(s), cursor=%s", changed, self.cursor)
        return changed

    def _failure(self, task, exc):
        uid = task.payload.get("unit_id") or f"right--{task.pair}--{task.window}"
        details = "".join(traceback.format_exception(exc))
        self.attempts[uid] += 1
        if self.attempts[uid] >= self.max_attempts:
            units = [uid]
            if task.kind == "prefetch":
                units = [
                    unit_id(task.pair, task.window, col)
                    for col in self.scheduler.manifests[task.pair]["columns"]
                ]
            for failed_uid in units:
                log.error(
                    "dead %s after %d attempt(s): %s",
                    failed_uid,
                    self.attempts[uid],
                    exc,
                    extra={"dtrack_traceback": details},
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
            return
        log.warning("retry %s attempt %d: %s", uid, self.attempts[uid], exc)
        if task.kind == "prefetch":
            self.scheduler.mark_prefetch_failed(task.pair, task.window)
        else:
            self.scheduler.requeue_unit(task.payload)

    def run_task(self, task):
        try:
            if task.kind == "prefetch":
                manifest = self.scheduler.manifests[task.pair]
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
                log.info(
                    "B right stats ready %s %s (%.3fs) -> %d unit(s) promoted to A-queue",
                    task.pair,
                    task.window,
                    time.monotonic() - started,
                    len(self.scheduler.ready),
                )
                return
            pointer = task.payload
            left = self.store.get_json(pointer["payload_key"])
            right = self.scheduler.right_stats[(task.pair, task.window)][pointer["column"]]
            result = compare_statistics(
                pair=task.pair, window=task.window, column=pointer["column"], left=left, right=right
            )
            result["unit_id"] = pointer["unit_id"]
            result["right_col"] = pointer.get("right_column") or pointer["column"]
            self.store.put_result(pointer["unit_id"], result)
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
                    len(self.store.list_keys("dead/")),
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
