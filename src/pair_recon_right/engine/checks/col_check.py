from __future__ import annotations

import logging
import threading
import time
import traceback
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from ..common.model.attribute_checks import attribute_check_names, get_attribute_check
from ..common.model.audit import SqlAudit, attach_sql_audit
from ..common.model.manifest import DEFAULT_STATISTICS, planned_unit_ids, unit_id_for, window_id
from ..common.model.protocol import (
    active_unit_ids,
    col_check_close_document,
    current_plan_ids,
    fixed_keys_document,
    list_failures,
    list_plans,
    list_result_batches,
    list_results,
    refresh_units_status,
    unit_batch_units,
    unit_prefix,
    unit_statistics,
    write_right_state,
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


# >>> "name=left|right" per compared statistic; only mismatches when diff_only <<< #
def result_detail(result: dict[str, Any], diff_only: bool = False) -> str:
    parts = []
    for name in attribute_check_names():
        if f"{name}_left" not in result:
            continue
        if diff_only and get_attribute_check(name).result_match(result) is not False:
            continue
        parts.append(f"{name}={result[f'{name}_left']}|{result[f'{name}_right']}")
    return " ".join(parts)


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
DEFAULT_RESULT_BATCH_SIZE = 64
DEFAULT_HEARTBEAT_TIMEOUT = 600.0


class PlanChangedError(RuntimeError):
    pass


# >>> fails once the Left heartbeat is older than the timeout; an unreadable heartbeat only logs <<< #
def require_left_heartbeat(store, timeout: float) -> float | None:
    try:
        age = store.heartbeat_age()
    except Exception:
        log.warning("left heartbeat unreadable; checking again next poll", exc_info=True)
        return None
    if age is not None and age > timeout:
        log.error("left heartbeat stale: %.1fs > %.1fs", age, timeout)
        raise RuntimeError(f"left heartbeat is stale ({age:.1f}s)")
    return age


class ResultBatchCommitError(RuntimeError):
    pass


class ColCheck:
    def __init__(
        self,
        store,
        target_factory,
        *,
        prefetch_depth=3,
        max_attempts=3,
        heartbeat_timeout=DEFAULT_HEARTBEAT_TIMEOUT,
        poll_interval=2,
        on_idle=None,
        right_workers=1,
        result_batch_size=DEFAULT_RESULT_BATCH_SIZE,
    ):
        self.store = store
        self.target_factory = target_factory
        self.right_workers = max(1, int(right_workers))
        self.scheduler = DrainScheduler(prefetch_depth=max(int(prefetch_depth), self.right_workers))
        self.max_attempts = max_attempts
        self.heartbeat_timeout = heartbeat_timeout
        self.poll_interval = poll_interval
        self.on_idle = on_idle
        self.manifests = set()
        stored_results = [value for _key, value in list_results(store)]
        stored_failures = [value for _key, value in list_failures(store)]
        self.result_ids = {value["unit_id"] for value in stored_results}
        self.matched_ids = {
            value["unit_id"] for value in stored_results if value.get("verdict") == "match"
        }
        self.different_ids = self.result_ids - self.matched_ids
        self.failure_ids = {value["unit_id"] for value in stored_failures}
        self.completed_ids = self.result_ids | self.failure_ids
        self.seen = set(self.completed_ids)
        self.active_ids = set()
        self.attempts = defaultdict(int)
        self.results = []
        self.detailed: set[tuple[str, str]] = set()
        self.targets = {}
        self.target_lock = threading.Lock()
        self.unit_batches = set()
        self.ready_windows: set[tuple[str, str]] = set()
        self.result_batch_size = max(1, int(result_batch_size))
        self.pending_matches: list[dict[str, Any]] = []
        self.completed_batches = len(list_result_batches(store))
        self.legacy_result_mode = False

    def _validated_plan_ids(self):
        current = current_plan_ids(self.store)
        loaded = {
            pair: manifest.get("plan_id") for pair, manifest in self.scheduler.manifests.items()
        }
        if loaded and current != loaded:
            raise PlanChangedError("plan changed while Right was running; restart the Right job")
        return current

    def _write_progress(self, phase="running"):
        active = self.active_ids
        write_right_state(
            self.store,
            {
                "phase": phase,
                "plan_ids": self._validated_plan_ids(),
                "expected_units": len(active),
                "completed_units": len(self.completed_ids & active),
                "matched_units": len(self.matched_ids & active),
                "different_units": len(self.different_ids & active),
                "failed_units": len(self.failure_ids & active),
                "completed_batches": self.completed_batches,
                "ready_windows": [
                    f"{pair}:{window}" for pair, window in sorted(self.ready_windows)
                ],
            },
        )

    def _flush_matches(self, *, force=False):
        while len(self.pending_matches) >= self.result_batch_size or (
            force and self.pending_matches
        ):
            count = min(self.result_batch_size, len(self.pending_matches))
            rows = self.pending_matches[:count]
            unit_ids = {row["unit_id"] for row in rows}
            for attempt in range(1, self.max_attempts + 1):
                try:
                    self.store.commit_results(rows)
                    break
                except Exception as exc:
                    self._reconcile_durable_outcomes()
                    if unit_ids <= self.result_ids:
                        self._write_progress()
                        break
                    if attempt >= self.max_attempts:
                        raise ResultBatchCommitError(
                            f"could not commit result batch after {attempt} attempts"
                        ) from exc
                    log.warning("retry result batch commit attempt %d: %s", attempt, exc)
                    time.sleep(min(1.0, max(0.0, self.poll_interval)))
            if unit_ids <= self.result_ids:
                continue
            del self.pending_matches[:count]
            self.result_ids.update(unit_ids)
            self.matched_ids.update(unit_ids)
            self.completed_ids.update(unit_ids)
            self.completed_batches += 1
            self._write_progress()

    def _reconcile_durable_outcomes(self):
        stored_results = [value for _key, value in list_results(self.store)]
        stored_failures = [value for _key, value in list_failures(self.store)]
        result_ids = {value["unit_id"] for value in stored_results}
        matched_ids = {
            value["unit_id"] for value in stored_results if value.get("verdict") == "match"
        }
        failure_ids = {value["unit_id"] for value in stored_failures}
        self.result_ids.update(result_ids)
        self.matched_ids.update(matched_ids)
        self.different_ids.update(result_ids - matched_ids)
        self.failure_ids.update(failure_ids)
        self.completed_ids.update(result_ids | failure_ids)
        self.pending_matches = [
            row for row in self.pending_matches if row["unit_id"] not in result_ids
        ]
        self.completed_batches = len(list_result_batches(self.store))

    def _target(self, manifest):
        pair = manifest["pair"]
        with self.target_lock:
            if pair not in self.targets:
                self.targets[pair] = attach_sql_audit(
                    self.target_factory(
                        {
                            **manifest["right"],
                            "partition_mode": manifest.get("partition_mode", "date"),
                        }
                    ),
                    SqlAudit(
                        self.store,
                        "right",
                        pair=pair,
                        enabled=bool(manifest.get("audit")),
                    ),
                )
        return self.targets[pair]

    def poll(self):
        changed = 0
        for key, manifest in list_plans(self.store):
            identity = (key, manifest.get("plan_id"))
            if identity in self.manifests:
                continue
            if any(known_key == key for known_key, _plan_id in self.manifests):
                raise PlanChangedError(
                    "plan changed while Right was running; restart the Right job"
                )
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
            self.legacy_result_mode |= int(manifest.get("protocol_version") or 2) < 3
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
        for pair in sorted(self.scheduler.manifests):
            for key in self.store.list_keys(unit_prefix(pair)):
                if key in self.unit_batches:
                    continue
                document = self.store.get_json(key)
                for pointer in unit_batch_units(document):
                    if pointer["unit_id"] not in self.active_ids or pointer["unit_id"] in self.seen:
                        continue
                    self.seen.add(pointer["unit_id"])
                    self.scheduler.add_unit(pointer)
                    changed += 1
                self.unit_batches.add(key)
        if changed:
            self._write_progress()
            log.info("poll: %d new protocol object(s)", changed)
        return changed

    def _failure(self, task, exc):
        uid = task.payload.get("unit_id") or f"right--{task.pair}--{task.window}"
        self._reconcile_durable_outcomes()
        if uid in self.completed_ids:
            log.warning("recovered durable outcome for %s after: %s", uid, exc)
            self._write_progress()
            return
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
                        "plan_id": self.scheduler.manifests[task.pair].get("plan_id"),
                    },
                )
                self.failure_ids.add(failed_uid)
                self.completed_ids.add(failed_uid)
            self._write_progress()
            return
        log.warning("retry %s attempt %d: %s", uid, self.attempts[uid], exc)
        if task.kind == "prefetch":
            self.scheduler.mark_prefetch_failed(task.pair, task.window)
        else:
            self.scheduler.requeue_unit(task.payload)

    def _fetch_right(self, task):
        manifest = self.scheduler.manifests[task.pair]
        started = time.monotonic()
        log.info(
            "B fetch_right %s %s: %d partition(s)",
            task.pair,
            task.window,
            len(task.payload["partitions"]),
        )
        stats = self._target(manifest).window_statistics(manifest, task.payload)
        if stats:
            column, payload = next(iter(stats.items()))
            log.info(
                "B right stats %s %s: %d column(s); first %s = %s",
                task.pair,
                task.window,
                len(stats),
                column,
                payload,
            )
        return stats, time.monotonic() - started

    def _complete_prefetch(self, task, stats, elapsed):
        manifest = self.scheduler.manifests[task.pair]
        mapped = {left: stats[right] for left, right in manifest["col_map"].items()}
        self.scheduler.mark_right_ready(task.pair, task.window, mapped)
        self.ready_windows.add((task.pair, task.window))
        self._write_progress()
        log.info(
            "B right stats ready %s %s (%.3fs) -> %d unit(s) promoted to A-queue",
            task.pair,
            task.window,
            elapsed,
            len(self.scheduler.ready),
        )

    def run_task(self, task):
        try:
            manifest = self.scheduler.manifests[task.pair]
            if task.kind == "prefetch":
                stats, elapsed = self._fetch_right(task)
                self._complete_prefetch(task, stats, elapsed)
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
            result["plan_id"] = manifest.get("plan_id")
            result["window_id"] = pointer.get("window_id")
            if result["verdict"] == "match" and not self.legacy_result_mode:
                self.pending_matches.append(result)
                self._flush_matches()
            else:
                self.store.put_result(pointer["unit_id"], result)
                self.result_ids.add(pointer["unit_id"])
                if result["verdict"] == "match":
                    self.matched_ids.add(pointer["unit_id"])
                else:
                    self.different_ids.add(pointer["unit_id"])
                self.completed_ids.add(pointer["unit_id"])
                self._write_progress()
            self.results.append(result)
            first = (task.pair, task.window) not in self.detailed
            self.detailed.add((task.pair, task.window))
            detail = result_detail(result, diff_only=not first)
            log.info(
                "A compare %s %s %s -> %s%s",
                task.pair,
                task.window,
                pointer["column"],
                result["verdict"].upper(),
                f" [{result['col_type']}] {detail}" if detail else "",
            )
        except (PlanChangedError, ResultBatchCommitError):
            raise
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
        return active_unit_ids(self.store) <= self.completed_ids

    def run(self):
        jobs = {}
        log.info("Databricks query worker budget=%d", self.right_workers)
        with ThreadPoolExecutor(
            max_workers=self.right_workers,
            thread_name_prefix="pair_recon-right",
        ) as executor:
            while True:
                self.poll()
                for future in [future for future in jobs if future.done()]:
                    task = jobs.pop(future)
                    try:
                        stats, elapsed = future.result()
                        self._complete_prefetch(task, stats, elapsed)
                    except PlanChangedError:
                        raise
                    except Exception as exc:
                        self._failure(task, exc)
                while len(jobs) < self.right_workers:
                    task = (
                        None
                        if not self.scheduler.ready and self.complete()
                        else self.scheduler.next_task()
                    )
                    if task is None:
                        break
                    if task.kind == "prefetch":
                        jobs[executor.submit(self._fetch_right, task)] = task
                    else:
                        self.run_task(task)
                if jobs:
                    wait(
                        tuple(jobs),
                        timeout=max(0.01, self.poll_interval),
                        return_when=FIRST_COMPLETED,
                    )
                    continue
                if self.pending_matches and not self.scheduler.ready:
                    self._flush_matches(force=True)
                if self.complete():
                    plans = [plan for _key, plan in list_plans(self.store)]
                    active = active_unit_ids(self.store)
                    durable_results = {value["unit_id"] for _key, value in list_results(self.store)}
                    durable_failures = {
                        value["unit_id"] for _key, value in list_failures(self.store)
                    }
                    if not active <= durable_results | durable_failures:
                        raise RuntimeError(
                            "Right completion checkpoint is missing durable outcomes"
                        )
                    refresh_units_status(self.store)
                    SqlAudit(
                        self.store,
                        "right",
                        enabled=any(bool(plan.get("audit")) for plan in plans),
                    ).refresh()
                    self._write_progress("complete")
                    log.info(
                        "col_check complete: %d result(s), %d dead",
                        len(self.results),
                        sum(
                            value["unit_id"] in active for _key, value in list_failures(self.store)
                        ),
                    )
                    return self.results
                if self.on_idle and self.on_idle():
                    continue
                require_left_heartbeat(self.store, self.heartbeat_timeout)
                time.sleep(self.poll_interval)
