from __future__ import annotations

import csv
import hashlib
import inspect
import itertools
import json
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import pandas as pd

from ...backends.sources import source_from_config
from ...backends.store import replace_local_file
from ..model.audit import SqlAudit, attach_sql_audit
from ..model.manifest import planned_unit_ids, unit_id_for, validate_manifest, window_id
from ..model.protocol import (
    COL_CHECK_CLOSED,
    current_plan_ids,
    fixed_keys_document,
    fixed_keys_path,
    get_left_state,
    get_right_state,
    get_run_status,
    left_row_check_close_document,
    list_failures,
    list_plans,
    list_results,
    list_units,
    row_check_is_closed,
    timestamp,
    unit_batch_path,
    write_left_state,
    write_run_status,
)
from ..runtime.duck import use_temp
from ..runtime.progress import StatisticsProgress
from ..runtime.run_logging import get_left_logger
from ..runtime.stats import compute_csv_statistics

log = get_left_logger("col_check")

BACKPRESSURE_LOG_INTERVAL = 30.0
DEFAULT_PROGRESS_INTERVAL = 10.0


def wait_for_manifests(
    store,
    *,
    timeout: float = 1800,
    poll_interval: float = 3,
    pair_names=None,
    abort_if=None,
):
    abort_if = abort_if or (lambda: None)
    started = time.monotonic()
    last_state = None
    while True:
        abort_if()
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
        if store.exists("_HEALTH_CHECK.json") and not store.get_json("_HEALTH_CHECK.json").get(
            "ok", True
        ):
            raise RuntimeError(
                f"Right side health check failed: {store.get_json('_HEALTH_CHECK.json')}"
            )
        if time.monotonic() - started > timeout:
            raise TimeoutError("Right side did not close the row check before timeout")
        time.sleep(poll_interval)


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
        source_from_config(
            {
                **manifest["left"],
                "partition_mode": "all"
                if manifest.get("left_snapshot")
                else manifest.get("partition_mode", "date"),
            }
        ),
        SqlAudit(
            store,
            "left",
            pair=manifest["pair"],
            enabled=bool(manifest.get("audit")),
        ),
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


class ColCheckPipeline:
    def __init__(
        self,
        store,
        *,
        pull_window: Callable,
        compute_stats: Callable,
        pull_workers: int = 2,
        wip: int = 200,
        backpressure_sleep: float = 1.0,
        progress_interval: float | None = None,
        wait_for_right: bool = True,
        abort_if=None,
    ):
        self.store = store
        self.pull_window = pull_window
        self.compute_stats = compute_stats
        self.pull_workers = max(1, pull_workers)
        self.stat_workers = 1
        self.wip = max(1, wip)
        self.backpressure_sleep = max(0, backpressure_sleep)
        self.wait_for_right = wait_for_right
        self.abort_if = abort_if or (lambda: None)
        self.progress_interval = max(
            0.01,
            float(
                progress_interval
                if progress_interval is not None
                else os.getenv("PAIR_RECON_PROGRESS_INTERVAL") or DEFAULT_PROGRESS_INTERVAL
            ),
        )
        try:
            parameters = inspect.signature(self.compute_stats).parameters
        except (TypeError, ValueError):
            parameters = {}
        self.compute_reports_progress = "progress" in parameters
        self.compute_reports_batches = "on_batch" in parameters
        self.compute_reports_shards = "shard_status" in parameters

    def _compute(self, manifest, window, value, progress, on_batch, shard_status):
        options = {}
        if self.compute_reports_progress:
            options["progress"] = progress
        if self.compute_reports_batches:
            options["on_batch"] = on_batch
        if self.compute_reports_shards:
            options["shard_status"] = shard_status
        return self.compute_stats(manifest, window, value, **options)

    def run(self, manifests: Iterable[dict[str, Any]]) -> int:
        self.abort_if()
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
        run_timezone = get_run_status(self.store).get("timezone")
        publication_lock = threading.Lock()
        reserved_ids = set(existing)
        plan_ids = {manifest["pair"]: manifest.get("plan_id") for manifest in manifests}
        published_batches = len(self.store.list_keys("units/"))
        ready = deque()
        plan_rows = []
        for manifest, window in iter_windows_round_robin(manifests):
            if all(
                unit_id_for(manifest, window, column) in existing for column in manifest["columns"]
            ):
                log.info(
                    "skip %s %s: every unit already indexed",
                    manifest["pair"],
                    window["bucket"],
                )
                continue
            plan_rows.append((manifest, window))
        plans = iter(plan_rows)
        exhausted = False
        self.store.delete_keys([COL_CHECK_CLOSED])
        write_run_status(self.store, {"state": "left_col_check", "role": "left"})
        write_left_state(
            self.store,
            {
                "phase": "col_check",
                "pairs": sorted(pair_names),
                "plan_ids": plan_ids,
                "expected_units": len(active_ids),
                "published_units": len(ready_ids),
                "published_batches": published_batches,
                "col_check_closed_at": None,
                "heartbeat_epoch_s": time.time(),
                "heartbeat_at": timestamp(self.store),
            },
        )
        log.info(
            "col_check start: %d plan(s), %d vintage(s), %d attribute unit(s) already ready, "
            "next sequence=%d, wip=%s, active_vintages=1",
            len(manifests),
            len(plan_rows),
            len(ready_ids),
            sequence,
            self.wip if self.wait_for_right else "disabled",
        )
        total_attributes = sum(len(manifest["columns"]) for manifest, _ in plan_rows)
        progress_steps = sum(1 + 4 * len(manifest["columns"]) for manifest, _ in plan_rows)
        with (
            StatisticsProgress(
                progress_steps,
                total_attributes=total_attributes,
                total_vintages=len(plan_rows),
            ) as progress,
            ThreadPoolExecutor(self.pull_workers, thread_name_prefix="pair_recon-pull") as pulls,
            ThreadPoolExecutor(1, thread_name_prefix="pair_recon-vintage") as stats,
        ):
            pull_jobs, stat_jobs = {}, {}

            def publisher_for(manifest, window, progress_key):
                published_columns = {
                    column
                    for column in manifest["columns"]
                    if unit_id_for(manifest, window, column) in existing
                }
                if published_columns:
                    progress.update(
                        progress_key,
                        "publish",
                        len(published_columns),
                        len(manifest["columns"]),
                    )

                def publish(rows):
                    nonlocal published_batches, sequence
                    self.abort_if()
                    with publication_lock:
                        pending = []
                        for column in manifest["columns"]:
                            if column not in rows:
                                continue
                            uid = unit_id_for(manifest, window, column)
                            if uid in reserved_ids:
                                continue
                            reserved_ids.add(uid)
                            pending.append(
                                {
                                    "unit_id": uid,
                                    "left_statistics": rows[column],
                                    "pair": manifest["pair"],
                                    "window": window["bucket"],
                                    "column": column,
                                    "right_column": manifest["col_map"][column],
                                    "plan_id": manifest.get("plan_id"),
                                    "window_id": window_id(manifest, window),
                                }
                            )
                        first = sequence
                        sequence += len(pending)
                    if not pending:
                        return
                    if self.wait_for_right:
                        self._wait_for_capacity(active_ids, version2)
                    path = unit_batch_path(first, len(pending))
                    log.info(
                        "statistics shard upload %s %s: attributes=%d -> %s",
                        manifest["pair"],
                        window["bucket"],
                        len(pending),
                        path,
                    )
                    try:
                        self.store.commit_units(
                            first,
                            pending,
                            timezone_name=run_timezone,
                        )
                    except BaseException:
                        with publication_lock:
                            reserved_ids.difference_update(unit["unit_id"] for unit in pending)
                        raise
                    with publication_lock:
                        for unit in pending:
                            published_columns.add(unit["column"])
                            existing.add(unit["unit_id"])
                            ready_ids.add(unit["unit_id"])
                        uploaded_for_vintage = len(published_columns)
                        run_ready = len(ready_ids)
                        published_batches += 1
                    progress.update(
                        progress_key,
                        "publish",
                        uploaded_for_vintage,
                        len(manifest["columns"]),
                    )
                    log.info(
                        "statistics shard published %s %s: attributes_uploaded=%d, "
                        "vintage_uploaded=%d/%d, run_ready=%d/%d, sequence=%08d..%08d",
                        manifest["pair"],
                        window["bucket"],
                        len(pending),
                        uploaded_for_vintage,
                        len(manifest["columns"]),
                        run_ready,
                        len(active_ids),
                        first,
                        first + len(pending) - 1,
                    )
                    write_left_state(
                        self.store,
                        {
                            "phase": "col_check",
                            "plan_ids": plan_ids,
                            "expected_units": len(active_ids),
                            "published_units": run_ready,
                            "published_batches": published_batches,
                            "heartbeat_epoch_s": time.time(),
                            "heartbeat_at": timestamp(self.store),
                        },
                    )

                return publish

            while pull_jobs or stat_jobs or ready or not exhausted:
                self.abort_if()
                while not exhausted and len(pull_jobs) < self.pull_workers:
                    self.abort_if()
                    try:
                        manifest, window = next(plans)
                    except StopIteration:
                        exhausted = True
                        break
                    log.info(
                        "left data pull started: pair=%s vintage=%s partitions=%d columns=%d",
                        manifest["pair"],
                        window["bucket"],
                        len(window["partitions"]),
                        len(manifest["columns"]),
                    )
                    job = pulls.submit(self.pull_window, manifest, window)
                    pull_jobs[job] = (manifest, window)
                while ready and len(stat_jobs) < self.stat_workers:
                    self.abort_if()
                    manifest, window, value = ready.popleft()
                    log.info(
                        "statistics %s %s: calculating %d column(s)",
                        manifest["pair"],
                        window["bucket"],
                        len(manifest["columns"]),
                    )
                    key = (manifest["pair"], window["bucket"])
                    progress.start(
                        key,
                        f"{manifest['pair']}/{window['bucket']}",
                        len(manifest["columns"]),
                    )
                    publish_rows = publisher_for(manifest, window, key)
                    job = stats.submit(
                        self._compute,
                        manifest,
                        window,
                        value,
                        progress.callback(key),
                        publish_rows,
                        progress.shard_callback(key),
                    )
                    stat_jobs[job] = (manifest, window, key, publish_rows)
                active = set(pull_jobs) | set(stat_jobs)
                if not active:
                    continue
                complete, _pending = wait(
                    active,
                    timeout=self.progress_interval,
                    return_when=FIRST_COMPLETED,
                )
                if not complete:
                    self.abort_if()
                    progress.report()
                    self.store.heartbeat()
                    continue
                for job in complete:
                    self.abort_if()
                    if job in pull_jobs:
                        manifest, window = pull_jobs.pop(job)
                        log.info(
                            "left data ready for statistics: pair=%s vintage=%s",
                            manifest["pair"],
                            window["bucket"],
                        )
                        ready.append((manifest, window, job.result()))
                        continue
                    manifest, window, progress_key, publish_rows = stat_jobs.pop(job)
                    rows = job.result()
                    publish_rows(rows)
                    progress.complete(progress_key)
                    log.info(
                        "statistics vintage complete %s %s: attributes_uploaded=%d/%d, "
                        "run_ready=%d/%d",
                        manifest["pair"],
                        window["bucket"],
                        len(
                            {
                                column
                                for column in manifest["columns"]
                                if unit_id_for(manifest, window, column) in existing
                            }
                        ),
                        len(manifest["columns"]),
                        len(ready_ids),
                        len(active_ids),
                    )
            progress.report()
        self.abort_if()
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
        SqlAudit(
            self.store,
            "left",
            enabled=any(bool(manifest.get("audit")) for manifest in manifests),
        ).refresh()
        return sequence - first_sequence

    def _unit_progress(self, active_ids: set[str]) -> tuple[int, int, int]:
        plan_ids = current_plan_ids(self.store)
        left_state = get_left_state(self.store)
        right_state = get_right_state(self.store)
        if left_state.get("plan_ids") == plan_ids and right_state.get("plan_ids") == plan_ids:
            left_ready = min(len(active_ids), int(left_state.get("published_units") or 0))
            right_finished = min(len(active_ids), int(right_state.get("completed_units") or 0))
            return left_ready, right_finished, max(0, left_ready - right_finished)
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
        return len(units), len(finished), len(units - finished)

    def _wait_for_capacity(self, active_ids: set[str], version2: bool) -> None:
        waiting_started = None
        last_wait_log = None
        while True:
            self.abort_if()
            left_ready, right_finished, active_ready_unclaimed = self._unit_progress(active_ids)
            ready_unclaimed = active_ready_unclaimed if version2 else self.store.ready_unclaimed()
            if ready_unclaimed < self.wip:
                if waiting_started is not None:
                    log.info(
                        "backpressure cleared: ready_unclaimed=%d, wip=%d, "
                        "left_ready=%d/%d, right_finished=%d/%d, waited=%.1fs",
                        ready_unclaimed,
                        self.wip,
                        left_ready,
                        len(active_ids),
                        right_finished,
                        len(active_ids),
                        time.monotonic() - waiting_started,
                    )
                return
            now = time.monotonic()
            if waiting_started is None or now - last_wait_log >= BACKPRESSURE_LOG_INTERVAL:
                if waiting_started is None:
                    waiting_started = now
                last_wait_log = now
                log.info(
                    "backpressure: ready_unclaimed=%d, wip=%d, over_limit=%d; "
                    "left_ready=%d/%d, right_finished=%d/%d; waiting for Right (%.1fs)",
                    ready_unclaimed,
                    self.wip,
                    max(0, ready_unclaimed - self.wip),
                    left_ready,
                    len(active_ids),
                    right_finished,
                    len(active_ids),
                    now - waiting_started,
                )
            time.sleep(self.backpressure_sleep)


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
                    source_from_config(
                        {
                            **manifest["left"],
                            "partition_mode": "all"
                            if manifest.get("left_snapshot")
                            else manifest.get("partition_mode", "date"),
                        }
                    ),
                    SqlAudit(
                        self.store,
                        "left",
                        pair=pair,
                        enabled=bool(manifest.get("audit")),
                    ),
                )
            return self.sources[pair]

    def pull_window(self, manifest, window):
        if manifest["sample"]["mode"] == "fixed" and "key_tuples" not in window:
            keys = fixed_keys_document(self.store, manifest["pair"])["windows"]
            window["key_tuples"] = keys.get(window["bucket"], [])
        output = self.work_dir / manifest["pair"] / window["bucket"] / "left.csv"
        metadata = output.with_name(output.name + ".pair_recon.json")
        selected = list(
            dict.fromkeys(
                [
                    *(
                        [manifest["left"]["date_col"]]
                        if manifest.get("partition_mode", "date") == "date"
                        and not manifest.get("left_snapshot")
                        else []
                    ),
                    *manifest["key_cols"],
                    *manifest["columns"],
                ]
            )
        )
        cache = {
            "format": "pair_recon-left-pull-v1",
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
                        "reuse left %s %s: %s",
                        manifest["pair"],
                        window["bucket"],
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
            "left data pull completed: pair=%s vintage=%s sampled_rows=%d path=%s",
            manifest["pair"],
            window["bucket"],
            _row_count(path),
            path,
        )
        return path

    @staticmethod
    def compute_stats(
        manifest,
        _window,
        path,
        *,
        progress=None,
        on_batch=None,
        shard_status=None,
    ):
        return compute_csv_statistics(
            path,
            manifest["columns"],
            strict=manifest.get("strict", False),
            col_types=manifest.get("col_type_overrides"),
            column_types=manifest.get("column_types"),
            side="left",
            statistics=manifest.get("statistics"),
            progress=progress,
            on_batch=on_batch,
            shard_status=shard_status,
        )
