from __future__ import annotations

import logging
import time
import traceback
from collections import defaultdict

from .comparison import compare_statistics
from .manifest import unit_id
from .scheduler import DrainScheduler

log = logging.getLogger("dtrack.cloud.consumer")


# >>> drain loop: A (compare) first, B (fetch own side) otherwise; exits on _CLOSED + all units accounted <<< #
class Consumer:
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
            self.targets[pair] = self.target_factory(manifest["right"])
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
        self.attempts[uid] += 1
        if self.attempts[uid] >= self.max_attempts:
            units = [uid]
            if task.kind == "prefetch":
                units = [
                    unit_id(task.pair, task.window, col)
                    for col in self.scheduler.manifests[task.pair]["columns"]
                ]
            for failed_uid in units:
                log.error("dead %s after %d attempt(s): %s", failed_uid, self.attempts[uid], exc)
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
                    "drain complete: %d result(s), %d dead",
                    len(self.results),
                    len(self.store.list_keys("dead/")),
                )
                return self.results
            if self.on_idle and self.on_idle():
                last_progress = time.monotonic()
                continue
            age = self.store.heartbeat_age()
            if age is not None and age > self.heartbeat_timeout:
                log.error("producer heartbeat stale: %.1fs > %.1fs", age, self.heartbeat_timeout)
                raise RuntimeError(f"producer heartbeat is stale ({age:.1f}s)")
            if time.monotonic() - last_progress > self.idle_timeout:
                raise TimeoutError("consumer idle timeout expired")
            time.sleep(self.poll_interval)
