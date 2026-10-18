from __future__ import annotations

import threading
import time
from collections.abc import Hashable

from tqdm.auto import tqdm

from .run_logging import get_left_logger

log = get_left_logger("progress")
PHASES = ("load", "infer", "aggregate", "finalize", "publish")


class StatisticsProgress:
    def __init__(
        self,
        total_steps: int,
        *,
        total_attributes: int = 0,
        total_vintages: int = 0,
    ):
        self.total_steps = max(0, total_steps)
        self.total_attributes = max(0, total_attributes)
        self.total_vintages = max(0, total_vintages)
        self.completed_steps = 0
        self.phase_completed = {phase: 0 for phase in PHASES}
        self.shards_done = 0
        self.shards_total = 0
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.jobs: dict[Hashable, dict] = {}
        self.bar = tqdm(
            total=self.total_steps,
            desc="Left statistics",
            unit="step",
            dynamic_ncols=True,
            disable=None,
        )

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()

    def start(self, key: Hashable, label: str, columns: int) -> None:
        with self.lock:
            self.jobs[key] = {
                "label": label,
                "totals": {"load": 1, **{phase: columns for phase in PHASES[1:]}},
                "done": {phase: 0 for phase in PHASES},
                "phase": "load",
                "shards": {"active": 0, "done": 0, "total": 0},
            }
            self._refresh_locked()

    def callback(self, key: Hashable):
        def update(phase: str, completed: int, total: int) -> None:
            self.update(key, phase, completed, total)

        return update

    def shard_callback(self, key: Hashable):
        def update(active: int, completed: int, total: int) -> None:
            with self.lock:
                job = self.jobs.get(key)
                if job is None:
                    return
                previous = job["shards"]
                self.shards_done += max(0, completed - previous["done"])
                self.shards_total += max(0, total - previous["total"])
                job["shards"] = {
                    "active": max(0, active),
                    "done": max(0, completed),
                    "total": max(0, total),
                }
                self._refresh_locked()

        return update

    def update(self, key: Hashable, phase: str, completed: int, total: int) -> None:
        if phase not in PHASES:
            raise ValueError(f"unknown statistics progress phase: {phase}")
        with self.lock:
            job = self.jobs.get(key)
            if job is None:
                return
            expected = job["totals"][phase]
            if total != expected:
                raise ValueError(
                    f"statistics progress total changed for {phase}: expected {expected}, got {total}"
                )
            current = job["done"][phase]
            completed = max(current, min(expected, completed))
            delta = completed - current
            job["done"][phase] = completed
            job["phase"] = phase
            if delta:
                self.completed_steps += delta
                self.phase_completed[phase] += delta
                self.bar.update(delta)
            self._refresh_locked()

    def complete(self, key: Hashable) -> None:
        with self.lock:
            job = self.jobs.get(key)
            if job is None:
                return
            remaining = {phase: job["totals"][phase] - job["done"][phase] for phase in PHASES}
            delta = sum(remaining.values())
            self.completed_steps += delta
            for phase, amount in remaining.items():
                self.phase_completed[phase] += amount
            if delta:
                self.bar.update(delta)
            del self.jobs[key]
            self._refresh_locked()

    def report(self) -> None:
        with self.lock:
            elapsed = max(0.0, time.monotonic() - self.started)
            remaining = max(0, self.total_steps - self.completed_steps)
            rate = self.completed_steps / elapsed if elapsed else 0.0
            eta = remaining / rate if rate else None
            shard_active = sum(job["shards"]["active"] for job in self.jobs.values())
            log.info(
                "statistics progress: vintages_active=%d/%d, shards_running=%d, "
                "shards_done=%d/%d, attributes_calculated=%d/%d, "
                "attributes_uploaded=%d/%d, phase_steps=%d/%d (%.1f%%), "
                "elapsed=%.1fs, eta=%s",
                len(self.jobs),
                self.total_vintages,
                shard_active,
                self.shards_done,
                self.shards_total,
                self.phase_completed["finalize"],
                self.total_attributes,
                self.phase_completed["publish"],
                self.total_attributes,
                self.completed_steps,
                self.total_steps,
                100.0 * self.completed_steps / self.total_steps if self.total_steps else 100.0,
                elapsed,
                "unknown" if eta is None else f"{eta:.1f}s",
            )
            self.bar.refresh()

    def close(self) -> None:
        with self.lock:
            self.bar.close()

    def _refresh_locked(self) -> None:
        shard_active = sum(job["shards"]["active"] for job in self.jobs.values())
        self.bar.set_postfix_str(
            f"vintages={len(self.jobs)} shards={shard_active} running/{self.shards_done} done/"
            f"{self.shards_total} total calc={self.phase_completed['finalize']}/"
            f"{self.total_attributes} upload={self.phase_completed['publish']}/"
            f"{self.total_attributes}",
            refresh=False,
        )
