from __future__ import annotations

from collections import defaultdict, deque

from .models import DrainTask


class DrainScheduler:
    """Ready comparisons always outrank speculative Right-side prefetch."""

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
