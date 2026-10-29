from __future__ import annotations

import logging
import threading
from collections import deque

from ..common.model.protocol import RIGHT_LOG

log = logging.getLogger("pair_recon.right.log")
FLUSH_SECONDS = 30.0
MAX_BYTES = 20 * 1024 * 1024
DROPPED = "... earlier lines dropped to keep the Volume log under its size cap ...\n"


# >>> keeps formatted records in memory and rewrites _protocol/right.log from a daemon thread every FLUSH_SECONDS; a Volume failure is retried on the next flush and never reaches the job <<< #
class VolumeLogHandler(logging.Handler):
    def __init__(self, store, *, interval: float = FLUSH_SECONDS, max_bytes: int = MAX_BYTES):
        super().__init__()
        self.store = store
        self.interval = interval
        self.max_bytes = max_bytes
        self.lines: deque[str] = deque()
        self.size = 0
        self.dropped = False
        self.dirty = False
        self.failing = False
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._run, name="pair_recon-right-log", daemon=True)
        self.thread.start()

    def emit(self, record):
        try:
            line = self.format(record) + "\n"
        except Exception:
            self.handleError(record)
            return
        self.lines.append(line)
        self.size += len(line.encode("utf-8"))
        self.dirty = True
        while self.size > self.max_bytes and len(self.lines) > 1:
            self.size -= len(self.lines.popleft().encode("utf-8"))
            self.dropped = True

    def _run(self):
        while not self.stopping.wait(self.interval):
            self.flush()

    def flush(self):
        self.acquire()
        try:
            if not self.dirty:
                return
            body = (DROPPED if self.dropped else "") + "".join(self.lines)
            self.dirty = False
        finally:
            self.release()
        try:
            self.store.put_bytes(RIGHT_LOG, body.encode("utf-8"), content_type="text/plain")
        except Exception as exc:
            self.acquire()
            try:
                self.dirty = True
            finally:
                self.release()
            if not self.failing:
                log.warning(
                    "Volume log write failed (%s: %s); retrying every %.0fs",
                    type(exc).__name__,
                    exc,
                    self.interval,
                )
            self.failing = True
            return
        if self.failing:
            log.info("Volume log write recovered")
        self.failing = False

    def close(self):
        self.stopping.set()
        if self.thread.is_alive() and threading.current_thread() is not self.thread:
            self.thread.join(timeout=self.interval)
        self.flush()
        super().close()
