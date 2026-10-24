from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from .run_logging import get_left_logger

log = get_left_logger("stop")
HEARTBEAT_INTERVAL = 60.0


class StopRequested(RuntimeError):
    pass


# >>> writes the Left heartbeat; a transient Volume error only logs so long waits survive it <<< #
def beat(store) -> bool:
    try:
        store.heartbeat()
    except Exception:
        log.warning("left heartbeat write failed; the next tick retries", exc_info=True)
        return False
    return True


# >>> rate-limited heartbeat for polling loops: beats at most once per interval <<< #
class Heartbeat:
    def __init__(self, store, interval: float = HEARTBEAT_INTERVAL):
        self.store = store
        self.interval = interval
        self.last = None

    def tick(self) -> bool:
        now = time.monotonic()
        if self.last is not None and now - self.last < self.interval:
            return False
        self.last = now
        return beat(self.store)


# >>> executor that joins its workers on success but abandons them when the body raises <<< #
@contextmanager
def abandoning_executor(workers: int, prefix: str):
    executor = ThreadPoolExecutor(max(1, workers), thread_name_prefix=prefix)
    try:
        yield executor
    except BaseException:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    executor.shutdown(wait=True)


# >>> first cancel()/interrupt() found on the driver handles, cursor before connection <<< #
def canceller(*handles) -> Callable[[], object] | None:
    for handle in handles:
        for name in ("cancel", "interrupt"):
            method = getattr(handle, name, None)
            if callable(method):
                return method
    return None


# >>> cooperative stop: loops poll check(), statements guarded here get cancelled on trigger <<< #
class StopSignal:
    def __init__(self):
        self.reason: BaseException | str | None = None
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._cancellers: dict[int, Callable[[], object]] = {}
        self._next_id = 0

    @property
    def triggered(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        if self._event.is_set():
            raise StopRequested(str(self.reason) or "stop requested")

    def trigger(self, reason: BaseException | str) -> int:
        with self._lock:
            if not self._event.is_set():
                self.reason = reason
                self._event.set()
            pending = list(self._cancellers.values())
        for cancel in pending:
            try:
                cancel()
            except Exception:
                log.debug("statement cancel failed", exc_info=True)
        if pending:
            log.info("stop requested: cancelled %d running statement(s)", len(pending))
        return len(pending)

    @contextmanager
    def guard(self, *handles):
        self.check()
        cancel = canceller(*handles)
        with self._lock:
            token = self._next_id
            self._next_id += 1
            if cancel is not None:
                self._cancellers[token] = cancel
        try:
            yield
        except StopRequested:
            raise
        except Exception as exc:
            if self._event.is_set():
                raise StopRequested(str(self.reason) or "stop requested") from exc
            raise
        finally:
            with self._lock:
                self._cancellers.pop(token, None)
        self.check()
