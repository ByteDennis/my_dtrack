import threading

import pytest

from pair_recon_left.common.runtime.stop import (
    Heartbeat,
    StopRequested,
    StopSignal,
    abandoning_executor,
    beat,
    canceller,
)


class Cursor:
    def __init__(self):
        self.cancelled = 0

    def cancel(self):
        self.cancelled += 1


class Connection:
    def __init__(self):
        self.interrupted = 0

    def interrupt(self):
        self.interrupted += 1


def test_canceller_prefers_cursor_cancel_over_connection_interrupt():
    cursor, connection = Cursor(), Connection()
    assert canceller(cursor, connection) == cursor.cancel
    assert canceller(connection) == connection.interrupt
    assert canceller(object()) is None


def test_trigger_cancels_only_statements_still_running():
    stop = StopSignal()
    running, finished = Cursor(), Cursor()
    with stop.guard(finished):
        pass
    with pytest.raises(StopRequested, match="Right failed"), stop.guard(running):
        assert stop.trigger("Right failed") == 1
    assert (running.cancelled, finished.cancelled) == (1, 0)
    assert stop.triggered and stop.trigger("again") == 0 and str(stop.reason) == "Right failed"


def test_guard_turns_driver_errors_into_stop_requested_once_triggered():
    stop = StopSignal()
    with pytest.raises(StopRequested, match="stop now"), stop.guard(Cursor()):
        stop.trigger("stop now")
        raise ValueError("ORA-01013: user requested cancel")
    with pytest.raises(ValueError, match="plain failure"), StopSignal().guard(Cursor()):
        raise ValueError("plain failure")


def test_guard_refuses_new_statements_after_trigger():
    stop = StopSignal()
    stop.trigger(RuntimeError("done"))
    with pytest.raises(StopRequested, match="done"), stop.guard(Cursor()):
        pytest.fail("statement must not start")


def test_abandoning_executor_does_not_join_workers_when_the_body_raises():
    release = threading.Event()
    started = threading.Event()

    def blocked():
        started.set()
        release.wait(5)

    with pytest.raises(RuntimeError, match="abort"), abandoning_executor(1, "test") as pool:
        pool.submit(blocked)
        assert started.wait(1)
        raise RuntimeError("abort")
    assert not release.is_set()
    release.set()


def test_abandoning_executor_joins_workers_on_success():
    seen = []
    with abandoning_executor(2, "test") as pool:
        pool.submit(seen.append, 1)
    assert seen == [1]


class FlakyStore:
    def __init__(self, failures=0):
        self.failures = failures
        self.beats = 0

    def heartbeat(self):
        if self.failures:
            self.failures -= 1
            raise OSError("IncompleteRead")
        self.beats += 1


def test_beat_survives_a_failed_heartbeat_write(caplog):
    store = FlakyStore(failures=1)
    with caplog.at_level("WARNING"):
        assert beat(store) is False
    assert "left heartbeat write failed" in caplog.text
    assert beat(store) is True and store.beats == 1


def test_heartbeat_ticks_at_most_once_per_interval():
    store = FlakyStore()
    heartbeat = Heartbeat(store, interval=3600)
    assert heartbeat.tick() is True
    assert heartbeat.tick() is False and store.beats == 1
    heartbeat.last -= 3600
    assert heartbeat.tick() is True and store.beats == 2
