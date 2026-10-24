import threading

import pytest

from pair_recon_left.common.runtime.stop import (
    StopRequested,
    StopSignal,
    abandoning_executor,
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
