import logging

import pytest

from pair_recon_left.common.runtime import progress, run_logging


class Bar:
    def __init__(self, **options):
        self.options = options
        self.refreshed = 0
        self.closed = False

    def update(self, _amount):
        pass

    def refresh(self):
        self.refreshed += 1

    def set_postfix_str(self, _value, *, refresh):
        assert refresh is False

    def close(self):
        self.closed = True


class Redirect:
    def __init__(self, events):
        self.events = events

    def __enter__(self):
        self.events.append("enter")

    def __exit__(self, *_args):
        self.events.append("exit")


@pytest.mark.parametrize("interactive", [True, False])
def test_statistics_progress_uses_one_terminal_line_or_log_fallback(monkeypatch, interactive):
    bars = []
    events = []
    records = []

    def make_bar(**options):
        bar = Bar(**options)
        bars.append(bar)
        return bar

    monkeypatch.setattr(progress, "_interactive_terminal", lambda: interactive)
    monkeypatch.setattr(progress, "tqdm", make_bar)
    monkeypatch.setattr(
        progress,
        "logging_redirect_tqdm",
        lambda **_options: Redirect(events),
    )
    monkeypatch.setattr(
        progress.log,
        "info",
        lambda *args, **kwargs: records.append((args, kwargs)),
    )

    with progress.StatisticsProgress(5) as current:
        current.report()

    assert bars[0].options["disable"] is (not interactive)
    assert bars[0].options["leave"] is False
    assert bars[0].refreshed == 1 and bars[0].closed
    assert events == (["enter", "exit"] if interactive else [])
    assert records[0][1]["extra"] == {"console_silent": interactive}


class RecordingBar(Bar):
    def __init__(self, calls, **options):
        super().__init__(**options)
        self.calls = calls

    def update(self, amount):
        self.calls.append(("update", amount))

    def refresh(self):
        self.calls.append(("refresh",))

    def set_postfix_str(self, value, *, refresh):
        assert refresh is False
        self.calls.append(("postfix", value))


def _postfixes(calls):
    return [call[1] for call in calls if call[0] == "postfix"]


def test_statistics_progress_shows_query_percentages_and_sets_postfix_before_update(monkeypatch):
    calls = []
    clock = [100.0]
    monkeypatch.setattr(progress, "_interactive_terminal", lambda: True)
    monkeypatch.setattr(progress, "tqdm", lambda **options: RecordingBar(calls, **options))
    monkeypatch.setattr(progress, "logging_redirect_tqdm", lambda **_options: Redirect([]))
    monkeypatch.setattr(progress.time, "monotonic", lambda: clock[0])

    with progress.StatisticsProgress(9, total_attributes=2, total_vintages=1) as current:
        current.start("w", "pair/w", 2)
        assert "phase=load " in _postfixes(calls)[-1] and calls[-1] == ("refresh",)
        query = current.query_callback("w")
        query("load", 0, 37.5)
        assert "phase=load 38% " in _postfixes(calls)[-1] and calls[-1][0] == "postfix"
        clock[0] += 1.0
        query("load", 0, 81.0)
        assert calls[-1] == ("refresh",)
        query("load", 0, None)
        assert "phase=load " in _postfixes(calls)[-1]
        current.update("w", "infer", 1, 2)
        assert calls[-2:] == [("postfix", _postfixes(calls)[-1]), ("update", 1)]
        assert "phase=infer " in _postfixes(calls)[-1]
        query("aggregate", 1, 55.0)
        query("aggregate", 0, 10.0)
        assert "phase=aggregate 10%|aggregate 55% " in _postfixes(calls)[-1]
        current.complete("w")
        assert calls[-2:] == [("postfix", _postfixes(calls)[-1]), ("update", 8)]
        assert "vintages=0 phase=- " in _postfixes(calls)[-1]


def test_statistics_progress_report_logs_the_active_phase(monkeypatch):
    records = []
    monkeypatch.setattr(progress, "_interactive_terminal", lambda: False)
    monkeypatch.setattr(progress, "tqdm", lambda **options: Bar(**options))
    monkeypatch.setattr(progress.log, "info", lambda *args, **kwargs: records.append(args))

    with progress.StatisticsProgress(5, total_vintages=1) as current:
        current.start("w", "pair/w", 1)
        current.query_callback("w")("aggregate", 0, 64.2)
        current.report()

    assert "phase=%s" in records[0][0] and records[0][3] == "aggregate 64%"


def test_console_filter_hides_file_only_progress_records():
    record = logging.LogRecord("pair_recon.left", logging.INFO, "", 1, "message", (), None)
    console_filter = run_logging._ConsoleFilter()

    assert console_filter.filter(record)
    record.console_silent = True
    assert not console_filter.filter(record)
