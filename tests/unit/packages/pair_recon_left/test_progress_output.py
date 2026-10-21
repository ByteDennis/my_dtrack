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


def test_console_filter_hides_file_only_progress_records():
    record = logging.LogRecord("pair_recon.left", logging.INFO, "", 1, "message", (), None)
    console_filter = run_logging._ConsoleFilter()

    assert console_filter.filter(record)
    record.console_silent = True
    assert not console_filter.filter(record)
