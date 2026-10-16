import io
import logging

import dtrack_local.cli as local_cli
from dtrack_left.common.run_logging import current_left_log, get_left_logger, get_logger
from dtrack_local.run_logging import configure_run_logging, current_error_log


def test_run_errors_go_to_a_dedicated_file_and_not_the_console(tmp_path):
    parent = get_logger()
    original_handlers, original_level, original_propagate = (
        parent.handlers[:],
        parent.level,
        parent.propagate,
    )
    console_stream = io.StringIO()
    console = logging.StreamHandler(console_stream)
    parent.handlers = [console]
    parent.setLevel(logging.DEBUG)
    parent.propagate = False
    try:
        path = configure_run_logging(tmp_path / "run")
        logger = get_logger("local.test")
        left_logger = get_left_logger("col_check")
        logger.info("visible progress")
        left_logger.info("persistent left progress")
        logger.error("private failure", extra={"dtrack_traceback": "traceback details\n"})
        for handler in parent.handlers:
            handler.flush()

        assert "visible progress" in console_stream.getvalue()
        assert "private failure" not in console_stream.getvalue()
        assert "private failure" in path.read_text()
        assert "traceback details" in path.read_text()
        assert "visible progress" not in path.read_text()
        assert current_error_log() == path
        assert current_left_log() == tmp_path / "run" / "_local" / "logs" / "left.log"
        assert "persistent left progress" in current_left_log().read_text()
    finally:
        for handler in parent.handlers:
            if getattr(handler, "dtrack_error_handler", False):
                handler.close()
        parent.handlers = original_handlers
        parent.setLevel(original_level)
        parent.propagate = original_propagate


def test_cli_keeps_an_unexpected_traceback_out_of_console(tmp_path, monkeypatch, capsys):
    parent = get_logger()
    path = configure_run_logging(tmp_path / "run")
    monkeypatch.setattr(
        local_cli.cli, "main", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    try:
        assert local_cli.main([]) == 1
        console = capsys.readouterr().err
        assert console == f"Command failed; details: {path}\n"
        logged = path.read_text()
        assert "local command failed" in logged and "RuntimeError: boom" in logged
    finally:
        for handler in list(parent.handlers):
            if getattr(handler, "dtrack_error_handler", False):
                parent.removeHandler(handler)
                handler.close()


def test_named_loggers_keep_one_stable_namespace():
    assert get_logger().name == "dtrack"
    assert get_logger("local").name == "dtrack.local"
    assert get_left_logger().name == "dtrack.left"
    assert get_left_logger("stats").name == "dtrack.left.stats"
