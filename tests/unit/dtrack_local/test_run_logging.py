import io
import logging

import dtrack_local.cli as local_cli
from dtrack_left.common.run_logging import current_left_log
from dtrack_local.run_logging import configure_run_logging, current_error_log


def test_run_errors_go_to_a_dedicated_file_and_not_the_console(tmp_path):
    root = logging.getLogger()
    original_handlers, original_level = root.handlers[:], root.level
    console_stream = io.StringIO()
    console = logging.StreamHandler(console_stream)
    root.handlers = [console]
    root.setLevel(logging.DEBUG)
    try:
        path = configure_run_logging(tmp_path / "run")
        logger = logging.getLogger("dtrack.local.test")
        left_logger = logging.getLogger("dtrack.left.col_check")
        logger.info("visible progress")
        left_logger.info("persistent left progress")
        logger.error("private failure", extra={"dtrack_traceback": "traceback details\n"})
        for handler in root.handlers:
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
        for handler in root.handlers:
            if getattr(handler, "dtrack_error_handler", False):
                handler.close()
        root.handlers = original_handlers
        root.setLevel(original_level)


def test_cli_keeps_an_unexpected_traceback_out_of_console(tmp_path, monkeypatch, capsys):
    root = logging.getLogger()
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
        for handler in list(root.handlers):
            if getattr(handler, "dtrack_error_handler", False):
                root.removeHandler(handler)
                handler.close()
