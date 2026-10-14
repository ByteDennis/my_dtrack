from __future__ import annotations

import logging
import time
from pathlib import Path

from dtrack_left.common.run_logging import configure_left_logging

ERROR_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(threadName)s %(name)s: %(message)s"
_current_error_log: Path | None = None


class _BelowError(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR


class _ErrorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        details = getattr(record, "dtrack_traceback", "")
        return rendered if not details else f"{rendered}\n{str(details).rstrip()}"


def _is_console(handler: logging.Handler) -> bool:
    return (
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
        and not handler.__class__.__module__.startswith("_pytest.")
    )


# >>> Left progress is appended to left.log; ERROR/CRITICAL records go only to errors.log <<< #
def configure_run_logging(run_folder: str | Path) -> Path:
    global _current_error_log

    path = Path(run_folder) / "_local" / "logs" / "errors.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    configure_left_logging(path.with_name("left.log"))
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "dtrack_error_handler", False):
            root.removeHandler(handler)
            handler.close()
    for handler in root.handlers:
        if _is_console(handler) and not any(
            isinstance(item, _BelowError) for item in handler.filters
        ):
            handler.addFilter(_BelowError())

    logging.Formatter.converter = time.gmtime
    errors = logging.FileHandler(path, encoding="utf-8")
    errors.setLevel(logging.ERROR)
    errors.setFormatter(_ErrorFormatter(ERROR_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))
    errors.dtrack_error_handler = True
    root.addHandler(errors)
    _current_error_log = path
    return path


def current_error_log() -> Path | None:
    return _current_error_log


def log_exception_once(logger: logging.Logger, message: str, error: Exception) -> None:
    if getattr(error, "dtrack_logged", False):
        return
    logger.exception(message)
    try:
        error.dtrack_logged = True
    except (AttributeError, TypeError):
        pass
