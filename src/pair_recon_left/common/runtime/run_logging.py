from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

LOGGER_NAME = "pair_recon"
LEFT_LOGGER_NAME = f"{LOGGER_NAME}.left"
CONSOLE_LOG_FORMAT = "%(asctime)s.%(msecs)03d%(tz)s %(levelname)s %(name)s: %(message)s"
LEFT_LOG_FORMAT = "%(asctime)s.%(msecs)03d%(tz)s %(levelname)s %(threadName)s %(name)s: %(message)s"
_current_left_log: Path | None = None


class _ConsoleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "console_silent", False)


class ZonedFormatter(logging.Formatter):
    def __init__(self, fmt: str, timezone_name: str | None = None):
        super().__init__(fmt, datefmt="%Y-%m-%dT%H:%M:%S")
        self.timezone = (
            ZoneInfo(timezone_name) if timezone_name else datetime.now().astimezone().tzinfo
        )

    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, self.timezone).strftime(
            datefmt or self.datefmt
        )

    def format(self, record):
        record.tz = datetime.fromtimestamp(record.created, self.timezone).strftime("%z")
        return super().format(record)


def get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(LOGGER_NAME if not name else f"{LOGGER_NAME}.{name}")


def get_left_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(LEFT_LOGGER_NAME if not name else f"{LEFT_LOGGER_NAME}.{name}")


def configure_console_logging(level: str, timezone_name: str | None = None) -> logging.Logger:
    logger = get_logger()
    for handler in list(logger.handlers):
        if getattr(handler, "pair_recon_console_handler", False):
            logger.removeHandler(handler)
            handler.close()

    resolved = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(resolved)
    handler.setFormatter(ZonedFormatter(CONSOLE_LOG_FORMAT, timezone_name))
    handler.addFilter(_ConsoleFilter())
    handler.pair_recon_console_handler = True
    logger.addHandler(handler)
    logger.setLevel(resolved)
    logger.propagate = False
    return logger


def configure_left_logging(path: str | Path, timezone_name: str | None = None) -> Path:
    global _current_left_log

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    logger = get_left_logger()
    for handler in list(logger.handlers):
        if getattr(handler, "pair_recon_left_handler", False):
            logger.removeHandler(handler)
            handler.close()

    handler = logging.FileHandler(target, mode="a", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(ZonedFormatter(LEFT_LOG_FORMAT, timezone_name))
    handler.pair_recon_left_handler = True
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    _current_left_log = target
    return target


def current_left_log() -> Path | None:
    return _current_left_log
