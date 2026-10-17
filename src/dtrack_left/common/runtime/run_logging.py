from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

LOGGER_NAME = "dtrack"
LEFT_LOGGER_NAME = f"{LOGGER_NAME}.left"
CONSOLE_LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s"
LEFT_LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(threadName)s %(name)s: %(message)s"
_current_left_log: Path | None = None


def get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(LOGGER_NAME if not name else f"{LOGGER_NAME}.{name}")


def get_left_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(LEFT_LOGGER_NAME if not name else f"{LEFT_LOGGER_NAME}.{name}")


def configure_console_logging(level: str) -> logging.Logger:
    """Configure one idempotent stderr handler on the stable dtrack logger tree."""
    logger = get_logger()
    for handler in list(logger.handlers):
        if getattr(handler, "dtrack_console_handler", False):
            logger.removeHandler(handler)
            handler.close()

    logging.Formatter.converter = time.gmtime
    resolved = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(resolved)
    handler.setFormatter(logging.Formatter(CONSOLE_LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))
    handler.dtrack_console_handler = True
    logger.addHandler(handler)
    logger.setLevel(resolved)
    logger.propagate = False
    return logger


def configure_left_logging(path: str | Path) -> Path:
    """Append every Left INFO-or-higher record to one run-scoped file."""
    global _current_left_log

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    logger = get_left_logger()
    for handler in list(logger.handlers):
        if getattr(handler, "dtrack_left_handler", False):
            logger.removeHandler(handler)
            handler.close()

    logging.Formatter.converter = time.gmtime
    handler = logging.FileHandler(target, mode="a", encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LEFT_LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S"))
    handler.dtrack_left_handler = True
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    _current_left_log = target
    return target


def current_left_log() -> Path | None:
    return _current_left_log
