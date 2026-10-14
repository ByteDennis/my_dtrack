from __future__ import annotations

import logging
import time
from pathlib import Path

LEFT_LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(threadName)s %(name)s: %(message)s"
_current_left_log: Path | None = None


def configure_left_logging(path: str | Path) -> Path:
    """Append every Left INFO-or-higher record to one run-scoped file."""
    global _current_left_log

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("dtrack.left")
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
    if logger.getEffectiveLevel() > logging.INFO:
        logger.setLevel(logging.INFO)
    _current_left_log = target
    return target


def current_left_log() -> Path | None:
    return _current_left_log
