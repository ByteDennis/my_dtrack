"""Unified logging: web API + CLI + full pipeline detail -> one rotating log file."""

import logging
import os
from logging.handlers import RotatingFileHandler

_logger = None


# >>> single log path; override with DTRACK_LOG, default <cwd>/output/run.log <<< #
def log_path():
    p = os.environ.get("DTRACK_LOG")
    if p:
        return os.path.expanduser(p)
    return os.path.join(os.getcwd(), "output", "run.log")


# >>> file log level; DTRACK_LOG_LEVEL overrides, default DEBUG (capture everything) <<< #
def _level():
    name = os.environ.get("DTRACK_LOG_LEVEL", "DEBUG").upper()
    return getattr(logging, name, logging.DEBUG)


# >>> shared 'dtrack' logger writing the single log file; idempotent <<< #
def get_logger():
    global _logger
    if _logger is not None:
        return _logger
    lg = logging.getLogger("dtrack")
    lg.setLevel(logging.DEBUG)
    lg.propagate = False
    if not any(isinstance(h, RotatingFileHandler) for h in lg.handlers):
        path = log_path()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fh = RotatingFileHandler(path, maxBytes=20_000_000, backupCount=3)
            fh.setLevel(_level())
            fh.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
                "%Y-%m-%d %H:%M:%S"))
            lg.addHandler(fh)
        except Exception:
            pass
    _logger = lg
    return lg


# >>> also echo the shared logger to stderr at INFO (used by the web server) <<< #
def enable_console():
    lg = get_logger()
    if not any(isinstance(h, logging.StreamHandler)
               and not isinstance(h, RotatingFileHandler) for h in lg.handlers):
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S"))
        lg.addHandler(sh)
    return lg
