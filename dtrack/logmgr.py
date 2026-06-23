"""Unified logging: web API calls + CLI commands -> one rotating log file."""

import logging
import os
from logging.handlers import RotatingFileHandler

_logger = None


# >>> single log path; override with DTRACK_LOG, default ~/.dtrack/dtrack.log <<< #
def log_path():
    p = os.environ.get("DTRACK_LOG")
    if p:
        return os.path.expanduser(p)
    return os.path.join(os.path.expanduser("~"), ".dtrack", "dtrack.log")


# >>> shared 'dtrack' logger writing to the single log file; idempotent <<< #
def get_logger():
    global _logger
    if _logger is not None:
        return _logger
    lg = logging.getLogger("dtrack")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    if not any(isinstance(h, RotatingFileHandler) for h in lg.handlers):
        path = log_path()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fh = RotatingFileHandler(path, maxBytes=5_000_000, backupCount=3)
            fh.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
            lg.addHandler(fh)
        except Exception:
            pass
    _logger = lg
    return lg


# >>> also echo the shared logger to stderr (used by the web server) <<< #
def enable_console():
    lg = get_logger()
    if not any(isinstance(h, logging.StreamHandler)
               and not isinstance(h, RotatingFileHandler) for h in lg.handlers):
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S"))
        lg.addHandler(sh)
    return lg
