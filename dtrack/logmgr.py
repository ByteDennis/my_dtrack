
import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

_logger = None

_BRK_LOCK = threading.Lock()

_BREAK_ON = None

# >>> arm the break-on-message debugger (verbatim substring; set via `dtrack --msg`) <<< #
def set_break_on(pattern):
    global _BREAK_ON
    _BREAK_ON = pattern or None

# >>> nearest frame outside logging/logmgr = the original log call site <<< #
def _call_site_frame():
    lg_dir = os.path.dirname(logging.__file__)
    here = os.path.abspath(__file__)
    f = sys._getframe(2)
    while f:
        fn = os.path.abspath(f.f_code.co_filename)
        if fn != here and not fn.startswith(lg_dir):
            return f
        f = f.f_back
    return sys._getframe(2)

# >>> stop a live debugger at the log call site; survives _quiet_extract's stdout/stderr redirect <<< #
def _enter_debugger(msg):
    if not _BRK_LOCK.acquire(blocking=False):
        return
    try:
        frame = _call_site_frame()
        sys.__stderr__.write(
            f"\n[dtrack] --msg matched -> "
            f"{frame.f_code.co_filename}:{frame.f_lineno}\n"
            f"[dtrack] message: {msg}\n")
        if "debugpy" in sys.modules:
            import debugpy
            debugpy.breakpoint()
            return
        if sys.stdout is sys.__stdout__:
            try:
                import ipdb
                ipdb.set_trace(frame)
                return
            except ImportError:
                pass
        import pdb
        pdb.Pdb(stdin=sys.__stdin__, stdout=sys.__stdout__).set_trace(frame)
    finally:
        _BRK_LOCK.release()

# >>> break into the debugger when the --msg text appears verbatim (case-insensitive) in a captured message <<< #
class _BreakOnFilter(logging.Filter):
    def filter(self, record):
        pat = _BREAK_ON
        if not pat:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if pat.lower() in msg.lower():
            _enter_debugger(msg)
        return True

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
    if not any(isinstance(f, _BreakOnFilter) for f in lg.filters):
        lg.addFilter(_BreakOnFilter())
    if not any(isinstance(h, RotatingFileHandler) for h in lg.handlers):
        path = log_path()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fh = RotatingFileHandler(path, maxBytes=20_000_000, backupCount=3,
                                     encoding="utf-8")
            fh.setLevel(_level())
            fh.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
                "%Y-%m-%d %H:%M:%S"))
            lg.addHandler(fh)
        except Exception:
            pass
    _logger = lg
    return lg

# >>> write a thick separator + title marking a new major call/recipe step <<< #
def banner(title):
    lg = get_logger()
    bar = "=" * 78
    lg.info(bar)
    lg.info("==== %s", title)
    lg.info(bar)

# >>> log a swallowed exception with context so try/except blocks stay traceable <<< #
def log_exc(context, exc, level="warning"):
    lg = get_logger()
    getattr(lg, level, lg.warning)("%s: %s: %s", context, type(exc).__name__, exc)

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
