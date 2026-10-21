import sys
from importlib import import_module

from pair_recon_left.common.runtime.run_logging import get_logger

log = get_logger("debug")


def debugging(mode=None) -> bool:
    return mode is not None or sys.gettrace() is not None


def run_debugger(mode, callback):
    if mode is None:
        return callback()
    if mode == "attach":
        debugger = import_module("debugpy")
        debugger.listen(("0.0.0.0", 5678))
        log.info("debugpy listening on port 5678; attach from VS Code")
        debugger.wait_for_client()
        debugger.breakpoint()
        return callback()
    if mode != "console":
        raise ValueError(f"unknown debug mode: {mode}")
    debugger = import_module("ipdb")
    try:
        return callback()
    except Exception:
        debugger.post_mortem(sys.exc_info()[2])
        raise
