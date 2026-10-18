from importlib import import_module

from pair_recon_left.common.runtime.run_logging import get_logger

log = get_logger("debug")


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
    debugger = import_module("ipdb")
    return debugger.runcall(callback)
