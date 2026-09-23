from __future__ import annotations

import logging
import sys

log = logging.getLogger("dtrack.left.debug")


# >>> run any dtrack subcommand under a debugger: ipdb in the terminal, or debugpy waiting for a VS Code attach <<< #
def run_under_debugger(main, argv: list[str], *, attach: int | None = None) -> int:
    if attach:
        import debugpy

        debugpy.listen(("0.0.0.0", attach))
        log.info(
            "debugpy listening on port %d; attach from VS Code (Python: Remote Attach), then continue",
            attach,
        )
        debugpy.wait_for_client()
        debugpy.breakpoint()
        return main(argv)
    try:
        import ipdb as debugger
    except ImportError:
        import pdb as debugger
    log.info("entering %s; `c` to run %s, `n`/`s` to step, `q` to quit", debugger.__name__, argv)
    try:
        return debugger.runcall(main, argv)
    except Exception:
        log.exception("subcommand raised; opening post-mortem")
        debugger.post_mortem(sys.exc_info()[2])
        return 1
