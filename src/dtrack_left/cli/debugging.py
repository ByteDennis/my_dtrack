from __future__ import annotations

import sys

from ..common.run_logging import get_left_logger

log = get_left_logger("debug")


def run_under_debugger(main, argv: list[str], *, attach: int | None = None) -> int:
    if attach:
        import debugpy

        debugpy.listen(("0.0.0.0", attach))
        log.info("debugpy listening on port %d; attach from VS Code, then continue", attach)
        debugpy.wait_for_client()
        debugpy.breakpoint()
        return main(argv)
    try:
        import ipdb
    except ImportError as exc:
        raise RuntimeError("ipdb is required for --debug ipdb") from exc
    log.info("entering ipdb; `c` to run %s, `n`/`s` to step, `q` to quit", argv)
    try:
        return ipdb.runcall(main, argv)
    except Exception:
        log.exception("command raised; opening ipdb post-mortem")
        ipdb.post_mortem(sys.exc_info()[2])
        return 1
