from __future__ import annotations

import logging
import sys

import click

from ..backends.sources import source_from_config
from ..backends.store import fetch_report
from ..checks import rollback as rollbacks
from .config import load_config
from .options import CONTEXT_SETTINGS, config_option, emit_json, log_level_option, store_option

log = logging.getLogger("dtrack.left.debug")


@click.group(
    "tools",
    help="operator and diagnostic helpers",
    context_settings=CONTEXT_SETTINGS,
)
def tools_group():
    pass


# >>> dtrack tools fetch-report: download a finished run's report set from the report store <<< #
@click.command(
    "fetch-report",
    help="download a finished run's report set from the report store",
    context_settings=CONTEXT_SETTINGS,
)
@store_option("--report-s3", "DTRACK_REPORT_S3", "report location, s3://bucket/prefix")
@click.option("--out", required=True, type=click.Path(file_okay=False), help="local folder")
@click.option("--run-id", help="defaults to the run named by latest/pointer.json")
@log_level_option
@emit_json
def fetch_report_command(store, out, run_id):
    outcome = fetch_report(store, out, run_id=run_id)
    files = {name: str(path) for name, path in sorted(outcome["files"].items())}
    return {"run_id": outcome["run_id"], "files": files}


# >>> what the Databricks job will read for a pair; the Left side never connects to it <<< #
def describe_right(spec: dict) -> dict:
    kind = {"volume_raw": "files"}.get(spec.get("kind"), spec.get("kind") or "table")
    if kind == "files":
        target = f"read_files({spec.get('path')}, format={spec.get('format')})"
    elif kind == "query":
        target = " ".join(str(spec.get("query")).split())[:120]
    else:
        macro = str(spec.get("conn_macro") or "")
        prefix = [spec.get("catalog"), spec.get("schema")]
        prefix = prefix if any(prefix) else macro.split(".", 1) if "." in macro else []
        target = ".".join(str(part) for part in [*prefix, spec.get("table")] if part)
    patched = kind == "files" and "{src}" in str(spec.get("query") or "")
    return {"kind": kind, "target": target, "patched": patched}


# >>> dtrack tools doctor: preflight every Left source and show what the Right side will read <<< #
@click.command(
    "doctor",
    help="preflight every Left source; show each pair's Right relation",
    context_settings=CONTEXT_SETTINGS,
)
@config_option
@log_level_option
@emit_json
def doctor_command(config):
    results = {
        name: {
            **source_from_config(pair["left"]).preflight(),
            "right": describe_right(pair["right"]),
            "col_type_overrides": dict(pair.get("col_type_overrides") or {}),
        }
        for name, pair in config["pairs"].items()
        if not pair.get("skip")
    }
    return {**results, "exit_code": 0 if all(row["ok"] for row in results.values()) else 1}


# >>> dtrack tools rollback: delete one pair's signals so a stage is produced again (dry run unless --yes) <<< #
@click.command(
    "rollback",
    help="delete one pair's Stage 1/2 signals so they are produced again; dry run unless --yes",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--run",
    "location",
    required=True,
    envvar="DTRACK_S3",
    help="run folder: s3://bucket/.../runs/<run_id> or a local run folder",
)
@click.option("--pair", required=True, help="the pair to roll back")
@click.option(
    "--stage",
    type=click.Choice(rollbacks.STAGES),
    required=True,
    help="dead: retry dead units | col: redo Stage 2 | row: redo Stage 1 and 2",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    help="with --stage col: rebuild the manifest from this config (Stage 2 settings only)",
)
@click.option("--yes", "apply", is_flag=True, help="delete; without it only the plan is printed")
@log_level_option
@emit_json
def rollback_command(location, pair, stage, config_path, apply):
    config = load_config(config_path) if config_path else None
    store = rollbacks.open_location(location)
    outcome = rollbacks.rollback(store, pair, stage, config=config, apply=apply)
    if not apply:
        outcome["dry_run"] = "nothing deleted; add --yes (stop the Right job first)"
    elif outcome["failed"]:
        outcome["exit_code"] = 1
    return outcome


# >>> run a dtrack command under ipdb in the terminal, or debugpy waiting for a VS Code attach <<< #
def run_under_debugger(main, argv: list[str], *, attach: int | None = None) -> int:
    if attach:
        import debugpy

        debugpy.listen(("0.0.0.0", attach))
        log.info("debugpy listening on port %d; attach from VS Code, then continue", attach)
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
        log.exception("command raised; opening post-mortem")
        debugger.post_mortem(sys.exc_info()[2])
        return 1


# >>> dtrack tools debug [--attach [PORT]] -- <command ...>: the same CLI under a debugger <<< #
@click.command(
    "debug",
    help="run a dtrack command under ipdb, or wait for a VS Code attach",
    context_settings={
        **CONTEXT_SETTINGS,
        "ignore_unknown_options": True,
        "allow_interspersed_args": False,
    },
)
@click.option(
    "--attach",
    type=int,
    is_flag=False,
    flag_value=5678,
    default=None,
    help="wait for VS Code on this port (bare --attach = 5678) instead of ipdb",
)
@click.argument("argv", nargs=-1, type=click.UNPROCESSED)
def debug_command(attach, argv):
    from . import main

    argv = [item for item in argv if item != "--"]
    if not argv:
        raise click.UsageError(
            "give the command to debug, e.g. dtrack tools debug -- row-check ..."
        )
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return run_under_debugger(main, argv, attach=attach)
