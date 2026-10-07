from __future__ import annotations

import functools
import logging

import click

from ..backends.sources import source_from_config
from ..backends.store import fetch_report, validate_run_id
from ..checks import rollback as rollbacks
from .config import load_config
from .debugging import run_under_debugger
from .options import CONTEXT_SETTINGS, config_option, emit_json, log_level_option

log = logging.getLogger("dtrack.left.debug")


@click.group(
    "tools",
    help="operator and diagnostic helpers",
    context_settings=CONTEXT_SETTINGS,
)
def tools_group():
    pass


# >>> dtrack tools fetch-report: download report/ from one run folder <<< #
@click.command(
    "fetch-report",
    help="download report/ from a finished run folder",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--run-s3",
    "runs_uri",
    required=True,
    envvar="DTRACK_RUN_S3",
    help="stable runs root: s3://bucket/.../runs or a local/Volume path",
)
@click.option("--run-id", help="run below --run-s3; defaults to the newest completed run")
@click.option("--out", required=True, type=click.Path(file_okay=False), help="local folder")
@log_level_option
@emit_json
def fetch_report_command(runs_uri, run_id, out):
    from dtrack_right.backends.store import open_store

    try:
        outcome = fetch_report(open_store(runs_uri), out, run_id=run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
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


# >>> dtrack tools rollback: safely reset one, several, or all pairs (dry run unless --yes) <<< #
@click.command(
    "rollback",
    help="delete selected pairs' Stage 1/2 signals so they are produced again; dry run unless --yes",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--run-s3",
    "runs_uri",
    required=True,
    envvar="DTRACK_RUN_S3",
    help="stable runs root (S3, Volume, or local directory)",
)
@click.option("--run-id", required=True, envvar="DTRACK_RUN_ID", help="exact run to modify")
@click.option(
    "--pair",
    "pairs",
    multiple=True,
    required=True,
    help="pair to roll back (repeatable or comma-separated); use 'all' alone for every run pair",
)
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
    help="col: rebuild manifests; row: immediately rerun Left row-check with this config",
)
@click.option("--yes", "apply", is_flag=True, help="delete; without it only the plan is printed")
@log_level_option
@emit_json
def rollback_command(runs_uri, run_id, pairs, stage, config_path, apply):
    try:
        run_id = validate_run_id(run_id)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from None
    location = runs_uri.rstrip("/\\") + "/" + run_id
    config = load_config(config_path) if config_path else None
    store = rollbacks.open_location(location)
    try:
        outcome = rollbacks.rollback_many(store, pairs, stage, config=config, apply=apply)
    except (KeyError, ValueError) as exc:
        raise click.UsageError(str(exc)) from None
    if not apply:
        outcome["dry_run"] = "nothing deleted; add --yes (stop the Right job first)"
    elif outcome["failed"]:
        outcome["exit_code"] = 1
    return outcome


# >>> invoke whichever root Click tree owns `tools debug`, without swallowing runtime exceptions <<< #
def _command_debug_main(command, prog_name: str, argv: list[str]) -> int:
    code = command.main(
        args=argv,
        prog_name=prog_name,
        standalone_mode=False,
    )
    return code if isinstance(code, int) else 0


# >>> dtrack tools debug [--attach [PORT]] -- <command ...>: any command in this root CLI <<< #
@click.command(
    "debug",
    help="run any dtrack command under ipdb, or wait for a VS Code attach",
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
@click.pass_context
def debug_command(ctx, attach, argv):
    argv = [item for item in argv if item != "--"]
    if not argv:
        raise click.UsageError("give the command to debug, e.g. dtrack tools debug -- row-check ...")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    root = ctx.find_root()
    target = functools.partial(_command_debug_main, root.command, root.info_name or "dtrack")
    return run_under_debugger(target, argv, attach=attach)
