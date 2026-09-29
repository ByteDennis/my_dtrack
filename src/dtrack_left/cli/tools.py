from __future__ import annotations

import functools
import logging
import sys

import click
from dotenv import find_dotenv, load_dotenv

from ..backends import databricks
from ..backends.sources import source_from_config
from ..backends.store import fetch_report
from .options import CONTEXT_SETTINGS, config_option, emit_json, log_level_option, store_option

log = logging.getLogger("dtrack.left.debug")
dbx_log = logging.getLogger("dtrack.left.dbx")


# >>> dtrack fetch-report: download a finished run's report set from the report store <<< #
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


# >>> dtrack doctor: preflight every Left source and show what the Right side will read <<< #
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


# >>> dtrack debug [--attach [PORT]] -- <command ...>: the same CLI under a debugger <<< #
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
        raise click.UsageError("give the command to debug, e.g. dtrack debug -- row-check ...")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return run_under_debugger(main, argv, attach=attach)


# >>> --profile for every dbx command: the constant default, overridden by DTRACK_DBX_PROFILE <<< #
profile_option = click.option(
    "--profile",
    default=databricks.DEFAULT_PROFILE,
    show_default=True,
    envvar="DTRACK_DBX_PROFILE",
    help="~/.databrickscfg profile",
)
wait_option = click.option(
    "--wait/--no-wait", default=False, help="block until the change finishes"
)
timeout_option = click.option(
    "--timeout", default=20, show_default=True, help="minutes to wait with --wait"
)


# >>> hand the command a WorkspaceClient; SDK, auth and timeout errors become {"error"} with exit 1 <<< #
def with_workspace(func):
    @functools.wraps(func)
    def wrapper(*args, profile, **kwargs):
        try:
            return func(*args, w=databricks.workspace(profile), **kwargs)
        except (OSError, RuntimeError, ValueError, ImportError) as exc:
            dbx_log.error("dbx %s failed: %s", func.__name__.removeprefix("dbx_"), exc)
            return {"error": f"{type(exc).__name__}: {exc}", "exit_code": 1}

    return wrapper


# >>> dtrack dbx: operator helpers for Databricks; the pipeline commands never import this <<< #
@click.group(
    "dbx",
    help="Databricks helpers: login, compute, warehouse, sql, runs (needs dtrack[dbx])",
    context_settings=CONTEXT_SETTINGS,
)
def dbx_group():
    load_dotenv(find_dotenv(usecwd=True))


# >>> dtrack dbx login: open the workspace in the browser and confirm the identity <<< #
@dbx_group.command(
    "login",
    help="open the workspace (SSO) and show who is logged in",
    context_settings=CONTEXT_SETTINGS,
)
@profile_option
@click.option(
    "--open/--no-open", "open_browser", default=True, show_default=True, help="open the browser"
)
@log_level_option
@emit_json
@with_workspace
def dbx_login(w, open_browser):
    return databricks.login(w, open_browser=open_browser)


# >>> dtrack dbx compute status|start|stop: one all-purpose cluster <<< #
@dbx_group.command(
    "compute", help="status | start | stop a cluster", context_settings=CONTEXT_SETTINGS
)
@click.argument(
    "action", type=click.Choice(["status", "start", "stop"]), default="status", required=False
)
@profile_option
@click.option("--cluster-id", required=True, envvar="DTRACK_DBX_CLUSTER_ID", help="cluster id")
@wait_option
@timeout_option
@log_level_option
@emit_json
@with_workspace
def dbx_compute(w, action, cluster_id, wait, timeout):
    if action == "status":
        return databricks.compute_status(w, cluster_id)
    change = databricks.compute_start if action == "start" else databricks.compute_stop
    return change(w, cluster_id, wait=wait, timeout_minutes=timeout)


# >>> dtrack dbx warehouse status|start|stop: one SQL warehouse <<< #
@dbx_group.command(
    "warehouse", help="status | start | stop a SQL warehouse", context_settings=CONTEXT_SETTINGS
)
@click.argument(
    "action", type=click.Choice(["status", "start", "stop"]), default="status", required=False
)
@profile_option
@click.option(
    "--warehouse-id", required=True, envvar="DTRACK_DBX_WAREHOUSE_ID", help="warehouse id"
)
@wait_option
@timeout_option
@log_level_option
@emit_json
@with_workspace
def dbx_warehouse(w, action, warehouse_id, wait, timeout):
    if action == "status":
        return databricks.warehouse_status(w, warehouse_id)
    change = databricks.warehouse_start if action == "start" else databricks.warehouse_stop
    return change(w, warehouse_id, wait=wait, timeout_minutes=timeout)


# >>> dtrack dbx sql: one statement (argument or --file) on a SQL warehouse <<< #
@dbx_group.command(
    "sql", help="run one SQL statement on a warehouse", context_settings=CONTEXT_SETTINGS
)
@click.argument("statement", required=False)
@click.option("--file", "sql_file", type=click.File(), help="read the statement from a file")
@profile_option
@click.option(
    "--warehouse-id", required=True, envvar="DTRACK_DBX_WAREHOUSE_ID", help="warehouse id"
)
@click.option("--limit", default=20, show_default=True, help="rows to print")
@log_level_option
@emit_json
@with_workspace
def dbx_sql(w, statement, sql_file, warehouse_id, limit):
    text = sql_file.read() if sql_file else statement
    if not text:
        raise click.UsageError("give a STATEMENT or --file")
    return databricks.run_sql(w, text, warehouse_id, limit=limit)


# >>> dtrack dbx runs: latest job runs, or one run with its tasks <<< #
@dbx_group.command(
    "runs", help="latest job runs, or one run and its tasks", context_settings=CONTEXT_SETTINGS
)
@profile_option
@click.option("--run-id", type=int, help="show this run and its tasks")
@click.option("--job-id", type=int, help="only runs of this job")
@click.option("--active", is_flag=True, help="only runs still in progress")
@click.option("--limit", default=10, show_default=True, help="runs to list")
@log_level_option
@emit_json
@with_workspace
def dbx_runs(w, run_id, job_id, active, limit):
    if run_id:
        return databricks.run_detail(w, run_id)
    return databricks.list_runs(w, limit=limit, job_id=job_id, active=active)
