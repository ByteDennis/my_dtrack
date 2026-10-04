from __future__ import annotations

import functools
import logging
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv

from ...backends import databricks
from ..options import CONTEXT_SETTINGS, emit_json, log_level_option

dbx_log = logging.getLogger("dtrack.left.dbx")


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
    help="Databricks helpers: login, workspace, compute, warehouse, sql, runs; s3 credentials",
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


# >>> dtrack dbx workspace: files and notebooks under /Users, /Shared and other Workspace paths <<< #
@dbx_group.group(
    "workspace",
    help="manage Workspace files and notebooks",
    context_settings=CONTEXT_SETTINGS,
)
def workspace_group():
    pass


# >>> dtrack dbx workspace upload: copy one local file or directory tree into the Workspace <<< #
@workspace_group.command(
    "upload",
    help="upload a local file or folder to a Workspace path",
    context_settings=CONTEXT_SETTINGS,
)
@click.argument("source", type=click.Path(exists=True, readable=True, path_type=Path))
@click.argument("target", required=False)
@profile_option
@click.option("--overwrite", is_flag=True, help="replace existing Workspace objects")
@click.option("--dry-run", is_flag=True, help="show planned remote paths without uploading")
@log_level_option
@emit_json
@with_workspace
def dbx_workspace_upload(w, source, target, overwrite, dry_run):
    return databricks.upload_workspace(w, source, target, overwrite=overwrite, dry_run=dry_run)


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
