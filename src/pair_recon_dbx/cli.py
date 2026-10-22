from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pair_recon.app.config import env_value, load_environment
from pair_recon.core.debug import debugging, run_debugger

from . import runtime
from . import workspace as workspace_ops


def add_profile(parser) -> None:
    parser.add_argument(
        "--profile",
        default=env_value("DBX_PROFILE", default=runtime.DEFAULT_PROFILE),
    )


def add_wait(parser) -> None:
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=20)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="pair_recon dbx")
    root.add_argument("--debug", nargs="?", const="console", choices=("console", "attach"))
    commands = root.add_subparsers(dest="command", required=True)
    login = commands.add_parser("login")
    add_profile(login)
    login.add_argument("--no-open", action="store_true")
    workspace = commands.add_parser("workspace")
    workspace_commands = workspace.add_subparsers(dest="workspace_command", required=True)
    for name in ("upload", "download"):
        command = workspace_commands.add_parser(name)
        command.add_argument("source", type=Path if name == "upload" else str)
        command.add_argument("target", nargs="?", type=Path if name == "download" else str)
        command.add_argument("--overwrite", action="store_true")
        command.add_argument("--dry-run", action="store_true")
        add_profile(command)
    ls = workspace_commands.add_parser("ls")
    ls.add_argument("source", nargs="?")
    ls.add_argument("-r", "-R", "--recursive", action="store_true")
    add_profile(ls)
    cp = workspace_commands.add_parser("cp")
    cp.add_argument("source")
    cp.add_argument("target")
    cp.add_argument("-r", "-R", "--recursive", action="store_true")
    cp.add_argument("-f", "--force", action="store_true")
    cp.add_argument("--dry-run", action="store_true")
    add_profile(cp)
    mv = workspace_commands.add_parser("mv")
    mv.add_argument("source")
    mv.add_argument("target")
    mv.add_argument("-f", "--force", action="store_true")
    mv.add_argument("--dry-run", action="store_true")
    add_profile(mv)
    rm = workspace_commands.add_parser("rm")
    rm.add_argument("source", nargs="+")
    rm.add_argument("-r", "-R", "--recursive", action="store_true")
    rm.add_argument("-f", "--force", action="store_true")
    rm.add_argument("--dry-run", action="store_true")
    add_profile(rm)
    compute = commands.add_parser("compute")
    compute.add_argument("action", nargs="?", choices=("status", "start", "stop"), default="status")
    compute.add_argument("--cluster-id", default=env_value("DBX_CLUSTER_ID"))
    add_wait(compute)
    add_profile(compute)
    warehouse = commands.add_parser("warehouse")
    warehouse.add_argument(
        "action", nargs="?", choices=("status", "start", "stop"), default="status"
    )
    warehouse.add_argument(
        "--warehouse-id",
        default=env_value("DBX_WAREHOUSE_ID"),
    )
    add_wait(warehouse)
    add_profile(warehouse)
    sql = commands.add_parser("sql")
    source = sql.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--unity",
        metavar="TABLE_OR_SQL",
        help="peek at catalog.schema.table or execute Unity Catalog SQL",
    )
    source.add_argument(
        "--glue",
        metavar="TABLE_OR_SQL",
        help="peek at database.table or execute Hive metastore SQL",
    )
    sql.add_argument("--limit", type=positive_limit, default=5, help="maximum rows (default: 5)")
    sql.set_defaults(
        profile=env_value("DBX_PROFILE", default=runtime.DEFAULT_PROFILE),
        warehouse_id=env_value("DBX_WAREHOUSE_ID"),
    )
    runs = commands.add_parser("runs")
    runs.add_argument("--run-id", type=int)
    runs.add_argument("--job-id", type=int)
    runs.add_argument("--active", action="store_true")
    runs.add_argument("--limit", type=int, default=10)
    add_profile(runs)
    return root


def require(value, name: str):
    if value in (None, ""):
        raise ValueError(f"{name} is required")
    return value


def positive_limit(value: str) -> int:
    limit = int(value)
    if limit < 1:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return limit


def execute(args) -> dict:
    workspace = runtime.client(args.profile)
    if args.command == "login":
        return runtime.login(workspace, open_browser=not args.no_open)
    if args.command == "workspace":
        if args.workspace_command == "ls":
            return workspace_ops.ls(workspace, args.source, recursive=args.recursive)
        if args.workspace_command == "cp":
            return workspace_ops.copy(
                workspace,
                args.source,
                args.target,
                recursive=args.recursive,
                force=args.force,
                dry_run=args.dry_run,
            )
        if args.workspace_command == "mv":
            return workspace_ops.move(
                workspace,
                args.source,
                args.target,
                force=args.force,
                dry_run=args.dry_run,
            )
        if args.workspace_command == "rm":
            return workspace_ops.rm(
                workspace,
                args.source,
                recursive=args.recursive,
                force=args.force,
                dry_run=args.dry_run,
            )
        operation = (
            workspace_ops.upload if args.workspace_command == "upload" else workspace_ops.download
        )
        return operation(
            workspace,
            args.source,
            args.target,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    if args.command == "compute":
        cluster_id = require(args.cluster_id, "--cluster-id")
        if args.action == "status":
            return runtime.compute_status(workspace, cluster_id)
        operation = runtime.compute_start if args.action == "start" else runtime.compute_stop
        return operation(workspace, cluster_id, wait=args.wait, timeout_minutes=args.timeout)
    if args.command == "warehouse":
        warehouse_id = require(args.warehouse_id, "--warehouse-id")
        if args.action == "status":
            return runtime.warehouse_status(workspace, warehouse_id)
        operation = runtime.warehouse_start if args.action == "start" else runtime.warehouse_stop
        return operation(workspace, warehouse_id, wait=args.wait, timeout_minutes=args.timeout)
    if args.command == "sql":
        warehouse_id = require(args.warehouse_id, "DBX_WAREHOUSE_ID")
        source = "unity" if args.unity is not None else "glue"
        statement = runtime.sql_statement(getattr(args, source), source, args.limit)
        return runtime.run_sql(
            workspace,
            statement,
            warehouse_id,
            limit=args.limit,
            catalog="hive_metastore" if source == "glue" else None,
            source=source,
        )
    if args.run_id is not None:
        return runtime.run_detail(workspace, args.run_id)
    return runtime.list_runs(workspace, limit=args.limit, job_id=args.job_id, active=args.active)


def main(argv=None) -> int:
    load_environment()
    args = parser().parse_args(argv)
    raise_errors = debugging(args.debug)

    def command() -> int:
        try:
            result = dict(execute(args))
            exit_code = int(result.pop("exit_code", 0))
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            if raise_errors:
                raise
            result, exit_code = {"error": f"{type(exc).__name__}: {exc}"}, 1
        if args.command == "sql" and not exit_code:
            print(runtime.format_sql_result(result))
        elif args.command == "sql":
            print(result["error"], file=sys.stderr)
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return exit_code

    return run_debugger(args.debug, command)
