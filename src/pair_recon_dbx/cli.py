from __future__ import annotations

import argparse
import json
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
    delete = workspace_commands.add_parser("delete", aliases=("rm",))
    delete.add_argument("source", nargs="+")
    delete.add_argument("-r", "-R", "--recursive", action="store_true")
    delete.add_argument("-f", "--force", action="store_true")
    delete.add_argument("--dry-run", action="store_true")
    add_profile(delete)
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
    sql.add_argument("statement", nargs="?")
    sql.add_argument("--file", type=Path)
    sql.add_argument(
        "--warehouse-id",
        default=env_value("DBX_WAREHOUSE_ID"),
    )
    sql.add_argument("--limit", type=int, default=20)
    add_profile(sql)
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


def execute(args) -> dict:
    workspace = runtime.client(args.profile)
    if args.command == "login":
        return runtime.login(workspace, open_browser=not args.no_open)
    if args.command == "workspace":
        if args.workspace_command in {"delete", "rm"}:
            return workspace_ops.delete(
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
        warehouse_id = require(args.warehouse_id, "--warehouse-id")
        statement = args.file.read_text(encoding="utf-8") if args.file else args.statement
        return runtime.run_sql(
            workspace, require(statement, "STATEMENT or --file"), warehouse_id, limit=args.limit
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
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return exit_code

    return run_debugger(args.debug, command)
