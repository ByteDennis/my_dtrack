from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from pair_recon.core.debug import run_debugger

from . import dispatch as operations
from . import snapshot


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="pair_recon gitlab")
    root.add_argument("--debug", nargs="?", const="ipdb", choices=("ipdb", "attach"))
    commands = root.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("project")
    create.add_argument("--group")
    create.add_argument("--description")
    create.add_argument("--host", default=os.getenv("GITLAB_HOST"))
    create.add_argument("--dry-run", action="store_true")
    plan = commands.add_parser("plan")
    add_snapshot(plan)
    publish = commands.add_parser("dispatch")
    publish.add_argument("repo")
    publish.add_argument("--branch", default="main")
    publish.add_argument("--message")
    publish.add_argument("--host", default=os.getenv("GITLAB_HOST", "gitlab.com"))
    publish.add_argument("--dry-run", action="store_true")
    add_snapshot(publish)
    return root


def add_snapshot(target) -> None:
    target.add_argument("--scope", choices=snapshot.SCOPES, default="both")
    target.add_argument("--root", type=Path, default=Path.cwd())
    target.add_argument("--ignore", type=Path, default=Path("pair_recon.ignore"))


def execute(args) -> dict:
    if args.command == "create":
        return operations.create_project(
            args.project,
            group=args.group,
            description=args.description,
            host=args.host,
            dry_run=args.dry_run,
        )
    ignore = args.ignore if args.ignore.is_absolute() else args.root / args.ignore
    if args.command == "plan":
        return snapshot.plan(args.root, args.scope, ignore)
    return operations.dispatch(
        args.root,
        args.repo,
        scope=args.scope,
        ignore=args.ignore,
        branch=args.branch,
        message=args.message,
        host=args.host,
        dry_run=args.dry_run,
    )


def main(argv=None) -> int:
    load_dotenv(find_dotenv(filename=".pair_recon.env", usecwd=True))
    args = parser().parse_args(argv)

    def command() -> int:
        try:
            result, exit_code = execute(args), 0
        except (OSError, RuntimeError, ValueError) as exc:
            result, exit_code = {"error": f"{type(exc).__name__}: {exc}"}, 1
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return exit_code

    return run_debugger(args.debug, command)
