from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from .col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from .config import load_config
from .debug import run_under_debugger
from .fetch import fetch_report
from .row_check import run_row_check
from .s3 import S3Store
from .sources import source_from_config


def _store(args) -> S3Store:
    return S3Store(args.s3, kms_key_id=getattr(args, "kms_key_id", None))


def row_check_command(args) -> int:
    config = load_config(args.config)
    run_id, documents = run_row_check(
        config, _store(args), run_id=args.run_id, pair_names=args.pair, pair_workers=args.workers
    )
    print(json.dumps({"run_id": run_id, "pairs": len(documents)}))
    return 0


def col_check_command(args) -> int:
    store = _store(args)
    manifests = wait_for_manifests(
        store, timeout=args.wait_timeout, poll_interval=args.poll_interval, pair_names=args.pair
    )
    for manifest in manifests:
        publish_fixed_keys(store, manifest, args.work_dir)
    runtime = LeftRuntime(args.work_dir, store)
    published = ColCheckPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=args.pull_workers,
        stat_workers=args.stat_workers,
        wip=args.wip,
    ).run(manifests)
    print(json.dumps({"published_units": published, "pairs": [m["pair"] for m in manifests]}))
    return 0


def fetch_report_command(args) -> int:
    store = S3Store(args.report_s3, kms_key_id=getattr(args, "kms_key_id", None))
    outcome = fetch_report(store, args.out, run_id=args.run_id)
    print(
        json.dumps(
            {
                "run_id": outcome["run_id"],
                "files": {name: str(path) for name, path in sorted(outcome["files"].items())},
            }
        )
    )
    return 0


def doctor_command(args) -> int:
    config = load_config(args.config)
    results = {
        name: source_from_config(pair["left"]).preflight()
        for name, pair in config["pairs"].items()
        if not pair.get("skip")
    }
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(row["ok"] for row in results.values()) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dtrack")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--log-level", default="INFO")
    commands = parser.add_subparsers(dest="command", required=True)

    row_check = commands.add_parser(
        "row-check",
        help="row check: count the Left side per Partition and hand off to the Right side",
        parents=[common],
    )
    row_check.add_argument("--config", required=True)
    row_check.add_argument("--s3", required=True)
    row_check.add_argument("--run-id")
    row_check.add_argument("--pair", action="append")
    row_check.add_argument("--workers", type=int, default=4)
    row_check.add_argument("--kms-key-id")
    row_check.set_defaults(func=row_check_command)

    col_check = commands.add_parser(
        "col-check",
        help="col check: wait for the Right side's manifests, then publish left-side Units",
        parents=[common],
    )
    col_check.add_argument("--s3", required=True)
    col_check.add_argument("--pair", action="append")
    col_check.add_argument("--work-dir", default=".dtrack-work/col_check")
    col_check.add_argument("--pull-workers", type=int, default=2)
    col_check.add_argument("--stat-workers", type=int, default=2)
    col_check.add_argument("--wip", type=int, default=200)
    col_check.add_argument("--wait-timeout", type=float, default=1800)
    col_check.add_argument("--poll-interval", type=float, default=3)
    col_check.add_argument("--kms-key-id")
    col_check.set_defaults(func=col_check_command)

    fetch = commands.add_parser(
        "fetch-report",
        help="download a finished run's report set from the report store",
        parents=[common],
    )
    fetch.add_argument("--report-s3", required=True)
    fetch.add_argument("--out", required=True)
    fetch.add_argument("--run-id", help="defaults to the run named by latest/pointer.json")
    fetch.add_argument("--kms-key-id")
    fetch.set_defaults(func=fetch_report_command)

    doctor = commands.add_parser("doctor", help="verify Left-side source access", parents=[common])
    doctor.add_argument("--config", required=True)
    doctor.set_defaults(func=doctor_command)
    return parser


# >>> dtrack debug [--attach PORT] <subcommand ...>: same CLI, under ipdb or a VS Code attach <<< #
def debug_command(args) -> int:
    return run_under_debugger(main, args.rest, attach=args.attach)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["debug"]:
        parser = argparse.ArgumentParser(prog="dtrack debug")
        parser.add_argument(
            "--attach",
            type=int,
            nargs="?",
            const=5678,
            help="listen for VS Code on this port (default 5678)",
        )
        parser.add_argument("rest", nargs=argparse.REMAINDER)
        args = parser.parse_args(argv[1:])
        rest = [item for item in args.rest if item != "--"]
        if not rest:
            parser.error(
                "give the subcommand to debug, e.g. dtrack debug -- row-check --config c.json --s3 ..."
            )
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr
        )
        return debug_command(argparse.Namespace(attach=args.attach, rest=rest))
    args = build_parser().parse_args(argv)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )
    return args.func(args)
