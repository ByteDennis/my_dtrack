from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from .adapters.sources.factory import source_from_config
from .adapters.storage.s3 import S3Store
from .config import load_config
from .workflows.fetch import fetch_report
from .workflows.producer import (
    ProducerPipeline,
    ProducerRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from .workflows.stage1 import run_stage1_left


def _store(args) -> S3Store:
    return S3Store(args.s3, kms_key_id=getattr(args, "kms_key_id", None))


def stage1_command(args) -> int:
    config = load_config(args.config)
    run_id, documents = run_stage1_left(
        config, _store(args), run_id=args.run_id, pair_names=args.pair, pair_workers=args.workers
    )
    print(json.dumps({"run_id": run_id, "pairs": len(documents)}))
    return 0


def produce_command(args) -> int:
    store = _store(args)
    manifests = wait_for_manifests(
        store, timeout=args.wait_timeout, poll_interval=args.poll_interval, pair_names=args.pair
    )
    for manifest in manifests:
        publish_fixed_keys(store, manifest, args.work_dir)
    runtime = ProducerRuntime(args.work_dir, store)
    published = ProducerPipeline(
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
    parser = argparse.ArgumentParser(prog="dtrack-distributed")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--log-level", default="INFO")
    commands = parser.add_subparsers(dest="command", required=True)

    stage1 = commands.add_parser(
        "stage1",
        help="count the Left side per Partition and hand off to the Right side",
        parents=[common],
    )
    stage1.add_argument("--config", required=True)
    stage1.add_argument("--s3", required=True)
    stage1.add_argument("--run-id")
    stage1.add_argument("--pair", action="append")
    stage1.add_argument("--workers", type=int, default=4)
    stage1.add_argument("--kms-key-id")
    stage1.set_defaults(func=stage1_command)

    produce = commands.add_parser(
        "produce",
        help="wait for the Right side's manifests, then publish left-side Units",
        parents=[common],
    )
    produce.add_argument("--s3", required=True)
    produce.add_argument("--pair", action="append")
    produce.add_argument("--work-dir", default=".dtrack-work/producer")
    produce.add_argument("--pull-workers", type=int, default=2)
    produce.add_argument("--stat-workers", type=int, default=2)
    produce.add_argument("--wip", type=int, default=200)
    produce.add_argument("--wait-timeout", type=float, default=1800)
    produce.add_argument("--poll-interval", type=float, default=3)
    produce.add_argument("--kms-key-id")
    produce.set_defaults(func=produce_command)

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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )
    return args.func(args)
