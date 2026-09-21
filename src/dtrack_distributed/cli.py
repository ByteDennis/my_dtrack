from __future__ import annotations

import argparse
import json
from pathlib import Path

from .adapters.checkpoint.local import LocalCheckpoint
from .adapters.kickstart.databricks_jobs import DatabricksJobsKickstarter
from .adapters.sources.factory import source_from_config
from .adapters.storage.s3 import S3Store
from .adapters.targets.csv_anchor import CsvAnchorTarget
from .adapters.targets.databricks import DatabricksTarget
from .config import load_config
from .workflows.consumer import Consumer, ConsumerRuntime
from .workflows.producer import ProducerPipeline, ProducerRuntime
from .workflows.stage1 import run_stage1


def _store(args) -> S3Store:
    return S3Store(args.s3, kms_key_id=getattr(args, "kms_key_id", None))


def stage1_command(args) -> int:
    config = load_config(args.config)
    kickstarter = DatabricksJobsKickstarter(args.job_id) if args.job_id else None
    run_id, documents, manifests = run_stage1(
        config,
        _store(args),
        run_id=args.run_id,
        gate=args.gate,
        pair_names=args.pair,
        kickstarter=kickstarter,
        pair_workers=args.workers,
    )
    print(json.dumps({"run_id": run_id, "pairs": len(documents), "released": len(manifests)}))
    return 0


def produce_command(args) -> int:
    store = _store(args)
    manifests = [
        value
        for key, value in store.list_json("manifest/")
        if key != "manifest/_CLOSED" and (not args.pair or value["pair"] in args.pair)
    ]
    runtime = ProducerRuntime(args.work_dir)
    published = ProducerPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=args.pull_workers,
        stat_workers=args.stat_workers,
        wip=args.wip,
    ).run(manifests)
    print(json.dumps({"published_units": published}))
    return 0


def consume_command(args) -> int:
    store = _store(args)
    target_factory = DatabricksTarget
    if args.right_csv_dir:
        root = Path(args.right_csv_dir)

        def target_factory(config):
            path = root / f"{config.get('table', 'right')}.csv"
            return CsvAnchorTarget({**config, "path": str(path)})

    runtime = ConsumerRuntime(target_factory, args.work_dir)
    checkpoint = LocalCheckpoint(args.checkpoint)
    consumer = Consumer(
        store,
        fetch_right=runtime.fetch_right,
        checkpoint=checkpoint,
        prefetch_depth=args.prefetch_depth,
        max_attempts=args.max_attempts,
        idle_timeout=args.idle_timeout,
        heartbeat_timeout=args.heartbeat_timeout,
        poll_interval=args.poll_interval,
    )
    rows = consumer.run()
    consumer.write_csv(args.output)
    print(json.dumps({"results": len(rows), "output": args.output}))
    return 0


def doctor_command(args) -> int:
    config = load_config(args.config)
    results = {}
    for name, pair in config["pairs"].items():
        if pair.get("skip"):
            continue
        results[name] = {
            "left": source_from_config(pair["left"]).preflight(),
            "right": DatabricksTarget(pair["right"]).preflight(),
        }
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(side["left"]["ok"] and side["right"]["ok"] for side in results.values()) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dtrack-distributed")
    commands = parser.add_subparsers(dest="command", required=True)

    stage1 = commands.add_parser("stage1", help="run preflight and exact Partition Barrier")
    stage1.add_argument("--config", required=True)
    stage1.add_argument("--s3", required=True)
    stage1.add_argument("--run-id")
    stage1.add_argument("--gate", default="auto")
    stage1.add_argument("--pair", action="append")
    stage1.add_argument("--job-id", type=int)
    stage1.add_argument("--workers", type=int, default=4)
    stage1.add_argument("--kms-key-id")
    stage1.set_defaults(func=stage1_command)

    produce = commands.add_parser("produce", help="publish left-side Units")
    produce.add_argument("--s3", required=True)
    produce.add_argument("--pair", action="append")
    produce.add_argument("--work-dir", default=".dtrack-work/producer")
    produce.add_argument("--pull-workers", type=int, default=2)
    produce.add_argument("--stat-workers", type=int, default=2)
    produce.add_argument("--wip", type=int, default=200)
    produce.add_argument("--kms-key-id")
    produce.set_defaults(func=produce_command)

    consume = commands.add_parser("consume", help="compare Units against Databricks")
    consume.add_argument("--s3", required=True)
    consume.add_argument("--output", default="compare_col.csv")
    consume.add_argument("--checkpoint", default=".dtrack-work/checkpoint.json")
    consume.add_argument("--work-dir", default=".dtrack-work/consumer")
    consume.add_argument("--right-csv-dir", help="local correctness anchor; not production")
    consume.add_argument("--prefetch-depth", type=int, default=3)
    consume.add_argument("--max-attempts", type=int, default=3)
    consume.add_argument("--idle-timeout", type=float, default=900)
    consume.add_argument("--heartbeat-timeout", type=float, default=180)
    consume.add_argument("--poll-interval", type=float, default=2)
    consume.add_argument("--kms-key-id")
    consume.set_defaults(func=consume_command)

    doctor = commands.add_parser("doctor", help="verify source and Databricks access")
    doctor.add_argument("--config", required=True)
    doctor.set_defaults(func=doctor_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
