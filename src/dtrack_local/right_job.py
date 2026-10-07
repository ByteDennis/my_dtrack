from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from dtrack_right.job import run

from .csv_target import CsvAnchorTarget


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m dtrack_local.right_job")
    parser.add_argument(
        "--run-s3",
        dest="run_s3",
        required=True,
        help="stable runs root; --run-id is appended (newest unfinished by default)",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--right-csv-dir", required=True)
    parser.add_argument("--out", default=".dtrack-right/report")
    parser.add_argument("--work-dir", default=".dtrack-right/work")
    parser.add_argument("--gate", default="auto")
    parser.add_argument("--poll-interval", type=float, default=2)
    parser.add_argument("--idle-timeout", type=float, default=900)
    parser.add_argument("--heartbeat-timeout", type=float, default=180)
    parser.add_argument("--wait-timeout", type=float, default=1800)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )
    factory = lambda config: CsvAnchorTarget(config, args.right_csv_dir, Path(args.work_dir))
    outcome = run(
        args.run_s3,
        factory,
        run_id=args.run_id,
        out_dir=args.out,
        gate=args.gate,
        poll_interval=args.poll_interval,
        idle_timeout=args.idle_timeout,
        heartbeat_timeout=args.heartbeat_timeout,
        wait_timeout=args.wait_timeout,
    )
    print(
        json.dumps(
            {
                "results": outcome["results"],
                "status": outcome["summary"]["status"],
                "published": outcome["published"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
