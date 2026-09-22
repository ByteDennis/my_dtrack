from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from .app import run
from .targets import CsvAnchorTarget


# >>> local stand-in for the Databricks notebook: same loop, CSV anchor instead of spark <<< #
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="dtrack_cloud")
    parser.add_argument(
        "--work",
        required=True,
        help="a run folder, or the runs root (newest unfinished run is picked)",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--report")
    parser.add_argument("--right-csv-dir", required=True)
    parser.add_argument("--out", default=".dtrack-cloud/report")
    parser.add_argument("--work-dir", default=".dtrack-cloud/work")
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
        args.work,
        factory,
        report=args.report,
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
