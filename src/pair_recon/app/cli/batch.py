import argparse
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

from ..config import load_defaults, save_defaults

DEFAULTS = Path(".pair_recon.yaml")
EXECUTED = {
    "passed",
    "different",
    "incomplete",
    "empty",
    "left_only",
    "right_only",
    "left_complete",
    "awaiting_sample",
    "finished",
}


# >>> the state printed by the run's final JSON result, None when the run did not get that far <<< #
def result_state(output: str) -> str | None:
    lines = output.splitlines()
    for index in range(len(lines) - 1, -1, -1):
        if lines[index].strip() == "{":
            try:
                return json.loads("\n".join(lines[index:])).get("state")
            except ValueError:
                continue
    return None


# >>> executed when the run exited 0 with a known state; everything else is cancelled or failed <<< #
def classify(returncode: int, state: str | None) -> str:
    if returncode == 130:
        return "cancelled"
    if returncode == 0 and state in EXECUTED:
        return "executed"
    return "failed"


def run_pair(pair: str, defaults: dict, flags: list[str], *, runner=None) -> dict:
    folder = Path(tempfile.mkdtemp(prefix="pair_recon-batch-"))
    save_defaults(folder / ".pair_recon.yaml", {**defaults, "pair": pair})
    command = [
        sys.executable,
        "-m",
        "pair_recon",
        "--defaults",
        str(folder / ".pair_recon.yaml"),
        *flags,
    ]
    started = time.monotonic()
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    returncode, output = (runner or _run)(command)
    state = result_state(output)
    return {
        "pair": pair,
        "started_at": started_at,
        "elapsed_s": round(time.monotonic() - started, 1),
        "returncode": returncode,
        "state": state,
        "outcome": classify(returncode, state),
    }


def _run(command: list[str]) -> tuple[int, str]:
    lines = []
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace"
    ) as process:
        for line in process.stdout:
            sys.stdout.write(line)
            lines.append(line)
    return process.returncode, "".join(lines)


def summary_table(rows: list[dict]) -> str:
    header = "| pair | outcome | state | returncode | elapsed_s |\n|---|---|---|---:|---:|\n"
    return header + "".join(
        f"| {row['pair']} | {row['outcome']} | {row['state'] or '-'} | {row['returncode']} | {row['elapsed_s']} |\n"
        for row in rows
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pair_recon batch", allow_abbrev=False)
    parser.add_argument("pairs", nargs="+")
    parser.add_argument("--defaults", dest="defaults_path", type=Path, default=DEFAULTS)
    parser.add_argument("--summary", type=Path, default=Path("batch_summary.json"))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--recheck", action="store_true")
    parser.add_argument("--last-failed", action="store_true")
    parser.add_argument("--earlystop", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    defaults = load_defaults(args.defaults_path)
    flags = ["--log-level", args.log_level] + [
        f"--{name.replace('_', '-')}"
        for name in ("force", "recheck", "last_failed", "earlystop")
        if getattr(args, name)
    ]
    rows = []
    try:
        for pair in args.pairs:
            print(f"Starting {pair}...", flush=True)
            rows.append(run_pair(pair, defaults, flags))
            print(
                f"Finished {pair}: {rows[-1]['outcome']} ({rows[-1]['state'] or '-'}), {rows[-1]['elapsed_s']}s",
                flush=True,
            )
            args.summary.write_text(
                json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8"
            )
    except KeyboardInterrupt:
        print("Cancelled")
        return 130
    print(summary_table(rows))
    return 1 if any(row["outcome"] != "executed" for row in rows) else 0
