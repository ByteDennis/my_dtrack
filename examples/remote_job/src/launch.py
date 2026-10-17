from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

from outlook_mail import require_outlook, send_report

from dtrack_left.backends import databricks
from dtrack_left.backends.store import S3Store, validate_run_id
from dtrack_left.checks.row_check import make_run_id
from dtrack_left.common.run_logging import configure_console_logging, get_logger

NAME = "example.remote_job"
log = get_logger(NAME)
EXAMPLE = Path(__file__).resolve().parents[1]
DEFAULT_STATE = EXAMPLE / ".setup.json"


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        document = json.load(handle)
    if not isinstance(document, dict):
        raise TypeError(f"JSON root must be an object: {path}")
    return document


def right_parameters(state: dict[str, Any], run_id: str) -> dict[str, str]:
    return {
        "run_s3": str(state["right_runs"]),
        "run_id": run_id,
        "gate": str(state.get("gate", "auto")),
        "idle_timeout": str(state.get("idle_timeout", 900)),
        "wait_timeout": str(state.get("wait_timeout", 1800)),
        "aws_secret_scope": str(state.get("aws_secret_scope", "")),
    }


def collector_parameters(state: dict[str, Any]) -> dict[str, str]:
    return {
        "run_roots": str(state["right_runs"]),
        "schema": str(state["schema"]),
        "keep_complete_days": str(state.get("keep_complete_days", 1)),
    }


def _pair_args(pairs: list[str]) -> list[str]:
    return [value for pair in pairs for value in ("--pair", pair)]


def left_commands(document: dict[str, Any], args: argparse.Namespace) -> list[list[str]]:
    base = [sys.executable, "-m", "dtrack"]
    config = str(args.config)
    shared = [
        "--config",
        config,
        "--run-s3",
        args.run_s3,
        "--run-id",
        args.run_id,
        *_pair_args(args.pairs),
        "--log-level",
        args.log_level,
    ]
    doctor = [*base, "tools", "doctor", "--config", config]
    if int(document.get("version") or 1) == 2:
        left = [
            *base,
            "left-run",
            *shared,
            "--workers",
            str(args.workers),
            "--work-dir",
            str(args.work_dir),
            "--pull-workers",
            str(args.pull_workers),
        ]
        return [doctor, left]
    row = [*base, "row-check", *shared, "--workers", str(args.workers)]
    col = [
        *base,
        "col-check",
        "--config",
        config,
        "--run-s3",
        args.run_s3,
        "--run-id",
        args.run_id,
        *_pair_args(args.pairs),
        "--work-dir",
        str(args.work_dir),
        "--pull-workers",
        str(args.pull_workers),
        "--wait-timeout",
        str(args.wait_timeout),
        "--poll-interval",
        str(args.poll_interval),
        "--log-level",
        args.log_level,
    ]
    return [doctor, row, col]


def _run_left(document: dict[str, Any], args: argparse.Namespace) -> None:
    for command in left_commands(document, args):
        log.info("starting Left step: %s", command[3])
        subprocess.run(command, check=True)


def _result(run: Any, label: str) -> None:
    result = getattr(getattr(run, "state", None), "result_state", None)
    value = getattr(result, "value", result)
    if value != "SUCCESS":
        message = getattr(getattr(run, "state", None), "state_message", None)
        raise RuntimeError(f"{label} finished with {value or 'unknown'}: {message or ''}".rstrip())


def _refresh(workspace: Any, state: dict[str, Any], timeout: float) -> int:
    wait = workspace.jobs.run_now(
        int(state["collector_job_id"]),
        notebook_params=collector_parameters(state),
    )
    log.info("collector started: run_id=%s", wait.run_id)
    completed = wait.result(timeout=timedelta(minutes=timeout))
    _result(completed, "collector")
    return int(wait.run_id)


def _send_email(
    state: dict[str, Any],
    run_id: str,
    run_uri: str,
    error: str | None = None,
) -> dict[str, Any]:
    recipients = list(state.get("email_to") or [])
    if not recipients:
        raise ValueError("setup state has no email_to; rerun the one-time setup")
    result = send_report(
        S3Store(run_uri),
        run_id=run_id,
        run_uri=run_uri,
        dashboard_url=str(state.get("dashboard_url") or ""),
        recipients=recipients,
        error=error,
    )
    log.info(
        "Outlook email sent: status=%s recipients=%s attachments=%s",
        result["status"],
        len(recipients),
        len(result["attachments"]),
    )
    return result


def run(args: argparse.Namespace) -> dict[str, Any]:
    state = load_json(args.state)
    document = load_json(args.config)
    args.run_id = validate_run_id(args.run_id or make_run_id(document))
    run_uri = f"{args.run_s3.rstrip('/')}/{args.run_id}"
    require_outlook()
    workspace = databricks.workspace(str(state.get("profile") or "corp"))
    right = workspace.jobs.run_now(
        int(state["right_job_id"]),
        notebook_params=right_parameters(state, args.run_id),
    )
    log.info("Right started: job_id=%s run_id=%s", state["right_job_id"], right.run_id)
    try:
        _run_left(document, args)
        completed = right.result(timeout=timedelta(minutes=args.remote_timeout))
        _result(completed, "Right")
    except BaseException as failure:
        log.exception("run failed; cancelling the Right run started by this launcher")
        try:
            workspace.jobs.cancel_run(int(right.run_id))
        except Exception as exc:  # noqa: BLE001 - cleanup must not hide the original failure
            log.error("could not cancel Right run %s: %s", right.run_id, exc)
        try:
            _refresh(workspace, state, args.collector_timeout)
        except Exception as exc:  # noqa: BLE001 - cleanup must not hide the original failure
            log.error("could not snapshot the failed run: %s", exc)
        try:
            _send_email(state, args.run_id, run_uri, error=str(failure))
        except Exception as exc:  # noqa: BLE001 - cleanup must not hide the original failure
            log.error("could not email the failed run: %s", exc)
        raise

    try:
        collector_run_id = _refresh(workspace, state, args.collector_timeout)
    except BaseException as failure:
        log.exception("collector failed after Right completed")
        try:
            _send_email(state, args.run_id, run_uri, error=str(failure))
        except Exception as exc:  # noqa: BLE001 - cleanup must not hide the original failure
            log.error("could not email the collector failure: %s", exc)
        raise
    email = _send_email(state, args.run_id, run_uri)
    outcome = {
        "run_id": args.run_id,
        "run": run_uri,
        "right_run_id": int(right.run_id),
        "collector_run_id": collector_run_id,
        "email_status": email["status"],
        "email_to": list(state.get("email_to") or []),
        "email_attachments": email["attachments"],
        "dashboard_url": state.get("dashboard_url"),
    }
    log.info("run complete: %s", json.dumps(outcome, ensure_ascii=False))
    return outcome


def _value(value: str | None, env: str, prompt: str, default: str | None = None) -> str:
    resolved = value or os.getenv(env)
    if resolved:
        return resolved
    suffix = f" [{default}]" if default else ""
    entered = input(f"{prompt}{suffix}: ").strip()
    if entered:
        return entered
    if default:
        return default
    raise ValueError(f"{prompt} is required")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start local Left work and the remote Right job, then refresh the dashboard view."
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--run-s3")
    parser.add_argument("--run-id")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--pair", dest="pairs", action="append", default=[])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--pull-workers", type=int, default=2)
    parser.add_argument("--work-dir", type=Path, default=Path(".dtrack-work/col_check"))
    parser.add_argument("--wait-timeout", type=float, default=1800)
    parser.add_argument("--poll-interval", type=float, default=3)
    parser.add_argument("--remote-timeout", type=float, default=60, help="Right timeout in minutes")
    parser.add_argument(
        "--collector-timeout", type=float, default=20, help="collector timeout in minutes"
    )
    parser.add_argument(
        "--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO"
    )
    args = parser.parse_args(argv)
    args.config = Path(
        _value(str(args.config) if args.config else None, "DTRACK_CONFIG", "Config JSON")
    )
    args.run_s3 = _value(args.run_s3, "DTRACK_RUN_S3", "Left runs root (s3://.../runs)")
    return args


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        configure_console_logging(args.log_level)
        print(json.dumps(run(args), indent=2, ensure_ascii=False))
        return 0
    except (OSError, TypeError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
