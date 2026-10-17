from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

EXAMPLE = Path(__file__).resolve().parents[1]
ROOT = EXAMPLE.parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(EXAMPLE / "src"))

from dashboard import build as build_dashboard

from dtrack_left.backends import databricks
from dtrack_left.common.run_logging import configure_console_logging, get_logger
from dtrack_right import progress

NAME = "example.remote_job.setup"
log = get_logger(NAME)
DEFAULT_STATE = EXAMPLE / ".setup.json"
SCHEMA_PATTERN = re.compile(r"[A-Za-z_]\w*\.[A-Za-z_]\w*")


def _task(key: str, notebook: str, parameters: dict[str, str], cluster_id: str | None) -> dict:
    task = {
        "task_key": key,
        "notebook_task": {
            "notebook_path": notebook,
            "base_parameters": parameters,
        },
    }
    if cluster_id:
        task["existing_cluster_id"] = cluster_id
    return task


def job_specs(
    workspace_dir: str,
    right_runs: str,
    schema: str,
    *,
    cluster_id: str | None,
    prefix: str,
) -> dict[str, dict[str, Any]]:
    folder = workspace_dir.rstrip("/") + "/dtrack_right"
    return {
        "right": {
            "name": f"{prefix}-right",
            "max_concurrent_runs": 3,
            "queue": {"enabled": True},
            "timeout_seconds": 0,
            "tasks": [
                _task(
                    "right",
                    f"{folder}/notebook",
                    {"run_s3": right_runs, "run_id": "", "gate": "auto"},
                    cluster_id,
                )
            ],
        },
        "collector": {
            "name": f"{prefix}-progress-collector",
            "max_concurrent_runs": 1,
            "queue": {"enabled": True},
            "timeout_seconds": 600,
            "schedule": {
                "quartz_cron_expression": "0 0/15 * * * ?",
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED",
            },
            "tasks": [
                _task(
                    "collect",
                    f"{folder}/progress_notebook",
                    {"run_roots": right_runs, "schema": schema},
                    cluster_id,
                )
            ],
        },
    }


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def plan_sql(schema: str, pairs: list[str]) -> str:
    rows = ",\n".join(f"({_literal(pair)}, {_literal(pair)})" for pair in pairs)
    return f"""MERGE INTO {schema}.dtrack_pair_plan AS target
USING (SELECT * FROM VALUES
{rows}
AS source(pair_name, display_name)) AS source
ON target.pair_name = source.pair_name
WHEN MATCHED THEN UPDATE SET display_name = source.display_name, active = true,
  updated_by = current_user(), updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (pair_name, display_name, priority, active, updated_by, updated_at)
  VALUES (source.pair_name, source.display_name, 100, true, current_user(), current_timestamp())"""


def write_state(path: Path, values: dict[str, Any]) -> None:
    document = {"version": 1, **values}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _pairs(path: Path) -> list[str]:
    with path.open(encoding="utf-8") as handle:
        document = json.load(handle)
    pairs = document.get("pairs") if isinstance(document, dict) else None
    if not isinstance(pairs, dict) or not pairs:
        raise ValueError("config must contain a non-empty pairs object")
    return sorted(str(name) for name in pairs)


def _upsert_jobs(workspace: Any, specs: dict[str, dict[str, Any]]) -> dict[str, int]:
    existing = {job.settings.name: int(job.job_id) for job in workspace.jobs.list()}
    ids = {}
    for kind, spec in specs.items():
        if spec["name"] in existing:
            job_id = existing[spec["name"]]
            workspace.api_client.do(
                "POST",
                "/api/2.2/jobs/reset",
                body={"job_id": job_id, "new_settings": spec},
            )
        else:
            response = workspace.api_client.do("POST", "/api/2.2/jobs/create", body=spec)
            job_id = int(response["job_id"])
        ids[kind] = job_id
        log.info("job ready: kind=%s name=%s job_id=%s", kind, spec["name"], job_id)
    return ids


def _publish_dashboard(
    workspace: Any,
    schema: str,
    workspace_dir: str,
    warehouse_id: str,
    display_name: str,
) -> tuple[str, str]:
    from databricks.sdk.service.dashboards import Dashboard

    serialized = json.dumps(build_dashboard(schema), ensure_ascii=False)
    existing = next(
        (item for item in workspace.lakeview.list() if item.display_name == display_name),
        None,
    )
    value = Dashboard(
        display_name=display_name,
        parent_path=workspace_dir,
        serialized_dashboard=serialized,
        warehouse_id=warehouse_id,
    )
    if existing:
        saved = workspace.lakeview.update(str(existing.dashboard_id), value)
    else:
        saved = workspace.lakeview.create(value)
    workspace.lakeview.publish(
        str(saved.dashboard_id),
        warehouse_id=warehouse_id,
        embed_credentials=True,
    )
    url = f"{workspace.config.host.rstrip('/')}/sql/dashboardsv3/{saved.dashboard_id}"
    return str(saved.dashboard_id), url


def install(args: argparse.Namespace) -> dict[str, Any]:
    if not SCHEMA_PATTERN.fullmatch(args.schema):
        raise ValueError("--schema must be catalog.schema")
    if not args.workspace_dir.startswith("/") or ".." in Path(args.workspace_dir).parts:
        raise ValueError("--workspace-dir must be an absolute Databricks Workspace path")
    pairs = _pairs(args.config)
    workspace = databricks.workspace(args.profile)
    target = args.workspace_dir.rstrip("/") + "/dtrack_right"
    uploads = (
        (ROOT / "src" / "dtrack_right", target),
        (EXAMPLE / "src" / "progress_notebook.py", f"{target}/progress_notebook.py"),
    )
    for source, destination in uploads:
        result = databricks.upload_workspace(
            workspace,
            source,
            destination,
            overwrite=True,
        )
        if result["failed"]:
            raise RuntimeError(f"workspace upload failed: {result['failed']}")
        log.info("uploaded %s object(s) to %s", result["uploaded"], result["target"])

    for statement in [*progress.ddl(args.schema), *progress.views(args.schema)]:
        databricks.run_sql(workspace, statement, args.warehouse_id)
    databricks.run_sql(workspace, plan_sql(args.schema, pairs), args.warehouse_id)
    log.info("tables/views ready: schema=%s pairs=%s", args.schema, len(pairs))

    dashboard_id, dashboard_url = _publish_dashboard(
        workspace,
        args.schema,
        args.workspace_dir,
        args.warehouse_id,
        args.dashboard_name,
    )
    specs = job_specs(
        args.workspace_dir,
        args.right_runs,
        args.schema,
        cluster_id=args.cluster_id,
        prefix=args.job_prefix,
    )
    jobs = _upsert_jobs(workspace, specs)
    state = {
        "profile": args.profile,
        "workspace_dir": args.workspace_dir,
        "right_runs": args.right_runs,
        "schema": args.schema,
        "right_job_id": jobs["right"],
        "collector_job_id": jobs["collector"],
        "email_to": args.email_to,
        "dashboard_id": dashboard_id,
        "dashboard_url": dashboard_url,
        "aws_secret_scope": args.aws_secret_scope,
        "gate": "auto",
        "idle_timeout": 900,
        "wait_timeout": 1800,
        "keep_complete_days": 1,
    }
    write_state(args.state, state)
    return {"state": str(args.state), "pairs": pairs, **state}


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


def email_recipients(configured: list[str]) -> list[str]:
    recipients = [
        address.strip()
        for value in configured
        for address in re.split(r"[,;]", value)
        if address.strip()
    ]
    if not recipients or any(
        "@" not in address or address.startswith("@") or address.endswith("@")
        for address in recipients
    ):
        raise ValueError("--email-to must name at least one valid email address")
    return list(dict.fromkeys(recipients))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time Databricks setup for the remote-job example."
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--right-runs")
    parser.add_argument("--workspace-dir")
    parser.add_argument("--schema")
    parser.add_argument("--warehouse-id")
    parser.add_argument("--profile", default=os.getenv("DTRACK_DBX_PROFILE", "corp"))
    parser.add_argument("--cluster-id", default=os.getenv("DTRACK_DBX_CLUSTER_ID") or None)
    parser.add_argument("--aws-secret-scope", default=os.getenv("DTRACK_LIVE_AWS_SECRET_SCOPE", ""))
    parser.add_argument("--job-prefix", default="dtrack-example")
    parser.add_argument("--dashboard-name", default="dtrack pair progress")
    parser.add_argument("--email-to", action="append", help="Outlook recipient; repeat as needed")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument(
        "--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO"
    )
    args = parser.parse_args(argv)
    args.config = Path(
        _value(str(args.config) if args.config else None, "DTRACK_CONFIG", "Config JSON")
    )
    args.right_runs = _value(
        args.right_runs,
        "DTRACK_RIGHT_RUNS",
        "Right-visible runs root (/Volumes/.../runs or s3://.../runs)",
    )
    args.workspace_dir = _value(
        args.workspace_dir,
        "DTRACK_WORKSPACE_DIR",
        "Databricks Workspace folder",
        "/Workspace/Shared/dtrack-example",
    )
    args.schema = _value(
        args.schema, "DTRACK_PROGRESS_SCHEMA", "Progress catalog.schema", "main.dtrack"
    )
    args.warehouse_id = _value(
        args.warehouse_id,
        "DTRACK_DBX_WAREHOUSE_ID",
        "SQL warehouse id",
    )
    configured_recipients = args.email_to or [
        _value(None, "DTRACK_EMAIL_TO", "Outlook recipient(s), comma-separated")
    ]
    args.email_to = email_recipients(configured_recipients)
    return args


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        configure_console_logging(args.log_level)
        print(json.dumps(install(args), indent=2, ensure_ascii=False))
        return 0
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        log.error("%s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
