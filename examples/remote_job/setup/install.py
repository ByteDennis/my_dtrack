from __future__ import annotations

import argparse
import html
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
    alert_id: str,
    warehouse_id: str,
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
        "alert": {
            "name": f"{prefix}-alert",
            "max_concurrent_runs": 1,
            "queue": {"enabled": True},
            "timeout_seconds": 600,
            "parameters": [{"name": "run_id", "default": ""}],
            "tasks": [
                _task(
                    "context",
                    f"{folder}/alert_context_notebook",
                    {"schema": schema, "run_id": "{{job.parameters.run_id}}"},
                    cluster_id,
                ),
                {
                    "task_key": "notify",
                    "depends_on": [{"task_key": "context"}],
                    "alert_task": {
                        "alert_id": alert_id,
                        "warehouse_id": warehouse_id,
                    },
                },
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


def alert_ddl(schema: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {schema}.dtrack_alert_context (
  context_key STRING NOT NULL, run_id STRING NOT NULL, updated_at TIMESTAMP NOT NULL)"""


def alert_sql(schema: str) -> str:
    return f"""WITH context AS (
  SELECT run_id FROM {schema}.dtrack_alert_context
  WHERE context_key = 'current'
  QUALIFY row_number() OVER (ORDER BY updated_at DESC) = 1),
latest AS (
  SELECT snapshot.*
  FROM {schema}.dtrack_progress_snapshot AS snapshot
  INNER JOIN context USING (run_id)
  QUALIFY row_number() OVER (
    PARTITION BY snapshot.run_id, snapshot.pair_name ORDER BY snapshot.snapshot_ts DESC) = 1),
rollup AS (
  SELECT context.run_id, count(latest.run_id) AS snapshot_rows,
         max(snapshot_ts) AS snapshot_at, max(run_root) AS run_uri,
         count_if(pair_name <> '*') AS pair_count,
         count_if(collector_error IS NOT NULL) AS collector_errors,
         count_if(pair_name <> '*' AND (run_state IS NULL OR run_state <> 'complete')) AS unfinished_pairs,
         count_if(pair_name <> '*' AND (gate_passed = false OR coalesce(units_dead, 0) > 0)) AS failed_pairs,
         count_if(pair_name <> '*' AND (
           coalesce(row_days_count_diff, 0) + coalesce(row_days_left_only, 0)
             + coalesce(row_days_right_only, 0) + coalesce(units_diff, 0) > 0)) AS diff_pairs,
         sum(coalesce(row_days_total, 0)) AS row_partitions,
         sum(coalesce(row_days_count_diff, 0) + coalesce(row_days_left_only, 0)
           + coalesce(row_days_right_only, 0)) AS row_mismatches,
         sum(coalesce(units_expected, 0)) AS units_expected,
         sum(coalesce(units_compared, 0)) AS units_compared,
         sum(coalesce(units_dead, 0)) AS units_dead,
         sum(coalesce(units_diff, 0)) AS differing_units
  FROM context LEFT JOIN latest ON latest.run_id = context.run_id
  GROUP BY context.run_id)
SELECT 1 AS notify, run_id,
       CASE WHEN snapshot_rows = 0 THEN 'NO_DATA'
            WHEN collector_errors > 0 THEN 'ERROR'
            WHEN unfinished_pairs > 0 THEN 'INCOMPLETE'
            WHEN failed_pairs > 0 THEN 'FAILED'
            WHEN diff_pairs > 0 THEN 'DIFF'
            ELSE 'MATCH' END AS result,
       pair_count, row_partitions, row_mismatches,
       units_expected, units_compared, units_dead, differing_units,
       snapshot_at, concat(run_uri, '/report/index.html') AS report_uri
FROM rollup"""


def alert_definition(
    schema: str,
    workspace_dir: str,
    warehouse_id: str,
    display_name: str,
    recipients: list[str],
    dashboard_url: str,
) -> Any:
    from databricks.sdk.service.sql import (
        AlertEvaluationState,
        AlertV2,
        AlertV2Evaluation,
        AlertV2Notification,
        AlertV2Operand,
        AlertV2OperandColumn,
        AlertV2OperandValue,
        AlertV2Subscription,
        ComparisonOperator,
        CronSchedule,
        SchedulePauseStatus,
    )

    dashboard_link = html.escape(dashboard_url, quote=True)
    return AlertV2(
        display_name=display_name,
        parent_path=workspace_dir,
        query_text=alert_sql(schema),
        warehouse_id=warehouse_id,
        evaluation=AlertV2Evaluation(
            source=AlertV2OperandColumn(name="notify", display="notify"),
            comparison_operator=ComparisonOperator.GREATER_THAN,
            empty_result_state=AlertEvaluationState.ERROR,
            threshold=AlertV2Operand(value=AlertV2OperandValue(double_value=0.0)),
            notification=AlertV2Notification(
                notify_on_ok=False,
                retrigger_seconds=1,
                subscriptions=[AlertV2Subscription(user_email=value) for value in recipients],
            ),
        ),
        schedule=CronSchedule(
            quartz_cron_schedule="0 0 0 * * ?",
            timezone_id="UTC",
            pause_status=SchedulePauseStatus.PAUSED,
        ),
        custom_summary="[DTrack] reconciliation result — {{ALERT_STATUS}}",
        custom_description=(
            "<h2>DTrack reconciliation result</h2>"
            "<p>The collector committed the latest snapshot for this run.</p>"
            "{{QUERY_RESULT_TABLE}}"
            f'<p><a href="{dashboard_link}">Open the live dashboard</a></p>'
        ),
    )


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


def _upsert_alert(workspace: Any, definition: Any) -> str:
    existing = next(
        (
            value
            for value in workspace.alerts_v2.list_alerts()
            if value.display_name == definition.display_name
            and value.parent_path == definition.parent_path
        ),
        None,
    )
    if existing:
        saved = workspace.alerts_v2.update_alert(
            str(existing.id),
            definition,
            update_mask=(
                "display_name,query_text,warehouse_id,evaluation,schedule,"
                "custom_summary,custom_description"
            ),
        )
    else:
        saved = workspace.alerts_v2.create_alert(definition)
    if not saved.id:
        raise RuntimeError("Databricks did not return an alert id")
    log.info("alert ready: name=%s alert_id=%s", definition.display_name, saved.id)
    return str(saved.id)


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
        (EXAMPLE / "src" / "alert_context_notebook.py", f"{target}/alert_context_notebook.py"),
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

    for statement in [
        *progress.ddl(args.schema),
        alert_ddl(args.schema),
        *progress.views(args.schema),
    ]:
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
    recipients = _alert_recipients(workspace, args.alert_email)
    alert_id = _upsert_alert(
        workspace,
        alert_definition(
            args.schema,
            args.workspace_dir,
            args.warehouse_id,
            args.alert_name,
            recipients,
            dashboard_url,
        ),
    )
    specs = job_specs(
        args.workspace_dir,
        args.right_runs,
        args.schema,
        alert_id=alert_id,
        warehouse_id=args.warehouse_id,
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
        "alert_job_id": jobs["alert"],
        "alert_id": alert_id,
        "alert_recipients": recipients,
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


def _alert_recipients(workspace: Any, configured: list[str] | None) -> list[str]:
    recipients = [value.strip() for value in configured or [] if value.strip()]
    if not recipients:
        current = workspace.current_user.me().user_name
        if current:
            recipients = [str(current)]
    if not recipients or any(
        "@" not in address or address.startswith("@") or address.endswith("@")
        for address in recipients
    ):
        raise ValueError("--alert-email must name a valid Databricks workspace user email")
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
    parser.add_argument("--alert-name", default="dtrack reconciliation result")
    parser.add_argument(
        "--alert-email", action="append", help="Databricks user email; repeat as needed"
    )
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
    configured_recipients = os.getenv("DTRACK_ALERT_EMAIL", "")
    if not args.alert_email and configured_recipients:
        args.alert_email = [
            value.strip() for value in re.split(r"[,;]", configured_recipients) if value.strip()
        ]
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
