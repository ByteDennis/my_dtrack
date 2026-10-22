from __future__ import annotations

import re
import time
import webbrowser
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Any

DEFAULT_PROFILE = "corp"
RUNNING_STATEMENT = {"PENDING", "RUNNING"}
SQL_PREFIX = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|VALUES)\b", re.IGNORECASE)


def client(profile: str):
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ImportError("pair_recon_dbx requires databricks-sdk") from exc
    return WorkspaceClient(profile=profile)


def value(item: Any) -> Any:
    return getattr(item, "value", item)


def stamp(millis: int | None) -> str | None:
    return (
        datetime.fromtimestamp(millis / 1000, UTC).isoformat(timespec="seconds") if millis else None
    )


def elapsed(millis: int | None, now: float | None = None) -> str | None:
    if not millis:
        return None
    minutes = int(((now or time.time()) - millis / 1000) // 60)
    return f"{minutes // 60}h{minutes % 60:02d}m"


def login(workspace, *, open_browser: bool = True, opener=webbrowser.open) -> dict:
    opened = bool(opener(workspace.config.host)) if open_browser else False
    user = workspace.current_user.me()
    return {
        "host": workspace.config.host,
        "profile": workspace.config.profile,
        "auth_type": workspace.config.auth_type,
        "user": user.user_name,
        "browser_opened": opened,
    }


def compute_status(workspace, cluster_id: str, *, now: float | None = None) -> dict:
    cluster = workspace.clusters.get(cluster_id)
    state = value(cluster.state)
    running = state in {"RUNNING", "RESIZING"}
    since = cluster.last_restarted_time or cluster.start_time
    autoscale = cluster.autoscale
    driver = cluster.driver
    return {
        "cluster_id": cluster_id,
        "name": cluster.cluster_name,
        "state": state,
        "message": cluster.state_message or None,
        "spark_version": cluster.spark_version,
        "driver_node_type": cluster.driver_node_type_id or cluster.node_type_id,
        "worker_node_type": cluster.node_type_id,
        "workers": f"{autoscale.min_workers}-{autoscale.max_workers}"
        if autoscale
        else cluster.num_workers,
        "driver_instance_id": driver.instance_id if driver else None,
        "worker_instance_ids": [node.instance_id for node in cluster.executors or []],
        "running_since": stamp(since) if running else None,
        "uptime": elapsed(since, now) if running else None,
        "terminated_at": None if running else stamp(cluster.terminated_time),
        "termination_reason": value(getattr(cluster.termination_reason, "code", None)),
        "autotermination_minutes": cluster.autotermination_minutes,
        "tags": dict(cluster.default_tags or {}),
    }


def compute_start(workspace, cluster_id: str, *, wait=False, timeout_minutes=20) -> dict:
    state = value(workspace.clusters.get(cluster_id).state)
    action = (
        "already running"
        if state in {"RUNNING", "RESIZING", "PENDING", "RESTARTING"}
        else "starting"
    )
    if action == "starting":
        waiter = workspace.clusters.start(cluster_id)
        if wait:
            waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": action, **compute_status(workspace, cluster_id)}


def compute_stop(workspace, cluster_id: str, *, wait=False, timeout_minutes=20) -> dict:
    waiter = workspace.clusters.delete(cluster_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "terminating", **compute_status(workspace, cluster_id)}


def warehouse_status(workspace, warehouse_id: str) -> dict:
    warehouse = workspace.warehouses.get(warehouse_id)
    return {
        "warehouse_id": warehouse_id,
        "name": warehouse.name,
        "state": value(warehouse.state),
        "type": value(warehouse.warehouse_type),
        "serverless": warehouse.enable_serverless_compute,
        "size": warehouse.cluster_size,
        "clusters": f"{warehouse.num_clusters or 0} of {warehouse.min_num_clusters}-{warehouse.max_num_clusters}",
        "active_sessions": warehouse.num_active_sessions,
        "auto_stop_minutes": warehouse.auto_stop_mins,
        "health": value(getattr(warehouse.health, "status", None)),
    }


def warehouse_start(workspace, warehouse_id: str, *, wait=False, timeout_minutes=20) -> dict:
    waiter = workspace.warehouses.start(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "starting", **warehouse_status(workspace, warehouse_id)}


def warehouse_stop(workspace, warehouse_id: str, *, wait=False, timeout_minutes=20) -> dict:
    waiter = workspace.warehouses.stop(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "stopping", **warehouse_status(workspace, warehouse_id)}


def sql_statement(value_: str, source: str, limit: int) -> str:
    text = str(value_ or "").strip()
    if not text:
        raise ValueError(f"--{source} requires a table name or SQL statement")
    if SQL_PREFIX.match(text):
        return text
    parts = [part.strip().strip("`") for part in text.split(".")]
    if any(not part for part in parts):
        raise ValueError(f"invalid table name: {text!r}")
    if source == "unity":
        if len(parts) != 3:
            raise ValueError("--unity table names must be catalog.schema.table")
    elif source == "glue":
        if len(parts) == 2:
            parts.insert(0, "hive_metastore")
        elif len(parts) != 3:
            raise ValueError("--glue table names must be database.table or catalog.database.table")
    else:
        raise ValueError(f"unsupported SQL source: {source}")
    relation = ".".join(f"`{part.replace('`', '``')}`" for part in parts)
    return f"SELECT * FROM {relation} LIMIT {limit}"


def run_sql(
    workspace,
    statement: str,
    warehouse_id: str,
    *,
    limit=5,
    catalog=None,
    source="unity",
    poll=2.0,
) -> dict:
    response = workspace.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        row_limit=limit,
        wait_timeout="30s",
    )
    while value(response.status.state) in RUNNING_STATEMENT:
        time.sleep(poll)
        response = workspace.statement_execution.get_statement(response.statement_id)
    state = value(response.status.state)
    if state != "SUCCEEDED":
        error = response.status.error.message if response.status.error else state
        raise RuntimeError(f"statement {state}: {error}")
    rows = (response.result.data_array if response.result else None) or []
    manifest = response.manifest
    schema = getattr(manifest, "schema", None)
    return {
        "statement_id": response.statement_id,
        "source": source,
        "columns": [column.name for column in (getattr(schema, "columns", None) or [])],
        "rows": rows[:limit],
        "total_rows": getattr(manifest, "total_row_count", len(rows)),
    }


def format_sql_result(result: dict) -> str:
    columns = [str(column) for column in result.get("columns") or []]
    rows = [[sql_cell(value_) for value_ in row] for row in result.get("rows") or []]
    if not columns:
        table = "(statement completed; no tabular result)"
    else:
        widths = [
            min(48, max(len(column), *(len(row[index]) for row in rows), 1))
            for index, column in enumerate(columns)
        ]

        def line(left: str, middle: str, right: str, fill: str) -> str:
            return left + middle.join(fill * (width + 2) for width in widths) + right

        def values(row) -> str:
            return (
                "| "
                + " | ".join(
                    clipped(value_, width).ljust(width) for value_, width in zip(row, widths)
                )
                + " |"
            )

        table_rows = [
            line("+", "+", "+", "-"),
            values(columns),
            line("+", "+", "+", "="),
            *(values(row) for row in rows),
            line("+", "+", "+", "-"),
        ]
        table = "\n".join(table_rows)
    shown = len(rows)
    total = result.get("total_rows")
    count = f"{shown} row(s)"
    if total not in (None, shown):
        count += f" shown; statement reported {total}"
    return f"{table}\n{count} | {result.get('source', 'unity')} | {result['statement_id']}"


def sql_cell(value_: Any) -> str:
    if value_ is None:
        return "NULL"
    return str(value_).replace("\r", "").replace("\n", "\\n").replace("\t", "\\t")


def clipped(value_: str, width: int) -> str:
    return value_ if len(value_) <= width else value_[: max(0, width - 3)] + "..."


def run_row(run) -> dict:
    return {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "name": run.run_name,
        "type": value(run.run_type),
        "state": value(getattr(run.state, "life_cycle_state", None)),
        "result": value(getattr(run.state, "result_state", None)),
        "started": stamp(run.start_time),
        "seconds": round((run.run_duration or run.execution_duration or 0) / 1000),
        "url": run.run_page_url,
    }


def list_runs(workspace, *, limit=10, job_id=None, active=False) -> dict:
    runs = workspace.jobs.list_runs(job_id=job_id, active_only=active or None)
    return {"runs": [run_row(run) for run in islice(runs, limit)]}


def run_detail(workspace, run_id: int) -> dict:
    run = workspace.jobs.get_run(run_id)
    tasks = [
        {
            "task_key": task.task_key,
            "state": value(getattr(task.state, "life_cycle_state", None)),
            "result": value(getattr(task.state, "result_state", None)),
            "message": getattr(task.state, "state_message", None) or None,
            "seconds": round((task.execution_duration or 0) / 1000),
        }
        for task in run.tasks or []
    ]
    return {
        **run_row(run),
        "message": getattr(run.state, "state_message", None) or None,
        "tasks": tasks,
    }
