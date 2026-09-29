from __future__ import annotations

import time
import webbrowser
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Any

DEFAULT_PROFILE = "corp"
RUNNING_STATEMENT = {"PENDING", "RUNNING"}


# >>> a WorkspaceClient for a ~/.databrickscfg profile; external-browser profiles log in here <<< #
def workspace(profile: str):
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ImportError("dtrack dbx requires dtrack[dbx] (databricks-sdk)") from exc
    return WorkspaceClient(profile=profile)


# >>> enum members as their plain value <<< #
def _value(item: Any) -> Any:
    return getattr(item, "value", item)


# >>> epoch milliseconds as an ISO UTC timestamp <<< #
def _stamp(millis: int | None) -> str | None:
    return (
        datetime.fromtimestamp(millis / 1000, UTC).isoformat(timespec="seconds") if millis else None
    )


# >>> time since epoch milliseconds as <h>h<mm>m <<< #
def _elapsed(millis: int | None, now: float | None = None) -> str | None:
    if not millis:
        return None
    minutes = int(((now or time.time()) - millis / 1000) // 60)
    return f"{minutes // 60}h{minutes % 60:02d}m"


# >>> open the workspace in the default browser (Windows `start`, macOS `open`) and confirm who is logged in <<< #
def login(w, *, open_browser: bool = True, opener=webbrowser.open) -> dict:
    opened = bool(opener(w.config.host)) if open_browser else False
    me = w.current_user.me()
    return {
        "host": w.config.host,
        "profile": w.config.profile,
        "auth_type": w.config.auth_type,
        "user": me.user_name,
        "browser_opened": opened,
    }


# >>> one cluster: state, EC2 node types and instance ids, uptime since the last (re)start <<< #
def compute_status(w, cluster_id: str, *, now: float | None = None) -> dict:
    cluster = w.clusters.get(cluster_id)
    state = _value(cluster.state)
    running = state in {"RUNNING", "RESIZING"}
    since = cluster.last_restarted_time or cluster.start_time
    driver = cluster.driver
    autoscale = cluster.autoscale
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
        "running_since": _stamp(since) if running else None,
        "uptime": _elapsed(since, now) if running else None,
        "terminated_at": None if running else _stamp(cluster.terminated_time),
        "termination_reason": _value(getattr(cluster.termination_reason, "code", None)),
        "autotermination_minutes": cluster.autotermination_minutes,
        "tags": dict(cluster.default_tags or {}),
    }


# >>> start a terminated cluster (a no-op when it is already up), optionally waiting until RUNNING <<< #
def compute_start(w, cluster_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    state = _value(w.clusters.get(cluster_id).state)
    action = (
        "already running"
        if state in {"RUNNING", "RESIZING", "PENDING", "RESTARTING"}
        else "starting"
    )
    if action == "starting":
        waiter = w.clusters.start(cluster_id)
        if wait:
            waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": action, **compute_status(w, cluster_id)}


# >>> terminate (not permanently delete) a cluster <<< #
def compute_stop(w, cluster_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.clusters.delete(cluster_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "terminating", **compute_status(w, cluster_id)}


# >>> one SQL warehouse: state, type, size, clusters, sessions, auto-stop <<< #
def warehouse_status(w, warehouse_id: str) -> dict:
    warehouse = w.warehouses.get(warehouse_id)
    return {
        "warehouse_id": warehouse_id,
        "name": warehouse.name,
        "state": _value(warehouse.state),
        "type": _value(warehouse.warehouse_type),
        "serverless": warehouse.enable_serverless_compute,
        "size": warehouse.cluster_size,
        "clusters": f"{warehouse.num_clusters or 0} of {warehouse.min_num_clusters}-{warehouse.max_num_clusters}",
        "active_sessions": warehouse.num_active_sessions,
        "auto_stop_minutes": warehouse.auto_stop_mins,
        "health": _value(getattr(warehouse.health, "status", None)),
    }


# >>> start a SQL warehouse, optionally waiting until RUNNING <<< #
def warehouse_start(w, warehouse_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.warehouses.start(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "starting", **warehouse_status(w, warehouse_id)}


# >>> stop a SQL warehouse, optionally waiting until STOPPED <<< #
def warehouse_stop(w, warehouse_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.warehouses.stop(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "stopping", **warehouse_status(w, warehouse_id)}


# >>> run one statement on a SQL warehouse (starting it if needed) and return the first rows <<< #
def run_sql(w, statement: str, warehouse_id: str, *, limit: int = 20, poll: float = 2.0) -> dict:
    response = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="30s"
    )
    while _value(response.status.state) in RUNNING_STATEMENT:
        time.sleep(poll)
        response = w.statement_execution.get_statement(response.statement_id)
    state = _value(response.status.state)
    if state != "SUCCEEDED":
        error = response.status.error.message if response.status.error else state
        raise RuntimeError(f"statement {state}: {error}")
    columns = [column.name for column in response.manifest.schema.columns]
    rows = (response.result.data_array if response.result else None) or []
    return {
        "statement_id": response.statement_id,
        "columns": columns,
        "rows": rows[:limit],
        "total_rows": response.manifest.total_row_count,
    }


# >>> one job run as a flat row <<< #
def _run_row(run) -> dict:
    return {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "name": run.run_name,
        "type": _value(run.run_type),
        "state": _value(getattr(run.state, "life_cycle_state", None)),
        "result": _value(getattr(run.state, "result_state", None)),
        "started": _stamp(run.start_time),
        "seconds": round((run.run_duration or run.execution_duration or 0) / 1000),
        "url": run.run_page_url,
    }


# >>> the latest job runs (one-time submits included), newest first <<< #
def list_runs(w, *, limit: int = 10, job_id: int | None = None, active: bool = False) -> dict:
    runs = w.jobs.list_runs(job_id=job_id, active_only=active or None)
    return {"runs": [_run_row(run) for run in islice(runs, limit)]}


# >>> one run with its tasks and the state message that explains a failure <<< #
def run_detail(w, run_id: int) -> dict:
    run = w.jobs.get_run(run_id)
    tasks = [
        {
            "task_key": task.task_key,
            "run_id": task.run_id,
            "state": _value(getattr(task.state, "life_cycle_state", None)),
            "result": _value(getattr(task.state, "result_state", None)),
            "message": getattr(task.state, "state_message", None) or None,
            "seconds": round((task.run_duration or task.execution_duration or 0) / 1000),
        }
        for task in run.tasks or []
    ]
    return {
        **_run_row(run),
        "message": getattr(run.state, "state_message", None) or None,
        "tasks": tasks,
    }
