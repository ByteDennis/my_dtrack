import sys
from enum import Enum
from types import SimpleNamespace as NS

import pytest

from dtrack_left.backends import databricks


class State(Enum):
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"


class Waiter:
    def __init__(self, log, name):
        self.log, self.name = log, name

    def result(self, timeout):
        self.log.append((self.name, timeout.total_seconds()))


START = 1_790_000_000_000


def cluster(state="RUNNING", **extra):
    fields = {
        "state": State[state],
        "state_message": "",
        "cluster_name": "etl",
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "m5.xlarge",
        "driver_node_type_id": None,
        "num_workers": 2,
        "autoscale": None,
        "driver": NS(instance_id="i-driver"),
        "executors": [NS(instance_id="i-w1"), NS(instance_id="i-w2")],
        "start_time": START,
        "last_restarted_time": START + 3_600_000,
        "terminated_time": START + 7_200_000,
        "termination_reason": NS(code=NS(value="INACTIVITY")),
        "autotermination_minutes": 60,
        "default_tags": {"ClusterName": "etl"},
    }
    return NS(**{**fields, **extra})


# >>> a WorkspaceClient stand-in that records every state-changing call <<< #
def client(details, *, statements=(), runs=()):
    log = []
    statements = list(statements)
    return NS(
        log=log,
        config=NS(
            host="https://x.cloud.databricks.com", profile="corp", auth_type="external-browser"
        ),
        current_user=NS(me=lambda: NS(user_name="me@corp.com")),
        clusters=NS(
            get=lambda cluster_id: details,
            start=lambda cluster_id: log.append(("start", cluster_id)) or Waiter(log, "start-wait"),
            delete=lambda cluster_id: (
                log.append(("delete", cluster_id)) or Waiter(log, "delete-wait")
            ),
        ),
        warehouses=NS(
            get=lambda warehouse_id: NS(
                name="wh",
                state=State.RUNNING,
                warehouse_type=NS(value="PRO"),
                enable_serverless_compute=True,
                cluster_size="2X-Small",
                num_clusters=1,
                min_num_clusters=1,
                max_num_clusters=2,
                num_active_sessions=3,
                auto_stop_mins=10,
                health=None,
            ),
            start=lambda warehouse_id: (
                log.append(("wh-start", warehouse_id)) or Waiter(log, "wh-start-wait")
            ),
            stop=lambda warehouse_id: (
                log.append(("wh-stop", warehouse_id)) or Waiter(log, "wh-stop-wait")
            ),
        ),
        statement_execution=NS(
            execute_statement=lambda **kwargs: (
                log.append(("sql", kwargs["statement"])) or statements.pop(0)
            ),
            get_statement=lambda statement_id: statements.pop(0),
        ),
        jobs=NS(
            list_runs=lambda **kwargs: log.append(("list_runs", kwargs)) or iter(runs),
            get_run=lambda run_id: runs[0],
        ),
    )


def test_workspace_explains_the_extra_when_the_sdk_is_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk", None)
    with pytest.raises(ImportError, match=r"dtrack\[dbx\]"):
        databricks.workspace("corp")


def test_workspace_builds_a_client_for_the_profile(monkeypatch):
    monkeypatch.setitem(
        sys.modules, "databricks.sdk", NS(WorkspaceClient=lambda profile: ("client", profile))
    )
    assert databricks.workspace("corp") == ("client", "corp")


def test_login_opens_the_host_and_reports_the_identity():
    opened = []
    result = databricks.login(client(cluster()), opener=lambda url: opened.append(url) or True)
    assert opened == ["https://x.cloud.databricks.com"] and result["browser_opened"] is True
    assert result["user"] == "me@corp.com" and result["auth_type"] == "external-browser"
    assert databricks.login(client(cluster()), open_browser=False)["browser_opened"] is False


def test_compute_status_reports_ec2_nodes_and_uptime_since_the_last_restart():
    status = databricks.compute_status(
        client(cluster()), "c1", now=START / 1000 + 3 * 3600 + 5 * 60
    )
    assert status["state"] == "RUNNING" and status["uptime"] == "2h05m"
    assert status["driver_node_type"] == "m5.xlarge" and status["workers"] == 2
    assert status["driver_instance_id"] == "i-driver" and status["worker_instance_ids"] == [
        "i-w1",
        "i-w2",
    ]
    assert status["running_since"].startswith("2026-") and status["terminated_at"] is None
    stopped = databricks.compute_status(
        client(
            cluster(
                "TERMINATED",
                driver=None,
                executors=None,
                autoscale=NS(min_workers=1, max_workers=4),
            )
        ),
        "c1",
    )
    assert stopped["uptime"] is None and stopped["terminated_at"] and stopped["workers"] == "1-4"
    assert stopped["termination_reason"] == "INACTIVITY" and stopped["driver_instance_id"] is None
    fresh = cluster(start_time=None, last_restarted_time=None)
    assert databricks.compute_status(client(fresh), "c1")["uptime"] is None


def test_compute_start_is_a_no_op_when_running_and_waits_when_asked():
    running = client(cluster())
    assert (
        databricks.compute_start(running, "c1")["action"] == "already running" and running.log == []
    )
    stopped = client(cluster("TERMINATED"))
    assert (
        databricks.compute_start(stopped, "c1", wait=True, timeout_minutes=5)["action"]
        == "starting"
    )
    assert stopped.log == [("start", "c1"), ("start-wait", 300.0)]
    stopping = client(cluster())
    databricks.compute_stop(stopping, "c1", wait=True)
    assert stopping.log == [("delete", "c1"), ("delete-wait", 1200.0)]
    databricks.compute_stop(client(cluster()), "c1")


def test_warehouse_status_start_and_stop():
    w = client(cluster())
    status = databricks.warehouse_status(w, "wh1")
    assert (
        status["state"] == "RUNNING"
        and status["type"] == "PRO"
        and status["clusters"] == "1 of 1-2"
    )
    assert databricks.warehouse_start(w, "wh1", wait=True)["action"] == "starting"
    assert databricks.warehouse_stop(w, "wh1")["action"] == "stopping"
    databricks.warehouse_stop(w, "wh1", wait=True, timeout_minutes=1)
    databricks.warehouse_start(w, "wh1")
    assert ("wh-start-wait", 1200.0) in w.log and ("wh-stop-wait", 60.0) in w.log


def _statement(state, rows=None, error=None):
    return NS(
        statement_id="s1",
        status=NS(state=State[state], error=NS(message=error) if error else None),
        manifest=NS(schema=NS(columns=[NS(name="n")]), total_row_count=len(rows or [])),
        result=NS(data_array=rows) if rows is not None else None,
    )


def test_run_sql_polls_until_done_and_limits_rows():
    w = client(
        cluster(),
        statements=[_statement("PENDING"), _statement("SUCCEEDED", [["1"], ["2"], ["3"]])],
    )
    result = databricks.run_sql(w, "SELECT 1", "wh1", limit=2, poll=0)
    assert result == {
        "statement_id": "s1",
        "columns": ["n"],
        "rows": [["1"], ["2"]],
        "total_rows": 3,
    }
    empty = client(cluster(), statements=[_statement("SUCCEEDED")])
    assert databricks.run_sql(empty, "SELECT 1", "wh1")["rows"] == []


def test_run_sql_raises_with_the_warehouse_error():
    w = client(cluster(), statements=[_statement("FAILED", error="TABLE_OR_VIEW_NOT_FOUND")])
    with pytest.raises(RuntimeError, match="FAILED: TABLE_OR_VIEW_NOT_FOUND"):
        databricks.run_sql(w, "SELECT * FROM nope", "wh1")
    bare = client(cluster(), statements=[_statement("FAILED")])
    with pytest.raises(RuntimeError, match="statement FAILED: FAILED"):
        databricks.run_sql(bare, "SELECT 1", "wh1")


def _run(run_id, tasks=None):
    return NS(
        run_id=run_id,
        job_id=7,
        run_name=f"r{run_id}",
        run_type=NS(value="SUBMIT_RUN"),
        state=NS(
            life_cycle_state=NS(value="TERMINATED"),
            result_state=NS(value="FAILED"),
            state_message="boom",
        ),
        start_time=START,
        run_duration=61_500,
        execution_duration=None,
        run_page_url=f"https://x/run/{run_id}",
        tasks=tasks,
    )


def test_runs_list_the_newest_and_show_one_with_its_tasks():
    task = NS(
        task_key="t",
        run_id=11,
        state=NS(
            life_cycle_state=NS(value="TERMINATED"),
            result_state=NS(value="FAILED"),
            state_message="E",
        ),
        run_duration=None,
        execution_duration=2_000,
    )
    w = client(cluster(), runs=[_run(1, [task]), _run(2), _run(3)])
    listed = databricks.list_runs(w, limit=2, job_id=7, active=True)
    assert [row["run_id"] for row in listed["runs"]] == [1, 2] and listed["runs"][0][
        "seconds"
    ] == 62
    assert ("list_runs", {"job_id": 7, "active_only": True}) in w.log
    detail = databricks.run_detail(w, 1)
    assert detail["message"] == "boom" and detail["tasks"][0] == {
        "task_key": "t",
        "run_id": 11,
        "state": "TERMINATED",
        "result": "FAILED",
        "message": "E",
        "seconds": 2,
    }
