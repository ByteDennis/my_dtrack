from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "remote_job"
sys.path.insert(0, str(EXAMPLE / "src"))


def _module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


launch = _module("remote_job_launch", EXAMPLE / "src" / "launch.py")
install = _module("remote_job_install", EXAMPLE / "setup" / "install.py")
dashboard = _module("remote_job_dashboard", EXAMPLE / "src" / "dashboard.py")


@pytest.mark.parametrize(
    ("version", "names"),
    [
        (1, ["tools", "row-check", "col-check"]),
        (2, ["tools", "left-run"]),
    ],
)
def test_commands(version, names, tmp_path):
    args = Namespace(
        config=tmp_path / "config.json",
        run_s3="s3://bucket/runs",
        run_id="run-1",
        pairs=["orders", "customers"],
        workers=3,
        pull_workers=2,
        work_dir=tmp_path / "work",
        wait_timeout=120.0,
        poll_interval=2.0,
        log_level="INFO",
    )
    commands = launch.left_commands({"version": version}, args)
    assert [command[3] for command in commands] == names
    assert all(command[:3] == [launch.sys.executable, "-m", "dtrack"] for command in commands)
    assert all(command.count("--pair") == 2 for command in commands[1:])
    assert all("--force" not in command for command in commands)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (
            "right",
            {
                "run_s3": "/Volumes/main/dtrack/runs",
                "run_id": "run-1",
                "gate": "auto",
                "idle_timeout": "900",
                "wait_timeout": "1800",
                "aws_secret_scope": "scope",
            },
        ),
        (
            "collector",
            {
                "run_roots": "/Volumes/main/dtrack/runs",
                "schema": "main.dtrack",
                "keep_complete_days": "1",
            },
        ),
    ],
)
def test_parameters(kind, expected):
    state = {
        "right_runs": "/Volumes/main/dtrack/runs",
        "schema": "main.dtrack",
        "aws_secret_scope": "scope",
    }
    actual = (
        launch.right_parameters(state, "run-1")
        if kind == "right"
        else launch.collector_parameters(state)
    )
    assert actual == expected


@pytest.mark.parametrize("cluster", [None, "cluster-1"])
def test_jobs(cluster):
    specs = install.job_specs(
        "/Workspace/Shared/dtrack",
        "/Volumes/main/dtrack/runs",
        "main.dtrack",
        alert_id="alert-1",
        warehouse_id="warehouse-1",
        cluster_id=cluster,
        prefix="example",
    )
    assert set(specs) == {"right", "collector", "alert"}
    assert specs["collector"]["schedule"]["pause_status"] == "UNPAUSED"
    assert all(spec["queue"] == {"enabled": True} for spec in specs.values())
    tasks = [specs[name]["tasks"][0] for name in ("right", "collector", "alert")]
    assert all((task.get("existing_cluster_id") == cluster) for task in tasks)
    alert = specs["alert"]
    assert alert["max_concurrent_runs"] == 1
    assert alert["parameters"] == [{"name": "run_id", "default": ""}]
    assert alert["tasks"][0]["notebook_task"]["base_parameters"]["run_id"] == (
        "{{job.parameters.run_id}}"
    )
    assert alert["tasks"][1]["alert_task"] == {
        "alert_id": "alert-1",
        "warehouse_id": "warehouse-1",
    }


def test_alert():
    query = install.alert_sql("main.dtrack")
    assert "dtrack_alert_context" in query
    assert "dtrack_progress_snapshot" in query
    assert "LEFT JOIN latest" in query
    assert "NO_DATA" in query
    assert "notify" in query
    value = install.alert_definition(
        "main.dtrack",
        "/Workspace/Shared/dtrack",
        "warehouse-1",
        "dtrack result",
        ["owner@example.com"],
        "https://example/dashboard",
    ).as_dict()
    assert value["schedule"]["pause_status"] == "PAUSED"
    assert value["evaluation"]["notification"]["retrigger_seconds"] == 1
    assert value["evaluation"]["notification"]["subscriptions"] == [
        {"user_email": "owner@example.com"}
    ]
    assert "{{QUERY_RESULT_TABLE}}" in value["custom_description"]


def test_dashboard():
    document = dashboard.build("main.dtrack")
    assert [page["name"] for page in document["pages"]] == ["executive", "detailed"]
    assert {dataset["name"] for dataset in document["datasets"]} == {
        "ds_status",
        "ds_daily",
        "ds_blockers",
    }
    assert all("main.dtrack." in "".join(dataset["queryLines"]) for dataset in document["datasets"])


def test_state(tmp_path):
    path = tmp_path / "state.json"
    install.write_state(path, {"dashboard_url": "https://example/dashboard"})
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == 1
    assert not path.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize("failure", [False, True])
def test_sequence(failure, tmp_path, monkeypatch):
    events = []
    config = tmp_path / "config.json"
    state = tmp_path / "state.json"
    config.write_text('{"version": 1, "pairs": {"orders": {}}}', encoding="utf-8")
    state.write_text(
        json.dumps(
            {
                "profile": "corp",
                "right_runs": "/Volumes/main/dtrack/runs",
                "schema": "main.dtrack",
                "right_job_id": 10,
                "collector_job_id": 20,
                "alert_job_id": 30,
                "alert_id": "alert-1",
                "alert_recipients": ["owner@example.com"],
            }
        ),
        encoding="utf-8",
    )

    class Wait:
        run_id = 101

        def result(self, timeout):
            events.append("right-wait")
            result = SimpleNamespace(value="FAILED" if failure else "SUCCESS")
            return SimpleNamespace(state=SimpleNamespace(result_state=result))

    jobs = SimpleNamespace(
        run_now=lambda job_id, notebook_params: events.append(("right", job_id)) or Wait(),
        cancel_run=lambda run_id: events.append(("cancel", run_id)),
    )
    monkeypatch.setattr(launch.databricks, "workspace", lambda profile: SimpleNamespace(jobs=jobs))
    monkeypatch.setattr(launch, "_run_left", lambda document, args: events.append("left"))
    monkeypatch.setattr(
        launch,
        "_refresh",
        lambda workspace, values, timeout: events.append("collector") or 202,
    )
    monkeypatch.setattr(
        launch,
        "_notify",
        lambda workspace, values, run_id, timeout: events.append("alert") or 303,
    )
    args = Namespace(
        state=state,
        config=config,
        run_s3="s3://bucket/runs",
        run_id="run-1",
        remote_timeout=60,
        collector_timeout=20,
        alert_timeout=20,
    )
    if failure:
        with pytest.raises(RuntimeError, match="Right"):
            launch.run(args)
        assert events == [
            ("right", 10),
            "left",
            "right-wait",
            ("cancel", 101),
            "collector",
            "alert",
        ]
    else:
        outcome = launch.run(args)
        assert events == [("right", 10), "left", "right-wait", "collector", "alert"]
        assert outcome["right_run_id"] == 101 and outcome["collector_run_id"] == 202
        assert outcome["alert_run_id"] == 303
        assert outcome["alert_recipients"] == ["owner@example.com"]


def test_notify():
    events = []

    class Wait:
        run_id = 303

        def result(self, timeout):
            events.append(timeout)
            result = SimpleNamespace(value="SUCCESS")
            return SimpleNamespace(state=SimpleNamespace(result_state=result))

    def run_now(job_id, **kwargs):
        events.append((job_id, kwargs))
        return Wait()

    workspace = SimpleNamespace(jobs=SimpleNamespace(run_now=run_now))
    actual = launch._notify(workspace, {"alert_job_id": 30}, "run-1", 12)
    assert actual == 303
    assert events[0] == (30, {"job_parameters": {"run_id": "run-1"}})
