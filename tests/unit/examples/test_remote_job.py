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


outlook_mail = _module("outlook_mail", EXAMPLE / "src" / "outlook_mail.py")
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
        cluster_id=cluster,
        prefix="example",
    )
    assert set(specs) == {"right", "collector"}
    assert specs["collector"]["schedule"]["pause_status"] == "UNPAUSED"
    assert all(spec["queue"] == {"enabled": True} for spec in specs.values())
    tasks = [specs[name]["tasks"][0] for name in ("right", "collector")]
    assert all((task.get("existing_cluster_id") == cluster) for task in tasks)


@pytest.mark.parametrize(
    ("files", "error", "status", "attached"),
    [
        (
            {
                "report/summary.json": (
                    b'{"status":"DONE","pairs":2,"units_expected":4,"units_completed":4,'
                    b'"findings":{"row_count_diff":1,"partition_missing":0,'
                    b'"col_stat_diff":1},"errors":[]}'
                ),
                "report/index.html": b"<html>report</html>",
                "report/report.xlsx": b"xlsx",
            },
            None,
            "DONE",
            ["index.html", "report.xlsx", "summary.json"],
        ),
        ({}, "Right failed: boom", "FAILED", []),
    ],
)
def test_email(files, error, status, attached):
    events = []
    attachments = {}

    class Store:
        def exists(self, path):
            return path in files

        def get_bytes(self, path):
            return files[path]

        def get_json(self, path):
            return json.loads(files[path])

    class Attachments:
        def Add(self, path):
            source = Path(path)
            attachments[source.name] = source.read_bytes()

    class Mail:
        def Save(self):
            events.append("save")

        def Send(self):
            events.append("send")

    mail = Mail()
    mail.Attachments = Attachments()

    class Outlook:
        def CreateItem(self, kind):
            assert kind == 0
            return mail

    def dispatch(name):
        assert name == "Outlook.Application"
        return Outlook()

    outcome = outlook_mail.send_report(
        Store(),
        run_id="run-1",
        run_uri="s3://bucket/runs/run-1",
        dashboard_url="https://example/dashboard?a=1&b=2",
        recipients=["owner@example.com"],
        error=error,
        dispatch=dispatch,
        platform="win32",
    )
    assert outcome == {"status": status, "attachments": attached}
    assert mail.To == "owner@example.com"
    assert mail.Subject == f"[DTrack] {status} — run-1"
    assert "https://example/dashboard?a=1&amp;b=2" in mail.HTMLBody
    assert sorted(attachments) == attached
    assert events == ["save", "send"]


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["owner@example.com", "owner@example.com"], ["owner@example.com"]),
        (["one@example.com; two@example.com"], ["one@example.com", "two@example.com"]),
    ],
)
def test_recipients(values, expected):
    assert install.email_recipients(values) == expected


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


@pytest.mark.parametrize("phase", ["success", "right", "collector"])
def test_sequence(phase, tmp_path, monkeypatch):
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
                "email_to": ["owner@example.com"],
                "dashboard_url": "https://example/dashboard",
            }
        ),
        encoding="utf-8",
    )

    class Wait:
        run_id = 101

        def result(self, timeout):
            events.append("right-wait")
            result = SimpleNamespace(value="FAILED" if phase == "right" else "SUCCESS")
            return SimpleNamespace(state=SimpleNamespace(result_state=result))

    jobs = SimpleNamespace(
        run_now=lambda job_id, notebook_params: events.append(("right", job_id)) or Wait(),
        cancel_run=lambda run_id: events.append(("cancel", run_id)),
    )
    monkeypatch.setattr(launch.databricks, "workspace", lambda profile: SimpleNamespace(jobs=jobs))
    monkeypatch.setattr(launch, "require_outlook", lambda: None)
    monkeypatch.setattr(launch, "_run_left", lambda document, args: events.append("left"))

    def refresh(workspace, values, timeout):
        events.append("collector")
        if phase == "collector":
            raise RuntimeError("collector boom")
        return 202

    monkeypatch.setattr(launch, "_refresh", refresh)
    monkeypatch.setattr(
        launch,
        "_send_email",
        lambda values, run_id, run_uri, error=None: (
            events.append("email")
            or {
                "status": "FAILED" if error else "DONE",
                "attachments": ["report.xlsx"],
            }
        ),
    )
    args = Namespace(
        state=state,
        config=config,
        run_s3="s3://bucket/runs",
        run_id="run-1",
        remote_timeout=60,
        collector_timeout=20,
    )
    if phase != "success":
        with pytest.raises(RuntimeError, match="Right|collector"):
            launch.run(args)
        expected = [("right", 10), "left", "right-wait"]
        if phase == "right":
            expected.append(("cancel", 101))
        assert events == [*expected, "collector", "email"]
    else:
        outcome = launch.run(args)
        assert events == [("right", 10), "left", "right-wait", "collector", "email"]
        assert outcome["right_run_id"] == 101 and outcome["collector_run_id"] == 202
        assert outcome["email_status"] == "DONE"
        assert outcome["email_to"] == ["owner@example.com"]
        assert outcome["email_attachments"] == ["report.xlsx"]
