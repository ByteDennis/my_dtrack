import base64
import subprocess
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


class WorkspaceFiles:
    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def mkdirs(self, path):
        self.calls.append(("mkdirs", path))

    def upload(self, path, content, **options):
        self.calls.append(("upload", path, content, options))
        if path == self.fail_on:
            raise OSError("upload failed")


def workspace_files(fail_on=None):
    files = WorkspaceFiles(fail_on)
    return NS(
        workspace=files,
        current_user=NS(me=lambda: NS(user_name="me@corp.com")),
    )


class WorkspaceDownloads:
    def __init__(self, objects, content):
        self.objects = {row.path: row for row in objects}
        self.content = content
        self.calls = []

    def get_status(self, path):
        self.calls.append(("get_status", path))
        return self.objects[path]

    def list(self, path):
        self.calls.append(("list", path))
        parent = path.rstrip("/") + "/"
        return iter(
            row
            for item, row in sorted(self.objects.items())
            if item.startswith(parent) and "/" not in item[len(parent) :]
        )

    def export(self, path, *, format):
        self.calls.append(("export", path, format.value))
        body, file_type = self.content[path]
        return NS(content=base64.b64encode(body).decode(), file_type=file_type)


def workspace_downloads(objects, content):
    return NS(workspace=WorkspaceDownloads(objects, content))


def workspace_object(path, type_, language=None):
    return NS(
        path=path,
        object_type=NS(value=type_),
        language=None if language is None else NS(value=language),
    )


def git_init(path):
    subprocess.run(["git", "init", "-q", str(path)], check=True, timeout=15)


def test_upload_workspace_file_uses_the_personal_default_and_preserves_plain_python(tmp_path):
    source = tmp_path / "helpers.py"
    source.write_text("def answer():\n    return 42\n")
    w = workspace_files()

    result = databricks.upload_workspace(w, source)

    assert result["target"] == "/Users/me@corp.com/helpers.py"
    assert result["uploaded"] == 1 and result["failed"] == []
    upload = next(call for call in w.workspace.calls if call[0] == "upload")
    assert upload[1:3] == ("/Users/me@corp.com/helpers.py", source.read_bytes())
    assert upload[3]["format"].value == "AUTO" and "language" not in upload[3]


def test_upload_workspace_folder_ignores_junk_and_imports_source_notebook(tmp_path):
    git_init(tmp_path)
    (tmp_path / ".gitignore").write_text("generated-data/\n*.scratch\n")
    source = tmp_path / "right"
    (source / "pkg").mkdir(parents=True)
    (source / "generated-data").mkdir()
    (source / "pkg" / "module.py").write_text("VALUE = 1\n")
    (source / "notebook.py").write_text("# DATABRICKS NOTEBOOK source\nprint('ok')\n")
    (source / "generated-data" / "rows.bin").write_bytes(b"cache")
    (source / "notes.scratch").write_text("ignored")
    w = workspace_files()

    result = databricks.upload_workspace(w, source, "/Users/me/right", overwrite=True)

    assert result["planned"] == result["uploaded"] == 2
    assert result["skipped"] == 2
    uploads = [call for call in w.workspace.calls if call[0] == "upload"]
    assert [call[1] for call in uploads] == [
        "/Users/me/right/notebook",
        "/Users/me/right/pkg/module.py",
    ]
    notebook_options = uploads[0][3]
    assert notebook_options["format"].value == "SOURCE"
    assert notebook_options["language"].value == "PYTHON"
    assert notebook_options["overwrite"] is True


def test_upload_workspace_honors_nested_gitignore_negation(tmp_path):
    git_init(tmp_path)
    (tmp_path / ".gitignore").write_text("*.secret\n")
    source = tmp_path / "project"
    source.mkdir()
    (source / ".gitignore").write_text("!keep.secret\n")
    (source / "drop.secret").write_text("drop")
    (source / "keep.secret").write_text("keep")
    w = workspace_files()

    result = databricks.upload_workspace(w, source, "/Shared/project", dry_run=True)

    assert result["skipped"] == 1
    assert result["paths"] == ["/Shared/project/.gitignore", "/Shared/project/keep.secret"]


def test_upload_workspace_without_gitignore_does_not_filter_names_or_suffixes(tmp_path):
    git_init(tmp_path)
    source = tmp_path / "project"
    (source / "__pycache__").mkdir(parents=True)
    (source / "__pycache__" / "module.pyc").write_bytes(b"cache")
    (source / ".DS_Store").write_bytes(b"metadata")
    w = workspace_files()

    result = databricks.upload_workspace(w, source, "/Shared/project", dry_run=True)

    assert result["skipped"] == 0
    assert result["paths"] == [
        "/Shared/project/.DS_Store",
        "/Shared/project/__pycache__/module.pyc",
    ]


def test_upload_workspace_accepts_a_relative_folder_inside_a_git_worktree(tmp_path, monkeypatch):
    git_init(tmp_path)
    (tmp_path / ".gitignore").write_text("*.tmp\n")
    source = tmp_path / "project"
    source.mkdir()
    (source / "keep.txt").write_text("keep")
    (source / "drop.tmp").write_text("drop")
    monkeypatch.chdir(tmp_path)

    result = databricks.upload_workspace(
        workspace_files(), "project", "/Shared/project", dry_run=True
    )

    assert result["paths"] == ["/Shared/project/keep.txt"]
    assert result["skipped"] == 1


def test_upload_workspace_imports_ipynb_as_a_jupyter_notebook(tmp_path):
    source = tmp_path / "analysis.ipynb"
    source.write_text('{"cells": [], "nbformat": 4, "nbformat_minor": 5}')
    w = workspace_files()

    result = databricks.upload_workspace(w, source, "/Users/me/analysis.ipynb")

    assert result["paths"] == ["/Users/me/analysis"]
    upload = next(call for call in w.workspace.calls if call[0] == "upload")
    assert upload[1] == "/Users/me/analysis"
    assert upload[3]["format"].value == "JUPYTER"
    assert "language" not in upload[3]


def test_upload_workspace_dry_run_has_no_writes_and_lists_remote_paths(tmp_path):
    source = tmp_path / "folder"
    source.mkdir()
    (source / "a.txt").write_text("a")
    w = workspace_files()

    result = databricks.upload_workspace(w, source, "/Shared/folder", dry_run=True)

    assert w.workspace.calls == []
    assert result["uploaded"] == 0
    assert result["paths"] == ["/Shared/folder/a.txt"]


def test_upload_workspace_rejects_relative_targets_and_symbolic_links(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("a")
    with pytest.raises(ValueError, match="must be absolute"):
        databricks.upload_workspace(workspace_files(), source, "Shared/a.txt")

    link = tmp_path / "link.txt"
    link.symlink_to(source)
    with pytest.raises(ValueError, match="symbolic link"):
        databricks.upload_workspace(workspace_files(), link, "/Shared/link.txt")


def test_upload_workspace_reports_individual_failures_and_exit_one(tmp_path):
    source = tmp_path / "folder"
    source.mkdir()
    (source / "a.txt").write_text("a")
    (source / "b.txt").write_text("b")
    w = workspace_files(fail_on="/Shared/folder/a.txt")

    result = databricks.upload_workspace(w, source, "/Shared/folder")

    assert result["uploaded"] == 1 and result["exit_code"] == 1
    assert result["paths"] == ["/Shared/folder/b.txt"]
    assert result["failed"] == [{"path": "/Shared/folder/a.txt", "error": "OSError: upload failed"}]


def test_download_workspace_mirrors_files_and_source_notebooks(tmp_path):
    objects = [
        workspace_object("/Shared/right", "DIRECTORY"),
        workspace_object("/Shared/right/module.py", "FILE"),
        workspace_object("/Shared/right/notebook", "NOTEBOOK", "PYTHON"),
        workspace_object("/Shared/right/empty", "DIRECTORY"),
        workspace_object("/Shared/right/pkg", "DIRECTORY"),
        workspace_object("/Shared/right/pkg/config.json", "FILE"),
    ]
    content = {
        "/Shared/right/module.py": (b"VALUE = 1\n", "py"),
        "/Shared/right/notebook": (b"# Databricks notebook source\nprint('ok')\n", "py"),
        "/Shared/right/pkg/config.json": (b'{"ok":true}\n', "json"),
    }
    w = workspace_downloads(objects, content)
    target = tmp_path / "right"

    result = databricks.download_workspace(w, "/Shared/right", target)

    assert result["planned"] == result["downloaded"] == 3
    assert result["directories"] == 3 and result["failed"] == []
    assert (target / "empty").is_dir()
    assert (target / "module.py").read_bytes() == b"VALUE = 1\n"
    assert (target / "notebook.py").read_text() == "# Databricks notebook source\nprint('ok')\n"
    assert (target / "pkg/config.json").read_text() == '{"ok":true}\n'


def test_download_workspace_dry_run_is_read_only_and_reports_local_paths(tmp_path):
    objects = [
        workspace_object("/Shared/right", "DIRECTORY"),
        workspace_object("/Shared/right/n", "NOTEBOOK", "SQL"),
    ]
    w = workspace_downloads(objects, {"/Shared/right/n": (b"SELECT 1\n", "sql")})
    target = tmp_path / "right"

    result = databricks.download_workspace(w, "/Shared/right", target, dry_run=True)

    assert result["paths"] == [str(target / "n.sql")]
    assert result["downloaded"] == 0 and not target.exists()
    assert [call[0] for call in w.workspace.calls] == ["get_status", "list"]


def test_download_workspace_requires_overwrite_and_continues_other_files(tmp_path):
    objects = [
        workspace_object("/Shared/right", "DIRECTORY"),
        workspace_object("/Shared/right/a.txt", "FILE"),
        workspace_object("/Shared/right/b.txt", "FILE"),
    ]
    content = {
        "/Shared/right/a.txt": (b"new-a", "txt"),
        "/Shared/right/b.txt": (b"new-b", "txt"),
    }
    target = tmp_path / "right"
    target.mkdir()
    (target / "a.txt").write_text("old-a")

    result = databricks.download_workspace(
        workspace_downloads(objects, content), "/Shared/right", target
    )

    assert (target / "a.txt").read_text() == "old-a"
    assert (target / "b.txt").read_text() == "new-b"
    assert result["downloaded"] == 1 and result["exit_code"] == 1
    assert result["failed"][0]["path"] == str(target / "a.txt")

    replaced = databricks.download_workspace(
        workspace_downloads(objects, content), "/Shared/right", target, overwrite=True
    )
    assert replaced["downloaded"] == 2
    assert (target / "a.txt").read_text() == "new-a"


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
