import base64
from enum import Enum
from types import SimpleNamespace as NS

import pytest
from databricks.sdk.errors import NotFound

from pair_recon_dbx import runtime
from pair_recon_dbx import workspace as workspace_ops


class State(Enum):
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"


class Waiter:
    def __init__(self, calls, name):
        self.calls, self.name = calls, name

    def result(self, timeout):
        self.calls.append((self.name, timeout.total_seconds()))


def cluster(state="RUNNING"):
    return NS(
        state=State[state],
        state_message="",
        cluster_name="etl",
        spark_version="16.4",
        node_type_id="m5.xlarge",
        driver_node_type_id=None,
        num_workers=2,
        autoscale=None,
        driver=NS(instance_id="i-driver"),
        executors=[NS(instance_id="i-worker")],
        start_time=1_790_000_000_000,
        last_restarted_time=None,
        terminated_time=1_790_003_600_000,
        termination_reason=NS(code=NS(value="INACTIVITY")),
        autotermination_minutes=60,
        default_tags={"Name": "etl"},
    )


def sdk_client(state="RUNNING", statements=(), runs=()):
    calls, statements = [], list(statements)
    details = cluster(state)
    warehouse = NS(
        name="warehouse",
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
    )
    return NS(
        calls=calls,
        config=NS(host="https://example.databricks.com", profile="corp", auth_type="pat"),
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
        clusters=NS(
            get=lambda _id: details,
            start=lambda value: calls.append(("start", value)) or Waiter(calls, "start-wait"),
            delete=lambda value: calls.append(("stop", value)) or Waiter(calls, "stop-wait"),
        ),
        warehouses=NS(
            get=lambda _id: warehouse,
            start=lambda value: calls.append(("wh-start", value)) or Waiter(calls, "wh-start-wait"),
            stop=lambda value: calls.append(("wh-stop", value)) or Waiter(calls, "wh-stop-wait"),
        ),
        statement_execution=NS(
            execute_statement=lambda **_kwargs: statements.pop(0),
            get_statement=lambda _id: statements.pop(0),
        ),
        jobs=NS(list_runs=lambda **_kwargs: iter(runs), get_run=lambda _id: runs[0]),
    )


def statement(state, rows=None):
    return NS(
        statement_id="s1",
        status=NS(state=State[state], error=None),
        manifest=NS(schema=NS(columns=[NS(name="n")]), total_row_count=len(rows or [])),
        result=NS(data_array=rows) if rows is not None else None,
    )


def test_runtime():
    workspace = sdk_client("TERMINATED", [statement("PENDING"), statement("SUCCEEDED", [["1"]])])
    assert (
        runtime.compute_start(workspace, "c1", wait=True, timeout_minutes=2)["action"] == "starting"
    )
    assert runtime.compute_stop(workspace, "c1", wait=True)["action"] == "terminating"
    assert runtime.warehouse_start(workspace, "w1", wait=True)["state"] == "RUNNING"
    assert runtime.warehouse_stop(workspace, "w1")["action"] == "stopping"
    assert runtime.run_sql(workspace, "SELECT 1", "w1", poll=0)["rows"] == [["1"]]
    assert ("start-wait", 120.0) in workspace.calls
    assert ("stop-wait", 1200.0) in workspace.calls


def test_runs():
    state = NS(life_cycle_state=State.RUNNING, result_state=None, state_message="")
    task = NS(task_key="right", state=state, execution_duration=1000)
    run = NS(
        run_id=7,
        job_id=9,
        run_name="reconcile",
        run_type=NS(value="JOB_RUN"),
        state=state,
        start_time=1_790_000_000_000,
        run_duration=2000,
        execution_duration=0,
        run_page_url="https://example/runs/7",
        tasks=[task],
    )
    workspace = sdk_client(runs=[run])
    assert runtime.list_runs(workspace)["runs"][0]["run_id"] == 7
    assert runtime.run_detail(workspace, 7)["tasks"][0]["task_key"] == "right"
    assert runtime.login(workspace, open_browser=False)["user"] == "me@example.com"


class Files:
    def __init__(self, objects=(), content=None):
        self.objects = {item.path: item for item in objects}
        self.content = content or {}
        self.calls = []

    def mkdirs(self, path):
        self.calls.append(("mkdirs", path))

    def upload(self, path, body, **options):
        self.calls.append(("upload", path, body, options))

    def get_status(self, path):
        if path not in self.objects:
            raise NotFound(path, error_code="RESOURCE_DOES_NOT_EXIST")
        return self.objects[path]

    def list(self, path):
        prefix = path.rstrip("/") + "/"
        return iter(
            item
            for name, item in sorted(self.objects.items())
            if name.startswith(prefix) and "/" not in name[len(prefix) :]
        )

    def export(self, path, *, format):
        body, file_type = self.content[path]
        return NS(content=base64.b64encode(body).decode(), file_type=file_type)

    def import_(self, path, **options):
        self.calls.append(("import", path, options))

    def delete(self, path, *, recursive=None):
        self.calls.append(("delete", path, recursive))
        prefix = path.rstrip("/") + "/"
        for name in list(self.objects):
            if name == path or (recursive and name.startswith(prefix)):
                del self.objects[name]


def item(path, kind, language=None):
    return NS(
        path=path,
        object_type=NS(value=kind),
        language=NS(value=language) if language else None,
    )


def test_upload(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "notebook.py").write_text(
        "# Databricks notebook source\nprint('ok')\n", encoding="utf-8"
    )
    (source / "data.txt").write_text("café", encoding="utf-8")
    files = Files()
    workspace = NS(workspace=files, current_user=NS(me=lambda: NS(user_name="me@example.com")))
    result = workspace_ops.upload(workspace, source, "/Shared/source", overwrite=True)
    paths = [call[1] for call in files.calls if call[0] == "upload"]
    assert result["uploaded"] == 2
    assert paths == ["/Shared/source/data.txt", "/Shared/source/notebook"]
    assert workspace_ops.upload(workspace, source, "/Shared/source", dry_run=True)["uploaded"] == 0
    assert workspace_ops.upload(workspace, source, "project/source", dry_run=True)["target"] == (
        "/Users/me@example.com/project/source"
    )


def test_download(tmp_path):
    objects = [
        item("/Shared/source", "DIRECTORY"),
        item("/Shared/source/empty", "DIRECTORY"),
        item("/Shared/source/notebook", "NOTEBOOK", "PYTHON"),
        item("/Shared/source/data.txt", "FILE"),
    ]
    content = {
        "/Shared/source/notebook": (b"# Databricks notebook source\n", "py"),
        "/Shared/source/data.txt": ("café".encode(), "txt"),
    }
    target = tmp_path / "source"
    result = workspace_ops.download(NS(workspace=Files(objects, content)), "/Shared/source", target)
    assert result["downloaded"] == 2
    assert (target / "empty").is_dir()
    assert (target / "notebook.py").is_file()
    assert (target / "data.txt").read_text(encoding="utf-8") == "café"
    with pytest.raises(ValueError, match="absolute"):
        workspace_ops.workspace_path("Shared/source")


def test_relative_workspace_path_uses_current_user():
    workspace = NS(current_user=NS(me=lambda: NS(user_name="me@example.com")))
    assert str(workspace_ops.resolve_workspace_path(workspace, "project/source")) == (
        "/Users/me@example.com/project/source"
    )
    assert str(workspace_ops.resolve_workspace_path(workspace, "/Shared/source")) == (
        "/Shared/source"
    )
    with pytest.raises(ValueError, match="invalid Workspace path"):
        workspace_ops.resolve_workspace_path(workspace, "../source")


def test_ls_lists_workspace_objects_recursively():
    objects = [
        item("/Users/me@example.com", "DIRECTORY"),
        item("/Users/me@example.com/source", "DIRECTORY"),
        item("/Users/me@example.com/source/notebook", "NOTEBOOK", "PYTHON"),
        item("/Users/me@example.com/data.txt", "FILE"),
    ]
    workspace = NS(
        workspace=Files(objects),
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
    )

    result = workspace_ops.ls(workspace)
    assert [entry["name"] for entry in result["entries"]] == ["data.txt", "source"]
    result = workspace_ops.ls(workspace, "source", recursive=True)
    assert result["entries"] == [
        {
            "path": "/Users/me@example.com/source/notebook",
            "name": "notebook",
            "type": "NOTEBOOK",
            "language": "PYTHON",
        }
    ]


def test_copy_and_move_workspace_objects():
    objects = [
        item("/Users/me@example.com/source", "DIRECTORY"),
        item("/Users/me@example.com/source/notebook", "NOTEBOOK", "PYTHON"),
        item("/Users/me@example.com/source/data.txt", "FILE"),
    ]
    content = {
        "/Users/me@example.com/source/notebook": (b"# Databricks notebook source\n", "py"),
        "/Users/me@example.com/source/data.txt": (b"data", "txt"),
    }
    files = Files(objects, content)
    workspace = NS(
        workspace=files,
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
    )

    with pytest.raises(IsADirectoryError, match="--recursive"):
        workspace_ops.copy(workspace, "source", "archive")
    result = workspace_ops.copy(workspace, "source", "archive", recursive=True)
    assert result["paths"] == [
        "/Users/me@example.com/archive/data.txt",
        "/Users/me@example.com/archive/notebook",
    ]
    imports = [call for call in files.calls if call[0] == "import"]
    assert imports[0][2]["format"].value == "AUTO"
    assert imports[1][2]["format"].value == "SOURCE"
    assert imports[1][2]["language"].value == "PYTHON"

    file_objects = [item("/Users/me@example.com/old.py", "FILE")]
    moving = Files(file_objects, {"/Users/me@example.com/old.py": (b"value", "py")})
    workspace.workspace = moving
    result = workspace_ops.move(workspace, "old.py", "new.py")
    assert result["moved"] == 1 and result["failed"] == []
    assert moving.calls[-1] == ("delete", "/Users/me@example.com/old.py", True)


def test_copy_rejects_existing_or_descendant_target():
    objects = [
        item("/Users/me@example.com/source", "DIRECTORY"),
        item("/Users/me@example.com/archive", "DIRECTORY"),
        item("/Users/me@example.com/archive/source", "DIRECTORY"),
    ]
    workspace = NS(
        workspace=Files(objects),
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
    )
    with pytest.raises(FileExistsError, match="--force"):
        workspace_ops.copy(workspace, "source", "archive", recursive=True)
    with pytest.raises(ValueError, match="into itself"):
        workspace_ops.copy(workspace, "source", "source/child", recursive=True)


def test_move_keeps_source_when_copy_fails():
    class BrokenFiles(Files):
        def export(self, path, *, format):
            raise RuntimeError("export failed")

    source = item("/Users/me@example.com/old.py", "FILE")
    files = BrokenFiles([source])
    workspace = NS(
        workspace=files,
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
    )

    result = workspace_ops.move(workspace, "old.py", "new.py")
    assert result["exit_code"] == 1 and result["moved"] == 0
    assert not [call for call in files.calls if call[0] == "delete"]
    assert "/Users/me@example.com/old.py" in files.objects


def test_rm_matches_linux_semantics():
    objects = [
        item("/Users/me@example.com/old.py", "FILE"),
        item("/Users/me@example.com/folder", "DIRECTORY"),
        item("/Users/me@example.com/folder/child.py", "FILE"),
    ]
    files = Files(objects)
    workspace = NS(
        workspace=files,
        current_user=NS(me=lambda: NS(user_name="me@example.com")),
    )

    result = workspace_ops.rm(workspace, "old.py")
    assert result["deleted"] == 1 and result["failed"] == []
    assert files.calls[-1] == ("delete", "/Users/me@example.com/old.py", False)

    result = workspace_ops.rm(workspace, ["folder", "missing"], force=True)
    assert result["deleted"] == 0
    assert result["failed"][0]["error"].startswith("IsADirectoryError:")

    result = workspace_ops.rm(workspace, "folder", recursive=True, dry_run=True)
    assert result["planned"] == 1 and result["deleted"] == 0
    assert result["paths"] == ["/Users/me@example.com/folder"]

    result = workspace_ops.rm(workspace, "folder", recursive=True)
    assert result["deleted"] == 1 and result["failed"] == []
    assert files.calls[-1] == ("delete", "/Users/me@example.com/folder", True)
