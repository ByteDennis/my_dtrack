import json

import pytest

from dtrack_left.cli import main
from dtrack_left.cli.ops import dbx


def run_tool(argv):
    return main(["tools", *argv])


# >>> dtrack tools dbx: every subcommand reaches its backend function with the resolved ids <<< #
@pytest.fixture
def dbx_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(dbx.databricks, "workspace", lambda profile: f"w:{profile}")
    for name in (
        "login",
        "compute_status",
        "compute_start",
        "compute_stop",
        "warehouse_status",
        "warehouse_start",
        "warehouse_stop",
        "run_sql",
        "list_runs",
        "run_detail",
        "upload_workspace",
        "download_workspace",
    ):
        monkeypatch.setattr(
            dbx.databricks,
            name,
            lambda *a, _n=name, **k: calls.append((_n, a, k)) or {"called": _n},
        )
    return calls


@pytest.mark.parametrize(
    ("argv", "call"),
    [
        (["dbx", "login", "--no-open"], ("login", ("w:corp",), {"open_browser": False})),
        (["dbx", "compute", "--cluster-id", "c1"], ("compute_status", ("w:corp", "c1"), {})),
        (
            ["dbx", "compute", "start", "--cluster-id", "c1", "--wait", "--timeout", "5"],
            ("compute_start", ("w:corp", "c1"), {"wait": True, "timeout_minutes": 5}),
        ),
        (
            ["dbx", "compute", "stop", "--cluster-id", "c1", "--profile", "work"],
            ("compute_stop", ("w:work", "c1"), {"wait": False, "timeout_minutes": 20}),
        ),
        (["dbx", "warehouse", "--warehouse-id", "wh"], ("warehouse_status", ("w:corp", "wh"), {})),
        (
            ["dbx", "warehouse", "start", "--warehouse-id", "wh"],
            ("warehouse_start", ("w:corp", "wh"), {"wait": False, "timeout_minutes": 20}),
        ),
        (
            ["dbx", "warehouse", "stop", "--warehouse-id", "wh"],
            ("warehouse_stop", ("w:corp", "wh"), {"wait": False, "timeout_minutes": 20}),
        ),
        (
            ["dbx", "sql", "SELECT 1", "--warehouse-id", "wh", "--limit", "3"],
            ("run_sql", ("w:corp", "SELECT 1", "wh"), {"limit": 3}),
        ),
        (
            ["dbx", "runs", "--job-id", "7", "--active"],
            ("list_runs", ("w:corp",), {"limit": 10, "job_id": 7, "active": True}),
        ),
        (["dbx", "runs", "--run-id", "42"], ("run_detail", ("w:corp", 42), {})),
    ],
)
def test_dbx_commands_call_their_backend(dbx_calls, capsys, argv, call):
    assert run_tool(argv) == 0
    assert dbx_calls == [call]
    assert json.loads(capsys.readouterr().out) == {"called": call[0]}


def test_dbx_ids_and_profile_come_from_the_environment(dbx_calls, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DTRACK_DBX_PROFILE", "work")
    monkeypatch.setenv("DTRACK_DBX_CLUSTER_ID", "c9")
    monkeypatch.setenv("DTRACK_DBX_WAREHOUSE_ID", "wh9")
    (tmp_path / "q.sql").write_text("SELECT 2")
    assert (
        run_tool(["dbx", "compute"]) == 0
        and run_tool(["dbx", "sql", "--file", str(tmp_path / "q.sql")]) == 0
    )
    assert dbx_calls[0][1] == ("w:work", "c9") and dbx_calls[1][1] == ("w:work", "SELECT 2", "wh9")


def test_dbx_workspace_upload_calls_backend_with_paths_and_flags(dbx_calls, tmp_path, capsys):
    source = tmp_path / "right"
    source.mkdir()

    assert (
        run_tool(
            [
                "dbx",
                "workspace",
                "upload",
                str(source),
                "/Users/me/right",
                "--profile",
                "work",
                "--overwrite",
                "--dry-run",
            ]
        )
        == 0
    )

    assert dbx_calls == [
        (
            "upload_workspace",
            ("w:work", source, "/Users/me/right"),
            {"overwrite": True, "dry_run": True},
        )
    ]
    assert json.loads(capsys.readouterr().out) == {"called": "upload_workspace"}


def test_dbx_workspace_upload_keeps_backend_exit_code(dbx_calls, monkeypatch, tmp_path, capsys):
    source = tmp_path / "a.txt"
    source.write_text("a")
    monkeypatch.setattr(
        dbx.databricks,
        "upload_workspace",
        lambda *_args, **_kwargs: {"uploaded": 0, "failed": ["a.txt"], "exit_code": 1},
    )

    assert run_tool(["dbx", "workspace", "upload", str(source)]) == 1
    assert json.loads(capsys.readouterr().out) == {"failed": ["a.txt"], "uploaded": 0}


def test_dbx_workspace_upload_requires_an_existing_source(dbx_calls, tmp_path):
    assert run_tool(["dbx", "workspace", "upload"]) == 2
    assert run_tool(["dbx", "workspace", "upload", str(tmp_path / "missing")]) == 2


def test_dbx_workspace_download_calls_backend_with_paths_and_flags(
    dbx_calls, tmp_path, capsys
):
    target = tmp_path / "right"

    assert (
        run_tool(
            [
                "dbx",
                "workspace",
                "download",
                "/Users/me/right",
                str(target),
                "--profile",
                "work",
                "--overwrite",
                "--dry-run",
            ]
        )
        == 0
    )

    assert dbx_calls == [
        (
            "download_workspace",
            ("w:work", "/Users/me/right", target),
            {"overwrite": True, "dry_run": True},
        )
    ]
    assert json.loads(capsys.readouterr().out) == {"called": "download_workspace"}


def test_dbx_workspace_download_requires_an_absolute_source(dbx_calls, tmp_path):
    assert run_tool(["dbx", "workspace", "download"]) == 2
    assert run_tool(["dbx", "workspace", "download", "Shared/right", str(tmp_path)]) == 2


def test_dbx_usage_errors_exit_two_and_sdk_errors_exit_one(dbx_calls, monkeypatch, capsys):
    monkeypatch.delenv("DTRACK_DBX_CLUSTER_ID", raising=False)
    monkeypatch.delenv("DTRACK_DBX_WAREHOUSE_ID", raising=False)
    assert run_tool(["dbx", "compute"]) == 2
    assert run_tool(["dbx", "sql", "--warehouse-id", "wh"]) == 2
    capsys.readouterr()

    def missing(*_args, **_kwargs):
        raise OSError("Cluster nope does not exist")

    monkeypatch.setattr(dbx.databricks, "compute_status", missing)
    assert run_tool(["dbx", "compute", "--cluster-id", "nope"]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "OSError: Cluster nope does not exist"}


def test_dbx_reads_its_defaults_from_dotenv(dbx_calls, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    for name in ("DTRACK_DBX_PROFILE", "DTRACK_DBX_CLUSTER_ID"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".env").write_text("DTRACK_DBX_PROFILE=dotenv\nDTRACK_DBX_CLUSTER_ID=c-dotenv\n")
    assert run_tool(["dbx", "compute"]) == 0
    assert dbx_calls == [("compute_status", ("w:dotenv", "c-dotenv"), {})]
    for name in ("DTRACK_DBX_PROFILE", "DTRACK_DBX_CLUSTER_ID"):
        monkeypatch.delenv(name, raising=False)
