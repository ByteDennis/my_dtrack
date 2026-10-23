from pathlib import PurePosixPath
from types import SimpleNamespace as NS

import pytest

from pair_recon_dbx import deploy


class Task:
    def __init__(self, value):
        self.value = value

    def as_dict(self):
        return {**self.value, "notebook_task": dict(self.value["notebook_task"])}


def job_client(path):
    calls = []
    settings = NS(tasks=[Task({"task_key": "right", "notebook_task": {"notebook_path": path}})])
    return NS(
        jobs=NS(get=lambda job_id: NS(settings=settings)),
        api_client=NS(do=lambda *args, **kwargs: calls.append((args, kwargs))),
    ), calls


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("", "pair_recon_right"),
        ("pair_recon_right -> pair_recon", "pair_recon"),
        ("pair_recon_right->pr", "pr"),
    ],
)
def test_remote_package_parses_rename_spec(spec, expected):
    assert deploy.remote_package(spec) == expected


@pytest.mark.parametrize(
    "spec",
    [
        "pair_recon",
        "other -> pair_recon",
        "pair_recon_right -> pair-recon",
        "pair_recon_right -> class",
    ],
)
def test_remote_package_rejects_bad_specs(spec):
    with pytest.raises(ValueError, match="DBX_PKG_RENAME"):
        deploy.remote_package(spec)


def test_right_target_requires_parent_and_uses_remote_package(monkeypatch):
    monkeypatch.setattr(
        deploy, "resolve_workspace_path", lambda _ws, path: PurePosixPath("/Users/me") / path
    )
    monkeypatch.delenv("DBX_PKG_RENAME", raising=False)
    assert deploy.right_target(object(), "apps") == "/Users/me/apps/pair_recon_right"
    monkeypatch.setenv("DBX_PKG_RENAME", "pair_recon_right -> pair_recon")
    assert deploy.right_target(object(), "apps") == "/Users/me/apps/pair_recon"
    assert deploy.right_notebook("/Users/me/apps/pair_recon") == ("/Users/me/apps/pair_recon_job")
    with pytest.raises(ValueError, match="DBX_WORKSPACE_PDIR"):
        deploy.right_target(object(), "")


def test_repoint_job_updates_only_when_path_differs():
    client, calls = job_client("/Users/me/apps/pair_recon/notebook")
    assert deploy.repoint_job(client, 7, "/Users/me/apps/pair_recon_job")
    body = calls[0][1]["body"]
    assert body["job_id"] == 7
    assert body["new_settings"]["tasks"][0]["notebook_task"]["notebook_path"] == (
        "/Users/me/apps/pair_recon_job"
    )
    client, calls = job_client("/Users/me/apps/pair_recon_job")
    assert not deploy.repoint_job(client, 7, "/Users/me/apps/pair_recon_job")
    assert calls == []


def test_create_job_returns_databricks_assigned_id():
    calls = []
    workspace = NS(
        api_client=NS(do=lambda *args, **kwargs: calls.append((args, kwargs)) or {"job_id": 314})
    )

    assert deploy.create_job(workspace, "daily", "/Users/me/right_job", "cluster-1") == 314
    args, kwargs = calls[0]
    assert args == ("POST", "/api/2.2/jobs/create")
    assert kwargs["body"] == deploy.job_spec("daily", "/Users/me/right_job", "cluster-1")


def test_sync_right_uploads_local_package_and_repoints(monkeypatch):
    seen = {}
    monkeypatch.setattr(deploy, "right_target", lambda _ws, parent: f"/W/{parent}/pair_recon")
    monkeypatch.setattr(
        deploy,
        "upload",
        lambda _ws, source, target: seen.update(source=source, target=target) or (["a"], ["b"]),
    )
    monkeypatch.setattr(
        deploy, "repoint_job", lambda _ws, job_id, notebook: seen.update(job=(job_id, notebook))
    )
    result = deploy.sync_right(object(), "apps", "7")
    assert seen["source"].name == "pair_recon_right"
    assert seen["target"] == "/W/apps/pair_recon"
    assert seen["job"] == (7, "/W/apps/pair_recon_job")
    assert result["notebook"] == "/W/apps/pair_recon_job"
    assert result["uploaded"] == 1 and result["deleted"] == ["b"]


def test_notebook_body_rewrites_package_imports(tmp_path):
    source = tmp_path / "notebook.py"
    source.write_text(
        '# MAGIC %pip install -r "__PAIR_RECON_RIGHT_REQUIREMENTS__"\n'
        "from pair_recon_right.catalog import publish\n"
        "from pair_recon_right.engine.job import run\n"
        "from pair_recon_left.backends import sources\n"
        "name = 'pair_recon_rightmost'\n",
        encoding="utf-8",
    )
    body = deploy.notebook_body(source, "pair_recon", "/Users/me/apps/pair_recon").decode("utf-8")
    assert '%pip install -r "/Workspace/Users/me/apps/pair_recon/requirements.txt"' in body
    assert "from pair_recon.catalog import publish" in body
    assert "from pair_recon.engine.job import run" in body
    assert "pair_recon_left.backends" in body
    assert "pair_recon_rightmost" in body
    assert "pair_recon_right." not in body


def test_right_package_only_names_itself_in_the_notebook():
    offenders = [
        str(path.relative_to(deploy.right_source()))
        for path in deploy.right_source().rglob("*.py")
        if path.name != "notebook.py" and "pair_recon_right" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []
