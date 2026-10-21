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


@pytest.mark.parametrize("name", ["pair-recon", "1right", "class", ""])
def test_right_package_rejects_invalid_names(name):
    if name == "":
        assert deploy.right_package(name) == "pair_recon_right"
        return
    with pytest.raises(ValueError, match="DBX_RIGHT_PACKAGE"):
        deploy.right_package(name)


def test_right_target_requires_parent_and_uses_name(monkeypatch):
    monkeypatch.setattr(
        deploy, "resolve_workspace_path", lambda _ws, path: PurePosixPath("/Users/me") / path
    )
    assert deploy.right_target(object(), "apps", "pair_recon") == "/Users/me/apps/pair_recon"
    with pytest.raises(ValueError, match="DBX_WORKSPACE_PDIR"):
        deploy.right_target(object(), "", "pair_recon")


def test_repoint_job_updates_only_when_path_differs():
    client, calls = job_client("/Users/me/apps/pair_recon_right/notebook")
    assert deploy.repoint_job(client, 7, "/Users/me/apps/pair_recon/notebook")
    body = calls[0][1]["body"]
    assert body["job_id"] == 7
    assert body["new_settings"]["tasks"][0]["notebook_task"]["notebook_path"] == (
        "/Users/me/apps/pair_recon/notebook"
    )
    client, calls = job_client("/Users/me/apps/pair_recon/notebook")
    assert not deploy.repoint_job(client, 7, "/Users/me/apps/pair_recon/notebook")
    assert calls == []


def test_sync_right_uploads_local_package_and_repoints(monkeypatch):
    seen = {}
    monkeypatch.setattr(deploy, "right_target", lambda _ws, parent, name: f"/W/{parent}/{name}")
    monkeypatch.setattr(
        deploy,
        "upload",
        lambda _ws, source, target: seen.update(source=source, target=target) or (["a"], ["b"]),
    )
    monkeypatch.setattr(
        deploy, "repoint_job", lambda _ws, job_id, notebook: seen.update(job=(job_id, notebook))
    )
    result = deploy.sync_right(object(), "apps", "pair_recon", "7")
    assert seen["source"].name == "pair_recon_right"
    assert seen["target"] == "/W/apps/pair_recon"
    assert seen["job"] == (7, "/W/apps/pair_recon/notebook")
    assert result["uploaded"] == 1 and result["deleted"] == ["b"]
