import keyword
import os
import re
from pathlib import Path, PurePosixPath

from .workspace import resolve_workspace_path

RIGHT_PACKAGE = "pair_recon_right"
WORKSPACE_DIR_ENV = "DBX_WORKSPACE_PDIR"
RENAME_ENV = "DBX_PKG_RENAME"
JOB_NAME_ENV = "DBX_JOB_NAME"
DEFAULT_JOB_NAME = "pair_recon_right"
_PACKAGE_NAME = re.compile(rb"\b" + RIGHT_PACKAGE.encode() + rb"\b")


# >>> remote package name from DBX_PKG_RENAME="pair_recon_right -> pair_recon" <<< #
def remote_package(spec: str | None = None) -> str:
    value = (os.getenv(RENAME_ENV, "") if spec is None else spec).strip()
    if not value:
        return RIGHT_PACKAGE
    local, arrow, remote = value.partition("->")
    local, remote = local.strip(), remote.strip()
    if (
        not arrow
        or local != RIGHT_PACKAGE
        or not remote.isidentifier()
        or keyword.iskeyword(remote)
    ):
        raise ValueError(f"{RENAME_ENV} must look like '{RIGHT_PACKAGE} -> new_package_name'")
    return remote


# >>> local pair_recon_right source folder <<< #
def right_source() -> Path:
    import pair_recon_right

    return Path(pair_recon_right.__file__).resolve().parent


# >>> remote workspace folder holding the right package <<< #
def right_target(workspace, parent: str | None) -> str:
    if not str(parent or "").strip():
        raise ValueError(f"{WORKSPACE_DIR_ENV} is required")
    return str(resolve_workspace_path(workspace, str(parent)) / remote_package())


# >>> job notebook sits beside the importable right package <<< #
def right_notebook(target: str) -> str:
    root = PurePosixPath(target)
    return str(root.parent / f"{root.name}_job")


# >>> notebook source with imports pointed at the remote package name <<< #
def notebook_body(path: Path, remote: str) -> bytes:
    return _PACKAGE_NAME.sub(remote.encode(), path.read_bytes())


# >>> mirror a local folder to a workspace folder <<< #
def upload(workspace, source: Path, target: str):
    from databricks.sdk.service.workspace import ImportFormat, Language

    root = PurePosixPath(target)
    workspace.workspace.mkdirs(str(root))
    uploaded = []
    expected = {str(root)}
    for path in sorted(source.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        remote = root.joinpath(*path.relative_to(source).parts)
        body = path.read_bytes()
        options = {"format": ImportFormat.AUTO, "overwrite": True}
        if path.name == "notebook.py":
            remote = PurePosixPath(right_notebook(str(root)))
            body = notebook_body(path, root.name)
            options = {
                "format": ImportFormat.SOURCE,
                "language": Language.PYTHON,
                "overwrite": True,
            }
        workspace.workspace.mkdirs(str(remote.parent))
        workspace.workspace.upload(str(remote), body, **options)
        uploaded.append(str(remote))
        expected.add(str(remote))
        parent = remote.parent
        while parent != root.parent:
            expected.add(str(parent))
            if parent == root:
                break
            parent = parent.parent
    stale = [item for item in workspace_objects(workspace, root) if item.path not in expected]
    deleted = []
    for item in sorted(
        stale,
        key=lambda value: len(PurePosixPath(value.path).parts),
        reverse=True,
    ):
        kind = getattr(item.object_type, "value", item.object_type)
        workspace.workspace.delete(item.path, recursive=kind in {"DIRECTORY", "REPO"})
        deleted.append(item.path)
    return uploaded, deleted


# >>> recursive workspace listing <<< #
def workspace_objects(workspace, root: PurePosixPath):
    objects = []

    def visit(path: str):
        for item in workspace.workspace.list(path):
            objects.append(item)
            kind = getattr(item.object_type, "value", item.object_type)
            if kind in {"DIRECTORY", "REPO"}:
                visit(item.path)

    visit(str(root))
    return objects


# >>> point a job's notebook tasks at the synced notebook <<< #
def repoint_job(workspace, job_id: int, notebook: str) -> bool:
    settings = workspace.jobs.get(job_id).settings
    tasks = [task.as_dict() for task in (getattr(settings, "tasks", None) or [])]
    changed = False
    for task in tasks:
        current = (task.get("notebook_task") or {}).get("notebook_path")
        if current is not None and current != notebook:
            task["notebook_task"]["notebook_path"] = notebook
            changed = True
    if changed:
        workspace.api_client.do(
            "POST",
            "/api/2.2/jobs/update",
            body={"job_id": int(job_id), "new_settings": {"tasks": tasks}},
        )
    return changed


# >>> Databricks Jobs API payload for the single Right notebook task <<< #
def job_spec(name: str, notebook: str, cluster_id: str) -> dict:
    return {
        "name": name,
        "max_concurrent_runs": 10,
        "queue": {"enabled": True},
        "timeout_seconds": 0,
        "tasks": [
            {
                "task_key": "right",
                "existing_cluster_id": cluster_id,
                "notebook_task": {"notebook_path": notebook, "base_parameters": {}},
            }
        ],
    }


# >>> create a Right job and return the Databricks-assigned numeric id <<< #
def create_job(workspace, name: str, notebook: str, cluster_id: str) -> int:
    response = workspace.api_client.do(
        "POST",
        "/api/2.2/jobs/create",
        body=job_spec(name, notebook, cluster_id),
    )
    return int(response["job_id"])


# >>> sync local pair_recon_right to the workspace (optionally renamed) and repoint the right job <<< #
def sync_right(workspace, parent: str | None, job_id=None) -> dict:
    target = right_target(workspace, parent)
    uploaded, deleted = upload(workspace, right_source(), target)
    notebook = right_notebook(target)
    result = {
        "target": target,
        "notebook": notebook,
        "uploaded": len(uploaded),
        "deleted": deleted,
    }
    if job_id is not None:
        result["job_id"] = int(job_id)
        result["job_updated"] = repoint_job(workspace, int(job_id), notebook)
    return result
