import keyword
from pathlib import Path, PurePosixPath

from .workspace import resolve_workspace_path

RIGHT_PACKAGE = "pair_recon_right"
RIGHT_PACKAGE_ENV = "DBX_RIGHT_PACKAGE"
WORKSPACE_DIR_ENV = "DBX_WORKSPACE_PDIR"


# >>> local pair_recon_right source folder <<< #
def right_source() -> Path:
    import pair_recon_right

    return Path(pair_recon_right.__file__).resolve().parent


# >>> validate the remote right package folder name <<< #
def right_package(name: str | None = None) -> str:
    value = str(name or RIGHT_PACKAGE).strip()
    if not value.isidentifier() or keyword.iskeyword(value):
        raise ValueError(f"{RIGHT_PACKAGE_ENV} must be a Python package name: {value!r}")
    return value


# >>> remote workspace folder holding the right package <<< #
def right_target(workspace, parent: str | None, name: str | None = None) -> str:
    if not str(parent or "").strip():
        raise ValueError(f"{WORKSPACE_DIR_ENV} is required")
    return str(resolve_workspace_path(workspace, str(parent)) / right_package(name))


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
        workspace.workspace.mkdirs(str(remote.parent))
        options = {"format": ImportFormat.AUTO, "overwrite": True}
        if path.name == "notebook.py":
            remote = remote.with_suffix("")
            options = {
                "format": ImportFormat.SOURCE,
                "language": Language.PYTHON,
                "overwrite": True,
            }
        workspace.workspace.upload(str(remote), path.read_bytes(), **options)
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


# >>> sync local pair_recon_right to the workspace and the right job <<< #
def sync_right(workspace, parent: str | None, name: str | None = None, job_id=None) -> dict:
    target = right_target(workspace, parent, name)
    uploaded, deleted = upload(workspace, right_source(), target)
    result = {"target": target, "uploaded": len(uploaded), "deleted": deleted}
    if job_id is not None:
        result["job_id"] = int(job_id)
        result["job_updated"] = repoint_job(workspace, int(job_id), target + "/notebook")
    return result
