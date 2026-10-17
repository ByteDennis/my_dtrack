from __future__ import annotations

import base64
import os
import subprocess
import time
import webbrowser
from datetime import UTC, datetime, timedelta
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any

DEFAULT_PROFILE = "corp"
RUNNING_STATEMENT = {"PENDING", "RUNNING"}
WORKSPACE_SOURCE_LANGUAGES = {".py": "PYTHON", ".r": "R"}
WORKSPACE_NOTEBOOK_HEADER = "# databricks notebook"


# >>> a WorkspaceClient for a ~/.databrickscfg profile; external-browser profiles log in here <<< #
def workspace(profile: str):
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ImportError("dtrack tools dbx requires dtrack[dbx] (databricks-sdk)") from exc
    return WorkspaceClient(profile=profile)


# >>> enum members as their plain value <<< #
def _value(item: Any) -> Any:
    return getattr(item, "value", item)


# >>> epoch milliseconds as an ISO UTC timestamp <<< #
def _stamp(millis: int | None) -> str | None:
    return (
        datetime.fromtimestamp(millis / 1000, UTC).isoformat(timespec="seconds") if millis else None
    )


# >>> time since epoch milliseconds as <h>h<mm>m <<< #
def _elapsed(millis: int | None, now: float | None = None) -> str | None:
    if not millis:
        return None
    minutes = int(((now or time.time()) - millis / 1000) // 60)
    return f"{minutes // 60}h{minutes % 60:02d}m"


# >>> open the workspace in the default browser (Windows `start`, macOS `open`) and confirm who is logged in <<< #
def login(w, *, open_browser: bool = True, opener=webbrowser.open) -> dict:
    opened = bool(opener(w.config.host)) if open_browser else False
    me = w.current_user.me()
    return {
        "host": w.config.host,
        "profile": w.config.profile,
        "auth_type": w.config.auth_type,
        "user": me.user_name,
        "browser_opened": opened,
    }


# >>> local file/folder -> Workspace files; source notebooks lose their language suffix remotely <<< #
def upload_workspace(
    w,
    source: str | Path,
    target: str | None = None,
    *,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    from databricks.sdk.service.workspace import ImportFormat, Language

    source = Path(source)
    if source.is_symlink():
        raise ValueError(f"source must not be a symbolic link: {source}")
    if not source.exists():
        raise ValueError(f"source does not exist: {source}")
    if not source.is_file() and not source.is_dir():
        raise ValueError(f"source must be a file or directory: {source}")

    if target is None:
        user = w.current_user.me().user_name
        target = f"/Users/{user}/{source.name}"
    remote_root = _workspace_path(target)

    if source.is_file():
        local_files = [source]
        skipped = 0
    else:
        local_files, skipped = _workspace_local_files(source)

    uploads = []
    directories = {remote_root.parent if source.is_file() else remote_root}
    for local in local_files:
        relative = Path(local.name) if source.is_file() else local.relative_to(source)
        remote = remote_root.joinpath(*relative.parts) if source.is_dir() else remote_root
        notebook = _workspace_notebook(local)
        if notebook == "JUPYTER":
            remote = remote.with_suffix("")
            format_ = ImportFormat.JUPYTER
            language_ = None
        elif notebook:
            remote = remote.with_suffix("")
            format_ = ImportFormat.SOURCE
            language_ = Language(notebook)
        else:
            format_ = ImportFormat.AUTO
            language_ = None
        directories.add(remote.parent)
        uploads.append((local, remote, format_, language_))

    ordered_directories = sorted(directories, key=lambda path: (len(path.parts), str(path)))
    failures = []
    uploaded = []
    if not dry_run:
        for directory in ordered_directories:
            try:
                w.workspace.mkdirs(str(directory))
            except Exception as exc:
                failures.append({"path": str(directory), "error": f"{type(exc).__name__}: {exc}"})
        for local, remote, format_, language in uploads:
            try:
                options = {"format": format_, "overwrite": overwrite}
                if language is not None:
                    options["language"] = language
                w.workspace.upload(str(remote), local.read_bytes(), **options)
                uploaded.append(str(remote))
            except Exception as exc:
                failures.append({"path": str(remote), "error": f"{type(exc).__name__}: {exc}"})

    planned = [str(remote) for _local, remote, _format, _language in uploads]
    return {
        "source": str(source),
        "target": str(remote_root),
        "dry_run": dry_run,
        "planned": len(uploads),
        "uploaded": len(uploaded),
        "directories": len(ordered_directories),
        "skipped": skipped,
        "paths": planned if dry_run else uploaded,
        "failed": failures,
        "exit_code": 1 if failures else 0,
    }


# >>> Workspace files/notebooks -> a local file or mirrored tree; notebooks regain the exported language suffix <<< #
def download_workspace(
    w,
    source: str,
    target: str | Path | None = None,
    *,
    overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    from databricks.sdk.service.workspace import ExportFormat

    remote_root = _workspace_path(source)
    root = w.workspace.get_status(str(remote_root))
    root_type = _value(root.object_type)
    is_directory = root_type in {"DIRECTORY", "REPO"}
    local_root = Path(target) if target is not None else Path(remote_root.name)
    if local_root.is_symlink():
        raise ValueError(f"target must not be a symbolic link: {local_root}")
    if is_directory and local_root.exists() and not local_root.is_dir():
        raise ValueError(f"directory download target is not a directory: {local_root}")
    if not is_directory and local_root.is_dir():
        local_root = local_root / remote_root.name

    directories, objects = _workspace_download_objects(w, root)
    planned_paths = []
    downloaded_paths = []
    failures = []
    downloaded_bytes = 0
    if is_directory and not dry_run:
        for item in directories:
            remote = PurePosixPath(item.path)
            relative = remote.relative_to(remote_root)
            destination = local_root.joinpath(*relative.parts)
            try:
                _validate_workspace_relative(relative)
                _validate_download_target(local_root, destination, is_directory=True)
                if destination.exists() and not destination.is_dir():
                    raise NotADirectoryError("directory path is an existing file")
                destination.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                failures.append({"path": str(destination), "error": f"{type(exc).__name__}: {exc}"})
    for item in objects:
        remote = PurePosixPath(item.path)
        relative = remote.relative_to(remote_root) if is_directory else PurePosixPath(remote.name)
        _validate_workspace_relative(relative)
        destination = local_root.joinpath(*relative.parts) if is_directory else local_root
        try:
            if dry_run:
                destination = _workspace_download_path(destination, item, None)
                planned_paths.append(str(destination))
                continue
            response = w.workspace.export(str(remote), format=ExportFormat.AUTO)
            content = response.content
            if content is None:
                raise ValueError("Workspace export returned no content")
            destination = _workspace_download_path(destination, item, response.file_type)
            body = base64.b64decode(content, validate=True)
            planned_paths.append(str(destination))
            _validate_download_target(local_root, destination, is_directory=is_directory)
            if destination.exists() and not overwrite:
                raise FileExistsError("target exists; pass --overwrite to replace it")
            if destination.exists() and destination.is_dir():
                raise IsADirectoryError("target path is a directory")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(destination.name + ".dtrack.part")
            try:
                temporary.write_bytes(body)
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
            downloaded_paths.append(str(destination))
            downloaded_bytes += len(body)
        except Exception as exc:
            failures.append({"path": str(destination), "error": f"{type(exc).__name__}: {exc}"})

    return {
        "source": str(remote_root),
        "target": str(local_root),
        "dry_run": dry_run,
        "planned": len(objects),
        "downloaded": len(downloaded_paths),
        "directories": len(directories),
        "bytes": downloaded_bytes,
        "paths": planned_paths if dry_run else downloaded_paths,
        "failed": failures,
        "exit_code": 1 if failures else 0,
    }


def _workspace_download_objects(w, root) -> tuple[list[Any], list[Any]]:
    directories = []
    objects = []

    def visit(item):
        type_ = _value(item.object_type)
        if type_ not in {"DIRECTORY", "REPO"}:
            objects.append(item)
            return
        directories.append(item)
        for child in sorted(w.workspace.list(item.path), key=lambda row: row.path or ""):
            visit(child)

    visit(root)
    return directories, objects


def _workspace_download_path(destination: Path, item, file_type: str | None) -> Path:
    if _value(item.object_type) != "NOTEBOOK" or destination.suffix:
        return destination
    fallback = {
        "PYTHON": "py",
        "R": "r",
        "SQL": "sql",
        "SCALA": "scala",
    }.get(_value(item.language))
    suffix = (file_type or fallback or "").strip().lstrip(".")
    if not suffix or Path(suffix).name != suffix or any(char in suffix for char in "/\\"):
        raise ValueError(f"invalid Workspace export file type: {file_type!r}")
    return destination.with_name(f"{destination.name}.{suffix}")


def _validate_workspace_relative(path: PurePosixPath) -> None:
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"invalid Workspace child path: {path}")


def _validate_download_target(root: Path, destination: Path, *, is_directory: bool) -> None:
    boundary = root.parent if is_directory else destination.parent
    current = destination
    while current != boundary:
        if current.is_symlink():
            raise ValueError(f"download target contains a symbolic link: {current}")
        current = current.parent


def _workspace_path(path: str) -> PurePosixPath:
    if not path.startswith("/"):
        raise ValueError(f"workspace target must be absolute: {path}")
    remote = PurePosixPath(path)
    if ".." in remote.parts or remote == PurePosixPath("/"):
        raise ValueError(f"invalid workspace target: {path}")
    return remote


def _workspace_local_files(root: Path) -> tuple[list[Path], int]:
    git_files = _git_workspace_files(root)
    if git_files is not None:
        return git_files

    files = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"source tree contains a symbolic link: {path}")
        if path.is_file():
            files.append(path)
    return files, 0


def _git_workspace_files(root: Path) -> tuple[list[Path], int] | None:
    repository = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if repository.returncode != 0:
        return None

    worktree = Path(repository.stdout.strip()).resolve()
    relative_root = root.resolve().relative_to(worktree)
    use_ignores = _has_gitignore(root.resolve(), worktree)
    command = ["git", "-C", str(worktree), "ls-files", "-z", "--cached", "--others"]
    if use_ignores:
        command.append("--exclude-standard")
    command.extend(["--", relative_root.as_posix()])
    listed = subprocess.run(command, capture_output=True, check=True).stdout

    files = []
    for name in filter(None, listed.decode().split("\0")):
        path = worktree / name
        if path.is_symlink():
            raise ValueError(f"source tree contains a symbolic link: {path}")
        if path.is_file():
            files.append(root / path.relative_to(root.resolve()))

    skipped = 0
    if use_ignores:
        ignored = subprocess.run(
            [
                "git",
                "-C",
                str(worktree),
                "ls-files",
                "-z",
                "--others",
                "--ignored",
                "--exclude-standard",
                "--",
                relative_root.as_posix(),
            ],
            capture_output=True,
            check=True,
        ).stdout
        skipped = sum(bool(name) for name in ignored.decode().split("\0"))
    return sorted(files), skipped


def _has_gitignore(root: Path, worktree: Path) -> bool:
    current = root
    while True:
        if (current / ".gitignore").is_file():
            return True
        if current == worktree:
            break
        current = current.parent
    return any(path.is_file() for path in root.rglob(".gitignore"))


def _workspace_notebook(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".ipynb":
        return "JUPYTER"
    language = WORKSPACE_SOURCE_LANGUAGES.get(suffix)
    if language is None:
        return None
    with path.open("rb") as stream:
        first_line = stream.readline(256).decode("utf-8-sig", errors="replace").rstrip("\r\n")
    return language if first_line.casefold().startswith(WORKSPACE_NOTEBOOK_HEADER) else None


# >>> one cluster: state, EC2 node types and instance ids, uptime since the last (re)start <<< #
def compute_status(w, cluster_id: str, *, now: float | None = None) -> dict:
    cluster = w.clusters.get(cluster_id)
    state = _value(cluster.state)
    running = state in {"RUNNING", "RESIZING"}
    since = cluster.last_restarted_time or cluster.start_time
    driver = cluster.driver
    autoscale = cluster.autoscale
    return {
        "cluster_id": cluster_id,
        "name": cluster.cluster_name,
        "state": state,
        "message": cluster.state_message or None,
        "spark_version": cluster.spark_version,
        "driver_node_type": cluster.driver_node_type_id or cluster.node_type_id,
        "worker_node_type": cluster.node_type_id,
        "workers": f"{autoscale.min_workers}-{autoscale.max_workers}"
        if autoscale
        else cluster.num_workers,
        "driver_instance_id": driver.instance_id if driver else None,
        "worker_instance_ids": [node.instance_id for node in cluster.executors or []],
        "running_since": _stamp(since) if running else None,
        "uptime": _elapsed(since, now) if running else None,
        "terminated_at": None if running else _stamp(cluster.terminated_time),
        "termination_reason": _value(getattr(cluster.termination_reason, "code", None)),
        "autotermination_minutes": cluster.autotermination_minutes,
        "tags": dict(cluster.default_tags or {}),
    }


# >>> start a terminated cluster (a no-op when it is already up), optionally waiting until RUNNING <<< #
def compute_start(w, cluster_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    state = _value(w.clusters.get(cluster_id).state)
    action = (
        "already running"
        if state in {"RUNNING", "RESIZING", "PENDING", "RESTARTING"}
        else "starting"
    )
    if action == "starting":
        waiter = w.clusters.start(cluster_id)
        if wait:
            waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": action, **compute_status(w, cluster_id)}


# >>> terminate (not permanently delete) a cluster <<< #
def compute_stop(w, cluster_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.clusters.delete(cluster_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "terminating", **compute_status(w, cluster_id)}


# >>> one SQL warehouse: state, type, size, clusters, sessions, auto-stop <<< #
def warehouse_status(w, warehouse_id: str) -> dict:
    warehouse = w.warehouses.get(warehouse_id)
    return {
        "warehouse_id": warehouse_id,
        "name": warehouse.name,
        "state": _value(warehouse.state),
        "type": _value(warehouse.warehouse_type),
        "serverless": warehouse.enable_serverless_compute,
        "size": warehouse.cluster_size,
        "clusters": f"{warehouse.num_clusters or 0} of {warehouse.min_num_clusters}-{warehouse.max_num_clusters}",
        "active_sessions": warehouse.num_active_sessions,
        "auto_stop_minutes": warehouse.auto_stop_mins,
        "health": _value(getattr(warehouse.health, "status", None)),
    }


# >>> start a SQL warehouse, optionally waiting until RUNNING <<< #
def warehouse_start(w, warehouse_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.warehouses.start(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "starting", **warehouse_status(w, warehouse_id)}


# >>> stop a SQL warehouse, optionally waiting until STOPPED <<< #
def warehouse_stop(w, warehouse_id: str, *, wait: bool = False, timeout_minutes: int = 20) -> dict:
    waiter = w.warehouses.stop(warehouse_id)
    if wait:
        waiter.result(timeout=timedelta(minutes=timeout_minutes))
    return {"action": "stopping", **warehouse_status(w, warehouse_id)}


# >>> run one statement on a SQL warehouse (starting it if needed) and return the first rows <<< #
def run_sql(w, statement: str, warehouse_id: str, *, limit: int = 20, poll: float = 2.0) -> dict:
    response = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="30s"
    )
    while _value(response.status.state) in RUNNING_STATEMENT:
        time.sleep(poll)
        response = w.statement_execution.get_statement(response.statement_id)
    state = _value(response.status.state)
    if state != "SUCCEEDED":
        error = response.status.error.message if response.status.error else state
        raise RuntimeError(f"statement {state}: {error}")
    columns = [column.name for column in response.manifest.schema.columns]
    rows = (response.result.data_array if response.result else None) or []
    return {
        "statement_id": response.statement_id,
        "columns": columns,
        "rows": rows[:limit],
        "total_rows": response.manifest.total_row_count,
    }


# >>> one job run as a flat row <<< #
def _run_row(run) -> dict:
    return {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "name": run.run_name,
        "type": _value(run.run_type),
        "state": _value(getattr(run.state, "life_cycle_state", None)),
        "result": _value(getattr(run.state, "result_state", None)),
        "started": _stamp(run.start_time),
        "seconds": round((run.run_duration or run.execution_duration or 0) / 1000),
        "url": run.run_page_url,
    }


# >>> the latest job runs (one-time submits included), newest first <<< #
def list_runs(w, *, limit: int = 10, job_id: int | None = None, active: bool = False) -> dict:
    runs = w.jobs.list_runs(job_id=job_id, active_only=active or None)
    return {"runs": [_run_row(run) for run in islice(runs, limit)]}


# >>> one run with its tasks and the state message that explains a failure <<< #
def run_detail(w, run_id: int) -> dict:
    run = w.jobs.get_run(run_id)
    tasks = [
        {
            "task_key": task.task_key,
            "run_id": task.run_id,
            "state": _value(getattr(task.state, "life_cycle_state", None)),
            "result": _value(getattr(task.state, "result_state", None)),
            "message": getattr(task.state, "state_message", None) or None,
            "seconds": round((task.run_duration or task.execution_duration or 0) / 1000),
        }
        for task in run.tasks or []
    ]
    return {
        **_run_row(run),
        "message": getattr(run.state, "state_message", None) or None,
        "tasks": tasks,
    }
