from __future__ import annotations

import base64
import os
import subprocess
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

from .runtime import value

SOURCE_LANGUAGES = {".py": "PYTHON", ".r": "R"}
NOTEBOOK_HEADER = "# databricks notebook"
DIRECTORY_TYPES = {"DIRECTORY", "REPO"}


def upload(
    workspace,
    source: str | Path,
    target: str | None = None,
    *,
    overwrite=False,
    dry_run=False,
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
        target = source.name
    remote_root = resolve_workspace_path(workspace, target)
    local_files, skipped = ([source], 0) if source.is_file() else local_files_for(source)
    uploads = []
    directories = {remote_root.parent if source.is_file() else remote_root}
    for local in local_files:
        relative = Path(local.name) if source.is_file() else local.relative_to(source)
        remote = remote_root.joinpath(*relative.parts) if source.is_dir() else remote_root
        kind = notebook_kind(local)
        if kind == "JUPYTER":
            remote, format_, language = remote.with_suffix(""), ImportFormat.JUPYTER, None
        elif kind:
            remote, format_, language = remote.with_suffix(""), ImportFormat.SOURCE, Language(kind)
        else:
            format_, language = ImportFormat.AUTO, None
        directories.add(remote.parent)
        uploads.append((local, remote, format_, language))
    ordered = sorted(directories, key=lambda path: (len(path.parts), str(path)))
    failures, uploaded = [], []
    if not dry_run:
        for directory in ordered:
            try:
                workspace.workspace.mkdirs(str(directory))
            except Exception as exc:
                failures.append(failure(directory, exc))
        for local, remote, format_, language in uploads:
            try:
                options = {"format": format_, "overwrite": overwrite}
                if language is not None:
                    options["language"] = language
                workspace.workspace.upload(str(remote), local.read_bytes(), **options)
                uploaded.append(str(remote))
            except Exception as exc:
                failures.append(failure(remote, exc))
    planned = [str(remote) for _, remote, _, _ in uploads]
    return {
        "source": str(source),
        "target": str(remote_root),
        "dry_run": dry_run,
        "planned": len(uploads),
        "uploaded": len(uploaded),
        "directories": len(ordered),
        "skipped": skipped,
        "paths": planned if dry_run else uploaded,
        "failed": failures,
        "exit_code": int(bool(failures)),
    }


def download(
    workspace,
    source: str,
    target: str | Path | None = None,
    *,
    overwrite=False,
    dry_run=False,
) -> dict:
    from databricks.sdk.service.workspace import ExportFormat

    remote_root = resolve_workspace_path(workspace, source)
    root = workspace.workspace.get_status(str(remote_root))
    is_directory = value(root.object_type) in {"DIRECTORY", "REPO"}
    local_root = Path(target) if target is not None else Path(remote_root.name)
    if local_root.is_symlink():
        raise ValueError(f"target must not be a symbolic link: {local_root}")
    if is_directory and local_root.exists() and not local_root.is_dir():
        raise ValueError(f"directory download target is not a directory: {local_root}")
    if not is_directory and local_root.is_dir():
        local_root /= remote_root.name
    directories, objects = download_objects(workspace, root)
    planned, downloaded, failures, size = [], [], [], 0
    if is_directory and not dry_run:
        for item in directories:
            remote = PurePosixPath(item.path)
            destination = local_root.joinpath(*remote.relative_to(remote_root).parts)
            try:
                validate_target(local_root, destination, is_directory=True)
                if destination.exists() and not destination.is_dir():
                    raise NotADirectoryError("directory path is an existing file")
                destination.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                failures.append(failure(destination, exc))
    for item in objects:
        remote = PurePosixPath(item.path)
        relative = remote.relative_to(remote_root) if is_directory else PurePosixPath(remote.name)
        validate_relative(relative)
        destination = local_root.joinpath(*relative.parts) if is_directory else local_root
        try:
            if dry_run:
                planned.append(str(download_path(destination, item, None)))
                continue
            response = workspace.workspace.export(str(remote), format=ExportFormat.AUTO)
            if response.content is None:
                raise ValueError("Workspace export returned no content")
            destination = download_path(destination, item, response.file_type)
            body = base64.b64decode(response.content, validate=True)
            planned.append(str(destination))
            validate_target(local_root, destination, is_directory=is_directory)
            if destination.exists() and not overwrite:
                raise FileExistsError("target exists; pass --overwrite to replace it")
            if destination.exists() and destination.is_dir():
                raise IsADirectoryError("target path is a directory")
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(destination.name + ".pair_recon.part")
            try:
                temporary.write_bytes(body)
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
            downloaded.append(str(destination))
            size += len(body)
        except Exception as exc:
            failures.append(failure(destination, exc))
    return {
        "source": str(remote_root),
        "target": str(local_root),
        "dry_run": dry_run,
        "planned": len(objects),
        "downloaded": len(downloaded),
        "directories": len(directories),
        "bytes": size,
        "paths": planned if dry_run else downloaded,
        "failed": failures,
        "exit_code": int(bool(failures)),
    }


def ls(workspace, source: str | None = None, *, recursive=False) -> dict:
    remote = (
        workspace_home(workspace) if source is None else resolve_workspace_path(workspace, source)
    )
    root = workspace.workspace.get_status(str(remote))
    if value(root.object_type) not in DIRECTORY_TYPES:
        objects = [root]
    else:
        objects = list(workspace.workspace.list(str(remote)))
        if recursive:
            pending = [item for item in objects if value(item.object_type) in DIRECTORY_TYPES]
            while pending:
                directory = pending.pop()
                children = list(workspace.workspace.list(directory.path))
                objects.extend(children)
                pending.extend(
                    item for item in children if value(item.object_type) in DIRECTORY_TYPES
                )
    entries = [workspace_entry(item) for item in sorted(objects, key=lambda item: item.path or "")]
    return {
        "source": str(remote),
        "recursive": recursive,
        "count": len(entries),
        "entries": entries,
    }


def copy(
    workspace,
    source: str,
    target: str,
    *,
    recursive=False,
    force=False,
    dry_run=False,
) -> dict:
    from databricks.sdk.service.workspace import ExportFormat, ImportFormat

    source_root = resolve_workspace_path(workspace, source)
    target_root = resolve_workspace_path(workspace, target)
    source_item = workspace.workspace.get_status(str(source_root))
    source_is_directory = value(source_item.object_type) in DIRECTORY_TYPES
    if source_is_directory and not recursive:
        raise IsADirectoryError(f"{source_root}: use --recursive to copy a directory")

    target_item = workspace_status(workspace, target_root)
    destination_root = (
        target_root / source_root.name
        if target_item is not None and value(target_item.object_type) in DIRECTORY_TYPES
        else target_root
    )
    validate_workspace_copy(source_root, destination_root, source_is_directory)
    destination_item = workspace_status(workspace, destination_root)
    if destination_item is not None and not force:
        raise FileExistsError(f"{destination_root}: target exists; pass --force to replace it")

    directories, objects = download_objects(workspace, source_item)
    directory_paths = [
        destination_root.joinpath(*PurePosixPath(item.path).relative_to(source_root).parts)
        for item in directories
    ]
    object_paths = [
        destination_root.joinpath(*PurePosixPath(item.path).relative_to(source_root).parts)
        for item in objects
    ]
    planned = [str(path) for path in object_paths]
    if dry_run:
        return {
            "source": str(source_root),
            "target": str(destination_root),
            "recursive": recursive,
            "force": force,
            "dry_run": True,
            "planned": len(objects),
            "copied": 0,
            "directories": len(directories),
            "paths": planned,
            "failed": [],
            "exit_code": 0,
        }

    payloads, failures = [], []
    for item, destination in zip(objects, object_paths):
        try:
            response = workspace.workspace.export(item.path, format=ExportFormat.AUTO)
            if response.content is None:
                raise ValueError("Workspace export returned no content")
            payloads.append((item, destination, response.content))
        except Exception as exc:
            failures.append(failure(item.path, exc))
    if failures:
        return copy_result(
            source_root,
            destination_root,
            recursive,
            force,
            directories,
            objects,
            (),
            failures,
        )

    if destination_item is not None:
        workspace.workspace.delete(
            str(destination_root),
            recursive=value(destination_item.object_type) in DIRECTORY_TYPES,
        )
    for directory in sorted(directory_paths, key=lambda path: (len(path.parts), str(path))):
        try:
            workspace.workspace.mkdirs(str(directory))
        except Exception as exc:
            failures.append(failure(directory, exc))
    copied = []
    for item, destination, content in payloads:
        try:
            options = {
                "content": content,
                "format": ImportFormat.SOURCE
                if value(item.object_type) == "NOTEBOOK"
                else ImportFormat.AUTO,
                "overwrite": force,
            }
            if value(item.object_type) == "NOTEBOOK" and item.language is not None:
                options["language"] = item.language
            workspace.workspace.import_(str(destination), **options)
            copied.append(str(destination))
        except Exception as exc:
            failures.append(failure(destination, exc))
    return copy_result(
        source_root,
        destination_root,
        recursive,
        force,
        directories,
        objects,
        copied,
        failures,
    )


def move(
    workspace,
    source: str,
    target: str,
    *,
    force=False,
    dry_run=False,
) -> dict:
    result = copy(
        workspace,
        source,
        target,
        recursive=True,
        force=force,
        dry_run=dry_run,
    )
    if result["exit_code"] or dry_run:
        return {**result, "moved": 0}
    removed = rm(workspace, source, recursive=True, force=False)
    failures = [*result["failed"], *removed["failed"]]
    return {
        **result,
        "moved": 1 if not failures else 0,
        "failed": failures,
        "exit_code": int(bool(failures)),
    }


def rm(
    workspace,
    sources: str | Iterable[str],
    *,
    recursive=False,
    force=False,
    dry_run=False,
) -> dict:
    requested = [sources] if isinstance(sources, str) else list(sources)
    planned, deleted, failures = [], [], []
    for source in requested:
        try:
            remote = resolve_workspace_path(workspace, source)
            try:
                item = workspace.workspace.get_status(str(remote))
            except Exception as exc:
                if force and resource_does_not_exist(exc):
                    continue
                raise
            if value(item.object_type) in {"DIRECTORY", "REPO"} and not recursive:
                raise IsADirectoryError(f"{remote}: use --recursive to remove a directory")
            planned.append(str(remote))
            if dry_run:
                continue
            try:
                workspace.workspace.delete(str(remote), recursive=recursive)
            except Exception as exc:
                if force and resource_does_not_exist(exc):
                    continue
                raise
            deleted.append(str(remote))
        except Exception as exc:
            failures.append(failure(source, exc))
    return {
        "sources": [str(source) for source in requested],
        "recursive": recursive,
        "force": force,
        "dry_run": dry_run,
        "planned": len(planned),
        "deleted": len(deleted),
        "paths": planned if dry_run else deleted,
        "failed": failures,
        "exit_code": int(bool(failures)),
    }


def copy_result(
    source: PurePosixPath,
    target: PurePosixPath,
    recursive: bool,
    force: bool,
    directories: list[Any],
    objects: list[Any],
    copied: Iterable[str],
    failures: list[dict],
) -> dict:
    copied = list(copied)
    return {
        "source": str(source),
        "target": str(target),
        "recursive": recursive,
        "force": force,
        "dry_run": False,
        "planned": len(objects),
        "copied": len(copied),
        "directories": len(directories),
        "paths": copied,
        "failed": failures,
        "exit_code": int(bool(failures)),
    }


def workspace_entry(item) -> dict:
    return {
        "path": item.path,
        "name": PurePosixPath(item.path).name,
        "type": value(item.object_type),
        "language": value(item.language) if item.language is not None else None,
    }


def workspace_status(workspace, path: PurePosixPath):
    try:
        return workspace.workspace.get_status(str(path))
    except Exception as exc:
        if resource_does_not_exist(exc):
            return None
        raise


def validate_workspace_copy(
    source: PurePosixPath, destination: PurePosixPath, source_is_directory: bool
) -> None:
    if source == destination:
        raise ValueError("source and target resolve to the same Workspace path")
    if source_is_directory and source in destination.parents:
        raise ValueError("cannot copy a Workspace directory into itself")


def failure(path, exc: Exception) -> dict:
    return {"path": str(path), "error": f"{type(exc).__name__}: {exc}"}


def resource_does_not_exist(exc: Exception) -> bool:
    return getattr(exc, "error_code", None) == "RESOURCE_DOES_NOT_EXIST" or type(exc).__name__ == (
        "NotFound"
    )


def download_objects(workspace, root) -> tuple[list[Any], list[Any]]:
    directories, objects = [], []

    def visit(item):
        if value(item.object_type) not in {"DIRECTORY", "REPO"}:
            objects.append(item)
            return
        directories.append(item)
        for child in sorted(workspace.workspace.list(item.path), key=lambda row: row.path or ""):
            visit(child)

    visit(root)
    return directories, objects


def download_path(destination: Path, item, file_type: str | None) -> Path:
    if value(item.object_type) != "NOTEBOOK" or destination.suffix:
        return destination
    fallback = {"PYTHON": "py", "R": "r", "SQL": "sql", "SCALA": "scala"}.get(value(item.language))
    suffix = (file_type or fallback or "").strip().lstrip(".")
    if not suffix or Path(suffix).name != suffix or any(char in suffix for char in "/\\"):
        raise ValueError(f"invalid Workspace export file type: {file_type!r}")
    return destination.with_name(f"{destination.name}.{suffix}")


def validate_relative(path: PurePosixPath) -> None:
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"invalid Workspace child path: {path}")


def validate_target(root: Path, destination: Path, *, is_directory: bool) -> None:
    boundary = root.parent if is_directory else destination.parent
    current = destination
    while current != boundary:
        if current.is_symlink():
            raise ValueError(f"download target contains a symbolic link: {current}")
        current = current.parent


def workspace_path(path: str) -> PurePosixPath:
    remote = PurePosixPath(path)
    if not path.startswith("/") or ".." in remote.parts or remote == PurePosixPath("/"):
        raise ValueError(f"invalid absolute Workspace path: {path}")
    return remote


def resolve_workspace_path(workspace, path: str) -> PurePosixPath:
    text = str(path).strip()
    remote = PurePosixPath(text)
    if not text or remote == PurePosixPath(".") or ".." in remote.parts:
        raise ValueError(f"invalid Workspace path: {path}")
    if remote.is_absolute():
        return workspace_path(text)
    return workspace_home(workspace) / remote


def workspace_home(workspace) -> PurePosixPath:
    user = str(workspace.current_user.me().user_name).strip()
    if not user or "/" in user or "\\" in user:
        raise ValueError("current Databricks user has an invalid Workspace home path")
    return workspace_path(str(PurePosixPath("/Users") / user))


def local_files_for(root: Path) -> tuple[list[Path], int]:
    git_files = git_files_for(root)
    if git_files is not None:
        return git_files
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"source tree contains a symbolic link: {path}")
        if path.is_file():
            files.append(path)
    return files, 0


def git_files_for(root: Path) -> tuple[list[Path], int] | None:
    repository = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if repository.returncode:
        return None
    worktree = Path(repository.stdout.strip()).resolve()
    relative = root.resolve().relative_to(worktree)
    use_ignores = has_gitignore(root.resolve(), worktree)
    command = ["git", "-C", str(worktree), "ls-files", "-z", "--cached", "--others"]
    if use_ignores:
        command.append("--exclude-standard")
    command.extend(["--", relative.as_posix()])
    listed = subprocess.run(command, capture_output=True, check=True).stdout
    files = []
    for name in filter(None, listed.decode("utf-8").split("\0")):
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
                relative.as_posix(),
            ],
            capture_output=True,
            check=True,
        ).stdout
        skipped = sum(bool(name) for name in ignored.decode("utf-8").split("\0"))
    return sorted(files), skipped


def has_gitignore(root: Path, worktree: Path) -> bool:
    current = root
    while True:
        if (current / ".gitignore").is_file():
            return True
        if current == worktree:
            break
        current = current.parent
    return any(path.is_file() for path in root.rglob(".gitignore"))


def notebook_kind(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".ipynb":
        return "JUPYTER"
    language = SOURCE_LANGUAGES.get(suffix)
    if language is None:
        return None
    with path.open("rb") as stream:
        first = stream.readline(256).decode("utf-8-sig", errors="replace").rstrip("\r\n")
    return language if first.casefold().startswith(NOTEBOOK_HEADER) else None
