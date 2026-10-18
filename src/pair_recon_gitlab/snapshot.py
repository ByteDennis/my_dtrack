from __future__ import annotations

import fnmatch
import json
import os
import re
import shutil
from pathlib import Path, PurePosixPath

SCOPES = ("left", "right", "both", "all")
PROTECTED = (
    ".git/",
    ".env*",
    ".pair_recon.env",
    ".pair_recon.yaml",
    "*.key",
    "*.pem",
    "*.p12",
    "*.pfx",
    "*.token",
    "credentials.json",
    "id_rsa",
    "id_ed25519",
)
ROOT_FILES = {
    ".pair_recon.env.example",
    ".gitlab-ci.yml",
    "README.md",
    "changelog.txt",
    "guideme.txt",
    "pair_recon.ignore",
    "pyproject.toml",
    "uv.lock",
}
BASE_TESTS = {"tests/conftest.py", "tests/support.py"}
SHARED_TESTS = ("tests/unit/common/", "tests/unit/conftest.py")


def patterns(path: Path) -> list[str]:
    if path.is_symlink():
        raise ValueError(f"ignore file must not be a symbolic link: {path}")
    if not path.is_file():
        raise ValueError(f"ignore file does not exist: {path}")
    values = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        value = line.strip().replace("\\", "/")
        if not value or value.startswith("#"):
            continue
        if value.startswith("!"):
            raise ValueError("pair_recon.ignore does not support negation")
        values.append(value.lstrip("/"))
    return values


def matches(path: str, pattern: str, *, directory=False) -> bool:
    path, pattern = path.strip("/"), pattern.strip()
    directory_pattern = pattern.endswith("/")
    pattern = pattern.rstrip("/")
    if "/" in pattern:
        exact = fnmatch.fnmatchcase(path, pattern)
        return exact or (directory_pattern and path.startswith(pattern + "/"))
    parts = path.split("/")
    return any(fnmatch.fnmatchcase(part, pattern) for part in parts) and (
        not directory_pattern or directory or len(parts) > 1
    )


def excluded(path: str, values: list[str], *, directory=False) -> bool:
    return any(matches(path, value, directory=directory) for value in (*PROTECTED, *values))


def in_scope(path: str, scope: str) -> bool:
    if scope == "all" or path in ROOT_FILES:
        return True
    base_test = path in BASE_TESTS
    shared = any(path == value.rstrip("/") or path.startswith(value) for value in SHARED_TESTS)
    left = path.startswith(("src/pair_recon_left/", "tests/unit/packages/pair_recon_left/"))
    right = path.startswith(("src/pair_recon_right/", "tests/unit/packages/pair_recon_right/"))
    if scope == "left":
        return left or base_test
    if scope == "right":
        return right or base_test
    return (
        left
        or right
        or base_test
        or shared
        or path.startswith(
            (
                "src/pair_recon/",
                "setup/",
                "tests/unit/packages/pair_recon/",
                "tests/unit/flows/",
                "tests/unit/examples/",
            )
        )
        or path in {"justfile", "run.cmd", "run.sh"}
    )


def select(root: Path, scope: str, ignore: Path) -> list[Path]:
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of: {', '.join(SCOPES)}")
    root = root.resolve()
    ignore = ignore if ignore.is_absolute() else root / ignore
    values = patterns(ignore)
    selected = []
    for current, directories, files in os.walk(root, topdown=True):
        current_path = Path(current)
        kept = []
        for name in sorted(directories):
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            if excluded(relative, values, directory=True):
                continue
            if path.is_symlink():
                raise ValueError(f"source tree contains a symbolic link: {path}")
            kept.append(name)
        directories[:] = kept
        for name in sorted(files):
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            if excluded(relative, values) and path.resolve() != ignore.resolve():
                continue
            if path.is_symlink():
                raise ValueError(f"source tree contains a symbolic link: {path}")
            if in_scope(relative, scope):
                selected.append(path)
    return selected


def plan(root: Path, scope: str, ignore: Path) -> dict:
    root = root.resolve()
    ignore = ignore if ignore.is_absolute() else root / ignore
    files = select(root, scope, ignore)
    return {
        "scope": scope,
        "root": str(root),
        "ignore": str(ignore.resolve()),
        "count": len(files),
        "bytes": sum(path.stat().st_size for path in files),
        "paths": [path.relative_to(root).as_posix() for path in files],
    }


def project_text(text: str, packages: set[str]) -> str:
    lines = []
    scripts = {
        "pair_recon": "pair_recon",
        "pair_recon_dbx": "pair_recon_dbx",
        "pair_recon_gitlab": "pair_recon_gitlab",
    }
    for line in text.splitlines():
        source_path = re.match(r'^"src/([^/]+)/', line)
        if source_path and source_path.group(1) not in packages:
            continue
        if line.startswith("packages = ["):
            values = ", ".join(f'"src/{name}"' for name in sorted(packages))
            line = f"packages = [{values}]"
        script = next((name for name in scripts if line.startswith(name + " = ")), None)
        if script and scripts[script] not in packages:
            continue
        lines.append(line)
    return "\n".join(lines) + "\n"


def just_text(text: str, packages: set[str]) -> str:
    blocked = {name for name in ("dbx", "gitlab") if f"pair_recon_{name}" not in packages}
    lines, output, skipping = text.splitlines(), [], False
    for line in lines:
        if line and not line[0].isspace() and line.endswith(" *args:"):
            skipping = line.removesuffix(" *args:") in blocked
        if skipping:
            if not line:
                skipping = False
            continue
        output.append(line)
    return "\n".join(output).rstrip() + "\n"


def copy(root: Path, checkout: Path, files: list[Path]) -> None:
    relatives = [source.relative_to(root) for source in files]
    packages = {
        relative.parts[1]
        for relative in relatives
        if len(relative.parts) > 2 and relative.parts[0] == "src"
    }
    for source, relative in zip(files, relatives, strict=True):
        destination = checkout / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if relative.as_posix() == "pyproject.toml":
            destination.write_text(
                project_text(source.read_text(encoding="utf-8"), packages), encoding="utf-8"
            )
        elif relative.as_posix() == "justfile":
            destination.write_text(
                just_text(source.read_text(encoding="utf-8"), packages), encoding="utf-8"
            )
        else:
            shutil.copy2(source, destination)


def sync(root: Path, checkout: Path, files: list[Path], scope: str) -> None:
    manifest = checkout / ".pair_recon-dispatch.json"
    previous = set()
    if manifest.is_symlink():
        raise ValueError("dispatch manifest must not be a symbolic link")
    if manifest.is_file():
        value = json.loads(manifest.read_text(encoding="utf-8"))
        names = value.get("files") if isinstance(value, dict) else None
        if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
            raise ValueError("invalid dispatch manifest")
        for name in names:
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or path.parts[:1] == (".git",):
                raise ValueError(f"invalid dispatch manifest path: {name}")
        previous = set(names)
    current = {source.relative_to(root).as_posix() for source in files}
    for name in sorted(previous - current, reverse=True):
        target = checkout / Path(name)
        if target.is_file() or target.is_symlink():
            target.unlink()
        parent = target.parent
        while parent != checkout and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
            parent = parent.parent
    copy(root, checkout, files)
    manifest.write_text(
        json.dumps(
            {"format": "pair-recon-dispatch-v1", "scope": scope, "files": sorted(current)},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
