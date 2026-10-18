from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

from . import snapshot


def command(args, *, cwd=None, env=None, ok=(0,)):
    result = subprocess.run(
        [str(value) for value in args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in ok:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise RuntimeError(f"{' '.join(str(value) for value in args[:3])}: {detail}")
    return result


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise OSError(f"{name} is not installed or not on PATH")


def create_project(
    path: str,
    *,
    group: str | None = None,
    description: str | None = None,
    host: str | None = None,
    dry_run=False,
    runner=command,
) -> dict:
    args = ["glab", "repo", "create", path, "--private"]
    if group:
        args.extend(["--group", group])
    if description:
        args.extend(["--description", description])
    if dry_run:
        return {"action": "create", "visibility": "private", "command": args, "dry_run": True}
    require_tool("glab")
    env = os.environ.copy()
    if host:
        env["GITLAB_HOST"] = host
    result = runner(args, env=env)
    return {
        "action": "created",
        "visibility": "private",
        "project": path,
        "output": result.stdout.strip(),
    }


def remote_url(repo: str, host: str) -> str:
    if repo.startswith("git@"):
        return repo
    if "://" in repo:
        parsed = urlsplit(repo)
        if parsed.username or parsed.password:
            raise ValueError("remote URL must not contain credentials")
        return repo
    parts = repo.removesuffix(".git").split("/")
    valid = re.compile(r"^[A-Za-z0-9_.-]+$")
    if len(parts) < 2 or not all(valid.fullmatch(part) for part in parts):
        raise ValueError("repo must be namespace/project or a credential-free Git URL")
    if not valid.fullmatch(host):
        raise ValueError("invalid GitLab host")
    return f"git@{host}:{'/'.join(parts)}.git"


def dispatch(
    root: Path,
    repo: str,
    *,
    scope="both",
    ignore=Path("pair_recon.ignore"),
    branch="main",
    message: str | None = None,
    host="gitlab.com",
    dry_run=False,
    runner=command,
) -> dict:
    root = root.resolve()
    ignore = ignore if ignore.is_absolute() else root / ignore
    result = snapshot.plan(root, scope, ignore)
    remote = remote_url(repo, host)
    result.update(repo=repo, remote=remote, branch=branch, dry_run=dry_run)
    if dry_run:
        return result
    if not re.fullmatch(r"(?!-)(?!.*\.\.)[A-Za-z0-9._/-]+", branch):
        raise ValueError("invalid branch")
    require_tool("git")
    with tempfile.TemporaryDirectory(prefix="pair_recon_gitlab_") as temporary:
        checkout = Path(temporary) / "repo"
        cloned = runner(
            ["git", "clone", "--depth", "1", "--branch", branch, remote, checkout],
            ok=(0, 1, 128),
        )
        if cloned.returncode:
            shutil.rmtree(checkout, ignore_errors=True)
            runner(["git", "clone", "--depth", "1", remote, checkout])
        runner(["git", "checkout", "-B", branch], cwd=checkout)
        snapshot.sync(root, checkout, snapshot.select(root, scope, ignore), scope)
        for key, fallback in (
            ("user.name", "PairRecon Dispatch"),
            ("user.email", "pair-recon@local.invalid"),
        ):
            identity = runner(["git", "config", "--get", key], cwd=root, ok=(0, 1)).stdout.strip()
            runner(["git", "config", key, identity or fallback], cwd=checkout)
        runner(["git", "add", "-A"], cwd=checkout)
        changed = runner(["git", "diff", "--cached", "--quiet"], cwd=checkout, ok=(0, 1)).returncode
        if not changed:
            return {**result, "dry_run": False, "state": "unchanged"}
        runner(["git", "commit", "-m", message or f"Dispatch {scope} validation"], cwd=checkout)
        runner(["git", "push", "-u", "origin", f"HEAD:{branch}"], cwd=checkout)
    return {**result, "dry_run": False, "state": "pushed"}
