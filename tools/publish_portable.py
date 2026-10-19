from __future__ import annotations

import argparse
import os
import platform
import shutil
import sys
import tempfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD_PARENT = ROOT / "build"
DIST_ROOT = ROOT / "dist"


def platform_tag() -> str:
    system = {"darwin": "macos"}.get(platform.system().lower(), platform.system().lower())
    machine = platform.machine().lower().replace("amd64", "x86_64").replace("aarch64", "arm64")
    return f"{system}-{machine}"


def project_version() -> str:
    value = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return str(value["project"]["version"])


def write_launcher(bundle: Path) -> None:
    if platform.system() == "Windows":
        (bundle / "run.cmd").write_text(
            "@echo off\n"
            "chcp 65001 > nul\n"
            '"%~dp0pair-recon.exe" %*\n'
            "set EXIT_CODE=%ERRORLEVEL%\n"
            'if not "%EXIT_CODE%"=="0" pause\n'
            "exit /b %EXIT_CODE%\n",
            encoding="utf-8",
        )
        return
    launcher = bundle / "run.sh"
    launcher.write_text(
        '#!/usr/bin/env sh\nexec "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/pair-recon" "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)


def archive_bundle(bundle: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        with zipfile.ZipFile(
            temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            for path in sorted(bundle.rglob("*")):
                if path.is_file():
                    archive.write(path, Path(bundle.name) / path.relative_to(bundle))
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def build(*, debug: bool = False) -> Path:
    from PyInstaller.__main__ import run as pyinstaller

    BUILD_PARENT.mkdir(parents=True, exist_ok=True)
    build_root = Path(tempfile.mkdtemp(prefix="pair-recon-portable-", dir=BUILD_PARENT))
    work_root = Path(tempfile.mkdtemp(prefix="pair-recon-pyinstaller-", dir=BUILD_PARENT))
    try:
        arguments = [
            str(ROOT / "tools" / "pair_recon_entry.py"),
            "--name=pair-recon",
            "--onedir",
            "--noconfirm",
            "--clean",
            f"--distpath={build_root}",
            f"--workpath={work_root}",
            f"--specpath={work_root}",
            "--exclude-module=pair_recon_dbx",
            "--exclude-module=pair_recon_gitlab",
        ]
        if debug:
            arguments.append("--log-level=INFO")
        pyinstaller(arguments)
        bundle = build_root / "pair-recon"
        executable = bundle / ("pair-recon.exe" if platform.system() == "Windows" else "pair-recon")
        if not executable.is_file():
            raise FileNotFoundError(f"portable executable was not created: {executable}")
        write_launcher(bundle)
        target = DIST_ROOT / f"pair-recon-{project_version()}-{platform_tag()}.zip"
        return archive_bundle(bundle, target)
    finally:
        shutil.rmtree(build_root, ignore_errors=True)
        shutil.rmtree(work_root, ignore_errors=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build a platform-native PairRecon portable ZIP")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)
    target = build(debug=args.debug)
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
