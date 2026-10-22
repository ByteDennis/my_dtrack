from __future__ import annotations

import argparse
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from uuid import uuid4

RELEASE_NAME = "pair_recon"
RUNTIME_PACKAGES = (
    "pair_recon",
    "pair_recon_dbx",
    "pair_recon_left",
    "pair_recon_right",
)
ROOT_FILES = (
    ".pair_recon.env.example",
    "run.cmd",
    "run.sh",
)
RUNTIME_EXCLUDES = {
    "src/pair_recon/core/metadata/mapping.py",
    "src/pair_recon/core/sas.py",
    "src/pair_recon_dbx/cli.py",
}
README = """PairRecon

Requirements: Python 3.11+, uv, and just.

Install
1. Extract the ZIP and open a terminal in the pair_recon folder.
2. Run: uv sync
3. Copy .pair_recon.env.example to .pair_recon.env.
4. Fill .pair_recon.env and setup/pairs.xlsx.

Run
just run

Without just
uv run python -m pair_recon

For Hadoop support, install with: uv sync --extra hadoop
"""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _remove_toml_tables(text: str, names: tuple[str, ...]) -> str:
    for name in names:
        text = re.sub(
            rf"(?ms)^\[{re.escape(name)}\]\n.*?(?=^\[|\Z)",
            "",
            text,
        )
    return text


def release_project_text(text: str) -> str:
    text = _remove_toml_tables(
        text,
        (
            "dependency-groups",
            "tool.uv",
            "tool.pytest.ini_options",
            "tool.coverage.run",
            "tool.coverage.report",
            "tool.ruff",
            "tool.ruff.lint.per-file-ignores",
        ),
    )
    text = re.sub(r"(?m)^debug\s*=.*\n", "", text)
    text = re.sub(r"(?m)^pair_recon_(?:dbx|gitlab)\s*=.*\n", "", text)
    packages = ", ".join(f'"src/{name}"' for name in RUNTIME_PACKAGES)
    text, count = re.subn(r"(?m)^packages\s*=.*$", f"packages = [{packages}]", text)
    if count != 1:
        raise ValueError("pyproject.toml must contain one Hatch wheel package list")
    return re.sub(r"\n{3,}", "\n\n", text).rstrip() + "\n"


def release_just_text(text: str) -> str:
    match = re.search(r"(?m)^run \*args:\n(?:^[ \t][^\n]*(?:\n|\Z))+", text)
    if match is None:
        raise ValueError("justfile must contain the run recipe")
    recipe, count = re.subn(
        r"(?m)^[ \t]+uv run(?: --extra debug)? pair_recon \{\{args\}\}[ \t]*$",
        "    uv run python -m pair_recon {{args}}",
        match.group(0),
    )
    if count != 1:
        raise ValueError("justfile run recipe must invoke pair_recon once")
    return recipe.rstrip() + "\n"


def _copy_file(source: Path, target: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"release source does not exist: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def stage_release(root: Path, target: Path) -> list[Path]:
    root = root.resolve()
    target.mkdir(parents=True, exist_ok=True)
    for name in ROOT_FILES:
        _copy_file(root / name, target / name)
    _copy_file(root / "setup" / "pairs.xlsx", target / "setup" / "pairs.xlsx")

    for package in RUNTIME_PACKAGES:
        source_root = root / "src" / package
        if not source_root.is_dir():
            raise FileNotFoundError(f"release package does not exist: {source_root}")
        for source in sorted(source_root.rglob("*.py")):
            relative = source.relative_to(root)
            if "__pycache__" in source.parts or relative.as_posix() in RUNTIME_EXCLUDES:
                continue
            _copy_file(source, target / relative)

    (target / "pyproject.toml").write_text(
        release_project_text((root / "pyproject.toml").read_text(encoding="utf-8")),
        encoding="utf-8",
    )
    (target / "justfile").write_text(
        release_just_text((root / "justfile").read_text(encoding="utf-8")),
        encoding="utf-8",
    )
    (target / "readme.txt").write_text(README, encoding="utf-8")
    return sorted(path for path in target.rglob("*") if path.is_file())


def create_archive(root: Path | None = None, output: Path | None = None) -> dict:
    root = (root or project_root()).resolve()
    output = Path(output) if output is not None else root / "dist" / f"{RELEASE_NAME}.zip"
    if not output.is_absolute():
        output = Path.cwd() / output
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output.with_name(f".{output.name}.{uuid4().hex}.part")

    try:
        with tempfile.TemporaryDirectory(prefix="pair_recon_release_") as temporary:
            release_root = Path(temporary) / RELEASE_NAME
            files = stage_release(root, release_root)
            with zipfile.ZipFile(
                temporary_output,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=9,
            ) as archive:
                for path in files:
                    archive.write(path, path.relative_to(release_root.parent).as_posix())
        os.replace(temporary_output, output)
    finally:
        temporary_output.unlink(missing_ok=True)

    return {
        "path": str(output),
        "files": len(files),
        "bytes": output.stat().st_size,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the minimal PairRecon source release ZIP")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    result = create_archive(output=args.output)
    print(f"Published {result['files']} files ({result['bytes']} bytes): {result['path']}")
    return 0
