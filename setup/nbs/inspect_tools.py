from __future__ import annotations

import inspect
from pathlib import Path


def label(value) -> str:
    return f"{value.__module__}.{value.__qualname__}"


def show(*values) -> None:
    for value in values:
        if isinstance(value, property):
            value = value.fget
        try:
            suffix = str(inspect.signature(value))
        except (TypeError, ValueError):
            suffix = ""
        print(f"\n{label(value)}{suffix}")
        print(inspect.getsource(value).rstrip())


def show_methods(value, *names: str) -> None:
    print(f"\n{label(value)}")
    for name in names:
        show(getattr(value, name))


def show_file(relative: str) -> None:
    root = next(
        candidate
        for candidate in (Path.cwd(), *Path.cwd().parents)
        if (candidate / "pyproject.toml").is_file()
    )
    path = root / relative
    print(f"\n{path.relative_to(root)}")
    print(path.read_text(encoding="utf-8").rstrip())
