from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path


def write_rows(path: str | Path, columns: list[str], rows: Iterable[Iterable[object]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    return target
