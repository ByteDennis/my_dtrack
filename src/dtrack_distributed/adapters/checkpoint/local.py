from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LocalCheckpoint:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.rows: dict[str, dict[str, Any]] = {}
        if self.path.exists():
            self.rows = json.loads(self.path.read_text())

    def completed(self) -> set[str]:
        return set(self.rows)

    def record(self, unit_id: str, result: dict[str, Any]) -> None:
        self.rows[unit_id] = result
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.rows, sort_keys=True, ensure_ascii=False))
        temporary.replace(self.path)
