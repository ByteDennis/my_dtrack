from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ...engine.dates import canonical_date


class CsvSource:
    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)
        self.path = Path(config.get("path") or config.get("conn_macro") or "")

    def _frame(self) -> pd.DataFrame:
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        return pd.read_csv(self.path, dtype=object)

    def preflight(self) -> dict[str, Any]:
        try:
            columns = list(self._frame().columns)
            required = {self.config["date_col"]}
            missing = sorted(required - set(columns))
            if missing:
                raise ValueError(f"missing columns: {', '.join(missing)}")
            return {"ok": True, "source": "csv", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "csv", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        values = self._frame()[self.config["date_col"]].map(canonical_date)
        if from_date:
            values = values[values >= canonical_date(from_date)]
        if to_date:
            values = values[values <= canonical_date(to_date)]
        return {str(key): int(value) for key, value in values.value_counts().items()}

    def pull_window(
        self,
        partitions: list[str],
        columns: list[str],
        output: Path,
        *,
        key_columns: list[str] | None = None,
    ) -> Path:
        frame = self._frame()
        dates = frame[self.config["date_col"]].map(canonical_date)
        selected = list(dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns]))
        output.parent.mkdir(parents=True, exist_ok=True)
        frame.loc[dates.isin(set(partitions)), selected].to_csv(output, index=False)
        return output
