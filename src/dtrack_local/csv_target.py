from __future__ import annotations

from pathlib import Path
from typing import Any

from dtrack_left.common.dates import canonical_date, resolve_date_type
from dtrack_left.common.stats import compute_csv_statistics
from dtrack_left.csv_sampling import sample_csv


class CsvAnchorTarget:
    """Local CSV stand-in for exercising the Right consumer without Databricks."""

    def __init__(self, config: dict[str, Any], root: str | Path, work_dir: str | Path):
        self.config = dict(config)
        self.path = Path(root) / f"{config.get('table', 'right')}.csv"
        self.work_dir = Path(work_dir)
        self._date_type = None

    def _frame(self):
        import pandas as pd

        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        return pd.read_csv(self.path, dtype=object)

    def date_type(self, frame=None):
        if self._date_type is not None:
            return self._date_type
        resolved = resolve_date_type(self.config.get("date_type"))
        if resolved.requires_probe:
            frame = self._frame() if frame is None else frame
            values = frame[self.config["date_col"]].dropna().head(16).tolist()
            resolved = resolved.detect(values)
        self._date_type = resolved
        self.config["date_type"] = resolved.spec(portable=True)
        return resolved

    def preflight(self) -> dict[str, Any]:
        try:
            frame = self._frame()
            columns = list(frame.columns)
            if (
                self.config.get("partition_mode") != "all"
                and self.config["date_col"] not in columns
            ):
                raise ValueError(f"missing column {self.config['date_col']}")
            if self.config.get("partition_mode") != "all":
                self.date_type(frame)
            return {"ok": True, "source": "csv-anchor", "columns": columns}
        except (OSError, KeyError, TypeError, ValueError) as exc:
            return {"ok": False, "source": "csv-anchor", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        frame = self._frame()
        if self.config.get("partition_mode") == "all":
            return {"all": len(frame)}
        normalize = self.date_type(frame).normalize
        values = frame[self.config["date_col"]].map(normalize)
        if from_date:
            values = values[values >= canonical_date(from_date)]
        if to_date:
            values = values[values <= canonical_date(to_date)]
        return {str(key): int(value) for key, value in values.value_counts().items()}

    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        frame = self._frame()
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        if self.config.get("partition_mode") == "all":
            selected = list(dict.fromkeys([*manifest["right_key_cols"], *right_columns]))
            rows = frame.loc[:, selected]
        else:
            normalize = self.date_type(frame).normalize
            dates = frame[self.config["date_col"]].map(normalize)
            selected = list(
                dict.fromkeys(
                    [self.config["date_col"], *manifest["right_key_cols"], *right_columns]
                )
            )
            rows = frame.loc[dates.isin(set(window["partitions"])), selected]
        output = self.work_dir / manifest["pair"] / window["bucket"] / "right.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        rows.to_csv(output, index=False)
        plan = manifest["sample"]
        sampled = sample_csv(
            output,
            key_columns=manifest["right_key_cols"],
            mode=plan["mode"],
            rate=plan.get("rate"),
            count=plan.get("count"),
            families=manifest["key_families"],
            key_tuples=window.get("key_tuples"),
            side="right",
        )
        overrides = manifest.get("col_type_overrides") or {}
        column_types = manifest.get("column_types") or {}
        return compute_csv_statistics(
            sampled,
            right_columns,
            strict=manifest.get("strict", False),
            col_types={manifest["col_map"][key]: value for key, value in overrides.items()},
            column_types={manifest["col_map"][key]: value for key, value in column_types.items()},
            side="right",
        )
