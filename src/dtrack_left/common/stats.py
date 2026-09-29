from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .hashing import collapse_decimal


def _hash16(value: object) -> int:
    text = "<<NULL>>" if value is None or pd.isna(value) else str(value)
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:16], 16)


def _top10(series: pd.Series) -> str:
    counts = series.dropna().astype(str).value_counts(dropna=False).head(10)
    return json.dumps(
        [{"value": value, "count": int(count)} for value, count in counts.items()],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def column_statistics(
    series: pd.Series, *, strict: bool = False, col_type: str | None = None
) -> dict[str, Any]:
    values = series.copy()
    if strict:
        values = values.map(
            lambda value: (
                value
                if value is None or pd.isna(value)
                else collapse_decimal(str(value).strip()).lower()
            )
        )
    non_null = values.dropna()
    numeric = pd.to_numeric(non_null, errors="coerce")
    is_numeric = len(non_null) > 0 and numeric.notna().all()
    if col_type:
        is_numeric = col_type == "numeric"
        numeric = numeric.dropna()
    if is_numeric:
        values = values.map(
            lambda value: (
                value if value is None or pd.isna(value) else collapse_decimal(str(value).strip())
            )
        )
        non_null = values.dropna()
    n_total = len(values)
    n_missing = int(values.isna().sum())
    n_unique = int(non_null.nunique(dropna=True))
    common: dict[str, Any] = {
        "col_type": "numeric" if is_numeric else "categorical",
        "n_total": n_total,
        "n_missing": n_missing,
        "n_unique": n_unique,
        "hash_sum": str(sum(_hash16(value) for value in values)),
    }
    if is_numeric:
        common.update(
            mean=None if numeric.empty else float(numeric.mean()),
            std=None if len(numeric) < 2 else float(numeric.std(ddof=1)),
            min_val=None if numeric.empty else str(numeric.min()),
            max_val=None if numeric.empty else str(numeric.max()),
            length_avg=None,
            length_max=None,
            top_10="",
        )
    else:
        strings = non_null.astype(str)
        common.update(
            mean=None,
            std=None,
            min_val=None if strings.empty else strings.min(),
            max_val=None if strings.empty else strings.max(),
            length_avg=None if strings.empty else float(strings.str.len().mean()),
            length_max=None if strings.empty else int(strings.str.len().max()),
            top_10=_top10(values),
        )
    return common


def compute_csv_statistics(
    path: str | Path,
    columns: list[str],
    *,
    strict: bool = False,
    col_types: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    frame = pd.read_csv(path, dtype=object)
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"CSV is missing columns: {', '.join(missing)}")
    types = col_types or {}
    return {
        column: column_statistics(frame[column], strict=strict, col_type=types.get(column))
        for column in columns
    }
