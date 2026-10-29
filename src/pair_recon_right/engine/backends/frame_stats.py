from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

NULL = "<<NULL>>"
EXTREMES = ("mean", "std", "min", "max")


# >>> first 16 hex digits of sha256 as an integer, what Spark's conv(substr(sha2(x, 256), 1, 16), 16, 10) yields <<< #
def hash16(text: str) -> int:
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:16], 16)


# >>> hash_sum of a text column; each distinct value is hashed once and weighted by its count <<< #
def hash_sum(series: pd.Series) -> int:
    counts = series.fillna(NULL).astype(str).value_counts()
    return sum(hash16(value) * int(count) for value, count in counts.items())


def _float(value) -> float | None:
    return None if value is None or pd.isna(value) else float(value)


# >>> the statistics one Spark aggregate row would carry, reduced from the fetched text and value columns <<< #
def frame_statistics(
    frame: pd.DataFrame, plans: list[dict[str, Any]], selected: list[str]
) -> dict[str, dict[str, Any]]:
    stats = {}
    n_total = len(frame)
    for plan in plans:
        numeric = plan["numeric"]
        text = frame[f"t{plan['index']}"]
        payload: dict[str, Any] = {
            "col_type": "numeric" if numeric else "categorical",
            "n_total": n_total,
        }
        if "n_missing" in selected:
            payload["n_missing"] = int(text.isna().sum())
        if "n_unique" in selected:
            payload["n_unique"] = int(text.nunique())
        if "hash_sum" in selected:
            payload["hash_sum"] = str(hash_sum(text))
        if any(name in selected for name in EXTREMES):
            number = frame[f"v{plan['index']}"].dropna() if numeric else None
            values = text.dropna() if number is None else number
            if "mean" in selected:
                payload["mean"] = _float(number.mean()) if numeric and len(number) else None
            if "std" in selected:
                payload["std"] = _float(number.std(ddof=1)) if numeric and len(number) > 1 else None
            for name in ("min", "max"):
                if name in selected:
                    found = None if values.empty else getattr(values, name)()
                    if found is not None and numeric and plan["integral"]:
                        found = int(found)
                    payload[f"{name}_val"] = None if found is None else str(found)
        if "length_avg" in selected or "length_max" in selected:
            lengths = (
                pd.Series([], dtype="int64") if numeric else text.dropna().astype(str).str.len()
            )
            if "length_avg" in selected:
                payload["length_avg"] = None if lengths.empty else float(lengths.mean())
            if "length_max" in selected:
                payload["length_max"] = None if lengths.empty else int(lengths.max())
        stats[plan["column"]] = payload
    return stats
