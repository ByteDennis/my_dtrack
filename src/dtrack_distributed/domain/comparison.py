from __future__ import annotations

from typing import Any


def _number(row: dict[str, Any], name: str) -> int:
    value = row.get(name)
    return 0 if value in (None, "") else int(value)


def _float_match(left: Any, right: Any, tolerance: float = 1e-6) -> bool | None:
    if left in (None, "") or right in (None, ""):
        return None
    try:
        left_number, right_number = float(left), float(right)
    except (TypeError, ValueError):
        return str(left).strip() == str(right).strip()
    return abs(left_number - right_number) <= max(
        1e-9, tolerance * max(abs(left_number), abs(right_number), 1.0)
    )


def compare_statistics(
    *,
    pair: str,
    window: str,
    column: str,
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {"pair": pair, "vintage": window, "column": column}
    for name in ("n_total", "n_missing", "n_unique"):
        left_value, right_value = _number(left, name), _number(right, name)
        result[f"{name}_left"] = left_value
        result[f"{name}_right"] = right_value
        result[f"{name}_diff"] = right_value - left_value
    checks = []
    for name in ("mean", "std", "length_avg", "length_max", "hash_sum"):
        match = _float_match(left.get(name), right.get(name))
        result[f"{name}_match"] = match
        if match is not None:
            checks.append(match)
    for name in ("min_val", "max_val"):
        left_value = "" if left.get(name) is None else str(left.get(name)).strip()
        right_value = "" if right.get(name) is None else str(right.get(name)).strip()
        label = "min" if name == "min_val" else "max"
        result[f"{label}_match"] = left_value == right_value
        checks.append(left_value == right_value)
    count_match = all(result[f"{name}_diff"] == 0 for name in ("n_total", "n_missing", "n_unique"))
    result["col_type"] = left.get("col_type") or right.get("col_type") or ""
    result["verdict"] = "match" if count_match and all(checks) else "diff"
    return result
