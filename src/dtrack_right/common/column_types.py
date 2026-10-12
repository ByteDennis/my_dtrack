from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .dates import resolve_date_type


class DateColumnType:
    """Portable side-aware calendar-day normalization for remote statistics."""

    def __init__(self, left_date_type: Any = None, right_date_type: Any = None):
        self.left = resolve_date_type(left_date_type or "date")
        self.right = resolve_date_type(right_date_type or "date")

    def _side(self, side: str):
        if side not in {"left", "right"}:
            raise ValueError("column type side must be left or right")
        return self.left if side == "left" else self.right

    def normalize_sql(self, column: str, dialect: str, side: str) -> str:
        return self._side(side).normalize_sql(column, dialect)

    def spec(self) -> dict[str, Any]:
        return {
            "type": "date",
            "left_date_type": self.left.spec(portable=True),
            "right_date_type": self.right.spec(portable=True),
        }


def resolve_column_type(spec: Any) -> DateColumnType:
    if isinstance(spec, DateColumnType):
        return spec
    if not isinstance(spec, Mapping):
        raise TypeError("remote column type must be a portable object")
    unknown = set(spec) - {"type", "left_date_type", "right_date_type"}
    if unknown:
        raise ValueError(f"unsupported column type fields: {', '.join(sorted(unknown))}")
    if spec.get("type") != "date":
        raise ValueError("portable column type must have type 'date'")
    return DateColumnType(spec.get("left_date_type"), spec.get("right_date_type"))
