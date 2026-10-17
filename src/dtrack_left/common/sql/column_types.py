from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from .dates import resolve_date_type


class ColumnType:
    """Side-aware value normalization shared by local and remote statistics."""

    name: ClassVar[str] = "custom"

    def normalize(self, value: object, side: str) -> str:
        raise NotImplementedError

    def normalize_sql(self, column: str, dialect: str, side: str) -> str:
        raise NotImplementedError

    def spec(self) -> dict[str, Any]:
        raise NotImplementedError


class DateColumnType(ColumnType):
    """Compare two physical date representations as one calendar-day value."""

    left_date_type: Any = "date"
    right_date_type: Any = "date"

    def __init__(self, left_date_type: Any = None, right_date_type: Any = None):
        self.left = resolve_date_type(
            self.left_date_type if left_date_type is None else left_date_type
        )
        self.right = resolve_date_type(
            self.right_date_type if right_date_type is None else right_date_type
        )

    def _side(self, side: str):
        if side not in {"left", "right"}:
            raise ValueError("column type side must be left or right")
        return self.left if side == "left" else self.right

    def normalize(self, value: object, side: str) -> str:
        return self._side(side).normalize(value)

    def normalize_sql(self, column: str, dialect: str, side: str) -> str:
        return self._side(side).normalize_sql(column, dialect)

    def spec(self) -> dict[str, Any]:
        return {
            "type": "date",
            "left_date_type": self.left.spec(portable=True),
            "right_date_type": self.right.spec(portable=True),
        }


COLUMN_TYPES: dict[str, ColumnType] = {}


def register_column_type(name: str):
    """Register a zero-argument ColumnType subclass under a short config name."""

    def add(cls: type[ColumnType]) -> type[ColumnType]:
        cls.name = name.lower()
        COLUMN_TYPES[cls.name] = cls()
        return cls

    return add


@register_column_type("sas_datetime_date")
class SasDatetimeDate(DateColumnType):
    left_date_type: ClassVar[Any] = {"type": "sas", "format": "datetime"}
    right_date_type: ClassVar[Any] = "date"


def resolve_column_type(spec: Any) -> ColumnType:
    if isinstance(spec, ColumnType):
        return spec
    if isinstance(spec, str):
        try:
            return COLUMN_TYPES[spec.lower()]
        except KeyError:
            known = ", ".join(sorted(COLUMN_TYPES))
            raise ValueError(f"unknown column type {spec!r}; registered types: {known}") from None
    if isinstance(spec, Mapping):
        unknown = set(spec) - {"type", "left_date_type", "right_date_type"}
        if unknown:
            raise ValueError(f"unsupported column type fields: {', '.join(sorted(unknown))}")
        if spec.get("type") != "date":
            raise ValueError("portable column type must have type 'date'")
        return DateColumnType(spec.get("left_date_type"), spec.get("right_date_type"))
    raise TypeError(f"column type must be a registered name or an object, got {spec!r}")
