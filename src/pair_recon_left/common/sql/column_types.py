from __future__ import annotations

from typing import Any

from ..model.hashing import FAMILIES, DateKeyFamily, KeyFamily, key_family, register_family

ColumnType = KeyFamily
DateColumnType = DateKeyFamily
COLUMN_TYPES = FAMILIES


# >>> registers a named normalizer class; column types and key families share one registry <<< #
def register_column_type(cls: type[KeyFamily]) -> type[KeyFamily]:
    return register_family(cls)


# >>> resolves a column type spec through the shared normalizer registry <<< #
def resolve_column_type(spec: Any) -> KeyFamily:
    return key_family(spec)
