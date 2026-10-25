from __future__ import annotations

from typing import Any

from ..model.hashing import FAMILIES, DateKeyFamily, KeyFamily, key_family, register_family

ColumnType = KeyFamily
DateColumnType = DateKeyFamily
COLUMN_TYPES = FAMILIES


# >>> registers a normalizer class under a name; column types and key families share one registry <<< #
def register_column_type(name: str):
    return register_family(name)


# >>> resolves a column type spec through the shared normalizer registry <<< #
def resolve_column_type(spec: Any) -> KeyFamily:
    return key_family(spec)
