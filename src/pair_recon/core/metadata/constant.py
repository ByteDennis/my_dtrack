from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

CATALOG_SCHEMA_ENV = "DBX_CATALOG_SCHEMA"
LEGACY_PAIR_TABLE_ENV = "DBX_PAIR_TABLE"
LEGACY_STATUS_TABLE_ENV = "DBX_STATUS_TABLE"
LEGACY_ERROR_TABLE_ENV = "DBX_ERROR_TABLE"

PAIR_META_TABLE = "pair_recon_pair_meta"
COLUMN_MAP_TABLE = "pair_recon_column_map"
PAIR_SQL_TABLE = "pair_recon_pair_sql"
RUN_STATUS_TABLE = "pair_recon_run_status"
RUN_ERROR_TABLE = "pair_recon_run_error"
WINDOW_HISTORY_TABLE = "pair_recon_window_history"
PROGRESS_SNAPSHOT_TABLE = "pair_recon_progress_snapshot"
REFERENCE_TABLE = "pair_recon_reference"
CACHE_INDEX_TABLE = "pair_recon_cache_index"
NORMALIZATION_TABLE = "pair_recon_normalization"

# Catalog-owned Pair settings that also provide run defaults.
CATALOG_RUN_DEFAULT_FIELDS = ("partition", "vintage", "excluded_columns")
# Missing catalog values explicitly clear a previously saved local selection.
CATALOG_RESETTABLE_RUN_DEFAULT_FIELDS = ("use_modular",)

TABLE_NAMES = (
    PAIR_META_TABLE,
    COLUMN_MAP_TABLE,
    PAIR_SQL_TABLE,
    RUN_STATUS_TABLE,
    RUN_ERROR_TABLE,
    WINDOW_HISTORY_TABLE,
    PROGRESS_SNAPSHOT_TABLE,
    REFERENCE_TABLE,
    CACHE_INDEX_TABLE,
    NORMALIZATION_TABLE,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){2}$")


def validate_catalog_schema(value: str) -> str:
    namespace = str(value or "").strip()
    parts = namespace.split(".")
    if len(parts) != 2 or any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError("catalog schema must be catalog.schema")
    return namespace


def legacy_catalog_tables(
    environment: Mapping[str, str] | None = None,
) -> tuple[str | None, dict[str, str]]:
    values = os.environ if environment is None else environment
    tables = {}
    for key, name in (
        ("pair_meta", LEGACY_PAIR_TABLE_ENV),
        ("run_status", LEGACY_STATUS_TABLE_ENV),
        ("run_error", LEGACY_ERROR_TABLE_ENV),
    ):
        value = str(values.get(name) or "").strip()
        if not value:
            continue
        if not _TABLE.fullmatch(value):
            raise ValueError(f"{name} must be catalog.schema.table")
        tables[key] = value
    first = next(
        (tables[key] for key in ("pair_meta", "run_status", "run_error") if key in tables),
        None,
    )
    return (first.rsplit(".", 1)[0] if first else None), tables


@dataclass(frozen=True, slots=True)
class CatalogTables:
    catalog_schema: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "catalog_schema", validate_catalog_schema(self.catalog_schema))

    def table(self, name: str) -> str:
        if name not in TABLE_NAMES:
            raise ValueError(f"unknown PairRecon table: {name}")
        return f"{self.catalog_schema}.{name}"

    @property
    def pair_meta(self) -> str:
        return self.table(PAIR_META_TABLE)

    @property
    def column_map(self) -> str:
        return self.table(COLUMN_MAP_TABLE)

    @property
    def pair_sql(self) -> str:
        return self.table(PAIR_SQL_TABLE)

    @property
    def run_status(self) -> str:
        return self.table(RUN_STATUS_TABLE)

    @property
    def run_error(self) -> str:
        return self.table(RUN_ERROR_TABLE)

    @property
    def window_history(self) -> str:
        return self.table(WINDOW_HISTORY_TABLE)

    @property
    def progress_snapshot(self) -> str:
        return self.table(PROGRESS_SNAPSHOT_TABLE)

    @property
    def reference(self) -> str:
        return self.table(REFERENCE_TABLE)

    @property
    def cache_index(self) -> str:
        return self.table(CACHE_INDEX_TABLE)

    @property
    def normalization(self) -> str:
        return self.table(NORMALIZATION_TABLE)


def catalog_tables(value: str) -> CatalogTables:
    return CatalogTables(value)
