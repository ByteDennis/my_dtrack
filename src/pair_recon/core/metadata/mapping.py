from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from pair_recon_left.backends.sources import OracleSource
from pair_recon_right.engine.backends.targets import SparkTarget

from . import validate_table_name


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str | None = None


def _type_name(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "name", None) or getattr(value, "__name__", None) or value)


def discover_oracle_columns(config: dict[str, Any], *, source_factory=OracleSource):
    if str(config.get("source") or "").casefold() != "oracle":
        raise ValueError("Left column discovery requires source='oracle'")
    source = source_factory(config)
    connection = source.connect()
    cursor = connection.cursor()
    try:
        statement = f"SELECT * FROM {source.relation()} WHERE 1=0"
        source._audit(statement, "describe_columns")
        cursor.execute(statement)
        return [
            ColumnInfo(str(item[0]), _type_name(item[1] if len(item) > 1 else None))
            for item in cursor.description or []
        ]
    finally:
        cursor.close()
        connection.close()


def discover_databricks_columns(sql, config: dict[str, Any]) -> list[ColumnInfo]:
    if str(config.get("source") or "").casefold() != "databricks":
        raise ValueError("Right column discovery requires source='databricks'")
    relation = SparkTarget(None, config).relation()
    rows = sql.execute(f"DESCRIBE QUERY SELECT * FROM {relation}")
    result = []
    for row in rows:
        name = row.get("col_name") or row.get("column_name")
        if name is None and row:
            name = next(iter(row.values()))
        if not name or str(name).startswith("#"):
            continue
        data_type = row.get("data_type") or row.get("type")
        result.append(ColumnInfo(str(name), None if data_type is None else str(data_type)))
    return result


def _column(value: ColumnInfo | dict[str, Any] | str) -> ColumnInfo:
    if isinstance(value, ColumnInfo):
        return value
    if isinstance(value, str):
        return ColumnInfo(value)
    return ColumnInfo(str(value["name"]), value.get("data_type"))


def _normalized(name: str) -> str:
    return "".join(character for character in name.casefold() if character.isalnum())


def automatch_columns(
    left_columns: Iterable[ColumnInfo | dict[str, Any] | str],
    right_columns: Iterable[ColumnInfo | dict[str, Any] | str],
) -> list[dict[str, Any]]:
    left = [_column(value) for value in left_columns]
    right = [_column(value) for value in right_columns]
    indexes = (
        ("exact", lambda value: value.name),
        ("casefold", lambda value: value.name.casefold()),
        ("normalized", lambda value: _normalized(value.name)),
    )
    lookups = []
    for method, key in indexes:
        lookup: dict[str, list[ColumnInfo]] = {}
        for column in right:
            lookup.setdefault(key(column), []).append(column)
        lookups.append((method, key, lookup))
    rows = []
    for column in left:
        matched = None
        method = None
        ambiguous = False
        for candidate_method, key, lookup in lookups:
            candidates = lookup.get(key(column), [])
            if len(candidates) == 1:
                matched, method = candidates[0], candidate_method
                break
            if len(candidates) > 1:
                ambiguous = True
                break
        rows.append(
            {
                "left_column": column.name,
                "left_data_type": column.data_type,
                "right_column": matched.name if matched else None,
                "right_data_type": matched.data_type if matched else None,
                "data_type": None,
                "status": "matched" if matched else "ambiguous" if ambiguous else "unmatched",
                "match_method": method,
            }
        )
    matched_right = {}
    for row in rows:
        if row["status"] == "matched":
            matched_right.setdefault(row["right_column"], []).append(row)
    for duplicates in matched_right.values():
        if len(duplicates) > 1:
            for row in duplicates:
                row["status"] = "ambiguous"
    return rows


def upsert_column_mappings(sql, table: str, pair_name: str, mappings: Iterable[dict]) -> dict:
    table = validate_table_name(table)
    pair = str(pair_name or "").strip()
    if not pair:
        raise ValueError("pair_name is required")
    statement = f"""MERGE INTO {table} AS t
USING (SELECT :pair_name AS pair_name, :left_column AS left_column) AS s
ON t.pair_name = s.pair_name AND t.left_column = s.left_column
WHEN MATCHED THEN UPDATE SET
  t.right_column = :right_column,
  t.data_type = :data_type
WHEN NOT MATCHED THEN INSERT (pair_name, left_column, right_column, data_type)
VALUES (:pair_name, :left_column, :right_column, :data_type)"""
    rows = []
    seen = set()
    for raw in mappings:
        left = str(raw.get("left_column") or "").strip()
        right = str(raw.get("right_column") or "").strip()
        if not left or not right:
            raise ValueError("column mappings require left_column and right_column")
        if left in seen:
            raise ValueError(f"duplicate left column mapping: {left}")
        seen.add(left)
        row = {
            "pair_name": pair,
            "left_column": left,
            "right_column": right,
            "data_type": str(raw.get("data_type") or "").strip() or None,
        }
        sql.execute(statement, row)
        rows.append(row)
    return {"table": table, "pair_name": pair, "rows": len(rows), "mappings": rows}
