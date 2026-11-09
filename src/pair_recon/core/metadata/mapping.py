from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pair_recon_left.backends.sources import CsvSource, HadoopSource, OracleSource
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


def _discover_sql_columns(config: dict[str, Any], source_factory) -> list[ColumnInfo]:
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


def discover_oracle_columns(
    config: dict[str, Any], *, source_factory=OracleSource
) -> list[ColumnInfo]:
    if str(config.get("source") or "").casefold() != "oracle":
        raise ValueError("Oracle column discovery requires source='oracle'")
    return _discover_sql_columns(config, source_factory)


def discover_hadoop_columns(
    config: dict[str, Any], *, source_factory=HadoopSource
) -> list[ColumnInfo]:
    if str(config.get("source") or "").casefold() != "hadoop":
        raise ValueError("Hadoop column discovery requires source='hadoop'")
    return _discover_sql_columns(config, source_factory)


def discover_csv_columns(config: dict[str, Any], *, source_factory=CsvSource) -> list[ColumnInfo]:
    if str(config.get("source") or "").casefold() != "csv":
        raise ValueError("CSV column discovery requires source='csv'")
    return _discover_sql_columns(config, source_factory)


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


def discover_columns(config: dict[str, Any], *, sql=None) -> list[ColumnInfo]:
    source = str(config.get("source") or "").casefold()
    if source == "oracle":
        return discover_oracle_columns(config)
    if source == "hadoop":
        return discover_hadoop_columns(config)
    if source == "csv":
        return discover_csv_columns(config)
    if source == "databricks":
        if sql is None:
            raise ValueError("Databricks column discovery requires a SQL executor")
        return discover_databricks_columns(sql, config)
    raise ValueError(f"Unsupported column discovery source: {source!r}")


def _configured_relation(config: dict[str, Any]) -> dict[str, Any]:
    return {key: config[key] for key in ("catalog", "schema", "table", "path") if config.get(key)}


def discover_oracle_relations(
    config: dict[str, Any], *, source_factory=OracleSource
) -> dict[str, Any]:
    if str(config.get("source") or "").casefold() != "oracle":
        raise ValueError("Oracle relation discovery requires source='oracle'")
    source = source_factory(config)
    relation = source.relation()
    result = {"source": "oracle", "relation": relation, **_configured_relation(config)}
    table = str(config.get("table") or "").strip()
    if not table:
        return {**result, "primary_keys": []}

    connection = source.connect()
    cursor = connection.cursor()
    try:
        owner = str(config.get("schema") or "").strip() or None
        statement = """
            SELECT text
            FROM all_views
            WHERE upper(view_name) = upper(:view_name)
              AND (:owner IS NULL OR upper(owner) = upper(:owner))
        """
        cursor.execute(statement, {"view_name": table, "owner": owner})
        row = cursor.fetchone()
        base_table = table
        if row and row[0]:
            match = re.search(r"\bFROM\s+(?:[\w$#]+\.)?([\w$#]+)", str(row[0]), flags=re.IGNORECASE)
            if match:
                base_table = match.group(1)
        cursor.execute(
            """
            SELECT acc.column_name
            FROM all_constraints ac
            JOIN all_cons_columns acc
              ON ac.owner = acc.owner
             AND ac.constraint_name = acc.constraint_name
            WHERE ac.constraint_type = 'P'
              AND upper(ac.table_name) = upper(:table_name)
              AND (:owner IS NULL OR upper(ac.owner) = upper(:owner))
            ORDER BY acc.position
            """,
            {"table_name": base_table, "owner": owner},
        )
        return {
            **result,
            "table": base_table,
            "primary_keys": [str(item[0]) for item in cursor.fetchall()],
        }
    finally:
        cursor.close()
        connection.close()


def discover_hadoop_relations(
    config: dict[str, Any], *, source_factory=HadoopSource
) -> dict[str, Any]:
    if str(config.get("source") or "").casefold() != "hadoop":
        raise ValueError("Hadoop relation discovery requires source='hadoop'")
    source = source_factory(config)
    result = {
        "source": "hadoop",
        "relation": source.relation(),
        **_configured_relation(config),
        "primary_keys": [],
    }
    table = str(config.get("table") or "").strip()
    if not table:
        return result

    connection = source.connect()
    cursor = connection.cursor()
    try:
        import pyodbc

        primary_keys = getattr(cursor, "primaryKeys", None)
        if primary_keys is None:
            return result
        rows = primary_keys(table=table, schema=config.get("schema")).fetchall()
        keys = []
        for row in rows:
            position = getattr(row, "KEY_SEQ", None)
            column = getattr(row, "COLUMN_NAME", None)
            keys.append((int(position if position is not None else row[4]), str(column or row[3])))
        keys.sort(key=lambda item: item[0])
        return {**result, "primary_keys": [name for _position, name in keys]}
    except pyodbc.Error:
        return result
    finally:
        cursor.close()
        connection.close()


def discover_csv_relations(config: dict[str, Any]) -> dict[str, Any]:
    if str(config.get("source") or "").casefold() != "csv":
        raise ValueError("CSV relation discovery requires source='csv'")
    path = str(config.get("path") or "").strip()
    if not path:
        raise ValueError("CSV relation discovery requires path")
    return {
        "source": "csv",
        "relation": path,
        "path": path,
        "table": Path(path).name,
        "primary_keys": [],
    }


def discover_databricks_relations(sql, config: dict[str, Any]) -> list[dict[str, str]]:
    if str(config.get("source") or "").casefold() != "databricks":
        raise ValueError("Databricks relation discovery requires source='databricks'")
    table = str(config.get("table") or "").strip()
    if not table:
        return []
    rows = sql.execute(
        """
        SELECT table_catalog, table_schema, table_name
        FROM system.information_schema.tables
        WHERE upper(table_name) LIKE upper(:table_name)
        ORDER BY table_catalog, table_schema, table_name
        """,
        {"table_name": f"%{table}%"},
    )
    return [
        {
            "catalog": str(row["table_catalog"]),
            "schema": str(row["table_schema"]),
            "table": str(row["table_name"]),
        }
        for row in rows
    ]


def discover_databricks_partition(sql, config: dict[str, Any]) -> str | None:
    if str(config.get("source") or "").casefold() != "databricks":
        raise ValueError("Databricks partition discovery requires source='databricks'")
    if str(config.get("kind") or "table").casefold() != "table":
        return None
    relation = SparkTarget(None, config).relation()
    rows = sql.execute(f"SHOW CREATE TABLE {relation}")
    ddl = "\n".join(
        str(row.get("createtab_stmt") or next(iter(row.values()))) for row in rows if row
    )
    match = re.search(r"PARTITIONED\s+BY\s*\(\s*`?([^`,)\s]+)`?", ddl, flags=re.IGNORECASE)
    return match.group(1) if match else None


def discover_relations(config: dict[str, Any], *, sql=None):
    source = str(config.get("source") or "").casefold()
    if source == "oracle":
        return discover_oracle_relations(config)
    if source == "hadoop":
        return discover_hadoop_relations(config)
    if source == "csv":
        return discover_csv_relations(config)
    if source == "databricks":
        if sql is None:
            raise ValueError("Databricks relation discovery requires a SQL executor")
        return discover_databricks_relations(sql, config)
    raise ValueError(f"Unsupported relation discovery source: {source!r}")


def get_date_column_type(config: dict[str, Any], columns, sql=None) -> dict[str, Any]:
    date_col = str(config.get("date_col") or "").strip()
    if not date_col:
        return {"column": None, "data_type": None}
    data_type = next(
        (
            column.data_type
            for column in (_column(value) for value in columns)
            if column.name.casefold() == date_col.casefold()
        ),
        None,
    )
    result = {"column": date_col, "data_type": data_type}
    if sql is not None and str(config.get("source") or "").casefold() == "databricks":
        relation = SparkTarget(None, config).relation()
        rows = sql.execute(f"SELECT `{date_col}` FROM {relation} LIMIT 1")
        result["sample_value"] = next(iter(rows[0].values())) if rows else None
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


def upsert_column_mappings(
    sql, table: str, pair_name: str, mappings: Iterable[dict], *, overwrite: bool = True
) -> dict:
    """Replace one Pair's mappings, or patch that Pair with overwrite=False.

    An empty overwrite clears only that Pair. Empty patches do no work.
    """
    if not isinstance(overwrite, bool):
        raise TypeError("overwrite must be true or false")
    table = validate_table_name(table)
    pair = str(pair_name or "").strip()
    if not pair:
        raise ValueError("pair_name is required")
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
        rows.append(row)
    if rows or overwrite:
        parameters = {"mappings_json": json.dumps(rows, ensure_ascii=False)}
        source = """SELECT m.* FROM (
  SELECT explode(from_json(:mappings_json,
    'ARRAY<STRUCT<pair_name:STRING,left_column:STRING,right_column:STRING,data_type:STRING>>'
  )) AS m
) AS incoming"""
        statement = f"""MERGE INTO {table} AS t
USING (
{source}
) AS s
ON t.pair_name = s.pair_name AND t.left_column = s.left_column
WHEN MATCHED THEN UPDATE SET
  t.right_column = s.right_column,
  t.data_type = s.data_type
WHEN NOT MATCHED THEN INSERT (pair_name, left_column, right_column, data_type)
VALUES (s.pair_name, s.left_column, s.right_column, s.data_type)"""
        if overwrite:
            parameters["pair_name"] = pair
            statement += "\nWHEN NOT MATCHED BY SOURCE AND t.pair_name = :pair_name THEN DELETE"
        sql.execute(statement, parameters)
    return {
        "table": table,
        "pair_name": pair,
        "rows": len(rows),
        "mappings": rows,
        "overwrite": overwrite,
    }
