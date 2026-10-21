from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pair_recon_left.common.model.manifest import sample_plan
from pair_recon_left.common.sql.query_windows import normalize_query_windows

from . import companion_table, validate_table_name
from .configuration import build_pair_config

PAIR_WORKBOOK_COLUMNS = (
    "pair_name",
    "enabled",
    "key_columns",
    "vintage",
    "left_source",
    "left_conn",
    "left_table",
    "left_date_col",
    "left_options_json",
    "right_source",
    "right_conn",
    "right_table",
    "right_date_col",
    "right_options_json",
    "run_defaults_json",
)
PAIR_SQL_WORKBOOK_COLUMNS = (
    "pair_name",
    "left_sql",
    "left_query_windows_json",
    "right_sql",
    "right_query_windows_json",
)
FIELD_GUIDE_COLUMNS = (
    "worksheet",
    "field_name",
    "required_when",
    "type_default",
    "description",
    "example",
)
LEFT_SOURCES = {"oracle", "hadoop", "csv"}
RIGHT_SOURCES = {"databricks_table", "databricks_query", "databricks_files", "csv"}
VINTAGES = {"day", "week", "month", "quarter", "year"}
LEFT_OPTION_FIELDS = {
    "where",
    "date_type",
    "snapshot",
    "service",
    "user_env",
    "password_env",
    "dsn_env",
    "autocommit",
    "nullstr",
    "date_format",
    "options",
    "sample_hash",
}
RIGHT_OPTION_FIELDS = {"where", "date_type", "format", "schema_hints", "options"}
RUN_DEFAULT_FIELDS = {
    "run_mode",
    "partition_mode",
    "sample",
    "strict",
    "checks",
    "vintage_sample_size",
    "vintage_values",
}

FIELD_GUIDE = (
    ("pairs", "pair_name", "always", "text", "Unique Pair name.", "orders"),
    ("pairs", "enabled", "always", "boolean", "Show Pair in runs.", "TRUE"),
    ("pairs", "key_columns", "when sampled", "comma list", "Stable sample keys.", "ORDER_ID"),
    ("pairs", "vintage", "always", "day|week|month|quarter|year", "Stats bucket.", "month"),
    ("pairs", "left_source", "always", "oracle|hadoop|csv", "Left source type.", "oracle"),
    ("pairs", "left_conn", "always", "text", "Macro, schema, or CSV path.", "PCDS"),
    ("pairs", "left_table", "unless SQL/CSV", "text", "Left table.", "SALES.ORDERS"),
    (
        "pairs",
        "left_date_col",
        "date mode except snapshot",
        "text",
        "Left date column.",
        "ORDER_DT",
    ),
    (
        "pairs",
        "left_options_json",
        "optional",
        "JSON object",
        "Rare Left settings.",
        '{"where":"ACTIVE=1"}',
    ),
    ("pairs", "right_source", "always", "source enum", "Right source type.", "databricks_table"),
    ("pairs", "right_conn", "depends on source", "text", "Catalog/schema or path.", "main.sales"),
    ("pairs", "right_table", "Databricks table", "text", "Right table.", "orders"),
    ("pairs", "right_date_col", "date mode", "text", "Right date column.", "order_dt"),
    (
        "pairs",
        "right_options_json",
        "optional",
        "JSON object",
        "Rare Right settings.",
        '{"format":"parquet"}',
    ),
    (
        "pairs",
        "run_defaults_json",
        "optional",
        "JSON object",
        "Run defaults; prefer sample fractions.",
        '{"sample":0.1}',
    ),
    ("pair_sql", "pair_name", "always", "text", "Must exist in pairs.", "orders"),
    ("pair_sql", "left_sql", "optional", "SQL", "Left base query.", "SELECT * FROM SALES.ORDERS"),
    (
        "pair_sql",
        "left_query_windows_json",
        "when Left markers exist",
        "JSON object",
        "Left marker specs.",
        '{"driver":{"expression":"l.DT"}}',
    ),
    (
        "pair_sql",
        "right_sql",
        "Databricks query",
        "SQL",
        "Right base query.",
        "SELECT * FROM main.sales.orders",
    ),
    (
        "pair_sql",
        "right_query_windows_json",
        "when Right markers exist",
        "JSON object",
        "Right marker specs.",
        '{"driver":{"expression":"r.dt"}}',
    ),
)


def create_pair_workbook(path: str | Path) -> Path:
    from openpyxl import Workbook
    from openpyxl.comments import Comment
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation
    from openpyxl.worksheet.table import Table, TableStyleInfo

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    pairs = workbook.active
    pairs.title = "pairs"
    pair_sql = workbook.create_sheet("pair_sql")
    guide = workbook.create_sheet("field_guide")

    descriptions = {(row[0], row[1]): row[4] for row in FIELD_GUIDE}
    for sheet, columns, table_name in (
        (pairs, PAIR_WORKBOOK_COLUMNS, "PairReconPairs"),
        (pair_sql, PAIR_SQL_WORKBOOK_COLUMNS, "PairReconSql"),
    ):
        sheet.append(columns)
        sheet.append([None] * len(columns))
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = f"A1:{get_column_letter(len(columns))}2"
        for index, name in enumerate(columns, 1):
            cell = sheet.cell(1, index)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1F4E78")
            description = descriptions[(sheet.title, name)]
            cell.comment = Comment(description, "PairRecon")
            sheet.column_dimensions[cell.column_letter].width = min(
                56, max(14, len(name) + 4, len(description) + 2)
            )
        table = Table(
            displayName=table_name,
            ref=f"A1:{get_column_letter(len(columns))}2",
        )
        table.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium2",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        sheet.add_table(table)

    validations = (
        ("B2:B1048576", '"TRUE,FALSE"'),
        ("D2:D1048576", '"day,week,month,quarter,year"'),
        ("E2:E1048576", '"oracle,hadoop,csv"'),
        (
            "J2:J1048576",
            '"databricks_table,databricks_query,databricks_files,csv"',
        ),
    )
    for cells, formula in validations:
        validation = DataValidation(type="list", formula1=formula, allow_blank=False)
        pairs.add_data_validation(validation)
        validation.add(cells)

    guide.append(FIELD_GUIDE_COLUMNS)
    for row in FIELD_GUIDE:
        guide.append(row)
    guide.freeze_panes = "A2"
    guide.auto_filter.ref = f"A1:F{guide.max_row}"
    for cell in guide[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="548235")
    for index, width in enumerate((14, 30, 28, 28, 34, 42), 1):
        guide.column_dimensions[get_column_letter(index)].width = width

    temporary = target.with_name(f".{target.name}.part")
    workbook.save(temporary)
    temporary.replace(target)
    return target


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _boolean(value: Any, *, row: int, field: str = "enabled") -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float) and value in {0, 1}:
        return bool(value)
    text = str(value or "").strip().casefold()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    raise ValueError(f"pairs.xlsx row {row}: {field} must be true or false")


def _text(value: Any) -> str | None:
    return None if _blank(value) else str(value).strip()


def _json_object(value: Any, *, row: int, field: str) -> dict[str, Any]:
    if _blank(value):
        return {}
    if isinstance(value, dict):
        parsed = value
    else:
        try:
            parsed = json.loads(str(value))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(f"pairs.xlsx row {row}: {field} must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise TypeError(f"pairs.xlsx row {row}: {field} must be a JSON object")
    return parsed


def _json_text(value: dict[str, Any]) -> str | None:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        if value
        else None
    )


def _key_columns(value: Any, *, row: int) -> list[str]:
    if _blank(value):
        return []
    values = [item.strip() for item in str(value).split(",") if item.strip()]
    if not values:
        raise ValueError(f"pairs.xlsx row {row}: key_columns must name one or more columns")
    return list(dict.fromkeys(values))


def _options(value: Any, *, row: int, field: str, allowed: set[str]) -> dict[str, Any]:
    parsed = _json_object(value, row=row, field=field)
    unknown = sorted(set(parsed) - allowed)
    if unknown:
        raise ValueError(
            f"pairs.xlsx row {row}: {field} has unsupported fields: {', '.join(unknown)}"
        )
    return parsed


def _validate_run_defaults(value: dict[str, Any], *, row: int, keys: list[str]) -> None:
    if value.get("run_mode") not in (None, "full", "left-only"):
        raise ValueError(f"pairs.xlsx row {row}: run_mode must be full or left-only")
    mode = value.get("partition_mode", "date")
    if mode not in {"date", "all"}:
        raise ValueError(f"pairs.xlsx row {row}: partition_mode must be date or all")
    if "strict" in value and not isinstance(value["strict"], bool):
        raise TypeError(f"pairs.xlsx row {row}: strict must be true or false")
    if "checks" in value and (
        not isinstance(value["checks"], list)
        or not value["checks"]
        or not all(isinstance(item, str) and item.strip() for item in value["checks"])
    ):
        raise TypeError(f"pairs.xlsx row {row}: checks must be a non-empty string array")
    sampling = sample_plan(value.get("sample", False))
    if sampling["mode"] != "all" and not keys:
        raise ValueError(f"pairs.xlsx row {row}: sampled runs require key_columns")
    size = value.get("vintage_sample_size")
    vintages = value.get("vintage_values")
    if size not in (None, "") and (
        isinstance(size, bool) or not isinstance(size, int) or size <= 0
    ):
        raise ValueError(f"pairs.xlsx row {row}: vintage_sample_size must be a positive integer")
    if vintages not in (None, []) and (
        not isinstance(vintages, list)
        or not all(isinstance(item, str) and item.strip() for item in vintages)
    ):
        raise TypeError(f"pairs.xlsx row {row}: vintage_values must be a string array")
    if size not in (None, "") and vintages not in (None, []):
        raise ValueError(
            f"pairs.xlsx row {row}: use vintage_sample_size or vintage_values, not both"
        )
    if mode == "all" and (size not in (None, "") or vintages not in (None, [])):
        raise ValueError(f"pairs.xlsx row {row}: all mode cannot select vintages")


def _read_rows(sheet, columns: tuple[str, ...]) -> list[tuple[int, dict[str, Any]]]:
    rows = sheet.iter_rows(values_only=True)
    header = tuple(str(value or "").strip() for value in next(rows, ()))
    if header != columns:
        raise ValueError(f"pairs.xlsx {sheet.title} columns must be exactly: {', '.join(columns)}")
    return [
        (number, dict(zip(columns, values)))
        for number, values in enumerate(rows, 2)
        if not all(_blank(value) for value in values)
    ]


def _read_pair_rows(sheet) -> tuple[list[dict[str, Any]], dict[str, int]]:
    result = []
    seen = {}
    for number, raw in _read_rows(sheet, PAIR_WORKBOOK_COLUMNS):
        pair = str(raw["pair_name"] or "").strip()
        if not pair:
            raise ValueError(f"pairs.xlsx row {number}: pair_name is required")
        if pair in seen:
            raise ValueError(f"pairs.xlsx row {number}: duplicate pair_name {pair!r}")
        seen[pair] = number
        vintage = str(raw["vintage"] or "").strip().casefold()
        if vintage not in VINTAGES:
            raise ValueError(
                f"pairs.xlsx row {number}: vintage must be one of {', '.join(sorted(VINTAGES))}"
            )
        left_source = str(raw["left_source"] or "").strip().casefold()
        right_source = str(raw["right_source"] or "").strip().casefold()
        if left_source not in LEFT_SOURCES:
            raise ValueError(f"pairs.xlsx row {number}: unsupported left_source {left_source!r}")
        if right_source not in RIGHT_SOURCES:
            raise ValueError(f"pairs.xlsx row {number}: unsupported right_source {right_source!r}")
        keys = _key_columns(raw["key_columns"], row=number)
        left_options = _options(
            raw["left_options_json"],
            row=number,
            field="left_options_json",
            allowed=LEFT_OPTION_FIELDS,
        )
        right_options = _options(
            raw["right_options_json"],
            row=number,
            field="right_options_json",
            allowed=RIGHT_OPTION_FIELDS,
        )
        defaults = _options(
            raw["run_defaults_json"],
            row=number,
            field="run_defaults_json",
            allowed=RUN_DEFAULT_FIELDS,
        )
        _validate_run_defaults(defaults, row=number, keys=keys)
        if "snapshot" in left_options and not isinstance(left_options["snapshot"], bool):
            raise TypeError(f"pairs.xlsx row {number}: snapshot must be true or false")
        result.append(
            {
                "pair_name": pair,
                "enabled": _boolean(raw["enabled"], row=number),
                "key_columns": keys,
                "vintage": vintage,
                "left_source": left_source,
                "left_conn": _text(raw["left_conn"]),
                "left_table": _text(raw["left_table"]),
                "left_date_col": _text(raw["left_date_col"]),
                "left_options_json": _json_text(left_options),
                "right_source": right_source,
                "right_conn": _text(raw["right_conn"]),
                "right_table": _text(raw["right_table"]),
                "right_date_col": _text(raw["right_date_col"]),
                "right_options_json": _json_text(right_options),
                "run_defaults_json": _json_text(defaults),
            }
        )
    return result, seen


def _read_sql_rows(sheet, pairs: dict[str, int]) -> dict[str, dict[str, Any]]:
    result = {}
    for number, raw in _read_rows(sheet, PAIR_SQL_WORKBOOK_COLUMNS):
        pair = str(raw["pair_name"] or "").strip()
        if not pair:
            raise ValueError(f"pairs.xlsx pair_sql row {number}: pair_name is required")
        if pair not in pairs:
            raise ValueError(f"pairs.xlsx pair_sql row {number}: unknown pair_name {pair!r}")
        if pair in result:
            raise ValueError(f"pairs.xlsx pair_sql row {number}: duplicate pair_name {pair!r}")
        result[pair] = {
            "left_sql": _text(raw["left_sql"]),
            "left_query_windows_json": _json_text(
                _json_object(
                    raw["left_query_windows_json"],
                    row=number,
                    field="left_query_windows_json",
                )
            ),
            "right_sql": _text(raw["right_sql"]),
            "right_query_windows_json": _json_text(
                _json_object(
                    raw["right_query_windows_json"],
                    row=number,
                    field="right_query_windows_json",
                )
            ),
        }
    return result


def _validate_pair(row: dict[str, Any], sql_row: dict[str, Any] | None, *, number: int) -> None:
    prefix = f"pairs.xlsx row {number}"
    left_sql = (sql_row or {}).get("left_sql")
    right_sql = (sql_row or {}).get("right_sql")
    if not row["left_conn"]:
        raise ValueError(f"{prefix}: left_conn is required")
    if row["left_source"] in {"oracle", "hadoop"} and not (row["left_table"] or left_sql):
        raise ValueError(f"{prefix}: left_table or left_sql is required")
    if row["left_source"] == "hadoop" and row["left_table"] and "." in row["left_table"]:
        raise ValueError(
            f"{prefix}: Hadoop left_table must be unqualified; use left_conn for schema"
        )
    if row["left_source"] == "csv" and (row["left_table"] or left_sql):
        raise ValueError(f"{prefix}: CSV Left cannot use left_table or left_sql")
    if row["right_source"] == "databricks_table":
        if not row["right_conn"] or not row["right_table"]:
            raise ValueError(f"{prefix}: Databricks table requires right_conn and right_table")
        if len(row["right_conn"].split(".")) != 2:
            raise ValueError(f"{prefix}: right_conn must be catalog.schema")
        if "." in row["right_table"]:
            raise ValueError(
                f"{prefix}: Databricks right_table must be unqualified; use right_conn for catalog.schema"
            )
        if right_sql:
            raise ValueError(f"{prefix}: use right_source=databricks_query for right_sql")
    elif row["right_source"] == "databricks_query":
        if not right_sql:
            raise ValueError(f"{prefix}: databricks_query requires right_sql")
        if row["right_conn"] or row["right_table"]:
            raise ValueError(f"{prefix}: databricks_query leaves right_conn/right_table blank")
    else:
        if not row["right_conn"]:
            raise ValueError(f"{prefix}: {row['right_source']} requires right_conn")
        if row["right_table"]:
            raise ValueError(f"{prefix}: {row['right_source']} leaves right_table blank")
        if row["right_source"] == "csv" and right_sql:
            raise ValueError(f"{prefix}: CSV Right cannot use right_sql")
    defaults = _json_object(row["run_defaults_json"], row=number, field="run_defaults_json")
    left_options = _json_object(row["left_options_json"], row=number, field="left_options_json")
    mode = defaults.get("partition_mode", "date")
    snapshot = left_options.get("snapshot", False)
    if mode == "date" and (
        not row["right_date_col"] or (not snapshot and not row["left_date_col"])
    ):
        raise ValueError(f"{prefix}: date mode requires Right and non-snapshot Left date columns")
    for side, source, statement, field in (
        ("Left", row["left_source"], left_sql, "left_query_windows_json"),
        (
            "Right",
            "databricks" if row["right_source"].startswith("databricks") else "csv",
            right_sql,
            "right_query_windows_json",
        ),
    ):
        try:
            windows = normalize_query_windows(
                statement,
                _json_object((sql_row or {}).get(field), row=number, field=field),
                source=source,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{prefix}: invalid {side} query windows: {exc}") from None
        if mode == "all" and windows:
            raise ValueError(f"{prefix}: all mode cannot use {side} query windows")


def read_pair_workbook(path: str | Path) -> list[dict[str, Any]]:
    from openpyxl import load_workbook

    workbook = load_workbook(Path(path), read_only=False, data_only=True)
    expected = ["pairs", "pair_sql", "field_guide"]
    if workbook.sheetnames != expected:
        raise ValueError(f"pairs.xlsx worksheets must be exactly: {', '.join(expected)}")
    rows, pair_numbers = _read_pair_rows(workbook["pairs"])
    sql_rows = _read_sql_rows(workbook["pair_sql"], pair_numbers)
    for row in rows:
        pair_sql = sql_rows.get(row["pair_name"])
        _validate_pair(row, pair_sql, number=pair_numbers[row["pair_name"]])
        row["pair_sql"] = pair_sql
    return rows


def upsert_pair_configuration(sql, pair_table: str, rows: list[dict[str, Any]]) -> dict:
    pair_table = validate_table_name(pair_table)
    pair_sql_table = companion_table(pair_table, "pair_recon_pair_sql")
    meta_statement = f"""MERGE INTO {pair_table} AS t
USING (SELECT :pair_name AS pair_name) AS s ON t.pair_name = s.pair_name
WHEN MATCHED THEN UPDATE SET
  t.enabled = CAST(:enabled AS BOOLEAN),
  t.key_columns = from_json(:key_columns, 'ARRAY<STRING>'),
  t.vintage = :vintage,
  t.left_source = :left_source,
  t.left_conn = :left_conn,
  t.left_table = :left_table,
  t.left_date_col = :left_date_col,
  t.left_options_json = :left_options_json,
  t.right_source = :right_source,
  t.right_conn = :right_conn,
  t.right_table = :right_table,
  t.right_date_col = :right_date_col,
  t.right_options_json = :right_options_json,
  t.run_defaults_json = :run_defaults_json,
  t.config_json = :config_json,
  t.updated_at = CAST(current_timestamp() AS TIMESTAMP_NTZ)
WHEN NOT MATCHED THEN INSERT (
  pair_name, enabled, key_columns, vintage,
  left_source, left_conn, left_table, left_date_col, left_options_json,
  right_source, right_conn, right_table, right_date_col, right_options_json,
  run_defaults_json, config_json, latest_run_id, latest_status, updated_at
) VALUES (
  :pair_name, CAST(:enabled AS BOOLEAN), from_json(:key_columns, 'ARRAY<STRING>'), :vintage,
  :left_source, :left_conn, :left_table, :left_date_col, :left_options_json,
  :right_source, :right_conn, :right_table, :right_date_col, :right_options_json,
  :run_defaults_json, :config_json, NULL, NULL, CAST(current_timestamp() AS TIMESTAMP_NTZ)
)"""
    pair_sql_statement = f"""MERGE INTO {pair_sql_table} AS t
USING (SELECT :pair_name AS pair_name) AS s ON t.pair_name = s.pair_name
WHEN MATCHED THEN UPDATE SET
  t.left_sql = :left_sql,
  t.left_query_windows_json = :left_query_windows_json,
  t.right_sql = :right_sql,
  t.right_query_windows_json = :right_query_windows_json,
  t.updated_at = CAST(current_timestamp() AS TIMESTAMP_NTZ)
WHEN NOT MATCHED THEN INSERT (
  pair_name, left_sql, left_query_windows_json,
  right_sql, right_query_windows_json, updated_at
) VALUES (
  :pair_name, :left_sql, :left_query_windows_json,
  :right_sql, :right_query_windows_json, CAST(current_timestamp() AS TIMESTAMP_NTZ)
)"""
    sql_rows = 0
    for row in rows:
        effective = build_pair_config(row)
        effective.pop("_run_defaults", None)
        values = {
            **row,
            "enabled": "true" if row["enabled"] else "false",
            "key_columns": json.dumps(row["key_columns"], ensure_ascii=False),
            "config_json": json.dumps(
                effective, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ),
            **{
                name: (row.get("pair_sql") or {}).get(name)
                for name in PAIR_SQL_WORKBOOK_COLUMNS
                if name != "pair_name"
            },
        }
        sql.execute(meta_statement, values)
        sql.execute(pair_sql_statement, values)
        sql_rows += int(row.get("pair_sql") is not None)
    return {
        "pair_meta_table": pair_table,
        "pair_sql_table": pair_sql_table,
        "rows": len(rows),
        "sql_rows": sql_rows,
        "pairs": [row["pair_name"] for row in rows],
    }
