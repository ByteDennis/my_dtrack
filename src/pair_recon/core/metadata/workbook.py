from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from pair_recon.core.metadata.constant import PAIR_SQL_TABLE
from pair_recon_left.common.model.manifest import sample_plan
from pair_recon_left.common.sql.query_windows import normalize_query_windows

from . import companion_table, validate_table_name
from .configuration import KEY_SPEC, build_pair_config, key_type_names, parse_key_columns

PAIR_WORKBOOK_COLUMNS = (
    "pair_name",
    "enabled",
    "key_columns",
    "vintage",
    "partition",
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
    "ldap_dsn",
    "ldap_dsn_env",
    "autocommit",
    "nullstr",
    "date_format",
    "options",
    "sample_hash",
    "arraysize",
    "prefetchrows",
    "fetchsize",
    "fetch_retries",
    "max_sessions",
    "sas_sample_csv",
}
LEFT_CREDENTIAL_FIELDS = {"user", "password"}
RIGHT_OPTION_FIELDS = {"where", "date_type", "format", "schema_hints", "options"}
RUN_DEFAULT_FIELDS = {
    "run_mode",
    "sample",
    "use_modular",
    "checks",
    "audit",
    "sync_remote",
    "left_workers",
    "right_workers",
    "sample_rows",
    "vintage",
    "from_date",
    "to_date",
    "excluded_partitions",
    "excluded_columns",
    "sample_columns",
    "sample_partitions",
    "vintage_sample_size",
    "vintage_values",
    "data_type",
}
RUN_DEFAULT_BOOLS = ("audit", "sync_remote")
RUN_DEFAULT_LISTS = (
    "checks",
    "excluded_partitions",
    "excluded_columns",
    "sample_columns",
    "sample_partitions",
    "vintage_values",
)
RUN_DEFAULT_WORKERS = ("left_workers", "right_workers", "sample_rows")
RUN_DEFAULT_DATE_ONLY = ("from_date", "to_date", "vintage", "excluded_partitions")
VINTAGES = ("day", "week", "month", "quarter", "year")
PARTITIONS = ("day", "month", "all")

FIELD_GUIDE = (
    ("pairs", "pair_name", "always", "text", "Unique Pair name.", "orders"),
    ("pairs", "enabled", "always", "boolean", "Show Pair in runs.", "TRUE"),
    (
        "pairs",
        "key_columns",
        "when sampled",
        "comma list of NAME[type]",
        f"Stable sample keys as NAME[type]; types: {', '.join(key_type_names())}.",
        "ACCT_ID[numeric],ORDER_DT[date]",
    ),
    ("pairs", "vintage", "always", "day|week|month|quarter|year", "Stats bucket.", "month"),
    (
        "pairs",
        "partition",
        "always",
        "day|month|all",
        "Row-count partition grain; all scans the whole relation.",
        "month",
    ),
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
    (
        "pairs",
        "run_defaults_json.checks",
        "optional",
        "comma list or JSON array",
        "Checks to run; comma text is normalized to a string array.",
        "row_count, hash_sum, n_unique",
    ),
    (
        "pairs",
        "run_defaults_json.excluded_columns",
        "optional",
        "comma list or JSON array",
        "Left column names omitted from column comparison.",
        "COMMENTS, INTERNAL_NOTE",
    ),
    (
        "pairs",
        "run_defaults_json.sample_partitions",
        "optional",
        "comma list or JSON array",
        "Vintage IDs eligible for difference samples, in preference order; blank picks the first differing vintage.",
        "2026-01-01, 2026-02-01",
    ),
    (
        "pairs",
        "run_defaults_json.sample_columns",
        "optional",
        "comma list or JSON array",
        "Left column names whose differing rows are saved as evidence; blank keeps all.",
        "AMOUNT, STATUS",
    ),
    (
        "pairs",
        "run_defaults_json.excluded_partitions",
        "optional",
        "comma list or JSON array",
        "Dates or inclusive date ranges omitted from comparison.",
        "2026-01-01, 2026-02-01->2026-02-03",
    ),
    (
        "pairs",
        "run_defaults_json.vintage_values",
        "optional",
        "comma list or JSON array",
        "Explicit vintage dates; cannot be combined with vintage_sample_size.",
        "2026-01-01, 2026-04-01",
    ),
    (
        "pairs",
        "run_defaults_json.from_date",
        "date partition",
        "YYYY-MM-DD",
        "Inclusive lower date bound; a single date, not a list.",
        "2026-01-01",
    ),
    (
        "pairs",
        "run_defaults_json.to_date",
        "date partition",
        "YYYY-MM-DD",
        "Inclusive upper date bound; defaults to today in the run timezone when no effective value is supplied.",
        "2026-12-31",
    ),
    (
        "pairs",
        "left_options_json.sas_sample_csv",
        "optional Oracle/Hadoop sampling",
        "path",
        "One combined SAS sample CSV for all selected vintage windows; SQL and request files are written beside it.",
        "C:/PairRecon/samples/orders/left_sample.csv",
    ),
    (
        "pairs",
        "left_options_json.where",
        "optional",
        "SQL predicate",
        "Additional Left source filter without the WHERE keyword.",
        "ACTIVE_FLAG = 1",
    ),
    (
        "pairs",
        "left_options_json.date_type",
        "optional",
        "date type spec",
        "Overrides automatic Left partition-date detection.",
        "date",
    ),
    (
        "pairs",
        "left_options_json.snapshot",
        "optional",
        "boolean",
        "Treat Left as a current snapshot while Right remains date partitioned.",
        "FALSE",
    ),
    (
        "pairs",
        "left_options_json.oracle_tuning",
        "optional Oracle",
        "JSON numbers",
        "Supported keys: arraysize, prefetchrows, fetchsize, fetch_retries, max_sessions.",
        '{"arraysize":1000,"max_sessions":4}',
    ),
    (
        "pairs",
        "right_options_json.where",
        "optional",
        "SQL predicate",
        "Additional Right source filter without the WHERE keyword.",
        "is_current = true",
    ),
    (
        "pairs",
        "right_options_json.date_type",
        "optional",
        "date type spec",
        "Overrides automatic Right partition-date detection.",
        "date",
    ),
    (
        "pairs",
        "right_options_json.format",
        "Databricks files",
        "text",
        "Input file format for a Databricks files source.",
        "parquet",
    ),
    (
        "pairs",
        "run_defaults_json.run_mode",
        "optional",
        "full|left-only",
        "Default execution mode.",
        "full",
    ),
    (
        "pairs",
        "run_defaults_json.sample",
        "optional",
        "all|fraction|count",
        "Deterministic attribute sample; fractions are recommended.",
        "0.1",
    ),
    (
        "pairs",
        "run_defaults_json.data_type",
        "optional mapping update",
        "JSON object of column to data type",
        "Upsert data_type for mapped Left columns; existing Right column names are preserved.",
        '{"AMOUNT":"numeric","ORDER_DT":"date"}',
    ),
    (
        "pairs",
        "run_defaults_json.use_modular",
        "percentage sampling",
        "Left column name",
        "Numeric cohort selector; sample keys remain record identity keys.",
        "CUST_ID",
    ),
    (
        "pairs",
        "run_defaults_json.audit",
        "optional",
        "boolean",
        "Persist generated SQL audit artifacts.",
        "FALSE",
    ),
    (
        "pairs",
        "run_defaults_json.sync_remote",
        "optional",
        "boolean",
        "Sync the Right package before a remote run.",
        "TRUE",
    ),
    (
        "pairs",
        "run_defaults_json.left_workers",
        "optional",
        "positive integer",
        "Concurrent Left pull worker budget.",
        "2",
    ),
    (
        "pairs",
        "run_defaults_json.right_workers",
        "optional",
        "positive integer",
        "Concurrent Right comparison worker budget.",
        "8",
    ),
    (
        "pairs",
        "run_defaults_json.sample_rows",
        "optional",
        "positive integer",
        "Maximum diagnostic difference rows saved per vintage.",
        "50",
    ),
    (
        "pairs",
        "run_defaults_json.vintage",
        "date partition",
        "day|week|month|quarter|year",
        "Default comparison bucket.",
        "month",
    ),
    (
        "pairs",
        "run_defaults_json.vintage_sample_size",
        "date partition",
        "positive integer",
        "Number of available vintage buckets selected deterministically for this run.",
        "4",
    ),
    (
        "pairs",
        "left_options_json.service",
        "optional Oracle",
        "text",
        "Oracle service override.",
        "orders.example.net",
    ),
    (
        "pairs",
        "left_options_json.user_env",
        "optional Oracle",
        "environment variable name",
        "Environment variable containing the Oracle user; never place the secret in Excel.",
        "ORDERS_ORACLE_USER",
    ),
    (
        "pairs",
        "left_options_json.password_env",
        "optional Oracle",
        "environment variable name",
        "Environment variable containing the Oracle password; never place the secret in Excel.",
        "ORDERS_ORACLE_PASSWORD",
    ),
    (
        "pairs",
        "left_options_json.dsn_env",
        "optional Oracle",
        "environment variable name",
        "Environment variable containing the Oracle DSN.",
        "ORDERS_ORACLE_DSN",
    ),
    (
        "pairs",
        "left_options_json.ldap_dsn",
        "optional Oracle",
        "LDAP URL",
        "Oracle LDAP naming URL without credentials.",
        "ldap://host/service,cn=OracleContext,...",
    ),
    (
        "pairs",
        "left_options_json.ldap_dsn_env",
        "optional Oracle",
        "environment variable name",
        "Environment variable containing the Oracle LDAP naming URL.",
        "ORDERS_LDAP_DSN",
    ),
    (
        "pairs",
        "left_options_json.autocommit",
        "optional Hadoop",
        "boolean",
        "ODBC autocommit setting.",
        "TRUE",
    ),
    (
        "pairs",
        "left_options_json.nullstr",
        "optional CSV",
        "string or JSON array",
        "CSV values treated as null.",
        "NA",
    ),
    (
        "pairs",
        "left_options_json.date_format",
        "optional CSV",
        "date format",
        "CSV partition date parsing format.",
        "%Y%m%d",
    ),
    (
        "pairs",
        "left_options_json.options",
        "optional CSV",
        "JSON object",
        "Additional DuckDB read_csv options.",
        '{"delimiter":"|"}',
    ),
    (
        "pairs",
        "left_options_json.sample_hash",
        "optional materialized sample",
        "JSON object",
        "Materialized deterministic sample-hash metadata.",
        '{"version":1,"column":"SAMPLE_HASH"}',
    ),
    *(
        (
            "pairs",
            f"left_options_json.{name}",
            "optional Oracle",
            "non-negative integer"
            if name in {"prefetchrows", "fetch_retries"}
            else "positive integer",
            description,
            example,
        )
        for name, description, example in (
            ("arraysize", "Oracle cursor array size.", "1000"),
            ("prefetchrows", "Oracle cursor prefetch row count.", "1000"),
            ("fetchsize", "Rows written per Oracle fetch batch.", "10000"),
            ("fetch_retries", "Complete-vintage retries after transient Oracle errors.", "3"),
            ("max_sessions", "Maximum concurrent Oracle sessions for this source.", "4"),
        )
    ),
    (
        "pairs",
        "right_options_json.schema_hints",
        "optional Databricks files",
        "JSON object",
        "Explicit input schema hints for file reads.",
        '{"ORDER_ID":"BIGINT"}',
    ),
    (
        "pairs",
        "right_options_json.options",
        "optional Databricks files/CSV",
        "JSON object",
        "Additional source reader options.",
        '{"header":true}',
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
    for item in values:
        match = KEY_SPEC.match(item)
        if match is None or not (match.group(2) or "").strip():
            raise ValueError(
                f"pairs.xlsx row {row}: key_columns entries must be NAME[type], got {item!r}; "
                f"types: {', '.join(key_type_names())}"
            )
    parse_key_columns(values, f"pairs.xlsx row {row}")
    return list(dict.fromkeys(values))


def _options(value: Any, *, row: int, field: str, allowed: set[str]) -> dict[str, Any]:
    parsed = _json_object(value, row=row, field=field)
    unknown = sorted(set(parsed) - allowed)
    if unknown:
        raise ValueError(
            f"pairs.xlsx row {row}: {field} has unsupported fields: {', '.join(unknown)}"
        )
    return parsed


# >>> YYYY-MM-DD string or raise naming the field <<< #
def _iso_day(value: Any, *, row: int, field: str) -> date:
    try:
        parsed = date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.isoformat() != value:
        raise ValueError(f"pairs.xlsx row {row}: {field} must be YYYY-MM-DD")
    return parsed


def _validate_run_defaults(
    value: dict[str, Any], *, row: int, keys: list[str], partition: str
) -> None:
    data_types = value.get("data_type")
    if data_types is not None:
        if not isinstance(data_types, dict):
            raise TypeError(f"pairs.xlsx row {row}: data_type must be a JSON object")
        if not all(isinstance(name, str) and name.strip() for name in data_types):
            raise ValueError(f"pairs.xlsx row {row}: data_type column names must be non-empty")
        if not all(isinstance(kind, str) and kind.strip() for kind in data_types.values()):
            raise TypeError(
                f"pairs.xlsx row {row}: data_type values must be non-empty strings"
            )
    if value.get("run_mode") not in (None, "full", "left-only"):
        raise ValueError(f"pairs.xlsx row {row}: run_mode must be full or left-only")
    for field in RUN_DEFAULT_BOOLS:
        if field in value and not isinstance(value[field], bool):
            raise TypeError(f"pairs.xlsx row {row}: {field} must be true or false")
    for field in RUN_DEFAULT_WORKERS:
        count = value.get(field)
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or count <= 0
        ):
            raise ValueError(f"pairs.xlsx row {row}: {field} must be a positive integer")
    if value.get("vintage") is not None and value["vintage"] not in VINTAGES:
        raise ValueError(f"pairs.xlsx row {row}: vintage must be one of {', '.join(VINTAGES)}")
    bounds = {
        field: _iso_day(value[field], row=row, field=field)
        for field in ("from_date", "to_date")
        if value.get(field) is not None
    }
    if len(bounds) == 2 and bounds["from_date"] > bounds["to_date"]:
        raise ValueError(f"pairs.xlsx row {row}: from_date must not be after to_date")
    for field in RUN_DEFAULT_LISTS:
        excluded = value.get(field)
        if excluded is not None and (
            not isinstance(excluded, list)
            or not all(isinstance(item, str) and item.strip() for item in excluded)
        ):
            raise TypeError(f"pairs.xlsx row {row}: {field} must be a string array")
    if partition == "all":
        present = [field for field in RUN_DEFAULT_DATE_ONLY if value.get(field) not in (None, [])]
        if present:
            raise ValueError(f"pairs.xlsx row {row}: all mode cannot set {', '.join(present)}")
    if "checks" in value and (
        not isinstance(value["checks"], list)
        or not value["checks"]
        or not all(isinstance(item, str) and item.strip() for item in value["checks"])
    ):
        raise TypeError(f"pairs.xlsx row {row}: checks must be a non-empty string array")
    sampling = sample_plan(value.get("sample", False))
    if sampling["mode"] != "all" and not keys:
        raise ValueError(f"pairs.xlsx row {row}: sampled runs require key_columns")
    selector = value.get("use_modular")
    if selector is not None and (not isinstance(selector, str) or not selector.strip()):
        raise TypeError(f"pairs.xlsx row {row}: use_modular must be a non-empty column name")
    if selector is not None and sampling["mode"] != "fraction":
        raise ValueError(f"pairs.xlsx row {row}: use_modular requires percentage sampling")
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
    if partition == "all" and (size not in (None, "") or vintages not in (None, [])):
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
        partition = str(raw["partition"] or "").strip().casefold()
        if partition not in PARTITIONS:
            raise ValueError(
                f"pairs.xlsx row {number}: partition must be one of {', '.join(PARTITIONS)}"
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
            allowed=LEFT_OPTION_FIELDS | LEFT_CREDENTIAL_FIELDS,
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
        for field in RUN_DEFAULT_LISTS:
            if field not in defaults:
                continue
            raw_values = defaults[field]
            if isinstance(raw_values, str):
                raw_values = raw_values.split(",")
            if isinstance(raw_values, list | tuple) and all(
                isinstance(item, str) for item in raw_values
            ):
                defaults[field] = list(
                    dict.fromkeys(item.strip() for item in raw_values if item.strip())
                )
        _validate_run_defaults(defaults, row=number, keys=keys, partition=partition)
        if "snapshot" in left_options and not isinstance(left_options["snapshot"], bool):
            raise TypeError(f"pairs.xlsx row {number}: snapshot must be true or false")
        if "sas_sample_csv" in left_options and not (
            isinstance(left_options["sas_sample_csv"], str)
            and left_options["sas_sample_csv"].strip()
        ):
            raise TypeError(f"pairs.xlsx row {number}: sas_sample_csv must be a path string")
        result.append(
            {
                "pair_name": pair,
                "enabled": _boolean(raw["enabled"], row=number),
                "key_columns": keys,
                "vintage": vintage,
                "partition": partition,
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
    left_options = _json_object(row["left_options_json"], row=number, field="left_options_json")
    mode = "all" if row["partition"] == "all" else "date"
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


def _normalized_json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value.strip()
    if isinstance(value, dict):
        return {key: _normalized_json(item) for key, item in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_normalized_json(item) for item in value]
    return value


def _normalized_enabled(value: Any) -> bool | str:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float) and value in {0, 1}:
        return bool(value)
    text = str(value or "").strip().casefold()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return text


def _normalized_keys(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = value.split(",")
    else:
        parsed = value
    if not isinstance(parsed, list | tuple):
        return [str(parsed).strip()]
    return [str(item).strip() for item in parsed]


def pair_configuration_signature(row: dict[str, Any]) -> dict[str, Any]:
    meta = {}
    for field in PAIR_WORKBOOK_COLUMNS:
        value = row.get(field)
        if field == "enabled":
            meta[field] = _normalized_enabled(value)
        elif field == "key_columns":
            meta[field] = _normalized_keys(value)
        elif field.endswith("_json"):
            meta[field] = _normalized_json(value)
        else:
            meta[field] = None if value in (None, "") else str(value).strip()
    effective = row.get("config_json")
    if "config_json" not in row:
        effective = build_pair_config(row)
        effective.pop("_run_defaults", None)
    meta["config_json"] = _normalized_json(effective)
    pair_sql = row.get("pair_sql") or {}
    sql = {
        field: (
            _normalized_json(pair_sql.get(field))
            if field.endswith("_json")
            else (None if pair_sql.get(field) in (None, "") else str(pair_sql[field]).strip())
        )
        for field in PAIR_SQL_WORKBOOK_COLUMNS
        if field != "pair_name"
    }
    return {"meta": meta, "sql": sql}


def changed_pair_configurations(
    workbook_rows: list[dict[str, Any]], remote_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    remote = {str(row.get("pair_name")): row for row in remote_rows}
    return [
        row
        for row in workbook_rows
        if row["pair_name"] not in remote
        or pair_configuration_signature(row)
        != pair_configuration_signature(remote[row["pair_name"]])
    ]


def upsert_pair_configuration(sql, pair_table: str, rows: list[dict[str, Any]]) -> dict:
    pair_table = validate_table_name(pair_table)
    pair_sql_table = companion_table(pair_table, PAIR_SQL_TABLE)
    meta_statement = f"""MERGE INTO {pair_table} AS t
USING (SELECT :pair_name AS pair_name) AS s ON t.pair_name = s.pair_name
WHEN MATCHED THEN UPDATE SET
  t.enabled = CAST(:enabled AS BOOLEAN),
  t.key_columns = from_json(:key_columns, 'ARRAY<STRING>'),
  t.vintage = :vintage,
  t.partition = :partition,
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
  pair_name, enabled, key_columns, vintage, partition,
  left_source, left_conn, left_table, left_date_col, left_options_json,
  right_source, right_conn, right_table, right_date_col, right_options_json,
  run_defaults_json, config_json, latest_run_id, latest_status, updated_at
) VALUES (
  :pair_name, CAST(:enabled AS BOOLEAN), from_json(:key_columns, 'ARRAY<STRING>'), :vintage,
  :partition,
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
