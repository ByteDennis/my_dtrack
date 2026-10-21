from __future__ import annotations

import json
from copy import deepcopy
from typing import Any


def _object(value: Any, field: str, pair: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if isinstance(value, dict):
        return deepcopy(value)
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{pair}: {field} must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise TypeError(f"{pair}: {field} must be a JSON object")
    return parsed


def _strings(value: Any, field: str, pair: str) -> list[str]:
    if value in (None, ""):
        return []
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{pair}: {field} must be a string array") from exc
        else:
            parsed = text.split(",")
    if not isinstance(parsed, list | tuple) or not all(
        isinstance(item, str) and item.strip() for item in parsed
    ):
        raise TypeError(f"{pair}: {field} must be a string array")
    return list(dict.fromkeys(item.strip() for item in parsed))


def _relation_parts(value: str) -> dict[str, str]:
    parts = [part.strip() for part in value.split(".") if part.strip()]
    if len(parts) == 1:
        return {"table": parts[0]}
    if len(parts) == 2:
        return {"schema": parts[0], "table": parts[1]}
    if len(parts) == 3:
        return {"catalog": parts[0], "schema": parts[1], "table": parts[2]}
    raise ValueError(f"invalid table reference: {value!r}")


def _source_side(row: dict[str, Any], side: str, pair: str) -> tuple[dict[str, Any], dict]:
    source = str(row.get(f"{side}_source") or "").strip().casefold()
    conn = str(row.get(f"{side}_conn") or "").strip()
    table = str(row.get(f"{side}_table") or "").strip()
    date_col = str(row.get(f"{side}_date_col") or "").strip()
    options = _object(row.get(f"{side}_options_json"), f"{side}_options_json", pair)
    pair_options = {}
    if side == "left":
        if source == "oracle":
            config = {"source": "oracle", "conn_macro": conn}
            if table:
                config.update(_relation_parts(table))
        elif source == "hadoop":
            config = {"source": "hadoop", "schema": conn}
            if table:
                config["table"] = table
        elif source == "csv":
            config = {"source": "csv", "path": conn}
        else:
            raise ValueError(f"{pair}: unsupported Left source {source!r}")
        if "snapshot" in options:
            pair_options["snapshot"] = options.pop("snapshot")
    else:
        if source == "databricks_table":
            config = {"source": "databricks", "kind": "table", **_relation_parts(table)}
            ref = [part.strip() for part in conn.split(".") if part.strip()]
            if len(ref) != 2:
                raise ValueError(f"{pair}: right_conn must be catalog.schema")
            config["catalog"], config["schema"] = ref
        elif source == "databricks_query":
            config = {"source": "databricks", "kind": "query"}
        elif source == "databricks_files":
            config = {"source": "databricks", "kind": "files", "path": conn}
            options.setdefault("format", "parquet")
        elif source == "csv":
            config = {"source": "csv", "path": conn}
        else:
            raise ValueError(f"{pair}: unsupported Right source {source!r}")
    if date_col:
        config["date_col"] = date_col
    reserved = set(config) & set(options)
    if reserved:
        raise ValueError(
            f"{pair}: {side}_options_json cannot replace {', '.join(sorted(reserved))}"
        )
    config.update(options)
    return config, pair_options


def _explicit_config(row: dict[str, Any], pair: str) -> dict[str, Any]:
    left, pair_options = _source_side(row, "left", pair)
    right, right_pair_options = _source_side(row, "right", pair)
    value = _object(row.get("run_defaults_json"), "run_defaults_json", pair)
    value.update(pair_options)
    value.update(right_pair_options)
    value["left"] = left
    value["right"] = right
    keys = _strings(row.get("key_columns"), "key_columns", pair)
    if keys:
        value["key"] = ",".join(keys)
    vintage = str(row.get("vintage") or "").strip().casefold()
    if vintage:
        value["vintage"] = vintage
    value["_run_defaults"] = _object(row.get("run_defaults_json"), "run_defaults_json", pair)
    return value


def _apply_mappings(value: dict[str, Any], row: dict[str, Any], pair: str) -> None:
    mappings = row.get("column_map_rows") or []
    if not mappings:
        return
    col_map = {}
    overrides = {}
    column_types = {}
    for mapping in mappings:
        left = str(mapping.get("left_column") or "").strip()
        right = str(mapping.get("right_column") or "").strip()
        if not left or not right:
            raise ValueError(f"{pair}: column mapping names must be non-empty")
        col_map[left] = right
        data_type = str(mapping.get("data_type") or "").strip()
        if not data_type:
            continue
        if data_type in {"numeric", "categorical"}:
            overrides[left] = data_type
        elif data_type.startswith("{"):
            try:
                column_types[left] = json.loads(data_type)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{pair}.{left}: invalid custom data_type JSON") from exc
        else:
            column_types[left] = data_type
    value["col_map"] = col_map
    value["col_type_overrides"] = overrides
    value["column_types"] = column_types


def _apply_sql(value: dict[str, Any], row: dict[str, Any], pair: str, *, legacy: bool) -> None:
    query_row = row.get("pair_sql")
    if query_row is None:
        return
    for side, sql_field, windows_field in (
        ("left", "left_sql", "left_query_windows_json"),
        ("right", "right_sql", "right_query_windows_json"),
    ):
        target = value.setdefault(side, {})
        sql = str(query_row.get(sql_field) or "").strip().rstrip(";").rstrip()
        if sql:
            target["query"] = sql
            windows = _object(query_row.get(windows_field), windows_field, pair)
            if windows:
                target["query_windows"] = windows
            else:
                target.pop("query_windows", None)
        else:
            target.pop("query", None)
            target.pop("query_windows", None)
    if not legacy:
        return
    sample_size = query_row.get("vintage_sample_size")
    if sample_size not in (None, ""):
        try:
            value["vintage_sample_size"] = int(sample_size)
        except (TypeError, ValueError):
            raise ValueError(f"{pair}: vintage_sample_size must be an integer") from None
    vintage_values = _strings(query_row.get("vintage_values"), "vintage_values", pair)
    if vintage_values:
        value["vintage_values"] = vintage_values


def build_pair_config(row: dict[str, Any]) -> dict[str, Any]:
    pair = str(row.get("pair_name") or "pair")
    explicit = bool(str(row.get("left_source") or "").strip())
    if explicit:
        value = _explicit_config(row, pair)
    else:
        value = row.get("config_json")
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, dict):
            raise TypeError("pair metadata requires explicit fields or config_json")
        value = deepcopy(value)
        value["_run_defaults"] = {}
    _apply_mappings(value, row, pair)
    _apply_sql(value, row, pair, legacy=not explicit)
    return value
