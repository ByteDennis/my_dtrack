from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any

from pair_recon_left.common.model.hashing import FAMILIES

KEY_TYPE_HINTS = ("numeric", "categorical")
KEY_SPEC = re.compile(r"^([^\[\]]+?)\s*(?:\[\s*([^\[\]]*?)\s*\])?$")


# >>> types allowed in NAME[type]: the statistics hints plus every registered normalizer <<< #
def key_type_names() -> list[str]:
    return sorted({*KEY_TYPE_HINTS, *FAMILIES})


# >>> "NAME[type]" key entries -> unique names in order and {name: type}; a bare NAME carries no type <<< #
def parse_key_columns(values: Any, pair: str = "pair") -> tuple[list[str], dict[str, str]]:
    names: list[str] = []
    types: dict[str, str] = {}
    for item in values or []:
        match = KEY_SPEC.match(str(item).strip())
        if match is None or not match.group(1).strip():
            raise ValueError(f"{pair}: key column {item!r} must be NAME or NAME[type]")
        name, kind = match.group(1).strip(), (match.group(2) or "").strip()
        if kind and kind not in key_type_names():
            raise ValueError(
                f"{pair}: key column {name!r} type {kind!r} must be one of "
                f"{', '.join(key_type_names())}"
            )
        if name not in names:
            names.append(name)
        if kind:
            types[name] = kind
    return names, types


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
    value.pop("partition_mode", None)
    value.pop("partition_grain", None)
    run_defaults = deepcopy(value)
    partition = str(row.get("partition") or "").strip().casefold()
    if partition not in {"day", "month", "all"}:
        raise ValueError(f"{pair}: partition must be day, month or all")
    value["partition"] = partition
    value.update(pair_options)
    value.update(right_pair_options)
    value["left"] = left
    value["right"] = right
    keys, key_types = parse_key_columns(_strings(row.get("key_columns"), "key_columns", pair), pair)
    if keys:
        value["key"] = ",".join(keys)
    value["_key_types"] = key_types
    vintage = str(row.get("vintage") or "").strip().casefold()
    if vintage and partition != "all":
        value["vintage"] = vintage
    value["_run_defaults"] = run_defaults
    value["_run_defaults"]["partition"] = partition
    return value


# >>> one data_type value lands in either the numeric|categorical hints or the normalizer types <<< #
def _assign_type(spec: str, left: str, overrides: dict, column_types: dict, pair: str) -> None:
    if spec in KEY_TYPE_HINTS:
        overrides[left] = spec
        column_types.pop(left, None)
        return
    if spec[:1] in "{[":
        try:
            column_types[left] = json.loads(spec)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{pair}.{left}: invalid custom data_type JSON") from exc
    else:
        column_types[left] = spec
    overrides.pop(left, None)


def _apply_mappings(value: dict[str, Any], row: dict[str, Any], pair: str) -> None:
    mappings = row.get("column_map_rows") or []
    key_types = value.pop("_key_types", None) or {}
    if not mappings and not key_types:
        return
    if mappings:
        col_map: dict[str, str] = {}
        overrides: dict[str, str] = {}
        column_types: dict[str, Any] = {}
    else:
        col_map = dict(value.get("col_map") or {})
        overrides = dict(value.get("col_type_overrides") or {})
        column_types = dict(value.get("column_types") or {})
    for mapping in mappings:
        left = str(mapping.get("left_column") or "").strip()
        right = str(mapping.get("right_column") or "").strip()
        if not left or not right:
            raise ValueError(f"{pair}: column mapping names must be non-empty")
        col_map[left] = right
        data_type = str(mapping.get("data_type") or "").strip()
        if data_type:
            _assign_type(data_type, left, overrides, column_types, pair)
    left_date = str((value.get("left") or {}).get("date_col") or "").casefold()
    right_date = str((value.get("right") or {}).get("date_col") or "")
    for name, kind in key_types.items():
        _assign_type(kind, name, overrides, column_types, pair)
        if name not in col_map:
            col_map[name] = right_date if right_date and name.casefold() == left_date else name
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
    value.pop("_key_types", None)
    _apply_sql(value, row, pair, legacy=not explicit)
    return value
