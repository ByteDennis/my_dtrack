from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import find_dotenv, load_dotenv

from ..common.column_types import resolve_column_type
from ..common.dates import canonical_date, resolve_date_type
from ..common.manifest import statistics_plan
from ..common.query_windows import normalize_query_windows

LEFT_SOURCES = {"oracle", "athena", "aws", "hadoop", "csv", "file", "duckdb"}
COL_TYPES = {"numeric", "categorical"}
RIGHT_KINDS = {"table": ("table",), "query": ("query",), "files": ("path", "format")}


# >>> expand conn_macro from named profiles (pair fields win); other macros resolve by name later <<< #
def resolve_connections(config: dict[str, Any]) -> dict[str, Any]:
    profiles = config.get("connections") or {}
    if not isinstance(profiles, dict):
        raise TypeError("connections must be an object of named profiles")
    for pair in (config.get("pairs") or {}).values():
        for side in ("left", "right"):
            spec = pair.get(side)
            macro = (spec or {}).get("conn_macro")
            if not macro or not isinstance(spec, dict):
                continue
            if macro in profiles:
                pair[side] = {**profiles[macro], **spec}
    return config


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    version = config.get("version", 1)
    if isinstance(version, bool) or not isinstance(version, int) or version not in {1, 2}:
        raise ValueError("config version must be 1 or 2")
    config["version"] = version
    pairs = config.get("pairs")
    if not isinstance(pairs, dict) or not pairs:
        raise ValueError("config requires a non-empty pairs object")
    if config.get("timezone"):
        try:
            ZoneInfo(str(config["timezone"]))
        except Exception as exc:
            raise ValueError(f"unsupported timezone {config['timezone']!r}") from exc
    resolve_connections(config)
    for name, pair in pairs.items():
        if pair.get("skip"):
            continue
        pair["version"] = version
        left, right = pair.get("left") or {}, pair.get("right") or {}
        left_source = str(left.get("source") or "").lower()
        right_source = str(right.get("source") or "").lower()
        if left_source not in LEFT_SOURCES:
            raise ValueError(f"{name}: unsupported Left source {left_source!r}")
        if left_source == "duckdb" and left.get("snapshot"):
            raise ValueError(
                f"{name}: Left duckdb snapshot is no longer supported; "
                "have the writer CHECKPOINT and close before DTrack reads the stable file"
            )
        if "nullstr" in left:
            nullstr = left["nullstr"]
            if left_source not in {"file", "csv"}:
                raise ValueError(f"{name}: Left nullstr requires source file/csv")
            if not isinstance(nullstr, str | list) or (
                isinstance(nullstr, list) and not all(isinstance(value, str) for value in nullstr)
            ):
                raise TypeError(f"{name}: Left nullstr must be a string or a list of strings")
            configured = (left.get("options") or {}).get("nullstr", nullstr)
            if configured != nullstr:
                raise ValueError(f"{name}: Left nullstr disagrees with options.nullstr")
        if right_source != "databricks":
            raise ValueError(f"{name}: Right side must be databricks")
        kind = {"volume_raw": "files"}.get(right.get("kind"), right.get("kind") or "table")
        if kind not in RIGHT_KINDS:
            raise ValueError(f"{name}: unsupported Right kind {kind!r}")
        needed = [field for field in RIGHT_KINDS[kind] if not right.get(field)]
        if needed:
            raise ValueError(f"{name}: Right kind {kind!r} requires {', '.join(needed)}")
        if "{src}" in str(right.get("query") or "") and kind != "files":
            raise ValueError(f"{name}: {{src}} in a Right query needs kind 'files'")
        raw_partition_mode = pair.get("partition_mode", "date")
        if not isinstance(raw_partition_mode, str):
            raise TypeError(f"{name}: partition_mode must be date or all")
        partition_mode = raw_partition_mode.strip().lower()
        if partition_mode not in {"date", "all"}:
            raise ValueError(f"{name}: partition_mode must be date or all")
        if partition_mode == "date" and (not left.get("date_col") or not right.get("date_col")):
            raise ValueError(f"{name}: both sides require date_col")
        if partition_mode == "all":
            pair["partition_mode"] = "all"
            date_only = [
                field
                for field in (
                    "fromDate",
                    "toDate",
                    "vintage",
                    "date_synonyms",
                    "excluded_partitions",
                )
                if pair.get(field)
            ]
            if date_only:
                raise ValueError(f"{name}: partition_mode all cannot use {', '.join(date_only)}")
        else:
            pair.pop("partition_mode", None)
        try:
            left["query_windows"] = normalize_query_windows(
                left.get("query"), left.get("query_windows"), source=left_source
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name}: invalid Left query_windows: {exc}") from None
        if partition_mode == "all" and left["query_windows"]:
            raise ValueError(f"{name}: partition_mode all cannot use Left query_windows")
        if not left["query_windows"]:
            left.pop("query_windows", None)
        for side_name, side in (("Left", left), ("Right", right)):
            if partition_mode == "date" or "date_type" in side:
                try:
                    resolve_date_type(side.get("date_type"))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{name}: invalid {side_name} date_type: {exc}") from None
        _expand_inline_col_map(pair)
        if not isinstance(pair.get("col_map"), dict) or not pair["col_map"]:
            raise ValueError(f"{name}: col_map must be non-empty")
        try:
            statistics = statistics_plan(pair.get("statistics"))
        except (TypeError, ValueError) as exc:
            raise type(exc)(f"{name}: {exc}") from None
        if statistics is None:
            pair.pop("statistics", None)
        else:
            pair["statistics"] = statistics
        excluded_partitions = pair.get("excluded_partitions") or []
        excluded_columns = pair.get("excluded_columns") or []
        if version == 1 and (excluded_partitions or excluded_columns):
            raise ValueError(f"{name}: exclusions require config version 2")
        if not isinstance(excluded_partitions, list) or not all(
            isinstance(value, str) for value in excluded_partitions
        ):
            raise TypeError(f"{name}: excluded_partitions must be a list of dates")
        if not isinstance(excluded_columns, list) or not all(
            isinstance(value, str) for value in excluded_columns
        ):
            raise TypeError(f"{name}: excluded_columns must be a list of Left column names")
        if partition_mode == "date":
            try:
                pair["excluded_partitions"] = sorted(
                    {canonical_date(value) for value in excluded_partitions}
                )
            except ValueError as exc:
                raise ValueError(f"{name}: invalid excluded_partitions: {exc}") from None
        else:
            pair["excluded_partitions"] = []
        unknown_excluded = sorted(set(excluded_columns) - set(pair["col_map"]))
        if unknown_excluded:
            raise ValueError(
                f"{name}: excluded_columns names columns outside col_map: "
                f"{', '.join(unknown_excluded)}"
            )
        date_col = str(left.get("date_col") or "").casefold()
        if partition_mode == "date" and any(
            str(column).casefold() == date_col for column in excluded_columns
        ):
            raise ValueError(f"{name}: excluded_columns cannot include the Left date_col")
        pair["excluded_columns"] = list(dict.fromkeys(excluded_columns))
        overrides = pair.get("col_type_overrides") or {}
        if set(overrides) - set(pair["col_map"]) or set(overrides.values()) - COL_TYPES:
            raise ValueError(f"{name}: col_type_overrides maps col_map keys to numeric|categorical")
        column_types = pair.get("column_types") or {}
        if set(column_types) - set(pair["col_map"]):
            raise ValueError(f"{name}: column_types names columns outside col_map")
        conflict = sorted(set(column_types) & set(overrides))
        if conflict:
            raise ValueError(
                f"{name}: columns cannot use both col_type_overrides and column_types: "
                f"{', '.join(conflict)}"
            )
        try:
            pair["column_types"] = {
                column: resolve_column_type(spec).spec() for column, spec in column_types.items()
            }
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{name}: type must be numeric|categorical or a registered column type: {exc}"
            ) from None
    return config


def _expand_inline_col_map(pair: dict[str, Any]) -> None:
    mapping = pair.get("col_map")
    if not isinstance(mapping, dict):
        return
    normalized, types = {}, dict(pair.get("column_types") or {})
    for left, value in mapping.items():
        if isinstance(value, str):
            normalized[left] = value
            continue
        if not isinstance(value, dict) or not isinstance(value.get("right"), str):
            raise ValueError(  # noqa: TRY004 - malformed config values are validation errors
                "col_map values must be right names or {right, type} objects"
            )
        unknown = set(value) - {"right", "type"}
        if unknown:
            raise ValueError(f"unsupported col_map fields: {', '.join(sorted(unknown))}")
        normalized[left] = value["right"]
        if value.get("type"):
            types[left] = value["type"]
    pair["col_map"] = normalized
    if types:
        pair["column_types"] = types


# >>> a col_map file as {left: right|{right,type}} or CSV left,right[,type] <<< #
def read_col_map(path: Path) -> tuple[dict[str, str], dict[str, str], dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"col_map file not found: {path}")
    if path.suffix.lower() == ".json":
        mapping = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(mapping, dict):
            raise ValueError(f"{path}: col_map JSON must be an object of left: right values")
        rows = []
        for key, value in mapping.items():
            if isinstance(value, str):
                rows.append({"left": key, "right": value})
            elif isinstance(value, dict):
                rows.append({"left": key, **value})
            else:
                raise ValueError(  # noqa: TRY004 - malformed config values are validation errors
                    f"{path}: col_map JSON must be an object of left: right names or objects"
                )
    elif path.suffix.lower() == ".csv":
        lines = path.read_text(encoding="utf-8-sig").splitlines()
        kept = [line for line in lines if line.strip() and not line.lstrip().startswith("#")]
        reader = csv.DictReader(kept)
        if not {"left", "right"} <= {name.strip() for name in reader.fieldnames or []}:
            raise ValueError(f"{path}: col_map CSV needs a left,right[,type] header")
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        rows = list(reader)
    else:
        raise ValueError(f"{path}: col_map file must be .csv or .json")
    mapping, overrides, column_types = {}, {}, {}
    for row in rows:
        left, right = (row.get("left") or "").strip(), (row.get("right") or "").strip()
        if not left or not right:
            raise ValueError(f"{path}: every col_map row needs left and right, got {row}")
        if left in mapping or right in mapping.values():
            raise ValueError(f"{path}: col_map maps {left!r} or {right!r} twice")
        mapping[left] = right
        if (row.get("type") or "").strip():
            kind = row["type"].strip().lower()
            if kind in COL_TYPES:
                overrides[left] = kind
            else:
                column_types[left] = kind
    return mapping, overrides, column_types


# >>> replace a col_map path (relative to the config file) with its mapping and merge its types <<< #
def expand_col_maps(config: dict[str, Any], base: Path) -> dict[str, Any]:
    for name, pair in (config.get("pairs") or {}).items():
        if not isinstance(pair.get("col_map"), str):
            continue
        mapping, types, column_types = read_col_map(base / pair["col_map"])
        overrides = dict(pair.get("col_type_overrides") or {})
        clash = {col for col, kind in types.items() if overrides.get(col, kind) != kind}
        if clash:
            raise ValueError(
                f"{name}: col_map type and col_type_overrides disagree on {sorted(clash)}"
            )
        pair["col_map"] = mapping
        if types or overrides:
            pair["col_type_overrides"] = {**types, **overrides}
        if column_types:
            pair["column_types"] = {**column_types, **(pair.get("column_types") or {})}
    return config


def expand_query_files(config: dict[str, Any], base: Path) -> dict[str, Any]:
    for name, pair in (config.get("pairs") or {}).items():
        for side_name in ("left", "right"):
            spec = pair.get(side_name)
            if not isinstance(spec, dict):
                continue
            query = spec.get("query")
            if not isinstance(query, str) or "\n" in query or not query.lower().endswith(".sql"):
                continue
            path = base / query
            if not path.is_file():
                raise FileNotFoundError(f"{name}: {side_name} query file not found: {path}")
            text = path.read_text(encoding="utf-8").strip()
            while text.endswith(";"):
                text = text[:-1].rstrip()
            if not text:
                raise ValueError(f"{name}: {side_name} query file is empty: {path}")
            spec["query"] = text
    return config


def load_config(path: str | Path) -> dict[str, Any]:
    load_dotenv(find_dotenv(usecwd=True))
    path = Path(path)
    with path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    base = path.resolve().parent
    resolve_connections(config)
    expand_query_files(config, base)
    expand_col_maps(config, base)
    return validate_config(config)
