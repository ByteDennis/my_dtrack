from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

from ..common.model.manifest import sample_plan, statistics_plan
from ..common.sql.column_types import resolve_column_type
from ..common.sql.dates import canonical_date, resolve_date_type
from ..common.sql.query_windows import normalize_query_windows

LEFT_SOURCES = {"oracle", "hadoop", "csv"}
RIGHT_SOURCES = {"databricks", "csv"}
COL_TYPES = {"numeric", "categorical"}
RIGHT_KINDS = {"table": ("table",), "query": ("query",), "files": ("path", "format")}


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
        if left_source == "csv":
            if not left.get("path"):
                raise ValueError(f"{name}: CSV Left source requires path")
            if "nullstr" in left:
                nullstr = left["nullstr"]
                if not isinstance(nullstr, str | list) or (
                    isinstance(nullstr, list)
                    and not all(isinstance(value, str) for value in nullstr)
                ):
                    raise TypeError(f"{name}: CSV nullstr must be a string or list")
        if (
            left_source == "hadoop"
            and not left.get("query")
            and not (left.get("schema") and left.get("table"))
        ):
            raise ValueError(f"{name}: Hadoop Left source requires query or schema and table")
        pre_sampled = pair.get("pre_sampled", False)
        if not isinstance(pre_sampled, bool):
            raise TypeError(f"{name}: pre_sampled must be true or false")
        if pre_sampled and left_source != "csv":
            raise ValueError(f"{name}: pre_sampled requires a CSV Left source")
        right_pre_sampled = pair.get("right_pre_sampled", False)
        if not isinstance(right_pre_sampled, bool):
            raise TypeError(f"{name}: right_pre_sampled must be true or false")
        if right_pre_sampled and right_source != "csv":
            raise ValueError(f"{name}: right_pre_sampled requires a CSV Right source")
        if pre_sampled or right_pre_sampled:
            sampling = sample_plan(pair.get("sample"))
            if sampling["mode"] == "fixed":
                raise ValueError(f"{name}: pre-sampled CSV requires all or fraction sampling")
            keys = [part.strip() for part in str(pair.get("key") or "").split(",") if part.strip()]
            if sampling["mode"] != "all" and not keys:
                raise ValueError(f"{name}: pre-sampled CSV fraction requires a key")
        if right_source not in RIGHT_SOURCES:
            raise ValueError(f"{name}: unsupported Right source {right_source!r}")
        if right_source == "csv":
            if not right.get("path"):
                raise ValueError(f"{name}: CSV Right source requires path")
        else:
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
        snapshot = pair.get("snapshot", False)
        if not isinstance(snapshot, bool):
            raise TypeError(f"{name}: snapshot must be true or false")
        if snapshot and partition_mode == "all":
            raise ValueError(f"{name}: snapshot requires partition_mode date")
        if partition_mode == "date" and (
            not right.get("date_col") or (not snapshot and not left.get("date_col"))
        ):
            raise ValueError(f"{name}: date mode requires Right date_col and Left date_col")
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
            if pair.get("vintage_values") or pair.get("vintage_sample_size") not in (None, ""):
                raise ValueError(f"{name}: partition_mode all cannot select vintages")
        else:
            pair.pop("partition_mode", None)
        vintage_values = pair.get("vintage_values") or []
        vintage_sample_size = pair.get("vintage_sample_size")
        if not isinstance(vintage_values, list) or not all(
            isinstance(value, str) and value.strip() for value in vintage_values
        ):
            raise TypeError(f"{name}: vintage_values must be a list of dates")
        pair["vintage_values"] = list(dict.fromkeys(value.strip() for value in vintage_values))
        if vintage_sample_size not in (None, ""):
            if isinstance(vintage_sample_size, bool):
                raise TypeError(f"{name}: vintage_sample_size must be a positive integer")
            try:
                parsed_sample_size = int(vintage_sample_size)
            except (TypeError, ValueError):
                raise TypeError(f"{name}: vintage_sample_size must be a positive integer") from None
            if isinstance(vintage_sample_size, float) and not vintage_sample_size.is_integer():
                raise TypeError(f"{name}: vintage_sample_size must be a positive integer")
            if isinstance(vintage_sample_size, str) and not vintage_sample_size.strip().isdigit():
                raise TypeError(f"{name}: vintage_sample_size must be a positive integer")
            if parsed_sample_size <= 0:
                raise ValueError(f"{name}: vintage_sample_size must be a positive integer")
            pair["vintage_sample_size"] = parsed_sample_size
        else:
            pair.pop("vintage_sample_size", None)
        if pair["vintage_values"] and "vintage_sample_size" in pair:
            raise ValueError(f"{name}: use vintage_values or vintage_sample_size, not both")
        if not pair["vintage_values"]:
            pair.pop("vintage_values", None)
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
        try:
            right["query_windows"] = normalize_query_windows(
                right.get("query"), right.get("query_windows"), source=right_source
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name}: invalid Right query_windows: {exc}") from None
        if partition_mode == "all" and right["query_windows"]:
            raise ValueError(f"{name}: partition_mode all cannot use Right query_windows")
        if not right["query_windows"]:
            right.pop("query_windows", None)
        for side_name, side in (("Left", left), ("Right", right)):
            if (partition_mode == "date" and (side_name == "Right" or not snapshot)) or (
                "date_type" in side
            ):
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
            raise TypeError("col_map values must be right names or {right, type} objects")
        unknown = set(value) - {"right", "type"}
        if unknown:
            raise ValueError(f"unsupported col_map fields: {', '.join(sorted(unknown))}")
        normalized[left] = value["right"]
        if value.get("type"):
            types[left] = value["type"]
    pair["col_map"] = normalized
    if types:
        pair["column_types"] = types
