from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import defaultdict
from typing import Any

from .. import PROTOCOL_VERSION
from ..sql.column_types import resolve_column_type
from ..sql.dates import bucket_date, resolve_date_type
from .attribute_checks import attribute_check_names
from .hashing import (
    HASH_BUCKET_SPACE,
    RUN_DATE_VALUE,
    canonical_normalization_rules,
    key_family,
)

STATS_VERSION = 1
DEFAULT_STATISTICS = attribute_check_names()


def normalization_date(run_id: str, rules: list[dict[str, str]]) -> str | None:
    if not any(rule.get("canonical_value") == RUN_DATE_VALUE for rule in rules):
        return None
    match = re.search(r"(?:^|-)([0-9]{4})([0-9]{2})([0-9]{2})$", run_id)
    if match is None:
        raise ValueError("@run_date normalization requires a run_id ending in YYYYMMDD")
    return "-".join(match.groups())


def statistics_plan(raw: Any) -> list[str] | None:
    if raw is None:
        return None
    if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
        raise TypeError("statistics must be a non-empty list")
    if len(raw) != len(set(raw)):
        raise ValueError("statistics must not contain duplicates")
    unknown = sorted(set(raw) - set(DEFAULT_STATISTICS))
    if unknown:
        raise ValueError(f"unsupported statistics: {', '.join(unknown)}")
    return [name for name in DEFAULT_STATISTICS if name in raw]


def mapping_csv_bytes(pair: dict[str, Any]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=["left_column", "right_column", "usage", "data_type"],
        lineterminator="\n",
    )
    writer.writeheader()
    excluded = {str(column).casefold() for column in pair.get("excluded_columns") or []}
    overrides = pair.get("col_type_overrides") or {}
    column_types = pair.get("column_types") or {}
    partition = str(pair.get("partition") or "day")
    date_col = str((pair.get("left") or {}).get("date_col") or "").casefold()
    for left_column, right_column in (pair.get("col_map") or {}).items():
        if partition != "all" and str(left_column).casefold() == date_col:
            usage = "partition"
        elif str(left_column).casefold() in excluded:
            usage = "excluded"
        else:
            usage = "compare"
        data_type = overrides.get(left_column)
        if data_type is None and left_column in column_types:
            spec = resolve_column_type(column_types[left_column]).spec()
            data_type = (
                spec
                if isinstance(spec, str)
                else json.dumps(spec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            )
        writer.writerow(
            {
                "left_column": left_column,
                "right_column": right_column,
                "usage": usage,
                "data_type": data_type or "",
            }
        )
    return output.getvalue().encode("utf-8")


def _slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    return text[:48] or "empty"


def unit_id(pair: str, window: str, column: str) -> str:
    raw = json.dumps([pair, window, column], ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"{_slug(pair)}--{_slug(window)}--{_slug(column)}--{digest}"


def _digest(value: Any, length: int = 16) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:length]


def window_id(manifest: dict[str, Any], window: dict[str, Any]) -> str:
    if int(manifest.get("version") or 1) == 1:
        return str(window["bucket"])
    return _digest(
        {
            "stats_version": STATS_VERSION,
            "run_id": manifest["run_id"],
            "pair": manifest["pair"],
            "bucket": window["bucket"],
            "partitions": window["partitions"],
            "sample": manifest["sample"],
            "key_cols": manifest.get("key_cols") or [],
            "right_key_cols": manifest.get("right_key_cols") or [],
            "key_families": manifest.get("key_families") or [],
            "left": manifest.get("left") or {},
        }
    )


def unit_id_for(manifest: dict[str, Any], window: dict[str, Any], column: str) -> str:
    if int(manifest.get("version") or 1) == 1:
        return unit_id(manifest["pair"], window["bucket"], column)
    identity = {
        "window_id": window_id(manifest, window),
        "column": column,
        "right_column": manifest["col_map"][column],
        "col_type_override": (manifest.get("col_type_overrides") or {}).get(column),
        "column_type": (manifest.get("column_types") or {}).get(column),
        "right": manifest.get("right") or {},
    }
    if "statistics" in manifest:
        identity["statistics"] = manifest["statistics"]
    digest = _digest(identity, 12)
    return f"{_slug(manifest['pair'])}--{_slug(window['bucket'])}--{_slug(column)}--{digest}"


def planned_unit_ids(manifest: dict[str, Any]) -> set[str]:
    return {
        unit_id_for(manifest, window, column)
        for window in manifest.get("windows") or []
        for column in manifest.get("columns") or []
    }


def sample_plan(raw: Any) -> dict[str, Any]:
    if raw is None or raw == "" or raw is False:
        return {"mode": "all"}
    if isinstance(raw, str):
        raw = float(raw) if any(char in raw for char in ".eE") else int(raw)
    if isinstance(raw, bool):
        raise TypeError("sample must be a fraction or fixed positive count")
    if isinstance(raw, float):
        if not 0 < raw <= 1:
            raise ValueError("fraction sample must be in (0, 1]")
        return {
            "mode": "fraction",
            "rate": raw,
            "threshold": max(1, round(raw * HASH_BUCKET_SPACE)),
        }
    count = int(raw)
    if count <= 0:
        raise ValueError("fixed sample must be positive")
    return {"mode": "fixed", "count": count}


def select_vintage_windows(
    run_id: str,
    pair_name: str,
    pair: dict[str, Any],
    windows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_values = pair.get("vintage_values") or []
    raw_size = pair.get("vintage_sample_size")
    checked = {str(value) for value in pair.get("checked_vintages") or []}
    if raw_values and raw_size not in (None, ""):
        raise ValueError(f"{pair_name}: use vintage_values or vintage_sample_size, not both")
    available = {window["bucket"]: window for window in windows}
    if raw_values:
        if not isinstance(raw_values, list) or not all(
            isinstance(value, str) and value.strip() for value in raw_values
        ):
            raise TypeError(f"{pair_name}: vintage_values must be a non-empty list of dates")
        vintage = str(pair.get("vintage") or "day")
        selected_values = list(
            dict.fromkeys(bucket_date(value.strip(), vintage) for value in raw_values)
        )
        missing = [value for value in selected_values if value not in available]
        if missing:
            raise ValueError(
                f"{pair_name}: requested vintages are not available: {', '.join(missing)}"
            )
        selected = [available[value] for value in sorted(selected_values)]
        return selected, {
            "mode": "specified",
            "available": len(windows),
            "selected": [window["bucket"] for window in selected],
        }
    if raw_size not in (None, ""):
        if isinstance(raw_size, bool):
            raise ValueError(f"{pair_name}: vintage_sample_size must be a positive integer")
        try:
            size = int(raw_size)
        except (TypeError, ValueError):
            raise ValueError(
                f"{pair_name}: vintage_sample_size must be a positive integer"
            ) from None
        if (
            size <= 0
            or (isinstance(raw_size, float) and not raw_size.is_integer())
            or (isinstance(raw_size, str) and not raw_size.strip().isdigit())
        ):
            raise ValueError(f"{pair_name}: vintage_sample_size must be a positive integer")
        checked_before = sorted(bucket for bucket in available if bucket in checked)
        candidates = [window for window in windows if window["bucket"] not in checked]
        if windows and not candidates:
            raise ValueError(
                f"{pair_name}: all {len(windows)} vintages were already checked by earlier runs "
                f"(coverage {len(windows)}/{len(windows)}); rerun with --recheck to ignore "
                "pair_recon_window_history or set vintage_values to recheck specific vintages"
            )
        ranked = sorted(
            candidates,
            key=lambda window: hashlib.sha256(
                f"{run_id}\0{pair_name}\0{window['bucket']}".encode()
            ).hexdigest(),
        )
        selected = sorted(ranked[:size], key=lambda window: window["bucket"])
        selection = {
            "mode": "random",
            "requested": size,
            "available": len(windows),
            "selected": [window["bucket"] for window in selected],
        }
        if checked_before:
            selection["checked_before"] = checked_before
        return selected, selection
    return windows, {
        "mode": "all",
        "available": len(windows),
        "selected": [window["bucket"] for window in windows],
    }


# >>> normalizer per key: explicit key_families, else the column's data_type, else partition date types <<< #
def key_family_specs(pair: dict[str, Any], keys: list[str]) -> list[dict[str, Any]]:
    configured = pair.get("key_families") or {}
    column_types = pair.get("column_types") or {}
    overrides = pair.get("col_type_overrides") or {}
    date_col = str(pair["left"].get("date_col") or "").casefold()
    specs = []
    for key in keys:
        value = configured.get(key)
        if value is None:
            value = column_types.get(key)
        if value is None and overrides.get(key) == "numeric":
            value = "numeric"
        if pair.get("partition", "day") != "all" and value is None and key.casefold() == date_col:
            value = {
                "type": "date",
                "left_date_type": resolve_date_type(pair["left"].get("date_type")).spec(
                    portable=True
                ),
                "right_date_type": resolve_date_type(pair["right"].get("date_type")).spec(
                    portable=True
                ),
            }
        specs.append(key_family(value).spec())
    return specs


# >>> Right names for the Left keys: the mapping row, right_date_col for the partition key, else the same name <<< #
def right_key_columns(
    pair_name: str,
    pair: dict[str, Any],
    keys: list[str],
    partition: str,
    date_col: str,
) -> list[str]:
    col_map = pair.get("col_map") or {}
    right_date_col = str((pair.get("right") or {}).get("date_col") or "")
    result = []
    for key in keys:
        mapped = col_map.get(key)
        if partition != "all" and key.lower() == date_col:
            if mapped and mapped.casefold() != right_date_col.casefold():
                raise ValueError(
                    f"{pair_name}: key {key} is the Left date column, which right_date_col maps "
                    f"to {right_date_col!r}, but its mapping row names {mapped!r}; make them agree"
                )
            mapped = mapped or right_date_col
        result.append(mapped or key)
    return result


def build_manifest(
    run_id: str,
    pair_name: str,
    pair: dict[str, Any],
    row_check: dict[str, Any],
) -> dict[str, Any]:
    version = int(pair.get("version") or 1)
    if version not in {1, 2}:
        raise ValueError(f"{pair_name}: version must be 1 or 2")
    if str((pair.get("right") or {}).get("source", "")).lower() not in {"databricks", "csv"}:
        raise ValueError(f"{pair_name}: unsupported Right source")
    if version == 1 and not row_check["gate_decision"].get("passed"):
        raise ValueError(f"{pair_name}: row check has not passed")
    partition_kind = str(pair.get("partition") or "day")
    if partition_kind not in {"day", "month", "all"}:
        raise ValueError(f"{pair_name}: partition must be day, month or all")
    excluded_partitions = sorted(set(pair.get("excluded_partitions") or []))
    mapped_columns = list((pair.get("col_map") or {}).keys())
    lookup = {column.casefold(): column for column in mapped_columns}
    requested = [str(column).strip() for column in pair.get("excluded_columns") or []]
    unknown_excluded = sorted({column for column in requested if column.casefold() not in lookup})
    if unknown_excluded:
        raise ValueError(
            f"{pair_name}: excluded_columns names unmapped columns: {', '.join(unknown_excluded)}"
        )
    excluded_columns = {lookup[column.casefold()] for column in requested}
    date_col = str(pair["left"].get("date_col") or "").lower()
    value_columns = (
        mapped_columns
        if partition_kind == "all"
        else [column for column in mapped_columns if column.lower() != date_col]
    )
    columns = [column for column in value_columns if column not in excluded_columns]
    if not columns:
        raise ValueError(f"{pair_name}: no mapped value columns")
    vintage = "all" if partition_kind == "all" else pair.get("vintage") or "day"
    grouped: dict[str, list[str]] = defaultdict(list)
    counts: dict[str, int] = {}
    for partition, row in sorted(row_check["partitions"].items()):
        admitted = (
            row.get("left") is not None and partition not in excluded_partitions
            if version == 2
            else row["result"] == "match"
        )
        if admitted:
            bucket = "all" if partition_kind == "all" else bucket_date(partition, vintage)
            grouped[bucket].append(partition)
            counts[partition] = int(row["left"])
    windows = [
        {
            "bucket": bucket,
            "partitions": partitions,
            "n_left": sum(counts[partition] for partition in partitions),
        }
        for bucket, partitions in sorted(grouped.items())
    ]
    windows, vintage_selection = select_vintage_windows(run_id, pair_name, pair, windows)
    keys = [part.strip() for part in str(pair.get("key") or "").split(",") if part.strip()]
    key_family_config = dict(pair.get("key_families") or {})
    unknown_keys = sorted(set(key_family_config) - set(keys))
    if keys and unknown_keys:
        raise ValueError(
            f"{pair_name}: key_families names non-key columns: {', '.join(unknown_keys)}"
        )

    try:
        key_families = key_family_specs(pair, keys)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{pair_name}: unsupported key families: {exc}") from None
    right_keys = right_key_columns(pair_name, pair, keys, partition_kind, date_col)
    raw_overrides = pair.get("col_type_overrides") or {}
    unknown = sorted(set(raw_overrides) - set(mapped_columns))
    invalid = sorted(set(raw_overrides.values()) - {"numeric", "categorical"})
    overrides = {column: kind for column, kind in raw_overrides.items() if column in columns}
    if unknown or invalid:
        raise ValueError(
            f"{pair_name}: col_type_overrides needs mapped columns and numeric|categorical, "
            f"got {', '.join(unknown + invalid)}"
        )
    raw_column_types = pair.get("column_types") or {}
    unknown_column_types = sorted(set(raw_column_types) - set(mapped_columns))
    configured_column_types = {
        column: spec for column, spec in raw_column_types.items() if column in columns
    }
    if unknown_column_types:
        raise ValueError(
            f"{pair_name}: column_types needs mapped columns, got {', '.join(unknown_column_types)}"
        )
    try:
        column_types = {
            column: resolve_column_type(spec).spec()
            for column, spec in configured_column_types.items()
        }
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{pair_name}: unsupported column types: {exc}") from None
    sampling = sample_plan(pair.get("sample"))
    if sampling["mode"] != "all" and not keys:
        raise ValueError(f"{pair_name}: sampling requires a key")
    difference_sample_rows = pair.get("difference_sample_rows", 100)
    if isinstance(difference_sample_rows, bool):
        raise TypeError(f"{pair_name}: difference_sample_rows must be a non-negative integer")
    try:
        difference_sample_rows = int(difference_sample_rows)
    except (TypeError, ValueError):
        raise TypeError(
            f"{pair_name}: difference_sample_rows must be a non-negative integer"
        ) from None
    if difference_sample_rows < 0:
        raise ValueError(f"{pair_name}: difference_sample_rows must be non-negative")
    try:
        normalization_rules = canonical_normalization_rules(pair.get("normalization_rules"))
        normalized_on = normalization_date(run_id, normalization_rules)
    except (TypeError, ValueError) as exc:
        raise type(exc)(f"{pair_name}: {exc}") from None
    manifest = {
        "protocol_version": PROTOCOL_VERSION,
        "version": version,
        "run_id": run_id,
        "pair": pair_name,
        "sample": sampling,
        "vintage": vintage,
        "partition": partition_kind,
        "vintage_selection": vintage_selection,
        "key_cols": keys,
        "right_key_cols": right_keys,
        "key_families": key_families,
        "columns": columns,
        "col_map": {column: pair["col_map"][column] for column in columns},
        "windows": windows,
        "left": dict(pair["left"]),
        "right": dict(pair["right"]),
        "date_synonyms": dict(pair.get("date_synonyms") or {}),
        "normalization_rules": normalization_rules,
        "audit": bool(pair.get("audit", False)),
        "difference_sample_rows": difference_sample_rows,
        "local_reconciliation": bool(pair.get("local_reconciliation", False)),
        "col_type_overrides": overrides,
        "column_types": column_types,
        "excluded_partitions": excluded_partitions,
        "excluded_columns": sorted(excluded_columns),
        "expected_units": len(windows) * len(columns),
    }
    if normalized_on is not None:
        manifest["normalization_date"] = normalized_on
    if pair.get("snapshot"):
        manifest["left_snapshot"] = True
    statistics = statistics_plan(pair.get("statistics"))
    if statistics is not None:
        manifest["statistics"] = statistics
    manifest["plan_id"] = _digest(
        {
            key: value
            for key, value in manifest.items()
            if key not in {"difference_sample_rows", "local_reconciliation"}
        }
    )
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: dict[str, Any]) -> None:
    required = {
        "protocol_version",
        "run_id",
        "pair",
        "sample",
        "vintage",
        "columns",
        "col_map",
        "windows",
        "right",
        "expected_units",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"manifest missing fields: {', '.join(missing)}")
    if manifest["protocol_version"] != PROTOCOL_VERSION:
        raise ValueError("unsupported protocol version")
    version = int(manifest.get("version") or 1)
    if version not in {1, 2}:
        raise ValueError("manifest version must be 1 or 2")
    if version == 2 and not manifest.get("plan_id"):
        raise ValueError("version 2 manifest requires plan_id")
    if str(manifest["right"].get("source", "")).lower() not in {"databricks", "csv"}:
        raise ValueError("manifest has unsupported Right source")
    partition_kind = str(manifest.get("partition") or "day")
    if partition_kind not in {"day", "month", "all"}:
        raise ValueError("manifest partition must be day, month or all")
    if partition_kind == "all" and manifest["vintage"] != "all":
        raise ValueError("all-partition manifest vintage must be all")
    statistics = statistics_plan(manifest.get("statistics"))
    if statistics is not None and manifest["statistics"] != statistics:
        raise ValueError("manifest statistics must use canonical order")
    difference_sample_rows = manifest.get("difference_sample_rows", 100)
    if (
        isinstance(difference_sample_rows, bool)
        or not isinstance(difference_sample_rows, int)
        or difference_sample_rows < 0
    ):
        raise ValueError("manifest difference_sample_rows must be a non-negative integer")
    if not isinstance(manifest.get("local_reconciliation", False), bool):
        raise TypeError("manifest local_reconciliation must be true or false")
    normalization_rules = canonical_normalization_rules(manifest.get("normalization_rules"))
    if manifest.get("normalization_rules") is not None and normalization_rules != manifest.get(
        "normalization_rules"
    ):
        raise ValueError("manifest normalization_rules must use canonical rule objects")
    normalized_on = normalization_date(str(manifest["run_id"]), normalization_rules)
    if normalized_on is not None and manifest.get("normalization_date") != normalized_on:
        raise ValueError("manifest normalization_date does not match its run_id")
    expected = len(manifest["windows"]) * len(manifest["columns"])
    if manifest["expected_units"] != expected:
        raise ValueError("expected_units does not equal Windows x columns")
    column_types = manifest.get("column_types") or {}
    unknown_column_types = sorted(set(column_types) - set(manifest["columns"]))
    if unknown_column_types:
        raise ValueError(
            f"column_types names columns outside manifest: {', '.join(unknown_column_types)}"
        )
    for spec in column_types.values():
        resolve_column_type(spec)
    for window in manifest["windows"]:
        if partition_kind == "all" and (
            window.get("bucket") != "all" or window.get("partitions") != ["all"]
        ):
            raise ValueError("all-partition manifest must contain only the all window")
        if window["partitions"] != sorted(set(window["partitions"])):
            raise ValueError("Window Partitions must be sorted and unique")
