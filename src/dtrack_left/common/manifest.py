from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from typing import Any

from .. import PROTOCOL_VERSION
from .column_types import portable_column_type, resolve_column_type
from .dates import bucket_date, portable_date_type
from .hashing import HASH_BUCKET_SPACE, key_family


def _slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    return text[:48] or "empty"


def unit_id(pair: str, window: str, column: str) -> str:
    raw = json.dumps([pair, window, column], ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"{_slug(pair)}--{_slug(window)}--{_slug(column)}--{digest}"


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


def build_manifest(
    run_id: str,
    pair_name: str,
    pair: dict[str, Any],
    row_check: dict[str, Any],
) -> dict[str, Any]:
    if str((pair.get("right") or {}).get("source", "")).lower() != "databricks":
        raise ValueError(f"{pair_name}: Right side must be databricks")
    if not row_check["gate_decision"].get("passed"):
        raise ValueError(f"{pair_name}: row check has not passed")
    columns = list((pair.get("col_map") or {}).keys())
    date_col = str(pair["left"].get("date_col") or "").lower()
    columns = [column for column in columns if column.lower() != date_col]
    if not columns:
        raise ValueError(f"{pair_name}: no mapped value columns")
    vintage = pair.get("vintage") or "day"
    grouped: dict[str, list[str]] = defaultdict(list)
    counts: dict[str, int] = {}
    for partition, row in sorted(row_check["partitions"].items()):
        if row["verdict"] == "match":
            grouped[bucket_date(partition, vintage)].append(partition)
            counts[partition] = int(row["left"])
    windows = [
        {
            "bucket": bucket,
            "partitions": partitions,
            "n_left": sum(counts[partition] for partition in partitions),
        }
        for bucket, partitions in sorted(grouped.items())
    ]
    keys = [part.strip() for part in str(pair.get("key") or "").split(",") if part.strip()]
    key_family_config = dict(pair.get("key_families") or {})
    unknown_keys = sorted(set(key_family_config) - set(keys))
    if keys and unknown_keys:
        raise ValueError(
            f"{pair_name}: key_families names non-key columns: {', '.join(unknown_keys)}"
        )
    def family_spec(key: str):
        configured = key_family_config.get(key)
        if configured is None and key.lower() == date_col:
            configured = {
                "family": "date_type",
                "left_date_type": portable_date_type(pair["left"].get("date_type")),
                "right_date_type": portable_date_type(pair["right"].get("date_type")),
            }
        return key_family(configured).spec()

    try:
        key_families = [family_spec(key) for key in keys]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{pair_name}: unsupported key families: {exc}") from None
    overrides = dict(pair.get("col_type_overrides") or {})
    unknown = sorted(set(overrides) - set(columns))
    invalid = sorted(set(overrides.values()) - {"numeric", "categorical"})
    if unknown or invalid:
        raise ValueError(
            f"{pair_name}: col_type_overrides needs mapped columns and numeric|categorical, "
            f"got {', '.join(unknown + invalid)}"
        )
    configured_column_types = dict(pair.get("column_types") or {})
    unknown_column_types = sorted(set(configured_column_types) - set(columns))
    if unknown_column_types:
        raise ValueError(
            f"{pair_name}: column_types needs mapped columns, got "
            f"{', '.join(unknown_column_types)}"
        )
    try:
        column_types = {
            column: portable_column_type(spec)
            for column, spec in configured_column_types.items()
        }
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{pair_name}: unsupported column types: {exc}") from None
    sampling = sample_plan(pair.get("sample"))
    if sampling["mode"] != "all" and not keys:
        raise ValueError(f"{pair_name}: sampling requires a key")
    manifest = {
        "protocol_version": PROTOCOL_VERSION,
        "run_id": run_id,
        "pair": pair_name,
        "sample": sampling,
        "vintage": vintage,
        "key_cols": keys,
        "right_key_cols": [(pair["col_map"]).get(key, key) for key in keys],
        "key_families": key_families,
        "columns": columns,
        "col_map": {column: pair["col_map"][column] for column in columns},
        "windows": windows,
        "left": dict(pair["left"]),
        "right": dict(pair["right"]),
        "date_synonyms": dict(pair.get("date_synonyms") or {}),
        "strict": bool(pair.get("strict")),
        "col_type_overrides": overrides,
        "column_types": column_types,
        "expected_units": len(windows) * len(columns),
    }
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
    if str(manifest["right"].get("source", "")).lower() != "databricks":
        raise ValueError("manifest Right side must be databricks")
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
        if window["partitions"] != sorted(set(window["partitions"])):
            raise ValueError("Window Partitions must be sorted and unique")
