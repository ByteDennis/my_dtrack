from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv

LEFT_SOURCES = {"oracle", "athena", "aws", "hadoop", "csv", "file"}
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
    pairs = config.get("pairs")
    if not isinstance(pairs, dict) or not pairs:
        raise ValueError("config requires a non-empty pairs object")
    resolve_connections(config)
    for name, pair in pairs.items():
        if pair.get("skip"):
            continue
        left, right = pair.get("left") or {}, pair.get("right") or {}
        left_source = str(left.get("source") or "").lower()
        right_source = str(right.get("source") or "").lower()
        if left_source not in LEFT_SOURCES:
            raise ValueError(f"{name}: unsupported Left source {left_source!r}")
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
        if not left.get("date_col") or not right.get("date_col"):
            raise ValueError(f"{name}: both sides require date_col")
        if not isinstance(pair.get("col_map"), dict) or not pair["col_map"]:
            raise ValueError(f"{name}: col_map must be non-empty")
        overrides = pair.get("col_type_overrides") or {}
        if set(overrides) - set(pair["col_map"]) or set(overrides.values()) - COL_TYPES:
            raise ValueError(f"{name}: col_type_overrides maps col_map keys to numeric|categorical")
    return config


def load_config(path: str | Path) -> dict[str, Any]:
    load_dotenv(find_dotenv(usecwd=True))
    with Path(path).open(encoding="utf-8") as handle:
        return validate_config(json.load(handle))
