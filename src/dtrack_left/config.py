from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

LEFT_SOURCES = {"oracle", "athena", "aws", "hadoop", "csv"}


# >>> expand side.conn_macro from top-level connections; pair-level fields win <<< #
def resolve_connections(config: dict[str, Any]) -> dict[str, Any]:
    profiles = config.get("connections") or {}
    if not isinstance(profiles, dict):
        raise TypeError("connections must be an object of named profiles")
    for name, pair in (config.get("pairs") or {}).items():
        for side in ("left", "right"):
            spec = pair.get(side)
            macro = (spec or {}).get("conn_macro")
            if not macro or not isinstance(spec, dict):
                continue
            if macro not in profiles:
                if str(spec.get("source") or "").lower() == "csv":
                    continue
                raise ValueError(f"{name}: unknown conn_macro {macro!r} for {side}")
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
        if not left.get("date_col") or not right.get("date_col"):
            raise ValueError(f"{name}: both sides require date_col")
        if not isinstance(pair.get("col_map"), dict) or not pair["col_map"]:
            raise ValueError(f"{name}: col_map must be non-empty")
    return config


def load_config(path: str | Path) -> dict[str, Any]:
    load_dotenv()
    with Path(path).open(encoding="utf-8") as handle:
        return validate_config(json.load(handle))
