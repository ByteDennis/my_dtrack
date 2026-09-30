from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv

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


# >>> a col_map file as {left: right} plus optional types: CSV left,right[,type] or a JSON object <<< #
def read_col_map(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"col_map file not found: {path}")
    if path.suffix.lower() == ".json":
        mapping = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(mapping, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in mapping.items()
        ):
            raise ValueError(f"{path}: col_map JSON must be an object of left: right names")
        rows = [{"left": key, "right": value} for key, value in mapping.items()]
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
    mapping, types = {}, {}
    for row in rows:
        left, right = (row.get("left") or "").strip(), (row.get("right") or "").strip()
        if not left or not right:
            raise ValueError(f"{path}: every col_map row needs left and right, got {row}")
        if left in mapping or right in mapping.values():
            raise ValueError(f"{path}: col_map maps {left!r} or {right!r} twice")
        mapping[left] = right
        if (row.get("type") or "").strip():
            types[left] = row["type"].strip().lower()
    return mapping, types


# >>> replace a col_map path (relative to the config file) with its mapping and merge its types <<< #
def expand_col_maps(config: dict[str, Any], base: Path) -> dict[str, Any]:
    for name, pair in (config.get("pairs") or {}).items():
        if not isinstance(pair.get("col_map"), str):
            continue
        mapping, types = read_col_map(base / pair["col_map"])
        overrides = dict(pair.get("col_type_overrides") or {})
        clash = {col for col, kind in types.items() if overrides.get(col, kind) != kind}
        if clash:
            raise ValueError(
                f"{name}: col_map type and col_type_overrides disagree on {sorted(clash)}"
            )
        pair["col_map"] = mapping
        if types or overrides:
            pair["col_type_overrides"] = {**types, **overrides}
    return config


def load_config(path: str | Path) -> dict[str, Any]:
    load_dotenv(find_dotenv(usecwd=True))
    path = Path(path)
    with path.open(encoding="utf-8") as handle:
        return validate_config(expand_col_maps(json.load(handle), path.resolve().parent))
