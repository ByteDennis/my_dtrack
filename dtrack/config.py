"""JSON configuration parsing for table pairs (unified format only)."""

import json
import re
from pathlib import Path


def _derive_side_name(side_cfg, pair_name):
    """Derive a SAS-safe identifier for a side from its `table` field.

    Two sides of one pair often reference different physical tables (e.g.
    left=v_xxxx, right=xxxx) — using pair_name for both would collide on
    qualified_name() and overwrite the SAS get_colstats_{sn} macro.

    Falls back to pair_name when `table` is missing.
    """
    tbl = (side_cfg.get("table") or "").strip()
    if not tbl:
        return pair_name
    return re.sub(r"[^a-z0-9_]+", "_", tbl.lower()).strip("_") or pair_name


def validate_unified_pair(pair_name, pair_config):
    """Validate a single pair in unified config format."""
    if "left" not in pair_config:
        raise ValueError(f"Pair '{pair_name}' must have 'left' table configuration")
    if "right" not in pair_config:
        raise ValueError(f"Pair '{pair_name}' must have 'right' table configuration")

    for side in ("left", "right"):
        tbl = pair_config[side]
        if not isinstance(tbl, dict):
            raise ValueError(f"Pair '{pair_name}': '{side}' must be a dictionary")
        if "source" not in tbl:
            raise ValueError(f"Pair '{pair_name}': {side} table must have 'source' field")

    # Validate HITL fields if present
    if "ignore_rows" in pair_config:
        if not isinstance(pair_config["ignore_rows"], list):
            raise ValueError(f"Pair '{pair_name}': 'ignore_rows' must be a list")
    if "ignore_columns" in pair_config:
        if not isinstance(pair_config["ignore_columns"], list):
            raise ValueError(f"Pair '{pair_name}': 'ignore_columns' must be a list")
    if "col_type_overrides" in pair_config:
        if not isinstance(pair_config["col_type_overrides"], dict):
            raise ValueError(f"Pair '{pair_name}': 'col_type_overrides' must be a dict")


def validate_date_types(date_types):
    """Validate the optional date_types configuration block.

    Each custom type must have: label (string), category (date|number|string),
    format (string). Optional: date_transform, parse_to_date (SQL with {col}).
    """
    if not isinstance(date_types, dict):
        raise ValueError("'date_types' must be a dictionary")
    valid_categories = ("date", "number", "string")
    for type_key, type_cfg in date_types.items():
        if not isinstance(type_cfg, dict):
            raise ValueError(f"date_types['{type_key}'] must be a dictionary")
        if "label" not in type_cfg or not isinstance(type_cfg["label"], str):
            raise ValueError(f"date_types['{type_key}'] must have a string 'label'")
        cat = type_cfg.get("category", "")
        if cat not in valid_categories:
            raise ValueError(
                f"date_types['{type_key}'].category must be one of {valid_categories}, got '{cat}'"
            )
        if "format" not in type_cfg or not isinstance(type_cfg["format"], str):
            raise ValueError(f"date_types['{type_key}'] must have a string 'format'")
        # Optional SQL expression fields
        for opt_key in ("date_transform", "parse_to_date"):
            if opt_key in type_cfg and not isinstance(type_cfg[opt_key], str):
                raise ValueError(f"date_types['{type_key}'].{opt_key} must be a string")


def validate_unified_config(config):
    """Validate unified configuration format."""
    if "pairs" not in config:
        raise ValueError("Configuration must have 'pairs' key")

    pairs = config["pairs"]
    if not isinstance(pairs, dict):
        raise ValueError("'pairs' must be a dictionary (not a list)")

    if not pairs:
        raise ValueError("'pairs' must contain at least one pair")

    for pair_name, pair_config in pairs.items():
        validate_unified_pair(pair_name, pair_config)

    # Validate optional date_types block
    if "date_types" in config:
        validate_date_types(config["date_types"])


def load_unified_config(config_path):
    """Load unified configuration from JSON file.

    Auto-injects 'name' = pair_name into left/right if not set.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, 'r') as f:
        config = json.load(f)

    validate_unified_config(config)

    # Auto-derive name from each side's `table` so left and right stay
    # distinct identifiers even when they sit in the same pair.
    for pair_name, pair_cfg in config.get("pairs", {}).items():
        for side in ("left", "right"):
            side_cfg = pair_cfg.get(side, {})
            if "name" not in side_cfg:
                side_cfg["name"] = _derive_side_name(side_cfg, pair_name)

    return config


def save_unified_config(config, config_path):
    """Save unified configuration to JSON file.

    Strips 'name' from left/right since it's auto-derived from pair_name on load.
    Preserves top-level keys like date_types, settings, metadata.
    """
    import copy
    out = copy.deepcopy(config)
    for pair_cfg in out.get("pairs", {}).values():
        for side in ("left", "right"):
            pair_cfg.get(side, {}).pop("name", None)
    validate_unified_config(out)
    with open(config_path, 'w') as f:
        json.dump(out, f, indent=2)


def get_all_tables_from_unified(config):
    """Extract all unique tables from unified config for extraction.

    Returns list of table configs tagged with pair membership.
    Respects skip and ignore settings.
    """
    tables = []
    seen = set()

    for pair_name, pair_config in config["pairs"].items():
        if pair_config.get("skip"):
            continue
        col_map = pair_config.get("col_map", {})
        pair_vintage = pair_config.get("vintage", "")
        col_filter = pair_config.get("col_filter") or {}

        # Resolve the pair's effective (left, right) col pairs after applying
        # include/exclude patterns. Falls back to all of col_map if no filter.
        selected_left, selected_right = [], []
        if col_map:
            from .compare import resolve_col_filter
            resolved = resolve_col_filter(
                col_map,
                include_patterns=col_filter.get("include"),
                exclude_patterns=col_filter.get("exclude"),
            )
            selected_left  = [l for l, _ in resolved["pairs"]]
            selected_right = [r for _, r in resolved["pairs"]]

        for side in ["left", "right"]:
            table_cfg = pair_config[side].copy()

            # Pair-level vintage propagates to both sides; per-side override wins
            # only if the per-side dict already specifies one (legacy configs).
            if pair_vintage and not table_cfg.get("vintage"):
                table_cfg["vintage"] = pair_vintage

            if col_map:
                if side == "left":
                    table_cfg["_col_map_columns"] = set(col_map.keys())
                    table_cfg["_selected_cols"] = selected_left
                else:
                    table_cfg["_col_map_columns"] = set(col_map.values())
                    table_cfg["_selected_cols"] = selected_right

            source = table_cfg.get("source", "")
            name = table_cfg.get("name", "")
            key = (source, name)

            if key not in seen:
                table_cfg["_pairs"] = [pair_name]
                tables.append(table_cfg)
                seen.add(key)
            else:
                for t in tables:
                    if t.get("source") == source and t.get("name") == name:
                        t["_pairs"].append(pair_name)
                        existing = t.get("_col_map_columns", set())
                        existing.update(table_cfg.get("_col_map_columns", set()))
                        t["_col_map_columns"] = existing
                        # Union selected_cols across pairs that share the
                        # same physical table so the single extraction covers
                        # every pair's subset.
                        new_sel = table_cfg.get("_selected_cols") or []
                        if new_sel:
                            cur = list(t.get("_selected_cols") or [])
                            seen_cols = {c.lower() for c in cur}
                            for c in new_sel:
                                if c.lower() not in seen_cols:
                                    cur.append(c)
                                    seen_cols.add(c.lower())
                            t["_selected_cols"] = cur
                        # If a later pair specifies a finer vintage for the same
                        # table, prefer it (otherwise first-pair vintage wins).
                        if pair_vintage and not t.get("vintage"):
                            t["vintage"] = pair_vintage
                        break

    return tables


# ============================================================================
# Pair field accessors
# ============================================================================

def get_pair_where_map(config, pair_name):
    """Get where_map for a specific pair."""
    return config.get("pairs", {}).get(pair_name, {}).get("where_map")


def set_pair_where_map(config, pair_name, where_left, where_right):
    """Set where_map for a specific pair. Modifies config in place."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["where_map"] = {
        "left": where_left,
        "right": where_right
    }


def get_pair_col_map(config, pair_name):
    """Get col_map for a specific pair."""
    return config.get("pairs", {}).get(pair_name, {}).get("col_map", {})


def set_pair_col_map(config, pair_name, col_map):
    """Set col_map for a specific pair. Modifies config in place."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["col_map"] = col_map


# ============================================================================
# HITL write-back functions
# ============================================================================

def mark_pair_skipped(config, pair_name, skipped=True):
    """Mark a pair as skipped. Persists HITL decision."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["skip"] = skipped


def add_ignored_rows(config, pair_name, dates):
    """Add dates to the ignore_rows list for a pair."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    existing = set(pair.get("ignore_rows", []))
    existing.update(dates)
    pair["ignore_rows"] = sorted(existing)


def add_ignored_columns(config, pair_name, columns):
    """Add columns to the ignore_columns list for a pair."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    existing = set(pair.get("ignore_columns", []))
    existing.update(columns)
    pair["ignore_columns"] = sorted(existing)


def set_col_type_override(config, pair_name, col_name, col_type):
    """Set a column type override for a pair."""
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    if "col_type_overrides" not in pair:
        pair["col_type_overrides"] = {}
    pair["col_type_overrides"][col_name] = col_type


def get_ignored_rows(config, pair_name):
    """Get the ignore_rows list for a pair."""
    return config.get("pairs", {}).get(pair_name, {}).get("ignore_rows", [])


def get_ignored_columns(config, pair_name):
    """Get the ignore_columns list for a pair."""
    return config.get("pairs", {}).get(pair_name, {}).get("ignore_columns", [])


def get_col_type_overrides(config, pair_name):
    """Get col_type_overrides dict for a pair."""
    return config.get("pairs", {}).get(pair_name, {}).get("col_type_overrides", {})


def ensure_pair_defaults(config, pair_name):
    """Ensure a pair has all default sub-keys (time_map, comment_map, diff_map)."""
    if pair_name not in config.get("pairs", {}):
        return
    pair = config["pairs"][pair_name]
    if "time_map" not in pair:
        pair["time_map"] = {
            "row": {"left": "—", "right": "—"},
            "col": {"left": "—", "right": "—"}
        }
    if "comment_map" not in pair:
        pair["comment_map"] = {
            "row": {"left": "", "right": ""},
            "col": {"left": "", "right": ""}
        }
    if "diff_map" not in pair:
        pair["diff_map"] = {}
