"""JSON configuration parsing for table pairs (unified format only)."""

import json
from pathlib import Path


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
        if "name" not in tbl:
            raise ValueError(f"Pair '{pair_name}': {side} table must have 'name' field")
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


def load_unified_config(config_path):
    """Load unified configuration from JSON file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, 'r') as f:
        config = json.load(f)

    validate_unified_config(config)
    return config


def save_unified_config(config, config_path):
    """Save unified configuration to JSON file."""
    validate_unified_config(config)
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)


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
        for side in ["left", "right"]:
            table_cfg = pair_config[side].copy()

            if col_map:
                if side == "left":
                    table_cfg["_col_map_columns"] = set(col_map.keys())
                else:
                    table_cfg["_col_map_columns"] = set(col_map.values())

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
