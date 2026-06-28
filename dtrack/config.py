
import json
import re
from pathlib import Path

# >>> Derive a safe identifier for a side from its `table` field <<< #
def _derive_side_name(side_cfg, pair_name):
    tbl = (side_cfg.get("table") or "").strip()
    if not tbl:
        return pair_name
    return re.sub(r"[^a-z0-9_]+", "_", tbl.lower()).strip("_") or pair_name

# >>> Validate a single pair in unified config format <<< #
def validate_unified_pair(pair_name, pair_config):
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
        if (tbl.get("source") or "").lower() == "csv" and not (tbl.get("conn_macro") or "").strip():
            raise ValueError(
                f"Pair '{pair_name}': {side} table source='csv' requires 'conn_macro' "
                f"(the CSV file path); csv uses conn_macro, not a 'csv_path' field")

    if "ignore_rows" in pair_config:
        if not isinstance(pair_config["ignore_rows"], list):
            raise ValueError(f"Pair '{pair_name}': 'ignore_rows' must be a list")
    if "ignore_columns" in pair_config:
        if not isinstance(pair_config["ignore_columns"], list):
            raise ValueError(f"Pair '{pair_name}': 'ignore_columns' must be a list")
    if "col_type_overrides" in pair_config:
        if not isinstance(pair_config["col_type_overrides"], dict):
            raise ValueError(f"Pair '{pair_name}': 'col_type_overrides' must be a dict")

# >>> Validate the optional date_types configuration block <<< #
def validate_date_types(date_types):
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
        for opt_key in ("date_transform", "parse_to_date"):
            if opt_key in type_cfg and not isinstance(type_cfg[opt_key], str):
                raise ValueError(f"date_types['{type_key}'].{opt_key} must be a string")

# >>> Validate unified configuration format <<< #
def validate_unified_config(config):
    if "pairs" not in config:
        raise ValueError("Configuration must have 'pairs' key")

    pairs = config["pairs"]
    if not isinstance(pairs, dict):
        raise ValueError("'pairs' must be a dictionary (not a list)")

    for pair_name, pair_config in pairs.items():
        validate_unified_pair(pair_name, pair_config)

    if "date_types" in config:
        validate_date_types(config["date_types"])

# >>> Load unified configuration from JSON file <<< #
def load_unified_config(config_path):
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, 'r') as f:
        config = json.load(f)

    validate_unified_config(config)

    for pair_name, pair_cfg in config.get("pairs", {}).items():
        for side in ("left", "right"):
            side_cfg = pair_cfg.get(side, {})
            if "name" not in side_cfg:
                side_cfg["name"] = _derive_side_name(side_cfg, pair_name)

    return config

# >>> Save unified configuration to JSON file <<< #
def save_unified_config(config, config_path):
    import copy
    out = copy.deepcopy(config)
    # >>> strip a side's name only when it matches the table-derived one, so divergent names survive reload <<< #
    for pair_name, pair_cfg in out.get("pairs", {}).items():
        for side in ("left", "right"):
            side_cfg = pair_cfg.get(side, {})
            if side_cfg.get("name") == _derive_side_name(side_cfg, pair_name):
                side_cfg.pop("name", None)
    validate_unified_config(out)
    with open(config_path, 'w') as f:
        json.dump(out, f, indent=2)

# >>> Extract all unique tables from unified config for extraction <<< #
def get_all_tables_from_unified(config):
    tables = []
    seen = set()

    for pair_name, pair_config in config["pairs"].items():
        if pair_config.get("skip"):
            continue
        col_map = pair_config.get("col_map", {})
        pair_vintage = pair_config.get("vintage", "")
        col_filter = pair_config.get("col_filter") or {}
        cf_has_patterns = bool(col_filter.get("include") or col_filter.get("exclude"))

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

        pair_strict = pair_config.get("strict")

        for side in ["left", "right"]:
            table_cfg = pair_config[side].copy()

            table_cfg.setdefault("_side", side)

            if pair_vintage and not table_cfg.get("vintage"):
                table_cfg["vintage"] = pair_vintage

            if pair_strict is not None and table_cfg.get("strict") is None:
                if isinstance(pair_strict, (list, tuple)):
                    # strict list is given in LEFT column names; map to the right
                    # side's columns so the same logical column is strict on both
                    if side == "right" and col_map:
                        table_cfg["strict"] = [col_map.get(c, c) for c in pair_strict]
                    else:
                        table_cfg["strict"] = list(pair_strict)
                else:
                    table_cfg["strict"] = pair_strict

            if col_map:
                if side == "left":
                    table_cfg["_col_map_columns"] = set(col_map.keys())
                    table_cfg["_selected_cols"] = selected_left
                else:
                    table_cfg["_col_map_columns"] = set(col_map.values())
                    table_cfg["_selected_cols"] = selected_right
            elif cf_has_patterns:
                table_cfg["_col_filter_patterns"] = {
                    "include": list(col_filter.get("include") or []),
                    "exclude": list(col_filter.get("exclude") or []),
                }

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
                        new_sel = table_cfg.get("_selected_cols") or []
                        if new_sel:
                            cur = list(t.get("_selected_cols") or [])
                            seen_cols = {c.lower() for c in cur}
                            for c in new_sel:
                                if c.lower() not in seen_cols:
                                    cur.append(c)
                                    seen_cols.add(c.lower())
                            t["_selected_cols"] = cur
                        if pair_vintage and not t.get("vintage"):
                            t["vintage"] = pair_vintage
                        new_cf = table_cfg.get("_col_filter_patterns")
                        if new_cf:
                            cur_cf = t.get("_col_filter_patterns") or {
                                "include": [], "exclude": [],
                            }
                            for bucket in ("include", "exclude"):
                                seen_p = {p.lower() for p in cur_cf.get(bucket) or []}
                                for p in new_cf.get(bucket) or []:
                                    if p.lower() not in seen_p:
                                        cur_cf.setdefault(bucket, []).append(p)
                                        seen_p.add(p.lower())
                            t["_col_filter_patterns"] = cur_cf
                        break

    return tables

# >>> Get where_map for a specific pair <<< #
def get_pair_where_map(config, pair_name):
    return config.get("pairs", {}).get(pair_name, {}).get("where_map")

# >>> Set where_map for a specific pair. Modifies config in place <<< #
def set_pair_where_map(config, pair_name, where_left, where_right):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["where_map"] = {
        "left": where_left,
        "right": where_right
    }

# >>> Get col_map for a specific pair <<< #
def get_pair_col_map(config, pair_name):
    return config.get("pairs", {}).get(pair_name, {}).get("col_map", {})

# >>> Set col_map for a specific pair. Modifies config in place <<< #
def set_pair_col_map(config, pair_name, col_map):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["col_map"] = col_map

# >>> Mark a pair as skipped. Persists HITL decision <<< #
def mark_pair_skipped(config, pair_name, skipped=True):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    config["pairs"][pair_name]["skip"] = skipped

# >>> Add dates to the ignore_rows list for a pair <<< #
def add_ignored_rows(config, pair_name, dates):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    existing = set(pair.get("ignore_rows", []))
    existing.update(dates)
    pair["ignore_rows"] = sorted(existing)

# >>> Add columns to the ignore_columns list for a pair <<< #
def add_ignored_columns(config, pair_name, columns):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    existing = set(pair.get("ignore_columns", []))
    existing.update(columns)
    pair["ignore_columns"] = sorted(existing)

# >>> Set a column type override for a pair <<< #
def set_col_type_override(config, pair_name, col_name, col_type):
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")
    pair = config["pairs"][pair_name]
    if "col_type_overrides" not in pair:
        pair["col_type_overrides"] = {}
    pair["col_type_overrides"][col_name] = col_type

# >>> Get the ignore_rows list for a pair <<< #
def get_ignored_rows(config, pair_name):
    return config.get("pairs", {}).get(pair_name, {}).get("ignore_rows", [])

# >>> Get the ignore_columns list for a pair <<< #
def get_ignored_columns(config, pair_name):
    return config.get("pairs", {}).get(pair_name, {}).get("ignore_columns", [])

# >>> Get col_type_overrides dict for a pair <<< #
def get_col_type_overrides(config, pair_name):
    return config.get("pairs", {}).get(pair_name, {}).get("col_type_overrides", {})

# >>> Ensure a pair has all default sub-keys (time_map, comment_map, diff_map) <<< #
def ensure_pair_defaults(config, pair_name):
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
