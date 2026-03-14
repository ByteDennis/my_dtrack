"""JSON configuration parsing for table pairs"""

import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime


def validate_pair_config(pair: Dict[str, Any]) -> None:
    """
    Validate a single pair configuration.

    Args:
        pair: Dictionary with pair configuration

    Raises:
        ValueError: If configuration is invalid
    """
    # Check required fields
    if "name" not in pair:
        raise ValueError("Pair configuration must have 'name' field")

    if "tables" not in pair:
        raise ValueError(f"Pair '{pair['name']}' must have 'tables' field")

    tables = pair["tables"]
    if not isinstance(tables, dict):
        raise ValueError(f"Pair '{pair['name']}': 'tables' must be a dictionary")

    if len(tables) < 2:
        raise ValueError(f"Pair '{pair['name']}' must have at least two tables")

    # Validate each table configuration
    for source_name, table_config in tables.items():
        if not isinstance(table_config, dict):
            raise ValueError(
                f"Pair '{pair['name']}', table '{source_name}': configuration must be a dictionary"
            )

        if "table_name" not in table_config:
            raise ValueError(
                f"Pair '{pair['name']}', table '{source_name}': missing 'table_name' field"
            )


def parse_pairs_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse pairs configuration from dictionary.

    Args:
        config: Dictionary with configuration (must have 'pairs' key)

    Returns:
        List of pair configurations

    Raises:
        ValueError: If configuration is invalid
    """
    if "pairs" not in config:
        raise ValueError("Configuration must have 'pairs' key")

    pairs = config["pairs"]
    if not isinstance(pairs, list):
        raise ValueError("'pairs' must be a list")

    # Validate each pair
    for pair in pairs:
        validate_pair_config(pair)

    return pairs


def load_pairs_config_from_file(config_path: str) -> List[Dict[str, Any]]:
    """
    Load pairs configuration from JSON file.

    Args:
        config_path: Path to JSON configuration file

    Returns:
        List of pair configurations

    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, 'r') as f:
        config = json.load(f)

    return parse_pairs_config(config)


def apply_table_defaults(table_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply default values to table configuration.

    Args:
        table_config: Table configuration dictionary

    Returns:
        Table configuration with defaults applied
    """
    defaults = {
        "vintage": "day",
        "mode": "upsert",
        "db": None,
        "source_table": None,
        "date_col": None,
    }

    # Create new dict with defaults, then override with provided values
    result = defaults.copy()
    result.update(table_config)
    return result


# ============================================================================
# UNIFIED CONFIG FORMAT (NEW)
# ============================================================================

def validate_unified_pair(pair_name: str, pair_config: Dict[str, Any]) -> None:
    """
    Validate a single pair in unified config format.

    Unified format:
    {
      "pair_name": {
        "left": {...table config...},
        "right": {...table config...},
        "col_map": {},
        "where_map": {},
        "metadata": {}
      }
    }

    Raises:
        ValueError: If configuration is invalid
    """
    if "left" not in pair_config:
        raise ValueError(f"Pair '{pair_name}' must have 'left' table configuration")

    if "right" not in pair_config:
        raise ValueError(f"Pair '{pair_name}' must have 'right' table configuration")

    # Validate left table
    left = pair_config["left"]
    if not isinstance(left, dict):
        raise ValueError(f"Pair '{pair_name}': 'left' must be a dictionary")
    if "name" not in left:
        raise ValueError(f"Pair '{pair_name}': left table must have 'name' field")
    if "source" not in left:
        raise ValueError(f"Pair '{pair_name}': left table must have 'source' field")

    # Validate right table
    right = pair_config["right"]
    if not isinstance(right, dict):
        raise ValueError(f"Pair '{pair_name}': 'right' must be a dictionary")
    if "name" not in right:
        raise ValueError(f"Pair '{pair_name}': right table must have 'name' field")
    if "source" not in right:
        raise ValueError(f"Pair '{pair_name}': right table must have 'source' field")


def validate_unified_config(config: Dict[str, Any]) -> None:
    """
    Validate unified configuration format.

    Args:
        config: Dictionary with unified configuration

    Raises:
        ValueError: If configuration is invalid
    """
    if "pairs" not in config:
        raise ValueError("Configuration must have 'pairs' key")

    pairs = config["pairs"]
    if not isinstance(pairs, dict):
        raise ValueError("'pairs' must be a dictionary (not a list)")

    if not pairs:
        raise ValueError("'pairs' must contain at least one pair")

    # Validate each pair
    for pair_name, pair_config in pairs.items():
        validate_unified_pair(pair_name, pair_config)


def load_unified_config(config_path: str) -> Dict[str, Any]:
    """
    Load unified configuration from JSON file.

    Args:
        config_path: Path to JSON configuration file

    Returns:
        Dictionary with unified configuration

    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, 'r') as f:
        config = json.load(f)

    validate_unified_config(config)
    return config


def save_unified_config(config: Dict[str, Any], config_path: str) -> None:
    """
    Save unified configuration to JSON file.

    Args:
        config: Dictionary with unified configuration
        config_path: Path to JSON configuration file

    Raises:
        ValueError: If configuration is invalid
    """
    validate_unified_config(config)

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)


def get_all_tables_from_unified(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract all unique tables from unified config for extraction.

    Returns list of table configs suitable for gen-sas/gen-aws.
    Each table gets tagged with which pair(s) it belongs to.

    Args:
        config: Unified configuration dictionary

    Returns:
        List of table configurations
    """
    tables = []
    seen = set()  # Track (source, name) to avoid duplicates

    for pair_name, pair_config in config["pairs"].items():
        # Skip pairs marked with skip: true
        if pair_config.get("skip"):
            continue
        col_map = pair_config.get("col_map", {})
        for side in ["left", "right"]:
            table_cfg = pair_config[side].copy()

            # Inject col_map column names for this side
            if col_map:
                if side == "left":
                    table_cfg["_col_map_columns"] = set(col_map.keys())
                else:
                    table_cfg["_col_map_columns"] = set(col_map.values())

            # Create unique key
            source = table_cfg.get("source", "")
            name = table_cfg.get("name", "")
            key = (source, name)

            if key not in seen:
                # Tag table with pair membership
                table_cfg["_pairs"] = [pair_name]
                tables.append(table_cfg)
                seen.add(key)
            else:
                # Table already exists, add this pair to its pair list
                for t in tables:
                    if t.get("source") == source and t.get("name") == name:
                        t["_pairs"].append(pair_name)
                        # Merge col_map columns
                        existing = t.get("_col_map_columns", set())
                        existing.update(table_cfg.get("_col_map_columns", set()))
                        t["_col_map_columns"] = existing
                        break

    return tables


def convert_old_to_unified(
    extract_config_path: Optional[str] = None,
    pairs_config_path: Optional[str] = None,
    extract_config: Optional[Dict] = None,
    pairs_config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Convert old two-file config format to unified format.

    Old format:
      - extract_config.json: {"tables": [{name, source, table, ...}]}
      - pairs_config.json: {"pairs": [{name, tables: {left, right}, col_map}]}

    New format:
      - pairs.json: {"pairs": {pair_name: {left, right, col_map, where_map, metadata}}}

    Args:
        extract_config_path: Path to old extract config (optional)
        pairs_config_path: Path to old pairs config (optional)
        extract_config: Extract config dict (optional)
        pairs_config: Pairs config dict (optional)

    Returns:
        Unified configuration dictionary
    """
    # Load configs if paths provided
    if extract_config_path:
        with open(extract_config_path) as f:
            extract_config = json.load(f)

    if pairs_config_path:
        with open(pairs_config_path) as f:
            pairs_config = json.load(f)

    if not pairs_config:
        raise ValueError("Must provide either pairs_config_path or pairs_config dict")

    # Build table lookup from extract config
    table_lookup = {}
    if extract_config and "tables" in extract_config:
        for table in extract_config["tables"]:
            source = table.get("source", "")
            name = table.get("name", "")
            key = (source, name)
            table_lookup[key] = table

    # Convert pairs
    unified = {"pairs": {}}

    for old_pair in pairs_config.get("pairs", []):
        pair_name = old_pair["name"]
        tables = old_pair.get("tables", {})

        # Extract left and right from old format
        # Old format can be: {"left": {...}, "right": {...}} OR {"pcds": {...}, "aws": {...}}
        sources = list(tables.keys())
        if len(sources) < 2:
            continue  # Skip pairs without 2 tables

        # Try standard left/right keys first
        if "left" in tables and "right" in tables:
            left_source = "pcds"
            right_source = "aws"
            left_name = tables["left"]["table_name"]
            right_name = tables["right"]["table_name"]
        else:
            # Use first two sources as left/right
            left_source = sources[0]
            right_source = sources[1]
            left_name = tables[left_source]["table_name"]
            right_name = tables[right_source]["table_name"]

        # Try to find full table configs by matching source and base name
        # The pair name often matches the table name in extract config
        left_cfg = None
        right_cfg = None

        for source, name in table_lookup.keys():
            if source.lower() in (left_source.lower(), 'pcds', 'oracle') and name == pair_name:
                left_cfg = table_lookup[(source, name)]
            if source.lower() == right_source.lower() and name == pair_name:
                right_cfg = table_lookup[(source, name)]

        # Fallback to minimal config if not found
        if not left_cfg:
            left_cfg = {
                "name": pair_name,  # Use pair name as base name
                "table": left_name.replace(f"{left_source}_", "").replace(f"{left_source.lower()}_", "").upper(),
                "source": left_source if left_source in ('pcds', 'oracle', 'aws') else 'pcds',
                "date_col": tables[left_source].get("date_col", "RPT_DT")
            }

        if not right_cfg:
            right_cfg = {
                "name": pair_name,  # Use pair name as base name
                "table": right_name.replace(f"{right_source}_", "").replace(f"{right_source.lower()}_", ""),
                "source": right_source if right_source in ('pcds', 'oracle', 'aws') else 'aws',
                "date_col": tables[right_source].get("date_col", "rpt_dt")
            }

        unified["pairs"][pair_name] = {
            "left": left_cfg,
            "right": right_cfg,
            "col_map": old_pair.get("col_map", {}),
            "where_map": {},
            "time_map": {
                "row": {"left": "—", "right": "—"},
                "col": {"left": "—", "right": "—"}
            },
            "comment_map": {
                "row": {"left": "", "right": ""},
                "col": {"left": "", "right": ""}
            },
            "diff_map": {}
        }

    # Add global metadata
    if "metadata" not in unified:
        unified["metadata"] = {
            "title": "Row Count Comparison",
            "subtitle": "updates every Thursday",
            "col_title": "Column Statistics Comparison",
            "col_subtitle": "Statistical validation report"
        }

    return unified


def get_pair_where_map(config: Dict[str, Any], pair_name: str) -> Optional[Dict[str, Any]]:
    """
    Get where_map for a specific pair from unified config.

    Args:
        config: Unified configuration dictionary
        pair_name: Name of the pair

    Returns:
        where_map dictionary or None if not set
    """
    return config.get("pairs", {}).get(pair_name, {}).get("where_map")


def set_pair_where_map(
    config: Dict[str, Any],
    pair_name: str,
    where_left: str,
    where_right: str
) -> None:
    """
    Set where_map for a specific pair in unified config.

    Stores WHERE statements (not date arrays) for manual editing.
    Date arrays are stored in database only.

    Modifies config in place and updates timestamp.

    Args:
        config: Unified configuration dictionary
        pair_name: Name of the pair
        where_left: WHERE clause for left table
        where_right: WHERE clause for right table
    """
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")

    config["pairs"][pair_name]["where_map"] = {
        "left": where_left,
        "right": where_right
    }


def get_pair_col_map(config: Dict[str, Any], pair_name: str) -> Dict[str, str]:
    """
    Get col_map for a specific pair from unified config.

    Args:
        config: Unified configuration dictionary
        pair_name: Name of the pair

    Returns:
        col_map dictionary (left -> right column name mapping)
    """
    return config.get("pairs", {}).get(pair_name, {}).get("col_map", {})


def set_pair_col_map(config: Dict[str, Any], pair_name: str, col_map: Dict[str, str]) -> None:
    """
    Set col_map for a specific pair in unified config.

    Modifies config in place.

    Args:
        config: Unified configuration dictionary
        pair_name: Name of the pair
        col_map: Dictionary mapping left column names to right column names
    """
    if pair_name not in config.get("pairs", {}):
        raise ValueError(f"Pair '{pair_name}' not found in config")

    config["pairs"][pair_name]["col_map"] = col_map
