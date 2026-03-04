"""JSON configuration parsing for table pairs"""

import json
from typing import Dict, List, Any
from pathlib import Path


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

        if "file" not in table_config:
            raise ValueError(
                f"Pair '{pair['name']}', table '{source_name}': missing 'file' field"
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
        "source": None,
        "db": None,
        "source_table": None,
        "date_col": None,
    }

    # Create new dict with defaults, then override with provided values
    result = defaults.copy()
    result.update(table_config)
    return result
