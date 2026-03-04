"""Load table pairs from JSON configuration"""

from typing import Dict, List, Any
from pathlib import Path

from .config import load_pairs_config_from_file, apply_table_defaults
from .loader import load_row_counts, load_column_data
from .db import register_table_pair


def load_pair_row_counts(
    db_path: str,
    pair_config: Dict[str, Any]
) -> None:
    """
    Load row counts for a table pair.

    Args:
        db_path: Path to SQLite database
        pair_config: Pair configuration dictionary
    """
    pair_name = pair_config["name"]
    tables = pair_config["tables"]
    col_map = pair_config.get("col_map", {})

    # Get table names for the pair
    table_sources = list(tables.keys())
    if len(table_sources) < 2:
        raise ValueError(f"Pair '{pair_name}' must have at least 2 tables")

    table_left_key = table_sources[0]
    table_right_key = table_sources[1]

    table_left_config = apply_table_defaults(tables[table_left_key])
    table_right_config = apply_table_defaults(tables[table_right_key])

    # Load left table
    print(f"Loading {table_left_key} table: {table_left_config['table_name']}")
    load_row_counts(
        db_path=db_path,
        file_or_folder=table_left_config["file"],
        table_name=table_left_config["table_name"],
        mode=table_left_config["mode"],
        vintage=table_left_config["vintage"],
        source=table_left_config.get("source", table_left_key),
        db_name=table_left_config.get("db"),
        source_table=table_left_config.get("source_table"),
        date_col=table_left_config.get("date_col"),
    )

    # Load right table
    print(f"Loading {table_right_key} table: {table_right_config['table_name']}")
    load_row_counts(
        db_path=db_path,
        file_or_folder=table_right_config["file"],
        table_name=table_right_config["table_name"],
        mode=table_right_config["mode"],
        vintage=table_right_config["vintage"],
        source=table_right_config.get("source", table_right_key),
        db_name=table_right_config.get("db"),
        source_table=table_right_config.get("source_table"),
        date_col=table_right_config.get("date_col"),
    )

    # Register the pair with column mappings
    print(f"Registering pair: {pair_name}")
    register_table_pair(
        db_path=db_path,
        pair_name=pair_name,
        table_left=table_left_config["table_name"],
        table_right=table_right_config["table_name"],
        source_left=table_left_config.get("source", table_left_key),
        source_right=table_right_config.get("source", table_right_key),
        col_mappings=col_map,
    )


def load_pair_column_data(
    db_path: str,
    pair_config: Dict[str, Any]
) -> None:
    """
    Load column statistics for a table pair.

    Args:
        db_path: Path to SQLite database
        pair_config: Pair configuration dictionary
    """
    pair_name = pair_config["name"]
    tables = pair_config["tables"]
    col_map = pair_config.get("col_map", {})

    # Get table names for the pair
    table_sources = list(tables.keys())
    if len(table_sources) < 2:
        raise ValueError(f"Pair '{pair_name}' must have at least 2 tables")

    table_left_key = table_sources[0]
    table_right_key = table_sources[1]

    table_left_config = apply_table_defaults(tables[table_left_key])
    table_right_config = apply_table_defaults(tables[table_right_key])

    # Validate that date_col is specified
    if not table_left_config.get("date_col"):
        raise ValueError(
            f"Pair '{pair_name}', table '{table_left_key}': 'date_col' is required for column statistics"
        )
    if not table_right_config.get("date_col"):
        raise ValueError(
            f"Pair '{pair_name}', table '{table_right_key}': 'date_col' is required for column statistics"
        )

    # Load left table
    print(f"Loading {table_left_key} column stats: {table_left_config['table_name']}")
    load_column_data(
        db_path=db_path,
        file_path=table_left_config["file"],
        source_table=table_left_config["table_name"],
        date_col=table_left_config["date_col"],
        columns=None,  # Analyze all columns
        mode=table_left_config["mode"],
        vintage=table_left_config["vintage"],
        from_date=table_left_config.get("from_date"),
        to_date=table_left_config.get("to_date"),
        source=table_left_config.get("source", table_left_key),
        db_name=table_left_config.get("db"),
    )

    # Load right table
    print(f"Loading {table_right_key} column stats: {table_right_config['table_name']}")
    load_column_data(
        db_path=db_path,
        file_path=table_right_config["file"],
        source_table=table_right_config["table_name"],
        date_col=table_right_config["date_col"],
        columns=None,  # Analyze all columns
        mode=table_right_config["mode"],
        vintage=table_right_config["vintage"],
        from_date=table_right_config.get("from_date"),
        to_date=table_right_config.get("to_date"),
        source=table_right_config.get("source", table_right_key),
        db_name=table_right_config.get("db"),
    )

    # Register the pair with column mappings
    print(f"Registering pair: {pair_name}")
    register_table_pair(
        db_path=db_path,
        pair_name=pair_name,
        table_left=table_left_config["table_name"],
        table_right=table_right_config["table_name"],
        source_left=table_left_config.get("source", table_left_key),
        source_right=table_right_config.get("source", table_right_key),
        col_mappings=col_map,
    )


def load_map(
    db_path: str,
    config_path: str,
    data_type: str = "row"
) -> None:
    """
    Load table pairs from JSON configuration.

    Args:
        db_path: Path to SQLite database
        config_path: Path to JSON configuration file
        data_type: Type of data to load ('row' or 'col')
    """
    # Load and validate config
    pairs = load_pairs_config_from_file(config_path)

    print(f"Loading {len(pairs)} table pair(s) from {config_path}")
    print(f"Data type: {data_type}")
    print()

    # Load each pair
    for pair in pairs:
        print(f"=" * 70)
        print(f"Processing pair: {pair['name']}")
        print(f"=" * 70)

        if data_type == "row":
            load_pair_row_counts(db_path, pair)
        elif data_type == "col":
            load_pair_column_data(db_path, pair)
        else:
            raise ValueError(f"Invalid data_type: {data_type}. Must be 'row' or 'col'")

        print(f"✓ Pair '{pair['name']}' loaded successfully")
        print()

    print(f"✓ All {len(pairs)} pair(s) loaded successfully")
