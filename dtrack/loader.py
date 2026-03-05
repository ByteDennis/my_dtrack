"""Data loading functionality for row counts and column statistics"""

import csv
import os
from pathlib import Path
from typing import List, Tuple, Optional
from collections import defaultdict
import pandas as pd

from .date_utils import parse_date, bucket_date
from .db import (
    create_row_count_table,
    upsert_row_counts,
    insert_col_stats,
    update_metadata,
)
from .stats import compute_column_stats


# Column name aliases for automatic detection
DATE_ALIASES = {'rpg_dt', 'eff_dt', 'dt', 'date', 'run_date', 'snap_dt', 'snapshot_dt'}
COUNT_ALIASES = {'row_count', 'rowcount', 'count', 'cnt', 'rows'}


def detect_date_column(headers: List[str]) -> Optional[str]:
    """
    Detect which column is the date column based on common aliases.

    Args:
        headers: List of column headers

    Returns:
        Name of the date column, or None if not found
    """
    for header in headers:
        if header.lower().strip() in DATE_ALIASES:
            return header
    return None


def detect_count_column(headers: List[str]) -> Optional[str]:
    """
    Detect which column is the count column based on common aliases.

    Args:
        headers: List of column headers

    Returns:
        Name of the count column, or None if not found
    """
    for header in headers:
        if header.lower().strip() in COUNT_ALIASES:
            return header
    return None


def load_row_count_csv(
    csv_path: str,
    vintage: str = "day",
    date_col: Optional[str] = None,
    count_col: Optional[str] = None,
) -> List[Tuple[str, int]]:
    """
    Load row counts from a CSV file.

    Args:
        csv_path: Path to CSV file
        vintage: Time granularity (day, week, month, quarter, year)
        date_col: Name of date column (auto-detected if None)
        count_col: Name of count column (auto-detected if None)

    Returns:
        List of (date, row_count) tuples with dates bucketed by vintage
    """
    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        # Auto-detect columns if not provided
        if date_col is None:
            date_col = detect_date_column(headers)
            if date_col is None:
                date_col = headers[0]  # Default to first column

        if count_col is None:
            count_col = detect_count_column(headers)
            if count_col is None:
                count_col = headers[1]  # Default to second column

        # Read and aggregate data
        data = defaultdict(int)
        for row in reader:
            date_str = row[date_col].strip()
            count_str = row[count_col].strip()

            # Parse date
            try:
                dt = parse_date(date_str)
            except ValueError as e:
                print(f"Warning: Skipping row with invalid date '{date_str}': {e}")
                continue

            # Bucket date by vintage
            dt_bucketed = bucket_date(dt, vintage)

            # Parse count
            try:
                count = int(count_str)
            except ValueError:
                print(f"Warning: Skipping row with invalid count '{count_str}'")
                continue

            # Aggregate counts for the same bucket
            data[dt_bucketed] += count

    # Convert to sorted list
    return sorted(data.items())


def load_row_counts(
    db_path: str,
    file_or_folder: str,
    table_name: str,
    mode: str = "upsert",
    vintage: str = "day",
    source: Optional[str] = None,
    db_name: Optional[str] = None,
    source_table: Optional[str] = None,
    date_col: Optional[str] = None,
) -> None:
    """
    Load row counts from CSV file(s) into the database.

    Args:
        db_path: Path to SQLite database
        file_or_folder: Path to CSV file or folder containing CSV files
        table_name: Name of the table to create/update
        mode: Load mode (replace, append, upsert)
        vintage: Time granularity
        source: Data source identifier (aws, pcds, oracle)
        db_name: Database or service name
        source_table: Original table name
        date_col: Date column name (auto-detected if None)
    """
    path = Path(file_or_folder)

    # Get list of CSV files
    if path.is_file():
        csv_files = [path]
    elif path.is_dir():
        csv_files = list(path.glob("*.csv"))
    else:
        raise ValueError(f"Path does not exist: {file_or_folder}")

    # Create table
    create_row_count_table(db_path, table_name)

    # Load data from all CSV files
    all_data = defaultdict(int)
    for csv_file in csv_files:
        data = load_row_count_csv(
            csv_path=str(csv_file),
            vintage=vintage,
            date_col=date_col,
        )
        # Aggregate data
        for dt, count in data:
            all_data[dt] += count

    # Convert to list
    data_list = sorted(all_data.items())

    # Insert data based on mode
    if mode == "replace":
        # Drop and recreate table
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.close()
        create_row_count_table(db_path, table_name)
        upsert_row_counts(db_path, table_name, data_list)
    elif mode == "upsert":
        upsert_row_counts(db_path, table_name, data_list)
    elif mode == "append":
        from .db import insert_row_counts
        insert_row_counts(db_path, table_name, data_list)
    else:
        raise ValueError(f"Invalid mode: {mode}")

    # Update metadata
    total_count = sum(count for _, count in data_list)
    update_metadata(db_path, {
        "table_name": table_name,
        "source": source,
        "db": db_name,
        "source_table": source_table,
        "date_var": date_col,
        "source_file": str(file_or_folder),
        "row_count_total": total_count,
        "load_mode": mode,
        "vintage": vintage,
        "data_type": "row",
    })


def load_column_data_csv(
    csv_path: str,
    date_col: str,
    vintage: str = "day",
    columns: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load column data from a CSV file into a pandas DataFrame.

    Args:
        csv_path: Path to CSV file
        date_col: Name of the date column
        vintage: Time granularity
        columns: List of columns to include (None = all columns)
        from_date: Start date filter (YYYY-MM-DD)
        to_date: End date filter (YYYY-MM-DD)

    Returns:
        Pandas DataFrame with data
    """
    # Read CSV
    df = pd.read_csv(csv_path)

    # Parse and bucket dates
    df[date_col] = df[date_col].apply(lambda x: parse_date(str(x)))
    df[date_col] = df[date_col].apply(lambda x: bucket_date(x, vintage))

    # Filter by date range
    if from_date:
        df = df[df[date_col] >= from_date]
    if to_date:
        df = df[df[date_col] <= to_date]

    # Select columns
    if columns:
        df = df[[date_col] + columns]

    return df


def load_column_data(
    db_path: str,
    file_path: str,
    source_table: str,
    date_col: str,
    columns: Optional[List[str]] = None,
    mode: str = "upsert",
    vintage: str = "day",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    source: Optional[str] = None,
    db_name: Optional[str] = None,
) -> None:
    """
    Load column statistics from a CSV file into the database.

    Args:
        db_path: Path to SQLite database
        file_path: Path to CSV file
        source_table: Name of the source table
        date_col: Name of the date column
        columns: List of columns to analyze (None = all except date_col)
        mode: Load mode (replace, upsert)
        vintage: Time granularity
        from_date: Start date filter
        to_date: End date filter
        source: Data source identifier
        db_name: Database or service name
    """
    # Load data
    df = load_column_data_csv(
        csv_path=file_path,
        date_col=date_col,
        vintage=vintage,
        columns=columns,
        from_date=from_date,
        to_date=to_date,
    )

    # Compute statistics
    stats = compute_column_stats(
        df=df,
        source_table=source_table,
        date_col=date_col,
        columns=columns,
    )

    # Insert stats
    if mode == "replace":
        # Delete existing stats for this source_table
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM _col_stats WHERE source_table = ?", (source_table,))
        conn.commit()
        conn.close()

    insert_col_stats(db_path, stats)

    # Update metadata
    update_metadata(db_path, {
        "table_name": source_table,
        "source": source,
        "db": db_name,
        "source_table": source_table,
        "date_var": date_col,
        "source_file": file_path,
        "load_mode": mode,
        "vintage": vintage,
        "data_type": "col",
    })


# Column name aliases for pre-computed stats CSV
_COL_STATS_ALIASES = {
    'mean': ['mean', 'col_avg', 'avg'],
    'std': ['std', 'col_std', 'stddev'],
    'min_val': ['min_val', 'col_min', 'min'],
    'max_val': ['max_val', 'col_max', 'max'],
    'top_10': ['top_10', 'col_freq', 'freq', 'top10'],
}


def _resolve_col(headers, field, aliases):
    """Find the actual CSV header for a field using aliases."""
    for alias in aliases:
        for h in headers:
            if h.lower().strip() == alias.lower():
                return h
    return None


def load_precomputed_col_stats(
    db_path: str,
    csv_path: str,
    table_name: str,
    mode: str = "upsert",
    source: Optional[str] = None,
    db_name: Optional[str] = None,
) -> int:
    """
    Load pre-computed column statistics from CSV into _col_stats table.

    Expected CSV columns (with alias support):
        column_name, dt, col_type, n_total, n_missing, n_unique,
        mean/col_avg, std/col_std, min_val/col_min, max_val/col_max,
        top_10/col_freq

    Args:
        db_path: Path to SQLite database
        csv_path: Path to CSV file with pre-computed stats
        table_name: Table name to associate stats with
        mode: Load mode (upsert or replace)
        source: Data source identifier
        db_name: Database name

    Returns:
        Number of rows loaded
    """
    import sqlite3

    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        # Resolve aliased column names
        col_map = {}
        for field, aliases in _COL_STATS_ALIASES.items():
            resolved = _resolve_col(headers, field, aliases)
            if resolved:
                col_map[field] = resolved

        if mode == "replace":
            conn = sqlite3.connect(db_path)
            conn.execute("DELETE FROM _col_stats WHERE source_table = ?", (table_name,))
            conn.commit()
            conn.close()

        stats = []
        for row in reader:
            def _get(field, default=None):
                mapped = col_map.get(field)
                if mapped and row.get(mapped):
                    return row[mapped]
                if row.get(field):
                    return row[field]
                return default

            stat = {
                "source_table": table_name,
                "column_name": row.get("column_name", ""),
                "dt": row.get("dt", ""),
                "col_type": row.get("col_type", "categorical"),
                "n_total": int(_get("n_total", 0) or 0),
                "n_missing": int(_get("n_missing", 0) or 0),
                "n_unique": int(_get("n_unique", 0) or 0),
                "mean": float(_get("mean")) if _get("mean") else None,
                "std": float(_get("std")) if _get("std") else None,
                "min_val": _get("min_val"),
                "max_val": _get("max_val"),
                "top_10": _get("top_10"),
            }
            stats.append(stat)

    insert_col_stats(db_path, stats)

    # Update metadata
    update_metadata(db_path, {
        "table_name": table_name,
        "source": source,
        "db": db_name,
        "source_table": table_name,
        "source_file": csv_path,
        "load_mode": mode,
        "data_type": "col",
    })

    return len(stats)
