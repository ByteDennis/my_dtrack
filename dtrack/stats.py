"""Statistics computation for column-level analysis"""

import json
from collections import Counter
from statistics import mean, stdev
from typing import List, Dict, Optional
import pandas as pd


def detect_column_type(values: List[str]) -> str:
    """
    Detect if a column is numeric or categorical.

    A column is numeric if >90% of non-missing values can be parsed as numbers.

    Args:
        values: List of string values (may contain None or empty strings)

    Returns:
        "numeric" or "categorical"
    """
    non_missing = [v for v in values if v and str(v).strip()]

    if not non_missing:
        return "categorical"

    numeric_count = 0
    for v in non_missing:
        try:
            float(v)
            numeric_count += 1
        except (ValueError, TypeError):
            pass

    threshold = 0.9
    if numeric_count / len(non_missing) > threshold:
        return "numeric"
    else:
        return "categorical"


def compute_numeric_stats(values: List[str]) -> Dict:
    """
    Compute statistics for a numeric column.

    Args:
        values: List of string values

    Returns:
        Dictionary with statistics: n_total, n_missing, n_unique, mean, std, min_val, max_val
    """
    n_total = len(values)

    # Filter missing values
    non_missing = []
    for v in values:
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        try:
            non_missing.append(float(v))
        except (ValueError, TypeError):
            pass

    n_missing = n_total - len(non_missing)
    n_unique = len(set(non_missing))

    if non_missing:
        mean_val = mean(non_missing)
        min_val = str(min(non_missing))
        max_val = str(max(non_missing))
        std_val = stdev(non_missing) if len(non_missing) > 1 else 0.0
    else:
        mean_val = None
        min_val = None
        max_val = None
        std_val = None

    return {
        "n_total": n_total,
        "n_missing": n_missing,
        "n_unique": n_unique,
        "mean": mean_val,
        "std": std_val,
        "min_val": min_val,
        "max_val": max_val,
        "top_10": None,
    }


def compute_categorical_stats(values: List[str]) -> Dict:
    """
    Compute statistics for a categorical column.

    Args:
        values: List of string values

    Returns:
        Dictionary with statistics: n_total, n_missing, n_unique, min_val, max_val, top_10
    """
    n_total = len(values)

    # Filter missing values
    non_missing = [v for v in values if v and str(v).strip()]
    n_missing = n_total - len(non_missing)
    n_unique = len(set(non_missing))

    if non_missing:
        min_val = min(non_missing)
        max_val = max(non_missing)

        # Compute top 10 frequency
        counter = Counter(non_missing)
        top_10 = [
            {"value": value, "count": count}
            for value, count in counter.most_common(10)
        ]
        top_10_json = json.dumps(top_10)
    else:
        min_val = None
        max_val = None
        top_10_json = None

    return {
        "n_total": n_total,
        "n_missing": n_missing,
        "n_unique": n_unique,
        "mean": None,
        "std": None,
        "min_val": min_val,
        "max_val": max_val,
        "top_10": top_10_json,
    }


def compute_column_stats(
    df: pd.DataFrame,
    source_table: str,
    date_col: str,
    columns: Optional[List[str]] = None
) -> List[Dict]:
    """
    Compute statistics for columns in a dataframe, grouped by date.

    Args:
        df: Pandas dataframe with data
        source_table: Name of the source table
        date_col: Name of the date column
        columns: List of columns to analyze (default: all except date_col)

    Returns:
        List of stat dictionaries suitable for insert_col_stats
    """
    if columns is None:
        columns = [col for col in df.columns if col != date_col]

    stats_list = []

    # Group by date
    for dt, group in df.groupby(date_col):
        for col in columns:
            values = group[col].astype(str).tolist()

            # Detect column type
            col_type = detect_column_type(values)

            # Compute stats based on type
            if col_type == "numeric":
                stats = compute_numeric_stats(values)
            else:
                stats = compute_categorical_stats(values)

            # Add metadata
            stats["source_table"] = source_table
            stats["column_name"] = col
            stats["dt"] = str(dt)
            stats["col_type"] = col_type

            stats_list.append(stats)

    return stats_list
