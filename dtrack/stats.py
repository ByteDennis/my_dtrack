"""Statistics computation for column-level analysis"""

import json
from collections import Counter
from statistics import mean, stdev
from typing import List, Dict, Optional
import pandas as pd


def normalize_value(value: str) -> str:
    """
    Normalize a string value for consistent comparison.

    Handles:
    - Whitespace trimming: "xxx " → "xxx"
    - Date normalization: "04MAR2026:00:00:00" → "2026-03-04"
    - Numeric normalization: "0.440" → "0.44"

    Args:
        value: String value to normalize

    Returns:
        Normalized string
    """
    if value is None or value == '':
        return ''

    # Strip whitespace first
    value = str(value).strip()

    if not value:
        return ''

    # Try to parse as date
    try:
        from .date_utils import parse_date
        normalized_date = parse_date(value)
        return normalized_date
    except:
        pass

    # Try to parse as number and normalize
    try:
        num = float(value)
        # Format to remove trailing zeros
        if '.' in str(value) or 'e' in str(value).lower():
            # Convert to float and format
            formatted = f'{num:.10f}'.rstrip('0').rstrip('.')
            return formatted
        else:
            # Integer-like, but might have been "5.0"
            if num == int(num):
                return str(int(num))
            else:
                return f'{num:.10f}'.rstrip('0').rstrip('.')
    except:
        pass

    # Return trimmed string
    return value


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
        mean_val = str(mean(non_missing))
        min_val = str(min(non_missing))
        max_val = str(max(non_missing))
        std_val = str(stdev(non_missing)) if len(non_missing) > 1 else "0.0"
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
    Compute statistics for a categorical column with normalization.

    mean/std/min/max are computed in a *category-aware* way to defeat the
    permutation pitfall: any reordering of counts across categories must
    produce different stats.

    For each (cat, count) pair, sort by UPPER(cat) and assign ranks 1..k.
    Then with weights = counts:
        mean = SUM(count_i * rank_i) / SUM(count_i)
        std  = SQRT( SUM(count_i*(rank_i - mean)^2) / SUM(count_i) )
        min  = "{cat_at_rank_1}={count_at_rank_1}"
        max  = "{cat_at_rank_k}={count_at_rank_k}"
    The UPPER ordering matches the SQL builders for cross-engine consistency.
    """
    n_total = len(values)

    non_missing_raw = [v for v in values if v and str(v).strip()]
    non_missing = [normalize_value(v) for v in non_missing_raw if normalize_value(v)]

    n_missing = n_total - len(non_missing)
    n_unique = len(set(non_missing))

    if non_missing:
        counter = Counter(non_missing)
        # Sort by UPPER for cross-engine consistency; raw value tie-breaks.
        sorted_pairs = sorted(counter.items(), key=lambda kv: (kv[0].upper(), kv[0]))
        counts = [c for _, c in sorted_pairs]
        ranks = list(range(1, len(sorted_pairs) + 1))
        total_w = sum(counts)
        if total_w > 0:
            wmean = sum(c * r for c, r in zip(counts, ranks)) / total_w
            wvar = sum(c * (r - wmean) ** 2 for c, r in zip(counts, ranks)) / total_w
            mean_val = str(wmean)
            std_val = str(wvar ** 0.5)
        else:
            mean_val = None
            std_val = None

        first_cat, first_count = sorted_pairs[0]
        last_cat, last_count = sorted_pairs[-1]
        min_val = f"{first_cat}={first_count}"
        max_val = f"{last_cat}={last_count}"

        top_10 = [{"value": v, "count": c} for v, c in counter.most_common(10)]
        top_10_json = json.dumps(top_10)
    else:
        mean_val = None
        std_val = None
        min_val = None
        max_val = None
        top_10_json = None

    return {
        "n_total": n_total,
        "n_missing": n_missing,
        "n_unique": n_unique,
        "mean": mean_val,
        "std": std_val,
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
