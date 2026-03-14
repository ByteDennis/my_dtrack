"""Comparison functionality for row counts and column statistics"""

import json
from typing import Dict, List, Tuple, Optional
from .db import get_row_counts, get_col_stats, get_table_pair, list_table_pairs
from .date_utils import parse_date


def _safe_int(val):
    """Parse a value to int, returning 0 for empty/None."""
    if val is None or val == '':
        return 0
    return int(float(str(val)))


def _safe_float(val):
    """Parse a value to float, returning None for empty/None."""
    if val is None or val == '':
        return None
    return float(str(val))


def _parse_top10(s):
    """Parse top_10 string (semicolon or JSON format) into {value: count} dict."""
    if not s or s == '[]':
        return {}
    s = str(s).strip()
    # JSON format: [{"value": "x", "count": 5}, ...]
    if s.startswith('['):
        try:
            entries = json.loads(s)
            return {str(e.get('value', '')): int(e.get('count', 0)) for e in entries}
        except (json.JSONDecodeError, TypeError):
            return {}
    # Semicolon format: x(5); y(3)
    result = {}
    for entry in s.split('; '):
        entry = entry.strip()
        if '(' in entry and entry.endswith(')'):
            val = entry[:entry.rfind('(')].strip()
            try:
                cnt = int(entry[entry.rfind('(') + 1:-1].strip())
            except ValueError:
                continue
            result[val] = cnt
    return result


def _compare_top10(left_str, right_str):
    """Compare two top_10 strings. Returns True if different."""
    left = _parse_top10(left_str)
    right = _parse_top10(right_str)
    return left != right


def parse_col_map_string(col_map_str: str) -> Dict[str, str]:
    """
    Parse column mapping string into dictionary.

    Args:
        col_map_str: String like "AMT=amount,STATUS=status"

    Returns:
        Dictionary mapping left columns to right columns
    """
    if not col_map_str:
        return {}

    mappings = {}
    for pair in col_map_str.split(','):
        pair = pair.strip()
        if '=' not in pair:
            continue
        left, right = pair.split('=', 1)
        mappings[left.strip()] = right.strip()

    return mappings


def get_column_mapping(
    db_path: str,
    table_left: str,
    table_right: str,
    pair_name: Optional[str] = None,
    col_map_override: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Get column mapping between two tables.

    Priority:
    1. col_map_override (from --col-map parameter)
    2. pair_name lookup in _table_pairs
    3. Auto-detect from _table_pairs by table names
    4. Empty dict (exact column name matching)

    Args:
        db_path: Path to SQLite database
        table_left: Left table name
        table_right: Right table name
        pair_name: Optional pair name to lookup
        col_map_override: Optional mapping dictionary to use instead

    Returns:
        Dictionary mapping left column names to right column names
    """
    # Priority 1: Override
    if col_map_override is not None:
        return col_map_override

    # Priority 2: Explicit pair name
    if pair_name:
        pair = get_table_pair(db_path, pair_name)
        if pair and pair.get('col_mappings'):
            return pair['col_mappings']

    # Priority 3: Auto-detect by table names
    pairs = list_table_pairs(db_path)
    for pair in pairs:
        if (pair['table_left'] == table_left and pair['table_right'] == table_right):
            return pair.get('col_mappings', {})
        if (pair['table_left'] == table_right and pair['table_right'] == table_left):
            # Reverse mapping if tables are swapped
            mappings = pair.get('col_mappings', {})
            return {v: k for k, v in mappings.items()}

    # Priority 4: Empty dict (exact matching)
    return {}


def compare_row_counts(
    db_path: str,
    table_left: str,
    table_right: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> Dict[str, List]:
    """
    Compare row counts between two tables.

    Args:
        db_path: Path to SQLite database
        table_left: Left table name
        table_right: Right table name
        from_date: Optional start date filter
        to_date: Optional end date filter

    Returns:
        Dictionary with:
        - only_left: [(date, count), ...]
        - only_right: [(date, count), ...]
        - matching: [(date, count), ...]
        - mismatched: [(date, count_left, count_right), ...]
        - summary: {date_range_left, date_range_right, total_left, total_right}
    """
    # Get row counts for both tables
    rows_left = get_row_counts(db_path, table_left, from_date, to_date)
    rows_right = get_row_counts(db_path, table_right, from_date, to_date)

    # Convert to dictionaries for easier lookup
    dict_left = {dt: count for dt, count in rows_left}
    dict_right = {dt: count for dt, count in rows_right}

    # Get all unique dates
    all_dates = set(dict_left.keys()) | set(dict_right.keys())

    # Categorize dates
    only_left = []
    only_right = []
    matching = []
    mismatched = []

    for dt in sorted(all_dates):
        if dt in dict_left and dt in dict_right:
            if dict_left[dt] == dict_right[dt]:
                matching.append((dt, dict_left[dt]))
            else:
                mismatched.append((dt, dict_left[dt], dict_right[dt]))
        elif dt in dict_left:
            only_left.append((dt, dict_left[dt]))
        else:
            only_right.append((dt, dict_right[dt]))

    # Calculate summary statistics
    summary = {
        "date_range_left": (
            min(dict_left.keys()) if dict_left else None,
            max(dict_left.keys()) if dict_left else None,
        ),
        "date_range_right": (
            min(dict_right.keys()) if dict_right else None,
            max(dict_right.keys()) if dict_right else None,
        ),
        "total_left": sum(dict_left.values()),
        "total_right": sum(dict_right.values()),
        "count_left": len(dict_left),
        "count_right": len(dict_right),
    }

    return {
        "only_left": only_left,
        "only_right": only_right,
        "matching": matching,
        "mismatched": mismatched,
        "summary": summary,
    }


def compare_column_stats(
    db_path: str,
    table_left: str,
    table_right: str,
    columns: Optional[List[str]] = None,
    col_mappings: Optional[Dict[str, str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    matched_dates: Optional[set] = None,
) -> Dict[str, List[Dict]]:
    """
    Compare column statistics between two tables.

    Args:
        db_path: Path to SQLite database
        table_left: Left table name
        table_right: Right table name
        columns: Optional list of columns to compare
        col_mappings: Optional column name mappings {left_col: right_col}
        from_date: Optional start date filter
        to_date: Optional end date filter
        matched_dates: Optional set of date strings to include (e.g. from row count matching)

    Returns:
        Dictionary mapping column names to list of comparison records
    """
    # Get stats for both tables
    stats_left = get_col_stats(
        db_path, table_left,
        from_date=from_date, to_date=to_date
    )
    stats_right = get_col_stats(
        db_path, table_right,
        from_date=from_date, to_date=to_date
    )

    # Organize stats by column and date (normalize dt to YYYY-MM-DD)
    def organize_stats(stats):
        organized = {}
        for stat in stats:
            col = stat['column_name']
            dt = parse_date(stat['dt']) if stat['dt'] else stat['dt']
            if col not in organized:
                organized[col] = {}
            organized[col][dt] = stat
        return organized

    left_by_col = organize_stats(stats_left)
    right_by_col = organize_stats(stats_right)

    # If no columns specified, use all columns from left table
    if columns is None:
        columns = list(left_by_col.keys())

    # If no mappings, use exact matching
    if col_mappings is None:
        col_mappings = {}

    result = {}

    for left_col in columns:
        # Get corresponding right column name
        right_col = col_mappings.get(left_col, left_col)

        if left_col not in left_by_col:
            continue
        if right_col not in right_by_col:
            continue

        left_dates = left_by_col[left_col]
        right_dates = right_by_col[right_col]

        # Compare for each date
        comparisons = []
        all_dates = set(left_dates.keys()) | set(right_dates.keys())
        if matched_dates is not None:
            all_dates = all_dates & matched_dates

        for dt in sorted(all_dates):
            if dt not in left_dates or dt not in right_dates:
                continue  # Skip dates not in both

            left_stat = left_dates[dt]
            right_stat = right_dates[dt]

            # Parse counts from strings
            l_total = _safe_int(left_stat["n_total"])
            r_total = _safe_int(right_stat["n_total"])
            l_missing = _safe_int(left_stat["n_missing"])
            r_missing = _safe_int(right_stat["n_missing"])
            l_unique = _safe_int(left_stat["n_unique"])
            r_unique = _safe_int(right_stat["n_unique"])

            comparison = {
                "dt": dt,
                "vintage_label": left_stat.get("vintage_label") or dt,
                "col_type": left_stat["col_type"],
                "left_col": left_col,
                "right_col": right_col,
                # Counts
                "n_total_left": l_total,
                "n_total_right": r_total,
                "n_total_diff": r_total - l_total,
                "n_missing_left": l_missing,
                "n_missing_right": r_missing,
                "n_missing_diff": r_missing - l_missing,
                "n_unique_left": l_unique,
                "n_unique_right": r_unique,
                "n_unique_diff": r_unique - l_unique,
            }

            # Numeric stats
            if left_stat["col_type"] == "numeric":
                l_mean = _safe_float(left_stat["mean"])
                r_mean = _safe_float(right_stat["mean"])
                l_std = _safe_float(left_stat["std"])
                r_std = _safe_float(right_stat["std"])
                comparison.update({
                    "mean_left": l_mean,
                    "mean_right": r_mean,
                    "mean_diff": (
                        r_mean - l_mean
                        if l_mean is not None and r_mean is not None
                        else None
                    ),
                    "std_left": l_std,
                    "std_right": r_std,
                    "std_diff": (
                        r_std - l_std
                        if l_std is not None and r_std is not None
                        else None
                    ),
                    "min_left": left_stat["min_val"],
                    "min_right": right_stat["min_val"],
                    "max_left": left_stat["max_val"],
                    "max_right": right_stat["max_val"],
                })
            else:
                # Categorical stats
                comparison.update({
                    "min_left": left_stat["min_val"],
                    "min_right": right_stat["min_val"],
                    "max_left": left_stat["max_val"],
                    "max_right": right_stat["max_val"],
                    "top_10_left": left_stat["top_10"],
                    "top_10_right": right_stat["top_10"],
                })

            comparisons.append(comparison)

        if comparisons:
            result[left_col] = comparisons

    return result


def _has_col_differences(comp: Dict) -> bool:
    """Check if a column comparison has any statistical differences.

    Args:
        comp: Single comparison record from compare_column_stats

    Returns:
        True if any metric shows a difference, False otherwise
    """
    # Check count differences
    if comp.get('n_total_diff', 0) != 0:
        return True
    if comp.get('n_missing_diff', 0) != 0:
        return True
    if comp.get('n_unique_diff', 0) != 0:
        return True

    # Check numeric differences (use threshold for floating point)
    mean_diff = comp.get('mean_diff')
    if mean_diff is not None and abs(mean_diff) > 0.01:
        return True

    std_diff = comp.get('std_diff')
    if std_diff is not None and abs(std_diff) > 0.01:
        return True

    return False
