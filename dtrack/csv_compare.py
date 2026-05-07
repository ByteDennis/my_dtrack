"""String-exact CSV-to-CSV comparison by primary key.

Both CSVs are read with `dtype=str, keep_default_na=False` so empty cells stay
as `""` rather than NaN, and `"10"` is not equal to `"10.0"`. Rows are merged
on a user-supplied primary key (one or more columns), then each user-supplied
compare column is checked for equality on the inner join. Per-column counts
plus the first N example mismatches are returned.
"""

from typing import List, Dict, Any
import pandas as pd


def read_csv_as_str(path_or_buf) -> pd.DataFrame:
    """Read a CSV with all values as strings (no NaN, no type inference)."""
    return pd.read_csv(path_or_buf, dtype=str, keep_default_na=False)


def compare_csvs(left_df: pd.DataFrame, right_df: pd.DataFrame,
                 pk_cols: List[str], compare_cols: List[str],
                 n_examples: int = 10) -> Dict[str, Any]:
    """Compare two DataFrames by primary key, return summary + per-column diffs.

    All values are coerced to str via .astype(str). pk_cols must exist on both
    sides. compare_cols missing on either side are reported as skipped.

    Returns:
        {
          "summary": {matched, only_left, only_right, total_mismatches, cols_with_mismatches},
          "only_left_examples":  [{pk: {...}}],
          "only_right_examples": [{pk: {...}}],
          "columns": [
            {"name": str, "skipped": bool, "reason"?: str, "n_unmatched": int,
             "examples": [{"pk": {...}, "left": str, "right": str}, ...]},
            ...
          ],
        }
    """
    if not pk_cols:
        raise ValueError("At least one primary-key column is required.")
    for c in pk_cols:
        if c not in left_df.columns:
            raise ValueError(f"PK column {c!r} missing on left side.")
        if c not in right_df.columns:
            raise ValueError(f"PK column {c!r} missing on right side.")

    left = left_df.copy()
    right = right_df.copy()
    for col in left.columns:
        left[col] = left[col].astype(str)
    for col in right.columns:
        right[col] = right[col].astype(str)

    left = left.sort_values(pk_cols).reset_index(drop=True)
    right = right.sort_values(pk_cols).reset_index(drop=True)

    merged = left.merge(
        right, how='outer', on=pk_cols, suffixes=('__L', '__R'), indicator=True,
    )

    only_left_mask  = merged['_merge'] == 'left_only'
    only_right_mask = merged['_merge'] == 'right_only'
    both_mask       = merged['_merge'] == 'both'

    n_only_left  = int(only_left_mask.sum())
    n_only_right = int(only_right_mask.sum())
    n_matched    = int(both_mask.sum())

    def _pk_dict(row) -> Dict[str, str]:
        return {c: str(row[c]) for c in pk_cols}

    only_left_examples = [
        _pk_dict(r) for _, r in merged[only_left_mask].head(n_examples).iterrows()
    ]
    only_right_examples = [
        _pk_dict(r) for _, r in merged[only_right_mask].head(n_examples).iterrows()
    ]

    columns_out: List[Dict[str, Any]] = []
    total_mismatches = 0
    cols_with_mismatches = 0

    inner = merged[both_mask]

    for col in compare_cols:
        if col in pk_cols:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "is a primary key", "n_unmatched": 0,
                                "examples": []})
            continue
        in_left  = col in left_df.columns
        in_right = col in right_df.columns
        if not in_left and not in_right:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "missing on both sides",
                                "n_unmatched": 0, "examples": []})
            continue
        if not in_left:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "missing on left side",
                                "n_unmatched": 0, "examples": []})
            continue
        if not in_right:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "missing on right side",
                                "n_unmatched": 0, "examples": []})
            continue

        lcol = f"{col}__L"
        rcol = f"{col}__R"
        diff = inner[lcol] != inner[rcol]
        n_unmatched = int(diff.sum())
        if n_unmatched:
            cols_with_mismatches += 1
            total_mismatches += n_unmatched

        examples = []
        for _, r in inner[diff].head(n_examples).iterrows():
            examples.append({
                "pk":    _pk_dict(r),
                "left":  str(r[lcol]),
                "right": str(r[rcol]),
            })

        columns_out.append({"name": col, "skipped": False,
                            "n_unmatched": n_unmatched, "examples": examples})

    return {
        "summary": {
            "matched": n_matched,
            "only_left": n_only_left,
            "only_right": n_only_right,
            "total_mismatches": total_mismatches,
            "cols_with_mismatches": cols_with_mismatches,
        },
        "only_left_examples": only_left_examples,
        "only_right_examples": only_right_examples,
        "columns": columns_out,
    }
