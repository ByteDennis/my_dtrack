
from datetime import datetime
import re
from typing import List, Dict, Any, Tuple
import pandas as pd

_DATE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"),                                 "%Y-%m-%d"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}"),             "%Y-%m-%d %H:%M:%S"),
    (re.compile(r"^\d{4}/\d{2}/\d{2}$"),                                 "%Y/%m/%d"),
    (re.compile(r"^\d{8}$"),                                             "%Y%m%d"),
    (re.compile(r"^\d{1,2}[A-Z]{3}\d{4}$", re.IGNORECASE),               "%d%b%Y"),
    (re.compile(r"^\d{1,2}-[A-Z]{3}-\d{4}$", re.IGNORECASE),             "%d-%b-%Y"),
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"),                             "%m/%d/%Y"),
]

# >>> Return the strptime format that parses every non-empty sample, or "" <<< #
def _detect_date_format(samples: List[str]) -> str:
    non_empty = [s for s in samples if s.strip()]
    if not non_empty:
        return ""
    for pattern, fmt in _DATE_PATTERNS:
        if not all(pattern.match(s.upper() if "%b" in fmt else s) for s in non_empty):
            continue
        try:
            for s in non_empty:
                src = s.upper() if "%b" in fmt else s
                datetime.strptime(src[:19] if "%H" in fmt else src, fmt)
            return fmt
        except ValueError:
            continue
    return ""

# >>> Parse `series` with strptime(`fmt`) and rewrite to ISO YYYY-MM-DD <<< #
def _normalize_dates(series: pd.Series, fmt: str) -> pd.Series:
    def _norm(s: str) -> str:
        if not s.strip():
            return s
        try:
            src = s.upper() if "%b" in fmt else s
            d = datetime.strptime(src[:19] if "%H" in fmt else src, fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            return s
    return series.map(_norm)

# >>> Auto-normalize two PK series to ISO if both look like dates <<< #
def _maybe_normalize_pk_pair(left: pd.Series,
                             right: pd.Series) -> Tuple[pd.Series, pd.Series, str]:
    sample_n = 50
    lfmt = _detect_date_format(left.head(sample_n).tolist())
    rfmt = _detect_date_format(right.head(sample_n).tolist())
    if not lfmt or not rfmt:
        return left, right, ""
    if lfmt == "%Y-%m-%d" and rfmt == "%Y-%m-%d":
        return left, right, ""
    return (_normalize_dates(left, lfmt),
            _normalize_dates(right, rfmt),
            f"date: left {lfmt!r}, right {rfmt!r} → ISO")

# >>> Read a CSV with all values as strings (no NaN, no type inference) <<< #
def read_csv_as_str(path_or_buf) -> pd.DataFrame:
    return pd.read_csv(path_or_buf, dtype=str, keep_default_na=False)

# >>> Compare two DataFrames by primary key, return summary + per-column diffs <<< #
def compare_csvs(left_df: pd.DataFrame, right_df: pd.DataFrame,
                 pk_cols: List[str], compare_cols: List[str],
                 n_examples: int = 10, *,
                 pk_cols_right: List[str] = None,
                 compare_cols_right: List[str] = None,
                 on_log=None) -> Dict[str, Any]:
    log = on_log or (lambda *_a, **_kw: None)
    log(f"left:  {len(left_df):,} rows × {len(left_df.columns)} cols")
    log(f"right: {len(right_df):,} rows × {len(right_df.columns)} cols")

    if not pk_cols:
        raise ValueError("At least one primary-key column is required.")

    if pk_cols_right is None:
        pk_cols_right = list(pk_cols)
    if len(pk_cols_right) != len(pk_cols):
        raise ValueError(
            f"pk_cols and pk_cols_right have different lengths "
            f"({len(pk_cols)} vs {len(pk_cols_right)})."
        )
    for c in pk_cols:
        if c not in left_df.columns:
            raise ValueError(f"PK column {c!r} missing on left side.")
    for c in pk_cols_right:
        if c not in right_df.columns:
            raise ValueError(f"PK column {c!r} missing on right side.")

    if compare_cols_right is None:
        compare_cols_right = list(compare_cols)
    if len(compare_cols_right) != len(compare_cols):
        raise ValueError(
            f"compare_cols and compare_cols_right have different lengths "
            f"({len(compare_cols)} vs {len(compare_cols_right)})."
        )

    left = left_df.copy()
    right = right_df.copy()
    for col in left.columns:
        left[col] = left[col].astype(str)
    for col in right.columns:
        right[col] = right[col].astype(str)

    rename = {}
    for l, r in zip(pk_cols, pk_cols_right):
        if l != r:
            rename[r] = l
    for l, r in zip(compare_cols, compare_cols_right):
        if l != r and r in right.columns and l not in rename.values():
            rename[r] = l
    if rename:
        right = right.rename(columns=rename)
        log(f"renamed right-side columns: {rename}")

    pk_normalizations: List[Dict[str, str]] = []
    for col in pk_cols:
        l_norm, r_norm, label = _maybe_normalize_pk_pair(left[col], right[col])
        if label:
            left[col] = l_norm
            right[col] = r_norm
            pk_normalizations.append({"column": col, "applied": label})
            log(f"normalized PK '{col}' — {label}")

    log(f"sorting both sides by PK and merging on {pk_cols}…")
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
    log(f"merge: {n_matched:,} matched, "
        f"{n_only_left:,} only-left, {n_only_right:,} only-right")

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

    for col, col_right in zip(compare_cols, compare_cols_right):
        if col in pk_cols:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "is a primary key", "n_unmatched": 0,
                                "examples": []})
            continue
        in_left  = col in left.columns
        in_right = col in right.columns
        if not in_left and not in_right:
            columns_out.append({"name": col, "skipped": True,
                                "reason": f"missing on both sides ({col!r}/{col_right!r})",
                                "n_unmatched": 0, "examples": []})
            continue
        if not in_left:
            columns_out.append({"name": col, "skipped": True,
                                "reason": "missing on left side",
                                "n_unmatched": 0, "examples": []})
            continue
        if not in_right:
            columns_out.append({"name": col, "skipped": True,
                                "reason": f"missing on right side ({col_right!r})",
                                "n_unmatched": 0, "examples": []})
            continue

        lcol = f"{col}__L"
        rcol = f"{col}__R"
        diff = inner[lcol] != inner[rcol]
        n_unmatched = int(diff.sum())
        if n_unmatched:
            cols_with_mismatches += 1
            total_mismatches += n_unmatched
            log(f"  {col!r}: {n_unmatched:,} unmatched")
        else:
            log(f"  {col!r}: all matched")

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
        "pk_normalizations": pk_normalizations,
    }
