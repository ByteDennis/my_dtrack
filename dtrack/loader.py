
import csv
import os
from pathlib import Path
from typing import List, Tuple, Optional
from collections import defaultdict
import pandas as pd

from .date_utils import parse_date, bucket_date, detect_format, format_vintage_label
from .db import (
    upsert_row_counts,
    insert_col_stats,
    update_metadata,
    get_metadata,
)
from .stats import compute_column_stats

DATE_ALIASES = {'rpg_dt', 'eff_dt', 'dt', 'date', 'run_date', 'snap_dt', 'snapshot_dt', 'date_value'}
COUNT_ALIASES = {'row_count', 'rowcount', 'count', 'cnt', 'rows'}

# >>> canonicalize a date to YYYY-MM-DD (raw on failure) for stable min/max <<< #
def _canon_date(d):
    try:
        return parse_date(d)
    except (ValueError, TypeError):
        return d

# >>> Detect which column is the date column based on common aliases <<< #
def detect_date_column(headers: List[str]) -> Optional[str]:
    for header in headers:
        if header.lower().strip() in DATE_ALIASES:
            return header
    return None

# >>> Detect which column is the count column based on common aliases <<< #
def detect_count_column(headers: List[str]) -> Optional[str]:
    for header in headers:
        if header.lower().strip() in COUNT_ALIASES:
            return header
    return None

# >>> Load row counts from a CSV file <<< #
def load_row_count_csv(
    csv_path: str,
    date_col: Optional[str] = None,
    count_col: Optional[str] = None,
) -> Tuple[List[Tuple[str, int]], Optional[str]]:
    detected_format = None

    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        if date_col is None or date_col not in headers:
            date_col = detect_date_column(headers)
            if date_col is None:
                date_col = headers[0]

        if count_col is None or count_col not in headers:
            count_col = detect_count_column(headers)
            if count_col is None:
                count_col = headers[1]

        data = defaultdict(int)
        for row in reader:
            date_str = row[date_col].strip()
            count_str = row[count_col].strip()

            if detected_format is None and date_str:
                try:
                    detected_format = detect_format(date_str)
                except ValueError:
                    pass

            dt = date_str

            try:
                count = int(count_str)
            except ValueError:
                print(f"Warning: Skipping row with invalid count '{count_str}'")
                continue

            data[dt] += count

    return sorted(data.items()), detected_format

# >>> Load row counts from CSV file(s) into the database <<< #
def load_row_counts(
    db_path: str,
    file_or_folder: str,
    table_name: str,
    mode: str = "upsert",
    vintage: Optional[str] = None,
    source: Optional[str] = None,
    db_name: Optional[str] = None,
    source_table: Optional[str] = None,
    date_col: Optional[str] = None,
    date_var_override: Optional[str] = None,
) -> None:
    path = Path(file_or_folder)

    if path.is_file():
        csv_files = [path]
    elif path.is_dir():
        csv_files = list(path.glob("*.csv"))
    else:
        raise ValueError(f"Path does not exist: {file_or_folder}")

    all_data = defaultdict(int)
    detected_format = None
    for csv_file in csv_files:
        data, fmt = load_row_count_csv(
            csv_path=str(csv_file),
            date_col=date_col,
        )
        if fmt and not detected_format:
            detected_format = fmt
        for dt, count in data:
            all_data[dt] += count

    data_list = sorted(all_data.items())

    if mode == "replace":
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM _row_counts WHERE source_table = ?", (table_name,))
        conn.commit()
        conn.close()
        upsert_row_counts(db_path, table_name, data_list)
    elif mode == "upsert":
        upsert_row_counts(db_path, table_name, data_list)
    elif mode == "append":
        from .db import insert_row_counts
        insert_row_counts(db_path, table_name, data_list)
    else:
        raise ValueError(f"Invalid mode: {mode}")

    # canonicalize dt before min/max so metadata stays YYYY-MM-DD (matches the
    # col loader); a raw source format like hadoop's compact YYYYMMDD would
    # otherwise mix with canonical values and pick min/max from different shapes
    canon_dts = [_canon_date(dt) for dt, _ in data_list]
    min_dt = min(canon_dts) if canon_dts else None
    max_dt = max(canon_dts) if canon_dts else None

    if mode == "upsert" and (min_dt or max_dt):
        existing = get_metadata(db_path, table_name)
        if existing:
            # canonicalize existing too so a previously-stored raw value heals
            if existing.get("min_date_loaded") and min_dt:
                min_dt = min(min_dt, _canon_date(existing["min_date_loaded"]))
            if existing.get("max_date_loaded") and max_dt:
                max_dt = max(max_dt, _canon_date(existing["max_date_loaded"]))

    total_count = sum(count for _, count in data_list)
    update_metadata(db_path, {
        "table_name": table_name,
        "source": source,
        "source_table": source_table,
        "date_var": date_var_override or date_col,
        "row_count_total": total_count,
        "vintage": vintage,
        "data_type": "row",
        "date_format": detected_format,
        "min_date_loaded": min_dt,
        "max_date_loaded": max_dt,
    })

# >>> read a {qname}_columns.csv sidecar (column_name/variable/name -> data_type), case-insensitive headers <<< #
def read_columns_sidecar(path):
    if not path or not os.path.exists(path):
        return {}
    out = {}
    try:
        with open(path, 'r', newline='') as f:
            for row in csv.DictReader(f):
                row = {(k or '').strip().lower(): v for k, v in row.items()}
                col = (row.get('column_name') or row.get('variable')
                       or row.get('name') or '')
                dt = row.get('data_type') or row.get('type') or ''
                if col:
                    out[col] = dt
    except Exception as e:
        from .logmgr import log_exc
        log_exc(f"read_columns_sidecar({path})", e)
        return {}
    return out

# >>> Load column data from a CSV file into a pandas DataFrame <<< #
def load_column_data_csv(
    csv_path: str,
    date_col: str,
    vintage: str = "day",
    columns: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    df[date_col] = df[date_col].apply(lambda x: parse_date(str(x)))
    df[date_col] = df[date_col].apply(lambda x: bucket_date(x, vintage))

    if from_date:
        df = df[df[date_col] >= from_date]
    if to_date:
        df = df[df[date_col] <= to_date]

    if columns:
        df = df[[date_col] + columns]

    return df

# >>> Load column statistics from a CSV file into the database <<< #
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
    df = load_column_data_csv(
        csv_path=file_path,
        date_col=date_col,
        vintage=vintage,
        columns=columns,
        from_date=from_date,
        to_date=to_date,
    )

    stats = compute_column_stats(
        df=df,
        source_table=source_table,
        date_col=date_col,
        columns=columns,
    )

    for stat in stats:
        try:
            stat["vintage_label"] = format_vintage_label(stat["dt"], vintage)
        except (ValueError, KeyError):
            stat["vintage_label"] = ""

    if mode == "replace":
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM _col_stats WHERE source_table = ?", (source_table,))
        conn.commit()
        conn.close()

    insert_col_stats(db_path, stats)

    update_metadata(db_path, {
        "table_name": source_table,
        "source": source,
        "source_table": source_table,
        "date_var": date_col,
        "vintage": vintage,
        "data_type": "col",
    })

_COL_STATS_ALIASES = {
    'n_total': ['n_total', 'col_count', 'count'],
    'n_missing': ['n_missing', 'col_missing', 'missing'],
    'n_unique': ['n_unique', 'col_distinct', 'distinct'],
    'mean': ['mean', 'col_avg', 'avg'],
    'std': ['std', 'col_std', 'stddev'],
    'min_val': ['min_val', 'col_min', 'min'],
    'max_val': ['max_val', 'col_max', 'max'],
    'top_10': ['top_10', 'col_freq', 'freq', 'top10'],
    'length_avg': ['length_avg', 'len_avg', 'avg_len'],
    'length_max': ['length_max', 'len_max', 'max_len'],
    'hash_sum': ['hash_sum', 'hashsum'],
}

# >>> Find the actual CSV header for a field using aliases <<< #
def _resolve_col(headers, field, aliases):
    for alias in aliases:
        for h in headers:
            if h.lower().strip() == alias.lower():
                return h
    return None

# >>> Load pre-computed column statistics from CSV into _col_stats table <<< #
def load_precomputed_col_stats(
    db_path: str,
    csv_path: str,
    table_name: str,
    mode: str = "upsert",
    source: Optional[str] = None,
    db_name: Optional[str] = None,
    vintage: str = "day",
    level: str = "fine",
) -> int:
    import sqlite3

    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        col_map = {}
        for field, aliases in _COL_STATS_ALIASES.items():
            resolved = _resolve_col(headers, field, aliases)
            if resolved:
                col_map[field] = resolved

        if mode == "replace" and level == "fine":
            conn = sqlite3.connect(db_path)
            conn.execute("DELETE FROM _col_stats WHERE source_table = ?", (table_name,))
            conn.commit()
            conn.close()
        elif mode == "replace" and level == "coarse":
            print(f"  [load-col] level=coarse: ignoring mode='replace' to "
                  f"preserve any existing fine rows for {table_name}")

        if vintage.startswith('sample@'):
            effective_vintage = 'day'
        elif vintage == 'all':
            effective_vintage = None
        else:
            effective_vintage = vintage

        stats = []
        for row in reader:
            def _get(field, default=None):
                mapped = col_map.get(field)
                if mapped and row.get(mapped):
                    return row[mapped]
                if row.get(field):
                    return row[field]
                return default

            raw_dt = row.get("dt", "")
            try:
                canonical_dt = parse_date(raw_dt) if raw_dt else raw_dt
            except ValueError as e:
                canonical_dt = raw_dt
                if not stats and raw_dt not in ('all', 'sample'):
                    print(f"  [vintage_label] WARNING: parse_date({raw_dt!r}) failed: {e}")
            label = ""
            if canonical_dt and effective_vintage:
                try:
                    label = format_vintage_label(canonical_dt, effective_vintage)
                except (ValueError, KeyError):
                    label = ""

            stat = {
                "source_table": table_name,
                "column_name": row.get("column_name", ""),
                "dt": raw_dt,
                "col_type": row.get("col_type", "categorical"),
                "n_total": _get("n_total", ""),
                "n_missing": _get("n_missing", ""),
                "n_unique": _get("n_unique", ""),
                "mean": _get("mean", ""),
                "std": _get("std", ""),
                "min_val": _get("min_val", ""),
                "max_val": _get("max_val", ""),
                "top_10": _get("top_10", ""),
                "vintage_label": label,
                "length_avg": _get("length_avg", ""),
                "length_max": _get("length_max", ""),
                "hash_sum": _get("hash_sum", ""),
            }
            stats.append(stat)

    insert_col_stats(db_path, stats,
                     mode="coarse_merge" if level == "coarse" else "fine")

    dt_values = [s["dt"] for s in stats if s.get("dt")]
    canonical_dts = [_canon_date(d) for d in dt_values]
    min_dt = min(canonical_dts) if canonical_dts else None
    max_dt = max(canonical_dts) if canonical_dts else None

    if mode == "upsert" and (min_dt or max_dt):
        existing = get_metadata(db_path, table_name)
        if existing:
            if existing.get("min_date_loaded") and min_dt:
                min_dt = min(min_dt, _canon_date(existing["min_date_loaded"]))
            if existing.get("max_date_loaded") and max_dt:
                max_dt = max(max_dt, _canon_date(existing["max_date_loaded"]))

    update_metadata(db_path, {
        "table_name": table_name,
        "source": source,
        "source_table": table_name,
        "vintage": vintage,
        "data_type": "col",
        "min_date_loaded": min_dt,
        "max_date_loaded": max_dt,
    })

    return len(stats)
