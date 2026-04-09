"""Base class and shared helpers for platform builders."""

import os
import re
from abc import ABC, abstractmethod

# Oracle numeric types
ORACLE_NUMERIC_TYPES = {
    'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE',
    'INTEGER', 'INT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'DEC',
}

# Athena numeric types
ATHENA_NUMERIC_TYPES = {
    'int', 'integer', 'bigint', 'smallint', 'tinyint',
    'double', 'float', 'decimal', 'real',
}


def is_numeric_type(data_type, is_oracle=True):
    """Check if a data type string represents a numeric column."""
    dt_upper = data_type.upper().split('(')[0].strip()
    if is_oracle:
        return dt_upper in ORACLE_NUMERIC_TYPES
    return dt_upper.lower() in ATHENA_NUMERIC_TYPES


def qualified_name(tbl_cfg):
    """Derive storage/file name: {source}_{name}."""
    source = tbl_cfg.get('source', '')
    name = tbl_cfg['name']
    return f"{source}_{name}" if source else name


def sas_safe_name(name, max_len=18):
    """Truncate name for SAS identifiers."""
    return name[:max_len]


def resolve_table(tbl_cfg, full_table=None):
    """Resolve table reference, wrapping with CTE if 'processed' is configured.

    For SAS tables (source=sas), processed is setup statements, not a CTE.
    Returns (table_ref, cte_prefix).
    """
    if is_sas_table(tbl_cfg):
        # SAS: table = conn_macro.table, no CTE
        conn = tbl_cfg.get('conn_macro', '')
        table = tbl_cfg.get('table', '')
        return f"{conn}.{table}" if conn else table, ""

    processed = tbl_cfg.get('processed')
    table = full_table or tbl_cfg['table']
    if not processed:
        return table, ""
    if isinstance(processed, list):
        processed = "\n".join(processed)
    alias = tbl_cfg['name']
    cte = f"WITH {alias} AS (\n{processed}\n)\n"
    return alias, cte


def is_sas_table(tbl_cfg):
    """Check if table config is a local SAS dataset (source=sas)."""
    return tbl_cfg.get('source', '').lower() == 'sas'


def reformat_date(d, date_format):
    """Reformat a YYYY-MM-DD date string to the target date_format."""
    from datetime import datetime as _dt
    if not date_format or date_format == 'YYYY-MM-DD':
        return str(d)
    try:
        dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
    except ValueError:
        return str(d)
    fmt_map = {
        'YYYYMMDD': '%Y%m%d',
        'DDMONYYYY': '%d%b%Y',
        'DD-MON-YYYY': '%d-%b-%Y',
        'MM/DD/YYYY': '%m/%d/%Y',
        'YYYYMM': '%Y%m',
    }
    py_fmt = fmt_map.get(date_format)
    if py_fmt:
        return dt_obj.strftime(py_fmt).upper() if 'MON' in date_format else dt_obj.strftime(py_fmt)
    return str(d)


def build_date_in_clause(date_col, dates, date_dtype, is_sas=False, date_format=None,
                         custom_date_types=None):
    """Build WHERE fragment for date IN (...) based on column type.

    Splits into multiple IN groups for Oracle's 1000-item limit.
    Accepts optional custom_date_types dict for extensible type handling.
    """
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    # Check custom date types first
    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            formatted = [reformat_date(d, fmt) for d in dates]
        elif cat == 'date':
            formatted = [f"DATE '{d}'" for d in dates]
        else:  # string
            formatted = [f"'{reformat_date(d, fmt)}'" for d in dates]
        col_expr = date_col
    elif dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        formatted = [str(d) for d in dates]
        col_expr = date_col
    elif is_string:
        formatted = [f"'{reformat_date(d, date_format)}'" for d in dates]
        col_expr = date_col
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype or dtype == 'TIME')
        if is_sas:
            from datetime import datetime as _dt
            formatted = []
            for d in dates:
                try:
                    dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
                    formatted.append(f"'{dt_obj.strftime('%d%b%Y').upper()}'d")
                except ValueError:
                    formatted.append(f"'{d}'")
            col_expr = f"datepart({date_col})" if is_datetime else date_col
        else:
            formatted = [f"DATE '{d}'" for d in dates]
            col_expr = f"TRUNC({date_col})" if is_datetime else date_col
    else:
        formatted = [f"'{reformat_date(d, date_format)}'" for d in dates]
        col_expr = date_col

    chunks = [formatted[i:i+999] for i in range(0, len(formatted), 999)]
    parts = [f"{col_expr} IN ({', '.join(chunk)})" for chunk in chunks]
    if len(parts) == 1:
        return parts[0]
    return "(" + " OR ".join(parts) + ")"


def build_date_between_clause(date_col, min_date, max_date, date_dtype, is_sas=False, date_format=None,
                              custom_date_types=None):
    """Build WHERE fragment using BETWEEN based on column type.

    Accepts optional custom_date_types dict for extensible type handling.
    """
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    # Check custom date types first
    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            fmt_min = reformat_date(min_date, fmt)
            fmt_max = reformat_date(max_date, fmt)
            return f"{date_col} BETWEEN {fmt_min} AND {fmt_max}"
        elif cat == 'date':
            return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
        else:  # string
            fmt_min = reformat_date(min_date, fmt)
            fmt_max = reformat_date(max_date, fmt)
            return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"

    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        return f"{date_col} BETWEEN {min_date} AND {max_date}"
    elif is_string:
        fmt_min = reformat_date(min_date, date_format)
        fmt_max = reformat_date(max_date, date_format)
        return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype or dtype == 'TIME')
        if is_sas:
            from datetime import datetime as _dt
            try:
                d_min = _dt.strptime(str(min_date), "%Y-%m-%d")
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d")
                sas_min = f"'{d_min.strftime('%d%b%Y').upper()}'d"
                sas_max = f"'{d_max.strftime('%d%b%Y').upper()}'d"
                col_expr = f"datepart({date_col})" if is_datetime else date_col
                return f"{col_expr} BETWEEN {sas_min} AND {sas_max}"
            except ValueError:
                return f"{date_col} BETWEEN '{min_date}' AND '{max_date}'"
        else:
            col_expr = f"TRUNC({date_col})" if is_datetime else date_col
            return f"{col_expr} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
    else:
        fmt_min = reformat_date(min_date, date_format)
        fmt_max = reformat_date(max_date, date_format)
        return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"


def compute_date_filter(tbl_cfg, db_path, vintage):
    """Compute date filter once in Python for both SAS and Athena paths.

    Returns dict with vintage, filter_type, min_date, max_date, dates, etc.
    """
    qname = qualified_name(tbl_cfg)

    result = {
        'vintage': vintage,
        'filter_type': 'none',
        'min_date': None,
        'max_date': None,
        'dates': [],
        'n_matching': 0,
        'n_buckets': 0,
        'date_dtype': None,
        'date_format': None,
    }

    if vintage == 'all' or not db_path:
        print(f"  [date filter] vintage: {vintage} | no date filter applied")
        return result

    is_sample, n_sample = _parse_sample_vintage(vintage)
    if is_sample:
        result['vintage'] = 'sample'

    from ..db import list_table_pairs, get_row_comparison, get_column_meta, get_metadata

    # Look up date column dtype
    date_col = tbl_cfg.get('date_col', '')
    col_meta = get_column_meta(db_path, qname)
    date_dtype = None
    for cm in (col_meta or []):
        if cm['column_name'].upper() == date_col.upper():
            date_dtype = (cm.get('data_type') or '').upper()
            break
    result['date_dtype'] = date_dtype

    meta = get_metadata(db_path, qname)
    result['date_format'] = meta.get('date_format') if meta else None

    # Look up matching dates
    pairs = list_table_pairs(db_path)
    pair_name = next(
        (p['pair_name'] for p in pairs
         if qname in (p['table_left'], p['table_right'])),
        None
    )
    matching_dates = []
    if pair_name:
        comp = get_row_comparison(db_path, pair_name)
        if comp and comp.get('matching_dates'):
            matching_dates = comp['matching_dates']

    # Sample dates if sample@N vintage
    if is_sample and matching_dates:
        matching_dates = _sample_matching_dates(db_path, tbl_cfg, matching_dates, n_sample)
        result['filter_type'] = 'in_list'
        result['min_date'] = min(matching_dates)
        result['max_date'] = max(matching_dates)
        result['n_matching'] = len(matching_dates)
        result['n_buckets'] = 1
        result['dates'] = matching_dates
        print(f"  [date filter] {vintage} | {len(matching_dates)} sampled dates | range: {result['min_date']} to {result['max_date']}")
        return result

    # Table has a WHERE clause (from where_map or direct config "where")
    has_where = tbl_cfg.get('_where_from_config') or bool(tbl_cfg.get('where', '').strip())

    if has_where or matching_dates:
        if matching_dates:
            from ..date_utils import bucket_date
            buckets = {}
            for dt in matching_dates:
                buckets.setdefault(bucket_date(dt, vintage), []).append(dt)
            result['filter_type'] = 'between'
            result['min_date'] = min(matching_dates)
            result['max_date'] = max(matching_dates)
            result['n_matching'] = len(matching_dates)
            result['n_buckets'] = len(buckets)
            result['dates'] = matching_dates
            print(f"  [date filter] vintage: {vintage} | range: {result['min_date']} to {result['max_date']} | {len(matching_dates)} dates -> {len(buckets)} buckets")
        else:
            print(f"  [date filter] vintage: {vintage} | no matching dates found")
        return result

    if not matching_dates:
        print(f"  [date filter] no matching dates for {qname}, skipping filter")
        return result

    from ..date_utils import bucket_date
    buckets = {}
    for dt in matching_dates:
        buckets.setdefault(bucket_date(dt, vintage), []).append(dt)

    result['filter_type'] = 'between'
    result['min_date'] = min(matching_dates)
    result['max_date'] = max(matching_dates)
    result['n_matching'] = len(matching_dates)
    result['n_buckets'] = len(buckets)
    result['dates'] = matching_dates

    print(f"  [date filter] vintage: {vintage} | range: {result['min_date']} to {result['max_date']} | {len(matching_dates)} dates -> {len(buckets)} buckets")
    return result


def _parse_sample_vintage(vintage_str):
    """Parse 'sample@N' into (is_sample, n_sample)."""
    if not vintage_str or not vintage_str.startswith('sample@'):
        return False, 0
    try:
        return True, int(vintage_str.split('@', 1)[1])
    except ValueError:
        return False, 0


def _sample_matching_dates(db_path, tbl_cfg, matching_dates, n_sample=100):
    """Sample N dates from matching_dates with reproducible seed."""
    import random
    from ..db import get_sampled_dates, save_sampled_dates, list_table_pairs

    seed = int(os.environ.get('SEED', '2025'))
    qname = qualified_name(tbl_cfg)

    if len(matching_dates) <= n_sample:
        print(f"  {qname}: {len(matching_dates)} dates <= sample@{n_sample}, using all")
        return matching_dates

    pairs = list_table_pairs(db_path)
    pair_name = None
    for pair in pairs:
        if qname in (pair['table_left'], pair['table_right']):
            pair_name = pair['pair_name']
            break
    if not pair_name:
        return matching_dates

    existing_samples = get_sampled_dates(db_path, pair_name, qname)
    random.seed(seed)
    new_samples = sorted(random.sample(matching_dates, n_sample))

    if existing_samples and set(existing_samples) == set(new_samples):
        print(f"  {qname}: using existing {len(existing_samples)} sampled dates (seed={seed})")
        return existing_samples

    if existing_samples:
        print(f"  {qname}: scope changed, saving new sample (seed={seed}, sample@{n_sample})")
    else:
        print(f"  {qname}: sampling {n_sample} from {len(matching_dates)} matching dates (seed={seed})")
    save_sampled_dates(db_path, pair_name, qname, new_samples)
    return new_samples


def build_stats_sql(table, col, date_col, where="", col_type="numeric", dialect="oracle"):
    # NOTE: Uses explicit column references — never SELECT *
    """Build column statistics SQL for Oracle or Athena."""
    where_clause = f"AND {where}" if where else ""
    is_athena = dialect == "athena"

    if col_type == "numeric":
        missing = f"COUNT(*) - COUNT({col})" if is_athena else f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)"
        avg_expr = f"AVG(CAST({col} AS DOUBLE))" if is_athena else f"AVG({col})"
        std_expr = f"STDDEV(CAST({col} AS DOUBLE))" if is_athena else f"STDDEV({col})"
        return f"""
SELECT {date_col} AS dt, '{col}' AS column_name, 'numeric' AS col_type,
    COUNT(*) AS n_total, {missing} AS n_missing, COUNT(DISTINCT {col}) AS n_unique,
    {avg_expr} AS mean, {std_expr} AS std,
    MIN({col}) AS min_val, MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}""".strip()
    else:
        missing = f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)"
        return f"""
SELECT {date_col} AS dt, '{col}' AS column_name, 'categorical' AS col_type,
    COUNT(*) AS n_total, {missing} AS n_missing, COUNT(DISTINCT {col}) AS n_unique,
    NULL AS mean, NULL AS std, MIN({col}) AS min_val, MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}""".strip()


def build_top10_sql(table, col, date_col, where="", dialect="oracle"):
    # NOTE: Uses explicit column references — never SELECT *
    """Build top 10 frequency SQL for Oracle or Athena."""
    where_clause = f"AND {where}" if where else ""
    alias = " t" if dialect == "athena" else ""
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col} AS dt, CAST({col} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col} IS NOT NULL {where_clause}
    GROUP BY {date_col}, {col}
){alias} WHERE rn <= 10""".strip()


def parse_stats_row(row):
    """Normalize a query result row to standard dict format (all strings)."""
    return {
        'dt': str(row['dt']),
        'column_name': row['column_name'],
        'col_type': row['col_type'],
        'n_total': str(row['n_total']) if row['n_total'] is not None else '0',
        'n_missing': str(row['n_missing']) if row['n_missing'] is not None else '0',
        'n_unique': str(row['n_unique']) if row['n_unique'] is not None else '0',
        'mean': str(row['mean']) if row['mean'] is not None else '',
        'std': str(row['std']) if row['std'] is not None else '',
        'min_val': str(row['min_val']) if row['min_val'] is not None else '',
        'max_val': str(row['max_val']) if row['max_val'] is not None else '',
    }


def load_tables_from_config(config):
    """Load tables from config (unified format only)."""
    if "pairs" in config and isinstance(config["pairs"], dict):
        from ..config import get_all_tables_from_unified
        return get_all_tables_from_unified(config)
    return config.get('tables', [])


def fill_columns_from_meta(tables, db_path):
    """Fill in columns from _column_meta for tables missing columns."""
    from ..db import get_column_meta
    for tbl in tables:
        if tbl.get('columns'):
            continue
        qn = qualified_name(tbl)
        meta = get_column_meta(db_path, qn)
        if not meta:
            meta = get_column_meta(db_path, tbl['name'])
        if not meta:
            meta = get_column_meta(db_path, tbl['table'])
        if meta:
            all_cols = {m['column_name']: m['data_type'] for m in meta}
            allowed = tbl.get('_col_map_columns')
            if allowed:
                tbl['columns'] = {c: t for c, t in all_cols.items() if c in allowed}
                print(f"  {tbl['name']}: {len(tbl['columns'])}/{len(all_cols)} columns (filtered by col_map)")
            else:
                tbl['columns'] = all_cols
                print(f"  {tbl['name']}: loaded {len(all_cols)} columns from _column_meta")


def inject_where_from_config(tables, config):
    """Inject where_map filters from pair config into table configs."""
    if "pairs" not in config:
        return
    for tbl in tables:
        tbl_table = tbl.get('table', '')
        for pair_name in tbl.get('_pairs', []):
            pair_cfg = config['pairs'].get(pair_name, {})
            where_map = pair_cfg.get('where_map', {})
            if not where_map:
                continue
            side = 'left' if pair_cfg.get('left', {}).get('table') == tbl_table else 'right'
            where_clause = where_map.get(side, '')
            if where_clause:
                tbl['where'] = where_clause
                tbl['_where_from_config'] = True
                print(f"  {qualified_name(tbl)}: using where_map[{side}] from config")
                break


def match_columns_from_dicts(left_cols, right_cols, left_label="left", right_label="right", outfile=None):
    """Compare two column dictionaries and match by case-insensitive name."""
    import json

    left_lower = {k.lower(): k for k in left_cols}
    right_lower = {k.lower(): k for k in right_cols}

    matched = {}
    left_only = []
    right_only = []

    for lower_name, left_name in sorted(left_lower.items()):
        if lower_name in right_lower:
            right_name = right_lower[lower_name]
            matched[left_name] = right_name
        else:
            left_only.append({"name": left_name, "type": left_cols[left_name]})

    for lower_name, right_name in sorted(right_lower.items()):
        if lower_name not in left_lower:
            right_only.append({"name": right_name, "type": right_cols[right_name]})

    result = {
        "matched": matched,
        f"{left_label}_only": left_only,
        f"{right_label}_only": right_only,
        "manual_mapping": {},
    }

    if outfile:
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"\nWritten to: {outfile}")

    return result


class PlatformBuilder(ABC):
    """Base for platform SQL/code generation."""

    def __init__(self, tbl_cfg, db_path=None):
        self.tbl_cfg = tbl_cfg
        self.db_path = db_path
        self.name = tbl_cfg['name']
        self.table = tbl_cfg['table']
        self.date_col = tbl_cfg.get('date_col', '')
        self.qname = qualified_name(tbl_cfg)

    @abstractmethod
    def build_row_sql(self, date_filter):
        """Build SQL for row count extraction."""
        ...

    @abstractmethod
    def build_continuous_sql(self, col, col_type, where):
        """Build continuous/numeric stats SQL."""
        ...

    @abstractmethod
    def build_categorical_sql(self, col, col_type, where, top_n=10):
        """Build categorical stats SQL."""
        ...

    @abstractmethod
    def generate_extraction(self, outdir, extract_type, **kw):
        """Generate extraction artifacts (SAS files or direct queries).

        Returns list of output file paths.
        """
        ...
