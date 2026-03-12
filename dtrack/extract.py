"""Data extraction: SAS code generation for Oracle, direct AWS Athena extraction"""

import csv
import json
import os
import threading


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


def _qualified_name(tbl_cfg):
    """Derive storage/file name: {source}_{name}."""
    source = tbl_cfg.get('source', '')
    name = tbl_cfg['name']
    return f"{source}_{name}" if source else name


def _is_sas_table(tbl_cfg):
    """Check if table config uses a SAS dataset ($ prefix in processed)."""
    processed = tbl_cfg.get('processed')
    if isinstance(processed, list):
        processed = " ".join(processed)
    return bool(processed and processed.startswith('$'))


def _sas_safe_name(name, max_len=18):
    """Truncate name for SAS identifiers (datasets max 32, macros max 32).

    The tightest constraint is macro names like get_colstats_{name} (14 prefix),
    so default max_len=18 keeps everything safe.
    """
    return name[:max_len]


def _resolve_table(tbl_cfg, full_table=None):
    """Resolve table reference, wrapping with CTE if 'processed' is configured.

    Args:
        tbl_cfg: Table config dict, may contain 'processed' (str or list of str)
        full_table: Fully qualified table name (e.g. "db.table"). If None, uses tbl_cfg['table'].

    Returns:
        (table_ref, cte_prefix): table_ref to use in FROM, cte_prefix to prepend to SQL.
        When no 'processed', cte_prefix is empty string.
    """
    processed = tbl_cfg.get('processed')
    table = full_table or tbl_cfg['table']
    if not processed:
        return table, ""
    if isinstance(processed, list):
        processed = "\n".join(processed)
    alias = tbl_cfg['name']
    cte = f"WITH {alias} AS (\n{processed}\n)\n"
    return alias, cte


def _load_tables_from_config(config):
    """Load tables from config (unified or old format)."""
    if "pairs" in config and isinstance(config["pairs"], dict):
        from .config import get_all_tables_from_unified
        return get_all_tables_from_unified(config)
    return config.get('tables', [])


def _fill_columns_from_meta(tables, db_path):
    """Fill in columns from _column_meta for tables missing columns, filtered by col_map."""
    from .db import get_column_meta
    for tbl in tables:
        if tbl.get('columns'):
            continue
        qn = _qualified_name(tbl)
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


def _inject_where_from_config(tables, config):
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
                print(f"  {_qualified_name(tbl)}: using where_map[{side}] from config")
                break


def _build_date_in_clause(date_col, dates, date_dtype, is_sas=False):
    """Build a WHERE fragment for date IN (...) based on column type."""
    dtype = (date_dtype or '').upper()
    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        date_list = ", ".join(str(d) for d in dates)
        return f"{date_col} IN ({date_list})"
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype
                       or (dtype == 'TIME'))
        if is_sas:
            from datetime import datetime as _dt
            sas_dates = []
            for d in dates:
                try:
                    dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
                    sas_dates.append(f"'{dt_obj.strftime('%d%b%Y').upper()}'d")
                except ValueError:
                    sas_dates.append(f"'{d}'")
            date_list = ", ".join(sas_dates)
            # DATETIME/TIMESTAMP: wrap with datepart() to compare date-to-date
            col_expr = f"datepart({date_col})" if is_datetime else date_col
            return f"{col_expr} IN ({date_list})"
        else:
            date_list = ", ".join(f"DATE '{d}'" for d in dates)
            return f"TRUNC({date_col}) IN ({date_list})"
    elif dtype.startswith('CHAR'):
        date_list = ", ".join(f"'{d}'" for d in dates)
        return f"TRIM({date_col}) IN ({date_list})"
    else:
        date_list = ", ".join(f"'{d}'" for d in dates)
        return f"{date_col} IN ({date_list})"


def _sample_matching_dates(db_path, tbl_cfg, matching_dates):
    """Sample N dates from matching_dates with reproducible seed. Returns sampled list or original."""
    import random
    from .db import get_sampled_dates, save_sampled_dates

    n_sample = int(os.environ.get('N_SAMPLE', '100'))
    seed = int(os.environ.get('SEED', '2025'))
    qname = _qualified_name(tbl_cfg)

    if len(matching_dates) <= n_sample:
        print(f"  {qname}: {len(matching_dates)} dates <= N_SAMPLE={n_sample}, using all")
        return matching_dates

    # Find pair_name for this table
    from .db import list_table_pairs
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
        print(f"  {qname}: scope changed, saving new sample (seed={seed}, N_SAMPLE={n_sample})")
    else:
        print(f"  {qname}: sampling {n_sample} from {len(matching_dates)} matching dates (seed={seed})")
    save_sampled_dates(db_path, pair_name, qname, new_samples)
    return new_samples


def _compute_date_filter(tbl_cfg, db_path, vintage):
    """Compute date filter once in Python for both SAS and Athena paths.

    Returns dict with:
        vintage, filter_type ('none'|'between'|'in_list'),
        min_date, max_date, dates (for in_list),
        n_matching, n_buckets, date_dtype
    """
    qname = _qualified_name(tbl_cfg)
    date_col = tbl_cfg.get('date_col', '')

    result = {
        'vintage': vintage,
        'filter_type': 'none',
        'min_date': None,
        'max_date': None,
        'dates': [],
        'n_matching': 0,
        'n_buckets': 0,
        'date_dtype': None,
    }

    if vintage == 'all' or not db_path:
        print(f"  [date filter] vintage: {vintage} | no date filter applied")
        return result

    from .db import list_table_pairs, get_row_comparison, get_column_meta

    # Find pair
    pairs = list_table_pairs(db_path)
    pair_name = next(
        (p['pair_name'] for p in pairs
         if qname in (p['table_left'], p['table_right'])),
        None
    )
    if not pair_name:
        print(f"  [date filter] no pair found for {qname}, skipping filter")
        return result

    comp = get_row_comparison(db_path, pair_name)
    if not comp or not comp.get('matching_dates'):
        print(f"  [date filter] no matching dates for {qname}, skipping filter")
        return result

    matching_dates = comp['matching_dates']

    # Look up date column dtype
    col_meta = get_column_meta(db_path, qname)
    date_dtype = None
    for cm in (col_meta or []):
        if cm['column_name'].upper() == date_col.upper():
            date_dtype = (cm.get('data_type') or '').upper()
            break
    result['date_dtype'] = date_dtype

    if vintage == 'sample':
        sampled = _sample_matching_dates(db_path, tbl_cfg, matching_dates)
        result['vintage'] = 'day'
        result['filter_type'] = 'in_list'
        result['dates'] = sampled
        result['n_matching'] = len(matching_dates)
        result['n_buckets'] = len(sampled)
        print(f"  [date filter] vintage: sample | {len(matching_dates)} dates → {len(sampled)} sampled")
        return result

    # between filter for day/week/month/quarter/year
    from .date_utils import bucket_date
    buckets = {}
    for dt in matching_dates:
        buckets.setdefault(bucket_date(dt, vintage), []).append(dt)

    result['filter_type'] = 'between'
    result['min_date'] = min(matching_dates)
    result['max_date'] = max(matching_dates)
    result['n_matching'] = len(matching_dates)
    result['n_buckets'] = len(buckets)
    result['dates'] = matching_dates

    print(f"  [date filter] vintage: {vintage} | range: {result['min_date']} to {result['max_date']} | {len(matching_dates)} dates → {len(buckets)} buckets")
    return result


def _build_date_between_clause(date_col, min_date, max_date, date_dtype, is_sas=False):
    """Build a WHERE fragment using BETWEEN based on column type."""
    dtype = (date_dtype or '').upper()
    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        return f"{date_col} BETWEEN {min_date} AND {max_date}"
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype
                       or (dtype == 'TIME'))
        if is_sas:
            from datetime import datetime as _dt
            try:
                d_min = _dt.strptime(str(min_date), "%Y-%m-%d")
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d")
                sas_min = f"'{d_min.strftime('%d%b%Y').upper()}'d"
                sas_max = f"'{d_max.strftime('%d%b%Y').upper()}'d"
                # DATETIME/TIMESTAMP: wrap with datepart() to compare date-to-date
                col_expr = f"datepart({date_col})" if is_datetime else date_col
                return f"{col_expr} BETWEEN {sas_min} AND {sas_max}"
            except ValueError:
                return f"{date_col} BETWEEN '{min_date}' AND '{max_date}'"
        else:
            return f"TRUNC({date_col}) BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
    elif dtype.startswith('CHAR'):
        return f"TRIM({date_col}) BETWEEN '{min_date}' AND '{max_date}'"
    else:
        return f"{date_col} BETWEEN '{min_date}' AND '{max_date}'"




def is_numeric_type(data_type, is_oracle=True):
    """Check if a data type string represents a numeric column."""
    dt_upper = data_type.upper().split('(')[0].strip()
    if is_oracle:
        return dt_upper in ORACLE_NUMERIC_TYPES
    else:
        return dt_upper.lower() in ATHENA_NUMERIC_TYPES


def _build_stats_sql(table, col, date_col, where="", col_type="numeric", dialect="oracle"):
    """Build column statistics SQL for Oracle or Athena.

    Args:
        col_type: 'numeric' or 'categorical'
        dialect: 'oracle' or 'athena'
    """
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


def _build_top10_sql(table, col, date_col, where="", dialect="oracle"):
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


# Backward-compatible aliases for tests and external callers
def build_continuous_sql_oracle(table, col, date_col, where=""):
    return _build_stats_sql(table, col, date_col, where, "numeric", "oracle")

def build_categorical_sql_oracle(table, col, date_col, where=""):
    return _build_stats_sql(table, col, date_col, where, "categorical", "oracle")

def build_continuous_sql_athena(table, col, date_col, where=""):
    return _build_stats_sql(table, col, date_col, where, "numeric", "athena")

def build_categorical_sql_athena(table, col, date_col, where=""):
    return _build_stats_sql(table, col, date_col, where, "categorical", "athena")

def build_top10_sql_oracle(table, col, date_col, where=""):
    return _build_top10_sql(table, col, date_col, where, "oracle")

def build_top10_sql_athena(table, col, date_col, where=""):
    return _build_top10_sql(table, col, date_col, where, "athena")


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


# ---------------------------------------------------------------------------
# SAS Generation
# ---------------------------------------------------------------------------

def _oracle_date_transform(date_col, transform):
    """Return Oracle SQL expression for date transformation (used inside passthrough)."""
    if transform and "{col}" in transform:
        return transform.replace("{col}", date_col)
    if transform == "datetime_to_date":
        return f"TRUNC({date_col})"
    elif transform == "to_char":
        return f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
    return date_col


# ── Oracle → SAS date transform mapping ──
# Oracle SQL expressions used in passthrough vs SAS native equivalents
# for $-prefixed processed tables (SAS datasets, not Oracle).
_ORACLE_TO_SAS_TRANSFORM = {
    'datetime_to_date': 'datepart({col})',
    'to_char':          "put({col}, yymmdd10.)",
}

# Oracle TRUNC vintage → SAS intnx equivalent
_ORACLE_VINTAGE_TO_SAS = {
    'day':     None,                                  # identity
    'week':    "intnx('week.2', {col}, 0, 'b')",
    'month':   "intnx('month', {col}, 0, 'b')",
    'quarter': "intnx('qtr', {col}, 0, 'b')",
    'year':    "intnx('year', {col}, 0, 'b')",
}


def _sas_date_transform(date_col, transform):
    """Return SAS expression for date transformation (for $ processed tables)."""
    if transform and "{col}" in transform:
        return transform.replace("{col}", date_col)
    sas_expr = _ORACLE_TO_SAS_TRANSFORM.get(transform)
    if sas_expr:
        return sas_expr.replace("{col}", date_col)
    return date_col


def _sas_vintage_date_expr(date_expr, vintage):
    """Wrap date_expr with SAS vintage bucketing (for $ processed tables)."""
    if vintage == 'all':
        return "'all'"
    if not vintage or vintage == 'day':
        return date_expr
    sas_expr = _ORACLE_VINTAGE_TO_SAS.get(vintage)
    if sas_expr:
        return sas_expr.replace("{col}", date_expr)
    return date_expr


# Oracle TRUNC format codes for vintage bucketing
_VINTAGE_TRUNC = {
    'day': None,       # no extra TRUNC needed (identity)
    'week': 'IW',
    'month': 'MM',
    'quarter': 'Q',
    'year': 'YYYY',
}


def _vintage_date_expr(date_expr, vintage, vintage_transform=None):
    """Wrap date_expr with vintage bucketing.

    If vintage_transform is provided (from config JSON), use it directly
    with {col} replaced by date_expr. Otherwise fall back to Oracle TRUNC.

    Examples:
        vintage_transform="TRUNC(TO_DATE(TO_CHAR({col}), 'YYYYMM'), 'Q')"
        vintage_transform="{col}"  (no-op, data already at desired granularity)
    """
    if vintage_transform:
        return vintage_transform.replace("{col}", date_expr)
    if vintage == 'all':
        return "'all'"
    if not vintage or vintage == 'day':
        return date_expr
    fmt = _VINTAGE_TRUNC.get(vintage)
    if fmt is None:
        return date_expr
    return f"TRUNC({date_expr}, '{fmt}')"


def _oracle_where_to_sas(where, quote=True):
    """Convert Oracle-style WHERE clause to SAS-compatible syntax.

    Converts DATE 'YYYY-MM-DD' literals to SAS 'ddMONyyyy'd format.
    If quote=True, also escapes remaining single quotes for SAS datalines
    while preserving SAS date literals.
    """
    import re
    from datetime import datetime as _dt

    sas_dates = []

    def _date_repl(m):
        d = _dt.strptime(m.group(1), "%Y-%m-%d")
        sas_lit = f"'{d.strftime('%d%b%Y').upper()}'d"
        placeholder = f"\x00SASDT{len(sas_dates)}\x00"
        sas_dates.append(sas_lit)
        return placeholder

    # DATE 'YYYY-MM-DD' or DATE('YYYY-MM-DD') -> placeholder
    s = re.sub(r"DATE\s*\(\s*'(\d{4}-\d{2}-\d{2})'\s*\)", _date_repl, where, flags=re.IGNORECASE)
    s = re.sub(r"DATE\s+'(\d{4}-\d{2}-\d{2})'", _date_repl, s, flags=re.IGNORECASE)

    # Quote remaining single quotes for SAS datalines
    if quote:
        s = s.replace("'", "''")

    # Restore SAS date literals (unquoted)
    for i, lit in enumerate(sas_dates):
        s = s.replace(f"\x00SASDT{i}\x00", lit)

    return s


def _sas_quote(s):
    """Escape single quotes for SAS by doubling them.

    SAS uses '' (two single quotes) to represent a literal single quote.
    This is the standard SAS quoting mechanism - simple and robust.

    Example: "WHERE STATUS = 'A'" becomes "WHERE STATUS = ''A''"
    """
    return s.replace("'", "''")


def _gen_sas_proc_contents(sas_tables, out_dir='.'):
    """Generate SAS code to export column metadata via proc contents for $ tables."""
    if not sas_tables:
        return ''

    tpl_path = os.path.join(os.path.dirname(__file__), 'templates', 'meta.sas')
    with open(tpl_path) as f:
        template = f.read()

    blocks = ["/* Column metadata discovery for SAS datasets */"]
    for tbl_cfg in sas_tables:
        processed = tbl_cfg.get('processed', '')
        if isinstance(processed, list):
            processed = " ".join(processed)
        replacements = {
            '/*{SN}*/': _sas_safe_name(tbl_cfg['name']),
            '/*{QNAME}*/': _qualified_name(tbl_cfg),
            '/*{SOURCE}*/': tbl_cfg.get('source', 'pcds'),
            '/*{TABLE}*/': tbl_cfg['name'],
            '/*{SAS_DATASET}*/': processed[1:].strip(),
        }
        block = template
        for k, v in replacements.items():
            block = block.replace(k, v)
        blocks.append(block)

    return '\n'.join(blocks)


def _gen_sas_row_datadriven(pcds_tables, sas_lib='WORK', out_dir='.'):
    """Generate data-driven SAS row extraction using template files.

    Builds mapping datasets and reusable macros for two modes:
    - Oracle tables: proc sql passthrough to Oracle (rows_oracle.sas)
    - SAS tables ($-prefixed processed): proc sql on local SAS dataset (rows_sas.sas)
    """
    oracle_datalines = []
    sas_datalines = []

    for idx, tbl_cfg in enumerate(pcds_tables, 1):
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)
        conn_macro = tbl_cfg.get('conn_macro', 'pcds')
        raw_where = tbl_cfg.get('where', '')
        transform = tbl_cfg.get('date_transform', '')
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)

        safe_ds = _sas_safe_name(qname, 29)

        if processed and processed.startswith('$'):
            sas_table = processed[1:].strip()
            where = _oracle_where_to_sas(raw_where, quote=False)
            date_expr = _sas_date_transform(date_col, transform) if transform else date_col
            sas_datalines.append(f"{sas_table}|{safe_ds}|{qname}|{date_expr}|{where}")
        else:
            where = _sas_quote(raw_where)
            date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
            if processed:
                table = name
            oracle_datalines.append(f"{table}|{safe_ds}|{qname}|{date_expr}|{conn_macro}|{idx}|{where}")

    # CTE %let statements for Oracle processed tables
    cte_lines = []
    for idx, tbl_cfg in enumerate(pcds_tables, 1):
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)
        if processed and not processed.startswith('$'):
            name = tbl_cfg['name']
            cte_lines.append(f"%let _cte{idx} = WITH {name} AS ({processed});")

    redo = str(int(os.environ.get('SAS_ROW_REDO', '0')))
    redo_line = f"%let _row_redo = {redo};"
    tpl_dir = os.path.join(os.path.dirname(__file__), 'templates')
    parts = []

    if oracle_datalines:
        with open(os.path.join(tpl_dir, 'rows_oracle.sas')) as f:
            tpl = f.read()
        tpl = tpl.replace('/*{CTE_VARS}*/', '\n'.join(cte_lines))
        tpl = tpl.replace('/*{ROW_REDO}*/', redo)
        tpl = tpl.replace('/*{ORA_DATALINES}*/', '\n'.join(oracle_datalines))
        parts.append(tpl)

    if sas_datalines:
        with open(os.path.join(tpl_dir, 'rows_sas.sas')) as f:
            tpl = f.read()
        # If Oracle block already emitted redo, don't duplicate
        if oracle_datalines:
            tpl = tpl.replace('/*{ROW_REDO}*/', '')
        else:
            tpl = tpl.replace('/*{ROW_REDO}*/', redo_line)
        tpl = tpl.replace('/*{SAS_DATALINES}*/', '\n'.join(sas_datalines))
        parts.append(tpl)

    return '\n\n'.join(parts)


def _resolve_table_and_cte(tbl_cfg):
    """Resolve table name and CTE prefix from config.

    Returns (table, cte_prefix, is_sas_table).
    """
    table = tbl_cfg['table']
    processed = tbl_cfg.get('processed')
    if isinstance(processed, list):
        processed = " ".join(processed)

    if processed and processed.startswith('$'):
        return processed[1:].strip(), "", True
    if processed:
        alias = tbl_cfg['name']
        return alias, f"WITH {alias} AS ({processed}) ", False
    return table, "", False




def _gen_sas_col_local(tbl_cfg, db_path=None, sas_lib='WORK', out_dir='.'):
    """Generate SAS macro for column statistics using the columns.sas template.

    Fills template placeholders with: column map, base SQL, vintage map,
    pull statement, cache check, and stack logic.
    """
    name = tbl_cfg['name']
    sn = _sas_safe_name(name)
    qname = _qualified_name(tbl_cfg)
    date_col = tbl_cfg['date_col']
    columns = tbl_cfg.get('columns', {})
    where = tbl_cfg.get('where', '')
    transform = tbl_cfg.get('date_transform', '')
    vintage = tbl_cfg.get('vintage', 'all')
    conn_macro = tbl_cfg.get('conn_macro', 'pcds')
    user_override = tbl_cfg.get('user', '')
    redo = int(os.environ.get('SAS_COL_REDO', '0'))

    # Early returns for empty column lists
    if not columns:
        return f"%macro get_colstats_{sn}();\n    %put WARNING: No columns specified for {name}.;\n%mend get_colstats_{sn};"
    col_list = [(c, d) for c, d in columns.items() if c.upper() != date_col.upper()]
    if not col_list:
        return f"%macro get_colstats_{sn}();\n    %put WARNING: No non-date columns to extract for {name};\n%mend get_colstats_{sn};"

    num_cols = [c for c, d in col_list if is_numeric_type(d, is_oracle=True)]
    cat_cols = [c for c, d in col_list if not is_numeric_type(d, is_oracle=True)]

    # Resolve table source
    table, cte_prefix, is_sas = _resolve_table_and_cte(tbl_cfg)

    # Date expression
    if is_sas:
        date_expr = _sas_date_transform(date_col, transform) if transform else date_col
    else:
        date_expr = _oracle_date_transform(date_col, transform) if transform else date_col

    # Compute unified date filter
    date_filter = _compute_date_filter(tbl_cfg, db_path, vintage)
    date_dtype = date_filter['date_dtype']
    has_filter = date_filter['filter_type'] != 'none'

    # For SAS DATETIME columns, wrap with datepart() if not already handled by date_transform
    if is_sas and date_dtype and ('DATETIME' in date_dtype.upper() or 'TIMESTAMP' in date_dtype.upper()):
        if 'datepart' not in date_expr.lower():
            date_expr = f"datepart({date_expr})"

    # Apply vintage bucketing to date expression when vintage is week/month/quarter/year
    effective_vintage = date_filter['vintage']
    if effective_vintage not in ('all', 'day', None) and date_filter['filter_type'] != 'none':
        vintage_transform = tbl_cfg.get('vintage_transform', None)
        if is_sas:
            date_expr = _sas_vintage_date_expr(date_expr, effective_vintage)
        else:
            date_expr = _vintage_date_expr(date_expr, effective_vintage, vintage_transform)

    # Build template values
    _ua = f", user=&{user_override}_usr, pwd=&{user_override}_pwd" if user_override else ""
    if is_sas:
        pull_stmt = "        proc sql; create table &raw_ds as &_full_sql; quit;"
    else:
        pull_stmt = f"        %pull_data(&_full_sql, &raw_ds, server={conn_macro}{_ua});"

    if redo:
        cache_start, cache_end = "", ""
    else:
        cache_start = ("        %if %sysfunc(exist(&cache_ds)) %then %do;\n"
                       "            %put NOTE: Cached stats found: &cache_ds - skipping;\n"
                       "        %end;\n"
                       "        %else %do;")
        cache_end = "        %end;"

    # Column map as assignment statements (no datalines — safe inside macros)
    col_map_rows = '\n'.join(
        f"        col_name='{c}'; col_type='{'numeric' if is_numeric_type(d, is_oracle=True) else 'categorical'}'; output;"
        for c, d in col_list
    )

    col_select = ", ".join([f"{date_expr} AS dt"] + [c for c, _ in col_list])
    base_sql = f"{cte_prefix}SELECT {col_select} FROM {table}"
    if where:
        base_where = f"AND ({where})" if has_filter else f"WHERE {where}"
    else:
        base_where = ""

    # Build vintage calls: data step sets _full_sql via call symputx, then macro runs
    # call symputx stores text literally — no macro quoting, no paren issues
    def _symputx_sql(full_sql):
        """Generate a data step that sets _full_sql macro variable."""
        # Use double quotes so single quotes inside (SAS date literals, intnx args) are safe
        escaped = full_sql.replace('"', '""')
        return f'    data _null_; call symputx("_full_sql", "{escaped}"); run;'

    vintage_calls = []
    if date_filter['filter_type'] == 'between':
        from .date_utils import bucket_date
        buckets = {}
        for dt in date_filter['dates']:
            buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
        n = len(buckets)
        for v_idx, (bucket_key, dates) in enumerate(sorted(buckets.items()), 1):
            bmin, bmax = min(dates), max(dates)
            dw = _build_date_between_clause(date_col, bmin, bmax, date_dtype, is_sas=is_sas)
            full_sql = f"{base_sql} WHERE {dw} {base_where}"
            vintage_calls.append(_symputx_sql(full_sql))
            vintage_calls.append(
                f"    %_process_vintage(raw_ds=_raw_{sn}, "
                f"cache_ds=cache._cs_{sn}_v{v_idx});")
        cache_list = " ".join(f"cache._cs_{sn}_v{i}" for i in range(1, n + 1))
        stack_caches = (f"    data _colstats_{sn};\n"
                        f"        set {cache_list};\n"
                        f"    run;")
    elif date_filter['filter_type'] == 'in_list':
        dw = _build_date_in_clause(
            date_col, date_filter['dates'], date_dtype, is_sas=is_sas
        )
        full_sql = f"{base_sql} WHERE {dw} {base_where}"
        vintage_calls.append(_symputx_sql(full_sql))
        vintage_calls.append(
            f"    %_process_vintage(raw_ds=_raw_{sn}, "
            f"cache_ds=cache._cs_{sn});")
        stack_caches = f"    data _colstats_{sn}; set cache._cs_{sn}; run;"
    else:
        full_sql = f"{base_sql} {base_where}"
        vintage_calls.append(_symputx_sql(full_sql))
        vintage_calls.append(
            f"    %_process_vintage(raw_ds=_raw_{sn}, "
            f"cache_ds=cache._cs_{sn});")
        stack_caches = f"    data _colstats_{sn}; set cache._cs_{sn}; run;"

    # Load template and fill placeholders
    tmpl_path = os.path.join(os.path.dirname(__file__), 'templates', 'columns.sas')
    with open(tmpl_path, 'r') as f:
        tmpl = f.read()

    replacements = {
        '/*{SN}*/': sn,
        '/*{NAME}*/': name,
        '/*{TABLE}*/': table,
        '/*{QNAME}*/': qname,
        '/*{N_NUMERIC}*/': str(len(num_cols)),
        '/*{N_CATEGORICAL}*/': str(len(cat_cols)),
        '/*{N_COLS}*/': str(len(col_list)),
        '/*{REDO}*/': str(redo),
        '/*{PULL_STMT}*/': pull_stmt,
        '/*{CACHE_CHECK_START}*/': cache_start,
        '/*{CACHE_CHECK_END}*/': cache_end,
        '/*{COL_MAP_ROWS}*/': col_map_rows,
        '/*{VINTAGE_CALLS}*/': '\n'.join(vintage_calls),
        '/*{STACK_CACHES}*/': stack_caches,
    }
    for placeholder, value in replacements.items():
        tmpl = tmpl.replace(placeholder, value)

    return tmpl



def gen_sas(config_path, outdir, types=None, env_path=None, db_path=None, vintage=None):
    """
    Generate a single combined SAS file for Oracle data extraction.

    Uses template.sas as the base, fills in credentials from .env,
    generates per-table macros, and creates a runner section with
    time tracking.

    Args:
        config_path: Path to extraction config JSON (supports both old and unified formats)
        outdir: Directory to write the combined .sas file
        types: List of types to generate ("row", "col"). Default: both.
        env_path: Path to .env file with pcds_usr, pcds_pw, email_to, lib_path
        db_path: Path to database for column metadata and where_map filtering
        vintage: Date bucketing granularity (day, week, month, quarter, year)
    """
    if types is None:
        types = ["row", "col"]

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    all_tables = _load_tables_from_config(config)

    pcds_tables = [t for t in all_tables if t.get('source', '').lower() in ('pcds', 'oracle')]

    if not pcds_tables:
        print("No PCDS/Oracle tables found in config")
        return

    # Use os.environ (already loaded from dtrack.conf by CLI)
    env_src = os.environ

    # Defaults, then override from env_src
    env_vars = {'SAS_LIB': 'WORK', 'OUT_DIR': '.', 'SEED': '2025'}
    for key in ['PCDS_USR', 'EMAIL_TO', 'SAS_LIB', 'OUT_DIR', 'SEED']:
        if key in env_src:
            env_vars[key] = env_src[key]

    # Connection macro passwords (e.g., PCDS_PWD, PB23_PWD, PB30_PWD)
    # Skip $ (SAS dataset) tables — they don't need Oracle credentials
    oracle_tables = [t for t in pcds_tables if not _is_sas_table(t)]
    conn_macros = set(t.get('conn_macro', 'pcds') for t in oracle_tables)
    for conn_macro in conn_macros:
        pwd_key = f"{conn_macro.upper()}_PWD"
        if pwd_key in env_src:
            env_vars[pwd_key] = env_src[pwd_key]
        else:
            raise KeyError(f"{pwd_key} not found in config or environment")

    # User override credentials (e.g., TMP_USR, TMP_PWD)
    for user_key in set(t.get('user', '') for t in pcds_tables if t.get('user')):
        for suffix in ('_USR', '_PWD'):
            k = f"{user_key.upper()}{suffix}"
            if k in env_src:
                env_vars[k] = env_src[k]
            else:
                raise KeyError(f"{k} not found in config or environment")

    # Read template
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'template.sas')
    with open(template_path, 'r') as f:
        template = f.read()

    if db_path and "col" in types:
        _fill_columns_from_meta(pcds_tables, db_path)
    for tbl in pcds_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    # Pass sas_lib and out_dir to table configs
    sas_lib = env_vars['SAS_LIB']
    out_dir = env_vars['OUT_DIR']

    # Generate table macros
    macro_parts = []

    # Emit proc contents for $ (SAS dataset) tables to export column metadata CSVs
    sas_dataset_tables = [
        t for t in pcds_tables
        if _is_sas_table(t)
    ]
    if sas_dataset_tables:
        macro_parts.append(_gen_sas_proc_contents(sas_dataset_tables, out_dir))

    if "row" in types:
        # Data-driven row extraction (single block for all tables)
        macro_parts.append("/* --- Row extraction (data-driven) --- */")
        macro_parts.append(_gen_sas_row_datadriven(pcds_tables, sas_lib, out_dir))

    if "col" in types:
        for tbl in pcds_tables:
            name = tbl['name']
            macro_parts.append(f"/* --- {name}: {tbl['table']} (col) --- */")
            macro_parts.append(_gen_sas_col_local(tbl, db_path, sas_lib, out_dir))

    # Generate runner section
    runner_parts = []

    # Row runner: data-driven macro handles its own timing via call execute
    # Just add email notification at the end
    if "row" in types:
        runner_parts.append("/* Row extraction driven by table_date_map (macros above) */")
        runner_parts.append("")

    if "col" in types:
        for tbl in pcds_tables:
            name = tbl['name']
            sn = _sas_safe_name(name)
            runner_parts.append(f"%start_timer();")
            runner_parts.append(f"%get_colstats_{sn}();")
            runner_parts.append(f"%log_time(table={name}, step=col, outpath=&out_dir.);")
            qname = _qualified_name(tbl)
            runner_parts.append(
                f'/* %send_email(subject=dtrack col done: {name}, '
                f'body=Table {name} col extraction complete. '
                f'Output: &out_dir./{qname}_col.csv); */'
            )
            runner_parts.append("")

    # Email after all extractions
    if "row" in types:
        runner_parts.append(f"%let _job_end = %sysfunc(datetime());")
        runner_parts.append(f"%let _job_elapsed = %sysevalf(&_job_end - &_job_start);")
        runner_parts.append(
            f'/* %send_email(subject=dtrack row extraction complete, '
            f'body=Row extraction finished. '
            f'Elapsed: %sysfunc(putn(%nrstr(&_job_elapsed), time8.)). '
            f'Output: &out_dir.); */'
        )
        runner_parts.append("")

    # Generate hash from runner content using SEED for reproducibility
    import hashlib
    runner_content = '\n'.join(runner_parts)
    seed = env_vars['SEED']
    hash_input = f"{seed}:{runner_content}"
    prefix = 'x' + hashlib.md5(hash_input.encode()).hexdigest()[:7]

    # Generate connection macros for only the servers used
    from .db import MACRO2SVC
    conn_macro_lines = []
    for macro in sorted(conn_macros):
        tns_path = f"@{MACRO2SVC.get(macro, macro)}"
        pwd_var = env_vars[f"{macro.upper()}_PWD"]
        conn_macro_lines.append(
            f'%macro {macro};\n'
            f'  connect to oracle(user="&iamusr" orapw="{pwd_var}" path="{tns_path}"\n'
            f'    buffsize=5000 preserve_comments);\n'
            f'%mend {macro};'
        )

    # Build user credential %let statements for override accounts
    user_overrides = set(t.get('user', '') for t in pcds_tables if t.get('user'))
    cred_lines = []
    for user_key in sorted(user_overrides):
        usr_var = f"{user_key.upper()}_USR"
        pwd_var = f"{user_key.upper()}_PWD"
        cred_lines.append(f"%let {user_key}_usr = {env_vars[usr_var]};")
        cred_lines.append(f"%let {user_key}_pwd = {env_vars[pwd_var]};")

    # SAS cache directory: {base}/{prefix}/ — prefix isolates runs by config
    sas_cache_base = os.environ.get('SAS_CACHE_DIR', sas_lib)
    sas_cache_dir = sas_cache_base.rstrip('/') + '/' + prefix

    # Build template vars
    template_vars = {
        'pcds_usr': env_vars['PCDS_USR'],
        'prefix': prefix,
        'email_to': env_vars['EMAIL_TO'],
        'out_dir': out_dir,
        'sas_lib': sas_lib,
        'sas_cache_dir': sas_cache_dir,
        'conn_macros': '\n'.join(conn_macro_lines),
        'table_macros': '\n'.join(macro_parts),
        'runner': runner_content,
        'user_credentials': '\n'.join(cred_lines),
    }

    sas_content = template.format(**template_vars)

    type_suffix = '_' + '_'.join(sorted(types)) if types != ['row', 'col'] else ''
    sas_path = os.path.join(outdir, f'extract{type_suffix}.sas')
    with open(sas_path, 'w') as f:
        f.write(sas_content)

    print(f"  Generated: {sas_path}")
    print(f"  Tables: {len(pcds_tables)}")
    print(f"  Types: {', '.join(types)}")
    if env_path:
        print(f"  Credentials: from {env_path}")

    # Check for mock mode - extract from mock CSVs instead of running SAS
    mock_dir = os.environ.get('DTRACK_ORACLE_MOCK')
    if mock_dir:
        print()
        _extract_oracle_mock(config_path, outdir, types, db_path, mock_dir)


# ---------------------------------------------------------------------------
# AWS Athena Direct Extraction
# ---------------------------------------------------------------------------

_aws_creds_expires = None
_aws_creds_lock = threading.Lock()


def aws_creds_renew(ttl_minutes=50, retries=3, retry_delay=10):
    """Renew AWS credentials via token-based auth (Windows only).

    On Windows, fetches a token from AWS_TOKEN_URL and uses it to obtain
    temporary credentials from AWS_ARN_URL. On Linux/containers, credentials
    come from IAM roles or environment variables, so this is a no-op.

    Thread-safe: uses a lock so only one thread renews at a time.
    Skips renewal if credentials were refreshed less than ttl_minutes ago.
    """
    global _aws_creds_expires

    if os.name != 'nt':
        return

    import time
    now = time.time()
    if _aws_creds_expires and now < _aws_creds_expires:
        return

    with _aws_creds_lock:
        # Double-check after acquiring lock (another thread may have renewed)
        now = time.time()
        if _aws_creds_expires and now < _aws_creds_expires:
            return

        import requests

        token_url = os.environ.get('AWS_TOKEN_URL')
        arn_url = os.environ.get('AWS_ARN_URL')
        if not token_url or not arn_url:
            return

        for attempt in range(1, retries + 1):
            try:
                token_resp = requests.get(token_url)
                token_resp.raise_for_status()
                token = token_resp.text.strip()

                creds_resp = requests.get(arn_url, headers={'Authorization': f'Bearer {token}'})
                creds_resp.raise_for_status()
                creds = creds_resp.json()

                os.environ['AWS_ACCESS_KEY_ID'] = creds['AccessKeyId']
                os.environ['AWS_SECRET_ACCESS_KEY'] = creds['SecretAccessKey']
                os.environ['AWS_SESSION_TOKEN'] = creds['SessionToken']
                _aws_creds_expires = now + ttl_minutes * 60
                return
            except Exception as e:
                if attempt < retries:
                    print(f"Warning: AWS credential renewal attempt {attempt}/{retries} failed: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"Warning: AWS credential renewal failed after {retries} attempts: {e}")


def athena_connect(data_base=None):
    """Connect to AWS Athena using environment variables.

    Uses AWS_DEFAULT_REGION, AWS_S3_WORK_GROUP, and AWS_S3_STAGING_DIR
    from the environment. None values are omitted so pyathena can fall
    back to its own defaults.
    """
    from pyathena import connect as athena_connect_raw

    kwargs = {
        'region_name': os.environ.get('AWS_DEFAULT_REGION'),
        'work_group': os.environ.get('AWS_S3_WORK_GROUP'),
        's3_staging_dir': os.environ.get('AWS_S3_STAGING_DIR'),
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return athena_connect_raw(schema_name=data_base, **kwargs)


# Athena date_trunc unit mappings for vintage bucketing
_VINTAGE_ATHENA = {
    'day': 'day',
    'week': 'week',
    'month': 'month',
    'quarter': 'quarter',
    'year': 'year',
}


def _athena_date_cast(date_expr, date_dtype):
    """Wrap date_expr with appropriate cast for Athena based on column type.

    - date/timestamp columns: use as-is (already date-compatible)
    - varchar/string columns: parse with date_parse assuming YYYYMMDD or YYYY-MM-DD
    - number columns: cast to varchar first, then date_parse
    """
    dtype = (date_dtype or '').lower()
    if 'date' in dtype or 'timestamp' in dtype:
        return date_expr
    if dtype in ('int', 'integer', 'bigint', 'smallint', 'tinyint', 'number'):
        return f"date_parse(CAST({date_expr} AS VARCHAR), '%Y%m%d')"
    # varchar/string: try ISO first, fall back to YYYYMMDD
    if 'char' in dtype or 'string' in dtype:
        return f"date_parse({date_expr}, '%Y%m%d')"
    return date_expr


def _vintage_date_expr_athena(date_expr, vintage, vintage_transform=None, date_dtype=None):
    """Wrap date_expr with Athena date_trunc for vintage bucketing.

    If vintage_transform is provided (from config JSON), use it directly.
    Otherwise fall back to date_trunc('unit', date_expr).
    Uses date_dtype to CAST string/number columns to date before date_trunc.
    """
    if vintage_transform:
        return vintage_transform.replace("{col}", date_expr)
    if vintage == 'all':
        return "'all'"
    if not vintage or vintage == 'day':
        return date_expr
    unit = _VINTAGE_ATHENA.get(vintage)
    if unit is None:
        return date_expr
    cast_expr = _athena_date_cast(date_expr, date_dtype)
    return f"date_trunc('{unit}', {cast_expr})"


def _discover_columns_athena(cursor, database, table):
    """Discover columns from information_schema."""
    sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{database}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}


def _extract_row_athena(cursor, tbl_cfg, outdir):
    """Extract row counts from Athena table. Returns (csv_path, elapsed_seconds)."""
    import time
    from datetime import datetime

    database = tbl_cfg['conn_macro']
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    qname = _qualified_name(tbl_cfg)
    where = tbl_cfg.get('where', '')

    raw_full_table = f"{database}.{table}"
    full_table, cte_prefix = _resolve_table(tbl_cfg, raw_full_table)
    date_expr = date_col
    where_clause = f"WHERE {where}" if where else ""

    sql = f"""{cte_prefix}
        SELECT {date_expr} AS date_value, COUNT(*) AS row_count
        FROM {full_table}
        {where_clause}
        GROUP BY {date_expr}
    """

    start_time = time.time()
    start_timestamp = datetime.now().isoformat()
    cursor.execute(sql)
    rows = cursor.fetchall()
    elapsed = time.time() - start_time
    end_timestamp = datetime.now().isoformat()

    csv_path = os.path.join(outdir, f"{qname}_row.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date_value', 'row_count'])
        for row in rows:
            writer.writerow([row[0], row[1]])

    print(f"  Row counts: {csv_path} ({len(rows)} dates, {elapsed:.1f}s)")
    return csv_path, elapsed, start_timestamp, end_timestamp


def _query_athena(sql, data_base=None):
    """Execute SQL on Athena via pandas read_sql_query with fresh connection.

    Renews credentials if needed, opens a new connection, runs the query,
    and returns a DataFrame with lowercase column names.
    """
    import pandas.io.sql as psql
    import warnings

    aws_creds_renew()
    conn = athena_connect(data_base=data_base)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df = psql.read_sql_query(sql, conn)
        df.columns = [c.lower() for c in df.columns]
        return df
    finally:
        conn.close()


_sql_log_file = None
_sql_log_lock = threading.Lock()


def _log_sql(col, sql):
    """Append SQL to log file (thread-safe). No console output."""
    if _sql_log_file:
        with _sql_log_lock:
            with open(_sql_log_file, 'a') as f:
                f.write(f"-- [{col}]\n{sql.strip()}\n;\n\n")


def _extract_col_athena(tbl_cfg, col, dtype, full_table, cte_prefix="", database=None, vintage=None, date_dtype=None):
    """Extract stats for a single column from Athena.

    Numeric columns: direct aggregation (no top-10).
    Categorical columns: single CTE query computing all stats server-side.
    When vintage != 'all', adds GROUP BY date_trunc to produce per-vintage rows.
    """
    date_col = tbl_cfg['date_col']
    where = tbl_cfg.get('where', '')
    if vintage is None:
        vintage = tbl_cfg.get('vintage', 'all')
    vintage_transform = tbl_cfg.get('vintage_transform', None)
    numeric = is_numeric_type(dtype, is_oracle=False)

    wc = where.strip() if where else "(1=1)"

    # Build date expression and GROUP BY for vintage bucketing
    if vintage == 'all':
        dt_select = f"'all' AS dt"
        group_by = ""
    else:
        dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype)
        dt_select = f"CAST({dt_expr} AS VARCHAR) AS dt"
        group_by = f"\nGROUP BY {dt_expr}"

    if numeric:
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{dtype}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col}) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    CAST(AVG(CAST({col} AS DOUBLE)) AS VARCHAR) AS mean,
    CAST(STDDEV_SAMP(CAST({col} AS DOUBLE)) AS VARCHAR) AS std,
    CAST(MIN({col}) AS VARCHAR) AS min_val,
    CAST(MAX({col}) AS VARCHAR) AS max_val
FROM {full_table} WHERE {wc}{group_by}"""

        _log_sql(col, sql)
        df = _query_athena(sql, data_base=database)
        results = []
        for _, rd in df.iterrows():
            rd = rd.to_dict()
            results.append({
                'dt': str(rd.get('dt', 'all')),
                'column_name': col,
                'col_type': 'numeric',
                'n_total': str(rd['n_total'] or 0),
                'n_missing': str(rd['n_missing'] or 0),
                'n_unique': str(rd['n_unique'] or 0),
                'mean': str(rd['mean']) if rd['mean'] else '',
                'std': str(rd['std']) if rd['std'] else '',
                'min_val': str(rd['min_val']) if rd['min_val'] is not None else '',
                'max_val': str(rd['max_val']) if rd['max_val'] is not None else '',
                'top_10': '',
            })
        return results

    # Categorical: single CTE query
    # For vintage != 'all', freq table includes dt grouping
    if vintage == 'all':
        freq_dt = ""
        freq_group = ""
        freq_partition = "ORDER BY value_freq DESC, p_col ASC"
        agg_group = ""
        agg_dt_select = "'all' AS dt"
    else:
        dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype)
        freq_dt = f"CAST({dt_expr} AS VARCHAR) AS dt,"
        freq_group = f", {dt_expr}"
        freq_partition = "PARTITION BY dt ORDER BY value_freq DESC, p_col ASC"
        agg_group = "\nGROUP BY dt"
        agg_dt_select = "dt"

    sql = f"""{cte_prefix}
WITH FreqTable_RAW AS (
    SELECT
        {freq_dt}
        {col} AS p_col,
        COUNT(*) AS value_freq
    FROM {full_table} WHERE {wc}
    GROUP BY {col}{freq_group}
), FreqTable AS (
    SELECT
        {'dt,' if vintage != 'all' else ''} p_col, value_freq,
        ROW_NUMBER() OVER ({freq_partition}) AS rn
    FROM FreqTable_RAW
)
SELECT
    {agg_dt_select},
    '{dtype}' AS col_type,
    SUM(value_freq) AS n_total,
    COUNT(value_freq) AS n_unique
FROM FreqTable{agg_group}"""

    _log_sql(col, sql)
    df = _query_athena(sql, data_base=database)

    results = []
    for _, rd in df.iterrows():
        rd = rd.to_dict()

        # Get top-10 for this dt via separate query (scalar subquery can't GROUP BY)
        dt_val = str(rd.get('dt', 'all'))
        if vintage == 'all':
            top10_sql = f"""{cte_prefix}
SELECT ARRAY_JOIN(ARRAY_AGG(entry ORDER BY cnt DESC), '; ') AS col_freq FROM (
    SELECT COALESCE(CAST({col} AS VARCHAR), '') || '(' || CAST(COUNT(*) AS VARCHAR) || ')' AS entry, COUNT(*) AS cnt
    FROM {full_table} WHERE {wc} AND {col} IS NOT NULL
    GROUP BY {col} ORDER BY COUNT(*) DESC LIMIT 10
) t"""
            missing_sql = f"""{cte_prefix}
SELECT COUNT(*) AS col_missing FROM {full_table} WHERE {wc} AND {col} IS NULL"""
        else:
            dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype)
            top10_sql = f"""{cte_prefix}
SELECT ARRAY_JOIN(ARRAY_AGG(entry ORDER BY cnt DESC), '; ') AS col_freq FROM (
    SELECT COALESCE(CAST({col} AS VARCHAR), '') || '(' || CAST(COUNT(*) AS VARCHAR) || ')' AS entry, COUNT(*) AS cnt
    FROM {full_table} WHERE {wc} AND {col} IS NOT NULL AND CAST({dt_expr} AS VARCHAR) = '{dt_val}'
    GROUP BY {col} ORDER BY COUNT(*) DESC LIMIT 10
) t"""
            missing_sql = f"""{cte_prefix}
SELECT COUNT(*) AS col_missing FROM {full_table} WHERE {wc} AND {col} IS NULL AND CAST({dt_expr} AS VARCHAR) = '{dt_val}'"""

        top10_df = _query_athena(top10_sql, data_base=database)
        col_freq = str(top10_df.iloc[0]['col_freq'] or '') if len(top10_df) > 0 else ''
        top_10 = col_freq if col_freq and col_freq != 'nan' else ''

        missing_df = _query_athena(missing_sql, data_base=database)
        col_missing = int(missing_df.iloc[0]['col_missing'] or 0) if len(missing_df) > 0 else 0

        results.append({
            'dt': dt_val,
            'column_name': col,
            'col_type': 'categorical',
            'n_total': str(rd['n_total'] or 0),
            'n_missing': str(col_missing),
            'n_unique': str(rd['n_unique'] or 0),
            'mean': '',
            'std': '',
            'min_val': '',
            'max_val': '',
            'top_10': top_10,
        })

    return results


def _extract_mock(config_path, outdir, types, db_path, mock_dir, source_filter):
    """Extract from mock CSV files instead of real database.

    Args:
        source_filter: Source type(s) to filter, e.g. ('pcds', 'oracle') or ('aws',)
    """
    import shutil

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)
    all_tables = _load_tables_from_config(config)
    tables = [t for t in all_tables if t.get('source', '').lower() in source_filter]
    if not tables:
        print(f"No {'/'.join(source_filter)} tables found in config")
        return

    is_aws = 'aws' in source_filter

    for tbl_cfg in tables:
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)

        if is_aws:
            database = tbl_cfg['conn_macro']
            table = tbl_cfg['table']
            label = f"{database}.{table}"
            mock_base = os.path.join(mock_dir, database, table)
            row_src, col_src = os.path.join(mock_base, 'row.csv'), os.path.join(mock_base, 'col.csv')
        else:
            table = tbl_cfg['table']
            label = table
            row_src = os.path.join(mock_dir, f"{table}_row.csv")
            col_src = os.path.join(mock_dir, f"{table}_col.csv")

        print(f"\n[mock] Extracting: {name} ({label})")

        for typ, src in [("row", row_src), ("col", col_src)]:
            if typ not in types:
                continue
            dst = os.path.join(outdir, f"{qname}_{typ}.csv")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                label_str = "Row counts" if typ == "row" else "Column stats"
                unit = "dates" if typ == "row" else "rows"
                print(f"  [mock] {label_str}: {dst} ({n_rows} {unit})")
            else:
                print(f"  [mock] File not found: {src}")

    print(f"\n[mock] Extraction complete. Output in: {outdir}")


# Backward-compatible wrappers
def _extract_oracle_mock(config_path, outdir, types, db_path, mock_dir):
    return _extract_mock(config_path, outdir, types, db_path, mock_dir, ('pcds', 'oracle'))

def _extract_aws_mock(config_path, outdir, types, db_path, mock_dir):
    return _extract_mock(config_path, outdir, types, db_path, mock_dir, ('aws',))


def extract_aws(config_path, outdir, types=None, max_workers=None, db_path=None, vintage=None, force=False):
    """
    Extract data directly from AWS Athena.

    Args:
        config_path: Path to extraction config JSON
        outdir: Directory to write CSV files
        types: List of types ("row", "col"). Default: both.
        max_workers: Max parallel workers for column extraction
        db_path: Optional database path to save discovered columns to _column_meta
        vintage: Date bucketing granularity (day, week, month, quarter, year)
    """
    if types is None:
        types = ["row", "col"]

    # Mock mode: read from pre-built CSVs
    mock_dir = os.environ.get('DTRACK_ATHENA_MOCK')
    if mock_dir:
        _extract_aws_mock(config_path, outdir, types, db_path, mock_dir)
        return

    try:
        import pyathena  # noqa: F401
    except ImportError:
        print("Error: pyathena is required for AWS extraction")
        print("Install with: pip install 'dtrack[aws]'")
        return

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    all_tables = _load_tables_from_config(config)

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    for tbl in aws_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    if db_path and "col" in types:
        _fill_columns_from_meta(aws_tables, db_path)

    aws_creds_renew()
    conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
    cursor = conn.cursor()

    # Track timing for all extractions
    timing_records = []

    # Confirm vintage for column extraction
    if "col" in types:
        # Collect unique vintages across tables
        table_vintages = set(t.get('vintage', 'all') for t in aws_tables)
        vintage_display = ', '.join(sorted(table_vintages))
        resp = input(f"  Column stats vintage: {vintage_display}. Proceed? [y]es / [c]hange: ").strip().lower()
        if resp == 'c':
            new_vintage = input(f"  Enter vintage (all/day/week/month/quarter/year/sample): ").strip().lower()
            if new_vintage in ('all', 'day', 'week', 'month', 'quarter', 'year', 'sample'):
                for t in aws_tables:
                    t['vintage'] = new_vintage
                print(f"  Vintage set to: {new_vintage}")
            else:
                print(f"  Invalid vintage '{new_vintage}', keeping original")

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        database = tbl_cfg['conn_macro']
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        raw_full_table = f"{database}.{table}"
        full_table, cte_prefix = _resolve_table(tbl_cfg, raw_full_table)

        print(f"\nExtracting: {name} ({raw_full_table})")

        if "row" in types:
            aws_creds_renew()
            csv_path, elapsed, start_ts, end_ts = _extract_row_athena(cursor, tbl_cfg, outdir)
            timing_records.append({
                'table': name,
                'step': 'row',
                'start': start_ts,
                'end': end_ts,
                'elapsed_sec': elapsed,
            })

        if "col" in types:
            import time
            from datetime import datetime

            columns = tbl_cfg.get('columns', {})
            if not columns:
                print(f"  WARNING: No columns specified for {name}. "
                      f"Use 'dtrack load-columns --source aws' or provide columns in config.")
                continue

            # Extract column stats
            from concurrent.futures import ThreadPoolExecutor, as_completed

            all_stats = []
            non_date_cols = [(c, d) for c, d in columns.items() if c.lower() != date_col.lower()]

            # Compute unified date filter
            table_vintage = tbl_cfg.get('vintage', vintage)
            date_filter = _compute_date_filter(tbl_cfg, db_path, table_vintage)

            if date_filter['filter_type'] == 'in_list':
                date_where = _build_date_in_clause(
                    date_col, date_filter['dates'], date_filter['date_dtype'])
                existing_where = tbl_cfg.get('where', '')
                if existing_where:
                    tbl_cfg['where'] = f"({existing_where}) AND {date_where}"
                else:
                    tbl_cfg['where'] = date_where
            elif date_filter['filter_type'] == 'between':
                date_where = _build_date_between_clause(
                    date_col, date_filter['min_date'], date_filter['max_date'],
                    date_filter['date_dtype'])
                existing_where = tbl_cfg.get('where', '')
                if existing_where:
                    tbl_cfg['where'] = f"({existing_where}) AND {date_where}"
                else:
                    tbl_cfg['where'] = date_where

            col_vintage = date_filter['vintage']

            # Set up SQL log file for this table
            global _sql_log_file
            log_path = os.path.join(outdir, f"{qname}_col.sql")
            with open(log_path, 'w') as f:
                f.write(f"-- SQL queries for {qname} col extraction\n-- {datetime.now().isoformat()}\n\n")
            _sql_log_file = log_path

            col_start_time = time.time()
            col_start_ts = datetime.now().isoformat()

            # Priority: CLI arg > MAX_WORKERS env var > default 4
            _max_workers = max_workers or (int(os.environ['MAX_WORKERS']) if os.environ.get('MAX_WORKERS') else 4)

            try:
                from tqdm import tqdm
            except ImportError:
                tqdm = None

            if _max_workers <= 1:
                # Serial path: simpler, no thread overhead, clean output
                col_iter = non_date_cols
                if tqdm is not None:
                    col_iter = tqdm(col_iter, desc=f"  {name} col stats", unit="col")
                for col, dtype in col_iter:
                    try:
                        stats = _extract_col_athena(tbl_cfg, col, dtype, full_table, cte_prefix, database=database, vintage=col_vintage, date_dtype=date_filter.get('date_dtype'))
                        all_stats.extend(stats)
                    except Exception as e:
                        print(f"  Warning: Failed to extract {col}: {e}")
            else:
                # Parallel path
                def _extract_one(col_dtype):
                    col, dtype = col_dtype
                    return _extract_col_athena(tbl_cfg, col, dtype, full_table, cte_prefix, database=database, vintage=col_vintage, date_dtype=date_filter.get('date_dtype'))

                with ThreadPoolExecutor(max_workers=_max_workers) as executor:
                    futures = {executor.submit(_extract_one, (c, d)): c for c, d in non_date_cols}
                    completed_iter = as_completed(futures)
                    if tqdm is not None:
                        completed_iter = tqdm(completed_iter, total=len(futures),
                                             desc=f"  {name} col stats", unit="col")
                    for future in completed_iter:
                        col_name = futures[future]
                        try:
                            stats = future.result()
                            all_stats.extend(stats)
                        except Exception as e:
                            print(f"  Warning: Failed to extract {col_name}: {e}")

            _sql_log_file = None
            print(f"  SQL log: {log_path}")
            col_elapsed = time.time() - col_start_time
            col_end_ts = datetime.now().isoformat()

            # Write CSV
            if all_stats:
                qname = _qualified_name(tbl_cfg)
                csv_path = os.path.join(outdir, f"{qname}_col.csv")
                fieldnames = [
                    'column_name', 'dt', 'col_type', 'n_total', 'n_missing',
                    'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
                ]
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for stat in sorted(all_stats, key=lambda x: (x['column_name'], x['dt'])):
                        writer.writerow({k: stat.get(k, '') for k in fieldnames})

                print(f"  Column stats: {csv_path} ({len(all_stats)} rows, {col_elapsed:.1f}s)")

                timing_records.append({
                    'table': name,
                    'step': 'col',
                    'start': col_start_ts,
                    'end': col_end_ts,
                    'elapsed_sec': col_elapsed,
                })

    cursor.close()
    conn.close()

    # Export timing log
    if timing_records:
        timing_path = os.path.join(outdir, '_timing.csv')
        with open(timing_path, 'w', newline='') as f:
            fieldnames = ['table', 'step', 'start', 'end', 'elapsed_sec']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(timing_records)
        print(f"\n✓ Timing log: {timing_path}")

    print(f"\nExtraction complete. Output in: {outdir}")


def discover_aws_columns(config_path, outdir, db_path=None):
    """
    Discover column metadata from AWS Athena tables (columns only, no data).

    Args:
        config_path: Path to extraction config JSON (supports both old and unified formats)
        outdir: Directory to write column CSV files
        db_path: Optional database path to save columns to _column_meta
    """
    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    all_tables = _load_tables_from_config(config)

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    # Mock mode: read from pre-built CSVs
    mock_dir = os.environ.get('DTRACK_ATHENA_MOCK')
    if mock_dir:
        _extract_aws_mock(config_path, outdir, [], db_path, mock_dir)
        # discover-only just needs columns
        for tbl_cfg in aws_tables:
            name = tbl_cfg['name']
            qname = _qualified_name(tbl_cfg)
            database = tbl_cfg['conn_macro']
            table = tbl_cfg['table']
            tbl_mock_dir = os.path.join(mock_dir, database, table)
            cols_src = os.path.join(tbl_mock_dir, 'columns.csv')
            if os.path.exists(cols_src):
                import shutil
                cols_dst = os.path.join(outdir, f"{qname}_columns.csv")
                shutil.copy2(cols_src, cols_dst)
                print(f"  [mock] Column metadata: {cols_dst}")
                if db_path:
                    columns = {}
                    with open(cols_src, 'r', newline='') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            col_name = row.get('column_name') or row.get('COLUMN_NAME', '')
                            col_type = row.get('data_type') or row.get('DATA_TYPE', '')
                            if col_name:
                                columns[col_name] = col_type
                    if columns:
                        from .db import insert_column_meta
                        insert_column_meta(db_path, qname, columns, source='aws')
                        print(f"  [mock] Saved to _column_meta in {db_path}")
            else:
                print(f"  [mock] File not found: {cols_src}")
        print(f"\n[mock] Discovery complete. Output in: {outdir}")
        return

    aws_creds_renew()
    conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
    cursor = conn.cursor()

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)
        database = tbl_cfg['conn_macro']
        table = tbl_cfg['table']

        print(f"\nDiscovering columns: {name} ({database}.{table})")
        columns = _discover_columns_athena(cursor, database, table)

        csv_path = os.path.join(outdir, f"{qname}_columns.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['COLUMN_NAME', 'DATA_TYPE'])
            for col_name, col_type in sorted(columns.items()):
                writer.writerow([col_name, col_type])

        print(f"  {csv_path} ({len(columns)} columns)")

        if db_path:
            from .db import insert_column_meta
            insert_column_meta(db_path, qname, columns, source='aws')
            print(f"  Saved to _column_meta in {db_path}")

    cursor.close()
    conn.close()
    print(f"\nDiscovery complete. Output in: {outdir}")


def match_columns_from_dicts(left_cols, right_cols, left_label="left", right_label="right", outfile=None):
    """
    Compare two column dictionaries and match by case-insensitive name.

    Args:
        left_cols: Dict mapping column names to data types
        right_cols: Dict mapping column names to data types
        left_label: Label for left source
        right_label: Label for right source
        outfile: Optional path to write JSON output
    """
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
        with open(outfile, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nWritten to: {outfile}")

    return result


