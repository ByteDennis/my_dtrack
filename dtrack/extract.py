"""Data extraction: SAS code generation for Oracle, direct AWS Athena extraction"""

import csv
import json
import os


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


def _build_col_where(tbl_cfg, db_path, vintage='day'):
    """Build a WHERE clause fragment filtering to matching dates from _row_comparison.

    Looks up which pair this table belongs to, loads saved comparison results,
    and builds a date IN (...) filter. For vintage='sample', picks N random dates.

    Returns (where_fragment, effective_vintage) or (None, vintage) if no results found.
    """
    import random

    if not db_path:
        return None, vintage

    from .db import list_table_pairs, get_row_comparison

    qname = _qualified_name(tbl_cfg)
    date_col = tbl_cfg.get('date_col', '')

    # Find which pair this table belongs to
    pairs = list_table_pairs(db_path)
    pair_name = None
    for pair in pairs:
        if qname in (pair['table_left'], pair['table_right']):
            pair_name = pair['pair_name']
            break

    if not pair_name:
        return None, vintage

    comp = get_row_comparison(db_path, pair_name)
    if not comp or not comp.get('matching_dates'):
        return None, vintage

    matching_dates = comp['matching_dates']

    if vintage == 'sample':
        from .db import get_sampled_dates, save_sampled_dates

        n_sample = int(os.environ.get('N_SAMPLE', '100'))
        seed = int(os.environ.get('SEED', '2025'))

        if len(matching_dates) > n_sample:
            # Check if samples already exist in database
            existing_samples = get_sampled_dates(db_path, pair_name, qname)

            # Sample with seed for reproducibility
            random.seed(seed)
            new_samples = sorted(random.sample(matching_dates, n_sample))

            if existing_samples:
                # Verify samples match (scope hasn't changed)
                if set(existing_samples) == set(new_samples):
                    print(f"  {qname}: using existing {len(existing_samples)} sampled dates (seed={seed})")
                    matching_dates = existing_samples
                else:
                    print(f"  ⚠️  {qname}: scope changed! Existing {len(existing_samples)} samples don't match new {len(new_samples)}")
                    print(f"  {qname}: saving new sample (seed={seed}, N_SAMPLE={n_sample})")
                    save_sampled_dates(db_path, pair_name, qname, new_samples)
                    matching_dates = new_samples
            else:
                # Save new samples
                print(f"  {qname}: sampling {n_sample} from {len(matching_dates)} matching dates (seed={seed}) → 1 vintage")
                save_sampled_dates(db_path, pair_name, qname, new_samples)
                matching_dates = new_samples
        effective_vintage = 'day'
    else:
        effective_vintage = vintage

        # Count unique vintage buckets
        from .date_utils import bucket_date
        unique_buckets = len(set(bucket_date(dt, vintage) for dt in matching_dates))

        print(f"  {qname}: using vintage='{vintage}' with {len(matching_dates)} matching dates → {unique_buckets} vintages")

    # Build IN clause - format based on date column type
    from .db import get_column_meta

    # Look up date column type from _column_meta
    col_meta = get_column_meta(db_path, qname)
    date_dtype = None
    for cm in col_meta:
        if cm['column_name'].upper() == date_col.upper():
            date_dtype = (cm.get('data_type') or '').upper()
            break

    # Format dates based on type
    if date_dtype and date_dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        # Integer date (e.g., YYYYMM like 202501) - no quotes!
        date_list = ", ".join(str(d) for d in matching_dates)
        where_fragment = f"{date_col} IN ({date_list})"
    elif date_dtype and 'TIMESTAMP' in date_dtype:
        # TIMESTAMP - use TRUNC to compare at day granularity
        date_list = ", ".join(f"DATE '{d}'" for d in matching_dates)
        where_fragment = f"TRUNC({date_col}) IN ({date_list})"
    elif date_dtype and 'DATE' in date_dtype:
        # DATE type - use TRUNC to handle time components (Oracle DATE includes time!)
        # Without TRUNC, only matches rows at exactly midnight (00:00:00)
        date_list = ", ".join(f"DATE '{d}'" for d in matching_dates)
        where_fragment = f"TRUNC({date_col}) IN ({date_list})"
    elif date_dtype and date_dtype.startswith('CHAR'):
        # CHAR - TRIM to handle trailing spaces
        date_list = ", ".join(f"'{d}'" for d in matching_dates)
        where_fragment = f"TRIM({date_col}) IN ({date_list})"
    else:
        # VARCHAR or unknown - use string literals (safe default)
        date_list = ", ".join(f"'{d}'" for d in matching_dates)
        where_fragment = f"{date_col} IN ({date_list})"

    return where_fragment, effective_vintage


def is_numeric_type(data_type, is_oracle=True):
    """Check if a data type string represents a numeric column."""
    dt_upper = data_type.upper().split('(')[0].strip()
    if is_oracle:
        return dt_upper in ORACLE_NUMERIC_TYPES
    else:
        return dt_upper.lower() in ATHENA_NUMERIC_TYPES


def build_continuous_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for continuous/numeric column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    AVG({col}) AS mean,
    STDDEV({col}) AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}""".strip()


def build_categorical_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for categorical column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'categorical' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    NULL AS mean,
    NULL AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}""".strip()


def build_continuous_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for continuous/numeric column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col}) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    AVG(CAST({col} AS DOUBLE)) AS mean,
    STDDEV(CAST({col} AS DOUBLE)) AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}"""


def build_categorical_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for categorical column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'categorical' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    NULL AS mean,
    NULL AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}"""


def build_top10_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for top 10 frequency values per date."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col} AS dt, CAST({col} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col} IS NOT NULL {where_clause}
    GROUP BY {date_col}, {col}
) WHERE rn <= 10
""".strip()


def build_top10_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for top 10 frequency values per date."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col} AS dt, CAST({col} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col} IS NOT NULL {where_clause}
    GROUP BY {date_col}, {col}
) t WHERE rn <= 10"""


def parse_stats_row(row):
    """Normalize a query result row to standard dict format."""
    return {
        'dt': str(row['dt']),
        'column_name': row['column_name'],
        'col_type': row['col_type'],
        'n_total': int(row['n_total']) if row['n_total'] is not None else 0,
        'n_missing': int(row['n_missing']) if row['n_missing'] is not None else 0,
        'n_unique': int(row['n_unique']) if row['n_unique'] is not None else 0,
        'mean': float(row['mean']) if row['mean'] is not None else None,
        'std': float(row['std']) if row['std'] is not None else None,
        'min_val': str(row['min_val']) if row['min_val'] is not None else None,
        'max_val': str(row['max_val']) if row['max_val'] is not None else None,
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
    'week':    "intnx('week', {col}, 0, 'b')",
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
    if not vintage or vintage == 'day':
        return date_expr
    fmt = _VINTAGE_TRUNC.get(vintage)
    if fmt is None:
        return date_expr
    return f"TRUNC({date_expr}, '{fmt}')"


def _sas_quote(s):
    """Escape single quotes for SAS by doubling them.

    SAS uses '' (two single quotes) to represent a literal single quote.
    This is the standard SAS quoting mechanism - simple and robust.

    Example: "WHERE STATUS = 'A'" becomes "WHERE STATUS = ''A''"
    """
    return s.replace("'", "''")


def _gen_sas_row_datadriven(pcds_tables, sas_lib='WORK', out_dir='.'):
    """Generate data-driven SAS row extraction using table_date_map + call execute.

    Builds mapping datasets and reusable macros for two modes:
    - Oracle tables: proc sql passthrough to Oracle
    - SAS tables ($-prefixed processed): proc sql on local SAS dataset

    Caches results: skips tables whose output dataset already exists.
    Supports 'processed' config field as CTE (Oracle) or SAS dataset reference ($).
    """
    lines = []

    # Split tables into Oracle vs SAS ($-prefixed processed)
    oracle_datalines = []
    sas_datalines = []

    for idx, tbl_cfg in enumerate(pcds_tables, 1):
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)
        conn_macro = tbl_cfg.get('conn_macro', 'pcds')
        where = _sas_quote(tbl_cfg.get('where', ''))
        transform = tbl_cfg.get('date_transform', '')
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)

        safe_ds = _sas_safe_name(qname, 29)

        if processed and processed.startswith('$'):
            # SAS dataset mode: $ prefix means local SAS table
            sas_table = processed[1:].strip()  # remove $ prefix
            date_expr = _sas_date_transform(date_col, transform) if transform else date_col
            sas_datalines.append(
                f"{sas_table}|{safe_ds}|{qname}|{date_expr}|{where}"
            )
        else:
            # Oracle mode
            date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
            if processed:
                table = name  # CTE alias
            oracle_datalines.append(
                f"{table}|{safe_ds}|{qname}|{date_expr}|{conn_macro}|{idx}|{where}"
            )

    # Emit CTE %let statements for Oracle processed tables
    for idx, tbl_cfg in enumerate(pcds_tables, 1):
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)
        if processed and not processed.startswith('$'):
            name = tbl_cfg['name']
            lines.append(f"%let _cte{idx} = WITH {name} AS ({processed});")

    # REDO flag
    redo = int(os.environ.get('SAS_ROW_REDO', '0'))
    lines.append(f"%let _row_redo = {redo};")
    lines.append("")

    # ── Oracle tables ──
    if oracle_datalines:
        lines.append("/* Oracle row extraction (passthrough) */")
        lines.append("data _ora_map;")
        lines.append("    length table $128 dsname $32 qname $64 date_expr $200 conn_macro $32 idx $4 where_clause $500;")
        lines.append("    infile datalines dlm='|' truncover;")
        lines.append("    input table $ dsname $ qname $ date_expr $ conn_macro $ idx $ where_clause $;")
        lines.append("    datalines;")
        for dl in oracle_datalines:
            lines.append(dl)
        lines.append(";")
        lines.append("run;")
        lines.append("")

        lines.append("%macro _row_oracle(table=, dsname=, qname=, date_expr=, conn_macro=, where_clause=, idx=);")
        lines.append("    %local _outpath _cte_val;")
        lines.append('    %let _outpath = &out_dir./&qname._row.csv;')
        lines.append("")
        lines.append("    %if &_row_redo = 0 and %sysfunc(exist(cache.rc_&dsname)) %then %do;")
        lines.append("        %put NOTE: Cached rc_&dsname found - skipping;")
        lines.append('        proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;')
        lines.append("        %return;")
        lines.append("    %end;")
        lines.append("")
        lines.append("    %if %symexist(_cte&idx) %then %let _cte_val = &&_cte&idx;")
        lines.append("    %else %let _cte_val = ;")
        lines.append("")
        lines.append("    %start_timer();")
        lines.append("    proc sql;")
        lines.append("        %&conn_macro")
        lines.append("        create table cache.rc_&dsname as")
        lines.append("        select * from connection to oracle (")
        lines.append("            &_cte_val")
        lines.append("            select &date_expr as date_value, count(*) as row_count")
        lines.append("            from &table")
        lines.append("            %if %length(&where_clause) > 0 %then where &where_clause;")
        lines.append("            group by &date_expr")
        lines.append("        );")
        lines.append("        disconnect from oracle;")
        lines.append("    quit;")
        lines.append("")
        lines.append('    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;')
        lines.append("    %log_time(table=&qname, step=row, outpath=&out_dir.);")
        lines.append("%mend _row_oracle;")
        lines.append("")

        lines.append("data _null_;")
        lines.append("    set _ora_map;")
        lines.append("    length _cmd $2000;")
        lines.append("    _cmd = cats(")
        lines.append("        '%nrstr(%_row_oracle)(',")
        lines.append("        'table=', strip(table),")
        lines.append("        ', dsname=', strip(dsname),")
        lines.append("        ', qname=', strip(qname),")
        lines.append("        ', date_expr=', strip(date_expr),")
        lines.append("        ', conn_macro=', strip(conn_macro),")
        lines.append("        ', where_clause=', strip(where_clause),")
        lines.append("        ', idx=', strip(idx),")
        lines.append("        ')'")
        lines.append("    );")
        lines.append("    call execute(_cmd);")
        lines.append("run;")
        lines.append("proc delete data=_ora_map; run;")
        lines.append("")

    # ── SAS dataset tables ($-prefixed) ──
    if sas_datalines:
        lines.append("/* SAS dataset row extraction (local proc sql) */")
        lines.append("data _sas_map;")
        lines.append("    length table $128 dsname $32 qname $64 date_expr $200 where_clause $500;")
        lines.append("    infile datalines dlm='|' truncover;")
        lines.append("    input table $ dsname $ qname $ date_expr $ where_clause $;")
        lines.append("    datalines;")
        for dl in sas_datalines:
            lines.append(dl)
        lines.append(";")
        lines.append("run;")
        lines.append("")

        lines.append("%macro _row_sas(table=, dsname=, qname=, date_expr=, where_clause=);")
        lines.append("    %local _outpath;")
        lines.append('    %let _outpath = &out_dir./&qname._row.csv;')
        lines.append("")
        lines.append("    %if &_row_redo = 0 and %sysfunc(exist(cache.rc_&dsname)) %then %do;")
        lines.append("        %put NOTE: Cached rc_&dsname found - skipping;")
        lines.append('        proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;')
        lines.append("        %return;")
        lines.append("    %end;")
        lines.append("")
        lines.append("    %start_timer();")
        lines.append("    proc sql;")
        lines.append("        create table cache.rc_&dsname as")
        lines.append("        select &date_expr as date_value, count(*) as row_count")
        lines.append("        from &table")
        lines.append("        %if %length(&where_clause) > 0 %then where &where_clause;")
        lines.append("        group by &date_expr;")
        lines.append("    quit;")
        lines.append("")
        lines.append('    proc export data=cache.rc_&dsname outfile="&_outpath" dbms=csv replace; run;')
        lines.append("    %log_time(table=&qname, step=row, outpath=&out_dir.);")
        lines.append("%mend _row_sas;")
        lines.append("")

        lines.append("data _null_;")
        lines.append("    set _sas_map;")
        lines.append("    length _cmd $2000;")
        lines.append("    _cmd = cats(")
        lines.append("        '%nrstr(%_row_sas)(',")
        lines.append("        'table=', strip(table),")
        lines.append("        ', dsname=', strip(dsname),")
        lines.append("        ', qname=', strip(qname),")
        lines.append("        ', date_expr=', strip(date_expr),")
        lines.append("        ', where_clause=', strip(where_clause),")
        lines.append("        ')'")
        lines.append("    );")
        lines.append("    call execute(_cmd);")
        lines.append("run;")
        lines.append("proc delete data=_sas_map; run;")
        lines.append("")

    return '\n'.join(lines)


def _gen_sas_col_local(tbl_cfg, db_path=None, sas_lib='WORK', out_dir='.'):
    """Generate SAS macro for column statistics using local compute.

    Pulls raw data per vintage bucket, computes stats via PROC FREQ/MEANS,
    saves computed stats to cache library. On re-run, skips vintages whose
    cached stats already exist (unless SAS_COL_REDO=1).
    """
    table = tbl_cfg['table']
    processed = tbl_cfg.get('processed')
    if isinstance(processed, list):
        processed = " ".join(processed)
    cte_prefix = ""
    is_sas_table = processed and processed.startswith('$')
    if is_sas_table:
        table = processed[1:].strip()  # SAS dataset path
    elif processed:
        alias = tbl_cfg['name']
        cte_prefix = f"WITH {alias} AS ({processed}) "
        table = alias
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    sn = _sas_safe_name(name)  # truncated name for SAS identifiers
    qname = _qualified_name(tbl_cfg)
    conn_macro = tbl_cfg.get('conn_macro', 'pcds')
    user_override = tbl_cfg.get('user', '')
    where = tbl_cfg.get('where', '')
    columns = tbl_cfg.get('columns', {})
    transform = tbl_cfg.get('date_transform', '')
    vintage = tbl_cfg.get('vintage', 'day')

    lines = []
    lines.append(f"%macro get_colstats_{sn}();")

    if not columns:
        lines.append(f"    %put WARNING: No columns specified for {name}.;")
        lines.append(f"%mend get_colstats_{sn};")
        return '\n'.join(lines)

    # Filter out date column
    col_list = [(col, dtype) for col, dtype in columns.items()
                if col.upper() != date_col.upper()]
    n_cols = len(col_list)

    if n_cols == 0:
        lines.append(f"    %put WARNING: No non-date columns to extract for {name};")
        lines.append(f"%mend get_colstats_{sn};")
        return '\n'.join(lines)

    num_cols = [(col, dtype) for col, dtype in col_list if is_numeric_type(dtype, is_oracle=True)]
    cat_cols = [(col, dtype) for col, dtype in col_list if not is_numeric_type(dtype, is_oracle=True)]

    # Get matching dates and bucket them by vintage
    vintage_buckets = {}
    if db_path:
        where_fragment, effective_vintage = _build_col_where(tbl_cfg, db_path, vintage)
        if where_fragment:
            from .db import get_row_comparison, list_table_pairs
            pairs = list_table_pairs(db_path)
            pair_name = None
            for pair in pairs:
                if qname in (pair['table_left'], pair['table_right']):
                    pair_name = pair['pair_name']
                    break
            if pair_name:
                comp = get_row_comparison(db_path, pair_name)
                if comp and comp.get('matching_dates'):
                    from .date_utils import bucket_date
                    matching_dates = comp['matching_dates']
                    for dt in matching_dates:
                        bucket = bucket_date(dt, effective_vintage)
                        if bucket not in vintage_buckets:
                            vintage_buckets[bucket] = []
                        vintage_buckets[bucket].append(dt)

    if is_sas_table:
        date_expr = _sas_date_transform(date_col, transform) if transform else date_col
    else:
        date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
    where_fragment_str = f"AND ({where})" if where else ""
    _ua = f", user=&{user_override}_usr, pwd=&{user_override}_pwd" if user_override else ""

    # REDO flag from environment (default 0 = use cache)
    redo = int(os.environ.get('SAS_COL_REDO', '0'))

    lines.append(f"    %put NOTE: ===== LOCAL COMPUTE EXTRACTION ====;")
    lines.append(f"    %put NOTE: Table: {name} ({table});")
    lines.append(f"    %put NOTE: Columns: {len(num_cols)} numeric + {len(cat_cols)} categorical = {n_cols} total;")
    lines.append(f"    %put NOTE: REDO={redo} (1=force re-pull, 0=use cached stats);")

    def _emit_vintage_compute(lines, v_idx, raw_ds, suffix):
        """Emit PROC FREQ/MEANS compute block for one vintage. Stats go to cache._cs_{sn}_v{idx}."""
        cache_ds = f"cache._cs_{sn}_v{v_idx}"

        # Numeric columns: PROC MEANS
        for col, dtype in num_cols:
            col_safe = col.replace("'", "''")
            lines.append(f"    proc means data={raw_ds} noprint;")
            lines.append(f"        class dt;")
            lines.append(f"        var {col};")
            lines.append(f"        output out=_means_{col}{suffix}(where=(_type_=1))")
            lines.append(f"            n={col}_n nmiss={col}_nmiss mean={col}_mean")
            lines.append(f"            std={col}_std min={col}_min max={col}_max;")
            lines.append(f"    run;")
            lines.append(f"    proc sql noprint;")
            lines.append(f"        create table _nuniq_{col}{suffix} as")
            lines.append(f"        select dt, count(distinct {col}) as {col}_n_unique")
            lines.append(f"        from {raw_ds} group by dt;")
            lines.append(f"    quit;")
            lines.append(f"    data _sn_{col}{suffix};")
            lines.append(f"        merge _means_{col}{suffix} _nuniq_{col}{suffix};")
            lines.append(f"        by dt;")
            lines.append(f"        length column_name $32 col_type $12 min_val $100 max_val $100 top_10 $2000;")
            lines.append(f"        column_name = '{col_safe}'; col_type = 'numeric';")
            lines.append(f"        n_total = {col}_n + {col}_nmiss; n_missing = {col}_nmiss;")
            lines.append(f"        n_unique = {col}_n_unique; mean = {col}_mean; std = {col}_std;")
            lines.append(f"        min_val = strip(put({col}_min, best32.));")
            lines.append(f"        max_val = strip(put({col}_max, best32.)); top_10 = '';")
            lines.append(f"        keep dt column_name col_type n_total n_missing n_unique mean std min_val max_val top_10;")
            lines.append(f"    run;")
            lines.append("")

        # Categorical columns: PROC FREQ → stats + top-10
        for col, dtype in cat_cols:
            col_safe = col.replace("'", "''")
            lines.append(f"    proc freq data={raw_ds} noprint;")
            lines.append(f"        tables dt * {col} / out=_freq_{col}{suffix} sparse;")
            lines.append(f"    run;")
            lines.append(f"    proc sql noprint;")
            lines.append(f"        create table _sc_{col}{suffix} as")
            lines.append(f"        select dt, '{col_safe}' as column_name length=32,")
            lines.append(f"            'categorical' as col_type length=12,")
            lines.append(f"            sum(count) as n_total,")
            lines.append(f"            sum(case when {col} is missing then count else 0 end) as n_missing,")
            lines.append(f"            count(distinct case when {col} is not missing then {col} end) as n_unique,")
            lines.append(f"            . as mean, . as std,")
            lines.append(f"            '' as min_val length=100, '' as max_val length=100,")
            lines.append(f"            '' as top_10 length=2000")
            lines.append(f"        from _freq_{col}{suffix} group by dt;")
            lines.append(f"    quit;")
            # Top-10
            lines.append(f"    proc sort data=_freq_{col}{suffix}; by dt descending count; run;")
            lines.append(f"    data _t10_{col}{suffix}(keep=dt top_10);")
            lines.append(f"        length top_10 $2000 _entry $200;")
            lines.append(f"        set _freq_{col}{suffix}; by dt;")
            lines.append(f"        retain top_10 _rn;")
            lines.append(f"        if first.dt then do; top_10 = ''; _rn = 0; end;")
            lines.append(f"        if {col} is not missing and _rn < 10 then do;")
            lines.append(f"            _rn + 1;")
            lines.append(f"            _entry = catx('', strip(vvalue({col})), '(', strip(put(count, best.)), ')');")
            lines.append(f"            if top_10 = '' then top_10 = _entry;")
            lines.append(f"            else top_10 = catx('; ', top_10, _entry);")
            lines.append(f"        end;")
            lines.append(f"        if last.dt then output;")
            lines.append(f"    run;")
            # Merge top_10
            lines.append(f"    proc sort data=_sc_{col}{suffix}; by dt; run;")
            lines.append(f"    proc sort data=_t10_{col}{suffix}; by dt; run;")
            lines.append(f"    data _sc_{col}{suffix};")
            lines.append(f"        merge _sc_{col}{suffix}(in=a) _t10_{col}{suffix}(in=b rename=(top_10=_top10));")
            lines.append(f"        by dt; if a; if b then top_10 = _top10; drop _top10;")
            lines.append(f"    run;")
            # Min/max
            lines.append(f"    proc sql noprint;")
            lines.append(f"        create table _mm_{col}{suffix} as")
            lines.append(f"        select dt,")
            lines.append(f"            min(case when {col} is not missing then {col} end) as min_val length=100,")
            lines.append(f"            max(case when {col} is not missing then {col} end) as max_val length=100")
            lines.append(f"        from {raw_ds} group by dt;")
            lines.append(f"    quit;")
            lines.append(f"    data _sc_{col}{suffix};")
            lines.append(f"        merge _sc_{col}{suffix}(in=a drop=min_val max_val) _mm_{col}{suffix}(in=b);")
            lines.append(f"        by dt; if a;")
            lines.append(f"    run;")
            lines.append("")

        # Stack all column stats for this vintage → save to cache
        stat_datasets = [f"_sn_{col}{suffix}" for col, _ in num_cols] + \
                         [f"_sc_{col}{suffix}" for col, _ in cat_cols]
        if stat_datasets:
            lines.append(f"    data {cache_ds};")
            lines.append(f"        set {' '.join(stat_datasets)};")
            lines.append(f"    run;")
            lines.append("")

        # Cleanup raw data + all intermediates from WORK
        lines.append(f"    proc delete data={raw_ds}; run;")
        cleanup = [f"_means_{col}{suffix}" for col, _ in num_cols] + \
                  [f"_nuniq_{col}{suffix}" for col, _ in num_cols] + \
                  [f"_sn_{col}{suffix}" for col, _ in num_cols] + \
                  [f"_freq_{col}{suffix}" for col, _ in cat_cols] + \
                  [f"_sc_{col}{suffix}" for col, _ in cat_cols] + \
                  [f"_t10_{col}{suffix}" for col, _ in cat_cols] + \
                  [f"_mm_{col}{suffix}" for col, _ in cat_cols]
        if cleanup:
            lines.append(f"    proc datasets lib=work nolist; delete {' '.join(cleanup)}; quit;")
        lines.append("")

    if vintage_buckets:
        n_vintages = len(vintage_buckets)
        lines.append(f"    %put NOTE: Vintages: {n_vintages} buckets (vintage={vintage});")
        lines.append("")

        # Look up date column type for IN clause formatting
        from .db import get_column_meta
        col_meta = get_column_meta(db_path, qname) if db_path else []
        date_dtype = None
        for cm in col_meta:
            if cm['column_name'].upper() == date_col.upper():
                date_dtype = (cm.get('data_type') or '').upper()
                break

        for v_idx, (bucket_key, dates) in enumerate(sorted(vintage_buckets.items()), 1):
            cache_ds = f"cache._cs_{sn}_v{v_idx}"
            suffix = f"_v{v_idx}"

            lines.append(f"    /* ===== Vintage {v_idx}/{n_vintages}: {bucket_key} ({len(dates)} dates) ===== */")

            # Skip if cached stats exist and REDO=0
            if not redo:
                lines.append(f"    %if %sysfunc(exist({cache_ds})) %then %do;")
                lines.append(f"        %put NOTE: Cached stats found: {cache_ds} — skipping;")
                lines.append(f"    %end;")
                lines.append(f"    %else %do;")

            # Build IN clause for this vintage's dates
            if date_dtype and date_dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
                date_list = ", ".join(str(d) for d in dates)
                date_where = f"{date_col} IN ({date_list})"
            elif date_dtype and ('TIMESTAMP' in date_dtype or 'DATE' in date_dtype):
                date_list = ", ".join(f"DATE '{d}'" for d in dates)
                date_where = f"TRUNC({date_col}) IN ({date_list})"
            elif date_dtype and date_dtype.startswith('CHAR'):
                date_list = ", ".join(f"'{d}'" for d in dates)
                date_where = f"TRIM({date_col}) IN ({date_list})"
            else:
                date_list = ", ".join(f"'{d}'" for d in dates)
                date_where = f"{date_col} IN ({date_list})"

            # Pull raw data into WORK (temporary)
            col_select = ", ".join([date_expr + " AS dt"] + [col for col, _ in col_list])
            raw_ds = f"_raw_{sn}{suffix}"

            if is_sas_table:
                # SAS dataset: direct proc sql (no Oracle passthrough)
                sas_where = f"WHERE {date_where}"
                if where_fragment_str:
                    sas_where += f" {where_fragment_str}"
                pull_sql = f"SELECT {col_select} FROM {table} {sas_where}"
                lines.append(f"    proc sql; create table {raw_ds} as {pull_sql}; quit;")
            else:
                # Oracle: passthrough via %pull_data
                pull_sql = f"{cte_prefix}SELECT {col_select} FROM {table} WHERE {date_where} {where_fragment_str}"
                lines.append(f"    %let _sql{suffix} = %nrstr({pull_sql});")
                lines.append(f"    %pull_data(&_sql{suffix}, {raw_ds}, server={conn_macro}{_ua});")
            lines.append("")

            # Compute stats → save to cache
            _emit_vintage_compute(lines, v_idx, raw_ds, suffix)

            if not redo:
                lines.append(f"    %end;")
            lines.append("")

        # Stack all cached vintage stats
        cache_sets = " ".join(f"cache._cs_{sn}_v{i}" for i in range(1, n_vintages + 1))
        lines.append(f"    /* Stack all {n_vintages} cached vintage stats */")
        lines.append(f"    data _colstats_{sn};")
        lines.append(f"        set {cache_sets};")
        lines.append(f"    run;")
        lines.append("")

    else:
        # No vintage buckets — pull everything, compute, save to cache
        cache_ds = f"cache._cs_{sn}"
        lines.append(f"    %put NOTE: No vintage buckets - pulling full table;")
        lines.append("")

        if not redo:
            lines.append(f"    %if %sysfunc(exist({cache_ds})) %then %do;")
            lines.append(f"        %put NOTE: Cached stats found: {cache_ds} — skipping pull;")
            lines.append(f"        data _colstats_{sn}; set {cache_ds}; run;")
            lines.append(f"    %end;")
            lines.append(f"    %else %do;")

        col_select = ", ".join([date_expr + " AS dt"] + [col for col, _ in col_list])
        raw_ds = f"_raw_{sn}"

        if is_sas_table:
            # SAS dataset: direct proc sql
            if where:
                pull_sql = f"SELECT {col_select} FROM {table} WHERE {where}"
            else:
                pull_sql = f"SELECT {col_select} FROM {table}"
            lines.append(f"    proc sql; create table {raw_ds} as {pull_sql}; quit;")
        else:
            # Oracle: passthrough via %pull_data
            if where:
                pull_sql = f"{cte_prefix}SELECT {col_select} FROM {table} WHERE {where}"
            else:
                pull_sql = f"{cte_prefix}SELECT {col_select} FROM {table}"
            lines.append(f"    %let _sql_full = %nrstr({pull_sql});")
            lines.append(f"    %pull_data(&_sql_full, {raw_ds}, server={conn_macro}{_ua});")
        lines.append("")

        # Compute stats → save to cache (v_idx=0, suffix='')
        # We'll use a special index for non-vintage mode
        _emit_vintage_compute(lines, 0, raw_ds, '')

        # Rename cache._cs_{name}_v0 to cache._cs_{name}
        lines.append(f"    data {cache_ds}; set cache._cs_{sn}_v0; run;")
        lines.append(f"    proc delete data=cache._cs_{sn}_v0; run;")
        lines.append(f"    data _colstats_{sn}; set {cache_ds}; run;")
        lines.append("")

        if not redo:
            lines.append(f"    %end;")
        lines.append("")

    # Export final
    lines.append(f"    proc export data=_colstats_{sn}")
    lines.append(f'        outfile="&out_dir./{qname}_col.csv"')
    lines.append(f"        dbms=csv replace;")
    lines.append(f"    run;")
    lines.append("")
    lines.append(f"    proc delete data=_colstats_{sn}; run;")
    lines.append(f"    %put NOTE: ===== EXTRACTION COMPLETE: {name} ====;")
    lines.append("")
    lines.append(f"%mend get_colstats_{sn};")
    return '\n'.join(lines)



def gen_sas(config_path, outdir, types=None, env_path=None, db_path=None, vintage='day'):
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

    # Detect config format and extract tables
    if "pairs" in config and isinstance(config["pairs"], dict):
        # Unified format - extract all tables
        from .config import get_all_tables_from_unified
        all_tables = get_all_tables_from_unified(config)
    else:
        # Old format
        all_tables = config.get('tables', [])

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
    conn_macros = set(t.get('conn_macro', 'pcds') for t in pcds_tables)
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
    template_path = os.path.join(os.path.dirname(__file__), 'template.sas')
    with open(template_path, 'r') as f:
        template = f.read()

    # Fill in columns from _column_meta, filtered by col_map if available
    if db_path and "col" in types:
        from .db import get_column_meta
        for tbl in pcds_tables:
            if not tbl.get('columns'):
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

    # Inject vintage into each table config (preserve table-specific vintage if set)
    for tbl in pcds_tables:
        if 'vintage' not in tbl:
            tbl['vintage'] = vintage

    # Inject date filtering from config where_map for col extraction
    if "col" in types and "pairs" in config:
        for tbl in pcds_tables:
            tbl_table = tbl.get('table', '')
            for pair_name in tbl.get('_pairs', []):
                pair_cfg = config['pairs'].get(pair_name, {})
                where_map = pair_cfg.get('where_map', {})
                if not where_map:
                    continue
                # Determine if this table is left or right in the pair
                if pair_cfg.get('left', {}).get('table') == tbl_table:
                    side = 'left'
                else:
                    side = 'right'
                where_clause = where_map.get(side, '')
                if where_clause:
                    tbl['where'] = where_clause
                    qn = _qualified_name(tbl)
                    print(f"  {qn}: using where_map[{side}] from config")
                    break

    # Pass sas_lib and out_dir to table configs
    sas_lib = env_vars['SAS_LIB']
    out_dir = env_vars['OUT_DIR']

    # Generate table macros
    macro_parts = []

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


def aws_creds_renew(ttl_minutes=50):
    """Renew AWS credentials via token-based auth (Windows only).

    On Windows, fetches a token from AWS_TOKEN_URL and uses it to obtain
    temporary credentials from AWS_ARN_URL. On Linux/containers, credentials
    come from IAM roles or environment variables, so this is a no-op.

    Skips renewal if credentials were refreshed less than ttl_minutes ago.
    """
    global _aws_creds_expires

    if os.name != 'nt':
        return

    import time
    now = time.time()
    if _aws_creds_expires and now < _aws_creds_expires:
        return

    import requests

    token_url = os.environ.get('AWS_TOKEN_URL')
    arn_url = os.environ.get('AWS_ARN_URL')
    if not token_url or not arn_url:
        return

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
    except Exception as e:
        print(f"Warning: AWS credential renewal failed: {e}")


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


def _vintage_date_expr_athena(date_expr, vintage, vintage_transform=None):
    """Wrap date_expr with Athena date_trunc for vintage bucketing.

    If vintage_transform is provided (from config JSON), use it directly.
    Otherwise fall back to date_trunc('unit', date_expr).
    """
    if vintage_transform:
        return vintage_transform.replace("{col}", date_expr)
    if not vintage or vintage == 'day':
        return date_expr
    unit = _VINTAGE_ATHENA.get(vintage)
    if unit is None:
        return date_expr
    return f"date_trunc('{unit}', CAST({date_expr} AS date))"


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


def _extract_col_athena(cursor, tbl_cfg, col, dtype, full_table, cte_prefix=""):
    """Extract stats for a single column from Athena.

    Numeric columns: direct aggregation (no top-10).
    Categorical columns: single frequency query, then derive all stats in Python.
    """
    date_col = tbl_cfg['date_col']
    where = tbl_cfg.get('where', '')
    vintage = tbl_cfg.get('vintage', 'day')
    vintage_transform = tbl_cfg.get('vintage_transform', None)
    numeric = is_numeric_type(dtype, is_oracle=False)

    date_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform)
    where_clause = f"AND {where}" if where else ""

    if numeric:
        # Direct aggregation for numeric columns (unchanged approach)
        sql = cte_prefix + build_continuous_sql_athena(full_table, col, date_expr, where)
        cursor.execute(sql)
        results = []
        col_descriptions = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            row_dict = dict(zip(col_descriptions, row))
            parsed = parse_stats_row(row_dict)
            results.append(parsed)
        return results

    # Categorical: single frequency query
    freq_sql = f"""{cte_prefix}
SELECT {date_expr} AS dt, CAST({col} AS VARCHAR) AS col_value, COUNT(*) AS freq
FROM {full_table}
WHERE 1=1 {where_clause}
GROUP BY {date_expr}, {col}"""

    cursor.execute(freq_sql)

    # Collect frequency data grouped by dt
    from collections import defaultdict
    freq_by_dt = defaultdict(list)
    for row in cursor.fetchall():
        dt_key = str(row[0])
        col_value = row[1]  # may be None
        freq = int(row[2])
        freq_by_dt[dt_key].append((col_value, freq))

    # Derive stats from frequency table per dt
    results = []
    for dt_key, freq_rows in freq_by_dt.items():
        n_total = sum(f for _, f in freq_rows)
        n_missing = sum(f for v, f in freq_rows if v is None)
        non_null = [(v, f) for v, f in freq_rows if v is not None]
        n_unique = len(non_null)

        # Top 10 by frequency (descending)
        top_sorted = sorted(non_null, key=lambda x: -x[1])[:10]
        top_10 = json.dumps([{"value": str(v), "count": c} for v, c in top_sorted])

        # Stats of the frequency distribution (non-null values only)
        if non_null:
            freqs = [f for _, f in non_null]
            import statistics
            mean_freq = statistics.mean(freqs)
            std_freq = statistics.pstdev(freqs) if len(freqs) > 1 else 0.0
            min_val = str(min(v for v, _ in non_null))
            max_val = str(max(v for v, _ in non_null))
        else:
            mean_freq = None
            std_freq = None
            min_val = None
            max_val = None

        results.append({
            'dt': dt_key,
            'column_name': col,
            'col_type': 'categorical',
            'n_total': n_total,
            'n_missing': n_missing,
            'n_unique': n_unique,
            'mean': mean_freq,
            'std': std_freq,
            'min_val': min_val,
            'max_val': max_val,
            'top_10': top_10,
        })

    return results


def _extract_oracle_mock(config_path, outdir, types, db_path, mock_dir):
    """Extract from mock CSV files instead of real Oracle."""
    import shutil

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    # Detect config format and extract tables
    if "pairs" in config and isinstance(config["pairs"], dict):
        from .config import get_all_tables_from_unified
        all_tables = get_all_tables_from_unified(config)
    else:
        all_tables = config.get('tables', [])

    oracle_tables = [t for t in all_tables if t.get('source', '').lower() in ('pcds', 'oracle')]
    if not oracle_tables:
        print("No Oracle/PCDS tables found in config")
        return

    for tbl_cfg in oracle_tables:
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)
        table = tbl_cfg['table']  # Oracle table name (uppercase)

        print(f"\n[mock] Extracting: {name} ({table})")

        if "row" in types:
            src = os.path.join(mock_dir, f"{table}_row.csv")
            dst = os.path.join(outdir, f"{qname}_row.csv")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                print(f"  [mock] Row counts: {dst} ({n_rows} dates)")
            else:
                print(f"  [mock] File not found: {src}")

        if "col" in types:
            src = os.path.join(mock_dir, f"{table}_col.csv")
            dst = os.path.join(outdir, f"{qname}_col.csv")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                print(f"  [mock] Column stats: {dst} ({n_rows} rows)")
            else:
                print(f"  [mock] File not found: {src}")

    print(f"\n[mock] Extraction complete. Output in: {outdir}")


def _extract_aws_mock(config_path, outdir, types, db_path, mock_dir):
    """Extract from mock CSV files instead of real Athena."""
    import shutil

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    # Detect config format and extract tables
    if "pairs" in config and isinstance(config["pairs"], dict):
        from .config import get_all_tables_from_unified
        all_tables = get_all_tables_from_unified(config)
    else:
        all_tables = config.get('tables', [])

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']
    if not aws_tables:
        print("No AWS tables found in config")
        return

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        qname = _qualified_name(tbl_cfg)
        database = tbl_cfg['conn_macro']
        table = tbl_cfg['table']
        tbl_mock_dir = os.path.join(mock_dir, database, table)

        print(f"\n[mock] Extracting: {name} ({database}.{table})")

        if "row" in types:
            src = os.path.join(tbl_mock_dir, 'row.csv')
            dst = os.path.join(outdir, f"{qname}_row.csv")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                print(f"  [mock] Row counts: {dst} ({n_rows} dates)")
            else:
                print(f"  [mock] File not found: {src}")

        if "col" in types:
            src = os.path.join(tbl_mock_dir, 'col.csv')
            dst = os.path.join(outdir, f"{qname}_col.csv")
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                print(f"  [mock] Column stats: {dst} ({n_rows} rows)")
            else:
                print(f"  [mock] File not found: {src}")


    print(f"\n[mock] Extraction complete. Output in: {outdir}")


def extract_aws(config_path, outdir, types=None, max_workers=4, db_path=None, vintage='day'):
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

    # Detect config format and extract tables
    if "pairs" in config and isinstance(config["pairs"], dict):
        # Unified format - extract all tables
        from .config import get_all_tables_from_unified
        all_tables = get_all_tables_from_unified(config)
    else:
        # Old format
        all_tables = config.get('tables', [])

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    # Inject vintage into each table config (per-table vintage_transform overrides)
    for tbl in aws_tables:
        if 'vintage' not in tbl:
            tbl['vintage'] = vintage

    # Inject date filtering from config where_map for col extraction
    if "col" in types and "pairs" in config:
        for tbl in aws_tables:
            tbl_table = tbl.get('table', '')
            for pair_name in tbl.get('_pairs', []):
                pair_cfg = config['pairs'].get(pair_name, {})
                where_map = pair_cfg.get('where_map', {})
                if not where_map:
                    continue
                if pair_cfg.get('left', {}).get('table') == tbl_table:
                    side = 'left'
                else:
                    side = 'right'
                where_clause = where_map.get(side, '')
                if where_clause:
                    tbl['where'] = where_clause
                    qn = _qualified_name(tbl)
                    print(f"  {qn}: using where_map[{side}] from config")
                    break

    # Fill in columns from _column_meta, filtered by col_map if available
    if db_path and "col" in types:
        from .db import get_column_meta
        for tbl in aws_tables:
            if not tbl.get('columns'):
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

    aws_creds_renew()
    conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
    cursor = conn.cursor()

    # Track timing for all extractions
    timing_records = []

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        database = tbl_cfg['conn_macro']
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        raw_full_table = f"{database}.{table}"
        full_table, cte_prefix = _resolve_table(tbl_cfg, raw_full_table)

        print(f"\nExtracting: {name} ({raw_full_table})")

        if "row" in types:
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

            # Extract column stats (parallel)
            from concurrent.futures import ThreadPoolExecutor, as_completed

            all_stats = []

            def _extract_one(col_dtype):
                col, dtype = col_dtype
                if col.lower() == date_col.lower():
                    return []
                # Each thread needs its own cursor
                thread_cursor = conn.cursor()
                try:
                    return _extract_col_athena(thread_cursor, tbl_cfg, col, dtype, full_table, cte_prefix)
                finally:
                    thread_cursor.close()

            col_start_time = time.time()
            col_start_ts = datetime.now().isoformat()

            _max_workers = int(os.environ.get('MAX_WORKERS', max_workers))
            with ThreadPoolExecutor(max_workers=_max_workers) as executor:
                futures = {executor.submit(_extract_one, (c, d)): c for c, d in columns.items()}
                for future in as_completed(futures):
                    col_name = futures[future]
                    try:
                        stats = future.result()
                        all_stats.extend(stats)
                    except Exception as e:
                        print(f"  Warning: Failed to extract {col_name}: {e}")

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

    # Detect config format and extract tables
    if "pairs" in config and isinstance(config["pairs"], dict):
        from .config import get_all_tables_from_unified
        all_tables = get_all_tables_from_unified(config)
    else:
        all_tables = config.get('tables', [])

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

    print(f"\nMatched ({len(matched)} columns):")
    for left_name, right_name in sorted(matched.items()):
        left_type = left_cols[left_name]
        right_type = right_cols[right_name]
        print(f"  {left_name} ({left_type}) <-> {right_name} ({right_type})")

    print(f"\n{left_label} only ({len(left_only)}):")
    if left_only:
        for col in left_only:
            print(f"  {col['name']}  {col['type']}")
    else:
        print("  (none)")

    print(f"\n{right_label} only ({len(right_only)}):")
    if right_only:
        for col in right_only:
            print(f"  {col['name']}  {col['type']}")
    else:
        print("  (none)")

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


