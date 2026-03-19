"""Oracle platform builder: SAS code generation for Oracle SQL via passthrough."""

import hashlib
import json
import os

from .base import (
    PlatformBuilder,
    qualified_name,
    sas_safe_name,
    is_numeric_type,
    is_sas_table,
    resolve_table,
    build_date_between_clause,
    build_date_in_clause,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    build_stats_sql,
    build_top10_sql,
)


# ---------------------------------------------------------------------------
# Oracle date/vintage transforms
# ---------------------------------------------------------------------------

# Oracle -> SAS date transform mapping
_ORACLE_TO_SAS_TRANSFORM = {
    'datetime_to_date': 'datepart({col})',
    'to_char':          "put({col}, yymmdd10.)",
}

# Oracle TRUNC vintage -> SAS intnx equivalent
_ORACLE_VINTAGE_TO_SAS = {
    'day':     None,                                  # identity
    'week':    "intnx('week.2', {col}, 0, 'b')",
    'month':   "intnx('month', {col}, 0, 'b')",
    'quarter': "intnx('qtr', {col}, 0, 'b')",
    'year':    "intnx('year', {col}, 0, 'b')",
}

# Oracle TRUNC format codes for vintage bucketing
_VINTAGE_TRUNC = {
    'day': None,       # no extra TRUNC needed (identity)
    'week': 'IW',
    'month': 'MM',
    'quarter': 'Q',
    'year': 'YYYY',
}


def _oracle_date_transform(date_col, transform):
    """Return Oracle SQL expression for date transformation (used inside passthrough)."""
    if transform and "{col}" in transform:
        return transform.replace("{col}", date_col)
    if transform == "datetime_to_date":
        return f"TRUNC({date_col})"
    elif transform == "to_char":
        return f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
    return date_col


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


# ---------------------------------------------------------------------------
# SAS quoting helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# SAS code generation helpers
# ---------------------------------------------------------------------------

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
            '/*{SN}*/': sas_safe_name(tbl_cfg['name']),
            '/*{QNAME}*/': qualified_name(tbl_cfg),
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
        qname = qualified_name(tbl_cfg)
        conn_macro = tbl_cfg.get('conn_macro', 'pcds')
        raw_where = tbl_cfg.get('where', '')
        transform = tbl_cfg.get('date_transform', '')
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)

        safe_ds = sas_safe_name(qname, 29)

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


def _gen_sas_col_local(tbl_cfg, db_path=None, sas_lib='WORK', out_dir='.'):
    """Generate SAS macro for column statistics using the columns.sas template.

    Fills template placeholders with: column map, base SQL, vintage map,
    pull statement, cache check, and stack logic.
    """
    name = tbl_cfg['name']
    sn = sas_safe_name(name)
    qname = qualified_name(tbl_cfg)
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
    date_filter = compute_date_filter(tbl_cfg, db_path, vintage)
    date_dtype = date_filter['date_dtype']
    has_filter = date_filter['filter_type'] != 'none'

    # For SAS DATETIME columns, wrap with datepart() if not already handled by date_transform
    if is_sas and date_dtype and ('DATETIME' in date_dtype.upper() or 'TIMESTAMP' in date_dtype.upper()):
        if 'datepart' not in date_expr.lower():
            date_expr = f"datepart({date_expr})"

    # Vintage bucketing is done in Python via bucket_date(), not in SQL.
    # Per-bucket BETWEEN queries use bucket_key as the dt label literal.
    # No intnx/date_trunc/vintage_transform needed in SQL.
    effective_vintage = date_filter['vintage']

    # Build template values
    _ua = f", user=&{user_override}_usr, pwd=&{user_override}_pwd" if user_override else ""
    if is_sas:
        pull_stmt = "        proc sql; create table &raw_ds as &_full_sql; quit;"
    else:
        pull_stmt = f"        %pull_data(%superq(_full_sql), &raw_ds, server={conn_macro}{_ua});"

    if redo:
        cache_start, cache_end = "", ""
    else:
        cache_start = ("        %if %sysfunc(exist(&cache_ds)) %then %do;\n"
                       "            %put NOTE: Cached stats found: &cache_ds - skipping;\n"
                       "        %end;\n"
                       "        %else %do;")
        cache_end = "        %end;"

    # Column map as assignment statements (no datalines -- safe inside macros)
    col_map_rows = '\n'.join(
        f"        col_name='{c}'; col_type='{'numeric' if is_numeric_type(d, is_oracle=True) else 'categorical'}'; output;"
        for c, d in col_list
    )

    # Build base SQL parts (dt column added per-case below)
    col_list_str = ", ".join(c for c, _ in col_list)
    if where:
        base_where = f"AND ({where})" if has_filter else f"WHERE {where}"
    else:
        base_where = ""

    def _make_sql(dt_expr):
        return f"{cte_prefix}SELECT {dt_expr} AS dt, {col_list_str} FROM {table}"

    # Build vintage calls: data step sets _full_sql via call symputx, then macro runs
    # call symputx stores text literally -- no macro quoting, no paren issues
    def _symputx_sql(full_sql):
        """Generate a data step that sets _full_sql macro variable.

        For long SQL, splits into multiple string assignments concatenated
        with cats() to avoid SAS quoted string length limits (~262 chars
        in some contexts).
        """
        escaped = full_sql.replace('"', '""')
        max_chunk = 250  # safe under SAS quoted string limit
        if len(escaped) <= max_chunk:
            return f'    data _null_; call symputx("_full_sql", "{escaped}"); run;'
        chunks = [escaped[i:i+max_chunk] for i in range(0, len(escaped), max_chunk)]
        lines = ['    data _null_;']
        lines.append(f'        length _sql ${len(escaped) + 100};')
        lines.append(f'        _sql = "{chunks[0]}";')
        for chunk in chunks[1:]:
            lines.append(f'        _sql = cats(_sql, "{chunk}");')
        lines.append('        call symputx("_full_sql", _sql);')
        lines.append('    run;')
        return '\n'.join(lines)

    vintage_calls = []
    if date_filter['filter_type'] == 'between':
        # Per-bucket queries: dt = bucket label (Python-computed), no vintage transform in SQL
        from ..date_utils import bucket_date
        buckets = {}
        for dt in date_filter['dates']:
            buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
        n = len(buckets)
        for v_idx, (bucket_key, dates) in enumerate(sorted(buckets.items()), 1):
            bmin, bmax = min(dates), max(dates)
            dw = build_date_between_clause(date_col, bmin, bmax, date_dtype, is_sas=is_sas, date_format=date_filter.get('date_format'))
            bucket_sql = _make_sql(f"'{bucket_key}'")
            full_sql = f"{bucket_sql} WHERE {dw} {base_where}"
            vintage_calls.append(_symputx_sql(full_sql))
            vintage_calls.append(
                f"    %_process_vintage(raw_ds=_raw_{sn}, "
                f"cache_ds=cache._cs_{sn}_v{v_idx});")
        cache_list = " ".join(f"cache._cs_{sn}_v{i}" for i in range(1, n + 1))
        stack_caches = (f"    data _colstats_{sn};\n"
                        f"        set {cache_list};\n"
                        f"    run;")
    elif date_filter['filter_type'] == 'in_list':
        # Sample dates: treated as one bucket
        dw = build_date_in_clause(
            date_col, date_filter['dates'], date_dtype, is_sas=is_sas,
            date_format=date_filter.get('date_format'),
        )
        base_sql = _make_sql("'sample'")
        full_sql = f"{base_sql} WHERE {dw} {base_where}"
        vintage_calls.append(_symputx_sql(full_sql))
        vintage_calls.append(
            f"    %_process_vintage(raw_ds=_raw_{sn}, "
            f"cache_ds=cache._cs_{sn});")
        stack_caches = f"    data _colstats_{sn}; set cache._cs_{sn}; run;"
    else:
        # No filter (vintage=all): dt = 'all'
        base_sql = _make_sql("'all'")
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

    all_tables = load_tables_from_config(config)
    inject_where_from_config(all_tables, config)

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
    # Skip $ (SAS dataset) tables -- they don't need Oracle credentials
    oracle_tables = [t for t in pcds_tables if not is_sas_table(t)]
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
        fill_columns_from_meta(pcds_tables, db_path)
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
        if is_sas_table(t)
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
            sn = sas_safe_name(name)
            runner_parts.append(f"%start_timer();")
            runner_parts.append(f"%get_colstats_{sn}();")
            runner_parts.append(f"%log_time(table={name}, step=col, outpath=&out_dir.);")
            qname = qualified_name(tbl)
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
    runner_content = '\n'.join(runner_parts)
    seed = env_vars['SEED']
    hash_input = f"{seed}:{runner_content}"
    prefix = 'x' + hashlib.md5(hash_input.encode()).hexdigest()[:7]

    # Generate connection macros for only the servers used
    from ..db import MACRO2SVC
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

    # SAS cache directory: {base}/{prefix}/ -- prefix isolates runs by config
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
    with open(sas_path, 'w', encoding='utf-8') as f:
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
# Mock extraction
# ---------------------------------------------------------------------------

def _extract_mock(config_path, outdir, types, db_path, mock_dir, source_filter):
    """Extract from mock CSV files instead of real database.

    Args:
        source_filter: Source type(s) to filter, e.g. ('pcds', 'oracle') or ('aws',)
    """
    import shutil

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)
    all_tables = load_tables_from_config(config)
    tables = [t for t in all_tables if t.get('source', '').lower() in source_filter]
    if not tables:
        print(f"No {'/'.join(source_filter)} tables found in config")
        return

    is_aws = 'aws' in source_filter

    for tbl_cfg in tables:
        name = tbl_cfg['name']
        qname = qualified_name(tbl_cfg)

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


# ---------------------------------------------------------------------------
# Convenience SQL builder functions (backward-compatible aliases)
# ---------------------------------------------------------------------------

def build_continuous_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "oracle")


def build_categorical_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "oracle")


def build_top10_sql_oracle(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "oracle")


# ---------------------------------------------------------------------------
# OracleBuilder class
# ---------------------------------------------------------------------------

class OracleBuilder(PlatformBuilder):
    """Oracle platform: generates SAS code for Oracle SQL via passthrough."""

    def build_row_sql(self, date_filter):
        """Build Oracle row count SQL (used for direct Oracle queries, not SAS)."""
        table, cte = resolve_table(self.tbl_cfg)
        date_expr = _oracle_date_transform(
            self.date_col,
            self.tbl_cfg.get('date_transform', ''),
        )
        where = self.tbl_cfg.get('where', '')
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""{cte}SELECT {date_expr} AS date_value, COUNT(*) AS row_count
FROM {table}
{where_clause}
GROUP BY {date_expr}"""
        return sql.strip()

    def build_continuous_sql(self, col, col_type, where):
        """Build continuous/numeric stats SQL for Oracle."""
        table, cte = resolve_table(self.tbl_cfg)
        date_expr = _oracle_date_transform(
            self.date_col,
            self.tbl_cfg.get('date_transform', ''),
        )
        return cte + build_stats_sql(table, col, date_expr, where, "numeric", "oracle")

    def build_categorical_sql(self, col, col_type, where, top_n=10):
        """Build categorical stats SQL for Oracle."""
        table, cte = resolve_table(self.tbl_cfg)
        date_expr = _oracle_date_transform(
            self.date_col,
            self.tbl_cfg.get('date_transform', ''),
        )
        return cte + build_stats_sql(table, col, date_expr, where, "categorical", "oracle")

    def generate_extraction(self, outdir, extract_type, **kw):
        """Generate SAS extraction files.

        Delegates to gen_sas() which handles the full SAS code generation
        pipeline including templates, credentials, and runner sections.

        Returns list of output file paths.
        """
        config_path = kw.get('config_path')
        if not config_path:
            raise ValueError("config_path is required for Oracle SAS generation")

        gen_sas(
            config_path=config_path,
            outdir=outdir,
            types=kw.get('types'),
            env_path=kw.get('env_path'),
            db_path=self.db_path,
            vintage=kw.get('vintage'),
        )

        types = kw.get('types', ['row', 'col'])
        type_suffix = '_' + '_'.join(sorted(types)) if types != ['row', 'col'] else ''
        sas_path = os.path.join(outdir, f'extract{type_suffix}.sas')
        return [sas_path]
