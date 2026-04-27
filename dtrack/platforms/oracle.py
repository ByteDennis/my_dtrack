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
    build_date_range_with_gaps,
    resolve_date_format,
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


def _format_date_bound(date_str, date_type, is_sas_src=False, is_upper=False,
                       custom_date_types=None):
    """Format a YYYY-MM-DD date string as the correct SQL literal for WHERE clauses.

    For Oracle tables (inside SAS passthrough), uses Oracle SQL syntax.
    For SAS tables (SAS proc sql), uses SAS date literal syntax.
    For timestamp/datetime upper bounds, uses 23:59:59 instead of 00:00:00.
    Accepts optional custom_date_types dict for extensible type handling.
    """
    dtype = date_type.lower() if date_type else ""
    time_part = "23:59:59" if is_upper else "00:00:00"

    # Check custom date types
    if custom_date_types and dtype in custom_date_types:
        from .base import reformat_date
        custom = custom_date_types[dtype]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            return reformat_date(date_str, fmt)
        elif cat == 'date':
            return f"DATE '{date_str}'"
        else:  # string
            return f"'{reformat_date(date_str, fmt)}'"

    # Numeric types: bare integer (YYYYMMDD)
    if dtype == 'num_yyyymm':
        return date_str[:4] + date_str[5:7]
    if dtype in ('num', 'integer', 'int', 'number'):
        return date_str.replace('-', '')

    # String types
    if dtype == 'string_compact':
        return f"'{date_str.replace('-', '')}'"
    if dtype in ('string_dash', 'string'):
        return f"'{date_str}'"

    # SAS source: SAS date/datetime literals
    if is_sas_src:
        from datetime import datetime as _dt
        try:
            d = _dt.strptime(date_str, "%Y-%m-%d")
            if dtype in ('datetime', 'timestamp'):
                return f"'{d.strftime('%d%b%Y').upper()}:{time_part}'dt"
            return f"'{d.strftime('%d%b%Y').upper()}'d"
        except ValueError:
            return f"'{date_str}'"

    # Oracle: TIMESTAMP or DATE literals
    if dtype == 'timestamp':
        return f"TIMESTAMP '{date_str} {time_part}'"
    if dtype == 'date':
        return f"DATE '{date_str}'"

    # Fallback: string literal
    return f"'{date_str}'"


# ---------------------------------------------------------------------------
# SAS code generation helpers
# ---------------------------------------------------------------------------

def _resolve_table_and_cte(tbl_cfg):
    """Resolve table name and CTE prefix from config.

    Returns (table, cte_prefix, is_sas_dataset).
    """
    table = tbl_cfg['table']

    if is_sas_table(tbl_cfg):
        conn = tbl_cfg.get('conn_macro', '')
        sas_table = f"{conn}.{table}" if conn else table
        return sas_table, "", True

    processed = tbl_cfg.get('processed')
    if isinstance(processed, list):
        processed = "\n".join(processed)

    if processed:
        # Prefix so the CTE alias can't shadow the real table name inside
        # `processed` (Oracle would then treat the inner FROM as a recursive
        # self-reference and raise ORA-32039).
        alias = f"cte_{tbl_cfg['name']}"
        return alias, f"WITH {alias} AS ({processed}) ", False
    return table, "", False


def _gen_sas_proc_contents(sas_tables, out_dir='.'):
    """Generate SAS code to export column metadata via proc contents for $ tables."""
    if not sas_tables:
        return ''

    tpl_path = os.path.join(os.path.dirname(__file__), 'templates', 'meta.sas')
    with open(tpl_path, encoding='utf-8') as f:
        template = f.read()

    blocks = ["/* Column metadata discovery for SAS datasets */"]
    for tbl_cfg in sas_tables:
        conn = tbl_cfg.get('conn_macro', '')
        table = tbl_cfg.get('table', '')
        sas_dataset = f"{conn}.{table}" if conn else table
        replacements = {
            '/*{SN}*/': sas_safe_name(tbl_cfg['name']),
            '/*{QNAME}*/': qualified_name(tbl_cfg),
            '/*{SOURCE}*/': tbl_cfg.get('source', 'oracle'),
            '/*{TABLE}*/': tbl_cfg['name'],
            '/*{SAS_DATASET}*/': sas_dataset,
        }
        block = template
        for k, v in replacements.items():
            block = block.replace(k, v)
        blocks.append(block)

    return '\n'.join(blocks)


def _gen_sas_row_datadriven(oracle_tables, sas_lib='WORK', out_dir='.'):
    """Generate data-driven SAS row extraction using template files.

    Builds mapping datasets and reusable macros for two modes:
    - Oracle tables: proc sql passthrough to Oracle (rows_oracle.sas)
    - SAS tables ($-prefixed processed): proc sql on local SAS dataset (rows_sas.sas)
    """
    oracle_datalines = []
    sas_datalines = []

    for idx, tbl_cfg in enumerate(oracle_tables, 1):
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        name = tbl_cfg['name']
        qname = qualified_name(tbl_cfg)
        conn_macro = tbl_cfg.get('conn_macro', 'pb23')
        raw_where = tbl_cfg.get('where', '')
        date_bounds = tbl_cfg.get('_date_bounds', '')
        transform = tbl_cfg.get('date_transform', '')
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)

        safe_ds = sas_safe_name(qname, 29)

        if is_sas_table(tbl_cfg):
            sas_table = f"{conn_macro}.{table}"
            where = _oracle_where_to_sas(raw_where, quote=False)
            # Append date bounds (already in SAS literal format, no quoting needed)
            if date_bounds:
                where = f"({where}) AND {date_bounds}" if where else date_bounds
            date_expr = _sas_date_transform(date_col, transform) if transform else date_col
            sas_datalines.append(f"{sas_table}|{safe_ds}|{qname}|{date_expr}|{where}")
        else:
            # SAS-quote user WHERE (doubles single quotes for SAS macro),
            # then append date bounds raw (they use Oracle SQL syntax, not SAS)
            where = _sas_quote(raw_where)
            if date_bounds:
                where = f"({where}) AND {date_bounds}" if where else date_bounds
            date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
            if processed:
                # Reference the CTE alias, not the bare table name — the CTE
                # is prefixed `cte_` below so it can't collide with the real
                # table name inside `processed` (otherwise Oracle raises
                # ORA-32039: recursive WITH clause must have column alias list).
                table = f"cte_{name}"
            oracle_datalines.append(f"{table}|{safe_ds}|{qname}|{date_expr}|{conn_macro}|{idx}|{where}")

    # CTE %let statements for Oracle processed tables (not SAS). Alias is
    # prefixed `cte_` so `processed` can reference the raw table name without
    # Oracle reading the inner FROM as self-recursive.
    cte_lines = []
    for idx, tbl_cfg in enumerate(oracle_tables, 1):
        if is_sas_table(tbl_cfg):
            continue
        processed = tbl_cfg.get('processed')
        if isinstance(processed, list):
            processed = " ".join(processed)
        if processed:
            name = tbl_cfg['name']
            cte_lines.append(f"%let _cte{idx} = WITH cte_{name} AS ({processed});")

    redo = str(int(os.environ.get('SAS_ROW_REDO', '0')))
    redo_line = f"%let _row_redo = {redo};"
    tpl_dir = os.path.join(os.path.dirname(__file__), 'templates')
    parts = []

    if oracle_datalines:
        with open(os.path.join(tpl_dir, 'rows_oracle.sas'), encoding='utf-8') as f:
            tpl = f.read()
        tpl = tpl.replace('/*{CTE_VARS}*/', '\n'.join(cte_lines))
        tpl = tpl.replace('/*{ROW_REDO}*/', redo)
        tpl = tpl.replace('/*{ORA_DATALINES}*/', '\n'.join(oracle_datalines))
        parts.append(tpl)

    if sas_datalines:
        with open(os.path.join(tpl_dir, 'rows_sas.sas'), encoding='utf-8') as f:
            tpl = f.read()
        # If Oracle block already emitted redo, don't duplicate
        if oracle_datalines:
            tpl = tpl.replace('/*{ROW_REDO}*/', '')
        else:
            tpl = tpl.replace('/*{ROW_REDO}*/', redo_line)
        tpl = tpl.replace('/*{SAS_DATALINES}*/', '\n'.join(sas_datalines))
        parts.append(tpl)

    return '\n\n'.join(parts)


def _gen_sas_row_hadoop(hadoop_tables, sas_lib='WORK', out_dir='.'):
    """Generate data-driven SAS row extraction for Hadoop/Hive tables."""
    hdp_datalines = []

    for idx, tbl_cfg in enumerate(hadoop_tables, 1):
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        qname = qualified_name(tbl_cfg)
        conn_macro = tbl_cfg.get('conn_macro', 'hdp')
        raw_where = tbl_cfg.get('where', '')
        date_bounds = tbl_cfg.get('_date_bounds', '')
        transform = tbl_cfg.get('date_transform', '')

        safe_ds = sas_safe_name(qname, 29)

        where = raw_where
        if date_bounds:
            where = f"({where}) AND {date_bounds}" if where else date_bounds
        date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
        hdp_datalines.append(f"{table}|{safe_ds}|{qname}|{date_expr}|{conn_macro}|{idx}|{where}")

    if not hdp_datalines:
        return ''

    redo = str(int(os.environ.get('SAS_ROW_REDO', '0')))
    tpl_dir = os.path.join(os.path.dirname(__file__), 'templates')

    with open(os.path.join(tpl_dir, 'rows_hadoop.sas'), encoding='utf-8') as f:
        tpl = f.read()
    tpl = tpl.replace('/*{CTE_VARS}*/', '')
    tpl = tpl.replace('/*{ROW_REDO}*/', redo)
    tpl = tpl.replace('/*{HDP_DATALINES}*/', '\n'.join(hdp_datalines))
    return tpl


def _date_trunc_expr(source, date_col, date_type, vintage):
    """DB-side expression that truncates date_col to the vintage unit and
    returns a 'YYYY-MM-DD' string. Used in SELECT/GROUP BY so vintage
    bucketing is pushed into the database -- one query per col returns all
    buckets (same shape as AWS/Athena does it).

    Handled combinations:
      oracle + date/timestamp/datetime           + {day,week,month,quarter,year}
      oracle + string_compact YYYYMMDD           + ditto
      oracle + string_dash YYYY-MM-DD / string   + ditto
      oracle + num/int/number (YYYYMMDD integer) + ditto
      hadoop + string_compact YYYYMMDD           + {day,month,quarter,year,week}
      hadoop + string_dash YYYY-MM-DD / string   + ditto
      hadoop + date/timestamp/datetime           + ditto

    Special labels: vintage in {'all','', None} -> literal 'all';
                    vintage == 'sample'        -> literal 'sample'.

    Returns None when the (source, date_type, vintage) combination has no
    mapping so the caller can raise/fall back with a clear message.
    """
    src = (source or '').lower()
    dtype = (date_type or '').lower()

    if vintage in (None, '', 'all'):
        return "'all'"
    if vintage == 'sample':
        return "'sample'"

    if src == 'oracle':
        if dtype in ('date', 'timestamp', 'datetime'):
            parsed = date_col
        elif dtype == 'string_compact':
            parsed = f"TO_DATE({date_col}, 'YYYYMMDD')"
        elif dtype in ('string_dash', 'string'):
            parsed = f"TO_DATE({date_col}, 'YYYY-MM-DD')"
        elif dtype in ('num', 'integer', 'int', 'number'):
            parsed = f"TO_DATE(TO_CHAR({date_col}), 'YYYYMMDD')"
        elif dtype == 'num_yyyymm':
            parsed = f"TO_DATE(TO_CHAR({date_col}), 'YYYYMM')"
        else:
            return None
        unit = {'day': 'DD', 'week': 'IW', 'month': 'MM',
                'quarter': 'Q', 'year': 'YYYY'}.get(vintage)
        if unit is None:
            return None
        return f"TO_CHAR(TRUNC({parsed}, '{unit}'), 'YYYY-MM-DD')"

    if src == 'hadoop':
        # Avoid Hive's date_trunc() -- it only exists in Hive 2.1+ / Spark and
        # older clusters (plus some HDP / CDH builds) raise
        # SemanticException 10011 "Invalid function date_trunc".
        # These expressions stick to year(), month(), quarter(), date_sub(),
        # next_day(), date_format(), substr() -- all present in Hive 0.12+.

        # day/month/year on string date types stay as substring composition
        # (no parsing overhead, partition-prune friendly).
        if dtype == 'string_compact':
            if vintage == 'day':
                return (f"concat(substr({date_col},1,4),'-',"
                        f"substr({date_col},5,2),'-',substr({date_col},7,2))")
            if vintage == 'month':
                return (f"concat(substr({date_col},1,4),'-',"
                        f"substr({date_col},5,2),'-01')")
            if vintage == 'year':
                return f"concat(substr({date_col},1,4),'-01-01')"
            if vintage == 'quarter':
                return (f"concat(substr({date_col},1,4),'-',"
                        f"case when substr({date_col},5,2) in ('01','02','03') then '01' "
                        f"when substr({date_col},5,2) in ('04','05','06') then '04' "
                        f"when substr({date_col},5,2) in ('07','08','09') then '07' "
                        f"else '10' end,'-01')")
        elif dtype in ('string_dash', 'string'):
            if vintage == 'day':
                return date_col
            if vintage == 'month':
                return f"concat(substr({date_col},1,7),'-01')"
            if vintage == 'year':
                return f"concat(substr({date_col},1,4),'-01-01')"
            if vintage == 'quarter':
                return (f"concat(substr({date_col},1,4),'-',"
                        f"case when substr({date_col},6,2) in ('01','02','03') then '01' "
                        f"when substr({date_col},6,2) in ('04','05','06') then '04' "
                        f"when substr({date_col},6,2) in ('07','08','09') then '07' "
                        f"else '10' end,'-01')")
        elif dtype in ('date', 'timestamp', 'datetime'):
            if vintage == 'day':
                return f"date_format({date_col},'yyyy-MM-dd')"
            if vintage == 'month':
                return f"date_format({date_col},'yyyy-MM-01')"
            if vintage == 'year':
                return f"date_format({date_col},'yyyy-01-01')"
            if vintage == 'quarter':
                return (
                    f"concat(cast(year({date_col}) as string),'-',"
                    f"case when month({date_col}) <= 3 then '01' "
                    f"when month({date_col}) <= 6 then '04' "
                    f"when month({date_col}) <= 9 then '07' "
                    f"else '10' end,'-01')"
                )
        # Week needs an actual date value; parse first, then align to Monday.
        # Monday-of-week = date_sub(next_day(d, 'MON'), 7): if d is already
        # Monday, next_day returns d+7, so subtracting 7 gives d; otherwise
        # it gives the current week's Monday.
        if vintage == 'week':
            if dtype == 'string_compact':
                parsed = f"to_date(from_unixtime(unix_timestamp({date_col},'yyyyMMdd')))"
            elif dtype in ('string_dash', 'string'):
                parsed = f"to_date({date_col})"
            elif dtype in ('date', 'timestamp', 'datetime'):
                parsed = f"to_date({date_col})"
            else:
                return None
            return (
                f"date_format(date_sub(next_day({parsed},'MON'), 7),'yyyy-MM-dd')"
            )
        return None

    return None


def _resolve_table_inline(tbl_cfg):
    """Like _resolve_table_and_cte but inlines any 'processed' SQL as a
    FROM-ready subquery (no separate WITH clause). Returns (table_from, is_sas).

    The stats SQL builders already use WITH for categorical top-N, so mixing
    a caller-supplied WITH and an inner WITH would break Oracle. Inlining
    keeps everything self-contained.
    """
    table = tbl_cfg['table']
    if is_sas_table(tbl_cfg):
        conn = tbl_cfg.get('conn_macro', '')
        return f"{conn}.{table}" if conn else table, True

    processed = tbl_cfg.get('processed')
    if isinstance(processed, list):
        processed = "\n".join(processed)
    if processed:
        return f"({processed})", False
    return table, False


_SAS_EXPORT_HELPER = r"""
/* Shared across all col-extraction paths. */

/* _export_col_stats
     dsname  - SAS-safe identifier (<=28 chars, no special chars); used to
               locate the cache dataset at cache._cs_<dsname>. Falls back to
               &qname when a caller doesn't pass one (legacy callers).
     qname   - human-friendly qualified name; used in the output CSV
               filename and log lines.
     outdir  - where to write the CSV. */
%macro _export_col_stats(dsname=, qname=, outdir=);
    %local _cds;
    %if %length(&dsname) = 0 %then %let _cds = &qname;
    %else %let _cds = &dsname;

    %if %sysfunc(exist(cache._cs_&_cds)) %then %do;
        data _colstats_;
            set cache._cs_&_cds;
            length dt_fmt $10;
            if vtype(dt)='N' then dt_fmt = put(dt, yymmdd10.);
            else dt_fmt = strip(vvalue(dt));
            drop dt; rename dt_fmt = dt;
        run;
        proc export data=_colstats_
            outfile="&outdir./&qname._col.csv"
            dbms=csv replace;
        run;
        proc delete data=_colstats_; run;
        %put NOTE: Exported col stats for &qname to &outdir./&qname._col.csv;
    %end;
    %else %do;
        %put WARNING: No cache found at cache._cs_&_cds -- nothing exported for &qname;
    %end;
%mend _export_col_stats;

/* Per-table banner + finalizer helpers. Take args explicitly (qname/dsname)
   rather than reading from global symputx'd macro vars, so Python can call
   them in open code with literal arguments that are resolved at macro-invoke
   time -- bypassing call-execute's macro/symputx ordering quirks entirely. */
%macro _table_start_banner(qname=, dsname=);
    %put NOTE: ##########################################################;
    %put NOTE: ## TABLE START: &qname;
    %put NOTE: ##########################################################;
    %start_timer();
%mend _table_start_banner;

%macro _table_done_footer(qname=, dsname=);
    %log_time(table=&qname, step=col, outpath=&out_dir.);
    %_export_col_stats(dsname=&dsname, qname=&qname, outdir=&out_dir);
    %put NOTE: ## TABLE DONE : &qname;
%mend _table_done_footer;
"""


_SAS_SOURCE_HELPERS = r"""
/* =====================================================================
 * SAS-source bucket helpers (emitted only when a config has SAS-source
 * tables). Oracle/Hadoop sources go through column_oracle.sas /
 * column_hadoop.sas -- they don't need these.
 * ===================================================================== */

%macro _col_numeric(raw_ds=, col=, out_ds=);
    proc sql noprint;
        create table &out_ds as
        select dt,
            "&col" as column_name length=32,
            'numeric' as col_type length=32,
            count(*) as n_total,
            count(*) - count(&col) as n_missing,
            count(distinct &col) as n_unique,
            avg(&col) as mean,
            std(&col) as std,
            strip(put(min(&col), best32.)) as min_val length=200,
            strip(put(max(&col), best32.)) as max_val length=200,
            '' as top_10 length=4000
        from &raw_ds
        group by dt;
    quit;
%mend _col_numeric;

%macro _col_categorical(raw_ds=, col=, out_ds=);
    proc sql noprint;
        create table _freq_raw_ as
        select dt, &col as p_col, count(*) as value_freq
        from &raw_ds
        group by dt, &col;
    quit;
    proc sort data=_freq_raw_ out=_freq_sorted_;
        by dt descending value_freq p_col;
    run;
    data _t10_(keep=dt top_10);
        length top_10 $4000 _entry $400;
        set _freq_sorted_; by dt;
        where p_col is not missing;
        retain top_10 _rn;
        if first.dt then do; top_10=''; _rn=0; end;
        if _rn < 10 then do;
            _rn + 1;
            _entry = cats(strip(vvalue(p_col)), '(', strip(put(value_freq, best.)), ')');
            if top_10 = '' then top_10 = _entry;
            else top_10 = catx('; ', top_10, _entry);
        end;
        if last.dt then output;
    run;
    proc sql noprint;
        create table _agg_ as
        select dt,
            "&col" as column_name length=32,
            'categorical' as col_type length=32,
            sum(value_freq) as n_total,
            sum(case when p_col is not missing then 1 else 0 end) as n_unique,
            avg(value_freq) as mean,
            std(value_freq) as std,
            strip(put(min(value_freq), best32.)) as min_val length=200,
            strip(put(max(value_freq), best32.)) as max_val length=200
        from _freq_raw_ group by dt;
    quit;
    proc sql noprint;
        create table _miss_ as
        select dt, coalesce(value_freq, 0) as n_missing
        from _freq_raw_ where p_col is missing;
    quit;
    proc sort data=_agg_; by dt; run;
    proc sort data=_t10_; by dt; run;
    proc sort data=_miss_; by dt; run;
    data &out_ds;
        merge _agg_(in=a) _t10_(in=b) _miss_(in=c);
        by dt;
        if a;
        if not b then top_10='';
        if not c then n_missing=0;
    run;
    proc datasets lib=work nolist;
        delete _freq_raw_ _freq_sorted_ _t10_ _agg_ _miss_;
    quit;
%mend _col_categorical;

/* SAS-source bucket mode: per-(bucket, col) raw pull from local SAS dataset
 * + SAS-side aggregation. Used only when _source = SAS. Oracle/Hadoop rows
 * don't reach this macro -- they go through column_oracle.sas / column_hadoop.sas.
 */
%macro _pull_one_col();
    %local _raw _cache;
    %let _raw = _raw_onecol;
    %let _cache = cache._cs_&_dsname;

    proc sql noprint;
        create table &_raw as
        select "&_dt_label" as dt, &_col
        from &_sas_table
        where &_date_where
        %if %length(%superq(_base_where)) > 0 %then AND (&_base_where) ;
        ;
    quit;

    %if %upcase(&_coltype) = NUMERIC %then %do;
        %_col_numeric(raw_ds=&_raw, col=&_col, out_ds=_cstat);
    %end;
    %else %do;
        %_col_categorical(raw_ds=&_raw, col=&_col, out_ds=_cstat);
    %end;

    proc append base=&_cache data=_cstat force; run;
    proc delete data=&_raw _cstat; run;
%mend _pull_one_col;
"""


def _sas_escape(s):
    """Double single quotes so the value is safe inside SAS single-quoted literals."""
    return str(s).replace("'", "''")


def _apply_col_filter(col_list, tbl_cfg):
    """Filter (col_name, col_dtype) list by the pair's col_filter selection.

    Resolution order:
      1. `_selected_cols`  - concrete list produced by resolve_col_filter
                             against col_map; used when col_map is present.
      2. `_col_filter_patterns` - raw include/exclude glob patterns from the
                             pair's col_filter; applied directly against
                             column names when no col_map exists (so users
                             who haven't done col-mapping yet still get
                             their saved filter honored).
      3. no filter         - pass through unchanged.
    """
    selected = tbl_cfg.get('_selected_cols')
    if selected:
        wanted = {c.lower() for c in selected}
        return [(c, d) for c, d in col_list if c.lower() in wanted]

    cf = tbl_cfg.get('_col_filter_patterns')
    if cf:
        from fnmatch import fnmatch
        includes = [p.strip().lower() for p in (cf.get('include') or []) if p and p.strip()]
        excludes = [p.strip().lower() for p in (cf.get('exclude') or []) if p and p.strip()]
        out = col_list
        if includes:
            out = [(c, d) for c, d in out
                   if any(fnmatch(c.lower(), p) for p in includes)]
        if excludes:
            out = [(c, d) for c, d in out
                   if not any(fnmatch(c.lower(), p) for p in excludes)]
        return out
    return col_list


def _compute_bucket_specs(tbl_cfg, db_path):
    """Return (bucket_specs, is_sas, date_dtype, col_list).

    bucket_specs: list of (bucket_key, date_where_sql, from_lit, to_lit).
    - date_where_sql is the actual SQL WHERE fragment used at runtime.
    - from_lit / to_lit are the platform-formatted date literals for the
      bucket's min/max (e.g., Oracle: DATE '2024-10-01'; Hadoop string_compact:
      '20241001'). Carried through to the driver dataset so users can
      eyeball the date format per (table, column) before running the job.
      For 'in_list'/'all' modes we still populate them with the overall
      min/max bounds (or blanks when there's no filter).
    """
    from .base import (build_date_range_with_gaps, build_date_between_clause,
                       build_date_in_clause, compute_date_filter,
                       resolve_date_format, format_date_bounds_literals)

    date_col = tbl_cfg['date_col']
    vintage = tbl_cfg.get('vintage', 'all') or 'all'
    columns = tbl_cfg.get('columns', {})

    col_list = [(c, d) for c, d in columns.items() if c.upper() != date_col.upper()]
    col_list = _apply_col_filter(col_list, tbl_cfg)

    is_sas = is_sas_table(tbl_cfg)

    date_filter = compute_date_filter(tbl_cfg, db_path, vintage)
    date_dtype = date_filter['date_dtype']
    resolve_date_format(date_filter, tbl_cfg)
    effective_vintage = date_filter['vintage']
    date_format = date_filter.get('date_format')

    if (date_filter['filter_type'] == 'none'
            and effective_vintage in ('day', 'week', 'month', 'quarter', 'year')
            and tbl_cfg.get('_from_date') and tbl_cfg.get('_to_date')):
        from ..date_utils import vintage_bucket_spans
        spans = vintage_bucket_spans(
            tbl_cfg['_from_date'], tbl_cfg['_to_date'], effective_vintage,
        )
        if spans:
            date_filter['filter_type'] = 'between'
            date_filter['vintage_spans'] = spans

    bucket_specs = []
    if date_filter['filter_type'] == 'between':
        if 'vintage_spans' in date_filter:
            for bucket_key, bmin, bmax in date_filter['vintage_spans']:
                dw = build_date_between_clause(
                    date_col, bmin, bmax, date_dtype,
                    is_sas=is_sas, date_format=date_format,
                )
                fl, tl = format_date_bounds_literals(
                    bmin, bmax, date_dtype, is_sas=is_sas, date_format=date_format,
                )
                bucket_specs.append((bucket_key, dw, fl, tl))
        else:
            from ..date_utils import bucket_date
            buckets = {}
            for dt in date_filter['dates']:
                buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
            for bucket_key, dates in sorted(buckets.items()):
                dw = build_date_range_with_gaps(
                    date_col, dates, date_dtype,
                    is_sas=is_sas, date_format=date_format,
                )
                fl, tl = format_date_bounds_literals(
                    min(dates), max(dates), date_dtype,
                    is_sas=is_sas, date_format=date_format,
                )
                bucket_specs.append((bucket_key, dw, fl, tl))
    elif date_filter['filter_type'] == 'in_list':
        dw = build_date_in_clause(
            date_col, date_filter['dates'], date_dtype,
            is_sas=is_sas, date_format=date_format,
        )
        fl, tl = format_date_bounds_literals(
            min(date_filter['dates']), max(date_filter['dates']),
            date_dtype, is_sas=is_sas, date_format=date_format,
        )
        bucket_specs.append(('sample', dw, fl, tl))
    else:
        bucket_specs.append(('all', '1=1', '', ''))

    return bucket_specs, is_sas, date_dtype, col_list


def _compute_col_spec(tbl_cfg, db_path):
    """Per-table info for DB-side vintage bucketing (Oracle / Hadoop path).

    Returns dict with:
      col_list        - [(col_name, col_dtype), ...] non-date columns after col_filter
      date_col        - raw date column name
      vintage         - resolved vintage label ('day'/..../'year' | 'all' | 'sample')
      vintage_expr    - SQL expression producing the bucket dt from date_col
                        (platform-specific TRUNC / substring / date_trunc)
      where_clause    - FULL-range WHERE body (date bounds + user where +
                        exclude-dates if any); NO per-bucket filtering
      date_from / date_to
                      - platform-formatted literals covering the FULL range,
                        purely for eyeballing _ora_col_map / _hdp_col_map
      date_dtype      - DB column type (from _column_meta), for reference
      is_sas          - True if SAS-source table (this function is only used
                        for Oracle/Hadoop, but kept consistent)

    Bucketing happens DB-side via GROUP BY &vintage_expr -- one SQL per col
    returns every bucket in a single round trip. This mirrors the AWS/Athena
    per-col pattern (same row count, same columns, same dt semantics).
    """
    from .base import (build_date_between_clause, build_date_in_clause,
                       build_date_range_with_gaps, compute_date_filter,
                       resolve_date_format, format_date_bounds_literals)

    date_col = tbl_cfg['date_col']
    vintage_req = tbl_cfg.get('vintage', 'all') or 'all'
    columns = tbl_cfg.get('columns', {})

    col_list = [(c, d) for c, d in columns.items() if c.upper() != date_col.upper()]
    col_list = _apply_col_filter(col_list, tbl_cfg)

    is_sas = is_sas_table(tbl_cfg)
    tbl_source = 'sas' if is_sas else (tbl_cfg.get('source') or 'oracle').lower()
    date_type = tbl_cfg.get('date_type')

    date_filter = compute_date_filter(tbl_cfg, db_path, vintage_req)
    date_dtype = date_filter['date_dtype']
    resolve_date_format(date_filter, tbl_cfg)
    effective_vintage = date_filter['vintage']
    date_format = date_filter.get('date_format')

    # Synthesize full range from _from_date/_to_date when no matching dates
    # loaded yet (lets vintage bucketing still fire on fresh configs).
    if (date_filter['filter_type'] == 'none'
            and effective_vintage in ('day', 'week', 'month', 'quarter', 'year')
            and tbl_cfg.get('_from_date') and tbl_cfg.get('_to_date')):
        from ..date_utils import vintage_bucket_spans
        spans = vintage_bucket_spans(
            tbl_cfg['_from_date'], tbl_cfg['_to_date'], effective_vintage,
        )
        if spans:
            date_filter['filter_type'] = 'between'
            date_filter['min_date'] = spans[0][1]
            date_filter['max_date'] = spans[-1][2]
            date_filter['vintage_spans'] = spans

    where_clause = '1=1'
    date_from_lit = ''
    date_to_lit = ''
    vintage_label = effective_vintage

    if date_filter['filter_type'] == 'between':
        bmin = date_filter.get('min_date')
        bmax = date_filter.get('max_date')
        if 'vintage_spans' in date_filter:
            where_clause = build_date_between_clause(
                date_col, bmin, bmax, date_dtype,
                is_sas=is_sas, date_format=date_format,
            )
        else:
            where_clause = build_date_range_with_gaps(
                date_col, date_filter['dates'], date_dtype,
                is_sas=is_sas, date_format=date_format,
            )
        date_from_lit, date_to_lit = format_date_bounds_literals(
            bmin, bmax, date_dtype, is_sas=is_sas, date_format=date_format,
        )
    elif date_filter['filter_type'] == 'in_list':
        where_clause = build_date_in_clause(
            date_col, date_filter['dates'], date_dtype,
            is_sas=is_sas, date_format=date_format,
        )
        bmin = min(date_filter['dates'])
        bmax = max(date_filter['dates'])
        date_from_lit, date_to_lit = format_date_bounds_literals(
            bmin, bmax, date_dtype, is_sas=is_sas, date_format=date_format,
        )
        vintage_label = 'sample'  # single bucket labelled 'sample'
    else:
        # No filter synthesized (typical: vintage='all' without matching_dates),
        # but from/to may still be set -- honor them as a WHERE bound so the
        # job only scans the user's requested range even without bucketing.
        from_d = tbl_cfg.get('_from_date')
        to_d = tbl_cfg.get('_to_date')
        if from_d and to_d:
            where_clause = build_date_between_clause(
                date_col, from_d, to_d, date_dtype,
                is_sas=is_sas, date_format=date_format,
            )
            date_from_lit, date_to_lit = format_date_bounds_literals(
                from_d, to_d, date_dtype,
                is_sas=is_sas, date_format=date_format,
            )

    # Resolve the DB-side truncation expression. For vintage='all'/'sample'
    # this is a literal string; for day/week/month/quarter/year it's a
    # platform-specific TRUNC/substring/date_trunc over date_col.
    vintage_expr = _date_trunc_expr(tbl_source, date_col, date_type, vintage_label)
    if vintage_expr is None:
        print(f"  WARNING: [{tbl_cfg.get('name')}] no vintage truncation expr for "
              f"source={tbl_source} date_type={date_type!r} vintage={vintage_label!r} "
              f"-- falling back to single 'all' bucket")
        vintage_expr = "'all'"
        vintage_label = 'all'

    return {
        'col_list': col_list,
        'date_col': date_col,
        'vintage': vintage_label,
        'vintage_expr': vintage_expr,
        'where_clause': where_clause,
        'date_from': date_from_lit,
        'date_to': date_to_lit,
        'date_dtype': date_dtype,
        'is_sas': is_sas,
    }


def _combine_where(date_where, base_where):
    """Combine a bucket date filter and the user's base WHERE into one clause
    body. Returns '1=1' if both are empty; drops '1=1' placeholders."""
    parts = []
    if date_where and date_where.strip() and date_where.strip() != '1=1':
        parts.append(f"({date_where})")
    if base_where and base_where.strip():
        parts.append(f"({base_where})")
    return " AND ".join(parts) if parts else "1=1"


def _render_col_template(tpl_name, rows, rows_placeholder,
                         run_placeholder, run_macro):
    """Fill column_oracle.sas / column_hadoop.sas with compact driver rows
    plus one per-(qname) run-macro invocation in open code.

    Each row carries only the per-col info (col_name, col_type, vintage,
    vintage_expr, date_*, from_table, where_clause). The stats SQL lives
    once in %_col_oracle / %_col_hadoop and is built at runtime via the
    symputx'd macro vars in the inner call-execute dispatch.

    Banner + CSV export + footer run in OPEN CODE (via %_run_one_*_table
    wrapping the per-qname inner data step) so their args are resolved at
    macro-invoke time, not via call-execute / symputx ordering.
    """
    tpl_path = os.path.join(os.path.dirname(__file__), 'templates', tpl_name)
    with open(tpl_path, 'r', encoding='utf-8') as f:
        tpl = f.read()

    redo = str(int(os.environ.get('SAS_COL_REDO', '0')))
    row_lines = []
    seen = {}
    for r in rows:
        row_lines.append(
            f"    qname='{_sas_escape(r['qname'])}'; "
            f"dsname='{_sas_escape(r['dsname'])}'; "
            f"conn_macro='{_sas_escape(r['conn_macro'])}'; "
            f"col_name='{_sas_escape(r['col_name'])}'; "
            f"col_type='{r['col_type']}'; "
            f"date_col='{_sas_escape(r['date_col'])}'; "
            f"vintage='{_sas_escape(r['vintage'])}'; "
            f"vintage_expr='{_sas_escape(r['vintage_expr'])}'; "
            f"from_table='{_sas_escape(r['from_table'])}'; "
            f"date_from='{_sas_escape(r['date_from'])}'; "
            f"date_to='{_sas_escape(r['date_to'])}'; "
            f"where_clause='{_sas_escape(r['where_clause'])}'; output;"
        )
        # Distinct (qname, dsname) pairs in first-seen order -- used to emit
        # one %_run_one_*_table(qname=..., dsname=...) invocation per qname.
        if r['qname'] not in seen:
            seen[r['qname']] = r['dsname']

    run_calls = [f"%{run_macro}(qname={q}, dsname={d});" for q, d in seen.items()]

    tpl = tpl.replace('/*{COL_REDO}*/', redo)
    tpl = tpl.replace(f'/*{{{rows_placeholder}}}*/', '\n'.join(row_lines))
    tpl = tpl.replace(f'/*{{{run_placeholder}}}*/', '\n'.join(run_calls))
    return tpl


def _gen_sas_col_driver(tables, db_path=None, out_dir='.'):
    """Emit SAS code for column-stats extraction across all tables.

    Dispatch by source:
      - SAS-source tables use the flat-driver bucket path (local pull +
        SAS-side aggregate via %_pull_one_col / %_col_numeric / %_col_categorical).
      - Oracle/Hadoop tables use column_oracle.sas / column_hadoop.sas:
        all aggregation happens in the DB via `connect to` passthrough,
        one SQL per (col, bucket), output schema matches athena/aws.

    Per-qname CSV export runs at the end from cache._cs_<dsname> via
    %_export_col_stats (defined in _SAS_COL_HELPERS).
    """
    sas_driver_rows = []
    ora_rows = []
    hdp_rows = []
    qnames_sas = []
    qnames_ora = []
    qnames_hdp = []

    for tbl_cfg in tables:
        qname = qualified_name(tbl_cfg)
        # cache._cs_<dsname> must be <= 32 chars total; '_cs_' prefix eats 4,
        # leaving 28 for dsname. (rows use 'rc_' prefix so they can afford 29.)
        dsname = sas_safe_name(qname, 28)
        conn_macro = tbl_cfg.get('conn_macro', 'pb23')
        # base_where carries only the user's config WHERE. Date filtering
        # lives on date_where (per bucket) -- mixing _date_bounds in here
        # would emit `date_col BETWEEN X AND Y AND date_col >= X AND date_col <= Y`
        # against the same column on every per-bucket pull. Same fix applied
        # to the Oracle DB-aggregation path below.
        base_where = tbl_cfg.get('where', '')

        bucket_specs, is_sas, _date_dtype, col_list = _compute_bucket_specs(
            tbl_cfg, db_path,
        )
        if not col_list:
            print(f"  {qname}: 0 cols (col_filter or empty) -- skipping")
            continue

        tbl_source = 'sas' if is_sas else (tbl_cfg.get('source') or 'oracle').lower()
        table_from, _ = _resolve_table_inline(tbl_cfg)
        # SAS path still needs the qualified conn.name reference only.
        if is_sas:
            sas_table_ref = table_from
        else:
            sas_table_ref = None  # unused on the DB path

        n_buckets = len(bucket_specs)
        n_cols = len(col_list)

        if tbl_source == 'sas':
            # SAS-source: keep per-bucket flat-driver path (local pull +
            # SAS-side aggregate). Vintage-bucketing-in-DB does not apply.
            qnames_sas.append((qname, dsname))
            for bucket_key, date_where, _fl, _tl in bucket_specs:
                for col_name, col_dtype in col_list:
                    col_type = 'numeric' if is_numeric_type(col_dtype, is_oracle=True) else 'categorical'
                    sas_driver_rows.append({
                        'qname': qname,
                        'dsname': dsname,
                        'sas_table': sas_table_ref,
                        'conn_macro': conn_macro,
                        'dt_label': bucket_key,
                        'date_where': date_where,
                        'base_where': base_where,
                        'col_name': col_name,
                        'col_type': col_type,
                    })
            print(f"[SAS col] {qname}: {n_cols} cols x {n_buckets} buckets "
                  f"= {n_cols * n_buckets} runs (sas source)")
            continue

        # Oracle / Hadoop: DB-side vintage bucketing. One driver row per
        # (qname, col); the macro's SQL does GROUP BY &_vintage_expr and
        # returns one result row per bucket. vintage_expr sits in the
        # driver so you can proc print _ora_col_map / _hdp_col_map and
        # eyeball that the truncation expression matches each column's
        # date type before the job runs.
        spec = _compute_col_spec(tbl_cfg, db_path)
        if tbl_source == 'oracle':
            rows_bucket = ora_rows
            qnames_ora.append(qname)
            numeric_is_oracle = True
        elif tbl_source == 'hadoop':
            rows_bucket = hdp_rows
            qnames_hdp.append(qname)
            numeric_is_oracle = False
        else:
            print(f"  {qname}: unknown source '{tbl_source}' -- skipping")
            continue

        # spec['where_clause'] already covers the from/to range; don't layer
        # _date_bounds on top (that would give 'BETWEEN X AND Y AND col >= X
        # AND col <= Y' redundancy). Only the user's config WHERE gets
        # appended here.
        user_where = tbl_cfg.get('where', '')
        full_where = _combine_where(spec['where_clause'], user_where)
        for col_name, col_dtype in spec['col_list']:
            col_type = 'numeric' if is_numeric_type(col_dtype, is_oracle=numeric_is_oracle) else 'categorical'
            rows_bucket.append({
                'qname': qname,
                'dsname': dsname,
                'conn_macro': conn_macro,
                'col_name': col_name,
                'col_type': col_type,
                'date_col': spec['date_col'],
                'vintage': spec['vintage'],
                'vintage_expr': spec['vintage_expr'],
                'from_table': table_from,
                'date_from': spec['date_from'],
                'date_to': spec['date_to'],
                'where_clause': full_where,
            })
        print(f"[{tbl_source} col] {qname}: {n_cols} cols (db-side "
              f"GROUP BY {spec['vintage_expr']!s:.60} vintage={spec['vintage']}) "
              f"= {n_cols} runs")

    total = len(sas_driver_rows) + len(ora_rows) + len(hdp_rows)
    if total == 0:
        return "\n/* No col extraction rows generated (col_filter zeroed everything out). */\n"
    n_tbl = len(qnames_sas) + len(qnames_ora) + len(qnames_hdp)
    print(f"[SAS col] TOTAL: {n_tbl} tables "
          f"(sas={len(qnames_sas)}, oracle={len(qnames_ora)}, hadoop={len(qnames_hdp)}), "
          f"{total} driver rows")

    # _export_col_stats is needed by all paths; the SAS-source bucket helpers
    # only make sense when we actually have SAS-source tables.
    parts = [_SAS_EXPORT_HELPER]
    if sas_driver_rows:
        parts.append(_SAS_SOURCE_HELPERS)

    # SAS-source flat driver (bucket mode).
    if sas_driver_rows:
        parts.append("")
        parts.append("/* SAS-source flat driver: one row per (qname, bucket, col) */")
        parts.append("data _driver_col_sas_;")
        parts.append("    length qname $64 dsname $32 sas_table $128 conn_macro $16")
        parts.append("           col_name $32 col_type $12 dt_label $32")
        parts.append("           date_where $4000 base_where $4000;")
        for r in sas_driver_rows:
            parts.append(
                f"    qname='{_sas_escape(r['qname'])}'; "
                f"dsname='{_sas_escape(r['dsname'])}'; "
                f"sas_table='{_sas_escape(r['sas_table'])}'; "
                f"conn_macro='{_sas_escape(r['conn_macro'])}'; "
                f"col_name='{_sas_escape(r['col_name'])}'; "
                f"col_type='{r['col_type']}'; "
                f"dt_label='{_sas_escape(r['dt_label'])}'; "
                f"date_where='{_sas_escape(r['date_where'])}'; "
                f"base_where='{_sas_escape(r['base_where'])}'; "
                f"output;"
            )
        parts.append("run;")
        parts.append("")
        parts.append("data _null_;")
        parts.append("    set _driver_col_sas_;")
        parts.append("    length _cmd $8000;")
        parts.append("    _cmd = cats(")
        parts.append("        'data _null_;',")
        parts.append("        'call symputx(\"_qname\", ', quote(strip(qname)), ');',")
        parts.append("        'call symputx(\"_dsname\", ', quote(strip(dsname)), ');',")
        parts.append("        'call symputx(\"_sas_table\", ', quote(strip(sas_table)), ');',")
        parts.append("        'call symputx(\"_conn_macro\", ', quote(strip(conn_macro)), ');',")
        parts.append("        'call symputx(\"_col\", ', quote(strip(col_name)), ');',")
        parts.append("        'call symputx(\"_coltype\", ', quote(strip(col_type)), ');',")
        parts.append("        'call symputx(\"_dt_label\", ', quote(strip(dt_label)), ');',")
        parts.append("        'call symputx(\"_date_where\", ', quote(strip(date_where)), ');',")
        parts.append("        'call symputx(\"_base_where\", ', quote(strip(base_where)), ');',")
        parts.append("        'run;',")
        parts.append("        '%nrstr(%_pull_one_col)();'")
        parts.append("    );")
        parts.append("    call execute(_cmd);")
        parts.append("run;")
        parts.append("proc delete data=_driver_col_sas_; run;")
        parts.append("")
        parts.append("/* Per-qname CSV export (sas source) */")
        for q, ds in qnames_sas:
            parts.append(
                f"%_export_col_stats(dsname={ds}, qname={q}, outdir=&out_dir);")

    # Oracle path: DB-aggregated stats via column_oracle.sas
    if ora_rows:
        parts.append("")
        parts.append("/* --- Oracle column extraction (DB aggregation) --- */")
        parts.append(_render_col_template(
            'column_oracle.sas', ora_rows,
            rows_placeholder='ORA_COL_ROWS',
            run_placeholder='ORA_RUN_CALLS',
            run_macro='_run_one_ora_table',
        ))

    # Hadoop path: DB-aggregated stats via column_hadoop.sas
    if hdp_rows:
        parts.append("")
        parts.append("/* --- Hadoop column extraction (DB aggregation) --- */")
        parts.append(_render_col_template(
            'column_hadoop.sas', hdp_rows,
            rows_placeholder='HDP_COL_ROWS',
            run_placeholder='HDP_RUN_CALLS',
            run_macro='_run_one_hdp_table',
        ))

    return "\n".join(parts)


def _gen_sas_col_local(tbl_cfg, db_path=None, sas_lib='WORK', out_dir='.'):
    # NOTE: Uses explicit column references -- never SELECT *
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
    date_bounds = tbl_cfg.get('_date_bounds', '')
    if date_bounds:
        where = f"({where}) AND {date_bounds}" if where else date_bounds
    transform = tbl_cfg.get('date_transform', '')
    vintage = tbl_cfg.get('vintage', 'all') or 'all'
    conn_macro = tbl_cfg.get('conn_macro', 'pb23')
    user_override = tbl_cfg.get('user', '')
    redo = int(os.environ.get('SAS_COL_REDO', '0'))

    # Early returns for empty column lists
    if not columns:
        return f"%macro get_colstats_{sn}();\n    %put WARNING: No columns specified for {name}.;\n%mend get_colstats_{sn};"
    col_list = [(c, d) for c, d in columns.items() if c.upper() != date_col.upper()]
    if not col_list:
        return f"%macro get_colstats_{sn}();\n    %put WARNING: No non-date columns to extract for {name};\n%mend get_colstats_{sn};"

    # Restrict to the pair's col_filter selection (case-insensitive), if set.
    # Empty/missing _selected_cols means "extract all non-date cols" (legacy).
    selected = tbl_cfg.get('_selected_cols')
    print(f"  [col_filter] {name}: _selected_cols={'<absent>' if selected is None else f'{len(selected)} cols'}, before={len(col_list)} cols")
    if selected:
        wanted = {c.lower() for c in selected}
        col_list = [(c, d) for c, d in col_list if c.lower() in wanted]
        print(f"  [col_filter] {name}: after filter={len(col_list)} cols")
        if not col_list:
            return (f"%macro get_colstats_{sn}();\n"
                    f"    %put WARNING: col_filter left 0 columns for {name};\n"
                    f"%mend get_colstats_{sn};")

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

    # Ensure date_format is set from config date_type
    resolve_date_format(date_filter, tbl_cfg)

    # For SAS DATETIME columns, wrap with datepart() if not already handled by date_transform
    if is_sas and date_dtype and ('DATETIME' in date_dtype.upper() or 'TIMESTAMP' in date_dtype.upper()):
        if 'datepart' not in date_expr.lower():
            date_expr = f"datepart({date_expr})"

    # Vintage bucketing is done in Python via bucket_date(), not in SQL.
    # Per-bucket BETWEEN queries use bucket_key as the dt label literal.
    # No intnx/date_trunc/vintage_transform needed in SQL.
    effective_vintage = date_filter['vintage']

    # No row-compare data yet but vintage requests bucketing -- compute
    # per-bucket (key, min, max) spans directly from from/to so the SAS macro
    # emits one BETWEEN query per month/week/etc. against the raw date column
    # (literals in the column's native format, no function on the LHS).
    if (date_filter['filter_type'] == 'none'
            and effective_vintage in ('day', 'week', 'month', 'quarter', 'year')
            and tbl_cfg.get('_from_date') and tbl_cfg.get('_to_date')):
        from ..date_utils import vintage_bucket_spans
        try:
            spans = vintage_bucket_spans(
                tbl_cfg['_from_date'], tbl_cfg['_to_date'], effective_vintage,
            )
            if spans:
                date_filter['filter_type'] = 'between'
                date_filter['vintage_spans'] = spans
                date_filter['min_date'] = spans[0][1]
                date_filter['max_date'] = spans[-1][2]
                date_filter['n_matching'] = len(spans)
                has_filter = True
                print(f"  [date filter] no row-compare data; synthesized "
                      f"{len(spans)} {effective_vintage} buckets from "
                      f"{spans[0][1]} to {spans[-1][2]}")
        except ValueError:
            pass

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
        # Data-driven vintage: build argument table, then loop with call execute.
        # Two flavors:
        #   - vintage_spans (synthesized from from/to): contiguous bucket spans,
        #     one bare BETWEEN per bucket -- no LHS function, no NOT IN gaps.
        #   - dates list (real row-compare data): may have gaps, use
        #     build_date_range_with_gaps which adds AND NOT (col IN (...)).
        from ..date_utils import bucket_date
        bucket_specs = []  # list of (bucket_key, where_fragment)
        if 'vintage_spans' in date_filter:
            for bucket_key, bmin, bmax in date_filter['vintage_spans']:
                from .base import build_date_between_clause
                dw = build_date_between_clause(
                    date_col, bmin, bmax, date_dtype,
                    is_sas=is_sas, date_format=date_filter.get('date_format'),
                )
                bucket_specs.append((bucket_key, dw))
        else:
            buckets = {}
            for dt in date_filter['dates']:
                buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
            for bucket_key, dates in sorted(buckets.items()):
                dw = build_date_range_with_gaps(
                    date_col, dates, date_dtype,
                    is_sas=is_sas, date_format=date_filter.get('date_format'),
                )
                bucket_specs.append((bucket_key, dw))
        n = len(bucket_specs)

        # Build _v_args_ dataset: v_idx, dt_label, date_where
        arg_rows = []
        for v_idx, (bucket_key, dw) in enumerate(bucket_specs, 1):
            dw_escaped = dw.replace("'", "''")
            arg_rows.append(
                f"        v_idx={v_idx}; dt_label='{bucket_key}'; "
                f"date_where='{dw_escaped}'; output;")

        # Static SQL parts (no date_where, no dt_label -- those come from dataset)
        base_sql_tpl = f"{cte_prefix}SELECT"
        col_part = f"{col_list_str} FROM {table}"
        base_where_escaped = base_where.replace("'", "''") if base_where.strip() else ""

        vintage_calls.append(f"    /* {n} vintage buckets -- data-driven */")
        vintage_calls.append(f"    data _v_args_;")
        vintage_calls.append(f"        length v_idx 8 dt_label $32 date_where $4000;")
        for row in arg_rows:
            vintage_calls.append(row)
        vintage_calls.append(f"    run;")
        vintage_calls.append(f"")
        vintage_calls.append(f"    data _null_;")
        vintage_calls.append(f"        set _v_args_;")
        vintage_calls.append(f"        length _full_sql $8000 _cmd $4000;")
        vintage_calls.append(f"        _full_sql = '{base_sql_tpl} ' || quote(strip(dt_label)) || ' AS dt, {col_part}';")
        vintage_calls.append(f"        _full_sql = strip(_full_sql) || ' WHERE ' || strip(date_where);")
        if base_where_escaped:
            vintage_calls.append(f"        _full_sql = strip(_full_sql) || ' {base_where_escaped}';")
        vintage_calls.append(f'        call execute(\'data _null_; call symputx("_full_sql", "\' || tranwrd(strip(_full_sql), \'"\', \'""\') || \'"); run;\');')
        vintage_calls.append(f"        _cmd = '%nrstr(%_process_vintage)(raw_ds=_raw_{sn}, cache_ds=cache._cs_{sn}_v' || strip(put(v_idx, best.)) || ')';")
        vintage_calls.append(f"        call execute(_cmd);")
        vintage_calls.append(f"    run;")
        vintage_calls.append(f"    proc delete data=_v_args_; run;")

        cache_list = " ".join(f"cache._cs_{sn}_v{i}" for i in range(1, n + 1))
        stack_caches = (f"    data _colstats_{sn};\n"
                        f"        set {cache_list};\n"
                        f"    run;")
    elif date_filter['filter_type'] == 'in_list':
        # Sample dates: single bucket
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
    with open(tmpl_path, 'r', encoding='utf-8') as f:
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


def gen_sas(config_path, outdir, types=None, env_path=None, db_path=None, vintage=None,
            from_date=None, to_date=None):
    """
    Generate a single combined SAS file for Oracle data extraction.

    Uses template.sas as the base, fills in credentials from .env,
    generates per-table macros, and creates a runner section with
    time tracking.

    Args:
        config_path: Path to extraction config JSON (supports both old and unified formats)
        outdir: Directory to write the combined .sas file
        types: List of types to generate ("row", "col"). Default: both.
        env_path: Path to .env file with oracle_usr, oracle_pw, email_to, lib_path
        db_path: Path to database for column metadata and where_map filtering
        vintage: Date bucketing granularity (day, week, month, quarter, year)
        from_date: Start date for incremental extraction (YYYY-MM-DD)
        to_date: End date for incremental extraction (YYYY-MM-DD)
    """
    if types is None:
        types = ["row", "col"]

    # Use load_unified_config (not raw json.load) so each side gets its
    # `name` auto-derived from `table` via _derive_side_name. Without this,
    # qualified_name(tbl) downstream hits KeyError on tbl['name'].
    from ..config import load_unified_config
    config = load_unified_config(config_path)

    # Hydrate col_map and col_filter from _table_pairs (where the web UI
    # persists them) into the in-memory config. Without this, get_all_tables_
    # from_unified sees empty col_map and the col_filter can't produce
    # _selected_cols -- CLI runs would emit all columns even though the DB
    # has the include/exclude saved.
    if db_path:
        from ..db import get_pair_col_map_from_db, get_pair_col_filter
        for pname, pcfg in config.get("pairs", {}).items():
            if not pcfg.get("col_map"):
                db_map = get_pair_col_map_from_db(db_path, pname)
                if db_map:
                    pcfg["col_map"] = db_map
            if not pcfg.get("col_filter"):
                cf = get_pair_col_filter(db_path, pname)
                if cf.get("include") or cf.get("exclude"):
                    pcfg["col_filter"] = cf

    os.makedirs(outdir, exist_ok=True)

    all_tables = load_tables_from_config(config)
    inject_where_from_config(all_tables, config)

    # Build per-table date bounds. Resolution order per table:
    #   1. per-pair fromDate/toDate saved on the pair config (frontend col_gen
    #      card) -- most specific, so it wins. First pair with a value wins
    #      when a table is in multiple.
    #   2. explicit gen_sas(from_date=..., to_date=...) kwargs (CLI default /
    #      global UI "Default Date Range") -- fallback.
    # This ordering matches /api/generate's own per-pair-wins policy and
    # prevents stale global UI inputs from overriding fresh per-pair edits.
    for tbl in all_tables:
        date_col = tbl.get('date_col', '')
        if not date_col:
            continue

        pair_from = pair_to = None
        for pn in tbl.get('_pairs', []):
            pc = config.get('pairs', {}).get(pn, {})
            pair_from = pc.get('fromDate') or pair_from
            pair_to = pc.get('toDate') or pair_to
            if pair_from and pair_to:
                break
        eff_from = pair_from or from_date
        eff_to = pair_to or to_date

        if not (eff_from or eff_to):
            continue

        date_type = (tbl.get('date_type') or '').lower()
        is_sas_src = is_sas_table(tbl)
        bounds = []
        if eff_from:
            lit = _format_date_bound(eff_from, date_type, is_sas_src, is_upper=False)
            bounds.append(f"{date_col} >= {lit}")
        if eff_to:
            lit = _format_date_bound(eff_to, date_type, is_sas_src, is_upper=True)
            bounds.append(f"{date_col} <= {lit}")
        if bounds:
            tbl['_date_bounds'] = " AND ".join(bounds)
        # Stash canonical from/to so col-stats can synthesize vintage buckets
        # when no row-compare matching_dates are available yet.
        tbl['_from_date'] = eff_from
        tbl['_to_date'] = eff_to

    oracle_tables = [t for t in all_tables if t.get('source', '').lower() in ('oracle', 'sas', 'hadoop')]

    if not oracle_tables:
        print("No Oracle/SAS/Hadoop tables found in config")
        return

    # Check for mock mode -- still generate SAS files but also copy mock CSVs
    mock_dir = get_mock_dir()

    # Use os.environ (already loaded from dtrack.conf by CLI)
    env_src = os.environ

    # Defaults, then override from env_src
    env_vars = {'SAS_LIB': 'WORK', 'OUT_DIR': '.', 'SEED': '2025'}
    for key in ['PCDS_USR', 'EMAIL_TO', 'SAS_LIB', 'OUT_DIR', 'SEED']:
        if key in env_src:
            env_vars[key] = env_src[key]
        elif mock_dir and key in ('PCDS_USR', 'EMAIL_TO'):
            env_vars[key] = f"MOCK_{key}"

    # Separate Hadoop tables from Oracle/SAS
    hadoop_tables = [t for t in oracle_tables if t.get('source', '').lower() == 'hadoop']
    oracle_tables = [t for t in oracle_tables if t.get('source', '').lower() != 'hadoop']

    # Connection macro passwords (e.g., PCDS_PWD, PB23_PWD, PB30_PWD)
    # SAS-source tables don't need Oracle credentials
    oracle_only = [t for t in oracle_tables if not is_sas_table(t)]
    conn_macros = set(t.get('conn_macro', 'pb23') for t in oracle_only)
    for conn_macro in conn_macros:
        pwd_key = f"{conn_macro.upper()}_PWD"
        if pwd_key in env_src:
            env_vars[pwd_key] = env_src[pwd_key]
        elif mock_dir:
            # In mock mode, use placeholder credentials
            env_vars[pwd_key] = f"MOCK_{conn_macro.upper()}_PWD"
        else:
            raise KeyError(f"{pwd_key} not found in config or environment")

    # Hadoop connection env vars
    hadoop_conn_macros = set(t.get('conn_macro', 'hdp') for t in hadoop_tables)
    for key in ['HDP_SERVER', 'HDP_URI', 'SAS_HADOOP_JAR_PATH', 'SAS_HADOOP_CONFIG_PATH']:
        if key in env_src:
            env_vars[key] = env_src[key]
        elif mock_dir:
            env_vars[key] = f"MOCK_{key}"

    # User override credentials (e.g., TMP_USR, TMP_PWD)
    for user_key in set(t.get('user', '') for t in oracle_tables if t.get('user')):
        for suffix in ('_USR', '_PWD'):
            k = f"{user_key.upper()}{suffix}"
            if k in env_src:
                env_vars[k] = env_src[k]
            elif mock_dir:
                env_vars[k] = f"MOCK_{k}"
            else:
                raise KeyError(f"{k} not found in config or environment")

    # Read template
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'template.sas')
    with open(template_path, 'r', encoding='utf-8') as f:
        template = f.read()

    # Col extraction uses proc sql passthrough through whatever conn_macro
    # a side declares (pb23, hdp, etc.), so oracle/sas/hadoop all go through
    # _gen_sas_col_local. Row extraction still splits because it uses
    # platform-specific templates (rows_oracle.sas vs rows_hadoop.sas).
    all_col_tables = oracle_tables + hadoop_tables
    if db_path and "col" in types:
        fill_columns_from_meta(all_col_tables, db_path)
    for tbl in all_col_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif not tbl.get('vintage'):
            tbl['vintage'] = 'all'
    # Pass sas_lib and out_dir to table configs
    sas_lib = env_vars['SAS_LIB']
    out_dir = env_vars['OUT_DIR']

    # Generate table macros
    macro_parts = []

    # Emit proc contents for $ (SAS dataset) tables to export column metadata CSVs
    sas_dataset_tables = [
        t for t in oracle_tables
        if is_sas_table(t)
    ]
    if sas_dataset_tables:
        macro_parts.append(_gen_sas_proc_contents(sas_dataset_tables, out_dir))

    if "row" in types:
        # Data-driven row extraction (single block for all tables)
        macro_parts.append("/* --- Row extraction (data-driven) --- */")
        macro_parts.append(_gen_sas_row_datadriven(oracle_tables, sas_lib, out_dir))

        if hadoop_tables:
            macro_parts.append("/* --- Hadoop row extraction (data-driven) --- */")
            macro_parts.append(_gen_sas_row_hadoop(hadoop_tables, sas_lib, out_dir))

    if "col" in types:
        # Flat-driver structure: ONE driver dataset across all tables, one
        # dispatcher loop, shared helper macros. Replaces the per-table
        # get_colstats_SN macro soup -- smaller extract_col.sas file, no
        # 32K SAS line-length issues, and col_filter is honored per-row.
        macro_parts.append("/* --- Col extraction (flat driver across all tables) --- */")
        macro_parts.append(_gen_sas_col_driver(all_col_tables, db_path, out_dir))

    # Generate runner section
    runner_parts = []

    # Row runner: data-driven macro handles its own timing via call execute
    # Just add email notification at the end
    if "row" in types:
        runner_parts.append("/* Row extraction driven by table_date_map (macros above) */")
        runner_parts.append("")

    if "col" in types:
        # Flat-driver dispatch runs inline in macro_parts (no per-table
        # runner calls needed). Just note in the runner that col gen is
        # handled by the driver block above.
        runner_parts.append("/* Col extraction driven by _driver_col_ (block above) */")
        runner_parts.append("%start_timer();")
        runner_parts.append("%put NOTE: ===== Col extraction starting (flat driver) =====;")
        runner_parts.append("%log_time(table=all, step=col, outpath=&out_dir.);")
        runner_parts.append("")

    # Global summary email once the whole job (row + col) is done --
    # one email per run, not per-table. Wrapped in /* ... */ the same way
    # row gen used to do, so users opt in by uncommenting at run time.
    runner_parts.append(f"%let _job_end = %sysfunc(datetime());")
    runner_parts.append(f"%let _job_elapsed = %sysevalf(&_job_end - &_job_start);")
    _types_label = " + ".join(types)
    runner_parts.append(
        f'/* %send_email(subject=dtrack extraction complete ({_types_label}), '
        f'body=dtrack {_types_label} extraction finished. '
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

    # Hadoop connection macros
    if hadoop_tables:
        hdp_server = env_vars.get('HDP_SERVER', '')
        hdp_uri = env_vars.get('HDP_URI', '')
        jar_path = env_vars.get('SAS_HADOOP_JAR_PATH', '')
        config_path = env_vars.get('SAS_HADOOP_CONFIG_PATH', '')
        conn_macro_lines.append('')
        conn_macro_lines.append('/* Hadoop connection setup */')
        if jar_path:
            conn_macro_lines.append(f'options set=SAS_HADOOP_JAR_PATH="{jar_path}";')
        if config_path:
            conn_macro_lines.append(f'options set=SAS_HADOOP_CONFIG_PATH="{config_path}";')
        for macro in sorted(hadoop_conn_macros):
            conn_macro_lines.append(
                f'\n%macro {macro};\n'
                f'  server="{hdp_server}"\n'
                f'  port=10000\n'
                f'  schema={macro}\n'
                f'  subprotocol=hive2\n'
                f'  login_timeout=300\n'
                f'  uri="{hdp_uri}"\n'
                f'%mend {macro};'
            )

    # Build user credential %let statements for override accounts
    user_overrides = set(t.get('user', '') for t in oracle_tables if t.get('user'))
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

    # Break the count out by source so the log is honest about what went in.
    n_oracle = sum(1 for t in oracle_tables if (t.get('source') or '').lower() == 'oracle')
    n_sas = sum(1 for t in oracle_tables if (t.get('source') or '').lower() == 'sas')
    n_hadoop = len(hadoop_tables)
    total = n_oracle + n_sas + n_hadoop
    print(f"  Generated: {sas_path}")
    print(f"  Tables: {total} (oracle={n_oracle}, sas={n_sas}, hadoop={n_hadoop})")
    print(f"  Types: {', '.join(types)}")
    if env_path:
        print(f"  Credentials: from {env_path}")

    # Discover and save column metadata CSVs (Oracle/SAS tables only)
    sas_tables = [t for t in all_tables
                  if t.get('source', '').lower() in ('oracle', 'sas', 'hadoop', '')]
    if db_path and sas_tables:
        _discover_and_write_columns(sas_tables, outdir, db_path)

    # In mock mode, also copy mock CSVs to outdir
    if mock_dir:
        _extract_oracle_mock(config_path, outdir, types, db_path, mock_dir)


# ---------------------------------------------------------------------------
# Column discovery and CSV writing
# ---------------------------------------------------------------------------

def _discover_and_write_columns(all_tables, outdir, db_path):
    """Discover columns for AWS tables and write {qname}_columns.csv files.

    For AWS tables: uses _discover_columns_athena.
    For Oracle/SAS/Hadoop: column discovery is done via the generated SAS file.
    If _column_meta already exists in DB, writes CSV from that.
    """
    import csv as csv_mod
    from ..db import insert_column_meta, get_column_meta

    os.makedirs(outdir, exist_ok=True)
    aws_cursors = {}

    for tbl in all_tables:
        source = (tbl.get('source') or '').lower()
        qname = qualified_name(tbl)
        raw_table = tbl['table']
        conn_macro = tbl.get('conn_macro', '')
        columns = None

        # Check existing DB metadata first
        existing = get_column_meta(db_path, qname)
        if existing:
            columns = {c["column_name"]: c.get("data_type", "") for c in existing}
            print(f"  {qname}: found {len(columns)} columns in _column_meta")
        elif source == 'aws':
            try:
                database = conn_macro or tbl.get('database', '')
                if database not in aws_cursors:
                    from ..platforms.athena import athena_connect, aws_creds_renew
                    print(f"  {qname}: connecting to Athena ({database})...")
                    aws_creds_renew()
                    conn = athena_connect(data_base=database)
                    aws_cursors[database] = conn.cursor()
                from ..platforms.athena import _discover_columns_athena
                columns = _discover_columns_athena(aws_cursors[database], database, raw_table)
                if columns:
                    insert_column_meta(db_path, qname, columns, source=source)
                    print(f"  {qname}: discovered {len(columns)} columns from Athena")
                else:
                    print(f"  {qname}: no columns found in Athena")
            except Exception as e:
                print(f"  {qname}: Athena column discovery failed ({e})")
        else:
            # Oracle/SAS/Hadoop -- columns discovered via generated SAS file
            print(f"  {qname}: columns will be discovered by SAS (source={source})")
            continue

        if columns:
            col_csv = os.path.join(outdir, f"{qname}_columns.csv")
            with open(col_csv, 'w', newline='') as f:
                writer = csv_mod.writer(f)
                writer.writerow(['COLUMN_NAME', 'DATA_TYPE'])
                for col_name, col_type in sorted(columns.items()):
                    writer.writerow([col_name, col_type])
            print(f"  {qname}: wrote {len(columns)} columns to {col_csv}")

    for cursor in aws_cursors.values():
        try:
            cursor.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Mock extraction
# ---------------------------------------------------------------------------

def _extract_mock(config_path, outdir, types, db_path, mock_dir, source_filter):
    """Extract from mock CSV files instead of real database.

    Mock directory uses flat layout: {qname}_row.csv, {qname}_col.csv
    where qname = {source}_{name} (from qualified_name()).

    Args:
        source_filter: Source type(s) to filter, e.g. ('oracle', 'sas') or ('aws',)
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

    for tbl_cfg in tables:
        qname = qualified_name(tbl_cfg)
        source = tbl_cfg.get('source', '')
        table = tbl_cfg['table']
        print(f"\n[mock] Extracting: {qname} ({source}/{table})")

        for typ in ("row", "col"):
            if typ not in types:
                continue
            src = os.path.join(mock_dir, f"{qname}_{typ}.csv")
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


def get_mock_dir():
    """Return mock directory from DTRACK_MOCK (preferred) or legacy env vars."""
    return os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ORACLE_MOCK')


def _extract_oracle_mock(config_path, outdir, types, db_path, mock_dir):
    return _extract_mock(config_path, outdir, types, db_path, mock_dir, ('oracle', 'sas'))


def _extract_mock_tables(tables, outdir, types, mock_dir):
    """Copy mock CSVs for a pre-built list of table configs."""
    import shutil
    os.makedirs(outdir, exist_ok=True)

    for tbl_cfg in tables:
        qname = qualified_name(tbl_cfg)
        source = tbl_cfg.get('source', '')
        table = tbl_cfg['table']
        print(f"\n[mock] Extracting: {qname} ({source}/{table})")

        for typ in ("row", "col"):
            if typ not in types:
                continue
            src = os.path.join(mock_dir, f"{qname}_{typ}.csv")
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
