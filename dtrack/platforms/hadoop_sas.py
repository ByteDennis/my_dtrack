"""SAS-template code-generation path for Hadoop / Oracle / Hive tables.

Split out of platforms/hadoop.py: this module holds the legacy SAS-driven
extraction path (gen_sas + its helpers + the inline SAS-helper macros).
The newer gen-first plain-Hive SQL path stays in platforms/hadoop.py and
is not affected by this split.

External callers (cli.py, web/app.py, pipeline.py, tests) import gen_sas
from platforms.hadoop, which re-exports it from this module for back-compat.
"""

import os
import re
import json
import hashlib
from datetime import datetime

from .base import (
    qualified_name,
    sas_safe_name,
    is_numeric_type,
    is_sas_table,
    resolve_table,
    quote_ident,
    apply_null_sentinels,
    build_date_between_clause,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    format_date_bounds_literals,
    get_mock_dir,
    _extract_mock,
)

# Shared helpers from platforms/hadoop. The import works at module load
# even though platforms/hadoop re-exports gen_sas from this file: that
# re-export sits at the BOTTOM of platforms/hadoop, so by the time this
# module loads, every helper named below is already defined in hadoop's
# module namespace and the import resolves cleanly.
from .hadoop import (
    _oracle_date_transform,
    _format_date_bound,
    _resolve_table_and_cte,
    _resolve_table_inline,
    _date_trunc_expr,
    _apply_col_filter,
    _n_unique_expr,
    _col_expr_for_source,
    _is_libname_table,
    _libname_mount_for,
    _discover_and_write_columns,
)


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

        if is_sas_table(tbl_cfg) or _is_libname_table(tbl_cfg):
            # SAS-syntax path: source=sas (local SAS dataset) OR libname-mode
            # (mounted via libref at the top of the script). Both reference
            # tables as <libref>.<name> and use SAS date/where syntax.
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
    """Generate data-driven SAS row extraction for Hadoop/Hive tables.

    Skips tables with `connect_method=libname` — those flow through the
    SAS-syntax pipeline alongside source=sas tables (see
    `_gen_sas_row_datadriven`).
    """
    hdp_datalines = []

    for idx, tbl_cfg in enumerate(hadoop_tables, 1):
        if _is_libname_table(tbl_cfg):
            continue  # routed via the SAS-syntax path
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

/* _col_categorical_freq
     freq_ds - input dataset with columns (dt, p_col, cnt). p_col may be
               missing (for the NULL-category row).
     col     - the original column name; baked into output as column_name.
     out_ds  - output dataset with same shape as the count-only categorical
               emit today (dt, column_name, col_type, n_total, n_missing,
               n_unique, mean, std, min_val, max_val, top_10).

   Mean/std are rank-weighted by the alphabetic (UPPER) ordering of cats
   per dt -- so any permutation of counts across categories produces
   different stats. min_val/max_val are formatted "{cat}={count}" at the
   alpha-first / alpha-last positions. top_10 is built SAS-side from the
   freq table (safe: the historical crashes were Hive collect_list/
   sort_array reducers, not local SAS sorts).

   Output mean/std/min_val/max_val are CHAR (matches today's emit, where
   the SQL casts them to STRING/VARCHAR2). n_total/n_missing/n_unique are
   numeric (matches today's emit). */
%macro _col_categorical_freq(freq_ds=, col=, out_ds=);
    /* Per-dt totals: n_total = SUM(cnt) including NULL p_col;
       n_unique counts non-missing p_col rows. */
    proc sql noprint;
        create table _ccf_meta_ as
        select dt,
               sum(cnt) as n_total,
               sum(case when p_col is not missing then 1 else 0 end) as n_unique
        from &freq_ds group by dt;
        create table _ccf_miss_ as
        select dt, coalesce(sum(cnt), 0) as n_missing
        from &freq_ds where p_col is missing group by dt;
    quit;

    /* Alpha-rank pass over non-missing categories. UPPER for cross-engine
       consistency with the Athena/Oracle/Python paths. */
    data _ccf_alpha_;
        set &freq_ds(where=(p_col is not missing));
        length _p_upper $400;
        _p_upper = upcase(strip(vvalue(p_col)));
    run;
    proc sort data=_ccf_alpha_; by dt _p_upper p_col; run;
    data _ccf_ranked_;
        set _ccf_alpha_;
        by dt;
        retain rnk;
        if first.dt then rnk = 0;
        rnk + 1;
    run;

    /* Top-10 by frequency (SAS-side; reuses the same freq_ds). In
       coarse mode we still need a dt-keyed _ccf_top10_ dataset so the
       downstream merge stays joinable, but we leave top_10 empty so
       the emitted CSV matches the Athena coarse SQL schema (mean / std /
       top_10 are fine-only output fields). */
    %if &_coarse_mode = 1 %then %do;
        proc sql noprint;
            create table _ccf_top10_ as
            select distinct dt, '' as top_10 length=4000
            from &freq_ds;
        quit;
    %end;
    %else %do;
        proc sort data=&freq_ds out=_ccf_freq_sorted_(where=(p_col is not missing));
            by dt descending cnt p_col;
        run;
        data _ccf_top10_(keep=dt top_10);
            length top_10 $4000 _entry $400;
            set _ccf_freq_sorted_;
            by dt;
            retain top_10 _rn;
            if first.dt then do; top_10 = ''; _rn = 0; end;
            if _rn < 10 then do;
                _rn + 1;
                _entry = cats(strip(vvalue(p_col)), '(', strip(put(cnt, best.)), ')');
                if top_10 = '' then top_10 = _entry;
                else top_10 = catx('; ', top_10, _entry);
            end;
            if last.dt then output;
        run;
    %end;

    /* Weighted mean = SUM(cnt*rnk)/SUM(cnt);
       std = sqrt(SUM(cnt*rnk^2)/SUM(cnt) - mean^2). Output CHAR.

       We also compute length_avg / length_max here -- weighted average
       of LENGTH(p_col) by cnt, and the max length seen. This is the
       same step that produces mean/std, so adding two columns can't
       desync row counts (which is what broke an earlier separate-table
       attempt that caused proc append to lose rows). hash_sum stays
       empty -- cross-platform SAS-vs-Athena hash compare is intrinsically
       different-function and would always flag mismatch otherwise. */
    /* p_col is always character here -- the freq table is created from
       SUBSTR(TO_CHAR(&_col_expr), 1, 200) on the DB side, so we can use
       it directly without vvalue() (which is data-step only and errors
       in proc sql).

       sum_hash is the cnt-weighted 16-bit MD5-truncated hash, matching
       the Athena coarse recipe (_athena_portable_hash16):
         strip(p_col) -> MD5 -> first 4 hex chars (16 bits, max 65535)
       Cross-platform SUM(cnt * hash16) stays inside SAS double's
       2^53 exact-integer range up to ~140 billion rows; identical data
       on Hadoop and AWS produces identical hash_sum values, and any
       value drift surfaces as a hash_sum diff -- the drill-down signal
       we want on Phase-1 cross-platform compares. 32-bit hashes were
       tried earlier but caused silent precision loss in DOUBLE past
       a few million rows, leading to spurious cross-platform mismatches
       from summation-order differences rather than real data drift.

       strip() (not trim()) so leading whitespace is also stripped,
       matching Trino's TRIM() which removes both ends. trim() would
       only strip trailing, so a value like '  CS' would hash differently
       on the two sides. */
    proc sql noprint;
        create table _ccf_stats_ as
        select dt,
               sum(cnt) as sum_cnt,
               sum(cnt * rnk) as sum_cnt_rnk,
               sum(cnt * rnk * rnk) as sum_cnt_rnk2,
               sum(cnt * length(strip(p_col))) as sum_cnt_len,
               max(length(strip(p_col))) as max_len,
               sum(cnt * input(substr(put(md5(strip(p_col)), $hex32.), 1, 4), hex4.))
                   as sum_hash
        from _ccf_ranked_ group by dt;
    quit;
    data _ccf_stats2_(keep=dt mean std length_avg length_max hash_sum);
        length mean std $32 length_avg length_max $32 hash_sum $32;
        set _ccf_stats_;
        if sum_cnt > 0 then do;
            /* mean / std are fine-only -- the Athena coarse SQL emits
               empty for these; we mirror that on the SAS side so the
               two CSV schemas line up. The freq table is still pulled
               (needed for n_total / n_missing / n_unique / val=cnt
               min/max / length / hash), so the saving is purely on
               output, not compute. */
            %if &_coarse_mode = 1 %then %do;
                mean = '';
                std  = '';
            %end;
            %else %do;
                _m = sum_cnt_rnk / sum_cnt;
                _s = sqrt(max(sum_cnt_rnk2 / sum_cnt - _m*_m, 0));
                mean = strip(put(_m, best32.));
                std  = strip(put(_s, best32.));
            %end;
            length_avg = strip(put(sum_cnt_len / sum_cnt, best32.));
            length_max = strip(put(max_len, best32.));
            hash_sum   = strip(put(sum_hash, best32.));
        end;
        else do;
            mean = '';
            std  = '';
            length_avg = '';
            length_max = '';
            hash_sum   = '';
        end;
    run;

    /* Alpha-min (rank=1) and alpha-max (last rank) per dt, encoded "cat=cnt".
       Sorted by val (alphabetically); freq is just for display. */
    proc sort data=_ccf_ranked_; by dt rnk; run;
    data _ccf_mins_(keep=dt min_val) _ccf_maxs_(keep=dt max_val);
        length min_val max_val $400;
        set _ccf_ranked_; by dt;
        if first.dt then do;
            min_val = catx('=', strip(vvalue(p_col)), strip(put(cnt, best.)));
            output _ccf_mins_;
        end;
        if last.dt then do;
            max_val = catx('=', strip(vvalue(p_col)), strip(put(cnt, best.)));
            output _ccf_maxs_;
        end;
    run;

    /* Merge totals + stats + min/max + miss + top_10. length_avg /
       length_max / hash_sum are intentionally left empty on the SAS
       categorical path: a previous attempt to compute them from the
       freq table via _ccf_lenhash_ produced cases where the data step
       did not emit rows, which broke the proc append in the caller --
       leaving cache._cs_<dsname> with only the first row. The
       comparison layer treats empty length/hash as match=None
       ("unknown") so this is the safe default. */
    proc sort data=_ccf_meta_;     by dt; run;
    proc sort data=_ccf_miss_;     by dt; run;
    proc sort data=_ccf_stats2_;   by dt; run;
    proc sort data=_ccf_mins_;     by dt; run;
    proc sort data=_ccf_maxs_;     by dt; run;
    proc sort data=_ccf_top10_;    by dt; run;
    data &out_ds;
        /* column_name $64 (not $32) so long DB identifiers like
           PSTBU_MASDECI_SETID (19 chars) don't get silently truncated
           if proc append later freezes a shorter base cache width. */
        length column_name $64 col_type $12 top_10 $4000
               mean $32 std $32 min_val $400 max_val $400
               length_avg $32 length_max $32 hash_sum $32;
        merge _ccf_meta_(in=a)
              _ccf_stats2_(in=b)
              _ccf_mins_(in=c)
              _ccf_maxs_(in=d)
              _ccf_miss_(in=e)
              _ccf_top10_(in=f);
        by dt;
        if a;
        column_name = "&col";
        col_type = 'categorical';
        /* hash_sum / length_avg / length_max come from _ccf_stats2_ via
           the portable MD5-truncated recipe -- agrees with the Athena
           coarse SQL on identical data so cross-platform drift surfaces
           as a real hash_sum diff. */
        if not e then n_missing = 0;
        if not c then min_val = '';
        if not d then max_val = '';
        if not b then do;
            mean = ''; std = '';
            length_avg = ''; length_max = ''; hash_sum = '';
        end;
        if not f then top_10 = '';
    run;

    /* _ccf_freq_sorted_ only exists in fine mode (top_10 build path).
       proc datasets warns but doesn't fail when deleting a missing
       table; still cleaner to gate the delete list. */
    %if &_coarse_mode = 1 %then %do;
        proc datasets lib=work nolist;
            delete _ccf_meta_ _ccf_miss_ _ccf_alpha_ _ccf_ranked_
                   _ccf_stats_ _ccf_stats2_ _ccf_mins_ _ccf_maxs_
                   _ccf_top10_;
        quit;
    %end;
    %else %do;
        proc datasets lib=work nolist;
            delete _ccf_meta_ _ccf_miss_ _ccf_alpha_ _ccf_ranked_
                   _ccf_stats_ _ccf_stats2_ _ccf_mins_ _ccf_maxs_
                   _ccf_freq_sorted_ _ccf_top10_;
        quit;
    %end;
%mend _col_categorical_freq;
"""


_SAS_LIBNAME_HELPERS = r"""
/* =====================================================================
 * Libname-mode bucket helper. Used when a table opts into
 * connect_method=libname; the libref is mounted at the top of the
 * script and `&_sas_table` is `<libref>.<remote_table>`. The SQL
 * aggregates server-side via SAS/ACCESS implicit pass-through, so we
 * don't pay the cost of streaming raw rows back to local SAS as the
 * source=sas path does.
 *
 * Each driver row carries a literal `dt_label` (one bucket per row), so
 * no `group by &_vintage_expr` is needed -- avoids the question of
 * whether SAS/ACCESS will push intnx() / TRUNC() down. Categorical pulls
 * a (dt-literal, p_col, cnt) freq; %_col_categorical_freq finishes
 * top_10 / mean / std / min / max SAS-side, identical to the oracle and
 * source=sas paths.
 * ===================================================================== */

%macro _col_libname();
    %local _cstat _cache _t0 _elapsed _full_where;
    %let _cstat = _cstat_lib;
    %let _cache = cache._cs_&_dsname;
    %let _t0 = %sysfunc(datetime());
    %if %length(%superq(_base_where)) > 0 %then
        %let _full_where = (&_date_where) AND (&_base_where);
    %else
        %let _full_where = &_date_where;

    /* Per-(qname,col,bucket) banner so the SAS log shows exactly which
       table / column / WHERE the upcoming proc sql is about to run --
       same shape as column_oracle.sas / column_hadoop.sas's banners. */
    %put NOTE: ---- [&_qname / &_col] (&_coltype, bucket=&_dt_label) libname stats pull START ----;
    %put NOTE:      FROM    : &_sas_table;
    %put NOTE:      BUCKET  : &_dt_label;
    %put NOTE:      WHERE   : %superq(_full_where);

    %if %upcase(&_coltype) = NUMERIC %then %do;
        %put NOTE:      SQL     : select "&_dt_label" as dt, count(*), count(distinct &_col), avg(&_col), std(&_col), min(&_col), max(&_col) from &_sas_table where %superq(_full_where);
        proc sql noprint;
            create table &_cstat as
            select "&_dt_label" as dt length=32,
                   "&_col"      as column_name length=32,
                   'numeric'    as col_type length=32,
                   count(*)                       as n_total,
                   count(*) - count(&_col)        as n_missing,
                   count(distinct &_col)          as n_unique,
                   avg(&_col)                     as mean,
                   std(&_col)                     as std,
                   strip(put(min(&_col), best32.)) as min_val length=200,
                   strip(put(max(&_col), best32.)) as max_val length=200,
                   ''           as top_10 length=4000
            from &_sas_table
            where &_date_where
            %if %length(%superq(_base_where)) > 0 %then AND (&_base_where) ;
            ;
        quit;
    %end;
    %else %do;
        /* Pull (dt-literal, p_col, cnt) freq for categorical post-processing. */
        %put NOTE:      SQL     : select "&_dt_label" as dt, substr(put(&_col,$200.),1,200) as p_col, count(*) as cnt from &_sas_table where %superq(_full_where) group by p_col;
        proc sql noprint;
            create table _lib_freq_ as
            select "&_dt_label" as dt length=32,
                   substr(strip(put(&_col, $200.)), 1, 200) as p_col length=200,
                   count(*) as cnt
            from &_sas_table
            where &_date_where
            %if %length(%superq(_base_where)) > 0 %then AND (&_base_where) ;
            group by p_col;
        quit;
        %_col_categorical_freq(freq_ds=_lib_freq_, col=&_col, out_ds=&_cstat);
        proc delete data=_lib_freq_; run;
    %end;

    proc append base=&_cache data=&_cstat force; run;
    proc delete data=&_cstat; run;

    %let _elapsed = %sysevalf(%sysfunc(datetime()) - &_t0);
    %put WARNING- ==== [&_qname / &_col] (&_coltype, bucket=&_dt_label) DONE in %sysfunc(putn(&_elapsed, 8.2))s ====;
%mend _col_libname;
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
    /* Build (dt, cat, count) freq table once. Categories sorted alphabetically
       (case-insensitive) get rank 1..k per dt; mean/std/min/max use these
       ranks so any permutation of counts across categories produces
       different stats. min/max are formatted "{cat}={count}". */
    proc sql noprint;
        create table _freq_raw_ as
        select dt, &col as p_col, count(*) as value_freq
        from &raw_ds
        group by dt, &col;
    quit;

    /* Top-10 by frequency (unchanged). */
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

    /* Alphabetic-rank pass: rank categories by upcase(cat) within each dt. */
    data _alpha_;
        set _freq_raw_;
        where p_col is not missing;
        length _p_upper $400;
        _p_upper = upcase(strip(vvalue(p_col)));
    run;
    proc sort data=_alpha_; by dt _p_upper p_col; run;
    data _ranked_;
        set _alpha_;
        by dt;
        retain rnk;
        if first.dt then rnk = 0;
        rnk + 1;
    run;

    /* Weighted aggregates: mean = sum(cnt*rnk)/sum(cnt),
                            std  = sqrt(sum(cnt*rnk^2)/sum(cnt) - mean^2). */
    proc sql noprint;
        create table _stats_ as
        select dt,
               sum(value_freq) as sum_cnt,
               sum(value_freq * rnk) as sum_cnt_rnk,
               sum(value_freq * rnk * rnk) as sum_cnt_rnk2
        from _ranked_ group by dt;
    quit;
    data _stats2_(keep=dt mean std);
        set _stats_;
        if sum_cnt > 0 then do;
            mean = sum_cnt_rnk / sum_cnt;
            std = sqrt(max(sum_cnt_rnk2 / sum_cnt - mean*mean, 0));
        end;
        else do;
            mean = .;
            std = .;
        end;
    run;

    /* Min: rank=1 row per dt. Max: last rank per dt.
       Sorted by val (alphabetically); freq is just for display. */
    proc sort data=_ranked_ out=_ranked_sorted_; by dt rnk; run;
    data _mins_(keep=dt min_val);
        set _ranked_sorted_; by dt;
        length min_val $400;
        if first.dt then min_val = catx('=', strip(vvalue(p_col)), strip(put(value_freq, best.)));
        if first.dt then output;
    run;
    data _maxs_(keep=dt max_val);
        set _ranked_sorted_; by dt;
        length max_val $400;
        if last.dt then max_val = catx('=', strip(vvalue(p_col)), strip(put(value_freq, best.)));
        if last.dt then output;
    run;

    /* n_total/n_unique from full freq (includes NULL p_col), n_missing
       from rows where p_col is missing. */
    proc sql noprint;
        create table _meta_ as
        select dt,
               sum(value_freq) as n_total,
               sum(case when p_col is not missing then 1 else 0 end) as n_unique
        from _freq_raw_ group by dt;
        create table _miss_ as
        select dt, coalesce(value_freq, 0) as n_missing
        from _freq_raw_ where p_col is missing;
    quit;

    proc sort data=_meta_; by dt; run;
    proc sort data=_t10_; by dt; run;
    proc sort data=_miss_; by dt; run;
    data &out_ds;
        length column_name $32 col_type $32;
        merge _meta_(in=a) _stats2_(in=b) _mins_(in=c) _maxs_(in=d) _t10_(in=e) _miss_(in=f);
        by dt;
        if a;
        column_name = "&col";
        col_type = 'categorical';
        if not e then top_10 = '';
        if not f then n_missing = 0;
    run;
    proc datasets lib=work nolist;
        delete _freq_raw_ _freq_sorted_ _alpha_ _ranked_ _ranked_sorted_
               _stats_ _stats2_ _mins_ _maxs_ _t10_ _meta_ _miss_;
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
    # Libname mode runs SAS proc sql against a libref-mounted Oracle/Hadoop
    # table. WHERE/date literals must use SAS form (`'15JAN2025'd`) so the
    # local SAS engine can plan the query — same plumbing the source=sas
    # path already uses. Keep the narrow `is_sas` for the dispatch-bucket
    # decision in `_gen_sas_col_driver` (libname tables get their own
    # bucket and shouldn't be confused with source=sas).
    is_sas_syntax = is_sas or _is_libname_table(tbl_cfg)

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
                    is_sas=is_sas_syntax, date_format=date_format,
                )
                fl, tl = format_date_bounds_literals(
                    bmin, bmax, date_dtype, is_sas=is_sas_syntax, date_format=date_format,
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
                    is_sas=is_sas_syntax, date_format=date_format,
                )
                fl, tl = format_date_bounds_literals(
                    min(dates), max(dates), date_dtype,
                    is_sas=is_sas_syntax, date_format=date_format,
                )
                bucket_specs.append((bucket_key, dw, fl, tl))
    elif date_filter['filter_type'] == 'in_list':
        dw = build_date_in_clause(
            date_col, date_filter['dates'], date_dtype,
            is_sas=is_sas_syntax, date_format=date_format,
        )
        fl, tl = format_date_bounds_literals(
            min(date_filter['dates']), max(date_filter['dates']),
            date_dtype, is_sas=is_sas_syntax, date_format=date_format,
        )
        bucket_specs.append(('sample', dw, fl, tl))
    else:
        # No filter and no synthesis (typical: vintage='all' without
        # matching_dates). If --from/--to are set, still emit a BETWEEN so the
        # SAS-source / libname pull is bounded to the user's range. Mirrors
        # the same fall-through in _compute_col_spec.
        from_d = tbl_cfg.get('_from_date')
        to_d = tbl_cfg.get('_to_date')
        if from_d and to_d:
            dw = build_date_between_clause(
                date_col, from_d, to_d, date_dtype,
                is_sas=is_sas_syntax, date_format=date_format,
            )
            fl, tl = format_date_bounds_literals(
                from_d, to_d, date_dtype,
                is_sas=is_sas_syntax, date_format=date_format,
            )
            bucket_specs.append(('all', dw, fl, tl))
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
        # Threaded through so the Oracle per-bucket helper can reuse the
        # synthesized vintage_spans (when from/to was used to build buckets
        # without matching_dates) without re-calling compute_date_filter --
        # that second call would skip the synthesis since the vintage_spans
        # synthesis happens here, not inside compute_date_filter.
        'date_filter': date_filter,
        'date_format': date_format,
    }


def _compute_oracle_bucket_specs(spec, tbl_cfg, db_path):
    """Per-bucket WHERE fragments for the Oracle col-extraction path.

    Oracle hits ORA-01652 (TEMP tablespace exhausted) when the single-query
    `GROUP BY vintage_expr` form materializes a large workarea across the
    full date range -- a problem Hadoop's local-spill model doesn't have.
    The fix is to emit one passthrough per (column × bucket) instead of
    one passthrough per column with a DB-side GROUP BY: each query then
    has a per-bucket WHERE narrowing the scan, no `GROUP BY vintage`, and
    its TEMP workarea fits regardless of how many years the overlap range
    spans.

    Returns [(dt_label, bucket_where), ...]:
      dt_label     - the bucket key (e.g. '2025-01-01' for year, '2025-04'
                     for quarter, 'all' for single-bucket modes)
      bucket_where - WHERE-body fragment that filters date_col to this
                     bucket only. Format follows date_dtype (DATE 'X' AND
                     DATE 'Y+1day' for date/timestamp, bare integers for
                     YYYYMMDD-stored numerics, quoted strings for VARCHAR
                     date columns).

    For `vintage=all` or in-list / sample modes there's a single bucket;
    bucket_specs has length 1 and behaves identically to the old
    single-query shape.

    Only called on the Oracle path. Hadoop keeps the original single-query
    `GROUP BY &_vintage_expr` form because Hive's per-task spill scales
    horizontally on HDFS and doesn't need the splitting.
    """
    from .base import (
        build_date_between_clause, build_date_in_clause,
        build_date_range_with_gaps,
    )

    date_col = spec['date_col']
    date_dtype = spec['date_dtype']
    vintage = spec['vintage']
    # Reuse the date_filter that _compute_col_spec already built. It
    # carries the synthesized vintage_spans for vintage=year/month/etc.
    # when from/to is set without matching_dates -- a second
    # compute_date_filter call here would miss that synthesis since it
    # happens inside _compute_col_spec.
    date_filter = spec.get('date_filter') or {}
    date_format = spec.get('date_format') or date_filter.get('date_format')
    effective_vintage = date_filter.get('vintage') or vintage

    bucket_specs = []

    if date_filter.get('filter_type') == 'between':
        if 'vintage_spans' in date_filter:
            # Contiguous bucket spans synthesized from from/to (typical when
            # no row-compare has run yet but vintage bucketing was asked
            # for). Each span is one bucket.
            for bucket_key, bmin, bmax in date_filter['vintage_spans']:
                dw = build_date_between_clause(
                    date_col, bmin, bmax, date_dtype,
                    is_sas=False, date_format=date_format,
                )
                bucket_specs.append((bucket_key, dw))
        elif date_filter.get('dates'):
            # Real row-compare matching_dates. Bucket them into vintage
            # spans the same way the SAS-source / libname paths do.
            from ..date_utils import bucket_date
            buckets = {}
            for dt in date_filter['dates']:
                buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
            for bucket_key, dates in sorted(buckets.items()):
                dw = build_date_range_with_gaps(
                    date_col, dates, date_dtype,
                    is_sas=False, date_format=date_format,
                )
                bucket_specs.append((bucket_key, dw))
    elif date_filter.get('filter_type') == 'in_list':
        # Sample-mode: single bucket with all sampled dates.
        dw = build_date_in_clause(
            date_col, date_filter['dates'], date_dtype,
            is_sas=False, date_format=date_format,
        )
        bucket_specs.append(('sample', dw))

    # Fall back to a single 'all' bucket reusing spec's full-range WHERE.
    # Covers vintage='all' and any path that didn't produce buckets.
    if not bucket_specs:
        bucket_specs.append((vintage or 'all', spec['where_clause']))

    return bucket_specs


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

    # _col_redo is always 0 by default in the generated SAS so resume
    # works out of the box. To force a full re-extract, either:
    #   - edit the generated extract_col.sas and change `%let _col_redo
    #     = 0;` to `= 1;` at the top, OR
    #   - `proc delete data=cache._cs_<dsname>; run;` before launching.
    # The env-var-driven override (SAS_COL_REDO) was removed because it
    # was easy to leave set from a previous shell and silently wipe
    # caches the user expected to be preserved.
    redo = '0'
    row_lines = []
    seen = {}
    for r in rows:
        # n_unique_expr defaults to COUNT(DISTINCT col) for back-compat with
        # any caller that didn't populate the new field; the dispatch loop
        # in _gen_sas_col_driver always sets it via _n_unique_expr().
        n_unique_expr = r.get('n_unique_expr',
                              f"COUNT(DISTINCT {r['col_name']})")
        # col_expr is the sentinel-wrapped column expression. For legacy
        # callers (and the existing back-compat default in n_unique_expr)
        # fall back to the bare col_name when not provided.
        col_expr = r.get('col_expr') or r['col_name']
        # dt_label is the BARE bucket key (e.g. '2022-01-01' without
        # surrounding SQL quotes). _col_oracle / _col_hadoop use it to
        # query cache._cs_<dsname> for already-completed (column_name,
        # dt) pairs and skip them, so an interrupted run can resume
        # without redoing finished buckets. Empty string for Hadoop
        # single-query mode (resume is per-column there, not per-bucket).
        dt_label = r.get('dt_label', '') or ''
        row_lines.append(
            f"    qname='{_sas_escape(r['qname'])}'; "
            f"dsname='{_sas_escape(r['dsname'])}'; "
            f"conn_macro='{_sas_escape(r['conn_macro'])}'; "
            f"col_name='{_sas_escape(r['col_name'])}'; "
            f"col_expr='{_sas_escape(col_expr)}'; "
            f"col_type='{r['col_type']}'; "
            f"date_col='{_sas_escape(r['date_col'])}'; "
            f"vintage='{_sas_escape(r['vintage'])}'; "
            f"vintage_expr='{_sas_escape(r['vintage_expr'])}'; "
            f"dt_label='{_sas_escape(dt_label)}'; "
            f"from_table='{_sas_escape(r['from_table'])}'; "
            f"date_from='{_sas_escape(r['date_from'])}'; "
            f"date_to='{_sas_escape(r['date_to'])}'; "
            f"where_clause='{_sas_escape(r['where_clause'])}'; "
            f"n_unique_expr='{_sas_escape(n_unique_expr)}'; output;"
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


def _gen_sas_col_driver(tables, db_path=None, out_dir='.', config=None, coarse=False):
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
    lib_driver_rows = []  # libname-mode (any source); aggregates via libref
    ora_rows = []
    hdp_rows = []
    qnames_sas = []
    qnames_lib = []
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

        is_libname = _is_libname_table(tbl_cfg)
        tbl_source = 'sas' if is_sas else (tbl_cfg.get('source') or 'oracle').lower()
        table_from, _ = _resolve_table_inline(tbl_cfg)
        # SAS path still needs the qualified conn.name reference only.
        if is_sas:
            sas_table_ref = table_from
        elif is_libname:
            # Libname mounts the remote schema as a libref; the table
            # reference inside SAS proc sql is `<libref>.<remote_table>`.
            sas_table_ref = f"{conn_macro}.{tbl_cfg['table']}"
        else:
            sas_table_ref = None  # unused on the DB path

        n_buckets = len(bucket_specs)
        n_cols = len(col_list)

        if is_libname:
            # connect_method=libname: aggregate server-side via SAS/ACCESS
            # implicit pass-through. Same fan-out shape as sas_driver_rows
            # (one row per qname × bucket × col), but %_col_libname does
            # the aggregation in a single SQL against `&_sas_table` rather
            # than pulling raw rows local first.
            qnames_lib.append((qname, dsname))
            # is_oracle drives is_numeric_type's type catalog; for hadoop
            # libname use the hadoop catalog.
            is_oracle_typed = (tbl_source != 'hadoop')
            for bucket_key, date_where, _fl, _tl in bucket_specs:
                for col_name, col_dtype in col_list:
                    col_type = ('numeric'
                                if is_numeric_type(col_dtype, is_oracle=is_oracle_typed)
                                else 'categorical')
                    lib_driver_rows.append({
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
            print(f"[libname col] {qname}: {n_cols} cols x {n_buckets} buckets "
                  f"= {n_cols * n_buckets} runs (libname source={tbl_source})")
            continue

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

        # Oracle / Hadoop: DB-side aggregation. Two driver shapes:
        #
        #   Hadoop : one row per (qname, col); the macro's SQL does
        #            GROUP BY &_vintage_expr DB-side and returns one
        #            result row per bucket. Hive's per-task local-disk
        #            spill scales horizontally, so a single full-range
        #            GROUP BY scales fine -- no TEMP wall.
        #
        #   Oracle : one row per (qname, col, vintage_bucket). The SQL
        #            emits each bucket as its own passthrough with the
        #            bucket label as a SQL literal and the WHERE clause
        #            narrowed to that bucket's date range. Avoids
        #            ORA-01652 (TEMP tablespace exhausted) on multi-year
        #            scans -- each per-bucket query's GROUP BY / COUNT
        #            DISTINCT workarea is sized by one bucket only.
        #            Trade-off: N_buckets more Oracle round trips per
        #            column (parse + plan + fetch overhead) instead of
        #            one wide query; in practice the overhead is much
        #            cheaper than failing outright.
        spec = _compute_col_spec(tbl_cfg, db_path)
        if tbl_source == 'oracle':
            rows_bucket = ora_rows
            qnames_ora.append(qname)
            numeric_is_oracle = True
            # Per-bucket WHERE fragments. For vintage='all' / sample /
            # plain modes this is a single bucket -- behaves like the
            # old single-query path.
            oracle_bucket_specs = _compute_oracle_bucket_specs(spec, tbl_cfg, db_path)
        elif tbl_source == 'hadoop':
            rows_bucket = hdp_rows
            qnames_hdp.append(qname)
            numeric_is_oracle = False
            oracle_bucket_specs = None
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
            # Sentinel-wrapped column expression. Every aggregate in the
            # SAS template (AVG/STDDEV/MIN/MAX/COUNT(DISTINCT)) sees this
            # instead of the bare col so configured sentinel values
            # (constants.NULL_SENTINELS[<source>]) collapse to NULL before
            # the aggregate runs. The literal output `'col_name' AS
            # column_name` still uses the bare name; col_expr only swaps
            # in at aggregation sites. Pass the raw col_dtype so DATE /
            # TIMESTAMP columns get DATE sentinels (DATE '9999-12-31',
            # ...) instead of string sentinels ('NA', 'N/A', ...) -- the
            # latter triggers ORA-01858 when Oracle tries to convert 'NA'
            # to a date for the IN comparison.
            col_expr = _col_expr_for_source(tbl_source, col_name, col_type, col_dtype)
            if oracle_bucket_specs is not None:
                # Oracle per-bucket expansion: emit N driver rows, one
                # per (col, bucket). vintage_expr becomes the bucket
                # label as a SQL string literal (e.g. "'2022-01-01'");
                # the SAS template's `SELECT &_vintage_expr AS dt` then
                # produces "SELECT '2022-01-01' AS dt" and the
                # accompanying GROUP BY collapses to a single constant
                # group per query. where_clause carries the per-bucket
                # BETWEEN narrowed to this bucket's range.
                for dt_label, bucket_where in oracle_bucket_specs:
                    bucket_full_where = _combine_where(bucket_where, user_where)
                    rows_bucket.append({
                        'qname': qname,
                        'dsname': dsname,
                        'conn_macro': conn_macro,
                        'col_name': col_name,
                        'col_type': col_type,
                        'col_expr': col_expr,
                        'date_col': spec['date_col'],
                        'vintage': spec['vintage'],
                        'vintage_expr': f"'{dt_label}'",
                        # dt_label (bare bucket key, no SQL quoting) lets
                        # the resume-on-restart check in _col_oracle look
                        # up `cache._cs_<dsname>` by (column_name, dt) and
                        # skip already-computed buckets. Stays a separate
                        # field from vintage_expr (which wraps the same
                        # value in SQL single quotes for the SELECT).
                        'dt_label': dt_label,
                        'from_table': table_from,
                        'date_from': spec['date_from'],
                        'date_to': spec['date_to'],
                        'where_clause': bucket_full_where,
                        'n_unique_expr': _n_unique_expr(
                            tbl_source, col_expr,
                            tbl_cfg=tbl_cfg, config=config),
                    })
                continue
            # Hadoop path (and any future single-query backends): one
            # row per (qname, col), DB-side GROUP BY vintage. The query
            # produces N result rows (one per vintage) atomically -- if
            # it returned at all, every vintage is present, so the
            # resume check uses column_name alone (any row hit = done).
            rows_bucket.append({
                'qname': qname,
                'dsname': dsname,
                'conn_macro': conn_macro,
                'col_name': col_name,
                'col_type': col_type,
                'col_expr': col_expr,
                'date_col': spec['date_col'],
                'vintage': spec['vintage'],
                'vintage_expr': spec['vintage_expr'],
                # Hadoop's single-query path doesn't produce per-row dt
                # labels (vintage_expr is a DB function evaluated DB-side);
                # the resume check looks at column_name only, so the
                # field is included as empty for schema consistency with
                # the Oracle path.
                'dt_label': '',
                'from_table': table_from,
                'date_from': spec['date_from'],
                'date_to': spec['date_to'],
                'where_clause': full_where,
                # Per-platform n_unique expression — uses col_expr so the
                # distinct-count is taken over sentinel-translated values.
                'n_unique_expr': _n_unique_expr(tbl_source, col_expr,
                                                tbl_cfg=tbl_cfg, config=config),
            })
        if oracle_bucket_specs is not None:
            nb = len(oracle_bucket_specs)
            print(f"[{tbl_source} col] {qname}: {n_cols} cols x {nb} buckets "
                  f"(per-bucket queries, vintage={spec['vintage']}) "
                  f"= {n_cols * nb} runs")
        else:
            print(f"[{tbl_source} col] {qname}: {n_cols} cols (db-side "
                  f"GROUP BY {spec['vintage_expr']!s:.60} vintage={spec['vintage']}) "
                  f"= {n_cols} runs")

    total = len(sas_driver_rows) + len(lib_driver_rows) + len(ora_rows) + len(hdp_rows)
    if total == 0:
        return "\n/* No col extraction rows generated (col_filter zeroed everything out). */\n"
    # _coarse_mode controls whether the fine-only output fields (mean,
    # std, top_10) are populated in the per-column CSV. The freq table
    # is still pulled for categorical so coarse signals (n_total /
    # n_missing / n_unique / min / max with val=cnt / length_* /
    # hash_sum) can be computed -- coarse only changes the OUTPUT
    # schema, not the work done. Matches Athena's coarse SQL (Athena
    # blanks mean/std/top_10 in _build_coarse_sql).
    coarse_mode_let = f"%let _coarse_mode = {1 if coarse else 0};\n"
    # Retry knobs for transient connection failures inside _col_oracle /
    # _col_hadoop (Hive's "couldn't open client transport" hiccups during
    # multi-hour extractions, Oracle network blips).
    # Resolution order (first present wins):
    #   1. dtrack.json metadata.{ora,hdp}_retry_{count,sleep}
    #      (set via /api/global_settings, settings modal dropdown)
    #   2. env var DTRACK_{ORA,HDP}_RETRY_{COUNT,SLEEP}
    #   3. defaults: 3 attempts, 30s cooldown
    _meta = (config or {}).get('metadata') or {}
    def _retry_val(meta_key, env_key, default):
        v = _meta.get(meta_key)
        if v is not None and v != "":
            try:
                return int(v)
            except (ValueError, TypeError):
                pass
        return int(os.environ.get(env_key, str(default)))
    _ora_retry_count = _retry_val('ora_retry_count', 'DTRACK_ORA_RETRY_COUNT', 3)
    _ora_retry_sleep = _retry_val('ora_retry_sleep', 'DTRACK_ORA_RETRY_SLEEP', 30)
    _hdp_retry_count = _retry_val('hdp_retry_count', 'DTRACK_HDP_RETRY_COUNT', 3)
    _hdp_retry_sleep = _retry_val('hdp_retry_sleep', 'DTRACK_HDP_RETRY_SLEEP', 30)
    retry_lets = (
        f"%let _ora_retry_count = {_ora_retry_count};\n"
        f"%let _ora_retry_sleep = {_ora_retry_sleep};\n"
        f"%let _hdp_retry_count = {_hdp_retry_count};\n"
        f"%let _hdp_retry_sleep = {_hdp_retry_sleep};\n"
    )
    n_tbl = (len(qnames_sas) + len(qnames_lib)
             + len(qnames_ora) + len(qnames_hdp))
    print(f"[SAS col] TOTAL: {n_tbl} tables "
          f"(sas={len(qnames_sas)}, libname={len(qnames_lib)}, "
          f"oracle={len(qnames_ora)}, hadoop={len(qnames_hdp)}), "
          f"{total} driver rows")

    # _export_col_stats is needed by all paths; the per-path helper macros
    # are only emitted when their bucket is non-empty. _coarse_mode + the
    # retry knobs are set FIRST so every downstream macro
    # (_col_categorical_freq, the per-source templates) can read them
    # via &_coarse_mode / &_ora_retry_* / &_hdp_retry_* at run time.
    parts = [coarse_mode_let, retry_lets, _SAS_EXPORT_HELPER]
    if sas_driver_rows:
        parts.append(_SAS_SOURCE_HELPERS)
    if lib_driver_rows:
        parts.append(_SAS_LIBNAME_HELPERS)

    # SAS-source flat driver (bucket mode).
    if sas_driver_rows:
        parts.append("")
        parts.append("/* Cache lifecycle (sas-source path). Each col step")
        parts.append("   proc-appends to cache._cs_<dsname>. Wipe only when")
        parts.append("   _col_redo=1 (SAS_COL_REDO=1 at gen time); otherwise")
        parts.append("   leave the cache intact so an interrupted run can")
        parts.append("   resume -- _pull_one_col / _col_libname check the")
        parts.append("   cache for (col, dt) before each pull and skip")
        parts.append("   already-completed work orders. */")
        seen_ds = set()
        for q, ds in qnames_sas:
            if ds in seen_ds:
                continue
            seen_ds.add(ds)
            parts.append(
                f"%if &_col_redo = 1 and %sysfunc(exist(cache._cs_{ds})) %then %do; "
                f"proc delete data=cache._cs_{ds}; run; %end;"
            )
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

    # Libname-mode flat driver: identical fan-out shape to sas-source but
    # dispatches to %_col_libname (server-side aggregation through the
    # libref) instead of %_pull_one_col (raw pull + local SAS aggregate).
    if lib_driver_rows:
        parts.append("")
        parts.append("/* Cache lifecycle (libname path). Same rule as the")
        parts.append("   sas-source path: wipe only when _col_redo=1. Default")
        parts.append("   keeps existing rows so resume can skip them. */")
        seen_ds = set()
        for q, ds in qnames_lib:
            if ds in seen_ds:
                continue
            seen_ds.add(ds)
            parts.append(
                f"%if &_col_redo = 1 and %sysfunc(exist(cache._cs_{ds})) %then %do; "
                f"proc delete data=cache._cs_{ds}; run; %end;"
            )
        parts.append("")
        parts.append("/* Libname flat driver: one row per (qname, bucket, col) */")
        parts.append("data _driver_col_lib_;")
        parts.append("    length qname $64 dsname $32 sas_table $128 conn_macro $16")
        parts.append("           col_name $32 col_type $12 dt_label $32")
        parts.append("           date_where $4000 base_where $4000;")
        for r in lib_driver_rows:
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
        parts.append("    set _driver_col_lib_;")
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
        parts.append("        '%nrstr(%_col_libname)();'")
        parts.append("    );")
        parts.append("    call execute(_cmd);")
        parts.append("run;")
        parts.append("proc delete data=_driver_col_lib_; run;")
        parts.append("")
        parts.append("/* Per-qname CSV export (libname) */")
        for q, ds in qnames_lib:
            parts.append(
                f"%_export_col_stats(dsname={ds}, qname={q}, outdir=&out_dir);")

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
            from_date=None, to_date=None, cells_filter=None, coarse=False,
            preset=None, stats_override=None):
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
        coarse: When True, suppress fine-only output fields (mean, std,
            top_10) on the SAS side so the emitted CSV matches the
            Athena coarse SQL output schema. Coarse signals
            (n_total, n_missing, n_unique, min, max, length_avg,
            length_max, hash_sum) are always emitted. The freq table
            is still pulled for categorical columns because the
            coarse signals are computed from it -- we only blank the
            "fine-only" fields in the merged output, so the cost is
            the same as fine mode.
    """
    if types is None:
        types = ["row", "col"]

    # Map --preset/--stats-override onto the SAS path. The SAS template
    # only branches on a single `coarse` flag (quick-equivalent), so:
    #   preset='quick'      → coarse=True
    #   preset='detailed'   → coarse=False
    #   preset='standard'   → coarse=False (full extraction; per-stat
    #                         masking happens post-extraction when CSVs
    #                         are loaded via stats_override-aware paths)
    # stats_override on the SAS side is not honored at SQL-gen time; the
    # web-UI col_gen path handles it via /api/generate, not gen_sas. Warn
    # so CLI callers know.
    if preset == 'quick':
        coarse = True
    if stats_override:
        print("WARNING: --stats-override has no effect on gen-sas (SAS "
              "template emits the full coarse/fine schema). Apply per-stat "
              "masking downstream or use gen-aws/gen-oracle.")

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

    # Fine-cell selection plumbing: cells_filter is keyed by qname and lists
    # which (col, [vintages]) the user picked from the coarse compare view.
    # Stash on each table dict; _apply_col_filter drops anything else.
    if cells_filter:
        for tbl in all_tables:
            qn = qualified_name(tbl)
            sel = cells_filter.get(qn)
            if sel:
                tbl['_fine_cells'] = sel

    # Auto-skip ID-like cols based on cached cardinality from prior load-col.
    # Cache source: _col_stats. Freshness: _metadata.last_loaded vs TTL
    # (DTRACK_ID_CACHE_TTL_DAYS, default 1.0). First-run on a never-loaded
    # table = empty cache = no skip = full extraction (current behaviour).
    # Subsequent runs within TTL skip ID cols (≥ DTRACK_ID_THRESHOLD ratio).
    if db_path and os.environ.get('DTRACK_ID_DETECTION', '1') != '0':
        from .base import read_id_cols_from_col_stats
        id_threshold = float(os.environ.get('DTRACK_ID_THRESHOLD', '0.99'))
        ttl_days = float(os.environ.get('DTRACK_ID_CACHE_TTL_DAYS', '1.0'))
        for tbl in all_tables:
            qn = qualified_name(tbl)
            id_cols = read_id_cols_from_col_stats(
                db_path, qn, threshold=id_threshold, ttl_days=ttl_days,
            )
            if id_cols:
                tbl['_id_cols_to_skip'] = id_cols
                print(f"  [id-cache] {qn}: skipping {len(id_cols)} ID-like "
                      f"cols (threshold={id_threshold}, TTL={ttl_days}d): "
                      f"{sorted(id_cols)}")

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

    # Oracle tables go through dtrack.platforms.oracle.extract_oracle now
    # (Python-direct via oracledb). gen_sas handles SAS-local and hadoop_sas
    # only -- `source=hadoop` runs directly through hadoop_connect (the
    # gen-first plain-Hive path in platforms/hadoop), so it is intentionally
    # excluded here. `hadoop_sas` is normalized to `hadoop` so every downstream
    # SAS dialect helper / template keeps working unchanged.
    oracle_tables = [t for t in all_tables if t.get('source', '').lower() in ('sas', 'hadoop_sas')]
    for t in oracle_tables:
        if t.get('source', '').lower() == 'hadoop_sas':
            t['source'] = 'hadoop'

    if not oracle_tables:
        print("No SAS/Hadoop tables found in config")
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
        # Data-driven row extraction. Libname-mode hadoop tables are merged
        # into the oracle/sas-bucket call so they get rows_sas.sas treatment
        # (their conn_macro is the libref, references are <libref>.<table>).
        # `_gen_sas_row_hadoop` skips them internally so they aren't double-emitted.
        libname_hadoop_tables = [t for t in hadoop_tables if _is_libname_table(t)]
        macro_parts.append("/* --- Row extraction (data-driven) --- */")
        macro_parts.append(
            _gen_sas_row_datadriven(oracle_tables + libname_hadoop_tables, sas_lib, out_dir)
        )

        if hadoop_tables and any(not _is_libname_table(t) for t in hadoop_tables):
            macro_parts.append("/* --- Hadoop row extraction (data-driven) --- */")
            macro_parts.append(_gen_sas_row_hadoop(hadoop_tables, sas_lib, out_dir))

    if "col" in types:
        # Flat-driver structure: ONE driver dataset across all tables, one
        # dispatcher loop, shared helper macros. Replaces the per-table
        # get_colstats_SN macro soup -- smaller extract_col.sas file, no
        # 32K SAS line-length issues, and col_filter is honored per-row.
        macro_parts.append("/* --- Col extraction (flat driver across all tables) --- */")
        macro_parts.append(_gen_sas_col_driver(all_col_tables, db_path, out_dir, config=config, coarse=coarse))

    # Collect libname mount/clear lines BEFORE runner_parts so the runner
    # can reference libname_clear_lines below. Each unique conn_macro that
    # has at least one libname-mode table contributes one mount + one clear.
    libname_macros_seen: set = set()
    libname_mount_lines: list = []
    libname_clear_lines: list = []
    for t in oracle_tables + hadoop_tables:
        if not _is_libname_table(t):
            continue
        macro = t.get('conn_macro', '')
        if not macro or macro in libname_macros_seen:
            continue
        libname_macros_seen.add(macro)
        libname_mount_lines.append(_libname_mount_for(t))
        libname_clear_lines.append(f"libname {macro} clear;")

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

    # Tear down libname mounts at the end so a re-run starts clean.
    if libname_clear_lines:
        runner_parts.append('')
        runner_parts.append('/* Clear libname mounts */')
        runner_parts.extend(libname_clear_lines)

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

    # Inject libname mount lines into the conn_macros block (collected above).
    if libname_mount_lines:
        conn_macro_lines.append('')
        conn_macro_lines.append('/* SAS LIBNAME mounts (connect_method=libname) */')
        conn_macro_lines.extend(libname_mount_lines)

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

    # Optional Oracle/Hadoop SQL traffic dump. Set DTRACK_SAS_TRACE=1 to
    # have every SAS/ACCESS passthrough block log the literal SQL it sends
    # plus any errors that come back. Useful for diagnosing things like
    # ORA-01861 ("literal does not match format string") where the WHERE
    # clause in the SAS NOTE doesn't tell you whether `DATE 'yyyy-mm-dd'`
    # vs bare `'yyyy-mm-dd'` is what actually hit the DB. Off by default
    # to keep logs quiet.
    if os.environ.get('DTRACK_SAS_TRACE', '0') == '1':
        sastrace_options = (
            "/* DTRACK_SAS_TRACE=1: dump SAS/ACCESS SQL traffic to the log */\n"
            "options sastrace=',,,d' sastraceloc=saslog nostsuffix;\n"
            "options msglevel=i;"
        )
    else:
        sastrace_options = ""

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
        'sastrace_options': sastrace_options,
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

    # Discover and save column metadata CSVs (Oracle/SAS tables only). Hadoop
    # is intentionally excluded: hadoop_sas tables (normalized to 'hadoop'
    # above) get their columns from the generated SAS file at runtime, and
    # `source=hadoop` (direct) is not in this gen path at all -- routing it
    # through _discover_and_write_columns here would trigger an unwanted
    # hadoop_connect DESCRIBE.
    sas_tables = [t for t in all_tables
                  if t.get('source', '').lower() in ('oracle', 'sas', '')]
    if db_path and sas_tables:
        _discover_and_write_columns(sas_tables, outdir, db_path)

    # In mock mode, also copy mock CSVs to outdir
    if mock_dir:
        _extract_sas_mock(config_path, outdir, types, db_path, mock_dir)


# ---------------------------------------------------------------------------
# Column discovery and CSV writing
# ---------------------------------------------------------------------------


def _extract_sas_mock(config_path, outdir, types, db_path, mock_dir):
    """Mock-mode entry for gen_sas. Copies pre-built CSVs for SAS / Hadoop tables."""
    return _extract_mock(config_path, outdir, types, db_path, mock_dir, ('sas', 'hadoop'))


# ---------------------------------------------------------------------------
# Convenience SQL builder functions (backward-compatible aliases)
# ---------------------------------------------------------------------------


