"""OracleBuilder: Python-direct Oracle extraction via oracledb.

Replaces the legacy SAS/ACCESS passthrough path. Per-column SQL is submitted
in parallel via ThreadPoolExecutor with a strict cap on concurrent sessions
(ORACLE_MAX_WORKERS, default 2) so we never breach the server-side per-user
session limit. Results are cached in {outdir}/.cache.db (shared format with
the Athena cache) so interrupted runs resume cheaply.

Output schema matches the Athena `{qname}_col.csv` 11-field layout:
    column_name, dt, col_type, n_total, n_missing, n_unique,
    mean, std, min_val, max_val, top_10
"""

import csv
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from .base import (
    PlatformBuilder,
    qualified_name,
    is_numeric_type,
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
    quote_ident,
    apply_null_sentinels,
    set_cache_db,
    cache_stats,
    cache_get,
    cache_put,
    get_mock_dir,
    _extract_mock,
    _extract_mock_tables,
)


# Oracle TRUNC fmt for each vintage label.
_VINTAGE_ORACLE_TRUNC = {
    'day': 'DD', 'week': 'IW', 'month': 'MM',
    'quarter': 'Q', 'year': 'YYYY',
}


_sql_log_file = None
_sql_log_lock = threading.Lock()


def _log_sql(col, sql):
    """Append SQL to per-table log file (thread-safe)."""
    if _sql_log_file:
        with _sql_log_lock:
            with open(_sql_log_file, 'a') as f:
                f.write(f"-- [{col}]\n{sql.strip()}\n;\n\n")


def _oracle_portable_hash16(value_sql):
    """Oracle SQL expression: 16-bit unsigned BIGINT from MD5 of TRIM(value).

    Mirrors Athena's `_athena_portable_hash16` (TRIM -> UTF-8 bytes -> MD5
    -> first 4 hex chars -> int) so SUM(cnt * hash16) agrees cross-engine
    on identical categorical data.

    UTL_I18N.STRING_TO_RAW(..., 'AL32UTF8') forces UTF-8 byte encoding
    BEFORE hashing, so the MD5 input is independent of the database's
    NLS_CHARACTERSET. Without this, an Oracle install configured for
    WE8ISO8859P1 or US7ASCII would hash different bytes than
    Athena/Trino's TO_UTF8(...) and `hash_sum` would falsely flag drift
    against an Athena counterpart that holds identical data.

    Result is a RAW that to-string yields uppercase hex; TO_NUMBER('XXXX')
    consumes the first 4 hex chars directly into [0, 65535].
    """
    return (
        f"TO_NUMBER(SUBSTR(STANDARD_HASH("
        f"UTL_I18N.STRING_TO_RAW(TRIM({value_sql}), 'AL32UTF8'), 'MD5'), "
        f"1, 4), 'XXXX')"
    )


def _vintage_date_expr_oracle(date_col_q, vintage, date_dtype=None, date_format=None):
    """Build Oracle SQL expression that buckets date_col to the vintage label.

    Handles string-stored dates (YYYYMMDD VARCHAR2), integer-stored dates
    (YYYYMMDD NUMBER), and native DATE/TIMESTAMP. Returns an expression
    suitable for GROUP BY and for SELECT (after TO_CHAR if you need a string).
    """
    v = (vintage or 'all').lower()
    if v == 'all':
        return date_col_q
    trunc_fmt = _VINTAGE_ORACLE_TRUNC.get(v, 'DD')
    dtype = (date_dtype or '').upper()
    if dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT')) and date_format:
        ora_fmt = {
            'YYYYMMDD': 'YYYYMMDD',
            'YYYY-MM-DD': 'YYYY-MM-DD',
            'YYYYMM': 'YYYYMM',
        }.get(date_format, 'YYYY-MM-DD')
        return f"TRUNC(TO_DATE({date_col_q}, '{ora_fmt}'), '{trunc_fmt}')"
    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT') and date_format:
        ora_fmt = {
            'YYYYMMDD': 'YYYYMMDD',
            'YYYYMM': 'YYYYMM',
        }.get(date_format, 'YYYYMMDD')
        return f"TRUNC(TO_DATE(TO_CHAR({date_col_q}), '{ora_fmt}'), '{trunc_fmt}')"
    return f"TRUNC({date_col_q}, '{trunc_fmt}')"


def _query_oracle(sql, conn_macro, conn=None):
    """Execute SQL on Oracle and return rows as list[dict].

    Consults the shared query cache (set_cache_db) before submitting; cache
    key is SHA256(normalized SQL + conn_macro). On miss, opens an oracledb
    connection via db.oracle_connect (unless `conn` is provided), runs the
    query, lowercases column names, stores the rowset in cache, returns.

    When DTRACK_ORACLE_MOCK / DTRACK_MOCK is set, oracle_connect returns
    None and this function returns an empty list -- callers handle empty.
    """
    cached = cache_get(sql, conn_macro)
    if cached is not None:
        return cached

    from ..db import oracle_connect
    owned = False
    if conn is None:
        conn = oracle_connect(conn_macro)
        owned = True
    if conn is None:
        return []

    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            col_names = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
            result = [dict(zip(col_names, r)) for r in rows]
        finally:
            cur.close()
    finally:
        if owned:
            conn.close()

    cache_put(sql, conn_macro, result, row_count=len(result))
    return result


def _build_col_sql_oracle(tbl_cfg, col, dtype, full_table, cte_prefix="",
                          vintage=None, date_dtype=None, date_format=None,
                          dt_label=None):
    """Build per-column stats SQL for Oracle, no execution.

    Returns (sql, is_numeric). Used both by _extract_col_oracle (direct-cursor
    path) and _write_combined_col_sql_oracle (gen-first file path), so the
    two paths emit byte-identical SQL for the same inputs.
    """
    date_col = tbl_cfg['date_col']
    col_q = quote_ident(col, "oracle")
    date_col_q = quote_ident(date_col, "oracle")
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=True)
    wc = where.strip() if where else "(1=1)"

    if dt_label is not None:
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
        freq_dt_select = f"'{dt_label}'"
        freq_dt_group = ""
    elif vintage == 'all':
        dt_select = "'all' AS dt"
        group_by = ""
        freq_dt_select = "'all'"
        freq_dt_group = ""
    else:
        dt_expr = _vintage_date_expr_oracle(date_col_q, vintage, date_dtype, date_format)
        dt_select = f"TO_CHAR({dt_expr}, 'YYYY-MM-DD') AS dt"
        group_by = f"\nGROUP BY {dt_expr}"
        freq_dt_select = f"TO_CHAR({dt_expr}, 'YYYY-MM-DD')"
        freq_dt_group = f", {dt_expr}"

    if numeric:
        col_expr = apply_null_sentinels(col_q, dtype or "numeric", "oracle")
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{dtype}' AS col_type,
    '{col}' AS column_name,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    COUNT(DISTINCT {col_expr}) AS n_unique,
    AVG({col_expr}) AS mean,
    STDDEV_SAMP({col_expr}) AS std,
    TO_CHAR(MIN({col_expr})) AS min_val,
    TO_CHAR(MAX({col_expr})) AS max_val,
    '' AS top_10,
    '' AS length_avg,
    '' AS length_max,
    '' AS hash_sum
FROM {full_table} WHERE {wc}{group_by}"""
        return sql, True

    col_expr_cat = apply_null_sentinels(col_q, dtype or "categorical", "oracle")
    sql = f"""{cte_prefix}
WITH freq AS (
    SELECT {freq_dt_select} AS dt,
           SUBSTR(TO_CHAR({col_expr_cat}), 1, 200) AS p_col,
           COUNT(*) AS cnt
    FROM {full_table} WHERE {wc}
    GROUP BY SUBSTR(TO_CHAR({col_expr_cat}), 1, 200){freq_dt_group}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(TRIM(p_col)), p_col) AS rnk_alpha,
           COUNT(*)     OVER (PARTITION BY dt) AS k_alpha,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC, p_col ASC) AS rn_freq
    FROM freq WHERE p_col IS NOT NULL
),
totals AS (
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN p_col IS NULL THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN p_col IS NOT NULL THEN 1 ELSE 0 END) AS n_unique
    FROM freq GROUP BY dt
),
agg AS (
    SELECT dt,
           SUM(cnt) AS sum_cnt,
           SUM(cnt * rnk_alpha) AS sum_cnt_rnk,
           SUM(cnt * rnk_alpha * rnk_alpha) AS sum_cnt_rnk2
    FROM ranked GROUP BY dt
),
sstats AS (
    SELECT dt,
           sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0) AS mean_v,
           SQRT(GREATEST(
               sum_cnt_rnk2 * 1.0 / NULLIF(sum_cnt, 0)
               - (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0))
               * (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0)), 0)) AS std_v
    FROM agg
),
minmax AS (
    SELECT dt,
           MAX(CASE WHEN rnk_alpha = 1       THEN TRIM(p_col) || '=' || TO_CHAR(cnt) END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN TRIM(p_col) || '=' || TO_CHAR(cnt) END) AS max_val
    FROM ranked GROUP BY dt
),
top_n AS (
    SELECT dt,
           LISTAGG(p_col || '(' || TO_CHAR(cnt) || ')', '; ')
               WITHIN GROUP (ORDER BY cnt DESC, p_col ASC) AS top_10
    FROM ranked WHERE rn_freq <= 10
    GROUP BY dt
),
lenhash AS (
    SELECT dt,
           SUM(cnt * LENGTH(TRIM(p_col))) / NULLIF(SUM(cnt), 0) AS length_avg,
           MAX(LENGTH(TRIM(p_col))) AS length_max,
           SUM(cnt * {_oracle_portable_hash16("p_col")}) AS hash_sum
    FROM ranked GROUP BY dt
)
SELECT t.dt,
       '{col}' AS column_name,
       '{dtype}' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       ss.mean_v AS mean, ss.std_v AS std,
       mm.min_val, mm.max_val,
       NVL(tn.top_10, '') AS top_10,
       TO_CHAR(lh.length_avg) AS length_avg,
       TO_CHAR(lh.length_max) AS length_max,
       TO_CHAR(lh.hash_sum)   AS hash_sum
FROM totals t
LEFT JOIN sstats  ss ON t.dt = ss.dt
LEFT JOIN minmax  mm ON t.dt = mm.dt
LEFT JOIN top_n   tn ON t.dt = tn.dt
LEFT JOIN lenhash lh ON t.dt = lh.dt
ORDER BY t.dt"""
    return sql, False


def _build_lifeboat_col_sql_oracle(tbl_cfg, col, dtype, full_table,
                                   cte_prefix="", vintage=None,
                                   date_dtype=None, date_format=None,
                                   dt_label=None):
    """Resource-exhaustion fallback SQL: same row schema as the full per-column
    query, but only computes streaming aggregates that never spill TEMP.

    Drops COUNT(DISTINCT) (n_unique → NULL), the freq-table-derived
    rank-weighted mean/std (NULL), min/max (NULL), top_10 (''), and the
    length_avg/length_max/hash_sum signals that depend on the freq table.
    What survives: n_total (COUNT(*)) and n_missing (COUNT(*) - COUNT(col)),
    both single-pass. Matches the column list of the normal builder so the
    CSV header stays stable across degrade and non-degrade rows.

    Called by `_write_combined_col_sql_oracle` to embed a `/* @degrade_sql */`
    block alongside every full column SQL — the gen-first runner swaps to
    this when it catches ORA-01652 (temp tablespace) or ORA-04030 (PGA).
    """
    date_col = tbl_cfg['date_col']
    col_q = quote_ident(col, "oracle")
    date_col_q = quote_ident(date_col, "oracle")
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=True)
    wc = where.strip() if where else "(1=1)"

    if dt_label is not None:
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
    elif vintage == 'all':
        dt_select = "'all' AS dt"
        group_by = ""
    else:
        dt_expr = _vintage_date_expr_oracle(date_col_q, vintage, date_dtype, date_format)
        dt_select = f"TO_CHAR({dt_expr}, 'YYYY-MM-DD') AS dt"
        group_by = f"\nGROUP BY {dt_expr}"

    col_expr = apply_null_sentinels(
        col_q, dtype or ("numeric" if numeric else "categorical"), "oracle")
    col_type_label = 'numeric' if numeric else 'categorical'
    return f"""{cte_prefix}
SELECT
    {dt_select},
    '{col_type_label}' AS col_type,
    '{col}' AS column_name,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    CAST(NULL AS NUMBER) AS n_unique,
    CAST(NULL AS NUMBER) AS mean,
    CAST(NULL AS NUMBER) AS std,
    CAST(NULL AS VARCHAR2(1)) AS min_val,
    CAST(NULL AS VARCHAR2(1)) AS max_val,
    '' AS top_10,
    '' AS length_avg,
    '' AS length_max,
    '' AS hash_sum
FROM {full_table} WHERE {wc}{group_by}"""


def _extract_col_oracle(tbl_cfg, col, dtype, full_table, cte_prefix="",
                       conn_macro=None, vintage=None,
                       date_dtype=None, date_format=None, dt_label=None):
    """Run per-column stats SQL on Oracle, return list of result dicts.

    Mirrors _extract_col_athena's structure and output (14-field CSV row
    dicts including length_avg/length_max/hash_sum) so the comparison engine
    treats Oracle/Athena/Hadoop outputs uniformly. SQL is built by
    _build_col_sql_oracle so the gen-first file path produces identical SQL.
    """
    sql, numeric = _build_col_sql_oracle(
        tbl_cfg, col, dtype, full_table, cte_prefix,
        vintage=vintage, date_dtype=date_dtype,
        date_format=date_format, dt_label=dt_label,
    )

    if numeric:
        _log_sql(col, sql)
        rows = _query_oracle(sql, conn_macro)
        results = []
        for rd in rows:
            # length_avg/length_max/hash_sum left empty for numeric -- matches
            # the Hadoop/Athena convention (storage formatting of numbers
            # diverges across engines and would generate false drift hits).
            results.append({
                'dt': str(rd.get('dt', 'all')),
                'column_name': col,
                'col_type': 'numeric',
                'n_total': str(rd['n_total'] or 0),
                'n_missing': str(rd['n_missing'] or 0),
                'n_unique': str(rd['n_unique'] or 0),
                'mean': str(rd['mean']) if rd['mean'] is not None else '',
                'std': str(rd['std']) if rd['std'] is not None else '',
                'min_val': str(rd['min_val']) if rd['min_val'] is not None else '',
                'max_val': str(rd['max_val']) if rd['max_val'] is not None else '',
                'top_10': '',
                'length_avg': '',
                'length_max': '',
                'hash_sum': '',
            })
        return results

    # Categorical: run the SQL _build_col_sql_oracle already built.
    _log_sql(col, sql)
    rows = _query_oracle(sql, conn_macro)
    results = []
    for rd in rows:
        results.append({
            'dt': str(rd.get('dt', 'all')),
            'column_name': col,
            'col_type': 'categorical',
            'n_total': str(rd['n_total'] or 0),
            'n_missing': str(rd['n_missing'] or 0),
            'n_unique': str(rd['n_unique'] or 0),
            'mean': str(rd['mean']) if rd.get('mean') is not None else '',
            'std': str(rd['std']) if rd.get('std') is not None else '',
            'min_val': str(rd['min_val']) if rd.get('min_val') is not None else '',
            'max_val': str(rd['max_val']) if rd.get('max_val') is not None else '',
            'top_10': str(rd.get('top_10') or ''),
            'length_avg': str(rd['length_avg']) if rd.get('length_avg') is not None else '',
            'length_max': str(rd['length_max']) if rd.get('length_max') is not None else '',
            'hash_sum':   str(rd['hash_sum'])   if rd.get('hash_sum')   is not None else '',
        })
    return results


def _write_combined_col_sql_oracle(oracle_tables, outdir, db_path=None, vintage=None):
    """Emit extract_col.sql with one block per (table, column, bucket).

    Mirrors Athena's `_write_combined_sql` shape: when the table has a
    'between' date filter (matching_dates loaded from row-compare and
    bucketed by vintage), this emits one query per (qname, col, bucket)
    rather than one per (qname, col) with a server-side GROUP BY. The
    finer granularity makes parallelism visible in the runner log, makes
    cancel granular (a queued bucket short-circuits cheaply), and matches
    Athena so cross-platform compare runs have the same shape on both
    sides. For 5 cols × 12 vintages the file has 60 blocks, not 5.

    For 'in_list' (sample@N) and 'all' / no-filter modes there's still
    only one block per (qname, col) since the data isn't bucketed.
    """
    from ..date_utils import bucket_date

    os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack Oracle col extraction queries",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in oracle_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get('conn_macro', '') or ''
        table = tbl['table']
        date_col = tbl.get('date_col', '')
        full_table, cte_prefix = resolve_table(tbl, table)

        columns = tbl.get('columns', {})
        if not columns:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: no columns defined")
            blocks.append("")
            continue

        non_date_cols = [(c, d) for c, d in columns.items()
                         if c.lower() != date_col.lower()]
        if not non_date_cols:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: no non-date columns")
            blocks.append("")
            continue

        # Resolve date filter once per table.
        table_vintage = tbl.get('vintage', vintage)
        date_filter = compute_date_filter(tbl, db_path, table_vintage)
        resolve_date_format(date_filter, tbl)
        effective_vintage = date_filter.get('vintage', 'all')
        date_dtype = date_filter.get('date_dtype')
        date_format = date_filter.get('date_format')
        orig_where = tbl.get('where', '')
        date_bounds = tbl.get('_date_bounds', '')

        # User-typed per-pair excludeDates (from the col_gen_rough card).
        # Mirrors Athena's `_exclude_dates` -> NOT IN plumbing so the WHERE
        # picks up manual excludes regardless of vintage / sql_mode.
        exclude_dates = tbl.get('_exclude_dates', []) or []
        exclude_clause = ""
        if exclude_dates:
            in_clause = build_date_in_clause(
                date_col, exclude_dates, date_dtype,
                date_format=date_format,
            )
            exclude_clause = f"NOT ({in_clause})"
            print(f"  {qname}: excluding {len(exclude_dates)} dates")

        def _and_exclude(wc):
            if not exclude_clause:
                return wc
            if not wc or not wc.strip():
                return exclude_clause
            return f"{wc} AND {exclude_clause}"

        # work_units: list of (bucket_label, where_for_this_bucket, dt_label)
        # bucket_label drives the block name suffix; dt_label is the literal
        # the SQL emits as the dt column.
        work_units = []
        if date_filter['filter_type'] == 'between':
            buckets = {}
            for dt in date_filter['dates']:
                buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
            for bucket_key in sorted(buckets.keys()):
                bucket_dates = buckets[bucket_key]
                dw = build_date_range_with_gaps(
                    date_col, bucket_dates, date_dtype,
                    date_format=date_format)
                bucket_where = f"({orig_where}) AND {dw}" if orig_where else dw
                work_units.append((bucket_key, _and_exclude(bucket_where), bucket_key))
        elif date_filter['filter_type'] == 'in_list':
            dw = build_date_in_clause(
                date_col, date_filter['dates'], date_dtype,
                date_format=date_format)
            bucket_where = f"({orig_where}) AND {dw}" if orig_where else dw
            work_units.append(('sample', _and_exclude(bucket_where), 'sample'))
        elif date_bounds:
            bw = f"({orig_where}) AND {date_bounds}" if orig_where else date_bounds
            work_units.append(('all', _and_exclude(bw), 'all'))
        else:
            work_units.append(('all', _and_exclude(orig_where), 'all'))

        for col, dtype in non_date_cols:
            for bucket_label, bucket_where, dt_label in work_units:
                extract_cfg = dict(tbl)
                extract_cfg['where'] = bucket_where
                try:
                    # Per-bucket block: dt becomes the literal bucket label,
                    # no GROUP BY inside the SQL. WHERE narrows to this
                    # bucket's date set. Matches the Athena bucket-mode
                    # pattern exactly.
                    sql, _ = _build_col_sql_oracle(
                        extract_cfg, col, dtype, full_table, cte_prefix,
                        vintage='all',
                        date_dtype=None, date_format=None,
                        dt_label=dt_label,
                    )
                    lifeboat_sql = _build_lifeboat_col_sql_oracle(
                        extract_cfg, col, dtype, full_table, cte_prefix,
                        vintage='all',
                        date_dtype=None, date_format=None,
                        dt_label=dt_label,
                    )
                except Exception as e:
                    blocks.append(f"-- {qname}/{col}/{bucket_label}")
                    blocks.append(f"-- SKIP: SQL build failed: {e}")
                    blocks.append("")
                    continue
                blocks.append(f"-- {qname}/{col}/{bucket_label}")
                blocks.append(f"/* @conn_macro={conn_macro} */")
                blocks.append(sql.strip())
                blocks.append(";")
                blocks.append("/* @degrade_sql")
                blocks.append(lifeboat_sql.strip())
                blocks.append("@end_degrade */")
                blocks.append("")

    sql_path = os.path.join(outdir, oracle_sql_filename("col"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path


def _extract_cols_for_table_oracle(tbl_cfg, outdir, max_workers=None,
                                   db_path=None, vintage=None):
    """Extract column stats for one Oracle table -> {qname}_col.csv.

    Parallel via ThreadPoolExecutor with cap = min(ORACLE_MAX_WORKERS, n_cols).
    Each worker opens its own oracledb connection inside _query_oracle, so
    at any instant <= max_workers sessions are open server-side.
    """
    import time

    global _sql_log_file
    name = tbl_cfg['name']
    conn_macro = tbl_cfg.get('conn_macro')
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    raw_full_table = table  # Oracle table refs are already schema-qualified or bare
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full_table)

    print(f"\nExtracting columns: {name} ({raw_full_table})")

    columns = tbl_cfg.get('columns', {})
    if not columns:
        print(f"  WARNING: No columns specified for {name}. "
              f"Use 'dtrack load-columns --source oracle' or provide columns in config.")
        return None

    non_date_cols = [(c, d) for c, d in columns.items() if c.lower() != date_col.lower()]

    table_vintage = tbl_cfg.get('vintage', vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)
    resolve_date_format(date_filter, tbl_cfg)

    qname = qualified_name(tbl_cfg)
    log_path = os.path.join(outdir, f"{qname}_col.sql")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"-- SQL queries for {qname} col extraction\n"
                f"-- {datetime.now().isoformat()}\n\n")
    _sql_log_file = log_path

    set_cache_db(os.path.join(outdir, ".cache.db"))

    col_start_time = time.time()
    col_start_ts = datetime.now().isoformat()

    # ORACLE_MAX_WORKERS hard cap: server enforces per-user concurrent
    # session limit, so the executor sizes itself low and never opens an
    # ad-hoc connection outside the pool.
    env_workers = os.environ.get('ORACLE_MAX_WORKERS')
    _max_workers = max_workers if max_workers else (
        int(env_workers) if env_workers else 2)
    _max_workers = max(1, min(_max_workers, len(non_date_cols)))

    extract_cfg = dict(tbl_cfg)
    orig_where = tbl_cfg.get('where', '')
    date_bounds = tbl_cfg.get('_date_bounds', '')
    effective_vintage = date_filter.get('vintage', 'all')
    extract_date_dtype = date_filter.get('date_dtype')
    extract_date_format = date_filter.get('date_format')

    exclude_dates = tbl_cfg.get('_exclude_dates', []) or []
    exclude_clause = ""
    if exclude_dates:
        in_clause = build_date_in_clause(
            date_col, exclude_dates, extract_date_dtype,
            date_format=extract_date_format,
        )
        exclude_clause = f"NOT ({in_clause})"

    if date_filter['filter_type'] == 'between':
        from ..date_utils import bucket_date
        buckets = {}
        for dt in date_filter['dates']:
            buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
        dw = build_date_range_with_gaps(
            date_col, date_filter['dates'],
            extract_date_dtype, date_format=date_filter.get('date_format'))
        extract_cfg['where'] = f"({orig_where}) AND {dw}" if orig_where else dw
        print(f"  {len(buckets)} vintage buckets ({effective_vintage}) -- GROUP BY")
    elif date_filter['filter_type'] == 'in_list':
        dw = build_date_in_clause(
            date_col, date_filter['dates'], extract_date_dtype,
            date_format=date_filter.get('date_format'))
        extract_cfg['where'] = f"({orig_where}) AND {dw}" if orig_where else dw
        effective_vintage = 'all'
        extract_date_dtype = None
    elif date_bounds:
        extract_cfg['where'] = (
            f"({orig_where}) AND {date_bounds}" if orig_where else date_bounds)

    if exclude_clause:
        wc = extract_cfg.get('where', '') or ''
        extract_cfg['where'] = f"{wc} AND {exclude_clause}" if wc.strip() else exclude_clause

    bucket_desc = 'sample' if date_filter['filter_type'] == 'in_list' else effective_vintage
    dt_label = 'sample' if date_filter['filter_type'] == 'in_list' else None

    where_summary = extract_cfg.get('where', '(none)').strip() or '(none)'
    print(f"  {len(non_date_cols)} columns, WHERE: {where_summary}")
    print(f"  Oracle workers: {_max_workers}")

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    all_stats = []

    def _extract_one(col_dtype, _cfg=extract_cfg, _lbl=dt_label):
        col, dtype = col_dtype
        return _extract_col_oracle(
            _cfg, col, dtype, full_table, cte_prefix,
            conn_macro=conn_macro, vintage=effective_vintage,
            date_dtype=extract_date_dtype, date_format=extract_date_format,
            dt_label=_lbl)

    if _max_workers <= 1:
        col_iter = non_date_cols
        if tqdm is not None:
            col_iter = tqdm(col_iter, desc=f"  {name} [{bucket_desc}]", unit="col")
        for col, dtype in col_iter:
            try:
                stats = _extract_one((col, dtype))
                all_stats.extend(stats)
            except Exception as e:
                print(f"  Warning: Failed to extract {col}: {e}")
    else:
        with ThreadPoolExecutor(max_workers=_max_workers) as executor:
            futures = {executor.submit(_extract_one, (c, d)): c
                       for c, d in non_date_cols}
            completed_iter = as_completed(futures)
            if tqdm is not None:
                completed_iter = tqdm(completed_iter, total=len(futures),
                                     desc=f"  {name} [{bucket_desc}]", unit="col")
            for future in completed_iter:
                col_name = futures[future]
                try:
                    stats = future.result()
                    all_stats.extend(stats)
                except Exception as e:
                    print(f"  Warning: Failed to extract {col_name}: {e}")

    _sql_log_file = None
    hits, misses = cache_stats()
    if hits or misses:
        print(f"  Oracle cache: {hits} hit / {misses} miss "
              f"({os.path.join(outdir, '.cache.db')})")
    set_cache_db(None)
    print(f"  SQL log: {log_path}")
    col_elapsed = time.time() - col_start_time
    col_end_ts = datetime.now().isoformat()

    if not all_stats:
        return None

    csv_path = os.path.join(outdir, f"{qname}_col.csv")
    fieldnames = [
        'column_name', 'dt', 'col_type', 'n_total', 'n_missing',
        'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
        # Coarse signals — required for cross-engine compare with Hadoop/Athena.
        # Numeric rows leave these empty (format divergence across engines
        # would generate false drift hits on numbers).
        'length_avg', 'length_max', 'hash_sum',
        # Lifeboat retry marker (always '' on the direct path — only the
        # gen-first runner can trigger the degrade fallback).
        'partial',
    ]
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for stat in sorted(all_stats, key=lambda x: (x['column_name'], x['dt'])):
            writer.writerow({k: stat.get(k, '') for k in fieldnames})

    total_records = 0
    for s in all_stats:
        try:
            total_records += int(float(s.get('n_total', 0) or 0))
        except (ValueError, TypeError):
            pass
    records_info = f", ~{total_records:,} records" if total_records else ""
    print(f"  Column stats: {csv_path} ({len(all_stats)} rows, "
          f"{col_elapsed:.1f}s{records_info})")

    return {
        'table': name, 'step': 'col',
        'start': col_start_ts, 'end': col_end_ts,
        'elapsed_sec': col_elapsed,
    }


def _build_row_sql_oracle(tbl_cfg, db_path=None, vintage=None):
    """Build the Oracle row-count SQL for one table, no execution.

    Used by _write_combined_row_sql_oracle to emit extract_row.sql blocks
    and by anyone who wants to inspect what would run.
    """
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    full_table, cte_prefix = resolve_table(tbl_cfg, table)

    table_vintage = tbl_cfg.get('vintage', vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)
    resolve_date_format(date_filter, tbl_cfg)

    where = tbl_cfg.get('where', '')
    wc = where.strip() if where else "(1=1)"
    date_col_q = quote_ident(date_col, "oracle")
    date_dtype = date_filter.get('date_dtype')
    date_format = date_filter.get('date_format')

    dt_expr = _vintage_date_expr_oracle(date_col_q, 'day', date_dtype, date_format)
    # No ORDER BY: a global sort over a wide date range funnels every
    # group through one coordinator node (SINGLE[] in EXPLAIN). For
    # high-cardinality date columns this is the dominant cost and the
    # usual cause of "Query exhausted resources at this scale factor"
    # on Athena and ORA-01652 on Oracle. We sort client-side after
    # loading the CSV — the row count is small and ordering there is
    # free.
    return f"""{cte_prefix}
SELECT TO_CHAR({dt_expr}, 'YYYY-MM-DD') AS dt,
       COUNT(*) AS row_count
FROM {full_table} WHERE {wc}
GROUP BY {dt_expr}""".strip()


def oracle_sql_filename(extract_type):
    """Convention: `extract_{row,col}.sql`.

    Same filename Athena uses — Oracle and Athena live in *different* outdirs
    (left vs right output dir on the UI), so no collision. CLI callers
    targeting a single dir for Oracle alone also get the canonical name.
    """
    return f"extract_{extract_type}.sql"


def _write_combined_row_sql_oracle(oracle_tables, outdir, db_path=None, vintage=None):
    """Emit a single extract_row.oracle.sql with one block per table.

    Block format mirrors Athena's _write_combined_sql:
        -- {qname}
        /* @conn_macro=<macro> */
        <SQL>
        ;

    The conn_macro marker is a SQL comment Oracle ignores; the parser reads
    it back out so each block routes to the right Oracle service when
    dispatched in parallel.
    """
    os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack Oracle row extraction queries",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in oracle_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get('conn_macro', '') or ''
        try:
            sql = _build_row_sql_oracle(tbl, db_path=db_path, vintage=vintage)
        except Exception as e:
            blocks.append(f"-- {qname}")
            blocks.append(f"-- SKIP: SQL build failed: {e}")
            blocks.append("")
            continue
        blocks.append(f"-- {qname}")
        blocks.append(f"/* @conn_macro={conn_macro} */")
        blocks.append(sql)
        blocks.append(";")
        blocks.append("")

    sql_path = os.path.join(outdir, oracle_sql_filename("row"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path


def parse_oracle_sql_file(sql_path):
    """Parse a combined Oracle SQL file into named query blocks.

    Block markers are `-- {qname}` lines (single-token after `-- `); each
    block's conn_macro is read from a `/* @conn_macro=NAME */` comment
    inside the SQL body. Returns list of dicts:
        [{"name": qname, "sql": "...", "conn_macro": "pb23"}, ...]
    Skips header lines starting with `-- dtrack` / `-- Generated`.
    """
    import re

    with open(sql_path, 'r', encoding='utf-8') as f:
        text = f.read()

    blocks = []
    current_name = None
    current_lines = []

    def _flush():
        if current_name is None:
            return
        raw = '\n'.join(current_lines).strip()
        # Strip the embedded `/* @degrade_sql ... @end_degrade */` lifeboat
        # comment off the main SQL before passing to the executor. The
        # lifeboat is captured separately so the runner can re-execute it
        # on resource exhaustion (ORA-01652 / ORA-04030).
        dm = re.search(
            r"/\*\s*@degrade_sql\s*(.*?)\s*@end_degrade\s*\*/",
            raw, flags=re.DOTALL)
        degrade_sql = None
        if dm:
            degrade_sql = dm.group(1).strip().rstrip(';').strip()
            raw = (raw[:dm.start()] + raw[dm.end():]).strip()
        sql = raw.strip().rstrip(';').strip()
        if not sql:
            return
        m = re.search(r"/\*\s*@conn_macro=(\S+?)\s*\*/", sql)
        conn_macro = m.group(1) if m else None
        blocks.append({"name": current_name, "sql": sql,
                       "conn_macro": conn_macro,
                       "degrade_sql": degrade_sql})

    for line in text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('-- dtrack') or stripped.startswith('-- Generated'):
            continue
        if stripped.startswith('-- ') and not stripped.startswith('-- ['):
            marker = stripped[3:].strip()
            if marker and ' ' not in marker:
                _flush()
                current_name = marker
                current_lines = []
                continue
        current_lines.append(line)
    _flush()

    return blocks


def run_oracle_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                       on_progress=None, resume=False, cancel_event=None):
    """Parse extract_row.sql (or extract_col.sql) and dispatch in parallel.

    Mirrors Athena's run_sql_file: ThreadPoolExecutor capped at
    ORACLE_MAX_WORKERS (default 2). Each block carries its own conn_macro
    in `/* @conn_macro=... */`, so a single SQL file can mix multiple
    Oracle services. Results land in `{outdir}/{qname}_row.csv` (row) or
    `{outdir}/{qname}_col.csv` (col). The shared .cache.db lets repeated
    runs of the same file skip already-executed blocks.

    Args:
        on_progress: optional callback(result_dict) per block (used by web UI).
        resume: not implemented for Oracle yet (kept for API parity).
        cancel_event: optional threading.Event. When set, queued workers
            fast-exit before issuing their SQL. Workers already inside
            cursor.execute() finish their query (oracledb has no clean
            mid-execution interrupt), but no new SQL is sent to Oracle
            after the event fires.
    """
    import sys
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    blocks = parse_oracle_sql_file(sql_path)
    if not blocks:
        print(f"No query blocks found in {sql_path}")
        return []

    set_cache_db(os.path.join(outdir, ".cache.db"))

    basename = os.path.basename(sql_path)
    # Filename convention: extract_{row,col}.oracle.sql (or legacy
    # extract_{row,col}.sql). "extract_col" in basename catches both.
    is_col = "extract_col" in basename
    file_suffix = "_col.csv" if is_col else "_row.csv"

    os.makedirs(outdir, exist_ok=True)
    env_workers = os.environ.get('ORACLE_MAX_WORKERS')
    _max_workers = max_workers if max_workers else (
        int(env_workers) if env_workers else 2)
    _max_workers = max(1, min(_max_workers, len(blocks)))

    # For col mode, block names are "qname/col_name". Group blocks by qname
    # so we can write one CSV per table after all that table's columns
    # finish (mirrors Athena's _col_rows_by_table pattern).
    _COL_HEADERS = [
        'dt', 'column_name', 'col_type', 'n_total', 'n_missing',
        'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
        'length_avg', 'length_max', 'hash_sum',
        # Lifeboat marker: 'oracle_degraded' on rows produced by the
        # @degrade_sql retry after ORA-01652/ORA-04030; empty otherwise.
        'partial',
    ]
    table_blocks = {}
    for block in blocks:
        qname = block['name'].split('/')[0] if '/' in block['name'] else block['name']
        table_blocks.setdefault(qname, []).append(block)
    _col_rows_lock = threading.Lock()
    _col_rows_by_table = {}

    print(f"  Parsed {len(blocks)} blocks ({len(table_blocks)} tables) "
          f"from {sql_path}, workers={_max_workers}", file=sys.stderr)

    results = []
    _done_count = [0]
    _done_lock = threading.Lock()
    _table_done = {q: {'done': 0, 'total': len(bs)}
                   for q, bs in table_blocks.items()}
    _table_lock = threading.Lock()
    # In-flight counter -- incremented just before _query_oracle, decremented
    # in finally. Reported on both start and finish events so the UI can
    # render "[start | running=4]" / "[N/total | running=3]" lines and the
    # user can see parallelism level at a glance.
    _running_count = [0]
    _running_lock = threading.Lock()
    # Wall-clock anchor for ETA. Set on the FIRST start event so the
    # estimate reflects actual extraction time, not connect / setup overhead.
    _run_start = [None]

    def _run_block(block):
        block_name = block['name']
        qname = block_name.split('/')[0] if '/' in block_name else block_name
        sql = block['sql']
        conn_macro = block.get('conn_macro')
        start_time = time.time()
        start_ts = datetime.now().isoformat()

        # Queue-time short-circuit: when the user cancelled, workers that
        # haven't started their query yet exit immediately instead of
        # opening a new Oracle session for work the user no longer wants.
        if cancel_event is not None and cancel_event.is_set():
            result = {"name": block_name, "ok": False, "error": "cancelled",
                      "elapsed": 0.0, "start": start_ts, "cancelled": True}
            with _done_lock:
                _done_count[0] += 1
                n = _done_count[0]
            total = len(blocks)
            if on_progress:
                result["done"] = n
                result["total"] = total
                result["table"] = qname
                on_progress(result)
            return result

        # Start event: announce this block is now occupying a worker slot.
        # Reported running count INCLUDES this block.
        with _running_lock:
            _running_count[0] += 1
            running_now = _running_count[0]
            if _run_start[0] is None:
                _run_start[0] = time.time()
        if on_progress:
            done_so_far = _done_count[0]
            total_blocks = len(blocks)
            eta_sec = None
            if done_so_far > 0 and _run_start[0]:
                elapsed_wall = time.time() - _run_start[0]
                eta_sec = elapsed_wall * (total_blocks - done_so_far) / done_so_far
            on_progress({"_event": "start", "name": block_name,
                         "table": qname, "running": running_now,
                         "total": total_blocks, "eta_sec": eta_sec})

        degraded = False
        try:
            try:
                rows = _query_oracle(sql, conn_macro)
            except Exception as exc:
                # ORA-01652 (temp tablespace exhausted) / ORA-04030 (PGA
                # limit) on a high-cardinality COUNT(DISTINCT) get one
                # retry with the embedded lifeboat SQL, which drops the
                # spilling aggregates. Any other error bubbles up
                # unchanged so legitimate failures still surface.
                lifeboat = block.get('degrade_sql')
                err_str = str(exc)
                is_resource_err = ('ORA-01652' in err_str
                                   or 'ORA-04030' in err_str)
                if is_col and lifeboat and is_resource_err:
                    print(f"  [degrade] {block_name}: {err_str.splitlines()[0]}"
                          f" — retrying without n_unique/hash_sum", file=sys.stderr)
                    rows = _query_oracle(lifeboat, conn_macro)
                    degraded = True
                else:
                    raise
            elapsed = time.time() - start_time

            if is_col:
                if degraded:
                    for r in rows:
                        r['partial'] = 'oracle_degraded'
                with _col_rows_lock:
                    _col_rows_by_table.setdefault(qname, []).extend(
                        [r for r in rows])
            else:
                csv_path = os.path.join(outdir, f"{qname}{file_suffix}")
                # Server-side ORDER BY was dropped from the row SQL (it
                # forces a single-coordinator funnel that blows up at
                # scale). Sort here on the way out so the CSV still reads
                # in date order for human inspection.
                sorted_rows = sorted(rows, key=lambda r: str(r.get('dt', '')))
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['dt', 'row_count'])
                    for r in sorted_rows:
                        writer.writerow([str(r.get('dt', '')), r.get('row_count', 0)])

            result = {"name": block_name, "ok": True, "rows": len(rows),
                      "elapsed": round(elapsed, 1), "start": start_ts,
                      "degraded": degraded}
            if not is_col:
                result["csv_path"] = os.path.join(outdir, f"{qname}{file_suffix}")
        except Exception as e:
            elapsed = time.time() - start_time
            result = {"name": block_name, "ok": False, "error": str(e),
                      "elapsed": round(elapsed, 1), "start": start_ts}
        finally:
            with _running_lock:
                _running_count[0] -= 1
                running_after = _running_count[0]

        # Per-table CSV flush: when this table's last column finishes, write
        # the collated {qname}_col.csv so downstream load-col can pick up
        # finished tables while others are still running.
        table_just_finished = False
        table_csv_path = None
        with _table_lock:
            _table_done[qname]['done'] += 1
            tbl_progress = dict(_table_done[qname])
            if is_col and _table_done[qname]['done'] == _table_done[qname]['total']:
                table_just_finished = True

        if table_just_finished:
            with _col_rows_lock:
                rows_for_qname = _col_rows_by_table.pop(qname, [])
            table_csv_path = os.path.join(outdir, f"{qname}_col.csv")
            with open(table_csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(_COL_HEADERS)
                for r in rows_for_qname:
                    writer.writerow([r.get(h, '') for h in _COL_HEADERS])
            print(f"  Wrote {len(rows_for_qname)} rows to {table_csv_path}",
                  file=sys.stderr)

        with _done_lock:
            _done_count[0] += 1
            n = _done_count[0]
        total = len(blocks)
        eta_sec = None
        if _run_start[0] and n > 0:
            elapsed_wall = time.time() - _run_start[0]
            eta_sec = elapsed_wall * (total - n) / n
        status = "ok" if result["ok"] else f"FAIL: {result.get('error', '')}"
        print(f"  [{n}/{total} | running={running_after}] {block_name}: "
              f"{status} ({elapsed:.1f}s)", file=sys.stderr)

        if on_progress:
            result["done"] = n
            result["total"] = total
            result["table"] = qname
            result["running"] = running_after
            result["eta_sec"] = eta_sec
            result["table_done"] = tbl_progress["done"]
            result["table_total"] = tbl_progress["total"]
            result["table_csv_written"] = table_just_finished
            if table_csv_path:
                result["table_csv_path"] = table_csv_path
            on_progress(result)
        return result

    if _max_workers <= 1:
        for block in blocks:
            results.append(_run_block(block))
    else:
        with ThreadPoolExecutor(max_workers=_max_workers) as pool:
            futures = {pool.submit(_run_block, b): b for b in blocks}
            for fut in as_completed(futures):
                results.append(fut.result())

    # Partial-failure flush: any table that didn't reach 'all columns done'
    # (because some block failed) gets whatever rows did come back, so the
    # user isn't left with an empty CSV.
    if is_col and _col_rows_by_table:
        for qname, row_data in _col_rows_by_table.items():
            csv_path = os.path.join(outdir, f"{qname}_col.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(_COL_HEADERS)
                for r in row_data:
                    writer.writerow([r.get(h, '') for h in _COL_HEADERS])
            print(f"  Wrote {len(row_data)} rows to {csv_path} "
                  f"(partial -- table had failures)", file=sys.stderr)

    set_cache_db(None)
    return results


def extract_oracle(config_path, outdir, types=None, max_workers=None, db_path=None,
                   vintage=None, force=False, from_date=None, to_date=None):
    """Extract data directly from Oracle via oracledb.

    Mirrors extract_aws layout: per-table loop, row extraction first (cheap,
    single query), then column extraction via ThreadPoolExecutor capped at
    ORACLE_MAX_WORKERS sessions. Results land in `{outdir}/{qname}_row.csv`
    and `{outdir}/{qname}_col.csv`. Cache in `{outdir}/.cache.db` lets
    interrupted runs resume cheaply.
    """
    if types is None:
        types = ["row", "col"]

    mock_dir = get_mock_dir()

    from ..config import load_unified_config
    config = load_unified_config(config_path)
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
    oracle_tables = [t for t in all_tables
                     if t.get('source', '').lower() == 'oracle']

    if not oracle_tables:
        print("No Oracle tables found in config")
        return

    for tbl in oracle_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    inject_where_from_config(oracle_tables, config)

    if from_date or to_date:
        for tbl in oracle_tables:
            date_col = tbl.get('date_col', '')
            if not date_col:
                continue
            date_col_q = quote_ident(date_col, "oracle")
            bounds = []
            if from_date:
                bounds.append(f"{date_col_q} >= DATE '{from_date}'")
            if to_date:
                bounds.append(f"{date_col_q} <= DATE '{to_date}'")
            if bounds:
                tbl['_date_bounds'] = " AND ".join(bounds)
            tbl['_from_date'] = from_date
            tbl['_to_date'] = to_date

    if mock_dir:
        _extract_mock_tables(oracle_tables, outdir, types, mock_dir)
        return

    try:
        import oracledb  # noqa: F401
    except ImportError:
        print("Error: oracledb is required for Oracle direct extraction")
        print("Install with: pip install oracledb")
        return

    if db_path and "col" in types:
        fill_columns_from_meta(oracle_tables, db_path)

    timing_records = []

    # --- Row extraction: write extract_row.sql then run it in parallel ---
    # Mirrors Athena's row pattern (gen-first, then run_sql_file). One block
    # per table, each carrying /* @conn_macro=... */ so the dispatcher
    # routes to the right Oracle service. Multiple Oracle services in one
    # file is supported via per-block conn_macro.
    if "row" in types:
        try:
            sql_path = _write_combined_row_sql_oracle(
                oracle_tables, outdir, db_path=db_path, vintage=vintage)
            print(f"\n--- Oracle row extraction from {sql_path} ---")
            row_results = run_oracle_sql_file(
                sql_path, outdir, max_workers=max_workers, db_path=db_path)
            for r in row_results:
                timing_records.append({
                    'table': r['name'], 'step': 'row',
                    'start': r.get('start', ''), 'end': '',
                    'elapsed_sec': r.get('elapsed', 0),
                    'status': '' if r.get('ok') else r.get('error', ''),
                })
        except Exception as e:
            print(f"  ERROR row extraction: {e}")

    for tbl_cfg in oracle_tables:
        if "col" in types:
            try:
                result = _extract_cols_for_table_oracle(
                    tbl_cfg, outdir, max_workers=max_workers,
                    db_path=db_path, vintage=vintage)
                if result:
                    timing_records.append(result)
            except Exception as e:
                print(f"  ERROR col extraction for {tbl_cfg['name']}: {e}")
                print("  Skipping to next table...")
                timing_records.append({
                    'table': tbl_cfg['name'], 'step': 'col',
                    'start': '', 'end': '', 'elapsed_sec': 0, 'status': str(e),
                })

    if timing_records:
        timing_path = os.path.join(outdir, '_timing.csv')
        with open(timing_path, 'w', newline='') as f:
            fieldnames = ['table', 'step', 'start', 'end', 'elapsed_sec']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(timing_records)
        print(f"\nTiming log: {timing_path}")

    print(f"\nExtraction complete. Output in: {outdir}")


# ---------------------------------------------------------------------------
# SQL builders — re-exported as Oracle-named wrappers for backward compat.
# ---------------------------------------------------------------------------

def build_continuous_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "oracle")


def build_categorical_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "oracle")


def build_top10_sql_oracle(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "oracle")


# ---------------------------------------------------------------------------
# OracleBuilder — symmetric to AthenaBuilder.
# ---------------------------------------------------------------------------

class OracleBuilder(PlatformBuilder):
    """Oracle platform: Python-direct extraction via oracledb."""

    def build_row_sql(self, date_filter):
        table, cte = resolve_table(self.tbl_cfg)
        date_col_q = quote_ident(self.date_col, "oracle")
        where = self.tbl_cfg.get('where', '')
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""{cte}SELECT {date_col_q} AS date_value, COUNT(*) AS row_count
FROM {table}
{where_clause}
GROUP BY {date_col_q}"""
        return sql.strip()

    def build_continuous_sql(self, col, col_type, where):
        table, cte = resolve_table(self.tbl_cfg)
        return build_stats_sql(table, col, self.date_col, where,
                              "numeric", "oracle", cte_prefix=cte)

    def build_categorical_sql(self, col, col_type, where, top_n=10):
        table, cte = resolve_table(self.tbl_cfg)
        return build_stats_sql(table, col, self.date_col, where,
                              "categorical", "oracle", cte_prefix=cte)

    def generate_extraction(self, outdir, extract_type, **kw):
        """Run Python-direct Oracle extraction via extract_oracle()."""
        config_path = kw.get('config_path')
        if not config_path:
            raise ValueError("config_path is required for Oracle extraction")
        extract_oracle(
            config_path=config_path,
            outdir=outdir,
            types=kw.get('types'),
            max_workers=kw.get('max_workers'),
            db_path=self.db_path,
            vintage=kw.get('vintage'),
            force=kw.get('force', False),
            from_date=kw.get('from_date'),
            to_date=kw.get('to_date'),
        )
        return [outdir]
