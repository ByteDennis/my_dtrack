
import csv
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from .base import (
    qualified_name,
    resolve_table,
    build_date_in_clause,
    format_date_bound_literal,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    build_stats_sql,
    build_top10_sql,
    quote_ident,
    apply_pair_date_bounds,
    set_cache_db,
    cache_db_path,
    cache_stats,
    cache_get,
    cache_put,
    _mask_stats_in_rows,
)

_VINTAGE_ORACLE_TRUNC = {
    'day': 'DD', 'week': 'IW', 'month': 'MM',
    'quarter': 'Q', 'year': 'YYYY',
}

_sql_log_file = None
_sql_log_lock = threading.Lock()

# >>> Append SQL to per-table log file (thread-safe) <<< #
def _log_sql(col, sql):
    if _sql_log_file:
        with _sql_log_lock:
            with open(_sql_log_file, 'a') as f:
                f.write(f"-- [{col}]\n{sql.strip()}\n;\n\n")

# >>> Oracle SQL expression: 16-bit unsigned BIGINT from MD5 of TRIM(value) <<< #
def _oracle_portable_hash16(value_sql):
    return (
        f"TO_NUMBER(SUBSTR(STANDARD_HASH("
        f"UTL_I18N.STRING_TO_RAW(TRIM({value_sql}), 'AL32UTF8'), 'MD5'), "
        f"1, 4), 'XXXX')"
    )

# >>> Build Oracle SQL expression that buckets date_col to the vintage label <<< #
def _vintage_date_expr_oracle(date_col_q, vintage, date_dtype=None, date_format=None):
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

# >>> Execute SQL on Oracle and return rows as list[dict] <<< #
def _query_oracle(sql, conn_macro, conn=None):
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

# >>> Build per-column stats SQL for Oracle. Thin wrapper over the <<< #
def _build_col_sql_oracle(tbl_cfg, col, dtype, full_table, cte_prefix="",
                          vintage=None, date_dtype=None, date_format=None,
                          dt_label=None, coarse=False, preset=None,
                          stats_override=None):
    from .base import build_col_sql
    return build_col_sql('oracle', tbl_cfg, col, dtype, full_table,
                          cte_prefix=cte_prefix, vintage=vintage,
                          date_dtype=date_dtype, date_format=date_format,
                          dt_label=dt_label, coarse=coarse, preset=preset,
                          stats_override=stats_override)

# >>> Resource-exhaustion fallback SQL — Oracle wrapper over <<< #
def _build_lifeboat_col_sql_oracle(tbl_cfg, col, dtype, full_table,
                                   cte_prefix="", vintage=None,
                                   date_dtype=None, date_format=None,
                                   dt_label=None):
    from .base import build_lifeboat_col_sql
    return build_lifeboat_col_sql('oracle', tbl_cfg, col, dtype, full_table,
                                    cte_prefix=cte_prefix, vintage=vintage,
                                    date_dtype=date_dtype, date_format=date_format,
                                    dt_label=dt_label)

# >>> Run per-column stats SQL on Oracle, return list of result dicts <<< #
def _extract_col_oracle(tbl_cfg, col, dtype, full_table, cte_prefix="",
                       conn_macro=None, vintage=None,
                       date_dtype=None, date_format=None, dt_label=None,
                       preset=None, stats_override=None):
    sql, numeric = _build_col_sql_oracle(
        tbl_cfg, col, dtype, full_table, cte_prefix,
        vintage=vintage, date_dtype=date_dtype,
        date_format=date_format, dt_label=dt_label,
        preset=preset, stats_override=stats_override,
    )

    if numeric:
        _log_sql(col, sql)
        rows = _query_oracle(sql, conn_macro)
        results = []
        for rd in rows:
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

# >>> Emit extract_col.sql with one block per (table, column, bucket) <<< #
def _write_combined_col_sql_oracle(oracle_tables, outdir, db_path=None,
                                     vintage=None, coarse=False, preset=None,
                                     stats_override=None):
    from ..date_utils import bucket_date

    os.makedirs(outdir, exist_ok=True)
    label = "coarse col" if coarse else "col"
    blocks = [
        f"-- dtrack Oracle {label} extraction queries",
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
            blocks.append("-- SKIP: no columns defined")
            blocks.append("")
            continue

        non_date_cols = [(c, d) for c, d in columns.items()
                         if c.lower() != date_col.lower()]
        from .hadoop import _apply_col_filter
        non_date_cols = _apply_col_filter(non_date_cols, tbl)
        if not non_date_cols:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no non-date columns "
                          "(col_filter may have left zero cols)")
            blocks.append("")
            continue

        table_vintage = tbl.get('vintage', vintage)
        date_filter = compute_date_filter(tbl, db_path, table_vintage)
        resolve_date_format(date_filter, tbl)
        effective_vintage = date_filter.get('vintage', 'all')
        date_dtype = date_filter.get('date_dtype')
        date_format = date_filter.get('date_format')
        orig_where = tbl.get('where', '')
        base_where = tbl.get('_base_where', orig_where)
        date_bounds = tbl.get('_date_bounds', '')

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
                bucket_where = f"{dw} AND ({base_where})" if base_where else dw
                work_units.append((bucket_key, _and_exclude(bucket_where), bucket_key))
        elif date_filter['filter_type'] == 'in_list':
            dw = build_date_in_clause(
                date_col, date_filter['dates'], date_dtype,
                date_format=date_format)
            bucket_where = f"{dw} AND ({base_where})" if base_where else dw
            work_units.append(('sample', _and_exclude(bucket_where), 'sample'))
        elif date_bounds:
            bw = f"{date_bounds} AND ({orig_where})" if orig_where else date_bounds
            work_units.append(('all', _and_exclude(bw), 'all'))
        else:
            work_units.append(('all', _and_exclude(orig_where), 'all'))

        for col, dtype in non_date_cols:
            for bucket_label, bucket_where, dt_label in work_units:
                extract_cfg = dict(tbl)
                extract_cfg['where'] = bucket_where
                try:
                    sql, _ = _build_col_sql_oracle(
                        extract_cfg, col, dtype, full_table, cte_prefix,
                        vintage='all',
                        date_dtype=None, date_format=None,
                        dt_label=dt_label,
                        coarse=coarse, preset=preset,
                        stats_override=stats_override,
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
                blocks.append("")

    sql_path = os.path.join(outdir, oracle_sql_filename("col"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path

# >>> Extract column stats for one Oracle table -> {qname}_col.csv <<< #
def _extract_cols_for_table_oracle(tbl_cfg, outdir, max_workers=None,
                                   db_path=None, vintage=None,
                                   preset=None, stats_override=None):
    import time

    global _sql_log_file
    name = tbl_cfg['name']
    conn_macro = tbl_cfg.get('conn_macro')
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    raw_full_table = table
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

    set_cache_db(cache_db_path(outdir))

    col_start_time = time.time()
    col_start_ts = datetime.now().isoformat()

    env_workers = os.environ.get('ORACLE_MAX_WORKERS')
    _max_workers = max_workers if max_workers else (
        int(env_workers) if env_workers else 2)
    _max_workers = max(1, min(_max_workers, len(non_date_cols)))

    extract_cfg = dict(tbl_cfg)
    orig_where = tbl_cfg.get('where', '')
    base_where = tbl_cfg.get('_base_where', orig_where)
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
        extract_cfg['where'] = f"{dw} AND ({base_where})" if base_where else dw
        print(f"  {len(buckets)} vintage buckets ({effective_vintage}) -- GROUP BY")
    elif date_filter['filter_type'] == 'in_list':
        dw = build_date_in_clause(
            date_col, date_filter['dates'], extract_date_dtype,
            date_format=date_filter.get('date_format'))
        extract_cfg['where'] = f"{dw} AND ({base_where})" if base_where else dw
        effective_vintage = 'all'
        extract_date_dtype = None
    elif date_bounds:
        extract_cfg['where'] = (
            f"{date_bounds} AND ({orig_where})" if orig_where else date_bounds)

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
            dt_label=_lbl, preset=preset, stats_override=stats_override)

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
              f"({cache_db_path(outdir)})")
    set_cache_db(None)
    print(f"  SQL log: {log_path}")
    col_elapsed = time.time() - col_start_time
    col_end_ts = datetime.now().isoformat()

    if not all_stats:
        return None

    _mask_stats_in_rows(all_stats, preset, stats_override)

    csv_path = os.path.join(outdir, f"{qname}_col.csv")
    fieldnames = [
        'column_name', 'dt', 'col_type', 'n_total', 'n_missing',
        'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
        'length_avg', 'length_max', 'hash_sum',
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

# >>> Build the Oracle row-count SQL for one table, no execution <<< #
def _build_row_sql_oracle(tbl_cfg, db_path=None, vintage=None):
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    transform = tbl_cfg.get('date_transform', '')
    full_table, cte_prefix = resolve_table(tbl_cfg, table)
    date_col_q = quote_ident(date_col, "oracle")

    parts = []
    base_where = tbl_cfg.get('where', '').strip()
    date_bounds = tbl_cfg.get('_date_bounds', '').strip()
    if date_bounds:
        parts.append(date_bounds)
    if base_where:
        parts.append(base_where)
    wc = " AND ".join(f"({p})" for p in parts) if parts else "(1=1)"

    if transform and "{col}" in transform:
        dt_expr = transform.replace("{col}", date_col_q)
    elif transform == "datetime_to_date":
        dt_expr = f"TRUNC({date_col_q})"
    elif transform == "to_char":
        dt_expr = f"TO_CHAR({date_col_q}, 'YYYY-MM-DD')"
    else:
        dt_expr = date_col_q

    return f"""{cte_prefix}
SELECT {dt_expr} AS dt,
       COUNT(*) AS row_count
FROM {full_table} WHERE {wc}
GROUP BY {dt_expr}""".strip()

# >>> Convention: `extract_{row,col}.sql` <<< #
def oracle_sql_filename(extract_type):
    return f"extract_{extract_type}.sql"

# >>> Emit a single extract_row.oracle.sql with one block per table <<< #
def _write_combined_row_sql_oracle(oracle_tables, outdir, db_path=None, vintage=None):
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

# >>> Thin wrapper over base.parse_extract_sql_file kept for back-compat <<< #
def parse_oracle_sql_file(sql_path):
    from .base import parse_extract_sql_file
    return parse_extract_sql_file(sql_path)

# >>> Thin wrapper over base.run_extract_sql_file with platform='oracle' <<< #
def run_oracle_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                       on_progress=None, resume=False, cancel_event=None):
    from .base import run_extract_sql_file
    set_cache_db(cache_db_path(outdir))
    try:
        return run_extract_sql_file(
            sql_path, outdir,
            query_fn=_query_oracle,
            platform='oracle',
            max_workers=max_workers,
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
    finally:
        set_cache_db(None)

# >>> Extract data directly from Oracle via oracledb <<< #
def extract_oracle(config_path, outdir, types=None, max_workers=None, db_path=None,
                   vintage=None, force=False, from_date=None, to_date=None,
                   preset=None, stats_override=None, gen_only=False):
    if types is None:
        types = ["row", "col"]

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

    def _oracle_bounds(tbl, date_col, eff_from, eff_to):
        date_col_q = quote_ident(date_col, "oracle")
        date_type = tbl.get('date_type')
        custom = config.get('date_types')
        bounds = []
        if eff_from:
            bounds.append(f"{date_col_q} >= "
                          f"{format_date_bound_literal(eff_from, date_type, is_upper=False, custom_date_types=custom)}")
        if eff_to:
            bounds.append(f"{date_col_q} <= "
                          f"{format_date_bound_literal(eff_to, date_type, is_upper=True, custom_date_types=custom)}")
        return " AND ".join(bounds)
    apply_pair_date_bounds(oracle_tables, config, from_date, to_date, _oracle_bounds)

    if gen_only:
        from .base import tag_generated_sql
        if "row" in types:
            _write_combined_row_sql_oracle(oracle_tables, outdir,
                                           db_path=db_path, vintage=vintage)
            tag_generated_sql(outdir, "row", "oracle")
        if "col" in types:
            if db_path:
                fill_columns_from_meta(oracle_tables, db_path)
            _write_combined_col_sql_oracle(oracle_tables, outdir,
                                           db_path=db_path, vintage=vintage,
                                           preset=preset, stats_override=stats_override)
            tag_generated_sql(outdir, "col", "oracle")
        print(f"[gen-only] wrote Oracle extract SQL to {outdir} (no extraction)")
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
                    db_path=db_path, vintage=vintage,
                    preset=preset, stats_override=stats_override)
                if result:
                    timing_records.append(result)
            except Exception as e:
                print(f"  ERROR col extraction for {tbl_cfg['name']}: {e}")
                print("  Skipping to next table...")
                timing_records.append({
                    'table': tbl_cfg['name'], 'step': 'col',
                    'start': '', 'end': '', 'elapsed_sec': 0, 'status': str(e),
                })

    from .base import write_timing_csv
    write_timing_csv(outdir, timing_records, oracle_tables)

    print(f"\nExtraction complete. Output in: {outdir}")

def build_continuous_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "oracle")

def build_categorical_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "oracle")

def build_top10_sql_oracle(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "oracle")

