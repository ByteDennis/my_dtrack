
import os
import threading
from typing import Optional

_HADOOP_CONNECT_LOCK = threading.Lock()

# >>> Connect to Hadoop / Hive via pyodbc <<< #
def hadoop_connect(dsn: Optional[str] = None, *,
                   autocommit: bool = True):
    if os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_HADOOP_MOCK'):
        print(f"[mock] Skipping Hadoop connection for DSN '{dsn or '?'}'")
        return None

    import pyodbc

    dsn = dsn or os.environ.get('HADOOP_DSN') or 'Hive_Prod'

    with _HADOOP_CONNECT_LOCK:
        saved = os.environ.pop('SSL_CERT_FILE', None)
        if os.environ.get('DTRACK_HADOOP_DEBUG'):
            import sys as _sys
            import threading as _th
            print(f"[hadoop] connect DSN={dsn!r} thread={_th.current_thread().name} "
                  f"SSL_CERT_FILE={'popped' if saved is not None else 'unset'} "
                  f"KRB5CCNAME={os.environ.get('KRB5CCNAME', '<unset>')} "
                  f"KRB5_CONFIG={os.environ.get('KRB5_CONFIG', '<unset>')} "
                  f"KRB5_KTNAME={os.environ.get('KRB5_KTNAME', '<unset>')} "
                  f"HTTPS_PROXY={os.environ.get('HTTPS_PROXY', '<unset>')}",
                  file=_sys.stderr, flush=True)
        try:
            try:
                conn = pyodbc.connect(f"DSN={dsn}", autocommit=autocommit)
            except pyodbc.InterfaceError as exc:
                msg = str(exc)
                if 'IM002' in msg or 'Data source name not found' in msg:
                    import struct
                    import sys
                    try:
                        seen = sorted(pyodbc.dataSources().keys())
                    except Exception:
                        seen = []
                    bits = struct.calcsize('P') * 8
                    odbcini = os.environ.get('ODBCINI', '(unset)')
                    odbcsysini = os.environ.get('ODBCSYSINI', '(unset)')
                    hint = (
                        f"\nDSN '{dsn}' not found by this Python's pyodbc.\n"
                        f"  sys.executable: {sys.executable}\n"
                        f"  bitness: {bits}-bit\n"
                        f"  pyodbc.dataSources(): {seen or '<empty>'}\n"
                        f"  ODBCINI={odbcini}  ODBCSYSINI={odbcsysini}\n"
                        f"Diagnose:\n"
                        f"  - Compare sys.executable above with the Python\n"
                        f"    that DOES see the DSN ('which python' vs venv).\n"
                        f"  - Windows: Get-OdbcDsn -Name {dsn} "
                        f"| ft Name,Platform,DsnType\n"
                        f"    A 32-bit DSN won't be visible to a 64-bit Python.\n"
                        f"  - User DSNs are per-user; service / scheduled task\n"
                        f"    accounts won't see another user's DSNs.\n"
                        f"  - Linux/unixODBC: odbcinst -j ; odbcinst -q -s\n"
                        f"  - Override DSN via HADOOP_DSN env var."
                    )
                    raise pyodbc.InterfaceError(msg + hint) from exc
                raise
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close()
            return conn
        finally:
            if saved is not None:
                os.environ['SSL_CERT_FILE'] = saved

from .base import (
    qualified_name,
    resolve_table,
    apply_null_sentinels,
    build_date_bounds_clause,
    apply_pair_date_bounds,
    build_date_in_clause,
    build_date_range_with_gaps,
    resolve_date_format,
    compute_date_filter,
    load_tables_from_config,
    fill_columns_from_meta,
    inject_where_from_config,
    build_stats_sql,
    build_top10_sql,
    get_mock_dir,
    _extract_mock_tables,
    set_cache_db,
    cache_get,
    cache_put,
)
from datetime import datetime

_VINTAGE_TRUNC = {
    'day': None,
    'week': 'IW',
    'month': 'MM',
    'quarter': 'Q',
    'year': 'YYYY',
}

# >>> Exact distinct count for n_unique; APPROX_* is forbidden in every dialect <<< #
def _n_unique_expr(source: str, col: str, tbl_cfg=None, config=None) -> str:
    return f"COUNT(DISTINCT {col})"

# >>> Build the sentinel-translated column expression for an Oracle/Hadoop <<< #
def _col_expr_for_source(source: str, col_name: str, col_type: str,
                         col_dtype: str = None) -> str:
    dialect = (source or "oracle").lower()
    if dialect not in ("oracle", "hadoop"):
        dialect = "oracle"
    effective_type = col_dtype or col_type
    return apply_null_sentinels(col_name, effective_type, dialect)

# >>> Return Oracle SQL expression for date transformation (used inside passthrough) <<< #
def _oracle_date_transform(date_col, transform):
    if transform and "{col}" in transform:
        return transform.replace("{col}", date_col)
    if transform == "datetime_to_date":
        return f"TRUNC({date_col})"
    elif transform == "to_char":
        return f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
    return date_col

# >>> Wrap date_expr with vintage bucketing <<< #
def _vintage_date_expr(date_expr, vintage, vintage_transform=None):
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

# >>> Format a YYYY-MM-DD date string as the correct SQL literal for WHERE clauses <<< #
def _format_date_bound(date_str, date_type, is_sas_src=False, is_upper=False,
                       custom_date_types=None):
    dtype = date_type.lower() if date_type else ""
    time_part = "23:59:59" if is_upper else "00:00:00"

    if custom_date_types and dtype in custom_date_types:
        from .base import reformat_date
        custom = custom_date_types[dtype]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            return reformat_date(date_str, fmt)
        elif cat == 'date':
            return f"DATE '{date_str}'"
        else:
            return f"'{reformat_date(date_str, fmt)}'"

    if dtype == 'num_yyyymm':
        return date_str[:4] + date_str[5:7]
    if dtype in ('num', 'integer', 'int', 'number'):
        return date_str.replace('-', '')

    if dtype == 'string_compact':
        return f"'{date_str.replace('-', '')}'"
    if dtype in ('string_dash', 'string'):
        return f"'{date_str}'"

    if dtype == 'timestamp':
        return f"TIMESTAMP '{date_str} {time_part}'"
    if dtype == 'date':
        return f"DATE '{date_str}'"

    return f"DATE '{date_str}'"

# >>> DB-side expression that truncates date_col to the vintage unit and <<< #
def _date_trunc_expr(source, date_col, date_type, vintage):
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

# >>> Filter (col_name, col_dtype) list by the pair's col_filter selection <<< #
def _apply_col_filter(col_list, tbl_cfg):
    selected = tbl_cfg.get('_selected_cols')
    if selected is not None:
        if not selected:
            print(f"  WARNING: {tbl_cfg.get('name')}: col_filter matched 0 "
                  f"mapped cols; this table will emit no col blocks")
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
        col_list = out

    skip = tbl_cfg.get('_id_cols_to_skip')
    if skip:
        col_list = [(c, d) for c, d in col_list if c not in skip]

    fine_cells = tbl_cfg.get('_fine_cells')
    if fine_cells:
        wanted = {c.lower() for c in fine_cells.keys()}
        col_list = [(c, d) for c, d in col_list if c.lower() in wanted]
    return col_list

# >>> Discover columns and write {qname}_columns.csv files <<< #
def _discover_and_write_columns(all_tables, outdir, db_path):
    import csv as csv_mod
    from ..db import insert_column_meta, get_column_meta

    os.makedirs(outdir, exist_ok=True)
    aws_cursors = {}
    oracle_conns = {}

    for tbl in all_tables:
        source = (tbl.get('source') or '').lower()
        qname = qualified_name(tbl)
        raw_table = tbl.get('table', '') or ''
        conn_macro = tbl.get('conn_macro', '')
        columns = None

        existing = get_column_meta(db_path, qname)
        if existing:
            columns = {c["column_name"]: c.get("data_type", "") for c in existing}
            print(f"  {qname}: found {len(columns)} columns in _column_meta")
        elif tbl.get('processed'):
            print(f"  {qname}: 'processed' CTE — discovery skipped; "
                  f"load {qname}_columns.csv via `dtrack load-columns` first")
            continue
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
        elif source == 'oracle':
            try:
                if conn_macro not in oracle_conns:
                    from ..db import oracle_connect
                    print(f"  {qname}: connecting to Oracle ({conn_macro})...")
                    oracle_conns[conn_macro] = oracle_connect(conn_macro)
                conn = oracle_conns[conn_macro]
                if conn is None:
                    print(f"  {qname}: Oracle connection skipped (mock mode)")
                else:
                    from ..db import discover_columns
                    columns = discover_columns(conn, raw_table)
                    if columns:
                        insert_column_meta(db_path, qname, columns, source=source)
                        print(f"  {qname}: discovered {len(columns)} columns from Oracle")
                    else:
                        print(f"  {qname}: no columns found in Oracle")
            except Exception as e:
                print(f"  {qname}: Oracle column discovery failed ({e})")
        elif source == 'hadoop':
            try:
                columns = _discover_columns_hadoop(conn_macro, raw_table)
                if columns:
                    insert_column_meta(db_path, qname, columns, source=source)
                    print(f"  {qname}: discovered {len(columns)} columns from Hadoop")
                else:
                    print(f"  {qname}: no columns found in Hadoop")
            except Exception as e:
                print(f"  {qname}: Hadoop column discovery failed ({e})")
        else:
            print(f"  {qname}: unsupported source {source!r}; column discovery skipped")
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
    for conn in oracle_conns.values():
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

def build_continuous_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "oracle")

def build_categorical_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "oracle")

def build_top10_sql_oracle(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "oracle")

def hadoop_sql_filename(extract_type):
    return f"extract_{extract_type}.sql"

# >>> Hive expression that buckets `date_col` to the configured vintage <<< #
def _hadoop_vintage_expr(tbl_cfg, date_col, vintage):
    src = (tbl_cfg.get('source') or 'hadoop').lower()
    date_type = tbl_cfg.get('date_type', 'string_dash')
    expr = _date_trunc_expr(src if src in ('hadoop', 'oracle') else 'hadoop',
                             date_col, date_type, vintage)
    return expr or date_col

def _build_col_sql_hadoop(tbl_cfg, col, dtype, full_table, cte_prefix="",
                           vintage=None, dt_label=None, coarse=False,
                           preset=None, stats_override=None):
    from .base import build_col_sql
    return build_col_sql('hadoop', tbl_cfg, col, dtype, full_table,
                          cte_prefix=cte_prefix, vintage=vintage,
                          dt_label=dt_label, coarse=coarse, preset=preset,
                          stats_override=stats_override)

# >>> Thin wrapper over base.build_lifeboat_col_sql('hadoop', ...) <<< #
def _build_lifeboat_col_sql_hadoop(tbl_cfg, col, dtype, full_table,
                                    cte_prefix="", vintage=None, dt_label=None):
    from .base import build_lifeboat_col_sql
    return build_lifeboat_col_sql('hadoop', tbl_cfg, col, dtype, full_table,
                                    cte_prefix=cte_prefix, vintage=vintage,
                                    dt_label=dt_label)

# >>> Plain Hive row-count SQL for one table <<< #
def _build_row_sql_hadoop(tbl_cfg, db_path=None, vintage=None):
    database = tbl_cfg.get('conn_macro', '') or ''
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    transform = tbl_cfg.get('date_transform', '')
    raw_full = f"{database}.{table}" if database else table
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full)

    parts = []
    base_where = tbl_cfg.get('where', '').strip()
    date_bounds = tbl_cfg.get('_date_bounds', '').strip()
    if base_where:
        parts.append(base_where)
    if date_bounds:
        parts.append(date_bounds)
    wc = " AND ".join(f"({p})" for p in parts) if parts else "1=1"

    dt_expr = _oracle_date_transform(date_col, transform) if transform else date_col
    return (f"{cte_prefix}\n"
            f"SELECT {dt_expr} AS dt, COUNT(*) AS row_count\n"
            f"FROM {full_table} WHERE {wc}\n"
            f"GROUP BY {dt_expr}").strip()

# >>> extract_row.sql, one block per Hadoop table. Block format mirrors <<< #
def _write_combined_row_sql_hadoop(hadoop_tables, outdir, db_path=None, vintage=None):
    os.makedirs(outdir, exist_ok=True)
    blocks = [
        "-- dtrack Hadoop row extraction queries",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in hadoop_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get('conn_macro', '') or ''
        try:
            sql = _build_row_sql_hadoop(tbl, db_path=db_path, vintage=vintage)
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
    sql_path = os.path.join(outdir, hadoop_sql_filename("row"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path

# >>> extract_col.sql for Hadoop, one block per (table, column, bucket) <<< #
def _write_combined_col_sql_hadoop(hadoop_tables, outdir, db_path=None,
                                     vintage=None, coarse=False, preset=None,
                                     stats_override=None):
    from ..date_utils import bucket_date

    os.makedirs(outdir, exist_ok=True)
    label = "coarse col" if coarse else "col"
    blocks = [
        f"-- dtrack Hadoop {label} extraction queries",
        f"-- Generated: {datetime.now().isoformat()}",
        "",
    ]
    for tbl in hadoop_tables:
        qname = qualified_name(tbl)
        conn_macro = tbl.get('conn_macro', '') or ''
        table = tbl['table']
        date_col = tbl.get('date_col', '')
        raw_full = f"{conn_macro}.{table}" if conn_macro else table
        full_table, cte_prefix = resolve_table(tbl, raw_full)

        columns = tbl.get('columns', {})
        if not columns:
            blocks.append(f"-- {qname}")
            blocks.append("-- SKIP: no columns defined")
            blocks.append("")
            continue
        non_date_cols = [(c, d) for c, d in columns.items()
                         if c.lower() != date_col.lower()]
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

        if date_filter['filter_type'] == 'between':
            dates = date_filter['dates']
            full_range = build_date_range_with_gaps(
                date_col, dates, date_dtype, date_format=date_format)
            bucket_where = f"({orig_where}) AND {full_range}" if orig_where else full_range
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = effective_vintage
            per_col_dt_label = None
            bucket_count = len(set(bucket_date(dt, effective_vintage) for dt in dates))
            print(f"  {qname}: emitting 1 block per col, server-side GROUP BY "
                  f"({bucket_count} buckets/col, vintage={effective_vintage})")
        elif date_filter['filter_type'] == 'in_list':
            dw = build_date_in_clause(
                date_col, date_filter['dates'], date_dtype,
                date_format=date_format)
            bucket_where = f"({orig_where}) AND {dw}" if orig_where else dw
            per_col_where = _and_exclude(bucket_where)
            per_col_vintage = 'all'
            per_col_dt_label = 'sample'
        elif date_bounds:
            bw = f"({orig_where}) AND {date_bounds}" if orig_where else date_bounds
            per_col_where = _and_exclude(bw)
            per_col_vintage = 'all'
            per_col_dt_label = 'all'
        else:
            per_col_where = _and_exclude(orig_where)
            per_col_vintage = 'all'
            per_col_dt_label = 'all'

        for col, dtype in non_date_cols:
            extract_cfg = dict(tbl)
            extract_cfg['where'] = per_col_where
            try:
                sql, _ = _build_col_sql_hadoop(
                    extract_cfg, col, dtype, full_table, cte_prefix,
                    vintage=per_col_vintage, dt_label=per_col_dt_label,
                    coarse=coarse, preset=preset,
                    stats_override=stats_override)
            except Exception as e:
                blocks.append(f"-- {qname}/{col}")
                blocks.append(f"-- SKIP: SQL build failed: {e}")
                blocks.append("")
                continue
            blocks.append(f"-- {qname}/{col}")
            blocks.append(f"/* @conn_macro={conn_macro} */")
            blocks.append(sql.strip())
            blocks.append(";")
            blocks.append("")

    sql_path = os.path.join(outdir, hadoop_sql_filename("col"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path

# >>> Thin wrapper over base.parse_extract_sql_file. Block format is <<< #
def parse_hadoop_sql_file(sql_path):
    from .base import parse_extract_sql_file
    return parse_extract_sql_file(sql_path)

# >>> Execute SQL against Hive via hadoop_connect; return list[dict] <<< #
def _query_hadoop(sql, conn_macro):
    cached = cache_get(sql, conn_macro)
    if cached is not None:
        return cached

    conn = hadoop_connect()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            if cur.description is None:
                result = []
            else:
                col_names = [d[0].lower().split('.')[-1] for d in cur.description]
                rows = cur.fetchall() or []
                result = [dict(zip(col_names, r)) for r in rows]
        finally:
            cur.close()
    finally:
        conn.close()

    cache_put(sql, conn_macro, result, row_count=len(result))
    return result

# >>> Discover (column_name -> data_type) for a Hive table via hadoop_connect <<< #
def _discover_columns_hadoop(conn_macro, table):
    full = f"{conn_macro}.{table}" if conn_macro else table
    conn = hadoop_connect()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"DESCRIBE {full}")
            cols = {}
            for row in cur.fetchall() or []:
                name = (str(row[0]).strip() if row[0] is not None else "")
                if not name or name.startswith('#'):
                    break
                dtype = (str(row[1]).strip() if len(row) > 1 and row[1] is not None else "")
                cols[name] = dtype
            return cols
        finally:
            cur.close()
    finally:
        conn.close()

# >>> Thin wrapper over base.run_extract_sql_file with platform='hadoop' <<< #
def run_hadoop_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                        on_progress=None, resume=False, cancel_event=None):
    from .base import run_extract_sql_file
    set_cache_db(os.path.join(outdir, ".cache.db"))
    try:
        return run_extract_sql_file(
            sql_path, outdir,
            query_fn=_query_hadoop,
            platform='hadoop',
            max_workers=max_workers,
            on_progress=on_progress,
            cancel_event=cancel_event,
        )
    finally:
        set_cache_db(None)

# >>> Direct Hive extraction (gen-then-run): write extract_{row,col}.sql then <<< #
def extract_hadoop(config_path, outdir, types=None, max_workers=None, db_path=None,
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
                m = get_pair_col_map_from_db(db_path, pname)
                if m:
                    pcfg["col_map"] = m
            if not pcfg.get("col_filter"):
                cf = get_pair_col_filter(db_path, pname)
                if cf.get("include") or cf.get("exclude"):
                    pcfg["col_filter"] = cf

    os.makedirs(outdir, exist_ok=True)
    all_tables = load_tables_from_config(config)
    hadoop_tables = [t for t in all_tables
                     if t.get('source', '').lower() == 'hadoop']
    if not hadoop_tables:
        print("No Hadoop tables found in config")
        return

    for tbl in hadoop_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    inject_where_from_config(hadoop_tables, config)

    apply_pair_date_bounds(
        hadoop_tables, config, from_date, to_date,
        lambda tbl, dc, f, t: build_date_bounds_clause(dc, f, t, tbl.get('date_type')))

    if gen_only:
        from .base import tag_generated_sql
        if "row" in types:
            _write_combined_row_sql_hadoop(hadoop_tables, outdir,
                                           db_path=db_path, vintage=vintage)
            tag_generated_sql(outdir, "row", "hadoop")
        if "col" in types:
            if db_path:
                fill_columns_from_meta(hadoop_tables, db_path)
            _write_combined_col_sql_hadoop(hadoop_tables, outdir, db_path=db_path,
                                           vintage=vintage, preset=preset,
                                           stats_override=stats_override)
            tag_generated_sql(outdir, "col", "hadoop")
        print(f"[gen-only] wrote Hadoop extract SQL to {outdir} (no extraction)")
        return

    mock_dir = get_mock_dir()
    if mock_dir:
        _extract_mock_tables(hadoop_tables, outdir, types, mock_dir)
        return

    if "col" in types:
        _discover_and_write_columns(hadoop_tables, outdir, db_path)
        if db_path:
            fill_columns_from_meta(hadoop_tables, db_path)

    if "row" in types:
        sql_path = _write_combined_row_sql_hadoop(
            hadoop_tables, outdir, db_path=db_path, vintage=vintage)
        print(f"\n--- Hadoop row extraction from {sql_path} ---")
        run_hadoop_sql_file(sql_path, outdir, max_workers=max_workers,
                            db_path=db_path)
    if "col" in types:
        sql_path = _write_combined_col_sql_hadoop(
            hadoop_tables, outdir, db_path=db_path, vintage=vintage,
            preset=preset, stats_override=stats_override)
        print(f"\n--- Hadoop col extraction from {sql_path} ---")
        run_hadoop_sql_file(sql_path, outdir, max_workers=max_workers,
                            db_path=db_path)

    print(f"\nExtraction complete. Output in: {outdir}")
