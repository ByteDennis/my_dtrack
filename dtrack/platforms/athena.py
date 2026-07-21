
import csv
import json
import os
import threading

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .base import (
    qualified_name,
    is_numeric_type,
    resolve_table,
    build_date_between_clause,
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
    build_col_sql,
    quote_ident,
    set_cache_db,
    cache_db_path,
    cache_stats,
    cache_get,
    cache_put,
    _mask_stats_in_rows,
)

set_athena_cache = set_cache_db
_cache_get = cache_get
_cache_put = cache_put

_VINTAGE_ATHENA = {
    'day': 'day', 'week': 'week', 'month': 'month',
    'quarter': 'quarter', 'year': 'year',
}

_ATHENA_DATE_PARSE = {
    "YYYYMMDD": "%Y%m%d", "YYYYMM": "%Y%m",
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%i:%s",
    "YYYY-MM-DD HH:MM:SS.ffffff": "%Y-%m-%d %H:%i:%s.%f",
    "YYYY-MM-DDTHH:MM:SS": "%Y-%m-%dT%H:%i:%s",
    "YYYY-MM-DDTHH:MM:SS.ffffff": "%Y-%m-%dT%H:%i:%s.%f",
    "DDMONYYYY": "%d%b%Y",
    "DDMONYYYY:HH:MM:SS": "%d%b%Y:%H:%i:%s",
    "DDMONYYYY:HH:MM:SS.ffffff": "%d%b%Y:%H:%i:%s.%f",
    "DDMONYY": "%d%b%y",
    "DD-MON-YYYY": "%d-%b-%Y",
    "DD-MON-YYYY HH24:MI:SS": "%d-%b-%Y %H:%i:%s",
    "DD-MON-YY": "%d-%b-%y",
    "YYYY/MM/DD": "%Y/%m/%d",
    "YYYY/MM/DD HH:MM:SS": "%Y/%m/%d %H:%i:%s",
    "MM/DD/YYYY": "%m/%d/%Y",
}

_aws_creds_expires = None
_aws_creds_lock = threading.Lock()

_sql_log_file = None
_sql_log_lock = threading.Lock()

# >>> Renew AWS credentials via proxied token-based auth (Windows only) <<< #
def aws_creds_renew(ttl_minutes=50, retries=3, retry_delay=10):
    global _aws_creds_expires

    if os.name != 'nt':
        return

    import time
    now = time.time()
    if _aws_creds_expires and now < _aws_creds_expires:
        return

    usr = os.getenv('AWS_USR')
    pwd = os.getenv('AWS_PWD')
    host = os.getenv('AWS_HOST')
    if not (usr and pwd and host):
        print("Warning: AWS_USR / AWS_PWD / AWS_HOST not all set; skipping cred renewal")
        return
    os.environ['HTTPS_PROXY'] = f'http://{usr}:{pwd}@{host}:8080'

    with _aws_creds_lock:
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
                token_resp = requests.post(
                    token_url,
                    headers={'Accept': '*/*'},
                    json={'username': usr, 'password': pwd},
                    verify=False,
                ).json()
                token = token_resp['token']

                creds = requests.get(
                    arn_url,
                    headers={'Authorization': f'Bearer {token}', 'Accept': '*/*'},
                    verify=False,
                ).json()['Credentials']

                os.environ['AWS_ACCESS_KEY_ID']     = creds['AccessKeyId']
                os.environ['AWS_SECRET_ACCESS_KEY'] = creds['SecretAccessKey']
                os.environ['AWS_SESSION_TOKEN']     = creds['SessionToken']

                try:
                    import s3fs
                    s3fs.S3FileSystem().connect(refresh=True)
                except Exception as e:
                    print(f"Warning: s3fs refresh failed: {e}")

                _aws_creds_expires = now + ttl_minutes * 60
                return
            except Exception as e:
                if attempt < retries:
                    print(f"Warning: AWS credential renewal attempt {attempt}/{retries} failed: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"Warning: AWS credential renewal failed after {retries} attempts: {e}")

# >>> Connect to AWS Athena <<< #
def athena_connect(data_base=None, *, region=None, work_group=None,
                   staging_dir=None):
    from pyathena import connect as athena_connect_raw

    kwargs = {
        'region_name':    region      or os.environ.get('AWS_DEFAULT_REGION'),
        'work_group':     work_group  or os.environ.get('AWS_S3_WORK_GROUP'),
        's3_staging_dir': staging_dir or os.environ.get('AWS_S3_STAGING_DIR'),
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return athena_connect_raw(schema_name=data_base, **kwargs)

# >>> Wrap date_expr with appropriate cast for Athena based on column type <<< #
def _athena_date_cast(date_expr, date_dtype, date_format=None):
    dtype = (date_dtype or '').lower()
    if 'date' in dtype or 'timestamp' in dtype:
        return date_expr

    athena_fmt = _ATHENA_DATE_PARSE.get(date_format, '%Y%m%d') if date_format else '%Y%m%d'

    if dtype in ('int', 'integer', 'bigint', 'smallint', 'tinyint', 'number'):
        return f"date_parse(CAST({date_expr} AS VARCHAR), '{athena_fmt}')"
    if 'char' in dtype or 'string' in dtype:
        return f"date_parse({date_expr}, '{athena_fmt}')"
    return date_expr

# >>> Wrap date_expr with Athena date_trunc for vintage bucketing <<< #
def _vintage_date_expr_athena(date_expr, vintage, vintage_transform=None, date_dtype=None, date_format=None):
    if vintage_transform:
        return vintage_transform.replace("{col}", date_expr)
    if vintage == 'all':
        return "'all'"
    if not vintage or vintage == 'day':
        return date_expr
    unit = _VINTAGE_ATHENA.get(vintage)
    if unit is None:
        return date_expr
    cast_expr = _athena_date_cast(date_expr, date_dtype, date_format)
    return f"date_trunc('{unit}', {cast_expr})"

# >>> Format a YYYY-MM-DD date as the correct Athena SQL literal <<< #
def _format_athena_date_bound(date_str, date_type, is_upper=False, custom_date_types=None):
    from .base import format_date_bound_literal
    return format_date_bound_literal(date_str, date_type, is_upper=is_upper,
                                     custom_date_types=custom_date_types)

# >>> Discover columns from information_schema <<< #
def _discover_columns_athena(cursor, database, table):
    sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{database}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}

# >>> Execute SQL on Athena via pandas read_sql_query with fresh connection <<< #
def _query_athena(sql, data_base=None):
    import pandas.io.sql as psql
    import warnings

    cached = _cache_get(sql, data_base)
    if cached is not None:
        return cached

    aws_creds_renew()
    conn = athena_connect(data_base=data_base)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            df = psql.read_sql_query(sql, conn)
        df.columns = [c.lower() for c in df.columns]
    finally:
        conn.close()

    _cache_put(sql, data_base, df, row_count=len(df))
    return df

# >>> Streaming sibling of _query_athena: DB-API fetchmany batches (no pandas, no cache); page capped at 1000 (Athena GetQueryResults MaxResults limit) <<< #
def _iter_athena(sql, data_base=None, arraysize=100000):
    aws_creds_renew()
    page = min(int(arraysize) if arraysize else 1000, 1000) or 1000
    conn = athena_connect(data_base=data_base)
    try:
        cur = conn.cursor()
        try:
            cur.arraysize = page
            cur.execute(sql)
            cols = [d[0].lower() for d in cur.description]
            while True:
                rows = cur.fetchmany(page)
                if not rows:
                    break
                yield [dict(zip(cols, r)) for r in rows]
        finally:
            cur.close()
    finally:
        conn.close()

# >>> Append SQL to log file (thread-safe). No console output <<< #
def _log_sql(col, sql):
    if _sql_log_file:
        with _sql_log_lock:
            with open(_sql_log_file, 'a') as f:
                f.write(f"-- [{col}]\n{sql.strip()}\n;\n\n")

# >>> Extract stats for a single column from Athena <<< #
def _extract_col_athena(tbl_cfg, col, dtype, full_table, cte_prefix="", database=None, vintage=None, date_dtype=None, date_format=None, dt_label=None):
    date_col = tbl_cfg['date_col']
    col_q = quote_ident(col, "athena")
    date_col_q = quote_ident(date_col, "athena")
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=False)

    wc = where.strip() if where else "(1=1)"

    if dt_label is not None:
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
    elif vintage == 'all':
        dt_select = "'all' AS dt"
        group_by = ""
    else:
        vintage_transform = tbl_cfg.get('vintage_transform', None)
        dt_expr = _vintage_date_expr_athena(date_col_q, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
        dt_select = f"CAST({dt_expr} AS VARCHAR) AS dt"
        group_by = f"\nGROUP BY {dt_expr}"

    if numeric:
        from .base import apply_null_sentinels
        col_expr = apply_null_sentinels(col_q, dtype or "numeric", "athena")
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{dtype}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    COUNT(DISTINCT {col_expr}) AS n_unique,
    AVG(CAST({col_expr} AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST({col_expr} AS DOUBLE)) AS std,
    MIN({col_expr}) AS min_val,
    MAX({col_expr}) AS max_val
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
                'length_avg': '',
                'length_max': '',
                'hash_sum': '',
            })
        return results

    if dt_label is not None:
        freq_dt_select = f"'{dt_label}'"
        freq_dt_group = ""
    elif vintage == 'all':
        freq_dt_select = "'all'"
        freq_dt_group = ""
    else:
        vintage_transform = tbl_cfg.get('vintage_transform', None)
        dt_expr = _vintage_date_expr_athena(date_col_q, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
        freq_dt_select = f"CAST({dt_expr} AS VARCHAR)"
        freq_dt_group = f", {dt_expr}"

    from .base import apply_null_sentinels
    col_expr_cat = apply_null_sentinels(col_q, dtype or "categorical", "athena")
    from .base import P_COL_MAX_LEN
    p_col_expr = f"SUBSTR(CAST({col_expr_cat} AS VARCHAR), 1, {P_COL_MAX_LEN})"
    sql = f"""{cte_prefix}
WITH freq AS (
    SELECT {freq_dt_select} AS dt,
           {p_col_expr} AS p_col,
           COUNT(*) AS cnt
    FROM {full_table} WHERE {wc}
    GROUP BY {p_col_expr}{freq_dt_group}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(TRIM(p_col)), p_col) AS rnk_alpha,
           COUNT(*)     OVER (PARTITION BY dt) AS k_alpha,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC, p_col ASC) AS rn_freq
    FROM freq WHERE p_col IS NOT NULL AND TRIM(p_col) <> ''
),
totals AS (
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN p_col IS NULL OR TRIM(p_col) = '' THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN p_col IS NOT NULL AND TRIM(p_col) <> '' THEN 1 ELSE 0 END) AS n_unique
    FROM freq GROUP BY dt
),
agg AS (
    SELECT dt,
           SUM(CAST(cnt AS DOUBLE)) AS sum_cnt,
           SUM(CAST(cnt AS DOUBLE) * rnk_alpha) AS sum_cnt_rnk,
           SUM(CAST(cnt AS DOUBLE) * rnk_alpha * rnk_alpha) AS sum_cnt_rnk2
    FROM ranked GROUP BY dt
),
sstats AS (
    SELECT dt,
           sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0) AS mean,
           SQRT(GREATEST(
               sum_cnt_rnk2 * 1.0 / NULLIF(sum_cnt, 0)
               - (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0))
               * (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0)), 0)) AS std
    FROM agg
),
minmax AS (
    SELECT dt,
           MAX(CASE WHEN rnk_alpha = 1       THEN TRIM(p_col) || '=' || CAST(cnt AS VARCHAR) END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN TRIM(p_col) || '=' || CAST(cnt AS VARCHAR) END) AS max_val
    FROM ranked GROUP BY dt
),
top_n AS (
    SELECT dt,
           ARRAY_JOIN(ARRAY_AGG(
               p_col || '(' || CAST(cnt AS VARCHAR) || ')'
               ORDER BY cnt DESC
           ), '; ') AS top_10
    FROM ranked WHERE rn_freq <= 10
    GROUP BY dt
),
lenhash AS (
    -- Cross-engine coarse signals. LENGTH(TRIM(p_col)) counts Unicode
    -- code points (matches Oracle LENGTH on AL32UTF8). hash16 hashes the
    -- UTF-8 bytes of the trimmed value -- identical recipe to the Oracle
    -- side's UTL_I18N.STRING_TO_RAW(... 'AL32UTF8') + STANDARD_HASH MD5.
    SELECT dt,
           SUM(CAST(cnt AS DOUBLE) * LENGTH(TRIM(p_col))) / NULLIF(SUM(cnt), 0) AS length_avg,
           MAX(LENGTH(TRIM(p_col))) AS length_max,
           SUM(CAST(cnt AS DOUBLE) * CAST({_athena_portable_hash16("p_col")} AS DOUBLE)) AS hash_sum
    FROM ranked GROUP BY dt
)
SELECT t.dt,
       '{dtype}' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       ss.mean, ss.std,
       mm.min_val, mm.max_val,
       COALESCE(tn.top_10, '') AS top_10,
       CAST(lh.length_avg AS VARCHAR) AS length_avg,
       CAST(lh.length_max AS VARCHAR) AS length_max,
       CAST(lh.hash_sum   AS VARCHAR) AS hash_sum
FROM totals t
LEFT JOIN sstats  ss ON t.dt = ss.dt
LEFT JOIN minmax  mm ON t.dt = mm.dt
LEFT JOIN top_n   tn ON t.dt = tn.dt
LEFT JOIN lenhash lh ON t.dt = lh.dt
ORDER BY t.dt"""

    _log_sql(col, sql)
    df = _query_athena(sql, data_base=database)

    results = []
    for _, rd in df.iterrows():
        rd = rd.to_dict()
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

# >>> Same .cache.db Athena query-result caching uses; one artifact per outdir <<< #
def _id_card_cache_path(outdir):
    return cache_db_path(outdir) if outdir else None

# >>> Cache hit returns (n_total, payload_dict) where payload is {col: approx_distinct} <<< #
def _read_id_card_cache(cache_db_path, qname, ttl_days):
    if not cache_db_path or not os.path.exists(cache_db_path):
        return None
    import sqlite3, json
    from datetime import datetime, timedelta
    try:
        conn = sqlite3.connect(cache_db_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS _id_card ("
            "qname TEXT PRIMARY KEY, scanned_at TEXT, n_total INTEGER, payload TEXT)"
        )
        cur.execute(
            "SELECT scanned_at, n_total, payload FROM _id_card WHERE qname = ?",
            (qname,),
        )
        row = cur.fetchone()
        conn.close()
    except Exception:
        return None
    if not row:
        return None
    scanned_at_str, n_total, payload_json = row
    try:
        scanned_at = datetime.fromisoformat(scanned_at_str)
    except (ValueError, TypeError):
        return None
    if datetime.now() - scanned_at > timedelta(days=ttl_days):
        return None
    try:
        payload = json.loads(payload_json)
    except Exception:
        return None
    if not isinstance(n_total, int) or n_total <= 0:
        return None
    return n_total, payload

# >>> Upsert one row in _id_card. Best-effort; silent on failure (writes are <<< #
def _write_id_card_cache(cache_db_path, qname, n_total, payload):
    if not cache_db_path:
        return
    import sqlite3, json
    from datetime import datetime
    try:
        os.makedirs(os.path.dirname(cache_db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(cache_db_path)
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS _id_card ("
            "qname TEXT PRIMARY KEY, scanned_at TEXT, n_total INTEGER, payload TEXT)"
        )
        cur.execute(
            "INSERT OR REPLACE INTO _id_card(qname, scanned_at, n_total, payload) "
            "VALUES (?, ?, ?, ?)",
            (qname, datetime.now().isoformat(), int(n_total), json.dumps(payload)),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  [id-cache] write failed for {qname}: {e}")

# >>> One-shot Athena probe — classify which cols are "ID-like" (cardinality <<< #
def _probe_id_cols_athena(full_table, cte_prefix, where, col_list, threshold,
                          database=None, qname=None, cache_db_path=None,
                          ttl_days=1.0):
    if not col_list:
        return set()
    if os.environ.get('DTRACK_ID_DETECTION', '1') == '0':
        return set()

    if cache_db_path and qname:
        cached = _read_id_card_cache(cache_db_path, qname, ttl_days)
        if cached is not None:
            n_total_c, payload_c = cached
            id_cols_c = {col for col, u in payload_c.items()
                         if u is not None and (u / n_total_c) >= threshold}
            print(f"  [id-cache] hit for {qname}: "
                  f"{len(id_cols_c)}/{len(col_list)} ID-like "
                  f"(TTL={ttl_days}d, threshold={threshold})")
            return id_cols_c

    parts = ["COUNT(*) AS n_total"]
    for i, (col, _) in enumerate(col_list):
        col_q = quote_ident(col, "athena")
        parts.append(f"COUNT(DISTINCT {col_q}) AS u_{i}")
    cols_part = ",\n    ".join(parts)
    where_clause = where.strip() or "1=1"
    sql = f"""{cte_prefix}SELECT
    {cols_part}
FROM {full_table}
WHERE {where_clause}"""

    try:
        df = _query_athena(sql, data_base=database)
    except Exception as e:
        print(f"  [id-probe] failed, no auto-skip: {type(e).__name__}: {e}")
        return set()
    if df.empty:
        return set()

    row = df.iloc[0].to_dict()
    try:
        n_total = int(row.get('n_total') or 0)
    except (ValueError, TypeError):
        return set()
    if n_total == 0:
        return set()

    id_cols = set()
    payload = {}
    for i, (col, _) in enumerate(col_list):
        u_raw = row.get(f'u_{i}')
        if u_raw is None:
            continue
        try:
            u = int(u_raw)
        except (ValueError, TypeError):
            continue
        payload[col] = u
        if (u / n_total) >= threshold:
            id_cols.add(col)

    if cache_db_path and qname:
        _write_id_card_cache(cache_db_path, qname, n_total, payload)

    if id_cols:
        print(f"  [id-probe] {len(id_cols)}/{len(col_list)} ID-like cols "
              f"(threshold={threshold}, cached for {ttl_days}d): {sorted(id_cols)}")
    return id_cols

# >>> Athena/Trino expression: 16-bit unsigned bigint MD5 hash of `value_sql` <<< #
def _athena_portable_hash16(value_sql):
    return (
        f"CAST(from_base(SUBSTR(LOWER(TO_HEX(MD5("
        f"TO_UTF8(TRIM({value_sql}))))), 1, 4), 16) AS BIGINT)"
    )

# >>> Coarse col-stats SQL — cheap aggregates for Phase-1 rough drift detection <<< #
def _build_coarse_sql(col_name, col_q, col_type, dt_select, group_by,
                      full_table, wc, cte_prefix, col_dtype=None,
                      preset=None, stats_override=None):
    from .base import apply_null_sentinels
    effective_type = col_dtype or col_type
    col_expr = apply_null_sentinels(col_q, effective_type, "athena")
    is_categorical = (col_type or "").lower() == "categorical"

    if preset in ('quick', 'standard'):
        from ..constants import STATS_PRESETS
        base = STATS_PRESETS[preset]
        if stats_override is not None:
            stats = {k: bool(stats_override.get(k, base[k])) for k in base}
        else:
            stats = base

        if is_categorical and stats['length']:
            cs = f"CAST({col_expr} AS VARCHAR)"
            len_avg = (
                f"CAST(CAST(SUM(LENGTH(TRIM({cs}))) AS DOUBLE) "
                f"/ NULLIF(COUNT({col_expr}), 0) AS VARCHAR)"
            )
            len_max = f"CAST(MAX(LENGTH(TRIM({cs}))) AS VARCHAR)"
        else:
            len_avg = "''"
            len_max = "''"

        if stats['n_unique']:
            n_unique_sql = f"COUNT(DISTINCT {col_expr})"
        else:
            n_unique_sql = "0"

        if stats['min_max']:
            min_sql = f"CAST(MIN({col_expr}) AS VARCHAR)"
            max_sql = f"CAST(MAX({col_expr}) AS VARCHAR)"
        else:
            min_sql = "''"
            max_sql = "''"

        if stats['hash_sum'] and is_categorical:
            hash_sum_sql = (
                f"CAST(SUM(CAST({_athena_portable_hash16(col_expr)} "
                f"AS DOUBLE)) AS VARCHAR)"
            )
        else:
            hash_sum_sql = "''"

        return f"""{cte_prefix}SELECT
    {dt_select},
    '{col_name}' AS column_name,
    '{col_type}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    {n_unique_sql} AS n_unique,
    '' AS mean,
    '' AS std,
    {min_sql} AS min_val,
    {max_sql} AS max_val,
    '' AS top_10,
    {len_avg} AS length_avg,
    {len_max} AS length_max,
    {hash_sum_sql} AS hash_sum
FROM {full_table}
WHERE {wc}{group_by};"""

    if not is_categorical:
        return f"""{cte_prefix}SELECT
    {dt_select},
    '{col_name}' AS column_name,
    '{col_type}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    COUNT(DISTINCT {col_expr}) AS n_unique,
    '' AS mean,
    '' AS std,
    CAST(MIN({col_expr}) AS VARCHAR) AS min_val,
    CAST(MAX({col_expr}) AS VARCHAR) AS max_val,
    '' AS top_10,
    '' AS length_avg,
    '' AS length_max,
    '' AS hash_sum
FROM {full_table}
WHERE {wc}{group_by};"""

    dt_select_clean = dt_select.strip()
    if dt_select_clean.lower().endswith(" as dt"):
        dt_expr_only = dt_select_clean[:-len(" AS dt")].strip()
    else:
        dt_expr_only = dt_select_clean
    dt_group_expr = ""
    if group_by.strip():
        dt_group_expr = group_by.strip().replace("GROUP BY", "", 1).strip()
    col_str_expr = f"CAST({col_expr} AS VARCHAR)"
    if dt_group_expr:
        freq_group_clause = f"GROUP BY {dt_group_expr}, {col_str_expr}"
    else:
        freq_group_clause = f"GROUP BY {col_str_expr}"

    return f"""{cte_prefix}WITH _coarse_freq AS (
    SELECT {dt_expr_only} AS dt,
           {col_str_expr} AS p_col,
           COUNT(*) AS cnt
    FROM {full_table}
    WHERE {wc}
    {freq_group_clause}
),
_coarse_ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(TRIM(p_col)), p_col) AS rnk_alpha,
           COUNT(*)     OVER (PARTITION BY dt) AS k_alpha
    FROM _coarse_freq WHERE p_col IS NOT NULL AND TRIM(p_col) <> ''
),
_coarse_totals AS (
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN p_col IS NULL OR TRIM(p_col) = '' THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN p_col IS NOT NULL AND TRIM(p_col) <> '' THEN 1 ELSE 0 END) AS n_unique
    FROM _coarse_freq GROUP BY dt
),
_coarse_minmax AS (
    SELECT dt,
           MAX(CASE WHEN rnk_alpha = 1       THEN TRIM(p_col) || '=' || CAST(cnt AS VARCHAR) END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN TRIM(p_col) || '=' || CAST(cnt AS VARCHAR) END) AS max_val
    FROM _coarse_ranked GROUP BY dt
),
_coarse_lenhash AS (
    SELECT dt,
           SUM(CAST(cnt AS DOUBLE) * LENGTH(p_col)) / NULLIF(SUM(cnt), 0) AS length_avg,
           MAX(LENGTH(p_col)) AS length_max,
           SUM(CAST(cnt AS DOUBLE) * CAST({_athena_portable_hash16("p_col")} AS DOUBLE)) AS hash_sum
    FROM _coarse_ranked GROUP BY dt
)
SELECT t.dt,
       '{col_name}' AS column_name,
       '{col_type}' AS col_type,
       t.n_total,
       t.n_missing,
       t.n_unique,
       '' AS mean,
       '' AS std,
       mm.min_val,
       mm.max_val,
       '' AS top_10,
       CAST(lh.length_avg AS VARCHAR) AS length_avg,
       CAST(lh.length_max AS VARCHAR) AS length_max,
       CAST(lh.hash_sum AS VARCHAR) AS hash_sum
FROM _coarse_totals t
LEFT JOIN _coarse_minmax  mm ON t.dt = mm.dt
LEFT JOIN _coarse_lenhash lh ON t.dt = lh.dt
ORDER BY t.dt;"""

# >>> Resource-exhaustion fallback for Athena: same per-col SQL shape but <<< #
def _build_lifeboat_col_sql_athena(col_name, col_q, col_dtype, is_numeric,
                                    dt_select, group_by, full_table, wc,
                                    cte_prefix):
    from .base import apply_null_sentinels as _apply_null_sentinels
    col_expr = _apply_null_sentinels(
        col_q, col_dtype or ("numeric" if is_numeric else "categorical"),
        "athena")
    col_type_label = 'numeric' if is_numeric else 'categorical'
    return f"""{cte_prefix}SELECT
    {dt_select},
    '{col_name}' AS column_name,
    '{col_type_label}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    CAST(NULL AS BIGINT) AS n_unique,
    CAST(NULL AS VARCHAR) AS mean,
    CAST(NULL AS VARCHAR) AS std,
    CAST(NULL AS VARCHAR) AS min_val,
    CAST(NULL AS VARCHAR) AS max_val,
    '' AS top_10
FROM {full_table}
WHERE {wc}{group_by}"""

# >>> Append a col-block emission (header + SQL + spacer) consistently <<< #
def _emit_col_block(blocks, header, sql):
    blocks.append(header)
    blocks.append(sql.strip())
    blocks.append(";")
    blocks.append("")

# >>> standalone Athena row SQL builder (extracted so the web /api/preview can call the same code path as the runner) <<< #
def _build_row_sql_athena(tbl_cfg):
    database = tbl_cfg.get("conn_macro", "")
    table = tbl_cfg.get("table", "")
    date_col = tbl_cfg.get("date_col", "")
    date_col_q = quote_ident(date_col, "athena")
    raw_full = f"{database}.{table}" if database else table
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full)
    where = tbl_cfg.get("where", "")
    date_bounds = tbl_cfg.get("_date_bounds", "")
    wc_parts = []
    if date_bounds and date_bounds.strip():
        wc_parts.append(date_bounds.strip())
    if where and where.strip():
        wc_parts.append(where.strip())
    full_where = " AND ".join(f"({p})" for p in wc_parts) if wc_parts else ""
    where_clause = f"WHERE {full_where}" if full_where else ""
    return (
        f"{cte_prefix}SELECT {date_col_q} AS date_value, COUNT(*) AS row_count\n"
        f"FROM {full_table}\n"
        f"{where_clause}\n"
        f"GROUP BY {date_col_q}"
    ).strip()

# >>> Write a single combined .sql file with all AWS table queries <<< #
def _write_combined_sql(aws_tables, outdir, extract_type, db_path=None,
                        coarse=False, preset=None, stats_override=None):
    from datetime import datetime
    label = "coarse" if (coarse and extract_type == "col") else extract_type
    blocks = [f"-- dtrack AWS {label} extraction queries",
              f"-- Generated: {datetime.now().isoformat()}", ""]

    for tbl in aws_tables:
        qname = qualified_name(tbl)
        database = tbl.get('conn_macro', '')
        table = tbl.get('table', '')
        date_col = tbl.get('date_col', '')
        date_col_q = quote_ident(date_col, "athena")
        raw_full = f"{database}.{table}"
        full_table, cte_prefix = resolve_table(tbl, raw_full)
        where = tbl.get('where', '')
        base_where = tbl.get('_base_where', where)

        if extract_type == "col":
            columns = tbl.get('columns', {})
            if not columns:
                blocks.append(f"-- {qname}")
                blocks.append(f"-- SKIP: no columns defined for {qname}")
                blocks.append("")
                continue

            col_list = [(c, d) for c, d in columns.items()
                        if c.lower() != date_col.lower()]

            selected = tbl.get('_selected_cols')
            if selected is not None:
                if not selected:
                    print(f"  WARNING: {qname}: col_filter matched 0 mapped cols; "
                          f"emitting no col blocks for this table")
                wanted = {c.lower() for c in selected}
                col_list = [(c, d) for c, d in col_list if c.lower() in wanted]

            fine_cells = tbl.get('_fine_cells')
            if fine_cells:
                wanted_cols = {c.lower() for c in fine_cells.keys()}
                col_list = [(c, d) for c, d in col_list if c.lower() in wanted_cols]

            if not col_list:
                blocks.append(f"-- {qname}")
                blocks.append(f"-- SKIP: no non-date columns for {qname}"
                              + (" (col_filter left 0)" if selected else ""))
                blocks.append("")
                continue

            vintage = tbl.get('vintage', 'all') or 'all'
            date_filter = compute_date_filter(tbl, db_path, vintage) if db_path else {
                'vintage': vintage, 'filter_type': 'none', 'dates': [],
                'date_dtype': None, 'date_format': None,
            }
            effective_vintage = date_filter['vintage']

            resolve_date_format(date_filter, tbl)

            exclude_dates = tbl.get('_exclude_dates', [])
            exclude_clause = ""
            if exclude_dates:
                date_dtype = date_filter.get('date_dtype')
                date_format = date_filter.get('date_format')
                in_clause = build_date_in_clause(
                    date_col_q, exclude_dates, date_dtype,
                    date_format=date_format,
                )
                exclude_clause = f"NOT ({in_clause})"
                print(f"  {qname}: excluding {len(exclude_dates)} dates")

            _TRUNC_MAP = {'day': 'day', 'week': 'week', 'month': 'month',
                          'quarter': 'quarter', 'year': 'year'}

            trunc_range_where = None
            if date_filter['filter_type'] == 'between':
                from ..date_utils import bucket_date
                buckets = {}
                for dt in date_filter['dates']:
                    buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
                vintage_specs = []
                for bucket_key, dates in sorted(buckets.items()):
                    dw = build_date_range_with_gaps(
                        date_col_q, dates,
                        date_filter['date_dtype'],
                        date_format=date_filter.get('date_format'),
                    )
                    vintage_specs.append((bucket_key, dw))
                sql_mode = 'bucket'
                print(f"  {qname}: vintage={vintage}, {len(vintage_specs)} buckets")
            elif date_filter['filter_type'] == 'in_list':
                dw = build_date_in_clause(
                    date_col_q, date_filter['dates'],
                    date_filter['date_dtype'],
                    date_format=date_filter.get('date_format'),
                )
                vintage_specs = [('sample', dw)]
                sql_mode = 'sample'
                print(f"  {qname}: vintage={vintage}, sample {len(date_filter['dates'])} dates")
            elif vintage in _TRUNC_MAP:
                vintage_specs = []
                sql_mode = 'trunc'
                trunc_range_where = None
                if tbl.get('_from_date') and tbl.get('_to_date'):
                    trunc_range_where = build_date_between_clause(
                        date_col_q, tbl['_from_date'], tbl['_to_date'],
                        date_filter.get('date_dtype'),
                        date_format=date_filter.get('date_format'),
                    )
                print(f"  {qname}: vintage={vintage}, trunc mode "
                      f"(1 query per col, all buckets in one scan)")
            else:
                vintage_specs = [('all', None)]
                sql_mode = 'all'
                trunc_range_where = None
                print(f"  {qname}: vintage={vintage}, no date filter")

            _buckets_count = 1 if sql_mode == 'trunc' else len(vintage_specs)
            _runs = len(col_list) * _buckets_count
            print(f"[AWS col] {qname}: {len(col_list)} cols x {_buckets_count} "
                  f"{'bucketed query' if sql_mode == 'trunc' else 'buckets'} "
                  f"= {_runs} runs")

            date_bounds = tbl.get('_date_bounds', '')
            # bucket/sample modes already scope dates via vintage_specs; use the
            # config base WHERE there so the matching-date filter isn't duplicated
            eff_where = base_where if sql_mode in ('bucket', 'sample') else where
            wc_parts = []
            if sql_mode == 'all' and date_bounds:
                wc_parts.append(date_bounds)
            if eff_where.strip():
                wc_parts.append(eff_where.strip())
            if exclude_clause:
                wc_parts.append(exclude_clause)
            " AND ".join(f"({p})" for p in wc_parts) if wc_parts else "1=1"

            if coarse or preset in ('quick', 'standard'):
                id_cols = set(c for c, _ in col_list)
            else:
                probe_wc_parts = []
                if date_bounds:
                    probe_wc_parts.append(date_bounds)
                if eff_where.strip():
                    probe_wc_parts.append(eff_where.strip())
                if exclude_clause:
                    probe_wc_parts.append(exclude_clause)
                probe_wc = " AND ".join(f"({p})" for p in probe_wc_parts) if probe_wc_parts else "1=1"
                id_threshold = float(os.environ.get('DTRACK_ID_THRESHOLD', '0.99'))
                ttl_days = float(os.environ.get('DTRACK_ID_CACHE_TTL_DAYS', '1.0'))
                id_cols = _probe_id_cols_athena(
                    full_table, cte_prefix, probe_wc, col_list,
                    threshold=id_threshold,
                    database=tbl.get('conn_macro'),
                    qname=qname,
                    cache_db_path=_id_card_cache_path(outdir),
                    ttl_days=ttl_days,
                )

            if sql_mode == 'trunc':
                trunc_col = _vintage_date_expr_athena(
                    date_col_q, vintage,
                    date_dtype=date_filter.get('date_dtype'),
                    date_format=date_filter.get('date_format'),
                )
                trunc_expr = f"CAST({trunc_col} AS VARCHAR)"
                wc_parts_t = []
                if where.strip():
                    wc_parts_t.append(where.strip())
                if trunc_range_where:
                    wc_parts_t.append(trunc_range_where)
                if exclude_clause:
                    wc_parts_t.append(exclude_clause)
                wc_t = " AND ".join(f"({p})" for p in wc_parts_t) if wc_parts_t else "1=1"

                for col_name, col_dtype in col_list:
                    is_num = is_numeric_type(col_dtype, is_oracle=False)
                    col_q = quote_ident(col_name, "athena")
                    fine_vintages = (fine_cells or {}).get(col_name) if fine_cells else None
                    wc_t_col = wc_t
                    if fine_vintages:
                        in_list = ", ".join(
                            "'" + str(v).replace("'", "''") + "'" for v in fine_vintages
                        )
                        wc_t_col = f"({wc_t}) AND ({trunc_expr} IN ({in_list}))"
                    if col_name in id_cols or preset == 'quick':
                        sql = _build_coarse_sql(
                            col_name, col_q,
                            'numeric' if is_num else 'categorical',
                            dt_select=f"{trunc_expr} AS dt",
                            group_by=f"\nGROUP BY {trunc_expr}",
                            full_table=full_table, wc=wc_t_col, cte_prefix=cte_prefix,
                            col_dtype=col_dtype,
                            preset=preset,
                            stats_override=stats_override,
                        )
                        _emit_col_block(
                            blocks, f"-- {qname}/{col_name}/_all", sql)
                        continue
                    col_cfg = dict(tbl)
                    col_cfg['where'] = wc_t_col
                    sql, _ = build_col_sql(
                        'athena', col_cfg, col_name, col_dtype, full_table,
                        cte_prefix=cte_prefix, vintage=vintage,
                        date_dtype=date_filter.get('date_dtype'),
                        date_format=date_filter.get('date_format'),
                        preset=preset, stats_override=stats_override)
                    _emit_col_block(
                        blocks, f"-- {qname}/{col_name}/_all", sql)
                continue

            for col_name, col_dtype in col_list:
                is_num = is_numeric_type(col_dtype, is_oracle=False)
                col_q = quote_ident(col_name, "athena")
                is_id_like = col_name in id_cols
                fine_vintages = (fine_cells or {}).get(col_name) if fine_cells else None

                for dt_label, date_where in vintage_specs:
                    if fine_vintages and dt_label not in fine_vintages:
                        continue
                    parts = list(wc_parts)
                    if date_where:
                        parts.append(date_where)
                    wc = " AND ".join(f"({p})" for p in parts) if parts else "1=1"

                    dt_expr = f"'{dt_label}'"
                    block_suffix = f"/{dt_label}" if len(vintage_specs) > 1 else ""

                    if is_id_like or preset == 'quick':
                        sql = _build_coarse_sql(
                            col_name, col_q,
                            'numeric' if is_num else 'categorical',
                            dt_select=f"{dt_expr} AS dt",
                            group_by="",
                            full_table=full_table, wc=wc, cte_prefix=cte_prefix,
                            col_dtype=col_dtype,
                            preset=preset,
                            stats_override=stats_override,
                        )
                        _emit_col_block(
                            blocks,
                            f"-- {qname}/{col_name}{block_suffix}",
                            sql)
                        continue
                    col_cfg = dict(tbl)
                    col_cfg['where'] = wc
                    sql, _ = build_col_sql(
                        'athena', col_cfg, col_name, col_dtype, full_table,
                        cte_prefix=cte_prefix, vintage='all', dt_label=dt_label,
                        preset=preset, stats_override=stats_override)

                    _emit_col_block(
                        blocks,
                        f"-- {qname}/{col_name}{block_suffix}",
                        sql)
        else:
            # >>> single source of truth: same builder as /api/preview <<< #
            sql = _build_row_sql_athena(tbl) + ";"
            blocks.append(f"-- {qname}")
            blocks.append(sql.strip())
            blocks.append("")

    sql_path = os.path.join(outdir, f"extract_{extract_type}.sql")
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    print(f"  SQL: {sql_path}")

# >>> Extract column stats for one AWS table -> {qname}_col.csv <<< #
def _extract_cols_for_table(tbl_cfg, outdir, max_workers=None, db_path=None, vintage=None,
                            preset=None, stats_override=None):
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    name = tbl_cfg['name']
    database = tbl_cfg['conn_macro']
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    raw_full_table = f"{database}.{table}"
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full_table)

    print(f"\nExtracting columns: {name} ({raw_full_table})")

    columns = tbl_cfg.get('columns', {})
    if not columns:
        print(f"  WARNING: No columns specified for {name}. "
              f"Use 'dtrack load-columns --source aws' or provide columns in config.")
        return None

    non_date_cols = [(c, d) for c, d in columns.items() if c.lower() != date_col.lower()]

    table_vintage = tbl_cfg.get('vintage', vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)

    resolve_date_format(date_filter, tbl_cfg)

    global _sql_log_file
    qname = qualified_name(tbl_cfg)
    log_path = os.path.join(outdir, f"{qname}_col.sql")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"-- SQL queries for {qname} col extraction\n-- {datetime.now().isoformat()}\n\n")
    _sql_log_file = log_path

    set_athena_cache(cache_db_path(outdir))

    col_start_time = time.time()
    col_start_ts = datetime.now().isoformat()

    _max_workers = max_workers or (int(os.environ['MAX_WORKERS']) if os.environ.get('MAX_WORKERS') else 4)

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    extract_cfg = dict(tbl_cfg)
    orig_where = tbl_cfg.get('where', '')
    base_where = tbl_cfg.get('_base_where', orig_where)
    date_bounds = tbl_cfg.get('_date_bounds', '')
    effective_vintage = date_filter.get('vintage', 'all')
    extract_date_dtype = date_filter.get('date_dtype')
    extract_date_format = date_filter.get('date_format')

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
        extract_cfg['where'] = f"{date_bounds} AND ({orig_where})" if orig_where else date_bounds

    bucket_desc = 'sample' if date_filter['filter_type'] == 'in_list' else effective_vintage
    dt_label = 'sample' if date_filter['filter_type'] == 'in_list' else None

    where_summary = extract_cfg.get('where', '(none)').strip() or '(none)'
    print(f"  {len(non_date_cols)} columns, WHERE: {where_summary}")

    all_stats = []

    if _max_workers <= 1:
        col_iter = non_date_cols
        if tqdm is not None:
            col_iter = tqdm(col_iter, desc=f"  {name} [{bucket_desc}]", unit="col")
        for col, dtype in col_iter:
            try:
                stats = _extract_col_athena(
                    extract_cfg, col, dtype, full_table, cte_prefix,
                    database=database, vintage=effective_vintage,
                    date_dtype=extract_date_dtype, date_format=extract_date_format,
                    dt_label=dt_label)
                all_stats.extend(stats)
            except Exception as e:
                print(f"  Warning: Failed to extract {col}: {e}")
    else:
        def _extract_one(col_dtype, _cfg=extract_cfg, _lbl=dt_label):
            col, dtype = col_dtype
            return _extract_col_athena(
                _cfg, col, dtype, full_table, cte_prefix,
                database=database, vintage=effective_vintage,
                date_dtype=extract_date_dtype, date_format=extract_date_format,
                dt_label=_lbl)

        with ThreadPoolExecutor(max_workers=_max_workers) as executor:
            futures = {executor.submit(_extract_one, (c, d)): c for c, d in non_date_cols}
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
        print(f"  Athena cache: {hits} hit / {misses} miss "
              f"({cache_db_path(outdir)})")
    set_athena_cache(None)
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

    first_col = all_stats[0]['column_name']
    total_records = 0
    for s in all_stats:
        if s['column_name'] == first_col:
            try:
                total_records += int(float(s.get('n_total', 0) or 0))
            except (ValueError, TypeError):
                pass
    records_info = f", ~{total_records:,} records" if total_records else ""
    print(f"  Column stats: {csv_path} ({len(all_stats)} rows, {col_elapsed:.1f}s{records_info})")

    return {
        'table': name, 'step': 'col',
        'start': col_start_ts, 'end': col_end_ts,
        'elapsed_sec': col_elapsed,
    }

# >>> Extract data directly from AWS Athena <<< #
def extract_aws(config_path, outdir, types=None, max_workers=None, db_path=None, vintage=None, force=False,
                from_date=None, to_date=None, preset=None, stats_override=None, gen_only=False):
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

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    for tbl in aws_tables:
        if vintage:
            tbl['vintage'] = vintage
        elif 'vintage' not in tbl:
            tbl['vintage'] = 'all'
    inject_where_from_config(aws_tables, config)

    def _athena_bounds(tbl, date_col, eff_from, eff_to):
        date_col_q = quote_ident(date_col, "athena")
        date_type = (tbl.get('date_type') or '').lower()
        custom = config.get('date_types')
        bounds = []
        if eff_from:
            bounds.append(f"{date_col_q} >= "
                          f"{_format_athena_date_bound(eff_from, date_type, is_upper=False, custom_date_types=custom)}")
        if eff_to:
            bounds.append(f"{date_col_q} <= "
                          f"{_format_athena_date_bound(eff_to, date_type, is_upper=True, custom_date_types=custom)}")
        return " AND ".join(bounds)
    apply_pair_date_bounds(aws_tables, config, from_date, to_date, _athena_bounds)
    if gen_only:
        from .base import tag_generated_sql
        if "row" in types:
            _write_combined_sql(aws_tables, outdir, "row",
                                preset=preset, stats_override=stats_override)
            tag_generated_sql(outdir, "row", "aws")
        if "col" in types:
            if db_path:
                fill_columns_from_meta(aws_tables, db_path)
            _write_combined_sql(aws_tables, outdir, "col", db_path=db_path,
                                preset=preset, stats_override=stats_override)
            tag_generated_sql(outdir, "col", "aws")
        print(f"[gen-only] wrote Athena extract SQL to {outdir} (no extraction)")
        return

    try:
        import pyathena  # noqa: F401
    except ImportError:
        print("Error: pyathena is required for AWS extraction")
        print("Install with: pip install 'dtrack[aws]'")
        return

    if db_path and "col" in types:
        fill_columns_from_meta(aws_tables, db_path)

    timing_records = []

    if "row" in types:
        _write_combined_sql(aws_tables, outdir, "row",
                            preset=preset, stats_override=stats_override)
        sql_path = os.path.join(outdir, "extract_row.sql")
        row_results = run_sql_file(sql_path, outdir, max_workers=max_workers, db_path=db_path)
        for r in row_results:
            timing_records.append({
                'table': r['name'], 'step': 'row',
                'start': r['start'], 'end': '',
                'elapsed_sec': r['elapsed'],
            })

    if "col" in types:
        aws_creds_renew()
        conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
        cursor = conn.cursor()

        for tbl_cfg in aws_tables:
            try:
                result = _extract_cols_for_table(
                    tbl_cfg, outdir, max_workers=max_workers, db_path=db_path, vintage=vintage,
                    preset=preset, stats_override=stats_override)
                if result:
                    timing_records.append(result)
            except Exception as e:
                print(f"  ERROR extracting columns for {tbl_cfg['name']}: {e}")
                print("  Skipping to next table...")
                timing_records.append({
                    'table': tbl_cfg['name'], 'step': 'col',
                    'start': '', 'end': '',
                    'elapsed_sec': 0, 'status': str(e),
                })

        cursor.close()
        conn.close()

    from .base import write_timing_csv
    write_timing_csv(outdir, timing_records, aws_tables)

    print(f"\nExtraction complete. Output in: {outdir}")

# >>> Parse a combined SQL file into named query blocks <<< #
def parse_sql_file(sql_path):
    with open(sql_path, 'r', encoding='utf-8') as f:
        text = f.read()

    blocks = []
    current_name = None
    current_lines = []

    def _save(name, lines):
        raw = '\n'.join(lines).strip()
        sql = raw.rstrip(';').strip()
        if sql:
            blocks.append({"name": name, "sql": sql})

    for line in text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('-- dtrack') or stripped.startswith('-- Generated'):
            continue
        if stripped.startswith('-- ') and not stripped.startswith('-- ['):
            marker = stripped[3:].strip()
            if marker and ' ' not in marker:
                if current_name is not None:
                    _save(current_name, current_lines)
                current_name = marker
                current_lines = []
                continue
        current_lines.append(line)

    if current_name is not None:
        _save(current_name, current_lines)

    return blocks

# >>> Read extract_{type}.sql, parse into blocks, and execute each via Athena <<< #
def run_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                 on_progress=None, resume=False, cancel_event=None):
    import sys
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    blocks = parse_sql_file(sql_path)
    if not blocks:
        print(f"No query blocks found in {sql_path}")
        return []

    set_athena_cache(cache_db_path(outdir))

    basename = os.path.basename(sql_path)
    is_col = "extract_col" in basename
    file_suffix = "_col.csv" if is_col else "_row.csv"
    step_label = "col" if is_col else "row"

    os.makedirs(outdir, exist_ok=True)
    _max_workers = max_workers or int(os.environ.get('MAX_WORKERS', '4'))

    table_blocks = {}
    for block in blocks:
        qname = block['name'].split('/')[0] if '/' in block['name'] else block['name']
        table_blocks.setdefault(qname, []).append(block)

    _COL_HEADERS = [
        'dt', 'column_name', 'col_type', 'n_total', 'n_missing',
        'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
        'length_avg', 'length_max', 'hash_sum',
    ]

    _col_rows_lock = threading.Lock()
    _col_rows_by_table = {}

    n_skipped = 0
    if resume and is_col:
        preload_rows = {}
        preload_seen = {}
        for qname in table_blocks:
            csv_path = os.path.join(outdir, f"{qname}_col.csv")
            if not os.path.exists(csv_path):
                continue
            rows = []
            seen = set()
            try:
                with open(csv_path, 'r', newline='') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    if header != _COL_HEADERS:
                        print(f"  [resume] skip preload for {qname}: header mismatch",
                              file=sys.stderr)
                        continue
                    for row in reader:
                        if len(row) < 2:
                            continue
                        rows.append(row)
                        seen.add((row[1], row[0]))
            except Exception as e:
                print(f"  [resume] failed to read {csv_path}: {e}", file=sys.stderr)
                continue
            preload_rows[qname] = rows
            preload_seen[qname] = seen

        def _block_done(block):
            parts = block['name'].split('/')
            if len(parts) < 2:
                return False
            q, col_name = parts[0], parts[1]
            dt_hint = parts[2] if len(parts) > 2 else None
            seen = preload_seen.get(q, set())
            if not seen:
                return False
            if dt_hint and dt_hint != '_all':
                return (col_name, dt_hint) in seen
            return any(c == col_name for c, _ in seen)

        filtered = {}
        for qname, tbl_blocks in table_blocks.items():
            kept = [b for b in tbl_blocks if not _block_done(b)]
            skipped = len(tbl_blocks) - len(kept)
            n_skipped += skipped
            if kept:
                filtered[qname] = kept
            if skipped:
                print(f"  [resume] {qname}: skipping {skipped}/{len(tbl_blocks)} "
                      f"already-done queries", file=sys.stderr)
        table_blocks = filtered
        blocks = [b for tbl_blocks in table_blocks.values() for b in tbl_blocks]
        for qname, rows in preload_rows.items():
            _col_rows_by_table[qname] = list(rows)

        if not blocks:
            print("  [resume] nothing to re-run — all queries already completed",
                  file=sys.stderr)
            return []

    n_tables = len(table_blocks)
    msg = (f"  Parsed {len(blocks)} queries ({n_tables} tables) from {sql_path}, "
           f"workers={_max_workers}")
    if resume and n_skipped:
        msg += f" | resume: skipped {n_skipped} done queries"
    print(msg, file=sys.stderr)

    results = []
    _done_count = [0]
    _table_done = {}
    _table_lock = threading.Lock()
    _running_count = [0]
    _running_lock = threading.Lock()
    _run_start = [None]

    for qname, tbl_blocks in table_blocks.items():
        _table_done[qname] = {"done": 0, "total": len(tbl_blocks)}

    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        _tqdm = None

    _tqdm_bars = {}
    if _tqdm and sys.stderr.isatty():
        for i, (qname, tbl_blocks) in enumerate(table_blocks.items()):
            _tqdm_bars[qname] = _tqdm(
                total=len(tbl_blocks), desc=f"  {qname}",
                unit="qry", position=i, file=sys.stderr, leave=True)

    def _run_block(block):
        block_name = block['name']
        qname = block_name.split('/')[0] if '/' in block_name else block_name
        sql = block['sql']
        start_time = time.time()
        start_ts = datetime.now().isoformat()

        if cancel_event is not None and cancel_event.is_set():
            result = {"name": block_name, "ok": False, "error": "cancelled",
                      "elapsed": 0.0, "start": start_ts, "cancelled": True}
            _done_count[0] += 1
            if on_progress:
                result["done"] = _done_count[0]
                result["total"] = len(blocks)
                result["table"] = qname
                on_progress(result)
            return result

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

        try:
            cached = _cache_get(sql, None)
            if cached is not None:
                rows, col_names = cached
            else:
                aws_creds_renew()
                conn = athena_connect()
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                col_names = (
                    [desc[0] for desc in cur.description]
                    if cur.description else [])
                cur.close()
                conn.close()
                _cache_put(sql, None,
                           (rows, col_names), row_count=len(rows))
            elapsed = time.time() - start_time

            if is_col:
                col_names_lc = [c.lower() for c in col_names]
                parsed_rows = []
                for row in rows:
                    row_dict = dict(zip(col_names_lc, row))
                    parsed_rows.append([row_dict.get(h, '') for h in _COL_HEADERS])
                with _col_rows_lock:
                    _col_rows_by_table.setdefault(qname, []).extend(parsed_rows)
            else:
                csv_path = os.path.join(outdir, f"{block_name}{file_suffix}")
                sorted_rows = sorted(rows, key=lambda r: str(r[0]) if r else '')
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['date_value', 'row_count'])
                    for row in sorted_rows:
                        writer.writerow([row[0], row[1]])

            result = {"name": block_name, "ok": True, "rows": len(rows),
                      "elapsed": round(elapsed, 1), "start": start_ts}
            if not is_col:
                result["csv_path"] = os.path.join(outdir, f"{block_name}{file_suffix}")
        except Exception as e:
            elapsed = time.time() - start_time
            result = {"name": block_name, "ok": False, "error": str(e),
                      "elapsed": round(elapsed, 1), "start": start_ts}
        finally:
            with _running_lock:
                _running_count[0] -= 1
                running_after = _running_count[0]

        _done_count[0] += 1
        table_just_finished = False
        with _table_lock:
            _table_done[qname]["done"] += 1
            tbl_progress = _table_done[qname]
            if (is_col
                    and tbl_progress["done"] == tbl_progress["total"]):
                table_just_finished = True

        n = _done_count[0]
        total = len(blocks)
        eta_sec = None
        if _run_start[0]:
            elapsed_wall = time.time() - _run_start[0]
            if n > 0:
                eta_sec = elapsed_wall * (total - n) / n

        if table_just_finished:
            with _col_rows_lock:
                rows_for_qname = _col_rows_by_table.pop(qname, [])
            csv_path = os.path.join(outdir, f"{qname}_col.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(_COL_HEADERS)
                for r in rows_for_qname:
                    writer.writerow(r)
            print(f"  Wrote {len(rows_for_qname)} rows to {csv_path}",
                  file=sys.stderr)

        if qname in _tqdm_bars:
            _tqdm_bars[qname].update(1)
        elif not _tqdm_bars:
            status = "ok" if result["ok"] else f"FAIL: {result.get('error', '')}"
            print(f"  [{n}/{total} | running={running_after}] "
                  f"{block_name}: {status} ({elapsed:.1f}s)", file=sys.stderr)

        if on_progress:
            result["done"] = n
            result["total"] = total
            result["table"] = qname
            result["running"] = running_after
            result["eta_sec"] = eta_sec
            result["table_done"] = tbl_progress["done"]
            result["table_total"] = tbl_progress["total"]
            result["table_csv_written"] = table_just_finished
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

    for bar in _tqdm_bars.values():
        bar.close()

    if is_col and _col_rows_by_table:
        for qname, row_data in _col_rows_by_table.items():
            csv_path = os.path.join(outdir, f"{qname}_col.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(_COL_HEADERS)
                for row in row_data:
                    writer.writerow(row)
            print(f"  Wrote {len(row_data)} rows to {csv_path} "
                  f"(partial — table had failures)")

    results.sort(key=lambda r: r['name'])

    timing_path = os.path.join(outdir, '_timing.csv')
    with open(timing_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['table', 'step', 'start', 'elapsed_sec', 'status'])
        writer.writeheader()
        for r in results:
            writer.writerow({
                'table': r['name'], 'step': step_label, 'start': r['start'],
                'elapsed_sec': r['elapsed'],
                'status': 'ok' if r['ok'] else r.get('error', 'failed'),
            })
    print(f"\nTiming: {timing_path}")

    ok_count = sum(1 for r in results if r['ok'])
    fail_count = len(results) - ok_count
    print(f"Done: {ok_count} succeeded, {fail_count} failed")

    hits, misses = cache_stats()
    if hits or misses:
        print(f"Athena cache: {hits} hit / {misses} miss "
              f"({cache_db_path(outdir)})")
    set_athena_cache(None)

    return results

# >>> Discover column metadata from AWS Athena tables (columns only, no data) <<< #
def discover_aws_columns(config_path, outdir, db_path=None):
    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    all_tables = load_tables_from_config(config)

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    aws_creds_renew()
    conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
    cursor = conn.cursor()

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        qname = qualified_name(tbl_cfg)
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
            from ..db import insert_column_meta
            insert_column_meta(db_path, qname, columns, source='aws')
            print(f"  Saved to _column_meta in {db_path}")

    cursor.close()
    conn.close()
    print(f"\nDiscovery complete. Output in: {outdir}")

def build_continuous_sql_athena(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "athena")

def build_categorical_sql_athena(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "athena")

def build_top10_sql_athena(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "athena")

