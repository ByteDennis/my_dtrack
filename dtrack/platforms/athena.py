"""AthenaBuilder: direct AWS Athena extraction platform."""

import csv
import json
import os
import threading

# `aws_creds_renew()` POSTs/GETs to the corporate auth endpoint with
# verify=False (proxy + self-signed certs). Suppress the per-call warning
# urllib3 emits in that case so it doesn't clutter the dtrack logs.
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

# Athena query result cache. When _cache_db is set, _query_athena() and
# _run_block() consult the SQLite file before submitting to Athena and
# write results back on success. Cache key = SHA256(normalized SQL + db).
# Disable via env var: DTRACK_ATHENA_CACHE=0. Invalidate by deleting the
# .cache.db file (or the matching row, see clear_athena_cache()).
_cache_db = None
_cache_lock = threading.Lock()
_cache_hits = 0
_cache_misses = 0


def set_athena_cache(path):
    """Enable Athena query caching at the given .cache.db path.

    Creates the SQLite schema if missing. Subsequent calls to
    _query_athena() and run_sql_file's _run_block consult this cache
    before hitting Athena. Pass None to disable.
    """
    global _cache_db, _cache_hits, _cache_misses
    if os.environ.get('DTRACK_ATHENA_CACHE') == '0':
        _cache_db = None
        return
    if path is None:
        _cache_db = None
        return
    _cache_db = path
    _cache_hits = 0
    _cache_misses = 0
    import sqlite3
    conn = sqlite3.connect(path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS athena_query_cache (
                sql_hash TEXT PRIMARY KEY,
                sql_text TEXT,
                database  TEXT,
                row_count INTEGER,
                created_at TEXT,
                payload BLOB
            )
        """)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()
    finally:
        conn.close()


def cache_stats():
    """Return (hits, misses) since the most recent set_athena_cache call."""
    return _cache_hits, _cache_misses


def _cache_key(sql, database):
    import hashlib
    import re as _re
    norm = _re.sub(r'\s+', ' ', sql.strip())
    raw = f"{norm}::{database or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _cache_get(sql, database):
    """Return the cached payload (whatever was stored) or None."""
    global _cache_hits, _cache_misses
    if not _cache_db:
        return None
    import sqlite3
    import pickle
    key = _cache_key(sql, database)
    conn = sqlite3.connect(_cache_db)
    try:
        row = conn.execute(
            "SELECT payload FROM athena_query_cache WHERE sql_hash = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        with _cache_lock:
            _cache_misses += 1
        return None
    with _cache_lock:
        _cache_hits += 1
    try:
        return pickle.loads(row[0])
    except Exception:
        return None


def _cache_put(sql, database, value, row_count=None):
    """Store value (pickled) under the SQL+database key."""
    if not _cache_db:
        return
    import sqlite3
    import pickle
    from datetime import datetime as _dt
    key = _cache_key(sql, database)
    blob = pickle.dumps(value)
    with _cache_lock:
        conn = sqlite3.connect(_cache_db)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO athena_query_cache "
                "(sql_hash, sql_text, database, row_count, created_at, payload) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (key, sql, database or '', row_count, _dt.now().isoformat(), blob),
            )
            conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# AWS credential management
# ---------------------------------------------------------------------------

def aws_creds_renew(ttl_minutes=50, retries=3, retry_delay=10):
    """Renew AWS credentials via proxied token-based auth (Windows only).

    Reads AWS_USR / AWS_PWD / AWS_HOST and routes traffic through
    http://{usr}:{pwd}@{host}:8080. POSTs username+password to AWS_TOKEN_URL
    to get a bearer token, then GETs AWS_ARN_URL for temporary credentials.
    On success, sets AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
    AWS_SESSION_TOKEN and refreshes any open s3fs filesystem.

    On Linux/containers credentials come from IAM roles or env vars, so this
    is a no-op. Thread-safe via _aws_creds_lock; skips renewal if last
    refresh was within ttl_minutes.
    """
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


# ---------------------------------------------------------------------------
# Athena connection
# ---------------------------------------------------------------------------

def athena_connect(data_base=None, *, region=None, work_group=None,
                   staging_dir=None):
    """Connect to AWS Athena.

    Each connection param falls back to env when not passed:
      region      <- AWS_DEFAULT_REGION
      work_group  <- AWS_S3_WORK_GROUP
      staging_dir <- AWS_S3_STAGING_DIR
    None values are omitted so pyathena can fall back to its own defaults.
    """
    from pyathena import connect as athena_connect_raw

    kwargs = {
        'region_name':    region      or os.environ.get('AWS_DEFAULT_REGION'),
        'work_group':     work_group  or os.environ.get('AWS_S3_WORK_GROUP'),
        's3_staging_dir': staging_dir or os.environ.get('AWS_S3_STAGING_DIR'),
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return athena_connect_raw(schema_name=data_base, **kwargs)


# ---------------------------------------------------------------------------
# Date casting and vintage bucketing
# ---------------------------------------------------------------------------

def _athena_date_cast(date_expr, date_dtype, date_format=None):
    """Wrap date_expr with appropriate cast for Athena based on column type.

    - date/timestamp columns: use as-is (already date-compatible)
    - varchar/string columns: parse with date_parse using date_format label
    - number columns: cast to varchar first, then date_parse
    - date_format: format label from _metadata (e.g. 'YYYYMMDD', 'DD-MON-YYYY')
    """
    dtype = (date_dtype or '').lower()
    if 'date' in dtype or 'timestamp' in dtype:
        return date_expr

    # Resolve Athena date_parse pattern from format label (default to %Y%m%d)
    athena_fmt = _ATHENA_DATE_PARSE.get(date_format, '%Y%m%d') if date_format else '%Y%m%d'

    if dtype in ('int', 'integer', 'bigint', 'smallint', 'tinyint', 'number'):
        return f"date_parse(CAST({date_expr} AS VARCHAR), '{athena_fmt}')"
    if 'char' in dtype or 'string' in dtype:
        return f"date_parse({date_expr}, '{athena_fmt}')"
    return date_expr


def _vintage_date_expr_athena(date_expr, vintage, vintage_transform=None, date_dtype=None, date_format=None):
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
    cast_expr = _athena_date_cast(date_expr, date_dtype, date_format)
    return f"date_trunc('{unit}', {cast_expr})"


def _format_athena_date_bound(date_str, date_type, is_upper=False):
    """Format a YYYY-MM-DD date as the correct Athena SQL literal."""
    dtype = date_type.lower() if date_type else ""
    time_part = "23:59:59" if is_upper else "00:00:00"

    if dtype == 'num_yyyymm':
        return date_str[:4] + date_str[5:7]
    if dtype in ('num', 'integer', 'int', 'number'):
        return date_str.replace('-', '')
    if dtype == 'string_compact':
        return f"'{date_str.replace('-', '')}'"
    if dtype in ('string_dash', 'string'):
        return f"'{date_str}'"
    if dtype in ('timestamp', 'datetime'):
        return f"TIMESTAMP '{date_str} {time_part}'"
    if dtype == 'date':
        return f"DATE '{date_str}'"
    return f"'{date_str}'"


# ---------------------------------------------------------------------------
# Column discovery
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _query_athena(sql, data_base=None):
    """Execute SQL on Athena via pandas read_sql_query with fresh connection.

    Renews credentials if needed, opens a new connection, runs the query,
    and returns a DataFrame with lowercase column names.

    Consults the Athena cache (if enabled via set_athena_cache) before
    submitting; a hit returns the cached DataFrame and skips the network
    round-trip entirely.
    """
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


def _log_sql(col, sql):
    """Append SQL to log file (thread-safe). No console output."""
    if _sql_log_file:
        with _sql_log_lock:
            with open(_sql_log_file, 'a') as f:
                f.write(f"-- [{col}]\n{sql.strip()}\n;\n\n")


# ---------------------------------------------------------------------------
# Column extraction
# ---------------------------------------------------------------------------

def _extract_col_athena(tbl_cfg, col, dtype, full_table, cte_prefix="", database=None, vintage=None, date_dtype=None, date_format=None, dt_label=None):
    """Extract stats for a single column from Athena.

    Numeric columns: direct aggregation (no top-10).
    Categorical columns: single CTE query computing all stats server-side.

    When vintage is not 'all' and dt_label is None, uses SQL-side GROUP BY
    with _vintage_date_expr_athena to bucket all dates in one query per column.
    When dt_label is set, uses it as a literal dt value (no GROUP BY).
    """
    date_col = tbl_cfg['date_col']
    # Quote identifiers when bare form is invalid (digit-leading, hyphenated,
    # etc.). Mirrors the file-gen path in _write_combined_sql so direct
    # extraction works for the same columns the SQL files do.
    col_q = quote_ident(col, "athena")
    date_col_q = quote_ident(date_col, "athena")
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=False)

    wc = where.strip() if where else "(1=1)"

    # Build date expression and GROUP BY
    if dt_label is not None:
        # Python-computed bucket: use literal label, no GROUP BY
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
    elif vintage == 'all':
        dt_select = f"'all' AS dt"
        group_by = ""
    else:
        # Legacy path: SQL-side vintage bucketing (kept for backward compat)
        vintage_transform = tbl_cfg.get('vintage_transform', None)
        dt_expr = _vintage_date_expr_athena(date_col_q, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
        dt_select = f"CAST({dt_expr} AS VARCHAR) AS dt"
        group_by = f"\nGROUP BY {dt_expr}"

    if numeric:
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{dtype}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_q}) AS n_missing,
    COUNT(DISTINCT {col_q}) AS n_unique,
    AVG(CAST({col_q} AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST({col_q} AS DOUBLE)) AS std,
    MIN({col_q}) AS min_val,
    MAX({col_q}) AS max_val
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

    # Categorical: single unified CTE query that emits everything in one
    # roundtrip -- n_total/n_missing/n_unique, alpha-rank-weighted mean/std,
    # alpha-bound "{cat}={count}" min/max, and top_10. Algorithm matches
    # _col_categorical_freq (Hadoop SAS) so values agree across engines.
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

    sql = f"""{cte_prefix}
WITH freq AS (
    SELECT {freq_dt_select} AS dt,
           CAST({col_q} AS VARCHAR) AS p_col,
           COUNT(*) AS cnt
    FROM {full_table} WHERE {wc}
    GROUP BY {col_q}{freq_dt_group}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(p_col), p_col) AS rnk_alpha,
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
           MAX(CASE WHEN rnk_alpha = 1       THEN p_col || '=' || CAST(cnt AS VARCHAR) END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN p_col || '=' || CAST(cnt AS VARCHAR) END) AS max_val
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
)
SELECT t.dt,
       '{dtype}' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       ss.mean, ss.std,
       mm.min_val, mm.max_val,
       COALESCE(tn.top_10, '') AS top_10
FROM totals t
LEFT JOIN sstats ss ON t.dt = ss.dt
LEFT JOIN minmax mm ON t.dt = mm.dt
LEFT JOIN top_n  tn ON t.dt = tn.dt
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
        })
    return results


# ---------------------------------------------------------------------------
# Mock extraction
# ---------------------------------------------------------------------------

def _extract_aws_mock(config_path, outdir, types, db_path, mock_dir):
    """Legacy mock entry — called when WHERE hasn't been injected yet."""
    from .oracle import _extract_mock
    _extract_mock(config_path, outdir, types, db_path, mock_dir, ('aws',))


def _extract_aws_mock_with_where(aws_tables, outdir, types, db_path, mock_dir):
    """Mock extraction with WHERE-injected tables — writes CSVs + combined .sql file."""
    from .oracle import _extract_mock_tables
    _extract_mock_tables(aws_tables, outdir, types, mock_dir)

    # Write single combined .sql file for verification
    if "row" in (types or ["row"]):
        _write_combined_sql(aws_tables, outdir, "row")


def _id_card_cache_path(outdir):
    """Same .cache.db Athena query-result caching uses; one artifact per outdir."""
    return os.path.join(outdir, ".cache.db") if outdir else None


def _read_id_card_cache(cache_db_path, qname, ttl_days):
    """Cache hit returns (n_total, payload_dict) where payload is {col: approx_distinct}.
    Miss / expired / corrupt → None. All-or-nothing per qname."""
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


def _write_id_card_cache(cache_db_path, qname, n_total, payload):
    """Upsert one row in _id_card. Best-effort; silent on failure (writes are
    optional — a write failure just means the next run re-probes)."""
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


def _probe_id_cols_athena(full_table, cte_prefix, where, col_list, threshold,
                          database=None, qname=None, cache_db_path=None,
                          ttl_days=1.0):
    """One-shot Athena probe — classify which cols are "ID-like" (cardinality
    ≥ threshold) so we can emit a lite SQL block (no GROUP BY p_col) for them
    instead of the full freq aggregation.

    Builds a single batch query over the table:

        SELECT COUNT(*) AS n_total,
               APPROX_DISTINCT(<col_0>) AS u_0,
               APPROX_DISTINCT(<col_1>) AS u_1, ...
        FROM <table> WHERE <where>

    Athena's Parquet column-pruning means this scans only the probed columns
    once — same column footprint the per-col extraction would scan one by one,
    just folded into a single round-trip.

    Returns: set of col names where APPROX_DISTINCT(col)/COUNT(*) ≥ threshold.
    Empty set on any failure (no AWS creds, query error, empty table) so the
    caller falls back to emitting full SQL for every column. Set
    `DTRACK_ID_DETECTION=0` to skip probing entirely.
    """
    if not col_list:
        return set()
    if os.environ.get('DTRACK_ID_DETECTION', '1') == '0':
        return set()

    # Cache lookup: all-or-nothing per qname, TTL configurable
    # (DTRACK_ID_CACHE_TTL_DAYS, default 1.0). Cached payload = {col: u}, so
    # threshold can change without invalidating cache.
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
        # Suffix _i (positional) instead of _<colname> to dodge identifier
        # issues when col names contain hyphens, dots, or start with a digit.
        parts.append(f"APPROX_DISTINCT({col_q}) AS u_{i}")
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

    # Persist to cache so the next gen within TTL skips the probe.
    if cache_db_path and qname:
        _write_id_card_cache(cache_db_path, qname, n_total, payload)

    if id_cols:
        print(f"  [id-probe] {len(id_cols)}/{len(col_list)} ID-like cols "
              f"(threshold={threshold}, cached for {ttl_days}d): {sorted(id_cols)}")
    return id_cols


def _build_coarse_sql(col_name, col_q, col_type, dt_select, group_by,
                      full_table, wc, cte_prefix):
    """Coarse col-stats SQL — single-pass aggregates only, no GROUP BY p_col.

    Emits the 14-column row schema (dt, column_name, col_type, n_total,
    n_missing, n_unique, mean, std, min_val, max_val, top_10, length_avg,
    length_max, hash_sum). The "fine-only" fields (mean/std/top_10) come
    back empty in coarse mode; the loader's level="coarse" path preserves
    any prior fine values for those rather than overwriting with empty.

    Drift signals captured cheaply per row:
      - n_total / n_missing / n_unique (size, null-rate, cardinality)
      - min_val / max_val (alpha) (value-range edges)
      - length_avg / length_max (structural shape — free text vs IDs)
      - hash_sum (multiset checksum via XXHASH64 → SUM)

    n_unique uses APPROX_DISTINCT (HyperLogLog) so we avoid the
    near-quadratic cost COUNT(DISTINCT) hits on high-cardinality columns.
    The tiny accuracy loss (~2%) is acceptable for "is this column worth
    drilling into" — a drift big enough to matter is well over 2%.

    Categorical min_val/max_val here are plain MIN/MAX(col) — alpha-first
    and alpha-last values, no '=cnt' suffix (which requires the freq table
    we're trying to avoid). Fine SQL re-emits min_val/max_val as 'val=cnt'.
    """
    lcol = f"LENGTH(CAST({col_q} AS VARCHAR))"
    # Trino's XXHASH64 returns VARBINARY (8 bytes), not BIGINT, so a direct
    # CAST(... AS DOUBLE) errors with "cannot cast varbinary to double".
    # from_big_endian_64() interprets the 8-byte hash as a signed 64-bit
    # integer; from there CAST AS DOUBLE accumulates safely.
    # SUM as DOUBLE avoids BIGINT overflow on large tables; precision loss
    # doesn't matter — both sides do the same arithmetic, equality is what
    # counts. Cross-platform note: hash_sum only round-trips when left and
    # right use the same hash function. Cross-platform compares (Oracle ↔
    # AWS) ignore this field; n_total/n_missing/n_unique/min/max/length_*
    # are the portable signals.
    hash_expr = (
        f"CAST(from_big_endian_64(xxhash64("
        f"CAST(CAST({col_q} AS VARCHAR) AS VARBINARY))) AS DOUBLE)"
    )
    return f"""{cte_prefix}SELECT
    {dt_select},
    '{col_name}' AS column_name,
    '{col_type}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_q}) AS n_missing,
    APPROX_DISTINCT({col_q}) AS n_unique,
    '' AS mean,
    '' AS std,
    CAST(MIN({col_q}) AS VARCHAR) AS min_val,
    CAST(MAX({col_q}) AS VARCHAR) AS max_val,
    '' AS top_10,
    CAST(AVG({lcol}) AS VARCHAR) AS length_avg,
    CAST(MAX({lcol}) AS VARCHAR) AS length_max,
    CAST(SUM({hash_expr}) AS VARCHAR) AS hash_sum
FROM {full_table}
WHERE {wc}{group_by};"""


def _write_combined_sql(aws_tables, outdir, extract_type, db_path=None,
                        coarse=False):
    """Write a single combined .sql file with all AWS table queries.

    `coarse=True` emits the cheap-aggregate "coarse" SQL for every column —
    no GROUP BY p_col, no freq table, no rank-weighted mean/std, no top_10.
    Output schema is the same 14 columns; the fine-only fields come back
    empty. The loader writes coarse rows with `level="coarse"`, which
    preserves any prior fine values for those rows on the same (col, dt).

    `coarse=False` (default) is the existing fine path: full freq aggregation,
    with the ID-skip auto-emitting coarse blocks for columns the probe
    flagged as near-unique.
    """
    from datetime import datetime
    label = "coarse" if (coarse and extract_type == "col") else extract_type
    blocks = [f"-- dtrack AWS {label} extraction queries",
              f"-- Generated: {datetime.now().isoformat()}", ""]

    for tbl in aws_tables:
        qname = qualified_name(tbl)
        database = tbl.get('conn_macro', '')
        table = tbl.get('table', '')
        date_col = tbl.get('date_col', '')
        # Athena identifier wrapping: only kicks in when the bare form is
        # invalid (digit-leading, hyphens, dots, etc.). Valid names pass
        # through unchanged so case-insensitive resolution still works.
        date_col_q = quote_ident(date_col, "athena")
        raw_full = f"{database}.{table}"
        full_table, cte_prefix = resolve_table(tbl, raw_full)
        where = tbl.get('where', '')

        if extract_type == "col":
            columns = tbl.get('columns', {})
            if not columns:
                blocks.append(f"-- {qname}")
                blocks.append(f"-- SKIP: no columns defined for {qname}")
                blocks.append("")
                continue

            col_list = [(c, d) for c, d in columns.items()
                        if c.lower() != date_col.lower()]

            # Restrict to pair-level col_filter selection, if set. Distinguish:
            #   None  = no filter set         → keep all non-date cols.
            #   []    = filter set, matched 0 → extract nothing.
            #   [...] = filter set, matched N → extract only those.
            # The earlier `if selected:` collapsed the second case into the
            # first, so a col_filter that matched 0 of N cols silently fell
            # through to "all cols".
            selected = tbl.get('_selected_cols')
            if selected is not None:
                if not selected:
                    print(f"  WARNING: {qname}: col_filter matched 0 mapped cols; "
                          f"emitting no col blocks for this table")
                wanted = {c.lower() for c in selected}
                col_list = [(c, d) for c, d in col_list if c.lower() in wanted]

            # Fine-cell filter: when the user asked to drill into specific
            # (col, vintage) cells, drop everything else from this table's
            # col_list. The vintage filter is applied per-bucket below.
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

            # Compute vintage-based date filter (same as SAS path)
            vintage = tbl.get('vintage', 'all') or 'all'
            date_filter = compute_date_filter(tbl, db_path, vintage) if db_path else {
                'vintage': vintage, 'filter_type': 'none', 'dates': [],
                'date_dtype': None, 'date_format': None,
            }
            effective_vintage = date_filter['vintage']

            # Ensure date_format is set from config date_type
            resolve_date_format(date_filter, tbl)

            # Build exclude dates NOT IN clause (per-pair, from config)
            # Uses build_date_in_clause to format dates correctly for the column type
            exclude_dates = tbl.get('_exclude_dates', [])
            exclude_clause = ""
            if exclude_dates:
                date_dtype = date_filter.get('date_dtype')
                date_format = date_filter.get('date_format')
                in_clause = build_date_in_clause(
                    date_col_q, exclude_dates, date_dtype,
                    date_format=date_format,
                )
                # Negate: IN → NOT IN
                exclude_clause = f"NOT ({in_clause})"
                print(f"  {qname}: excluding {len(exclude_dates)} dates")

            # Determine SQL generation mode:
            #   'bucket'   — matching dates exist, one query per bucket (synthetic dt)
            #   'trunc'    — no matching dates but vintage != 'all', use DATE_TRUNC + GROUP BY
            #   'all'      — vintage='all', aggregate everything, dt='all'
            #   'sample'   — sample@N dates, aggregate with dt='sample'
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
                # Vintage set but no matching_dates from row-compare. Use
                # DATE_TRUNC in SELECT/GROUP BY to produce all buckets in ONE
                # scan per column — drastically fewer queries than emitting
                # per-bucket BETWEEN blocks. WHERE stays bare so partition
                # pruning still works.
                vintage_specs = []
                sql_mode = 'trunc'
                # Bare BETWEEN on WHERE when from/to set, so the one-shot
                # scan is still bounded to the user's range.
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
                # vintage='all' or no from/to bounds: aggregate everything
                vintage_specs = [('all', None)]
                sql_mode = 'all'
                trunc_range_where = None
                print(f"  {qname}: vintage={vintage}, no date filter")

            # Summarize what we're emitting so the running-log line matches
            # the SAS "cols x buckets = runs" format.
            _buckets_count = 1 if sql_mode == 'trunc' else len(vintage_specs)
            _runs = len(col_list) * _buckets_count
            print(f"[AWS col] {qname}: {len(col_list)} cols x {_buckets_count} "
                  f"{'bucketed query' if sql_mode == 'trunc' else 'buckets'} "
                  f"= {_runs} runs")

            # Generate per-column SQL blocks
            # Build base WHERE: table where + exclude dates. When there's no
            # BETWEEN/IN from date_filter (sql_mode='all'), layer _date_bounds
            # so --from/--to still bound the scan.
            wc_parts = []
            if where.strip():
                wc_parts.append(where.strip())
            if exclude_clause:
                wc_parts.append(exclude_clause)
            date_bounds = tbl.get('_date_bounds', '')
            if sql_mode == 'all' and date_bounds:
                wc_parts.append(date_bounds)
            wc_base = " AND ".join(f"({p})" for p in wc_parts) if wc_parts else "1=1"

            # Auto-detect ID-like columns (cardinality / row-count >= threshold)
            # via one batched APPROX_DISTINCT probe over the table. ID-like
            # cols get a lite SQL block (no GROUP BY p_col) — saves a heavy
            # freq aggregation per col. Probe WHERE matches the broadest
            # table-level filter so the cardinality classification reflects
            # what the col SQL would actually scan.
            if coarse:
                # In coarse mode, every column emits the cheap-aggregate
                # SQL — no probe needed (id-skip is irrelevant when
                # everything is "lite").
                id_cols = set(c for c, _ in col_list)
            else:
                probe_wc_parts = []
                if where.strip():
                    probe_wc_parts.append(where.strip())
                if exclude_clause:
                    probe_wc_parts.append(exclude_clause)
                if date_bounds:
                    probe_wc_parts.append(date_bounds)
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

            # 'trunc' mode: ONE SQL per column, with DATE_TRUNC in SELECT/
            # GROUP BY to produce all buckets in a single scan. WHERE stays
            # bare so partition pruning still kicks in. This is the fast
            # default when vintage is set but no row-compare data exists.
            if sql_mode == 'trunc':
                unit = _TRUNC_MAP[vintage]
                trunc_col = _vintage_date_expr_athena(
                    date_col_q, vintage,
                    date_dtype=date_filter.get('date_dtype'),
                    date_format=date_filter.get('date_format'),
                )
                trunc_expr = f"CAST({trunc_col} AS VARCHAR)"
                # Base WHERE: user's WHERE + optional from/to bound. The
                # date column is NEVER wrapped in WHERE so Athena can
                # partition-prune. exclude_dates appended if set.
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
                    # Quote col_name as an identifier when bare form is invalid
                    # (e.g. starts with a digit). The string-literal occurrence
                    # `'{col_name}' AS column_name` keeps the raw name so the
                    # downstream loader still maps results back by original name.
                    col_q = quote_ident(col_name, "athena")
                    # Per-col fine-cell vintage filter: when the user picked
                    # specific vintage labels for this col on /col_compare,
                    # add an IN clause on trunc_expr so only those buckets
                    # come back. NULL/missing → emit all buckets (default).
                    fine_vintages = (fine_cells or {}).get(col_name) if fine_cells else None
                    wc_t_col = wc_t
                    if fine_vintages:
                        in_list = ", ".join(
                            "'" + str(v).replace("'", "''") + "'" for v in fine_vintages
                        )
                        wc_t_col = f"({wc_t}) AND ({trunc_expr} IN ({in_list}))"
                    if col_name in id_cols:
                        # ID-like: lite SQL, single GROUP BY trunc_expr pass.
                        sql = _build_coarse_sql(
                            col_name, col_q,
                            'numeric' if is_num else 'categorical',
                            dt_select=f"{trunc_expr} AS dt",
                            group_by=f"\nGROUP BY {trunc_expr}",
                            full_table=full_table, wc=wc_t_col, cte_prefix=cte_prefix,
                        )
                        blocks.append(f"-- {qname}/{col_name}/_all")
                        blocks.append(sql.strip())
                        blocks.append("")
                        continue
                    if is_num:
                        sql = f"""{cte_prefix}SELECT
    {trunc_expr} AS dt,
    '{col_name}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_q}) AS n_missing,
    COUNT(DISTINCT {col_q}) AS n_unique,
    CAST(AVG(CAST({col_q} AS DOUBLE)) AS VARCHAR) AS mean,
    CAST(STDDEV_SAMP(CAST({col_q} AS DOUBLE)) AS VARCHAR) AS std,
    CAST(MIN({col_q}) AS VARCHAR) AS min_val,
    CAST(MAX({col_q}) AS VARCHAR) AS max_val,
    '' AS top_10
FROM {full_table}
WHERE {wc_t_col}
GROUP BY {trunc_expr};"""
                    else:
                        sql = f"""{cte_prefix}WITH freq AS (
    SELECT {trunc_expr} AS dt,
           CAST({col_q} AS VARCHAR) AS p_col,
           COUNT(*) AS cnt
    FROM {full_table}
    WHERE {wc_t_col}
    GROUP BY {trunc_expr}, {col_q}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(p_col), p_col) AS rnk_alpha,
           COUNT(*) OVER (PARTITION BY dt) AS k_alpha,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC, p_col ASC) AS rn
    FROM freq WHERE p_col IS NOT NULL AND TRIM(p_col) <> ''
),
top_n AS (
    SELECT dt,
           ARRAY_JOIN(ARRAY_AGG(
               p_col || '(' || CAST(cnt AS VARCHAR) || ')'
               ORDER BY cnt DESC
           ), '; ') AS top_10
    FROM ranked WHERE rn <= 10
    GROUP BY dt
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
           CAST(sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0) AS VARCHAR) AS mean,
           CAST(SQRT(GREATEST(
               sum_cnt_rnk2 * 1.0 / NULLIF(sum_cnt, 0)
               - (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0))
               * (sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0)), 0)) AS VARCHAR) AS std
    FROM agg
),
minmax AS (
    SELECT dt,
           MAX(CASE WHEN rnk_alpha = 1       THEN p_col || '=' || CAST(cnt AS VARCHAR) END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN p_col || '=' || CAST(cnt AS VARCHAR) END) AS max_val
    FROM ranked GROUP BY dt
)
SELECT s.dt, '{col_name}' AS column_name, 'categorical' AS col_type,
       s.n_total, s.n_missing, s.n_unique,
       ss.mean, ss.std, mm.min_val, mm.max_val,
       COALESCE(t.top_10, '') AS top_10
FROM totals s
LEFT JOIN sstats ss ON s.dt = ss.dt
LEFT JOIN minmax mm ON s.dt = mm.dt
LEFT JOIN top_n t ON s.dt = t.dt
ORDER BY s.dt;"""
                    # Header name must be space-free so parse_sql_file picks
                    # it up as a block marker. `_all` suffix conveys that this
                    # block covers every bucket in one scan.
                    blocks.append(f"-- {qname}/{col_name}/_all")
                    blocks.append(sql.strip())
                    blocks.append("")
                continue  # next table; don't fall through to per-bucket loop

            # bucket / sample / all modes: one block per (column, vintage bucket).
            # dt_label is emitted as a SQL string literal; WHERE stays bare.
            # wc_parts already covers user where + exclude_dates + (in 'all'
            # mode) the --from/--to date_bounds; the per-bucket date_where (if
            # any) layers a BETWEEN/IN on top.
            for col_name, col_dtype in col_list:
                is_num = is_numeric_type(col_dtype, is_oracle=False)
                # Same identifier-quoting rule as the trunc path above:
                # col_q is the SQL-safe ref, '{col_name}' stays the raw label.
                col_q = quote_ident(col_name, "athena")
                is_id_like = col_name in id_cols
                fine_vintages = (fine_cells or {}).get(col_name) if fine_cells else None

                for dt_label, date_where in vintage_specs:
                    if fine_vintages and dt_label not in fine_vintages:
                        # User selected specific vintages for this col;
                        # skip non-selected buckets.
                        continue
                    parts = list(wc_parts)
                    if date_where:
                        parts.append(date_where)
                    wc = " AND ".join(f"({p})" for p in parts) if parts else "1=1"

                    dt_expr = f"'{dt_label}'"
                    block_suffix = f"/{dt_label}" if len(vintage_specs) > 1 else ""

                    if is_id_like:
                        sql = _build_coarse_sql(
                            col_name, col_q,
                            'numeric' if is_num else 'categorical',
                            dt_select=f"{dt_expr} AS dt",
                            group_by="",
                            full_table=full_table, wc=wc, cte_prefix=cte_prefix,
                        )
                        blocks.append(f"-- {qname}/{col_name}{block_suffix}")
                        blocks.append(sql.strip())
                        blocks.append("")
                        continue
                    if is_num:
                        sql = f"""{cte_prefix}SELECT
    {dt_expr} AS dt,
    '{col_name}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_q}) AS n_missing,
    COUNT(DISTINCT {col_q}) AS n_unique,
    CAST(AVG(CAST({col_q} AS DOUBLE)) AS VARCHAR) AS mean,
    CAST(STDDEV_SAMP(CAST({col_q} AS DOUBLE)) AS VARCHAR) AS std,
    CAST(MIN({col_q}) AS VARCHAR) AS min_val,
    CAST(MAX({col_q}) AS VARCHAR) AS max_val,
    '' AS top_10
FROM {full_table}
WHERE {wc};"""
                    else:
                        sql = f"""{cte_prefix}WITH freq AS (
    SELECT
        CAST({col_q} AS VARCHAR) AS p_col,
        COUNT(*) AS cnt
    FROM {full_table}
    WHERE {wc}
    GROUP BY {col_q}
),
ranked AS (
    SELECT p_col, cnt,
        ROW_NUMBER() OVER (ORDER BY UPPER(p_col), p_col) AS rnk_alpha,
        COUNT(*) OVER () AS k_alpha,
        ROW_NUMBER() OVER (ORDER BY cnt DESC, p_col ASC) AS rn
    FROM freq WHERE p_col IS NOT NULL AND TRIM(p_col) <> ''
)
SELECT
    {dt_expr} AS dt,
    '{col_name}' AS column_name,
    'categorical' AS col_type,
    (SELECT SUM(cnt) FROM freq) AS n_total,
    COALESCE((SELECT SUM(cnt) FROM freq WHERE p_col IS NULL OR TRIM(p_col) = ''), 0) AS n_missing,
    (SELECT SUM(CASE WHEN p_col IS NOT NULL AND TRIM(p_col) <> '' THEN 1 ELSE 0 END) FROM freq) AS n_unique,
    CAST((SELECT SUM(CAST(cnt AS DOUBLE) * rnk_alpha) / NULLIF(SUM(CAST(cnt AS DOUBLE)), 0) FROM ranked) AS VARCHAR) AS mean,
    CAST((SELECT SQRT(GREATEST(
        SUM(CAST(cnt AS DOUBLE) * rnk_alpha * rnk_alpha) / NULLIF(SUM(CAST(cnt AS DOUBLE)), 0)
        - POWER(SUM(CAST(cnt AS DOUBLE) * rnk_alpha) / NULLIF(SUM(CAST(cnt AS DOUBLE)), 0), 2),
        0)) FROM ranked) AS VARCHAR) AS std,
    (SELECT MAX(CASE WHEN rnk_alpha = 1       THEN p_col || '=' || CAST(cnt AS VARCHAR) END) FROM ranked) AS min_val,
    (SELECT MAX(CASE WHEN rnk_alpha = k_alpha THEN p_col || '=' || CAST(cnt AS VARCHAR) END) FROM ranked) AS max_val,
    COALESCE((SELECT ARRAY_JOIN(ARRAY_AGG(
        p_col || '(' || CAST(cnt AS VARCHAR) || ')'
        ORDER BY cnt DESC
    ), '; ') FROM ranked WHERE rn <= 10), '') AS top_10;"""

                    blocks.append(f"-- {qname}/{col_name}{block_suffix}")
                    blocks.append(sql.strip())
                    blocks.append("")
        else:
            # Row count query: layer user where + --from/--to bounds.
            date_bounds = tbl.get('_date_bounds', '')
            wc_parts = []
            if where.strip():
                wc_parts.append(where.strip())
            if date_bounds:
                wc_parts.append(date_bounds)
            full_where = " AND ".join(f"({p})" for p in wc_parts) if wc_parts else ""
            where_clause = f"WHERE {full_where}" if full_where else ""
            sql = f"""{cte_prefix}SELECT {date_col_q} AS date_value, COUNT(*) AS row_count
FROM {full_table}
{where_clause}
GROUP BY {date_col_q};"""
            blocks.append(f"-- {qname}")
            blocks.append(sql.strip())
            blocks.append("")

    sql_path = os.path.join(outdir, f"extract_{extract_type}.sql")
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    print(f"  SQL: {sql_path}")


# ---------------------------------------------------------------------------
# Per-table column extraction helper
# ---------------------------------------------------------------------------

def _extract_cols_for_table(tbl_cfg, outdir, max_workers=None, db_path=None, vintage=None):
    """Extract column stats for one AWS table -> {qname}_col.csv.

    Returns a timing dict on success, or None if skipped.
    """
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

    # Compute unified date filter from database matching dates
    table_vintage = tbl_cfg.get('vintage', vintage)
    date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)

    # Ensure date_format is set from config date_type
    resolve_date_format(date_filter, tbl_cfg)

    # Set up SQL log file
    global _sql_log_file
    qname = qualified_name(tbl_cfg)
    log_path = os.path.join(outdir, f"{qname}_col.sql")
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(f"-- SQL queries for {qname} col extraction\n-- {datetime.now().isoformat()}\n\n")
    _sql_log_file = log_path

    # Enable per-outdir Athena query cache. Hits skip the network round-trip;
    # misses store the result for future runs. Delete .cache.db to invalidate.
    set_athena_cache(os.path.join(outdir, ".cache.db"))

    col_start_time = time.time()
    col_start_ts = datetime.now().isoformat()

    _max_workers = max_workers or (int(os.environ['MAX_WORKERS']) if os.environ.get('MAX_WORKERS') else 4)

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

    # Build WHERE clause and vintage parameters for _extract_col_athena.
    # When date_filter emits a BETWEEN/IN clause (matching_dates present), it
    # already covers the full from/to range, so _date_bounds must NOT be
    # layered on top -- that produced `col >= X AND col <= Y AND col BETWEEN
    # X AND Y` duplication. Only fall back to _date_bounds when no
    # matching-date filter is active.
    extract_cfg = dict(tbl_cfg)
    orig_where = tbl_cfg.get('where', '')
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
        extract_cfg['where'] = f"({orig_where}) AND {date_bounds}" if orig_where else date_bounds

    bucket_desc = 'sample' if date_filter['filter_type'] == 'in_list' else effective_vintage
    dt_label = 'sample' if date_filter['filter_type'] == 'in_list' else None

    # Summary instead of full SQL preview
    where_summary = extract_cfg.get('where', '(none)').strip() or '(none)'
    print(f"  {len(non_date_cols)} columns, WHERE: {where_summary}")

    # Run extraction
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
              f"({os.path.join(outdir, '.cache.db')})")
    set_athena_cache(None)
    print(f"  SQL log: {log_path}")
    col_elapsed = time.time() - col_start_time
    col_end_ts = datetime.now().isoformat()

    if not all_stats:
        return None

    # Write CSV
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

    # Compute total sampled records
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


# ---------------------------------------------------------------------------
# Main extraction entry point
# ---------------------------------------------------------------------------

def extract_aws(config_path, outdir, types=None, max_workers=None, db_path=None, vintage=None, force=False,
                from_date=None, to_date=None):
    """
    Extract data directly from AWS Athena.

    Args:
        config_path: Path to extraction config JSON
        outdir: Directory to write CSV files
        types: List of types ("row", "col"). Default: both.
        max_workers: Max parallel workers for column extraction
        db_path: Optional database path to save discovered columns to _column_meta
        vintage: Date bucketing granularity (day, week, month, quarter, year)
        force: Unused (kept for backward compatibility)
        from_date: Start date for incremental extraction (YYYY-MM-DD)
        to_date: End date for incremental extraction (YYYY-MM-DD)
    """
    if types is None:
        types = ["row", "col"]

    mock_dir = os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ATHENA_MOCK')

    # Use load_unified_config so each side gets `name` auto-derived, and
    # hydrate col_map + col_filter from _table_pairs (where the UI persists
    # them). Without this, get_all_tables_from_unified sees empty col_map
    # and col_filter can't produce _selected_cols — CLI runs would emit all
    # columns even when the DB has include/exclude saved.
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

    # Stash --from-date / --to-date bounds separately so the col path can skip
    # them when it already emits a BETWEEN/IN filter from matching_dates
    # (otherwise we'd get `col >= X AND col <= Y AND col BETWEEN X AND Y`
    # redundancy, matching the oracle/hadoop fix).
    if from_date or to_date:
        for tbl in aws_tables:
            date_col = tbl.get('date_col', '')
            if not date_col:
                continue
            date_col_q = quote_ident(date_col, "athena")
            date_type = (tbl.get('date_type') or '').lower()
            bounds = []
            if from_date:
                lit = _format_athena_date_bound(from_date, date_type, is_upper=False)
                bounds.append(f"{date_col_q} >= {lit}")
            if to_date:
                lit = _format_athena_date_bound(to_date, date_type, is_upper=True)
                bounds.append(f"{date_col_q} <= {lit}")
            if bounds:
                tbl['_date_bounds'] = " AND ".join(bounds)
            tbl['_from_date'] = from_date
            tbl['_to_date'] = to_date
    # Mock mode: copy CSVs + write SQL files, then return
    if mock_dir:
        _extract_aws_mock_with_where(aws_tables, outdir, types, db_path, mock_dir)
        return

    try:
        import pyathena  # noqa: F401
    except ImportError:
        print("Error: pyathena is required for AWS extraction")
        print("Install with: pip install 'dtrack[aws]'")
        return

    if db_path and "col" in types:
        fill_columns_from_meta(aws_tables, db_path)

    # Track timing for all extractions
    timing_records = []

    # --- Row extraction: write SQL file then run it ---
    if "row" in types:
        _write_combined_sql(aws_tables, outdir, "row")
        sql_path = os.path.join(outdir, "extract_row.sql")
        row_results = run_sql_file(sql_path, outdir, max_workers=max_workers, db_path=db_path)
        for r in row_results:
            timing_records.append({
                'table': r['name'], 'step': 'row',
                'start': r['start'], 'end': '',
                'elapsed_sec': r['elapsed'],
            })

    # --- Col extraction: needs per-column logic, uses cursor directly ---
    if "col" in types:
        aws_creds_renew()
        conn = athena_connect(data_base=aws_tables[0].get('conn_macro'))
        cursor = conn.cursor()

        for tbl_cfg in aws_tables:
            try:
                result = _extract_cols_for_table(
                    tbl_cfg, outdir, max_workers=max_workers, db_path=db_path, vintage=vintage)
                if result:
                    timing_records.append(result)
            except Exception as e:
                print(f"  ERROR extracting columns for {tbl_cfg['name']}: {e}")
                print(f"  Skipping to next table...")
                timing_records.append({
                    'table': tbl_cfg['name'], 'step': 'col',
                    'start': '', 'end': '',
                    'elapsed_sec': 0, 'status': str(e),
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
        print(f"\nTiming log: {timing_path}")

    print(f"\nExtraction complete. Output in: {outdir}")


# ---------------------------------------------------------------------------
# SQL-file-based extraction (parse → run)
# ---------------------------------------------------------------------------

def parse_sql_file(sql_path):
    """Parse a combined SQL file into named query blocks.

    Returns list of dicts: [{"name": "aws_cust_daily", "sql": "SELECT ..."}]

    The file format uses '-- {qname}' as block markers.  Everything between
    two markers (or between a marker and EOF) is one query.  Header lines
    (starting with '-- dtrack' or '-- Generated') are skipped.
    """
    with open(sql_path, 'r', encoding='utf-8') as f:
        text = f.read()

    blocks = []
    current_name = None
    current_lines = []

    for line in text.split('\n'):
        stripped = line.strip()
        # Skip header comments
        if stripped.startswith('-- dtrack') or stripped.startswith('-- Generated'):
            continue
        # Block marker: "-- some_name" (single token after --)
        if stripped.startswith('-- ') and not stripped.startswith('-- ['):
            marker = stripped[3:].strip()
            # Only treat as a block marker if it looks like a qname (no spaces)
            if marker and ' ' not in marker:
                # Save previous block
                if current_name is not None:
                    sql = '\n'.join(current_lines).strip().rstrip(';').strip()
                    if sql:
                        blocks.append({"name": current_name, "sql": sql})
                current_name = marker
                current_lines = []
                continue
        current_lines.append(line)

    # Save last block
    if current_name is not None:
        sql = '\n'.join(current_lines).strip().rstrip(';').strip()
        if sql:
            blocks.append({"name": current_name, "sql": sql})

    return blocks


def run_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                 on_progress=None, resume=False):
    """Read extract_{type}.sql, parse into blocks, and execute each via Athena.

    Writes per-table CSVs:
      row type: {outdir}/{qname}_row.csv  (date_value, row_count)
      col type: {outdir}/{qname}_col.csv  (dt, column_name, col_type, n_total, ...)

    Supports parallel execution via ThreadPoolExecutor.

    Args:
        on_progress: Optional callback(result_dict) called after each query
                     completes. Useful for streaming progress to web/terminal.
        resume: If True (col mode only), skip blocks whose output is already
                present in the existing {qname}_col.csv. Preloaded rows are
                merged with newly-extracted rows on flush.

    Returns list of result dicts with timing info.
    """
    import sys
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    blocks = parse_sql_file(sql_path)
    if not blocks:
        print(f"No query blocks found in {sql_path}")
        return []

    # Enable per-outdir Athena cache. Located alongside the .sql file so
    # repeat runs of the same extract_*.sql replay locally instead of
    # re-submitting expensive scans. Delete .cache.db to invalidate.
    set_athena_cache(os.path.join(outdir, ".cache.db"))

    # Detect type from filename: extract_col.sql → col, else row
    basename = os.path.basename(sql_path)
    is_col = "extract_col" in basename
    file_suffix = "_col.csv" if is_col else "_row.csv"
    step_label = "col" if is_col else "row"

    os.makedirs(outdir, exist_ok=True)
    _max_workers = max_workers or int(os.environ.get('MAX_WORKERS', '4'))

    # Group blocks by table (qname) for per-table progress
    table_blocks = {}  # qname -> [block, ...]
    for block in blocks:
        qname = block['name'].split('/')[0] if '/' in block['name'] else block['name']
        table_blocks.setdefault(qname, []).append(block)

    _COL_HEADERS = [
        'dt', 'column_name', 'col_type', 'n_total', 'n_missing',
        'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
        # Coarse signals — emitted by both coarse and (planned) fine SQL,
        # NULL-tolerant when read back. Fine rows from older runs that
        # lacked these fields surface as empty strings and the compare
        # treats absent hash_sum as "unknown" (no diff flag).
        'length_avg', 'length_max', 'hash_sum',
    ]

    # For col mode, collect rows per table (blocks named "qname/col_name")
    _col_rows_lock = threading.Lock()
    _col_rows_by_table = {}  # qname -> list of row dicts

    # Resume: preload existing rows from prior successful runs, filter blocks.
    # A block "qname/col_name[/dt_label]" is skipped if the existing CSV has
    # >=1 row for that (column_name[, dt]) pair. The dt=='_all' suffix is a
    # trunc-mode placeholder (one query covers all buckets) — treat the block
    # as done if ANY row exists for col_name, since partial trunc output
    # cannot be distinguished from a clean run.
    n_skipped = 0
    if resume and is_col:
        preload_rows = {}  # qname -> list of row lists
        preload_seen = {}  # qname -> set of (col_name, dt)
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
                        seen.add((row[1], row[0]))  # (column_name, dt)
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
            # '_all' (trunc) or single-bucket/'all'/'sample': treat col as
            # done if any row for col_name exists.
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
        # Seed the in-memory buffer with existing rows so the final CSV flush
        # merges preloaded + newly-extracted rows.
        for qname, rows in preload_rows.items():
            _col_rows_by_table[qname] = list(rows)

        if not blocks:
            print(f"  [resume] nothing to re-run — all queries already completed",
                  file=sys.stderr)
            return []

    n_tables = len(table_blocks)
    msg = (f"  Parsed {len(blocks)} queries ({n_tables} tables) from {sql_path}, "
           f"workers={_max_workers}")
    if resume and n_skipped:
        msg += f" | resume: skipped {n_skipped} done queries"
    print(msg, file=sys.stderr)

    results = []
    _done_count = [0]  # mutable counter for threads
    _table_done = {}  # qname -> {done: int, total: int}
    _table_lock = threading.Lock()

    # Init per-table counters
    for qname, tbl_blocks in table_blocks.items():
        _table_done[qname] = {"done": 0, "total": len(tbl_blocks)}

    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        _tqdm = None

    # Console tqdm bars per table (stderr)
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

        try:
            cached = _cache_get(sql, None)
            if cached is not None:
                rows, col_names = cached
            else:
                aws_creds_renew()
                conn = athena_connect()
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description] if cursor.description else []
                cursor.close()
                conn.close()
                _cache_put(sql, None, (rows, col_names), row_count=len(rows))
            elapsed = time.time() - start_time

            if is_col:
                # Lowercase result-column names so per-row dict lookups match
                # _COL_HEADERS regardless of how pyathena/Athena cases the
                # SELECT-list alias in cursor.description. _query_athena does
                # the same on the pandas path; this aligns the gen-and-run
                # path with it. Without this, a single mismatched alias makes
                # the loaded CSV have an empty column → 0 after _safe_int.
                col_names_lc = [c.lower() for c in col_names]
                parsed_rows = []
                for row in rows:
                    row_dict = dict(zip(col_names_lc, row))
                    parsed_rows.append([row_dict.get(h, '') for h in _COL_HEADERS])
                with _col_rows_lock:
                    _col_rows_by_table.setdefault(qname, []).extend(parsed_rows)
            else:
                csv_path = os.path.join(outdir, f"{block_name}{file_suffix}")
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['date_value', 'row_count'])
                    for row in rows:
                        writer.writerow([row[0], row[1]])

            result = {"name": block_name, "ok": True, "rows": len(rows),
                      "elapsed": round(elapsed, 1), "start": start_ts}
            if not is_col:
                result["csv_path"] = os.path.join(outdir, f"{block_name}{file_suffix}")
        except Exception as e:
            elapsed = time.time() - start_time
            result = {"name": block_name, "ok": False, "error": str(e),
                      "elapsed": round(elapsed, 1), "start": start_ts}

        # Update counters
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

        # Flush this table's CSV as soon as its last query completes —
        # downstream Load Col can start picking up finished tables while
        # other tables are still being extracted.
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

        # Console tqdm update
        if qname in _tqdm_bars:
            _tqdm_bars[qname].update(1)
        elif not _tqdm_bars:
            # No tqdm — plain stderr
            status = "ok" if result["ok"] else f"FAIL: {result.get('error', '')}"
            print(f"  [{n}/{total}] {block_name}: {status} ({elapsed:.1f}s)", file=sys.stderr)

        # Web progress callback — include per-table progress
        if on_progress:
            result["done"] = n
            result["total"] = total
            result["table"] = qname
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

    # Close tqdm bars
    for bar in _tqdm_bars.values():
        bar.close()

    # For col mode the per-table CSVs are written as each table finishes
    # (see table_just_finished block above). Anything left in the buffer
    # here means its last query failed or the counters drifted — flush
    # whatever we have so the user isn't left with an empty CSV.
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

    # Write timing log
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
              f"({os.path.join(outdir, '.cache.db')})")
    set_athena_cache(None)

    return results


# ---------------------------------------------------------------------------
# Column discovery entry point
# ---------------------------------------------------------------------------

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

    all_tables = load_tables_from_config(config)

    aws_tables = [t for t in all_tables if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    # Mock mode: read from pre-built CSVs
    mock_dir = os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ATHENA_MOCK')
    if mock_dir:
        _extract_aws_mock(config_path, outdir, [], db_path, mock_dir)
        # discover-only just needs columns
        for tbl_cfg in aws_tables:
            name = tbl_cfg['name']
            qname = qualified_name(tbl_cfg)
            cols_src = os.path.join(mock_dir, f"{qname}_columns.csv")
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
                            # Strip whitespace from keys to handle \r\n line endings
                            row = {k.strip(): v for k, v in row.items()}
                            col_name = row.get('column_name') or row.get('COLUMN_NAME', '')
                            col_type = row.get('data_type') or row.get('DATA_TYPE', '')
                            if col_name:
                                columns[col_name] = col_type
                    if columns:
                        from ..db import insert_column_meta
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


# ---------------------------------------------------------------------------
# Convenience SQL builders
# ---------------------------------------------------------------------------

def build_continuous_sql_athena(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "athena")


def build_categorical_sql_athena(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "athena")


def build_top10_sql_athena(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "athena")


# ---------------------------------------------------------------------------
# AthenaBuilder class
# ---------------------------------------------------------------------------

class AthenaBuilder(PlatformBuilder):
    """Platform builder for direct AWS Athena queries."""

    def __init__(self, tbl_cfg, db_path=None):
        super().__init__(tbl_cfg, db_path)
        self.database = tbl_cfg.get('conn_macro', '')

    def build_row_sql(self, date_filter):
        """Build row count SQL for Athena."""
        raw_full_table = f"{self.database}.{self.table}"
        full_table, cte_prefix = resolve_table(self.tbl_cfg, raw_full_table)
        date_expr = self.date_col
        where = self.tbl_cfg.get('where', '')
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""{cte_prefix}
SELECT {date_expr} AS date_value, COUNT(*) AS row_count
FROM {full_table}
{where_clause}
GROUP BY {date_expr}"""
        return sql

    def build_continuous_sql(self, col, col_type, where):
        return build_continuous_sql_athena(self.table, col, self.date_col, where)

    def build_categorical_sql(self, col, col_type, where, top_n=10):
        return build_categorical_sql_athena(self.table, col, self.date_col, where)

    def generate_extraction(self, outdir, extract_type, **kw):
        """Generate Athena extraction (delegates to extract_aws)."""
        return extract_aws(
            config_path=kw.get('config_path'),
            outdir=outdir,
            types=[extract_type] if isinstance(extract_type, str) else extract_type,
            max_workers=kw.get('max_workers'),
            db_path=self.db_path,
            vintage=kw.get('vintage'),
            force=kw.get('force', False),
        )
