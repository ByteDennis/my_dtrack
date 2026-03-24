"""AthenaBuilder: direct AWS Athena extraction platform."""

import csv
import json
import os
import threading

from .base import (
    PlatformBuilder,
    qualified_name,
    is_numeric_type,
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


# ---------------------------------------------------------------------------
# AWS credential management
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Athena connection
# ---------------------------------------------------------------------------

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
# Row extraction
# ---------------------------------------------------------------------------

def _extract_row_athena(cursor, tbl_cfg, outdir):
    """Extract row counts from Athena table. Returns (csv_path, elapsed_seconds)."""
    import time
    from datetime import datetime

    database = tbl_cfg['conn_macro']
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    qname = qualified_name(tbl_cfg)
    where = tbl_cfg.get('where', '')

    raw_full_table = f"{database}.{table}"
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full_table)
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


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

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
    where = tbl_cfg.get('where', '')
    if vintage is None:
        vintage = tbl_cfg.get('vintage', 'all')
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
        dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
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
    AVG(CAST({col} AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST({col} AS DOUBLE)) AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
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

    # Categorical: full CTE query
    if vintage == 'all':
        freq_dt = ""
        freq_group = ""
        freq_partition = "ORDER BY value_freq DESC, p_col ASC"
        agg_group = ""
        agg_dt_select = "'all' AS dt"
    else:
        vintage_transform = tbl_cfg.get('vintage_transform', None)
        dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
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
    COUNT(value_freq) AS n_unique,
    AVG(CAST(value_freq AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS std,
    MIN(value_freq) AS min_val,
    MAX(value_freq) AS max_val
FROM FreqTable{agg_group}"""

    _log_sql(col, sql)
    df = _query_athena(sql, data_base=database)

    results = []
    for _, rd in df.iterrows():
        rd = rd.to_dict()
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
            dt_expr = _vintage_date_expr_athena(date_col, vintage, vintage_transform, date_dtype=date_dtype, date_format=date_format)
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
            'mean': str(rd['mean']) if rd.get('mean') else '',
            'std': str(rd['std']) if rd.get('std') else '',
            'min_val': str(rd['min_val']) if rd.get('min_val') is not None else '',
            'max_val': str(rd['max_val']) if rd.get('max_val') is not None else '',
            'top_10': top_10,
        })
    return results


# ---------------------------------------------------------------------------
# Mock extraction
# ---------------------------------------------------------------------------

def _extract_aws_mock(config_path, outdir, types, db_path, mock_dir):
    from .oracle import _extract_mock
    return _extract_mock(config_path, outdir, types, db_path, mock_dir, ('aws',))


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
        force: Skip confirmation prompts
        from_date: Start date for incremental extraction (YYYY-MM-DD)
        to_date: End date for incremental extraction (YYYY-MM-DD)
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

    # Inject --from-date / --to-date bounds into WHERE clauses
    if from_date or to_date:
        for tbl in aws_tables:
            date_col = tbl.get('date_col', '')
            if not date_col:
                continue
            bounds = []
            if from_date:
                bounds.append(f"{date_col} >= '{from_date}'")
            if to_date:
                bounds.append(f"{date_col} <= '{to_date}'")
            extra = " AND ".join(bounds)
            existing = tbl.get('where', '').strip()
            tbl['where'] = f"({existing}) AND {extra}" if existing else extra
    if db_path and "col" in types:
        fill_columns_from_meta(aws_tables, db_path)

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
        full_table, cte_prefix = resolve_table(tbl_cfg, raw_full_table)

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

            # Compute unified date filter -- always from database matching dates
            table_vintage = tbl_cfg.get('vintage', vintage)
            date_filter = compute_date_filter(tbl_cfg, db_path, table_vintage)

            # Set up SQL log file for this table
            global _sql_log_file
            qname = qualified_name(tbl_cfg)
            log_path = os.path.join(outdir, f"{qname}_col.sql")
            with open(log_path, 'w', encoding='utf-8') as f:
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

            # Build WHERE clause and vintage parameters for _extract_col_athena
            extract_cfg = dict(tbl_cfg)
            orig_where = tbl_cfg.get('where', '')
            effective_vintage = date_filter.get('vintage', 'all')
            extract_date_dtype = date_filter.get('date_dtype')
            extract_date_format = date_filter.get('date_format')

            if date_filter['filter_type'] == 'between':
                # Single WHERE covering full range; GROUP BY handles bucketing
                from ..date_utils import bucket_date
                buckets = {}
                for dt in date_filter['dates']:
                    buckets.setdefault(bucket_date(dt, effective_vintage), []).append(dt)
                dw = build_date_between_clause(
                    date_col, date_filter['min_date'], date_filter['max_date'],
                    extract_date_dtype, date_format=date_filter.get('date_format'))
                extract_cfg['where'] = f"({orig_where}) AND {dw}" if orig_where else dw
                print(f"  {len(buckets)} vintage buckets ({effective_vintage}) -- GROUP BY")
            elif date_filter['filter_type'] == 'in_list':
                # Sample: IN list, literal dt_label
                dw = build_date_in_clause(
                    date_col, date_filter['dates'], extract_date_dtype,
                    date_format=date_filter.get('date_format'))
                extract_cfg['where'] = f"({orig_where}) AND {dw}" if orig_where else dw
                effective_vintage = 'all'  # no GROUP BY for sample
                extract_date_dtype = None

            bucket_desc = 'sample' if date_filter['filter_type'] == 'in_list' else effective_vintage
            dt_label = 'sample' if date_filter['filter_type'] == 'in_list' else None

            # Preview: show SQL for the first column, ask before executing anything
            if non_date_cols and not os.environ.get('DTRACK_NO_PREVIEW') and not force:
                preview_col, preview_dtype = non_date_cols[0]
                preview_numeric = is_numeric_type(preview_dtype, is_oracle=False)
                preview_wc = extract_cfg.get('where', '').strip() or "(1=1)"

                # Build dt_select / group_by for preview
                if dt_label is not None:
                    p_dt_select = f"'{dt_label}' AS dt"
                    p_group_by = ""
                elif effective_vintage == 'all':
                    p_dt_select = "'all' AS dt"
                    p_group_by = ""
                else:
                    p_vt = extract_cfg.get('vintage_transform', None)
                    p_dt_expr = _vintage_date_expr_athena(date_col, effective_vintage, p_vt, date_dtype=extract_date_dtype, date_format=extract_date_format)
                    p_dt_select = f"CAST({p_dt_expr} AS VARCHAR) AS dt"
                    p_group_by = f"\nGROUP BY {p_dt_expr}"

                if preview_numeric:
                    preview_sql = f"""{cte_prefix}
SELECT
    {p_dt_select},
    '{preview_dtype}' AS col_type,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({preview_col}) AS n_missing,
    COUNT(DISTINCT {preview_col}) AS n_unique,
    AVG(CAST({preview_col} AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST({preview_col} AS DOUBLE)) AS std,
    MIN({preview_col}) AS min_val,
    MAX({preview_col}) AS max_val
FROM {full_table} WHERE {preview_wc}{p_group_by}"""
                else:
                    # Categorical: FreqTable CTE with freq-based stats
                    if effective_vintage == 'all' or dt_label is not None:
                        p_freq_dt = ""
                        p_freq_group = ""
                        p_agg_group = ""
                        p_agg_dt = p_dt_select
                    else:
                        p_vt = extract_cfg.get('vintage_transform', None)
                        p_dt_expr = _vintage_date_expr_athena(date_col, effective_vintage, p_vt, date_dtype=extract_date_dtype, date_format=extract_date_format)
                        p_freq_dt = f"CAST({p_dt_expr} AS VARCHAR) AS dt,"
                        p_freq_group = f", {p_dt_expr}"
                        p_agg_group = "\nGROUP BY dt"
                        p_agg_dt = "dt"
                    preview_sql = f"""{cte_prefix}
WITH FreqTable AS (
    SELECT
        {p_freq_dt}
        {preview_col} AS p_col,
        COUNT(*) AS value_freq
    FROM {full_table} WHERE {preview_wc}
    GROUP BY {preview_col}{p_freq_group}
)
SELECT
    {p_agg_dt},
    '{preview_dtype}' AS col_type,
    SUM(value_freq) AS n_total,
    COUNT(value_freq) AS n_unique,
    AVG(CAST(value_freq AS DOUBLE)) AS mean,
    STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS std,
    MIN(value_freq) AS min_val,
    MAX(value_freq) AS max_val
FROM FreqTable{p_agg_group}"""

                print(f"\n  -- Preview SQL for: {preview_col} ({preview_dtype})")
                print(f"  {preview_sql.strip()}")
                print(f"  ;")
                print(f"\n  {len(non_date_cols)} columns total")
                resp = input("  Proceed? [y]es / [q]uit: ").strip().lower()
                if resp in ('q', 'quit', 'n', 'no'):
                    print("  Skipped col extraction.")
                    continue

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
            print(f"  SQL log: {log_path}")
            col_elapsed = time.time() - col_start_time
            col_end_ts = datetime.now().isoformat()

            # Write CSV
            if all_stats:
                qname = qualified_name(tbl_cfg)
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

                # Compute total sampled records (n_total summed across vintages for one column)
                first_col = all_stats[0]['column_name'] if all_stats else None
                total_records = 0
                if first_col:
                    for s in all_stats:
                        if s['column_name'] == first_col:
                            try:
                                total_records += int(float(s.get('n_total', 0) or 0))
                            except (ValueError, TypeError):
                                pass
                records_info = f", ~{total_records:,} records" if total_records else ""
                print(f"  Column stats: {csv_path} ({len(all_stats)} rows, {col_elapsed:.1f}s{records_info})")

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
        print(f"\nTiming log: {timing_path}")

    print(f"\nExtraction complete. Output in: {outdir}")


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
    mock_dir = os.environ.get('DTRACK_ATHENA_MOCK')
    if mock_dir:
        _extract_aws_mock(config_path, outdir, [], db_path, mock_dir)
        # discover-only just needs columns
        for tbl_cfg in aws_tables:
            name = tbl_cfg['name']
            qname = qualified_name(tbl_cfg)
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
