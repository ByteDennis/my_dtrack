"""Oracle platform builder: SAS code generation for Oracle SQL via passthrough."""

import hashlib
import json
import os
import threading
from typing import Optional


# Module-level lock around the SSL_CERT_FILE env mutation in hadoop_connect.
# Two threads calling hadoop_connect concurrently would otherwise race on
# the env pop/restore; the lock serializes the pop+connect+restore so a
# late thread can't restore the value before an earlier thread's connect
# is finished. Athena (boto) calls running in parallel can still observe
# the env briefly absent — that's the trade-off the recipe accepts.
_HADOOP_CONNECT_LOCK = threading.Lock()


def hadoop_connect(dsn: Optional[str] = None, *,
                   autocommit: bool = True):
    """Connect to Hadoop / Hive via pyodbc.

    DSN lookup order:
      1. explicit `dsn` kwarg
      2. `HADOOP_DSN` environment variable
      3. literal default `'Hive_Prod'`

    SSL_CERT_FILE is temporarily popped around the pyodbc.connect call
    because Cloudera/Hortonworks Hive ODBC drivers read the CA bundle
    from the DSN config; leaving SSL_CERT_FILE set (as boto3 expects)
    makes the driver pick the wrong cert and the TLS handshake fails
    with "SSL_connect: certificate verify failed". The original value
    is restored in `finally` so concurrent Athena work isn't disturbed
    once the handshake completes.

    Mock mode: when `DTRACK_MOCK` or `DTRACK_HADOOP_MOCK` is set, returns
    None so unit / integration tests don't need a live Hive cluster.
    Mirrors the `oracle_connect` mock contract.

    Raises pyodbc.InterfaceError with a richer message when the DSN
    isn't registered (the bare ODBC error "Data source name not found
    and no default driver specified" is opaque about WHERE the driver
    manager looked for the DSN).
    """
    if os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_HADOOP_MOCK'):
        print(f"[mock] Skipping Hadoop connection for DSN '{dsn or '?'}'")
        return None

    import pyodbc

    dsn = dsn or os.environ.get('HADOOP_DSN') or 'Hive_Prod'

    with _HADOOP_CONNECT_LOCK:
        saved = os.environ.pop('SSL_CERT_FILE', None)
        try:
            try:
                return pyodbc.connect(f"DSN={dsn}", autocommit=autocommit)
            except pyodbc.InterfaceError as exc:
                msg = str(exc)
                if 'IM002' in msg or 'Data source name not found' in msg:
                    # Surface what THIS Python actually sees so the user
                    # can compare against `python -c "import pyodbc; ...
                    # pyodbc.dataSources()"` from their shell. The bare
                    # ODBC text just says the DSN is missing and gives
                    # no hint that two Pythons might disagree.
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
        finally:
            if saved is not None:
                os.environ['SSL_CERT_FILE'] = saved

from .base import (
    PlatformBuilder,
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
    build_stats_sql,
    build_top10_sql,
    get_mock_dir,
    _extract_mock,
    _extract_mock_tables,
    set_cache_db,
    cache_get,
    cache_put,
)
from datetime import datetime


# ---------------------------------------------------------------------------
# Oracle date/vintage transforms
# ---------------------------------------------------------------------------

# Oracle -> SAS date transform mapping
# Oracle TRUNC format codes for vintage bucketing
_VINTAGE_TRUNC = {
    'day': None,       # no extra TRUNC needed (identity)
    'week': 'IW',
    'month': 'MM',
    'quarter': 'Q',
    'year': 'YYYY',
}


def _bool_from_value(v) -> Optional[bool]:
    """Loose bool parse: True/False, 1/0, '1'/'0', 'true'/'false'/'yes'/'no'.
    Returns None for None / empty / unrecognized strings (i.e. "not set")."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ('1', 'true', 'yes', 'on'):
        return True
    if s in ('0', 'false', 'no', 'off'):
        return False
    return None


def _approx_distinct_enabled(tbl_cfg=None, config=None) -> bool:
    """Resolve approx_distinct preference with the documented fallback chain.

    Order (first hit wins):
      1. tbl_cfg['approx_distinct']        -- per-pair-side override
      2. tbl_cfg['_pair_approx_distinct']  -- pair-level (set by gen_sas hydration)
      3. config['metadata']['approx_distinct']  -- global default in dtrack.json
      4. DTRACK_APPROX_DISTINCT env var
      5. False

    When True, gen-col swaps `COUNT(DISTINCT col)` for each platform's
    HyperLogLog-based approximate (`APPROX_COUNT_DISTINCT` on Oracle,
    `APPROX_DISTINCT` on Hive, `approx_distinct` on Trino/Athena).
    Off by default — exact counts are the safer choice for compliance-
    style validation. HLL has ~1-2% standard error at high cardinality
    and is exact for ≤256 unique values.
    """
    if tbl_cfg is not None:
        for key in ('approx_distinct', '_pair_approx_distinct'):
            v = _bool_from_value(tbl_cfg.get(key))
            if v is not None:
                return v
    if config is not None:
        v = _bool_from_value((config.get('metadata') or {}).get('approx_distinct'))
        if v is not None:
            return v
    return _bool_from_value(os.environ.get('DTRACK_APPROX_DISTINCT')) or False


def _approx_distinct_error() -> str:
    """Optional precision arg for Trino/Athena's `approx_distinct(col, e)`.

    Hive's `approx_distinct` doesn't accept this arg; Oracle's
    `APPROX_COUNT_DISTINCT` doesn't either. Only used in the Athena SQL
    builder. Default empty (use engine default ~2.3%).
    """
    return os.environ.get('DTRACK_APPROX_DISTINCT_ERROR', '').strip()


def _n_unique_expr(source: str, col: str, tbl_cfg=None, config=None) -> str:
    """Per-platform SQL fragment for n_unique. Honors the resolution chain
    in `_approx_distinct_enabled` (per-pair > global > env var > False).

    SAS-source and libname mode keep `COUNT(DISTINCT col)` regardless —
    SAS proc sql doesn't have an approx function, and SAS/ACCESS already
    pushes count-distinct down to the platform-native function over JDBC.

    `col` is expected to already be the sentinel-wrapped column expression
    (`apply_null_sentinels` output). Counting distinct on the wrapped form
    is what makes sentinel and real-null rows collapse to the same bucket.
    """
    s = (source or '').lower()
    if not _approx_distinct_enabled(tbl_cfg=tbl_cfg, config=config) or s in ('sas',):
        return f"COUNT(DISTINCT {col})"
    if s == 'oracle':
        return f"APPROX_COUNT_DISTINCT({col})"
    if s == 'hadoop':
        return f"APPROX_DISTINCT({col})"
    if s == 'aws':
        e = _approx_distinct_error()
        return f"approx_distinct({col}, {e})" if e else f"approx_distinct({col})"
    return f"COUNT(DISTINCT {col})"


def _col_expr_for_source(source: str, col_name: str, col_type: str,
                         col_dtype: str = None) -> str:
    """Build the sentinel-translated column expression for an Oracle/Hadoop
    passthrough SQL pulled from a SAS step.

    `col_name` is the bare DB identifier (e.g. `EVENT_DATE`); the SAS
    templates pre-quote nothing, so we hand back a raw `CASE WHEN ... END`.
    `col_type` is the dtrack tag ('numeric' / 'categorical').
    `col_dtype` is the raw DB type ('DATE', 'TIMESTAMP', 'VARCHAR2',
    'NUMBER', ...). When provided it takes precedence over col_type --
    the dtrack tag collapses DATE / TIMESTAMP / VARCHAR all into
    'categorical', but the sentinel family differs by underlying type:
      DATE / TIMESTAMP -> compare with DATE 'YYYY-MM-DD' literals
      VARCHAR / CHAR   -> compare with 'NA' / 'N/A' / '.' string literals
      NUMBER / INT     -> compare with -999 / -9999 numeric literals
    Without col_dtype, a DATE column tagged 'categorical' gets string
    sentinels and Oracle raises ORA-01858 trying to convert 'NA' into
    a date for the IN comparison (and Athena rejects with TYPE_MISMATCH
    at SQL compile time).
    """
    from .base import apply_null_sentinels
    # `source` maps to the dialect arg directly for oracle/hadoop.
    dialect = (source or "oracle").lower()
    if dialect not in ("oracle", "hadoop"):
        dialect = "oracle"
    # Raw dtype wins; fall back to high-level tag when load-columns
    # hasn't been run and we don't have it.
    effective_type = col_dtype or col_type
    return apply_null_sentinels(col_name, effective_type, dialect)


def _is_libname_table(tbl_cfg):
    """True if this table's per-side config opts into SAS LIBNAME mode.

    Per-pair-side `connect_method` field defaults to 'sql' (the existing
    PROC SQL CONNECT path). Setting it to 'libname' routes the table
    through the SAS-syntax pipeline: the libref `<conn_macro>` is mounted
    at the top of the script, and queries reference `<conn_macro>.<table>`
    using SAS SQL functions (datepart, intnx, count, ...) instead of
    Oracle TRUNC / Hive date_format.
    """
    return (tbl_cfg.get('connect_method') or 'sql').lower() == 'libname'


def _libname_mount_for(tbl_cfg):
    """Return the SAS LIBNAME statement to mount this table's source.

    Oracle: `libname <macro> oracle user="&iamusr" orapw="&<macro>_pwd" path="@<svc>";`
            (matches extraction.sas connect macro format).
    Hadoop: looks up Hadoop server/host/schema/port from HADOOP_LIBNAME_PROFILES
            keyed by `conn_macro` (falls back to 'default' profile).
    """
    macro = tbl_cfg.get('conn_macro', '')
    source = (tbl_cfg.get('source') or '').lower()

    if source == 'hadoop':
        from .. import constants as C
        profiles = getattr(C, 'HADOOP_LIBNAME_PROFILES', {}) or {}
        profile = profiles.get(macro) or profiles.get('default') or {}
        server = profile.get('server', '')
        host   = profile.get('host', '')
        schema = profile.get('schema', macro)
        port   = int(profile.get('port', 10000))
        ts     = profile.get('truststore') or '/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks'
        return (
            f'libname {macro} hadoop server="{server}"\n'
            f'    port={port} schema={schema} subprotocol=hive2 LOGIN_TIMEOUT=300\n'
            f"    uri='jdbc:hive2://{server}:{port}/usrf;"
            f"principal=hive/{server}@{host};ssl=true;sslTrustStore={ts};';"
        )

    # Oracle (and anything else falling back to oracle libname syntax)
    from ..db import resolve_oracle_macro
    svc = resolve_oracle_macro(macro) or macro
    return f'libname {macro} oracle user="&iamusr" orapw="&{macro}_pwd" path="@{svc}";'


def _oracle_date_transform(date_col, transform):
    """Return Oracle SQL expression for date transformation (used inside passthrough)."""
    if transform and "{col}" in transform:
        return transform.replace("{col}", date_col)
    if transform == "datetime_to_date":
        return f"TRUNC({date_col})"
    elif transform == "to_char":
        return f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
    return date_col


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

    # Fallback: emit an Oracle DATE literal in ISO format. A bare-string
    # fallback like '2026-04-01' relied on the session's NLS_DATE_FORMAT
    # accepting ISO, which most Oracle defaults (DD-MON-YY) do NOT — that
    # raised ORA-01861 ("literal does not match format string") when the
    # pair was missing/unrecognized date_type. DATE 'yyyy-mm-dd' is
    # syntactically explicit so it works regardless of session NLS.
    return f"DATE '{date_str}'"


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
    # `_selected_cols` semantics:
    #   None  → no col_filter set: pass-through, every mapped col extracted.
    #   []    → filter set, matched zero col_map entries: extract nothing
    #            (the earlier `if selected:` falsy-check silently fell through
    #             to all cols, which is why a non-matching col_filter was
    #             ignored on /col_gen).
    #   [...] → filter matched N entries: extract only those.
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

    # Auto-skip ID-like cols (cardinality >= threshold from prior load-col).
    # Populated by gen_sas via read_id_cols_from_col_stats. Cols are dropped
    # from extraction entirely; their cached _col_stats rows from the prior
    # run continue to satisfy compare.py downstream (use upsert mode on
    # load-col so previous rows aren't wiped).
    skip = tbl_cfg.get('_id_cols_to_skip')
    if skip:
        col_list = [(c, d) for c, d in col_list if c not in skip]

    # Fine-cell selection: when /api/generate carries `cells`, gen_sas stashes
    # `_fine_cells = {col_name: [vintage_labels]}` on the table. Drop cols
    # the user didn't pick. (Per-vintage filtering in SAS macros is harder
    # and we leave it for now — every selected col extracts every vintage
    # the table has, which is over-extraction but correct.)
    fine_cells = tbl_cfg.get('_fine_cells')
    if fine_cells:
        wanted = {c.lower() for c in fine_cells.keys()}
        col_list = [(c, d) for c, d in col_list if c.lower() in wanted]
    return col_list


def _discover_and_write_columns(all_tables, outdir, db_path):
    """Discover columns and write {qname}_columns.csv files.

    Per source:
      - aws    -> queries Athena via _discover_columns_athena
      - oracle -> queries Oracle via db.discover_columns (oracledb)
      - sas / hadoop -> still flow through the SAS file at extraction time
    Existing _column_meta rows win (no re-query); otherwise discover, insert,
    and write the CSV alongside the SQL files.
    """
    import csv as csv_mod
    from ..db import insert_column_meta, get_column_meta

    os.makedirs(outdir, exist_ok=True)
    aws_cursors = {}
    oracle_conns = {}

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
        else:
            # SAS / Hadoop -- columns discovered via generated SAS file
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
    for conn in oracle_conns.values():
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Mock extraction
# ---------------------------------------------------------------------------

def build_continuous_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "numeric", "oracle")


def build_categorical_sql_oracle(table, col, date_col, where=""):
    return build_stats_sql(table, col, date_col, where, "categorical", "oracle")


def build_top10_sql_oracle(table, col, date_col, where=""):
    return build_top10_sql(table, col, date_col, where, "oracle")


# ---------------------------------------------------------------------------
# HadoopBuilder class — SAS-based extraction for Hadoop/Hive (and, transitionally,
# Oracle until the direct Python path takes over). The class name reflects what
# this module is becoming after the Oracle-direct migration; SQL flavor is still
# selected per-table from `source` for any remaining Oracle-via-SAS callers.
# ---------------------------------------------------------------------------

class HadoopBuilder(PlatformBuilder):
    """Hadoop platform: generates SAS code for Hadoop/Hive via SAS passthrough."""

    def build_row_sql(self, date_filter):
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
        table, cte = resolve_table(self.tbl_cfg)
        date_expr = _oracle_date_transform(
            self.date_col,
            self.tbl_cfg.get('date_transform', ''),
        )
        return build_stats_sql(table, col, date_expr, where, "numeric", "oracle", cte_prefix=cte)

    def build_categorical_sql(self, col, col_type, where, top_n=10):
        table, cte = resolve_table(self.tbl_cfg)
        date_expr = _oracle_date_transform(
            self.date_col,
            self.tbl_cfg.get('date_transform', ''),
        )
        return build_stats_sql(table, col, date_expr, where, "categorical", "oracle", cte_prefix=cte)

    def generate_extraction(self, outdir, extract_type, **kw):
        """Generate SAS extraction files via gen_sas()."""
        config_path = kw.get('config_path')
        if not config_path:
            raise ValueError("config_path is required for SAS generation")

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


# ---------------------------------------------------------------------------
# Plain-Hive gen-first path (mirrors Oracle's _write_combined_*_sql_oracle).
# Emits extract_{row,col}.sql with /* @conn_macro=DSN */ markers; runner
# parses the file and dispatches via hadoop_connect.
# ---------------------------------------------------------------------------

def hadoop_sql_filename(extract_type):
    return f"extract_{extract_type}.sql"


def _hadoop_vintage_expr(tbl_cfg, date_col, vintage):
    """Hive expression that buckets `date_col` to the configured vintage
    and returns 'YYYY-MM-DD'. Falls back to the literal date_col when no
    truncation is needed."""
    src = (tbl_cfg.get('source') or 'hadoop').lower()
    date_type = tbl_cfg.get('date_type', 'string_dash')
    expr = _date_trunc_expr(src if src in ('hadoop', 'oracle') else 'hadoop',
                             date_col, date_type, vintage)
    return expr or date_col


def _build_col_sql_hadoop(tbl_cfg, col, dtype, full_table, cte_prefix="",
                           vintage=None, dt_label=None, coarse=False):
    """Thin wrapper over base.build_col_sql('hadoop', ...). The categorical
    WITH-chain template is shared with Oracle; per-dialect fragments
    (concat operator, cast-to-string, listagg-equivalent, missing/present
    conds, hash function) live in base._col_sql_dialect_ops('hadoop').

    coarse=True emits Phase-1 cheap-aggregate SQL (flat numeric, slim
    categorical chain) for the col_gen_rough page. Drops the freq-table-
    derived rank-weighted mean/std and top_10."""
    from .base import build_col_sql
    return build_col_sql('hadoop', tbl_cfg, col, dtype, full_table,
                          cte_prefix=cte_prefix, vintage=vintage,
                          dt_label=dt_label, coarse=coarse)


def _build_lifeboat_col_sql_hadoop(tbl_cfg, col, dtype, full_table,
                                    cte_prefix="", vintage=None, dt_label=None):
    """Thin wrapper over base.build_lifeboat_col_sql('hadoop', ...).
    Streaming-aggregate fallback used by the runner on Hive OOM-class
    errors. Keeps n_total / n_missing (single-pass), nulls everything
    that needs the freq table."""
    from .base import build_lifeboat_col_sql
    return build_lifeboat_col_sql('hadoop', tbl_cfg, col, dtype, full_table,
                                    cte_prefix=cte_prefix, vintage=vintage,
                                    dt_label=dt_label)


def _build_row_sql_hadoop(tbl_cfg, db_path=None, vintage=None):
    """Plain Hive row-count SQL for one table.

    Mirrors the old SAS template (`rows_hadoop.sas`) and Athena: emit the
    bare `date_col AS dt` with no normalization. Whatever native format
    Hive returns (string YYYYMMDD, string YYYY-MM-DD, native DATE, etc.)
    flows through to the row CSV; the row-compare side runs `parse_date`
    on each value to canonicalize. Optional `date_transform` from config
    wraps the column expression (matching the old SAS path's behavior).

    `conn_macro` is the Hive schema/database (Athena pattern) — used in
    the FROM clause, NOT as the ODBC DSN.
    """
    database = tbl_cfg.get('conn_macro', '') or ''
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    transform = tbl_cfg.get('date_transform', '')
    raw_full = f"{database}.{table}" if database else table
    full_table, cte_prefix = resolve_table(tbl_cfg, raw_full)

    # Layer the user's config WHERE with the global from/to bounds. The
    # web layer stashes from/to as `_date_bounds` (separate from `where`)
    # so the col gen can ignore them when the bucket BETWEEN already
    # constrains the scan. Row gen has no other date filter, so the
    # bounds need to be ANDed in here -- otherwise the row count covers
    # the whole table instead of the requested window.
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


def _write_combined_row_sql_hadoop(hadoop_tables, outdir, db_path=None, vintage=None):
    """extract_row.sql, one block per Hadoop table. Block format mirrors
    Oracle: `-- {qname}` / `/* @conn_macro=DSN */` / SQL / `;`."""
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


def _write_combined_col_sql_hadoop(hadoop_tables, outdir, db_path=None,
                                     vintage=None, coarse=False):
    """extract_col.sql for Hadoop, one block per (table, column, bucket).
    Mirrors `_write_combined_col_sql_oracle`. User-typed `_exclude_dates`
    from the pair card is honored here via NOT IN appended to each WHERE.

    coarse=True (col_gen_rough) emits the cheap-aggregate form (flat
    numeric, slim categorical chain — no rank-weighted mean/std, no
    top_10). Drops the freq-table-derived signals that bloat the SQL
    and cost the most on Hive."""
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
        # `conn_macro` is the Hive schema/database (Athena pattern), not the
        # ODBC DSN. The DSN is resolved from HADOOP_DSN env / 'Hive_Prod'
        # default inside hadoop_connect() at run time.
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
        # Honor the per-pair col_filter selection (set by the col_gen UI
        # via web/app.py hydration). Without this, the writer emits a
        # block per (table × column × bucket) for ALL mapped columns
        # even when the user only ticked a subset on the card -- the
        # AWS path already filters this way; missing it on Hadoop was
        # producing 12x the work the user actually asked for.
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
                # The bucket BETWEEN already constrains the scan to
                # this bucket's exact date range, so we don't also
                # AND in `_date_bounds` (the global from/to). Layering
                # both produces empty result sets when the row_compare
                # matching_dates were captured under different from/to
                # than what's currently set on the page (saw this in
                # the field: from=2026-05 + saved matching_dates=2022
                # generated `dt >= '20260501' AND dt BETWEEN '20220101'
                # AND '20220101'` which matched zero rows).
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
                    sql, _ = _build_col_sql_hadoop(
                        extract_cfg, col, dtype, full_table, cte_prefix,
                        vintage='all', dt_label=dt_label, coarse=coarse)
                    lifeboat_sql = _build_lifeboat_col_sql_hadoop(
                        extract_cfg, col, dtype, full_table, cte_prefix,
                        vintage='all', dt_label=dt_label)
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

    sql_path = os.path.join(outdir, hadoop_sql_filename("col"))
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(blocks))
    return sql_path


# ---------------------------------------------------------------------------
# Hadoop runner: parse extract_{row,col}.sql and execute via hadoop_connect.
# Mirrors oracle.run_oracle_sql_file (block format, lifeboat retry, per-table
# CSV flush).
# ---------------------------------------------------------------------------

def parse_hadoop_sql_file(sql_path):
    """Thin wrapper over base.parse_extract_sql_file. Block format is
    identical to Oracle so the parser lives in base.py."""
    from .base import parse_extract_sql_file
    return parse_extract_sql_file(sql_path)


def _query_hadoop(sql, conn_macro):
    """Execute SQL against Hive via hadoop_connect; return list[dict]
    (lowercased col names). Returns [] when DTRACK_MOCK is set or when
    the query produced no result set (some Hive ODBC drivers leave
    `cur.description` as None on empty rowsets — without the guard the
    list-comp below raises 'NoneType' object is not iterable, masking
    the real cause "the WHERE matched zero rows").

    Consults the shared SQL-result cache (set_cache_db) before submitting;
    cache key is SHA256(normalized SQL + conn_macro). On miss, opens a
    pyodbc connection, runs the query, lowercases column names, stores
    the rowset in cache, returns. Re-running the same `extract_*.sql`
    without changes therefore skips every block that already succeeded —
    matters most for partial / cancelled runs of large col extractions
    where pulling the freq table from Hive dominates wall time.

    `conn_macro` is the Hive schema/database (Athena pattern) — already
    baked into the FROM clause when the SQL was generated, so it is NOT
    used to look up an ODBC DSN here. The DSN comes from HADOOP_DSN env
    or the 'Hive_Prod' default inside hadoop_connect(), which also
    pops/restores SSL_CERT_FILE around pyodbc.connect (it's set by boto3
    for Athena and would otherwise make the Hive ODBC driver fail with
    'Data source name not found' / 'IM002' because the driver re-reads
    the wrong CA bundle and aborts the TLS handshake before DSN lookup).
    """
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


def run_hadoop_sql_file(sql_path, outdir, max_workers=None, db_path=None,
                        on_progress=None, resume=False, cancel_event=None):
    """Thin wrapper over base.run_extract_sql_file with platform='hadoop'.

    Wraps the call with the SQL-result cache lifecycle (set_cache_db on/off)
    so re-running the same extract_*.sql skips blocks already in
    .cache.db — matches Oracle's behavior. Especially useful for partial
    or cancelled runs of large col extractions (cancel + resume re-issues
    only the blocks that hadn't completed in the previous attempt).

    The runner skeleton (parse, dispatch, lifeboat retry, per-qname CSV
    flush, cancel handling, on_progress events) is shared with Oracle and
    lives in base.py; only the per-row query function and the platform
    config (worker env, lifeboat error patterns, degraded label) differ.
    """
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


# ---------------------------------------------------------------------------
# Back-compat re-exports from the SAS-template module. The legacy SAS
# code-generation path moved to platforms/hadoop_sas.py to shrink this
# file; external callers (cli.py, web/app.py, pipeline.py, tests) still
# import these names from platforms/hadoop, so re-export them here.
#
# NOTE: this import sits at the BOTTOM of the file so platforms/hadoop_sas
# can do `from . import hadoop` at function level without a circular
# import — by the time hadoop_sas is loaded here, all of hadoop's shared
# helpers (_oracle_date_transform, _format_date_bound, _date_trunc_expr,
# _apply_col_filter, _is_libname_table, etc.) are already defined.
# ---------------------------------------------------------------------------
from .hadoop_sas import gen_sas, _extract_sas_mock  # noqa: E402,F401
