"""Base class and shared helpers for platform builders."""

import os
import re
import threading
from abc import ABC, abstractmethod


# ---------------------------------------------------------------------------
# Query result cache (shared across platforms).
#
# When _cache_db is set to a SQLite path, query runners on Athena/Oracle/...
# consult the file before submitting to the backend and write results back on
# success. Cache key = SHA256(normalized SQL + scope), where scope is the
# Athena database or Oracle conn_macro -- whatever distinguishes the backend
# the SQL was issued against.
#
# Disable via env var DTRACK_QUERY_CACHE=0 (or legacy DTRACK_ATHENA_CACHE=0).
# Invalidate by deleting the .cache.db file in the outdir.
# ---------------------------------------------------------------------------

_cache_db = None
_cache_lock = threading.Lock()
_cache_hits = 0
_cache_misses = 0
# When True, cache_get returns None unconditionally (forcing re-execution),
# but cache_put still writes -- so the stale entry gets overwritten by the
# fresh result. Toggled per-run from the web layer via set_cache_overwrite.
_cache_overwrite = False


def set_cache_overwrite(flag):
    """Force cache misses (re-execute and overwrite) while still persisting
    fresh results. Returns the previous value so callers can restore it.
    """
    global _cache_overwrite
    prev = _cache_overwrite
    _cache_overwrite = bool(flag)
    return prev


def set_cache_db(path):
    """Enable per-process query caching at the given .cache.db path.

    Creates the SQLite schema if missing. Subsequent cache_get/cache_put
    calls consult this file. Pass None to disable.
    """
    global _cache_db, _cache_hits, _cache_misses
    if (os.environ.get('DTRACK_QUERY_CACHE') == '0'
            or os.environ.get('DTRACK_ATHENA_CACHE') == '0'):
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
            CREATE TABLE IF NOT EXISTS query_cache (
                sql_hash TEXT PRIMARY KEY,
                sql_text TEXT,
                scope     TEXT,
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
    """Return (hits, misses) since the most recent set_cache_db call."""
    return _cache_hits, _cache_misses


def _cache_key(sql, scope):
    import hashlib
    norm = re.sub(r'\s+', ' ', sql.strip())
    raw = f"{norm}::{scope or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()


def cache_get(sql, scope):
    """Return the cached payload (whatever was stored) or None.

    `scope` is the backend identifier (Athena database, Oracle conn_macro,
    etc.) -- keeps entries from different backends from colliding.
    """
    global _cache_hits, _cache_misses
    if not _cache_db:
        return None
    if _cache_overwrite:
        with _cache_lock:
            _cache_misses += 1
        return None
    import sqlite3
    import pickle
    key = _cache_key(sql, scope)
    conn = sqlite3.connect(_cache_db)
    try:
        row = conn.execute(
            "SELECT payload FROM query_cache WHERE sql_hash = ?",
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


def cache_put(sql, scope, value, row_count=None):
    """Store value (pickled) under the (SQL, scope) key."""
    if not _cache_db:
        return
    import sqlite3
    import pickle
    from datetime import datetime as _dt
    key = _cache_key(sql, scope)
    blob = pickle.dumps(value)
    with _cache_lock:
        conn = sqlite3.connect(_cache_db)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO query_cache "
                "(sql_hash, sql_text, scope, row_count, created_at, payload) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (key, sql, scope or '', row_count, _dt.now().isoformat(), blob),
            )
            conn.commit()
        finally:
            conn.close()

# Oracle numeric types
ORACLE_NUMERIC_TYPES = {
    'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE',
    'INTEGER', 'INT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'DEC',
}

# Athena numeric types
ATHENA_NUMERIC_TYPES = {
    'int', 'integer', 'bigint', 'smallint', 'tinyint',
    'double', 'float', 'decimal', 'real',
}

# Date-type registry lives in constants.py (single source of truth for
# date_type -> format and date_type -> canonical dtype). Imported here
# so resolve_date_format() can apply both mappings to date_filter.
from ..constants import DATE_TYPE_FORMATS, DATE_TYPE_TO_CANONICAL_DTYPE


def resolve_date_format(date_filter, tbl_cfg):
    """Ensure date_filter has date_format AND date_dtype set.

    Two-step resolution, both honoring the user's tbl_cfg.date_type:
      1. date_format: only filled when missing (a value already set by
         load-columns wins). Falls back to DATE_TYPE_FORMATS[date_type].
      2. date_dtype: when tbl_cfg.date_type is provided, ALWAYS overrides
         the metadata-derived value. The user's explicit semantic config
         is authoritative -- this prevents Oracle ORA-01858 ("non-numeric
         character was found...") when the column is actually NUMBER /
         VARCHAR2 / YYYYMMDD but the metadata happens to say DATE.

    Call after compute_date_filter() and before building any SQL clause.
    """
    cfg_date_type = (tbl_cfg.get('date_type') or '').lower()
    if not date_filter.get('date_format'):
        if cfg_date_type in DATE_TYPE_FORMATS:
            date_filter['date_format'] = DATE_TYPE_FORMATS[cfg_date_type]
    if cfg_date_type in DATE_TYPE_TO_CANONICAL_DTYPE:
        date_filter['date_dtype'] = DATE_TYPE_TO_CANONICAL_DTYPE[cfg_date_type]


_NUMERIC_TYPE_SET = {t.upper() for t in ORACLE_NUMERIC_TYPES} | {
    t.upper() for t in ATHENA_NUMERIC_TYPES
}


def is_numeric_type(data_type, is_oracle=True):
    """Check if a data type string represents a numeric column.

    Unions Oracle and Hive/Athena numeric type names so this works regardless
    of which system produced the type label. Hadoop-source tables frequently
    carry Oracle-style labels like 'NUMBER' (because SAS proc contents over
    a Hive passthrough emits SAS type codes), and Oracle tables occasionally
    show 'BIGINT' via third-party meta loaders -- the union handles both.
    The is_oracle flag is kept for backwards compatibility and is no longer
    needed for correctness.
    """
    if not data_type:
        return False
    dt_upper = data_type.upper().split('(')[0].strip()
    return dt_upper in _NUMERIC_TYPE_SET


_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_ident(name, dialect="oracle"):
    """Wrap a SQL identifier in the dialect's quote char ONLY when the bare
    form is invalid (starts with a digit, contains spaces / hyphens / dots).

    - Athena / Trino / Oracle:        ANSI double quotes  →  "123col"
    - Hive / Hadoop / Databricks/Spark: backticks         →  `123col`
    - SAS:                            no change            (caller handles via macro var)

    Databricks/Spark uses backticks like Hive — ANSI double quotes are STRING
    literals there, so a digit-leading column like `2_amt` double-quoted would
    silently become the constant '2_amt' (and error in ANSI type checks).

    Conservative: leaves valid bare names unchanged so existing extractions
    keep their case-insensitive resolution against Oracle/Hive tables. Quoted
    identifiers are case-sensitive in Oracle and case-preserving in Athena —
    blindly quoting every column would break configs that store columns in
    one case and reference them in another.
    """
    if not name or _BARE_IDENT_RE.match(name):
        return name
    if dialect in ("hadoop", "hive", "databricks", "spark"):
        return f"`{name}`"
    return f'"{name}"'


def _sentinel_dialect_key(dialect):
    """Map SQL dialect string to NULL_SENTINELS top-level key."""
    d = (dialect or "").lower()
    if d in ("athena", "trino", "aws", "databricks", "spark"):
        return "aws"
    if d in ("hadoop", "hive"):
        return "hadoop"
    if d == "sas":
        return "sas"
    return "oracle"


def _is_numeric_col_type(col_type):
    """True if col_type label looks numeric (NUMBER/DOUBLE/INT/...).

    Accepts both the high-level dtrack tag ('numeric' / 'categorical') and
    raw DB type strings. Used by apply_null_sentinels() to pick the right
    sentinel list.
    """
    if not col_type:
        return False
    t = str(col_type).strip().lower()
    if t == "numeric":
        return True
    if t in ("categorical", "string", "char", "varchar", "text"):
        return False
    return is_numeric_type(t)


def _is_date_col_type(col_type):
    """True if col_type label looks like a date/timestamp."""
    if not col_type:
        return False
    t = str(col_type).strip().lower()
    return "date" in t or "time" in t


def _format_sentinel_literal(val, kind, dialect):
    """Render a sentinel value as a SQL literal for the given dialect.

    - numeric: bare number (no quotes)
    - string:  single-quoted, with embedded quotes doubled
    - date:    DATE 'YYYY-MM-DD' on Oracle/Hadoop/Athena; SAS uses 'YYYY-MM-DD'd
    """
    if kind == "numeric":
        return str(val)
    if kind == "string":
        esc = str(val).replace("'", "''")
        return f"'{esc}'"
    # date
    s = str(val)
    if dialect == "sas":
        return f"'{s}'d"
    return f"DATE '{s}'"


# >>> stat-key → emitted CSV fields, used by _mask_stats_in_rows <<< #
_STAT_KEY_FIELDS = {
    'n_total':   ('n_total',),
    'n_missing': ('n_missing',),
    'n_unique':  ('n_unique',),
    'min_max':   ('min_val', 'max_val'),
    'length':    ('length_avg', 'length_max'),
    'hash_sum':  ('hash_sum',),
    'top_10':    ('top_10',),
}


# >>> blank stat fields that preset/stats_override disabled <<< #
def _mask_stats_in_rows(rows, preset=None, stats_override=None):
    if preset is None and not stats_override:
        return
    from ..constants import STATS_PRESETS
    base = STATS_PRESETS[preset or 'detailed']
    effective = {k: bool((stats_override or {}).get(k, base[k])) for k in base}
    blank_fields = []
    for key, on in effective.items():
        if not on:
            blank_fields.extend(_STAT_KEY_FIELDS.get(key, ()))
    if not blank_fields:
        return
    for row in rows:
        for f in blank_fields:
            if f in row:
                row[f] = ''


def apply_null_sentinels(col_ref, col_type, dialect, sentinels=None,
                         include_blank=True):
    """Wrap a SQL column reference with sentinel-to-NULL translation.

    Returns a SQL expression equivalent to:
        CASE WHEN col IN (<sentinels>) [OR TRIM(col)='']
             THEN NULL ELSE col END

    The wrapped expression is what stats SQL should feed to MIN/MAX/AVG/
    COUNT(DISTINCT)/LENGTH/etc. so sentinel rows (-999 for missing, 9999-12-31
    for "infinity", etc.) collapse into NULL and don't skew the result.

    Args:
        col_ref: the dialect-quoted column reference, e.g. `"col_name"` or
            `&_col_name` for SAS-templated SQL.
        col_type: tag indicating numeric / categorical / date so we pick the
            right sentinel list. Accepts dtrack tags ('numeric',
            'categorical') or raw DB types ('NUMBER', 'VARCHAR2', 'DATE').
        dialect: 'oracle', 'hadoop' (Hive), 'athena', 'sas'. Selects literal
            formatting + sentinel set.
        sentinels: optional explicit sentinel list; when None, falls back to
            constants.NULL_SENTINELS for the dialect/type combo.
        include_blank: also map empty / whitespace-only strings to NULL on
            string columns. Aligned with SAS "is missing" semantics on
            Athena/Hive where '' isn't equivalent to NULL.

    No-ops (returns col_ref unchanged) when no sentinels apply.
    """
    from ..constants import NULL_SENTINELS

    is_numeric = _is_numeric_col_type(col_type)
    is_date = _is_date_col_type(col_type)
    kind = "numeric" if is_numeric else ("date" if is_date else "string")

    if sentinels is None:
        key = _sentinel_dialect_key(dialect)
        sentinels = NULL_SENTINELS.get(key, {}).get(kind, []) or []

    literals = [_format_sentinel_literal(v, kind, dialect) for v in sentinels]

    conds = []
    if literals:
        conds.append(f"{col_ref} IN ({', '.join(literals)})")
    if include_blank and kind == "string" and dialect != "oracle":
        # Oracle collapses '' to NULL natively, so the IS NULL check the
        # caller emits already covers blank. Athena/Hive/SAS distinguish
        # '' from NULL and need the TRIM check to align.
        conds.append(f"TRIM({col_ref}) = ''")

    if not conds:
        return col_ref

    return f"CASE WHEN {' OR '.join(conds)} THEN NULL ELSE {col_ref} END"


def qualified_name(tbl_cfg):
    """Derive storage/file name: {source}_{name}."""
    source = tbl_cfg.get('source', '')
    name = tbl_cfg['name']
    return f"{source}_{name}" if source else name


def sas_safe_name(name, max_len=18):
    """Make `name` safe for use as a SAS identifier (dataset / variable name).

    SAS identifier rules:
      - max 32 characters (callers may pass smaller max to leave room for a
        prefix like 'cache._cs_');
      - must start with a letter or underscore;
      - body may contain letters, digits, underscores only.

    Non-conforming characters are replaced with '_' and a leading digit
    gets an underscore prefix. After sanitization the name is truncated.
    Without this, SAS raises "invalid data set name" at runtime for any
    qname that happened to contain '.', '-', or other special characters,
    or for a qname that sits right at the length boundary.
    """
    if not name:
        return '_'
    safe = re.sub(r'[^A-Za-z0-9_]', '_', str(name))
    if safe[0].isdigit():
        safe = '_' + safe
    return safe[:max_len]


def resolve_table(tbl_cfg, full_table=None):
    """Resolve table reference, wrapping with CTE if 'processed' is configured.

    For SAS tables (source=sas), processed is setup statements, not a CTE.
    Returns (table_ref, cte_prefix).
    """
    if is_sas_table(tbl_cfg):
        # SAS: table = conn_macro.table, no CTE
        conn = tbl_cfg.get('conn_macro', '')
        table = tbl_cfg.get('table', '')
        return f"{conn}.{table}" if conn else table, ""

    processed = tbl_cfg.get('processed')
    table = full_table or tbl_cfg['table']
    if not processed:
        return table, ""
    if isinstance(processed, list):
        processed = "\n".join(processed)
    # Prefix the CTE alias so it can't collide with the real table name.
    # When `processed` references the base table (common pattern:
    # `SELECT * FROM T WHERE eff_dt = (SELECT MAX(eff_dt) FROM T)`) and the
    # CTE is named `T`, Oracle reads the inner `FROM T` as a self-reference
    # and fails with ORA-32039 (recursive WITH requires column alias list).
    alias = f"cte_{tbl_cfg['name']}"
    cte = f"WITH {alias} AS (\n{processed}\n)\n"
    return alias, cte


def is_sas_table(tbl_cfg):
    """Check if table config is a local SAS dataset (source=sas)."""
    return tbl_cfg.get('source', '').lower() == 'sas'


def reformat_date(d, date_format):
    """Reformat a YYYY-MM-DD date string to the target date_format.

    Step 2 of adding a new date format: add a strftime pattern to fmt_map below.
    See DATE_TYPE_FORMATS comment block above for full instructions.
    """
    from datetime import datetime as _dt
    if not date_format or date_format == 'YYYY-MM-DD':
        return str(d)
    try:
        dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
    except ValueError:
        return str(d)
    # Format label → Python strftime pattern
    # To add a new format: add 'LABEL': '%pattern' here
    fmt_map = {
        'YYYYMMDD':    '%Y%m%d',       # 20260115
        'DDMONYYYY':   '%d%b%Y',       # 15JAN2026
        'DD-MON-YYYY': '%d-%b-%Y',     # 15-JAN-2026
        'MM/DD/YYYY':  '%m/%d/%Y',     # 01/15/2026
        'YYYYMM':      '%Y%m',         # 202601
    }
    py_fmt = fmt_map.get(date_format)
    if py_fmt:
        return dt_obj.strftime(py_fmt).upper() if 'MON' in date_format else dt_obj.strftime(py_fmt)
    return str(d)


def build_date_in_clause(date_col, dates, date_dtype, is_sas=False, date_format=None,
                         custom_date_types=None):
    """Build WHERE fragment for date IN (...) based on column type.

    Splits into multiple IN groups for Oracle's 1000-item limit.
    Accepts optional custom_date_types dict for extensible type handling.
    """
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    # Check custom date types first
    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            formatted = [reformat_date(d, fmt) for d in dates]
        elif cat == 'date':
            formatted = [f"DATE '{d}'" for d in dates]
        else:  # string
            formatted = [f"'{reformat_date(d, fmt)}'" for d in dates]
        col_expr = date_col
    elif dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        # Reformat to the column's integer storage format (e.g. YYYYMMDD)
        # so the IN list emits as `col IN (20260505, 20260506, ...)` not
        # `col IN (2026-05-05, ...)` -- the bare-string fallback was
        # silently invalid SQL on NUMBER columns.
        formatted = [reformat_date(d, date_format) if date_format else str(d)
                     for d in dates]
        col_expr = date_col
    elif is_string:
        formatted = [f"'{reformat_date(d, date_format)}'" for d in dates]
        col_expr = date_col
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype or dtype == 'TIME')
        if is_sas:
            from datetime import datetime as _dt
            formatted = []
            for d in dates:
                try:
                    dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
                    formatted.append(f"'{dt_obj.strftime('%d%b%Y').upper()}'d")
                except ValueError:
                    formatted.append(f"'{d}'")
            col_expr = f"datepart({date_col})" if is_datetime else date_col
        else:
            formatted = [f"DATE '{d}'" for d in dates]
            col_expr = f"TRUNC({date_col})" if is_datetime else date_col
    else:
        # Unknown / empty dtype — happens when _column_meta hasn't been
        # populated for this table yet. Bare-string literals here triggered
        # ORA-01861 ("literal does not match format string") on Oracle when
        # the session NLS_DATE_FORMAT differs from YYYY-MM-DD. Emit an ISO
        # DATE literal for the non-SAS path (Oracle) so it parses regardless
        # of session NLS; for SAS-source, fall back to plain strings since
        # SAS evaluates the comparison locally against its own typed date.
        if is_sas:
            formatted = [f"'{reformat_date(d, date_format)}'" for d in dates]
        else:
            formatted = [f"DATE '{d}'" for d in dates]
        col_expr = date_col

    chunks = [formatted[i:i+999] for i in range(0, len(formatted), 999)]
    parts = [f"{col_expr} IN ({', '.join(chunk)})" for chunk in chunks]
    if len(parts) == 1:
        return parts[0]
    return "(" + " OR ".join(parts) + ")"


def build_date_between_clause(date_col, min_date, max_date, date_dtype, is_sas=False, date_format=None,
                              custom_date_types=None):
    """Build WHERE fragment using BETWEEN based on column type.

    Accepts optional custom_date_types dict for extensible type handling.
    """
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    # Check custom date types first
    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            fmt_min = reformat_date(min_date, fmt)
            fmt_max = reformat_date(max_date, fmt)
            return f"{date_col} BETWEEN {fmt_min} AND {fmt_max}"
        elif cat == 'date':
            return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
        else:  # string
            fmt_min = reformat_date(min_date, fmt)
            fmt_max = reformat_date(max_date, fmt)
            return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"

    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        # Reformat min/max from canonical YYYY-MM-DD to the column's
        # stored integer format (e.g. YYYYMMDD -> 20260505, YYYYMM ->
        # 202605). Without this, the bare string '2026-05-05' would be
        # spliced as `col BETWEEN 2026-05-05 AND ...` -- invalid SQL
        # (Oracle tries to parse it as numeric arithmetic and raises
        # ORA-01858).
        fmt_min = reformat_date(min_date, date_format) if date_format else str(min_date)
        fmt_max = reformat_date(max_date, date_format) if date_format else str(max_date)
        return f"{date_col} BETWEEN {fmt_min} AND {fmt_max}"
    elif is_string:
        fmt_min = reformat_date(min_date, date_format)
        fmt_max = reformat_date(max_date, date_format)
        return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"
    elif 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype or dtype == 'TIME')
        if is_sas:
            from datetime import datetime as _dt
            try:
                d_min = _dt.strptime(str(min_date), "%Y-%m-%d")
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d")
                sas_min = f"'{d_min.strftime('%d%b%Y').upper()}'d"
                sas_max = f"'{d_max.strftime('%d%b%Y').upper()}'d"
                col_expr = f"datepart({date_col})" if is_datetime else date_col
                return f"{col_expr} BETWEEN {sas_min} AND {sas_max}"
            except ValueError:
                return f"{date_col} BETWEEN '{min_date}' AND '{max_date}'"
        # Non-SAS path. For datetime/timestamp columns we want to capture
        # every row that falls on any day in [min, max], including rows
        # with non-zero time parts on the upper day. Two ways to write
        # that:
        #   (a) TRUNC(col) BETWEEN DATE 'min' AND DATE 'max'
        #   (b) col >= DATE 'min' AND col < DATE 'max+1day'
        # We use (b). Reasons:
        #   - (a) wraps the column in a function (TRUNC), which prevents
        #     the optimizer from using an index on `col`. (b) keeps the
        #     column bare on the LHS, transformation lives entirely on
        #     the RHS literal.
        #   - TRUNC() is Oracle-specific; Athena/Trino doesn't have it
        #     (would need date_trunc('day', col)). (b) is portable.
        #   - (a) at upper bound = DATE 'max' = max 00:00:00, so rows
        #     after midnight on the upper day still pass via TRUNC --
        #     but (b)'s `< max+1day` captures them too, including
        #     anything in the last sub-second.
        # For plain DATE columns (no time, e.g. Athena DATE), the
        # half-open range still works identically.
        if is_datetime:
            from datetime import datetime as _dt, timedelta as _td
            try:
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d") + _td(days=1)
                max_next = d_max.strftime("%Y-%m-%d")
                return (f"{date_col} >= DATE '{min_date}' "
                        f"AND {date_col} < DATE '{max_next}'")
            except ValueError:
                # Fall back to inclusive BETWEEN if max_date isn't ISO --
                # the date helper upstream should canonicalize, but stay
                # defensive.
                return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
        return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
    else:
        # Unknown / empty dtype — usually means _column_meta isn't loaded
        # for this table. Bare-string literals here triggered ORA-01861
        # ("literal does not match format string") on Oracle when the
        # session NLS_DATE_FORMAT differs from YYYY-MM-DD. Emit ISO DATE
        # literals for the non-SAS path so it parses regardless of NLS.
        if is_sas:
            fmt_min = reformat_date(min_date, date_format)
            fmt_max = reformat_date(max_date, date_format)
            return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"
        return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"


def format_date_bounds_literals(min_date, max_date, date_dtype, is_sas=False,
                                date_format=None, custom_date_types=None):
    """Return (from_lit, to_lit): the platform-formatted date literals that
    build_date_between_clause would splice into a BETWEEN. Exposed so driver
    datasets can carry the raw literal strings for inspection (useful when
    cross-checking Oracle/Hadoop/SAS date formatting before running).
    """
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            return reformat_date(min_date, fmt), reformat_date(max_date, fmt)
        if cat == 'date':
            return f"DATE '{min_date}'", f"DATE '{max_date}'"
        return f"'{reformat_date(min_date, fmt)}'", f"'{reformat_date(max_date, fmt)}'"

    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
        fmt_min = reformat_date(min_date, date_format) if date_format else str(min_date)
        fmt_max = reformat_date(max_date, date_format) if date_format else str(max_date)
        return fmt_min, fmt_max
    if is_string:
        return (f"'{reformat_date(min_date, date_format)}'",
                f"'{reformat_date(max_date, date_format)}'")
    if 'TIMESTAMP' in dtype or 'DATE' in dtype or 'TIME' in dtype:
        is_datetime = ('TIMESTAMP' in dtype or 'DATETIME' in dtype or dtype == 'TIME')
        if is_sas:
            from datetime import datetime as _dt
            try:
                d_min = _dt.strptime(str(min_date), "%Y-%m-%d")
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d")
                return (f"'{d_min.strftime('%d%b%Y').upper()}'d",
                        f"'{d_max.strftime('%d%b%Y').upper()}'d")
            except ValueError:
                return f"'{min_date}'", f"'{max_date}'"
        if is_datetime:
            return f"DATE '{min_date}'", f"DATE '{max_date}'"
        return f"DATE '{min_date}'", f"DATE '{max_date}'"
    # Unknown / empty dtype — emit ISO DATE literal for non-SAS so Oracle
    # doesn't try to implicit-convert a bare string against NLS_DATE_FORMAT.
    if is_sas:
        return (f"'{reformat_date(min_date, date_format)}'",
                f"'{reformat_date(max_date, date_format)}'")
    return f"DATE '{min_date}'", f"DATE '{max_date}'"


def build_date_range_with_gaps(date_col, matching_dates, date_dtype,
                               is_sas=False, date_format=None, custom_date_types=None):
    """Build a BETWEEN clause covering [min(matching_dates), max(matching_dates)].

    User-typed excludes go through a separate `_exclude_dates` /
    `exclude_clause` plumbed by the per-platform col-gen drivers, not through
    here. Row_comparison-derived auto-excludes were removed: they conflated
    both-zero days with real diffs and bloated NOT IN lists; the user-typed
    field is now the only source of NOT IN exclusions.
    """
    bmin, bmax = min(matching_dates), max(matching_dates)
    return build_date_between_clause(
        date_col, bmin, bmax, date_dtype,
        is_sas=is_sas, date_format=date_format,
        custom_date_types=custom_date_types,
    )


def compute_date_filter(tbl_cfg, db_path, vintage):
    """Compute date filter once in Python for both SAS and Athena paths.

    Returns dict with vintage, filter_type, min_date, max_date, dates, etc.
    """
    qname = qualified_name(tbl_cfg)

    result = {
        'vintage': vintage,
        'filter_type': 'none',
        'min_date': None,
        'max_date': None,
        'dates': [],
        'n_matching': 0,
        'n_buckets': 0,
        'date_dtype': None,
        'date_format': None,
    }

    # Always read the date column's dtype + format from the DB if we can —
    # downstream WHERE-clause builders need it to emit the right literal
    # form (DATE 'yyyy-mm-dd' vs '20260401' vs an integer). Previously this
    # was skipped when vintage='all' (the early-return), which made
    # build_date_between_clause fall through to the bare-string fallback
    # and triggered ORA-01861 on Oracle when the session NLS_DATE_FORMAT
    # didn't accept ISO. Only the matching-dates lookup needs to skip when
    # vintage='all', since 'all' means "no per-bucket filter".
    if db_path:
        from ..db import get_column_meta, get_metadata
        date_col = tbl_cfg.get('date_col', '')
        try:
            for cm in (get_column_meta(db_path, qname) or []):
                if cm['column_name'].upper() == date_col.upper():
                    result['date_dtype'] = (cm.get('data_type') or '').upper() or None
                    break
        except Exception:
            pass  # _column_meta absent / read failed — leave date_dtype None
        try:
            meta = get_metadata(db_path, qname)
            if meta:
                result['date_format'] = meta.get('date_format')
        except Exception:
            pass  # _metadata absent — leave date_format None

    if vintage == 'all' or not db_path:
        print(f"  [date filter] vintage: {vintage} | no date filter applied "
              f"(date_dtype={result['date_dtype']!r})")
        return result

    is_sample, n_sample = _parse_sample_vintage(vintage)
    if is_sample:
        result['vintage'] = 'sample'

    from ..db import list_table_pairs, get_row_comparison, get_column_meta, get_metadata

    # Look up matching dates
    pairs = list_table_pairs(db_path)
    pair_name = next(
        (p['pair_name'] for p in pairs
         if qname in (p['table_left'], p['table_right'])),
        None
    )
    matching_dates = []
    if pair_name:
        comp = get_row_comparison(db_path, pair_name)
        if comp:
            if comp.get('matching_dates'):
                matching_dates = comp['matching_dates']
            # Carry the row_comparison overlap window for callers that want
            # it for display/diagnostics. The row-compare-derived
            # `excluded_dates` is intentionally NOT propagated here -- the
            # only NOT IN source is the user-typed `_exclude_dates` from
            # the pair card, plumbed by each platform's col-gen driver.
            result['overlap_start'] = comp.get('overlap_start')
            result['overlap_end']   = comp.get('overlap_end')

    # Intersect matching_dates with the user's from/to window. The web
    # layer stashes the resolved per-pair (or global fallback) from/to as
    # `_from_date` / `_to_date`. Without this clip, the bucket emission
    # covers every historical matching_date saved in `_row_comparison`
    # (which can span years) instead of the requested window. Symptom:
    # user sets from=2026-05-01 to=2026-05-09 expecting ~2 weekly buckets
    # but the col gen produces 181 (every week from 2022 to 2026 because
    # row-compare was last run over the full overlap range).
    from_date = tbl_cfg.get('_from_date')
    to_date   = tbl_cfg.get('_to_date')
    if matching_dates and (from_date or to_date):
        before_n = len(matching_dates)
        clipped = [
            d for d in matching_dates
            if (not from_date or d >= from_date)
            and (not to_date or d <= to_date)
        ]
        if len(clipped) != before_n:
            print(f"  [date filter] clipping {before_n} matching_dates to "
                  f"[{from_date or 'min'}..{to_date or 'max'}] -> {len(clipped)} dates")
        matching_dates = clipped

    # Sample dates if sample@N vintage
    if is_sample and matching_dates:
        matching_dates = _sample_matching_dates(db_path, tbl_cfg, matching_dates, n_sample)
        result['filter_type'] = 'in_list'
        result['min_date'] = min(matching_dates)
        result['max_date'] = max(matching_dates)
        result['n_matching'] = len(matching_dates)
        result['n_buckets'] = 1
        result['dates'] = matching_dates
        print(f"  [date filter] {vintage} | {len(matching_dates)} sampled dates | range: {result['min_date']} to {result['max_date']}")
        return result

    # Table has a WHERE clause (from where_map or direct config "where")
    has_where = tbl_cfg.get('_where_from_config') or bool(tbl_cfg.get('where', '').strip())

    if has_where or matching_dates:
        if matching_dates:
            from ..date_utils import bucket_date
            buckets = {}
            for dt in matching_dates:
                buckets.setdefault(bucket_date(dt, vintage), []).append(dt)
            result['filter_type'] = 'between'
            result['min_date'] = min(matching_dates)
            result['max_date'] = max(matching_dates)
            result['n_matching'] = len(matching_dates)
            result['n_buckets'] = len(buckets)
            result['dates'] = matching_dates
            print(f"  [date filter] vintage: {vintage} | range: {result['min_date']} to {result['max_date']} | {len(matching_dates)} dates -> {len(buckets)} buckets")
        else:
            print(f"  [date filter] vintage: {vintage} | no matching dates found")
        return result

    if not matching_dates:
        print(f"  [date filter] no matching dates for {qname}, skipping filter")
        return result

    from ..date_utils import bucket_date
    buckets = {}
    for dt in matching_dates:
        buckets.setdefault(bucket_date(dt, vintage), []).append(dt)

    result['filter_type'] = 'between'
    result['min_date'] = min(matching_dates)
    result['max_date'] = max(matching_dates)
    result['n_matching'] = len(matching_dates)
    result['n_buckets'] = len(buckets)
    result['dates'] = matching_dates

    print(f"  [date filter] vintage: {vintage} | range: {result['min_date']} to {result['max_date']} | {len(matching_dates)} dates -> {len(buckets)} buckets")
    return result


def _parse_sample_vintage(vintage_str):
    """Parse 'sample@N' into (is_sample, n_sample)."""
    if not vintage_str or not vintage_str.startswith('sample@'):
        return False, 0
    try:
        return True, int(vintage_str.split('@', 1)[1])
    except ValueError:
        return False, 0


def _sample_matching_dates(db_path, tbl_cfg, matching_dates, n_sample=100):
    """Sample N dates from matching_dates with reproducible seed."""
    import random
    from ..db import get_sampled_dates, save_sampled_dates, list_table_pairs

    seed = int(os.environ.get('SEED', '2025'))
    qname = qualified_name(tbl_cfg)

    if len(matching_dates) <= n_sample:
        print(f"  {qname}: {len(matching_dates)} dates <= sample@{n_sample}, using all")
        return matching_dates

    pairs = list_table_pairs(db_path)
    pair_name = None
    for pair in pairs:
        if qname in (pair['table_left'], pair['table_right']):
            pair_name = pair['pair_name']
            break
    if not pair_name:
        return matching_dates

    existing_samples = get_sampled_dates(db_path, pair_name, qname)
    random.seed(seed)
    new_samples = sorted(random.sample(matching_dates, n_sample))

    if existing_samples and set(existing_samples) == set(new_samples):
        print(f"  {qname}: using existing {len(existing_samples)} sampled dates (seed={seed})")
        return existing_samples

    if existing_samples:
        print(f"  {qname}: scope changed, saving new sample (seed={seed}, sample@{n_sample})")
    else:
        print(f"  {qname}: sampling {n_sample} from {len(matching_dates)} matching dates (seed={seed})")
    save_sampled_dates(db_path, pair_name, qname, new_samples)
    return new_samples


def _merge_cte_chain(cte_prefix, new_ctes):
    """Splice extra CTEs onto an existing 'WITH ... ' prefix (or start one).

    new_ctes is a list of (alias, body_sql) tuples. body_sql is the inner
    select (no surrounding parens). Returns a 'WITH ... ' string that can
    be prefixed directly to a SELECT.
    """
    new_clauses = [f"{name} AS (\n{body.strip()}\n)" for name, body in new_ctes]
    if not cte_prefix.strip():
        return "WITH " + ",\n".join(new_clauses) + "\n"
    existing = cte_prefix.rstrip()
    if existing.upper().lstrip().startswith("WITH "):
        return existing + ",\n" + ",\n".join(new_clauses) + "\n"
    return existing + "\nWITH " + ",\n".join(new_clauses) + "\n"


def build_stats_sql(table, col, date_col, where="", col_type="numeric", dialect="oracle", cte_prefix=""):
    # NOTE: Uses explicit column references — never SELECT *
    """Build column statistics SQL for Oracle or Athena.

    For categorical, mean/std/min/max are *category-aware* (rank-weighted by
    UPPER(cat) ordering) so any permutation of counts across categories
    produces different stats. min_val/max_val are formatted "{cat}={count}".

    Identifiers (col / date_col) are auto-quoted via `quote_ident` only when
    the bare form is invalid SQL (e.g. starts with a digit). The literal
    column-name string baked into the SELECT (`'{col}' AS column_name`)
    keeps the unquoted name — it's the output value, not a reference.
    """
    where_clause = f"AND {where}" if where else ""
    is_athena = dialect == "athena"
    # Reference forms (used in COUNT/AVG/etc.). Original `col` / `date_col`
    # stay as-is for the output-name literal in the SELECT.
    col_ref = quote_ident(col, dialect)
    date_col_ref = quote_ident(date_col, dialect)
    # Wrap the column reference with the sentinel-to-NULL translation so
    # missing-value sentinels (-999, '9999-12-31', 'NA', ...) collapse to
    # NULL before any aggregate sees them. After this wrap n_missing /
    # n_unique / min / max / mean / std all agree across platforms even
    # when one side persists sentinels and the other uses real NULLs.
    col_expr = apply_null_sentinels(col_ref, col_type, dialect)

    if col_type == "numeric":
        # Numeric n_missing = total - non-null after sentinel translation.
        missing = f"COUNT(*) - COUNT({col_expr})"
        avg_expr = f"AVG(CAST({col_expr} AS DOUBLE))" if is_athena else f"AVG({col_expr})"
        std_expr = f"STDDEV(CAST({col_expr} AS DOUBLE))" if is_athena else f"STDDEV({col_expr})"
        return cte_prefix + f"""
SELECT {date_col_ref} AS dt, '{col}' AS column_name, 'numeric' AS col_type,
    COUNT(*) AS n_total, {missing} AS n_missing, COUNT(DISTINCT {col_expr}) AS n_unique,
    {avg_expr} AS mean, {std_expr} AS std,
    MIN({col_expr}) AS min_val, MAX({col_expr}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col_ref}""".strip()

    # Categorical branch: rank-weighted stats based on alpha (UPPER) ordering.
    # col_expr already wraps the column with sentinel-to-NULL translation,
    # so 'NA' / '#N/A' / '.' etc. collapse before the freq table groups them.
    cast_expr = f"CAST({col_expr} AS VARCHAR)" if is_athena else f"TO_CHAR({col_expr})"
    cnt_to_str = "CAST(cnt AS VARCHAR)" if is_athena else "cnt"
    # Use DOUBLE on Athena to avoid BIGINT overflow on cnt*rnk*rnk.
    cnt_num = "CAST(cnt AS DOUBLE)" if is_athena else "cnt"
    # Missing semantics, post-sentinel: count NULL on every platform. We
    # also fold blank/whitespace strings into "missing" on engines that
    # distinguish '' from NULL (Athena/Hive/SAS) — Oracle collapses '' to
    # NULL natively so the extra TRIM check is a no-op there.
    if is_athena:
        present_cond = "p_col IS NOT NULL AND TRIM(p_col) <> ''"
        missing_cond = "p_col IS NULL OR TRIM(p_col) = ''"
    else:
        present_cond = "p_col IS NOT NULL"
        missing_cond = "p_col IS NULL"

    new_ctes = [
        ("_dt_freq", f"""
    SELECT {date_col_ref} AS dt, {cast_expr} AS p_col, COUNT(*) AS cnt
    FROM {table}
    WHERE 1=1 {where_clause}
    GROUP BY {date_col_ref}, {col_ref}"""),
        ("_dt_ranked", f"""
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(p_col), p_col) AS rnk,
           COUNT(*) OVER (PARTITION BY dt) AS k
    FROM _dt_freq WHERE {present_cond}"""),
        ("_dt_agg", f"""
    SELECT dt,
           SUM({cnt_num}) AS sum_cnt,
           SUM({cnt_num} * rnk) AS sum_cnt_rnk,
           SUM({cnt_num} * rnk * rnk) AS sum_cnt_rnk2
    FROM _dt_ranked GROUP BY dt"""),
        ("_dt_stats", """
    SELECT dt,
           sum_cnt_rnk * 1.0 / NULLIF(sum_cnt, 0) AS mean_v,
           CASE WHEN sum_cnt > 0 THEN
               SQRT(GREATEST(sum_cnt_rnk2 * 1.0 / sum_cnt
                             - (sum_cnt_rnk * 1.0 / sum_cnt)
                             * (sum_cnt_rnk * 1.0 / sum_cnt), 0))
           END AS std_v
    FROM _dt_agg"""),
        # min_v = alpha-first p_col, max_v = alpha-last p_col, encoded as
        # "p_col=cnt". Sorted by val (alphabetically); freq is just for display.
        ("_dt_minmax", f"""
    SELECT dt,
           MAX(CASE WHEN rnk = 1 THEN p_col || '=' || {cnt_to_str} END) AS min_v,
           MAX(CASE WHEN rnk = k THEN p_col || '=' || {cnt_to_str} END) AS max_v
    FROM _dt_ranked GROUP BY dt"""),
        ("_dt_totals", f"""
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN {missing_cond} THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN {present_cond} THEN 1 ELSE 0 END) AS n_unique
    FROM _dt_freq GROUP BY dt"""),
    ]

    head = _merge_cte_chain(cte_prefix, new_ctes)
    return head + f"""
SELECT t.dt AS dt, '{col}' AS column_name, 'categorical' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       s.mean_v AS mean, s.std_v AS std,
       m.min_v AS min_val, m.max_v AS max_val
FROM _dt_totals t
LEFT JOIN _dt_stats s ON s.dt = t.dt
LEFT JOIN _dt_minmax m ON m.dt = t.dt""".rstrip()


def build_top10_sql(table, col, date_col, where="", dialect="oracle"):
    # NOTE: Uses explicit column references — never SELECT *
    """Build top 10 frequency SQL for Oracle or Athena.

    Auto-quotes col / date_col when the bare name is invalid SQL (e.g. a
    column starting with a digit). See `quote_ident` for the rules.
    """
    where_clause = f"AND {where}" if where else ""
    alias = " t" if dialect == "athena" else ""
    col_ref = quote_ident(col, dialect)
    date_col_ref = quote_ident(date_col, dialect)
    # Sentinel-translated column reference — keeps sentinel rows out of
    # the top-10. Type defaults to 'categorical' since top-10 only ever
    # runs on categorical columns; the string sentinel set is correct.
    col_expr = apply_null_sentinels(col_ref, "categorical", dialect)
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col_ref} AS dt, CAST({col_expr} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col_ref} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col_expr} IS NOT NULL {where_clause}
    GROUP BY {date_col_ref}, {col_expr}
){alias} WHERE rn <= 10""".strip()


def parse_stats_row(row):
    """Normalize a query result row to standard dict format (all strings)."""
    return {
        'dt': str(row['dt']),
        'column_name': row['column_name'],
        'col_type': row['col_type'],
        'n_total': str(row['n_total']) if row['n_total'] is not None else '0',
        'n_missing': str(row['n_missing']) if row['n_missing'] is not None else '0',
        'n_unique': str(row['n_unique']) if row['n_unique'] is not None else '0',
        'mean': str(row['mean']) if row['mean'] is not None else '',
        'std': str(row['std']) if row['std'] is not None else '',
        'min_val': str(row['min_val']) if row['min_val'] is not None else '',
        'max_val': str(row['max_val']) if row['max_val'] is not None else '',
    }


def read_id_cols_from_col_stats(db_path, table_name, threshold=0.99, ttl_days=1.0):
    """Read cardinality from _col_stats and return the set of cols whose
    mean(n_unique/n_total) across dts >= threshold.

    Used by gen-sas to auto-skip ID-like columns on subsequent runs without
    needing to query Oracle/Hadoop directly. Cache freshness is gated on
    `_metadata.last_loaded` for the table — older than ttl_days → returns
    empty set (treated as "no cache").

    First-run behaviour: _col_stats is empty for a never-extracted table, so
    this returns empty set and gen-sas emits the full per-col extraction
    (current behaviour). After the first successful load-col, subsequent
    gen-sas calls skip ID cols.

    Returns set() (never None) so callers can `tbl.get('_id_cols_to_skip', set())`.
    Set DTRACK_ID_DETECTION=0 to disable entirely.
    """
    import os as _os
    if _os.environ.get('DTRACK_ID_DETECTION', '1') == '0':
        return set()
    if not db_path or not _os.path.exists(db_path):
        return set()
    import sqlite3
    from datetime import datetime, timedelta
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # TTL gate via _metadata.last_loaded — same column dtrack already
        # stamps when load-col / load-row writes data. ttl_days=0 means
        # "always treat as expired"; pass None to skip the gate entirely.
        if ttl_days is not None:
            try:
                cur.execute(
                    "SELECT last_loaded FROM _metadata WHERE table_name = ?",
                    (table_name,),
                )
                mrow = cur.fetchone()
                if mrow and mrow[0]:
                    try:
                        last = datetime.fromisoformat(mrow[0])
                    except ValueError:
                        last = None
                    if last and (datetime.now() - last) > timedelta(days=ttl_days):
                        conn.close()
                        return set()
            except sqlite3.OperationalError:
                pass  # _metadata may lack last_loaded on older DBs
        cur.execute(
            "SELECT column_name, n_total, n_unique FROM _col_stats "
            "WHERE source_table = ? "
            "  AND n_total IS NOT NULL AND n_total <> '' "
            "  AND n_unique IS NOT NULL AND n_unique <> ''",
            (table_name,),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return set()
    if not rows:
        return set()
    by_col = {}
    for col, n_total, n_unique in rows:
        try:
            t = float(n_total)
            u = float(n_unique)
        except (ValueError, TypeError):
            continue
        if t <= 0:
            continue
        by_col.setdefault(col, []).append(u / t)
    return {col for col, rs in by_col.items()
            if rs and (sum(rs) / len(rs)) >= threshold}


# ---------------------------------------------------------------------------
# Cross-engine categorical hash / length helpers
#
# These are the Python reference implementations of the categorical-coarse
# signals (length_avg, length_max, hash_sum). The Oracle and Athena SQL
# builders emit expressions that reproduce these formulas on each engine.
# Tests assert that the SQL strings call the right primitives so the two
# engines produce identical output on identical data.
#
# Recipe (must match across engines):
#   trim    = strip ASCII space only (the SQL TRIM default; non-ASCII
#             whitespace stays in the value on both engines)
#   utf8    = encode as UTF-8 bytes
#   md5     = MD5 of the UTF-8 bytes (32 hex chars)
#   hash16  = int(md5[:4], 16)   in [0, 65535]
#   length  = number of Unicode characters in the trimmed value
#             (NOT bytes -- both engines must count characters)
# ---------------------------------------------------------------------------

# Max-length cap applied by ALL engines to the freq table's p_col before
# computing length_avg / length_max / hash_sum / min_val / max_val. Picked
# to match Hadoop's `column_hadoop.sas` which truncates via
# `SUBSTR(CAST(col AS STRING), 1, 200)` -- the value SAS already produces
# in shipped data. Oracle (`SUBSTR(TO_CHAR(col), 1, 200)`) and Athena
# (`SUBSTR(CAST(col AS VARCHAR), 1, 200)`) apply the same cap so all three
# engines hash/measure the same truncated bytes.
#
# Why 200: keeps cross-engine compares meaningful on long-text columns
# while matching the existing Hadoop output. p_col is just the key for
# freq bucketing -- 200 chars is plenty to distinguish categorical values
# in practice; pathological >200-char values land in a single
# truncation-collision bucket on every engine.
P_COL_MAX_LEN = 200


def categorical_truncate(value):
    """Apply the shared p_col truncation (matches SUBSTR(..., 1, 4000))."""
    if value is None:
        return None
    s = str(value)
    return s[:P_COL_MAX_LEN]


def categorical_trim(value):
    """ASCII-space TRIM matching SQL TRIM(value) default.

    NB: both Oracle TRIM and Trino/Athena TRIM default to ASCII space only.
    Non-ASCII whitespace (NBSP \\u00a0, tab, etc.) is intentionally left in
    place by both engines, so this helper does the same.
    """
    if value is None:
        return None
    return str(value).strip(' ')


def categorical_length(value):
    """Character length of the trimmed value, or None if value is None.

    Counts Unicode code points -- matches Trino LENGTH() and Oracle
    LENGTH() on a UTF-8 database (NLS_CHARACTERSET=AL32UTF8). Both
    engines therefore report the same number for 'café' (= 4), '🎉' (= 1),
    etc., regardless of underlying byte width.
    """
    if value is None:
        return None
    return len(categorical_trim(value))


def categorical_hash16(value):
    """16-bit categorical hash matching the cross-engine SQL recipe.

    Engine SQL emits the equivalent of int(md5(utf8(trim(v)))[:4], 16) so
    SUM(cnt * hash16) is comparable across Oracle / Athena / Hadoop on the
    same data. Returns None for NULL input.
    """
    import hashlib
    if value is None:
        return None
    raw = categorical_trim(value).encode('utf-8')
    return int(hashlib.md5(raw).hexdigest()[:4], 16)


def categorical_aggregates(freq_rows):
    """Compute length_avg / length_max / hash_sum for a list of (p_col, cnt).

    The reference implementation the SQL builders are tested against.
    Mirrors the SQL behavior of the categorical CTE chain:
      - SUBSTR(..., 1, P_COL_MAX_LEN) truncation before measurement / hashing
      - NULL p_col rows excluded from length and hash (they only count
        toward n_missing)
      - SUM(cnt * X) with cnt-weighting
    """
    truncated = [(categorical_truncate(p), c) for p, c in freq_rows]
    non_null = [(p, c) for p, c in truncated if p is not None]
    if not non_null:
        return {'length_avg': None, 'length_max': None, 'hash_sum': 0}
    sum_cnt = sum(c for _, c in non_null)
    sum_cnt_len = sum(c * categorical_length(p) for p, c in non_null)
    max_len = max(categorical_length(p) for p, _ in non_null)
    sum_hash = sum(c * categorical_hash16(p) for p, c in non_null)
    return {
        'length_avg': sum_cnt_len / sum_cnt if sum_cnt > 0 else None,
        'length_max': max_len,
        'hash_sum': sum_hash,
    }


def get_mock_dir():
    """Return mock directory from DTRACK_MOCK (preferred) or legacy env vars."""
    return os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ORACLE_MOCK')


def _extract_mock(config_path, outdir, types, db_path, mock_dir, source_filter):
    """Extract from mock CSV files instead of real database.

    Mock directory uses flat layout: {qname}_row.csv, {qname}_col.csv
    where qname = {source}_{name} (from qualified_name()).

    Args:
        source_filter: Source type(s) to filter, e.g. ('oracle', 'sas') or ('aws',)
    """
    import json
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


# >>> copy mock CSVs for a pre-built list of table configs (+ per-table timing) <<< #
def _extract_mock_tables(tables, outdir, types, mock_dir):
    import shutil
    from datetime import datetime
    os.makedirs(outdir, exist_ok=True)

    timing_records = []
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
            start = datetime.now()
            if os.path.exists(src):
                shutil.copy2(src, dst)
                with open(dst) as f:
                    n_rows = sum(1 for _ in f) - 1
                label_str = "Row counts" if typ == "row" else "Column stats"
                unit = "dates" if typ == "row" else "rows"
                print(f"  [mock] {label_str}: {dst} ({n_rows} {unit})")
                status = ''
            else:
                print(f"  [mock] File not found: {src}")
                status = 'mock file not found'
            timing_records.append({
                'table': tbl_cfg.get('name', qname), 'step': typ,
                'start': start.strftime('%Y-%m-%d %H:%M:%S'), 'end': '',
                'elapsed_sec': round((datetime.now() - start).total_seconds(), 3),
                'status': status,
            })

    write_timing_csv(outdir, timing_records, tables)
    print(f"\n[mock] Extraction complete. Output in: {outdir}")


# >>> rename a just-generated extract_{type}.sql to extract_{type}.{platform}.sql so platforms sharing one dir don't clobber (review only) <<< #
def tag_generated_sql(outdir, extract_type, platform):
    src = os.path.join(outdir, f"extract_{extract_type}.sql")
    if not os.path.exists(src):
        return None
    dst = os.path.join(outdir, f"extract_{extract_type}.{platform}.sql")
    os.replace(src, dst)
    print(f"[gen-only] {dst}")
    return dst


# >>> append timing rows to per-table {outdir}/{qname}_timing.csv (row + col logs accumulate) <<< #
def write_timing_csv(outdir, records, tables=None):
    if not records:
        return None
    import csv
    name2qname = {t.get('name', ''): qualified_name(t) for t in (tables or [])}
    fields = ['step', 'start', 'end', 'elapsed_sec', 'status']
    by_table = {}
    for r in records:
        by_table.setdefault(r.get('table', ''), []).append(r)
    written = []
    for tname, recs in by_table.items():
        qname = name2qname.get(tname) or tname
        timing_path = os.path.join(outdir, f"{qname}_timing.csv")
        is_new = not os.path.exists(timing_path)
        with open(timing_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            if is_new:
                writer.writeheader()
            for r in recs:
                writer.writerow({k: r.get(k, '') for k in fields})
        print(f"Timing log: {timing_path}")
        written.append(timing_path)
    return written


def load_tables_from_config(config):
    """Load tables from config (unified format only)."""
    if "pairs" in config and isinstance(config["pairs"], dict):
        from ..config import get_all_tables_from_unified
        return get_all_tables_from_unified(config)
    return config.get('tables', [])


def fill_columns_from_meta(tables, db_path):
    """Fill in columns from _column_meta for tables missing columns."""
    from ..db import get_column_meta
    for tbl in tables:
        if tbl.get('columns'):
            continue
        qn = qualified_name(tbl)
        meta = get_column_meta(db_path, qn)
        if not meta:
            meta = get_column_meta(db_path, tbl['name'])
        if not meta:
            meta = get_column_meta(db_path, tbl['table'])
        if meta:
            all_cols = {m['column_name']: m['data_type'] for m in meta}
            allowed = tbl.get('_col_map_columns')
            if allowed:
                tbl['columns'] = {c: t for c, t in all_cols.items() if c in allowed}
                print(f"  {tbl['name']}: {len(tbl['columns'])}/{len(all_cols)} columns (filtered by col_map)")
            else:
                tbl['columns'] = all_cols
                print(f"  {tbl['name']}: loaded {len(all_cols)} columns from _column_meta")


def inject_where_from_config(tables, config):
    """Inject where_map filters from pair config into table configs."""
    if "pairs" not in config:
        return
    for tbl in tables:
        tbl_table = tbl.get('table', '')
        for pair_name in tbl.get('_pairs', []):
            pair_cfg = config['pairs'].get(pair_name, {})
            where_map = pair_cfg.get('where_map', {})
            if not where_map:
                continue
            side = 'left' if pair_cfg.get('left', {}).get('table') == tbl_table else 'right'
            where_clause = where_map.get(side, '')
            if where_clause:
                tbl['where'] = where_clause
                tbl['_where_from_config'] = True
                print(f"  {qualified_name(tbl)}: using where_map[{side}] from config")
                break


def match_columns_from_dicts(left_cols, right_cols, left_label="left", right_label="right", outfile=None):
    """Compare two column dictionaries and match by case-insensitive name."""
    import json

    left_lower = {k.lower(): k for k in left_cols}
    right_lower = {k.lower(): k for k in right_cols}

    matched = {}
    left_only = []
    right_only = []

    for lower_name, left_name in sorted(left_lower.items()):
        if lower_name in right_lower:
            right_name = right_lower[lower_name]
            matched[left_name] = right_name
        else:
            left_only.append({"name": left_name, "type": left_cols[left_name]})

    for lower_name, right_name in sorted(right_lower.items()):
        if lower_name not in left_lower:
            right_only.append({"name": right_name, "type": right_cols[right_name]})

    result = {
        "matched": matched,
        f"{left_label}_only": left_only,
        f"{right_label}_only": right_only,
        "manual_mapping": {},
    }

    if outfile:
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"\nWritten to: {outfile}")

    return result


# ---------------------------------------------------------------------------
# Shared extract-SQL utilities. The block-file format and the manual-exclude
# AND'ing pattern are identical across Oracle / Hadoop / (and the gen-first
# Athena path), so the parser and the helper live here once.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Dialect-parameterized column-stats SQL builders. The categorical CTE chain
# (freq → ranked → totals → agg → sstats → minmax → top_n → lenhash) is
# structurally identical across dialects; only a small set of fragments
# differ (string concat operator, cast-to-string syntax, listagg-equivalent,
# NVL/COALESCE, hash function, NULL cast literals). Each dialect supplies
# those fragments via _col_sql_dialect_ops; the rest of the SQL is composed
# in build_col_sql / build_lifeboat_col_sql.
#
# The two Oracle and Hadoop per-platform builders that used to live in
# their respective modules are now thin wrappers calling these helpers.
# ---------------------------------------------------------------------------

def _col_sql_dialect_ops(dialect):
    """Returns a dict of dialect-specific SQL fragments used by build_col_sql.

    To add a new dialect: provide all the same keys. Helpers that take or
    return SQL fragments are callables so the caller can compose them.
    """
    if dialect == 'oracle':
        # Imports inside the branch so base.py doesn't depend on oracle.py
        # at import time (oracle.py imports from base.py).
        from .oracle import _vintage_date_expr_oracle, _oracle_portable_hash16
        return {
            'wc_empty':           '(1=1)',
            'is_oracle_typed':    True,
            'cast_str':           lambda x: f'TO_CHAR({x})',
            'concat':             lambda *parts: ' || '.join(parts),
            'top10_coalesce':     lambda x, d: f'NVL({x}, {d})',
            'mean_inner':         lambda x: f'AVG({x})',
            'std_inner':          lambda x: f'STDDEV_SAMP({x})',
            'mean_outer':         lambda x: x,
            'std_outer':          lambda x: x,
            'minmax_strcast':     lambda x: f'TO_CHAR({x})',
            # Lifeboat NULL casts. Oracle: NUMBER for n_unique/mean/std,
            # VARCHAR2(1) for min/max. Hadoop has different per-field types
            # (see hadoop branch). Three keys map cleanly.
            'lifeboat_null_count':    'CAST(NULL AS NUMBER)',
            'lifeboat_null_meanstd':  'CAST(NULL AS NUMBER)',
            'lifeboat_null_minmax':   'CAST(NULL AS VARCHAR2(1))',
            'top10_listagg':      lambda item: (
                f"LISTAGG({item}, '; ')\n"
                f"               WITHIN GROUP (ORDER BY cnt DESC, p_col ASC)"
            ),
            'hash_sum_expr':      lambda p_col: f'SUM(cnt * {_oracle_portable_hash16(p_col)})',
            'cat_present':        'p_col IS NOT NULL',
            'cat_missing':        'p_col IS NULL',
            'final_order_by':     '\nORDER BY t.dt',
            'vintage_expr':       lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format: (
                _vintage_date_expr_oracle(date_col_q, vintage, date_dtype, date_format)
            ),
            'dt_select_wrap':     lambda dt_expr: f"TO_CHAR({dt_expr}, 'YYYY-MM-DD')",
        }
    if dialect in ('hadoop', 'databricks'):
        from .hadoop import _hadoop_vintage_expr
        ops = {
            'wc_empty':           '1=1',
            'is_oracle_typed':    False,
            'cast_str':           lambda x: f'CAST({x} AS STRING)',
            'concat':             lambda *parts: f'concat({", ".join(parts)})',
            'top10_coalesce':     lambda x, d: f'COALESCE({x}, {d})',
            'mean_inner':         lambda x: f'AVG(CAST({x} AS DOUBLE))',
            'std_inner':          lambda x: f'STDDEV_SAMP(CAST({x} AS DOUBLE))',
            'mean_outer':         lambda x: f'CAST({x} AS STRING)',
            'std_outer':          lambda x: f'CAST({x} AS STRING)',
            'minmax_strcast':     lambda x: f'CAST({x} AS STRING)',
            # Hadoop: BIGINT for n_unique only; mean/std/min/max all STRING.
            'lifeboat_null_count':    'CAST(NULL AS BIGINT)',
            'lifeboat_null_meanstd':  'CAST(NULL AS STRING)',
            'lifeboat_null_minmax':   'CAST(NULL AS STRING)',
            'top10_listagg':      lambda item: (
                f"concat_ws('; ', collect_list({item}))"
            ),
            # Cross-engine-portable hash16: int(md5(utf8(trim(v)))[:4], 16),
            # same formula as Oracle's _oracle_portable_hash16 and Athena's
            # _athena_portable_hash16, so SUM(cnt * hash16) is comparable
            # byte-for-byte across all three engines on identical data.
            # An earlier version of this file left hash_sum as ''; that
            # was the right call for Hive's built-in HASH() (which uses
            # Java's hashCode and never agrees cross-engine) but wrong for
            # the portable md5 formula. The legacy SAS template pulled the
            # freq table and computed hash16 SAS-side via the exact same
            # Python function (base.categorical_hash16) — this puts the
            # computation back on the engine where it scales.
            # CAST(cnt AS DOUBLE) prevents BIGINT overflow on the product
            # for very-high-cardinality columns (matches Athena's choice).
            'hash_sum_expr':      lambda p_col: (
                f"SUM(CAST(cnt AS DOUBLE) * "
                f"CAST(conv(substr(md5(trim({p_col})), 1, 4), 16, 10) AS DOUBLE))"
            ),
            'cat_present':        "p_col IS NOT NULL AND TRIM(p_col) <> ''",
            'cat_missing':        "p_col IS NULL OR TRIM(p_col) = ''",
            'final_order_by':     '',
            'vintage_expr':       lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format: (
                # Hadoop's vintage helper takes tbl_cfg + raw date_col, not
                # the quoted form, and ignores date_dtype / date_format
                # (it inspects tbl_cfg.date_type instead).
                _hadoop_vintage_expr(tbl_cfg, tbl_cfg['date_col'], vintage)
            ),
            # Hadoop's vintage_expr already returns a string-shaped
            # expression (concat/substr or date_format), so no outer wrap.
            'dt_select_wrap':     lambda dt_expr: dt_expr,
        }
        if dialect == 'databricks':
            from .athena import _vintage_date_expr_athena
            ops['vintage_expr'] = (
                lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format:
                _vintage_date_expr_athena(date_col_q, vintage,
                                          date_dtype=date_dtype, date_format=date_format)
            )
            ops['dt_select_wrap'] = lambda dt_expr: f"CAST({dt_expr} AS STRING)"
        return ops
    if dialect == 'duckdb':
        # DuckDB powers the csv-source path (platforms/csv_duckdb.py).
        # SQL flavor is closest to Postgres / standard SQL with most
        # Hive built-ins also present, so this dialect borrows from
        # both. Notable deviations:
        #   - no `conv(hex, 16, 10)` -> hex parsing for hash16 uses
        #     `('0x' || hex_str)::INTEGER` which DuckDB accepts.
        #   - LISTAGG-equivalent is `string_agg(expr, ', ' ORDER BY ...)`
        #     which DuckDB supports natively (better than Hive's
        #     non-deterministic collect_list).
        #   - date_trunc is standard SQL.
        #   - is_oracle_typed=False (uses the same Hive-style numeric
        #     dtype catalog).
        return {
            'wc_empty':           '1=1',
            'is_oracle_typed':    False,
            'cast_str':           lambda x: f'CAST({x} AS VARCHAR)',
            'concat':             lambda *parts: f'concat({", ".join(parts)})',
            'top10_coalesce':     lambda x, d: f'COALESCE({x}, {d})',
            'mean_inner':         lambda x: f'AVG(CAST({x} AS DOUBLE))',
            'std_inner':          lambda x: f'STDDEV_SAMP(CAST({x} AS DOUBLE))',
            'mean_outer':         lambda x: f'CAST({x} AS VARCHAR)',
            'std_outer':          lambda x: f'CAST({x} AS VARCHAR)',
            'minmax_strcast':     lambda x: f'CAST({x} AS VARCHAR)',
            'lifeboat_null_count':    'CAST(NULL AS BIGINT)',
            'lifeboat_null_meanstd':  'CAST(NULL AS VARCHAR)',
            'lifeboat_null_minmax':   'CAST(NULL AS VARCHAR)',
            # DuckDB's string_agg honors ORDER BY -- gives a deterministic
            # top_10 (unlike Hive's collect_list).
            'top10_listagg':      lambda item: (
                f"string_agg({item}, '; ' ORDER BY cnt DESC, p_col ASC)"
            ),
            # Portable hash16: same MD5-truncated-to-16-bit recipe as the
            # other dialects. DuckDB has md5() returning lowercase hex,
            # plus '0x'-prefixed hex literals are castable to INTEGER.
            'hash_sum_expr':      lambda p_col: (
                f"SUM(CAST(cnt AS DOUBLE) * "
                f"CAST(('0x' || substr(md5(trim({p_col})), 1, 4))::INTEGER AS DOUBLE))"
            ),
            'cat_present':        "p_col IS NOT NULL AND TRIM(p_col) <> ''",
            'cat_missing':        "p_col IS NULL OR TRIM(p_col) = ''",
            'final_order_by':     '\nORDER BY t.dt',
            'vintage_expr':       lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format: (
                _duckdb_vintage_expr(tbl_cfg, tbl_cfg['date_col'], vintage)
            ),
            'dt_select_wrap':     lambda dt_expr: dt_expr,
        }
    raise ValueError(f"unknown dialect {dialect!r} for col SQL builder")


def _duckdb_vintage_expr(tbl_cfg, date_col, vintage):
    """DuckDB expression that buckets `date_col` to the configured vintage.

    Returns a string-typed expression that downstream GROUP BY / dt_select
    can use directly. DuckDB has standard `date_trunc('week', ...)` plus
    full ISO date parsing for string columns, so this is shorter than
    the Hive equivalent.
    """
    date_type = (tbl_cfg.get('date_type') or 'string_dash').lower()
    # Coerce to DATE. CAST(... AS VARCHAR) first so strptime works whether
    # read_csv_auto inferred the column as VARCHAR, DATE, or BIGINT (a YYYYMMDD
    # int still casts to its digit string). For dash/ISO dates CAST(... AS DATE)
    # parses both a 'YYYY-MM-DD' string and an already-DATE column.
    if date_type in ('string_compact', 'num'):
        parsed = f"strptime(CAST({date_col} AS VARCHAR), '%Y%m%d')::DATE"
    elif date_type in ('string_dash', 'string'):
        parsed = f"CAST({date_col} AS DATE)"
    else:
        parsed = f"CAST({date_col} AS DATE)"

    if vintage in (None, '', 'all'):
        return "'all'"
    if vintage == 'sample':
        return "'sample'"
    if vintage == 'day':
        return f"strftime({parsed}, '%Y-%m-%d')"
    unit = {'week': 'week', 'month': 'month',
            'quarter': 'quarter', 'year': 'year'}.get(vintage)
    if unit is None:
        return f"strftime({parsed}, '%Y-%m-%d')"
    return f"strftime(date_trunc('{unit}', {parsed}), '%Y-%m-%d')"


def _hash16_flat_expr(dialect, col_expr):
    """Single-row hash16 expression for the Quick/Standard flat SQL paths.

    The freq-table-based hash_sum is SUM(cnt * hash16(p_col)). Mathematically
    equivalent to SUM(hash16(col)) over every row (because hash16 is a pure
    function, hashing each of N rows of the same value yields N times the
    same hash16 value, identical to cnt × hash16(value)). The flat form
    saves a GROUP BY <col_expr> and lets the Quick/Standard paths stay
    single-pass.

    Returns the SUM(...) expression directly so the caller drops it into
    the top-level SELECT alongside the other flat aggregates.
    """
    if dialect == 'oracle':
        from .oracle import _oracle_portable_hash16
        return f"SUM(NVL({_oracle_portable_hash16(col_expr)}, 0))"
    if dialect in ('hadoop', 'databricks'):
        return (
            f"SUM(CAST(conv(substr(md5(trim(CAST({col_expr} AS STRING))), 1, 4), "
            f"16, 10) AS DOUBLE))"
        )
    if dialect == 'duckdb':
        # DuckDB lacks `conv()` but accepts hex literals via '0x' prefix
        # cast to INTEGER. Same SUM(hash16) semantics as the other
        # dialects -- byte-identical answer on identical data.
        return (
            f"SUM(CAST(('0x' || substr(md5(trim(CAST({col_expr} AS VARCHAR))), 1, 4))"
            f"::INTEGER AS DOUBLE))"
        )
    raise ValueError(f"unknown dialect {dialect!r} for hash16 flat expr")


def build_col_sql(dialect, tbl_cfg, col, dtype, full_table, cte_prefix="",
                   vintage=None, date_dtype=None, date_format=None,
                   dt_label=None, coarse=False, preset=None,
                   stats_override=None):
    """Build per-column stats SQL for any supported dialect.

    Returns (sql, is_numeric). The output schema is the canonical 14-field
    col CSV row (dt, column_name, col_type, n_total, n_missing, n_unique,
    mean, std, min_val, max_val, top_10, length_avg, length_max, hash_sum)
    for both numeric and categorical branches, so the downstream loader
    sees the same shape regardless of platform.

    `preset` selects which stats to compute (see constants.STATS_PRESETS):

      - 'quick' (default for the col_gen_rough page): flat SELECT, no
        freq table. n_total / n_missing / scalar MIN/MAX / single-pass
        length stats. Categorical n_unique / hash_sum / top_10 are ''.
        Cheapest path — partition-pruning friendly on Hive.

      - 'standard': flat SELECT + n_unique (COUNT DISTINCT) + hash_sum
        (SUM of single-row hash16, mathematically equivalent to the
        freq-weighted SUM(cnt * hash16(p_col))). Still no freq table.

      - 'detailed' (default for the col_gen page): full WITH-chain with
        top_10 + alpha-rank-weighted min/max in `val=cnt` form. Only
        path that does GROUP BY <col_expr>.

    Mean/std (rank-weighted from the freq table) are dropped from every
    preset — empirically poor signal and doubled the cost. Emitted as
    '' to keep the CSV schema stable.

    `coarse=True` is back-compat for legacy callers — aliases to
    preset='quick' when preset is None. New code should pass preset=
    explicitly.

    See _col_sql_dialect_ops for the per-dialect SQL fragments.
    """
    from ..constants import STATS_PRESETS
    if preset is None:
        preset = 'quick' if coarse else 'detailed'
    if preset not in STATS_PRESETS:
        raise ValueError(f"unknown preset {preset!r}; expected one of "
                         f"{list(STATS_PRESETS.keys())}")
    if stats_override is not None:
        # Custom flag set wins: every key in the canonical preset shape
        # is taken from `stats_override` if present, else falls back to
        # the named preset's value. This lets col_gen's Custom radio
        # turn OFF a stat the preset would have turned on (e.g. tick
        # n_unique while keeping hash_sum disabled).
        base_stats = STATS_PRESETS[preset]
        stats = {k: bool(stats_override.get(k, base_stats[k])) for k in base_stats}
    else:
        stats = STATS_PRESETS[preset]

    ops = _col_sql_dialect_ops(dialect)
    date_col = tbl_cfg['date_col']
    col_q = quote_ident(col, dialect)
    date_col_q = quote_ident(date_col, dialect)
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=ops['is_oracle_typed'])
    wc = where.strip() if where else ops['wc_empty']

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
        dt_expr = ops['vintage_expr'](tbl_cfg, date_col_q, vintage, date_dtype, date_format)
        wrapped = ops['dt_select_wrap'](dt_expr)
        dt_select = f"{wrapped} AS dt"
        group_by = f"\nGROUP BY {dt_expr}"
        freq_dt_select = wrapped
        freq_dt_group = f", {dt_expr}"

    # Quick / Standard share the flat-aggregate path: one SELECT, no WITH
    # chain, GROUP BY dt only (or no GROUP BY when dt is a literal).
    # Detailed uses the freq-table WITH chain. The branch decision is
    # `stats['top_10']` -- only Detailed wants the freq table for top_10
    # plus the val=cnt min/max form.
    if not stats['top_10']:
        col_expr = apply_null_sentinels(
            col_q, dtype or ("numeric" if numeric else "categorical"), dialect)
        n_unique_sql = (
            f"COUNT(DISTINCT {col_expr})" if stats['n_unique'] else "0"
        )
        min_sql = (
            ops['minmax_strcast'](f'MIN({col_expr})') if stats['min_max'] else "''"
        )
        max_sql = (
            ops['minmax_strcast'](f'MAX({col_expr})') if stats['min_max'] else "''"
        )
        # Length stats only meaningful on categorical -- numeric LENGTH
        # is dominated by display format (BIGINT "0" = 1 char, DOUBLE
        # "0.0" = 3 chars), so kept '' on numeric even when stats['length'].
        if stats['length'] and not numeric:
            len_avg_sql = (
                ops['cast_str'](f"SUM(LENGTH(TRIM(CAST({col_expr} AS "
                                f"{'VARCHAR2(4000)' if dialect=='oracle' else 'STRING'})))) "
                                f"/ NULLIF(COUNT({col_expr}), 0)")
            )
            len_max_sql = ops['cast_str'](
                f"MAX(LENGTH(TRIM(CAST({col_expr} AS "
                f"{'VARCHAR2(4000)' if dialect=='oracle' else 'STRING'}))))"
            )
        else:
            len_avg_sql = "''"
            len_max_sql = "''"
        # Hash_sum is supported on Hadoop only when stats requests it
        # (Standard / Detailed). On numeric it stays '' because hashing
        # a number's string representation is storage-format-dependent.
        if stats['hash_sum'] and not numeric:
            hash_sum_sql = ops['cast_str'](_hash16_flat_expr(dialect, col_expr))
        else:
            hash_sum_sql = "''"
        col_type_label = dtype if dtype else ('numeric' if numeric else 'categorical')
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{col_type_label}' AS col_type,
    '{col}' AS column_name,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    {n_unique_sql} AS n_unique,
    '' AS mean,
    '' AS std,
    {min_sql} AS min_val,
    {max_sql} AS max_val,
    '' AS top_10,
    {len_avg_sql} AS length_avg,
    {len_max_sql} AS length_max,
    {hash_sum_sql} AS hash_sum
FROM {full_table} WHERE {wc}{group_by}"""
        return sql, numeric

    # Detailed-only: full WITH-chain path for the rest of the function.
    # The pre-existing code below handles this (kept verbatim).
    if numeric:
        col_expr = apply_null_sentinels(col_q, dtype or "numeric", dialect)
        mean_sql = ops['mean_outer'](ops['mean_inner'](col_expr))
        std_sql  = ops['std_outer'](ops['std_inner'](col_expr))
        min_sql  = ops['minmax_strcast'](f'MIN({col_expr})')
        max_sql  = ops['minmax_strcast'](f'MAX({col_expr})')
        sql = f"""{cte_prefix}
SELECT
    {dt_select},
    '{dtype}' AS col_type,
    '{col}' AS column_name,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    COUNT(DISTINCT {col_expr}) AS n_unique,
    {mean_sql} AS mean,
    {std_sql} AS std,
    {min_sql} AS min_val,
    {max_sql} AS max_val,
    '' AS top_10,
    '' AS length_avg,
    '' AS length_max,
    '' AS hash_sum
FROM {full_table} WHERE {wc}{group_by}"""
        return sql, True

    # Categorical: WITH-chain. Per-dialect deltas:
    #   - SUBSTR(<cast_str>(col_expr_cat), 1, 200) for the freq grouping key
    #   - present/missing CASE conditions (Hive treats '' as distinct from NULL)
    #   - minmax concat: `||` (Oracle) vs concat() (Hive)
    #   - top_n listagg: LISTAGG WITHIN GROUP (Oracle) vs concat_ws+collect_list (Hive)
    #   - hash_sum: real portable hash (Oracle) vs '' placeholder (Hive)
    #   - outer NVL/COALESCE for top_10
    #   - outer cast for mean_v/std_v/length_avg/length_max/hash_sum
    #   - trailing ORDER BY t.dt (Oracle only)
    col_expr_cat = apply_null_sentinels(col_q, dtype or "categorical", dialect)
    p_col_substr = f"SUBSTR({ops['cast_str'](col_expr_cat)}, 1, 200)"
    minmax_concat_min = ops['concat']("TRIM(p_col)", "'='", ops['cast_str']("cnt"))
    minmax_concat_max = minmax_concat_min  # same shape; rnk_alpha decides which row

    if coarse:
        # Coarse categorical: drop agg/sstats (no rank-weighted mean/std),
        # drop top_n (no top_10). Keep freq/ranked/totals/minmax/lenhash so
        # min/max + length/hash signals stay populated for cross-platform
        # Phase-1 drift detection. Mirrors Athena's _build_coarse_sql shape.
        sql = f"""{cte_prefix}
WITH freq AS (
    SELECT {freq_dt_select} AS dt,
           {p_col_substr} AS p_col,
           COUNT(*) AS cnt
    FROM {full_table} WHERE {wc}
    GROUP BY {p_col_substr}{freq_dt_group}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(TRIM(p_col)), p_col) AS rnk_alpha,
           COUNT(*)     OVER (PARTITION BY dt) AS k_alpha
    FROM freq WHERE {ops['cat_present']}
),
totals AS (
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN {ops['cat_missing']} THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN {ops['cat_present']} THEN 1 ELSE 0 END) AS n_unique
    FROM freq GROUP BY dt
),
minmax AS (
    SELECT dt,
           MAX(CASE WHEN rnk_alpha = 1       THEN {minmax_concat_min} END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN {minmax_concat_max} END) AS max_val
    FROM ranked GROUP BY dt
),
lenhash AS (
    SELECT dt,
           SUM(cnt * LENGTH(TRIM(p_col))) / NULLIF(SUM(cnt), 0) AS length_avg,
           MAX(LENGTH(TRIM(p_col))) AS length_max,
           {ops['hash_sum_expr']("p_col")} AS hash_sum
    FROM ranked GROUP BY dt
)
SELECT t.dt,
       '{col}' AS column_name,
       '{dtype}' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       '' AS mean, '' AS std,
       mm.min_val, mm.max_val,
       '' AS top_10,
       {ops['cast_str']('lh.length_avg')} AS length_avg,
       {ops['cast_str']('lh.length_max')} AS length_max,
       {ops['cast_str']('lh.hash_sum')}   AS hash_sum
FROM totals t
LEFT JOIN minmax  mm ON t.dt = mm.dt
LEFT JOIN lenhash lh ON t.dt = lh.dt{ops['final_order_by']}"""
        return sql, False

    top10_item = ops['concat']("p_col", "'('", ops['cast_str']("cnt"), "')'")
    top10_expr = ops['top10_listagg'](top10_item)
    hash_sum_expr = ops['hash_sum_expr']("p_col")

    sql = f"""{cte_prefix}
WITH freq AS (
    SELECT {freq_dt_select} AS dt,
           {p_col_substr} AS p_col,
           COUNT(*) AS cnt
    FROM {full_table} WHERE {wc}
    GROUP BY {p_col_substr}{freq_dt_group}
),
ranked AS (
    SELECT dt, p_col, cnt,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY UPPER(TRIM(p_col)), p_col) AS rnk_alpha,
           COUNT(*)     OVER (PARTITION BY dt) AS k_alpha,
           ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC, p_col ASC) AS rn_freq
    FROM freq WHERE {ops['cat_present']}
),
totals AS (
    SELECT dt,
           SUM(cnt) AS n_total,
           COALESCE(SUM(CASE WHEN {ops['cat_missing']} THEN cnt ELSE 0 END), 0) AS n_missing,
           SUM(CASE WHEN {ops['cat_present']} THEN 1 ELSE 0 END) AS n_unique
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
           MAX(CASE WHEN rnk_alpha = 1       THEN {minmax_concat_min} END) AS min_val,
           MAX(CASE WHEN rnk_alpha = k_alpha THEN {minmax_concat_max} END) AS max_val
    FROM ranked GROUP BY dt
),
top_n AS (
    SELECT dt,
           {top10_expr} AS top_10
    FROM ranked WHERE rn_freq <= 10
    GROUP BY dt
),
lenhash AS (
    SELECT dt,
           SUM(cnt * LENGTH(TRIM(p_col))) / NULLIF(SUM(cnt), 0) AS length_avg,
           MAX(LENGTH(TRIM(p_col))) AS length_max,
           {hash_sum_expr} AS hash_sum
    FROM ranked GROUP BY dt
)
SELECT t.dt,
       '{col}' AS column_name,
       '{dtype}' AS col_type,
       t.n_total, t.n_missing, t.n_unique,
       {ops['mean_outer']('ss.mean_v')} AS mean, {ops['std_outer']('ss.std_v')} AS std,
       mm.min_val, mm.max_val,
       {ops['top10_coalesce']('tn.top_10', "''")} AS top_10,
       {ops['cast_str']('lh.length_avg')} AS length_avg,
       {ops['cast_str']('lh.length_max')} AS length_max,
       {ops['cast_str']('lh.hash_sum')}   AS hash_sum
FROM totals t
LEFT JOIN sstats  ss ON t.dt = ss.dt
LEFT JOIN minmax  mm ON t.dt = mm.dt
LEFT JOIN top_n   tn ON t.dt = tn.dt
LEFT JOIN lenhash lh ON t.dt = lh.dt{ops['final_order_by']}"""
    return sql, False


def build_lifeboat_col_sql(dialect, tbl_cfg, col, dtype, full_table,
                            cte_prefix="", vintage=None,
                            date_dtype=None, date_format=None, dt_label=None):
    """Streaming-aggregate fallback. Same row schema as the full per-column
    query, but only computes single-pass aggregates (n_total, n_missing).
    Everything that needs the freq table (n_unique, mean/std, min/max,
    top_10, length, hash) is NULL/empty. Used by the runner when the full
    SQL hits a resource exhaustion error (Oracle ORA-01652/ORA-04030,
    Hive OOM)."""
    ops = _col_sql_dialect_ops(dialect)
    date_col = tbl_cfg['date_col']
    col_q = quote_ident(col, dialect)
    date_col_q = quote_ident(date_col, dialect)
    where = tbl_cfg.get('where', '')
    if not vintage:
        vintage = tbl_cfg.get('vintage', 'all') or 'all'
    numeric = is_numeric_type(dtype, is_oracle=ops['is_oracle_typed'])
    wc = where.strip() if where else ops['wc_empty']

    if dt_label is not None:
        dt_select = f"'{dt_label}' AS dt"
        group_by = ""
    elif vintage == 'all':
        dt_select = "'all' AS dt"
        group_by = ""
    else:
        dt_expr = ops['vintage_expr'](tbl_cfg, date_col_q, vintage, date_dtype, date_format)
        wrapped = ops['dt_select_wrap'](dt_expr)
        dt_select = f"{wrapped} AS dt"
        group_by = f"\nGROUP BY {dt_expr}"

    col_expr = apply_null_sentinels(
        col_q, dtype or ("numeric" if numeric else "categorical"), dialect)
    col_type_label = 'numeric' if numeric else 'categorical'
    return f"""{cte_prefix}
SELECT
    {dt_select},
    '{col_type_label}' AS col_type,
    '{col}' AS column_name,
    COUNT(*) AS n_total,
    COUNT(*) - COUNT({col_expr}) AS n_missing,
    {ops['lifeboat_null_count']} AS n_unique,
    {ops['lifeboat_null_meanstd']} AS mean,
    {ops['lifeboat_null_meanstd']} AS std,
    {ops['lifeboat_null_minmax']} AS min_val,
    {ops['lifeboat_null_minmax']} AS max_val,
    '' AS top_10,
    '' AS length_avg,
    '' AS length_max,
    '' AS hash_sum
FROM {full_table} WHERE {wc}{group_by}"""


def parse_extract_sql_file(sql_path):
    """Parse an extract_{row,col}.sql block file written by any platform's
    gen-first writer. Returns list[dict] with keys:
        name        — block name (e.g. "qname" for row, "qname/col/bucket" for col)
        sql         — the SQL body, conn_macro marker preserved inline
        conn_macro  — the value extracted from /* @conn_macro=X */, or None
        degrade_sql — the body inside /* @degrade_sql ... @end_degrade */, or None

    Block format:
        -- {block_name}
        /* @conn_macro=X */
        <SQL>
        ;
        /* @degrade_sql
        <fallback SQL>
        @end_degrade */

    The leading "-- dtrack ..." / "-- Generated: ..." header lines and any
    "-- [foo]" status comments are skipped; only "-- name" (a bare token,
    no spaces) starts a new block.
    """
    import re
    from ..constants import (SQL_MARKER_CONN_MACRO, SQL_MARKER_DEGRADE_SQL,
                              SQL_MARKER_END_DEGRADE)

    blocks = []
    text = open(sql_path).read()
    current_name = None
    current_lines = []

    degrade_re = re.compile(
        rf"/\*\s*{re.escape(SQL_MARKER_DEGRADE_SQL)}\s*(.*?)"
        rf"\s*{re.escape(SQL_MARKER_END_DEGRADE)}\s*\*/",
        flags=re.DOTALL)
    conn_re = re.compile(
        rf"/\*\s*{re.escape(SQL_MARKER_CONN_MACRO)}=(\S*?)\s*\*/")

    def _flush():
        if current_name is None:
            return
        raw = '\n'.join(current_lines).strip()
        dm = degrade_re.search(raw)
        degrade_sql = None
        if dm:
            degrade_sql = dm.group(1).strip().rstrip(';').strip()
            raw = (raw[:dm.start()] + raw[dm.end():]).strip()
        sql = raw.strip().rstrip(';').strip()
        if not sql:
            return
        m = conn_re.search(sql)
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


def and_clauses(*parts):
    """AND-combine WHERE fragments, dropping empty / 1=1 placeholders.

    Used by every platform's col-gen writer to layer optional bucket
    BETWEEN, manual-exclude NOT IN, base table WHERE, etc. without
    emitting `... AND 1=1` or stray leading/trailing AND.

    Returns '1=1' if all parts are empty -- the universal pass-through
    so callers don't need to special-case "no filter".
    """
    cleaned = []
    for p in parts:
        if not p:
            continue
        s = str(p).strip()
        if not s or s == '1=1':
            continue
        cleaned.append(s)
    if not cleaned:
        return '1=1'
    return ' AND '.join(cleaned)


def run_extract_sql_file(sql_path, outdir, *,
                          query_fn,
                          platform,
                          max_workers=None,
                          on_progress=None,
                          cancel_event=None,
                          row_csv_writer=None):
    """Generic runner for extract_{row,col}.sql block files.

    Parses the file via parse_extract_sql_file, dispatches blocks across a
    ThreadPoolExecutor capped by `<platform>_MAX_WORKERS` env var (or the
    PLATFORM_RUNNER default), and writes per-qname _row.csv / _col.csv.
    Block format and bookkeeping are platform-agnostic; the only deltas
    are wired through:

        query_fn(sql, conn_macro) -> list[dict]
            Executes one block and returns rows as list[dict] with
            lowercased column names. Raises on engine error.
        platform: 'oracle' | 'hadoop' | 'athena' (key into PLATFORM_RUNNER
            for worker_env, worker_default, degraded_label, lifeboat_errors).
        row_csv_writer: optional callable(rows, csv_path) for row-mode CSV
            output. Defaults to a sorted dt-then-row_count writer matching
            both Oracle and Hadoop's existing behavior.

    Returns list[result_dict] -- one per block. Each result has at minimum
    {name, ok, elapsed, start}; ok-rows also carry rows count and (for row
    mode) csv_path; failed rows carry error.

    Mirrors the threading / cancel / per-table CSV-flush semantics of the
    original Oracle and Hadoop runners byte-for-byte.
    """
    import csv
    import sys
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from ..constants import COL_CSV_HEADERS, ROW_CSV_HEADERS, PLATFORM_RUNNER

    cfg = PLATFORM_RUNNER[platform]
    label = platform.capitalize()

    blocks = parse_extract_sql_file(sql_path)
    if not blocks:
        print(f"No query blocks found in {sql_path}")
        return []

    basename = os.path.basename(sql_path)
    is_col = "extract_col" in basename
    file_suffix = "_col.csv" if is_col else "_row.csv"

    os.makedirs(outdir, exist_ok=True)
    env_workers = os.environ.get(cfg['worker_env'])
    _max_workers = max_workers if max_workers else (
        int(env_workers) if env_workers else cfg['worker_default'])
    _max_workers = max(1, min(_max_workers, len(blocks)))

    # Block names are "qname/col_name[/bucket]" for col mode, "qname" for row.
    table_blocks = {}
    for block in blocks:
        qname = block['name'].split('/')[0] if '/' in block['name'] else block['name']
        table_blocks.setdefault(qname, []).append(block)
    _col_rows_lock = threading.Lock()
    _col_rows_by_table = {}

    print(f"  Parsed {len(blocks)} blocks ({len(table_blocks)} tables) "
          f"from {sql_path}, workers={_max_workers}", file=sys.stderr)

    def _default_row_writer(rows, csv_path):
        # Server-side ORDER BY was dropped from the row SQL (forces a
        # single-coordinator funnel). Sort here so the CSV reads in date
        # order for human inspection.
        sorted_rows = sorted(rows, key=lambda r: str(r.get('dt', '')))
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(ROW_CSV_HEADERS)
            for r in sorted_rows:
                writer.writerow([str(r.get('dt', '')), r.get('row_count', 0)])
    if row_csv_writer is None:
        row_csv_writer = _default_row_writer

    results = []
    _done_count = [0]
    _done_lock = threading.Lock()
    _table_done = {q: {'done': 0, 'total': len(bs)}
                   for q, bs in table_blocks.items()}
    _table_lock = threading.Lock()
    _running_count = [0]
    _running_lock = threading.Lock()
    _run_start = [None]

    def _run_block(block):
        block_name = block['name']
        qname = block_name.split('/')[0] if '/' in block_name else block_name
        sql = block['sql']
        conn_macro = block.get('conn_macro')
        start_time = time.time()
        start_ts = datetime.now().isoformat()

        if cancel_event is not None and cancel_event.is_set():
            result = {"name": block_name, "ok": False, "error": "cancelled",
                      "elapsed": 0.0, "start": start_ts, "cancelled": True}
            with _done_lock:
                _done_count[0] += 1
                n = _done_count[0]
            if on_progress:
                result["done"] = n
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
            rows = query_fn(sql, conn_macro)
            elapsed = time.time() - start_time

            if is_col:
                with _col_rows_lock:
                    _col_rows_by_table.setdefault(qname, []).extend(rows)
            else:
                csv_path = os.path.join(outdir, f"{qname}{file_suffix}")
                row_csv_writer(rows, csv_path)

            result = {"name": block_name, "ok": True, "rows": len(rows),
                      "elapsed": round(elapsed, 1), "start": start_ts}
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

        # Per-table CSV flush when the last block of this qname finishes.
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
                writer.writerow(COL_CSV_HEADERS)
                for r in rows_for_qname:
                    writer.writerow([r.get(h, '') for h in COL_CSV_HEADERS])
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

    # Partial-failure flush for col mode: any qname that didn't reach
    # 'all blocks done' still gets whatever rows came back, so the user
    # isn't left with an empty CSV.
    if is_col and _col_rows_by_table:
        for qname, row_data in _col_rows_by_table.items():
            csv_path = os.path.join(outdir, f"{qname}_col.csv")
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(COL_CSV_HEADERS)
                for r in row_data:
                    writer.writerow([r.get(h, '') for h in COL_CSV_HEADERS])
            print(f"  Wrote {len(row_data)} rows to {csv_path} "
                  f"(partial -- table had failures)", file=sys.stderr)

    return results


class PlatformBuilder(ABC):
    """Base for platform SQL/code generation."""

    def __init__(self, tbl_cfg, db_path=None):
        self.tbl_cfg = tbl_cfg
        self.db_path = db_path
        self.name = tbl_cfg['name']
        self.table = tbl_cfg['table']
        self.date_col = tbl_cfg.get('date_col', '')
        self.qname = qualified_name(tbl_cfg)

    @abstractmethod
    def build_row_sql(self, date_filter):
        """Build SQL for row count extraction."""
        ...

    @abstractmethod
    def build_continuous_sql(self, col, col_type, where):
        """Build continuous/numeric stats SQL."""
        ...

    @abstractmethod
    def build_categorical_sql(self, col, col_type, where, top_n=10):
        """Build categorical stats SQL."""
        ...

    @abstractmethod
    def generate_extraction(self, outdir, extract_type, **kw):
        """Generate extraction artifacts (SAS files or direct queries).

        Returns list of output file paths.
        """
        ...
