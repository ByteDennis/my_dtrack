
import os
import re
import threading

_cache_db = None
_cache_lock = threading.Lock()
_cache_hits = 0
_cache_misses = 0
_cache_overwrite = False

# >>> Force cache misses (re-execute and overwrite) while still persisting <<< #
def set_cache_overwrite(flag):
    global _cache_overwrite
    prev = _cache_overwrite
    _cache_overwrite = bool(flag)
    return prev

# >>> regenerable cache/state dir for an outdir; a csv/ leaf anchors .state as its sibling <<< #
def state_dir(outdir):
    base = os.path.normpath(outdir)
    if os.path.basename(base) == "csv":
        base = os.path.dirname(base) or "."
    return os.path.join(base, ".state")

# >>> canonical query-cache sqlite path for an outdir (under the state dir) <<< #
def cache_db_path(outdir):
    return os.path.join(state_dir(outdir), ".cache.db")

# >>> Enable per-process query caching at the given .cache.db path <<< #
def set_cache_db(path):
    global _cache_db, _cache_hits, _cache_misses
    if os.environ.get('DTRACK_QUERY_CACHE') == '0':
        _cache_db = None
        return
    if path is None:
        _cache_db = None
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS statement_ids (
                sql_hash TEXT PRIMARY KEY,
                statement_id TEXT,
                scope     TEXT,
                created_at TEXT
            )
        """)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.commit()
    finally:
        conn.close()

# >>> Persisted in-flight Databricks statement_id for a (SQL, scope) key (resume by ID) <<< #
def statement_id_get(sql, scope):
    if not _cache_db:
        return None
    import sqlite3
    key = _cache_key(sql, scope)
    conn = sqlite3.connect(_cache_db)
    try:
        row = conn.execute(
            "SELECT statement_id FROM statement_ids WHERE sql_hash = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None

# >>> Record the statement_id right after submit so a crash can resume fetch <<< #
def statement_id_put(sql, scope, statement_id):
    if not _cache_db:
        return
    import sqlite3
    from datetime import datetime as _dt
    key = _cache_key(sql, scope)
    with _cache_lock:
        conn = sqlite3.connect(_cache_db)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO statement_ids "
                "(sql_hash, statement_id, scope, created_at) VALUES (?, ?, ?, ?)",
                (key, statement_id, scope or '', _dt.now().isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

# >>> Drop the statement_id once results are cached (or the statement is dead) <<< #
def statement_id_delete(sql, scope):
    if not _cache_db:
        return
    import sqlite3
    key = _cache_key(sql, scope)
    with _cache_lock:
        conn = sqlite3.connect(_cache_db)
        try:
            conn.execute("DELETE FROM statement_ids WHERE sql_hash = ?", (key,))
            conn.commit()
        finally:
            conn.close()

# >>> Return (hits, misses) since the most recent set_cache_db call <<< #
def cache_stats():
    return _cache_hits, _cache_misses

def _cache_key(sql, scope):
    import hashlib
    norm = re.sub(r'\s+', ' ', sql.strip())
    raw = f"{norm}::{scope or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()

# >>> Return the cached payload (whatever was stored) or None <<< #
def cache_get(sql, scope):
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
        payload = pickle.loads(row[0])
        from ..logmgr import get_logger
        n = len(payload) if hasattr(payload, '__len__') else '?'
        get_logger().debug("query: CACHE HIT (%s rows) scope=%s", n, scope or '')
        return payload
    except Exception as e:
        from ..logmgr import log_exc
        log_exc(f"cache unpickle failed (scope={scope})", e)
        return None

# >>> Store value (pickled) under the (SQL, scope) key <<< #
def cache_put(sql, scope, value, row_count=None):
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
    from ..logmgr import get_logger
    get_logger().debug("query: LIVE DB read -> cached (%s rows) scope=%s",
                       row_count if row_count is not None else '?', scope or '')

ORACLE_NUMERIC_TYPES = {
    'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE',
    'INTEGER', 'INT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'DEC',
}

ATHENA_NUMERIC_TYPES = {
    'int', 'integer', 'bigint', 'smallint', 'tinyint',
    'double', 'float', 'decimal', 'real',
}

from ..constants import DATE_TYPE_FORMATS, DATE_TYPE_TO_CANONICAL_DTYPE

# >>> Ensure date_filter has date_format AND date_dtype set <<< #
def resolve_date_format(date_filter, tbl_cfg):
    cfg_date_type = (tbl_cfg.get('date_type') or '').lower()
    if not date_filter.get('date_format'):
        if cfg_date_type in DATE_TYPE_FORMATS:
            date_filter['date_format'] = DATE_TYPE_FORMATS[cfg_date_type]
    if cfg_date_type in DATE_TYPE_TO_CANONICAL_DTYPE:
        date_filter['date_dtype'] = DATE_TYPE_TO_CANONICAL_DTYPE[cfg_date_type]

_NUMERIC_TYPE_SET = {t.upper() for t in ORACLE_NUMERIC_TYPES} | {
    t.upper() for t in ATHENA_NUMERIC_TYPES
}

# >>> Check if a data type string represents a numeric column <<< #
def is_numeric_type(data_type, is_oracle=True):
    if not data_type:
        return False
    dt_upper = data_type.upper().split('(')[0].strip()
    return dt_upper in _NUMERIC_TYPE_SET

_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# >>> Wrap a SQL identifier in the dialect's quote char ONLY when the bare <<< #
def quote_ident(name, dialect="oracle"):
    if not name or _BARE_IDENT_RE.match(name):
        return name
    if dialect in ("hadoop", "hive", "databricks", "spark"):
        return f"`{name}`"
    return f'"{name}"'

# >>> Map SQL dialect string to NULL_SENTINELS top-level key <<< #
def _sentinel_dialect_key(dialect):
    d = (dialect or "").lower()
    if d in ("athena", "trino", "aws", "databricks", "spark"):
        return "aws"
    if d in ("hadoop", "hive"):
        return "hadoop"
    return "oracle"

# >>> True if col_type label looks numeric (NUMBER/DOUBLE/INT/...) <<< #
def _is_numeric_col_type(col_type):
    if not col_type:
        return False
    t = str(col_type).strip().lower()
    if t == "numeric":
        return True
    if t in ("categorical", "string", "char", "varchar", "text"):
        return False
    return is_numeric_type(t)

# >>> True if col_type label looks like a date/timestamp <<< #
def _is_date_col_type(col_type):
    if not col_type:
        return False
    t = str(col_type).strip().lower()
    return "date" in t or "time" in t

# >>> Render a sentinel value as a SQL literal for the given dialect <<< #
def _format_sentinel_literal(val, kind, dialect):
    if kind == "numeric":
        return str(val)
    if kind == "string":
        esc = str(val).replace("'", "''")
        return f"'{esc}'"
    s = str(val)
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

# >>> Wrap a SQL column reference with sentinel-to-NULL translation <<< #
def apply_null_sentinels(col_ref, col_type, dialect, sentinels=None,
                         include_blank=True):
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
    if include_blank and kind == "string":
        # blank/whitespace-only -> NULL. Oracle TRIM('   ') is NULL (empty == NULL),
        # so use IS NULL there; other engines compare to '' after TRIM.
        conds.append(f"TRIM({col_ref}) IS NULL" if dialect == "oracle"
                     else f"TRIM({col_ref}) = ''")

    if not conds:
        return col_ref

    return f"CASE WHEN {' OR '.join(conds)} THEN NULL ELSE {col_ref} END"

# >>> Derive storage/file name: {source}_{name} <<< #
def qualified_name(tbl_cfg):
    source = tbl_cfg.get('source', '')
    name = tbl_cfg['name']
    return f"{source}_{name}" if source else name

# >>> Resolve table reference, wrapping with CTE if 'processed' is configured <<< #
def resolve_table(tbl_cfg, full_table=None):
    processed = tbl_cfg.get('processed')
    table = full_table or tbl_cfg['table']
    if not processed:
        return table, ""
    if isinstance(processed, list):
        processed = "\n".join(processed)
    alias = f"cte_{tbl_cfg['name']}"
    cte = f"WITH {alias} AS (\n{processed}\n)\n"
    return alias, cte

# >>> Reformat a YYYY-MM-DD date string to the target date_format <<< #
def reformat_date(d, date_format):
    from datetime import datetime as _dt
    if not date_format or date_format == 'YYYY-MM-DD':
        return str(d)
    try:
        dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
    except ValueError:
        return str(d)
    fmt_map = {
        'YYYYMMDD':    '%Y%m%d',
        'DDMONYYYY':   '%d%b%Y',
        'DD-MON-YYYY': '%d-%b-%Y',
        'MM/DD/YYYY':  '%m/%d/%Y',
        'YYYYMM':      '%Y%m',
    }
    py_fmt = fmt_map.get(date_format)
    if py_fmt:
        return dt_obj.strftime(py_fmt).upper() if 'MON' in date_format else dt_obj.strftime(py_fmt)
    return str(d)

# >>> date_type-aware literal for one date bound; complete across every date_type <<< #
def format_date_bound_literal(date_str, date_type, is_upper=False, custom_date_types=None):
    dt = (date_type or '').lower()
    time_part = "23:59:59" if is_upper else "00:00:00"
    if custom_date_types and dt in custom_date_types:
        custom = custom_date_types[dt]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            return reformat_date(date_str, fmt)
        if cat == 'date':
            return f"DATE '{date_str}'"
        return f"'{reformat_date(date_str, fmt)}'"
    if dt in ('timestamp', 'datetime'):
        return f"TIMESTAMP '{date_str} {time_part}'"
    if dt == 'date':
        return f"DATE '{date_str}'"
    from ..constants import DATE_TYPE_FORMATS, DATE_TYPE_TO_CANONICAL_DTYPE
    canon = DATE_TYPE_TO_CANONICAL_DTYPE.get(dt, 'DATE')
    fmt = DATE_TYPE_FORMATS.get(dt, 'YYYY-MM-DD')
    if canon == 'NUMBER':
        return reformat_date(date_str, fmt)
    if canon == 'VARCHAR2':
        return f"'{reformat_date(date_str, fmt)}'"
    return f"DATE '{date_str}'"

# >>> Build WHERE fragment for date IN (...) based on column type <<< #
def build_date_in_clause(date_col, dates, date_dtype, is_sas=False, date_format=None,
                         custom_date_types=None):
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

    if custom_date_types and date_dtype and date_dtype.lower() in custom_date_types:
        custom = custom_date_types[date_dtype.lower()]
        cat = custom.get('category', 'string')
        fmt = custom.get('format')
        if cat == 'number':
            formatted = [reformat_date(d, fmt) for d in dates]
        elif cat == 'date':
            formatted = [f"DATE '{d}'" for d in dates]
        else:
            formatted = [f"'{reformat_date(d, fmt)}'" for d in dates]
        col_expr = date_col
    elif dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
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

# >>> Build WHERE fragment using BETWEEN based on column type <<< #
def build_date_between_clause(date_col, min_date, max_date, date_dtype, is_sas=False, date_format=None,
                              custom_date_types=None):
    dtype = (date_dtype or '').upper()
    is_string = dtype.startswith(('CHAR', 'VARCHAR', 'STRING', 'TEXT'))

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
        else:
            fmt_min = reformat_date(min_date, fmt)
            fmt_max = reformat_date(max_date, fmt)
            return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"

    if dtype in ('NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT'):
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
        if is_datetime:
            from datetime import datetime as _dt, timedelta as _td
            try:
                d_max = _dt.strptime(str(max_date), "%Y-%m-%d") + _td(days=1)
                max_next = d_max.strftime("%Y-%m-%d")
                return (f"{date_col} >= DATE '{min_date}' "
                        f"AND {date_col} < DATE '{max_next}'")
            except ValueError:
                return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
        return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"
    else:
        if is_sas:
            fmt_min = reformat_date(min_date, date_format)
            fmt_max = reformat_date(max_date, date_format)
            return f"{date_col} BETWEEN '{fmt_min}' AND '{fmt_max}'"
        return f"{date_col} BETWEEN DATE '{min_date}' AND DATE '{max_date}'"

# >>> effective (from,to): CLI args win, else the table's pair-config fromDate/toDate <<< #
def resolve_pair_date_bounds(tbl, config, from_date, to_date):
    eff_from = from_date or None
    eff_to = to_date or None
    if eff_from and eff_to:
        return eff_from, eff_to
    pairs = (config or {}).get('pairs', {}) or {}
    for pname in tbl.get('_pairs', []) or []:
        pc = pairs.get(pname, {}) or {}
        cf = pc.get('fromDate')
        ct = pc.get('toDate')
        cf = cf.strip() if isinstance(cf, str) else cf
        ct = ct.strip() if isinstance(ct, str) else ct
        if not eff_from and cf:
            eff_from = cf
        if not eff_to and ct:
            eff_to = ct
    return eff_from, eff_to

# >>> date-bounds WHERE fragment honoring single-sided bounds; '' if neither <<< #
def build_date_bounds_clause(date_col, from_date, to_date, date_dtype,
                             date_format=None, custom_date_types=None):
    if from_date and to_date:
        return build_date_between_clause(
            date_col, from_date, to_date, date_dtype,
            date_format=date_format, custom_date_types=custom_date_types)
    if from_date:
        lit = format_date_bounds_literals(
            from_date, from_date, date_dtype, date_format=date_format,
            custom_date_types=custom_date_types)[0]
        return f"{date_col} >= {lit}"
    if to_date:
        lit = format_date_bounds_literals(
            to_date, to_date, date_dtype, date_format=date_format,
            custom_date_types=custom_date_types)[1]
        return f"{date_col} <= {lit}"
    return ""

# >>> uniform date-bounds application for every platform: CLI args or config <<< #
# >>> fromDate/toDate, single-sided ok; clause_fn renders the engine literal <<< #
def apply_pair_date_bounds(tables, config, from_date, to_date, clause_fn):
    for tbl in tables:
        date_col = tbl.get('date_col', '')
        if not date_col:
            continue
        eff_from, eff_to = resolve_pair_date_bounds(tbl, config, from_date, to_date)
        if not (eff_from or eff_to):
            continue
        clause = clause_fn(tbl, date_col, eff_from, eff_to)
        if clause:
            tbl['_date_bounds'] = clause
        tbl['_from_date'] = eff_from
        tbl['_to_date'] = eff_to

# >>> Return (from_lit, to_lit): the platform-formatted date literals that <<< #
def format_date_bounds_literals(min_date, max_date, date_dtype, is_sas=False,
                                date_format=None, custom_date_types=None):
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
    if is_sas:
        return (f"'{reformat_date(min_date, date_format)}'",
                f"'{reformat_date(max_date, date_format)}'")
    return f"DATE '{min_date}'", f"DATE '{max_date}'"

# >>> Build a BETWEEN clause covering [min(matching_dates), max(matching_dates)] <<< #
def build_date_range_with_gaps(date_col, matching_dates, date_dtype,
                               is_sas=False, date_format=None, custom_date_types=None):
    bmin, bmax = min(matching_dates), max(matching_dates)
    return build_date_between_clause(
        date_col, bmin, bmax, date_dtype,
        is_sas=is_sas, date_format=date_format,
        custom_date_types=custom_date_types,
    )

# >>> Compute date filter once in Python for both SAS and Athena paths <<< #
def compute_date_filter(tbl_cfg, db_path, vintage):
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

    if db_path:
        from ..db import get_column_meta, get_metadata
        date_col = tbl_cfg.get('date_col', '')
        try:
            for cm in (get_column_meta(db_path, qname) or []):
                if cm['column_name'].upper() == date_col.upper():
                    result['date_dtype'] = (cm.get('data_type') or '').upper() or None
                    break
        except Exception:
            pass
        try:
            meta = get_metadata(db_path, qname)
            if meta:
                result['date_format'] = meta.get('date_format')
        except Exception:
            pass

    if vintage == 'all' or not db_path:
        print(f"  [date filter] vintage: {vintage} | no date filter applied "
              f"(date_dtype={result['date_dtype']!r})")
        return result

    is_sample, n_sample = _parse_sample_vintage(vintage)
    if is_sample:
        result['vintage'] = 'sample'

    from ..db import list_table_pairs, get_row_comparison, get_column_meta, get_metadata

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
            result['overlap_start'] = comp.get('overlap_start')
            result['overlap_end']   = comp.get('overlap_end')

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

# >>> Parse 'sample@N' into (is_sample, n_sample) <<< #
def _parse_sample_vintage(vintage_str):
    if not vintage_str or not vintage_str.startswith('sample@'):
        return False, 0
    try:
        return True, int(vintage_str.split('@', 1)[1])
    except ValueError:
        return False, 0

# >>> Sample N dates from matching_dates with reproducible seed <<< #
def _sample_matching_dates(db_path, tbl_cfg, matching_dates, n_sample=100):
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

# >>> Splice extra CTEs onto an existing 'WITH ... ' prefix (or start one) <<< #
def _merge_cte_chain(cte_prefix, new_ctes):
    new_clauses = [f"{name} AS (\n{body.strip()}\n)" for name, body in new_ctes]
    if not cte_prefix.strip():
        return "WITH " + ",\n".join(new_clauses) + "\n"
    existing = cte_prefix.rstrip()
    if existing.upper().lstrip().startswith("WITH "):
        return existing + ",\n" + ",\n".join(new_clauses) + "\n"
    return existing + "\nWITH " + ",\n".join(new_clauses) + "\n"

# >>> Build column statistics SQL for Oracle or Athena <<< #
def build_stats_sql(table, col, date_col, where="", col_type="numeric", dialect="oracle", cte_prefix=""):
    where_clause = f"AND {where}" if where else ""
    is_athena = dialect == "athena"
    col_ref = quote_ident(col, dialect)
    date_col_ref = quote_ident(date_col, dialect)
    col_expr = apply_null_sentinels(col_ref, col_type, dialect)

    if col_type == "numeric":
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

    cast_expr = f"CAST({col_expr} AS VARCHAR)" if is_athena else f"TO_CHAR({col_expr})"
    cnt_to_str = "CAST(cnt AS VARCHAR)" if is_athena else "cnt"
    cnt_num = "CAST(cnt AS DOUBLE)" if is_athena else "cnt"
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

# >>> Build top 10 frequency SQL for Oracle or Athena <<< #
def build_top10_sql(table, col, date_col, where="", dialect="oracle"):
    where_clause = f"AND {where}" if where else ""
    alias = " t" if dialect == "athena" else ""
    col_ref = quote_ident(col, dialect)
    date_col_ref = quote_ident(date_col, dialect)
    col_expr = apply_null_sentinels(col_ref, "categorical", dialect)
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col_ref} AS dt, CAST({col_expr} AS VARCHAR(200)) AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col_ref} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col_expr} IS NOT NULL {where_clause}
    GROUP BY {date_col_ref}, {col_expr}
){alias} WHERE rn <= 10""".strip()

# >>> Normalize a query result row to standard dict format (all strings) <<< #
def parse_stats_row(row):
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

# >>> Read cardinality from _col_stats and return the set of cols whose <<< #
def read_id_cols_from_col_stats(db_path, table_name, threshold=0.99, ttl_days=1.0):
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
                pass
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

P_COL_MAX_LEN = 200

# >>> Apply the shared p_col truncation (matches SUBSTR(..., 1, 4000)) <<< #
def categorical_truncate(value):
    if value is None:
        return None
    s = str(value)
    return s[:P_COL_MAX_LEN]

# >>> ASCII-space TRIM matching SQL TRIM(value) default <<< #
def categorical_trim(value):
    if value is None:
        return None
    return str(value).strip(' ')

# >>> Character length of the trimmed value, or None if value is None <<< #
def categorical_length(value):
    if value is None:
        return None
    return len(categorical_trim(value))

# >>> 16-bit categorical hash matching the cross-engine SQL recipe <<< #
def categorical_hash16(value):
    import hashlib
    if value is None:
        return None
    raw = categorical_trim(value).encode('utf-8')
    return int(hashlib.md5(raw).hexdigest()[:4], 16)

# >>> Compute length_avg / length_max / hash_sum for a list of (p_col, cnt) <<< #
def categorical_aggregates(freq_rows):
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

# >>> rename a just-generated extract_{type}.sql to extract_{type}.{platform}.sql so platforms sharing one dir don't clobber (review only) <<< #
def tag_generated_sql(outdir, extract_type, platform):
    src = os.path.join(outdir, f"extract_{extract_type}.sql")
    if not os.path.exists(src):
        return None
    dst = os.path.join(outdir, f"extract_{extract_type}.{platform}.sql")
    os.replace(src, dst)
    abspath = os.path.abspath(dst)
    from ..logmgr import get_logger
    get_logger().info("[generate] %s %s SQL -> %s", platform, extract_type, abspath)
    print(f"[gen-only] {abspath}")
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

# >>> Load tables from config (unified format only) <<< #
def load_tables_from_config(config):
    if "pairs" in config and isinstance(config["pairs"], dict):
        from ..config import get_all_tables_from_unified
        return get_all_tables_from_unified(config)
    return config.get('tables', [])

# >>> Fill in columns from _column_meta for tables missing columns <<< #
def fill_columns_from_meta(tables, db_path):
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
                dc = (tbl.get('date_col') or '').lower()
                tbl['columns'] = {c: t for c, t in all_cols.items()
                                  if c in allowed or c.lower() == dc}
                print(f"  {tbl['name']}: {len(tbl['columns'])}/{len(all_cols)} columns (filtered by col_map)")
            else:
                tbl['columns'] = all_cols
                print(f"  {tbl['name']}: loaded {len(all_cols)} columns from _column_meta")

# >>> Resolve a table's columns from config: inline `columns` or a `columns_file` <<< #
def resolve_config_columns(tbl):
    if tbl.get('columns'):
        return dict(tbl['columns'])
    path = tbl.get('columns_file')
    if path:
        from ..loader import read_columns_sidecar
        cols = read_columns_sidecar(os.path.expanduser(path))
        if cols:
            tbl['columns'] = cols
            return dict(cols)
        from ..logmgr import get_logger
        get_logger().warning("%s: columns_file %r yielded no columns",
                             qualified_name(tbl), path)
    return {}

# >>> Discover columns per source and write {qname}_columns.csv + _column_meta. <<< #
# >>> Honors config-defined columns / columns_file (skips live discovery). <<< #
def discover_and_write_columns(all_tables, outdir, db_path):
    import csv as csv_mod
    from ..db import insert_column_meta, get_column_meta
    from ..logmgr import get_logger, log_exc

    log = get_logger()
    os.makedirs(outdir, exist_ok=True)
    aws_cursors = {}
    oracle_conns = {}

    for tbl in all_tables:
        source = (tbl.get('source') or '').lower()
        qname = qualified_name(tbl)
        raw_table = tbl.get('table', '') or ''
        conn_macro = tbl.get('conn_macro', '')
        columns = None

        cfg_cols = resolve_config_columns(tbl)
        if cfg_cols:
            columns = cfg_cols
            if db_path:
                insert_column_meta(db_path, qname, columns, source=source)
            log.info("%s: using %d config-defined columns (discovery skipped)",
                     qname, len(columns))
        else:
            existing = get_column_meta(db_path, qname)
            if existing:
                columns = {c["column_name"]: c.get("data_type", "") for c in existing}
                log.info("%s: found %d columns in _column_meta", qname, len(columns))
            elif tbl.get('processed'):
                log.info("%s: 'processed' CTE — discovery skipped; load "
                         "%s_columns.csv via `dtrack load-columns` first", qname, qname)
                continue
            elif source == 'aws':
                try:
                    database = conn_macro or tbl.get('database', '')
                    if database not in aws_cursors:
                        from ..platforms.athena import athena_connect, aws_creds_renew
                        log.info("%s: connecting to Athena (%s)...", qname, database)
                        aws_creds_renew()
                        conn = athena_connect(data_base=database)
                        aws_cursors[database] = conn.cursor()
                    from ..platforms.athena import _discover_columns_athena
                    columns = _discover_columns_athena(aws_cursors[database], database, raw_table)
                    if columns:
                        insert_column_meta(db_path, qname, columns, source=source)
                        log.info("%s: discovered %d columns from Athena", qname, len(columns))
                    else:
                        log.warning("%s: no columns found in Athena", qname)
                except Exception as e:
                    log_exc(f"{qname}: Athena column discovery failed", e)
            elif source == 'oracle':
                try:
                    if conn_macro not in oracle_conns:
                        from ..db import oracle_connect
                        log.info("%s: connecting to Oracle (%s)...", qname, conn_macro)
                        oracle_conns[conn_macro] = oracle_connect(conn_macro)
                    conn = oracle_conns[conn_macro]
                    if conn is None:
                        log.warning("%s: Oracle connection unavailable", qname)
                    else:
                        from ..db import discover_columns_oracle
                        columns = discover_columns_oracle(conn, raw_table)
                        if columns:
                            insert_column_meta(db_path, qname, columns, source=source)
                            log.info("%s: discovered %d columns from Oracle", qname, len(columns))
                        else:
                            log.warning("%s: no columns found in Oracle", qname)
                except Exception as e:
                    log_exc(f"{qname}: Oracle column discovery failed", e)
            elif source == 'hadoop':
                try:
                    from ..platforms.hadoop import _discover_columns_hadoop
                    columns = _discover_columns_hadoop(conn_macro, raw_table)
                    if columns:
                        insert_column_meta(db_path, qname, columns, source=source)
                        log.info("%s: discovered %d columns from Hadoop", qname, len(columns))
                    else:
                        log.warning("%s: no columns found in Hadoop", qname)
                except Exception as e:
                    log_exc(f"{qname}: Hadoop column discovery failed", e)
            else:
                log.warning("%s: unsupported source %r; column discovery skipped",
                            qname, source)
                continue

        if columns:
            col_csv = os.path.join(outdir, f"{qname}_columns.csv")
            with open(col_csv, 'w', newline='') as f:
                writer = csv_mod.writer(f)
                writer.writerow(['COLUMN_NAME', 'DATA_TYPE'])
                for col_name, col_type in sorted(columns.items()):
                    writer.writerow([col_name, col_type])
            log.info("%s: wrote %d columns to %s", qname, len(columns), col_csv)

    for cursor in aws_cursors.values():
        try:
            cursor.close()
        except Exception as e:
            log_exc("athena cursor close", e, level="debug")
    for conn in oracle_conns.values():
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            log_exc("oracle conn close", e, level="debug")

_ignore_where_map = False

# >>> Toggle skipping where_map injection (set by --ignore-where) <<< #
def set_ignore_where_map(flag):
    global _ignore_where_map
    _ignore_where_map = bool(flag)

# >>> Inject where_map filters from pair config into table configs <<< #
def inject_where_from_config(tables, config):
    if _ignore_where_map:
        print("  where_map injection disabled (--ignore-where)")
        return
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
                # preserve the config base WHERE (no date range): col writers use it
                # for date-scoped branches so the matching-date filter isn't duplicated
                tbl['_base_where'] = (pair_cfg.get(side, {}).get('where', '') or '').strip()
                tbl['where'] = where_clause
                tbl['_where_from_config'] = True
                print(f"  {qualified_name(tbl)}: using where_map[{side}] from config")
                break

# >>> Compare two column dictionaries and match by case-insensitive name <<< #
def match_columns_from_dicts(left_cols, right_cols, left_label="left", right_label="right", outfile=None):
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

# >>> Returns a dict of dialect-specific SQL fragments used by build_col_sql <<< #
def _col_sql_dialect_ops(dialect):
    if dialect == 'oracle':
        from .oracle import _vintage_date_expr_oracle, _oracle_portable_hash16
        return {
            'wc_empty':           '(1=1)',
            'is_oracle_typed':    True,
            'nullif':             lambda x: f'NULLIF({x}, 0)',
            'cast_str':           lambda x: f'TO_CHAR({x})',
            'concat':             lambda *parts: ' || '.join(parts),
            'top10_coalesce':     lambda x, d: f'NVL({x}, {d})',
            'mean_inner':         lambda x: f'AVG({x})',
            'std_inner':          lambda x: f'STDDEV_SAMP({x})',
            'mean_outer':         lambda x: x,
            'std_outer':          lambda x: x,
            'minmax_strcast':     lambda x: f'TO_CHAR({x})',
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
            'lifeboat_null_count':    'CAST(NULL AS BIGINT)',
            'lifeboat_null_meanstd':  'CAST(NULL AS STRING)',
            'lifeboat_null_minmax':   'CAST(NULL AS STRING)',
            'top10_listagg':      lambda item: (
                f"concat_ws('; ', collect_list({item}))"
            ),
            'hash_sum_expr':      lambda p_col: (
                f"SUM(CAST(cnt AS DOUBLE) * "
                f"CAST(conv(substr(md5(trim({p_col})), 1, 4), 16, 10) AS DOUBLE))"
            ),
            'cat_present':        "p_col IS NOT NULL AND TRIM(p_col) <> ''",
            'cat_missing':        "p_col IS NULL OR TRIM(p_col) = ''",
            'final_order_by':     '',
            'vintage_expr':       lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format: (
                _hadoop_vintage_expr(tbl_cfg, tbl_cfg['date_col'], vintage)
            ),
            'dt_select_wrap':     lambda dt_expr: dt_expr,
        }
        # Hive lacks NULLIF on some distros (SemanticException 10011); use CASE WHEN.
        ops['nullif'] = ((lambda x: f'CASE WHEN ({x}) = 0 THEN NULL ELSE ({x}) END')
                         if dialect == 'hadoop' else (lambda x: f'NULLIF({x}, 0)'))
        return ops
    if dialect == 'duckdb':
        return {
            'wc_empty':           '1=1',
            'is_oracle_typed':    False,
            'nullif':             lambda x: f'NULLIF({x}, 0)',
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
            'top10_listagg':      lambda item: (
                f"string_agg({item}, '; ' ORDER BY cnt DESC, p_col ASC)"
            ),
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
    if dialect in ('athena', 'trino', 'aws'):
        from .athena import _athena_portable_hash16, _vintage_date_expr_athena
        return {
            'wc_empty':           '(1=1)',
            'is_oracle_typed':    False,
            'nullif':             lambda x: f'NULLIF({x}, 0)',
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
            'top10_listagg':      lambda item: (
                f"ARRAY_JOIN(ARRAY_AGG({item} ORDER BY cnt DESC, p_col ASC), '; ')"
            ),
            'hash_sum_expr':      lambda p_col: (
                f"SUM(CAST(cnt AS DOUBLE) * "
                f"CAST({_athena_portable_hash16(p_col)} AS DOUBLE))"
            ),
            'cat_present':        "p_col IS NOT NULL AND TRIM(p_col) <> ''",
            'cat_missing':        "p_col IS NULL OR TRIM(p_col) = ''",
            'final_order_by':     '\nORDER BY t.dt',
            'vintage_expr':       lambda tbl_cfg, date_col_q, vintage, date_dtype, date_format: (
                _vintage_date_expr_athena(date_col_q, vintage,
                                          tbl_cfg.get('vintage_transform'),
                                          date_dtype, date_format)
            ),
            'dt_select_wrap':     lambda dt_expr: f'CAST({dt_expr} AS VARCHAR)',
        }
    raise ValueError(f"unknown dialect {dialect!r} for col SQL builder")

# >>> DuckDB expression that buckets `date_col` to the configured vintage <<< #
def _duckdb_vintage_expr(tbl_cfg, date_col, vintage):
    date_type = (tbl_cfg.get('date_type') or 'string_dash').lower()
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

# >>> Single-row hash16 expression for the Quick/Standard flat SQL paths <<< #
def _hash16_flat_expr(dialect, col_expr):
    if dialect == 'oracle':
        from .oracle import _oracle_portable_hash16
        return f"SUM(NVL({_oracle_portable_hash16(col_expr)}, 0))"
    if dialect in ('hadoop', 'databricks'):
        return (
            f"SUM(CAST(conv(substr(md5(trim(CAST({col_expr} AS STRING))), 1, 4), "
            f"16, 10) AS DOUBLE))"
        )
    if dialect == 'duckdb':
        return (
            f"SUM(CAST(('0x' || substr(md5(trim(CAST({col_expr} AS VARCHAR))), 1, 4))"
            f"::INTEGER AS DOUBLE))"
        )
    if dialect in ('athena', 'trino', 'aws'):
        from .athena import _athena_portable_hash16
        return f"SUM(CAST({_athena_portable_hash16(col_expr)} AS DOUBLE))"
    raise ValueError(f"unknown dialect {dialect!r} for hash16 flat expr")

# >>> truthiness of the DTRACK_STRICT env flag (single source for cli + compare) <<< #
def strict_env_on():
    return os.environ.get('DTRACK_STRICT', '') not in ('', '0', 'false', 'False', 'no')

# >>> resolve STRICT mode: explicit arg > per-pair config > DTRACK_STRICT env <<< #
def _resolve_strict(tbl_cfg, strict=None, col=None):
    v = strict if strict is not None else (tbl_cfg or {}).get('strict')
    if isinstance(v, (list, tuple, set)):
        cols = {str(c).strip().lower() for c in v}
        if col is None:
            return bool(cols)
        return (col or '').strip().lower() in cols
    if v is not None:
        return bool(v)
    return strict_env_on()

# >>> STRICT value family derived from dtype: date | numeric | string <<< #
def _strict_family(dtype, numeric):
    d = (dtype or '').upper()
    if any(k in d for k in ('DATE', 'TIMESTAMP', 'DATETIME')):
        return 'date'
    if numeric:
        return 'numeric'
    return 'string'

# >>> wrap a normalized string expr so blank/whitespace collapses to ONE token <<< #
# Both sides bucket empties identically (counted, not engine-dependent NULL),
# so n_unique / hash agree regardless of how each engine trims/treats ''/NULL.
# CASE WHEN (not NULLIF/COALESCE) so it runs on Hive < 2.3 too.
def _strict_blank_token(inner):
    return f"CASE WHEN ({inner}) IS NULL OR ({inner}) = '' THEN '<<NULL>>' ELSE ({inner}) END"

# >>> collapse trailing decimal zeros ONLY on pure decimals; leading zeros ('007') kept <<< #
def _strict_num_str_collapse(dialect, inner):
    if dialect == 'duckdb':
        c = (f"regexp_replace(regexp_replace({inner}, '(\\.[0-9]*[1-9])0+$', "
             f"'\\1'), '\\.0+$', '')")
        return (f"CASE WHEN regexp_full_match({inner}, '-?[0-9]+\\.[0-9]+') "
                f"THEN {c} ELSE {inner} END")
    if dialect in ('hadoop', 'databricks'):
        c = (f"regexp_replace(regexp_replace({inner}, '(\\\\.[0-9]*[1-9])0+$', "
             f"'$1'), '\\\\.0+$', '')")
        return (f"CASE WHEN {inner} rlike '^-?[0-9]+\\\\.[0-9]+$' "
                f"THEN {c} ELSE {inner} END")
    if dialect == 'oracle':
        c = (f"REGEXP_REPLACE(REGEXP_REPLACE({inner}, '(\\.[0-9]*[1-9])0+$', "
             f"'\\1'), '\\.0+$', '')")
        return (f"CASE WHEN REGEXP_LIKE({inner}, '^-?[0-9]+\\.[0-9]+$') "
                f"THEN {c} ELSE {inner} END")
    if dialect in ('athena', 'trino', 'aws'):
        c = (f"regexp_replace(regexp_replace({inner}, '(\\.[0-9]*[1-9])0+$', "
             f"'$1'), '\\.0+$', '')")
        return (f"CASE WHEN regexp_like({inner}, '^-?[0-9]+\\.[0-9]+$') "
                f"THEN {c} ELSE {inner} END")
    return inner

# >>> STRICT canonical string form of `expr` per dialect/family (extract-time) <<< #
def _strict_normalize(dialect, expr, family, num_str=False):
    def _str(inner):
        return _strict_blank_token(
            _strict_num_str_collapse(dialect, inner) if num_str else inner)
    if dialect in ('hadoop', 'databricks'):
        s = f'CAST({expr} AS STRING)'
        if family == 'date':
            return ("regexp_replace(date_format(CAST(" + expr +
                    " AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss'), ' 00:00:00$', '')")
        if family == 'numeric':
            return (f"regexp_replace(regexp_replace({s}, "
                    f"'(\\\\.[0-9]*[1-9])0+$', '$1'), '\\\\.0+$', '')")
        return _str(f"lower(trim({s}))")
    if dialect == 'duckdb':
        s = f'CAST({expr} AS VARCHAR)'
        if family == 'date':
            return ("regexp_replace(strftime(CAST(" + expr +
                    " AS TIMESTAMP), '%Y-%m-%d %H:%M:%S'), ' 00:00:00$', '')")
        if family == 'numeric':
            return (f"regexp_replace(regexp_replace({s}, "
                    f"'(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')")
        return _str(f"lower(trim(nfc_normalize({s})))")
    if dialect == 'oracle':
        s = f'TO_CHAR({expr})'
        if family == 'date':
            return ("REGEXP_REPLACE(TO_CHAR(CAST(" + expr +
                    " AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS'), ' 00:00:00$', '')")
        if family == 'numeric':
            s2 = (f"REGEXP_REPLACE(REGEXP_REPLACE({s}, "
                  f"'(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')")
            return (f"REGEXP_REPLACE(REGEXP_REPLACE({s2}, '^-\\.', '-0.'), "
                    f"'^\\.', '0.')")
        return _str(f"LOWER(TRIM(COMPOSE({s})))")
    if dialect in ('athena', 'trino', 'aws'):
        s = f'CAST({expr} AS VARCHAR)'
        if family == 'date':
            return ("regexp_replace(date_format(CAST(" + expr +
                    " AS TIMESTAMP), '%Y-%m-%d %H:%i:%s'), ' 00:00:00$', '')")
        if family == 'numeric':
            return (f"regexp_replace(regexp_replace({s}, "
                    f"'(\\.[0-9]*[1-9])0+$', '$1'), '\\.0+$', '')")
        return _str(f"lower(trim(normalize({s}, NFC)))")
    return expr

# >>> Build per-column stats SQL for any supported dialect <<< #
def build_col_sql(dialect, tbl_cfg, col, dtype, full_table, cte_prefix="",
                   vintage=None, date_dtype=None, date_format=None,
                   dt_label=None, coarse=False, preset=None,
                   stats_override=None, strict=None):
    from ..constants import STATS_PRESETS
    if preset is None:
        preset = 'quick' if coarse else 'detailed'
    if preset not in STATS_PRESETS:
        raise ValueError(f"unknown preset {preset!r}; expected one of "
                         f"{list(STATS_PRESETS.keys())}")
    if stats_override is not None:
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
    strict = _resolve_strict(tbl_cfg, strict, col=col)
    numeric_strings = bool((tbl_cfg or {}).get('strict_numeric_strings'))
    family = _strict_family(dtype, numeric)
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

    if not stats['top_10']:
        col_expr = apply_null_sentinels(
            col_q, dtype or ("numeric" if numeric else "categorical"), dialect)
        if strict and not numeric:
            col_expr = _strict_normalize(dialect, col_expr, family, numeric_strings)
        n_unique_sql = (
            f"COUNT(DISTINCT {col_expr})" if stats['n_unique'] else "0"
        )
        min_sql = (
            ops['minmax_strcast'](f'MIN({col_expr})') if stats['min_max'] else "''"
        )
        max_sql = (
            ops['minmax_strcast'](f'MAX({col_expr})') if stats['min_max'] else "''"
        )
        if stats['length'] and not numeric:
            len_avg_sql = (
                ops['cast_str'](f"SUM(LENGTH(TRIM(CAST({col_expr} AS "
                                f"{'VARCHAR2(4000)' if dialect=='oracle' else 'STRING'})))) * 1.0 "
                                f"/ {ops['nullif'](f'COUNT({col_expr})')}")
            )
            len_max_sql = ops['cast_str'](
                f"MAX(LENGTH(TRIM(CAST({col_expr} AS "
                f"{'VARCHAR2(4000)' if dialect=='oracle' else 'STRING'}))))"
            )
        else:
            len_avg_sql = "''"
            len_max_sql = "''"
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

    col_expr_cat = apply_null_sentinels(col_q, dtype or "categorical", dialect)
    if strict:
        col_expr_cat = _strict_normalize(dialect, col_expr_cat, family, numeric_strings)
    p_col_substr = f"SUBSTR({ops['cast_str'](col_expr_cat)}, 1, 200)"
    minmax_concat_min = ops['concat']("TRIM(p_col)", "'='", ops['cast_str']("cnt"))
    minmax_concat_max = minmax_concat_min

    if coarse:
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
           SUM(cnt * LENGTH(TRIM(p_col))) * 1.0 / {ops['nullif']('SUM(cnt)')} AS length_avg,
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
           sum_cnt_rnk * 1.0 / {ops['nullif']('sum_cnt')} AS mean_v,
           SQRT(GREATEST(
               sum_cnt_rnk2 * 1.0 / {ops['nullif']('sum_cnt')}
               - (sum_cnt_rnk * 1.0 / {ops['nullif']('sum_cnt')})
               * (sum_cnt_rnk * 1.0 / {ops['nullif']('sum_cnt')}), 0)) AS std_v
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
           SUM(cnt * LENGTH(TRIM(p_col))) * 1.0 / {ops['nullif']('SUM(cnt)')} AS length_avg,
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

# >>> Streaming-aggregate fallback. Same row schema as the full per-column <<< #
def build_lifeboat_col_sql(dialect, tbl_cfg, col, dtype, full_table,
                            cte_prefix="", vintage=None,
                            date_dtype=None, date_format=None, dt_label=None):
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

# >>> Parse an extract_{row,col}.sql block file written by any platform's <<< #
def parse_extract_sql_file(sql_path):
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
        if m:
            sql = conn_re.sub('', sql).strip()
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

# >>> AND-combine WHERE fragments, dropping empty / 1=1 placeholders <<< #
def and_clauses(*parts):
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

# >>> Generic runner for extract_{row,col}.sql block files <<< #
def run_extract_sql_file(sql_path, outdir, *,
                          query_fn,
                          platform,
                          max_workers=None,
                          on_progress=None,
                          cancel_event=None,
                          row_csv_writer=None):
    import csv
    import sys
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    from ..constants import COL_CSV_HEADERS, ROW_CSV_HEADERS, PLATFORM_RUNNER
    from ..logmgr import get_logger

    cfg = PLATFORM_RUNNER[platform]
    platform.capitalize()

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

    table_blocks = {}
    for block in blocks:
        qname = block['name'].split('/')[0] if '/' in block['name'] else block['name']
        table_blocks.setdefault(qname, []).append(block)
    _col_rows_lock = threading.Lock()
    _col_rows_by_table = {}

    print(f"  Parsed {len(blocks)} blocks ({len(table_blocks)} tables) "
          f"from {sql_path}, workers={_max_workers}", file=sys.stderr)

    def _default_row_writer(rows, csv_path):
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
        get_logger().info("query %s [%s]: %s (%s rows, %.1fs) [%d/%d]",
                          block_name, qname, "ok" if result["ok"] else "FAIL",
                          result.get("rows", 0), elapsed, n, total)

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
