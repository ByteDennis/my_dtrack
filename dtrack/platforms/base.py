"""Base class and shared helpers for platform builders."""

import os
import re
from abc import ABC, abstractmethod

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

    - Athena / Trino / Oracle: ANSI double quotes  →  "123col"
    - Hive / Hadoop:           backticks            →  `123col`
    - SAS:                     no change            (caller handles via macro var)

    Conservative: leaves valid bare names unchanged so existing extractions
    keep their case-insensitive resolution against Oracle/Hive tables. Quoted
    identifiers are case-sensitive in Oracle and case-preserving in Athena —
    blindly quoting every column would break configs that store columns in
    one case and reference them in another.
    """
    if not name or _BARE_IDENT_RE.match(name):
        return name
    if dialect == "hadoop":
        return f"`{name}`"
    return f'"{name}"'


def _sentinel_dialect_key(dialect):
    """Map SQL dialect string to NULL_SENTINELS top-level key."""
    d = (dialect or "").lower()
    if d in ("athena", "trino", "aws"):
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
                               is_sas=False, date_format=None, custom_date_types=None,
                               excluded_dates=None):
    """Build a BETWEEN clause covering [min(matching_dates), max(matching_dates)],
    optionally with `AND NOT (col IN (excluded_dates))` for real-diff days.

    `excluded_dates` is the row_comparison-derived list of days INSIDE the
    overlap range that the user wants the extract to skip -- mismatched
    counts and misloads (where one side has data and the other has zero).
    It is NOT a generic gap list: days where BOTH sides are zero rows
    are intentionally absent. Those produce empty extracts naturally
    (nothing to scan), so listing them in NOT IN would only bloat the
    SQL and prevent partition pruning.

    User-typed excludes go through a separate `_exclude_dates` /
    `exclude_clause` plumbed by the AWS/SAS drivers, not through here.

    Note: an earlier version auto-computed gap_dates = (full range) -
    matching_dates and shoved them into NOT IN. That lumped both-zero
    days in with real diffs and produced huge NOT IN lists. Caller now
    supplies the curated list explicitly.
    """
    bmin, bmax = min(matching_dates), max(matching_dates)
    between = build_date_between_clause(
        date_col, bmin, bmax, date_dtype,
        is_sas=is_sas, date_format=date_format,
        custom_date_types=custom_date_types,
    )
    if not excluded_dates:
        return between
    # Filter excludes to the bucket window. excluded_dates passed in from
    # the caller may span the whole overlap range; only the ones inside
    # the current bucket apply to this WHERE clause.
    in_window = [d for d in excluded_dates if bmin <= d <= bmax]
    if not in_window:
        return between
    not_in = build_date_in_clause(
        date_col, in_window, date_dtype,
        is_sas=is_sas, date_format=date_format,
        custom_date_types=custom_date_types,
    )
    return f"{between} AND NOT ({not_in})"


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
            # Pull the row_comparison-derived overlap window + real-diff
            # excludes so downstream WHERE builders can emit
            # `BETWEEN overlap_start AND overlap_end AND NOT (col IN
            # (excluded_dates))`. excluded_dates contains the days inside
            # the overlap range where left and right disagree on non-zero
            # row counts (mismatched + misload). Days where both sides
            # are zero stay OUT of this list -- they produce empty
            # extracts naturally and don't deserve NOT IN bloat.
            result['overlap_start'] = comp.get('overlap_start')
            result['overlap_end']   = comp.get('overlap_end')
            result['excluded_dates'] = list(comp.get('excluded_dates') or [])

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
