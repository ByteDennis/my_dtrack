from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta


def canonical_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d%b%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()  # noqa: DTZ007
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        raise ValueError(f"unsupported date value: {value!r}") from None


def bucket_date(value: str, vintage: str) -> str:
    day = date.fromisoformat(canonical_date(value))
    if vintage == "day":
        return day.isoformat()
    if vintage == "week":
        return (day - timedelta(days=day.weekday())).isoformat()
    if vintage == "month":
        return day.replace(day=1).isoformat()
    if vintage == "quarter":
        return day.replace(month=((day.month - 1) // 3) * 3 + 1, day=1).isoformat()
    if vintage == "year":
        return day.replace(month=1, day=1).isoformat()
    raise ValueError(f"unsupported vintage: {vintage}")


def apply_synonyms(
    counts: dict[str, int], synonyms: dict[str, str] | None = None
) -> dict[str, int]:
    synonyms = synonyms or {}
    result: dict[str, int] = defaultdict(int)
    for raw, count in counts.items():
        canonical = synonyms.get(str(raw), canonical_date(raw))
        result[canonical_date(canonical)] += int(count)
    return dict(result)


def contiguous_runs(partitions: list[str]) -> list[tuple[str, str]]:
    days = sorted({date.fromisoformat(canonical_date(item)) for item in partitions})
    if not days:
        return []
    runs: list[tuple[str, str]] = []
    start = previous = days[0]
    for current in days[1:]:
        if current != previous + timedelta(days=1):
            runs.append((start.isoformat(), previous.isoformat()))
            start = current
        previous = current
    runs.append((start.isoformat(), previous.isoformat()))
    return runs


def quote_ident(name: str, dialect: str) -> str:
    if dialect == "spark":
        return ".".join("`" + part.replace("`", "``") + "`" for part in name.split("."))
    if dialect == "oracle":
        return name
    if dialect in {"athena", "hadoop", "databricks"}:
        return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in name.split("."))
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


NATIVE_DAY_TYPES = frozenset({"date", "timestamp", "datetime"})
COMPACT_STRING_TYPES = frozenset({"string_compact"})
SAS_DATE9_TYPES = frozenset({"string_sas", "string_sas_date9", "sas_date9"})
SAS_DAY_TYPES = frozenset({"sas_days", "sas_date"})
DAY_TYPES = NATIVE_DAY_TYPES | SAS_DATE9_TYPES | SAS_DAY_TYPES


def _sas_date9_expr(column: str, dialect: str) -> str:
    text = f"TRIM({column})"
    if dialect == "oracle":
        return f"TO_DATE({text}, 'DDMONYYYY', 'NLS_DATE_LANGUAGE=English')"
    if dialect in {"spark", "databricks", "hadoop"}:
        return f"to_date({text}, 'ddMMMyyyy')"
    if dialect == "athena":
        return f"CAST(date_parse({text}, '%d%b%Y') AS DATE)"
    if dialect == "duckdb":
        return f"strptime(TRIM(CAST({column} AS VARCHAR)), '%d%b%Y')::DATE"
    raise ValueError(f"string_sas dates do not support dialect {dialect!r}")


def _sas_day_number_expr(column: str, dialect: str) -> str:
    if dialect == "oracle":
        return f"TRUNC(TO_NUMBER({column}))"
    return f"CAST(CAST({column} AS DOUBLE) AS BIGINT)"


def _sas_day_expr(column: str, dialect: str) -> str:
    number = _sas_day_number_expr(column, dialect)
    if dialect == "oracle":
        return f"DATE '1960-01-01' + {number}"
    if dialect in {"spark", "databricks", "hadoop"}:
        return f"date_add(DATE '1960-01-01', CAST({number} AS INT))"
    if dialect == "athena":
        return f"date_add('day', {number}, DATE '1960-01-01')"
    if dialect == "duckdb":
        return f"DATE '1960-01-01' + CAST({number} AS INTEGER)"
    raise ValueError(f"sas_days dates do not support dialect {dialect!r}")


def _predicate_expr(date_col: str, dialect: str, date_type: str | None) -> str:
    column = quote_ident(date_col, dialect)
    kind = (date_type or "").lower()
    if kind in SAS_DATE9_TYPES:
        return _sas_date9_expr(column, dialect)
    if kind in SAS_DAY_TYPES:
        return _sas_day_number_expr(column, dialect)
    return column


# >>> the partition key of a row: the column itself, or its calendar day when it can carry a time <<< #
def day_expr(date_col: str, dialect: str, date_type: str | None) -> str:
    column = quote_ident(date_col, dialect)
    kind = (date_type or "").lower()
    if kind in SAS_DATE9_TYPES:
        return _sas_date9_expr(column, dialect)
    if kind in SAS_DAY_TYPES:
        return _sas_day_expr(column, dialect)
    if kind not in NATIVE_DAY_TYPES:
        return column
    if dialect == "oracle":
        return f"TRUNC({column})"
    if dialect in {"spark", "hadoop"}:
        return f"to_date({column})"
    return f"CAST({column} AS DATE)"


# >>> the ISO day after `value` <<< #
def next_day(value: str) -> str:
    return (date.fromisoformat(value) + timedelta(days=1)).isoformat()


def date_literal(value: str, dialect: str, date_type: str | None) -> str:
    kind = (date_type or "").lower()
    if kind in NATIVE_DAY_TYPES | SAS_DATE9_TYPES:
        return f"DATE '{value}'"
    if kind in SAS_DAY_TYPES:
        origin = date(1960, 1, 1)
        return str((date.fromisoformat(canonical_date(value)) - origin).days)
    if kind in COMPACT_STRING_TYPES:
        return "'" + canonical_date(value).replace("-", "") + "'"
    if kind in {"num", "integer", "int", "number"}:
        return value.replace("-", "")
    return "'" + value.replace("'", "''") + "'"


def partition_where(
    date_col: str,
    partitions: list[str],
    *,
    dialect: str,
    date_type: str | None = None,
) -> str:
    column = _predicate_expr(date_col, dialect, date_type)
    days = (date_type or "").lower() in DAY_TYPES
    clauses = []
    for start, end in contiguous_runs(partitions):
        lo = date_literal(start, dialect, date_type)
        if days:
            after = date_literal(next_day(end), dialect, date_type)
            clauses.append(f"{column} >= {lo} AND {column} < {after}")
            continue
        hi = date_literal(end, dialect, date_type)
        clauses.append(f"{column} = {lo}" if start == end else f"{column} BETWEEN {lo} AND {hi}")
    if not clauses:
        return "1=0"
    return "(" + " OR ".join(f"({item})" for item in clauses) + ")"


def bounds_where(
    date_col: str,
    *,
    dialect: str,
    date_type: str | None,
    from_date: str | None,
    to_date: str | None,
) -> str | None:
    column = _predicate_expr(date_col, dialect, date_type)
    clauses = []
    if from_date:
        clauses.append(f"{column} >= {date_literal(from_date, dialect, date_type)}")
    if to_date and (date_type or "").lower() in DAY_TYPES:
        clauses.append(f"{column} < {date_literal(next_day(to_date), dialect, date_type)}")
    elif to_date:
        clauses.append(f"{column} <= {date_literal(to_date, dialect, date_type)}")
    return " AND ".join(clauses) or None
