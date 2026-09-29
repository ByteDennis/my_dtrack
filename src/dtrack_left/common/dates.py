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
    raise ValueError(f"unsupported date value: {value!r}")


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
    if dialect in {"oracle", "athena", "hadoop", "databricks"}:
        return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in name.split("."))
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def date_literal(value: str, dialect: str, date_type: str | None) -> str:
    kind = (date_type or "").lower()
    if kind in {"date", "timestamp", "datetime"}:
        return f"DATE '{value}'"
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
    column = quote_ident(date_col, dialect)
    clauses = []
    for start, end in contiguous_runs(partitions):
        lo = date_literal(start, dialect, date_type)
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
    column = quote_ident(date_col, dialect)
    clauses = []
    if from_date:
        clauses.append(f"{column} >= {date_literal(from_date, dialect, date_type)}")
    if to_date:
        clauses.append(f"{column} <= {date_literal(to_date, dialect, date_type)}")
    return " AND ".join(clauses) or None
