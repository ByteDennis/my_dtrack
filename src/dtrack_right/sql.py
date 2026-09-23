from __future__ import annotations

from .dates import contiguous_runs


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
