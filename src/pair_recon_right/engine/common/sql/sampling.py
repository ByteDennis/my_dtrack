from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..model.hashing import HASH_BUCKET_SPACE, KeyFamily, key_families, spark_literal


class SamplingSqlNotSupported(ValueError):
    pass


def _spark_dialect(dialect: str) -> str:
    normalized = {"databricks": "spark"}.get(dialect.lower(), dialect.lower())
    if normalized != "spark":
        raise SamplingSqlNotSupported(
            f"the Right package supports only Spark/Databricks sampling SQL, got {dialect!r}"
        )
    return normalized


def _literal(value: object) -> str:
    return "NULL" if value is None else spark_literal(str(value))


def normalize_sql(
    expr: str,
    family: KeyFamily,
    dialect: str,
    side: str,
    data_type: str | None = None,
) -> str:
    del data_type
    dialect = _spark_dialect(dialect)
    if hasattr(family, "sql_dialect"):
        return family.sql_dialect(expr, dialect, side)
    return family.sql(expr, side)


def composite_sql(
    key_columns: Iterable[str],
    families: Iterable[Any],
    dialect: str,
    side: str,
    key_types: Iterable[str | None] | None = None,
) -> str:
    _spark_dialect(dialect)
    columns = list(key_columns)
    resolved = key_families(families, len(columns))
    types = [*(key_types or [])]
    types += [None] * (len(columns) - len(types))
    parts = [
        normalize_sql(column, family, "spark", side, data_type)
        for column, family, data_type in zip(columns, resolved, types)
    ]
    if not parts:
        raise ValueError("sampling requires key columns")
    return "concat_ws('|', " + ", ".join(parts) + ")"


def hash_hex_sql(expr: str, dialect: str) -> str:
    _spark_dialect(dialect)
    return f"LOWER(md5({expr}))"


def hash32_sql(expr: str, dialect: str) -> str:
    _spark_dialect(dialect)
    return f"conv(substr(md5({expr}), 1, 8), 16, 10)"


def sample_predicate_sql(
    plan: dict[str, Any] | None,
    key_columns: Iterable[str],
    families: Iterable[Any],
    dialect: str,
    side: str,
    key_tuples: Iterable[Iterable[object]] | None = None,
    key_types: Iterable[str | None] | None = None,
) -> str | None:
    _spark_dialect(dialect)
    plan = plan or {"mode": "all"}
    columns = list(key_columns)
    if plan["mode"] == "all" or not columns:
        return None
    composite = composite_sql(columns, families, "spark", side, key_types)
    if plan["mode"] == "fraction":
        threshold = int(
            plan.get("threshold") or max(1, round(float(plan["rate"]) * HASH_BUCKET_SPACE))
        )
        return f"{hash32_sql(composite, 'spark')} < {threshold}"
    if plan["mode"] != "fixed":
        raise ValueError(f"unsupported sample mode: {plan['mode']}")
    resolved = key_families(families, len(columns))
    values = list(
        dict.fromkeys(
            _literal(
                "|".join(family.normalize(value, "left") for value, family in zip(row, resolved))
            )
            for row in (key_tuples or [])
        )
    )
    if not values:
        return "1=0"
    return f"{composite} IN ({', '.join(values)})"
