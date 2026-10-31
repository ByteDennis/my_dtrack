from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..model.hashing import (
    HASH_BUCKET_SPACE,
    KeyFamily,
    SamplingSqlNotSupported,
    key_families,
    spark_literal,
)
from .dates import quote_ident

MODULAR_BUCKET_SPACE = 2_147_483_647
MODULAR_MULTIPLIER = 1_103_515_245


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
    return family.sql_dialect(expr, _spark_dialect(dialect), side, data_type)


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


def modular_sample_condition(column: str, rate: float, seed: int, dialect: str) -> str:
    _spark_dialect(dialect)
    numeric = f"try_cast({quote_ident(column, 'spark')} AS DECIMAL(38,0))"
    residue = f"MOD(MOD({numeric}, {MODULAR_BUCKET_SPACE}) + {MODULAR_BUCKET_SPACE}, {MODULAR_BUCKET_SPACE})"
    bucket = (
        f"MOD(({residue} * {MODULAR_MULTIPLIER}) + "
        f"{int(seed) % MODULAR_BUCKET_SPACE}, {MODULAR_BUCKET_SPACE})"
    )
    threshold = max(1, round(float(rate) * MODULAR_BUCKET_SPACE))
    return f"({numeric} IS NOT NULL AND {bucket} < {threshold})"


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
    if plan["mode"] == "fraction":
        if plan.get("modular_column"):
            return modular_sample_condition(
                str(plan["modular_column"]),
                float(plan["rate"]),
                int(plan["modular_seed"]),
                "spark",
            )
        composite = composite_sql(columns, families, "spark", side, key_types)
        threshold = int(
            plan.get("threshold") or max(1, round(float(plan["rate"]) * HASH_BUCKET_SPACE))
        )
        return f"{hash32_sql(composite, 'spark')} < {threshold}"
    if plan["mode"] != "fixed":
        raise ValueError(f"unsupported sample mode: {plan['mode']}")
    composite = composite_sql(columns, families, "spark", side, key_types)
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
