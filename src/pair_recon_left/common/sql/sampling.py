from __future__ import annotations

import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from ..model.hashing import (
    HASH_BUCKET_SPACE,
    KeyFamily,
    SamplingSqlNotSupported,
    composite_key,
    dialect_name,
    hash32,
    key_families,
    sql_literal,
)


# >>> key normalization SQL for one side; timezone-bearing Oracle keys have no canonical text <<< #
def normalize_sql(
    expr: str,
    family: KeyFamily,
    dialect: str,
    side: str,
    data_type: str | None = None,
) -> str:
    dialect = dialect_name(dialect)
    kind = str(data_type or "").upper()
    if dialect == "oracle" and (
        "TIMESTAMP_TZ" in kind
        or "TIMESTAMP_LTZ" in kind
        or "WITH TIME ZONE" in kind
        or "WITH LOCAL TIME ZONE" in kind
    ):
        raise SamplingSqlNotSupported(
            "timezone-bearing Oracle keys need an explicit canonical source expression"
        )
    return family.sql_dialect(expr, dialect, side, data_type)


def composite_sql(
    key_columns: Iterable[str],
    families: Iterable[Any],
    dialect: str,
    side: str,
    key_types: Iterable[str | None] | None = None,
) -> str:
    columns = list(key_columns)
    resolved = key_families(families, len(columns))
    types = [*(key_types or [])]
    types += [None] * (len(columns) - len(types))
    parts = [
        normalize_sql(column, family, dialect, side, data_type)
        for column, family, data_type in zip(columns, resolved, types)
    ]
    if not parts:
        raise ValueError("sampling requires key columns")
    if len(parts) == 1:
        if dialect_name(dialect) == "spark":
            return "concat_ws('|', " + parts[0] + ")"
        return parts[0]
    if dialect_name(dialect) == "oracle":
        return " || '|' || ".join(parts)
    return "concat_ws('|', " + ", ".join(parts) + ")"


def hash_hex_sql(expr: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    if dialect == "oracle":
        return f"LOWER(RAWTOHEX(STANDARD_HASH(UTL_I18N.STRING_TO_RAW({expr}, 'AL32UTF8'), 'MD5')))"
    if dialect == "athena":
        return f"LOWER(TO_HEX(MD5(TO_UTF8({expr}))))"
    if dialect in {"spark", "hadoop"}:
        return f"LOWER(md5({expr}))"
    if dialect == "duckdb":
        return f"LOWER(md5({expr}))"
    raise SamplingSqlNotSupported(f"sampling hash does not support dialect {dialect!r}")


def hash32_sql(expr: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    hashed = hash_hex_sql(expr, dialect)
    if dialect == "oracle":
        return f"TO_NUMBER(SUBSTR({hashed}, 1, 8), 'XXXXXXXX')"
    if dialect == "athena":
        return f"from_base(SUBSTR({hashed}, 1, 8), 16)"
    if dialect == "spark":
        return f"conv(substr(md5({expr}), 1, 8), 16, 10)"
    if dialect == "hadoop":
        return f"CAST(conv(SUBSTR({hashed}, 1, 8), 16, 10) AS BIGINT)"
    if dialect == "duckdb":
        return f"CAST(('0x' || SUBSTR({hashed}, 1, 8)) AS BIGINT)"
    raise SamplingSqlNotSupported(f"sampling hash does not support dialect {dialect!r}")


def sample_predicate_sql(
    plan: dict[str, Any] | None,
    key_columns: Iterable[str],
    families: Iterable[Any],
    dialect: str,
    side: str,
    key_tuples: Iterable[Iterable[object]] | None = None,
    key_types: Iterable[str | None] | None = None,
) -> str | None:
    plan = plan or {"mode": "all"}
    columns = list(key_columns)
    if plan["mode"] == "all" or not columns:
        return None
    composite = composite_sql(columns, families, dialect, side, key_types)
    if plan["mode"] == "fraction":
        threshold = int(
            plan.get("threshold") or max(1, round(float(plan["rate"]) * HASH_BUCKET_SPACE))
        )
        return f"{hash32_sql(composite, dialect)} < {threshold}"
    if plan["mode"] != "fixed":
        raise ValueError(f"unsupported sample mode: {plan['mode']}")
    resolved = key_families(families, len(columns))
    values = list(
        dict.fromkeys(
            sql_literal(
                "|".join(family.normalize(value, "left") for value, family in zip(row, resolved)),
                dialect,
            )
            for row in (key_tuples or [])
        )
    )
    if not values:
        return "1=0"
    chunk_size = 1000 if dialect_name(dialect) == "oracle" else len(values)
    predicates = [
        f"{composite} IN ({', '.join(values[start : start + chunk_size])})"
        for start in range(0, len(values), chunk_size)
    ]
    return predicates[0] if len(predicates) == 1 else "(" + " OR ".join(predicates) + ")"


def fixed_keys_sql(
    relation: str,
    where: str,
    key_columns: Iterable[str],
    families: Iterable[Any],
    dialect: str,
    count: int,
    key_types: Iterable[str | None] | None = None,
) -> str:
    dialect = dialect_name(dialect)
    columns = list(key_columns)
    composite = composite_sql(columns, families, dialect, "left", key_types)
    selected = ", ".join(columns)
    inner = (
        f"SELECT {selected}, {hash_hex_sql(composite, dialect)} AS pair_recon_rank, "
        f"{composite} AS pair_recon_norm FROM {relation} WHERE {where} GROUP BY {selected}"
    )
    limit = f"FETCH FIRST {int(count)} ROWS ONLY" if dialect == "oracle" else f"LIMIT {int(count)}"
    return (
        f"SELECT {selected} FROM ({inner}) pair_recon_keys "
        f"ORDER BY pair_recon_rank, pair_recon_norm, {selected} {limit}"
    )


def sample_csv(
    path: str | Path,
    *,
    key_columns: list[str],
    mode: str,
    rate: float | None = None,
    count: int | None = None,
    families: list[object] | None = None,
    key_tuples: list[list[object]] | None = None,
    side: str = "left",
) -> Path:
    if mode == "all":
        return Path(path)
    if not key_columns:
        raise ValueError("sampling requires key columns")
    frame = pd.read_csv(path, dtype=object)
    resolved = key_families(families, len(key_columns))
    composite = pd.Series(
        [composite_key(list(row), resolved, side) for row in frame[key_columns].itertuples(False)],
        index=frame.index,
        dtype=object,
    )
    if mode == "fraction":
        threshold = max(1, round(float(rate) * HASH_BUCKET_SPACE))
        sampled = frame[composite.map(hash32) < threshold]
    elif mode == "fixed":
        if key_tuples is not None:
            wanted = {composite_key(list(row), resolved, "left") for row in key_tuples}
            sampled = frame[composite.isin(wanted)]
        else:
            ranked = frame.assign(
                __rank=composite.map(lambda text: hashlib.md5(text.encode()).hexdigest())
            )
            keys = ranked.sort_values("__rank")[key_columns].drop_duplicates().head(int(count))
            sampled = frame.merge(keys, on=key_columns, how="inner")
    else:
        raise ValueError(f"unsupported sample mode: {mode}")
    output = Path(path).with_name(Path(path).stem + ".sampled.csv")
    sampled.to_csv(output, index=False)
    return output
