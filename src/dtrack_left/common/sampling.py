from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .hashing import HASH_BUCKET_SPACE, NULL, KeyFamily, key_families, spark_literal


class SamplingSqlNotSupported(ValueError):
    pass


def _dialect(value: str) -> str:
    return {
        "aws": "athena",
        "trino": "athena",
        "databricks": "spark",
        "hive": "hadoop",
    }.get(value.lower(), value.lower())


def _literal(value: object, dialect: str) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if _dialect(dialect) in {"spark", "hadoop"}:
        return spark_literal(text)
    return "'" + text.replace("'", "''") + "'"


def _cast_text(expr: str, dialect: str, data_type: str | None = None) -> str:
    dialect = _dialect(dialect)
    if dialect == "oracle":
        kind = str(data_type or "").upper()
        if "TIMESTAMP" in kind:
            rendered = f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
            return f"REGEXP_REPLACE({rendered}, '\\.000000$', '')"
        if "DATE" in kind:
            return f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        if any(
            name in kind for name in ("NUMBER", "FLOAT", "DOUBLE", "INTEGER", "DECIMAL", "NUMERIC")
        ):
            return f"TO_CHAR({expr}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')"
        return f"TO_CHAR({expr})"
    kind = "STRING" if dialect in {"spark", "hadoop"} else "VARCHAR"
    return f"CAST({expr} AS {kind})"


def _regex_replace(expr: str, pattern: str, replacement: str, dialect: str) -> str:
    dialect = _dialect(dialect)
    args = f"{expr}, {_literal(pattern, dialect)}, {_literal(replacement, dialect)}"
    if dialect == "duckdb":
        return f"regexp_replace({args}, 'g')"
    name = "REGEXP_REPLACE" if dialect == "oracle" else "regexp_replace"
    return f"{name}({args})"


def _regex_matches(expr: str, pattern: str, dialect: str) -> str:
    dialect = _dialect(dialect)
    literal = _literal(pattern, dialect)
    if dialect == "duckdb":
        return f"regexp_full_match({expr}, {literal})"
    if dialect == "oracle":
        return f"REGEXP_LIKE({expr}, {literal})"
    if dialect == "athena":
        return f"regexp_like({expr}, {literal})"
    return f"{expr} rlike {literal}"


def _date_sql(expr: str, dialect: str) -> str:
    dialect = _dialect(dialect)
    if dialect in {"spark", "hadoop"}:
        undashed = f"replace({expr}, '-', '')"
        day = (
            f"CASE WHEN {expr} rlike '^[0-9]{{8}}$' "
            f"THEN cast(try_to_timestamp({expr}, 'yyyyMMdd') as date) "
            f"WHEN {expr} rlike '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' "
            f"THEN cast(try_to_timestamp({expr}, 'M/d/yyyy') as date) "
            f"WHEN {expr} rlike '^[0-9]{{1,2}}-?[A-Za-z]{{3}}-?[0-9]{{4}}$' "
            f"THEN cast(try_to_timestamp({undashed}, 'dMMMyyyy') as date) "
            f"ELSE try_cast({expr} as date) END"
        )
        return f"coalesce(date_format({day}, 'yyyy-MM-dd'), '')"
    if dialect == "duckdb":
        parsed = (
            f"coalesce(try_strptime({expr}, '%Y%m%d'), try_strptime({expr}, '%m/%d/%Y'), "
            f"try_strptime(replace({expr}, '-', ''), '%d%b%Y'), try_cast({expr} as timestamp))"
        )
        return f"coalesce(strftime({parsed}, '%Y-%m-%d'), '')"
    if dialect == "athena":
        parsed = (
            f"coalesce(try(date_parse({expr}, '%Y%m%d')), try(date_parse({expr}, '%m/%d/%Y')), "
            f"try(date_parse(replace({expr}, '-', ''), '%d%b%Y')), try(cast({expr} as timestamp)))"
        )
        return f"coalesce(date_format({parsed}, '%Y-%m-%d'), '')"
    if dialect == "oracle":
        parsed = (
            f"COALESCE(TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'YYYYMMDD'), "
            f"TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'MM/DD/YYYY'), "
            f"TO_DATE(REPLACE({expr}, '-', '') DEFAULT NULL ON CONVERSION ERROR, "
            f"'DDMONYYYY', 'NLS_DATE_LANGUAGE=English'), "
            f"TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS.FF') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD\"T\"HH24:MI:SS.FF') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD\"T\"HH24:MI:SS') AS DATE))"
        )
        return f"COALESCE(TO_CHAR({parsed}, 'YYYY-MM-DD'), '')"
    raise SamplingSqlNotSupported(f"sampling SQL does not support dialect {dialect!r}")


def _step_sql(expr: str, name: str, arg: str | None, dialect: str) -> str:
    dialect = _dialect(dialect)
    if name == "trim":
        return _regex_replace(expr, r"^\s+|\s+$", "", dialect)
    if name == "lower":
        return f"lower({expr})"
    if name == "upper":
        return f"upper({expr})"
    if name == "collapse_decimal":
        replacement = r"\1" if dialect in {"duckdb", "oracle"} else "$1"
        collapsed = _regex_replace(expr, r"(\.[0-9]*[1-9])0+$", replacement, dialect)
        collapsed = _regex_replace(collapsed, r"\.0+$", "", dialect)
        decimal_pattern = r"^-?[0-9]+\.[0-9]+$"
        return (
            f"CASE WHEN {_regex_matches(expr, decimal_pattern, dialect)} "
            f"THEN {collapsed} ELSE {expr} END"
        )
    if name == "strip_leading_zeros":
        replacement = r"\1" if dialect in {"duckdb", "oracle"} else "$1"
        text = _regex_replace(expr, r"^\+", "", dialect)
        text = _regex_replace(text, r"^-0+([0-9])", "-" + replacement, dialect)
        return _regex_replace(text, r"^0+([0-9])", replacement, dialect)
    if name == "digits":
        return _regex_replace(expr, "[^0-9]", "", dialect)
    if name == "alnum":
        return _regex_replace(expr, "[^0-9A-Za-z]", "", dialect)
    if name == "date":
        return _date_sql(expr, dialect)
    if name == "lpad":
        size = int(arg or "0")
        return f"CASE WHEN length({expr}) >= {size} THEN {expr} ELSE lpad({expr}, {size}, '0') END"
    if name == "strip_prefix":
        prefix = _literal(arg or "", dialect)
        size = len(arg or "")
        return (
            f"CASE WHEN substr({expr}, 1, {size}) = {prefix} "
            f"THEN substr({expr}, {size + 1}) ELSE {expr} END"
        )
    if name == "strip_suffix":
        suffix = _literal(arg or "", dialect)
        size = len(arg or "")
        return (
            f"CASE WHEN length({expr}) >= {size} AND substr({expr}, -{size}) = {suffix} "
            f"THEN substr({expr}, 1, length({expr}) - {size}) ELSE {expr} END"
        )
    raise SamplingSqlNotSupported(f"sampling SQL does not support key step {name!r} for {dialect}")


def normalize_sql(
    expr: str,
    family: KeyFamily,
    dialect: str,
    side: str,
    data_type: str | None = None,
) -> str:
    dialect = _dialect(dialect)
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
    if hasattr(family, "sql_dialect"):
        return family.sql_dialect(expr, dialect, side)
    if dialect == "spark":
        return family.sql(expr, side)
    methods = ("transform", "transform_sql", f"{side}_transform", f"{side}_transform_sql")
    overridden = [
        method
        for method in methods
        if getattr(type(family), method) is not getattr(KeyFamily, method)
    ]
    if overridden:
        raise SamplingSqlNotSupported(
            f"custom key family {family.name!r} needs {dialect} SQL for {', '.join(overridden)}"
        )
    steps = family.left_steps if side == "left" else family.right_steps
    parsed = family._parsed if steps is None else family._left if side == "left" else family._right
    text = _cast_text(expr, dialect, data_type)
    for step, arg in parsed:
        text = _step_sql(text, step.name, arg, dialect)
    return f"CASE WHEN ({text}) IS NULL OR ({text}) = '' THEN '{NULL}' ELSE ({text}) END"


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
        if _dialect(dialect) == "spark":
            return "concat_ws('|', " + parts[0] + ")"
        return parts[0]
    if _dialect(dialect) == "oracle":
        return " || '|' || ".join(parts)
    return "concat_ws('|', " + ", ".join(parts) + ")"


def hash_hex_sql(expr: str, dialect: str) -> str:
    dialect = _dialect(dialect)
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
    dialect = _dialect(dialect)
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
            _literal(
                "|".join(family.normalize(value, "left") for value, family in zip(row, resolved)),
                dialect,
            )
            for row in (key_tuples or [])
        )
    )
    if not values:
        return "1=0"
    chunk_size = 1000 if _dialect(dialect) == "oracle" else len(values)
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
    dialect = _dialect(dialect)
    columns = list(key_columns)
    composite = composite_sql(columns, families, dialect, "left", key_types)
    selected = ", ".join(columns)
    inner = (
        f"SELECT {selected}, {hash_hex_sql(composite, dialect)} AS dtrack_rank, "
        f"{composite} AS dtrack_norm FROM {relation} WHERE {where} GROUP BY {selected}"
    )
    limit = f"FETCH FIRST {int(count)} ROWS ONLY" if dialect == "oracle" else f"LIMIT {int(count)}"
    return (
        f"SELECT {selected} FROM ({inner}) dtrack_keys "
        f"ORDER BY dtrack_rank, dtrack_norm, {selected} {limit}"
    )
