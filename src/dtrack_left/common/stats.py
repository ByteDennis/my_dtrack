from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .column_types import resolve_column_type
from .hashing import collapse_decimal


def _hash16(value: object) -> int:
    text = "<<NULL>>" if value is None or pd.isna(value) else str(value)
    return int(hashlib.sha256(text.encode("utf-8")).hexdigest()[:16], 16)


def _top10(series: pd.Series) -> str:
    counts = series.dropna().astype(str).value_counts(dropna=False).head(10)
    return json.dumps(
        [{"value": value, "count": int(count)} for value, count in counts.items()],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def column_statistics(
    series: pd.Series, *, strict: bool = False, col_type: str | None = None
) -> dict[str, Any]:
    values = series.copy()
    if strict:
        values = values.map(
            lambda value: (
                value
                if value is None or pd.isna(value)
                else collapse_decimal(str(value).strip()).lower()
            )
        )
    non_null = values.dropna()
    numeric = pd.to_numeric(non_null, errors="coerce")
    is_numeric = len(non_null) > 0 and numeric.notna().all()
    if col_type:
        is_numeric = col_type == "numeric"
        numeric = numeric.dropna()
    if is_numeric:
        values = values.map(
            lambda value: (
                value if value is None or pd.isna(value) else collapse_decimal(str(value).strip())
            )
        )
        non_null = values.dropna()
    n_total = len(values)
    n_missing = int(values.isna().sum())
    n_unique = int(non_null.nunique(dropna=True))
    common: dict[str, Any] = {
        "col_type": "numeric" if is_numeric else "categorical",
        "n_total": n_total,
        "n_missing": n_missing,
        "n_unique": n_unique,
        "hash_sum": str(sum(_hash16(value) for value in values)),
    }
    if is_numeric:
        common.update(
            mean=None if numeric.empty else float(numeric.mean()),
            std=None if len(numeric) < 2 else float(numeric.std(ddof=1)),
            min_val=None if numeric.empty else str(numeric.min()),
            max_val=None if numeric.empty else str(numeric.max()),
            length_avg=None,
            length_max=None,
            top_10="",
        )
    else:
        strings = non_null.astype(str)
        common.update(
            mean=None,
            std=None,
            min_val=None if strings.empty else strings.min(),
            max_val=None if strings.empty else strings.max(),
            length_avg=None if strings.empty else float(strings.str.len().mean()),
            length_max=None if strings.empty else int(strings.str.len().max()),
            top_10=_top10(values),
        )
    return common


def compute_csv_statistics(
    path: str | Path,
    columns: list[str],
    *,
    strict: bool = False,
    col_types: dict[str, str] | None = None,
    column_types: dict[str, Any] | None = None,
    side: str = "left",
) -> dict[str, dict[str, Any]]:
    import duckdb

    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def collapse(expr: str) -> str:
        trimmed = f"trim({expr})"
        collapsed = (
            f"regexp_replace(regexp_replace({trimmed}, "
            "'(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')"
        )
        return (
            f"CASE WHEN regexp_matches({trimmed}, '^-?[0-9]+\\.[0-9]+$') "
            f"THEN {collapsed} ELSE {trimmed} END"
        )

    path_sql = "'" + str(Path(path)).replace("'", "''") + "'"
    scan = f"read_csv({path_sql}, all_varchar=true, header=true)"
    requested_types = col_types or {}
    comparisons = column_types or {}
    with duckdb.connect() as connection:
        available = [row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()]
        missing = [column for column in columns if column not in available]
        if missing:
            raise ValueError(f"CSV is missing columns: {', '.join(missing)}")

        base = {}
        for column in columns:
            ref = quote(column)
            value = ref
            if column in comparisons:
                normalized = resolve_column_type(comparisons[column]).normalize_sql(
                    ref, "duckdb", side
                )
                value = f"CASE WHEN {ref} IS NULL THEN NULL ELSE {normalized} END"
            if strict:
                value = f"lower({collapse(value)})"
            base[column] = value

        infer_selects = []
        for index, column in enumerate(columns):
            value = base[column]
            infer_selects += [
                f"count({value}) AS present_{index}",
                f"count(try_cast({value} AS double)) AS numeric_{index}",
                (
                    f"count_if(regexp_matches({value}, '^[+-]?[0-9]+$')) "
                    f"AS integral_{index}"
                ),
            ]
        inferred_row = connection.execute(
            f"SELECT {', '.join(infer_selects)} FROM {scan}"
        ).fetchone()
        numeric, integral = {}, {}
        for index, column in enumerate(columns):
            forced = requested_types.get(column)
            present, parsed, integers = inferred_row[index * 3 : index * 3 + 3]
            numeric[column] = (
                forced == "numeric"
                if forced
                else column not in comparisons and present > 0 and present == parsed
            )
            integral[column] = numeric[column] and present == parsed == integers

        selects = ["count(*) AS n_total"]
        values = {}
        for index, column in enumerate(columns):
            raw, value = quote(column), base[column]
            if numeric[column]:
                value = collapse(value)
            values[column] = value
            number = f"try_cast({value} AS double)"
            extreme = f"try_cast({value} AS hugeint)" if integral[column] else number
            hash_value = (
                f"cast('0x' || substr(sha256(coalesce({value}, '<<NULL>>')), 1, 16) "
                "as ubigint)"
            )
            selects += [
                f"count_if({raw} IS NULL) AS n_missing_{index}",
                f"count(DISTINCT {value}) AS n_unique_{index}",
                f"sum(cast({hash_value} AS hugeint)) AS hash_sum_{index}",
                f"avg({number}) AS mean_{index}" if numeric[column] else f"NULL AS mean_{index}",
                (
                    f"stddev_samp({number}) AS std_{index}"
                    if numeric[column]
                    else f"NULL AS std_{index}"
                ),
                (
                    f"min({extreme}) AS min_{index}"
                    if numeric[column]
                    else f"min({value}) AS min_{index}"
                ),
                (
                    f"max({extreme}) AS max_{index}"
                    if numeric[column]
                    else f"max({value}) AS max_{index}"
                ),
                (
                    f"NULL AS length_avg_{index}"
                    if numeric[column]
                    else f"avg(length({value})) AS length_avg_{index}"
                ),
                (
                    f"NULL AS length_max_{index}"
                    if numeric[column]
                    else f"max(length({value})) AS length_max_{index}"
                ),
            ]
        cursor = connection.execute(f"SELECT {', '.join(selects)} FROM {scan}")
        row = dict(zip([item[0] for item in cursor.description], cursor.fetchone()))
        result = {}
        for index, column in enumerate(columns):
            is_numeric = numeric[column]
            minimum, maximum = row[f"min_{index}"], row[f"max_{index}"]
            top = ""
            if not is_numeric:
                value = values[column]
                top_rows = connection.execute(
                    "WITH numbered AS ("
                    f"SELECT {value} AS value, row_number() OVER () AS position FROM {scan}), "
                    "counts AS (SELECT value, count(*) AS n, min(position) AS position "
                    "FROM numbered WHERE value IS NOT NULL GROUP BY value) "
                    "SELECT value, n FROM counts ORDER BY n DESC, position LIMIT 10"
                ).fetchall()
                top = json.dumps(
                    [{"value": value, "count": int(count)} for value, count in top_rows],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            result[column] = {
                "col_type": "numeric" if is_numeric else "categorical",
                "n_total": int(row["n_total"]),
                "n_missing": int(row[f"n_missing_{index}"] or 0),
                "n_unique": int(row[f"n_unique_{index}"] or 0),
                "hash_sum": str(int(row[f"hash_sum_{index}"] or 0)),
                "mean": None if row[f"mean_{index}"] is None else float(row[f"mean_{index}"]),
                "std": None if row[f"std_{index}"] is None else float(row[f"std_{index}"]),
                "min_val": None if minimum is None else str(minimum),
                "max_val": None if maximum is None else str(maximum),
                "length_avg": (
                    None
                    if row[f"length_avg_{index}"] is None
                    else float(row[f"length_avg_{index}"])
                ),
                "length_max": (
                    None
                    if row[f"length_max_{index}"] is None
                    else int(row[f"length_max_{index}"])
                ),
                "top_10": top,
            }
        return result
