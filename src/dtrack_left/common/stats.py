from __future__ import annotations

import csv
import hashlib
import json
import os
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from .column_types import resolve_column_type
from .hashing import collapse_decimal
from .run_logging import get_left_logger

log = get_left_logger("stats")


def infer_column_type(
    present: int,
    parsed: int,
    integers: int | None = None,
    *,
    requested: str | None = None,
    comparison: bool = False,
) -> tuple[str, bool]:
    """Return the statistics type and whether every present value is integral text."""
    is_integral = integers is not None and present > 0 and present == parsed == integers
    is_numeric = (
        requested == "numeric"
        if requested
        else not comparison and present > 0 and present == parsed and not is_integral
    )
    return ("numeric" if is_numeric else "categorical"), is_integral


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
    original = values.dropna().astype(str)
    integers = int(original.str.fullmatch(r"[+-]?[0-9]+").sum())
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
    inferred_type, is_integral = infer_column_type(
        len(non_null),
        int(numeric.notna().sum()),
        integers,
        requested=col_type,
    )
    is_numeric = inferred_type == "numeric"
    if col_type:
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
            top_10="" if is_integral else _top10(values),
        )
    return common


DEFAULT_BATCH = 64


# >>> DTRACK_STATS_BATCH columns per aggregate query, so at most that many DISTINCT hash tables live at once <<< #
def stats_batch() -> int:
    return max(1, int(os.getenv("DTRACK_STATS_BATCH") or DEFAULT_BATCH))


def _batches(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# >>> trim, then drop trailing decimal zeros ("1.50" -> "1.5", "2.0" -> "2"); idempotent <<< #
def _collapse(expr: str) -> str:
    trimmed = f"trim({expr})"
    collapsed = (
        f"regexp_replace(regexp_replace({trimmed}, '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')"
    )
    return (
        f"CASE WHEN regexp_matches({trimmed}, '^-?[0-9]+\\.[0-9]+$') "
        f"THEN {collapsed} ELSE {trimmed} END"
    )


# >>> one single-threaded CSV load into a file-backed table (parallel readers buffer every column; rowid = file order), then batched aggregates <<< #
def compute_csv_statistics(
    path: str | Path,
    columns: list[str],
    *,
    strict: bool = False,
    col_types: dict[str, str] | None = None,
    column_types: dict[str, Any] | None = None,
    side: str = "left",
    progress: Callable[[str, int, int], None] | None = None,
    on_batch: Callable[[dict[str, dict[str, Any]]], None] | None = None,
    shard_status: Callable[[int, int, int], None] | None = None,
) -> dict[str, dict[str, Any]]:
    from .duck import connect, temp_directory

    with Path(path).open(encoding="utf-8", newline="") as handle:
        available = next(csv.reader(handle), [])
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(f"CSV is missing columns: {', '.join(missing)}")
    path_sql = "'" + str(Path(path)).replace("'", "''") + "'"
    scan = f"read_csv({path_sql}, all_varchar=true, header=true)"
    workspace = Path(temp_directory()) / f"stats-{uuid.uuid4().hex}.duckdb"
    try:
        workspace.parent.mkdir(parents=True, exist_ok=True)
        with connect(str(workspace), resource_workers=1) as connection:
            projection = ", ".join(_quote(column) for column in dict.fromkeys(columns))
            connection.execute("SET threads = 1")
            connection.execute(f"CREATE TABLE scanned AS SELECT {projection} FROM {scan}")
            if progress:
                progress("load", 1, 1)
        return _parallel_statistics(
            workspace,
            columns,
            strict,
            col_types or {},
            column_types or {},
            side,
            progress,
            on_batch,
            shard_status,
        )
    finally:
        for suffix in ("", ".wal"):
            Path(str(workspace) + suffix).unlink(missing_ok=True)


def _parallel_statistics(
    workspace,
    columns,
    strict,
    requested_types,
    comparisons,
    side,
    progress,
    on_batch,
    shard_status,
):
    from .duck import connect, worker_count

    groups = list(_batches(columns, stats_batch()))
    if not groups:
        return {}
    workers = min(worker_count(), len(groups))
    log.info(
        "statistics shards start: attributes=%d, shard_size=%d, shards=%d, parallel=%d",
        len(columns),
        stats_batch(),
        len(groups),
        workers,
    )
    if shard_status:
        shard_status(workers, 0, len(groups))
    phase_done = {phase: [0] * len(groups) for phase in ("infer", "aggregate", "finalize")}
    progress_lock = threading.Lock()

    def group_progress(group_index):
        def update(phase, completed, total):
            if phase not in phase_done:
                return
            with progress_lock:
                phase_done[phase][group_index] = min(total, completed)
                overall = sum(phase_done[phase])
            if progress:
                progress(phase, overall, len(columns))

        return update

    def calculate(group_index, group):
        with connect(
            str(workspace),
            read_only=True,
            resource_workers=workers,
        ) as connection:
            return _statistics(
                connection,
                group,
                strict,
                requested_types,
                comparisons,
                side,
                group_progress(group_index),
            )

    completed = {}
    completed_groups = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dtrack-stats") as executor:
        jobs = {
            executor.submit(calculate, index, group): index for index, group in enumerate(groups)
        }
        try:
            for job in as_completed(jobs):
                rows = job.result()
                completed.update(rows)
                completed_groups += 1
                if shard_status:
                    shard_status(
                        min(workers, len(groups) - completed_groups),
                        completed_groups,
                        len(groups),
                    )
                log.info(
                    "statistics shard calculated: shard=%d/%d, attributes=%d, "
                    "attributes_calculated=%d/%d",
                    completed_groups,
                    len(groups),
                    len(rows),
                    len(completed),
                    len(columns),
                )
                if on_batch:
                    on_batch(rows)
        except Exception:
            for job in jobs:
                job.cancel()
            raise
    return {column: completed[column] for column in columns}


def _statistics(connection, columns, strict, requested_types, comparisons, side, progress=None):
    batch = stats_batch()
    base = {}
    integral_values = {}
    for column in columns:
        ref = _quote(column)
        value = ref
        if column in comparisons:
            normalized = resolve_column_type(comparisons[column]).normalize_sql(ref, "duckdb", side)
            value = f"CASE WHEN {ref} IS NULL THEN NULL ELSE {normalized} END"
        integral_values[column] = value
        if strict:
            value = f"lower({_collapse(value)})"
        base[column] = value

    indexed = list(enumerate(columns))
    inferred: dict[int, tuple] = {}
    completed = 0
    for group in _batches(indexed, batch):
        infer_selects = []
        for index, column in group:
            value = base[column]
            integral_value = integral_values[column]
            infer_selects += [
                f"count({value}) AS present_{index}",
                f"count(try_cast({value} AS double)) AS numeric_{index}",
                (
                    f"count_if(regexp_matches({integral_value}, '^[+-]?[0-9]+$')) "
                    f"AS integral_{index}"
                ),
            ]
        inferred_row = connection.execute(
            f"SELECT {', '.join(infer_selects)} FROM scanned"
        ).fetchone()
        for offset, (index, _column) in enumerate(group):
            inferred[index] = inferred_row[offset * 3 : offset * 3 + 3]
        completed += len(group)
        if progress:
            progress("infer", completed, len(columns))
    numeric, integral = {}, {}
    for index, column in indexed:
        forced = requested_types.get(column)
        present, parsed, integers = inferred[index]
        inferred_type, integral[column] = infer_column_type(
            present,
            parsed,
            integers,
            requested=forced,
            comparison=column in comparisons,
        )
        numeric[column] = inferred_type == "numeric"

    row: dict[str, Any] = {}
    values = {}
    completed = 0
    for group in _batches(indexed, batch):
        selects = ["count(*) AS n_total"]
        for index, column in group:
            raw, value = _quote(column), base[column]
            if numeric[column] and not strict:
                value = _collapse(value)
            values[column] = value
            number = f"try_cast({value} AS double)"
            extreme = f"try_cast({value} AS hugeint)" if integral[column] else number
            hash_value = (
                f"cast('0x' || substr(sha256(coalesce({value}, '<<NULL>>')), 1, 16) as ubigint)"
            )
            selects += [
                f"count_if({raw} IS NULL) AS n_missing_{index}",
                f"count(DISTINCT {value}) AS n_unique_{index}",
                f"sum(cast({hash_value} AS hugeint)) AS hash_sum_{index}",
                f"avg({number}) AS mean_{index}" if numeric[column] else f"NULL AS mean_{index}",
                f"stddev_samp({number}) AS std_{index}"
                if numeric[column]
                else f"NULL AS std_{index}",
                f"min({extreme}) AS min_{index}"
                if numeric[column]
                else f"min({value}) AS min_{index}",
                f"max({extreme}) AS max_{index}"
                if numeric[column]
                else f"max({value}) AS max_{index}",
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
        cursor = connection.execute(f"SELECT {', '.join(selects)} FROM scanned")
        row.update(zip([item[0] for item in cursor.description], cursor.fetchone()))
        completed += len(group)
        if progress:
            progress("aggregate", completed, len(columns))

    result = {}
    for index, column in indexed:
        is_numeric = numeric[column]
        minimum, maximum = row[f"min_{index}"], row[f"max_{index}"]
        top = ""
        if not is_numeric and not integral[column]:
            top_rows = connection.execute(
                "WITH counts AS (SELECT value, count(*) AS n, min(position) AS position "
                f"FROM (SELECT {values[column]} AS value, rowid AS position FROM scanned) "
                "WHERE value IS NOT NULL GROUP BY value) "
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
                None if row[f"length_avg_{index}"] is None else float(row[f"length_avg_{index}"])
            ),
            "length_max": (
                None if row[f"length_max_{index}"] is None else int(row[f"length_max_{index}"])
            ),
            "top_10": top,
        }
        if progress:
            progress("finalize", index + 1, len(columns))
    return result
