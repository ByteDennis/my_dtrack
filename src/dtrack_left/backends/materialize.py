from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..cli.config import load_config
from ..common.model.hashing import HASH_BUCKET_SPACE, DateKeyFamily, key_family
from ..common.runtime.run_logging import get_left_logger
from ..common.sql.dates import canonical_date, day_expr, quote_ident
from ..common.sql.sampling import composite_sql, hash32_sql
from .sources import FileSource, _duckdb_value

HASH_VERSION = 1
SAMPLE_RATES = (0.01, 0.02)
DAY_PARTITION_MIN_BYTES = 128 * 1024 * 1024
PROGRESS_INTERVAL_SECONDS = 30
log = get_left_logger("materialize")


def _enable_duckdb_progress(connection) -> None:
    """Use DuckDB's query-plan progress only for an interactive terminal."""

    if not sys.stderr.isatty():
        return
    connection.execute("SET enable_progress_bar = true")
    connection.execute("SET enable_progress_bar_print = true")
    connection.execute("SET progress_bar_time = 2000")


def _size(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


@contextmanager
def _stage(name: str, output: Path | None = None):
    started = time.monotonic()
    stopped = threading.Event()

    def heartbeat():
        while not stopped.wait(PROGRESS_INTERVAL_SECONDS):
            written = _size(output)
            suffix = f", written={written / (1024**3):.2f} GiB" if written else ""
            log.info(
                "materialize %s still running: elapsed=%.0fs%s",
                name,
                time.monotonic() - started,
                suffix,
            )

    log.info("materialize %s started", name)
    worker = None
    if not sys.stderr.isatty():
        worker = threading.Thread(
            target=heartbeat,
            name=f"dtrack-materialize-{name}",
            daemon=True,
        )
        worker.start()
    try:
        yield
    finally:
        stopped.set()
        if worker is not None:
            worker.join(timeout=1)
        log.info("materialize %s finished: elapsed=%.2fs", name, time.monotonic() - started)


def _keys(pair: dict[str, Any]) -> list[str]:
    return [part.strip() for part in str(pair.get("key") or "").split(",") if part.strip()]


def _resolve_name(columns: list[str], requested: str) -> str:
    if requested in columns:
        return requested
    matches = [column for column in columns if column.casefold() == requested.casefold()]
    if len(matches) == 1:
        return matches[0]
    if matches:
        raise ValueError(f"ambiguous column {requested!r}: {', '.join(matches)}")
    raise ValueError(f"missing key column {requested!r}")


def _families(pair: dict[str, Any], keys: list[str]):
    configured = pair.get("key_families") or {}
    unknown = sorted(set(configured) - set(keys))
    if unknown:
        raise ValueError(f"key_families names non-key columns: {', '.join(unknown)}")
    return [key_family(configured.get(key, "string")) for key in keys]


def _family_specs(families) -> list[Any] | None:
    if any(isinstance(family, DateKeyFamily) for family in families):
        return None
    return [family.spec() for family in families]


def _summary(rows) -> dict[str, dict[str, int]]:
    return {
        canonical_date(day): {"rows": int(total), "sample_1pct": int(one), "sample_2pct": int(two)}
        for day, total, one, two in rows
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _update_config(path: Path, pair_name: str, left: dict[str, Any]) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    document["pairs"][pair_name]["left"] = left
    temporary = path.with_name(f".{path.name}.materialize.tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def materialize_pair(
    config_path: str | Path,
    pair_name: str,
    *,
    output: str | Path | None = None,
    partition: str = "auto",
    delete_source: bool = False,
) -> dict[str, Any]:
    """Replace one local CSV source with verified, hash-sorted partitioned Parquet."""

    config_path = Path(config_path).resolve()
    config = load_config(config_path)
    try:
        pair = config["pairs"][pair_name]
    except KeyError:
        raise ValueError(f"unknown pair {pair_name!r}") from None
    if pair.get("partition_mode") == "all":
        raise ValueError("materialize requires partition_mode date")
    source_spec = dict(pair.get("left") or {})
    if str(source_spec.get("source") or "").lower() not in {"file", "csv"}:
        raise ValueError("materialize requires a Left source=file or source=csv")
    source = FileSource(source_spec)
    if source.format != "csv":
        raise ValueError("materialize currently requires one CSV/TXT source file")
    if any(char in source.path for char in "*?[") or "://" in source.path:
        raise ValueError("materialize requires one local CSV file, not a glob or URL")
    source_path = Path(source.path).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if delete_source and (source_spec.get("where") or source_spec.get("query")):
        raise ValueError("cannot delete a source when where/query materializes only a derived view")
    keys = _keys(pair)
    if not keys:
        raise ValueError("materialize requires pair.key so dtrack_hash is reproducible")
    if partition not in {"auto", "day", "month"}:
        raise ValueError("partition must be auto, day, or month")

    with _stage("preflight"):
        health = source.preflight()
    if not health.get("ok"):
        raise ValueError(f"source preflight failed: {health.get('error')}")
    columns = list(health["columns"])
    reserved = {"dtrack_hash", "dtrack_partition"} & set(columns)
    if reserved:
        raise ValueError(f"source already contains reserved columns: {', '.join(sorted(reserved))}")
    resolved_keys = [_resolve_name(columns, key) for key in keys]
    families = _families(pair, keys)
    key_refs = [quote_ident(key, "duckdb") for key in resolved_keys]
    composite = composite_sql(key_refs, families, "duckdb", "left")
    hash_sql = hash32_sql(composite, "duckdb")
    day_sql = day_expr(source.config["date_col"], "duckdb", source.config.get("date_type"))
    thresholds = [round(rate * HASH_BUCKET_SPACE) for rate in SAMPLE_RATES]
    where = source._where()
    summary_sql = (
        f"SELECT {day_sql} AS dtrack_day, count(*) AS n, "
        f"count_if({hash_sql} < {thresholds[0]}) AS sample_1pct, "
        f"count_if({hash_sql} < {thresholds[1]}) AS sample_2pct "
        f"FROM {source.relation()} WHERE {where} GROUP BY dtrack_day"
    )
    with _stage("source scan and hash probe"), source.connect() as connection:
        _enable_duckdb_progress(connection)
        source._audit(summary_sql, "materialize_probe")
        rows = connection.execute(summary_sql).fetchall()
    if not rows:
        raise ValueError("source contains no rows to materialize")
    if any(row[0] is None for row in rows):
        raise ValueError("source contains date values that cannot be normalized")
    before = _summary(rows)
    total = sum(item["rows"] for item in before.values())
    chosen_partition = partition
    if partition == "auto":
        average_day_bytes = source_path.stat().st_size / max(1, len(before))
        chosen_partition = "day" if average_day_bytes >= DAY_PARTITION_MIN_BYTES else "month"
    log.info(
        "materialize plan: source=%.2f GiB rows=%d days=%d partition=%s",
        source_path.stat().st_size / (1024**3),
        total,
        len(before),
        chosen_partition,
    )

    final = (
        Path(output).expanduser().resolve()
        if output
        else source_path.with_name(f"{source_path.stem}.dtrack-parquet")
    )
    if final.exists():
        raise FileExistsError(f"materialized destination already exists: {final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix=f".{final.name}.materialize-", dir=final.parent))
    dataset = workspace / "dataset"
    date_ref = quote_ident(source.config["date_col"], "duckdb")
    partition_format = "%Y-%m-%d" if chosen_partition == "day" else "%Y-%m"
    partition_sql = f"strftime(CAST({day_sql} AS DATE), '{partition_format}')"
    projection = (
        f"* REPLACE (CAST({day_sql} AS DATE) AS {date_ref}), "
        f"{hash_sql} AS dtrack_hash, {partition_sql} AS dtrack_partition"
    )
    copy_sql = (
        f"COPY (SELECT {projection} FROM {source.relation()} WHERE {where} "
        "ORDER BY dtrack_partition, dtrack_hash) "
        f"TO {_duckdb_value(str(dataset))} (FORMAT PARQUET, PARTITION_BY (dtrack_partition), "
        "COMPRESSION ZSTD, ROW_GROUP_SIZE 122880, FILENAME_PATTERN 'part_{i}')"
    )
    try:
        connection = source.connect()
        try:
            _enable_duckdb_progress(connection)
            spill = workspace / "spill"
            connection.execute(f"SET temp_directory={_duckdb_value(str(spill))}")
            with _stage("Parquet write and hash sort", dataset):
                connection.execute(copy_sql)
        finally:
            connection.close()
        parquet_glob = str(dataset / "**" / "*.parquet")
        reader = f"read_parquet({_duckdb_value(parquet_glob)}, hive_partitioning=true)"
        validate_sql = (
            f"SELECT {date_ref}, count(*), "
            f"count_if(dtrack_hash < {thresholds[0]}), "
            f"count_if(dtrack_hash < {thresholds[1]}) FROM {reader} GROUP BY {date_ref}"
        )
        with _stage("Parquet validation", dataset), source.connect() as connection:
            _enable_duckdb_progress(connection)
            after = _summary(connection.execute(validate_sql).fetchall())
            output_columns = [
                row[0] for row in connection.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
            ]
        expected_columns = {*columns, "dtrack_hash", "dtrack_partition"}
        if set(output_columns) != expected_columns:
            raise ValueError("materialized schema does not match the source plus dtrack columns")
        if before != after:
            raise ValueError("materialized row/date/hash validation failed")

        family_specs = _family_specs(families)
        new_left: dict[str, Any] = {
            "source": "file",
            "path": str(final / "**" / "*.parquet"),
            "format": "parquet",
            "options": {"hive_partitioning": True},
            "date_col": source.config["date_col"],
            "date_type": "date",
        }
        if family_specs is not None:
            new_left["sample_hash"] = {
                "column": "dtrack_hash",
                "version": HASH_VERSION,
                "keys": keys,
                "families": family_specs,
            }
        manifest = {
            "version": 1,
            "pair": pair_name,
            "source": str(source_path),
            "source_bytes": source_path.stat().st_size,
            "source_deleted": False,
            "output": str(final),
            "partition": chosen_partition,
            "date_col": source.config["date_col"],
            "hash": new_left.get("sample_hash"),
            "summary": before,
            "total_rows": total,
            "left_before": source_spec,
            "left_after": new_left,
        }
        _write_json(dataset / "_dtrack_materialization.json", manifest)
        os.replace(dataset, final)
        _update_config(config_path, pair_name, new_left)
        log.info("materialize committed dataset and config: %s", final)
        deleted = False
        if delete_source:
            source_path.unlink()
            deleted = True
            manifest["source_deleted"] = True
            _write_json(final / "_dtrack_materialization.json", manifest)
            log.info("materialize deleted verified source: %s", source_path)
        return {
            "pair": pair_name,
            "source": str(source_path),
            "source_deleted": deleted,
            "output": str(final),
            "partition": chosen_partition,
            "rows": total,
            "config": str(config_path),
            "hash_fast_path": family_specs is not None,
        }
    except Exception:
        if dataset.exists():
            shutil.rmtree(dataset)
        raise
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
