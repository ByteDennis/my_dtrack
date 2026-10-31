from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ..common.model.hashing import (
    DateKeyFamily,
    active_normalization_rules,
    canonical_numeric_sql,
    key_family,
    normalize_system_sql,
)
from ..common.model.manifest import DEFAULT_STATISTICS, window_sample_plan
from ..common.sql.column_types import resolve_column_type
from ..common.sql.dates import (
    DateType,
    NativeDateType,
    NumericDateType,
    SasDateType,
    StringDateType,
    aggregate_partition_counts,
    bounds_where,
    canonical_date,
    day_expr,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.sql.query_windows import normalize_query_windows, render_query_windows
from ..common.sql.sampling import composite_sql, sample_predicate_sql
from .frame_stats import EXTREMES, frame_statistics

log = logging.getLogger("pair_recon.right.target")
NUMERIC_TYPES = (
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "double",
    "float",
    "decimal",
    "long",
    "short",
)
INTEGRAL_TYPES = ("int", "bigint", "smallint", "tinyint", "long", "short")
TEMPORAL_TYPES = ("date", "timestamp")
TEXT_DATE_TYPES = (StringDateType, NumericDateType, SasDateType)


# >>> an explicit date type must fit the physical Spark column, otherwise ANSI casts fail or parse to NULL <<< #
def check_date_column(column: str, column_type: str | None, date_type: DateType, role: str) -> None:
    if not column_type or date_type.requires_probe:
        return
    kind = column_type.lower()
    temporal = kind.startswith(TEMPORAL_TYPES)
    spec = date_type.spec(portable=True)
    if isinstance(date_type, NativeDateType) and not temporal:
        raise ValueError(
            f"{role} {column!r} is {kind}; date type {spec!r} needs a date or timestamp column, "
            "declare a string or number date type for it"
        )
    if temporal and isinstance(date_type, TEXT_DATE_TYPES):
        raise ValueError(
            f"{role} {column!r} is {kind}; date type {spec!r} needs a text or numeric column, "
            "declare 'date' for it or use the text column instead"
        )


# >>> rows streamed partition by partition, so the driver never holds the whole result <<< #
def _iterate(frame, operation: str):
    started = time.monotonic()
    count = 0
    for row in frame.toLocalIterator():
        count += 1
        yield list(row)
    log.info("spark.sql %s: %d rows streamed (%.3fs)", operation, count, time.monotonic() - started)


def _bucket(window: dict[str, Any]) -> str:
    return str(window.get("bucket") or "|".join(map(str, window.get("partitions") or [])))


STATS_COLUMNS = int(os.environ.get("RIGHT_STATS_COLUMNS") or 50)
STATS_DRIVER_ROWS = int(os.environ.get("RIGHT_STATS_DRIVER_ROWS") or 2000000)
STATS_DRIVER_CELLS = int(os.environ.get("RIGHT_STATS_DRIVER_CELLS") or 5000000)
DRIVER_FETCHES = max(1, int(os.environ.get("RIGHT_DRIVER_FETCHES") or 1))
_driver_slots = threading.BoundedSemaphore(DRIVER_FETCHES)


class CsvTarget:
    def __init__(self, config: dict[str, Any]):
        from pair_recon_left.backends.sources import CsvSource

        self.config = dict(config)
        self.source = CsvSource(self.config)

    def set_sql_audit(self, audit):
        self.source.set_sql_audit(audit)
        return self

    def health_check(self):
        health = self.source.health_check()
        self.config.update(self.source.config)
        return health

    def partition_counts(self, from_date=None, to_date=None, **options):
        if self.config.get("pre_sampled"):
            options = {key: value for key, value in options.items() if key == "partitions"}
        return self.source.partition_counts(from_date, to_date, **options)

    def count_keys(self, from_date=None, to_date=None, **options):
        return self.source.count_keys(from_date, to_date, side="right", **options)

    def window_statistics(self, manifest, window, progress=None):
        from pair_recon_left.common.runtime.stats import compute_csv_statistics

        mapping = manifest["col_map"]
        columns = [mapping[column] for column in manifest["columns"]]
        overrides = {
            mapping[column]: value
            for column, value in (manifest.get("col_type_overrides") or {}).items()
        }
        comparisons = {
            mapping[column]: value for column, value in (manifest.get("column_types") or {}).items()
        }
        with TemporaryDirectory(prefix="pair_recon-right-") as folder:
            path, _rows = self.source.pull_window(
                window["partitions"],
                columns,
                Path(folder) / "window.csv",
                key_columns=manifest["right_key_cols"],
                sample_plan=window_sample_plan(manifest, window, "right"),
                key_families=manifest["key_families"],
                side="right",
                key_tuples=window.get("key_tuples"),
            )
            return compute_csv_statistics(
                path,
                columns,
                normalization_rules=manifest.get("normalization_rules"),
                normalization_source=(manifest.get("right") or {}).get("source")
                or self.config["source"],
                normalization_date=manifest.get("normalization_date"),
                col_types=overrides,
                column_types=comparisons,
                side="right",
                statistics=manifest.get("statistics"),
            )


class SparkTarget:
    date_probe_size = 16

    def __init__(self, spark, config: dict[str, Any]):
        self.spark = spark
        self.config = dict(config)
        self.sql_audit = None
        self._date_type = None
        self._date_type_lock = threading.Lock()
        self._views: dict[str, dict[str, Any]] = {}
        self._view_lock = threading.Lock()
        macro = str(config.get("conn_macro") or "")
        if "." in macro and not (config.get("catalog") or config.get("schema")):
            self.config["catalog"], self.config["schema"] = macro.split(".", 1)
        windows = normalize_query_windows(
            self.config.get("query"),
            self.config.get("query_windows"),
            source=str(self.config.get("source") or "databricks"),
        )
        if windows:
            self.config["query_windows"] = windows
        else:
            self.config.pop("query_windows", None)

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    @property
    def partition(self) -> str:
        return str(self.config.get("partition") or "day")

    def _audit(self, sql: str, operation: str = "query") -> None:
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend="spark", operation=operation)

    def _files(self) -> str:
        options = {"format": self.config.get("format") or "parquet"}
        options.update(self.config.get("options") or {})
        if self.config.get("schema_hints"):
            options["schemaHints"] = self.config["schema_hints"]
        arguments = ["'" + str(self.config["path"]).replace("'", "''") + "'"]
        for key, value in options.items():
            arguments.append(f"{key} => '" + str(value).replace("'", "''") + "'")
        return f"read_files({', '.join(arguments)})"

    def relation(
        self,
        *,
        partitions: list[str] | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> str:
        kind = {"volume_raw": "files"}.get(self.config.get("kind"), self.config.get("kind"))
        if kind == "files":
            query = self.config.get("query")
            if not query:
                return self._files()
            query = query.replace("{src}", self._files())
        else:
            query = self.config.get("query")
        if query:
            query = render_query_windows(
                query,
                self.config.get("query_windows") or {},
                dialect="spark",
                partitions=partitions,
                partition=self.partition,
                from_date=from_date,
                to_date=to_date,
            )
            return f"({query}) pair_recon_source"
        parts = [self.config.get(name) for name in ("catalog", "schema", "table")]
        return ".".join(quote_ident(str(part), "spark") for part in parts if part)

    def _where(self, extra: str | None) -> str:
        clauses = [item for item in (self.config.get("where"), extra) if item]
        return " AND ".join(f"({item})" for item in clauses) or "1=1"

    def _collect(
        self, sql: str, operation: str = "query", *, log_context: str | None = None
    ) -> list:
        self._audit(sql, operation)
        started = time.monotonic()
        label = f"{operation} {log_context}" if log_context else operation
        try:
            rows = self.spark.sql(sql).collect()
        except Exception:
            log.exception("spark.sql %s failed after %.3fs", label, time.monotonic() - started)
            raise
        log.info("spark.sql %s: %d rows (%.3fs)", label, len(rows), time.monotonic() - started)
        return rows

    def _rows(self, sql: str, operation: str = "query") -> list[tuple]:
        return [tuple(row) for row in self._collect(sql, operation)]

    def date_type(self):
        with self._date_type_lock:
            if self._date_type is not None:
                return self._date_type
            resolved = resolve_date_type(self.config.get("date_type"))
            if resolved.requires_probe:
                column = quote_ident(self.config["date_col"], "spark")
                rows = self._rows(
                    f"SELECT {column} FROM {self.relation()} "
                    f"WHERE {self._where(f'{column} IS NOT NULL')} LIMIT {self.date_probe_size}",
                    "date_type_probe",
                )
                resolved = resolved.detect([row[0] for row in rows])
            self._check_date_column(self.config["date_col"], resolved, "date_col")
            self._date_type = resolved
            self.config["date_type"] = resolved.spec(portable=True)
            return resolved

    def _check_date_column(self, column: str, date_type: DateType, role: str) -> None:
        types = getattr(self, "column_types", None) or {}
        check_date_column(column, types.get(column), date_type, role)

    def _key_types(self, keys: list[str]) -> list[str | None]:
        types = getattr(self, "column_types", None) or {}
        return [types.get(key) for key in keys]

    def _check_key_families(self, keys: list[str], families: list[Any]) -> None:
        for column, spec in zip(keys, families):
            family = key_family(spec)
            if isinstance(family, DateKeyFamily):
                self._check_date_column(column, family.right_date_type, "key column")

    def health_check(self) -> dict[str, Any]:
        relation = self.relation()
        log.info(
            "health check relation=%s kind=%s partition=%s date_col=%s date_type=%s where=%s",
            relation,
            self.config.get("kind") or "table",
            self.partition,
            self.config.get("date_col"),
            self.config.get("date_type"),
            self.config.get("where"),
        )
        try:
            columns = {
                row[0]: str(row[1]).lower()
                for row in self._rows(f"DESCRIBE QUERY SELECT * FROM {relation}", "health_check")
                if row[0] and not str(row[0]).startswith("#")
            }
            if self.partition != "all" and self.config["date_col"] not in columns:
                raise ValueError(f"date_col {self.config['date_col']!r} not in {sorted(columns)}")
            self.column_types = columns
            if self.partition != "all":
                log.info(
                    "health check date_type resolved: %s", self.date_type().spec(portable=True)
                )
            return {
                "ok": True,
                "source": "databricks",
                "relation": relation,
                "columns": sorted(columns),
            }
        except Exception as exc:
            log.exception("health check failed for relation %s", relation)
            return {
                "ok": False,
                "source": "databricks",
                "relation": relation,
                "error": f"{type(exc).__name__}: {exc}",
            }

    def partition_counts(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        *,
        sample: dict[str, Any] | None = None,
        key_columns: list[str] | None = None,
        key_families: list[Any] | None = None,
        partitions: list[str] | None = None,
    ) -> dict[str, int]:
        self._check_key_families(key_columns or [], key_families or [])
        sampled = sample_predicate_sql(
            sample,
            [quote_ident(key, "spark") for key in key_columns or []],
            key_families or [],
            "spark",
            "right",
            key_types=self._key_types(key_columns or []),
        )
        if self.partition == "all":
            rows = self._rows(
                f"SELECT COUNT(*) FROM {self.relation()} WHERE {self._where(sampled)}",
                "partition_counts",
            )
            return {"all": int(rows[0][0]) if rows else 0}
        self.date_type()
        date_col = self.config["date_col"]
        ref = day_expr(date_col, "spark", self.config.get("date_type"))
        bounds = bounds_where(
            date_col,
            dialect="spark",
            date_type=self.config.get("date_type"),
            from_date=from_date,
            to_date=to_date,
        )
        selected = (
            partition_where(
                date_col,
                partitions,
                dialect="spark",
                date_type=self.config.get("date_type"),
                grain=self.partition,
            )
            if partitions
            else None
        )
        predicate = f"({bounds}) AND ({sampled})" if bounds and sampled else bounds or sampled
        if selected:
            predicate = f"({predicate}) AND ({selected})" if predicate else selected
        rows = self._rows(
            f"SELECT {ref}, COUNT(*) FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate)} GROUP BY {ref}",
            "partition_counts",
        )
        counts: dict[str, int] = {}
        for value, count in rows:
            day = canonical_date(value)
            counts[day] = counts.get(day, 0) + int(count)
        return aggregate_partition_counts(counts, self.partition)

    # >>> how many of the Left's sampled key tuples the real predicate also selects here (smoke stage) <<< #
    def count_keys(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        *,
        key_columns: list[str],
        key_families: list[Any],
        sample: dict[str, Any] | None,
        key_tuples: list[list[object]],
    ) -> int:
        if not hasattr(self, "column_types"):
            self.health_check()
        self._check_key_families(key_columns, key_families)
        keys = [quote_ident(key, "spark") for key in key_columns]
        types = self._key_types(key_columns)
        sampled = sample_predicate_sql(
            sample, keys, key_families, "spark", "right", key_types=types
        )
        fixed = sample_predicate_sql(
            {"mode": "fixed"}, keys, key_families, "spark", "right", key_tuples, types
        )
        bounds = None
        if self.partition != "all":
            self.date_type()
            bounds = bounds_where(
                self.config["date_col"],
                dialect="spark",
                date_type=self.config.get("date_type"),
                from_date=from_date,
                to_date=to_date,
            )
        predicate = " AND ".join(f"({item})" for item in (bounds, sampled, fixed) if item)
        rows = self._rows(
            f"SELECT count(1) FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate or None)}",
            "smoke_keys",
        )
        return int(rows[0][0]) if rows else 0

    def _sample_predicate(self, manifest: dict[str, Any], window: dict[str, Any]) -> str | None:
        keys, families = manifest["right_key_cols"], manifest["key_families"]
        self._check_key_families(keys, families)
        return sample_predicate_sql(
            window_sample_plan(manifest, window, "right"),
            [quote_ident(key, "spark") for key in keys],
            families,
            "spark",
            "right",
            window.get("key_tuples"),
            key_types=self._key_types(keys),
        )

    def _window_where(self, manifest: dict[str, Any], window: dict[str, Any]) -> tuple[str, bool]:
        if not hasattr(self, "column_types"):
            self.health_check()
        if self.partition != "all":
            self.date_type()
        predicate = (
            partition_where(
                self.config["date_col"],
                window["partitions"],
                dialect="spark",
                date_type=self.config.get("date_type"),
                grain=self.partition,
            )
            if self.partition != "all"
            else None
        )
        sample = self._sample_predicate(manifest, window)
        filters = " AND ".join(f"({item})" for item in (predicate, sample) if item)
        return self._where(filters or None), bool(sample)

    # >>> sampled row count of one window with the same predicate as window_statistics, one scan <<< #
    def window_count(self, manifest: dict[str, Any], window: dict[str, Any]) -> int:
        relation, where = self._scope(manifest, window)
        rows = self._rows(f"SELECT count(1) FROM {relation} WHERE {where}", "window_count")
        return int(rows[0][0]) if rows else 0

    # >>> relation and predicate for one window: the cached view without predicate once materialized, else the source relation with the window predicate <<< #
    def _scope(self, manifest: dict[str, Any], window: dict[str, Any]) -> tuple[str, str]:
        with self._view_lock:
            view = self._views.get(_bucket(window))
        if view is not None:
            return view["relation"], "1=1"
        where, _sampled = self._window_where(manifest, window)
        return self.relation(partitions=window["partitions"]), where

    # >>> the sampled window materialized once as a persisted temp view, so count, statistics, samples, and rows never re-plan or re-scan the relation <<< #
    def _window_view(self, manifest: dict[str, Any], window: dict[str, Any]) -> dict[str, Any]:
        bucket = _bucket(window)
        with self._view_lock:
            if bucket in self._views:
                return self._views[bucket]
        where, _sampled = self._window_where(manifest, window)
        sql = f"SELECT * FROM {self.relation(partitions=window['partitions'])} WHERE {where}"
        self._audit(sql, "window_materialize")
        started = time.monotonic()
        name = "pair_recon_" + hashlib.sha256(f"{id(self)}|{bucket}".encode()).hexdigest()[:16]
        frame = self.spark.sql(sql)
        try:
            from pyspark import StorageLevel

            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
        except ImportError:
            frame = frame.persist()
        frame.createOrReplaceTempView(name)
        rows = int(frame.count())
        log.info(
            "window materialized %s: %d sampled row(s) (%.3fs)",
            bucket,
            rows,
            time.monotonic() - started,
        )
        view = {"name": name, "relation": quote_ident(name, "spark"), "frame": frame, "rows": rows}
        with self._view_lock:
            self._views[bucket] = view
        return view

    def release_window(self, window: dict[str, Any]) -> None:
        with self._view_lock:
            view = self._views.pop(_bucket(window), None)
        if view is None:
            return
        view["frame"].unpersist()
        self.spark.catalog.dropTempView(view["name"])

    # >>> per-column Spark expressions: normalized text, double value, and the min/max operand <<< #
    def _column_plans(self, manifest: dict[str, Any]) -> list[dict[str, Any]]:
        normalization_rules = active_normalization_rules(
            manifest.get("normalization_rules"),
            (manifest.get("right") or {}).get("source") or self.config["source"],
        )
        overrides = manifest.get("col_type_overrides") or {}
        column_types = manifest.get("column_types") or {}
        plans = []
        for i, left_column in enumerate(manifest["columns"]):
            column = manifest["col_map"][left_column]
            ref = quote_ident(column, "spark")
            column_type = self.column_types.get(column, "")
            native = any(column_type.startswith(t) for t in NUMERIC_TYPES)
            integral = native and any(column_type.startswith(t) for t in INTEGRAL_TYPES)
            comparison = column_types.get(left_column)
            requested = overrides.get(left_column)
            numeric = not comparison and (
                requested == "numeric" or (requested is None and native and not integral)
            )
            text = f"cast({ref} as string)"
            if comparison:
                normalizer = resolve_column_type(comparison)
                if isinstance(normalizer, DateKeyFamily):
                    self._check_date_column(column, normalizer.right_date_type, "column")
                text = normalizer.normalize_sql(ref, "spark", "right")
            if numeric:
                text = canonical_numeric_sql(text, "spark")
            if normalization_rules:
                text = normalize_system_sql(
                    text,
                    "spark",
                    normalization_rules,
                    run_date=manifest.get("normalization_date"),
                )
            value = (
                (f"cast({ref} as double)" if native else f"try_cast({text} as double)")
                if numeric
                else text
            )
            plans.append(
                {
                    "index": i,
                    "left": left_column,
                    "column": column,
                    "text": text,
                    "value": value,
                    "extreme": ref if integral and numeric else value,
                    "numeric": numeric,
                    "integral": integral,
                }
            )
        return plans

    # >>> one Spark aggregate query over a batch of columns; the row carries <name>_<index> fields <<< #
    def _spark_statistics(
        self, plans, selected, relation, where, *, log_context: str
    ) -> dict[str, dict[str, Any]]:
        selects = ["count(1) AS n_total"]
        for plan in plans:
            i, text, value, extreme = plan["index"], plan["text"], plan["value"], plan["extreme"]
            numeric = plan["numeric"]
            if "n_missing" in selected:
                selects.append(f"count_if({text} IS NULL) AS n_missing_{i}")
            if "n_unique" in selected:
                selects.append(f"count(DISTINCT {text}) AS n_unique_{i}")
            if "hash_sum" in selected:
                selects.append(
                    "sum(cast(conv(substr(sha2(coalesce("
                    f"{text}, '<<NULL>>'), 256), 1, 16), 16, 10) as decimal(38,0))) "
                    f"AS hash_sum_{i}"
                )
            if "mean" in selected:
                selects.append(f"avg({value}) AS mean_{i}" if numeric else f"NULL AS mean_{i}")
            if "std" in selected:
                selects.append(
                    f"stddev_samp({value}) AS std_{i}" if numeric else f"NULL AS std_{i}"
                )
            if "min" in selected:
                selects.append(f"cast(min({extreme}) as string) AS min_{i}")
            if "max" in selected:
                selects.append(f"cast(max({extreme}) as string) AS max_{i}")
            if "length_avg" in selected:
                selects.append(
                    f"NULL AS length_avg_{i}"
                    if numeric
                    else f"avg(length({text})) AS length_avg_{i}"
                )
            if "length_max" in selected:
                selects.append(
                    f"NULL AS length_max_{i}"
                    if numeric
                    else f"max(length({text})) AS length_max_{i}"
                )
        sql = f"SELECT {', '.join(selects)} FROM {relation} WHERE {where}"
        row = self._collect(sql, "window_statistics", log_context=log_context)[0].asDict()
        stats = {}
        for plan in plans:
            i = plan["index"]
            payload: dict[str, Any] = {
                "col_type": "numeric" if plan["numeric"] else "categorical",
                "n_total": int(row["n_total"]),
            }
            if "n_missing" in selected:
                payload["n_missing"] = int(row[f"n_missing_{i}"] or 0)
            if "n_unique" in selected:
                payload["n_unique"] = int(row[f"n_unique_{i}"] or 0)
            if "hash_sum" in selected:
                payload["hash_sum"] = str(int(row[f"hash_sum_{i}"] or 0))
            for name in ("mean", "std", "length_avg"):
                if name in selected:
                    value = row[f"{name}_{i}"]
                    payload[name] = None if value is None else float(value)
            if "length_max" in selected:
                value = row[f"length_max_{i}"]
                payload["length_max"] = None if value is None else int(value)
            for name in ("min", "max"):
                if name in selected:
                    payload[f"{name}_val"] = row[f"{name}_{i}"]
            stats[plan["column"]] = payload
        return stats

    # >>> Spark only projects the normalized text (and double value) of a batch; pandas reduces it on the driver <<< #
    def _driver_statistics(
        self, plans, selected, relation, where, *, log_context: str
    ) -> dict[str, dict[str, Any]]:
        selects = []
        for plan in plans:
            selects.append(f"{plan['text']} AS t{plan['index']}")
            if plan["numeric"] and any(name in selected for name in EXTREMES):
                selects.append(f"{plan['value']} AS v{plan['index']}")
        sql = f"SELECT {', '.join(selects)} FROM {relation} WHERE {where}"
        self._audit(sql, "window_statistics")
        started = time.monotonic()
        with _driver_slots:
            try:
                frame = self.spark.sql(sql).toPandas()
            except Exception:
                log.exception(
                    "spark.sql window_statistics %s failed after %.3fs",
                    log_context,
                    time.monotonic() - started,
                )
                raise
            log.info(
                "spark.sql window_statistics %s: %d rows x %d columns to pandas (%.3fs)",
                log_context,
                len(frame),
                len(frame.columns),
                time.monotonic() - started,
            )
            return frame_statistics(frame, plans, selected)

    # >>> windows up to STATS_DRIVER_ROWS rows are reduced in pandas on the driver, STATS_DRIVER_CELLS cells per scan; larger ones aggregate in Spark, STATS_COLUMNS columns per query <<< #
    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any], progress=None
    ) -> dict[str, dict[str, Any]]:
        where, sample = self._window_where(manifest, window)
        plans = self._column_plans(manifest)
        selected = manifest.get("statistics") or list(DEFAULT_STATISTICS)
        view = self._window_view(manifest, window) if STATS_DRIVER_ROWS > 0 else None
        rows = view["rows"] if view is not None else None
        on_driver = rows is not None and rows <= STATS_DRIVER_ROWS
        if view is not None and not on_driver:
            self.release_window(window)
        relation, where = self._scope(manifest, window)
        batch = max(1, STATS_DRIVER_CELLS // max(rows or 0, 1) if on_driver else STATS_COLUMNS)
        starts = range(0, len(plans), batch)
        pair = manifest.get("pair") or "?"
        vintage = window.get("bucket") or "all"
        log.info(
            "window_statistics pair=%s vintage=%s partitions=%s columns=%d statistics=%s "
            "sample=%s rows=%s engine=%s batches=%d",
            pair,
            vintage,
            window["partitions"],
            len(plans),
            selected,
            bool(sample),
            rows,
            "pandas" if on_driver else "spark",
            len(starts),
        )
        reduce = self._driver_statistics if on_driver else self._spark_statistics
        stats: dict[str, dict[str, Any]] = {}
        for batch_index, start in enumerate(starts, 1):
            context = f"pair={pair} vintage={vintage} batch={batch_index}/{len(starts)}"
            stats.update(
                reduce(
                    plans[start : start + batch],
                    selected,
                    relation,
                    where,
                    log_context=context,
                )
            )
            if progress is not None:
                progress(min(start + batch, len(plans)), len(plans))
        if "statistics" not in manifest:
            for payload in stats.values():
                payload["top_10"] = ""
        return stats

    # >>> key, partition, and attribute column pairs of a sample, one entry per distinct Left/Right name pair <<< #
    def _sample_pairs(self, manifest: dict[str, Any], columns: list[str]) -> list[dict[str, Any]]:
        pairs: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def include(left_column: str | None, right_column: str | None, *, role: str) -> None:
            if not left_column or not right_column:
                return
            identity = (str(left_column).casefold(), str(right_column).casefold())
            if identity in seen:
                existing = next(row for row in pairs if row["identity"] == identity)
                if role not in existing["roles"]:
                    existing["roles"].append(role)
                return
            seen.add(identity)
            pairs.append(
                {
                    "identity": identity,
                    "left_column": str(left_column),
                    "right_column": str(right_column),
                    "roles": [role],
                }
            )

        for left_column, right_column in zip(
            manifest.get("key_cols") or [], manifest.get("right_key_cols") or []
        ):
            include(left_column, right_column, role="key")
        if manifest.get("partition", "day") != "all" and not manifest.get("left_snapshot"):
            include(
                (manifest.get("left") or {}).get("date_col"),
                (manifest.get("right") or {}).get("date_col"),
                role="partition",
            )
        for left_column in columns:
            include(left_column, manifest["col_map"][left_column], role="attribute")
        for row in pairs:
            row.pop("identity")
        return pairs

    # >>> keep a bounded raw sample in the Right process; it is written only if comparison finds a diff <<< #
    def window_sample(
        self, manifest: dict[str, Any], window: dict[str, Any], limit: int
    ) -> dict[str, Any]:
        pairs = self._sample_pairs(manifest, manifest["columns"])
        relation, where = self._scope(manifest, window)
        diagnostic_keys = window.get("diagnostic_key_tuples")
        if diagnostic_keys is not None:
            keys = manifest.get("right_key_cols") or []
            fixed = sample_predicate_sql(
                {"mode": "fixed"},
                [quote_ident(key, "spark") for key in keys],
                manifest.get("key_families") or [],
                "spark",
                "right",
                diagnostic_keys,
                self._key_types(keys),
            )
            where = f"({where}) AND ({fixed})"
        selected = ", ".join(quote_ident(row["right_column"], "spark") for row in pairs)
        key_types = self._key_types(manifest.get("right_key_cols") or [])
        order = []
        for index, (column, family_spec) in enumerate(
            zip(manifest.get("right_key_cols") or [], manifest.get("key_families") or [])
        ):
            order.append(
                key_family(family_spec).sql_dialect(
                    quote_ident(column, "spark"),
                    "spark",
                    "right",
                    data_type=key_types[index] if index < len(key_types) else None,
                )
            )
        order_by = f" ORDER BY {', '.join(order)}" if order else ""
        sql = f"SELECT {selected} FROM {relation} WHERE {where}{order_by} LIMIT {int(limit)}"
        rows = [list(row) for row in self._collect(sql, "difference_sample")]
        return {"pairs": pairs, "rows": rows, "limit": int(limit)}

    # >>> every sampled row of a window for the differing columns, streamed: normalized composite key, raw keys, raw and normalized values <<< #
    def window_rows(
        self, manifest: dict[str, Any], window: dict[str, Any], columns: list[str]
    ) -> dict[str, Any]:
        relation, where = self._scope(manifest, window)
        keys = list(manifest.get("right_key_cols") or [])
        key_sql = composite_sql(
            [quote_ident(key, "spark") for key in keys],
            manifest.get("key_families") or [],
            "spark",
            "right",
            self._key_types(keys),
        )
        plans = {plan["left"]: plan for plan in self._column_plans(manifest)}
        pairs = [
            {"left_column": column, "right_column": manifest["col_map"][column]}
            for column in columns
        ]
        selects = [f"{key_sql} AS _key"]
        selects += [f"{quote_ident(key, 'spark')} AS k{i}" for i, key in enumerate(keys)]
        selects += [
            f"{quote_ident(row['right_column'], 'spark')} AS v{i}" for i, row in enumerate(pairs)
        ]
        selects += [f"{plans[row['left_column']]['text']} AS n{i}" for i, row in enumerate(pairs)]
        sql = f"SELECT {', '.join(selects)} FROM {relation} WHERE {where}"
        self._audit(sql, "difference_rows")
        frame = self.spark.sql(sql)
        return {"pairs": pairs, "key_columns": keys, "rows": _iterate(frame, "difference_rows")}
