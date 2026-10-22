from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ..common.model.hashing import collapse_decimal_sql as _collapse_decimal_sql
from ..common.model.manifest import DEFAULT_STATISTICS
from ..common.sql.column_types import resolve_column_type
from ..common.sql.dates import (
    bounds_where,
    canonical_date,
    day_expr,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.sql.query_windows import normalize_query_windows, render_query_windows
from ..common.sql.sampling import sample_predicate_sql

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
SQL_LOG_WIDTH = 4000


# >>> single-line sql for logs, clipped to SQL_LOG_WIDTH <<< #
def sql_summary(sql: str) -> str:
    text = " ".join(sql.split())
    return text if len(text) <= SQL_LOG_WIDTH else text[: SQL_LOG_WIDTH - 3] + "..."


class CsvTarget:
    def __init__(self, config: dict[str, Any]):
        from pair_recon_left.backends.sources import CsvSource

        self.config = dict(config)
        self.source = CsvSource(self.config)

    def set_sql_audit(self, audit):
        self.source.set_sql_audit(audit)
        return self

    def preflight(self):
        health = self.source.preflight()
        self.config.update(self.source.config)
        return health

    def partition_counts(self, from_date=None, to_date=None, **options):
        if self.config.get("pre_sampled"):
            options = {}
        return self.source.partition_counts(from_date, to_date, **options)

    def window_statistics(self, manifest, window):
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
            path = self.source.pull_window(
                window["partitions"],
                columns,
                Path(folder) / "window.csv",
                key_columns=manifest["right_key_cols"],
                sample_plan=manifest["sample"],
                key_families=manifest["key_families"],
                side="right",
                key_tuples=window.get("key_tuples"),
            )
            return compute_csv_statistics(
                path,
                columns,
                strict=manifest.get("strict", False),
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
    def partition_mode(self) -> str:
        return str(self.config.get("partition_mode") or "date")

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
                from_date=from_date,
                to_date=to_date,
            )
            return f"({query}) pair_recon_source"
        parts = [self.config.get(name) for name in ("catalog", "schema", "table")]
        return ".".join(quote_ident(str(part), "spark") for part in parts if part)

    def _where(self, extra: str | None) -> str:
        clauses = [item for item in (self.config.get("where"), extra) if item]
        return " AND ".join(f"({item})" for item in clauses) or "1=1"

    def _collect(self, sql: str, operation: str = "query") -> list:
        log.info("spark.sql %s: %s", operation, sql_summary(sql))
        self._audit(sql, operation)
        started = time.monotonic()
        try:
            rows = self.spark.sql(sql).collect()
        except Exception:
            log.exception("spark.sql %s failed after %.3fs", operation, time.monotonic() - started)
            raise
        log.info("spark.sql %s: %d rows (%.3fs)", operation, len(rows), time.monotonic() - started)
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
            self._date_type = resolved
            self.config["date_type"] = resolved.spec(portable=True)
            return resolved

    def preflight(self) -> dict[str, Any]:
        relation = self.relation()
        log.info(
            "preflight relation=%s kind=%s partition_mode=%s date_col=%s date_type=%s where=%s",
            relation,
            self.config.get("kind") or "table",
            self.partition_mode,
            self.config.get("date_col"),
            self.config.get("date_type"),
            self.config.get("where"),
        )
        try:
            columns = {
                row[0]: str(row[1]).lower()
                for row in self._rows(f"DESCRIBE QUERY SELECT * FROM {relation}", "preflight")
                if row[0] and not str(row[0]).startswith("#")
            }
            log.info("preflight columns: %s", columns)
            if self.partition_mode == "date" and self.config["date_col"] not in columns:
                raise ValueError(f"date_col {self.config['date_col']!r} not in {sorted(columns)}")
            self.column_types = columns
            if self.partition_mode == "date":
                log.info("preflight date_type resolved: %s", self.date_type().spec(portable=True))
            return {
                "ok": True,
                "source": "databricks",
                "relation": relation,
                "columns": sorted(columns),
            }
        except Exception as exc:
            log.exception("preflight failed for relation %s", relation)
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
    ) -> dict[str, int]:
        sampled = sample_predicate_sql(
            sample,
            [quote_ident(key, "spark") for key in key_columns or []],
            key_families or [],
            "spark",
            "right",
        )
        if self.partition_mode == "all":
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
        predicate = f"({bounds}) AND ({sampled})" if bounds and sampled else bounds or sampled
        rows = self._rows(
            f"SELECT {ref}, COUNT(*) FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate)} GROUP BY {ref}",
            "partition_counts",
        )
        counts: dict[str, int] = {}
        for value, count in rows:
            day = canonical_date(value)
            counts[day] = counts.get(day, 0) + int(count)
        return counts

    def _sample_predicate(self, manifest: dict[str, Any], window: dict[str, Any]) -> str | None:
        keys, families = manifest["right_key_cols"], manifest["key_families"]
        return sample_predicate_sql(
            manifest["sample"],
            [quote_ident(key, "spark") for key in keys],
            families,
            "spark",
            "right",
            window.get("key_tuples"),
        )

    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        if not hasattr(self, "column_types"):
            self.preflight()
        if self.partition_mode == "date":
            self.date_type()
        strict = bool(manifest.get("strict"))
        predicate = (
            partition_where(
                self.config["date_col"],
                window["partitions"],
                dialect="spark",
                date_type=self.config.get("date_type"),
            )
            if self.partition_mode == "date"
            else None
        )
        sample = self._sample_predicate(manifest, window)
        filters = " AND ".join(f"({item})" for item in (predicate, sample) if item)
        where = self._where(filters or None)
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        overrides = manifest.get("col_type_overrides") or {}
        column_types = manifest.get("column_types") or {}
        selected = manifest.get("statistics") or list(DEFAULT_STATISTICS)
        include_top = "statistics" not in manifest
        kinds = {}
        selects = ["count(1) AS n_total"] if "n_total" in selected else []
        for i, (left_column, column) in enumerate(zip(manifest["columns"], right_columns)):
            ref = quote_ident(column, "spark")
            column_type = self.column_types.get(column, "")
            native = any(column_type.startswith(t) for t in NUMERIC_TYPES)
            integral = native and any(column_type.startswith(t) for t in INTEGRAL_TYPES)
            comparison = column_types.get(left_column)
            requested = overrides.get(left_column)
            numeric = not comparison and (
                requested == "numeric" or (requested is None and native and not integral)
            )
            kinds[column] = numeric
            text = (
                resolve_column_type(comparison).normalize_sql(ref, "spark", "right")
                if comparison
                else f"trim(cast({ref} as string))"
            )
            if numeric:
                text = _collapse_decimal_sql(text)
            if strict:
                text = f"lower({_collapse_decimal_sql(text) if not numeric else text})"
            value = (
                (f"cast({ref} as double)" if native else f"try_cast({text} as double)")
                if numeric
                else text
            )
            extreme = ref if integral and numeric else value
            if "n_missing" in selected:
                selects.append(f"count_if({ref} IS NULL) AS n_missing_{i}")
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
        log.info(
            "window_statistics partitions=%s columns=%d statistics=%s strict=%s sample=%s",
            window["partitions"],
            len(right_columns),
            selected,
            strict,
            bool(sample),
        )
        sql = (
            f"SELECT {', '.join(selects)} "
            f"FROM {self.relation(partitions=window['partitions'])} WHERE {where}"
        )
        row = self._collect(sql, "window_statistics")[0].asDict()
        stats = {}
        for i, column in enumerate(right_columns):
            numeric = kinds[column]
            payload: dict[str, Any] = {"col_type": "numeric" if numeric else "categorical"}
            if "n_total" in selected:
                payload["n_total"] = int(row["n_total"])
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
            if include_top:
                payload["top_10"] = ""
            stats[column] = payload
        return stats
